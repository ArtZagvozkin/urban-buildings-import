from datetime import datetime, timezone
from pathlib import Path

import pytest

import urban_import.workflow as workflow
from urban_import.api import ApiError
from urban_import.config import TerritoryConfig
from urban_import.storage import RunPaths, json_digest, read_json, write_json


GEOMETRY = {
    "type": "Polygon",
    "coordinates": [[[37, 55], [37.001, 55], [37.001, 55.001],
                     [37, 55.001], [37, 55]]],
}


def territory() -> TerritoryConfig:
    raw = {"stable": "configuration"}
    return TerritoryConfig(
        key="sample", name="Тест", territory_id=99,
        api_name_contains="Тест", allowed_type_ids=(4, 5), sources=(),
        replace_geometry_types=("Polygon", "MultiPolygon"),
        include_descendants=True, expected_preparation={}, raw=raw,
    )


def record(osm_id: str, object_type: int = 5, building=None) -> dict:
    return {
        "osm_id": osm_id,
        "physical_object": {
            "physical_object_type_id": object_type,
            "territory_id": 99,
            "geometry": GEOMETRY,
            "osm_id": osm_id,
            "address": None,
            "name": None,
            "properties": {},
        },
        "building": building,
    }


def server_object(osm_id: str, object_id: int, geometry_id: int,
                  object_type: int = 5, building=None) -> dict:
    return {
        "physical_object_id": object_id,
        "object_geometry_id": geometry_id,
        "physical_object_type": {"physical_object_type_id": object_type},
        "territory": {"id": 99},
        "geometry": GEOMETRY,
        "osm_id": osm_id,
        "address": None,
        "name": None,
        "properties": {},
        "building": building,
    }


def make_run(tmp_path: Path, accepted: list[dict], *, old_geometry_ids=()) -> RunPaths:
    paths = RunPaths("sample", "run", tmp_path / "sample" / "run")
    audit = {"config_sha256": json_digest(territory().raw), "source_sha256": {}}
    backup = {"count": 0, "objects": [], "retained": []}
    write_json(paths.audit, audit)
    write_json(paths.accepted, accepted)
    write_json(paths.backup, backup)
    plan = {
        "format_version": 1,
        "territory": "sample",
        "territory_id": 99,
        "allowed_type_ids": [4, 5],
        "config_sha256": json_digest(territory().raw),
        "audit_sha256": json_digest(audit),
        "accepted_sha256": json_digest(accepted),
        "backup_file": "backup.json",
        "backup_sha256": json_digest(backup),
        "descendant_ids": [99],
        "existing_ids": [],
        "existing_fingerprints": {},
        "retained_ids": [],
        "retained_fingerprints": {},
        "geometry_ids": list(old_geometry_ids),
        "counts": {"source": {}, "accepted": len(accepted)},
        "unresolved": [],
    }
    plan["sha256"] = json_digest(plan)
    write_json(paths.plan, plan)
    return paths


def test_partial_resume_reconciles_uncertain_post_without_duplicate(tmp_path, monkeypatch):
    accepted = [record("way/1"), record("way/2"), record("way/3")]
    paths = make_run(tmp_path, accepted)
    state = workflow.OperationState(paths)
    state.data.update({
        "stage": "creating_objects",
        "created": {"0": {"physical_object_id": 10, "object_geometry_id": 20}},
        "pending": {"action": "create_object", "key": "1", "absent_checks": []},
    })
    state.save()

    class Api:
        posted = []

        def validate_write_contract(self):
            return None

        def export_area_objects(self, territory_id, allowed):
            return [99], [server_object("way/2", 11, 21)]

        def post(self, path, body):
            self.posted.append(body["osm_id"])
            return {
                "physical_object": {"physical_object_id": 12},
                "object_geometry": {"object_geometry_id": 22},
            }

    api = Api()
    monkeypatch.setattr(workflow, "verify_run", lambda api, config, paths: {"issues": []})
    assert workflow.apply_plan(api, territory(), paths) == {"issues": []}
    assert api.posted == ["way/3"]
    final = read_json(paths.state)
    assert set(final["created"]) == {"0", "1", "2"}
    assert final["pending"] is None


def test_uncertain_post_requires_two_separated_absence_checks(tmp_path):
    paths = make_run(tmp_path, [record("way/1")])
    state = workflow.OperationState(paths)
    state.data["pending"] = {
        "action": "create_object", "key": "0", "absent_checks": [],
    }
    state.save()

    class Api:
        def export_area_objects(self, territory_id, allowed):
            return [99], []

    with pytest.raises(RuntimeError, match="нужна повторная"):
        workflow.reconcile_pending(Api(), territory(), state, [record("way/1")])
    state.data["pending"]["absent_checks"][-1] = "2000-01-01T00:00:00+00:00"
    state.save()
    workflow.reconcile_pending(Api(), territory(), state, [record("way/1")])
    assert read_json(paths.state)["pending"] is None


def test_duplicate_match_stops_uncertain_post_reconciliation(tmp_path):
    paths = make_run(tmp_path, [record("way/1")])
    state = workflow.OperationState(paths)
    state.data["pending"] = {
        "action": "create_object", "key": "0", "absent_checks": [],
    }
    state.save()

    class Api:
        def export_area_objects(self, territory_id, allowed):
            return [99], [server_object("way/1", 1, 11),
                          server_object("way/1", 2, 12)]

    with pytest.raises(RuntimeError, match="несколько объектов"):
        workflow.reconcile_pending(Api(), territory(), state, [record("way/1")])


def test_independent_verify_checks_fresh_server_data_and_old_geometry(tmp_path):
    accepted = [record("way/1", 4, {"floors": 2})]
    paths = make_run(tmp_path, accepted, old_geometry_ids=(777,))
    state = workflow.OperationState(paths)
    state.data.update({
        "stage": "verifying",
        "created": {"0": {"physical_object_id": 10, "object_geometry_id": 20}},
        "buildings": [0],
    })
    state.save()

    class Api:
        def export_area_objects(self, territory_id, allowed):
            return [99], [server_object("way/1", 10, 20, 4, {"floors": 2})]

        def get(self, path):
            raise ApiError("GET", path, 404, "not found")

    report = workflow.verify_run(Api(), territory(), paths)
    assert report["issues"] == []
    assert report["counts"]["actual_created"] == 1
    assert not paths.verify_checkpoint.exists()
    assert read_json(paths.state)["stage"] == "complete"
