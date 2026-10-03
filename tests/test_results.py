"""Компактный результат достаточен без прежнего запуска и удалённой записи."""

from copy import deepcopy
import gzip
import json
from pathlib import Path

import pytest

import urban_import.results as results
from urban_import.api import ApiError
from urban_import.storage import read_json
from urban_import.storage import write_json


@pytest.mark.parametrize("territory", ["reutov", "zhukovsky", "lotoshino", "odintsovsky"])
def test_tracked_sources_reproduce_every_original_api_payload(territory):
    report, accepted = results.reproduce(territory)
    entry, data = results.load_result(territory)
    assert report["offline"] and not report["issues"]
    assert report["accepted"] == len(data["mapping"]) == entry["counts"]["accepted"]
    assert report["payload_sha256"] == results.payload_digest(accepted)


@pytest.mark.parametrize("method", ["POST", "DELETE", "PATCH"])
def test_read_only_client_cannot_write(method):
    with pytest.raises(RuntimeError, match="только GET"):
        results.ReadOnlyApi().request(method, "/api/v1/physical_objects")


def test_gzip_is_deterministic_and_readable(tmp_path):
    one, two = tmp_path / "one.json.gz", tmp_path / "two.json.gz"
    results.write_compressed_json(one, {"value": [1, "данные"]})
    results.write_compressed_json(two, {"value": [1, "данные"]})
    assert one.read_bytes() == two.read_bytes()
    assert json.loads(gzip.decompress(one.read_bytes()))["value"][0] == 1


def test_normalized_relationships_keep_external_dependencies_and_share_services():
    service = {"service_id": 7, "properties": {"name": "Сервис"}}
    raw = {"services": [service], "geometries": [{"object_geometry_id": 202}],
           "urban_objects": [
               {"urban_object_id": 1, "physical_object": {"physical_object_id": 102},
                "object_geometry": {"object_geometry_id": 202}, "service": service},
               {"urban_object_id": 2, "physical_object": {"physical_object_id": 999},
                "object_geometry": {"object_geometry_id": 888, "geometry": {"type": "Point", "coordinates": [37, 55]}},
                "service": service}]}
    normalized = results.normalize_relations(raw, 102, 202)
    assert normalized["services"] == [service]
    assert normalized["external_physical_objects"]["999"]["physical_object_id"] == 999
    assert normalized["external_geometries"]["888"]["geometry"] == raw["urban_objects"][1]["object_geometry"]["geometry"]
    assert normalized["urban_objects"][1]["service_id"] == 7


def test_compact_result_rejects_missing_service_payload(tmp_path):
    entry, data = results.load_result("odintsovsky")
    data["service_records"].pop(next(iter(data["service_records"])))
    path = tmp_path / entry["data"]["path"]
    results.write_compressed_json(path, data)
    index = read_json(results.RESULTS / "index.json")
    index["territories"]["odintsovsky"]["data"]["sha256"] = results.file_sha256(path)
    report = tmp_path / entry["report"]["path"]
    report.write_bytes((results.RESULTS / entry["report"]["path"]).read_bytes())
    write_json(tmp_path / "index.json", index)
    with pytest.raises(RuntimeError, match="сервиса"):
        results.load_result("odintsovsky", tmp_path)


def test_result_checksum_rejects_changed_mapping(tmp_path):
    entry, _ = results.load_result("reutov")
    index = read_json(results.RESULTS / "index.json")
    for reference in (entry["data"], entry["report"]):
        path = tmp_path / reference["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((results.RESULTS / reference["path"]).read_bytes())
    (tmp_path / "index.json").write_text(json.dumps(index), encoding="utf-8")
    with (tmp_path / entry["data"]["path"]).open("ab") as stream:
        stream.write(b"changed")
    with pytest.raises(RuntimeError, match="Контрольная сумма"):
        results.load_result("reutov", tmp_path)


@pytest.fixture
def small_result(monkeypatch, tmp_path):
    polygon = {"type": "Polygon", "coordinates": [[[37, 55], [37.01, 55], [37.01, 55.01], [37, 55.01], [37, 55]]]}
    physical = {"geometry": polygon, "territory_id": 5231, "physical_object_type_id": 4,
                "osm_id": "way/1", "properties": {"source": "test"}, "address": "Улица, 1"}
    record = {"osm_id": "way/1", "physical_object": physical, "building": {"floors": 2}}
    imported = {"physical_object_id": 101, "object_geometry_id": 201,
                "physical_object_type": {"physical_object_type_id": 4}, "territory": {"id": 5231},
                "geometry": polygon, "osm_id": "way/1", "properties": {"source": "test"},
                "address": "Улица, 1", "name": None, "building": {"id": 301, "floors": 2}}
    point = {"physical_object_id": 102, "object_geometry_id": 202,
             "physical_object_type": {"physical_object_type_id": 5}, "territory": {"id": 5231},
             "geometry": {"type": "Point", "coordinates": [37, 55]}, "osm_id": None,
             "properties": {}, "address": None, "name": "Сохраняемый объект", "building": None}
    relationships = {"services": [], "geometries": [{"object_geometry_id": 202}], "urban_objects": []}
    data = {"territory": "reutov", "run_id": "test", "original_plan_sha256": "original-plan",
            "mapping": [["way/1", 101, 201, 301]], "descendant_ids": [5231],
            "retained": [{"object": results.normalize_server_object(point), "relations": relationships}],
            "replaced_physical_object_ids": [11], "replaced_geometry_ids": [21]}
    entry = {"data": {"sha256": "result-checksum"}}
    monkeypatch.setattr(results, "load_result", lambda *args: (entry, data))
    monkeypatch.setattr(results, "reproduce", lambda *args: ({}, [record]))
    monkeypatch.setattr(results, "ARTIFACTS", tmp_path / "artifacts")

    class Api:
        def __init__(self):
            self.objects = [deepcopy(imported), deepcopy(point)]
            self.exports = 0
            self.calls = []
            self.relationships = deepcopy(relationships)

        def export_area_objects(self, territory, types):
            self.exports += 1
            return [5231], deepcopy(self.objects)

        def validate_write_contract(self):
            self.calls.append("GET /api/openapi")

        def get(self, path, params=None):
            self.calls.append(path)
            if path.endswith("/services"):
                return self.relationships["services"]
            if path.endswith("/geometries"):
                return self.relationships["geometries"]
            if "urban_objects_by_object_geometry" in path:
                return self.relationships["urban_objects"]
            raise ApiError("GET", path, 404, "absent")

    return Api(), data


def test_independent_verification_uses_new_export_and_checks_ids(small_result):
    api, _ = small_result
    assert not results.verify_current(api, "reutov", check_old_geometries=True)["issues"]
    api.objects[0]["object_geometry_id"] = 999
    report = results.verify_current(api, "reutov")
    assert api.exports == 2
    assert "way/1:object_geometry_id" in report["issues"]


@pytest.mark.parametrize("field,value", [("address", "Другой адрес"), ("properties", {}), ("osm_id", "way/2")])
def test_verification_detects_attribute_changes_despite_same_counts(small_result, field, value):
    api, _ = small_result
    api.objects[0][field] = value
    assert any(issue.endswith(":" + field) for issue in results.verify_current(api, "reutov")["issues"])


def test_verification_detects_retained_service_changes(small_result):
    api, _ = small_result
    api.relationships["services"] = [{"service_id": 7}]
    assert "retained:102:relations" in results.verify_current(api, "reutov")["issues"]


def test_restore_plan_recreates_missing_import_and_retained_point_without_old_ids(small_result, tmp_path):
    api, _ = small_result
    api.objects = []
    plan = results.create_restoration_plan(api, "reutov", tmp_path / "restore.json")
    assert len(plan["create"]) == 2 and not plan["unresolved"]
    assert {r["physical_object"]["geometry"]["type"] for r in plan["create"]} == {"Polygon", "Point"}
    assert all("physical_object_id" not in r["physical_object"] for r in plan["create"])
    assert not plan["destructive_apply_authorized"] and plan["requires_separate_authorization"]
    assert plan["delete"] == []
    with pytest.raises(RuntimeError, match="уже существует"):
        results.create_restoration_plan(api, "reutov", tmp_path / "restore.json")


def test_restore_plan_does_not_duplicate_present_objects(small_result, tmp_path):
    api, _ = small_result
    plan = results.create_restoration_plan(api, "reutov", tmp_path / "restore.json")
    assert plan["create"] == [] and len(plan["preserve"]) == 2


def test_restore_plan_can_add_missing_building_without_recreating_object(small_result, tmp_path):
    api, _ = small_result
    api.objects[0]["building"] = None
    plan = results.create_restoration_plan(api, "reutov", tmp_path / "restore.json")
    assert plan["create"] == [] and not plan["unresolved"]
    assert len(plan["create_buildings"]) == 1
    assert plan["create_buildings"][0]["physical_object_id"] == 101
    assert plan["create_buildings"][0]["body"] == {"floors": 2}


def test_restore_plan_blocks_unknown_objects_and_lost_service_dependencies(small_result, tmp_path):
    api, data = small_result
    data["retained"][0]["relations"]["services"] = [{"service_id": 7}]
    api.objects = [api.objects[0]]
    plan = results.create_restoration_plan(api, "reutov", tmp_path / "restore.json")
    assert any("service_relationship" in issue for issue in plan["unresolved"])
    assert plan["relationship_actions"][0]["requires_new_id_binding"]


def test_status_document_matches_machine_readable_catalog():
    assert (results.ROOT / "docs/status.md").read_text(encoding="utf-8") == results.status_markdown()


@pytest.mark.parametrize("damage", ["pending", "worker_id", "server_id"])
def test_result_semantics_reject_corruption_even_if_file_hash_updated(tmp_path, damage):
    entry, data = results.load_result("odintsovsky")
    index = read_json(results.RESULTS / "index.json")
    if damage == "pending":
        data["completion_evidence"]["pending"] = ["uncertain POST"]
    elif damage == "worker_id":
        data["completion_evidence"]["workers"][1]["worker_id"] = 0
    else:
        data["mapping"][1][1] = data["mapping"][0][1]
    path = tmp_path / entry["data"]["path"]
    results.write_compressed_json(path, data)
    index["territories"]["odintsovsky"]["data"]["sha256"] = results.file_sha256(path)
    report = tmp_path / entry["report"]["path"]
    report.write_bytes((results.RESULTS / entry["report"]["path"]).read_bytes())
    write_json(tmp_path / "index.json", index)
    with pytest.raises(RuntimeError):
        results.load_result("odintsovsky", tmp_path)


@pytest.mark.parametrize("failure_stage", ["create_object", "create_building"])
def test_restore_resume_reconciles_lost_post_and_does_not_duplicate(small_result, tmp_path, failure_stage):
    from urban_import.api import UncertainWrite
    from urban_import.restoration import apply_restoration

    api, _ = small_result
    desired = deepcopy(api.objects[0])
    api.objects = [api.objects[1]]
    path = tmp_path / "restore.json"
    plan = results.create_restoration_plan(api, "reutov", path)
    calls = []
    original_get = api.get

    def get(endpoint, params=None):
        if endpoint.startswith("/api/v1/object_geometries/"):
            geometry_id = int(endpoint.split("/")[-2])
            return [deepcopy(item) for item in api.objects if item["object_geometry_id"] == geometry_id]
        return original_get(endpoint, params)

    def post(endpoint, body):
        calls.append(endpoint)
        if endpoint.endswith("physical_objects"):
            item = deepcopy(desired)
            item.update(physical_object_id=501, object_geometry_id=601, building=None)
            api.objects.append(item)
            response = {"physical_object": {"physical_object_id": 501},
                        "object_geometry": {"object_geometry_id": 601}}
            if failure_stage == "create_object" and calls.count(endpoint) == 1:
                raise UncertainWrite("response lost")
            return response
        item = next(x for x in api.objects if x["physical_object_id"] == body["physical_object_id"])
        item["building"] = {"id": 701, **{k: v for k, v in body.items() if k != "physical_object_id"}}
        if failure_stage == "create_building" and calls.count(endpoint) == 1:
            raise UncertainWrite("building response lost")
        return {"id": 701}

    api.get = get
    api.post = post
    with pytest.raises(UncertainWrite):
        apply_restoration(api, path, plan["sha256"])
    report = apply_restoration(api, path, plan["sha256"])
    assert report["stage"] == "complete" and not report["issues"]
    assert calls.count("/api/v1/physical_objects") == 1
    assert calls.count("/api/v1/buildings") == 1
    assert any(x["physical_object_id"] == 501 for x in report["bindings"])
    apply_restoration(api, path, plan["sha256"])
    assert len(calls) == 2


def test_restore_executor_requires_authorization_for_exact_new_plan(small_result, tmp_path):
    from urban_import.restoration import apply_restoration
    api, _ = small_result
    path = tmp_path / "restore.json"
    results.create_restoration_plan(api, "reutov", path)
    with pytest.raises(RuntimeError, match="разрешения"):
        apply_restoration(api, path, "authorization-from-old-plan")


def test_identical_retained_points_use_known_ids_and_pending_excludes_preserved_twin(small_result, tmp_path):
    from urban_import.restoration import apply_restoration
    from urban_import.storage import RunPaths
    from urban_import.workflow import OperationState
    api, data = small_result
    original = deepcopy(api.objects[1])
    twin = deepcopy(original)
    twin.update(physical_object_id=103, object_geometry_id=203)
    api.objects.append(twin)
    data["retained"].append({"object": results.normalize_server_object(twin),
                             "relations": {"services": [], "geometries": [{"object_geometry_id": 203}], "urban_objects": []}})
    intact = results.create_restoration_plan(api, "reutov", tmp_path / "intact.json")
    assert not intact["unresolved"] and not intact["create"] and len(intact["preserve"]) == 3
    api.objects = [item for item in api.objects if item["physical_object_id"] != 102]
    path = tmp_path / "missing.json"
    plan = results.create_restoration_plan(api, "reutov", path)
    assert len(plan["create"]) == 1 and not plan["unresolved"]
    state = OperationState(RunPaths("reutov", "test", tmp_path / "missing-execution"), path)
    state.data["stage"] = "creating_objects"
    state.save()
    state.begin("create_object", "0")
    restored = deepcopy(original)
    restored.update(physical_object_id=501, object_geometry_id=601)
    api.objects.append(restored)
    # Нет метода post: попытка слепой повторной записи немедленно уронит тест.
    report = apply_restoration(api, path, plan["sha256"])
    assert not report["issues"] and report["stage"] == "complete"
