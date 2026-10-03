from pathlib import Path
from datetime import datetime, timedelta, timezone
import shutil
import threading

import pytest

import urban_import.distributed as distributed
import urban_import.worker_cli as worker_cli
from urban_import.api import ApiError, TransientReadError, UncertainWrite
from urban_import.config import get_territory
from urban_import.storage import RunPaths, json_digest, read_json, write_json


GEOMETRY = {
    "type": "Polygon",
    "coordinates": [[[37, 55], [37.001, 55], [37.001, 55.001],
                     [37, 55.001], [37, 55]]],
}


def record(index: int, residential: bool = False) -> dict:
    osm_id = f"way/{index + 1}"
    building = {"floors": 2} if residential else None
    return {
        "input_index": index,
        "osm_id": osm_id,
        "source": {"file": "source.geojson", "index": index},
        "physical_object": {
            "physical_object_type_id": 4 if residential else 5,
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
                  residential: bool = False) -> dict:
    return {
        "physical_object_id": object_id,
        "object_geometry_id": geometry_id,
        "physical_object_type": {
            "physical_object_type_id": 4 if residential else 5,
        },
        "territory": {"id": 99},
        "geometry": GEOMETRY,
        "osm_id": osm_id,
        "address": None,
        "name": None,
        "properties": {},
        "building": {"floors": 2} if residential else None,
    }


def build_distribution(tmp_path: Path) -> tuple[Path, dict]:
    existing = [server_object(f"old/{index}", 100 + index, 200 + index)
                for index in range(4)]
    records = [record(index, index % 2 == 0) for index in range(8)]
    assignments = distributed._worker_assignments(existing, records, 4)
    manifest = {"territory": "sample", "records": records}
    backup = {
        "objects": existing,
        "retained": [],
        "relations": {str(item["physical_object_id"]): {
            "services": [],
            "geometries": [{"object_geometry_id": item["object_geometry_id"]}],
        } for item in existing},
        "geometry_links": {str(item["object_geometry_id"]): [{
            "physical_object_id": item["physical_object_id"],
        }] for item in existing},
    }
    master = {
        "format_version": 1,
        "kind": "distributed_master_plan",
        "run_id": "test-run",
        "territory": "sample",
        "territory_id": 99,
        "allowed_type_ids": [4, 5],
        "workers": 4,
        "barrier_poll_seconds": 1,
        "manifest_file": "manifest.json",
        "manifest_sha256": json_digest(manifest),
        "manifest_count": len(records),
        "backup_file": "master-backup.json",
        "backup_sha256": json_digest(backup),
        "descendant_ids": [99],
        "existing_ids": [item["physical_object_id"] for item in existing],
        "existing_fingerprints": distributed.fingerprint(existing),
        "geometry_ids": [item["object_geometry_id"] for item in existing],
        "retained_ids": [],
        "retained_fingerprints": {},
        "building_indexes": [item["input_index"] for item in records
                             if item["building"] is not None],
        "worker_assignments": assignments,
        "counts": {"accepted": len(records)},
        "unresolved": ["replacement_not_confirmed"],
    }
    distributed.validate_worker_partition(master)
    master["sha256"] = json_digest(master)
    run = RunPaths("sample", "test-run", tmp_path / "artifacts" / "sample" / "test-run")
    write_json(run.directory / "master-plan.json", master)
    write_json(run.directory / "manifest.json", manifest)
    write_json(run.directory / "master-backup.json", backup)
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    (runtime / "urban-import.exe").write_bytes(b"fake executable")
    destination = tmp_path / "distribution" / "sample-test-run"
    distributed.package_distribution(run, destination, runtime)
    return destination, master


def complete_worker(bundle: Path, worker_id: int) -> None:
    plan = read_json(bundle / "worker-plan.json")
    state = distributed.WorkerState(bundle, plan)
    state.data.update({
        "stage": "complete",
        "deleted_objects": plan["physical_object_ids"],
        "deleted_geometries": plan["geometry_ids"],
        "created": {str(index): {
            "osm_id": f"way/{index + 1}",
            "physical_object_id": 1000 + index,
            "object_geometry_id": 2000 + index,
        } for index in plan["input_indexes"]},
        "buildings": plan["building_indexes"],
        "pending": None,
        "errors": [],
        "complete": True,
    })
    state.save()


def test_worker_partition_is_stable_complete_and_building_follows_owner():
    existing = [server_object("old", index, index + 100) for index in range(40)]
    accepted = [record(index, index % 3 == 0) for index in range(100)]
    first = distributed._worker_assignments(existing, accepted, 4)
    second = distributed._worker_assignments(list(reversed(existing)), accepted, 4)
    for assignment in second:
        for field in ("physical_object_ids", "geometry_ids", "input_indexes", "building_indexes"):
            assignment[field].sort()
    assert first == second
    master = {
        "workers": 4,
        "existing_ids": [item["physical_object_id"] for item in existing],
        "geometry_ids": [item["object_geometry_id"] for item in existing],
        "manifest_count": len(accepted),
        "building_indexes": [index for index, item in enumerate(accepted)
                             if item["building"] is not None],
        "worker_assignments": first,
    }
    distributed.validate_worker_partition(master)
    for assignment in first:
        assert set(assignment["building_indexes"]) <= set(assignment["input_indexes"])


def test_master_geometry_relation_blocks_service_or_external_owner():
    owner = {20: 10}
    safe = [{
        "physical_object": {"physical_object_id": 10},
        "object_geometry": {"object_geometry_id": 20},
        "service": None,
    }]
    distributed._validate_geometry_links(20, safe, owner)
    with pytest.raises(RuntimeError, match="сервисные связи"):
        distributed._validate_geometry_links(20, [
            {**safe[0], "service": {"service_id": 30}},
        ], owner)
    with pytest.raises(RuntimeError, match="связана извне"):
        distributed._validate_geometry_links(20, [safe[0], {
            "physical_object": {"physical_object_id": 11},
            "object_geometry": {"object_geometry_id": 20},
            "service": None,
        }], owner)

    linked = [{**safe[0], "urban_object_id": 40,
               "service": {"service_id": 30}}]
    issues = distributed._relationship_issues(
        {"physical_object_id": 10, "object_geometry_id": 20},
        {"services": [], "geometries": [{"object_geometry_id": 20}]},
        linked, owner,
    )
    assert issues == [{
        "kind": "object_geometry_relationship",
        "physical_object_id": 10,
        "object_geometry_id": 20,
        "message": "У геометрии 20 есть сервисные связи",
        "urban_object_ids": [40],
        "physical_object_ids": [10],
        "linked_geometry_ids": [20],
        "service_ids": [30],
    }]
    assert distributed._relationship_issues(
        {"physical_object_id": 10, "object_geometry_id": 20},
        {"services": [], "geometries": [{"object_geometry_id": 20}]},
        linked, owner, (30,),
    ) == []


def test_odintsovsky_declares_linked_object_as_retained():
    territory = get_territory("odintsovsky")
    assert [(item.physical_object_id, item.object_geometry_id, item.service_ids)
            for item in territory.retained_server_objects] == [
        (694562, 694559, (735682, 736856)),
    ]


def test_protected_relationship_fingerprint_must_stay_unchanged():
    linked = [{
        "urban_object_id": 1,
        "physical_object": {"physical_object_id": 2},
        "object_geometry": {"object_geometry_id": 3},
        "service": {"service_id": 4},
    }]
    protected = {"3": {
        "object_geometry_id": 3,
        "fingerprint": distributed.relationship_fingerprint(linked),
    }}

    class Api:
        def __init__(self, payload):
            self.payload = payload

        def get(self, path):
            assert path.endswith("object_geometry_id=3")
            return self.payload

    distributed._verify_protected_relationships(Api(linked), protected)
    with pytest.raises(RuntimeError, match="изменились"):
        distributed._verify_protected_relationships(Api([]), protected)


def test_changed_master_plan_or_worker_file_is_rejected(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    (bundle / "master-plan.sha256").write_text("foreign\n", encoding="ascii")
    with pytest.raises(RuntimeError, match="изменён|другому master"):
        distributed.load_worker_bundle(bundle)


def test_portable_folder_contains_mutable_state_and_stop_controls(tmp_path):
    destination, master = build_distribution(tmp_path)
    for worker_id in range(4):
        bundle = destination / f"node-{worker_id + 1}"
        plan, _, _, _ = distributed.load_worker_bundle(bundle)
        assert plan["master_plan_sha256"] == master["sha256"]
        assert read_json(bundle / "state.json")["stage"] == "planned"
        assert (bundle / "events.jsonl").is_file()
        assert (bundle / "stop.ps1").is_file()
        for launcher in ("run.cmd", "resume.cmd", "stop.cmd", "status.cmd",
                         "export-result.cmd"):
            assert (bundle / launcher).read_bytes().startswith(b"@echo off\r\n")
        assert (bundle / "settings.json").is_file()
        assert (bundle / "result").is_dir()
        assert not (bundle / "authorization.json").exists()
        checksums = read_json(bundle / "checksums.json")["files"]
        assert not {"state.json", "events.jsonl", "settings.json"} & checksums.keys()


def test_local_settings_cannot_raise_worker_request_rate(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    plan, _, _, _ = distributed.load_worker_bundle(bundle)
    settings = read_json(bundle / "settings.json")
    settings["request_interval_seconds"] = plan["rate_delay_seconds"] / 2
    write_json(bundle / "settings.json", settings)
    with pytest.raises(RuntimeError, match="settings.json"):
        distributed._worker_settings(bundle, plan)


def test_direct_route_settings_require_valid_ip_pair(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    plan, _, _, _ = distributed.load_worker_bundle(bundle)
    settings = read_json(bundle / "settings.json")
    settings["direct_server_ip"] = "51.250.31.107"
    write_json(bundle / "settings.json", settings)
    with pytest.raises(RuntimeError, match="нужны вместе"):
        distributed._worker_settings(bundle, plan)
    settings["source_ip"] = "192.168.1.33"
    write_json(bundle / "settings.json", settings)
    assert distributed._worker_settings(bundle, plan)["source_ip"] == "192.168.1.33"
    settings["source_ip"] = "invalid"
    write_json(bundle / "settings.json", settings)
    with pytest.raises(RuntimeError, match="IPv4"):
        distributed._worker_settings(bundle, plan)


def test_different_worker_folders_can_run_on_one_computer(tmp_path):
    destination, _ = build_distribution(tmp_path)
    first = destination / "node-1"
    second = destination / "node-2"
    with distributed.worker_lock(first), distributed.worker_lock(second):
        with pytest.raises(RuntimeError, match="второй процесс"):
            with distributed.worker_lock(first):
                pass


def test_stop_after_current_post_does_not_start_next_post(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = next(
        destination / f"node-{worker_id}"
        for worker_id in range(1, 5)
        if len(read_json(destination / f"node-{worker_id}" / "worker-plan.json")[
            "input_indexes"
        ]) >= 2
    )
    plan, inputs, backup, barriers = distributed.load_worker_bundle(bundle)
    assert len(plan["input_indexes"]) >= 2
    state = distributed.WorkerState(bundle, plan)
    state.stage("creating_objects")

    class Api:
        calls = 0

        def post(self, path, body):
            self.calls += 1
            distributed.request_worker_stop(bundle)
            return {
                "physical_object": {"physical_object_id": 5000},
                "object_geometry": {"object_geometry_id": 6000},
            }

    api = Api()
    with distributed.worker_lock(bundle):
        with pytest.raises(distributed.StoppedByOperator):
            distributed._run_worker_once(api, bundle, plan, inputs, backup, barriers, state)
    assert api.calls == 1
    saved = read_json(bundle / "state.json")
    assert saved["stage"] == "creating_objects"
    assert saved["pending"] is None
    assert len(saved["created"]) == 1
    assert saved["errors"] == []


def test_stop_interrupts_barrier_poll_without_server_write(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    plan = read_json(bundle / "worker-plan.json")
    state = distributed.WorkerState(bundle, plan)
    state.stage("waiting_objects_deleted")
    entered = threading.Event()
    result = []

    def wait_for_stop():
        try:
            with distributed.worker_lock(bundle):
                entered.set()
                distributed._wait_at_barrier(state, 30)
        except distributed.StoppedByOperator:
            result.append("stopped")

    thread = threading.Thread(target=wait_for_stop)
    thread.start()
    assert entered.wait(5)
    distributed.request_worker_stop(bundle)
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert result == ["stopped"]
    assert read_json(bundle / "state.json")["stage"] == "waiting_objects_deleted"


def test_stop_on_idle_folder_does_not_leave_stale_request(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    (bundle / "stop-request.json").write_text("stale", encoding="utf-8")
    result = distributed.request_worker_stop(bundle)
    assert result["status"] == "already_stopped"
    assert not (bundle / "stop-request.json").exists()


def test_replayed_stop_signal_is_consumed_once(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    plan = read_json(bundle / "worker-plan.json")
    state = distributed.WorkerState(bundle, plan)
    state.data["last_stop"] = datetime.now(timezone.utc).isoformat()
    state.save()
    write_json(bundle / "stop-request.json", {
        "run_id": plan["run_id"], "worker_id": plan["worker_id"],
        "worker_plan_sha256": plan["sha256"],
        "requested_at": (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat(),
    })
    distributed._check_stop_request(state)
    assert not (bundle / "stop-request.json").exists()
    assert state.data["stage"] == "planned"


def test_stop_interrupts_long_geometry_barrier_scan(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    plan = read_json(bundle / "worker-plan.json")
    state = distributed.WorkerState(bundle, plan)
    state.stage("waiting_geometries_deleted")
    seen = []

    class Api:
        def get(self, path):
            seen.append(path)
            if len(seen) == 1:
                distributed.request_worker_stop(bundle)
            raise ApiError("GET", path, 404, "not found")

    with distributed.worker_lock(bundle):
        with pytest.raises(distributed.StoppedByOperator):
            distributed._first_existing_geometry(
                Api(), [1, 2, 3], lambda: distributed._check_stop_request(state)
            )
    assert len(seen) == 1
    assert read_json(bundle / "state.json")["stage"] == "waiting_geometries_deleted"


def test_geometry_barrier_resumes_from_saved_get_checkpoint(tmp_path, monkeypatch):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    plan, inputs, backup, barriers = distributed.load_worker_bundle(bundle)
    barriers["old_geometry_ids"] = [101, 102, 103, 104]
    monkeypatch.setattr(distributed, "GEOMETRY_BARRIER_CHECKPOINT_EVERY", 2)
    state = distributed.WorkerState(bundle, plan)
    state.stage("waiting_geometries_deleted")

    class InterruptedApi:
        def get(self, path):
            geometry_id = int(path.split("/")[4])
            if geometry_id == 103:
                raise TransientReadError("connection reset")
            raise ApiError("GET", path, 404, "not found")

    with pytest.raises(TransientReadError):
        distributed._run_worker_once(
            InterruptedApi(), bundle, plan, inputs, backup, barriers, state
        )
    assert read_json(bundle / "state.json")["geometry_barrier_index"] == 2
    assert read_json(bundle / "state.json")["geometry_barrier_total"] == 4

    seen = []

    class ResumedApi:
        def get(self, path):
            seen.append(path)
            return []  # Геометрия 103 ещё существует: ждать на ней.

    resumed = distributed.WorkerState(bundle, plan)
    with pytest.raises(distributed.WaitingForPeers, match="103"):
        distributed._run_worker_once(
            ResumedApi(), bundle, plan, inputs, backup, barriers, resumed
        )
    assert seen == ["/api/v1/object_geometries/103/physical_objects"]
    assert read_json(bundle / "state.json")["geometry_barrier_index"] == 2


def test_late_worker_start_accepts_peers_previous_deletions(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    plan, inputs, backup, barriers = distributed.load_worker_bundle(bundle)
    state = distributed.WorkerState(bundle, plan)
    own_ids = set(plan["physical_object_ids"])
    current = [
        server_object(f"old/{index}", 100 + index, 200 + index)
        for index in range(4) if 100 + index in own_ids
    ]

    class Api:
        def export_area_objects(self, territory_id, allowed, strict_count=True):
            assert strict_count is False
            distributed.request_worker_stop(bundle)
            return [99], current

        def delete(self, *args):
            pytest.fail("late worker started a DELETE after stop request")

    with distributed.worker_lock(bundle):
        with pytest.raises(distributed.StoppedByOperator):
            distributed._run_worker_once(Api(), bundle, plan, inputs, backup,
                                         barriers, state)
    assert read_json(bundle / "state.json")["stage"] == "deleting_objects"


def test_default_barrier_wait_auto_continues_without_restart(tmp_path, monkeypatch):
    destination, _ = build_distribution(tmp_path)
    distributed.create_authorization(
        destination, replacement_confirmed=True,
        destructive_apply_authorized=True,
    )
    bundle = destination / "node-1"
    calls = []

    def run_once(api, bundle, plan, inputs, backup, barriers, state):
        calls.append(state.data["stage"])
        if len(calls) == 1:
            state.wait("old_physical_objects", 1)
            raise distributed.WaitingForPeers("Остался worker с другим ID")
        return {"stage": "complete"}

    monkeypatch.setattr(distributed, "_run_worker_once", run_once)
    monkeypatch.setattr(distributed, "_wait_at_barrier", lambda state, seconds: None)

    class Api:
        delay = 0

        def validate_write_contract(self):
            pass

        def get(self, path):
            return {"territory_id": 99}

    result = distributed.worker_run(bundle, Api())
    assert result == {"stage": "complete"}
    assert len(calls) == 2


@pytest.mark.parametrize("failure", [
    UncertainWrite("DELETE response lost"),
    TransientReadError("GET connection reset"),
])
def test_worker_retries_temporary_api_failure_without_losing_pending(
        tmp_path, monkeypatch, failure):
    destination, _ = build_distribution(tmp_path)
    distributed.create_authorization(
        destination, replacement_confirmed=True,
        destructive_apply_authorized=True,
    )
    bundle = destination / "node-1"
    plan = read_json(bundle / "worker-plan.json")
    state = distributed.WorkerState(bundle, plan)
    state.stage("deleting_objects")
    calls = []
    waits = []

    def run_once(api, bundle, plan, inputs, backup, barriers, state):
        calls.append(state.data["pending"])
        if len(calls) == 1:
            if isinstance(failure, UncertainWrite):
                state.begin("delete_object", plan["physical_object_ids"][0])
            raise failure
        if isinstance(failure, UncertainWrite):
            assert state.data["pending"]["action"] == "delete_object"
            state.finish("deleted_objects", plan["physical_object_ids"][0])
        return {"stage": "resumed"}

    monkeypatch.setattr(distributed, "_run_worker_once", run_once)
    monkeypatch.setattr(distributed, "_wait_at_barrier",
                        lambda state, seconds: waits.append(seconds))

    class Api:
        delay = 0

        def validate_write_contract(self):
            pass

        def get(self, path):
            return {"territory_id": 99}

    assert distributed.worker_run(bundle, Api()) == {"stage": "resumed"}
    assert len(calls) == 2
    assert waits == [5.0]
    saved = read_json(bundle / "state.json")
    assert saved["errors"] == []
    assert saved["pending"] is None


def test_worker_stops_on_permanent_invariant_error(tmp_path, monkeypatch):
    destination, _ = build_distribution(tmp_path)
    distributed.create_authorization(
        destination, replacement_confirmed=True,
        destructive_apply_authorized=True,
    )
    bundle = destination / "node-1"
    monkeypatch.setattr(distributed, "_run_worker_once", lambda *args:
                        (_ for _ in ()).throw(RuntimeError("Связи изменились")))

    class Api:
        delay = 0

        def validate_write_contract(self):
            pass

        def get(self, path):
            return {"territory_id": 99}

    with pytest.raises(RuntimeError, match="Связи изменились"):
        distributed.worker_run(bundle, Api())
    assert len(read_json(bundle / "state.json")["errors"]) == 1


def test_full_folder_move_reconciles_pending_delete_and_resumes(tmp_path):
    destination, _ = build_distribution(tmp_path)
    original = destination / "node-1"
    plan = read_json(original / "worker-plan.json")
    object_id = plan["physical_object_ids"][0]
    geometry_id = plan["geometry_ids"][0]
    state = distributed.WorkerState(original, plan)
    state.stage("waiting_objects_deleted")
    state.begin("delete_object", object_id)
    distributed.create_authorization(
        destination, replacement_confirmed=True,
        destructive_apply_authorized=True,
    )
    moved = tmp_path / "другой компьютер" / "worker 1"
    shutil.copytree(original, moved)

    class Api:
        delay = 0

        def validate_write_contract(self):
            pass

        def get(self, path):
            if path.startswith("/api/v1/territory/"):
                return {"territory_id": 99}
            assert path == f"/api/v1/object_geometries/{geometry_id}/physical_objects"
            return []

        def export_area_objects(self, territory_id, allowed):
            return [99], [server_object("old/0", 100, 200)]

        def post(self, *args):
            pytest.fail("POST during pending DELETE reconciliation")

        def delete(self, *args):
            pytest.fail("DELETE during pending DELETE reconciliation")

    result = distributed.worker_run(moved, Api(), wait=False)
    assert result["stage"] == "waiting_objects_deleted"
    assert result["waiting"]
    saved = read_json(moved / "state.json")
    assert saved["pending"] is None
    assert object_id in saved["deleted_objects"]
    assert read_json(original / "state.json")["pending"] is not None


def test_worker_cannot_start_without_separate_authorization(tmp_path):
    destination, _ = build_distribution(tmp_path)

    class Api:
        def validate_write_contract(self):
            pytest.fail("GET OpenAPI before authorization check")

        def get(self, path):
            pytest.fail("GET territory before authorization check")

    with pytest.raises(RuntimeError, match="ЗАБЛОКИРОВАН"):
        distributed.worker_run(destination / "node-1", Api(), wait=False)


def test_worker_cli_reports_preflight_error_without_traceback(tmp_path, monkeypatch,
                                                              capsys):
    monkeypatch.setattr("sys.argv", ["urban-import", "worker-run", "--bundle",
                                  str(tmp_path)])

    def unavailable_api(*args, **kwargs):
        raise RuntimeError("API недоступен")

    monkeypatch.setattr(worker_cli, "worker_run", unavailable_api)
    with pytest.raises(SystemExit) as exit_info:
        worker_cli.main()
    assert exit_info.value.code == 2
    stderr = capsys.readouterr().err
    assert "Ошибка: API недоступен" in stderr
    assert "Traceback" not in stderr


def test_global_barrier_waits_when_another_worker_stopped(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    plan, inputs, backup, barriers = distributed.load_worker_bundle(bundle)
    state = distributed.WorkerState(bundle, plan)
    state.data["stage"] = "waiting_objects_deleted"
    state.save()

    class Api:
        def export_area_objects(self, territory_id, allowed):
            return [99], [server_object("old", barriers["old_physical_object_ids"][0], 999)]

    with pytest.raises(distributed.WaitingForPeers, match="Осталось"):
        distributed._run_worker_once(Api(), bundle, plan, inputs, backup, barriers, state)
    assert read_json(bundle / "state.json")["stage"] == "waiting_objects_deleted"


def test_global_barrier_waits_while_peer_changes_page_count(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    plan, inputs, backup, barriers = distributed.load_worker_bundle(bundle)
    state = distributed.WorkerState(bundle, plan)
    state.stage("waiting_objects_deleted")

    class Api:
        def export_area_objects(self, territory_id, allowed):
            raise distributed.ChangingPageCount("Количество API изменилось")

    with pytest.raises(distributed.WaitingForPeers, match="Состав области"):
        distributed._run_worker_once(Api(), bundle, plan, inputs, backup,
                                     barriers, state)
    assert read_json(bundle / "state.json")["last_wait"]["reason"] == "area_count_changing"


def test_worker_stops_if_retained_object_changed_or_disappeared(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    plan, inputs, backup, barriers = distributed.load_worker_bundle(bundle)
    retained = server_object("point/1", 900, 901)
    barriers["retained_ids"] = [900]
    barriers["retained_fingerprints"] = {"900": distributed.retained_fingerprint(retained)}
    state = distributed.WorkerState(bundle, plan)

    class Api:
        def export_area_objects(self, territory_id, allowed, strict_count=True):
            assert strict_count is False
            return [99], backup["objects"]

    with pytest.raises(RuntimeError, match="Исчезли сохраняемые"):
        distributed._run_worker_once(Api(), bundle, plan, inputs, backup, barriers, state)


def test_uncertain_distributed_post_reconciles_without_duplicate(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    plan, inputs, backup, barriers = distributed.load_worker_bundle(bundle)
    if not plan["input_indexes"]:
        pytest.skip("SHA distribution assigned no records to worker 0")
    index = plan["input_indexes"][0]
    target = next(item for item in inputs["records"] if item["input_index"] == index)
    state = distributed.WorkerState(bundle, plan)
    state.data["stage"] = "creating_objects"
    state.data["pending"] = {"action": "create_object", "key": index, "absent_checks": []}
    state.save()

    class Api:
        def export_area_objects(self, territory_id, allowed, strict_count=True):
            assert strict_count is False
            return [99], [server_object(
                target["osm_id"], 5000, 6000,
                target["physical_object"]["physical_object_type_id"] == 4,
            )]

    distributed._reconcile_worker_pending(Api(), plan, inputs, backup, barriers, state)
    assert state.data["pending"] is None
    assert state.data["created"][str(index)]["physical_object_id"] == 5000


def test_uncertain_distributed_post_stops_on_duplicate(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    plan, inputs, backup, barriers = distributed.load_worker_bundle(bundle)
    if not plan["input_indexes"]:
        pytest.skip("SHA distribution assigned no records to worker 0")
    index = plan["input_indexes"][0]
    target = next(item for item in inputs["records"] if item["input_index"] == index)
    state = distributed.WorkerState(bundle, plan)
    state.data["pending"] = {"action": "create_object", "key": index, "absent_checks": []}
    state.save()
    residential = target["physical_object"]["physical_object_type_id"] == 4

    class Api:
        def export_area_objects(self, territory_id, allowed, strict_count=True):
            assert strict_count is False
            return [99], [server_object(target["osm_id"], 1, 11, residential),
                          server_object(target["osm_id"], 2, 12, residential)]

    with pytest.raises(RuntimeError, match="дубль OSM"):
        distributed._reconcile_worker_pending(Api(), plan, inputs, backup, barriers, state)


def test_uncertain_post_requires_two_separated_absence_checks(tmp_path):
    destination, _ = build_distribution(tmp_path)
    bundle = destination / "node-1"
    plan = read_json(bundle / "worker-plan.json")
    state = distributed.WorkerState(bundle, plan)
    state.begin("create_object", plan["input_indexes"][0])
    with pytest.raises(distributed.WaitingForPeers, match="вторая проверка"):
        distributed._confirm_absence(state, "OSM missing")
    assert state.data["pending"] is not None
    with pytest.raises(distributed.WaitingForPeers, match="через 60 секунд"):
        distributed._confirm_absence(state, "OSM missing")
    state.data["pending"]["absent_checks"][0] = (
        datetime.now(timezone.utc) - timedelta(seconds=61)
    ).isoformat()
    state.save()
    distributed._confirm_absence(state, "OSM missing")
    assert state.data["pending"] is None


def test_collector_supports_legacy_computer_folder_names(tmp_path):
    destination, _ = build_distribution(tmp_path)
    for worker_id in range(4):
        (destination / f"node-{worker_id + 1}").rename(
            destination / f"computer-{worker_id + 1}"
        )
    assert distributed._worker_folder(destination, 1).name == "computer-2"


def test_collect_requires_all_workers_and_checks_exact_unions(tmp_path):
    destination, master = build_distribution(tmp_path)
    results = tmp_path / "results"
    results.mkdir()
    for worker_id in range(4):
        bundle = destination / f"node-{worker_id + 1}"
        complete_worker(bundle, worker_id)
        archive, complete = distributed.export_worker_result(bundle)
        assert complete
        shutil.copy2(archive, results / archive.name)
    collected = distributed.collect_worker_results(destination, results)
    assert collected["counts"]["created_objects"] == master["manifest_count"]
    assert collected["pending"] == []
    first = next(results.iterdir())
    duplicate = results / f"duplicate-{first.name}"
    shutil.copy2(first, duplicate)
    with pytest.raises(RuntimeError, match="несколько раз"):
        distributed.collect_worker_results(destination, results)
    duplicate.unlink()
    first.unlink()
    with pytest.raises(RuntimeError, match="ровно по одному"):
        distributed.collect_worker_results(destination, results)


def test_final_verify_uses_fresh_server_export(tmp_path):
    destination, master = build_distribution(tmp_path)
    results = tmp_path / "results"
    results.mkdir()
    for worker_id in range(4):
        bundle = destination / f"node-{worker_id + 1}"
        complete_worker(bundle, worker_id)
        archive, _ = distributed.export_worker_result(bundle)
        shutil.copy2(archive, results / archive.name)
    collected = distributed.collect_worker_results(destination, results)
    records = read_json(destination / "manifest.json")["records"]
    by_index = {item["input_index"]: item for item in records}
    current = []
    for mapping in collected["mapping"]:
        source = by_index[mapping["input_index"]]
        current.append(server_object(
            source["osm_id"], mapping["physical_object_id"],
            mapping["object_geometry_id"], source["building"] is not None,
        ))

    class Api:
        def export_area_objects(self, territory_id, allowed):
            return master["descendant_ids"], current

        def get(self, path):
            raise ApiError("GET", path, 404, "not found")

    report = distributed.final_verify(Api(), destination)
    assert report["issues"] == []
    assert report["stage"] == "complete"
    # После прерывания checkpoint обязан сохранять и найденные проблемы.
    snapshot = json_digest(sorted(current, key=lambda item: item["physical_object_id"]))
    checkpoint = distributed.PreviewDatabase(destination / "final-verify.sqlite3", snapshot)
    old_geometry = master["geometry_ids"][0]
    checkpoint.put("geometry_links", old_geometry, {"absent": False, "linked": []})
    checkpoint.close()
    resumed = distributed.final_verify(Api(), destination)
    assert f"old_geometry_remains:{old_geometry}" in resumed["issues"]
