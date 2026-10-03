"""Выполнение worker, серверные барьеры и сбор результатов."""

from __future__ import annotations
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import ipaddress
import json
import math
import os
from pathlib import Path
import tempfile
import time
import zipfile
from shapely.geometry import shape
from .api import ApiError, ChangingPageCount, TransientReadError, UncertainWrite, UrbanApi
from .storage import json_digest, read_json, write_json
from .workflow import fingerprint, retained_fingerprint
from .distributed_plan import (
    DEFAULT_BARRIER_POLL_SECONDS,
    MASTER_PREVIEW_REQUESTS_PER_SECOND,
    PreviewDatabase,
    WORKER_RATE_DELAY,
    _assert_signed,
    _fetch_geometry_links,
    _fetch_relation,
    _fetch_relationship_job,
    _file_sha256,
    _relationship_issues,
    _server_boundary_issues,
    _unsigned_digest,
    _validate_geometry_links,
    _validate_object_relation,
    _validate_prepared_sources,
    _verify_file_checksums,
    _worker_assignments,
    _write_checksums,
    create_distributed_master_plan,
    relationship_fingerprint,
    stable_worker,
    validate_worker_partition,
)
from .worker_package import build_worker_runtime, package_distribution

GEOMETRY_BARRIER_CHECKPOINT_EVERY = 100


MUTABLE_WORKER_FILES = {
    "authorization.json", "state.json", "events.jsonl", "settings.json",
    "worker.lock", "stop-request.json", "checksums.json",
}


class WaitingForPeers(RuntimeError):
    """Worker безопасно остановлен на глобальном серверном барьере."""


class StoppedByOperator(RuntimeError):
    """Worker завершил текущую безопасную точку после stop.ps1."""


class WorkerState:
    """Атомарное состояние одного worker, независимое от других компьютеров."""

    def __init__(self, bundle: Path, plan: dict):
        self.bundle = bundle
        self.plan = plan
        self.path = bundle / "state.json"
        self.events = bundle / "events.jsonl"
        if self.path.exists():
            self.data = read_json(self.path)
            if self.data["worker_plan_sha256"] != plan["sha256"]:
                raise RuntimeError("State относится к другому worker plan")
        else:
            self.data = {
                "worker_plan_sha256": plan["sha256"], "stage": "planned",
                "deleted_objects": [], "deleted_geometries": [],
                "created": {}, "buildings": [], "pending": None,
                "last_wait": None, "errors": [], "resolved_errors": [],
                "last_stop": None, "complete": False,
            }
            self.save()
        self.events.touch(exist_ok=True)

    def save(self) -> None:
        write_json(self.path, self.data)

    def log(self, action: str, detail) -> None:
        with self.events.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({
                "at": datetime.now(timezone.utc).isoformat(), "action": action,
                "detail": detail,
            }, ensure_ascii=False, separators=(",", ":")) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def begin(self, action: str, key) -> None:
        if self.data["pending"]:
            raise RuntimeError("Не разрешена предыдущая pending-операция")
        self.data["pending"] = {"action": action, "key": key, "absent_checks": []}
        self.save()
        self.log("begin", self.data["pending"])

    def finish(self, field: str, key, value=None) -> None:
        if field == "created":
            self.data[field][str(key)] = value
        else:
            self.data[field].append(key)
        self.log("done", {"field": field, "key": key, "value": value})
        self.data["pending"] = None
        self.data["last_retry"] = None
        self.save()

    def stage(self, value: str) -> None:
        self.data["stage"] = value
        self.data["last_wait"] = None
        self.save()
        self.log("stage", value)

    def wait(self, reason: str, remaining: int) -> None:
        self.data["last_wait"] = {
            "reason": reason, "remaining": remaining,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        self.save()
        self.log("waiting", self.data["last_wait"])


@contextmanager
def worker_lock(bundle: Path):
    """Запретить второй локальный процесс той же worker-папки."""

    path = bundle / "worker.lock"
    stream = None
    try:
        stream = path.open("a+b")
        stream.seek(0)
        if stream.read(1) == b"":
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if stream is not None:
            stream.close()
        raise RuntimeError("Уже запущен второй процесс этой worker-папки") from exc
    try:
        yield
    finally:
        stream.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        stream.close()


def load_worker_bundle(bundle: Path) -> tuple[dict, dict, dict, dict]:
    bundle = Path(bundle).resolve()
    _verify_file_checksums(bundle, bundle / "checksums.json")
    plan = read_json(bundle / "worker-plan.json")
    _assert_signed(plan, "worker plan")
    master_sha = (bundle / "master-plan.sha256").read_text(encoding="ascii").strip()
    if master_sha != plan["master_plan_sha256"]:
        raise RuntimeError("Worker-папка относится к другому master plan")
    inputs, backup, barriers = (
        read_json(bundle / "input.json"), read_json(bundle / "backup.json"),
        read_json(bundle / "barriers.json"),
    )
    if (json_digest(inputs) != plan["input_sha256"] or
            json_digest(backup) != plan["backup_sha256"] or
            json_digest(barriers) != plan["barriers_sha256"]):
        raise RuntimeError("Worker data не соответствует worker plan")
    return plan, inputs, backup, barriers


def _worker_settings(bundle: Path, plan: dict) -> dict:
    """Разрешить локально только более медленный темп, чем подписанный план."""

    settings = read_json(bundle / "settings.json")
    limits = {
        "request_interval_seconds": float(plan["rate_delay_seconds"]),
        "barrier_poll_seconds": float(plan["barrier_poll_seconds"]),
    }
    for field, minimum in limits.items():
        value = settings.get(field)
        if (isinstance(value, bool) or not isinstance(value, (int, float)) or
                not math.isfinite(value) or value < minimum):
            raise RuntimeError(
                f"settings.json: {field} должен быть не меньше {minimum}"
            )
    direct_server_ip = settings.get("direct_server_ip")
    source_ip = settings.get("source_ip")
    if bool(direct_server_ip) != bool(source_ip):
        raise RuntimeError("settings.json: direct_server_ip и source_ip нужны вместе")
    if direct_server_ip:
        try:
            ipaddress.IPv4Address(direct_server_ip)
            ipaddress.IPv4Address(source_ip)
        except (ValueError, ipaddress.AddressValueError) as exc:
            raise RuntimeError("settings.json: прямой маршрут требует IPv4") from exc
    return settings


def request_worker_stop(bundle: Path) -> dict:
    """Атомарно попросить работающий worker остановиться на безопасной точке."""

    bundle = Path(bundle).resolve()
    plan, _, _, _ = load_worker_bundle(bundle)
    try:
        with worker_lock(bundle):
            # Если процесс уже завершился, прежний сигнал не должен останавливать
            # его следующий запуск. Состояние операций при этом не меняется.
            (bundle / "stop-request.json").unlink(missing_ok=True)
            return {
                "run_id": plan["run_id"], "worker_id": plan["worker_id"],
                "status": "already_stopped",
            }
    except RuntimeError as exc:
        if str(exc) != "Уже запущен второй процесс этой worker-папки":
            raise
    request = {
        "run_id": plan["run_id"], "worker_id": plan["worker_id"],
        "worker_plan_sha256": plan["sha256"],
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json(bundle / "stop-request.json", request)
    return {**request, "status": "stop_requested"}


def _check_stop_request(state: WorkerState) -> None:
    """Завершить работу между операциями, оставив pending для сверки при resume."""

    path = state.bundle / "stop-request.json"
    if not path.exists():
        return
    request = read_json(path)
    if (request.get("run_id") != state.plan["run_id"] or
            request.get("worker_id") != state.plan["worker_id"] or
            request.get("worker_plan_sha256") != state.plan["sha256"]):
        raise RuntimeError("Запрос остановки относится к другому worker plan")
    last_stop = state.data.get("last_stop")
    if (last_stop and datetime.fromisoformat(request["requested_at"]) <=
            datetime.fromisoformat(last_stop)):
        # Повторно появившийся уже обработанный сигнал не является новой остановкой.
        path.unlink()
        state.log("stale_stop_discarded", {"requested_at": request["requested_at"]})
        return
    state.data["last_stop"] = datetime.now(timezone.utc).isoformat()
    state.save()
    state.log("stopped", {"stage": state.data["stage"],
                          "requested_at": request["requested_at"]})
    path.unlink()
    raise StoppedByOperator("Штатная остановка; запустите run.ps1 для продолжения")


def _wait_at_barrier(state: WorkerState, seconds: float) -> None:
    """Ожидать серверный барьер с быстрым откликом на stop.ps1."""

    deadline = time.monotonic() + seconds
    while True:
        _check_stop_request(state)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(1.0, remaining))


def _validate_authorization(bundle: Path, plan: dict) -> None:
    path = bundle / "authorization.json"
    if not path.is_file():
        raise RuntimeError(
            "DESTRUCTIVE APPLY ЗАБЛОКИРОВАН: нет authorization.json основного узла"
        )
    document = read_json(path)
    _assert_signed(document, "authorization")
    expected = {
        "master_plan_sha256": plan["master_plan_sha256"],
        "run_id": plan["run_id"], "territory_id": plan["territory_id"],
        "replacement_confirmed": True, "destructive_apply_authorized": True,
    }
    if any(document.get(key) != value for key, value in expected.items()):
        raise RuntimeError("Authorization не подтверждает этот master plan")


def create_authorization(distribution: Path, *, replacement_confirmed: bool,
                         destructive_apply_authorized: bool) -> list[Path]:
    """Создать отдельный допуск только после двух явных подтверждений."""

    if not replacement_confirmed or not destructive_apply_authorized:
        raise RuntimeError("Требуются оба явных подтверждения")
    distribution = Path(distribution)
    _verify_file_checksums(distribution, distribution / "checksums.json")
    master = read_json(distribution / "master-plan.json")
    _assert_signed(master, "master plan")
    document = {
        "master_plan_sha256": master["sha256"], "run_id": master["run_id"],
        "territory_id": master["territory_id"], "replacement_confirmed": True,
        "destructive_apply_authorized": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    document["sha256"] = json_digest(document)
    paths = []
    for worker_id in range(master["workers"]):
        path = _worker_folder(distribution, worker_id) / "authorization.json"
        write_json(path, document)
        paths.append(path)
    return paths


def _worker_folder(distribution: Path, worker_id: int) -> Path:
    """Найти папку worker; старое имя допускается для уже работающих копий."""

    node = distribution / f"node-{worker_id + 1}"
    legacy = distribution / f"computer-{worker_id + 1}"
    if node.is_dir() and legacy.is_dir():
        raise RuntimeError(f"Две папки worker {worker_id}: node и computer")
    if node.is_dir():
        return node
    if legacy.is_dir():
        return legacy
    raise RuntimeError(f"Отсутствует папка worker {worker_id}")


def _worker_snapshot(api: UrbanApi, plan: dict, barriers: dict,
                     *, allow_changing_count: bool = False) -> list[dict]:
    """Читать область; ослаблять счётчик только при старте на фоне DELETE коллег."""

    options = {"strict_count": False} if allow_changing_count else {}
    descendants, objects = api.export_area_objects(
        plan["territory_id"], tuple(plan["allowed_type_ids"]), **options
    )
    if descendants != barriers["descendant_ids"]:
        raise RuntimeError("Иерархия территории изменилась после master plan")
    return objects


def _stable_barrier_snapshot(api: UrbanApi, plan: dict, barriers: dict,
                             state: WorkerState) -> list[dict]:
    """На барьере изменение счётчика означает ожидание соседнего worker."""

    try:
        return _worker_snapshot(api, plan, barriers)
    except ChangingPageCount as exc:
        state.wait("area_count_changing", 0)
        raise WaitingForPeers("Состав области ещё меняется") from exc


def _verify_protected_relationships(api: UrbanApi, protected: dict) -> None:
    """Убедиться, что явно сохраняемые сервисные связи не изменились."""

    for relationship in protected.values():
        geometry_id = relationship["object_geometry_id"]
        linked = api.get(
            "/api/v1/urban_objects_by_object_geometry"
            f"?object_geometry_id={geometry_id}"
        )
        if relationship_fingerprint(linked) != relationship["fingerprint"]:
            raise RuntimeError(
                f"Связи сохраняемой геометрии {geometry_id} изменились после master plan"
            )


def _record_by_index(inputs: dict) -> dict[int, dict]:
    return {int(record["input_index"]): record for record in inputs["records"]}


def _confirm_absence(state: WorkerState, description: str) -> None:
    pending = state.data["pending"]
    now = datetime.now(timezone.utc)
    checks = pending.setdefault("absent_checks", [])
    if checks and (now - datetime.fromisoformat(checks[-1])).total_seconds() < 60:
        state.save()
        state.wait("uncertain_post_confirmation", 1)
        raise WaitingForPeers(f"{description}: повторная проверка допустима через 60 секунд")
    checks.append(now.isoformat())
    state.save()
    state.log("uncertain_post_absent", {"description": description, "count": len(checks)})
    if len(checks) < 2:
        state.wait("uncertain_post_confirmation", 1)
        raise WaitingForPeers(f"{description}: нужна вторая проверка отсутствия через 60 секунд")
    state.data["pending"] = None
    state.save()


def _reconcile_worker_pending(api: UrbanApi, plan: dict, inputs: dict,
                              backup: dict, barriers: dict, state: WorkerState) -> None:
    pending = state.data["pending"]
    if not pending:
        return
    action, key = pending["action"], pending["key"]
    records = _record_by_index(inputs)
    if action == "delete_object":
        item = next(value for value in backup["objects"]
                    if value["physical_object_id"] == key)
        try:
            linked = api.get(
                f"/api/v1/object_geometries/{item['object_geometry_id']}/physical_objects"
            )
        except ApiError as exc:
            if exc.status != 404:
                raise
            linked = []
        if not any(value["physical_object_id"] == key for value in linked):
            state.finish("deleted_objects", key)
        else:
            state.data["pending"] = None
            state.save()
    elif action == "delete_geometry":
        try:
            api.get(f"/api/v1/object_geometries/{key}/physical_objects")
        except ApiError as exc:
            if exc.status == 404:
                state.finish("deleted_geometries", key)
                return
            raise
        state.data["pending"] = None
        state.save()
    elif action == "create_object":
        record = records[int(key)]
        # Другие worker могут одновременно создавать объекты: count меняется.
        # Искать нужно все записи с OSM ID, чтобы чужая геометрия или тип не
        # выглядели как «отсутствие» и не вызвали повторный POST.
        same_osm = [item for item in _worker_snapshot(
            api, plan, barriers, allow_changing_count=True
        ) if item.get("osm_id") == record["osm_id"]]
        if len(same_osm) == 1:
            item = same_osm[0]
            if (item["physical_object_type"]["physical_object_type_id"] !=
                    record["physical_object"]["physical_object_type_id"] or
                    not shape(item["geometry"]).equals(
                        shape(record["physical_object"]["geometry"]))):
                raise RuntimeError(f"OSM {record['osm_id']} найден с другим типом или геометрией")
            state.finish("created", key, {
                "osm_id": record["osm_id"],
                "physical_object_id": item["physical_object_id"],
                "object_geometry_id": item["object_geometry_id"],
            })
        elif not same_osm:
            _confirm_absence(state, f"OSM {record['osm_id']}")
        else:
            raise RuntimeError(f"Найден дубль OSM {record['osm_id']}")
    elif action == "create_building":
        ids = state.data["created"][str(key)]
        linked = api.get(
            f"/api/v1/object_geometries/{ids['object_geometry_id']}/physical_objects"
        )
        matches = [item for item in linked
                   if item["physical_object_id"] == ids["physical_object_id"]]
        if len(matches) != 1:
            raise RuntimeError("Связь объекта для building изменилась")
        if matches[0].get("building"):
            state.finish("buildings", key)
        else:
            _confirm_absence(state, f"building объекта {ids['physical_object_id']}")
    else:
        raise RuntimeError(f"Неизвестная pending-операция: {action}")


def _first_existing_geometry(api: UrbanApi, geometry_ids: list[int],
                             before_get=None, *, start_index: int = 0,
                             on_absent=None) -> int | None:
    """Найти старую геометрию и продолжить GET-обход с подтверждённой позиции."""

    for index in range(start_index, len(geometry_ids)):
        if before_get is not None:
            before_get()
        geometry_id = geometry_ids[index]
        try:
            api.get(f"/api/v1/object_geometries/{geometry_id}/physical_objects")
        except ApiError as exc:
            if exc.status == 404:
                if on_absent is not None:
                    on_absent(index + 1)
                continue
            raise
        return geometry_id
    return None


def _run_worker_once(api: UrbanApi, bundle: Path, plan: dict, inputs: dict,
                     backup: dict, barriers: dict, state: WorkerState) -> dict:
    records = _record_by_index(inputs)
    _reconcile_worker_pending(api, plan, inputs, backup, barriers, state)
    _check_stop_request(state)
    if state.data["stage"] == "planned":
        # Полный master plan уже фиксирует область. Пока коллеги удаляют свои ID,
        # общий count меняется; каждый увиденный ID всё равно сверяется с планом,
        # а собственные и сохраняемые ID обязаны присутствовать и совпадать.
        current = _worker_snapshot(api, plan, barriers,
                                   allow_changing_count=True)
        by_id = {item["physical_object_id"]: item for item in current}
        unexpected = set(by_id) - set(barriers["old_physical_object_ids"]) - set(barriers["retained_ids"])
        if unexpected:
            raise RuntimeError(f"До старта появились неожиданные объекты: {len(unexpected)}")
        for object_id in barriers["old_physical_object_ids"]:
            if object_id in by_id and json_digest(by_id[object_id]) != barriers["old_fingerprints"][str(object_id)]:
                raise RuntimeError(f"Серверный объект {object_id} изменился после master plan")
        missing_retained = set(barriers["retained_ids"]) - by_id.keys()
        if missing_retained:
            raise RuntimeError(f"Исчезли сохраняемые объекты: {len(missing_retained)}")
        for object_id in barriers["retained_ids"]:
            if retained_fingerprint(by_id[object_id]) != barriers["retained_fingerprints"][str(object_id)]:
                raise RuntimeError(f"Сохраняемый объект {object_id} изменился после master plan")
        _verify_protected_relationships(
            api, barriers.get("protected_relationships", {})
        )
        missing_own = set(plan["physical_object_ids"]) - by_id.keys()
        if missing_own:
            raise RuntimeError("Своя часть уже изменена; возможен повтор worker ID")
        state.stage("deleting_objects")
    if state.data["stage"] == "deleting_objects":
        done = set(state.data["deleted_objects"])
        backup_by_id = {item["physical_object_id"]: item for item in backup["objects"]}
        for object_id in plan["physical_object_ids"]:
            _check_stop_request(state)
            if object_id in done:
                continue
            live = {
                "services": api.get(f"/api/v1/physical_objects/{object_id}/services"),
                "geometries": api.get(f"/api/v1/physical_objects/{object_id}/geometries"),
            }
            expected_geometry = backup_by_id[object_id]["object_geometry_id"]
            if live["services"] or {item["object_geometry_id"] for item in live["geometries"]} != {expected_geometry}:
                raise RuntimeError(f"Связи объекта {object_id} изменились перед DELETE")
            _check_stop_request(state)
            state.begin("delete_object", object_id)
            api.delete(f"/api/v1/physical_objects/{object_id}")
            state.finish("deleted_objects", object_id)
        state.stage("waiting_objects_deleted")
    if state.data["stage"] == "waiting_objects_deleted":
        _check_stop_request(state)
        current_ids = {item["physical_object_id"] for item in
                       _stable_barrier_snapshot(api, plan, barriers, state)}
        remaining = current_ids & set(barriers["old_physical_object_ids"])
        if remaining:
            state.wait("old_physical_objects", len(remaining))
            raise WaitingForPeers(f"Осталось физических объектов: {len(remaining)}")
        state.stage("deleting_geometries")
    if state.data["stage"] == "deleting_geometries":
        done = set(state.data["deleted_geometries"])
        for geometry_id in plan["geometry_ids"]:
            _check_stop_request(state)
            if geometry_id in done:
                continue
            linked = api.get(f"/api/v1/object_geometries/{geometry_id}/physical_objects")
            if linked:
                raise RuntimeError(f"Геометрия {geometry_id} всё ещё используется")
            _check_stop_request(state)
            state.begin("delete_geometry", geometry_id)
            api.delete(f"/api/v1/object_geometries/{geometry_id}")
            state.finish("deleted_geometries", geometry_id)
        state.stage("waiting_geometries_deleted")
    if state.data["stage"] == "waiting_geometries_deleted":
        _check_stop_request(state)
        old_geometry_ids = barriers["old_geometry_ids"]
        total_geometries = len(old_geometry_ids)
        checked = state.data.get("geometry_barrier_index", 0)
        if (isinstance(checked, bool) or not isinstance(checked, int) or
                checked < 0 or checked > total_geometries):
            raise RuntimeError("Некорректная позиция проверки старых геометрий")
        if (state.data.get("geometry_barrier_total") != total_geometries or
                "geometry_barrier_index" not in state.data):
            state.data["geometry_barrier_total"] = total_geometries
            state.data["geometry_barrier_index"] = checked
            state.save()

        def record_absent_geometry(next_index: int) -> None:
            # Подтверждённый GET 404 сохраняется для продолжения; независимая
            # финальная сверка повторно проверит все старые ID после импорта.
            state.data["geometry_barrier_index"] = next_index
            clear_old_error = state.data.get("last_retry") is not None
            if (next_index % GEOMETRY_BARRIER_CHECKPOINT_EVERY == 0 or
                    next_index == total_geometries or clear_old_error):
                state.data["last_retry"] = None
                state.save()
                state.log("geometry_barrier_progress", {
                    "checked": next_index, "total": total_geometries,
                })

        existing_geometry = _first_existing_geometry(
            api, old_geometry_ids,
            lambda: _check_stop_request(state),
            start_index=checked, on_absent=record_absent_geometry,
        )
        if existing_geometry is not None:
            state.wait("old_geometries", 1)
            raise WaitingForPeers(f"Старая геометрия ещё существует: {existing_geometry}")
        state.stage("creating_objects")
    if state.data["stage"] == "creating_objects":
        for input_index in plan["input_indexes"]:
            _check_stop_request(state)
            if str(input_index) in state.data["created"]:
                continue
            record = records[input_index]
            state.begin("create_object", input_index)
            response = api.post("/api/v1/physical_objects", record["physical_object"])
            state.finish("created", input_index, {
                "osm_id": record["osm_id"],
                "physical_object_id": response["physical_object"]["physical_object_id"],
                "object_geometry_id": response["object_geometry"]["object_geometry_id"],
            })
        state.stage("creating_buildings")
    if state.data["stage"] == "creating_buildings":
        done = set(state.data["buildings"])
        for input_index in plan["building_indexes"]:
            _check_stop_request(state)
            if input_index in done:
                continue
            record = records[input_index]
            state.begin("create_building", input_index)
            api.post("/api/v1/buildings", {
                "physical_object_id": state.data["created"][str(input_index)]["physical_object_id"],
                **record["building"],
            })
            state.finish("buildings", input_index)
        state.stage("waiting_expected_objects")
    if state.data["stage"] == "waiting_expected_objects":
        _check_stop_request(state)
        current = _stable_barrier_snapshot(api, plan, barriers, state)
        by_osm: dict[str, list[dict]] = {}
        for item in current:
            if item.get("osm_id"):
                by_osm.setdefault(item["osm_id"], []).append(item)
        expected_records = {item["osm_id"]: item for item in barriers["expected"]}
        expected = set(expected_records)
        duplicates = [osm for osm in expected if len(by_osm.get(osm, [])) > 1]
        if duplicates:
            raise RuntimeError(f"На сервере появились дубли OSM: {duplicates[:5]}")
        missing = expected - by_osm.keys()
        if missing:
            state.wait("expected_osm_ids", len(missing))
            raise WaitingForPeers(f"Ожидаются OSM ID: {len(missing)}")
        for osm_id, expected_record in expected_records.items():
            item = by_osm[osm_id][0]
            type_id = item["physical_object_type"]["physical_object_type_id"]
            if type_id != expected_record["physical_object_type_id"]:
                raise RuntimeError(f"У OSM {osm_id} появился неверный тип")
            if json_digest(item["geometry"]) != expected_record["geometry_sha256"]:
                # Сериализация колец может отличаться; окончательная проверка использует
                # топологическое равенство. На барьере оно тоже безопаснее сырого JSON.
                if not shape(item["geometry"]).equals(shape(expected_record["geometry"])):
                    raise RuntimeError(f"У OSM {osm_id} появилась неверная геометрия")
        if state.data["errors"]:
            resolved_at = datetime.now(timezone.utc).isoformat()
            state.data.setdefault("resolved_errors", []).extend([
                {**item, "resolved_at": resolved_at} for item in state.data["errors"]
            ])
            state.data["errors"] = []
        state.data["complete"] = True
        state.stage("complete")
    return worker_status(bundle)


def _progress_counts(state: WorkerState) -> tuple[int, int, int, int]:
    return (len(state.data["deleted_objects"]), len(state.data["deleted_geometries"]),
            len(state.data["created"]), len(state.data["buildings"]))


def _retryable_api_failure(exc: Exception) -> bool:
    """Повторять лишь временные HTTP/сетевые сбои; ошибки плана завершают этап."""

    return (isinstance(exc, (TransientReadError, UncertainWrite)) or
            isinstance(exc, ApiError) and exc.status in {429, 500, 502, 503, 504})


def _wait_after_api_failure(state: WorkerState, exc: Exception,
                            consecutive_failures: int) -> None:
    """Сохранить причину и ждать с ограниченным backoff перед сверкой pending."""

    if isinstance(exc, UncertainWrite) and state.data["pending"] is None:
        raise RuntimeError("Неопределённая запись без сохранённой pending-операции") from exc
    seconds = min(60.0, 5.0 * 2 ** min(consecutive_failures - 1, 4))
    if isinstance(exc, ApiError) and exc.retry_after is not None:
        if math.isfinite(exc.retry_after) and exc.retry_after > 0:
            seconds = max(seconds, min(300.0, exc.retry_after))
    detail = {
        "at": datetime.now(timezone.utc).isoformat(),
        "stage": state.data["stage"],
        "error": str(exc),
        "seconds": seconds,
        "pending": state.data["pending"],
    }
    state.data["last_retry"] = detail
    state.save()
    state.log("transient_retry", detail)
    _wait_at_barrier(state, seconds)


def worker_run(bundle: Path, api: UrbanApi, *, dry_run: bool = False,
               wait: bool = True) -> dict:
    """Проверить или продолжить свой worker plan, не захватывая чужую работу."""

    bundle = Path(bundle).resolve()
    with worker_lock(bundle):
        plan, inputs, backup, barriers = load_worker_bundle(bundle)
        settings = _worker_settings(bundle, plan)
        if settings.get("direct_server_ip"):
            api.configure_direct_address(settings["direct_server_ip"],
                                         settings["source_ip"])
        # Обычный запуск без отдельного допуска завершается до сетевых GET.
        # Так недоступность API не скрывает причину блокировки записи.
        if not dry_run:
            _validate_authorization(bundle, plan)
        # Параметр CLI не может ослабить общий лимит, зафиксированный планом.
        api.delay = max(float(getattr(api, "delay", 0)),
                        float(settings["request_interval_seconds"]))
        summary = {"run_id": plan["run_id"], "worker_id": plan["worker_id"],
                   "counts": plan["counts"], "master_plan_sha256": plan["master_plan_sha256"]}
        if dry_run:
            api.validate_write_contract()
            territory = api.get(f"/api/v1/territory/{plan['territory_id']}")
            if territory["territory_id"] != plan["territory_id"]:
                raise RuntimeError("API вернул другую территорию")
            return {**summary, "dry_run": True, "destructive_apply": "BLOCKED"}
        state = WorkerState(bundle, plan)
        preflight_complete = False
        consecutive_failures = 0
        while True:
            before = _progress_counts(state)
            try:
                if not preflight_complete:
                    api.validate_write_contract()
                    territory = api.get(f"/api/v1/territory/{plan['territory_id']}")
                    if territory["territory_id"] != plan["territory_id"]:
                        raise RuntimeError("API вернул другую территорию")
                    preflight_complete = True
                    if state.data.get("last_retry") is not None:
                        state.data["last_retry"] = None
                        state.save()
                return _run_worker_once(api, bundle, plan, inputs, backup, barriers, state)
            except StoppedByOperator as exc:
                return {**summary, "stage": state.data["stage"],
                        "stopped": str(exc), "complete": False}
            except WaitingForPeers as exc:
                consecutive_failures = 0
                if not wait:
                    return {**summary, "stage": state.data["stage"], "waiting": str(exc)}
                try:
                    _wait_at_barrier(state, settings["barrier_poll_seconds"])
                except StoppedByOperator as stop:
                    return {**summary, "stage": state.data["stage"],
                            "stopped": str(stop), "complete": False}
            except Exception as exc:
                if _retryable_api_failure(exc) and not (
                    isinstance(exc, UncertainWrite) and state.data["pending"] is None
                ):
                    if _progress_counts(state) != before:
                        consecutive_failures = 0
                    consecutive_failures += 1
                    try:
                        _wait_after_api_failure(state, exc, consecutive_failures)
                    except StoppedByOperator as stop:
                        return {**summary, "stage": state.data["stage"],
                                "stopped": str(stop), "complete": False}
                    continue
                state.data["errors"].append({
                    "at": datetime.now(timezone.utc).isoformat(),
                    "stage": state.data["stage"], "error": str(exc),
                })
                state.save()
                state.log("error", state.data["errors"][-1])
                raise


def worker_status(bundle: Path) -> dict:
    bundle = Path(bundle).resolve()
    plan = read_json(bundle / "worker-plan.json")
    _assert_signed(plan, "worker plan")
    state_path = bundle / "state.json"
    state = read_json(state_path) if state_path.exists() else None
    return {
        "run_id": plan["run_id"], "worker_id": plan["worker_id"],
        "master_plan_sha256": plan["master_plan_sha256"], "counts": plan["counts"],
        "state": state, "settings": _worker_settings(bundle, plan),
        "stop_requested": (bundle / "stop-request.json").exists(),
    }


def export_worker_result(bundle: Path) -> tuple[Path, bool]:
    """Вернуть компактный ZIP без runtime, input и backup."""

    bundle = Path(bundle).resolve()
    plan, _, _, _ = load_worker_bundle(bundle)
    state = read_json(bundle / "state.json") if (bundle / "state.json").exists() else {
        "stage": "not_started", "pending": None, "created": {}, "buildings": [],
        "deleted_objects": [], "deleted_geometries": [], "errors": [], "complete": False,
    }
    complete = bool(state.get("complete") and state.get("stage") == "complete"
                    and state.get("pending") is None and not state.get("errors"))
    result_directory = bundle / "result"
    result_directory.mkdir(exist_ok=True)
    archive = result_directory / f"worker-{plan['worker_id']}-{plan['run_id']}.zip"
    with tempfile.TemporaryDirectory(prefix="urban-worker-result-") as temporary:
        root = Path(temporary)
        report = {
            "run_id": plan["run_id"], "worker_id": plan["worker_id"],
            "master_plan_sha256": plan["master_plan_sha256"],
            "worker_plan_sha256": plan["sha256"], "stage": state.get("stage"),
            "complete": complete, "pending": state.get("pending"),
            "counts": {
                "deleted_objects": len(state.get("deleted_objects", [])),
                "deleted_geometries": len(state.get("deleted_geometries", [])),
                "created_objects": len(state.get("created", {})),
                "created_buildings": len(state.get("buildings", [])),
            },
        }
        write_json(root / "worker-result.json", report)
        write_json(root / "state.json", state)
        mapping = [{"input_index": int(index), **value}
                   for index, value in state.get("created", {}).items()]
        mapping.sort(key=lambda item: item["input_index"])
        write_json(root / "mapping.json", mapping)
        building_records = [{
            "input_index": int(index),
            "physical_object_id": state["created"][str(index)]["physical_object_id"],
        } for index in sorted(state.get("buildings", []))]
        write_json(root / "buildings.json", building_records)
        write_json(root / "errors.json", {
            "unresolved": state.get("errors", []),
            "resolved": state.get("resolved_errors", []),
        })
        events = bundle / "events.jsonl"
        (root / "events.jsonl").write_bytes(events.read_bytes() if events.exists() else b"")
        if complete:
            (root / "READY_TO_COLLECT").write_text("ready\n", encoding="ascii")
        files = [path for path in root.iterdir()
                 if path.is_file() and path.name != "checksums.json"]
        _write_checksums(root, files, root / "checksums.json")
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as stream:
            for path in sorted(root.iterdir()):
                stream.write(path, path.name)
    return archive, complete


def _safe_extract(stream: zipfile.ZipFile, destination: Path) -> None:
    for member in stream.infolist():
        target = (destination / member.filename).resolve()
        if destination.resolve() not in target.parents and target != destination.resolve():
            raise RuntimeError("Небезопасный путь в worker ZIP")
    stream.extractall(destination)


def collect_worker_results(distribution: Path, results: Path) -> dict:
    """Объединить ровно четыре законченных worker-результата без API-записи."""

    distribution, results = Path(distribution).resolve(), Path(results).resolve()
    _verify_file_checksums(distribution, distribution / "checksums.json")
    master = read_json(distribution / "master-plan.json")
    _assert_signed(master, "master plan")
    validate_worker_partition(master)
    archives = sorted(results.glob("*.zip"))
    workers: dict[int, dict] = {}
    states: dict[int, dict] = {}
    mappings: list[dict] = []
    buildings: list[dict] = []
    with tempfile.TemporaryDirectory(prefix="urban-collect-") as temporary:
        temporary = Path(temporary)
        for archive in archives:
            target = temporary / archive.stem
            target.mkdir()
            with zipfile.ZipFile(archive) as stream:
                _safe_extract(stream, target)
            _verify_file_checksums(target, target / "checksums.json")
            report = read_json(target / "worker-result.json")
            worker_id = int(report["worker_id"])
            if worker_id in workers:
                raise RuntimeError(f"Worker {worker_id} представлен несколько раз")
            if (report["master_plan_sha256"] != master["sha256"] or
                    report["run_id"] != master["run_id"]):
                raise RuntimeError("Worker относится к другому master plan")
            expected_plan = read_json(
                _worker_folder(distribution, worker_id) / "worker-plan.json"
            )
            _assert_signed(expected_plan, f"worker plan {worker_id}")
            if report["worker_plan_sha256"] != expected_plan["sha256"]:
                raise RuntimeError(f"Worker {worker_id} вернул чужой план")
            assignment = master["worker_assignments"][worker_id]
            for field in ("physical_object_ids", "geometry_ids", "input_indexes",
                          "building_indexes"):
                if expected_plan[field] != assignment[field]:
                    raise RuntimeError(f"Worker {worker_id} расходится с master plan")
            if not (target / "READY_TO_COLLECT").is_file() or not report["complete"]:
                raise RuntimeError(f"Worker {worker_id} не готов к сборке")
            state = read_json(target / "state.json")
            if state.get("pending") or state.get("stage") != "complete" or state.get("errors"):
                raise RuntimeError(f"Worker {worker_id} содержит pending или ошибки")
            workers[worker_id] = report
            states[worker_id] = state
            mappings.extend(read_json(target / "mapping.json"))
            buildings.extend(read_json(target / "buildings.json"))
    if set(workers) != set(range(master["workers"])):
        raise RuntimeError("Нужны worker 0, 1, 2 и 3 ровно по одному разу")
    deleted_objects = [value for state in states.values() for value in state["deleted_objects"]]
    deleted_geometries = [value for state in states.values() for value in state["deleted_geometries"]]
    input_indexes = [item["input_index"] for item in mappings]
    building_indexes = [item["input_index"] for item in buildings]
    checks = [
        (deleted_objects, master["existing_ids"], "удалённые physical_object_id"),
        (deleted_geometries, master["geometry_ids"], "удалённые object_geometry_id"),
        (input_indexes, list(range(master["manifest_count"])), "созданные входные индексы"),
        (building_indexes, master["building_indexes"], "записи buildings"),
    ]
    for actual, expected, label in checks:
        if len(actual) != len(set(actual)) or set(actual) != set(expected):
            raise RuntimeError(f"Неполное или пересекающееся объединение: {label}")
    server_ids = [item["physical_object_id"] for item in mappings]
    geometry_ids = [item["object_geometry_id"] for item in mappings]
    osm_ids = [item["osm_id"] for item in mappings]
    if (len(server_ids) != len(set(server_ids)) or len(geometry_ids) != len(set(geometry_ids))
            or len(osm_ids) != len(set(osm_ids))):
        raise RuntimeError("В собранной карте повторяются серверные ID или OSM ID")
    server_by_input = {item["input_index"]: item["physical_object_id"] for item in mappings}
    if any(server_by_input.get(item["input_index"]) != item["physical_object_id"]
           for item in buildings):
        raise RuntimeError("Запись building не принадлежит своему физическому объекту")
    collected = {
        "run_id": master["run_id"], "master_plan_sha256": master["sha256"],
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "workers": [workers[index] for index in sorted(workers)],
        "mapping": sorted(mappings, key=lambda item: item["input_index"]),
        "buildings": sorted(buildings, key=lambda item: item["input_index"]),
        "counts": {"deleted_objects": len(deleted_objects),
                   "deleted_geometries": len(deleted_geometries),
                   "created_objects": len(mappings), "created_buildings": len(buildings)},
        "pending": [], "errors": [],
    }
    collected["sha256"] = json_digest(collected)
    write_json(distribution / "collected.json", collected)
    return collected


def final_verify(api: UrbanApi, distribution: Path) -> dict:
    """Новой полной GET-выгрузкой проверить объединённый результат."""

    distribution = Path(distribution).resolve()
    _verify_file_checksums(distribution, distribution / "checksums.json")
    master, manifest, backup, collected = (
        read_json(distribution / "master-plan.json"),
        read_json(distribution / "manifest.json"),
        read_json(distribution / "master-backup.json"),
        read_json(distribution / "collected.json"),
    )
    _assert_signed(master, "master plan")
    _assert_signed(collected, "collected result")
    if (collected["master_plan_sha256"] != master["sha256"] or collected["pending"]
            or collected["errors"]):
        raise RuntimeError("Собранный результат не готов к final verify")
    if json_digest(manifest) != master["manifest_sha256"] or json_digest(backup) != master["backup_sha256"]:
        raise RuntimeError("Master plan потерял связь с manifest или backup")
    descendants, current = api.export_area_objects(
        master["territory_id"], tuple(master.get("allowed_type_ids", [4, 5]))
    )
    if descendants != master["descendant_ids"]:
        raise RuntimeError("Иерархия территории изменилась")
    by_id = {item["physical_object_id"]: item for item in current}
    mapping = {item["input_index"]: item for item in collected["mapping"]}
    records = {item["input_index"]: item for item in manifest["records"]}
    issues: list[str] = []
    if set(master["existing_ids"]) & by_id.keys():
        issues.append("old_physical_objects_remain")
    for index, record in records.items():
        ids = mapping.get(index)
        if not ids:
            issues.append(f"missing_mapping:{index}")
            continue
        item = by_id.get(ids["physical_object_id"])
        if not item:
            issues.append(f"missing_server_object:{index}")
            continue
        expected = record["physical_object"]
        if item["object_geometry_id"] != ids["object_geometry_id"] or item.get("osm_id") != record["osm_id"]:
            issues.append(f"id_or_osm_mismatch:{index}")
        if item["physical_object_type"]["physical_object_type_id"] != expected["physical_object_type_id"]:
            issues.append(f"type_mismatch:{index}")
        if item["territory"]["id"] != expected["territory_id"]:
            issues.append(f"territory_mismatch:{index}")
        if not shape(item["geometry"]).equals(shape(expected["geometry"])):
            issues.append(f"geometry_mismatch:{index}")
        if (item.get("address") != expected.get("address") or item.get("name") != expected.get("name")
                or item.get("properties") != expected.get("properties")):
            issues.append(f"attributes_mismatch:{index}")
        building = record["building"]
        if building is not None:
            if not item.get("building") or any(
                    item["building"].get(field) != value for field, value in building.items()):
                issues.append(f"building_mismatch:{index}")
        elif item.get("building"):
            issues.append(f"unexpected_building:{index}")
    retained_ids = set(master["retained_ids"])
    if retained_ids - by_id.keys():
        issues.append(f"missing_retained:{len(retained_ids - by_id.keys())}")
    for object_id in retained_ids & by_id.keys():
        if retained_fingerprint(by_id[object_id]) != master["retained_fingerprints"][str(object_id)]:
            issues.append(f"retained_mismatch:{object_id}")
    for relationship in master.get("protected_relationships", {}).values():
        geometry_id = relationship["object_geometry_id"]
        try:
            linked = api.get(
                "/api/v1/urban_objects_by_object_geometry"
                f"?object_geometry_id={geometry_id}"
            )
        except ApiError:
            issues.append(f"protected_relationship_missing:{geometry_id}")
        else:
            if relationship_fingerprint(linked) != relationship["fingerprint"]:
                issues.append(f"protected_relationship_mismatch:{geometry_id}")
    created_ids = {item["physical_object_id"] for item in collected["mapping"]}
    unexpected = set(by_id) - created_ids - retained_ids
    if unexpected:
        issues.append(f"unexpected_objects:{len(unexpected)}")
    osm_counts = Counter(item.get("osm_id") for item in current if item.get("osm_id"))
    if any(value > 1 for value in osm_counts.values()):
        issues.append("duplicate_osm_ids")
    snapshot_sha = json_digest(sorted(current, key=lambda item: item["physical_object_id"]))
    checkpoint = PreviewDatabase(distribution / "final-verify.sqlite3", snapshot_sha)
    try:
        for geometry_id in master["geometry_ids"]:
            cached = checkpoint.get("geometry_links", geometry_id)
            if cached is not None:
                if not cached["absent"]:
                    issues.append(f"old_geometry_remains:{geometry_id}")
                continue
            try:
                linked = api.get(f"/api/v1/object_geometries/{geometry_id}/physical_objects")
            except ApiError as exc:
                if exc.status != 404:
                    raise
                checkpoint.put("geometry_links", geometry_id, {"absent": True})
            else:
                checkpoint.put("geometry_links", geometry_id, {"absent": False, "linked": linked})
                issues.append(f"old_geometry_remains:{geometry_id}")
    finally:
        checkpoint.close()
    report = {
        "run_id": master["run_id"], "master_plan_sha256": master["sha256"],
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "counts": {"expected": master["manifest_count"], "created": len(created_ids),
                   "buildings": len(collected["buildings"]), "retained": len(retained_ids),
                   "server_objects": len(current), "old_objects": len(master["existing_ids"]),
                   "old_geometries": len(master["geometry_ids"])},
        "issues": issues, "stage": "complete" if not issues else "verification_failed",
    }
    report["sha256"] = json_digest(report)
    write_json(distribution / "final-verification.json", report)
    return report
