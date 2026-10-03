"""Исполнение отдельно разрешённого нового плана текущего результата.

Во время реорганизации этот модуль проверяется только имитационными тестами.
Он не удаляет объекты и не пересоздаёт сервисные связи без доказанного правила.
"""

from datetime import datetime, timezone
from pathlib import Path
import tempfile

from shapely.geometry import shape

from .config import get_territory
from .distributed import worker_lock
from .results import create_restoration_plan, read_restoration_plan
from .storage import RunPaths, json_digest, write_json
from .workflow import OperationState, reconcile_pending, server_snapshot


def apply_restoration(api, plan_path: Path, authorized_plan_sha256: str) -> dict:
    """Продолжить отдельный план по точной SHA-256 нового разрешения.

    Состояние и pending используют тот же механизм, что обычный загрузчик.
    После оборванного POST он ищет OSM ID, тип и геометрию; повтор возможен только
    после двух разнесённых проверок отсутствия. Для точек OSM ID может быть None.
    """

    plan_path = Path(plan_path).resolve()
    plan = read_restoration_plan(plan_path)
    if authorized_plan_sha256 != plan["sha256"]:
        raise RuntimeError("Нет отдельного разрешения записи по SHA-256 нового плана")
    if plan["unresolved"] or plan["delete"]:
        raise RuntimeError("В плане есть нерешённые вопросы или удаления")
    territory = get_territory(plan["territory"])
    paths = RunPaths(territory.key, plan["sha256"], plan_path.parent / (plan_path.stem + "-execution"))
    paths.directory.mkdir(parents=True, exist_ok=True)
    with worker_lock(paths.directory):
        state = OperationState(paths, plan_path)
        records = [dict(record, osm_id=record["physical_object"].get("osm_id"))
                   for record in plan["create"]]
        records += [{"identity": task["identity"], "osm_id": task["physical_object"].get("osm_id"),
                     "physical_object": task["physical_object"], "building": task["body"]}
                    for task in plan["create_buildings"]]
        contract = api.validate_write_contract()
        if plan.get("api_contract_sha256") and json_digest(contract) != plan["api_contract_sha256"]:
            raise RuntimeError("Контракт API изменился после плана восстановления")
        if state.data["stage"] == "planned":
            current = server_snapshot(api, territory, plan)
            if json_digest(sorted(current, key=lambda x: x["physical_object_id"])) != plan["server_snapshot_sha256"]:
                raise RuntimeError("Сервер изменился после плана восстановления")
            for index, task in enumerate(plan["create_buildings"], start=len(plan["create"])):
                state.data["created"][str(index)] = {
                    "physical_object_id": task["physical_object_id"],
                    "object_geometry_id": task["object_geometry_id"],
                }
            state.data["stage"] = "creating_objects"
            state.save()
        reconcile_pending(api, territory, state, records)
        if state.data["stage"] == "creating_objects":
            current = server_snapshot(api, territory, plan)
            by_osm = {}
            for item in current:
                by_osm.setdefault(item.get("osm_id"), []).append(item)
            for index, record in enumerate(records[:len(plan["create"])]):
                key = str(index)
                if key in state.data["created"]:
                    continue
                # Возобновление не отправляет запись, если объект уже существует.
                # begin/reconcile выполняет ту же проверку при неопределённом POST.
                matches = [item for item in by_osm.get(record["osm_id"], [])
                           if item["physical_object_type"]["physical_object_type_id"] == record["physical_object"]["physical_object_type_id"]
                           and shape(item["geometry"]).equals(shape(record["physical_object"]["geometry"]))]
                if record["osm_id"] is None:
                    owned_ids = {item["physical_object_id"] for item in plan["preserve"]}
                    owned_ids.update(ids["physical_object_id"] for k, ids in state.data["created"].items()
                                     if k != key)
                    matches = [item for item in matches if item["physical_object_id"] not in owned_ids]
                if matches:
                    if len(matches) != 1:
                        raise RuntimeError("Неоднозначный объект восстановления")
                    state.begin("create_object", key)
                    reconcile_pending(api, territory, state, records)
                    continue
                state.begin("create_object", key)
                response = api.post("/api/v1/physical_objects", record["physical_object"])
                state.finish("created", key, {
                    "physical_object_id": response["physical_object"]["physical_object_id"],
                    "object_geometry_id": response["object_geometry"]["object_geometry_id"],
                })
            state.data["stage"] = "creating_buildings"
            state.save()
        if state.data["stage"] == "creating_buildings":
            for index, record in enumerate(records):
                if record["building"] is None or index in state.data["buildings"]:
                    continue
                state.begin("create_building", index)
                api.post("/api/v1/buildings", {
                    "physical_object_id": state.data["created"][str(index)]["physical_object_id"],
                    **record["building"],
                })
                state.finish("buildings", index)
            state.data["stage"] = "verifying"
            state.save()
        # Повторное GET-планирование должно показать нулевую недостающую работу.
        # Здесь сравнивается содержимое; прежние серверные ID не требуются.
        with tempfile.TemporaryDirectory(prefix="urban-restore-check-") as temporary:
            remaining = create_restoration_plan(api, territory.key, Path(temporary) / "check.json")
        issues = list(remaining["unresolved"])
        if remaining["create"] or remaining["create_buildings"] or state.data["pending"]:
            issues.append("restoration_incomplete")
        report = {
            "kind": "restored-current-result", "plan_sha256": plan["sha256"],
            "verified_at": datetime.now(timezone.utc).isoformat(), "issues": issues,
            "bindings": remaining["preserve"], "stage": "complete" if not issues else "verification_failed",
        }
        write_json(paths.verification, report)
        state.data["stage"] = report["stage"]
        state.data["verified"] = not issues
        state.save()
        return report
