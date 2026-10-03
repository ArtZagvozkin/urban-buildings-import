"""Планирование, безопасное выполнение, продолжение и независимая проверка."""

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

from shapely.geometry import shape

from .api import ApiError
from .config import TerritoryConfig
from .storage import RunPaths, json_digest, read_json, write_json


ABSENT_RECHECK_SECONDS = 60


def fingerprint(items: list[dict]) -> dict[str, str]:
    return {str(item["physical_object_id"]): json_digest(item) for item in items}


def retained_fingerprint(item: dict) -> str:
    """Сравнить сохранённый объект без зависимости от времени ответа API."""

    stable = {
        "object_geometry_id": item.get("object_geometry_id"),
        "type_id": item.get("physical_object_type", {}).get("physical_object_type_id"),
        "territory_id": item.get("territory", {}).get("id"),
        "osm_id": item.get("osm_id"),
        "geometry": item.get("geometry"),
        "address": item.get("address"),
        "name": item.get("name"),
        "properties": item.get("properties"),
        "building": item.get("building"),
    }
    return json_digest(stable)


def split_replacement_scope(items: list[dict], geometry_types: tuple[str, ...]):
    """Отделить заменяемые полигоны от точек и иных сохраняемых объектов."""

    allowed = set(geometry_types)
    selected = [item for item in items if item["geometry"]["type"] in allowed]
    retained = [item for item in items if item["geometry"]["type"] not in allowed]
    return selected, retained


def validate_replacement_boundary(items: list[dict], boundary) -> None:
    """Запретить план, если серверный объект выходит из муниципальной границы."""

    for item in items:
        if not boundary.covers(shape(item["geometry"])):
            raise RuntimeError(
                f"Серверный объект {item['physical_object_id']} пересекает "
                "муниципальную границу или лежит вне неё"
            )


def _validate_sources(territory: TerritoryConfig, audit: dict) -> None:
    if audit.get("config_sha256") != json_digest(territory.raw):
        raise RuntimeError("Конфигурация территории изменилась после prepare")
    for source in territory.sources:
        actual = hashlib.sha256(source.path.read_bytes()).hexdigest()
        if audit["source_sha256"].get(source.path.name) != actual:
            raise RuntimeError(f"Исходный файл изменился после prepare: {source.path.name}")


def create_plan(api, territory: TerritoryConfig, paths: RunPaths):
    """Создать backup и неизменяемый точный план без записи в удалённую базу."""

    if paths.plan.exists():
        raise RuntimeError(f"План уже существует: {paths.plan}")
    if not paths.audit.is_file() or not paths.accepted.is_file():
        raise RuntimeError("Сначала выполните prepare для этого запуска")
    audit = read_json(paths.audit)
    accepted = read_json(paths.accepted)
    _validate_sources(territory, audit)
    live_types = {
        item["physical_object_type_id"]
        for item in api.get("/api/v1/physical_object_types")
    }
    missing_types = set(territory.allowed_type_ids) - live_types
    if missing_types:
        raise RuntimeError(f"В живом API отсутствуют типы: {sorted(missing_types)}")
    live_territory = api.get(f"/api/v1/territory/{territory.territory_id}")
    boundary_hash = hashlib.sha256(
        json.dumps(live_territory["geometry"], sort_keys=True).encode()
    ).hexdigest()
    if audit["boundary_sha256"] != boundary_hash:
        raise RuntimeError("Муниципальная граница изменилась после prepare")
    descendant_ids, all_existing = api.export_area_objects(
        territory.territory_id, territory.allowed_type_ids
    )
    existing, retained = split_replacement_scope(
        all_existing, territory.replace_geometry_types
    )
    validate_replacement_boundary(existing, shape(live_territory["geometry"]))
    if any(item["territory"]["id"] not in descendant_ids for item in all_existing):
        raise RuntimeError("API вернул объект вне иерархии территории")
    backup = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "territory_id": territory.territory_id,
        "descendant_ids": descendant_ids,
        "count": len(existing),
        "objects": existing,
        "retained": retained,
    }
    write_json(paths.backup, backup)

    # Checkpoint сохраняет дорогие GET-проверки, но привязан к точному листингу.
    listing_hash = json_digest(existing)
    if paths.preview_checkpoint.exists():
        checkpoint = read_json(paths.preview_checkpoint)
        if checkpoint["listing_sha256"] != listing_hash:
            raise RuntimeError("Серверный листинг изменился после checkpoint preview")
    else:
        checkpoint = {
            "listing_sha256": listing_hash,
            "relations": {},
            "checked_geometries": [],
        }
    relations = checkpoint["relations"]
    for index, item in enumerate(existing):
        object_id = item["physical_object_id"]
        key = str(object_id)
        if key not in relations:
            relations[key] = {
                "services": api.get(f"/api/v1/physical_objects/{object_id}/services"),
                "geometries": api.get(f"/api/v1/physical_objects/{object_id}/geometries"),
            }
            if index % 25 == 0:
                write_json(paths.preview_checkpoint, checkpoint)
        related = relations[key]
        if related["services"]:
            raise RuntimeError(
                f"У физического объекта {object_id} есть сервисные связи"
            )
        expected_geometry = {item["object_geometry_id"]}
        actual_geometries = {
            value["object_geometry_id"] for value in related["geometries"]
        }
        if actual_geometries != expected_geometry:
            raise RuntimeError(
                f"У физического объекта {object_id} есть дополнительные геометрии"
            )
    backup["relations"] = relations
    write_json(paths.backup, backup)

    existing_ids = {item["physical_object_id"] for item in existing}
    geometry_ids = sorted({item["object_geometry_id"] for item in existing})
    checked_geometries = set(checkpoint["checked_geometries"])
    shared_outside: list[int] = []
    for index, geometry_id in enumerate(geometry_ids):
        if geometry_id in checked_geometries:
            continue
        linked = api.get(
            f"/api/v1/object_geometries/{geometry_id}/physical_objects"
        )
        if any(item["physical_object_id"] not in existing_ids for item in linked):
            shared_outside.append(geometry_id)
        for item in linked:
            if item["physical_object_id"] in existing_ids:
                linked_territories = item.get("territories") or []
                if any(value["id"] not in descendant_ids for value in linked_territories):
                    shared_outside.append(geometry_id)
        checkpoint["checked_geometries"].append(geometry_id)
        if index % 25 == 0:
            write_json(paths.preview_checkpoint, checkpoint)
    write_json(paths.preview_checkpoint, checkpoint)
    if shared_outside:
        raise RuntimeError(
            f"Найдены разделяемые или внешние геометрии: {shared_outside[:20]}"
        )
    plan = {
        "format_version": 1,
        "territory": territory.key,
        "territory_id": territory.territory_id,
        "allowed_type_ids": list(territory.allowed_type_ids),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "config_sha256": json_digest(territory.raw),
        "audit_sha256": json_digest(audit),
        "accepted_sha256": json_digest(accepted),
        "backup_file": paths.backup.name,
        "backup_sha256": json_digest(backup),
        "descendant_ids": descendant_ids,
        "existing_ids": sorted(existing_ids),
        "existing_fingerprints": fingerprint(existing),
        "retained_ids": sorted(item["physical_object_id"] for item in retained),
        "retained_fingerprints": {
            str(item["physical_object_id"]): retained_fingerprint(item)
            for item in retained
        },
        "geometry_ids": geometry_ids,
        "counts": {
            "source": audit["counts"],
            "existing": len(existing),
            "retained": len(retained),
            "accepted": len(accepted),
            "planned_buildings": audit["counts"].get("accepted_4", 0),
        },
        "unresolved": [],
    }
    plan["sha256"] = json_digest(plan)
    write_json(paths.plan, plan)
    paths.preview_checkpoint.unlink(missing_ok=True)
    return plan


class OperationState:
    """Долговечное состояние: запись выполняется между begin и finish."""

    def __init__(self, paths: RunPaths, plan_path: Path | None = None):
        self.paths = paths
        self.plan = read_json(plan_path or paths.plan)
        unsigned = {key: value for key, value in self.plan.items() if key != "sha256"}
        if json_digest(unsigned) != self.plan["sha256"]:
            raise RuntimeError("Нарушена целостность плана")
        if paths.state.exists():
            self.data = read_json(paths.state)
            if self.data["plan_sha256"] != self.plan["sha256"]:
                raise RuntimeError("Состояние относится к другому плану")
        else:
            self.data = {
                "plan_sha256": self.plan["sha256"],
                "stage": "planned",
                "deleted_objects": [],
                "deleted_geometries": [],
                "created": {},
                "buildings": [],
                "pending": None,
                "verified": False,
            }
            self.save()

    def save(self) -> None:
        write_json(self.paths.state, self.data)

    def log(self, action: str, detail) -> None:
        self.paths.events.parent.mkdir(parents=True, exist_ok=True)
        with self.paths.events.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({
                "at": datetime.now(timezone.utc).isoformat(),
                "action": action,
                "detail": detail,
            }, ensure_ascii=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def begin(self, action: str, key) -> None:
        if self.data["pending"]:
            raise RuntimeError(f"Не разрешена предыдущая операция: {self.data['pending']}")
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
        self.save()


def server_snapshot(api, territory: TerritoryConfig, plan: dict) -> list[dict]:
    descendant_ids, objects = api.export_area_objects(
        territory.territory_id, territory.allowed_type_ids
    )
    if descendant_ids != plan["descendant_ids"]:
        raise RuntimeError("Иерархия территорий изменилась после preview")
    return objects


def _backup_path(state: OperationState) -> Path:
    relative = Path(state.plan["backup_file"])
    if relative.is_absolute() or relative.parent != Path("."):
        raise RuntimeError("План содержит небезопасный путь backup")
    return state.paths.directory / relative


def _confirm_absence_before_retry(state: OperationState, description: str) -> None:
    """Разрешить повтор POST только после двух проверок с интервалом в минуту."""

    pending = state.data["pending"]
    now = datetime.now(timezone.utc)
    checks = pending.setdefault("absent_checks", [])
    if checks:
        last = datetime.fromisoformat(checks[-1])
        elapsed = (now - last).total_seconds()
        if elapsed < ABSENT_RECHECK_SECONDS:
            state.save()
            raise RuntimeError(
                f"{description} не найден; повторите сверку не раньше чем через "
                f"{int(ABSENT_RECHECK_SECONDS - elapsed) + 1} сек. POST не повторён"
            )
    checks.append(now.isoformat())
    state.save()
    state.log("uncertain_post_absent", {
        "pending": pending,
        "description": description,
        "check_number": len(checks),
    })
    if len(checks) < 2:
        raise RuntimeError(
            f"{description} не найден; нужна повторная серверная сверка через "
            f"{ABSENT_RECHECK_SECONDS} сек. POST не повторён"
        )
    state.log("retry_confirmed_absent_post", {
        "pending": pending,
        "description": description,
    })
    state.data["pending"] = None
    state.save()


def reconcile_pending(api, territory: TerritoryConfig, state: OperationState,
                      accepted: list[dict]) -> None:
    """Установить результат оборванной записи до продолжения этапа."""

    pending = state.data["pending"]
    if not pending:
        return
    action, key = pending["action"], pending["key"]
    if action == "delete_object":
        backup = read_json(_backup_path(state))
        matches = [item for item in backup["objects"]
                   if item["physical_object_id"] == key]
        if len(matches) != 1:
            raise RuntimeError(f"Backup неоднозначно описывает объект {key}")
        geometry_id = matches[0]["object_geometry_id"]
        try:
            linked = api.get(
                f"/api/v1/object_geometries/{geometry_id}/physical_objects"
            )
        except ApiError as exc:
            if exc.status != 404:
                raise
            linked = []
        if not any(item["physical_object_id"] == key for item in linked):
            state.finish("deleted_objects", key)
        else:
            state.log("retry_uncertain_object_delete", {
                "physical_object_id": key,
                "object_geometry_id": geometry_id,
            })
            state.data["pending"] = None
            state.save()
    elif action == "delete_geometry":
        try:
            linked = api.get(f"/api/v1/object_geometries/{key}/physical_objects")
        except ApiError as exc:
            if exc.status == 404:
                state.finish("deleted_geometries", key)
                return
            raise
        if linked:
            raise RuntimeError(f"После DELETE у геометрии {key} появились связи")
        state.log("retry_uncertain_geometry_delete", key)
        state.data["pending"] = None
        state.save()
    elif action == "create_object":
        record = accepted[int(key)]
        excluded_ids = set()
        if record["osm_id"] is None:
            # В восстановлении одинаковые точки без OSM ID различаются
            # сохранёнными/уже созданными ID, а не выдуманным OSM-идентификатором.
            excluded_ids = {ids["physical_object_id"] for index, ids in state.data["created"].items()
                            if str(index) != str(key)}
            excluded_ids.update(item["physical_object_id"] for item in state.plan.get("preserve", []))
        matches = [
            item for item in server_snapshot(api, territory, state.plan)
            if item["physical_object_id"] not in excluded_ids
            and item.get("osm_id") == record["osm_id"]
            and item["physical_object_type"]["physical_object_type_id"]
            == record["physical_object"]["physical_object_type_id"]
            and shape(item["geometry"]).equals(
                shape(record["physical_object"]["geometry"])
            )
        ]
        if len(matches) == 1:
            item = matches[0]
            state.finish("created", key, {
                "physical_object_id": item["physical_object_id"],
                "object_geometry_id": item["object_geometry_id"],
            })
        elif not matches:
            _confirm_absence_before_retry(
                state, f"физический объект OSM {record['osm_id']}"
            )
        else:
            raise RuntimeError(f"Найдено несколько объектов OSM {record['osm_id']}")
    elif action == "create_building":
        ids = state.data["created"][str(key)]
        object_id = ids["physical_object_id"]
        linked = api.get(
            f"/api/v1/object_geometries/{ids['object_geometry_id']}/physical_objects"
        )
        matches = [item for item in linked
                   if item["physical_object_id"] == object_id]
        if len(matches) != 1:
            raise RuntimeError(
                f"Связь физического объекта {object_id} с геометрией изменилась"
            )
        if matches[0].get("building"):
            state.finish("buildings", key)
        else:
            _confirm_absence_before_retry(
                state, f"запись building физического объекта {object_id}"
            )
    else:
        raise RuntimeError(f"Неизвестная pending-операция: {action}")


def _load_inputs(territory: TerritoryConfig, state: OperationState):
    plan = state.plan
    if plan["territory"] != territory.key:
        raise RuntimeError("План относится к другой территории")
    if plan["config_sha256"] != json_digest(territory.raw):
        raise RuntimeError("Конфигурация территории изменилась после preview")
    audit = read_json(state.paths.audit)
    accepted = read_json(state.paths.accepted)
    backup = read_json(_backup_path(state))
    if (json_digest(audit) != plan["audit_sha256"] or
            json_digest(accepted) != plan["accepted_sha256"] or
            json_digest(backup) != plan["backup_sha256"]):
        raise RuntimeError("Prepared data или backup изменились после preview")
    if backup["count"] != len(backup["objects"]):
        raise RuntimeError("Backup неполон")
    return audit, accepted, backup


def apply_plan(api, territory: TerritoryConfig, paths: RunPaths) -> dict:
    """Выполнить только сохранённый план и продолжить его после прерывания."""

    if not paths.plan.is_file():
        raise RuntimeError("Нет плана; сначала выполните preview")
    state = OperationState(paths)
    plan = state.plan
    _, accepted, backup = _load_inputs(territory, state)
    if state.data["stage"] not in {"verifying", "complete", "verification_failed"}:
        api.validate_write_contract()
    reconcile_pending(api, territory, state, accepted)
    if state.data["stage"] == "planned":
        current = server_snapshot(api, territory, plan)
        current_ids = {item["physical_object_id"] for item in current}
        allowed_ids = set(plan["existing_ids"]) | set(plan["retained_ids"])
        selected = [item for item in current
                    if item["physical_object_id"] in set(plan["existing_ids"])]
        if (current_ids != allowed_ids or
                fingerprint(selected) != plan["existing_fingerprints"]):
            raise RuntimeError("Серверные объекты изменились после preview")
        state.data["stage"] = "deleting_objects"
        state.save()
    if state.data["stage"] == "deleting_objects":
        done = set(state.data["deleted_objects"])
        backup_by_id = {item["physical_object_id"]: item for item in backup["objects"]}
        for object_id in plan["existing_ids"]:
            if object_id in done:
                continue
            related = {
                "services": api.get(f"/api/v1/physical_objects/{object_id}/services"),
                "geometries": api.get(f"/api/v1/physical_objects/{object_id}/geometries"),
            }
            if related["services"]:
                raise RuntimeError(f"У объекта {object_id} появились сервисные связи")
            geometry_ids = {
                item["object_geometry_id"] for item in related["geometries"]
            }
            if geometry_ids != {backup_by_id[object_id]["object_geometry_id"]}:
                raise RuntimeError(f"У объекта {object_id} изменились геометрии")
            state.begin("delete_object", object_id)
            api.delete(f"/api/v1/physical_objects/{object_id}")
            state.finish("deleted_objects", object_id)
        remaining = {
            item["physical_object_id"]
            for item in server_snapshot(api, territory, plan)
        } & set(plan["existing_ids"])
        if remaining:
            raise RuntimeError(f"Запланированные объекты остались: {list(remaining)[:10]}")
        state.data["stage"] = "deleting_geometries"
        state.save()
    if state.data["stage"] == "deleting_geometries":
        done = set(state.data["deleted_geometries"])
        for geometry_id in plan["geometry_ids"]:
            if geometry_id in done:
                continue
            try:
                linked = api.get(
                    f"/api/v1/object_geometries/{geometry_id}/physical_objects"
                )
            except ApiError as exc:
                if exc.status == 404:
                    state.begin("delete_geometry", geometry_id)
                    state.finish("deleted_geometries", geometry_id)
                    continue
                raise
            if linked:
                raise RuntimeError(f"Геометрия {geometry_id} ещё используется")
            state.begin("delete_geometry", geometry_id)
            try:
                api.delete(f"/api/v1/object_geometries/{geometry_id}")
            except ApiError as exc:
                if exc.status != 404:
                    raise
                try:
                    api.get(
                        f"/api/v1/object_geometries/{geometry_id}/physical_objects"
                    )
                except ApiError as check:
                    if check.status != 404:
                        raise
                else:
                    raise RuntimeError(
                        f"DELETE геометрии {geometry_id} вернул 404, но она существует"
                    )
            state.finish("deleted_geometries", geometry_id)
        state.data["stage"] = "creating_objects"
        state.save()
    if state.data["stage"] == "creating_objects":
        for index, record in enumerate(accepted):
            key = str(index)
            if key in state.data["created"]:
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
        done = set(state.data["buildings"])
        for index, record in enumerate(accepted):
            if record["building"] is None or index in done:
                continue
            object_id = state.data["created"][str(index)]["physical_object_id"]
            state.begin("create_building", index)
            api.post("/api/v1/buildings", {
                "physical_object_id": object_id,
                **record["building"],
            })
            state.finish("buildings", index)
        state.data["stage"] = "verifying"
        state.save()
    return verify_run(api, territory, paths)


def verify_run(api, territory: TerritoryConfig, paths: RunPaths) -> dict:
    """Новой выгрузкой независимо проверить результат и старые геометрии."""

    state = OperationState(paths)
    plan = state.plan
    _, accepted, backup = _load_inputs(territory, state)
    current = server_snapshot(api, territory, plan)
    snapshot_hash = json_digest(
        sorted(current, key=lambda item: item["physical_object_id"])
    )
    if paths.verify_checkpoint.exists():
        checkpoint = read_json(paths.verify_checkpoint)
        if (checkpoint.get("plan_sha256") != plan["sha256"] or
                checkpoint.get("server_snapshot_sha256") != snapshot_hash):
            raise RuntimeError(
                "Серверные объекты изменились во время verify; удалите только "
                "verify_checkpoint после разбора и начните проверку заново"
            )
    else:
        checkpoint = {
            "plan_sha256": plan["sha256"],
            "started_at": datetime.now(timezone.utc).isoformat(),
            "server_snapshot_sha256": snapshot_hash,
            "checked_geometries": [],
            "old_geometry_issues": [],
        }
        write_json(paths.verify_checkpoint, checkpoint)
    by_id = {item["physical_object_id"]: item for item in current}
    issues: list[str] = []
    old_ids = set(plan["existing_ids"])
    if old_ids & by_id.keys():
        issues.append("planned_old_objects_remain")
    if len(state.data["created"]) != len(accepted):
        issues.append("creation_state_incomplete")
    for key, ids in state.data["created"].items():
        record = accepted[int(key)]
        item = by_id.get(ids["physical_object_id"])
        if not item:
            issues.append(f"missing_server_object:{key}")
            continue
        if (item["object_geometry_id"] != ids["object_geometry_id"] or
                item.get("osm_id") != record["osm_id"]):
            issues.append(f"id_or_osm_mismatch:{key}")
        if (item["physical_object_type"]["physical_object_type_id"] !=
                record["physical_object"]["physical_object_type_id"]):
            issues.append(f"type_mismatch:{key}")
        if item["territory"]["id"] != territory.territory_id:
            issues.append(f"territory_mismatch:{key}")
        if not shape(item["geometry"]).equals(
                shape(record["physical_object"]["geometry"])):
            issues.append(f"geometry_mismatch:{key}")
        expected = record["physical_object"]
        if (item.get("address") != expected.get("address") or
                item.get("name") != expected.get("name")):
            issues.append(f"address_or_name_mismatch:{key}")
        if item.get("properties") != expected.get("properties"):
            issues.append(f"properties_mismatch:{key}")
        if record["building"] is not None and not item.get("building"):
            issues.append(f"missing_building:{key}")
        elif record["building"] is not None and any(
                item["building"].get(field) != value
                for field, value in record["building"].items()):
            issues.append(f"building_attributes_mismatch:{key}")
    created_ids = {
        value["physical_object_id"] for value in state.data["created"].values()
    }
    if len(created_ids) != len(state.data["created"]):
        issues.append("duplicate_created_ids")
    retained_ids = set(plan["retained_ids"])
    if retained_ids - by_id.keys():
        issues.append(f"missing_retained_objects:{len(retained_ids - by_id.keys())}")
    for object_id in retained_ids & by_id.keys():
        expected_fingerprint = plan.get("retained_fingerprints", {}).get(str(object_id))
        if expected_fingerprint and retained_fingerprint(by_id[object_id]) != expected_fingerprint:
            issues.append(f"retained_object_mismatch:{object_id}")
    unexpected = set(by_id) - created_ids - retained_ids
    if unexpected:
        issues.append(f"unexpected_server_objects:{len(unexpected)}")
    osm_counts = Counter(item.get("osm_id") for item in current if item.get("osm_id"))
    if any(count > 1 for count in osm_counts.values()):
        issues.append("duplicate_server_osm_ids")
    checked_geometries = set(checkpoint["checked_geometries"])
    for geometry_id in plan["geometry_ids"]:
        if geometry_id in checked_geometries:
            continue
        try:
            api.get(f"/api/v1/object_geometries/{geometry_id}/physical_objects")
        except ApiError as exc:
            if exc.status != 404:
                write_json(paths.verify_checkpoint, checkpoint)
                raise
        except Exception:
            write_json(paths.verify_checkpoint, checkpoint)
            raise
        else:
            checkpoint["old_geometry_issues"].append(
                f"old_geometry_still_exists:{geometry_id}"
            )
        checkpoint["checked_geometries"].append(geometry_id)
        if len(checkpoint["checked_geometries"]) % 25 == 0:
            write_json(paths.verify_checkpoint, checkpoint)
    write_json(paths.verify_checkpoint, checkpoint)
    issues.extend(checkpoint["old_geometry_issues"])
    if state.data["pending"]:
        issues.append("pending_operation")
    report = {
        "territory": territory.key,
        "run_id": paths.run_id,
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "counts": {
            "source": plan["counts"]["source"],
            "planned_delete": len(old_ids),
            "actual_deleted": len(state.data["deleted_objects"]),
            "planned_create": len(accepted),
            "actual_created": len(state.data["created"]),
            "actual_buildings": len(state.data["buildings"]),
            "retained": len(retained_ids),
            "server_objects": len(current),
        },
        "issues": issues,
        "stage": "complete" if not issues else "verification_failed",
    }
    write_json(paths.verification, report)
    if not issues:
        state.data["verified"] = True
        state.data["stage"] = "complete"
        state.save()
        paths.verify_checkpoint.unlink(missing_ok=True)
    else:
        state.data["verified"] = False
        state.data["stage"] = "verification_failed"
        state.save()
    return report
