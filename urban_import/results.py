"""Самодостаточные завершённые результаты: воспроизведение, GET и восстановление.

Компактный результат является производным свидетельством, а не старым планом.
Импортированные тела запросов выводятся из неизменяемых исходников; исключение
составляют сохраняемые объекты, которых в исходниках нет.
"""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

from shapely.geometry import shape

from .api import ApiError, ParallelGet, UrbanApi
from .config import ROOT, get_territory
from .prepare import prepare_territory
from .storage import ARTIFACTS, RunPaths, json_digest, read_json, write_json


RESULTS = ROOT / "results"
BUILDING_FIELDS = (
    "properties", "floors", "building_area_official", "building_area_modeled",
    "project_type", "floor_type", "wall_material", "built_year",
    "exploitation_start_year",
)


class ReadOnlyApi(UrbanApi):
    """Запретить запись на уровне клиента команд проверки и восстановления."""

    def request(self, method: str, path: str, **kwargs):
        if method != "GET":
            raise RuntimeError("Этот клиент разрешает только GET")
        return super().request(method, path, **kwargs)


def file_sha256(path: Path) -> str:
    """Побайтовый хеш: исходные переводы строк не преобразуются."""
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_compressed_json(path: Path, value) -> None:
    """Детерминированный gzip: контрольная сумма не зависит от времени упаковки."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    path.write_bytes(gzip.compress(encoded, mtime=0))


def _checked_path(root: Path, reference: dict) -> Path:
    path = (root / reference["path"]).resolve()
    if not path.is_relative_to(root.resolve()):
        raise RuntimeError("Ссылка результата выходит из results/")
    if file_sha256(path) != reference["sha256"]:
        raise RuntimeError(f"Контрольная сумма не совпадает: {path}")
    return path


def load_result(key: str, root: Path = RESULTS) -> tuple[dict, dict]:
    """Прочитать один канонический результат и проверить его зависимости."""

    index = read_json(root / "index.json")
    if index.get("format_version") != 1:
        raise RuntimeError("Неподдерживаемый каталог результатов")
    for relative, digest in index.get("reference_sha256", {}).items():
        reference = (ROOT / relative).resolve()
        if not reference.is_relative_to(ROOT.resolve()) or file_sha256(reference) != digest:
            raise RuntimeError(f"Изменён справочный файл: {relative}")
    entry = index["territories"][key]
    data_path = _checked_path(root, entry["data"])
    report = read_json(_checked_path(root, entry["report"]))
    if (entry["status"] != "complete" or report["stage"] != "complete" or report["issues"]
            or report["run_id"] != entry["run_id"] or report["verified_at"] != entry["last_verified_at"]):
        raise RuntimeError("Каталог и подтверждённый отчёт не согласованы")
    data = json.loads(gzip.decompress(data_path.read_bytes()))
    if (data.get("kind") != "completed-result" or data.get("format_version") != 1
            or data["run_id"] != entry["run_id"] or data["territory"] != key
            or data["original_plan_sha256"] != entry["original_plan_sha256"]):
        raise RuntimeError("Несогласованный компактный результат")
    territory = get_territory(key)
    if any(entry["counts"].get(name) != count for name, count in territory.expected_preparation.items()):
        raise RuntimeError("Количества каталога отличаются от конфигурации")
    if json_digest(territory.raw) != data["config_sha256"]:
        raise RuntimeError("Конфигурация отличается от завершённого результата")
    for source in territory.sources:
        if file_sha256(source.path) != data["source_sha256"][source.path.name]:
            raise RuntimeError(f"Изменён исходник: {source.path.name}")
    boundary_path = (ROOT / entry["boundary_path"]).resolve()
    if not boundary_path.is_relative_to((ROOT / "reference" / "boundaries").resolve()):
        raise RuntimeError("Граница должна находиться в reference/boundaries")
    boundary = read_json(boundary_path)
    boundary_hash = hashlib.sha256(
        json.dumps(boundary["geometry"], sort_keys=True).encode()
    ).hexdigest()
    if boundary_hash != data["boundary_sha256"]:
        raise RuntimeError("Изменена историческая муниципальная граница")
    rows = data["mapping"]
    if (len(rows) != territory.expected_preparation["accepted"]
            or len({row[0] for row in rows}) != len(rows)
            or len({row[1] for row in rows}) != len(rows)
            or len({row[2] for row in rows}) != len(rows)):
        raise RuntimeError("Неполная карта ID или дубли в результате")
    if len(rows) + len(data["retained"]) != entry["counts"]["server_objects"]:
        raise RuntimeError("Количество сохраняемых объектов не согласовано")
    ids = {row[1] for row in rows}
    retained_ids = {r["object"]["physical_object_id"] for r in data["retained"]}
    building_ids = [row[3] for row in rows if row[3] is not None]
    if (ids & retained_ids or len(retained_ids) != len(data["retained"])
            or len(building_ids) != entry["counts"]["buildings"]
            or len(set(building_ids)) != len(building_ids)):
        raise RuntimeError("Дубли или неверное количество серверных ID")
    if set(data["replaced_physical_object_ids"]) & (ids | retained_ids):
        raise RuntimeError("Заменённые ID присутствуют в текущем результате")
    absence = data["historical_geometry_absence"]
    if not absence["all_absent"] or absence["ids_sha256"] != json_digest(data["replaced_geometry_ids"]):
        raise RuntimeError("Свидетельство отсутствия геометрий не соответствует списку ID")
    completion = data["completion_evidence"]
    if completion.get("pending") or completion.get("errors"):
        raise RuntimeError("Результат содержит незавершённые операции")
    if "workers" in completion:
        if (sorted(w["worker_id"] for w in completion["workers"]) != [0, 1, 2, 3]
                or not all(w["complete"] and w["pending"] is None for w in completion["workers"])):
            raise RuntimeError("Неполный комплект завершённых worker")
    if data.get("relations_storage") != "ids-with-service-catalog":
        raise RuntimeError("Неизвестный формат связей сохраняемых объектов")
    used_services = set()
    for retained in data["retained"]:
        relations = retained["relations"]
        service_ids = relations["service_ids"]
        if len(set(service_ids)) != len(service_ids):
            raise RuntimeError("Повтор сервисной связи")
        used_services.update(str(key) for key in service_ids)
        for key in service_ids:
            if data["service_records"].get(str(key), {}).get("service_id") != key:
                raise RuntimeError("Отсутствуют сведения связанного сервиса")
        if any(row["service_id"] is not None and row["service_id"] not in service_ids
               for row in relations["urban_objects"]):
            raise RuntimeError("Urban-object ссылается на неизвестный сервис")
    if used_services != set(data["service_records"]):
        raise RuntimeError("Каталог сервисов не соответствует связям")
    return entry, data


def prepare_completed_result(key: str, paths: RunPaths) -> dict:
    """Подготовить данные по сохранённой границе и зафиксированному году."""

    entry, data = load_result(key)
    return prepare_territory(
        _HistoricalBoundaryApi(read_json(ROOT / entry["boundary_path"])),
        get_territory(key), paths, validation_year=data["validation_year"],
    )


class _HistoricalBoundaryApi:
    def __init__(self, boundary: dict):
        self.boundary = boundary

    def get(self, path: str):
        if path != f"/api/v1/territory/{self.boundary['territory_id']}":
            raise RuntimeError("Offline-подготовка не допускает сетевые запросы")
        return self.boundary


def payload_digest(records: list[dict]) -> str:
    """Сравнить все передаваемые поля без изменявшейся служебной метаинформации."""

    return json_digest([
        {name: record[name] for name in ("osm_id", "physical_object", "building")}
        for record in records
    ])


def reproduce(key: str, root: Path = RESULTS) -> tuple[dict, list[dict]]:
    """Воспроизвести каждое тело запроса без API и старых каталогов запусков."""

    entry, data = load_result(key, root)
    territory = get_territory(key)
    boundary = read_json(ROOT / entry["boundary_path"])
    with tempfile.TemporaryDirectory(prefix="urban-prepare-") as temporary:
        paths = RunPaths(key, data["run_id"], Path(temporary))
        audit = prepare_territory(
            _HistoricalBoundaryApi(boundary), territory, paths,
            validation_year=data["validation_year"],
        )
        accepted = read_json(paths.accepted)
    if payload_digest(accepted) != data["payload_sha256"]:
        raise RuntimeError("Тела запросов отличаются от завершённого импорта")
    if json_digest(audit) != data["preparation_audit_sha256"]:
        raise RuntimeError("Решения подготовки отличаются от завершённого импорта")
    if [record["osm_id"] for record in accepted] != [row[0] for row in data["mapping"]]:
        raise RuntimeError("Порядок OSM ID не соответствует карте результата")
    report = {
        "territory": key, "source": territory.expected_preparation["source"],
        "accepted": len(accepted), "excluded": territory.expected_preparation["excluded"],
        "payload_sha256": data["payload_sha256"], "issues": [], "offline": True,
    }
    return report, accepted


def normalize_server_object(item: dict) -> dict:
    """Сохранить содержимое и ID, исключив изменяемые служебные timestamps."""

    return {
        "physical_object_id": item["physical_object_id"],
        "object_geometry_id": item["object_geometry_id"],
        "physical_object_type_id": item["physical_object_type"]["physical_object_type_id"],
        "territory_id": item["territory"]["id"],
        "osm_id": item.get("osm_id"), "geometry": item["geometry"],
        "address": item.get("address"), "name": item.get("name"),
        "properties": item.get("properties"), "building": item.get("building"),
    }


def compare_payload(item: dict, physical: dict, building: dict | None) -> list[str]:
    """Независимо сравнить геометрию и каждый переданный атрибут."""

    issues = []
    normalized = normalize_server_object(item)
    for field in ("physical_object_type_id", "territory_id", "osm_id", "address", "name", "properties"):
        if normalized.get(field) != physical.get(field):
            issues.append(field)
    if not shape(normalized["geometry"]).equals(shape(physical["geometry"])):
        issues.append("geometry")
    actual_building = normalized["building"]
    if building is None:
        if actual_building:
            issues.append("unexpected_building")
    elif not actual_building:
        issues.append("missing_building")
    else:
        for field, value in building.items():
            if actual_building.get(field) != value:
                issues.append(f"building.{field}")
    return issues


def retained_payload(record: dict) -> dict:
    """Тело создания сохраняемого объекта без прежних серверных ID."""

    item = record["object"]
    physical = {field: item[field] for field in (
        "physical_object_type_id", "territory_id", "geometry", "properties"
    )}
    for field in ("osm_id", "address", "name"):
        if item.get(field) is not None:
            physical[field] = item[field]
    building = item.get("building")
    if building is not None:
        building = {field: building[field] for field in BUILDING_FIELDS
                    if field in building}
    return {"physical_object": physical, "building": building}


def retained_relations(api, physical_id: int, geometry_id: int) -> dict:
    """Сохранить доступные связи объектов, отсутствующих в исходниках."""

    raw = {
        "services": api.get(f"/api/v1/physical_objects/{physical_id}/services"),
        "geometries": api.get(f"/api/v1/physical_objects/{physical_id}/geometries"),
        "urban_objects": api.get(
            "/api/v1/urban_objects_by_object_geometry",
            {"object_geometry_id": geometry_id},
        ),
    }
    return normalize_relations(raw, physical_id, geometry_id)


def normalize_relations(raw: dict, physical_id: int, geometry_id: int) -> dict:
    """Ссылки хранят ID, а не повторяют геометрии и атрибуты своих объектов."""

    services = {item["service_id"]: item for item in raw["services"]}
    links, outside_objects, outside_geometries = [], {}, {}
    for row in raw["urban_objects"]:
        physical, geometry, service = (row.get(name) for name in ("physical_object", "object_geometry", "service"))
        if service:
            services.setdefault(service["service_id"], service)
        owner = physical["physical_object_id"] if physical else None
        linked_geometry = geometry["object_geometry_id"] if geometry else None
        links.append({"urban_object_id": row["urban_object_id"], "physical_object_id": owner,
                      "object_geometry_id": linked_geometry, "service_id": service["service_id"] if service else None})
        if physical and owner != physical_id:
            outside_objects[str(owner)] = physical
        if geometry and linked_geometry != geometry_id:
            outside_geometries[str(linked_geometry)] = geometry
    normalized = {
        "services": [services[key] for key in sorted(services)],
        "geometries": [{"object_geometry_id": key} for key in sorted({g["object_geometry_id"] for g in raw["geometries"]})],
        "urban_objects": sorted(links, key=lambda row: row["urban_object_id"]),
    }
    if outside_objects:
        normalized["external_physical_objects"] = outside_objects
    if outside_geometries:
        normalized["external_geometries"] = outside_geometries
    return normalized


def expanded_relations(record: dict, data: dict) -> dict:
    """Получить сведения сервисов из единственного каталога территории."""

    relations = dict(record["relations"])
    if "service_ids" in relations:
        ids = relations.pop("service_ids")
        relations["services"] = [data["service_records"][str(key)] for key in ids]
    return relations


def fetch_retained_relations(api, objects: list[dict]) -> dict[int, dict]:
    """Четыре GET-потока с общим лимитом, а не четыре независимых лимита."""

    delay = getattr(api, "delay", .1)
    getter = ParallelGet(api, requests_per_second=min(40, 1 / max(delay, .025)))
    def fetch(item):
        return item["physical_object_id"], retained_relations(
            getter, item["physical_object_id"], item["object_geometry_id"]
        )
    with ThreadPoolExecutor(max_workers=4) as executor:
        return dict(executor.map(fetch, objects))


def _code_revision() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def verify_current(api, key: str, *, root: Path = RESULTS,
                   check_old_geometries: bool = False) -> dict:
    """Новая GET-выгрузка; доверие к состоянию apply и старым worker не требуется."""

    entry, data = load_result(key, root)
    _, accepted = reproduce(key, root)
    territory = get_territory(key)
    descendants, objects = api.export_area_objects(territory.territory_id, territory.allowed_type_ids)
    issues = []
    if sorted(descendants) != sorted(data["descendant_ids"]):
        issues.append("territory_tree_changed")
    by_id = {item["physical_object_id"]: item for item in objects}
    expected_ids = {row[1] for row in data["mapping"]}
    expected_ids.update(record["object"]["physical_object_id"] for record in data["retained"])
    if set(by_id) != expected_ids:
        issues.append(f"server_id_set_mismatch:missing={len(expected_ids-set(by_id))},unexpected={len(set(by_id)-expected_ids)}")
    if set(data["replaced_physical_object_ids"]) & set(by_id):
        issues.append("replaced_objects_remain")
    osm_counts = Counter(item["osm_id"] for item in objects if item.get("osm_id"))
    if any(count > 1 for count in osm_counts.values()):
        issues.append("duplicate_osm_ids")
    if len({item["object_geometry_id"] for item in objects}) != len(objects):
        issues.append("shared_object_geometries")
    for record, row in zip(accepted, data["mapping"], strict=True):
        item = by_id.get(row[1])
        if item is None:
            continue
        mismatches = compare_payload(item, record["physical_object"], record["building"])
        if item["object_geometry_id"] != row[2]:
            mismatches.append("object_geometry_id")
        if (item.get("building") or {}).get("id") != row[3]:
            mismatches.append("building_id")
        issues.extend(f"{row[0]}:{field}" for field in mismatches)
    retained_live = [by_id[r["object"]["physical_object_id"]]
                     for r in data["retained"] if r["object"]["physical_object_id"] in by_id]
    relations_by_id = fetch_retained_relations(api, retained_live)
    for record in data["retained"]:
        saved = record["object"]
        item = by_id.get(saved["physical_object_id"])
        if item is None:
            continue
        expected = retained_payload(record)
        mismatches = compare_payload(item, expected["physical_object"], expected["building"])
        if item["object_geometry_id"] != saved["object_geometry_id"]:
            mismatches.append("object_geometry_id")
        if item.get("building") != saved.get("building"):
            mismatches.append("building_details")
        live_relations = relations_by_id[saved["physical_object_id"]]
        if json_digest(live_relations) != json_digest(expanded_relations(record, data)):
            mismatches.append("relations")
        issues.extend(f"retained:{saved['physical_object_id']}:{field}" for field in mismatches)
    if check_old_geometries:
        for geometry_id in data["replaced_geometry_ids"]:
            try:
                api.get(f"/api/v1/object_geometries/{geometry_id}/physical_objects")
            except ApiError as exc:
                if exc.status != 404:
                    raise
            else:
                issues.append(f"replaced_geometry_remains:{geometry_id}")
    report = {
        "territory": key, "run_id": data["run_id"],
        "original_plan_sha256": data["original_plan_sha256"],
        "result_sha256": entry["data"]["sha256"],
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "code_revision": _code_revision(), "stage": "complete" if not issues else "verification_failed",
        "counts": {"imported": len(accepted), "buildings": sum(r["building"] is not None for r in accepted),
                   "retained": len(data["retained"]), "server_objects": len(objects)},
        "checks": {"objects": "fresh_GET", "retained_relations": "fresh_GET",
                   "old_geometries": "fresh_GET" if check_old_geometries else "preserved_historical_evidence"},
        "issues": issues,
    }
    write_json(ARTIFACTS / "verification" / f"{key}.json", report)
    return report






def status_markdown(root: Path = RESULTS) -> str:
    """Формировать статус только из проверяемого каталога результатов."""

    index = read_json(root / "index.json")
    lines = ["# Подтверждённый результат", "", "Источник состояния — `results/index.json`. Команда `status-doc --check` проверяет этот документ.", "",
             "| Территория | Исходных | Принято | Исключено | Buildings | Сохранено | На сервере | Проверка UTC |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |"]
    for key, entry in index["territories"].items():
        load_result(key, root)
        c = entry["counts"]
        lines.append(f"| {entry['name']} | {c['source']} | {c['accepted']} | {c['excluded']} | {c['buildings']} | {c['retained']} | {c['server_objects']} | {entry['last_verified_at']} |")
    lines += ["", "## Ограничения", ""]
    common = set.intersection(*(set(entry["limitations"]) for entry in index["territories"].values()))
    for limitation in next(iter(index["territories"].values()))["limitations"]:
        if limitation in common:
            lines.append(f"- {limitation}")
    for entry in index["territories"].values():
        for limitation in entry["limitations"]:
            if limitation not in common:
                lines.append(f"- {entry['name']}: {limitation}")
    lines += ["", "Это подтверждение на указанное время. Новая GET-проверка: `python -m urban_import verify-current all`.", ""]
    return "\n".join(lines)
