"""GET-аудит, целостность и создание распределённого master plan."""

from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from shapely.geometry import shape
from .api import UrbanApi
from .config import TerritoryConfig
from .storage import RunPaths, json_digest, read_json, write_json
from .workflow import fingerprint, retained_fingerprint, split_replacement_scope
from .api import ParallelGet as _LimitedParallelGet

WORKER_RATE_DELAY = 0.4


MASTER_PREVIEW_REQUESTS_PER_SECOND = 40


DEFAULT_BARRIER_POLL_SECONDS = 120


def stable_worker(value: str | int, workers: int) -> int:
    """Детерминированно назначить значение worker через SHA-256 modulo N."""

    digest = hashlib.sha256(str(value).encode("utf-8")).digest()
    return int.from_bytes(digest, "big") % workers


def _unsigned_digest(document: dict) -> str:
    return json_digest({key: value for key, value in document.items() if key != "sha256"})


def _assert_signed(document: dict, description: str) -> None:
    if document.get("sha256") != _unsigned_digest(document):
        raise RuntimeError(f"Нарушена контрольная сумма: {description}")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify_file_checksums(root: Path, checksums_file: Path) -> None:
    checksums = read_json(checksums_file)
    for relative, expected in checksums["files"].items():
        path = root / relative
        if not path.is_file() or _file_sha256(path) != expected:
            raise RuntimeError(f"Файл отсутствует или изменён: {relative}")


def _write_checksums(root: Path, relative_paths: list[Path], destination: Path) -> None:
    files = {
        path.relative_to(root).as_posix(): _file_sha256(path)
        for path in sorted(relative_paths)
    }
    write_json(destination, {"algorithm": "sha256", "files": files})


class PreviewDatabase:
    """SQLite-checkpoint дорогой GET-проверки связей master plan."""

    def __init__(self, path: Path, listing_sha256: str):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS relations (object_id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS geometry_links (geometry_id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"
        )
        current = self.connection.execute(
            "SELECT value FROM meta WHERE key='listing_sha256'"
        ).fetchone()
        if current and current[0] != listing_sha256:
            raise RuntimeError("Серверный список изменился после distributed checkpoint")
        self.connection.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES('listing_sha256',?)",
            (listing_sha256,),
        )
        self.connection.commit()

    def get(self, table: str, identifier: int):
        column = "object_id" if table == "relations" else "geometry_id"
        row = self.connection.execute(
            f"SELECT payload FROM {table} WHERE {column}=?", (identifier,)
        ).fetchone()
        return json.loads(row[0]) if row else None

    def put(self, table: str, identifier: int, payload) -> None:
        column = "object_id" if table == "relations" else "geometry_id"
        self.connection.execute(
            f"INSERT OR REPLACE INTO {table}({column},payload) VALUES(?,?)",
            (identifier, json.dumps(payload, ensure_ascii=False, separators=(",", ":"))),
        )
        self.connection.commit()

    def all(self, table: str) -> dict[str, object]:
        column = "object_id" if table == "relations" else "geometry_id"
        return {
            str(identifier): json.loads(payload)
            for identifier, payload in self.connection.execute(
                f"SELECT {column},payload FROM {table} ORDER BY {column}"
            )
        }

    def close(self) -> None:
        self.connection.close()


def _fetch_relation(getter: _LimitedParallelGet, item: dict) -> tuple[int, dict]:
    object_id = item["physical_object_id"]
    return object_id, {
        # Сервисы проверяются полными urban-object связями каждой геометрии.
        "services": [],
        "service_check": "urban_objects_by_object_geometry",
        "geometries": getter.get(f"/api/v1/physical_objects/{object_id}/geometries"),
    }


def _fetch_geometry_links(getter: _LimitedParallelGet, geometry_id: int):
    return geometry_id, getter.get(
        f"/api/v1/urban_objects_by_object_geometry?object_geometry_id={geometry_id}"
    )


def _validate_object_relation(item: dict, related: dict) -> None:
    object_id = item["physical_object_id"]
    if related["services"]:
        raise RuntimeError(f"У объекта {object_id} есть сервисные связи")
    if {value["object_geometry_id"] for value in related["geometries"]} != {
            item["object_geometry_id"]}:
        raise RuntimeError(f"У объекта {object_id} изменились геометрии")


def _validate_geometry_links(geometry_id: int, linked: list[dict],
                             geometry_owner: dict[int, int]) -> None:
    physical_ids = {
        item["physical_object"]["physical_object_id"]
        for item in linked if item.get("physical_object") is not None
    }
    services = [item["service"] for item in linked if item.get("service") is not None]
    geometry_ids = {
        item["object_geometry"]["object_geometry_id"]
        for item in linked if item.get("object_geometry") is not None
    }
    if physical_ids != {geometry_owner[geometry_id]} or geometry_ids != {geometry_id}:
        raise RuntimeError(f"Геометрия {geometry_id} разделяется или связана извне")
    if services:
        raise RuntimeError(f"У геометрии {geometry_id} есть сервисные связи")


def relationship_fingerprint(linked: list[dict]) -> str:
    """Хешировать только устойчивые ID связей сохраняемой геометрии."""

    stable = [{
        "urban_object_id": item.get("urban_object_id"),
        "physical_object_id": (
            item["physical_object"].get("physical_object_id")
            if item.get("physical_object") is not None else None
        ),
        "object_geometry_id": (
            item["object_geometry"].get("object_geometry_id")
            if item.get("object_geometry") is not None else None
        ),
        "service_id": (
            item["service"].get("service_id")
            if item.get("service") is not None else None
        ),
    } for item in linked]
    stable.sort(key=lambda item: (
        item["urban_object_id"] or -1, item["physical_object_id"] or -1,
        item["object_geometry_id"] or -1, item["service_id"] or -1,
    ))
    return json_digest(stable)


def _fetch_relationship_job(getter: _LimitedParallelGet, item: dict,
                            related: dict | None, linked: list[dict] | None):
    """Получить объект и его urban-object связь одним ограниченным заданием."""

    relation_was_missing = related is None
    geometry_was_missing = linked is None
    if related is None:
        _, related = _fetch_relation(getter, item)
    geometry_id = item["object_geometry_id"]
    if linked is None:
        _, linked = _fetch_geometry_links(getter, geometry_id)
    return (item, related, geometry_id, linked,
            relation_was_missing, geometry_was_missing)


def _relationship_issues(item: dict, related: dict, linked: list[dict],
                         geometry_owner: dict[int, int],
                         allowed_service_ids: tuple[int, ...] = ()) -> list[dict]:
    """Описать все связи, запрещающие удаление, не прерывая GET-аудит."""

    issues = []
    object_id = item["physical_object_id"]
    geometry_id = item["object_geometry_id"]
    try:
        _validate_object_relation(item, related)
    except RuntimeError as error:
        issues.append({
            "kind": "physical_object_relationship",
            "physical_object_id": object_id,
            "object_geometry_id": geometry_id,
            "message": str(error),
            "service_ids": sorted({
                service["service_id"] for service in related.get("services", [])
            }),
            "related_geometry_ids": sorted({
                value["object_geometry_id"] for value in related.get("geometries", [])
            }),
        })
    ownership_view = [{**value, "service": None} for value in linked]
    geometry_error = None
    try:
        _validate_geometry_links(geometry_id, ownership_view, geometry_owner)
    except RuntimeError as error:
        geometry_error = str(error)
    actual_service_ids = sorted({
        value["service"]["service_id"] for value in linked
        if value.get("service") is not None
    })
    if actual_service_ids != list(allowed_service_ids):
        if not allowed_service_ids:
            geometry_error = f"У геометрии {geometry_id} есть сервисные связи"
        else:
            geometry_error = (
                f"У геометрии {geometry_id} неожиданный набор сервисных связей: "
                f"{actual_service_ids} вместо {list(allowed_service_ids)}"
            )
    if geometry_error:
        issues.append({
            "kind": "object_geometry_relationship",
            "physical_object_id": object_id,
            "object_geometry_id": geometry_id,
            "message": geometry_error,
            "urban_object_ids": sorted({
                value["urban_object_id"] for value in linked
                if value.get("urban_object_id") is not None
            }),
            "physical_object_ids": sorted({
                value["physical_object"]["physical_object_id"] for value in linked
                if value.get("physical_object") is not None
            }),
            "linked_geometry_ids": sorted({
                value["object_geometry"]["object_geometry_id"] for value in linked
                if value.get("object_geometry") is not None
            }),
            "service_ids": actual_service_ids,
        })
    return issues


def _validate_prepared_sources(territory: TerritoryConfig, audit: dict) -> None:
    if audit.get("config_sha256") != json_digest(territory.raw):
        raise RuntimeError("Конфигурация изменилась после prepare")
    for source in territory.sources:
        if audit["source_sha256"].get(source.path.name) != _file_sha256(source.path):
            raise RuntimeError(f"Исходник изменился: {source.path.name}")


def _worker_assignments(existing: list[dict], accepted: list[dict], workers: int) -> list[dict]:
    assignments = [
        {"worker_id": worker_id, "physical_object_ids": [], "geometry_ids": [],
         "input_indexes": [], "building_indexes": []}
        for worker_id in range(workers)
    ]
    for item in existing:
        owner = stable_worker(item["physical_object_id"], workers)
        assignments[owner]["physical_object_ids"].append(item["physical_object_id"])
        assignments[owner]["geometry_ids"].append(item["object_geometry_id"])
    for index, record in enumerate(accepted):
        owner = stable_worker(record["osm_id"], workers)
        assignments[owner]["input_indexes"].append(index)
        if record["building"] is not None:
            assignments[owner]["building_indexes"].append(index)
    for assignment in assignments:
        for field in ("physical_object_ids", "geometry_ids", "input_indexes", "building_indexes"):
            assignment[field].sort()
    return assignments


def validate_worker_partition(master: dict) -> None:
    """Доказать полноту и непересечение четырёх частей master plan."""

    workers = master["worker_assignments"]
    if [item["worker_id"] for item in workers] != list(range(master["workers"])):
        raise RuntimeError("Неверный набор worker ID")
    expectations = {
        "physical_object_ids": set(master["existing_ids"]),
        "geometry_ids": set(master["geometry_ids"]),
        "input_indexes": set(range(master["manifest_count"])),
        "building_indexes": set(master["building_indexes"]),
    }
    for field, expected in expectations.items():
        observed: set[int] = set()
        for worker in workers:
            values = set(worker[field])
            if len(values) != len(worker[field]) or observed & values:
                raise RuntimeError(f"Worker-планы пересекаются по {field}")
            observed.update(values)
        if observed != expected:
            raise RuntimeError(f"Worker-планы неполны по {field}")


def _server_boundary_issues(items: list[dict], boundary) -> list[dict]:
    """Зафиксировать геометрию сервера вне границы, не меняя область дерева API.

    При полной замене 5223 область удаления задают назначения территории и её
    потомков. Небольшое рассогласование серверной геометрии с муниципальным
    полигоном важно для аудита, но исключение такого объекта оставило бы старый
    объект внутри заменяемого дерева.
    """

    issues = []
    for item in items:
        geometry = shape(item["geometry"])
        if boundary.covers(geometry):
            continue
        issues.append({
            "physical_object_id": item["physical_object_id"],
            "object_geometry_id": item["object_geometry_id"],
            "territory_id": item["territory"]["id"],
            "reason": "crosses_municipal_boundary" if boundary.intersects(geometry)
                      else "outside_municipal_boundary",
        })
    return issues


def create_distributed_master_plan(api: UrbanApi, territory: TerritoryConfig,
                                   paths: RunPaths) -> dict:
    """Только через GET создать backup и master plan четырёх worker."""

    if territory.distributed_workers < 2:
        raise RuntimeError("Территория не настроена для распределённой обработки")
    master_path = paths.directory / "master-plan.json"
    if master_path.exists():
        master = read_json(master_path)
        _assert_signed(master, "master plan")
        return master
    audit, accepted = read_json(paths.audit), read_json(paths.accepted)
    _validate_prepared_sources(territory, audit)
    live_types = {item["physical_object_type_id"] for item in api.get("/api/v1/physical_object_types")}
    if set(territory.allowed_type_ids) - live_types:
        raise RuntimeError("В живом API отсутствуют разрешённые типы")
    live_territory = api.get(f"/api/v1/territory/{territory.territory_id}")
    boundary_sha256 = hashlib.sha256(
        json.dumps(live_territory["geometry"], sort_keys=True).encode()
    ).hexdigest()
    if boundary_sha256 != audit["boundary_sha256"]:
        raise RuntimeError("Муниципальная граница изменилась после prepare")
    descendant_ids, server_objects = api.export_area_objects(
        territory.territory_id, territory.allowed_type_ids
    )
    replacement_candidates, retained = split_replacement_scope(
        server_objects, territory.replace_geometry_types
    )
    candidate_by_id = {
        item["physical_object_id"]: item for item in replacement_candidates
    }
    protected = []
    protected_by_object_id = {
        item.physical_object_id: item for item in territory.retained_server_objects
    }
    for configured in territory.retained_server_objects:
        item = candidate_by_id.get(configured.physical_object_id)
        if item is None:
            raise RuntimeError(
                f"Сохраняемый связанный объект {configured.physical_object_id} исчез"
            )
        if item["object_geometry_id"] != configured.object_geometry_id:
            raise RuntimeError(
                f"У сохраняемого объекта {configured.physical_object_id} изменилась геометрия"
            )
        protected.append(item)
    existing = [
        item for item in replacement_candidates
        if item["physical_object_id"] not in protected_by_object_id
    ]
    retained = [*retained, *protected]
    checked_objects = replacement_candidates
    if any(item["territory"]["id"] not in descendant_ids for item in server_objects):
        raise RuntimeError("API вернул объект вне дерева территории")
    boundary_issues = _server_boundary_issues(
        checked_objects, shape(live_territory["geometry"])
    )
    status_path = paths.directory / "distributed-preview-status.json"
    write_json(status_path, {
        "stage": "checking_object_relations", "server_objects": len(server_objects),
        "replacement_objects": len(existing), "retained_objects": len(retained),
        "protected_linked_objects": len(protected),
        "relations_checked": 0, "geometries_checked": 0,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    listing_sha256 = json_digest(sorted(
        checked_objects, key=lambda item: item["physical_object_id"]
    ))
    checkpoint = PreviewDatabase(paths.directory / "distributed-preview.sqlite3", listing_sha256)
    try:
        existing_ids = {item["physical_object_id"] for item in existing}
        geometry_ids = sorted({item["object_geometry_id"] for item in existing})
        checked_geometry_ids = sorted({
            item["object_geometry_id"] for item in checked_objects
        })
        if len(checked_geometry_ids) != len(checked_objects):
            raise RuntimeError("Несколько заменяемых объектов используют одну геометрию")
        geometry_owner = {
            item["object_geometry_id"]: item["physical_object_id"]
            for item in checked_objects
        }
        jobs = []
        relationship_issues = []
        completed_relations = 0
        completed_geometries = 0
        for item in checked_objects:
            object_id = item["physical_object_id"]
            geometry_id = item["object_geometry_id"]
            related = checkpoint.get("relations", object_id)
            linked = checkpoint.get("geometry_links", geometry_id)
            if related is not None:
                completed_relations += 1
            if linked is not None:
                completed_geometries += 1
            if related is None or linked is None:
                jobs.append((item, related, linked))
            else:
                relationship_issues.extend(
                    _relationship_issues(
                        item, related, linked, geometry_owner,
                        (protected_by_object_id[object_id].service_ids
                         if object_id in protected_by_object_id else ()),
                    )
                )
        getter = _LimitedParallelGet(api)
        with ThreadPoolExecutor(max_workers=4) as executor:
            for start in range(0, len(jobs), 100):
                batch = jobs[start:start + 100]
                for result in executor.map(
                        lambda job: _fetch_relationship_job(
                            getter, job[0], job[1], job[2]
                        ), batch):
                    item, related, geometry_id, linked, new_relation, new_geometry = result
                    object_id = item["physical_object_id"]
                    if new_relation:
                        checkpoint.put("relations", object_id, related)
                        completed_relations += 1
                    if new_geometry:
                        checkpoint.put("geometry_links", geometry_id, linked)
                        completed_geometries += 1
                    relationship_issues.extend(
                        _relationship_issues(
                            item, related, linked, geometry_owner,
                            (protected_by_object_id[object_id].service_ids
                             if object_id in protected_by_object_id else ()),
                        )
                    )
                    if (completed_relations + completed_geometries) % 100 == 0:
                        write_json(status_path, {
                            "stage": "checking_object_and_geometry_relations",
                            "server_objects": len(server_objects),
                            "replacement_objects": len(existing),
                            "retained_objects": len(retained),
                            "protected_linked_objects": len(protected),
                            "relations_checked": completed_relations,
                            "geometries_checked": completed_geometries,
                            "blocking_relationships": len(relationship_issues),
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        })
        if (completed_relations != len(checked_objects) or
                completed_geometries != len(checked_geometry_ids)):
            raise RuntimeError("Checkpoint связей master plan неполон")
        relations = checkpoint.all("relations")
        geometry_links = checkpoint.all("geometry_links")
    finally:
        checkpoint.close()

    if relationship_issues:
        blockers = {
            "kind": "distributed_preview_blockers",
            "run_id": paths.run_id,
            "territory": territory.key,
            "territory_id": territory.territory_id,
            "checks_complete": True,
            "replacement_objects": len(existing),
            "retained_objects": len(retained),
            "protected_linked_objects": len(protected),
            "issues": relationship_issues,
        }
        blockers["sha256"] = json_digest(blockers)
        write_json(paths.directory / "distributed-preview-blockers.json", blockers)
        write_json(status_path, {
            "stage": "blocked_by_relationships",
            "server_objects": len(server_objects),
            "replacement_objects": len(existing),
            "retained_objects": len(retained),
            "protected_linked_objects": len(protected),
            "relations_checked": len(checked_objects),
            "geometries_checked": len(checked_geometry_ids),
            "blocking_relationships": len(relationship_issues),
            "blockers_sha256": blockers["sha256"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        raise RuntimeError(
            "Master plan заблокирован внешними или сервисными связями: "
            f"{len(relationship_issues)}; см. distributed-preview-blockers.json"
        )

    blockers_path = paths.directory / "distributed-preview-blockers.json"
    if blockers_path.exists():
        blockers_path.unlink()
    protected_relationships = {
        str(configured.object_geometry_id): {
            "physical_object_id": configured.physical_object_id,
            "object_geometry_id": configured.object_geometry_id,
            "service_ids": list(configured.service_ids),
            "reason": configured.reason,
            "fingerprint": relationship_fingerprint(
                geometry_links[str(configured.object_geometry_id)]
            ),
        }
        for configured in territory.retained_server_objects
    }

    manifest = {
        "territory": territory.key,
        "records": [{"input_index": index, **record} for index, record in enumerate(accepted)],
    }
    manifest_path = paths.directory / "manifest.json"
    write_json(manifest_path, manifest)
    backup = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "territory_id": territory.territory_id,
        "descendant_ids": descendant_ids,
        "count": len(existing),
        "objects": existing,
        "retained": retained,
        "relations": relations,
        "geometry_links": geometry_links,
        "protected_relationships": protected_relationships,
        "relationship_check": "physical_geometries_and_urban_objects_by_geometry",
        "server_boundary_issues": boundary_issues,
    }
    backup_path = paths.directory / "master-backup.json"
    write_json(backup_path, backup)
    assignments = _worker_assignments(existing, accepted, territory.distributed_workers)
    master = {
        "format_version": 1,
        "kind": "distributed_master_plan",
        "run_id": paths.run_id,
        "territory": territory.key,
        "territory_id": territory.territory_id,
        "allowed_type_ids": list(territory.allowed_type_ids),
        "workers": territory.distributed_workers,
        "worker_rate_limit": 2.5,
        "barrier_poll_seconds": DEFAULT_BARRIER_POLL_SECONDS,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "config_sha256": json_digest(territory.raw),
        "boundary_sha256": boundary_sha256,
        "source_sha256": audit["source_sha256"],
        "audit_sha256": json_digest(audit),
        "manifest_file": manifest_path.name,
        "manifest_sha256": json_digest(manifest),
        "manifest_count": len(accepted),
        "backup_file": backup_path.name,
        "backup_sha256": json_digest(backup),
        "descendant_ids": descendant_ids,
        "existing_ids": sorted(existing_ids),
        "existing_fingerprints": fingerprint(existing),
        "geometry_ids": geometry_ids,
        "retained_ids": sorted(item["physical_object_id"] for item in retained),
        "retained_fingerprints": {
            str(item["physical_object_id"]): retained_fingerprint(item) for item in retained
        },
        "protected_relationships": protected_relationships,
        "server_boundary_issues": boundary_issues,
        "building_indexes": [index for index, record in enumerate(accepted)
                             if record["building"] is not None],
        "worker_assignments": assignments,
        "counts": {
            "source": audit["counts"], "existing": len(existing),
            "retained": len(retained), "accepted": len(accepted),
            "protected_linked": len(protected),
            "server_boundary_issues": len(boundary_issues),
            "planned_buildings": sum(record["building"] is not None for record in accepted),
        },
        "destructive_apply_authorized": False,
        "replacement_confirmed": territory.replacement_confirmed,
        "unresolved": ([] if territory.replacement_confirmed else
                       ["parts_1_4_complete_replacement_not_confirmed"]),
    }
    validate_worker_partition(master)
    master["sha256"] = json_digest(master)
    write_json(master_path, master)
    write_json(status_path, {
        "stage": "complete", "server_objects": len(server_objects),
        "replacement_objects": len(existing), "retained_objects": len(retained),
        "protected_linked_objects": len(protected),
        "relations_checked": len(checked_objects),
        "geometries_checked": len(checked_geometry_ids),
        "master_plan_sha256": master["sha256"],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    return master
