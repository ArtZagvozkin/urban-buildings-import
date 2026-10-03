"""Декларативная конфигурация территорий импорта."""

from dataclasses import dataclass
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = ROOT / "config" / "territories.json"


@dataclass(frozen=True)
class SourceConfig:
    """Один неизменяемый GeoJSON и его назначение в URBAN API."""

    path: Path
    physical_object_type_id: int
    is_living: bool
    expected_features: int


@dataclass(frozen=True)
class RetainedServerObjectConfig:
    """Серверный объект, который нельзя удалять из-за доказанных связей."""

    physical_object_id: int
    object_geometry_id: int
    service_ids: tuple[int, ...]
    reason: str


@dataclass(frozen=True)
class TerritoryConfig:
    """Все различия территории, которые не должны попадать в алгоритмы."""

    key: str
    name: str
    territory_id: int
    api_name_contains: str
    allowed_type_ids: tuple[int, ...]
    sources: tuple[SourceConfig, ...]
    replace_geometry_types: tuple[str, ...]
    include_descendants: bool
    expected_preparation: dict[str, int]
    raw: dict
    distributed_workers: int = 1
    replacement_confirmed: bool = True
    retained_server_objects: tuple[RetainedServerObjectConfig, ...] = ()


def load_territories(path: Path = DEFAULT_CONFIG_PATH) -> dict[str, TerritoryConfig]:
    """Загрузить и строго проверить конфигурацию активных территорий."""

    document = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, dict) or not document:
        raise RuntimeError("Конфигурация территорий должна быть непустым объектом")
    result: dict[str, TerritoryConfig] = {}
    used_ids: set[int] = set()
    used_sources: set[Path] = set()
    for key, raw in document.items():
        if not key.isascii() or not key.replace("_", "").isalnum():
            raise RuntimeError(f"Некорректное стабильное имя территории: {key}")
        territory_id = int(raw["territory_id"])
        if territory_id in used_ids:
            raise RuntimeError(f"Повтор territory_id={territory_id}")
        used_ids.add(territory_id)
        allowed = tuple(int(value) for value in raw["allowed_physical_object_type_ids"])
        if not allowed or len(set(allowed)) != len(allowed):
            raise RuntimeError(f"Некорректные разрешённые типы: {key}")
        classification = raw.get("classification", {})
        if (classification.get("conflict_policy") != "exclude" or
                classification.get("require_is_living_match") is not True):
            raise RuntimeError(f"Неподдерживаемые правила классификации: {key}")
        scope = raw.get("scope", {})
        geometry_types = tuple(scope.get("replace_geometry_types", ()))
        if (scope.get("boundary") != "live_municipal_geometry" or
                scope.get("include_descendants") is not True or
                not geometry_types):
            raise RuntimeError(f"Неподдерживаемые ограничения области: {key}")
        sources = []
        source_types = set()
        for item in raw["sources"]:
            source_path = (ROOT / item["path"]).resolve()
            raw_root = (ROOT / "data" / "raw").resolve()
            if source_path.parent != raw_root or source_path.suffix.lower() != ".geojson":
                raise RuntimeError(f"Исходник должен лежать непосредственно в data/raw: {source_path}")
            if source_path in used_sources:
                raise RuntimeError(f"Исходник назначен дважды: {source_path}")
            if not source_path.is_file():
                raise RuntimeError(f"Исходник не найден: {source_path}")
            used_sources.add(source_path)
            object_type = int(item["physical_object_type_id"])
            if object_type not in allowed:
                raise RuntimeError(f"Некорректный тип исходника для {key}: {object_type}")
            source_types.add(object_type)
            sources.append(SourceConfig(
                path=source_path,
                physical_object_type_id=object_type,
                is_living=bool(item["is_living"]),
                expected_features=int(item["expected_features"]),
            ))
        if source_types != set(allowed):
            raise RuntimeError(f"Не каждый разрешённый тип имеет исходный файл: {key}")
        distributed = raw.get("distributed", {})
        retained_server_objects = []
        retained_object_ids: set[int] = set()
        retained_geometry_ids: set[int] = set()
        for item in distributed.get("retained_server_objects", []):
            object_id = int(item["physical_object_id"])
            geometry_id = int(item["object_geometry_id"])
            service_ids = tuple(sorted(int(value) for value in item["service_ids"]))
            reason = str(item.get("reason", "")).strip()
            if (object_id in retained_object_ids or geometry_id in retained_geometry_ids or
                    not service_ids or len(set(service_ids)) != len(service_ids) or not reason):
                raise RuntimeError(f"Некорректное исключение серверного объекта: {key}")
            retained_object_ids.add(object_id)
            retained_geometry_ids.add(geometry_id)
            retained_server_objects.append(RetainedServerObjectConfig(
                physical_object_id=object_id,
                object_geometry_id=geometry_id,
                service_ids=service_ids,
                reason=reason,
            ))
        result[key] = TerritoryConfig(
            key=key,
            name=str(raw["name"]),
            territory_id=territory_id,
            api_name_contains=str(raw["api_name_contains"]),
            allowed_type_ids=allowed,
            sources=tuple(sources),
            replace_geometry_types=geometry_types,
            include_descendants=bool(scope.get("include_descendants", True)),
            expected_preparation={name: int(value) for name, value in raw["expected_preparation"].items()},
            distributed_workers=int(distributed.get("workers", 1)),
            replacement_confirmed=bool(
                distributed.get("replacement_confirmed", True)
            ),
            retained_server_objects=tuple(retained_server_objects),
            raw=raw,
        )
        if result[key].distributed_workers < 1:
            raise RuntimeError(f"Некорректное число worker для {key}")
    return result


def get_territory(key: str, path: Path = DEFAULT_CONFIG_PATH) -> TerritoryConfig:
    """Вернуть активную территорию или завершить работу до обращения к API."""

    territories = load_territories(path)
    try:
        return territories[key]
    except KeyError as exc:
        available = ", ".join(sorted(territories))
        raise RuntimeError(f"Неизвестная или неактивная территория {key!r}; доступны: {available}") from exc
