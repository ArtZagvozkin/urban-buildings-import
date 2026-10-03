"""Проверка исходных GeoJSON и построение запросов URBAN API."""

from collections import Counter
from datetime import date
import hashlib
import json
import re

from pyproj import Geod
from shapely import make_valid, normalize
from shapely.geometry import mapping, shape

from .config import SourceConfig, TerritoryConfig
from .storage import RunPaths, json_digest, write_json


RESIDENTIAL_TAGS = {
    "apartments", "house", "residential", "detached", "terrace",
    "dormitory", "semidetached_house",
}
NONRESIDENTIAL_TAGS = {
    "retail", "commercial", "industrial", "office", "school",
    "kindergarten", "hospital", "warehouse", "garages", "garage",
    "service", "shop", "church", "chapel", "hangar", "greenhouse",
    "shed", "roof", "civic", "public", "construction", "ruins",
}
STRONG_NONRESIDENTIAL_AMENITIES = {
    "townhall", "courthouse", "fire_station", "police", "school",
    "kindergarten", "college", "university", "hospital", "clinic",
    "mortuary", "music_school", "place_of_worship", "public_building",
    "theatre", "car_wash", "fuel", "parking",
}
GEOD = Geod(ellps="WGS84")


def geodesic_area(geometry) -> float:
    """Площадь в квадратных метрах на эллипсоиде WGS84."""

    return abs(GEOD.geometry_area_perimeter(geometry)[0])


def parse_positive_integer(value, upper: int | None = None) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    if not re.fullmatch(r"[0-9]+", text):
        return None
    number = int(text)
    return number if number > 0 and (upper is None or number <= upper) else None


def format_address(properties: dict) -> str | None:
    """Вернуть пригодный адрес только при наличии улицы/места и номера дома."""

    number = properties.get("addr:housenumber")
    street = properties.get("addr:street") or properties.get("addr:place")
    city = properties.get("addr:city")
    if not number or not street:
        return None
    parts = [str(value).strip() for value in (city, street, number)
             if value and str(value).strip()]
    return ", ".join(parts) if len(parts) >= 2 else None


def build_api_record(feature: dict, territory_id: int, object_type_id: int,
                     geometry, validation_year: int | None = None) -> dict:
    """Построить запросы без подмены OSM ID серверным идентификатором."""

    properties = feature["properties"]
    osm_id = str(properties.get("@id") or properties.get("id"))
    osm_tags = {key: value for key, value in properties.items()
                if value is not None and key not in {"id", "@id"}}
    physical_object = {
        "geometry": mapping(geometry),
        "territory_id": territory_id,
        "physical_object_type_id": object_type_id,
        "osm_id": osm_id,
        "properties": {"source": "URBAN Buildings Import", "osm_tags": osm_tags},
    }
    address = format_address(properties)
    if address:
        physical_object["address"] = address
    if properties.get("name"):
        physical_object["name"] = str(properties["name"])
    building = None
    if object_type_id == 4:
        building = {}
        floors = parse_positive_integer(properties.get("building:levels"), 150)
        built_year = parse_positive_integer(
            properties.get("start_date"), validation_year or date.today().year
        )
        if floors:
            building["floors"] = floors
        if built_year and built_year >= 1700:
            building["built_year"] = built_year
    return {"osm_id": osm_id, "physical_object": physical_object, "building": building}


def validate_feature(feature: dict, boundary, source: SourceConfig,
                     seen_osm_ids: set[str], seen_geometry_hashes: set[str]):
    """Проверить назначение, топологию, дубли и муниципальную область."""

    reasons: list[str] = []
    fixes: list[str] = []
    properties = feature.get("properties") or {}
    osm_id = properties.get("@id") or properties.get("id")
    if not osm_id:
        reasons.append("missing_osm_id")
    elif str(osm_id) in seen_osm_ids:
        reasons.append("duplicate_osm_id")
    else:
        seen_osm_ids.add(str(osm_id))
    if properties.get("is_living") is not source.is_living:
        reasons.append("is_living_conflicts_with_source")
    building_tag = str(properties.get("building") or "").lower()
    if source.is_living and building_tag in NONRESIDENTIAL_TAGS:
        reasons.append("building_tag_nonresidential")
    if not source.is_living and building_tag in RESIDENTIAL_TAGS:
        reasons.append("building_tag_residential")
    if source.is_living and building_tag == "yes":
        if properties.get("shop") or properties.get("office"):
            reasons.append("commercial_use_conflicts_with_residential")
        if str(properties.get("amenity") or "").lower() in STRONG_NONRESIDENTIAL_AMENITIES:
            reasons.append("civic_or_service_use_conflicts_with_residential")
        if (properties.get("power") or properties.get("man_made") or
                properties.get("landuse") == "industrial"):
            reasons.append("infrastructure_use_conflicts_with_residential")
    try:
        geometry = shape(feature["geometry"])
    except Exception:
        return None, reasons + ["unreadable_geometry"], fixes
    if geometry.is_empty or geometry.geom_type not in {"Polygon", "MultiPolygon"}:
        return None, reasons + ["empty_or_nonpolygon"], fixes
    if not geometry.is_valid:
        repaired = make_valid(geometry)
        if repaired.geom_type in {"Polygon", "MultiPolygon"} and repaired.is_valid and not repaired.is_empty:
            old_area = geodesic_area(geometry)
            new_area = geodesic_area(repaired)
            if old_area > 0 and abs(old_area - new_area) / old_area <= 0.001:
                geometry = repaired
                fixes.append("make_valid_area_change_le_0.1_percent")
            else:
                reasons.append("invalid_geometry_material_change")
        else:
            reasons.append("invalid_geometry_unrepairable")
    if geometry.is_valid:
        if geodesic_area(geometry) < 1:
            reasons.append("area_under_1_square_metre")
        if not boundary.covers(geometry):
            reasons.append("crosses_municipal_boundary" if boundary.intersects(geometry)
                           else "outside_municipal_boundary")
        geometry_hash = hashlib.sha256(normalize(geometry).wkb).hexdigest()
        if geometry_hash in seen_geometry_hashes:
            reasons.append("duplicate_geometry")
        else:
            seen_geometry_hashes.add(geometry_hash)
    return geometry, reasons, fixes


def _assert_expected_counts(territory: TerritoryConfig, counts: Counter) -> None:
    actual = {
        "source": counts["source_4"] + counts["source_5"],
        "accepted": counts["accepted_4"] + counts["accepted_5"],
        "excluded": counts["excluded"],
        "residential": counts["accepted_4"],
        "nonresidential": counts["accepted_5"],
    }
    mismatches = {
        key: (expected, actual.get(key))
        for key, expected in territory.expected_preparation.items()
        if actual.get(key) != expected
    }
    if mismatches:
        raise RuntimeError(
            f"Результат подготовки {territory.key} изменился: "
            f"расхождения {mismatches}, получено {actual}"
        )


def prepare_territory(api, territory: TerritoryConfig, paths: RunPaths, *,
                      validation_year: int | None = None) -> dict:
    """Проверить все исходники территории и сохранить воспроизводимый результат."""

    live = api.get(f"/api/v1/territory/{territory.territory_id}")
    if (live["territory_id"] != territory.territory_id or
            territory.api_name_contains.lower() not in live["name"].lower()):
        raise RuntimeError("Территория API не соответствует конфигурации")
    boundary = shape(live["geometry"])
    if boundary.is_empty or not boundary.is_valid:
        raise RuntimeError("Муниципальная граница API пуста или невалидна")
    accepted: list[dict] = []
    disputed: list[dict] = []
    counts: Counter = Counter()
    reason_counts: Counter = Counter()
    seen_osm_ids: set[str] = set()
    seen_geometry_hashes: set[str] = set()
    source_hashes: dict[str, str] = {}
    for source in territory.sources:
        source_hashes[source.path.name] = hashlib.sha256(source.path.read_bytes()).hexdigest()
        collection = json.loads(source.path.read_text(encoding="utf-8"))
        if collection.get("type") != "FeatureCollection":
            raise RuntimeError(f"Файл не является FeatureCollection: {source.path}")
        features = collection.get("features", [])
        if len(features) != source.expected_features:
            raise RuntimeError(
                f"Число объектов изменилось в {source.path.name}: "
                f"{len(features)} вместо {source.expected_features}"
            )
        object_type = source.physical_object_type_id
        for index, feature in enumerate(features):
            counts[f"source_{object_type}"] += 1
            geometry, reasons, fixes = validate_feature(
                feature, boundary, source, seen_osm_ids, seen_geometry_hashes
            )
            if fixes:
                counts["fixed"] += 1
            if reasons:
                counts["excluded"] += 1
                reason_counts.update(reasons)
                disputed.append({
                    "type": "Feature",
                    "geometry": feature.get("geometry"),
                    "properties": {
                        "source_file": source.path.name,
                        "source_index": index,
                        "osm_id": (feature.get("properties") or {}).get("@id"),
                        "reasons": reasons,
                        "fixes": fixes,
                    },
                })
            else:
                counts[f"accepted_{object_type}"] += 1
                record = build_api_record(
                    feature, territory.territory_id, object_type, geometry, validation_year
                )
                record["source"] = {"file": source.path.name, "index": index}
                record["fixes"] = fixes
                accepted.append(record)
    _assert_expected_counts(territory, counts)
    audit = {
        "territory": territory.key,
        "territory_id": territory.territory_id,
        "name": live["name"],
        "config_sha256": json_digest(territory.raw),
        "boundary_sha256": hashlib.sha256(
            json.dumps(live["geometry"], sort_keys=True).encode()
        ).hexdigest(),
        "source_sha256": source_hashes,
        "counts": dict(counts),
        "reasons": dict(reason_counts),
    }
    write_json(paths.accepted, accepted)
    write_json(paths.disputed, {"type": "FeatureCollection", "features": disputed})
    write_json(paths.audit, audit)
    return audit
