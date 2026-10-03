import json

from shapely.geometry import Polygon

import urban_import.config as config_module
from urban_import.config import SourceConfig, load_territories
from urban_import.prepare import build_api_record, prepare_territory, validate_feature
from urban_import.storage import RunPaths


def test_spatial_scope_and_conflicting_classification():
    boundary = Polygon([(0, 0), (0.01, 0), (0.01, 0.01), (0, 0.01)])
    source = SourceConfig(None, 4, True, 1)
    outside_retail = {
        "properties": {"@id": "way/1", "is_living": True, "building": "retail"},
        "geometry": {"type": "Polygon", "coordinates": [[
            (0.02, 0), (0.021, 0), (0.021, 0.001), (0.02, 0.001), (0.02, 0),
        ]]},
    }
    _, reasons, _ = validate_feature(outside_retail, boundary, source, set(), set())
    assert "outside_municipal_boundary" in reasons
    assert "building_tag_nonresidential" in reasons

    crossing = {
        "properties": {"@id": "way/2", "is_living": True, "building": "yes"},
        "geometry": {"type": "Polygon", "coordinates": [[
            (0.009, 0.005), (0.011, 0.005), (0.011, 0.006),
            (0.009, 0.006), (0.009, 0.005),
        ]]},
    }
    _, reasons, _ = validate_feature(crossing, boundary, source, set(), set())
    assert "crosses_municipal_boundary" in reasons


def test_request_transformation_keeps_osm_id_separate():
    feature = {"properties": {
        "@id": "way/10", "is_living": True,
        "addr:street": "Лесная", "addr:housenumber": "2",
        "building:levels": "5", "start_date": "1986",
    }}
    geometry = Polygon([(37, 55), (37.001, 55), (37.001, 55.001), (37, 55.001)])
    record = build_api_record(feature, 5231, 4, geometry)
    assert record["osm_id"] == "way/10"
    assert record["physical_object"]["osm_id"] == "way/10"
    assert "physical_object_id" not in record["physical_object"]
    assert record["physical_object"]["territory_id"] == 5231
    assert record["building"] == {"floors": 5, "built_year": 1986}


def test_new_territory_is_added_only_by_configuration(tmp_path, monkeypatch):
    monkeypatch.setattr(config_module, "ROOT", tmp_path)
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    geometry = {
        "type": "Polygon",
        "coordinates": [[[0, 0], [0.004, 0], [0.004, 0.002], [0, 0.002], [0, 0]]],
    }
    source_paths = [raw / "sample_part1.geojson", raw / "sample_part2.geojson"]
    for index, source_path in enumerate(source_paths, start=1):
        offset = (index - 1) * 0.002
        part_geometry = {
            "type": "Polygon",
            "coordinates": [[[offset, 0], [offset + 0.001, 0],
                             [offset + 0.001, 0.001], [offset, 0.001], [offset, 0]]],
        }
        source_path.write_text(json.dumps({
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature", "geometry": part_geometry,
                "properties": {"@id": f"way/{index}", "is_living": True,
                               "building": "house"},
            }],
        }), encoding="utf-8")
    document = {
        "sample": {
            "name": "Тест",
            "territory_id": 99,
            "api_name_contains": "Тест",
            "allowed_physical_object_type_ids": [4],
            "sources": [{
                "path": f"data/raw/{source_path.name}",
                "physical_object_type_id": 4,
                "is_living": True,
                "expected_features": 1,
            } for source_path in source_paths],
            "classification": {
                "conflict_policy": "exclude", "require_is_living_match": True,
            },
            "scope": {
                "boundary": "live_municipal_geometry",
                "include_descendants": True,
                "replace_geometry_types": ["Polygon", "MultiPolygon"],
            },
            "expected_preparation": {
                "source": 2, "accepted": 2, "excluded": 0,
                "residential": 2, "nonresidential": 0,
            },
        }
    }
    config_path = tmp_path / "territories.json"
    config_path.write_text(json.dumps(document), encoding="utf-8")
    territory = load_territories(config_path)["sample"]
    run = RunPaths("sample", "test", tmp_path / "artifacts" / "sample" / "test")

    class Api:
        def get(self, path):
            return {"territory_id": 99, "name": "Тестовая территория", "geometry": geometry}

    audit = prepare_territory(Api(), territory, run)
    assert audit["counts"]["accepted_4"] == 2
    assert len(json.loads(run.accepted.read_text(encoding="utf-8"))) == 2
