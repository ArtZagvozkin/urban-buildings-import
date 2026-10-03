from types import SimpleNamespace

import pytest

from urban_import.api import ApiError, ChangingPageCount, UrbanApi


def test_direct_address_uses_wifi_source_and_original_tls_name(monkeypatch):
    api = UrbanApi(delay=0)
    api.configure_direct_address("51.250.31.107", "192.168.1.33")
    adapter = api.session.get_adapter("https://51.250.31.107/api/openapi")
    options = adapter.poolmanager.connection_pool_kw
    assert options["source_address"] == ("192.168.1.33", 0)
    assert options["server_hostname"] == "urban-api.testing.idulab.ru"
    assert options["assert_hostname"] == "urban-api.testing.idulab.ru"
    assert not api.session.trust_env

    observed = {}

    def fake_request(method, url, **kwargs):
        observed.update(method=method, url=url, **kwargs)
        return SimpleNamespace(ok=True, status_code=200, json=lambda: {"ok": True})

    monkeypatch.setattr(api.session, "request", fake_request)
    assert api.get("/api/openapi") == {"ok": True}
    assert observed["url"] == "https://51.250.31.107/api/openapi"
    assert observed["headers"] == {"Host": "urban-api.testing.idulab.ru"}


def test_direct_address_rejects_invalid_ips():
    api = UrbanApi()
    with pytest.raises(ValueError):
        api.configure_direct_address("invalid", "192.168.1.33")


def test_cursor_pagination_checks_count_and_duplicate_ids():
    api = UrbanApi()
    pages = iter([
        {"count": 2, "results": [{"physical_object_id": 1}],
         "next": "/api/v2/x?cursor=abc"},
        {"count": 2, "results": [{"physical_object_id": 2}], "next": None},
    ])
    api.get = lambda path, params=None: next(pages)
    assert [item["physical_object_id"] for item in
            api.list_physical_objects(5231, 4)] == [1, 2]

    api.get = lambda path, params=None: {
        "count": 2, "results": [{"physical_object_id": 1}], "next": None,
    }
    with pytest.raises(RuntimeError, match="Неполная выгрузка"):
        api.list_physical_objects(5231, 4)

    pages = iter([
        {"count": 2, "results": [{"physical_object_id": 1}],
         "next": "/api/v2/x?cursor=abc"},
        {"count": 2, "results": [{"physical_object_id": 1}], "next": None},
    ])
    api.get = lambda path, params=None: next(pages)
    with pytest.raises(RuntimeError, match="Повтор physical_object_id"):
        api.list_physical_objects(5231, 4)


def test_cursor_pagination_tolerates_deletions_only_when_requested():
    api = UrbanApi()
    pages = [
        {"count": 3, "results": [{"physical_object_id": 1}],
         "next": "/api/v2/x?cursor=abc"},
        {"count": 2, "results": [{"physical_object_id": 3}], "next": None},
    ]
    api.get = lambda path, params=None: pages.pop(0)
    with pytest.raises(ChangingPageCount):
        api.list_physical_objects(5223, 4)
    pages = [
        {"count": 3, "results": [{"physical_object_id": 1}],
         "next": "/api/v2/x?cursor=abc"},
        {"count": 2, "results": [{"physical_object_id": 3}], "next": None},
    ]
    assert [item["physical_object_id"] for item in
            api.list_physical_objects(5223, 4, strict_count=False)] == [1, 3]


def test_descendant_pagination_and_explicit_area_queries():
    api = UrbanApi()
    responses = {
        (1, 1): {"count": 2, "results": [{"territory_id": 2}], "next": "next"},
        (1, 2): {"count": 2, "results": [{"territory_id": 3}], "next": None},
        (2, 1): {"count": 0, "results": [], "next": None},
        (3, 1): {"count": 0, "results": [], "next": None},
    }
    api.get = lambda path, params=None: responses[(params["parent_id"], params["page"])]
    assert api.list_descendant_territory_ids(1) == [1, 2, 3]

    api.list_descendant_territory_ids = lambda territory_id: [territory_id, 2]
    calls = []

    def list_objects(territory_id, object_type_id, include_children=False,
                     page_size=250):
        calls.append((territory_id, object_type_id, include_children))
        return [{"physical_object_id": territory_id * 10 + object_type_id}]

    api.list_physical_objects = list_objects
    descendants, objects = api.export_area_objects(1, (4, 5))
    assert descendants == [1, 2]
    assert len(objects) == 4
    assert calls == [(1, 4, False), (1, 5, False),
                     (2, 4, False), (2, 5, False)]


def test_live_schema_check_requires_all_single_object_write_methods():
    api = UrbanApi()
    requested = []
    api.get = lambda path: requested.append(path) or {"paths": {
        "/api/v1/physical_objects": {"post": {}},
        "/api/v1/buildings": {"post": {}},
        "/api/v1/physical_objects/{physical_object_id}": {"delete": {}},
        "/api/v1/object_geometries/{object_geometry_id}": {"delete": {}},
    }}
    api.validate_write_contract()
    assert requested == ["/api/openapi"]
    api.get = lambda path: {"paths": {}}
    with pytest.raises(RuntimeError, match="не содержит методы"):
        api.validate_write_contract()


def test_write_retries_only_explicit_rate_limit(monkeypatch):
    api = UrbanApi()
    calls = []

    def request(method, path, body=None):
        calls.append((method, path))
        if len(calls) == 1:
            raise ApiError(method, path, 429, "slow down", retry_after=0)
        return {"ok": True}

    api.request = request
    monkeypatch.setattr("urban_import.api.time.sleep", lambda seconds: None)
    assert api.post("/api/v1/physical_objects", {}) == {"ok": True}
    assert len(calls) == 2

    api.request = lambda method, path, body=None: (_ for _ in ()).throw(
        ApiError(method, path, 503, "temporary")
    )
    with pytest.raises(ApiError):
        api.delete("/api/v1/physical_objects/1")
