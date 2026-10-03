"""Небольшой HTTP-клиент с безопасной пагинацией и записью."""

import ipaddress
import threading
import time
from urllib.parse import parse_qs, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter


DEFAULT_BASE_URL = "https://urban-api.testing.idulab.ru"


class ApiError(RuntimeError):
    def __init__(self, method: str, path: str, status: int, detail: str,
                 retry_after: float | None = None):
        super().__init__(f"{method} {path}: HTTP {status}: {detail[:400]}")
        self.status = status
        self.retry_after = retry_after


class UncertainWrite(RuntimeError):
    """Сервер мог применить POST/DELETE, но клиент не получил доказательство."""


class TransientReadError(RuntimeError):
    """Сетевой сбой GET: безопасно повторить чтение после паузы."""


class ChangingPageCount(RuntimeError):
    """Состав территории меняется во время cursor-пагинации."""


class _DirectAddressAdapter(HTTPAdapter):
    """Подключаться к заданному IP, проверяя TLS по исходному имени API."""

    def __init__(self, server_name: str, source_ip: str):
        self.server_name = server_name
        self.source_ip = source_ip
        super().__init__()

    def init_poolmanager(self, connections, maxsize, block=False, **kwargs):
        kwargs.update(source_address=(self.source_ip, 0),
                      server_hostname=self.server_name,
                      assert_hostname=self.server_name)
        return super().init_poolmanager(connections, maxsize, block=block, **kwargs)


class UrbanApi:
    """Клиент только для относительных путей одного настроенного сервера."""

    def __init__(self, base_url: str = DEFAULT_BASE_URL, delay: float = 0.2,
                 timeout: float = 90):
        self.base_url = base_url.rstrip("/")
        self.delay = delay
        self.timeout = timeout
        self.session = requests.Session()
        self._last_request_started = 0.0
        self._direct_server_ip = None
        self._direct_source_ip = None

    def configure_direct_address(self, server_ip: str, source_ip: str) -> None:
        """Обойти сломанный TUN-маршрут только для этого клиента URBAN API.

        IP сервера и локального интерфейса задаются в settings.json worker.
        Имя API остаётся именем TLS SNI и проверки сертификата. Настройка не
        меняет системные маршруты, план, состояние и другие worker.
        """

        server_ip = str(ipaddress.IPv4Address(server_ip))
        source_ip = str(ipaddress.IPv4Address(source_ip))
        parsed = urlparse(self.base_url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("Прямой маршрут допускается только для HTTPS API")
        netloc = server_ip + (f":{parsed.port}" if parsed.port else "")
        self.session.trust_env = False
        self.session.mount(f"https://{netloc}/",
                           _DirectAddressAdapter(parsed.hostname, source_ip))
        self._direct_server_ip = server_ip
        self._direct_source_ip = source_ip

    def request(self, method: str, path: str, *, params=None, body=None):
        parsed = urlparse(path)
        if parsed.scheme or parsed.netloc or not path.startswith("/"):
            raise ValueError("Разрешены только относительные пути настроенного API")
        wait = self.delay - (time.monotonic() - self._last_request_started)
        if wait > 0:
            time.sleep(wait)
        self._last_request_started = time.monotonic()
        try:
            url = self.base_url + path
            headers = None
            if self._direct_server_ip:
                target = urlparse(url)
                host = target.netloc
                netloc = self._direct_server_ip + (
                    f":{target.port}" if target.port else "")
                url = urlunparse(target._replace(netloc=netloc))
                headers = {"Host": host}
            response = self.session.request(
                method, url, params=params, json=body, headers=headers,
                timeout=(min(15, self.timeout), self.timeout),
            )
        except requests.RequestException as exc:
            if method != "GET":
                raise UncertainWrite(f"{method} {path}: {exc}") from exc
            raise TransientReadError(f"GET {path}: {exc}") from exc
        if not response.ok:
            retry_after = response.headers.get("Retry-After")
            try:
                retry_after = float(retry_after) if retry_after else None
            except ValueError:
                retry_after = None
            raise ApiError(method, path, response.status_code, response.text, retry_after)
        if response.status_code == 204:
            return None
        try:
            return response.json()
        except ValueError as exc:
            if method != "GET":
                raise UncertainWrite(f"{method} {path}: ответ не является JSON") from exc
            raise RuntimeError(f"GET {path}: ответ не является JSON") from exc

    def get(self, path: str, params=None):
        """Повторить только безопасный GET при временной сетевой ошибке."""

        for attempt in range(3):
            retry_after = None
            try:
                return self.request("GET", path, params=params)
            except ApiError as exc:
                if exc.status not in {429, 500, 502, 503, 504} or attempt == 2:
                    raise
                retry_after = exc.retry_after
            except TransientReadError:
                if attempt == 2:
                    raise
            time.sleep(retry_after if retry_after is not None else 1 + attempt * 2)

    def post(self, path: str, body: dict):
        return self._rate_limited_write("POST", path, body)

    def delete(self, path: str):
        return self._rate_limited_write("DELETE", path, None)

    def _rate_limited_write(self, method: str, path: str, body):
        """Повторить только явно отклонённый сервером HTTP 429."""

        for attempt in range(4):
            try:
                return self.request(method, path, body=body)
            except ApiError as exc:
                if exc.status != 429 or attempt == 3:
                    raise
                time.sleep(exc.retry_after if exc.retry_after is not None else 2 ** attempt)

    def validate_write_contract(self) -> dict:
        """Перед записью подтвердить нужные одиночные методы в живой OpenAPI."""

        schema = self.get("/api/openapi")
        required = {
            "/api/v1/physical_objects": "post",
            "/api/v1/buildings": "post",
            "/api/v1/physical_objects/{physical_object_id}": "delete",
            "/api/v1/object_geometries/{object_geometry_id}": "delete",
        }
        paths = schema.get("paths", {})
        missing = [f"{method.upper()} {path}" for path, method in required.items()
                   if method not in paths.get(path, {})]
        if missing:
            raise RuntimeError(f"Живая схема API не содержит методы: {', '.join(missing)}")
        return schema

    def list_physical_objects(self, territory_id: int, object_type_id: int,
                              include_children: bool = False,
                              page_size: int = 250,
                              strict_count: bool = True) -> list[dict]:
        """Пройти cursor-пагинацию и закрыться с ошибкой при неполной выгрузке."""

        path = f"/api/v2/territory/{territory_id}/physical_objects_with_geometry"
        params = {
            "physical_object_type_id": object_type_id,
            "include_child_territories": str(include_children).lower(),
            "page_size": page_size,
        }
        seen_next_urls: set[str] = set()
        seen_ids: set[int] = set()
        result: list[dict] = []
        expected_count = None
        while True:
            page = self.get(path, params)
            if expected_count is None:
                expected_count = page["count"]
            elif strict_count and page["count"] != expected_count:
                raise ChangingPageCount("Количество API изменилось во время пагинации")
            for item in page["results"]:
                object_id = item["physical_object_id"]
                if object_id in seen_ids:
                    raise RuntimeError(f"Повтор physical_object_id={object_id}")
                seen_ids.add(object_id)
                result.append(item)
            next_url = page.get("next")
            if not next_url:
                break
            if next_url in seen_next_urls:
                raise RuntimeError("API вернул цикл cursor-пагинации")
            seen_next_urls.add(next_url)
            parsed = urlparse(next_url)
            if parsed.netloc and parsed.netloc != urlparse(self.base_url).netloc:
                raise RuntimeError("Следующая страница указывает на другой сервер")
            cursor = parse_qs(parsed.query).get("cursor", [None])[0]
            if not cursor:
                raise RuntimeError("Следующая страница не содержит cursor")
            params["cursor"] = cursor
        if strict_count and len(result) != expected_count:
            raise RuntimeError(
                f"Неполная выгрузка API: получено {len(result)}, ожидалось {expected_count}"
            )
        return result

    def list_descendant_territory_ids(self, territory_id: int) -> list[int]:
        """Обойти всё дерево территорий с проверкой страниц и циклов."""

        ids = [territory_id]
        known = {territory_id}
        position = 0
        while position < len(ids):
            parent_id = ids[position]
            position += 1
            page_number = 1
            children: list[dict] = []
            while True:
                page = self.get(
                    "/api/v1/territories_without_geometry",
                    {"parent_id": parent_id, "page": page_number, "page_size": 100},
                )
                children.extend(page["results"])
                if not page.get("next"):
                    if len(children) != page["count"]:
                        raise RuntimeError("Неполный список дочерних территорий")
                    break
                page_number += 1
            for child in children:
                child_id = child["territory_id"]
                if child_id in known:
                    raise RuntimeError("Иерархия территорий содержит повтор или цикл")
                known.add(child_id)
                ids.append(child_id)
        return ids

    def export_area_objects(self, territory_id: int,
                            allowed_type_ids: tuple[int, ...],
                            strict_count: bool = True) -> tuple[list[int], list[dict]]:
        """Выгрузить каждый тип отдельно для каждой дочерней территории.

        Общий `include_child_territories=true` на тестовом сервере нестабилен,
        поэтому полнота доказывается явным обходом дерева.
        """

        descendants = self.list_descendant_territory_ids(territory_id)
        objects: list[dict] = []
        seen_ids: set[int] = set()
        for descendant_id in descendants:
            for object_type_id in allowed_type_ids:
                query_options = {} if strict_count else {"strict_count": False}
                for item in self.list_physical_objects(
                        descendant_id, object_type_id, include_children=False,
                        **query_options):
                    object_id = item["physical_object_id"]
                    if object_id in seen_ids:
                        raise RuntimeError(
                            f"Объект {object_id} повторился между территориями"
                        )
                    seen_ids.add(object_id)
                    objects.append(item)
        return descendants, objects


class ParallelGet:
    """Ограничивать общий темп GET с отдельной HTTP-сессией каждого потока."""

    def __init__(self, api, requests_per_second: float = 40):
        self.api = api
        self.interval = 1 / requests_per_second
        self.lock = threading.Lock()
        self.local = threading.local()
        self.last_started = 0.0

    def get(self, path: str, params=None):
        with self.lock:
            wait = self.interval - (time.monotonic() - self.last_started)
            if wait > 0:
                time.sleep(wait)
            self.last_started = time.monotonic()
        client = getattr(self.local, "client", None)
        if client is None:
            if isinstance(self.api, UrbanApi):
                client = type(self.api)(self.api.base_url, delay=0, timeout=self.api.timeout)
                if self.api._direct_server_ip:
                    client.configure_direct_address(self.api._direct_server_ip, self.api._direct_source_ip)
            else:
                client = self.api
            self.local.client = client
        return client.get(path) if params is None else client.get(path, params)
