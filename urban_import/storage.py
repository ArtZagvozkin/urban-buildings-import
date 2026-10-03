"""Единая раскладка локальных артефактов и атомарная запись JSON."""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import time

from .config import ROOT


ARTIFACTS = ROOT / "artifacts"


@dataclass(frozen=True)
class RunPaths:
    """Пути одного запуска; все файлы территории находятся рядом."""

    territory: str
    run_id: str
    directory: Path

    @property
    def prepared(self) -> Path:
        return self.directory / "prepared"

    @property
    def audit(self) -> Path:
        return self.prepared / "audit.json"

    @property
    def accepted(self) -> Path:
        return self.prepared / "accepted.json"

    @property
    def disputed(self) -> Path:
        return self.prepared / "disputed.geojson"

    @property
    def plan(self) -> Path:
        return self.directory / "plan.json"

    @property
    def backup(self) -> Path:
        return self.directory / "backup.json"

    @property
    def preview_checkpoint(self) -> Path:
        return self.directory / "preview_checkpoint.json"

    @property
    def state(self) -> Path:
        return self.directory / "state.json"

    @property
    def events(self) -> Path:
        return self.directory / "events.jsonl"

    @property
    def verification(self) -> Path:
        return self.directory / "verification.json"

    @property
    def verify_checkpoint(self) -> Path:
        return self.directory / "verify_checkpoint.json"


def json_digest(value) -> str:
    """Стабильная SHA-256 для защиты плана и его входов."""

    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def read_json(path: Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path: Path, value) -> None:
    """Атомарно записать JSON и принудительно сбросить данные на диск."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, separators=(",", ":"))
        stream.flush()
        os.fsync(stream.fileno())
    for attempt in range(10):
        try:
            temporary.replace(path)
            return
        except PermissionError:
            if attempt == 9:
                raise
            time.sleep(0.05 * (attempt + 1))


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def run_paths(territory: str, run_id: str, root: Path = ARTIFACTS) -> RunPaths:
    directory = Path(root) / territory / run_id
    return RunPaths(territory, run_id, directory)


def activate_run(paths: RunPaths, root: Path = ARTIFACTS) -> None:
    pointer = Path(root) / paths.territory / "active_run.json"
    write_json(pointer, {"run_id": paths.run_id})


def create_run(territory: str, run_id: str | None = None,
               root: Path = ARTIFACTS, activate: bool = True) -> RunPaths:
    """Создать пустой запуск; существующий каталог никогда не перезаписывается."""

    paths = run_paths(territory, run_id or new_run_id(), root)
    if paths.directory.exists() and any(paths.directory.iterdir()):
        raise RuntimeError(f"Каталог запуска уже содержит файлы: {paths.directory}")
    paths.prepared.mkdir(parents=True, exist_ok=True)
    if activate:
        activate_run(paths, root)
    return paths


def resolve_run(territory: str, run_id: str | None = None,
                root: Path = ARTIFACTS) -> RunPaths:
    """Найти указанный или активный запуск территории."""

    if run_id is None:
        pointer = Path(root) / territory / "active_run.json"
        if not pointer.is_file():
            raise RuntimeError(f"Нет активного запуска для {territory}; сначала выполните prepare")
        run_id = str(read_json(pointer)["run_id"])
    paths = run_paths(territory, run_id, root)
    if not paths.directory.is_dir():
        raise RuntimeError(f"Запуск не найден: {paths.directory}")
    return paths
