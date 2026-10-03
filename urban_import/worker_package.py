"""Сборка runtime и переносимых Windows-папок."""

from __future__ import annotations
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from .storage import RunPaths, json_digest, read_json, write_json
from .distributed_plan import WORKER_RATE_DELAY, _assert_signed, _write_checksums, stable_worker, validate_worker_partition

def _template(name: str) -> str:
    return (Path(__file__).parent / "templates" / name).read_text(encoding="utf-8")


def build_worker_runtime(destination: Path) -> Path:
    """Собрать проверенный PyInstaller onedir; обычная venv не переносится."""

    destination = Path(destination)
    if destination.exists():
        if any(destination.iterdir()):
            raise RuntimeError(f"Каталог runtime уже содержит файлы: {destination}")
        destination.rmdir()
    entry = Path(__file__).resolve().parents[1] / "scripts" / "worker_entry.py"
    with tempfile.TemporaryDirectory(prefix="urban-pyinstaller-") as temporary:
        temporary = Path(temporary)
        command = [
            sys.executable, "-m", "PyInstaller", "--noconfirm", "--clean",
            "--onedir", "--collect-data", "urban_import", "--name", "urban-import", "--distpath", str(temporary / "dist"),
            "--workpath", str(temporary / "build"), "--specpath", str(temporary),
            str(entry),
        ]
        subprocess.run(command, check=True)
        shutil.copytree(temporary / "dist" / "urban-import", destination)
    executable = destination / "urban-import.exe"
    if not executable.is_file():
        raise RuntimeError("PyInstaller не создал urban-import.exe")
    return destination


RUN_PS1 = _template('run_ps1.ps1')

RESUME_PS1 = _template('resume_ps1.ps1')

STOP_PS1 = _template('stop_ps1.ps1')

STATUS_PS1 = _template('status_ps1.ps1')

EXPORT_PS1 = _template('export_ps1.ps1')

RUN_CMD = _template('run_cmd.cmd')

RESUME_CMD = _template('resume_cmd.cmd')

STOP_CMD = _template('stop_cmd.cmd')

STATUS_CMD = _template('status_cmd.cmd')

EXPORT_CMD = _template('export_cmd.cmd')

WORKER_README = _template('worker_readme.txt')

ROOT_README = _template('root_readme.txt')

COLLECT_PS1 = _template('collect_ps1.ps1')

VERIFY_PS1 = _template('verify_ps1.ps1')

def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.replace("\n", "\r\n"), encoding="utf-8-sig")


def _write_cmd(path: Path, content: str) -> None:
    """Записать launcher без UTF-8 BOM, который cmd.exe ошибочно считает командой."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content.replace("\n", "\r\n").encode("ascii"))


def package_distribution(paths: RunPaths, destination: Path, runtime: Path) -> Path:
    """Создать четыре полностью автономные папки из master plan."""

    from .distributed import MUTABLE_WORKER_FILES, WorkerState

    master_source = paths.directory / "master-plan.json"
    master = read_json(master_source)
    _assert_signed(master, "master plan")
    validate_worker_partition(master)
    manifest = read_json(paths.directory / master["manifest_file"])
    backup = read_json(paths.directory / master["backup_file"])
    if json_digest(manifest) != master["manifest_sha256"] or json_digest(backup) != master["backup_sha256"]:
        raise RuntimeError("Manifest или backup не соответствует master plan")
    assignments = {item["worker_id"]: item for item in master["worker_assignments"]}
    object_by_id = {item["physical_object_id"]: item for item in backup["objects"]}
    for object_id, item in object_by_id.items():
        owner = stable_worker(object_id, master["workers"])
        if (object_id not in assignments[owner]["physical_object_ids"] or
                item["object_geometry_id"] not in assignments[owner]["geometry_ids"]):
            raise RuntimeError("Удаление назначено не владельцу стабильного хеша")
    for record in manifest["records"]:
        owner = stable_worker(record["osm_id"], master["workers"])
        index = record["input_index"]
        if index not in assignments[owner]["input_indexes"]:
            raise RuntimeError("Создание назначено не владельцу стабильного хеша OSM")
        if record["building"] is not None and index not in assignments[owner]["building_indexes"]:
            raise RuntimeError("Building отделён от своего физического объекта")
    destination = Path(destination)
    if destination.exists() and any(destination.iterdir()):
        raise RuntimeError(f"Каталог distribution уже содержит файлы: {destination}")
    destination.mkdir(parents=True, exist_ok=True)
    shutil.copy2(master_source, destination / "master-plan.json")
    shutil.copy2(paths.directory / master["manifest_file"], destination / "manifest.json")
    shutil.copy2(paths.directory / master["backup_file"], destination / "master-backup.json")
    _write_text(destination / "README-RU.txt", ROOT_README)
    _write_text(destination / "collect-results.ps1", COLLECT_PS1)
    _write_text(destination / "verify-final.ps1", VERIFY_PS1)

    records = manifest["records"]
    worker_integrity_paths: list[Path] = []
    for assignment in master["worker_assignments"]:
        worker_id = assignment["worker_id"]
        folder = destination / f"node-{worker_id + 1}"
        folder.mkdir(parents=True)
        selected_records = [records[index] for index in assignment["input_indexes"]]
        selected_objects = [object_by_id[object_id] for object_id in assignment["physical_object_ids"]]
        worker_backup = {
            "objects": selected_objects,
            "relations": {str(item["physical_object_id"]): backup["relations"][str(item["physical_object_id"])]
                          for item in selected_objects},
            "geometry_links": {str(item["object_geometry_id"]): backup["geometry_links"][str(item["object_geometry_id"])]
                               for item in selected_objects},
        }
        barriers = {
            "descendant_ids": master["descendant_ids"],
            "old_physical_object_ids": master["existing_ids"],
            "old_fingerprints": master["existing_fingerprints"],
            "old_geometry_ids": master["geometry_ids"],
            "retained_ids": master["retained_ids"],
            "retained_fingerprints": master["retained_fingerprints"],
            "protected_relationships": master.get("protected_relationships", {}),
            "expected": [{
                "input_index": item["input_index"],
                "osm_id": item["osm_id"],
                "physical_object_type_id": item["physical_object"]["physical_object_type_id"],
                "geometry": item["physical_object"]["geometry"],
                "geometry_sha256": json_digest(item["physical_object"]["geometry"]),
            } for item in records],
        }
        input_document = {"records": selected_records}
        write_json(folder / "input.json", input_document)
        write_json(folder / "backup.json", worker_backup)
        write_json(folder / "barriers.json", barriers)
        worker_plan = {
            "format_version": 1, "kind": "distributed_worker_plan",
            "run_id": master["run_id"], "territory": master["territory"],
            "territory_id": master["territory_id"],
            "allowed_type_ids": master["allowed_type_ids"],
            "worker_id": worker_id, "workers": master["workers"],
            "master_plan_sha256": master["sha256"],
            "rate_delay_seconds": WORKER_RATE_DELAY,
            "barrier_poll_seconds": master["barrier_poll_seconds"],
            **assignment,
            "input_sha256": json_digest(input_document),
            "backup_sha256": json_digest(worker_backup),
            "barriers_sha256": json_digest(barriers),
            "counts": {"delete_objects": len(assignment["physical_object_ids"]),
                       "delete_geometries": len(assignment["geometry_ids"]),
                       "create_objects": len(selected_records),
                       "create_buildings": len(assignment["building_indexes"])},
            "authorization_required": True,
        }
        worker_plan["sha256"] = json_digest(worker_plan)
        write_json(folder / "worker-plan.json", worker_plan)
        (folder / "master-plan.sha256").write_text(master["sha256"] + "\n", encoding="ascii")
        _write_text(folder / "run.ps1", RUN_PS1)
        _write_text(folder / "resume.ps1", RESUME_PS1)
        _write_text(folder / "stop.ps1", STOP_PS1)
        _write_text(folder / "status.ps1", STATUS_PS1)
        _write_text(folder / "export-result.ps1", EXPORT_PS1)
        _write_cmd(folder / "run.cmd", RUN_CMD)
        _write_cmd(folder / "resume.cmd", RESUME_CMD)
        _write_cmd(folder / "stop.cmd", STOP_CMD)
        _write_cmd(folder / "status.cmd", STATUS_CMD)
        _write_cmd(folder / "export-result.cmd", EXPORT_CMD)
        _write_text(folder / "README-RU.txt", WORKER_README)
        write_json(folder / "settings.json", {
            "request_interval_seconds": WORKER_RATE_DELAY,
            "barrier_poll_seconds": master["barrier_poll_seconds"],
        })
        WorkerState(folder, worker_plan)
        shutil.copytree(runtime, folder / "runtime")
        (folder / "result").mkdir()
        immutable = [path for path in folder.rglob("*")
                     if path.is_file() and "result" not in path.parts
                     and path.name not in MUTABLE_WORKER_FILES]
        _write_checksums(folder, immutable, folder / "checksums.json")
        worker_integrity_paths.extend([
            folder / "worker-plan.json", folder / "master-plan.sha256",
            folder / "checksums.json",
        ])

    root_files = [destination / name for name in (
        "master-plan.json", "manifest.json", "master-backup.json", "README-RU.txt",
        "collect-results.ps1", "verify-final.ps1")]
    root_files.extend(worker_integrity_paths)
    _write_checksums(destination, root_files, destination / "checksums.json")
    return destination
