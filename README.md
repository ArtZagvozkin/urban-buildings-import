# URBAN Buildings Import

История собрана тематически из завершённого проекта. Даты коммитов обозначают
пересборку истории, а не время выполнения импортов. Исторические SHA в
свидетельствах являются документальными идентификаторами и не требуют старых Git-объектов.

## Установка

Windows x64, CPython 3.14.4. Каждый checkout имеет собственную `.venv`.

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.lock
.\.venv\Scripts\python -m pip install -e . --no-deps --no-build-isolation
```

## Доступные команды

```powershell
python -m urban_import prepare <territory>
python -m urban_import preview <territory>
python -m urban_import apply <territory>
python -m urban_import verify <territory>
```

Исходники `data/raw/` и исторические границы `reference/boundaries/` неизменяемы.
Рабочие планы, backup, журналы и runtime находятся вне Git.
Справка команд на русском: `python -m urban_import <command> --help`.
