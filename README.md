# URBAN Buildings Import

Python-проект импортов Реутова, Жуковского, Лотошина и Одинцовского округа.
Содержит исходники, исторические границы, компактные окончательные результаты
и код воспроизведения, проверки и восстановления.

**Источник подтверждённого состояния - `results/index.json`.** Рабочие
состояния новых операций находятся в `artifacts/` и не заменяют этот каталог.


## Установка в PowerShell

Проверенное окружение: Windows x64, CPython 3.14.4. Версии библиотек и GEOS/PROJ
сохранены в `reference/environment.json`; зависимости закреплены одним lock-файлом.

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.lock
.\.venv\Scripts\python -m pip install -e . --no-deps --no-build-isolation
.\.venv\Scripts\python -m pytest -q
```

Каждый checkout использует собственную `.venv`. Junction на окружение другого
checkout запрещён.

## Демонстрация и повторение

```powershell
.\.venv\Scripts\python -m urban_import status
.\.venv\Scripts\python -m urban_import reproduce all
.\.venv\Scripts\python -m urban_import verify-current all
.\.venv\Scripts\python -m urban_import restore-preview reutov
```

`status` и `reproduce` работают без сети. `reproduce` заново проверяет каждый
исходный объект по сохранённой границе и сравнивает все тела запросов с
контрольной суммой завершённого результата. `verify-current` получает новую
полную GET-выгрузку. `restore-preview` создаёт отдельный новый план без записи
в API. У каждой команды русская справка `--help`.

## Структура

```text
config/territories.json   территории, исходники и правила
data/raw/                 14 неизменяемых GeoJSON
reference/                исторические границы и окружение
results/index.json        текущие подтверждённые результаты
results/<territory>/      компактный результат и итоговая проверка
urban_import/             подготовка, API, выполнение и проверка
tests/                    критические локальные тесты
docs/                     правила, оператор и сформированный статус
scripts/                  ast-index и entry point сборки worker
artifacts/                временные операции и новые отчёты, не отслеживается git
distribution/, build/     новые переносимые сборки, не отслеживается git
```

Данные импортированных объектов выводятся из исходников. В каждом
`result.json.gz` хранится карта OSM/server ID, содержимое сохраняемых объектов,
доступные связи и доказательства завершения. Формат описан в
[правилах](docs/rules.md). Старые backup, рабочие папки узлов и переносимые
комплекты для этого результата не нужны.

[Оператору](docs/operator.md) - установка, проверка, новые планы и resume.
[Статус](docs/status.md) формируется из каталога результатов.
[Агенту](AGENTS.md) - карта кода и ограничения безопасности.

## Разработка

```powershell
.\.venv\Scripts\python -m pytest -q
powershell -ExecutionPolicy Bypass -File scripts/update-ast-index.ps1
.\.venv\Scripts\python -m urban_import status-doc --check
```

Обычный и распределённый загрузчики сохранены для новых разрешённых импортов.
Их рабочие планы нельзя подменять компактным результатом. Во время
реорганизации удалённая запись запрещена; проверка использует только GET.
