Work from the root of your checkout. Read `README.md`, `docs/status.md`, and
the relevant sections of `docs/rules.md`. Run `git status --short`; do not
overwrite other people's changes. Update the index:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/update-ast-index.ps1
```

For Python, use `ast-index` first; for text and JSON, use `rg`.

## Project map

- `results/index.json`: the sole source of the current verified status.
- `results/<territory>/result.json.gz`: ID mappings, retained objects, and evidence.
- `reference/boundaries/`: historical boundaries; `reference/environment.json`
  and `requirements.lock`: the verified reproduction environment.
- `config/territories.json`: territory-specific configuration.
- `urban_import/prepare.py`: GeoJSON validation and request preparation;
  `urban_import/results.py`: offline reproduction, GET verification, and new
  restoration plans.
- `urban_import/restoration.py`: execution of a new restoration plan under
  separate authorization.
- `urban_import/api.py`: HTTP, pagination, and descendant territories;
  `urban_import/storage.py`: atomic artifact writes.
- `urban_import/workflow.py`: the standard importer and shared `OperationState`.
- `urban_import/distributed_plan.py`: GET audit and master plan;
  `urban_import/worker_package.py` and `urban_import/templates/`: the portable
  worker package; `urban_import/distributed.py`: workers, barriers, and result
  collection.
- `artifacts/`, `distribution/`, `build/`: new working materials excluded from Git.

## Invariants

All four imports are complete. Run CLI commands with
`.\.venv\Scripts\python -m urban_import`. `reproduce all` works without the API
and verifies every request body. `verify-current all` fetches fresh server data;
by default, it relies on preserved historical evidence for the absence of old
geometries. Add `--check-old-geometries` to check every replaced geometry ID
with fresh GET requests. `docs/status.md` is generated from `results/index.json`
and checked by a test.

Do not modify `data/raw/`. Never substitute an OSM ID for a server ID.
Each checkout must have its own `.venv`; sharing an environment through a
directory junction is prohibited.

During reorganization, only GET requests are allowed. POST and DELETE require
a separate current task authorizing writes and a new immutable plan. Previous
authorization does not apply to a new plan. Check relationships before deletion;
delete physical objects before their geometries. Reconcile an uncertain POST
before retrying it. Retry only after two server checks confirm absence, at least
60 seconds apart. Duplicates or ambiguity must stop the stage.

Restoration targets the current completed result, not the database state before
the import. The API assigns new IDs to recreated objects. For the 149 points in
Reutov, former third-party service relationships cannot be verified because no
pre-incident snapshot is available. Their current data and old/new physical
object IDs are preserved. Physical object `694562`, geometry `694559`, and
services `735682`/`736856` in Odintsovsky are protected.

Do not commit environments, runtime bundles, working plans, backups, or logs.
Keep one compact final result per territory and unique supporting evidence in
Git. A completed result is evidence, not an executable plan or authorization to
write to the API. Do not delete materials until the replacement has been proven
sufficient and a file-by-file inventory of the materials to remove is available.

Before handing off:

```powershell
.\.venv\Scripts\python -m pytest -q
git diff --check
powershell -ExecutionPolicy Bypass -File scripts/update-ast-index.ps1
.\.venv\Scripts\python -m urban_import status-doc --check
```
