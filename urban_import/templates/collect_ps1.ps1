param([Parameter(Mandatory=$true)][string]$ResultsDirectory)
$ErrorActionPreference = "Stop"
$repo = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$python = Join-Path $repo ".venv\Scripts\python.exe"
& $python -m urban_import collect-results --distribution $PSScriptRoot --results $ResultsDirectory
exit $LASTEXITCODE
