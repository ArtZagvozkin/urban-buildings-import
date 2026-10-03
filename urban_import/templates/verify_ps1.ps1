param([string]$BaseUrl = "https://urban-api.testing.idulab.ru")
$ErrorActionPreference = "Stop"
$repo = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$python = Join-Path $repo ".venv\Scripts\python.exe"
& $python -m urban_import verify-final --distribution $PSScriptRoot --base-url $BaseUrl
exit $LASTEXITCODE
