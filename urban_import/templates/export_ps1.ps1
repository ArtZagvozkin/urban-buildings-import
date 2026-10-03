$exe = Join-Path $PSScriptRoot "runtime\urban-import.exe"
$ErrorActionPreference = "Stop"
if (-not (Test-Path -LiteralPath $exe)) { Write-Error "Не найден runtime"; exit 11 }
& $exe worker-export --bundle $PSScriptRoot
exit $LASTEXITCODE
