$ErrorActionPreference = "Stop"
$exe = Join-Path $PSScriptRoot "runtime\urban-import.exe"
if (-not (Test-Path -LiteralPath $exe)) { Write-Error "Не найден runtime"; exit 11 }
& $exe worker-stop --bundle $PSScriptRoot
exit $LASTEXITCODE
