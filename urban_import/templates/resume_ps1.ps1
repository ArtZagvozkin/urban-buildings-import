param(
  [switch]$DryRun,
  [switch]$Once,
  [string]$BaseUrl = "https://urban-api.testing.idulab.ru"
)
& (Join-Path $PSScriptRoot "run.ps1") -BaseUrl $BaseUrl -DryRun:$DryRun -Once:$Once
exit $LASTEXITCODE
