param(
  [switch]$DryRun,
  [switch]$Once,
  [string]$BaseUrl = "https://urban-api.testing.idulab.ru"
)
$ErrorActionPreference = "Stop"
if (-not [Environment]::Is64BitOperatingSystem) {
  Write-Error "Требуется 64-битная Windows"
  exit 10
}
$exe = Join-Path $PSScriptRoot "runtime\urban-import.exe"
if (-not (Test-Path -LiteralPath $exe)) { Write-Error "Не найден runtime"; exit 11 }
foreach ($name in @("REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE")) {
  $path = [Environment]::GetEnvironmentVariable($name, "Process")
  if ($path -and -not (Test-Path -LiteralPath $path)) {
    [Environment]::SetEnvironmentVariable($name, $null, "Process")
    Write-Warning ("Игнорируется отсутствующий CA bundle из {0}: {1}" -f $name, $path)
  }
}
if (-not $DryRun -and -not (Test-Path -LiteralPath (Join-Path $PSScriptRoot "authorization.json"))) {
  [Console]::Error.WriteLine("Обработка НЕ запущена: нет authorization.json. Требуется подтверждение полной замены территории 5223 и разрешение записи по master plan.")
  exit 2
}
$arguments = @("worker-run", "--bundle", $PSScriptRoot, "--base-url", $BaseUrl)
if ($DryRun) { $arguments += "--dry-run" }
if ($Once) { $arguments += "--once" }
& $exe @arguments
exit $LASTEXITCODE
