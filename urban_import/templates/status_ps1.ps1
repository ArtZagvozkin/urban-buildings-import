param(
  [switch]$Watch,
  [switch]$Json,
  [int]$IntervalSeconds = 15
)
$exe = Join-Path $PSScriptRoot "runtime\urban-import.exe"
$ErrorActionPreference = "Stop"
if (-not (Test-Path -LiteralPath $exe)) { Write-Error "Не найден runtime"; exit 11 }
if ($IntervalSeconds -lt 2) { Write-Error "Интервал должен быть не меньше 2 секунд"; exit 12 }
do {
  $raw = & $exe worker-status --bundle $PSScriptRoot
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  $document = $raw -join [Environment]::NewLine
  if ($Json) { Write-Output $document; exit 0 }
  $status = $document | ConvertFrom-Json
  $state = $status.state
  $running = @(Get-Process -Name "urban-import" -ErrorAction SilentlyContinue |
    Where-Object { $_.Path -ieq $exe }).Count -gt 0
  $processText = if ($running) { "работает" } else { "не запущен" }
  Write-Output ("{0} | worker {1} | этап {2} | процесс {3}" -f
    (Get-Date -Format "HH:mm:ss"), $status.worker_id, $state.stage, $processText)
  Write-Output ("Удалено: объекты {0}/{1}, геометрии {2}/{3}; создано: объекты {4}/{5}, buildings {6}/{7}" -f
    @($state.deleted_objects).Count, $status.counts.delete_objects,
    @($state.deleted_geometries).Count, $status.counts.delete_geometries,
    @($state.created.PSObject.Properties).Count, $status.counts.create_objects,
    @($state.buildings).Count, $status.counts.create_buildings)
  if ($state.pending) { Write-Output ("Незавершённая операция: {0} {1}" -f $state.pending.action, $state.pending.key) }
  if ($state.stage -eq "waiting_geometries_deleted" -and $state.geometry_barrier_total) {
    Write-Output ("Проверено старых геометрий: {0}/{1}" -f $state.geometry_barrier_index, $state.geometry_barrier_total)
  }
  if ($state.last_retry) { Write-Output ("Временный сбой; автоматическое ожидание {0} с: {1}" -f $state.last_retry.seconds, $state.last_retry.error) }
  if (@($state.errors).Count -gt 0) { Write-Output ("Ошибок прежних запусков: {0}" -f @($state.errors).Count) }
  if (-not (Test-Path -LiteralPath (Join-Path $PSScriptRoot "authorization.json"))) {
    Write-Output "Запись заблокирована: authorization.json отсутствует."
  }
  if ($Watch -and -not $running) {
    Write-Output "Наблюдение завершено: процесс worker не запущен."
    exit 0
  }
  if ($Watch -and $state.complete) { exit 0 }
  if ($Watch) { Start-Sleep -Seconds $IntervalSeconds }
} while ($Watch)
