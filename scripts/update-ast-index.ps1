param(
    [switch]$Rebuild
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot

if (-not (Get-Command ast-index -ErrorAction SilentlyContinue)) {
    throw "ast-index is not installed or is unavailable in PATH."
}

Push-Location $projectRoot
try {
    if ($Rebuild) {
        & ast-index rebuild
    }
    else {
        # ast-index может вернуть код 0 для отсутствующей БД. Чистый checkout
        # должен создать индекс, а не считать такой ответ успешным update.
        $previousStats = (& ast-index stats 2>&1 | Out-String)
        if ($LASTEXITCODE -eq 0 -and $previousStats -notmatch 'Index not found') {
            & ast-index update
        }
        else {
            & ast-index rebuild
        }
    }

    if ($LASTEXITCODE -ne 0) {
        throw "ast-index failed with exit code $LASTEXITCODE."
    }

    & ast-index stats
    if ($LASTEXITCODE -ne 0) {
        throw "ast-index stats failed with exit code $LASTEXITCODE."
    }
}
finally {
    Pop-Location
}
