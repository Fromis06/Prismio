param(
    [switch]$KeepContainers
)

$ErrorActionPreference = "Stop"
$composeFile = Join-Path $PSScriptRoot "..\compose.integration.yml"
$sourceURLWasSet = Test-Path Env:TEST_SOURCE_DB_URL
$targetURLWasSet = Test-Path Env:TEST_TARGET_DB_URL
$goCacheWasSet = Test-Path Env:GOCACHE
$previousSourceURL = $env:TEST_SOURCE_DB_URL
$previousTargetURL = $env:TEST_TARGET_DB_URL
$previousGoCache = $env:GOCACHE
$containersStarted = $false

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker is not running. Start Docker Desktop, then run this script again."
}

try {
    docker compose -f $composeFile up -d --wait
    if ($LASTEXITCODE -ne 0) {
        throw "Could not start the integration-test databases."
    }
    $containersStarted = $true

    if (-not $sourceURLWasSet) {
        $env:TEST_SOURCE_DB_URL = "postgres://postgres:password@localhost:15432/sourcedb?sslmode=disable"
    }
    if (-not $targetURLWasSet) {
        $env:TEST_TARGET_DB_URL = "postgres://postgres:password@localhost:15433/targetdb?sslmode=disable"
    }
    if (-not $goCacheWasSet) {
        $env:GOCACHE = Join-Path $PSScriptRoot "..\.tmp-go-cache"
    }

    go test -tags=integration ./tests/integration/... -v -count=1 -timeout=60s
    if ($LASTEXITCODE -ne 0) {
        Write-Host "`nIntegration test failed. PostgreSQL logs:" -ForegroundColor Red
        docker compose -f $composeFile logs
        throw "Integration test failed."
    }
}
finally {
    if ($containersStarted -and -not $KeepContainers) {
        docker compose -f $composeFile down -v
    }

    if ($sourceURLWasSet) {
        $env:TEST_SOURCE_DB_URL = $previousSourceURL
    }
    else {
        Remove-Item Env:TEST_SOURCE_DB_URL -ErrorAction SilentlyContinue
    }

    if ($targetURLWasSet) {
        $env:TEST_TARGET_DB_URL = $previousTargetURL
    }
    else {
        Remove-Item Env:TEST_TARGET_DB_URL -ErrorAction SilentlyContinue
    }

    if ($goCacheWasSet) {
        $env:GOCACHE = $previousGoCache
    }
    else {
        Remove-Item Env:GOCACHE -ErrorAction SilentlyContinue
    }
}
