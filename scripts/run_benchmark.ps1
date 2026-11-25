param(
    [int]$Concurrency = 6,
    [int]$Rounds = 3,
    [string]$Baseline = "127.0.0.1:7897",
    [string]$Raptor = "127.0.0.1:17908",
    [switch]$MeshRelay,
    [switch]$SkipConnect
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    if (-not (Test-Path "bin\raptor-bench.exe")) {
        Write-Host "[Raptor-Mesh] Binaries not found, building first..." -ForegroundColor Yellow
        & "$PSScriptRoot\build.ps1"
    }

    $argsList = @(
        "-baseline", $Baseline,
        "-raptor", $Raptor,
        "-concurrency", $Concurrency,
        "-rounds", $Rounds
    )
    if ($MeshRelay) { $argsList += "-mesh-relay" }
    if ($SkipConnect) { $argsList += "-include-connect=false" }

    Write-Host "[Raptor-Mesh] Executing E2E Benchmark: bin\raptor-bench.exe $($argsList -join ' ')" -ForegroundColor Cyan
    & ".\bin\raptor-bench.exe" @argsList
} finally {
    Pop-Location
}
