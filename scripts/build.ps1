# Build optimized native Windows binaries for Raptor-Mesh
$ErrorActionPreference = "Stop"

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    if (Test-Path "C:\Program Files\Go\bin\go.exe") {
        $env:PATH = "C:\Program Files\Go\bin;" + $env:PATH
    } else {
        Write-Error "Go toolchain not found. Install via: winget install --id GoLang.Go --exact"
    }
}

$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    Write-Host "[Raptor-Mesh] Running unit tests..." -ForegroundColor Cyan
    go test ./...
    if ($LASTEXITCODE -ne 0) { throw "go test failed with exit code $LASTEXITCODE" }

    New-Item -ItemType Directory -Force -Path "bin" | Out-Null

    # Stop background binary if currently running so Windows releases the file lock during rebuild
    $runningBg = Get-Process -Name "raptor-client-bg" -ErrorAction SilentlyContinue
    if ($runningBg) {
        Write-Host "[Raptor-Mesh] Stopping running raptor-client-bg.exe for rebuild..." -ForegroundColor Yellow
        $runningBg | Stop-Process -Force
        Start-Sleep -Milliseconds 300
    }

    Write-Host "[Raptor-Mesh] Building bin\raptor-client.exe (console)..." -ForegroundColor Cyan
    go build -trimpath -ldflags="-s -w" -o "bin\raptor-client.exe" .\cmd\raptor-client
    if ($LASTEXITCODE -ne 0) { throw "build raptor-client failed" }

    Write-Host "[Raptor-Mesh] Building bin\raptor-client-bg.exe (windowless background)..." -ForegroundColor Cyan
    go build -trimpath -ldflags="-s -w -H=windowsgui" -o "bin\raptor-client-bg.exe" .\cmd\raptor-client
    if ($LASTEXITCODE -ne 0) { throw "build raptor-client-bg failed" }

    Write-Host "[Raptor-Mesh] Building bin\raptor-server.exe..." -ForegroundColor Cyan
    go build -trimpath -ldflags="-s -w" -o "bin\raptor-server.exe" .\cmd\raptor-server
    if ($LASTEXITCODE -ne 0) { throw "build raptor-server failed" }

    Write-Host "[Raptor-Mesh] Building bin\raptor-bench.exe..." -ForegroundColor Cyan
    go build -trimpath -ldflags="-s -w" -o "bin\raptor-bench.exe" .\cmd\raptor-bench
    if ($LASTEXITCODE -ne 0) { throw "build raptor-bench failed" }

    Write-Host "`n[Raptor-Mesh] Build complete:" -ForegroundColor Green
    Get-ChildItem "bin" | Format-Table Name, Length, LastWriteTime -AutoSize

    if ($runningBg) {
        $task = Get-ScheduledTask -TaskName "Raptor-Mesh" -ErrorAction SilentlyContinue
        if ($task) {
            Write-Host "[Raptor-Mesh] Restarting scheduled task 'Raptor-Mesh'..." -ForegroundColor Cyan
            Start-ScheduledTask -TaskName "Raptor-Mesh"
        }
    }
} finally {
    Pop-Location
}
