param(
    [string]$TaskName = "Raptor-Mesh",
    [string]$ListenAddr = "127.0.0.1:17908",
    [string]$Upstream = "",
    [switch]$SkipSystemProxy
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$bgExe = Join-Path $root "bin\raptor-client-bg.exe"
$cfgFile = Join-Path $root "config\targets.json"
$logDir = Join-Path $root "logs"
$logFile = Join-Path $logDir "raptor-client.log"

if (-not (Test-Path $bgExe)) {
    Write-Host "[Raptor-Mesh] Building windowless background binary first..." -ForegroundColor Yellow
    & "$PSScriptRoot\build.ps1"
}

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

# Stop any existing task/process cleanly before re-registering
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}
Get-Process -Name "raptor-client-bg" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 300

# Build CLI args
$argParts = @(
    "-config", "`"$cfgFile`"",
    "-listen", $ListenAddr,
    "-logfile", "`"$logFile`""
)
if ($Upstream -ne "") {
    $argParts += @("-upstream", $Upstream)
}
$argString = $argParts -join " "

$action = New-ScheduledTaskAction -Execute $bgExe -Argument $argString -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -StartWhenAvailable

Write-Host "[1/3] Registering Windows Scheduled Task '$TaskName' (At Logon, silent background, auto-restart)..." -ForegroundColor Cyan
Register-ScheduledTask `
    -TaskName $TaskName `
    -Description "Raptor-Mesh (RMESH/1) ultra-low-latency network accelerator listening on $ListenAddr" `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Settings $settings `
    -Force | Out-Null

Write-Host "[2/3] Starting task '$TaskName' and waiting for health check..." -ForegroundColor Cyan
Start-ScheduledTask -TaskName $TaskName

$healthURL = "http://$ListenAddr/raptor/health"
$pacURL = "http://$ListenAddr/raptor/proxy.pac"
$ready = $false
for ($i = 0; $i -lt 40; $i++) {
    try {
        $resp = Invoke-WebRequest -Uri $healthURL -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        if ($resp.StatusCode -eq 200) {
            $ready = $true
            break
        }
    } catch {
        Start-Sleep -Milliseconds 350
    }
}

if (-not $ready) {
    Write-Warning "Raptor-Mesh did not respond on $healthURL within timeout. Check log: $logFile"
    exit 1
}
Write-Host "      Healthy! $healthURL -> 200 OK" -ForegroundColor Green

if (-not $SkipSystemProxy) {
    Write-Host "[3/3] Enabling Fail-Safe Windows Auto-PAC ($pacURL -> PROXY $ListenAddr; DIRECT)..." -ForegroundColor Cyan
    $regPath = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Internet Settings"
    # Disable manual rigid proxy so fail-safe PAC governs routing (auto-falls back to DIRECT if stopped)
    Set-ItemProperty -Path $regPath -Name ProxyEnable -Value 0
    Set-ItemProperty -Path $regPath -Name AutoConfigURL -Value $pacURL

    # Notify WinINet so Chrome / Edge / Brave / Windows apply PAC immediately without restart
    Add-Type -Namespace WinInetRefresh -Name Native -MemberDefinition @"
[System.Runtime.InteropServices.DllImport("wininet.dll", SetLastError=true)]
public static extern bool InternetSetOption(System.IntPtr hInternet, int dwOption, System.IntPtr lpBuffer, int dwBufferLength);
"@
    [WinInetRefresh.Native]::InternetSetOption([IntPtr]::Zero, 39, [IntPtr]::Zero, 0) | Out-Null # INTERNET_OPTION_SETTINGS_CHANGED
    [WinInetRefresh.Native]::InternetSetOption([IntPtr]::Zero, 37, [IntPtr]::Zero, 0) | Out-Null # INTERNET_OPTION_REFRESH

    Write-Host "      Windows Auto-PAC active! Chrome/Edge/Brave will route via Raptor-Mesh automatically." -ForegroundColor Green
} else {
    Write-Host "[3/3] Skipped Windows system proxy configuration (-SkipSystemProxy)." -ForegroundColor Yellow
}

Write-Host "`n[Raptor-Mesh] Automation ready!" -ForegroundColor Green
Write-Host "  Scheduled Task : $TaskName (AtLogOn, Auto-Restart, Windowless)"
Write-Host "  Proxy Endpoint : http://$ListenAddr"
Write-Host "  PAC Fail-Safe  : $pacURL"
Write-Host "  Live Telemetry : http://$ListenAddr/raptor/stats"
Write-Host "  Log File       : $logFile"
