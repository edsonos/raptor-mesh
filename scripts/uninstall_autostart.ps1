param(
    [string]$TaskName = "Raptor-Mesh"
)

$ErrorActionPreference = "Stop"

Write-Host "[1/3] Removing Windows Auto-PAC proxy setting..." -ForegroundColor Cyan
$regPath = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Internet Settings"
Remove-ItemProperty -Path $regPath -Name AutoConfigURL -ErrorAction SilentlyContinue
Set-ItemProperty -Path $regPath -Name ProxyEnable -Value 0 -ErrorAction SilentlyContinue

Add-Type -Namespace WinInetCleanup -Name Native -MemberDefinition @"
[System.Runtime.InteropServices.DllImport("wininet.dll", SetLastError=true)]
public static extern bool InternetSetOption(System.IntPtr hInternet, int dwOption, System.IntPtr lpBuffer, int dwBufferLength);
"@
[WinInetCleanup.Native]::InternetSetOption([IntPtr]::Zero, 39, [IntPtr]::Zero, 0) | Out-Null
[WinInetCleanup.Native]::InternetSetOption([IntPtr]::Zero, 37, [IntPtr]::Zero, 0) | Out-Null

Write-Host "[2/3] Stopping and unregistering Scheduled Task '$TaskName'..." -ForegroundColor Cyan
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "      Scheduled Task '$TaskName' removed." -ForegroundColor Green
} else {
    Write-Host "      Scheduled Task '$TaskName' was not registered." -ForegroundColor Yellow
}

Write-Host "[3/3] Stopping any running raptor-client-bg process..." -ForegroundColor Cyan
Get-Process -Name "raptor-client-bg" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue

Write-Host "`n[Raptor-Mesh] Autostart and system PAC removed cleanly. Internet settings restored to default." -ForegroundColor Green
