param(
    [string]$TaskName = "Raptor-Mesh",
    [string]$ListenAddr = "127.0.0.1:17908"
)

Write-Host "================ RAPTOR-MESH STATUS ================" -ForegroundColor Cyan

$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($task) {
    $info = Get-ScheduledTaskInfo -TaskName $TaskName
    Write-Host "Scheduled Task : $($task.TaskName) | State=$($task.State) | LastRun=$($info.LastRunTime)"
} else {
    Write-Host "Scheduled Task : NOT INSTALLED" -ForegroundColor Yellow
}

$proc = Get-Process -Name "raptor-client-bg","raptor-client" -ErrorAction SilentlyContinue | Select-Object -First 1
if ($proc) {
    $ramMB = [math]::Round($proc.WorkingSet64 / 1MB, 1)
    Write-Host "Process        : $($proc.ProcessName) (PID=$($proc.Id), RAM=${ramMB} MB)" -ForegroundColor Green
} else {
    Write-Host "Process        : NOT RUNNING" -ForegroundColor Red
}

$reg = Get-ItemProperty "HKCU:\Software\Microsoft\Windows\CurrentVersion\Internet Settings"
Write-Host "Windows Proxy  : ProxyEnable=$($reg.ProxyEnable) | AutoConfigURL=$($reg.AutoConfigURL)"

try {
    $stats = Invoke-RestMethod -Uri "http://$ListenAddr/raptor/stats" -TimeoutSec 2
    Write-Host "Service Health : ONLINE ($($stats.mode), uptime=$([math]::Round($stats.uptime_sec, 1))s)" -ForegroundColor Green
    Write-Host "Traffic        : total=$($stats.total_requests) | connect=$($stats.connect_tunnels) | fastpath=$($stats.fast_path_requests) | active_conns=$($stats.active_conns)"
    Write-Host "DNS SWR Cache  : hits=$($stats.dns.cache_hits) / misses=$($stats.dns.cache_misses) | geo_cdn=$($stats.dns.geo_cdn_lookups) (cached=$($stats.dns.cached_hosts), winner=$($stats.dns.last_winner_server))"
    Write-Host "Kernel Tuning  : TCP_FASTOPEN=$($stats.sockets.tfo_enabled) | LoopbackFastPath=$($stats.sockets.loopback_fast_path_enabled)"
    Write-Host "Speculative TCP: hits=$($stats.tcp_pool.hits) / misses=$($stats.tcp_pool.misses) | dead_reaped=$($stats.tcp_pool.dead_reaped)"
    Write-Host "H3/H2 Edge Pool: stripes=$($stats.hyper_transport.stripes) | H3=$($stats.hyper_transport.h3_requests) | H2=$($stats.hyper_transport.h2_requests)"
} catch {
    Write-Host "Service Health : UNREACHABLE on http://$ListenAddr/raptor/stats" -ForegroundColor Red
}

Write-Host "====================================================" -ForegroundColor Cyan
