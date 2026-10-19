# ModelRotator control script (PowerShell 5.1+ compatible; ASCII-only for WinPS)
# Usage: powershell -ExecutionPolicy Bypass -File rotator-ctl.ps1 [start|stop|status|restart|install|uninstall|testautostart]
$Dir      = $PSScriptRoot
$Health   = 'http://127.0.0.1:3166/health'
$TaskName = 'ModelRotator'

# Resolve pythonw:
#   1) $env:PYTHONW if set
#   2) the executable already registered in the scheduled task (preserves a prior install)
#   3) pythonw.exe found on PATH
#   4) plain 'pythonw.exe' (relies on PATH lookup)
function Resolve-Python {
    if ($env:PYTHONW) { return $env:PYTHONW }
    $t = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($t) {
        $a = $t.Actions | Select-Object -First 1
        if ($a -and $a.Execute -and $a.Execute -match 'pythonw?\.exe') { return $a.Execute }
    }
    $cmd = Get-Command pythonw.exe -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    return 'pythonw.exe'
}
$Py = Resolve-Python

# Some environments define both HTTP_PROXY and http_proxy; Start-Process then dies with
# "Item has already been added. Key in dictionary: 'HTTPS_PROXY'". Collapse the duplicates.
foreach ($dup in @('HTTP_PROXY','http_proxy','HTTPS_PROXY','https_proxy')) {
    if ([Environment]::GetEnvironmentVariable($dup, 'Process')) {
        [Environment]::SetEnvironmentVariable($dup.ToLower(), $null, 'Process')
    }
}

function Get-Health {
    try { (Invoke-WebRequest -Uri $Health -TimeoutSec 2 -UseBasicParsing).Content }
    catch { $null }
}

function Do-Start {
    $s = Get-Health
    if ($s) { Write-Host "Already running: $s"; return }   # idempotent: keepalive relies on this

    # Prefer the scheduled task: Task Scheduler owns the process, so it survives the
    # launching shell/session. The task runs pythonw directly (no Start-Process), which
    # also sidesteps the duplicate-HTTPS_PROXY env bug that kills Start-Process.
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        Start-ScheduledTask -TaskName $TaskName
        Start-Sleep -Seconds 5
        $s = Get-Health
        if ($s) { Write-Host "Started via scheduled task: $s"; return }
        $rc = (Get-ScheduledTaskInfo -TaskName $TaskName).LastTaskResult
        Write-Host "Scheduled task did not bring it up (LastTaskResult=$rc), falling back"
    }

    Start-Process -FilePath $Py -ArgumentList 'rotator.py' -WorkingDirectory $Dir -WindowStyle Hidden
    Start-Sleep -Seconds 3
    $s = Get-Health
    if ($s) { Write-Host "Started OK: $s" }
    else { Write-Host 'Start failed, check rotator.log'; exit 1 }
}

function Do-Stop {
    # Must actually free the port: a silently-failed Stop-Process used to make
    # `restart` look successful while the stale (old-config) instance kept serving.
    $killed = @()
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        $conns = Get-NetTCPConnection -LocalPort 3166 -State Listen -ErrorAction SilentlyContinue
        if (-not $conns) { break }
        $conns | Select-Object -ExpandProperty OwningProcess -Unique | ForEach-Object {
            $procId = $_
            try {
                Stop-Process -Id $procId -Force -ErrorAction Stop
                $killed += $procId
            } catch {
                Write-Host "Stop-Process failed for PID ${procId}: $($_.Exception.Message)"
                & taskkill /F /PID $procId 2>&1 | Out-Host   # escalate
            }
        }
        Start-Sleep -Seconds 1
    }
    $left = Get-NetTCPConnection -LocalPort 3166 -State Listen -ErrorAction SilentlyContinue
    if ($left) {
        Write-Host "ERROR: port 3166 still held by PID(s) $($left.OwningProcess -join ',') - refusing to continue"
        exit 1
    }
    if ($killed.Count -gt 0) { Write-Host "Stopped (killed PID $($killed -join ','))" }
    else { Write-Host 'Not running' }
}

function Do-Install {
    # The task runs pythonw directly with the proxy env cleared. pythonw is required
    # (Task Scheduler cannot launch a console app hidden); the duplicate
    # HTTPS_PROXY/https_proxy vars must go or the child inherits a broken env block.
    $action = New-ScheduledTaskAction `
        -Execute $Py `
        -Argument 'rotator.py' `
        -WorkingDirectory $Dir
    $trigLogon = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
    # RepetitionDuration must stay within the Task Scheduler limit (max ~ P99999999D),
    # so use a finite 10-year span instead of [TimeSpan]::MaxValue.
    $trigKeep  = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
                    -RepetitionInterval (New-TimeSpan -Minutes 5) `
                    -RepetitionDuration (New-TimeSpan -Days 3650)
    $set = New-ScheduledTaskSettingsSet `
              -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
              -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) `
              -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew
    $prin = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger @($trigLogon, $trigKeep) `
        -Settings $set -Principal $prin -Force -ErrorAction Stop `
        -Description 'ModelRotator: OpenAI-compatible model auto-rotate proxy (time-window + 429 failover). Keepalive every 5 min.' | Out-Null
    if (-not (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue)) {
        Write-Host "Register reported success but task '$TaskName' is missing"; exit 1
    }
    Write-Host "Scheduled task '$TaskName' installed: at-logon + 5min keepalive"
}

$action = if ($args.Count -ge 1) { $args[0].ToLower() } else { 'status' }

if ($action -eq 'status') {
    $s = Get-Health
    if ($s) { Write-Host "RUNNING $s" } else { Write-Host 'STOPPED' }
} elseif ($action -eq 'start') {
    Do-Start
} elseif ($action -eq 'stop') {
    Do-Stop
} elseif ($action -eq 'restart') {
    Do-Stop; Start-Sleep -Seconds 1; Do-Start
} elseif ($action -eq 'install') {
    Do-Install
} elseif ($action -eq 'uninstall') {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host 'Task removed'
} elseif ($action -eq 'testautostart') {
    Do-Stop
    Start-Sleep -Seconds 1
    Start-ScheduledTask -TaskName $TaskName
    Start-Sleep -Seconds 5
    $s = Get-Health
    if ($s) { Write-Host "AUTOSTART OK: $s" } else { Write-Host 'AUTOSTART FAILED'; exit 1 }
} else {
    Write-Host 'Usage: rotator-ctl.ps1 [start|stop|status|restart|install|uninstall|testautostart]'
    exit 1
}
