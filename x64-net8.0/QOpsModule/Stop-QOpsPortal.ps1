<#
.SYNOPSIS
    Stops the QOps Portal, if one is running, before the installer removes files.

.DESCRIPTION
    QOPS-495. The Windows packages removed files without stopping the Portal first, so a running
    Portal could hold a handle under the module root while QOPS-51's recursive delete
    (util:RemoveFolderEx on INSTALLFOLDER) ran, and left a half-removed tree behind. macOS has done
    this since QOPS-420, in CICD/installer-macos/uninstall-qops.sh; this is the same step for Windows,
    with the two load-bearing safety properties of that script carried over deliberately:

      * THE PID COMES FROM THE PID FILE, NEVER FROM A NAME MATCH.
        %USERPROFILE%\qopsconfig\portal.pid is what QOps-StartPortal writes and QOps-StopPortal reads,
        so this uses the same identity the product uses. Searching a process list by name is how an
        uninstaller kills the wrong thing - and it can match ITSELF, because this script's own command
        line contains the very name being searched for.

      * A PID IS NOT AN IDENTITY. A pid file goes stale (the machine rebooted, the Portal was killed),
        and by then that number may belong to something else entirely. So the process behind the pid
        has to AGREE it is the Portal before anything is signalled. If it does not, that process is
        left alone - the one outcome that must never happen is "kill it anyway".

      * A STALE PID FILE IS NOT A REASON TO FAIL (QOPS-705). Until QOPS-705 a pid that belonged to
        another process made this script exit 1, and the installer action runs with Return="check",
        so the usual state after a reboot or a killed Portal aborted every uninstall - and every
        UPGRADE, because MajorUpgrade removes the old version by running the old package's own
        uninstall. A process that is not the Portal holds no handle under the module root, so there
        is nothing to protect: the stale pid file is deleted and the script exits 0. The other
        process is still never signalled.

    It ships at the root of the installed module, exactly like Start-QOpsPortal.ps1 (QOPS-329) and for
    the same reasons: both installers pick it up through the heat harvest they already run, both
    editions get it, and QOps-Update refreshes it with the rest of the module.

.PARAMETER PidFile
    Override the pid file location. Exists for the tests; the installer never passes it.

.PARAMETER TimeoutSeconds
    How long to wait for a graceful exit before forcing. Matches the macOS script's 5 seconds.

.OUTPUTS
    Exit code 0 - there is no Portal running, or there was and it is now stopped. A stale pid file
                  (no pid in it, a pid that is not running, or a pid that now belongs to a process
                  that is not the Portal) is deleted and also gives 0.
    Exit code 1 - a real Portal.Api process is still running after it was asked to stop, and files
                  must NOT be removed. This is the ONLY non-zero outcome (QOPS-705).

.NOTES
    The exit code is the whole contract with the installer: the custom action runs with Return="check",
    so a 1 aborts before RemoveFiles rather than producing the half-removed tree this exists to
    prevent. An aborted uninstall leaves the product installed, which is a state the user can retry
    from; a partial delete is not. Because an abort here also aborts every upgrade of this version,
    the 1 is reserved for the one case that justifies it: a Portal that is really running and will
    not stop.
#>
[CmdletBinding()]
param(
    [string] $PidFile,
    [int]    $TimeoutSeconds = 5
)

$ErrorActionPreference = 'Stop'

# The process name the Portal runs under. Deliberately a constant with a name rather than a literal
# buried in a comparison: it is the identity check, and identity checks should be findable.
$script:PortalProcessName = 'Portal.Api'

function Get-QOpsPortalPidFile {
    <#
        %USERPROFILE%\qopsconfig\portal.pid — the same path QopsStartPortal.GetPortalPidPath() builds.
        Resolved through $HOME so this is testable off Windows; on Windows PowerShell $HOME is the
        user profile.
    #>
    if ($PidFile) { return $PidFile }
    return (Join-Path (Join-Path $HOME 'qopsconfig') 'portal.pid')
}

function Resolve-PortalStopAction {
    <#
    .SYNOPSIS
        Decides WHAT to do about the pid file, without doing any of it.

    .DESCRIPTION
        Split from the acting half on purpose. This is the part with the safety properties in it, and
        it is the part that can be tested without an installer and without Windows: every branch below
        is reachable with a real pid file and a real process on any OS. The acting half is three lines
        of Stop-Process.

        Returns an object with:
          Action  None | RemoveStalePidFile | Stop
                  (QOPS-705 removed 'Refuse': a pid owned by another process is a stale pid file.)
          Pid     the pid, when there is one
          Name    the process name found behind that pid, when it is running
          Reason  one line, for the log the installer captures
    #>
    param([Parameter(Mandatory)][string] $Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return [pscustomobject]@{ Action = 'None'; Pid = $null; Name = $null
                                  Reason = "no pid file at $Path - nothing to stop" }
    }

    $raw = (Get-Content -LiteralPath $Path -Raw -ErrorAction SilentlyContinue)
    $text = if ($null -eq $raw) { '' } else { $raw.Trim() }

    if ($text -notmatch '^\d+$') {
        return [pscustomobject]@{ Action = 'RemoveStalePidFile'; Pid = $null; Name = $null
                                  Reason = "pid file does not contain a pid - removing it" }
    }

    $processId = [int] $text
    $proc = Get-Process -Id $processId -ErrorAction SilentlyContinue
    if (-not $proc) {
        return [pscustomobject]@{ Action = 'RemoveStalePidFile'; Pid = $processId; Name = $null
                                  Reason = "pid $processId is not running (stale pid file) - removing it" }
    }

    if ($proc.ProcessName -ne $script:PortalProcessName) {
        # This pid belongs to somebody else's process now. Signalling it would be the uninstaller
        # killing an unrelated program, so it is never signalled. But it is not a reason to fail
        # either (QOPS-705): that process holds no handle under the module root, so the Portal this
        # script exists to stop is simply not running. Only the pid FILE is removed.
        return [pscustomobject]@{ Action = 'RemoveStalePidFile'; Pid = $processId; Name = $proc.ProcessName
                                  Reason = ("pid $processId is '" + $proc.ProcessName + "', not $script:PortalProcessName - " +
                                            "the pid file is stale and that process belongs to something else. " +
                                            "NOT signalling it; removing the stale pid file.") }
    }

    return [pscustomobject]@{ Action = 'Stop'; Pid = $processId; Name = $proc.ProcessName
                              Reason = "stopping pid $processId ($($proc.ProcessName))" }
}

function Stop-PortalProcess {
    param([Parameter(Mandatory)][int] $ProcessId, [int] $TimeoutSeconds = 5)

    Stop-Process -Id $ProcessId -ErrorAction SilentlyContinue
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (-not (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)) { return $true }
        Start-Sleep -Milliseconds 250
    }

    Write-Host "Portal: pid $ProcessId did not exit after ${TimeoutSeconds}s - forcing"
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 500
    return -not (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)
}

# Dot-sourced by the tests, which want the functions and not the run. The installer invokes the file,
# where $MyInvocation.InvocationName is the path rather than '.'.
if ($MyInvocation.InvocationName -eq '.') { return }

$path = Get-QOpsPortalPidFile
$decision = Resolve-PortalStopAction -Path $path
Write-Host "Portal: $($decision.Reason)"

switch ($decision.Action) {
    'None'                { exit 0 }
    'RemoveStalePidFile'  { Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue; exit 0 }
    'Stop' {
        if (Stop-PortalProcess -ProcessId $decision.Pid -TimeoutSeconds $TimeoutSeconds) {
            Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
            Write-Host 'Portal: stopped.'
            exit 0
        }
        Write-Host "Portal: pid $($decision.Pid) is STILL running. Stop it yourself before removing the files,"
        Write-Host '        or the module directory will be left half-deleted.'
        exit 1
    }
}
