<#
.SYNOPSIS
    Starts the QOps Portal and opens it in the default browser.

.DESCRIPTION
    This is what the "QOps Portal" Start menu and Desktop shortcuts run (QOPS-329). It exists so the
    shortcut's own command line stays trivial — `powershell.exe -File <this file>` — because a
    shortcut argument string is authored in XML, escaped twice, and cannot be tried out anywhere
    except on a machine that has already installed the MSI. Everything that needs a decision lives
    here instead, in a file that can be read, run and changed without rebuilding an installer.

    The script ships at the root of the installed module, so $PSScriptRoot IS the module directory.
    The module is imported from there by path rather than by name: the shortcut points at one
    specific edition (the netstandard2.0 build, under Windows PowerShell), and importing by path is
    what guarantees the edition that gets loaded is the one the shortcut meant.

.PARAMETER Port
    TCP port for the Portal. Deliberately has NO default (QOPS-437): when it is omitted the port is
    left for QOps-StartPortal to resolve — Portal:Port in %USERPROFILE%\qopsconfig\portal.settings.json,
    or 5555. Given here, it wins, exactly as `QOps-StartPortal -Port` does.

.PARAMETER NoBrowser
    Start the Portal but do not open a browser.
#>
[CmdletBinding()]
param(
    [int]    $Port,
    [switch] $NoBrowser
)

$ErrorActionPreference = 'Stop'

function Wait-BeforeClosing {
    <#
        A shortcut runs in a console window of its own, and Windows destroys that window the instant
        the script returns. Without this pause every failure below is invisible: the window appears,
        flashes, and the user is left with no Portal and no explanation — which is the exact failure
        this ticket exists to prevent.

        Only the failure paths call it. A successful start has already put the Portal in the browser,
        and holding a console open after that would leave the user staring at the command line the
        shortcut was added to avoid.
    #>
    if ([Environment]::UserInteractive) {
        Write-Host ''
        Write-Host 'Press Enter to close this window.' -ForegroundColor Yellow
        [void](Read-Host)
    }
}

try {
    $manifest = Join-Path $PSScriptRoot 'QOpsModule.psd1'
    if (-not (Test-Path -LiteralPath $manifest)) {
        Write-Host "QOps module manifest not found at '$manifest'." -ForegroundColor Red
        Write-Host 'The installation looks incomplete. Reinstall QOps and try again.' -ForegroundColor Red
        Wait-BeforeClosing
        exit 1
    }

    Import-Module -Name $manifest -ErrorAction Stop

    # QOps-StartPortal writes its progress to the console itself (Logger uses Console.WriteLine, not
    # the PowerShell output stream), so the ONLY thing on the pipeline is the Portal URL, and only on
    # a successful start. An empty result therefore means "it did not start", with the reason already
    # printed above by the cmdlet.
    # QOPS-437: forward -Port ONLY when the caller actually passed one, hence the splat. The shortcut
    # runs this script with no arguments at all, and the cmdlet decides "was a port chosen" from its
    # BoundParameters — so a defaulted -Port 5555 forwarded unconditionally would be indistinguishable
    # from a typed one, and the Start menu / Desktop shortcut would become the single launch path that
    # can never honour a persisted Portal:Port. That is the path with no command line to type it on.
    $forward = @{}
    if ($PSBoundParameters.ContainsKey('Port')) { $forward['Port'] = $Port }
    if ($NoBrowser) { $forward['NoBrowser'] = $true }

    $url = QOps-StartPortal @forward

    if (-not $url) {
        Write-Host ''
        Write-Host 'The QOps Portal did not start. The messages above explain why.' -ForegroundColor Red
        Wait-BeforeClosing
        exit 1
    }

    exit 0
}
catch {
    Write-Host ''
    Write-Host "Could not start the QOps Portal: $($_.Exception.Message)" -ForegroundColor Red
    Wait-BeforeClosing
    exit 1
}
