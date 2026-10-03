<#
.SYNOPSIS
    Starts the QOps Portal and opens it in the default browser.

.DESCRIPTION
    This is what the "QOps Portal" Start menu and Desktop shortcuts run (QOPS-329). It exists so the
    shortcut's own command line stays trivial - `powershell.exe -File <this file>` - because a
    shortcut argument string is authored in XML, escaped twice, and cannot be tried out anywhere
    except on a machine that has already installed the MSI. Everything that needs a decision lives
    here instead, in a file that can be read, run and changed without rebuilding an installer.

    The script ships at the root of the installed module, so $PSScriptRoot IS the module directory.
    The module is imported by path rather than by name: the shortcut points at one specific edition
    (the netstandard2.0 build, under Windows PowerShell), and importing by path is what guarantees
    the edition that gets loaded is the one the shortcut meant.

    WHICH DIRECTORY (QOPS-673). The installer writes the path of the version it installs into the
    shortcut, and QOps-Update installs every later version into a NEW directory beside it. So the
    directory this script sits in is only the version the shortcut was LAST POINTED AT. The version
    to start is the one recorded in %USERPROFILE%\qopsconfig\portal.settings.json under
    Portal:ModuleDirectory - written by QOps-Update, QOps-SetVersion and the installers - for THIS
    host's edition ("Desktop" under Windows PowerShell 5.1, "Core" under PowerShell 7). It is a full
    directory per edition, never "the newest version number": the newest number can live in the other
    edition's root, and importing that build here fails.

    The recorded directory is used only when it can be: a full path, holding QOpsModule.psd1, built
    for this edition (checked against QOpsModule.deps.json when it is there). Anything else - no
    file, no entry, an unreadable file, a directory that was removed - falls back to this script's own
    directory, which is exactly what the shortcut did before, and says why on the console.

.PARAMETER Port
    TCP port for the Portal. Deliberately has NO default (QOPS-437): when it is omitted the port is
    left for QOps-StartPortal to resolve - Portal:Port in %USERPROFILE%\qopsconfig\portal.settings.json,
    or 5555. Given here, it wins, exactly as `QOps-StartPortal -Port` does.

.PARAMETER NoBrowser
    Start the Portal but do not open a browser.

.PARAMETER UseScriptDirectory
    Start the module this script ships in, ignoring the recorded directory. For the installers' own
    "start the Portal when setup finishes", which must start what was just installed.

.PARAMETER RecordInstall
    Do not start anything: record this script's directory - for the edition of the host running
    it - (and -Net8Manifest's directory for PowerShell 7, when given) as the directories the
    launcher starts, then exit. Run by the Windows installers and the macOS package after they lay
    the module down, because an installation is itself a version switch.

.PARAMETER Net8Manifest
    With -RecordInstall: the QOpsModule.psd1 of the PowerShell 7 edition the same installer laid
    down. A manifest path rather than a directory, because an installer directory property ends in
    a backslash, and a quoted argument ending in \" is read by Windows as an escaped quote.
#>
[CmdletBinding(DefaultParameterSetName = 'Start')]
param(
    [Parameter(ParameterSetName = 'Start')]
    [int]    $Port,

    [Parameter(ParameterSetName = 'Start')]
    [switch] $NoBrowser,

    [Parameter(ParameterSetName = 'Start')]
    [switch] $UseScriptDirectory,

    [Parameter(ParameterSetName = 'Record', Mandatory = $true)]
    [switch] $RecordInstall,

    [Parameter(ParameterSetName = 'Record')]
    [string] $Net8Manifest
)

# The name the installers give both shortcuts. Only shortcuts with this name are ever touched.
$script:QOpsPortalShortcutName = 'QOps Portal.lnk'

function Get-QOpsPortalEdition {
    <# The edition of the host running this script, named as $PSVersionTable.PSEdition names it.
       Windows PowerShell 5.1 says 'Desktop'; so does anything older, which has no PSEdition at all. #>
    if ($PSVersionTable.PSEdition -eq 'Core') { return 'Core' }
    return 'Desktop'
}

function Get-QOpsPortalSettingsPath {
    <# The same file QOps-StartPortal reads the port from: UserProfile, as the module resolves it, and
       not $HOME, which on a domain machine can point at a redirected home drive. #>
    $profileDir = [Environment]::GetFolderPath('UserProfile')
    return (Join-Path (Join-Path $profileDir 'qopsconfig') 'portal.settings.json')
}

function Test-QOpsSamePath {
    param([string] $A, [string] $B)
    if (-not $A -or -not $B) { return $false }
    return ($A.TrimEnd('\', '/') -eq $B.TrimEnd('\', '/'))
}

function Get-QOpsPortalRecordedDirectory {
    <#
    .SYNOPSIS
        Reads Portal:ModuleDirectory:<Edition> from the settings file.
    .OUTPUTS
        Directory - the recorded path, or $null when nothing is recorded.
        Problem   - why the file could not be used, or $null. A missing file is not a problem.
    #>
    param([Parameter(Mandatory = $true)][string] $SettingsPath, [Parameter(Mandatory = $true)][string] $Edition)

    $none = [pscustomobject]@{ Directory = $null; Problem = $null }
    if (-not (Test-Path -LiteralPath $SettingsPath -PathType Leaf)) { return $none }

    try { $raw = Get-Content -LiteralPath $SettingsPath -Raw -ErrorAction Stop }
    catch { return [pscustomobject]@{ Directory = $null; Problem = "Could not read $SettingsPath ($($_.Exception.Message))" } }
    if ([string]::IsNullOrWhiteSpace($raw)) { return $none }

    try { $json = $raw | ConvertFrom-Json -ErrorAction Stop }
    catch { return [pscustomobject]@{ Directory = $null; Problem = "$SettingsPath is not valid JSON" } }

    # Member access is case-insensitive in PowerShell, matching how the Portal reads the same file.
    $dir = $null
    if ($json -and $json.Portal -and $json.Portal.ModuleDirectory) { $dir = $json.Portal.ModuleDirectory.$Edition }
    if ($dir -isnot [string] -or [string]::IsNullOrWhiteSpace($dir)) { return $none }
    return [pscustomobject]@{ Directory = $dir; Problem = $null }
}

function Get-QOpsModuleBuildEdition {
    <#
        Which build a module directory holds, from the runtimeTarget its QOpsModule.deps.json names:
        '.NETStandard,Version=v2.0/' is the Windows PowerShell build, '.NETCoreApp,Version=v8.0' the
        PowerShell 7 one. $null when the file is absent or says neither - "unknown", not "wrong".
        Read with a pattern rather than ConvertFrom-Json: only one value is needed, and the file is
        the largest in the directory.
    #>
    param([Parameter(Mandatory = $true)][string] $Directory)
    $deps = Join-Path $Directory 'QOpsModule.deps.json'
    if (-not (Test-Path -LiteralPath $deps -PathType Leaf)) { return $null }
    try { $text = Get-Content -LiteralPath $deps -Raw -ErrorAction Stop } catch { return $null }
    $m = [regex]::Match([string]$text, '"runtimeTarget"\s*:\s*\{\s*"name"\s*:\s*"([^"]*)"')
    if (-not $m.Success) { return $null }
    if ($m.Groups[1].Value -like '.NETStandard*') { return 'Desktop' }
    if ($m.Groups[1].Value -like '.NETCoreApp*') { return 'Core' }
    return $null
}

function Test-QOpsPortalModuleDirectory {
    <# $null when -Directory can be started under -Edition; otherwise the reason, for the console. #>
    param([Parameter(Mandatory = $true)][string] $Directory, [Parameter(Mandatory = $true)][string] $Edition)

    if (-not [System.IO.Path]::IsPathRooted($Directory)) { return "'$Directory' is not a full path" }
    if (-not (Test-Path -LiteralPath (Join-Path $Directory 'QOpsModule.psd1') -PathType Leaf)) {
        return "'$Directory' does not exist or holds no QOpsModule.psd1"
    }
    $built = Get-QOpsModuleBuildEdition -Directory $Directory
    if ($built -and $built -ne $Edition) {
        return "'$Directory' holds the build for the other PowerShell edition"
    }
    return $null
}

function Resolve-QOpsPortalModuleDirectory {
    <#
    .SYNOPSIS
        Decides which module directory to start, without starting anything.
    .OUTPUTS
        Directory - the module directory to import.
        Source    - 'Recorded' (from the settings file) or 'Script' (where this script sits).
        Note      - one line for the console, or $null when there is nothing worth saying.
        Warn      - $true when the note reports a recorded directory that could not be used.
    #>
    param(
        [Parameter(Mandatory = $true)][string] $ScriptRoot,
        [string] $SettingsPath,
        [string] $Edition,
        [switch] $IgnoreRecorded
    )
    if (-not $SettingsPath) { $SettingsPath = Get-QOpsPortalSettingsPath }
    if (-not $Edition) { $Edition = Get-QOpsPortalEdition }

    $own = [pscustomobject]@{ Directory = $ScriptRoot; Source = 'Script'; Note = $null; Warn = $false }
    if ($IgnoreRecorded) { return $own }

    $recorded = Get-QOpsPortalRecordedDirectory -SettingsPath $SettingsPath -Edition $Edition
    if ($recorded.Problem) {
        $own.Note = "$($recorded.Problem). Starting the copy this shortcut points at: $ScriptRoot"
        $own.Warn = $true
        return $own
    }
    if (-not $recorded.Directory) { return $own }

    $why = Test-QOpsPortalModuleDirectory -Directory $recorded.Directory -Edition $Edition
    if ($why) {
        $own.Note = "The QOps directory recorded in $SettingsPath cannot be used: $why. " +
                    "Starting the copy this shortcut points at: $ScriptRoot"
        $own.Warn = $true
        return $own
    }

    $note = $null
    if (-not (Test-QOpsSamePath $recorded.Directory $ScriptRoot)) {
        $note = "Starting QOps from $($recorded.Directory) (recorded in $SettingsPath)."
    }
    return [pscustomobject]@{ Directory = $recorded.Directory; Source = 'Recorded'; Note = $note; Warn = $false }
}

function Get-QOpsPortalShortcutChange {
    <#
    .SYNOPSIS
        Decides what to do with ONE shortcut, from its target, arguments and working directory.
    .DESCRIPTION
        A shortcut is ours when it starts powershell.exe with -File "...\Start-QOpsPortal.ps1" - the
        shape both installers author. Anything else with the same name is left alone. Only the quoted
        launcher path and the working directory change; every other argument is kept as it is.
    .OUTPUTS
        Action - 'NotOurs' | 'Current' | 'Update'; Arguments and WorkingDirectory - the values to save.
    #>
    param(
        [string] $TargetPath,
        [string] $Arguments,
        [string] $WorkingDirectory,
        [Parameter(Mandatory = $true)][string] $ModuleDirectory
    )
    $leaf = ''
    if ($TargetPath) { $leaf = ($TargetPath -split '[\\/]')[-1] }
    $m = [regex]::Match([string]$Arguments, '-File\s+"([^"]*Start-QOpsPortal\.ps1)"', 'IgnoreCase')
    if ($leaf -ne 'powershell.exe' -or -not $m.Success) {
        return [pscustomobject]@{ Action = 'NotOurs'; Arguments = $Arguments; WorkingDirectory = $WorkingDirectory }
    }

    # The shortcut is a Windows object, so the path is joined with a backslash whatever runs this.
    $dir = $ModuleDirectory.TrimEnd('\', '/')
    $launcher = $dir + '\Start-QOpsPortal.ps1'
    $g = $m.Groups[1]
    $newArguments = $Arguments.Substring(0, $g.Index) + $launcher + $Arguments.Substring($g.Index + $g.Length)

    if (($newArguments -eq $Arguments) -and (Test-QOpsSamePath $WorkingDirectory $dir)) {
        return [pscustomobject]@{ Action = 'Current'; Arguments = $Arguments; WorkingDirectory = $WorkingDirectory }
    }
    return [pscustomobject]@{ Action = 'Update'; Arguments = $newArguments; WorkingDirectory = $dir }
}

function Get-QOpsPortalShortcutPaths {
    <# Where the installers put the shortcuts: this user's Start menu and Desktop for a per-user
       install, the all-users ones for a per-machine install. Another user's own shortcuts are in
       that user's profile and are not reachable from here. #>
    foreach ($folder in 'Programs', 'Desktop', 'CommonPrograms', 'CommonDesktopDirectory') {
        $dir = [Environment]::GetFolderPath($folder)
        if ($dir) { Join-Path $dir $script:QOpsPortalShortcutName }
    }
}

function Update-QOpsPortalShortcut {
    <#
    .SYNOPSIS
        Points this user's "QOps Portal" shortcuts at -ModuleDirectory. Called by QOps-Update and
        QOps-SetVersion; returns one object per shortcut (Path, Outcome, Detail) and never throws.
    .DESCRIPTION
        Outcome: Updated | Current | Skipped | NotFound | Failed.
        Windows only - a shortcut is a Windows Shell object, written through WScript.Shell.
    #>
    param([Parameter(Mandatory = $true)][string] $ModuleDirectory, [string[]] $Path)

    # Not `if ($IsWindows)`: that variable does not exist in Windows PowerShell 5.1, where it reads
    # as $null and would send Windows down the non-Windows branch.
    $onWindows = ($PSVersionTable.PSVersion.Major -lt 6) -or $IsWindows
    if (-not $onWindows) {
        return [pscustomobject]@{ Path = $null; Outcome = 'Skipped'
            Detail = 'The QOps Portal shortcuts exist only on Windows; nothing to update on this system.' }
    }

    $launcher = Join-Path $ModuleDirectory 'Start-QOpsPortal.ps1'
    if (-not (Test-Path -LiteralPath $launcher -PathType Leaf)) {
        return [pscustomobject]@{ Path = $null; Outcome = 'Skipped'
            Detail = "$ModuleDirectory has no Start-QOpsPortal.ps1 (it predates the Portal shortcuts); the shortcuts were left as they are." }
    }

    if (-not $Path) { $Path = @(Get-QOpsPortalShortcutPaths) }
    $existing = @($Path | Where-Object { $_ -and (Test-Path -LiteralPath $_ -PathType Leaf) })
    if ($existing.Count -eq 0) {
        return [pscustomobject]@{ Path = $null; Outcome = 'NotFound'
            Detail = "No 'QOps Portal' shortcut for this Windows user (looked in: $($Path -join '; '))." }
    }

    try { $shell = New-Object -ComObject WScript.Shell -ErrorAction Stop }
    catch {
        return [pscustomobject]@{ Path = $null; Outcome = 'Failed'
            Detail = "Could not open the Windows Shell to edit shortcuts: $($_.Exception.Message)" }
    }

    foreach ($p in $existing) {
        try {
            $lnk = $shell.CreateShortcut($p)
            $change = Get-QOpsPortalShortcutChange -TargetPath $lnk.TargetPath -Arguments $lnk.Arguments `
                                                   -WorkingDirectory $lnk.WorkingDirectory -ModuleDirectory $ModuleDirectory
            if ($change.Action -eq 'NotOurs') {
                [pscustomobject]@{ Path = $p; Outcome = 'Skipped'; Detail = 'not a QOps Portal launcher; left alone' }
            }
            elseif ($change.Action -eq 'Current') {
                [pscustomobject]@{ Path = $p; Outcome = 'Current'; Detail = "already starts $ModuleDirectory" }
            }
            else {
                $lnk.Arguments = $change.Arguments
                $lnk.WorkingDirectory = $change.WorkingDirectory
                $lnk.Save()
                [pscustomobject]@{ Path = $p; Outcome = 'Updated'; Detail = "now starts $ModuleDirectory" }
            }
        }
        catch {
            [pscustomobject]@{ Path = $p; Outcome = 'Failed'; Detail = $_.Exception.Message }
        }
    }
}

function Wait-BeforeClosing {
    <#
        A shortcut runs in a console window of its own, and Windows destroys that window the instant
        the script returns. Without this pause every failure below is invisible: the window appears,
        flashes, and the user is left with no Portal and no explanation - which is the exact failure
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

# Dot-sourced by the tests and by QOps-Update / QOps-SetVersion, which want the functions and not the
# run. A shortcut or an installer invokes the file, where InvocationName is the path rather than '.'.
if ($MyInvocation.InvocationName -eq '.') { return }

$ErrorActionPreference = 'Stop'

if ($RecordInstall) {
    # No Read-Host on any path here: the installer runs this with no one watching its console.
    try {
        Import-Module -Name (Join-Path $PSScriptRoot 'QOpsModule.psd1') -DisableNameChecking -ErrorAction Stop
        # This script's own directory is recorded for the edition of the host running it: the MSIs
        # run it under powershell.exe from the netstandard2.0 directory (Desktop), the macOS package
        # under pwsh from the only, net8.0, directory (Core).
        $desktopDir = $null
        $coreDir = $null
        if ((Get-QOpsPortalEdition) -eq 'Core') { $coreDir = $PSScriptRoot } else { $desktopDir = $PSScriptRoot }
        if ($Net8Manifest) { $coreDir = Split-Path -Parent $Net8Manifest }
        Write-Host ([QopsModule.Helpers.PortalLaunchPin]::RecordInstall($desktopDir, $coreDir))
        exit 0
    }
    catch {
        Write-Host "Portal launcher: could not record the installed directories - $($_.Exception.Message)"
        exit 1
    }
}

try {
    $resolved = Resolve-QOpsPortalModuleDirectory -ScriptRoot $PSScriptRoot -IgnoreRecorded:$UseScriptDirectory
    if ($resolved.Note) {
        if ($resolved.Warn) { Write-Host $resolved.Note -ForegroundColor Yellow }
        else { Write-Host $resolved.Note }
    }

    $manifest = Join-Path $resolved.Directory 'QOpsModule.psd1'
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
    # BoundParameters - so a defaulted -Port 5555 forwarded unconditionally would be indistinguishable
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
