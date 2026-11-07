# QOPS-755: finishes a forced reinstall of a version that was running when QOps-Update -Force ran on Windows.
#
# QOps-Update -Force cannot replace files a PowerShell window has loaded, so it stages the new copy in
# <module root>\.qops-staged\<version>\, load-checks it there, and writes <module root>\.qops-staged\<version>.swap-pending
# beside it. QOpsModule.psm1 calls this script at the next session start, BEFORE it imports QOpsModule.dll, so this
# session holds nothing in the folder yet. Kept to what Windows PowerShell 5.1 runs.
#
# The swap is done FILE BY FILE inside <version>, which is never renamed, so QOpsModule.psd1 and QOpsModule.psm1 stay
# in it and a start interrupted at any point still runs the psm1 that resumes it. Per file: check it is not in use,
# move the current file to .qops-staged\<version>.previous\<relpath>, move the staged file in. The files to swap are
# listed once in .qops-staged\<version>.files, so a resumed run knows them; each file's state is read from where it
# is (staged / backup / module folder), so resuming is idempotent. QOpsModule.dll goes first (the file another
# window most likely holds, so a refusal costs nothing to undo); the psd1 and the psm1 go last, each in one
# File.Replace call, and are skipped when unchanged: they are what makes the folder loadable at all.
#
#   - A file in use (another window or the Portal has it loaded): every file moved so far goes back, the staged copy
#     stays whole, the swap stays pending, one warning. The folder holds the OLD version only, never a mix.
#   - The staged copy lacks a file the module needs, or the swapped folder is not complete: every file is restored
#     from the backup and the staged copy is dropped.
#   - Done: the backup, the staged copy and the list are removed, the marker last.
param(
    [Parameter(Mandatory = $true)][string] $Target,
    [Parameter(Mandatory = $true)][string] $Staged
)

$version = Split-Path -Leaf $Target
$marker = $Staged + '.swap-pending'
$list = $Staged + '.files'
$backup = $Staged + '.previous'
$required = @('QOpsModule.psd1', 'QOpsModule.psm1', 'QOpsModule.dll', '.ready')
$last = @('QOpsModule.psd1', 'QOpsModule.psm1')

function Test-QOpsPath([string] $path) { return [System.IO.File]::Exists($path) }

function Move-QOpsFile([string] $from, [string] $to) {
    $dir = [System.IO.Path]::GetDirectoryName($to)
    if (-not [System.IO.Directory]::Exists($dir)) { [void][System.IO.Directory]::CreateDirectory($dir) }
    [System.IO.File]::Move($from, $to)
}

function Assert-QOpsNotInUse([string] $path) {
    # A loaded DLL can be RENAMED on Windows but not opened for writing, so this, not the move, finds the holder.
    $stream = [System.IO.File]::Open($path, 'Open', 'ReadWrite', 'None')
    $stream.Dispose()
}

function Remove-QOpsStaging([bool] $keepPending) {
    if (-not $keepPending) {
        if ([System.IO.Directory]::Exists($Staged)) { Remove-Item -LiteralPath $Staged -Recurse -Force -ErrorAction SilentlyContinue }
        Remove-Item -LiteralPath $list -Force -ErrorAction SilentlyContinue
    }
    if ([System.IO.Directory]::Exists($backup)) { Remove-Item -LiteralPath $backup -Recurse -Force -ErrorAction SilentlyContinue }
    if (-not $keepPending -and -not [System.IO.Directory]::Exists($backup)) {
        Remove-Item -LiteralPath $marker -Force -ErrorAction SilentlyContinue
    }
}

# Puts every swapped file back. $keepPending: the new files go back into the staged copy for a later start;
# otherwise they are deleted and the staged copy dropped.
function Undo-QOpsSwap([object[]] $files, [bool] $keepPending) {
    $reversed = @($files); [array]::Reverse($reversed)
    foreach ($e in $reversed) {
        $s = Join-Path $Staged $e.Rel; $t = Join-Path $Target $e.Rel; $b = Join-Path $backup $e.Rel
        if (Test-QOpsPath $b) {
            if (Test-QOpsPath $t) {
                if ($keepPending -and -not (Test-QOpsPath $s)) { Move-QOpsFile $t $s } else { [System.IO.File]::Delete($t) }
            }
            Move-QOpsFile $b $t
        }
        elseif (-not $e.Original) {
            # A file only the new copy has: moved in when it is in the module folder and no longer staged.
            if ((Test-QOpsPath $t) -and -not (Test-QOpsPath $s)) {
                if ($keepPending) { Move-QOpsFile $t $s } else { [System.IO.File]::Delete($t) }
            }
        }
        elseif ($keepPending -and ($last -contains $e.Rel) -and -not (Test-QOpsPath $s)) {
            [System.IO.File]::Copy($t, $s)   # unchanged, so skipped and never moved: the staged copy gets it back
        }
    }
    Remove-QOpsStaging $keepPending
}

# One entry per staged file: its path relative to the folder, and whether the module folder had it before the swap
# (written once, as O or N and a tab before the path, so a resumed run does not have to guess).
function Read-QOpsList {
    if (-not (Test-QOpsPath $list)) { Write-QOpsList }
    return @([System.IO.File]::ReadAllLines($list) | Where-Object { $_ } |
             ForEach-Object { New-Object PSObject -Property @{ Original = ($_[0] -eq 'O'); Rel = $_.Substring(2) } })
}

function Write-QOpsList {

    # First run for this staged copy: it must be whole, and a backup left from an earlier swap must be gone.
    foreach ($f in $required) {
        if (-not (Test-QOpsPath (Join-Path $Staged $f))) { throw "QOPS755-INCOMPLETE $f is missing from the new copy" }
    }
    if ([System.IO.Directory]::Exists($backup)) { Remove-Item -LiteralPath $backup -Recurse -Force -ErrorAction Stop }
    $root = [System.IO.Path]::GetFullPath($Staged).TrimEnd('\', '/')
    $all = @(Get-ChildItem -LiteralPath $Staged -Recurse -Force | Where-Object { -not $_.PSIsContainer } |
             ForEach-Object { $_.FullName.Substring($root.Length + 1) })
    $ordered = @($all | Where-Object { $_ -eq 'QOpsModule.dll' }) +
               @($all | Where-Object { $_ -ne 'QOpsModule.dll' -and $last -notcontains $_ } | Sort-Object) +
               @($last | Where-Object { $all -contains $_ })
    $lines = @($ordered | ForEach-Object { $(if (Test-QOpsPath (Join-Path $Target $_)) { 'O' } else { 'N' }) + "`t" + $_ })
    [System.IO.File]::WriteAllLines($list + '.tmp', [string[]]$lines)
    [System.IO.File]::Move($list + '.tmp', $list)
}

if (-not [System.IO.Directory]::Exists($Staged) -and -not (Test-QOpsPath $list)) {
    Remove-Item -LiteralPath $marker -Force -ErrorAction SilentlyContinue   # stale: nothing staged
    return
}

$files = @()
try {
    $files = Read-QOpsList
}
catch {
    if ($_.Exception.Message -like 'QOPS755-INCOMPLETE *') {
        Remove-QOpsStaging $false
        Write-Warning ("QOps: the reinstall of $version could not be finished (" + $_.Exception.Message.Substring(19) + "). The version you had is kept.")
    }
    else {
        Write-Warning ("QOps: the reinstall of $version is waiting, because another PowerShell window or the QOps Portal " +
                       "still uses it. Close all PowerShell windows and the Portal, then open a new PowerShell window to finish it.")
    }
    return
}

$stopAfter = 0
if ($env:QOPS755_TEST_STOP_AFTER) { $stopAfter = [int]$env:QOPS755_TEST_STOP_AFTER }   # test seam: simulates a crash
$moved = 0
try {
    foreach ($e in $files) {
        $rel = $e.Rel
        $s = Join-Path $Staged $rel; $t = Join-Path $Target $rel; $b = Join-Path $backup $rel
        if (-not (Test-QOpsPath $s)) { continue }                     # swapped already (or lost: the check below finds it)
        if ($last -contains $rel) {
            if ((Test-QOpsPath $t) -and ([System.IO.File]::ReadAllText($s) -ceq [System.IO.File]::ReadAllText($t))) {
                [System.IO.File]::Delete($s); continue                 # unchanged: nothing to replace
            }
            [void][System.IO.Directory]::CreateDirectory([System.IO.Path]::GetDirectoryName($b))
            [System.IO.File]::Replace($s, $t, $b)
        }
        else {
            if ($e.Original -and (Test-QOpsPath $t) -and -not (Test-QOpsPath $b)) {
                Assert-QOpsNotInUse $t
                Move-QOpsFile $t $b
            }
            if (Test-QOpsPath $t) { throw "$rel is in the module folder before the new copy was moved in" }
            Move-QOpsFile $s $t
        }
        $moved++
        if ($stopAfter -gt 0 -and $moved -ge $stopAfter) { [System.Environment]::Exit(99) }
    }
}
catch {
    Undo-QOpsSwap $files $true
    Write-Warning ("QOps: the reinstall of $version is waiting, because another PowerShell window or the QOps Portal " +
                   "still uses it. Close all PowerShell windows and the Portal, then open a new PowerShell window to finish it.")
    return
}

# Complete: every listed file is in the module folder and none is left staged.
$problem = $null
foreach ($e in $files) {
    $rel = $e.Rel
    $swapped = Test-QOpsPath (Join-Path $backup $rel)
    $unchanged = ($last -contains $rel) -and -not $swapped
    if (-not (Test-QOpsPath (Join-Path $Target $rel))) { $problem = "$rel is missing"; break }
    if (Test-QOpsPath (Join-Path $Staged $rel)) { $problem = "$rel was not moved"; break }
    if ($e.Original -and -not $swapped -and -not $unchanged) { $problem = "$rel of the new copy is missing"; break }
}
if ($problem) {
    Undo-QOpsSwap $files $false
    Write-Warning "QOps: the reinstall of $version could not be finished ($problem). The version you had is kept."
    return
}

Remove-QOpsStaging $false
if (Test-QOpsPath $marker) { return }   # the backup is still held: a later start removes it
Write-Host "QOps: the reinstall of $version is finished."
