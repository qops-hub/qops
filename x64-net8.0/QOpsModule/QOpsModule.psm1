# QOPS-755: a forced reinstall of this version, staged while it was loaded, is swapped in before the DLL loads.
$qopsStaged = Join-Path (Join-Path (Split-Path -Parent $PSScriptRoot) '.qops-staged') (Split-Path -Leaf $PSScriptRoot)
if (Test-Path -LiteralPath ($qopsStaged + '.swap-pending')) {
    & (Join-Path $PSScriptRoot 'Complete-QOpsReinstall.ps1') -Target $PSScriptRoot -Staged $qopsStaged
}
Remove-Variable -Name qopsStaged

Import-Module (Join-Path $PSScriptRoot 'QOpsModule.dll') -DisableNameChecking

# Load format files resiliently — missing files degrade gracefully instead of blocking module load
$formatFiles = @(
    'SenseAppInfo.Formats.ps1xml',
    'QlikSenseBaseOutput.Formats.ps1xml',
    'QlikSenseBaseOutputNoId.Formats.ps1xml',
    'QrsCustomPropertyFull.Formats.ps1xml',
    'ReloadResult.Formats.ps1xml',
    'ReloadTaskOutput.Formats.ps1xml'
)
foreach ($f in $formatFiles) {
    $path = Join-Path $PSScriptRoot $f
    if (Test-Path $path) {
        Update-FormatData -PrependPath $path -ErrorAction SilentlyContinue
    }
}
