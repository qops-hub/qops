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
