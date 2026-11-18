# QOPS-755: a forced reinstall of this version, staged while it was loaded, is swapped in before the DLL loads.
$qopsStaged = Join-Path (Join-Path (Split-Path -Parent $PSScriptRoot) '.qops-staged') (Split-Path -Leaf $PSScriptRoot)
if (Test-Path -LiteralPath ($qopsStaged + '.swap-pending')) {
    & (Join-Path $PSScriptRoot 'Complete-QOpsReinstall.ps1') -Target $PSScriptRoot -Staged $qopsStaged
}
Remove-Variable -Name qopsStaged

# QOPS-857: QOps ships two builds - Windows PowerShell 5.1 (netstandard2.0, with the .NET Framework Qlik SDK)
# and PowerShell 7 (net8.0). Loaded into PowerShell 7, the 5.1 build fails later, deep in the Qlik SDK (a
# NullReferenceException in a static constructor on the first Qlik Cloud call). Refuse it here instead, with
# the fix. The build is told by the DLL itself: a netstandard2.0 assembly references 'netstandard', the net8.0
# one does not. Only PowerShell 7 checks, so Windows PowerShell 5.1 runs nothing new; a check that cannot
# read the DLL lets the module load as before.
# QOPS-861: a session that already runs a 5.1 build of QOps in PowerShell 7 is the one an earlier version
# leaves behind (a Portal started from Windows PowerShell restarts itself this way). Refusing there only
# blocks the switch away from it, so that session gets a warning and the load goes on; a fresh session
# is still refused.
if ($PSEdition -eq 'Core') {
    $qopsDesktopBuild = $false
    try {
        $qopsStream = [System.IO.File]::OpenRead((Join-Path $PSScriptRoot 'QOpsModule.dll'))
        try {
            $qopsPe = [System.Reflection.PortableExecutable.PEReader]::new($qopsStream)
            $qopsMd = [System.Reflection.Metadata.PEReaderExtensions]::GetMetadataReader($qopsPe)
            foreach ($qopsRef in $qopsMd.AssemblyReferences) {
                if ($qopsMd.GetString($qopsMd.GetAssemblyReference($qopsRef).Name) -eq 'netstandard') { $qopsDesktopBuild = $true; break }
            }
            $qopsPe.Dispose()
        } finally { $qopsStream.Dispose() }
    } catch { $qopsDesktopBuild = $false }
    Remove-Variable -Name qopsStream, qopsPe, qopsMd, qopsRef -ErrorAction SilentlyContinue
    $qopsDesktopLoaded = $false
    if ($qopsDesktopBuild) {
        foreach ($qopsAsm in [System.AppDomain]::CurrentDomain.GetAssemblies()) {
            try {
                if ($qopsAsm.GetName().Name -like 'QOpsModule*' -and ($qopsAsm.GetReferencedAssemblies() | Where-Object { $_.Name -eq 'netstandard' })) { $qopsDesktopLoaded = $true; break }
            } catch { }
        }
        Remove-Variable -Name qopsAsm -ErrorAction SilentlyContinue
    }
    if ($qopsDesktopBuild -and $qopsDesktopLoaded) {
        Write-Warning ("This QOps module at '$PSScriptRoot' is the Windows PowerShell 5.1 build. It is loaded because this " +
            "PowerShell $($PSVersionTable.PSVersion) session already runs a Windows PowerShell 5.1 build of QOps. " +
            "Start a new PowerShell 7 session, or start the QOps Portal from PowerShell 7 (pwsh), to use the PowerShell 7 build.")
    } elseif ($qopsDesktopBuild) {
        Remove-Variable -Name qopsDesktopBuild, qopsDesktopLoaded
        throw ("This QOps module at '$PSScriptRoot' is the Windows PowerShell 5.1 build and cannot run in PowerShell $($PSVersionTable.PSVersion). " +
            "Install QOps for PowerShell 7, or use this module from Windows PowerShell 5.1. " +
            "If the QOps Portal was started from Windows PowerShell, start it from PowerShell 7 (pwsh) instead.")
    }
    Remove-Variable -Name qopsDesktopBuild, qopsDesktopLoaded
}

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
