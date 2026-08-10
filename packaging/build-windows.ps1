<#
.SYNOPSIS
  Build the frozen Windows client binary, dist\tcpbroke-windows.exe.

.DESCRIPTION
  Produces the single-file exe that both the VM (agent mode) and the operator machine (listen mode)
  run. Deliberately a script rather than a .spec file: the flags below are the whole build, and a
  spec is one more thing to drift out of sync.

  NOT the primary build path. .github/workflows/release.yml builds Windows and Linux binaries on
  every push to main and publishes them as the 'nightly' prerelease - take them from there. This
  script exists to test a build locally before pushing, and it must be kept in step with the
  PyInstaller flags in that workflow.

  Requires a real Python on PATH - the Microsoft Store alias in WindowsApps is a stub and will not
  do. Install from python.org, then:

      py -m pip install -r requirements-dev.txt
      powershell -ExecutionPolicy Bypass -File packaging\build-windows.ps1

.PARAMETER Clean
  Remove build\ and dist\ before building.
#>
[CmdletBinding()]
param(
    [switch]$Clean
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$RepoDir = (Resolve-Path "$PSScriptRoot\..").Path
Set-Location $RepoDir

if ($Clean) {
    foreach ($d in @('build', 'dist')) {
        $p = Join-Path $RepoDir $d
        if (Test-Path -LiteralPath $p) {
            Write-Host "removing $p"
            Remove-Item -LiteralPath $p -Recurse -Force
        }
    }
}

# --collect-data certifi: the CA bundle is a data file, so PyInstaller does not pick it up from the
# import graph alone. Without it every wss:// connection fails to verify in the frozen build.
$pyinstallerArgs = @(
    '--onefile',
    '--console',
    '--name', 'tcpbroke-windows',
    '--collect-data', 'certifi',
    '--noconfirm',
    'entrypoint_cli.py'
)

Write-Host "building: pyinstaller $($pyinstallerArgs -join ' ')"
& py -m PyInstaller @pyinstallerArgs
if ($LASTEXITCODE -ne 0) {
    throw "pyinstaller failed with exit code $LASTEXITCODE"
}

$exe = Join-Path $RepoDir 'dist\tcpbroke-windows.exe'
if (-not (Test-Path -LiteralPath $exe)) {
    throw "expected output not found: $exe"
}

$hash = (Get-FileHash -LiteralPath $exe -Algorithm SHA256).Hash
$size = [math]::Round((Get-Item -LiteralPath $exe).Length / 1MB, 1)
Write-Host ''
Write-Host ("built  : {0} ({1} MB)" -f $exe, $size)
Write-Host ("sha256 : {0}" -f $hash)
Write-Host ("commit : {0}" -f (& git rev-parse --short HEAD))
Write-Host ''
Write-Host 'smoke test:'
Write-Host ("  {0} --help" -f $exe)
