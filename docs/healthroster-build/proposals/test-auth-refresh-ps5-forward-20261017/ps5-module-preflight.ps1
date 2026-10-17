param([Parameter(Mandatory=$true)][string]$PublicDirectory)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$expectedDirectory='D:\Development\GitHub\.codex-worktrees\healthroster-v2-report-build-13102026\docs\healthroster-build\proposals\test-auth-refresh-ps5-forward-20261017'
if([IO.Path]::GetFullPath($PublicDirectory) -cne $expectedDirectory){throw 'PS5_PUBLIC_PROBE_PATH_REQUIRED'}
if($PSVersionTable.PSVersion.Major -ne 5 -or $PSVersionTable.PSEdition -cne 'Desktop'){throw 'NATIVE_PS5_REQUIRED'}
Import-Module Microsoft.PowerShell.Security -ErrorAction Stop
# The manifest imports the native binary cmdlets whose observed ModuleBase is PSHOME.
$expectedModule=$PSHOME
$getter=Get-Command Get-Acl -CommandType Cmdlet -ErrorAction Stop
$setter=Get-Command Set-Acl -CommandType Cmdlet -ErrorAction Stop
if($getter.ModuleName -cne 'Microsoft.PowerShell.Security' -or $setter.ModuleName -cne 'Microsoft.PowerShell.Security' -or $getter.Module.ModuleBase -cne $expectedModule -or $setter.Module.ModuleBase -cne $expectedModule){throw 'NATIVE_SECURITY_MODULE_REQUIRED'}
# Read public-directory metadata only. Set-Acl is resolved, never invoked.
$acl=Get-Acl -LiteralPath $PublicDirectory
if($null -eq $acl){throw 'PUBLIC_ACL_READ_REQUIRED'}
[Console]::WriteLine('PS5_SECURITY_IMPORT_PASS')
