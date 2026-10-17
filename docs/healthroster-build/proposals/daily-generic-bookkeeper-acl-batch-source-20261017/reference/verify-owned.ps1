param([Parameter(Mandatory=$true)][string]$OwnedPath)
$ErrorActionPreference='Stop'
Import-Module Microsoft.PowerShell.Security -ErrorAction Stop
$resolved=(Get-Item -LiteralPath $OwnedPath -Force).FullName
if ($resolved -ne $OwnedPath.Replace('/','\')) { throw 'FIXTURE_PRIVATE_DIRECTORY_REQUIRED' }
$item=Get-Item -LiteralPath $OwnedPath -Force
if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or -not $item.PSIsContainer) { throw 'FIXTURE_PRIVATE_DIRECTORY_REQUIRED' }
$acl=Get-Acl -LiteralPath $OwnedPath
$sid=[Security.Principal.WindowsIdentity]::GetCurrent().User
if ($acl.GetOwner([Security.Principal.SecurityIdentifier]).Value -ne $sid.Value -or -not $acl.AreAccessRulesProtected) { throw 'FIXTURE_PRIVATE_ACL_REQUIRED' }
$rules=@($acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier]))
if ($rules.Count -ne 1 -or $rules[0].IdentityReference.Value -ne $sid.Value -or $rules[0].AccessControlType -ne 'Allow' -or $rules[0].IsInherited -or $rules[0].FileSystemRights -ne [Security.AccessControl.FileSystemRights]::FullControl) { throw 'FIXTURE_PRIVATE_ACL_REQUIRED' }
# Metadata-only check. No state/environment/credential file is opened.
