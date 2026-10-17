param([Parameter(Mandatory=$true)][string]$OwnedPath)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$expectedParent='D:\Development\GitHub\.codex-worktrees\healthroster-install2-14102026\.codex-tmp'
$resolved=[IO.Path]::GetFullPath($OwnedPath)
if([IO.Path]::GetDirectoryName($resolved) -cne $expectedParent -or [IO.Path]::GetFileName($resolved) -cnotmatch '^daily-s0-catalogue-20261017-[a-f0-9]{32}$'){throw 'AUTH_OWNED_PATH_REQUIRED'}
$item=Get-Item -LiteralPath $resolved -Force
if(-not $item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)){throw 'AUTH_OWNED_DIRECTORY_REQUIRED'}
$sid=[Security.Principal.WindowsIdentity]::GetCurrent().User
$acl=Get-Acl -LiteralPath $resolved
$acl.SetAccessRuleProtection($true,$false)
$acl.SetOwner($sid)
$rule=[Security.AccessControl.FileSystemAccessRule]::new($sid,'FullControl','ContainerInherit,ObjectInherit','None','Allow')
$acl.SetAccessRule($rule)
Set-Acl -LiteralPath $resolved -AclObject $acl
$after=Get-Acl -LiteralPath $resolved
$rules=@($after.Access)
if(-not $after.AreAccessRulesProtected -or $rules.Count -ne 1 -or $rules[0].IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -ne $sid.Value -or $rules[0].AccessControlType -ne 'Allow' -or $rules[0].FileSystemRights -ne 'FullControl'){throw 'AUTH_PRIVATE_DIRECTORY_ACL_FAILED'}
