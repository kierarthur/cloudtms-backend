param([string]$TaskPath,[string]$TaskRoot,[string]$TaskMarker)
$ErrorActionPreference='Stop'
$resolved=(Resolve-Path -LiteralPath $TaskPath).Path
$approved=(Resolve-Path -LiteralPath $TaskRoot).Path.TrimEnd('\')+'\'
if(-not $resolved.StartsWith($approved,[StringComparison]::OrdinalIgnoreCase)){throw 'Fixture escapes approved owner'}
if((Split-Path -Leaf $resolved) -notmatch '^final-definer-git-buffer-[a-f0-9]{32}$'){throw 'Unexpected fixture leaf'}
$item=Get-Item -LiteralPath $resolved
if($item.Attributes -band [IO.FileAttributes]::ReparsePoint){throw 'Fixture reparse point refused'}
if([IO.File]::ReadAllText((Join-Path $resolved 'OWNER.txt')) -cne $TaskMarker){throw 'Fixture ownership marker mismatch'}
Remove-Item -LiteralPath $resolved -Recurse -Force
