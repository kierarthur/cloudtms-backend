param(
 [Parameter(Mandatory=$true)][string]$ReviewedSourceRoot,
 [Parameter(Mandatory=$true)][string]$PrivateOutputDirectory,
 [Parameter(Mandatory=$true)][Guid]$ReviewedAgencyId,
 [Parameter(Mandatory=$true)][Guid]$ReviewedAgencyAnchorGroupId,
 [switch]$MultiRowLineage,
 [switch]$StopAfterFin
)
$ErrorActionPreference='Stop'
# This script writes reviewable psql source. It never launches a SQL client,
# reads credentials, takes a turn, opens a network connection or runs a query.
$nfxRecipeFolder=$PSScriptRoot.Replace('\','/')
$nfxBatchFolder=if($MultiRowLineage){"$nfxRecipeFolder/multirow-lineage-fixture"}else{$nfxRecipeFolder}
$nfxReviewedRoot=(Resolve-Path -LiteralPath $ReviewedSourceRoot).Path.Replace('\','/')
$nfxOutputPath=[IO.Path]::GetFullPath($PrivateOutputDirectory)
if (Test-Path -LiteralPath $nfxOutputPath) { throw 'Use a fresh task-private output directory; existing files will not be overwritten.' }
if ($nfxOutputPath -notmatch '^C:\\tmp\\nfx-native-[a-zA-Z0-9_-]+$') { throw 'Private output must be a distinctly named C:\tmp\nfx-native-* directory.' }
if ($nfxOutputPath.Contains("'") -or $nfxReviewedRoot.Contains('"') -or $nfxRecipeFolder.Contains('"')) { throw 'Unsupported path quoting.' }
New-Item -ItemType Directory -Path $nfxOutputPath | Out-Null
$nfxOutput=$nfxOutputPath.Replace('\','/')
$nfxBridge="$nfxRecipeFolder/native-pure-bridge.mjs"
$nfxLines=[Collections.Generic.List[string]]::new()
$nfxLines.Add('-- SOURCE ONLY: Root owns the one existing BEGIN/ROLLBACK and the separate SEND_7 turn connection.')
$nfxLines.Add('-- Do not execute as standalone autocommit SQL. Install reviewed exact prerequisite owners within that same outer transaction first.')
$nfxLines.Add('\set ON_ERROR_STOP on')
$nfxLines.Add("select set_config('cloudtms.nfx.agency_id','$ReviewedAgencyId',true),set_config('cloudtms.nfx.agency_anchor_group_id','$ReviewedAgencyAnchorGroupId',true);")
$nfxLines.Add("\ir '$nfxBatchFolder/01_current_null_setup.sqlinc'")
$nfxLines.Add("\ir '$nfxBatchFolder/10_current_rule_save_assign_off.sqlinc'")
$nfxLines.Add('\pset format unaligned')
$nfxLines.Add('\pset tuples_only on')
$nfxPhases=if($StopAfterFin){@('stage','projection')}else{@('stage','projection','pay_snapshot')}
foreach ($nfxPhase in $nfxPhases) {
 $nfxContextFile="$nfxOutput/$nfxPhase-context.json"
 $nfxGeneratedFile="$nfxOutput/$nfxPhase-produced.sql"
 $nfxInput= switch ($nfxPhase) {'stage' {'02_actual_stage_context.sqlinc'} 'projection' {'03_actual_projection_context.sqlinc'} 'pay_snapshot' {'05_actual_pay_snapshot_context.sqlinc'}}
 if($nfxPhase -eq 'pay_snapshot') {$nfxLines.Add("\ir '$nfxBatchFolder/04_prepare_and_fin.sqlinc'")}
 $nfxLines.Add("\o '$nfxContextFile'")
 $nfxLines.Add("\ir '$nfxBatchFolder/$nfxInput'")
 $nfxLines.Add('\o')
 if($nfxPhase -eq 'pay_snapshot') {
  $nfxResultFile="$nfxOutput/pay-calculation.json"
  $nfxApplyContext="$nfxOutput/pay-apply-context.json"
  $nfxLines.Add("\! node `"$nfxBridge`" `"$nfxReviewedRoot`" `"$nfxContextFile`" `"$nfxResultFile`"")
  $nfxLines.Add("\! node `"$nfxRecipeFolder/merge-pay-context.mjs`" `"$nfxContextFile`" `"$nfxResultFile`" `"$nfxApplyContext`"")
  $nfxLines.Add("\! node `"$nfxBridge`" `"$nfxReviewedRoot`" `"$nfxApplyContext`" `"$nfxGeneratedFile`"")
 } else {$nfxLines.Add("\! node `"$nfxBridge`" `"$nfxReviewedRoot`" `"$nfxContextFile`" `"$nfxGeneratedFile`"")}
 # Missing output/producer failure fails the subsequent exact include. Never
 # continue with an old output from another phase or transaction.
 $nfxLines.Add("\ir '$nfxGeneratedFile'")
}
if($StopAfterFin){$nfxLines.Add("\ir '$nfxBatchFolder/04_prepare_and_fin.sqlinc'")}else{
 $nfxLines.Add("\ir '$nfxBatchFolder/07_invoice_admit_issue_model.sqlinc'")
 $nfxLines.Add("\ir '$nfxBatchFolder/08_null_pay_and_invoice_assertions.sqlinc'")
 $nfxLines.Add("\ir '$nfxBatchFolder/11_current_null_and_missing_negatives.sqlinc'")
}
$nfxLines.Add('-- Root now unconditionally ROLLBACKs, then performs fresh fingerprints/09 while still holding its SEND_7 turn, releases the turn and removes only this exact private directory.')
[IO.File]::WriteAllText("$nfxOutputPath\native-current-recipe.psql",($nfxLines -join "`n")+"`n",[Text.UTF8Encoding]::new($false))
Write-Output 'Native psql recipe generated only; no database or network calls were made.'
