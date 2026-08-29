param(
  [string]$Container = '',
  [int]$Port = 55438,
  [switch]$FocusedWithdrawalProof
)

$ErrorActionPreference = 'Stop'
$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$repo = Split-Path -Parent $PSScriptRoot

function Invoke-CandidateSqlFile([string]$RelativePath) {
  $full = Join-Path $repo $RelativePath
  if (-not (Test-Path -LiteralPath $full)) { throw "Missing SQL file: $RelativePath" }
  if ($Container) {
    Get-Content -LiteralPath $full -Raw | docker exec -i $Container `
      psql -X -h 127.0.0.1 -U postgres -d postgres -v ON_ERROR_STOP=1 -q
  } else {
    Get-Content -LiteralPath $full -Raw | psql -X -h 127.0.0.1 -p $Port `
      -U postgres -d postgres -v ON_ERROR_STOP=1 -q
  }
  if ($LASTEXITCODE -ne 0) { throw "SQL failed: $RelativePath" }
}

$installFiles = @(
  'tests/fixtures/07082026_2155_candidate_app_local_compile_base.sql',
  'supabase/migrations/07082026_2055_candidate_app_foundation_schema.sql',
  'supabase/migrations/07082026_2055_candidate_app_timesheet_evidence_integrity.sql',
  'supabase/migrations/07082026_2306_candidate_app_manager_review_documents.sql',
  'supabase/migrations/11082026_1708_candidate_workflow_replacement_lineage.sql',
  'supabase/migrations/11082026_2112_candidate_workflow_creation_identity.sql',
  'supabase/migrations/11082026_2254_candidate_workflow_mutation_receipt_index.sql',
  'supabase/migrations/12082026_1355_candidate_auth_mutation_receipt_index.sql',
  'supabase/migrations/17082026_0010_candidate_daily_phase2_authority_schema.sql',
  'supabase/migrations/18082026_0802_candidate_daily_identity_integrity.sql',
  'supabase/migrations/23082026_0116_candidate_manager_email_route_receipts.sql',
  'supabase/repeatable/07082026_2059_candidate_app_private_helpers_v1.sql',
  'supabase/repeatable/25082026_1529_candidate_signature_evidence_timestamp_compatibility_v1.sql',
  'supabase/repeatable/07082026_2103_candidate_auth_rpcs_v1.sql',
  'supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql',
  'supabase/repeatable/18082026_0131_candidate_daily_first_generation_source_link_v1.sql',
  'supabase/repeatable/18082026_1051_candidate_daily_authority_transition_source_identity_v1.sql',
  'supabase/repeatable/07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql',
  'supabase/repeatable/07082026_2113_candidate_expense_placement_rpcs_v1.sql',
  'supabase/repeatable/23082026_0117_candidate_manager_email_authority_v1.sql',
  'supabase/repeatable/27082026_1255_candidate_weekly_withdrawal_reset_v1.sql',
  'supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql',
  'supabase/repeatable/07082026_2128_candidate_finalize_reject_no_work_rpcs_v1.sql',
  'supabase/repeatable/23072026_2207_email_outbox_claim_ready_batch.sql',
  'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/24072026_1217_invoice_work_complete_batch.sql',
  'supabase/repeatable/07082026_2224_candidate_app_weekly_office_replacements_v1.sql',
  'supabase/repeatable/07082026_2225_candidate_app_qr_settings_invoice_replacements_v1.sql',
  'supabase/repeatable/07082026_2310_candidate_manager_review_helpers_v1.sql',
  'supabase/repeatable/08082026_2035_timesheet_route_version_rotate.sql',
  'supabase/repeatable/11082026_1832_cloudtms_office_candidate_adapter_v1.sql',
  'supabase/repeatable/14082026_1310_timesheet_processing_status_and_authorise_authority_v1.sql',
  'supabase/repeatable/27082026_0244_candidate_processed_action_projection_v1.sql',
  'supabase/repeatable/27082026_0423_candidate_electronic_rejection_resubmission_v1.sql'
)

$suites = @(
  'tests/07082026_2155_candidate_app_local_runtime_verification.sql',
  'tests/07082026_2333_candidate_app_manager_review_handover_verification.sql',
  'tests/08082026_1040_candidate_app_policy_corrections_runtime_verification.sql',
  'tests/08082026_1055_candidate_app_claim_concurrency_verification.sql',
  'tests/08082026_1200_candidate_app_expense_workflow_runtime_verification.sql',
  'tests/08082026_1210_candidate_app_flag_acl_runtime_verification.sql',
  'tests/08082026_1220_candidate_app_read_contract_runtime_verification.sql',
  'tests/08082026_1235_candidate_app_authority_corrections_runtime_verification.sql',
  'tests/08082026_1522_candidate_route_daily_runtime_verification.sql',
  'tests/09082026_0100_candidate_qr_version_runtime_verification.sql',
  'tests/09082026_0944_candidate_route_confirmation_runtime_verification.sql',
  'tests/09082026_1211_candidate_route_final_closure_verification.sql',
  'tests/09082026_1329_candidate_route_legacy_signature_policy_verification.sql',
  'tests/10082026_1113_candidate_paper_mail_authority_verification.sql',
  'tests/10082026_1535_candidate_paper_generation_retirement_verification.sql',
  'tests/10082026_1545_candidate_paper_retirement_concurrency_verification.sql',
  'tests/10082026_1651_candidate_digest_and_anchor_rejection_verification.sql',
  'tests/10082026_1817_candidate_finalised_rejection_verification.sql',
  'tests/10082026_2005_candidate_finalised_paper_rejection_verification.sql',
  'tests/10082026_2350_candidate_paper_rejection_lock_order_verification.sql',
  'tests/11082026_0051_candidate_paper_caller_closure_runtime_verification.sql',
  'tests/11082026_0506_candidate_paper_source_owner_selection_verification.sql',
  'tests/11082026_0824_candidate_paper_predelivery_route_guard_verification.sql',
  'tests/11082026_1023_candidate_electronic_route_workflow_guard_verification.sql',
  'tests/11082026_1214_candidate_manager_mail_lifecycle_verification.sql',
  'tests/11082026_1215_candidate_timesheet_current_history_verification.sql',
  'tests/11082026_1405_candidate_manager_action_contract_verification.sql',
  'tests/11082026_1428_candidate_action_hub_closure_verification.sql',
  'tests/11082026_1715_candidate_resubmission_idempotency_verification.sql',
  'tests/11082026_1851_cloudtms_office_candidate_adapter_verification.sql',
  'tests/11082026_2145_candidate_office_rejection_replay_verification.sql',
  'tests/12082026_0059_candidate_execution_replay_paper_authority_verification.sql',
  'tests/12082026_0922_candidate_execution_boundary_closure_verification.sql',
  'tests/12082026_1408_candidate_auth_receipt_runtime_verification.sql',
  'tests/12082026_1859_candidate_public_auth_operation_family_verification.sql',
  'tests/12082026_2146_candidate_public_auth_final_correction_verification.sql',
  'tests/12082026_2223_candidate_public_auth_concurrency_verification.sql',
  'tests/13082026_0002_candidate_auth_mixed_version_concurrency_verification.sql',
  'tests/13082026_0617_candidate_phone_refresh_security_verification.sql',
  'tests/13082026_0927_candidate_session_invalidation_concurrency_verification.sql',
  'tests/14082026_1410_candidate_office_legacy_route_retirement_verification.sql',
  'tests/17082026_0053_candidate_daily_phase2_runtime_verification.sql',
  'tests/17082026_0955_candidate_daily_authority_transition_runtime_verification.sql',
  'tests/18082026_0138_candidate_daily_first_generation_source_link_runtime_verification.sql',
  'tests/18082026_0807_candidate_daily_r16_identity_integrity_runtime_verification.sql',
  'tests/18082026_1105_candidate_daily_r17_authority_transition_source_identity_runtime_verification.sql',
  'supabase/verification/27082026_1327_candidate_daily_withdrawal_reset_verification.sql'
)

foreach ($file in $installFiles) { Invoke-CandidateSqlFile $file }
if ($FocusedWithdrawalProof) {
  Invoke-CandidateSqlFile 'supabase/verification/27082026_1327_candidate_daily_withdrawal_reset_verification.sql'
  if ($Container) {
    $version = docker exec $Container psql -X -U postgres -d postgres -tAc 'show server_version'
  } else {
    $version = psql -X -h 127.0.0.1 -p $Port -U postgres -d postgres -tAc 'show server_version'
  }
  if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL version proof failed' }
  "PostgreSQL=$($version.Trim()) CandidateDailyWithdrawalFirstUse=PASS"
  return
}
foreach ($file in $suites) { Invoke-CandidateSqlFile $file }

$env:CANDIDATE_DAILY_POSTGRES_CHAIN = '1'
$env:CANDIDATE_DAILY_PG_CONTAINER = $Container
$env:CANDIDATE_DAILY_PG_PORT = [string]$Port
$env:CANDIDATE_AUTH_POSTGRES_CHAIN = '1'
$env:CANDIDATE_AUTH_PG_CONTAINER = $Container
$env:CANDIDATE_AUTH_PG_PORT = [string]$Port
Push-Location $repo
try {
  node --test tests/candidate-public-auth-postgres-chain.integration.js
  if ($LASTEXITCODE -ne 0) { throw 'Candidate public-auth PostgreSQL chain failed' }
  node --test --test-concurrency=1 tests/candidate-auth-mixed-version-concurrency.test.js
  if ($LASTEXITCODE -ne 0) { throw 'Candidate mixed-version concurrency failed' }
  node --test tests/candidate-daily-authority-transition-concurrency.integration.js
  if ($LASTEXITCODE -ne 0) { throw 'Candidate Daily authority-transition concurrency failed' }
  node --test tests/candidate-daily-first-generation-source-link-concurrency.integration.js
  if ($LASTEXITCODE -ne 0) { throw 'Candidate Daily first-generation concurrency failed' }
  node --test tests/candidate-daily-r16-identity-integrity-concurrency.integration.js
  if ($LASTEXITCODE -ne 0) { throw 'Candidate Daily R16 identity-integrity concurrency failed' }
  node --test tests/candidate-daily-r17-authority-transition-source-identity-concurrency.integration.js
  if ($LASTEXITCODE -ne 0) { throw 'Candidate Daily R17 authority-transition source-identity concurrency failed' }
} finally {
  Pop-Location
  Remove-Item Env:CANDIDATE_DAILY_POSTGRES_CHAIN -ErrorAction SilentlyContinue
  Remove-Item Env:CANDIDATE_DAILY_PG_CONTAINER -ErrorAction SilentlyContinue
  Remove-Item Env:CANDIDATE_DAILY_PG_PORT -ErrorAction SilentlyContinue
  Remove-Item Env:CANDIDATE_AUTH_POSTGRES_CHAIN -ErrorAction SilentlyContinue
  Remove-Item Env:CANDIDATE_AUTH_PG_CONTAINER -ErrorAction SilentlyContinue
  Remove-Item Env:CANDIDATE_AUTH_PG_PORT -ErrorAction SilentlyContinue
}

if ($Container) {
  $version = docker exec $Container psql -X -U postgres -d postgres -tAc 'show server_version'
} else {
  $version = psql -X -h 127.0.0.1 -p $Port -U postgres -d postgres -tAc 'show server_version'
}
if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL version proof failed' }
"PostgreSQL=$($version.Trim()) CandidateSuites=$($suites.Count) Result=PASS"
