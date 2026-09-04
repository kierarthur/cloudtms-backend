import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');
const repeatablePath = 'supabase/repeatable/30082026_1903_candidate_expense_carrier_anchor_route_v1.sql';
const verifierPath = 'supabase/verification/30082026_1910_candidate_expense_carrier_anchor_route_verification.sql';
const duplicateReviewVerifierPath = 'supabase/verification/31082026_0915_candidate_duplicate_expense_review_verification.sql';
const mileageLineTypeVerifierPath = 'tests/04092026_1020_candidate_mileage_line_type_finalisation_verification.sql';
const separationDeliveryPath = 'supabase/repeatable/02092026_1834_candidate_expense_separation_delivery_v1.sql';
const repeatable = read(repeatablePath);
const verifier = read(verifierPath);
const duplicateReviewVerifier = read(duplicateReviewVerifierPath);
const authoriseAuthority = read('supabase/repeatable/14082026_1310_timesheet_processing_status_and_authorise_authority_v1.sql');
const bulkDataset = read('supabase/repeatable/29082026_0326_banking_pay_release_authority_repair_v1.sql');
const broker = read('broker/src/index.js');
const candidateBroker = read('broker/src/candidate-app-backend.js');
const compileFixture = read('tests/fixtures/07082026_2155_candidate_app_local_compile_base.sql');
const release = JSON.parse(read('supabase/release/current-release.json'));
const runtime = read('.github/workflows/candidate-db-runtime.yml');

test('expense workflow creation derives approval route from the worked anchor before carrier admission', () => {
  const anchorResolution = repeatable.indexOf("if nullif(v_payload->>'anchor_timesheet_id','') is not null then");
  const routeResolution = repeatable.indexOf('v_route_authority:=private._candidate_route_family_v1(', anchorResolution);
  const workflowInsert = repeatable.indexOf('insert into public.candidate_submission_workflows(', routeResolution);

  assert.ok(anchorResolution > 0, 'worked-anchor resolution must exist');
  assert.ok(routeResolution > anchorResolution, 'worked anchor must be resolved before route admission');
  assert.ok(workflowInsert > routeResolution, 'route admission must precede workflow creation');
  assert.match(
    repeatable,
    /case when v_workflow_kind='CONTRACT_EXPENSE' then v_anchor_week\.timesheet_id else v_week\.timesheet_id end[\s\S]*case when v_workflow_kind='CONTRACT_EXPENSE' then v_anchor_week\.id else v_week\.id end/i
  );
  assert.match(
    repeatable,
    /v_workflow_kind='CONTRACT_EXPENSE'[\s\S]*CANDIDATE_WORKFLOW_ANCHOR_NOT_WORKED[\s\S]*v_route_authority:=private\._candidate_route_family_v1/i
  );
  assert.match(
    repeatable,
    /v_workflow_kind='CONTRACT_EXPENSE'[\s\S]*route_family'='QR'[\s\S]*v_route:='PAPER'/i
  );
  assert.doesNotMatch(repeatable, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('expense finalisation projects the exact final line type onto new and reused carriers', () => {
  for (const sourcePath of [
    'supabase/repeatable/07082026_2113_candidate_expense_placement_rpcs_v1.sql',
    'supabase/repeatable/26082026_2121_candidate_expense_category_authority_v2.sql',
    'supabase/repeatable/26082026_2225_candidate_expense_finalise_signature_recheck_v3.sql',
    'supabase/repeatable/30082026_2156_candidate_paper_evidence_manifest_labels_v1.sql'
  ]) assert.match(
    read(sourcePath),
    /v_expense_line_type:=case[\s\S]*mileage_units[\s\S]*then 'MILEAGE'[\s\S]*else 'EXPENSES'[\s\S]*'line_type',v_expense_line_type[\s\S]*p_timesheet_patch_json[\s\S]*jsonb_build_object\(\s*'line_type',v_expense_line_type/i,
    `${sourcePath} must retain the pure-Mileage carrier correction`
  );
  assert.match(read(mileageLineTypeVerifierPath), /begin;[\s\S]*mileage_units',25[\s\S]*line_type','EXPENSES'[\s\S]*timesheet_expense_apply_atomic_v1[\s\S]*v_line_type<>'MILEAGE'[\s\S]*rollback;/i);
  const installedSuccessor = read('supabase/repeatable/30082026_2156_candidate_paper_evidence_manifest_labels_v1.sql');
  assert.match(
    installedSuccessor,
    /p_timesheet_create_json[\s\S]*'status','RECEIVED'[\s\S]*'line_type',v_expense_line_type/i,
    'a newly materialised expense carrier must use the Timesheet status enum, not the contract-week status'
  );
  assert.doesNotMatch(
    installedSuccessor,
    /p_timesheet_create_json[\s\S]*?'status','SUBMITTED'[\s\S]*?'line_type',v_expense_line_type/i,
    'SUBMITTED is a contract-week state and is invalid for a Timesheet carrier'
  );
});

test('an additional expense carrier stays expense-only on an import-authoritative Client', () => {
  const separationDelivery = read(separationDeliveryPath);
  const roleClassifier = separationDelivery.match(/if v_protected then v_role:='PROTECTED'[\s\S]*?else v_role:='HOURS_ONLY';\s*end if;/i)?.[0] || '';
  assert.match(
    roleClassifier,
    /line_type in \('EXPENSES','MILEAGE'\)[\s\S]*v_expenses<>0[\s\S]*v_hours=0[\s\S]*v_additional=0[\s\S]*v_role:='EXPENSE_ONLY'/i
  );
  assert.ok(
    roleClassifier.indexOf("v_role:='EXPENSE_ONLY'") < roleClassifier.indexOf("v_import and v_expenses<>0"),
    'the explicit expense-carrier identity must be recognised before an inherited import route is treated as mixed-source data'
  );
});

test('rollback-contained proof reproduces the app carrier-first sequence and protects finance', () => {
  assert.match(verifier, /expense_carrier_resolve_or_create_atomic_v1[\s\S]*candidate_workflow_transition_atomic_v1/i);
  assert.match(verifier, /submission_mode_snapshot='MANUAL'[\s\S]*route','ELECTRONIC'/i);
  assert.match(verifier, /idempotent_replay[\s\S]*count\(\*\)[\s\S]*candidate_submission_workflows/i);
  assert.match(verifier, /v_after_timesheet is distinct from v_before_timesheet/i);
  assert.match(verifier, /v_after_financial is distinct from v_before_financial/i);
  assert.match(verifier, /qr_status,qr_token[\s\S]*'PENDING','carrier-qr-route-token'/i);
  assert.match(
    verifier,
    /creation_identity_json#>>'\{request,initial_route\}'='ELECTRONIC'[\s\S]*creation_identity_json#>>'\{derived,initial_route\}'='PAPER'/i
  );
  assert.match(verifier, /QR-backed expense replay created duplicate state/i);
  assert.match(verifier, /v_qr_after_financial is distinct from v_qr_before_financial/i);
  assert.match(verifier, /begin;[\s\S]*rollback;/i);
});

test('release and Candidate runtime install the successor before executing its first-use verifier', () => {
  assert.match(
    compileFixture,
    /create table public\.contracts[\s\S]*pay_method_snapshot text not null default 'PAYE'/i
  );
  assert.match(
    compileFixture,
    /create view public\.v_timesheets_summary_base[\s\S]*client_requires_hr[\s\S]*hr_validation_required_for_invoice[\s\S]*validation_status/i
  );
  assert.match(
    compileFixture,
    /create table public\.timesheets_financials[\s\S]*basis public\.timesheet_fin_basis_enum/i
  );
  assert.match(
    compileFixture,
    /create or replace function public\._pay_timesheet_rotation_scope\(p_timesheet_ids uuid\[\]\)/i
  );
  assert.match(
    compileFixture,
    /create table public\.timesheet_payment_overrides\s*\(/i
  );
  assert.ok(release.verificationFiles.includes(verifierPath));
  assert.ok(release.newVerificationFiles.includes(verifierPath));
  assert.ok(release.verificationFiles.includes(duplicateReviewVerifierPath));
  assert.ok(release.newVerificationFiles.includes(duplicateReviewVerifierPath));
  assert.ok(release.verificationFiles.includes(mileageLineTypeVerifierPath));
  assert.ok(release.newVerificationFiles.includes(mileageLineTypeVerifierPath));
  assert.match(
    runtime,
    /29082026_0951_candidate_expense_resubmission_anchor_v1\.sql[\s\S]*30082026_1903_candidate_expense_carrier_anchor_route_v1\.sql[\s\S]*30082026_1910_candidate_expense_carrier_anchor_route_verification\.sql/i
  );
  assert.match(runtime, /31082026_0915_candidate_duplicate_expense_review_verification\.sql/i);
  assert.match(runtime, /21072026_1235_00_import_correction_policy_helpers\.sql/i);
});

test('duplicate review is category-specific and excludes the official expense summary', () => {
  assert.match(duplicateReviewVerifier, /information_schema\.columns[\s\S]*column_name='password_hash'/i);
  assert.match(duplicateReviewVerifier, /column_name='requires_hr'[\s\S]*alter table public\.contracts add column requires_hr boolean not null default false/i);
  assert.match(duplicateReviewVerifier, /to_regprocedure\('public\._temp_diag_log\(text,text,text,jsonb\)'\)[\s\S]*create function public\._temp_diag_log[\s\S]*returns void[\s\S]*language plpgsql/i);
  assert.match(duplicateReviewVerifier, /to_regprocedure\('public\.timesheet_lifecycle_signature_v1\(uuid,uuid,boolean\)'\)[\s\S]*create function public\.timesheet_lifecycle_signature_v1[\s\S]*returns jsonb[\s\S]*verification:/i);
  assert.match(duplicateReviewVerifier, /to_regclass\('public\.timesheet_validations'\)[\s\S]*create table public\.timesheet_validations[\s\S]*pre_validated boolean not null default false/i);
  assert.doesNotMatch(duplicateReviewVerifier, /create function public\._ctms_import_correction_classify_v1/i);
  assert.match(duplicateReviewVerifier, /insert into public\.tms_users\(id,email,is_active\)/i);
  assert.match(repeatable, /_expense_duplicate_review_v1[\s\S]*'MILEAGE','TRAVEL','ACCOMMODATION','OTHER'/i);
  assert.match(repeatable, /component\.component_kind in \('MILEAGE_FORM','EXPENSE_EVIDENCE'\)/i);
  assert.doesNotMatch(
    repeatable.match(/prior_component_claims as \([\s\S]*?\), prior_financial_claims as \(/i)?.[0] || '',
    /EXPENSE_SUMMARY/i
  );
  assert.match(repeatable, /CANDIDATE_DUPLICATE_EXPENSE_CONFIRMATION_REQUIRED/i);
  assert.match(repeatable, /confirmation_digest/i);
  assert.match(repeatable, /DUPLICATE_EXPENSE_REVIEW[\s\S]*DUPLICATE_EXPENSE_/i);
  assert.match(candidateBroker, /CANDIDATE_DUPLICATE_EXPENSE_CONFIRMATION_REQUIRED/i);
  assert.match(candidateBroker, /duplicate_expense_confirmation/i);
});

test('Office authorisation requires deliberate review and bulk authorisation excludes the claim', () => {
  assert.match(authoriseAuthority, /_timesheet_duplicate_expense_review_v1[\s\S]*DUPLICATE_EXPENSE_REVIEW_REQUIRED/i);
  assert.match(authoriseAuthority, /timesheet_authorise_reviewed_atomic[\s\S]*duplicate_expense_reviewed/i);
  assert.match(authoriseAuthority, /timesheet_authorise_bulk_work[\s\S]*DUPLICATE_EXPENSE_REVIEW_REQUIRED/i);
  assert.match(bulkDataset, /DUPLICATE_EXPENSE_REVIEW[\s\S]*can_bulk_authorise_calc/i);
  assert.match(bulkDataset, /processed_review_required/i);
  assert.match(broker, /duplicate_expense_confirmation[\s\S]*timesheet_authorise_reviewed_atomic/i);
  assert.match(broker, /DUPLICATE_EXPENSE_REVIEW_REQUIRED[\s\S]*duplicate_expense_confirmation_required/i);
});
