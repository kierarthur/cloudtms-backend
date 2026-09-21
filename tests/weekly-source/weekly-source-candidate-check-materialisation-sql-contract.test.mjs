import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8')
  .replaceAll('\r\n', '\n');

const owner = read('supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql');
const verifier = read('supabase/verification/15092026_2203_weekly_source_candidate_app_contract_v1.sql');

const functionSource = name => {
  const marker = `create or replace function ${name}`;
  const start = owner.toLowerCase().indexOf(marker.toLowerCase());
  assert.notEqual(start, -1, `${name} is missing`);
  const end = owner.indexOf('$function$;', start);
  assert.notEqual(end, -1, `${name} has no closing function delimiter`);
  return owner.slice(start, end + '$function$;'.length);
};

const materialise = functionSource('public.weekly_source_candidate_check_materialise_atomic_v1');
const compareSync = functionSource('private.weekly_source_candidate_submission_compare_sync_v1');
const submit = functionSource('public.weekly_source_candidate_app_submit_atomic_v1');

const position = (source, value, label) => {
  const found = source.indexOf(value);
  assert.notEqual(found, -1, `${label} is missing`);
  return found;
};

test('the Candidate materialisation seam is service-only and its internal comparison core is private', () => {
  assert.match(owner, /revoke all on function public\.weekly_source_candidate_check_materialise_atomic_v1\(jsonb,timestamptz\) from public,anon,authenticated;/i);
  assert.match(owner, /grant execute on function public\.weekly_source_candidate_check_materialise_atomic_v1\(jsonb,timestamptz\) to service_role;/i);
  assert.match(owner, /revoke all on function private\.weekly_source_candidate_submission_compare_sync_v1\(uuid,uuid,uuid,uuid,bytea,timestamptz\) from public,anon,authenticated,service_role;/i);
  assert.match(materialise, /weekly_source_query_require_service_v1\(\)/i);
  assert.match(compareSync, /weekly_source_query_require_service_v1\(\)/i);
});

test('only the Plan 6 whole-week submit path invokes the narrow owner', () => {
  assert.equal(
    (submit.match(/weekly_source_candidate_check_materialise_atomic_v1\s*\(/gi) || []).length,
    1,
  );
  assert.doesNotMatch(submit, /candidate_workflow_transition_atomic_v1/i);
  assert.match(submit, /v_scope->>'request_kind'='CHECK_HOURS'[\s\S]*weekly_source_candidate_app_assert_week_revision_v1/i);
  assert.match(submit, /else[\s\S]*weekly_source_candidate_app_assert_new_week_submission_v1/i);
  assert.match(submit, /'request_kind',v_scope->>'request_kind'/i);
  assert.doesNotMatch(materialise, /weekly_source_query_sync_atomic_v1|weekly_source_office_authority_v1|RECHECK_SOURCE/i);
});

test('the save is bound to the exact account, environment, request generation, publication and scope', () => {
  const workflowLock = position(materialise, 'WEEKLY_SOURCE_CANDIDATE_MATERIALISE:', 'workflow advisory lock');
  const workflowRowLock = position(materialise, 'where id=v_workflow_id for update', 'workflow row lock');
  const currentPublication = position(materialise, 'weekly_source_query_current_publication_v1', 'current publication guard');
  const projection = position(materialise, 'weekly_source_candidate_app_projection_v1', 'current Candidate projection');
  const scopeFingerprint = position(materialise, "p_request->>'scope_fingerprint' is distinct from v_scope->>'scope_fingerprint'", 'scope fingerprint guard');
  assert.ok(workflowLock < workflowRowLock);
  assert.ok(workflowRowLock < currentPublication);
  assert.ok(currentPublication < projection);
  assert.ok(projection < scopeFingerprint);
  assert.match(materialise, /environment=v_workflow\.environment and status='ACTIVE'/i);
  assert.match(materialise, /weekly_source_groups[\s\S]*environment=v_workflow\.environment/i);
  assert.match(materialise, /request_kind=v_kind and state='ACTIVE' for update/i);
  assert.match(materialise, /v_workflow\.generation<>v_expected_generation[\s\S]*v_workflow\.state<>'WORKER_DRAFT'/i);
  assert.match(materialise, /v_scope->>'request_kind' is distinct from v_kind/i);
});

test('the owner accepts only a signed immutable hours week and refuses locked financial history', () => {
  assert.match(materialise, /weekly_source_candidate_app_assert_hours_only_v1/i);
  assert.match(materialise, /component_kind='CANDIDATE_SIGNATURE'[\s\S]*document_role='CANDIDATE_SIGNATURE'[\s\S]*state='IMMUTABLE'/i);
  assert.match(materialise, /octet_length\(source_content_sha256\)=32/i);
  assert.match(materialise, /sheet_scope<>'WEEKLY'[\s\S]*line_type<>'HOURS'/i);
  assert.match(materialise, /authorised_at_server is not null[\s\S]*WEEKLY_SOURCE_CANDIDATE_TIMESHEET_LOCKED/i);
  assert.match(materialise, /exists\(select 1 from public\.timesheets_financials financial[\s\S]*where financial\.timesheet_id=v_timesheet\.timesheet_id\)/i);
  assert.match(materialise, /exists\(select 1 from public\.invoice_lines invoice_line[\s\S]*where invoice_line\.timesheet_id=v_timesheet\.timesheet_id\)/i);
  assert.match(materialise, /exists\(select 1 from public\.timesheet_pay_state pay_state[\s\S]*where pay_state\.timesheet_id=v_timesheet\.timesheet_id\)/i);
  assert.match(materialise, /exists\(select 1 from public\.pay_batch_items batch_item[\s\S]*where batch_item\.timesheet_id=v_timesheet\.timesheet_id\)/i);
  assert.match(materialise, /exists\(select 1 from public\.pay_advances finance_case[\s\S]*where finance_case\.linked_timesheet_id=v_timesheet\.timesheet_id\)/i);
  assert.match(materialise, /WEEKLY_SOURCE_CANDIDATE_TIMESHEET_FINANCIAL_STATE_EXISTS/i);
  assert.match(materialise, /WEEKLY_SOURCE_CANDIDATE_WEEK_OVERLAP/i);
});

test('the saved row remains ordinary unapproved Candidate evidence with no manager or financial finalisation', () => {
  assert.match(materialise, /'RECEIVED','WEEKLY','MANUAL','HOURS'/i);
  assert.match(materialise, /submission_mode_snapshot='ELECTRONIC'/i);
  assert.match(materialise, /state='WORKER_SUBMITTED'/i);
  assert.match(materialise, /manager_name=null,manager_position=null,manager_signature_component_id=null/i);
  assert.match(materialise, /review_render_state,final_signed_render_state[\s\S]*'NOT_REQUIRED','NOT_REQUIRED'/i);
  assert.match(materialise, /canonical_save_financials_id=null/i);
  assert.match(materialise, /'cloudtms\.lifecycle_mutation_context','ordinary_timesheet_save'/i);
  assert.match(materialise, /'cloudtms\.lifecycle_defer_summary_refresh','on'/i);
  assert.doesNotMatch(materialise, /WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT|READY_FOR_MANAGER_APPROVAL|READY_TO_FINALISE/i);
  assert.doesNotMatch(materialise, /insert\s+into\s+public\.(?:timesheets_financials|invoice_lines|pay_[a-z0-9_]+|weekly_source_timesheet_lineage)/i);
  assert.doesNotMatch(materialise, /update\s+public\.(?:timesheets_financials|invoice_lines|pay_[a-z0-9_]+|weekly_source_timesheet_lineage)/i);
  assert.doesNotMatch(materialise, /delete\s+from\s+public\.(?:timesheets_financials|invoice_lines|pay_[a-z0-9_]+|weekly_source_timesheet_lineage)/i);
});

test('late submission compares only after the exact saved Timesheet hash exists and then uses the existing completion owner', () => {
  const save = position(materialise, 'insert into public.timesheets(', 'Timesheet materialisation');
  const signature = position(materialise, 'insert into public.candidate_submission_components(', 'immutable signature copy');
  const hash = position(materialise, 'v_timesheet_hash:=private.weekly_source_query_candidate_timesheet_hash_v1(', 'saved Timesheet hash');
  const compare = position(materialise, 'v_compare:=private.weekly_source_candidate_submission_compare_sync_v1(', 'private comparison sync');
  const complete = position(materialise, 'v_completion:=public.weekly_source_timesheet_submission_complete_atomic_v1(', 'existing completion owner');
  assert.ok(save < signature);
  assert.ok(signature < hash);
  assert.ok(hash < compare);
  assert.ok(compare < complete);
  assert.match(materialise, /'timesheet_revision',v_timesheet\.version[\s\S]*'timesheet_hash',pg_catalog\.encode\(v_timesheet_hash,'hex'\)[\s\S]*'outcome',v_compare->>'outcome'/i);
});

test('the private comparer reuses discrepancy identity while keeping Candidate complete and manager escalation downstream', () => {
  assert.match(compareSync, /where source_group_id=v_group\.id and work_event_id=v_work_event\.id[\s\S]*and state='OPEN'/i);
  assert.match(compareSync, /SOURCE_HOURS_DIFFER/i);
  assert.match(compareSync, /SOURCE_MISSING_OR_NOT_AUTHORISED/i);
  assert.match(compareSync, /HEALTHROSTER_NOT_FINALISED/i);
  assert.match(compareSync, /FULL_NEGATIVE_SOURCE','ZERO_SOURCE/i);
  assert.match(compareSync, /candidate_action_state[\s\S]*'NOT_REQUIRED'/i);
  assert.match(compareSync, /'origin','CANDIDATE_TIMESHEET_SUBMISSION'/i);
  assert.doesNotMatch(compareSync, /actor_user_id|weekly_source_office_authority_v1|weekly_source_query_sync_atomic_v1/i);
  assert.doesNotMatch(compareSync, /weekly_source_query_manager_generation_v1|weekly_source_query_manager_intent_v1|weekly_message_intents/i);
});

test('runtime verification covers exact, mismatch, missing, reversal, stale and replay paths without finance', () => {
  assert.match(verifier, /insert into public\.candidate_app_global_membership_links\([\s\S]*'ACTIVE'/i);
  assert.match(verifier, /create temp table candidate_app_workbench_jobs_before[\s\S]*except[\s\S]*candidate_app_workbench_jobs_before/i);
  for (const marker of [
    'stale late-submit source scope was accepted',
    'stale Candidate Timesheet revision was accepted',
    'service-owned materialisation exact replay was not stable',
    'materialisation idempotency key accepted a different immutable request',
    'exact late Timesheet did not auto-complete without an issue',
    'late Timesheet exact replay was not receipt-first',
    'mismatching late Timesheet did not enter the manager issue lifecycle',
    'Candidate-only late shift did not create a source-missing incident',
    'full-negative source movement was treated as worked Candidate hours',
    'late Candidate evidence created TSFIN/payment economics',
    'late completion did not persist the exact saved Timesheet hash',
    'late Candidate evidence touched Banking Pay or Workbench state',
  ]) {
    assert.match(verifier, new RegExp(marker.replaceAll(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i'));
  }
});
