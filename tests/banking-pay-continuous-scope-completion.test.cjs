const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const runtime = fs.readFileSync(path.join(root, 'supabase/repeatable/31072026_2350_banking_pay_continuous_scope_runtime.sql'), 'utf8');
const functionsSql = fs.readFileSync(path.join(root, 'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'), 'utf8');
const draftSql = fs.readFileSync(path.join(root, 'supabase/repeatable/21072026_1235_46_pay_workbench_prepare_draft_scope_seed.sql'), 'utf8');
const migration = fs.readFileSync(path.join(root, 'supabase/migrations/01082026_0145_banking_pay_continuous_scope_completion.sql'), 'utf8');
const worker = fs.readFileSync(path.join(root, 'broker/src/index.js'), 'utf8');

function lastFunction(source, name) {
  const marker = `CREATE OR REPLACE FUNCTION public.${name}(`;
  const start = source.lastIndexOf(marker);
  assert.ok(start >= 0, `${name} must exist`);
  const end = source.indexOf('$function$;', start);
  assert.ok(end > start, `${name} must terminate`);
  return source.slice(start, end + '$function$;'.length);
}

test('generated blockers are recovery aware only inside the requested generation prefix', () => {
  const body = lastFunction(runtime, 'pay_workbench_scope_blocker_state_v1');
  assert.match(body, /scope_change_generation <= p_target_generation/);
  assert.match(body, /status IN \('QUEUED', 'RUNNING', 'FAILED', 'DEAD', 'SUCCEEDED'\)/);
  assert.match(body, /PARTITION BY generated_classified\.work_identity[\s\S]*scope_change_generation DESC/);
  assert.match(body, /authority_ordinal = 1/);
  assert.match(body, /status IN \('FAILED', 'DEAD'\)/);
  assert.doesNotMatch(body, /scope_change_generation IS NULL[\s\S]*status IN/);
});

test('blockers distinguish current failure authority from superseded execution history', () => {
  const body = lastFunction(runtime, 'pay_workbench_scope_blocker_state_v1');
  assert.match(body, /non_blocking_terminal_failure/);
  assert.match(body, /WORKBENCH_FINANCE_DIRTY_SUPERSEDED_BY_CANCEL_FULL_CANDIDATE_REFRESH/);
  assert.match(body, /superseding_session_id'[\s\S]*p_session_id::text/);
  assert.match(body, /refresh_scope_kind'[\s\S]*CANDIDATE_FULL_LIVE/);
  assert.match(body, /full_candidate_recovery_reallocation_required/);
  assert.match(body, /effective_status IN \('FAILED', 'DEAD'\)/);
  assert.match(body, /Session job rows are immutable execution history/);
  assert.match(body, /session_job\.status IN \('QUEUED', 'RUNNING'\)/);
  assert.match(body, /session_scope_failed_count/);
  assert.match(body, /session_line_failed_count/);
});

test('coordinator promotes targets and implements a genuinely non-authoritative shadow pass', () => {
  const body = lastFunction(runtime, 'pay_workbench_scope_reconcile_drain_one_v1');
  assert.match(body, /v_target := GREATEST\(v_target, v_current_generation\)/);
  assert.match(body, /scope_change_generation_shadow_checked/);
  assert.match(body, /IF v_mode = 'AUTHORITATIVE' AND v_would_admit > 0 THEN/);
  assert.match(body, /IF v_mode = 'SHADOW' THEN[\s\S]*scope_change_generation_shadow_checked = v_target/);
  assert.match(body, /pay_workbench_scope_blocker_state_v1\(v_session\.id, v_target/);
  assert.match(body, /UPSTREAM_SCOPE_FAILURE_UNRESOLVED/);
  assert.match(body, /previously observed blocker may have recovered[\s\S]*last_error_json = NULL::jsonb/);
});

test('admission is bounded, empty-match-none and output-isolated', () => {
  const preview = lastFunction(functionsSql, 'pay_preview_build_context');
  const admission = lastFunction(runtime, 'pay_workbench_scope_admission_candidates_v1');
  assert.match(preview, /v_scope_admission_mode boolean := false/);
  assert.match(preview, /PAY_WORKBENCH_SCOPE_ADMISSION_NOT_AUTHORISED/);
  assert.match(preview, /v_scope_admission_mode IS NOT TRUE[\s\S]*candidate_scope_row\.id = ANY/);
  assert.match(admission, /array_length\(v_input_candidate_ids, 1\), 0\) = 0/);
  assert.match(admission, /'shortlist_empty', true/);
  assert.match(admission, /PAY_WORKBENCH_SCOPE_ADMISSION_OUTPUT_OUTSIDE_SHORTLIST/);
  assert.match(admission, /p_target_generation > v_current_generation/);
});

test('draft creation consumers independently enforce frozen Policy X provenance', () => {
  const chunks = lastFunction(functionsSql, 'banking_pay_operation_seed_chunks');
  const finish = lastFunction(functionsSql, 'banking_pay_operation_finish');
  const shell = lastFunction(functionsSql, 'pay_batch_shell_ensure_from_operation');
  const integrity = lastFunction(functionsSql, 'pay_batch_assert_integrity');
  assert.match(draftSql, /PAY_WORKBENCH_SCOPE_RECONCILIATION_SHADOW_MODE/);
  assert.match(draftSql, /pay_workbench_scope_blocker_state_v1/);
  assert.match(chunks, /DRAFT_CREATE_OPERATION_SCOPE_NOT_FROZEN/);
  assert.match(chunks, /DRAFT_CREATE_FROZEN_SCOPE_PROVENANCE_MISMATCH/);
  assert.match(finish, /DRAFT_CREATE_OPERATION_CHUNKS_INCOMPLETE/);
  assert.match(finish, /PENDING_SCOPE_CHANGE_RELEVANCE_FAILED/);
  assert.match(shell, /pay_workbench_scope_blocker_state_v1/);
  assert.match(integrity, /DRAFT_CREATE_OPERATION_BATCH_PROVENANCE_MISMATCH/);
  assert.match(integrity, /batch_scope_change_generation/);
});

test('natural expiry metadata is no-dirty and due work is durable and fair', () => {
  const dirty = lastFunction(functionsSql, 'pay_workbench_mark_candidate_dirty');
  const expiry = lastFunction(runtime, 'pay_workbench_enqueue_expired_snooze_refreshes_v1');
  for (const field of [
    'natural_expiry_source_fingerprint',
    'natural_expiry_checked_fingerprint',
    'natural_expiry_checked_at_utc',
    'natural_expiry_state_changed',
    'natural_expiry_result_code'
  ]) assert.match(dirty, new RegExp(field));
  assert.match(expiry, /natural_expiry_checked_fingerprint[\s\S]*IS DISTINCT FROM snooze_row\.natural_expiry_source_fingerprint/);
  assert.match(expiry, /FOR UPDATE SKIP LOCKED[\s\S]*LIMIT v_limit/);
  assert.match(expiry, /transition_failed_count/);
  assert.match(runtime, /idx_pay_item_snoozes_unchecked_natural_expiry/);
});

test('completion remains on the existing queue and aggregate Worker RPC', () => {
  assert.match(migration, /scope_change_generation_shadow_checked bigint NOT NULL DEFAULT 0/);
  assert.match(migration, /natural_expiry_source_fingerprint text/);
  assert.match(worker, /pay_workbench_worker_drain_chunk_revalidated_v1/);
  assert.doesNotMatch(worker, /pay_workbench_scope_reconcile_drain_one_v1/);
  assert.doesNotMatch(runtime, /GRANT EXECUTE[\s\S]*TO anon/);
});
