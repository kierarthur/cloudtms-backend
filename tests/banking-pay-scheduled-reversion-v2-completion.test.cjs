const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const assert = require('node:assert/strict');

const root = path.resolve(__dirname, '..');
const read = (...parts) => fs.readFileSync(path.join(root, ...parts), 'utf8');

const controls = read('supabase', 'migrations',
  '11082026_0706_banking_pay_scheduled_reversion_v2_controls.sql');
const requestStart = read('supabase', 'repeatable',
  '04082026_1207_pay_payment_correction_request_start.sql');
const processChunk = read('supabase', 'repeatable',
  '04082026_1209_pay_payment_correction_process_chunk.sql');
const dirtyRuntime = read('supabase', 'repeatable',
  '07082026_1016_banking_pay_targeted_delta_runtime.sql');
const helpers = read('supabase', 'repeatable',
  '09082026_0712_banking_pay_semantic_ready_helpers.sql');
const cancelSafe = read('supabase', 'repeatable',
  '19072026_1816_cancel_refresh_supersede_finance_dirty.sql');
const statusReader = read('supabase', 'repeatable',
  '04082026_1145_pay_payment_correction_status_get_v1.sql');
const completeJob = read('supabase', 'repeatable',
  '04082026_1219_pay_workbench_complete_job.sql');
const worker = read('broker', 'src', 'index.js');

test('scheduled V2 and held-job absorption install inert behind independent controls', () => {
  for (const setting of [
    'banking_pay_scheduled_cancellation_reversion_v2_observe_enabled',
    'banking_pay_scheduled_cancellation_reversion_v2_publish_enabled',
    'banking_pay_correction_held_dirty_route_absorption_v1_enabled',
  ]) {
    assert.match(controls, new RegExp(`${setting}[\\s\\S]*DEFAULT false`, 'i'));
  }
  assert.match(controls,
    /scheduled_cancellation_reversion_v2_publish_enabled[\s\S]*scheduled_cancellation_reversion_v2_observe_enabled[\s\S]*cancellation_reversion_publish_v1_enabled/i);
  assert.match(requestStart, /scheduled_cancellation_reversion_v2_observe_enabled/);
  assert.match(processChunk, /scheduled_cancellation_reversion_v2_publish_enabled/);
  assert.match(processChunk, /correction_held_dirty_route_absorption_v1_enabled/);
});

test('one shared proof core owns real admission and service-only route truth', () => {
  assert.match(helpers,
    /CREATE OR REPLACE FUNCTION private\.pay_workbench_cancel_reversion_proof_core_v1/);
  assert.match(helpers,
    /PRE_REQUEST.*DRAFT_OVERLAY_PREFLIGHT.*PRE_FINANCIAL.*POST_FINANCIAL.*OBSERVE_ONLY/s);
  assert.match(helpers, /pay_batch_payment_status_page_v1/);
  assert.match(helpers, /CURRENT_CANCELABILITY_AUTHORITY_REJECTED/);
  assert.match(helpers, /invariant_family_mismatch_count/);
  assert.match(helpers,
    /CREATE OR REPLACE FUNCTION private\.pay_workbench_cancel_reversion_admission_page_v1[\s\S]*pay_workbench_cancel_reversion_proof_core_v1/);
  assert.match(helpers,
    /CREATE OR REPLACE FUNCTION public\.pay_payment_cancellation_route_diagnostic_v1/);
  assert.match(helpers,
    /pay_batch_payment_status_page_v1[\s\S]*pay_workbench_cancel_reversion_proof_core_v1/);
  assert.match(helpers, /read_only',true,'mutation_count',0/);
  assert.match(helpers,
    /REVOKE ALL ON FUNCTION public\.pay_payment_cancellation_route_diagnostic_v1[\s\S]*PUBLIC, anon, authenticated/);
  assert.match(helpers,
    /GRANT EXECUTE ON FUNCTION public\.pay_payment_cancellation_route_diagnostic_v1[\s\S]*postgres, service_role/);
  assert.match(helpers,
    /current_build\.id=CASE[\s\S]*certified_preview_publication_attestation_json[\s\S]*economic_build_id/);
  assert.doesNotMatch(helpers,
    /current_build\.source_build_run_id=current_scope\.certified_preview_publication_source_build_run_id/);
});

test('scheduled PREPARE and START capture server-owned V2 authority without changing selection identity', () => {
  assert.match(requestStart,
    /v_action IN \('PRE_BANK_CANCEL','CANCEL_PAYMENT'\)[\s\S]*pay_workbench_cancel_reversion_proof_core_v1[\s\S]*'PRE_REQUEST'/);
  assert.match(requestStart, /CANCELLATION_REVERSION_PRE_REQUEST_AUTHORITY_V2/);
  assert.match(requestStart, /CANCELLATION_REVERSION_START_AUTHORITY_V2/);
  assert.match(requestStart,
    /v_selection - 'command' - 'draft_overlay_fast_pre_request_authorities'[\s\S]*- 'cancellation_reversion_pre_request_authorities_v2'/);
  assert.match(requestStart, /request_owned_dirty_job_id/);
  assert.match(requestStart, /ECONOMIC_AUTHORITY_CHANGED_BEFORE_CANCELLATION_START/);
});

test('financial page persists mutation proof and parks the same causal job for route election', () => {
  assert.match(helpers, /POST_FINANCIAL_CANCELLATION_AUTHORITY_V1/);
  assert.match(helpers,
    /UPDATE public\.pay_payment_correction_work_items AS applied_work[\s\S]*result_json=COALESCE\(applied_work\.result_json,'\{\}'::jsonb\)\|\|v_result/);
  assert.match(helpers, /waiting_for_correction_route_election',true/);
  assert.match(helpers,
    /banking_pay_correction_held_dirty_route_absorption_v1_enabled/);
  assert.match(helpers, /held_job\.id=CASE[\s\S]*request_owned_dirty_job_id/);
});

test('post-commit V3 authority replaces the premature pre-commit terminal claim', () => {
  const awaitingIndex = processChunk.indexOf('POST_FINANCIAL_TERMINAL_AWAITING_COMMIT_V3');
  const refreshIndex = processChunk.indexOf("v_phase = 'REFRESH_WORKBENCH'");
  const deriveIndex = processChunk.indexOf('pay_workbench_correction_post_commit_authority_page_v1');
  const admissionIndex = processChunk.indexOf('pay_workbench_cancel_reversion_admission_page_v1');
  assert.ok(awaitingIndex >= 0);
  assert.ok(refreshIndex > awaitingIndex);
  assert.ok(deriveIndex > refreshIndex);
  assert.ok(admissionIndex > deriveIndex);
  assert.doesNotMatch(processChunk, /POST_FINANCIAL_TERMINAL_AUTHORITY_V2/);
  assert.match(processChunk,
    /cancellation_reversion_post_financial_terminal_awaiting_commit_v3/);
  assert.match(helpers,
    /CREATE OR REPLACE FUNCTION private\.pay_workbench_correction_post_commit_authority_page_v1/);
  assert.match(helpers, /scope_tx\.state='FINALIZED'/i);
  assert.match(helpers, /held_job_generation IS DISTINCT FROM allocated_generation/);
  assert.match(helpers, /ORDER BY held_job\.id[\s\S]*FOR UPDATE OF held_job/);
  assert.match(helpers, /POST_FINANCIAL_TERMINAL_AUTHORITY_V3/);
  assert.match(helpers, /'session_id',p_session_id/);
  assert.match(helpers, /cancellation_reversion_post_commit_authorities_v3/);
  assert.match(processChunk,
    /pay_workbench_correction_held_dirty_job_resolve_v1[\s\S]*INSERT INTO public\.banking_pay_operation_chunks/);
  assert.match(processChunk,
    /'original_source_build_run_id',admitted\.value->>'original_source_build_run_id'[\s\S]{0,180}'replay_source_build_run_id',admitted\.value->>'replay_source_build_run_id'/);
  assert.match(processChunk, /FINANCIAL_COMPLETE_WORKBENCH_PENDING|workbench_refresh_nudge/);
});

test('fallback enqueue proves the exact finalized token/generation or does normal invalidation', () => {
  assert.match(cancelSafe, /POST_FINANCIAL_TERMINAL_AUTHORITY_V3/);
  assert.match(cancelSafe, /bounded_scope_state_precedes_job',v_preceding_scope_proven/);
  assert.match(cancelSafe, /scope_change_tx_token'[\s\S]*scope_change_generation'/);
  assert.doesNotMatch(cancelSafe, /'bounded_scope_state_precedes_job',true/);
});

test('status and retry authority separate financial completion from physical currentness', () => {
  assert.match(statusReader, /pay_workbench_candidate_physical_currentness_page_v1/);
  assert.doesNotMatch(statusReader,
    /source_change_seq\s*>?=\s*candidate_freshness\.job_generation/);
  assert.match(statusReader,
    /v_financial_complete[\s\S]*v_workbench_status='FAILED'[\s\S]*RETRY_PROCESSING/);
  assert.doesNotMatch(worker,
    /result\.processing_retry_available\s*=\s*true/);
  assert.match(worker,
    /PAYMENT_CORRECTION_RETRY_NOT_AVAILABLE/);
});

test('fallback source completion emits one correction-current batch signal', () => {
  assert.match(completeJob, /correction_terminal_current_signal_emitted/);
  assert.match(completeJob, /PAYMENT_CORRECTION_WORKBENCH_CURRENT/);
  assert.match(completeJob, /p_touch_alerts:=false/);
});

test('candidate dirty owner retains the same job through the route-election boundary', () => {
  assert.match(dirtyRuntime, /WAITING_FOR_CORRECTION_ROUTE_ELECTION/);
  assert.match(dirtyRuntime, /FINANCIAL_PAGE_APPLIED.*FINANCIAL_TERMINAL/s);
  assert.match(dirtyRuntime, /source_build_enqueue_skipped_by_request_boundary', true/);
  assert.match(dirtyRuntime, /SET status = 'QUEUED'/);
  assert.doesNotMatch(dirtyRuntime,
    /WAITING_FOR_CORRECTION_ROUTE_ELECTION[\s\S]{0,900}WORKBENCH_CANDIDATE_SOURCE_BUILD/);
});

test('held-job resolver validates current or one active owner and reuses generic completion', () => {
  assert.match(helpers,
    /CREATE OR REPLACE FUNCTION private\.pay_workbench_correction_held_dirty_job_resolve_v1/);
  assert.match(helpers, /CORRECTION_HELD_DIRTY_CAUSAL_MISMATCH/);
  assert.match(helpers, /MATERIALISED','READY','COMPLETE/);
  assert.match(helpers, /CORRECTION_HELD_DIRTY_ROUTE_NOT_ELECTED/);
  assert.match(helpers, /public\.pay_workbench_complete_job/);
  assert.doesNotMatch(helpers, /v_job\.result_json/);
  assert.match(helpers,
    /v_completion:=public\.pay_workbench_complete_job\([\s\S]*'COALESCED_TO_CORRECTION_ROUTE_ELECTION'/);
  assert.doesNotMatch(helpers,
    /pay_workbench_correction_held_dirty_job_resolve_v1[\s\S]*SET status='SUCCEEDED'/);
});

test('Policy X remains explicit and no payment formula owner is introduced', () => {
  for (const source of [requestStart, processChunk, dirtyRuntime, helpers]) {
    assert.match(source, /POST_DRAFT|Policy X|policy_x/i);
  }
  assert.doesNotMatch(helpers, /CREATE OR REPLACE FUNCTION public\.pay_sync_overpayments/);
  assert.doesNotMatch(processChunk, /statement_timeout TO '(?:[7-9]|[1-9][0-9]+)000ms'/);
});
