const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');

const residual = read('supabase/repeatable/14082026_1254_pay_workbench_execution_residual_identity_proof_page_v1.sql');
const parent = read('supabase/repeatable/13082026_1245_pay_workbench_execution_refresh_owner_proof_page_v1.sql');
const requestStart = read('supabase/repeatable/04082026_1207_pay_payment_correction_request_start.sql');
const processChunk = read('supabase/repeatable/04082026_1209_pay_payment_correction_process_chunk.sql');
const worker = read('broker/src/index.js');

test('the residual proof is private, bounded and derives Q only from frozen request candidates', () => {
  assert.match(residual, /pay_workbench_execution_residual_identity_proof_page_v1\([\s\S]*p_expected_selected_anchor_digest_by_candidate jsonb,[\s\S]*p_mode text[\s\S]*\)\s*RETURNS jsonb/);
  assert.match(residual, /STABLE[\s\S]*PARALLEL RESTRICTED[\s\S]*SECURITY DEFINER[\s\S]*SET search_path TO ''/);
  assert.match(residual, /request_candidate\.pay_batch_item_ids/);
  assert.match(residual, /pay_payment_correction_request_candidates AS request_candidate/);
  assert.match(residual, /WHERE NOT EXISTS \([\s\S]*batch_candidate\.candidate_id=supplied\.candidate_id/);
  assert.match(residual, /v_mode NOT IN \('PRE_REQUEST_START','ROUTE_REPLAY'\)/);
  assert.match(residual, /v_supplied_candidate_count NOT BETWEEN 1 AND 100/);
  assert.match(residual, /expected\.key~'\^\[0-9a-f\]\{8\}-\[0-9a-f\]\{4\}-\[0-9a-f\]\{4\}-\[0-9a-f\]\{4\}-\[0-9a-f\]\{12\}\$'/);
  assert.match(residual, /terminal_source_change_seq',''\)!~'\^\[0-9\]\{1,18\}\$'/);
  assert.match(residual, /semantic_ready',''\)\)[\s\S]*NOT IN \('true','t','1','yes','y','on'\)/);
  assert.doesNotMatch(residual, /current.*pay_batch_item_ids/i);
});

test('the residual identity is a closed F minus A equals C equals P multiset proof', () => {
  for (const token of [
    'v_f_minus_partition', 'v_partition_minus_f',
    'v_e_minus_c', 'v_c_minus_e',
    'v_e_minus_p', 'v_p_minus_e',
    'v_c_minus_p', 'v_p_minus_c'
  ]) assert.match(residual, new RegExp(token));
  assert.ok((residual.match(/EXCEPT ALL/g) || []).length >= 8);
  assert.match(residual, /EXECUTION_RESIDUAL_IDENTITY_MISMATCH/);
  assert.match(residual, /EXECUTION_RESIDUAL_SELECTED_ITEM_ALLOCATION_MISSING/);
  assert.match(residual, /EXECUTION_RESIDUAL_AFFECTED_SCOPE_UNPROVED/);
  assert.match(residual, /EXECUTION_RESIDUAL_V2_TRANSITION_CONTEXT_MISSING/);
  assert.match(residual, /EXECUTION_RESIDUAL_AFFECTED_SCOPE_AMBIGUOUS/);
  assert.match(residual, /execution_overlay_schedule_contexts/);
  assert.match(residual, /execution_overlay_contexts/);
  assert.match(residual, /chain_context_digests/);
  assert.match(residual, /transition_context_digest/);
  assert.match(residual, /SELECTED_FROZEN_RECOVERY_ANCHOR/);
  assert.match(residual, /SELECTED_RECOVERY_TIMESHEET_PARENT/);
  assert.doesNotMatch(residual, /missing_anchors/);
  assert.match(residual, /provider_attempt_count/);
  assert.match(residual, /rail_transaction_count/);
  assert.match(residual, /settlement_count/);
  assert.match(residual, /remittance_count/);
});

test('request start persists Q-bound authority after reauth checks and before REQUEST_START mutation', () => {
  const proof = requestStart.indexOf("'cancellation_reversion_q_bound_pre_request_start_v1'");
  const lifecycle = requestStart.indexOf("IF v_command IN ('START_AUTO', 'START_PREPARED') THEN");
  assert.ok(proof > 0 && lifecycle > proof);
  assert.match(requestStart, /boundary','AFTER_REQUEST_PREPARE_BEFORE_REQUEST_START'/);
  assert.match(requestStart, /CANCELLATION_REVERSION_START_AUTHORITY_V3/);
  assert.match(requestStart, /request_owned_scope_change_tx_token/);
  assert.match(requestStart, /pg_catalog\.min\(id::text\)::uuid/);
  assert.doesNotMatch(requestStart, /pg_catalog\.min\(id\)::uuid/);
  assert.match(requestStart, /execution_unsent_overlay_chain_v2,execution_operation_id/);
  assert.match(requestStart, /execution_unsent_overlay_chain_v2,pay_batch_id/);
  assert.match(requestStart, /EXECUTION_RESIDUAL_REQUEST_SCOPE_EXCEEDS_BOUND/);
  assert.match(requestStart, /v_q_bound_candidate_distinct_count=pg_catalog\.cardinality\(v_q_bound_candidate_ids\)/);
  assert.match(requestStart, /EXECUTION_RESIDUAL_REQUEST_CANDIDATE_SET_AMBIGUOUS/);
});

test('one parent owns PRE_REQUEST_START and ROUTE_REPLAY and binds child proof digest', () => {
  assert.match(parent, /v_mode NOT IN \('OBSERVE_ONLY','PRE_REQUEST','ROUTE_REPLAY'\)/);
  assert.match(parent, /pay_workbench_execution_residual_identity_proof_page_v1/);
  assert.match(parent, /CASE WHEN v_mode='ROUTE_REPLAY' THEN 'ROUTE_REPLAY' ELSE 'PRE_REQUEST_START' END/);
  assert.match(parent, /expected_parent_proof_digest_by_candidate/);
  assert.match(parent, /v_mode='PRE_REQUEST'[\s\S]*jsonb_object_keys[\s\S]*expected_parent_proof_digest_by_candidate/);
  assert.match(parent, /v_mode='ROUTE_REPLAY'[\s\S]*expected\.value!~'\^\[0-9a-f\]\{64\}\$'/);
  assert.match(parent, /'residual_proof_digest',residual_proof_digest/);
  assert.match(parent, /EXECUTION_REFRESH_OWNER_PROOF_V2/);
  assert.match(parent, /CASE WHEN v_options_version='2'[\s\S]*EXECUTION_REFRESH_OWNER_PROOF_V1/);
  assert.match(parent, /pg_catalog\.concat_ws\('\|','EXECUTION_REFRESH_OWNER_PROOF_V1'/);
});

test('route input replay binds all frozen authority components before election', () => {
  const replay = processChunk.indexOf("'ROUTE_REPLAY'");
  const routeInput = processChunk.indexOf("'contract_version','CANCELLATION_ROUTE_INPUT_V1'", replay);
  const admission = processChunk.indexOf('pay_workbench_cancel_reversion_admission_page_v1', routeInput);
  assert.ok(replay > 0 && routeInput > replay && admission > routeInput);
  for (const key of [
    'authority_components_version', 'request_selection_hash', 'request_plan_hash',
    'pre_request_authority_digest_by_candidate', 'pre_request_authority_set_digest',
    'selected_anchor_digest_by_candidate', 'affected_physical_closure_digest_by_candidate',
    'residual_proof_digest_by_candidate', 'parent_proof_digest_by_candidate'
  ]) assert.match(processChunk, new RegExp(`'${key}'`));
  assert.match(processChunk, /POST_FINANCIAL_TERMINAL_AUTHORITY_CHANGED/);
  assert.match(processChunk, /Q_BOUND_RESIDUAL_ROUTE_REPLAY_V1/);
  assert.match(processChunk, /authority_set_digest_recomputed/);
  assert.match(processChunk, /candidate_count'\)::integer,0\)[\s\S]*=v_q_bound_route_eligible_candidate_count/);
  assert.match(processChunk, /jsonb_array_length\([\s\S]*candidate_results[\s\S]*=v_q_bound_route_eligible_candidate_count/);
  assert.match(processChunk, /v2_chain_digest',''\)~'\^\[0-9a-f\]\{32\}\$'/);
  assert.doesNotMatch(processChunk, /CANCELLATION_ROUTE_INPUT_V2/);
});

test('mixed-candidate replay is candidate-local while the full authority set stays bound', () => {
  assert.match(processChunk, /v_q_bound_route_eligible_candidate_ids/);
  assert.match(processChunk, /candidate_authorities',v_q_bound_candidate_authorities/);
  assert.match(processChunk, /authority_set_digest',''\)[\s\S]*=v_q_bound_authority_set_digest_recomputed/);
  assert.doesNotMatch(processChunk, /all_admitted'\)::boolean,false\)[\s\S]*v_q_bound_route_candidate_ids/);
  assert.match(processChunk, /v_q_bound_route_replay_complete[\s\S]*replay_rows\.result_json->>'admitted'/);
  assert.match(processChunk, /q_bound_residual_route_replay_complete/);
});

test('FINALISE lock failure is typed but remains unclassified until the TEST contention harness', () => {
  assert.match(processChunk, /PAYMENT_CORRECTION_FINALISE_ERROR_V1/);
  assert.match(processChunk, /FINALISE_CANDIDATE_SERIAL_LOCK/);
  assert.match(processChunk, /'retry_class','UNCLASSIFIED'/);
  assert.match(processChunk, /original_sqlstate',SQLSTATE/);
  assert.match(worker, /BANKING_PAY_PAYMENT_CORRECTION_FINALISE_RETRY_V1_ENABLED/);
  assert.match(worker, /upperText\(envelope\.original_sqlstate\) !== '55P03'/);
  assert.match(worker, /\[1, 2, 5\]\[retryCount\]/);
  assert.match(worker, /payment_correction_finalise_retry_count/);
  assert.match(worker, /WAITING_FINALISE_LOCK/);
  assert.match(worker, /PAYMENT_CORRECTION_FINALISE_SYSTEM_RETRY_EXHAUSTED/);
  assert.match(worker, /runner_state: 'SYSTEM_FAILURE'/);
  assert.match(worker, /requires_user_action: false/);
  assert.match(worker, /review_required: false/);
  assert.match(worker, /advanced: !advanceError[\s\S]*paymentCorrectionFinaliseRetry\.eligible/);
  assert.match(worker, /release_failure_classification:[\s\S]*PAYMENT_CORRECTION_FINALISE_LOCK_RETRY/);
});

test('FINALISE retry rechecks financial terminality, route absence and money-movement fences', () => {
  const evidenceStart = worker.indexOf('const readProviderDispatchEvidenceForClaim = async () =>');
  const evidenceEnd = worker.indexOf('const readOperationDurableStateForClaim = async () =>', evidenceStart);
  assert.ok(evidenceStart > 0 && evidenceEnd > evidenceStart);
  const finaliseEvidenceReader = worker.slice(evidenceStart, evidenceEnd);
  assert.match(finaliseEvidenceReader, /banking_pay_operation_settlement_scope/);
  assert.match(finaliseEvidenceReader, /banking_pay_operation_remittance_scope/);
  assert.match(worker, /workCounts\.terminal === workCounts\.total/);
  assert.match(worker, /workCounts\.nonterminal === 0/);
  assert.match(worker, /execution_commit_state === 'NOT_SUBMITTED'/);
  assert.match(worker, /route_chunk_count === 0/);
  assert.match(worker, /route_input_count === 0/);
  assert.match(worker, /provider_call_started !== true/);
  assert.match(worker, /rail_transaction_present !== true/);
  assert.match(worker, /settlement_scope_count === 0/);
  assert.match(worker, /remittance_scope_count === 0/);
});

test('Policy X and proven owners remain outside this correction', () => {
  const changedOwners = [residual, parent, requestStart, processChunk].join('\n');
  assert.doesNotMatch(changedOwners, /CREATE OR REPLACE FUNCTION private\.pay_workbench_unit_economic_occurrence_page_v1/);
  assert.doesNotMatch(changedOwners, /CREATE OR REPLACE FUNCTION private\.pay_workbench_sealed_rate_component_projection_v1/);
  assert.doesNotMatch(changedOwners, /CREATE OR REPLACE FUNCTION private\.pay_sync_overpayments_from_workbench_workspace_v1/);
  assert.doesNotMatch(changedOwners, /CREATE OR REPLACE FUNCTION private\.pay_workbench_cancel_reversion_proof_core_v1/);
  assert.doesNotMatch(changedOwners, /CREATE OR REPLACE FUNCTION private\.pay_workbench_unsent_execution_overlay_proof_page_v1/);
});
