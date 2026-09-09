import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (...parts) => fs.readFileSync(path.join(root, ...parts), 'utf8');
const hash = (value) => crypto.createHash('sha256').update(value).digest('hex');
const historical = read('supabase', 'repeatable', '04082026_1207_pay_payment_correction_request_start.sql');
const candidate = read('supabase', 'repeatable', '08092026_0510_banking_pay_payment_correction_prepare_idempotency_v1.sql');
const prepare = read('supabase', 'repeatable', '07092026_2013_banking_pay_unpaid_cancellation_communication_v2_prepare_v1.sql');
const verifier = read('supabase', 'verification', '08092026_0511_banking_pay_payment_correction_prepare_idempotency_verification.sql');
const runtime = read('tests', '08092026_0512_banking_pay_payment_correction_prepare_idempotency_runtime_verification.sql');

test('historical owners are immutable and the candidate is one exact later replacement', () => {
  assert.equal(hash(historical), 'e8b6ace569f4d2f9e48b54ba7c88b90d2b4fac47a140d398483c2e1d83080ccb');
  assert.equal(hash(prepare), '3766f3541a90044e8d0797039de9548ac41521cc407e0e27abacb456e3d308cb');
  assert.equal((candidate.match(/CREATE OR REPLACE FUNCTION public\.pay_payment_correction_request_start\(/g) || []).length, 1);
  assert.doesNotMatch(candidate, /\b(?:CREATE|ALTER|DROP)\s+(?:TABLE|INDEX|TYPE|SCHEMA)\b/i);
});

test('candidate preserves signature, security, owner, ACL and exact budgets', () => {
  assert.match(candidate, /public\.pay_payment_correction_request_start\(\s*p_pay_batch_id uuid,\s*p_selection_json jsonb,\s*p_reason text,\s*p_actor_user_id uuid,\s*p_source_bank_event_id uuid DEFAULT NULL::uuid,\s*p_auto_requested boolean DEFAULT false,\s*p_accepted_resolution_json jsonb DEFAULT NULL::jsonb\s*\)/s);
  assert.match(candidate, /LANGUAGE plpgsql\s+VOLATILE\s+SECURITY DEFINER/s);
  assert.match(candidate, /SET search_path TO pg_catalog, private, extensions, pg_temp/);
  assert.match(candidate, /SET statement_timeout TO '6000ms'/);
  assert.match(candidate, /SET lock_timeout TO '1000ms'/);
  assert.match(candidate, /ALTER FUNCTION public\.pay_payment_correction_request_start\(uuid,jsonb,text,uuid,uuid,boolean,jsonb\) OWNER TO postgres/);
  assert.match(candidate, /REVOKE ALL ON FUNCTION[\s\S]*FROM PUBLIC[\s\S]*FROM anon[\s\S]*FROM authenticated[\s\S]*FROM service_role[\s\S]*GRANT EXECUTE[\s\S]*TO service_role/);
});

test('PREPARE replay uses immutable selection and indexed all-status operation identity before freshness', () => {
  const replay = candidate.indexOf('An explicit operation key is the durable serialisation point');
  const batchRead = candidate.indexOf('SELECT batch_row.*', replay);
  const snapshotGate = candidate.indexOf("RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_SNAPSHOT_STALE'", replay);
  assert.ok(replay > 0 && batchRead > replay && snapshotGate > batchRead);
  assert.match(candidate.slice(replay, batchRead), /idx_banking_pay_operations_idempotency_key|banking_pay_operations AS keyed_operation/);
  assert.match(candidate.slice(replay, batchRead), /banking_pay_operation_start:PAYMENT_CORRECTION:/);
  assert.match(candidate.slice(replay, batchRead), /v_replay_expected_selection IS DISTINCT FROM v_replay_request\.selection_json/);
  assert.match(candidate.slice(replay, batchRead), /OPERATION_IDEMPOTENCY_KEY_AMBIGUOUS/);
  assert.match(candidate.slice(replay, batchRead), /REQUEST_OPERATION_LINK_MISSING/);
  assert.match(candidate.slice(replay, batchRead), /REQUEST_OPERATION_LINK_AMBIGUOUS/);
  assert.doesNotMatch(candidate.slice(replay, batchRead), /v_replay_request\.plan_json->>'idempotency_key'/);
});

test('same key requires every original intent and server-owned linkage field', () => {
  const required = [
    'mode', 'requested_action', 'filter_json', 'sort_key', 'sort_direction',
    'snapshot_token', 'scope_fence_hash', 'requested_explicit_count',
    'requested_explicit_hash', 'canonical_explicit_candidate_tokens',
    'idempotency_key', 'correction_kind', 'requested_by_user_id',
    'auto_requested', 'source_bank_event_id', 'accepted_resolution_json',
    'accepted_resolution_hash', 'correction_request_id'
  ];
  for (const field of required) assert.match(candidate, new RegExp(field));
  assert.match(candidate, /draft_overlay_fast_pre_request_authorities/);
  assert.match(candidate, /cancellation_reversion_pre_request_authorities_v2/);
  assert.match(candidate, /cancellation_reversion_pre_request_authorities_v3/);
  assert.match(candidate, /IMMUTABLE_REQUEST_IDENTITY_MISMATCH/);
});

test('START_PREPARED exact replay precedes mutable start fences and needs consumed proof', () => {
  const exactReplay = candidate.indexOf('Returning an already-started request is a read-only lost-response replay');
  const startFence = candidate.indexOf('Recheck the pre-request fence before START_PREPARED', exactReplay);
  assert.ok(exactReplay > 0 && startFence > exactReplay);
  assert.match(candidate.slice(exactReplay, startFence), /reauth_consumed_at_utc IS NULL/);
  assert.match(candidate.slice(exactReplay, startFence), /reauth_proof_hash IS DISTINCT FROM v_proof_hash/);
  assert.match(candidate.slice(exactReplay, startFence), /selection_hash/);
  assert.match(candidate.slice(exactReplay, startFence), /plan_hash/);
  assert.match(candidate.slice(exactReplay, startFence), /EXACT_CONSUMED_PROOF_OR_REQUEST_MISMATCH/);
  assert.match(candidate.slice(exactReplay, startFence), /REQUEST_ALREADY_STARTED/);
});

test('exact replays reject a request/operation lifecycle mismatch', () => {
  const prepareReplay = candidate.indexOf('An explicit operation key is the durable serialisation point');
  const startReplay = candidate.indexOf('Returning an already-started request is a read-only lost-response replay');
  const startFence = candidate.indexOf('Recheck the pre-request fence before START_PREPARED', startReplay);
  const prepareBlock = candidate.slice(prepareReplay, startReplay);
  const startBlock = candidate.slice(startReplay, startFence);

  assert.match(prepareBlock, /REQUEST_OPERATION_LIFECYCLE_MISMATCH/);
  assert.match(startBlock, /REQUEST_OPERATION_LIFECYCLE_MISMATCH/);
  for (const [requestStatus, operationPhase] of [
    ['PLANNING', 'PREPARE_SELECTION'],
    ['PLANNED', 'AWAITING_REAUTHENTICATION'],
    ['AUTHORISED', 'EXPAND_WORK'],
    ['EXPANDED', 'PROCESS_CHUNKS'],
    ['PROCESSING', 'FINALISE'],
    ['APPLIED', 'REFRESH_WORKBENCH'],
    ['APPLIED', 'COMPLETE']
  ]) {
    assert.match(prepareBlock, new RegExp(`'${requestStatus}'`));
    assert.match(prepareBlock, new RegExp(`'${operationPhase}'`));
  }
  assert.match(startBlock, /v_operation\.phase IN \('PROCESS_CHUNKS','FINALISE'\)/);
  assert.match(startBlock, /v_operation\.phase = 'COMPLETE'/);
});

test('historical one-authoriser REQUESTED/AWAITING resume is not intercepted as a read-only replay', () => {
  const resumeAssignment = candidate.indexOf('Preserve the historical one-authoriser resume branch exactly');
  const exactReplay = candidate.indexOf('Returning an already-started request is a read-only lost-response replay');
  const startFence = candidate.indexOf('Recheck the pre-request fence before START_PREPARED', exactReplay);
  const block = candidate.slice(resumeAssignment, startFence);
  assert.ok(resumeAssignment > 0 && exactReplay > resumeAssignment && startFence > exactReplay);
  assert.match(block, /v_resume_reauthenticated_request\s*:=/);
  assert.match(block, /v_request\.status IN \('REQUESTED', 'AWAITING_AUTHORISATION'\)/);
  assert.match(block, /greatest\(coalesce\(v_request\.required_quantity, 1\), 1\) = 1/);
  assert.match(block, /coalesce\(v_request\.approved_count, 0\) = 0/);
  assert.match(block, /AND coalesce\(v_resume_reauthenticated_request, false\) IS NOT TRUE THEN/);
  assert.match(candidate, /LEGACY_REQUESTER_REAUTHORISED_CANCELLATION_RESUMED/);
  assert.match(candidate, /SET status = 'AUTHORISED',[\s\S]*approved_count = 1/);
});

test('cutover verification is zero-active-old and terminal history is not rewritten', () => {
  for (const status of ['PLANNING','PLANNED','REQUESTED','AWAITING_AUTHORISATION','AUTHORISED','EXPANDED','PROCESSING']) {
    assert.match(verifier, new RegExp(`'${status}'`));
  }
  assert.match(verifier, /ACTIVE_REQUEST_REPLAY_IDENTITY_INVALID/);
  assert.doesNotMatch(verifier, /UPDATE\s+public\.pay_payment_correction_requests/i);
});

test('runtime proof is self-contained, bounded and restores the exact prior setting through rollback', () => {
  assert.match(runtime, /^-- Self-contained rollback evidence/m);
  assert.match(runtime, /It requires only an existing DRAFT batch and active actor/);
  assert.match(runtime, /BEGIN;[\s\S]*ROLLBACK;\s*$/);
  assert.match(runtime, /v_fixture_request_id constant uuid := '80510000-0000-4510-8510-000000000001'/);
  assert.match(runtime, /v_fixture_operation_id constant uuid := '80510000-0000-4510-8510-000000000002'/);
  assert.match(runtime, /INSERT INTO public\.pay_payment_correction_requests/);
  assert.match(runtime, /INSERT INTO public\.banking_pay_operations/);
  assert.match(runtime, /INSERT INTO public\.pay_payment_correction_actions/);
  assert.match(runtime, /SET banking_pay_candidate_cancellation_enabled = true/);
  assert.doesNotMatch(runtime, /SET banking_pay_candidate_cancellation_enabled = false/);
  assert.match(runtime, /PAYMENT_CORRECTION_REPLAY_CALL_EXCEEDED_6000MS/);
  assert.match(runtime, /PAYMENT_CORRECTION_REPLAY_MUTATED_STATE/);
  assert.match(runtime, /PAYMENT_CORRECTION_REPLAY_CHANGED_EXACT_ROWS/);
  assert.doesNotMatch(runtime, /(?:provider|settlement|remittance).*(?:prepare|submit|execute|send)\s*\(/i);
});
