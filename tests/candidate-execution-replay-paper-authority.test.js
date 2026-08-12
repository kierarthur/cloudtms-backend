import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const read = (path) => readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');

const backend = read('broker/src/candidate-app-backend.js');
const workflow = read('supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql');
const finalise = read('supabase/repeatable/07082026_2128_candidate_finalize_reject_no_work_rpcs_v1.sql');
const qr = read('supabase/repeatable/07082026_2224_candidate_app_weekly_office_replacements_v1.sql');
const office = read('supabase/repeatable/11082026_1832_cloudtms_office_candidate_adapter_v1.sql');

test('Candidate HTTP enrichment is stable and signing time is an explicit factual input', () => {
  assert.match(backend, /CANDIDATE_SIGNATURE_TIMESTAMP_REQUIRED/);
  assert.match(backend, /candidate-email-handoff-v1[\s\S]*mutationKey[\s\S]*approvalIdentity/);
  assert.doesNotMatch(
    backend,
    /CREATE_EMAIL_APPROVAL_REQUEST'[\s\S]{0,180}randomToken\(32\)/
  );
  assert.match(workflow, /v_action='SELECT_PHONE_APPROVAL'[\s\S]*-'expires_at_utc'/);
  assert.match(workflow, /v_action='WORKER_SUBMIT'[\s\S]*-'official_presentation'/);
});

test('finalisation probes the durable receipt before mutable lifecycle validation', () => {
  assert.match(backend, /probeFinalisationReplay[\s\S]*replay_probe_only: true/);
  assert.match(finalise, /CANDIDATE_FINALISATION_MUTATION_REQUEST_V2/);
  const receipt = finalise.indexOf("v_mutation_receipt:=private._candidate_workflow_mutation_receipt_v1");
  const generation = finalise.indexOf('if v_workflow.generation<>p_expected_generation');
  const serviceValidation = finalise.indexOf("if p_session_id is null then", receipt + 1);
  assert.ok(receipt > -1 && generation > receipt && serviceValidation > receipt);
  assert.match(office, /FINALISE_REPLAY_LOOKUP/);
});

test('PAPER execution has one database-owned attempt lease, backoff and terminal stop contract', () => {
  assert.match(workflow, /PAPER_PACK_ATTEMPT_CLAIM/);
  assert.match(workflow, /candidate_paper_pack_attempt_token/);
  assert.match(workflow, /candidate_paper_pack_next_retry_at_utc/);
  assert.match(workflow, /CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE/);
  assert.match(workflow, /CANDIDATE_PAPER_PACK_FAILED_TERMINAL/);
  assert.match(backend, /execution_state: 'FAILED_TERMINAL'/);
  assert.match(backend, /execution_state: 'BACKOFF'/);
  assert.match(backend, /claimCandidatePaperPackAttempt/);
  assert.match(backend, /claim_acquired_new/);
  assert.match(backend, /PAPER_RETRY_REPLAY/);
  assert.match(backend, /PAPER_RETRY_RECORD/);
  assert.match(backend, /OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3/);
  assert.match(office, /cloudtms_office_candidate_paper_retry/);
  assert.match(office, /OFFICE_CANDIDATE_PAPER_RETRY_RECEIPT_V1/);
});

test('PAPER preparation owns an enforceable deadline and pre-outbox failure receipt', () => {
  assert.match(qr, /candidate_paper_pack_preparation_started_at_utc/);
  assert.match(qr, /candidate_paper_pack_preparation_deadline_at_utc/);
  assert.match(backend, /CANDIDATE_PAPER_DOCUMENT_PENDING_TIMEOUT/);
  assert.match(workflow, /'failure_scope','WORKFLOW'/);
  assert.match(workflow, /'mail_outbox_id',null/);
});

test('canonical rejection replay is bound to the complete request after a key lock', () => {
  const keyLock = finalise.indexOf('CANDIDATE_REJECTION_IDEMPOTENCY:');
  const hash = finalise.indexOf('CANDIDATE_REJECTION_REQUEST_V2');
  const receipt = finalise.indexOf("object_type='candidate_submission_rejection_receipt'");
  assert.ok(keyLock > -1 && hash > keyLock && receipt > hash);
  assert.match(finalise, /'expected_row_signature',btrim\(p_expected_row_signature\)/);
  assert.match(finalise, /'reason',btrim\(p_reason\)/);
  assert.match(finalise, /CANDIDATE_IDEMPOTENCY_CONFLICT/);
});

test('Office PAPER retry responses never expose complete-pack storage keys', () => {
  const start = backend.indexOf('async function handleOfficePaperRetry');
  const end = backend.indexOf('\nfunction routeMatch', start);
  const source = backend.slice(start, end);
  assert.doesNotMatch(source, /complete_pack_storage_key/);
  assert.doesNotMatch(source, /complete_pack:\s*(context|replayContext)\.complete/);
  assert.match(source, /paper_pack_state: 'RETRY_IN_PROGRESS'/);
});
