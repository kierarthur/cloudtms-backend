import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');

const enqueue = read('supabase/repeatable/07082026_1017_pay_workbench_enqueue_candidate_refresh.sql');
const runtime = read('supabase/repeatable/07082026_1016_banking_pay_targeted_delta_runtime.sql');
const worker = read('broker/src/index.js');
const wrangler = read('wrangler.toml');

test('full-source owner identity is economic and excludes diagnostic reason', () => {
  const fingerprintStart = enqueue.indexOf('v_authority_fingerprint_text := concat_ws');
  const fingerprintEnd = enqueue.indexOf('v_authority_fingerprint :=', fingerprintStart);
  assert.ok(fingerprintStart >= 0 && fingerprintEnd > fingerprintStart);
  const fingerprint = enqueue.slice(fingerprintStart, fingerprintEnd);

  assert.match(fingerprint, /WORKBENCH_SOURCE_OWNER_V[23]/);
  assert.match(fingerprint, /v_session_id/);
  assert.match(fingerprint, /v_source_change_seq/);
  assert.match(fingerprint, /v_registry_dirty_generation/);
  assert.match(fingerprint, /v_pay_channel_scope/);
  assert.match(fingerprint, /FULL_CANDIDATE/);
  assert.doesNotMatch(fingerprint, /v_reason|trigger|actor_user_id|source_dirty_job_id/i);
  assert.match(enqueue, /v_dedupe_key := CASE WHEN v_authority_fingerprint_version\s*=\s*3[\s\S]+WORKBENCH_SOURCE_OWNER_V3:[\s\S]+WORKBENCH_SOURCE_OWNER_V2:[\s\S]+v_authority_fingerprint/);
  assert.match(enqueue, /v_requested_source_build_run_id := v_source_build_run_id_text::uuid/);
});

test('one active full build covers every same-authority source-build reason', () => {
  assert.match(enqueue, /build_row\.source_change_seq = COALESCE\(v_source_change_seq, 0\)/);
  assert.match(enqueue, /build_row\.captured_candidate_generation = COALESCE\(v_registry_dirty_generation, 0\)/);
  assert.match(enqueue, /owner_active_job\.economic_build_id = build_row\.id[\s\S]+owner_active_job\.status[\s\S]+QUEUED[\s\S]+RUNNING/);
  assert.match(enqueue, /source_job\.economic_build_id = v_owner_build\.id[\s\S]+ORDER BY source_job\.created_at_utc, source_job\.id[\s\S]+LIMIT 1/);
  assert.match(enqueue, /Every WORKBENCH_CANDIDATE_SOURCE_BUILD owns complete candidate truth/);
  assert.match(enqueue, /v_owner_resolution := 'ACTIVE_CURRENT_OWNER_COVERS_REQUEST'/);
  assert.match(enqueue, /'owner_active_job_id'/);
  assert.match(enqueue, /'requested_coverage', 'FULL_CANDIDATE'/);
  assert.match(enqueue, /'owner_coverage', 'FULL_CANDIDATE'/);
});

test('published current full authority is a certified no-op', () => {
  assert.match(enqueue, /build_row\.status, ''\)\)\) = 'COMPLETE'/);
  assert.match(enqueue, /current_scope\.certified_preview_publication_required IS TRUE/);
  assert.match(enqueue, /current_scope\.certified_preview_publication_parity_ok IS TRUE/);
  assert.match(enqueue, /current_scope\.certified_preview_publication_source_change_seq = build_row\.source_change_seq/);
  assert.match(enqueue, /current_scope\.certified_preview_publication_source_build_run_id = build_row\.source_build_run_id/);
  assert.match(enqueue, /v_owner_resolution := 'COMPLETE_CURRENT_AUTHORITY'/);
  assert.match(enqueue, /'no_op', v_owner_resolution = 'COMPLETE_CURRENT_AUTHORITY'/);
});

test('older pre-invalidated backlog can only be absorbed by newer current authority', () => {
  assert.match(enqueue, /v_stale_preinvalidated_absorb_only :=/);
  assert.match(enqueue, /v_payload_scope_change_generation < COALESCE\(v_live_scope_change_generation,0\)/);
  assert.match(enqueue, /COALESCE\(v_registry_dirty_generation,0\)=COALESCE\(v_live_scope_change_generation,0\)/);
  assert.match(enqueue, /COALESCE\(v_registry_source_change_seq_before,0\)=COALESCE\(v_source_change_seq,0\)/);
  assert.match(enqueue, /'stale_preinvalidated_absorb_only', v_stale_preinvalidated_absorb_only/);
  assert.match(enqueue, /PAY_WORKBENCH_STALE_PREINVALIDATED_AUTHORITY_NOT_CURRENT/);

  const failClosedAt = enqueue.indexOf("PAY_WORKBENCH_STALE_PREINVALIDATED_AUTHORITY_NOT_CURRENT");
  const newDeltaOwnerAt = enqueue.indexOf("IF v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'", failClosedAt);
  assert.ok(failClosedAt >= 0 && newDeltaOwnerAt > failClosedAt);
});

test('diagnostic reasons merge onto the elected owner but never change its identity', () => {
  assert.match(enqueue, /'primary_reason'/);
  assert.match(enqueue, /'reason_latest'/);
  assert.match(enqueue, /'reasons', v_owner_reasons_json/);
  assert.match(enqueue, /'coalesced_request_count', v_owner_request_count/);
  assert.match(enqueue, /LIMIT 16/);
  assert.match(enqueue, /WHERE owner_job\.id = v_owner_root_job\.id/);
});

test('dirty apply resolves current authority before any public dirty marking', () => {
  const functionStart = runtime.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_candidate_dirty_apply_job_process');
  const enqueueAt = runtime.indexOf('SELECT public.pay_workbench_enqueue_candidate_refresh(', functionStart);
  const scopeDirtyAt = runtime.indexOf('UPDATE public.banking_pay_workbench_session_scope AS scope_row', enqueueAt);
  assert.ok(functionStart >= 0 && enqueueAt > functionStart && scopeDirtyAt > enqueueAt);

  assert.match(runtime.slice(functionStart, enqueueAt), /v_candidate_serial_blocked := NOT v_candidate_lock_acquired/);
  assert.doesNotMatch(runtime.slice(functionStart, enqueueAt), /v_candidate_serial_state->>'blocked'/);
  assert.match(runtime.slice(enqueueAt, scopeDirtyAt), /coalesced_to_current_refresh_authority/);
  assert.match(runtime.slice(enqueueAt, scopeDirtyAt), /CONTINUE;/);
  assert.match(runtime, /'jobs_coalesced_active'/);
  assert.match(runtime, /'jobs_coalesced_complete'/);
});

test('durable continuation uses the existing bounded queue wake and cron remains fallback', () => {
  assert.match(worker, /BANKING_PAY_WORKBENCH_DRAIN_WAKE_V1/);
  assert.match(worker, /BANKING_PAY_CONTINUATION_QUEUE\.send\(message, \{ delaySeconds: 1 \}\)/);
  assert.match(worker, /canAutoContinueFrom\(finalSummary\)/);
  assert.match(worker, /MAX_DURABLE_WAKE_DEPTH/);
  assert.match(worker, /cron_fallback_required: true/);
  assert.match(wrangler, /binding = "BANKING_PAY_CONTINUATION_QUEUE"/);
  assert.match(wrangler, /BANKING_PAY_CONTINUATION_ENABLED = "true"/);
  assert.match(wrangler, /queue = "test-cloudtms-banking-pay-continuation"/);
  assert.match(wrangler, /max_batch_size = 1/);
  assert.match(worker, /await scheduleDraftCreateDrainBestEffort\(publicPayload\);/);
  assert.match(worker, /await scheduleDraftCreateDrainBestEffort\(operationPayload\);/);
  assert.doesNotMatch(worker, /(?<!await )scheduleDraftCreateDrainBestEffort\((?:publicPayload|operationPayload)\);/);
});

test('the correction does not modify economic, provider or settlement authority', () => {
  for (const sql of [enqueue, runtime]) {
    assert.doesNotMatch(sql, /provider_submission_execute|bank_transfer_execute|settlement_execute/);
  }
  assert.match(enqueue, /'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'/);
});
