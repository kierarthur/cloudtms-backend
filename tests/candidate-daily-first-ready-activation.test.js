import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

import { findCandidateDailyRoute } from '../broker/src/candidate-daily-contract-v1.js';
import { candidateDailyPhase1bInternals } from '../broker/src/candidate-daily-phase1b.js';

const sql = fs.readFileSync(path.join(process.cwd(), 'supabase', 'repeatable',
  '25082026_1721_candidate_daily_first_ready_activation_v1.sql'), 'utf8');
const homeSql = fs.readFileSync(path.join(process.cwd(), 'supabase', 'repeatable',
  '25082026_1616_candidate_home_daily_read_v1.sql'), 'utf8');
const correlationId = `0${'A'.repeat(25)}`;

function rpcRecorder(resultForPrimary = { outcomes: [{ status: 'COMMITTED' }] }) {
  const calls = [];
  return {
    calls,
    deps: {
      async rpc(name, args) {
        calls.push({ name, args });
        if (name === 'candidate_daily_system_policy_activate_ready_v1') {
          return { ok: true, policy_version: 'CANDIDATE_FIRST_READY_ACTIVATION_V1', outcomes: [] };
        }
        if (name === 'candidate_daily_projection_complete_atomic_v1') {
          return {
            batch_receipt_id: '00000000-0000-4000-8000-000000000206',
            outcomes: [{ index: 0, accepted: true, state: 'DELIVERED' }]
          };
        }
        if (name === 'candidate_daily_reconciliation_apply_atomic_v1') {
          return {
            batch_receipt_id: '00000000-0000-4000-8000-000000000207',
            outcomes: [{ index: 0, classification: 'MATCH' }]
          };
        }
        return resultForPrimary;
      }
    }
  };
}

test('first-ready policy reuses the sole locked transition and retains every safety barrier', () => {
  assert.match(sql, /candidate_daily_authority_transition_atomic_v1\s*\(/i);
  assert.match(sql, /expected_day_count<>14[\s\S]*actual_day_count<>14[\s\S]*v_day_count<>14/i);
  assert.match(sql, /published_at_utc<pg_catalog\.clock_timestamp\(\)-interval '120 seconds'/i);
  assert.match(sql, /v_source_primary_count<>1[\s\S]*v_source_group_count<>1/i);
  assert.match(sql, /v_sync\.state<>'READY'[\s\S]*pending_count<>0[\s\S]*retry_count<>0[\s\S]*terminal_count<>0/i);
  assert.match(sql, /v_blocking_count,0\)<>0/i);
  assert.match(sql, /expected_authority_mode','GOOGLE_PRIMARY'[\s\S]*new_authority_mode','SUPABASE_PRIMARY'/i);
  assert.match(sql, /entitlement_enabled',true/i);
  assert.doesNotMatch(sql, /insert\s+into\s+public\.candidates|update\s+public\.candidates/i);
  assert.match(sql, /revoke all on function[\s\S]*from public,anon,authenticated/i);
});

test('Home announcement validation uses callable PostgreSQL string functions', () => {
  assert.match(homeSql, /pg_catalog\.strpos\(v_text,'<'\)>0/i);
  assert.match(homeSql, /pg_catalog\.strpos\(v_text,'>'\)>0/i);
  assert.doesNotMatch(homeSql, /pg_catalog\.position\s*\(/i);
});

test('a completed projection retries activation only for safely visible outcomes', async () => {
  const route = findCandidateDailyRoute('POST', '/candidate-system/v1/google-availability/projection/complete');
  const delivered = '00000000-0000-4000-8000-000000000201';
  const deferred = '00000000-0000-4000-8000-000000000202';
  const retry = '00000000-0000-4000-8000-000000000203';
  const recorder = rpcRecorder();
  const response = await candidateDailyPhase1bInternals.invokeSystemRpc({
    route,
    body: {
      batch_request_id: '00000000-0000-4000-8000-000000000204',
      items: [
        { outbox_id: delivered, lease_token: 'lease-token-delivered', outcome: 'DELIVERED', observed_sheet_revision: 'r1' },
        { outbox_id: deferred, lease_token: 'lease-token-deferred', outcome: 'DEFERRED_OVERLAY', observed_sheet_revision: 'r1' },
        { outbox_id: retry, lease_token: 'lease-token-for-retry', outcome: 'RETRY', error_code: 'TRANSIENT' }
      ]
    },
    correlationId,
    idempotencyKey: 'projection-complete-fixed-key'
  }, { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, recorder.deps);
  assert.equal(response.status, 200);
  assert.equal(recorder.calls[0].name, 'candidate_daily_projection_complete_atomic_v1');
  assert.equal(recorder.calls[1].name, 'candidate_daily_system_policy_activate_ready_v1');
  assert.deepEqual(recorder.calls[1].args.p_projection_outbox_ids, [delivered, deferred]);
});

test('a reconciliation retries activation for each unique Candidate identity without changing its result', async () => {
  const route = findCandidateDailyRoute('POST', '/candidate-system/v1/google-availability/reconciliation');
  const hmac = 'a'.repeat(64);
  const recorder = rpcRecorder();
  const response = await candidateDailyPhase1bInternals.invokeSystemRpc({
    route,
    body: {
      batch_request_id: '00000000-0000-4000-8000-000000000205',
      observations: [{
        candidate_source_hmac: hmac,
        date: '2026-08-25', observed_value: 'LD', observed_sheet_revision: 'r2',
        source_event_id: 'reconcile.fixed.event', source_revision: '2',
        source_event_time: '2026-08-25T17:00:00.000Z', source_hash: 'b'.repeat(64),
        item_key: 'reconcile.fixed.item'
      }]
    },
    correlationId,
    idempotencyKey: 'reconciliation-fixed-key'
  }, { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, recorder.deps);
  assert.equal(response.status, 200);
  assert.deepEqual(recorder.calls[1].args.p_candidate_source_hmacs, [hmac]);
  assert.match(await response.text(), /MATCH/);
});
