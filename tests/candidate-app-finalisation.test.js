import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { buildTsq1Payload, buildTsq1String, verifyTsq1String } from '../broker/src/timesheet-qr-payload.js';
import { candidateAppBackendInternals as candidateAppInternals } from '../broker/src/candidate-app-backend.js';
import { candidateBrokerInternals } from '../candidate-broker/src/candidate-broker.js';

const context = Object.freeze({
  applicable: true,
  mode: 'DURATION_MINUTES',
  context_version: 'CANDIDATE_BREAK_ENTRY_V1',
  context_token: 'a'.repeat(64)
});

test('editable printed-document Timesheets retain adaptive break entry', () => {
  const sql = readFileSync(new URL(
    '../supabase/repeatable/02092026_0325_candidate_paper_break_entry_v1.sql',
    import.meta.url
  ), 'utf8');
  assert.match(sql, /\\ir 30082026_1531_candidate_paper_return_proof_transaction_boundary_reassert_v1\.sql/);
  assert.match(sql, /\\ir 31082026_0557_candidate_empty_expense_carrier_action_v1\.sql/);
  assert.match(sql, /create or replace function public\.candidate_app_timesheet_detail_v2/);
  assert.match(sql, /if v_result#>>'\{timesheet,sheet_scope\}'='DAILY' then/);
  assert.match(sql, /create or replace function public\.candidate_break_entry_context_get_v1/);
  assert.match(sql, /if v_workflow\.workflow_kind='DAILY' then/);
  assert.match(sql, /route_family',''\) in \('ELECTRONIC','PAPER','QR'\)/);
  assert.match(sql, /not coalesce\(\(p_capabilities->>'import_authoritative'\)::boolean,false\)/);
  assert.match(sql, /not in \('ELECTRONIC','PAPER','QR'\)\s+then 'NON_ELECTRONIC_ROUTE'/);
});

test('editable Weekly Timesheets expose stable break authority before route selection', () => {
  const sql = readFileSync(new URL(
    '../supabase/repeatable/03092026_1215_candidate_weekly_preroute_break_entry_v1.sql',
    import.meta.url
  ), 'utf8');
  assert.match(sql, /route_family',''\) in \('','MANUAL_NON_QR','ELECTRONIC','PAPER','QR'\)/);
  assert.match(sql, /not in \('','MANUAL_NON_QR','ELECTRONIC','PAPER','QR'\)\s+then 'NON_ELECTRONIC_ROUTE'/);
  assert.doesNotMatch(sql, /and not coalesce\(\(v_resolution->>'is_nhsp'\)::boolean,false\)/);
  assert.match(sql, /revoke all on function private\._candidate_break_entry_context_core_v1\(uuid,uuid,date,jsonb\)\s+from public,anon,authenticated,service_role/);
  const verification = readFileSync(new URL(
    '../supabase/verification/03092026_1216_candidate_weekly_preroute_break_entry_verification.sql',
    import.meta.url
  ), 'utf8');
  assert.match(verification, /CANDIDATE_PRE_ROUTE_BREAK_ENTRY_NOT_EXPOSED/);
  assert.match(verification, /CANDIDATE_PROJECTED_PRE_ROUTE_BREAK_ENTRY_NOT_EXPOSED/);
  assert.match(verification, /CANDIDATE_EDITABLE_NHSP_WEEKLY_BREAK_ENTRY_HIDDEN/);
  assert.match(verification, /'route_family','MANUAL_NON_QR'/);
  assert.match(verification, /CANDIDATE_SELECTED_ROUTE_BREAK_ENTRY_DRIFT/);
  assert.match(verification, /CANDIDATE_IMPORT_BREAK_ENTRY_BROADENED/);
  assert.match(verification, /CANDIDATE_UNRELATED_ROUTE_BREAK_ENTRY_BROADENED/);
  assert.match(verification, /CANDIDATE_NO_TIMESHEET_BREAK_ENTRY_BROADENED/);
});

test('TSQ1 lower-level verifier accepts only the exact signed v1 token payload', async () => {
  const env = { QR_SIGNING_SECRET: 'test-only-qr-secret-material' };
  const token = 'returned-paper-token-00000001';
  const value = await buildTsq1String(buildTsq1Payload({ qr_token: token }), env);
  assert.deepEqual(await verifyTsq1String(value, env), { v: 1, tok: token });
  await assert.rejects(
    verifyTsq1String(`${value.slice(0, -1)}${value.endsWith('A') ? 'B' : 'A'}`, env),
    /TSQ1_SIGNATURE_INVALID/
  );
});

test('adaptive duration breaks normalise into the existing financial input without endpoints', () => {
  const result = candidateAppInternals.normaliseCandidateBreakSubmission({
    break_entry_context: {
      context_version: context.context_version,
      context_token: context.context_token,
      mode: context.mode
    },
    actual_schedule_json: [{
      date: '2026-08-23', start: '08:00', end: '16:00',
      break_entry: { kind: 'DURATION_MINUTES', break_minutes: 30 }
    }]
  }, context);
  assert.equal(result.break_entry_context, undefined);
  assert.deepEqual(result.actual_schedule_json[0], {
    date: '2026-08-23', start: '08:00', end: '16:00', break_minutes: 30
  });
});

test('standalone expense workflows never require an hours break context', () => {
  assert.equal(candidateAppInternals.candidateWorkflowRequiresBreakEntry({
    workflow_kind: 'CONTRACT_EXPENSE'
  }), false);
  assert.equal(candidateAppInternals.candidateWorkflowRequiresBreakEntry({
    workflow_kind: 'CONTRACT_COMBINED'
  }), true);
  assert.equal(candidateAppInternals.candidateWorkflowRequiresBreakEntry({
    workflow_kind: 'CONTRACT_HOURS'
  }), true);
  assert.equal(candidateAppInternals.candidateWorkflowRequiresBreakEntry({
    workflow_kind: 'DAILY'
  }), true);
});

test('adaptive break input fails closed on stale context or wrong-mode fields', () => {
  assert.throws(() => candidateAppInternals.normaliseCandidateBreakSubmission({
    break_entry_context: {
      context_version: context.context_version,
      context_token: 'b'.repeat(64), mode: context.mode
    },
    actual_schedule_json: [{ break_entry: { kind: 'DURATION_MINUTES', break_minutes: 30 } }]
  }, context), /CANDIDATE_BREAK_ENTRY_CONTEXT_STALE/);
  assert.throws(() => candidateAppInternals.normaliseAdaptiveBreakEntry({
    kind: 'DURATION_MINUTES', break_minutes: 30, break_start: '12:00'
  }, 'DURATION_MINUTES'), /CANDIDATE_BREAK_ENTRY_INVALID/);
});

test('no-break remains an explicit zero with no endpoints in either adaptive mode', () => {
  assert.deepEqual(candidateAppInternals.normaliseAdaptiveBreakEntry({
    kind: 'NO_BREAK', no_break: true, break_minutes: 0
  }, 'START_END_TIMES'), { no_break: true, break_minutes: 0 });
});

test('Candidate submission rejects contradictory saved and visible work-time aliases before database use', () => {
  assert.throws(() => candidateAppInternals.assertCandidateScheduleAliasConsistency({
    hours_submission: {
      timesheet_patch_json: {
        actual_schedule_json: [{
          date: '2026-07-20',
          start: '09:00', end: '17:00',
          start_time: '17:00', end_time: '18:00'
        }]
      }
    }
  }), error => (
    error?.status === 400
    && error?.code === 'CANDIDATE_WORK_INTERVAL_CONTRADICTORY'
    && error?.details?.boundary === 'START'
  ));

  assert.doesNotThrow(() => candidateAppInternals.assertCandidateScheduleAliasConsistency({
    actual_schedule_json: [{
      date: '2026-07-20', start_time: '17:00', end_time: '18:00'
    }]
  }));
});

test('public broker enforces the closed returned-paper QR proof envelope', () => {
  const body = {
    generation: 2,
    component_kind: 'SIGNED_RETURN',
    media_type: 'image/jpeg',
    byte_size: 1024,
    document_role: 'SIGNED_RETURN',
    paper_return_page_key: 'HOURS_TIMESHEET',
    idempotency_key: '7f13a66b-8f97-47b5-bc94-8898bfd9d342',
    signed_return_proof: {
      proof_contract_version: 'CANDIDATE_PAPER_RETURN_PROOF_V1',
      paper_return_manifest_sha256: 'b'.repeat(64),
      paper_return_page_key: 'HOURS_TIMESHEET',
      detected_qr_count: 1,
      qr_text: `TSQ1.${'a'.repeat(20)}.${'c'.repeat(43)}`
    }
  };
  assert.equal(candidateBrokerInternals.validateCandidateFinalisationBody(
    '/candidate-app/v1/workflows/f1000000-0000-4000-8000-000000000008/components/prepare',
    body
  ), body);
  assert.throws(() => candidateBrokerInternals.validateCandidateFinalisationBody(
    '/candidate-app/v1/workflows/f1000000-0000-4000-8000-000000000008/components/prepare',
    { ...body, signed_return_proof: { ...body.signed_return_proof, agency_id: 'forged' } }
  ), /CANDIDATE_PAPER_QR_PROOF_INVALID/);
});

test('public broker accepts the QR-required v2 proof and rejects a missing QR', () => {
  const path = '/candidate-app/v1/workflows/f1000000-0000-4000-8000-000000000008/components/prepare';
  const body = {
    generation: 2,
    component_kind: 'SIGNED_RETURN',
    media_type: 'image/jpeg',
    byte_size: 1024,
    document_role: 'SIGNED_RETURN',
    paper_return_page_key: 'HOURS_TIMESHEET',
    idempotency_key: '3f74fd8c-2261-45a2-9e49-5f412917c31e',
    signed_return_proof: {
      proof_contract_version: 'CANDIDATE_PAPER_RETURN_PROOF_V2',
      paper_return_manifest_sha256: 'c'.repeat(64),
      paper_return_page_key: 'HOURS_TIMESHEET',
      detected_qr_count: 1,
      qr_text: `TSQ2.${'d'.repeat(20)}.${'e'.repeat(43)}`
    }
  };
  assert.equal(candidateBrokerInternals.validateCandidateFinalisationBody(path, body), body);
  assert.throws(() => candidateBrokerInternals.validateCandidateFinalisationBody(path, {
    ...body,
    signed_return_proof: {
      ...body.signed_return_proof,
      detected_qr_count: 0,
      qr_text: undefined
    }
  }), /CANDIDATE_PAPER_QR_PROOF_INVALID/);
});

test('public broker rejects an inapplicable or mixed-shape adaptive break before routing', () => {
  const path = '/candidate-app/v1/workflows/f1000000-0000-4000-8000-000000000008/actions/worker-submit';
  assert.throws(() => candidateBrokerInternals.validateCandidateFinalisationBody(path, {
    generation: 1,
    idempotency_key: '04d67c68-8994-44e0-a189-c51a799838bf',
    immutable_submission: {
      break_entry_context: {
        context_version: 'CANDIDATE_BREAK_ENTRY_V1',
        context_token: 'a'.repeat(64),
        mode: 'DURATION_MINUTES'
      },
      actual_schedule_json: [{
        break_entry: { kind: 'DURATION_MINUTES', break_minutes: 30, break_start: '12:00' }
      }]
    }
  }), /CANDIDATE_BREAK_ENTRY_INVALID/);

  assert.throws(() => candidateAppInternals.normaliseCandidateBreakSubmission({
    break_entry: { kind: 'DURATION_MINUTES', break_minutes: 30 }
  }, { applicable: false }), /CANDIDATE_BREAK_ENTRY_NOT_APPLICABLE/);
});
