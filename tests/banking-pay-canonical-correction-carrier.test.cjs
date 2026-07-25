const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = (relativePath) =>
  fs.readFileSync(path.join(root, relativePath), 'utf8');

const migration = read(
  'supabase/migrations/25072026_1614_banking_pay_case_resolution_carry_registrations.sql'
);
const carrier = read(
  'supabase/repeatable/25072026_1615_banking_pay_canonical_correction_carrier.sql'
);
const runtimeGuards = read(
  'supabase/repeatable/21072026_1235_00b_import_correction_runtime_guards.sql'
);
const residual = read(
  'supabase/repeatable/21072026_1235_09_pay_correction_chain_residual_v1.sql'
);
const applyResolution = read(
  'supabase/repeatable/21072026_1235_41_pay_workbench_session_apply_case_resolution.sql'
);
const freeze = read(
  'supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql'
);
const worker = read('broker/src/index.js');
const importReview = read('broker/src/import-review.js');
const sessionSql = read(
  'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'
);

test('durable carry registrations have bounded states, immutable authorities and RLS', () => {
  assert.match(
    migration,
    /CREATE TABLE IF NOT EXISTS public\.banking_pay_workbench_case_resolution_carry_registrations/
  );
  for (const column of [
    'target_session_id',
    'source_session_id',
    'candidate_id',
    'source_resolution_id',
    'canonical_resolution_key',
    'source_economic_fingerprint',
    'source_resolution_snapshot_json',
    'target_resolution_id',
    'target_economic_fingerprint',
    'attempt_count',
  ]) {
    assert.match(migration, new RegExp(`\\b${column}\\b`));
  }
  assert.match(
    migration,
    /status IN \('PENDING', 'CARRIED', 'STALE', 'INCOMPATIBLE', 'SUPERSEDED'\)/
  );
  assert.match(migration, /ENABLE ROW LEVEL SECURITY/);
  assert.match(
    migration,
    /REVOKE ALL[\s\S]*FROM PUBLIC, anon, authenticated, service_role/
  );
  assert.match(migration, /PENDING rows are never age-cleaned/);
});

test('canonical identity is exact, TS_DAY-only and internal', () => {
  assert.match(
    carrier,
    /CREATE OR REPLACE FUNCTION public\._ctms_correction_carrier_identity_v1/
  );
  assert.match(
    carrier,
    /'CORRECTION_CHAIN_V1'[\s\S]*p_candidate_id::text[\s\S]*p_correction_root_id::text[\s\S]*v_key_type[\s\S]*v_key_value/
  );
  assert.match(carrier, /IF v_key_type <> 'TS_DAY'/);
  assert.match(carrier, /IMMUTABLE[\s\S]*STRICT[\s\S]*PARALLEL SAFE[\s\S]*SECURITY INVOKER/);
  assert.match(
    carrier,
    /REVOKE ALL ON FUNCTION public\._ctms_correction_carrier_identity_v1[\s\S]*FROM PUBLIC, anon, authenticated, service_role/
  );
});

test('session rollover registers before source retirement and missing provenance becomes review work', () => {
  const carryCall = sessionSql.indexOf(
    'pay_workbench_session_carry_forward_case_resolutions_v1'
  );
  const sourceRetirement = sessionSql.indexOf(
    'WITH retired_sources AS (',
    carryCall
  );
  assert.ok(carryCall >= 0, 'session open must invoke durable carry registration');
  assert.ok(
    sourceRetirement > carryCall,
    'source retirement must happen after carry registration'
  );
  assert.match(
    carrier,
    /'INCOMPATIBLE\|' \|\| v_resolution\.id::text/
  );
  assert.match(carrier, /'STABLE_IDENTITY_UNAVAILABLE'/);
  assert.doesNotMatch(
    carrier,
    /WORKBENCH_CORRECTION_CARRY_PROVENANCE_REQUIRED/
  );
});

test('candidate build consumes pending carries without swallowing expenses or additional lines', () => {
  assert.match(
    runtimeGuards,
    /_pay_workbench_case_resolution_carry_process_candidate_v1/
  );
  assert.match(
    runtimeGuards,
    /economic_key_json->>'key_type',''\)\)='TS_TOTAL'/
  );
});

test('residual and applied resolution use the same canonical component identity', () => {
  for (const marker of [
    'canonical_correction_key',
    'correction_identity_version',
    'ordered_member_timesheet_ids',
    'component_lineage_fingerprint',
    'resolution_economic_fingerprint',
  ]) {
    assert.match(residual, new RegExp(marker));
  }
  assert.match(
    applyResolution,
    /_ctms_correction_carrier_identity_v1/
  );
  assert.match(
    applyResolution,
    /CORRECTION_CARRIER_IDENTITY_MISMATCH/
  );
});

test('draft freeze adds provenance only to the four existing frozen JSON authorities', () => {
  assert.match(
    freeze,
    /STAGE_16C0_FREEZE_CANONICAL_CORRECTION_PROVENANCE/
  );
  for (const column of [
    'frozen_component_snapshot_json',
    'frozen_source_basis_json',
    'frozen_resolution_payload_json',
    'frozen_resolution_result_json',
  ]) {
    assert.match(freeze, new RegExp(`\\b${column}\\b`));
  }
  assert.match(
    freeze,
    /PAY_BATCH_CANONICAL_CORRECTION_PROVENANCE_INVALID/
  );
});

test('Banking Pay mutation RPCs remain Worker-only', () => {
  assert.match(
    applyResolution,
    /REVOKE ALL ON FUNCTION public\.pay_workbench_session_apply_case_resolution\(uuid, uuid, jsonb\)[\s\S]*FROM PUBLIC, anon, authenticated[\s\S]*GRANT EXECUTE[\s\S]*TO service_role/
  );
  assert.match(
    freeze,
    /REVOKE ALL ON FUNCTION public\.pay_batch_apply_finance_adjustments\([\s\S]*FROM PUBLIC, anon, authenticated[\s\S]*GRANT EXECUTE[\s\S]*TO service_role/
  );
});

test('Worker fails closed before public mutations and background job claims', () => {
  assert.match(
    worker,
    /BANKING_PAY_WORKBENCH_DB_V1/
  );
  assert.match(
    worker,
    /BANKING_PAY_CANONICAL_CORRECTION_CARRIER_V1/
  );
  const gate = worker.indexOf('async function assertBankingPayWorkbenchContract');
  const route = worker.indexOf("p.startsWith('/api/banking/pay/workbench/')");
  const draftStart = worker.indexOf(
    'async function advanceBankingPayDraftCreateOperation'
  );
  const draftEnd = worker.indexOf('\nasync function ', draftStart + 10);
  const draftBody = worker.slice(draftStart, draftEnd);
  const jobStart = worker.indexOf(
    'async function drainBankingPayWorkbenchJobs'
  );
  const jobEnd = worker.indexOf('\nasync function ', jobStart + 10);
  const jobBody = worker.slice(jobStart, jobEnd);
  assert.ok(gate >= 0 && route > gate);
  assert.ok(
    draftBody.indexOf('assertBankingPayWorkbenchContract') >= 0
  );
  assert.ok(
    draftBody.indexOf('assertBankingPayWorkbenchContract')
      < draftBody.indexOf('sbRpc(')
  );
  assert.ok(
    jobBody.indexOf('assertBankingPayWorkbenchContract') >= 0
  );
  assert.ok(
    jobBody.indexOf('assertBankingPayWorkbenchContract')
      < jobBody.indexOf('sbRpc(')
  );
  assert.match(worker, /workbench_job_claimed: false/);
  assert.match(worker, /mutation_attempted: false/);
});

test('import review contract requires the canonical carrier marker', () => {
  assert.match(
    importReview,
    /BANKING_PAY_CANONICAL_CORRECTION_CARRIER_V1/
  );
  assert.match(
    importReview,
    /canonical_correction_carrier_version/
  );
  assert.match(
    carrier,
    /'canonical_correction_carrier_version'[\s\S]*v_projection_contract/
  );
});
