const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = (relativePath) =>
  fs.readFileSync(path.join(root, relativePath), 'utf8');
const listFiles = (relativeDirectory) => {
  const pending = [path.join(root, relativeDirectory)];
  const files = [];
  while (pending.length > 0) {
    const current = pending.pop();
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const absolutePath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        pending.push(absolutePath);
      } else {
        files.push(absolutePath);
      }
    }
  }
  return files;
};

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
const clearResolution = read(
  'supabase/repeatable/21072026_1235_42_pay_workbench_session_clear_case_resolution.sql'
);
const freeze = read(
  'supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql'
);
const worker = read('broker/src/index.js');
const importReview = read('broker/src/import-review.js');
const sessionSql = read(
  'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'
);
const selectionCarryMigration = read(
  'supabase/migrations/25072026_2152_banking_pay_selection_carry_registrations.sql'
);
const selectionCarryRuntime = read(
  'supabase/repeatable/25072026_2153_banking_pay_selection_carry_runtime.sql'
);
const finalFreshnessWrapper = read(
  'supabase/repeatable/26072026_1519_pay_batch_validate_freshness_correction_chain_wrapper.sql'
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

test('carry compares stable authority before replaying and validating the saved decision', () => {
  const authorityStart = carrier.indexOf(
    'public._ctms_correction_resolution_authority_fingerprint_v1'
  );
  const authorityEnd = carrier.indexOf(
    '\nCREATE OR REPLACE FUNCTION ',
    authorityStart + 10
  );
  const authority = carrier.slice(authorityStart, authorityEnd);
  const processStart = carrier.indexOf(
    'public._pay_workbench_case_resolution_carry_process_candidate_v1'
  );
  const processEnd = carrier.indexOf(
    '\nCREATE OR REPLACE FUNCTION ',
    processStart + 10
  );
  const process = carrier.slice(processStart, processEnd);

  assert.ok(authorityStart >= 0);
  assert.match(authority, /IMMUTABLE[\s\S]*STRICT[\s\S]*SECURITY INVOKER/);
  for (const authorityField of [
    'canonical_correction_key',
    'ordered_member_timesheet_ids',
    'component_lineage_fingerprint',
    'source_basis_fingerprint',
    'correction_financials_policy_envelope_fingerprint',
    'target_pay_method',
    'effective_source_outstanding_ex_vat',
  ]) {
    assert.match(authority, new RegExp(authorityField));
  }
  assert.doesNotMatch(authority, /target_outstanding_ex_vat/);
  assert.doesNotMatch(authority, /resolved_target_amount_ex_vat/);
  assert.doesNotMatch(authority, /resolution_complete/);

  assert.match(
    process,
    /v_target_authority_fingerprint[\s\S]*IS DISTINCT FROM v_source_authority_fingerprint/
  );
  assert.match(
    process,
    /_ctms_normalise_correction_case_resolutions_v1/
  );
  assert.match(
    process,
    /PAY_WORKBENCH_CARRY_VALIDATE_DECISION/
  );
  assert.match(process, /DECISION_RESULT_CHANGED/);
  assert.ok(
    process.indexOf(
      'v_target_authority_fingerprint'
    ) < process.indexOf(
      '_ctms_normalise_correction_case_resolutions_v1'
    ),
    'authority must be compared before replaying a saved decision'
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
  assert.match(
    applyResolution,
    /banking_pay_workbench_case_resolution_carry_registrations[\s\S]*status IN \('PENDING', 'STALE'\)[\s\S]*TARGET_AUTHORITATIVE_DECISION_EXISTS/
  );
  assert.match(
    applyResolution,
    /target_resolution\.resolution_identity_key =[\s\S]*carry_row\.canonical_resolution_key/
  );
  assert.match(applyResolution, /'CARRY_SUPERSEDED'/);
});

test('only the canonical carry body is installable and changed economics fail closed', () => {
  const repeatableSql = listFiles('supabase/repeatable')
    .filter((file) => file.toLowerCase().endsWith('.sql'))
    .map((file) => fs.readFileSync(file, 'utf8'))
    .join('\n');
  const carryDefinitions = repeatableSql.match(
    /CREATE OR REPLACE FUNCTION public\.pay_workbench_session_carry_forward_case_resolutions_v1\s*\(/gi
  ) || [];

  assert.equal(
    carryDefinitions.length,
    1,
    'an obsolete repeatable must never be able to restore an older carry body'
  );
  assert.match(
    residual,
    /saved_resolution_economic_fingerprint[\s\S]*current_resolution_economic_fingerprint/
  );
  assert.match(
    residual,
    /verified_resolution_not_stale[\s\S]*verified_resolution_complete/
  );
  assert.match(
    residual,
    /saved_resolution_economic_fingerprint[\s\S]*IS DISTINCT FROM[\s\S]*current_resolution_economic_fingerprint/
  );
});

test('stale full-shift suggestions are rebased to the current correction residual', () => {
  const materializerStart = runtimeGuards.indexOf(
    'create or replace function public._ctms_materialise_candidate_correction_residuals_v1'
  );
  const materializerEnd = runtimeGuards.indexOf(
    '\ncreate or replace function public.',
    materializerStart + 10
  );
  const materializer = runtimeGuards.slice(
    materializerStart,
    materializerEnd > materializerStart
      ? materializerEnd
      : runtimeGuards.length
  );

  assert.ok(materializerStart >= 0);
  assert.match(
    materializer,
    /v_suggestion_current_basis:=round\([\s\S]*v_component_source_outstanding/
  );
  assert.match(
    materializer,
    /v_suggestion_target_per_source:=case[\s\S]*target_amount_ex_vat_per_source_ex_vat/
  );
  assert.match(
    materializer,
    /v_suggestion_current_target_amount:=case[\s\S]*v_suggestion_current_basis[\s\S]*\* v_suggestion_target_per_source/
  );
  assert.match(
    materializer,
    /v_suggestion_scale:=[\s\S]*v_suggestion_current_basis\/v_suggestion_original_basis/
  );
  assert.match(
    materializer,
    /target_amount_ex_vat_per_source_ex_vat[\s\S]*v_suggestion_current_basis/
  );
  assert.match(
    materializer,
    /'correction_residual_basis_rebased',true/
  );
  assert.match(
    materializer,
    /'suggested_resolution_payload_json',v_suggestion_payload[\s\S]*'suggested_resolution_result_json',v_suggestion_result/
  );
  assert.match(
    materializer,
    /'source_pay_ex_vat',case[\s\S]*when v_component_needs_resolution[\s\S]*then v_suggestion_current_basis/
  );
  assert.match(
    materializer,
    /'source_reservation_amount_ex_vat',case[\s\S]*when v_component_needs_resolution[\s\S]*then v_suggestion_current_basis/
  );
  assert.match(
    materializer,
    /'source_charge_ex_vat',case[\s\S]*v_suggested_component->>'source_charge_ex_vat'/
  );

  const sourceResidual = 5.22;
  const sourceToTargetRatio = 40 / 34.78;
  assert.equal(
    Number((sourceResidual * sourceToTargetRatio).toFixed(2)),
    6.00
  );
  assert.notEqual(
    Number((6.5 * 20).toFixed(2)),
    Number((sourceResidual * sourceToTargetRatio).toFixed(2))
  );

  const targetAmount = 40;
  const targetPerSource = 1.1500353857;
  const historicalSourceAmount = 113.04;
  const historicalUnits = 6.5;
  const currentSourceAmount = Math.round(
    (targetAmount / targetPerSource) * 100
  ) / 100;
  const currentUnits = Math.round(
    (historicalUnits * currentSourceAmount / historicalSourceAmount) * 1e6
  ) / 1e6;
  const recomputedTarget = Math.round(
    currentSourceAmount * targetPerSource * 100
  ) / 100;

  assert.equal(currentSourceAmount, 34.78);
  assert.ok(Math.abs(currentUnits - 1.9999) < 0.0001);
  assert.equal(recomputedTarget, 40);
});

test('clearing a live decision ignores frozen history but still blocks active reservations', () => {
  const boundaryStart = clearResolution.indexOf(
    'CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_clear_batch_boundary'
  );
  const boundaryEnd = clearResolution.indexOf(
    "IF v_operation = 'LIST_CLEARABLE'",
    boundaryStart
  );
  const boundary = clearResolution.slice(boundaryStart, boundaryEnd);

  assert.ok(boundaryStart >= 0);
  assert.ok(boundaryEnd > boundaryStart);
  assert.match(
    boundary,
    /public\._pay_batch_status_is_active_reservation\(batch_row\.status\)/
  );
  assert.doesNotMatch(
    boundary,
    /batch_row\.status[\s\S]*NOT IN \('CANCELLED', 'CANCELED'\)/
  );
  assert.match(
    clearResolution,
    /Cleared current live taxable finance case resolution without changing case balance, reservations, settled history, Snooze, fixed components, or frozen Draft items\./
  );
});

test('the correction authority fingerprint excludes circular decision results', () => {
  const fingerprintStart = residual.indexOf(
    'component_fingerprinted AS ('
  );
  const fingerprintEnd = residual.indexOf(
    'component_verified AS (',
    fingerprintStart + 10
  );
  const fingerprint = residual.slice(fingerprintStart, fingerprintEnd);

  assert.ok(fingerprintStart >= 0);
  assert.match(
    fingerprint,
    /'effective_source_outstanding_ex_vat'/
  );
  assert.match(
    fingerprint,
    /'resolution_required'/
  );
  assert.doesNotMatch(
    fingerprint,
    /'target_outstanding_ex_vat'/
  );
  assert.doesNotMatch(
    fingerprint,
    /'resolution_complete'/
  );
  assert.doesNotMatch(
    fingerprint,
    /'resolved_target_amount_ex_vat'/
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
  assert.match(
    freeze,
    /'target_pay_method', n\.pay_channel/
  );
  assert.match(
    freeze,
    /'target_amount_ex_vat', round\(n\.take_target_ex, 2\)/
  );
  assert.match(
    freeze,
    /resolution_row\.source_basis_fingerprint[\s\S]*as current_source_basis_fingerprint/
  );
  assert.match(
    freeze,
    /'source_basis_fingerprint',coalesce\([\s\S]*frozen\.current_source_basis_fingerprint/
  );
  assert.match(
    freeze,
    /'source_build_run_id',coalesce\([\s\S]*batch_item\.frozen_resolution_payload_json[\s\S]*->>'source_build_run_id'[\s\S]*frozen\.current_resolution_payload_json/
  );
});

test('the approved finance-adjustment freeze has one repeatable definition', () => {
  const repeatableFiles = listFiles('supabase/repeatable').filter((file) =>
    /\.(?:sql|txt)$/i.test(file)
  );
  const definitions = [];

  for (const file of repeatableFiles) {
    const source = fs.readFileSync(file, 'utf8');
    const relativePath = path.relative(root, file).replaceAll('\\', '/');
    for (const match of source.matchAll(
      /^CREATE OR REPLACE FUNCTION public\.pay_batch_apply_finance_adjustments\(/gim
    )) {
      definitions.push({ relativePath, index: match.index });
    }
  }

  assert.deepEqual(
    definitions.map(({ relativePath }) => relativePath),
    [
      'supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql',
    ]
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
  assert.match(
    worker,
    /async function assertBankingPayWorkbenchContract[\s\S]*contract = unwrapRpcJsonb\([\s\S]*pay_workbench_contract_version_get_v1/
  );
  assert.match(worker, /workbench_job_claimed: false/);
  assert.match(worker, /mutation_attempted: false/);
  assert.match(
    worker,
    /function bankingPayWorkbenchContractFailure[\s\S]*status: 503[\s\S]*headers: JSON_HEADERS/
  );
  assert.doesNotMatch(
    worker,
    /function bankingPayWorkbenchContractFailure[\s\S]{0,200}\breturn fail\(/
  );
});

test('case-resolution financial boundary follows active reservations, not settled history', () => {
  const applySql = read(
    'supabase/repeatable/21072026_1235_41_pay_workbench_session_apply_case_resolution.sql'
  );
  const boundaryStart = applySql.indexOf(
    'CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_resolution_batch_boundary'
  );
  const boundaryEnd = applySql.indexOf(
    'SELECT COUNT(*)::integer',
    boundaryStart
  );
  assert.ok(boundaryStart >= 0 && boundaryEnd > boundaryStart);
  const boundarySql = applySql.slice(boundaryStart, boundaryEnd);
  assert.equal(
    (boundarySql.match(/public\._pay_batch_status_is_active_reservation\(batch_row\.status\)/g) || []).length,
    2
  );
  assert.doesNotMatch(
    boundarySql,
    /batch_row\.status[\s\S]{0,120}NOT IN \('CANCELLED', 'CANCELED'\)/
  );
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
  assert.match(carrier, /NOTIFY pgrst, 'reload schema'/);
});

test('canonical contract fails closed until durable selection carry is installed', () => {
  assert.match(
    carrier,
    /banking_pay_workbench_selection_carry_registrations/
  );
  assert.match(
    carrier,
    /pay_workbench_session_carry_forward_preview_selections_v1\(uuid,uuid,jsonb\)/
  );
  assert.match(
    carrier,
    /trg_banking_pay_preview_selection_carry_apply/
  );
  assert.match(
    carrier,
    /BANKING_PAY_CANONICAL_CORRECTION_CARRIER_INCOMPLETE/
  );
});

test('shared workbench replacement carries decisions across authorised actors', () => {
  const openSharedStart = sessionSql.indexOf(
    'CREATE OR REPLACE FUNCTION public.pay_workbench_session_open_shared_v2'
  );
  const openSharedEnd = sessionSql.indexOf(
    '\nCREATE OR REPLACE FUNCTION ',
    openSharedStart + 10
  );
  const openShared = sessionSql.slice(openSharedStart, openSharedEnd);

  assert.ok(openSharedStart >= 0);
  assert.doesNotMatch(openShared, /SOURCE_ACTOR_MISMATCH/);
  assert.doesNotMatch(
    openShared,
    /source_session\.actor_user_id\s*=\s*p_actor_user_id/
  );
  assert.match(
    openShared,
    /pay_workbench_session_carry_forward_case_resolutions_v1/
  );
  assert.match(
    openShared,
    /pay_workbench_session_carry_forward_preview_selections_v1/
  );
  assert.ok(
    openShared.indexOf(
      'pay_workbench_session_carry_forward_preview_selections_v1'
    ) < openShared.indexOf('WITH retired_sources AS (')
  );
});

test('selection carry stores pre-draft intent durably without browser grants', () => {
  assert.match(
    selectionCarryMigration,
    /CREATE TABLE IF NOT EXISTS public\.banking_pay_workbench_selection_carry_registrations/
  );
  assert.match(
    selectionCarryMigration,
    /status IN \('PENDING', 'APPLIED', 'SUPERSEDED', 'AMBIGUOUS'\)/
  );
  assert.match(selectionCarryMigration, /ENABLE ROW LEVEL SECURITY/);
  assert.match(
    selectionCarryMigration,
    /REVOKE ALL[\s\S]*FROM PUBLIC, anon, authenticated, service_role/
  );
  assert.match(
    selectionCarryMigration,
    /CREATE TRIGGER trg_banking_pay_preview_selection_carry_apply/
  );
  assert.match(
    selectionCarryRuntime,
    /CREATE OR REPLACE FUNCTION public\._pay_workbench_preview_selection_key_v1/
  );
  assert.match(
    selectionCarryRuntime,
    /CREATE OR REPLACE FUNCTION public\.pay_workbench_session_carry_forward_preview_selections_v1/
  );
  assert.match(
    selectionCarryRuntime,
    /CREATE OR REPLACE FUNCTION public\.trg_banking_pay_preview_selection_carry_apply/
  );
  assert.match(
    selectionCarryRuntime,
    /policy_x_authority_scope', 'PRE_DRAFT_SELECTION_INTENT_ONLY/
  );
  assert.doesNotMatch(
    selectionCarryRuntime,
    /GRANT EXECUTE[\s\S]*TO (?:anon|authenticated|service_role)/
  );
});

test('selection carry restores explicit unselection and selected-row authority by stable component key', () => {
  assert.match(
    selectionCarryRuntime,
    /'CORRECTION'[\s\S]*v_canonical_key/
  );
  assert.match(
    selectionCarryRuntime,
    /'TIMESHEET_COMPONENT'[\s\S]*p_timesheet_id::text[\s\S]*v_key_type[\s\S]*v_key_value/
  );
  assert.match(
    selectionCarryRuntime,
    /IN \('SELECTED', 'UNSELECTED'\)/
  );
  assert.match(
    selectionCarryRuntime,
    /SET selected = v_registration\.selected,[\s\S]*selection_state = v_registration\.selection_state/
  );
  assert.match(
    selectionCarryRuntime,
    /server_selected_preview_row_ids = v_selected_ids/
  );
  assert.match(
    selectionCarryRuntime,
    /'selection_intent_v1'[\s\S]*'canonical_preview_lines'[\s\S]*'mode', 'EXPLICIT_INCLUDE'[\s\S]*'source', 'SESSION_REPLACEMENT_CARRY'/
  );
});

test('deselecting one row converts implicit-all selection into a durable explicit subset', () => {
  const setSelectedStart = sessionSql.indexOf(
    'CREATE OR REPLACE FUNCTION public.pay_workbench_session_set_selected_rows'
  );
  const setSelectedEnd = sessionSql.indexOf(
    'CREATE OR REPLACE FUNCTION',
    setSelectedStart + 1
  );
  const setSelected = sessionSql.slice(
    setSelectedStart,
    setSelectedEnd > setSelectedStart ? setSelectedEnd : undefined
  );

  assert.ok(setSelectedStart >= 0, 'selected-row RPC must exist');
  assert.match(
    setSelected,
    /WHEN jsonb_array_length\(COALESCE\(v_deselect_ids_source, '\[\]'::jsonb\)\) > 0 THEN 'EXPLICIT_INCLUDE'/
  );
  assert.match(
    setSelected,
    /WHEN jsonb_array_length\(COALESCE\(v_select_ids_source, '\[\]'::jsonb\)\) > 0 THEN 'EXPLICIT_INCLUDE'/,
    'an explicit row tick must become durable explicit authority instead of remaining an ambiguous implicit-all selection'
  );
});

test('preview materialisation keeps later correction decisions visible without selecting them for draft', () => {
  const materialiserStart = sessionSql.indexOf(
    'CREATE OR REPLACE FUNCTION public.pay_workbench_preview_rows_materialise_chunk('
  );
  const materialiserEnd = sessionSql.indexOf(
    'CREATE OR REPLACE FUNCTION',
    materialiserStart + 1
  );
  const materialiser = sessionSql.slice(
    materialiserStart,
    materialiserEnd > materialiserStart ? materialiserEnd : undefined
  );

  assert.ok(materialiserStart >= 0, 'preview materialiser must exist');
  assert.doesNotMatch(materialiser, /resolution_anchor_financial_boundaries AS \(/);
  assert.doesNotMatch(materialiser, /suppress_resolution_anchor_financial_boundary/);
  assert.doesNotMatch(materialiser, /RESOLUTION_ANCHOR_ALREADY_IN_NON_CANCELLED_BATCH/);
  assert.match(
    materialiser,
    /component_probe_rows\.target_section = 'canonical_preview_lines'[\s\S]*THEN true[\s\S]*ELSE false[\s\S]*END AS target_selected/
  );
  assert.match(
    runtimeGuards,
    /not exists \([\s\S]*live_component\(value\)[\s\S]*target_outstanding_ex_vat[\s\S]*\) <> 0[\s\S]*and exists \([\s\S]*from public\.pay_batch_items active_batch_item/
  );
});

test('post-draft freshness selects the correction carrier snapshot before family deduplication', () => {
  const wrapperStart = finalFreshnessWrapper.lastIndexOf(
    'CREATE OR REPLACE FUNCTION public.pay_batch_validate_freshness('
  );
  const wrapper = finalFreshnessWrapper.slice(wrapperStart);
  const familyFilter = wrapper.indexOf("LIKE 'correction-chain:%'");
  const snapshotFilter = wrapper.indexOf(
    "jsonb_typeof(\n          COALESCE(\n            pay_batch_item.frozen_source_basis_json->'correction_chain_residual'"
  );
  const distinctResultFilter = wrapper.indexOf(
    "jsonb_typeof(correction_item.frozen_residual) = 'object'"
  );

  assert.ok(wrapperStart >= 0, 'active freshness wrapper must exist');
  assert.ok(familyFilter >= 0, 'correction-chain family filter must exist');
  assert.ok(
    snapshotFilter > familyFilter,
    'frozen chain snapshot must be required in the correction-item source query'
  );
  assert.ok(
    distinctResultFilter > snapshotFilter,
    'the source-row snapshot filter must run before DISTINCT ON result filtering'
  );
  assert.match(
    wrapper,
    /pay_correction_chain_residual_v1\([\s\S]*p_pay_batch_id,[\s\S]*100/
  );
  assert.match(
    finalFreshnessWrapper,
    /REVOKE ALL ON FUNCTION public\.pay_batch_validate_freshness\([\s\S]*FROM PUBLIC, anon, authenticated;[\s\S]*GRANT EXECUTE[\s\S]*TO service_role;/
  );
});

test('post-draft freshness has one public repeatable definition and one private base helper', () => {
  const repeatableFiles = listFiles('supabase/repeatable').filter((file) =>
    /\.(?:sql|txt)$/i.test(file)
  );
  const publicDefinitions = [];
  const privateBaseDefinitions = [];
  const publicDrops = [];

  for (const file of repeatableFiles) {
    const source = fs.readFileSync(file, 'utf8');
    const relativePath = path.relative(root, file).replaceAll('\\', '/');
    for (const match of source.matchAll(
      /^CREATE OR REPLACE FUNCTION public\.pay_batch_validate_freshness\(/gim
    )) {
      publicDefinitions.push({ relativePath, index: match.index });
    }
    for (const match of source.matchAll(
      /^CREATE OR REPLACE FUNCTION public\._pay_batch_validate_freshness_base_v1\(/gim
    )) {
      privateBaseDefinitions.push({ relativePath, index: match.index });
    }
    for (const match of source.matchAll(
      /^DROP FUNCTION IF EXISTS public\.pay_batch_validate_freshness/gim
    )) {
      publicDrops.push({ relativePath, index: match.index });
    }
  }

  assert.deepEqual(
    publicDefinitions.map(({ relativePath }) => relativePath),
    [
      'supabase/repeatable/26072026_1519_pay_batch_validate_freshness_correction_chain_wrapper.sql',
    ]
  );
  assert.deepEqual(
    privateBaseDefinitions.map(({ relativePath }) => relativePath),
    ['supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql']
  );
  assert.deepEqual(publicDrops, []);
});
