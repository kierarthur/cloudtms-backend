import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8')
  .replaceAll('\r\n', '\n');

const owner = read('supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql');
const verifier = read('supabase/verification/15092026_1534_weekly_source_finalisation_v1.sql');
const schema = read('supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql');
const lineageOwner = read('supabase/repeatable/15092026_1534_weekly_source_timesheet_lineage_v1.sql');
const projectionVerifier = read('supabase/verification/15092026_1534_weekly_source_projection_build_v1.sql');
const projectionOwner = read('supabase/repeatable/15092026_1534_weekly_source_projection_build_v1.sql');

const position = (source, value, label) => {
  const found = source.indexOf(value);
  assert.notEqual(found, -1, `${label} is missing`);
  return found;
};

test('weekly source finalisation exposes only the service orchestration RPC', () => {
  assert.equal(
    (owner.match(/create or replace function public\.weekly_source_finalise_atomic_v1\(\s*p_request jsonb\s*\)/gi) || []).length,
    1,
  );
  assert.match(owner, /revoke all on function public\.weekly_source_finalise_atomic_v1\(jsonb\)\s+from public,anon,authenticated;/i);
  assert.match(owner, /grant execute on function public\.weekly_source_finalise_atomic_v1\(jsonb\) to service_role;/i);
  assert.match(owner, /notify pgrst, 'reload schema';/i);
  assert.doesNotMatch(owner, /\bMAGNIT\b/i);
});

test('lock order seals one current publication and one logical authority history', () => {
  const timeout = position(owner, "set_config('lock_timeout','5s',true)", 'bounded lock timeout');
  const cycleLock = position(owner, 'from public.weekly_source_cycles where id=v_cycle_id for update', 'cycle lock');
  const publicationGuard = position(owner, 'weekly_source_current_publication_guard_v1', 'current-publication guard');
  const orderLock = position(owner, "'weekly_source_finalise_order:'||v_group.id::text||':'||v_client_id::text||':'||v_source_profile_kind", 'logical-history lock');
  const currentRevision = position(owner, 'if coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id) is not null then', 'current-revision decision');
  const orderingGate = position(owner, "raise exception 'WEEKLY_SOURCE_FINALISATION_OUT_OF_ORDER'", 'out-of-order gate');
  const lineage = position(owner, 'weekly_source_timesheet_lineage_ensure_atomic_v1', 'Timesheet-lineage owner');
  assert.ok(timeout < cycleLock);
  assert.ok(cycleLock < publicationGuard);
  assert.ok(publicationGuard < orderLock);
  assert.ok(orderLock < currentRevision);
  assert.ok(currentRevision < orderingGate);
  assert.ok(orderingGate < lineage);
});

test('generic and HealthRoster paths are source-state transitions with C1 roster identity', () => {
  assert.match(owner, /'GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS'/);
  assert.match(owner, /when v_transition\.prior_snapshot_id is null then 'ADD'[\s\S]*when v_transition\.current_snapshot_id is null then 'CANCEL'[\s\S]*when v_prior_state=v_current_state then 'NO_CHANGE'[\s\S]*else 'AMEND'/);
  assert.match(owner, /v_source_mode:='HEALTHROSTER_WEEKLY'/);
  assert.match(owner, /source_row\.row_finalisation_state='SOURCE_WORKED'[\s\S]*row_finalisation_state='NOT_APPLICABLE'/);
  assert.doesNotMatch(
    owner.slice(
      position(owner, '-- The source Timesheet owner', 'lineage-loop start'),
      position(owner, "if v_scope_kind='CYCLE' then", 'prior-revision lookup'),
    ),
    /SOURCE_ABSENT_ZERO/,
  );
  assert.match(verifier, /zero-valued SOURCE_ABSENT_ZERO must not create Timesheet lineage/);
  assert.match(lineageOwner, /WEEKLY_SOURCE_ZERO_HOUR_EXPENSE_LINEAGE_INVALID/);
  assert.match(
    projectionVerifier,
    /sqlerrm is distinct from 'WEEKLY_SOURCE_ZERO_HOUR_EXPENSE_LINEAGE_INVALID'/,
  );
  assert.match(verifier, /WEEKLY_SOURCE_FINALISATION_OUT_OF_ORDER/);
});

test('source-fixed expenses use immutable policy provenance and the same ordinary source root', () => {
  assert.match(owner, /weekly_source_row_expense_policy_snapshots/);
  assert.match(owner, /row_expense_policy_snapshot_id/);
  assert.match(owner, /source_observation_kind/);
  assert.match(owner, /OMITTED_IN_COMPLETE_COVERAGE/);
  assert.match(schema, /VALIDATED_SOURCE_PENCE/);
  assert.match(verifier, /worked source hours and their source-fixed expense must share one ordinary Weekly HOURS root/);
  assert.match(verifier, /expense-only zero-hours authority must create no worked source position/);
  assert.match(verifier, /expense-only authority must use exact source cents on the ordinary Weekly HOURS root/);
  assert.match(verifier, /explicit source expense zero must reverse the prior expense without worked time/);
  assert.match(verifier, /complete-coverage omission must clear the latest expense-only authority/);
  assert.match(verifier, /disabled source-fixed expenses must not contaminate any expense or invoice route/);
  assert.match(verifier, /absent expense snapshot was accepted/);
  assert.match(verifier, /duplicate expense snapshot was accepted/);
  assert.match(verifier, /stale expense policy was accepted/);
  assert.match(verifier, /tampered expense snapshot was accepted/);
  assert.match(verifier, /zero-hours row with worked snapshot was accepted/);
  assert.match(verifier, /zero-valued source expense created lineage/);
  assert.equal(
    (verifier.match(/disable trigger weekly_source_immutable_record_guard/gi) || []).length,
    2,
  );
  assert.equal(
    (verifier.match(/enable trigger weekly_source_immutable_record_guard/gi) || []).length,
    2,
  );
});

test('NHSP consumes exact signed physical rows, admits bound warnings and blocks unsafe pricing', () => {
  assert.match(owner, /v_profile\.omission_meaning<>'NO_INFERENCE'/);
  assert.doesNotMatch(owner, /source_shift_charge_pence is null\s+or source_row\.source_shift_charge_pence=0/);
  // Plan 6.2 `25 §6`, `24 §13`, `14 §4.3` items 1 and 5, NHSP-BR-013 and
  // PRC-007 replace the Plan 6 directional band with a symmetric one, so the
  // gate is pinned to the same-non-zero-sign absolute-one-penny form.
  assert.match(
    owner,
    /comparison_result='SOURCE_ROUNDING_EQUIVALENT'\s+and v_charge_check\.row_sign_kind in \('POSITIVE','FULL_NEGATIVE'\)/,
  );
  assert.match(
    owner,
    /source_charge_difference_pence=1\s+or v_charge_check\.source_charge_difference_pence=-1/,
  );
  assert.match(
    owner,
    /source_shift_charge_pence>0\s+and v_charge_check\.calculated_segment_charge_pence>0/,
  );
  assert.match(
    owner,
    /source_shift_charge_pence<0\s+and v_charge_check\.calculated_segment_charge_pence<0/,
  );
  assert.doesNotMatch(owner, /row_sign_kind='POSITIVE'\s+and v_charge_check\.source_charge_difference_pence=1/);
  assert.doesNotMatch(owner, /row_sign_kind='FULL_NEGATIVE'\s+and v_charge_check\.source_charge_difference_pence=-1/);
  assert.match(owner, /raise exception 'WEEKLY_SOURCE_NHSP_PRICE_GATE_FAILED'/);
  assert.match(owner, /'ACCEPTED_DISPARITY','ACCEPTED_ZERO'/);
  assert.match(owner, /weekly_source_charge_acceptances/);
  assert.match(owner, /weekly_source_charge_acceptance_policy_fingerprint_v1/);
  assert.match(owner, /comparison_result='ZERO_SOURCE_CHARGE'[\s\S]*source_shift_charge_pence=0[\s\S]*calculated_segment_charge_pence>0/);
  assert.match(
    owner,
    /case when v_row\.source_shift_charge_pence<0 then -1 else 1 end/,
  );
  assert.match(
    projectionOwner,
    /case when coalesce\(v_source_row\.source_shift_charge_pence,0\)<0 then -1 else 1 end/,
  );
  assert.match(owner, /v_row\.source_shift_charge_pence,v_row\.source_shift_charge_pence,/);
  assert.match(owner, /backing_report_number/);
  assert.match(verifier, /first-appearance physical NHSP full-negative/);
  assert.match(verifier, /NHSP omission must never infer a cancellation or synthetic movement/);
  assert.match(verifier, /NHSP finalisation price blocker must roll back every finalisation-side effect/);
});

test('the projection owner re-derives the source-price verdict server-side', () => {
  // G6-2: the database must not trust the caller's comparison_result.
  assert.match(projectionOwner, /WEEKLY_SOURCE_CHARGE_CHECK_RESULT_NOT_REDERIVED/);
  assert.match(projectionOwner, /WEEKLY_SOURCE_CHARGE_CHECK_SIGN_NOT_REDERIVED/);
  assert.match(projectionOwner, /WEEKLY_SOURCE_QUALIFICATION_RESULT_NOT_REDERIVED/);
  assert.match(projectionOwner, /WEEKLY_SOURCE_QUALIFICATION_SOURCE_PENCE_MISMATCH/);
  assert.equal(
    (projectionOwner.match(/when \(v_charge_difference_pence=1 or v_charge_difference_pence=-1\)/g) || []).length,
    2,
  );
  assert.equal(
    (projectionOwner.match(/and \(\(v_charge_source_pence>0 and v_charge_calculated_pence>0\)/g) || []).length,
    2,
  );
  assert.equal(
    (projectionOwner.match(/or \(v_charge_source_pence<0 and v_charge_calculated_pence<0\)\)/g) || []).length,
    2,
  );
  // The re-derived verdict, not the caller's arithmetic, is what is stored.
  assert.match(projectionOwner, /'NHSP_TWO_COMPONENT_PENCE_V1',v_charge_claimed_result,/);
  assert.doesNotMatch(projectionOwner, /'NHSP_TWO_COMPONENT_PENCE_V1',pg_catalog\.upper\(v_charge->>'comparison_result'\)/);
});

test('the projection owner proves the Contract selection method server-side', () => {
  // G6-4: 24 §8 forbids a chooser when only one Contract is eligible, and a
  // durable-lineage selection must be provable from a prior resolved mapping.
  assert.match(projectionOwner, /WEEKLY_SOURCE_AUTO_UNIQUE_NOT_UNIQUE/);
  assert.match(projectionOwner, /WEEKLY_SOURCE_OFFICE_CHOICE_NOT_WARRANTED/);
  assert.match(projectionOwner, /WEEKLY_SOURCE_DURABLE_LINEAGE_UNPROVEN/);
  assert.match(
    projectionOwner,
    /v_selection_method='AUTO_UNIQUE'\s+and pg_catalog\.jsonb_array_length\(v_eligible_ids\)<>1/,
  );
  assert.match(
    projectionOwner,
    /v_selection_method='OFFICE_SELECTED'\s+and pg_catalog\.jsonb_array_length\(v_eligible_ids\)<2/,
  );
  assert.match(projectionOwner, /prior_resolution\.mapping_state='RESOLVED'/);
  assert.match(projectionOwner, /prior_resolution\.contract_id=v_contract_id/);
});

test('finalisation remains isolated from protected pay, queries and Banking Pay', () => {
  assert.doesNotMatch(owner, /weekly_protected_pay/i);
  assert.doesNotMatch(owner, /weekly_query/i);
  assert.doesNotMatch(owner, /pay_batch/i);
  assert.doesNotMatch(owner, /banking_pay/i);
  assert.match(owner, /weekly_source_billing_movements/);
  assert.match(owner, /placement_state/);
});
