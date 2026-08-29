import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = path => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');

const dispatcher = read('supabase/repeatable/04082026_1213_pay_workbench_candidate_source_build_chunk.sql');
const transition = read('supabase/repeatable/05082026_1539_pay_workbench_fact_cursor_transition_v3.sql');
const authority = read('supabase/repeatable/05082026_1540_pay_workbench_financial_source_authority_v3.sql');
const continuation = read('supabase/repeatable/05082026_1541_pay_workbench_physical_source_continuation_v1.sql');
const expected = read('supabase/repeatable/05082026_1403_pay_workbench_presentation_allocation_expected_v1.sql');
const canonical = read('supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql');

test('V1.2.10 every family and unit transition produces a complete consumer-valid cursor', () => {
  assert.equal((dispatcher.match(/pay_workbench_fact_cursor_transition_v3\s*\(/g) ?? []).length, 4);
  assert.match(dispatcher, /v_unit_families\[v_family_ordinal\+1\]/);
  assert.match(dispatcher, /v_global_families\[v_family_ordinal\+1\]/);
  assert.match(transition, /v_unit_key='GLOBAL'.*v_input_phase<>'PHYSICAL_SOURCE'/s);
  assert.match(transition, /v_fact_family='LIVE_ENTITLEMENT_INPUT'.*v_input_phase<>'PROJECTION'/s);
  assert.match(transition, /v_fact_family<>'LIVE_ENTITLEMENT_INPUT'.*v_input_phase<>'COMPONENTS'/s);
  assert.match(transition, /RETURN private\.pay_workbench_fact_cursor_preserve_v2\(v_result\)/);
  assert.match(transition, /'input_projection_id',p_input_projection_id/);
});

test('V1.2.10 physical source continuation keeps projection transitions separate from raw lookahead', () => {
  assert.match(dispatcher, /v_raw_source_has_more:=COALESCE\(\s*\(v_occurrence_bundle->>'raw_source_has_more'\)::boolean,false\);\s*v_has_more:=v_raw_source_has_more/s);
  assert.match(dispatcher, /pay_workbench_physical_source_continuation_v1\(\s*v_raw_source_has_more,\s*COALESCE\(\(v_occurrence_bundle->>'raw_source_exhausted'\)::boolean,false\),\s*v_projection_family_transition\)/s);
  assert.match(continuation, /v_raw_exhausted IS DISTINCT FROM NOT v_raw_has_more/);
  assert.match(continuation, /RETURN v_raw_has_more OR v_another_projection/);
  assert.doesNotMatch(dispatcher, /raw_source_exhausted'\)::boolean,false\) IS DISTINCT FROM NOT v_has_more/);
});

test('V1.2.10 reservation and linked-item frozen documents are independent authority sources', () => {
  assert.match(dispatcher, /pay_workbench_financial_source_authority_v3\(/);
  assert.match(dispatcher, /'source','RESERVATION'.*reservation\.frozen_source_basis_json/s);
  assert.match(dispatcher, /'source','PAY_BATCH_ITEM'.*item\.frozen_source_basis_json/s);
  assert.match(dispatcher, /'source','RESERVATION'.*reservation\.frozen_component_snapshot_json/s);
  assert.match(dispatcher, /'source','PAY_BATCH_ITEM'.*item\.frozen_component_snapshot_json/s);
  assert.doesNotMatch(dispatcher, /COALESCE\(reservation\.frozen_(?:source_basis|component_snapshot)_json\s*,\s*item\.frozen_/);
  assert.match(authority, /v_basis_documents jsonb:=COALESCE\(p_frozen_source_basis_documents/);
  assert.match(authority, /v_snapshot_documents jsonb:=COALESCE\(p_frozen_component_snapshot_documents/);
  assert.match(authority, /jsonb_array_elements\(v_basis_documents\)/);
  assert.match(authority, /jsonb_array_elements\(v_snapshot_documents\)/);
  assert.match(authority, /v_context\|\|'_OWNER_CONFLICT'/);
  assert.match(authority, /RESERVATION_COMPONENT_CONFLICT/);
  assert.match(authority, /RESERVATION_COMPONENT_KEY_CONFLICT/);
});

test('V1.2.10 private presentation state retains zero-net and indefinite-snooze targets before public suppression', () => {
  const seedStart = canonical.indexOf('create temporary table canonical_timesheet_lines');
  const seedEnd = canonical.indexOf('create temporary table canonical_timesheet_segment_rows');
  assert.ok(seedStart >= 0 && seedEnd > seedStart);
  const seed = canonical.slice(seedStart, seedEnd);
  assert.match(seed, /from timesheet_case_rollup_effective tcr/);
  assert.doesNotMatch(seed, /round\(coalesce\(tcr\.payment_amount_ex_vat,0\),2\)\s*<>\s*0/);
  assert.doesNotMatch(seed, /ats\.snooze_until_date\s+is\s+not\s+null/i);

  assert.match(canonical, /has_ready_presentation = true\s+and ctpp\.is_ready_for_draft = true\s+and round\(coalesce\(ctpp\.amount_ex_vat,0\),2\) <> 0/s);
  assert.match(canonical, /has_blocked_presentation = true\s+and round\(coalesce\(ctpp\.amount_ex_vat,0\),2\) <> 0\s+and not \(ctpp\.snooze_id is not null and ctpp\.snooze_until_date is null\)/s);
  assert.match(expected, /v_has_ready:=v_total_amount<>0/);
  assert.match(expected, /v_whole_snooze_indefinite:=v_has_whole_snooze AND v_snooze_until_date IS NULL/);
  assert.match(expected, /v_has_blocked:=v_total_amount<>0/);
});

test('V1.2.10 preserves public function signatures and defaults', () => {
  assert.match(dispatcher, /CREATE OR REPLACE FUNCTION public\.pay_workbench_candidate_source_build_chunk\(\s*p_session_id uuid,\s*p_candidate_id uuid,\s*p_cursor_json jsonb DEFAULT NULL::jsonb,\s*p_payload_json jsonb DEFAULT '\{\}'::jsonb,\s*p_limit integer DEFAULT 100\s*\)/s);
  assert.match(canonical, /CREATE OR REPLACE FUNCTION public\.pay_preview_candidate_build_canonical_lines\(p_context_json jsonb, p_candidate_id uuid\)/);
});
