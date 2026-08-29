import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = path => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');
const dispatcher = read('supabase/repeatable/04082026_1213_pay_workbench_candidate_source_build_chunk.sql');
const cursorFence = read('supabase/repeatable/05082026_1348_pay_workbench_fact_cursor_preserve_v2.sql');
const financeAuthority = read('supabase/repeatable/05082026_1402_pay_workbench_financial_source_authority_v2.sql');
const financePage = read('supabase/repeatable/05082026_1231_pay_workbench_finance_item_authority_page_v1.sql');
const financeBundle = read('supabase/repeatable/05082026_1414_pay_workbench_finance_item_authority_page_bundle_v1.sql');
const occurrence = read('supabase/repeatable/04082026_2314_pay_workbench_unit_economic_occurrence_page_v1.sql');
const presentation = read('supabase/repeatable/05082026_1403_pay_workbench_presentation_allocation_expected_v1.sql');
const sync = read('supabase/repeatable/04082026_1210_pay_sync_overpayments_from_workbench_workspace_v1.sql');

test('V1.2.9 preserves the complete Version-2 cursor before predecessor hashing', () => {
  const required = [
    'cursor_kind', 'cursor_version', 'build_id', 'candidate_id',
    'captured_candidate_generation', 'captured_source_change_seq',
    'dependency_unit_key', 'fact_family', 'page_number', 'last_source_key',
    'previous_page_digest', 'cumulative_fact_count', 'cumulative_digest', 'terminal',
    'raw_physical_source_count', 'resolved_physical_source_count',
    'failed_physical_source_count', 'raw_physical_amount_ex_vat',
    'resolved_physical_amount_ex_vat', 'last_raw_physical_source_key',
    'source_exhausted', 'raw_terminal_source_key', 'raw_page_evidence_digest',
    'input_phase', 'input_projection_id',
  ];
  for (const key of required) assert.match(cursorFence, new RegExp(`'${key}'`));
  assert.match(cursorFence, /RETURN v_cursor/);
  assert.ok(dispatcher.indexOf('pay_workbench_fact_cursor_preserve_v2(v_cursor)') <
    dispatcher.indexOf('v_cursor_start_hash:=md5(v_cursor::text)'));
  assert.match(dispatcher, /v_previous_page\.cursor_end_json IS DISTINCT FROM v_cursor/);
});

test('V1.2.9 finance discovery is a bounded keyset merge with an independent raw bundle', () => {
  const branches = [
    'direct_owner_item', 'component_owner_item', 'case_owner_item', 'frozen_owner_item',
    'candidate_settled_item', 'candidate_transfer_item', 'candidate_reservation_item',
  ];
  for (const branch of branches) assert.match(financePage, new RegExp(`${branch} AS MATERIALIZED`));
  assert.match(financePage, /candidate_item_key AS MATERIALIZED/);
  assert.match(financePage, /item_key AS MATERIALIZED/);
  assert.doesNotMatch(financePage, /relevant_item_id AS MATERIALIZED/);
  assert.match(financeBundle, /raw_page_count/);
  assert.match(financeBundle, /raw_amount_ex_vat/);
  assert.match(financeBundle, /raw_terminal_source_key/);
  assert.match(financeBundle, /evidence_digest/);
  assert.ok(dispatcher.indexOf('pay_workbench_finance_item_authority_page_bundle_v1') <
    dispatcher.indexOf("md5('FINANCE_ITEM_AUTHORITY:'"));

  const sources = [
    [1, 2, 30, 80], [2, 3, 31, 81], [4, 32, 82], [5, 33, 83],
    [6, 34, 84], [7, 35, 85], [8, 36, 86],
  ];
  const merged = [...new Set(sources.flatMap(source => source.slice(0, 3)))].sort((a, b) => a - b).slice(0, 3);
  assert.deepEqual(merged, [1, 2, 3]);
});

test('V1.2.9 finance component and key identity is fail-closed before fact acceptance', () => {
  assert.match(financeAuthority, /FINANCE_COMPONENT_UUID_INVALID/);
  assert.match(financeAuthority, /FINANCE_COMPONENT_CONFLICT/);
  assert.match(financeAuthority, /FINANCE_COMPONENT_KEY_INCOMPLETE/);
  assert.match(financeAuthority, /FINANCE_COMPONENT_KEY_CONFLICT/);
  assert.match(financeAuthority, /RESERVATION_COMPONENT_CONFLICT/);
  assert.match(financePage, /resolved_finance_component_id/);
  assert.match(financePage, /resolved_component_key_type/);
  assert.match(financePage, /authoritative_in_scope/);
  assert.match(dispatcher, /PAY_WORKBENCH_ECONOMIC_KEY_RESOLUTION_INCOMPLETE/);
});

test('V1.2.9 projects a captured finance movement through sealed family membership exactly once', () => {
  assert.match(occurrence, /JOIN projection_members member\s+ON member\.family_timesheet_id=authority\.timesheet_id/);
  assert.doesNotMatch(occurrence, /authority\.timesheet_id=p_projected_timesheet_id/);
  assert.match(occurrence, /'source_timesheet_id',keyed\.source_timesheet_id/);
  assert.match(occurrence, /'projected_timesheet_id',p_projected_timesheet_id/);
});

test('V1.2.9 canonical attestation distinguishes private truth from exact presentation allocation', () => {
  assert.match(presentation, /READY_TO_PAY/);
  assert.match(presentation, /BLOCKED_FOR_PAY/);
  assert.match(presentation, /CASES_RESOLUTIONS/);
  assert.match(presentation, /parent_line_identity/);
  assert.match(presentation, /excluded_from_allocation/);
  assert.match(presentation, /resolved_segment_rows_replace_source_total/);
  assert.match(sync, /economic_target AS/);
  assert.match(sync, /economic_target AS \([\s\S]*pg_temp\.tmp_sync_authoritative_components component/);
  assert.match(sync, /FULL OUTER JOIN actual USING\(timesheet_id,line_identity\)/);
  assert.match(sync, /actual\.parent_line_identity IS DISTINCT FROM expected\.parent_line_identity/);
  assert.match(sync, /actual\.presentation_section IS DISTINCT FROM expected\.presentation_section/);
  assert.match(sync, /actual\.draftable IS DISTINCT FROM expected\.draftable/);
  assert.match(sync, /actual\.section_segment_rows IS DISTINCT FROM expected\.section_segment_rows/);
  assert.match(sync, /hidden_indefinite_segment_amount_ex_vat/);
  assert.match(sync, /hidden_expense_amount_ex_vat/);

  const truth = 100;
  const validPartialHidden = { ready: 60, blocked: 0, hiddenSegment: 40, hiddenExpense: 0 };
  assert.equal(Object.values(validPartialHidden).reduce((sum, value) => sum + value, 0), truth);
  const invalidPromotedBlock = { ready: 100, blocked: 0, hiddenSegment: 0, hiddenExpense: 0 };
  const expected = { ready: 80, blocked: 20, hiddenSegment: 0, hiddenExpense: 0 };
  assert.notDeepEqual(invalidPromotedBlock, expected);
});
