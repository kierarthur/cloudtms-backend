import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = path => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');

const authority = read('supabase/repeatable/05082026_1540_pay_workbench_financial_source_authority_v3.sql');
const baseAuthority = read('supabase/repeatable/05082026_1229_pay_workbench_financial_source_authority_v1.sql');
const financePage = read('supabase/repeatable/05082026_1231_pay_workbench_finance_item_authority_page_v1.sql');
const canonical = read('supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql');
const verification = read('supabase/verification/05082026_1638_banking_pay_bounded_scope_v1211_semantics.sql');

test('V1.2.11 preserves every component-key alias as an independent assertion', () => {
  const assertionStart = authority.indexOf('), supplied_assertion AS (');
  const assertionEnd = authority.indexOf('), all_assertion AS (');
  assert.ok(assertionStart >= 0 && assertionEnd > assertionStart);
  const assertions = authority.slice(assertionStart, assertionEnd);

  assert.match(assertions, /assertion\.value->>'component_key_type'[\s\S]*assertion\.value->>'key_type'/);
  assert.doesNotMatch(assertions, /COALESCE\(assertion\.value->>'(?:component_)?key_type'/);

  const paths = [
    "document.body->>'component_key_type'",
    "document.body->>'key_type'",
    "document.body#>>'{economic_key,component_key_type}'",
    "document.body#>>'{economic_key,key_type}'",
    "document.body#>>'{source_basis_json,component_key_type}'",
    "document.body#>>'{source_basis_json,key_type}'",
    "document.body#>>'{source_basis_json,economic_key,component_key_type}'",
    "document.body#>>'{source_basis_json,economic_key,key_type}'",
    "document.body#>>'{component,component_key_type}'",
    "document.body#>>'{component,key_type}'"
  ];
  for (const path of paths) assert.ok(assertions.includes(path), `${path} must be independently asserted`);
  assert.doesNotMatch(assertions, /COALESCE\(document\.body(?:->>|#>>).*?(?:component_key_type|key_type)/s);
  assert.match(authority, /INTO v_incomplete_key_count,component_key_pairs/);
  assert.match(authority, /cardinality\(component_key_pairs\)>1/);
});

test('V1.2.11 public timesheet rollups derive from emitted public timesheet lines', () => {
  const rollupStart = canonical.indexOf('create temporary table candidate_preview_timesheet_rollup');
  const rollupEnd = canonical.indexOf('v_canonical_elapsed_ms', rollupStart);
  assert.ok(rollupStart >= 0 && rollupEnd > rollupStart);
  const rollup = canonical.slice(rollupStart, rollupEnd);

  assert.match(rollup, /with emitted_public_timesheets as \(/i);
  assert.match(rollup, /from timesheet_canonical_preview_lines tcpl/);
  assert.match(rollup, /line_type',''\)\) = 'TIMESHEET_PAYMENT'/);
  assert.match(rollup, /presentation_section',''\)\) = 'READY_TO_PAY'/);
  assert.match(rollup, /presentation_section',''\)\) = 'BLOCKED_FOR_PAY'/);
  assert.match(rollup, /sum\(ept\.ready_public_amount_ex_vat\)/);
  assert.match(rollup, /coalesce\(ept\.has_blocked_public_line, false\) = true/);
  assert.match(rollup, /coalesce\(ept\.has_ready_public_line, false\) = true/);
  assert.match(rollup, /filter \(where coalesce\(ept\.has_ready_public_line, false\) = true\)/);
  assert.doesNotMatch(rollup, /count\(\*\) filter \(\s*where ctpp\.has_blocked_presentation = true/s);
  assert.doesNotMatch(rollup, /sum\(case when ctpp\.has_active_timesheet_snooze/);
});

test('V1.2.11 executable verification covers alias conflicts and hidden public-state parity', () => {
  assert.match(verification, /V1211_RESERVATION_ALIAS_CONFLICT_NOT_DETECTED/);
  assert.match(verification, /V1211_FINANCE_NESTED_ALIAS_CONFLICT_NOT_DETECTED/);
  assert.match(verification, /V1211_INCOMPLETE_ALIAS_NOT_DETECTED/);
  assert.match(verification, /V1211_PUBLIC_TIMESHEET_ROLLUP_MISMATCH/);
  assert.match(verification, /private_state_count/);
  assert.match(verification, /ready_public_amount_ex_vat/);
});

test('V1.2.11 preserves function signatures, ownership and Policy X boundary', () => {
  assert.match(authority, /CREATE OR REPLACE FUNCTION private\.pay_workbench_financial_source_authority_v3\(/);
  assert.match(authority, /SECURITY DEFINER\s+SET search_path = ''/s);
  assert.match(authority, /Policy X remains frozen post-draft authority/);
  assert.match(canonical, /CREATE OR REPLACE FUNCTION public\.pay_preview_candidate_build_canonical_lines\(p_context_json jsonb, p_candidate_id uuid\)/);
  assert.match(canonical, /SET search_path TO 'public'/);
});

test('V1.2.16 settled rotation evidence selects frozen ownership only inside one sealed unit', () => {
  assert.match(baseAuthority, /'frozen_owner_role'/);
  assert.match(baseAuthority, /'frozen_owner_id',v_frozen_owner/);
  assert.match(financePage, /pay_workbench_financial_source_authority_v2\(/);
  assert.match(financePage, /authority_failure='FINANCE_OWNER_CONFLICT'/);
  assert.match(financePage, /authority_owner_in_scope_count=\s*scoped_authority\.authority_owner_count/);
  assert.match(financePage, /authority_dependency_unit_count=1/);
  assert.match(financePage, /FROZEN_OWNER_SELECTED_SAME_DEPENDENCY_UNIT/);
  assert.match(financePage, /THEN resolution_policy\.frozen_owner_id\s+ELSE resolution_policy\.resolved_timesheet_id/);
  assert.match(financePage, /cardinality\(COALESCE\(scoped_authority\.component_key_pairs,ARRAY\[\]::text\[\]\)\)=1/);
});

test('V1.2.16 ownerless terminal history remains explicit evidence and cannot bypass active authority', () => {
  assert.match(financePage, /legacy_terminal_unowned_evidence/);
  assert.match(financePage, /authority_failure='FINANCE_OWNER_UNRESOLVED'/);
  assert.match(financePage, /case_status,''\)\)\)='PAID_OFF'/);
  assert.match(financePage, /case_outstanding_amount,0\),2\)=0/);
  assert.match(financePage, /NOT EXISTS\(SELECT 1\s+FROM public\.pay_finance_case_components legacy_component/s);
  assert.match(financePage, /NOT EXISTS\(SELECT 1\s+FROM public\.pay_advance_reservations legacy_reservation/s);
  assert.match(financePage, /NOT EXISTS\(SELECT 1\s+FROM public\.pay_payment_correction_items legacy_correction/s);
  assert.match(financePage, /NOT EXISTS\(SELECT 1\s+FROM public\.pay_item_snoozes legacy_snooze/s);
  assert.match(financePage, /NOT EXISTS\(SELECT 1\s+FROM public\.pay_finance_case_events legacy_event/s);
  assert.match(financePage, /LEGACY_TERMINAL_UNOWNED_EVIDENCE/);
  assert.match(financePage, /'authority_resolution_status',projected\.authority_resolution_status/);
  assert.match(financePage, /'source_timesheet_id',projected\.effective_timesheet_id/);
});
