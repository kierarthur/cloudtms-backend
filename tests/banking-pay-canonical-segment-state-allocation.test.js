import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..');
const canonical = fs.readFileSync(
  path.join(root, 'supabase', 'repeatable', '05082026_1545_pay_preview_candidate_build_canonical_lines.sql'),
  'utf8'
);

test('exact hidden or blocked timesheet segments cannot emit Ready allocation children', () => {
  assert.match(canonical, /component_rows\.component_key_type <> 'TS_DAY'[\s\S]*not exists\s*\([\s\S]*canonical_timesheet_segment_rows as exact_non_ready_segment[\s\S]*exact_non_ready_segment\.segment_stable_key is not distinct from component_rows\.stable_component_identity[\s\S]*exact_non_ready_segment\.presentation_segment_state in \('BLOCKED_VISIBLE', 'HIDDEN_INDEFINITE'\)/i);
});

test('missing and ambiguous exact identities still fail closed', () => {
  assert.match(canonical, /segment_row\.segment_base_json as segment_json/i);
  assert.doesNotMatch(canonical, /segment_row\.segment_json\b/i);
  assert.match(canonical, /'ref_num', eligible_components\.matched_segment_json->'ref_num'\s*\)\s*\|\| jsonb_build_object\(\s*'case_resolution_summary'/i);
  assert.match(canonical, /segment_row\.presentation_segment_state = 'READY'/i);
  assert.match(canonical, /PAY_WORKBENCH_ALLOCATION_SEGMENT_IDENTITY_MISSING/i);
  assert.match(canonical, /PAY_WORKBENCH_ALLOCATION_SEGMENT_IDENTITY_AMBIGUOUS/i);
  assert.match(canonical, /segment_match_count = 0/i);
  assert.match(canonical, /segment_match_count > 1/i);
});

test('non-ready segment economics remain in protected presentation totals', () => {
  assert.match(canonical, /sum\(case when ctsr\.presentation_segment_state = 'BLOCKED_VISIBLE' then ctsr\.presentation_amount_ex_vat else 0 end\)/i);
  assert.match(canonical, /sum\(case when ctsr\.presentation_segment_state = 'HIDDEN_INDEFINITE' then ctsr\.presentation_amount_ex_vat else 0 end\)/i);
  assert.match(canonical, /hidden_indefinite_segment_amount_ex_vat/i);
  assert.match(canonical, /blocked_visible_segment_amount_ex_vat/i);
});

test('the correction does not redefine James rate or post-Draft owners', () => {
  for (const frozenOwner of [
    'pay_workbench_unit_economic_occurrence_page_v1',
    'pay_workbench_sealed_rate_component_projection_v1',
    'pay_sync_overpayments_from_workbench_workspace_v1',
    'pay_workbench_session_apply_case_resolution',
    'pay_workbench_session_clear_case_resolution',
    'banking_pay_draft_create_step_v1',
    'pay_payment_execute',
    'pay_payment_settle'
  ]) {
    assert.doesNotMatch(
      canonical,
      new RegExp(`CREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\s+(?:public|private)\\.${frozenOwner}\\b`, 'i')
    );
  }
});
