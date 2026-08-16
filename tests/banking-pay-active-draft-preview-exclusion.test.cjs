const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const repeatableDir = path.join(root, 'supabase', 'repeatable');
const historicalFunctionsPath = path.join(
  repeatableDir,
  '26052026_2100HRS_NEW_FUNCTIONS.sql'
);
const historicalFunctionsSql = fs.readFileSync(historicalFunctionsPath, 'utf8');
const currentCandidatePreviewPath = path.join(
  repeatableDir,
  '16082026_2035_pay_workbench_candidate_preview_effective_section_v1.sql'
);
const currentCandidatePreviewSql = fs.readFileSync(currentCandidatePreviewPath, 'utf8');
const currentPreviewPagePath = path.join(
  repeatableDir,
  '20072026_0117_banking_pay_preview_selection_revision.sql'
);
const currentPreviewPageSql = fs.readFileSync(currentPreviewPagePath, 'utf8');

function functionBody(source, qualifiedName) {
  const startPattern = new RegExp(
    `CREATE OR REPLACE FUNCTION\\s+${qualifiedName.replace('.', '\\.')}\\s*\\(`,
    'i'
  );
  const startMatch = startPattern.exec(source);
  assert.ok(startMatch, `${qualifiedName} must exist`);
  const end = source.indexOf('$function$;', startMatch.index);
  assert.ok(end > startMatch.index, `${qualifiedName} must have a complete body`);
  return source.slice(startMatch.index, end + '$function$;'.length);
}

test('candidate preview preserves the existing bounded four-argument contract', () => {
  assert.match(
    currentCandidatePreviewSql,
    /CREATE OR REPLACE FUNCTION public\.pay_workbench_session_get_candidate_preview\(\s*p_session_id uuid,\s*p_candidate_id uuid,\s*p_cursor_json jsonb DEFAULT '\{\}'::jsonb,\s*p_limit integer DEFAULT 100\s*\)/
  );
  assert.match(
    historicalFunctionsSql,
    /DROP FUNCTION IF EXISTS public\.pay_workbench_session_get_candidate_preview\(uuid, uuid\);/
  );
  assert.match(
    currentCandidatePreviewSql,
    /REVOKE ALL ON FUNCTION public\.pay_workbench_session_get_candidate_preview\(uuid,\s*uuid,\s*jsonb,\s*integer\)\s+FROM PUBLIC,\s*anon,\s*authenticated;/
  );
  assert.match(
    currentCandidatePreviewSql,
    /GRANT EXECUTE ON FUNCTION public\.pay_workbench_session_get_candidate_preview\(uuid,\s*uuid,\s*jsonb,\s*integer\)\s+TO service_role;/
  );
});

test('candidate preview excludes rows reserved by an active draft or later payment stage', () => {
  const body = functionBody(
    currentCandidatePreviewSql,
    'public.pay_workbench_session_get_candidate_preview'
  );

  assert.match(body, /preview_row\.status = 'READY'/);
  assert.match(body, /row_json->>'post_draft_unavailable'/);
  assert.match(body, /row_json->>'post_draft_overlay_applied'/);
  assert.match(body, /row_json->>'post_draft_overlay_operation_type'/);
  assert.match(body, /'DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE'/);
  assert.match(body, /row_json->>'post_draft_overlay_active'/);
  assert.match(body, /NOT IN \('false', 'f', '0', 'no', 'n', 'off'\)/);
  assert.match(body, /AND NOT EXISTS \(\s*SELECT 1\s*FROM public\.pay_batch_items AS active_item/);
  assert.match(body, /active_candidate\.candidate_id = preview_row\.candidate_id/);
  assert.match(body, /active_batch\.cancelled_at_utc IS NULL/);
  assert.match(body, /active_item\.finance_component_id::text/);
  assert.match(body, /active_item\.finance_case_id::text/);
  assert.match(body, /active_item\.frozen_component_snapshot_json->>'canonical_correction_key'/);
  assert.match(body, /'DRAFT'[\s\S]*'EXECUTING'[\s\S]*'AUTHORISED_FOR_PAYMENT'/);
});

test('paged Ready to Pay count and rows use the same active-batch exclusion', () => {
  const body = functionBody(
    currentPreviewPageSql,
    'public.pay_workbench_session_get_preview_page'
  );

  assert.equal(
    (body.match(/FROM public\.pay_batch_items AS active_batch_item/g) || []).length,
    2
  );
  assert.match(body, /active_batch_candidate\.candidate_id = preview_count_row\.candidate_id/);
  assert.match(body, /active_batch_candidate\.candidate_id = preview_row\.candidate_id/);
  assert.match(body, /active_batch\.cancelled_at_utc IS NULL/);
  assert.match(body, /active_batch_item\.finance_component_id::text/);
  assert.match(body, /active_batch_item\.finance_case_id::text/);
  assert.match(body, /'DRAFT'[\s\S]*'EXECUTING'[\s\S]*'AUTHORISED_FOR_PAYMENT'/);
  assert.match(
    body,
    /COUNT\(\*\) FILTER \(WHERE preview_count_row\.selected IS TRUE\)::integer/
  );
  assert.match(
    body,
    /WHEN v_resolved_section = 'canonical_preview_lines'[\s\S]*THEN COALESCE\(v_selected_eligible_count, 0\)/
  );
  assert.match(
    currentPreviewPageSql,
    /REVOKE ALL ON FUNCTION public\.pay_workbench_session_get_preview_page\(uuid, text, jsonb, integer\)\s+FROM PUBLIC, anon, authenticated;/
  );
  assert.match(
    currentPreviewPageSql,
    /GRANT EXECUTE ON FUNCTION public\.pay_workbench_session_get_preview_page\(uuid, text, jsonb, integer\)\s+TO service_role;/
  );
});

test('only the current preview-page function body remains in repeatable sources', () => {
  const definitions = [];
  for (const entry of fs.readdirSync(repeatableDir, { withFileTypes: true })) {
    if (!entry.isFile()) continue;
    const filePath = path.join(repeatableDir, entry.name);
    const source = fs.readFileSync(filePath, 'utf8');
    const matches = source.match(
      /CREATE OR REPLACE FUNCTION\s+public\.pay_workbench_session_get_preview_page\s*\(/gi
    );
    for (let index = 0; index < (matches || []).length; index += 1) {
      definitions.push(entry.name);
    }
  }

  assert.deepEqual(definitions, ['20072026_0117_banking_pay_preview_selection_revision.sql']);
});

test('the historical candidate-preview owner is superseded by one later exact reassertion', () => {
  const definitions = [];
  for (const entry of fs.readdirSync(repeatableDir, { withFileTypes: true })) {
    if (!entry.isFile()) continue;
    const filePath = path.join(repeatableDir, entry.name);
    const source = fs.readFileSync(filePath, 'utf8');
    const matches = source.match(
      /CREATE OR REPLACE FUNCTION\s+public\.pay_workbench_session_get_candidate_preview\s*\(/gi
    );
    for (let index = 0; index < (matches || []).length; index += 1) {
      definitions.push(entry.name);
    }
  }

  assert.deepEqual(definitions, [
    '16082026_2035_pay_workbench_candidate_preview_effective_section_v1.sql',
    '26052026_2100HRS_NEW_FUNCTIONS.sql'
  ]);
});

test('draft cancellation still restores preview eligibility in the batch overlay function', () => {
  const body = functionBody(
    historicalFunctionsSql,
    'public.pay_workbench_patch_preview_after_batch_mutation'
  );

  assert.match(
    body,
    /v_is_cancel_delete := v_operation_type IN \('DRAFT_DELETE', 'DRAFT_CANCEL'\)/
  );
  assert.match(body, /'post_draft_unavailable', false/);
  assert.match(body, /'post_draft_overlay_active', false/);
  assert.match(body, /'post_draft_overlay_operation_type', v_operation_type/);
});
