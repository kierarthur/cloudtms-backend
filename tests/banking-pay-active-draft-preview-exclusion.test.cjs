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
const recoveryResidualHelperSql = fs.readFileSync(path.join(
  repeatableDir,
  '20082026_1502_pay_workbench_preview_recovery_residual_current_v1.sql'
), 'utf8');
const financeCancellationAuthoritySql = fs.readFileSync(path.join(
  repeatableDir,
  '17082026_2052_pay_finance_resolution_cancel_authority.sql'
), 'utf8');
const currentBatchOverlaySql = fs.readFileSync(path.join(
  repeatableDir,
  '09082026_0825_pay_workbench_patch_preview_after_batch_mutation.sql'
), 'utf8');
const currentDraftScopeSeedSql = fs.readFileSync(path.join(
  repeatableDir,
  '21072026_1235_46_pay_workbench_prepare_draft_scope_seed.sql'
), 'utf8');
const recoveryResidualHelperInclude =
  /\\ir 20082026_1502_pay_workbench_preview_recovery_residual_current_v1\.sql/;

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
  assert.match(currentCandidatePreviewSql, recoveryResidualHelperInclude);
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

test('candidate preview excludes frozen rows but admits only a current positive recovery residual', () => {
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
  assert.match(body, /recovery_reservation_totals AS MATERIALIZED/);
  assert.equal((body.match(/public\.pay_advance_reservations AS reservation/g) || []).length, 1);
  assert.match(body, /active_item_overlap_rows AS MATERIALIZED/);
  assert.match(body, /JOIN public\.pay_batch_items AS active_item/);
  assert.match(body, /active_candidate\.candidate_id = preview_row\.candidate_id/);
  assert.match(body, /active_batch\.cancelled_at_utc IS NULL/);
  assert.match(body, /active_item\.finance_component_id = CASE/);
  assert.match(body, /active_item\.finance_case_id = preview_row\.typed_finance_case_id/);
  assert.match(body, /active_item\.frozen_component_snapshot_json->>'canonical_correction_key'/);
  assert.match(body, /active_item\.frozen_source_basis_json->>'linked_timesheet_id'/);
  assert.match(body, /active_item\.frozen_component_snapshot_json#>>'\{source_basis_json,linked_timesheet_id\}'/);
  assert.match(body, /private\.pay_workbench_preview_recovery_residual_is_current_v1\(/);
  assert.match(body, /preview_row\.recovery_residual_is_current/);
  assert.match(body, /preview_row\.exact_active_item_overlap IS NOT TRUE/);
  assert.match(body, /recovery_active_reserved_ex_vat'[\s\S]*::numeric > 0/);
  assert.match(body, /'DRAFT'[\s\S]*'EXECUTING'[\s\S]*'AUTHORISED_FOR_PAYMENT'/);
});

test('candidate preview suppresses a keyless negative parent only for an exact frozen recovery sibling', () => {
  const body = functionBody(
    currentCandidatePreviewSql,
    'public.pay_workbench_session_get_candidate_preview'
  );

  assert.match(body, /FROM session_preview_rows AS recovery_sibling/);
  assert.equal((body.match(/recovery_sibling\.status = 'SUPERSEDED'/g) || []).length, 1);
  assert.match(body, /recovery_sibling\.row_json->>'post_draft_unavailable'/);
  assert.match(body, /recovery_sibling\.row_json->>'post_draft_overlay_applied'/);
  assert.match(body, /recovery_sibling\.row_json->>'post_draft_overlay_active'/);
  assert.match(body, /recovery_sibling\.typed_overlay_pay_batch_id/);
  assert.match(body, /FROM public\.pay_batch_items AS frozen_recovery_item/);
  assert.match(body, /frozen_recovery_candidate\.candidate_id = recovery_sibling\.candidate_id/);
  assert.match(body, /frozen_recovery_item\.item_type[\s\S]*= 'OVERPAYMENT_RECOVERY'/);
  assert.match(body, /frozen_recovery_item\.finance_case_id = recovery_sibling\.typed_finance_case_id/);
  assert.match(body, /frozen_recovery_item\.finance_component_id = recovery_sibling\.typed_finance_component_id/);
  assert.match(body, /frozen_recovery_batch\.id = recovery_sibling\.typed_overlay_pay_batch_id/);
  assert.match(body, /frozen_recovery_item\.frozen_component_key_type/);
  assert.match(body, /frozen_recovery_item\.frozen_component_key_value/);
  assert.match(body, /frozen_recovery_item\.frozen_source_basis_json->>'timesheet_id'/);
  assert.match(body, /frozen_recovery_item\.frozen_source_basis_json->>'linked_timesheet_id'/);
  assert.match(body, /frozen_recovery_item\.frozen_component_snapshot_json#>>'\{source_basis_json,linked_timesheet_id\}'/);
  assert.match(body, /frozen_recovery_batch\.cancelled_at_utc IS NULL/);
});

test('paged count and rows share one set-wise residual eligibility identity set', () => {
  assert.match(currentPreviewPageSql, recoveryResidualHelperInclude);
  const body = functionBody(
    currentPreviewPageSql,
    'public.pay_workbench_session_get_preview_page'
  );

  assert.match(body, /v_eligible_row_ids uuid\[\]/);
  assert.match(body, /recovery_reservation_totals AS MATERIALIZED/);
  assert.equal((body.match(/public\.pay_advance_reservations AS reservation/g) || []).length, 1);
  assert.match(body, /active_item_overlap_rows AS MATERIALIZED/);
  assert.match(body, /strict_recovery_siblings AS MATERIALIZED/);
  assert.match(body, /private\.pay_workbench_preview_recovery_residual_is_current_v1\(/);
  assert.match(body, /COALESCE\(ARRAY_AGG\(eligible_row\.id ORDER BY eligible_row\.id\), ARRAY\[\]::uuid\[\]\)/);
  assert.match(body, /WHERE preview_row\.id = ANY\(v_eligible_row_ids\)/);
  assert.match(body, /active_batch\.cancelled_at_utc IS NULL/);
  assert.match(body, /active_item\.finance_component_id = preview_row\.typed_finance_component_id/);
  assert.match(body, /active_item\.finance_case_id = preview_row\.typed_finance_case_id/);
  assert.match(body, /active_item\.frozen_source_basis_json->>'linked_timesheet_id'/);
  assert.match(body, /active_item\.frozen_component_snapshot_json#>>'\{source_basis_json,linked_timesheet_id\}'/);
  assert.match(body, /preview_row\.exact_active_item_overlap IS NOT TRUE/);
  assert.match(body, /'DRAFT'[\s\S]*'EXECUTING'[\s\S]*'AUTHORISED_FOR_PAYMENT'/);
  assert.match(
    body,
    /COUNT\(\*\) FILTER \(WHERE eligible_row\.selected IS TRUE\)::integer/
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

test('paged count and returned rows share the exact frozen recovery sibling fence', () => {
  const body = functionBody(
    currentPreviewPageSql,
    'public.pay_workbench_session_get_preview_page'
  );

  assert.match(body, /strict_recovery_siblings AS MATERIALIZED/);
  assert.match(body, /recovery_sibling\.status = 'SUPERSEDED'/);
  assert.match(body, /JOIN public\.pay_batch_items AS frozen_recovery_item/);
  assert.match(body, /frozen_recovery_candidate\.candidate_id = recovery_sibling\.candidate_id/);
  assert.match(body, /frozen_recovery_item\.finance_case_id = recovery_sibling\.typed_finance_case_id/);
  assert.match(body, /frozen_recovery_item\.finance_component_id = recovery_sibling\.typed_finance_component_id/);
  assert.match(body, /frozen_recovery_item\.frozen_component_key_type/);
  assert.match(body, /frozen_recovery_item\.frozen_component_key_value/);
  assert.match(body, /frozen_recovery_item\.frozen_source_basis_json->>'timesheet_id'/);
  assert.match(body, /frozen_recovery_batch\.cancelled_at_utc IS NULL/);
});

test('the residual helper is pure, owner-only and fails malformed contracts closed', () => {
  const body = functionBody(
    recoveryResidualHelperSql,
    'private.pay_workbench_preview_recovery_residual_is_current_v1'
  );

  assert.match(body, /LANGUAGE plpgsql[\s\S]*IMMUTABLE[\s\S]*PARALLEL SAFE[\s\S]*SECURITY INVOKER/);
  assert.match(body, /SET search_path TO ''/);
  assert.doesNotMatch(body, /FROM\s+(?:public|private)\./i);
  assert.match(body, /recovery_residual_contract_version/);
  assert.match(body, /recovery_source_outstanding_ex_vat/);
  assert.match(body, /recovery_active_reserved_ex_vat/);
  assert.match(body, /recovery_residual_outstanding_ex_vat/);
  assert.match(body, /WHEN OTHERS THEN[\s\S]*RETURN false/);
  assert.match(recoveryResidualHelperSql, /FROM PUBLIC,anon,authenticated,service_role/);
  assert.match(recoveryResidualHelperSql, /TO postgres;/);
});

test('canonical overpayment rows publish the versioned source, reserved and residual arithmetic', () => {
  const body = functionBody(
    financeCancellationAuthoritySql,
    'public.pay_preview_candidate_build_canonical_lines'
  );

  assert.match(body, /finance_case_baseline\.outstanding_amount/);
  assert.match(body, /finance_case_baseline\.active_reserved_amount/);
  assert.match(body, /when fcl\.line_type = 'OVERPAYMENT_RECOVERY' then jsonb_build_object\(/);
  assert.match(body, /'recovery_residual_contract_version', 1/);
  assert.match(body, /'recovery_source_outstanding_ex_vat'/);
  assert.match(body, /'recovery_active_reserved_ex_vat'/);
  assert.match(body, /'recovery_residual_outstanding_ex_vat'/);
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

test('current batch overlay resolves the sealed linked-timesheet legacy shape and still restores cancellation eligibility', () => {
  const body = functionBody(
    currentBatchOverlaySql,
    'public.pay_workbench_patch_preview_after_batch_mutation'
  );

  assert.match(body, /batch_item\.frozen_source_basis_json->>'linked_timesheet_id'/);
  assert.match(body, /batch_item\.frozen_component_snapshot_json#>>'\{source_basis_json,linked_timesheet_id\}'/);
  assert.match(
    body,
    /v_is_cancel_delete := v_operation_type IN \('DRAFT_DELETE', 'DRAFT_CANCEL'\)/
  );
  assert.match(body, /'post_draft_unavailable', false/);
  assert.match(body, /'post_draft_overlay_active', false/);
  assert.match(body, /'post_draft_overlay_operation_type', v_operation_type/);
});

test('draft seed independently rejects rows already frozen into an active batch', () => {
  const body = functionBody(
    currentDraftScopeSeedSql,
    'public.pay_workbench_prepare_draft_scope_seed'
  );

  assert.match(body, /AS post_draft_overlay_unavailable/);
  assert.match(body, /FROM public\.pay_batch_items AS active_item/);
  assert.match(body, /active_candidate\.candidate_id = preview_row\.candidate_id/);
  assert.match(body, /active_batch\.cancelled_at_utc IS NULL/);
  assert.match(body, /active_item\.frozen_source_basis_json->>'linked_timesheet_id'/);
  assert.match(body, /active_item\.frozen_component_snapshot_json#>>'\{source_basis_json,linked_timesheet_id\}'/);
  assert.match(body, /active_item\.finance_component_id::text/);
  assert.match(body, /active_item\.finance_case_id::text/);
  assert.match(body, /COALESCE\(selected_candidate\.post_draft_overlay_unavailable, false\) IS TRUE/);
});
