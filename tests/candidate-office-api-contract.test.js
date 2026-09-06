import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const backendUrl = new URL('../broker/src/candidate-app-backend.js', import.meta.url);
const openapiUrl = new URL('../docs/candidate-app/CLOUDTMS_OFFICE_CANDIDATE_API_V1.yaml', import.meta.url);
const officeSqlUrl = new URL('../supabase/repeatable/11082026_1832_cloudtms_office_candidate_adapter_v1.sql', import.meta.url);
const invoiceGenerateRowsUrl = new URL('../supabase/repeatable/26072026_1947_invoice_batch_generate_candidate_rows_v1.sql', import.meta.url);
const invoiceIssueRowsUrl = new URL('../supabase/repeatable/26072026_1947_invoice_batch_issue_candidate_rows_v1.sql', import.meta.url);

const routes = Object.freeze([
  ['GET', '/api/candidate-app/office-capabilities'],
  ['POST', '/api/candidate-app/timesheets/office-projections'],
  ['GET', '/api/candidate-app/manager-reminder-eligibility'],
  ['POST', '/api/candidate-app/manager-reminder-batches/preview'],
  ['POST', '/api/candidate-app/manager-reminder-batches'],
  ['GET', '/api/candidate-app/manager-reminder-batches/{batchId}'],
  ['GET', '/api/candidate-app/timesheets/{timesheetId}/office-detail'],
  ['GET', '/api/candidate-app/timesheets/{timesheetId}/route-preview'],
  ['POST', '/api/candidate-app/timesheets/{timesheetId}/route-confirm'],
  ['GET', '/api/candidate-app/timesheets/{timesheetId}/reject-preview'],
  ['POST', '/api/candidate-app/timesheets/{timesheetId}/reject'],
  ['GET', '/api/candidate-app/workflows/{workflowId}/paper-pack'],
  ['GET', '/api/candidate-app/workflows/{workflowId}/paper-return-review'],
  ['GET', '/api/candidate-app/workflows/{workflowId}/components/{componentId}/document'],
  ['POST', '/api/candidate-app/workflows/{workflowId}/actions/{action}'],
  ['POST', '/api/candidate-app/workflows/{workflowId}/signature/prepare'],
  ['PUT', '/api/candidate-app/uploads/{ticket}']
]);

function sourcePath(path) {
  return path.replaceAll('{batchId}', ':batchId')
    .replaceAll('{timesheetId}', ':timesheetId')
    .replaceAll('{workflowId}', ':workflowId')
    .replaceAll('{componentId}', ':componentId')
    .replaceAll('{ticket}', ':ticket')
    .replaceAll('{action}', ':action');
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

test('office OpenAPI and normal backend expose the same exact method/path inventory', async () => {
  const [backend, openapi] = await Promise.all([
    readFile(backendUrl, 'utf8'), readFile(openapiUrl, 'utf8')
  ]);
  for (const [method, path] of routes) {
    const runtimePath = sourcePath(path);
    assert.match(backend, new RegExp(escapeRegex(runtimePath)), `${runtimePath} must exist in the normal backend`);
    const block = new RegExp(`^  ${escapeRegex(path)}:\\r?\\n([\\s\\S]*?)(?=^  \/|^components:)`, 'm').exec(openapi)?.[1] || '';
    assert.match(block, new RegExp(`^    ${method.toLowerCase()}:`, 'm'), `${method} ${path} must be documented`);
    for (const wrong of ['get', 'post', 'put', 'patch', 'delete'].filter(value => value !== method.toLowerCase())) {
      assert.doesNotMatch(block, new RegExp(`^    ${wrong}:`, 'm'), `${wrong.toUpperCase()} ${path} must not be documented`);
    }
  }
  assert.match(openapi, /audience:\s*OFFICE/);
  assert.match(openapi, /direct_browser_rpc:\s*false/);
  assert.match(openapi, /candidate_broker:\s*false/);
  assert.match(openapi, /maximum_projection_rows:\s*100/);
  assert.match(openapi, /maximum_manager_reminder_batch_rows:\s*1000/);
  assert.match(openapi, /CLOUDTMS_OFFICE_CANDIDATE_API_V1/);
  assert.match(openapi, /mode:\s*\{type:\s*string, const:\s*ENABLED\}/);
  assert.match(openapi, /required_office_role:\s*\{type:\s*string, const:\s*admin\}/);
  assert.match(openapi, /permission_source:\s*\{type:\s*string, const:\s*OFFICE_ADMIN_ROLE_V1\}/);
  assert.match(openapi, /office_service_context:\s*CANDIDATE_OFFICE_SERVICE_CONTEXT_V1/);
  assert.match(openapi, /candidate_client_flags_required_for_office_actions:\s*false/);
  assert.match(openapi, /candidate_client_flags_continue_to_block_candidate_sessions:\s*true/);
  assert.match(backend, /cancel:\s*'MANAGER_REQUEST_CANCEL'/);
  assert.match(backend, /cancel:\s*'cancel_manager_request'/);
  assert.match(backend, /NOT_FOUND_ERROR_CODES[\s\S]*CANDIDATE_REMINDER_BATCH_NOT_FOUND/);
  assert.match(backend, /requireOfficeUser\(request, \['admin'\]\)/);
  assert.match(openapi, /enum:\s*\[remind, renew, cancel, cancel-manager-handoff, phone-review, phone-progress, phone-approve, phone-refuse, retry-finalisation, retry-paper-preparation\]/);
  assert.match(openapi, /decision_code:\s*\{const:\s*REJECT_OR_MANUAL\}/);
  assert.match(openapi, /title:\s*\{const:\s*'Does the candidate need to resubmit instead\?'\}/);
  assert.match(openapi, /label:\s*\{const:\s*'Use Reject Candidate Submission'\}/);
  assert.match(openapi, /label:\s*\{const:\s*'Continue to Manual conversion'\}/);
  assert.doesNotMatch(openapi, /mode:\s*\{[^\r\n]*(?:READ_ONLY|DISABLED)/);
  assert.match(openapi, /cancel action cancels only the exact current manager approval/i);
  assert.match(openapi, /It does not cancel the Candidate claim/i);
});

test('office contract preserves bounded projection and server-owned reminder batching', async () => {
  const openapi = await readFile(openapiUrl, 'utf8');
  assert.match(openapi, /selected_rows:\s*\{type:\s*array, minItems:\s*1, maxItems:\s*100/);
  assert.match(openapi, /contract_version:\s*\{const:\s*OFFICE_CANDIDATE_REMINDER_ELIGIBILITY_PAGE_V1\}/);
  assert.match(openapi, /mode:\s*\{const:\s*ALL_ELIGIBLE\}/);
  assert.match(openapi, /catalogue_revision:\s*\{\$ref:\s*'#\/components\/schemas\/Sha256'\}/);
  assert.match(openapi, /name:\s*surname_query[\s\S]*Case-insensitive Candidate-surname substring filter/);
  assert.match(openapi, /enum:\s*\[CANDIDATE_SURNAME, LAST_MANAGER_EMAIL\]/);
  assert.match(openapi, /matching_selection_keys:/);
  assert.match(openapi, /enum:\s*\[SWITCH_TO_MANUAL, SWITCH_DAILY_TO_MANUAL, CONVERT_QR_TO_MANUAL, ALLOW_ELECTRONIC_AGAIN, ALLOW_QR_AGAIN,/);
  assert.match(openapi, /selected_rows:\s*\{type:\s*array, minItems:\s*1, maxItems:\s*1000/);
  assert.match(openapi, /Exact lost-response replay is resolved from the[\s\S]*durable batch receipt before current eligibility is recalculated/i);
  assert.match(openapi, /per-row browser\s+reminder\s+requests are prohibited/i);
  assert.match(openapi, /EXPENSE_EMAIL_MISSING/);
  assert.match(openapi, /calculation_effect:\s*\{const:\s*NONE\}/);
  assert.match(openapi, /name:\s*expected_row_signature, in:\s*query/);
  assert.doesNotMatch(openapi, /name:\s*row_signature, in:\s*query/);
  assert.match(openapi, /additionalProperties:\s*false\s*\r?\n\s+required:\s*\[generation, idempotency_key\]/);
  assert.match(openapi, /manifest_sha256_hex:\s*\{\$ref:\s*'#\/components\/schemas\/Sha256'\}/);
  assert.match(openapi, /\/api\/candidate-app\/uploads\/\{ticket\}/);
  assert.match(openapi, /CLIENT_DESTINATION/);
  assert.match(openapi, /never call Candidate business RPCs directly/i);
  assert.match(openapi, /delivery_generation:/);
  assert.match(openapi, /FINALISED workflows use the preceding generation/i);
  assert.match(openapi, /FAILED_RETRYABLE, FAILED_TERMINAL/);
  assert.match(openapi, /is_current_action_workflow:/);
  assert.match(openapi, /Terminal workflow history is returned for audit but never drives current status/);
  assert.match(openapi, /rejection_actionable:/);
  assert.match(openapi, /Null once durable direct replacement lineage exists/);
  assert.match(openapi, /DAILY Candidate rows are timesheet\/booking-family\s+owned and are requested by timesheet_id without a contract_week_id/);
  assert.match(openapi, /Financial completion alone is not proof of a\s+completed Candidate submission/);
  assert.match(openapi, /timesheet_summary_after_render_hydration:\s*false/);
  assert.match(openapi, /paints with the initial grid rather than through after-render hydration/);
  for (const code of [
    'OFFICE_AUTH_REQUIRED', 'CANDIDATE_OFFICE_PERMISSION_DENIED',
    'CANDIDATE_CONTEXT_STALE', 'CANDIDATE_TIMESHEET_MOVED',
    'CANDIDATE_ACTION_NOT_ELIGIBLE', 'CANDIDATE_REASON_REQUIRED', 'CANDIDATE_REASON_INVALID',
    'CANDIDATE_REQUEST_GENERATION_STALE', 'CANDIDATE_REQUIRES_UNAUTHORISE',
    'CANDIDATE_PROTECTED_FINANCIAL_HISTORY', 'CANDIDATE_IMPORT_AUTHORITATIVE',
    'CANDIDATE_PROVIDER_HANDOFF_IN_PROGRESS', 'CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT',
    'CANDIDATE_TOO_MANY_AFFECTED_WORKFLOWS', 'CANDIDATE_REJECTION_SCOPE_CONFLICT',
    'CANDIDATE_IDEMPOTENCY_CONFLICT', 'CANDIDATE_PAPER_OUTBOX_NOT_READY',
    'CANDIDATE_PAPER_PACK_RETRY_NOT_READY', 'CANDIDATE_PAPER_PACK_ASSEMBLY_TRANSIENT',
    'CANDIDATE_PAPER_PACK_FAILURE_RECEIPT_INVALID', 'CANDIDATE_ROUTE_NOT_FOUND'
  ]) assert.match(openapi, new RegExp(`- ${code}\\b`));
});

test('Office projection SQL preserves DAILY timesheet-only identity without weakening WEEKLY exactness', async () => {
  const officeSql = await readFile(officeSqlUrl, 'utf8');
  assert.match(officeSql, /v_current\.sheet_scope='DAILY'::public\.timesheet_scope_enum/);
  assert.match(officeSql, /DAILY is owned by its current timesheet\/booking family/);
  assert.match(officeSql, /if v_week_count<>1 then\s+raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID'/);
  assert.match(officeSql, /'contract_week_id',v_week\.id/);
  assert.match(officeSql, /'additional_seq',v_week\.additional_seq/);
});

test('Office projection keeps accepted hours separate from a later active expense claim', async () => {
  const [officeSql, openapi] = await Promise.all([
    readFile(officeSqlUrl, 'utf8'),
    readFile(openapiUrl, 'utf8')
  ]);
  assert.match(officeSql, /v_current\.candidate_workflow_id is distinct from v_workflow\.id/);
  assert.match(officeSql, /retained_workflow\.manager_approved_at_utc is not null/);
  assert.match(officeSql, /retained_workflow\.state not in \('CANCELLED','SUPERSEDED','REFUSED','REJECTED','EXPIRED'\)/);
  assert.match(officeSql, /retained_approval\.workflow_generation=v_current\.candidate_workflow_generation/);
  assert.match(officeSql, /retained_approval\.state='APPROVED'/);
  assert.match(officeSql, /retained_approval\.approved_at_utc is not null/);
  assert.match(officeSql, /'retained_manager_approval',case when v_retained_approval\.id is null then null else jsonb_build_object/);
  assert.match(officeSql, /when v_retained_workflow\.workflow_kind='CONTRACT_EXPENSE' then 'EXPENSE'/);
  assert.match(openapi, /retained_manager_approval:\s*\{\$ref: '#\/components\/schemas\/RetainedManagerApproval'\}/);
  assert.match(openapi, /unfinished standalone expense claim[\s\S]*has no Timesheet yet[\s\S]*not projected onto the worked Timesheet row/i);
});

test('Office projection keeps the exact approved generation and returned paper facts after finalisation', async () => {
  const officeSql = await readFile(officeSqlUrl, 'utf8');
  assert.match(officeSql, /v_workflow_artifact_generation:=case[\s\S]*when v_workflow\.state='FINALISED' then greatest\(v_workflow\.generation-1,1\)/);
  assert.match(officeSql, /ar\.workflow_generation=v_workflow_artifact_generation/);
  assert.match(officeSql, /m\.context_id=v_workflow\.target_timesheet_id[\s\S]*or m\.context_id=v_workflow\.anchor_timesheet_id/);
  assert.match(officeSql, /when v_workflow\.state='FINALISED' then[\s\S]*v_paper_state:='RETURN_RECEIVED'/);
  assert.match(officeSql, /returned_page\.paper_return_verified_at_utc[\s\S]*returned_page\.component_kind='SIGNED_RETURN'/);
  assert.match(officeSql, /paper-return-review\?generation='\|\|v_paper_delivery_generation::text/);
});

test('durable reminder batch PARTIAL and FAILED outcomes remain successful structured results', async () => {
  const [officeSql, openapi] = await Promise.all([
    readFile(officeSqlUrl, 'utf8'),
    readFile(openapiUrl, 'utf8')
  ]);
  assert.match(officeSql, /'ok',true,'contract_version','OFFICE_CANDIDATE_REMINDER_BATCH_RESULT_V1'/);
  assert.doesNotMatch(officeSql, /'ok',v_failure_count=0,'contract_version','OFFICE_CANDIDATE_REMINDER_BATCH_RESULT_V1'/);
  assert.match(openapi, /PARTIAL and FAILED describe per-item outcomes/);
  assert.match(openapi, /ok: \{const: true, description: "True once the batch outcome has been durably completed, including PARTIAL or FAILED item outcomes\."\}/);
});

test('invoice row projections expose the office expense-email diagnostic without changing authority fields', async () => {
  const [generateRows, issueRows] = await Promise.all([
    readFile(invoiceGenerateRowsUrl, 'utf8'),
    readFile(invoiceIssueRowsUrl, 'utf8')
  ]);
  for (const source of [generateRows, issueRows]) {
    assert.match(source, /EXPENSE_INVOICE_EMAIL_REQUIRED/);
    assert.match(source, /EXPENSE_EMAIL_MISSING/);
    assert.match(source, /informational_codes/);
  }
  assert.match(generateRows, /action_blocker_codes/);
  assert.match(issueRows, /delivery_blocker_codes/);
  assert.match(issueRows, /selectable/);
});
