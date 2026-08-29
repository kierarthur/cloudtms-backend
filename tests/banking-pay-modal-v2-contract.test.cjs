const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const sqlPath = path.join(root, 'supabase', 'repeatable', '28082026_1159_banking_pay_modal_structure_v2.sql');
const workerPath = path.join(root, 'broker', 'src', 'banking-pay-modal-v2.js');
const routerPath = path.join(root, 'broker', 'src', 'index.js');

const read = (file) => fs.readFileSync(file, 'utf8');
const repeatable = name => read(path.join(root, 'supabase', 'repeatable', name));
// Explicit split-source inventory. These are source/ACL checks, not a claim
// that the current hosted database has installed any v2 definition.
const publicSources = [
  '28082026_1159_banking_pay_modal_structure_v2.sql',
  '28082026_2035_banking_pay_modal_summary.sql',
  '28082026_2046_banking_pay_modal_issue_detail.sql',
  '28082026_2130_banking_pay_modal_task_pages.sql',
  '28082026_2201_banking_pay_modal_blocked_pages.sql',
  '28082026_2313_banking_pay_modal_candidate_selection_response.sql',
  '29082026_0002_banking_pay_modal_global_selection_response.sql',
  '29082026_0019_banking_pay_modal_row_selection_response.sql',
  '29082026_0114_banking_pay_modal_group_selection_response.sql'
];

test('v2 installs the complete additive Banking Pay modal contract', () => {
  const sql = publicSources.map(repeatable).join('\n');
  for (const name of [
    'pay_workbench_session_get_candidate_summary_page_v1',
    'pay_workbench_session_set_candidate_ready_selection_v1',
    'pay_workbench_session_set_filtered_ready_selection_v1',
    'pay_workbench_session_set_ready_rows_v1',
    'pay_workbench_session_set_ready_group_v1',
    'pay_workbench_session_get_candidate_ready_page_v1',
    'pay_workbench_session_get_action_required_page_v1',
    'pay_workbench_session_get_action_required_detail_v1',
    'pay_workbench_session_get_blocked_page_v1',
    'pay_workbench_session_get_blocked_detail_v1',
    'pay_workbench_session_get_selected_ready_timesheets_v1'
  ]) {
    assert.match(sql, new RegExp(`CREATE OR REPLACE FUNCTION public\\.${name}\\(`));
    assert.match(sql, new RegExp(`REVOKE ALL ON FUNCTION public\\.${name}\\(`));
    assert.match(sql, new RegExp(`GRANT EXECUTE ON FUNCTION public\\.${name}\\([^;]+ TO service_role`));
  }
  const ready = repeatable('28082026_1308_banking_pay_modal_ready_members.sql');
  assert.match(ready, /private\.pay_workbench_preview_effective_section_v1/);
  assert.match(ready, /public\.pay_workbench_preview_line_contract_ok/);
  assert.match(repeatable('28082026_1424_banking_pay_modal_candidate_selection_core.sql'),
    /public\.pay_workbench_revalidate_zero_retained_recovery_headroom_v1/);
  assert.match(sql, /'BANKING_PAY_MODAL_STRUCTURE_V2'/);
  assert.match(sql, /NOTIFY pgrst, 'reload schema'/i);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(sql, /\bEXECUTE\s+format\s*\(/i);
  const capability = repeatable('29082026_0245_banking_pay_modal_capability_v2.sql');
  assert.match(capability, /'banking_pay_workbench_v2', jsonb_build_object\(/);
  assert.match(capability, /'available', v_banking_pay_workbench_v2_available/);
  assert.match(capability, /'surface_contract', 'BANKING_PAY_MODAL_STRUCTURE_V2'/);
  const release = JSON.parse(read(path.join(root,'supabase','release','current-release.json')));
  assert.ok(release.verificationFiles.includes('supabase/verification/29082026_0246_banking_pay_modal_structure_v2_verification.sql'));
  assert.ok(release.newVerificationFiles.includes('supabase/verification/29082026_0246_banking_pay_modal_structure_v2_verification.sql'));
});

test('general browser isolation inventory admits only the nine new service RPCs', () => {
  const verification = read(path.join(root, 'supabase', 'verification',
    '22082026_1302_general_browser_isolation_verification.sql'));
  assert.match(verification, /v_count<>652 or v_service_missing<>72 or v_browser_executable<>0/);
  assert.match(verification, /v_hash<>'951cb626cae1497249be73898f9906cd'/);
});

test('Candidate-named isolation inventory admits only the four new service RPCs', () => {
  const verification = read(path.join(root, 'supabase', 'verification',
    '27082026_1947_candidate_named_security_verification_v3.sql'));
  assert.match(verification, /v_count<>112 or v_service_missing<>8 or v_browser_executable<>0/);
  assert.match(verification, /v_hash<>'6d3c3fe23de37f1e7dc74edd08bfceed'/);
});

test('v2 summary is server-owned, selected-only and keyset paged before the browser', () => {
  const sql = read(sqlPath);
  assert.match(sql, /selected_display_amount/);
  assert.match(sql, /selected_deduction_exists/);
  assert.match(sql, /selected_timesheet_count/);
  assert.match(sql, /selected_ready_display_amount/);
  assert.match(sql, /candidate_sort_name/);
  assert.match(sql, /p_sort_key/);
  assert.match(sql, /p_sort_direction/);
  assert.match(sql, /candidate_id/);
  assert.match(sql, /v_limit integer := LEAST\(GREATEST\(COALESCE\(p_limit, 100\), 1\), 100\)/);
  assert.match(sql, /private\.pay_workbench_modal_hidden_v2/);
  assert.match(repeatable('28082026_1342_banking_pay_modal_visibility.sql'), /HIDDEN_INDEFINITE_SNOOZE/);
});

test('candidate-wide selection is one revision-fenced server mutation', () => {
  const response = repeatable('28082026_2313_banking_pay_modal_candidate_selection_response.sql');
  const bridge = repeatable('28082026_1424_banking_pay_modal_selection_owner_bridge.sql');
  const core = repeatable('28082026_1424_banking_pay_modal_candidate_selection_core.sql');
  const context = read(sqlPath);
  const publicPart = response.slice(response.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_session_set_candidate_ready_selection_v1'));
  assert.match(publicPart, /private\.pay_workbench_modal_candidate_selection_response_v2\(/);
  assert.equal((response.match(/public\.pay_workbench_session_set_selected_rows\(/g)||[]).length,1);
  assert.match(bridge, /RETURN private\.pay_workbench_modal_candidate_selection_apply_v2\(/);
  assert.match(core, /FOR UPDATE/);
  assert.match(core, /private\.pay_workbench_modal_context_v2\(/);
  assert.match(context, /expected_session_version/);
  assert.match(context, /expected_progress_counter_version/);
  assert.match(core, /pay_workbench_revalidate_zero_retained_recovery_headroom_v1/);
  assert.doesNotMatch(response + core, /LOOP\s+[\s\S]*pay_workbench_session_set_selected_rows/i);
});

test('Worker exposes only the bounded typed v2 adapter routes', () => {
  const worker = read(workerPath);
  const router = read(routerPath);
  for (const route of [
    '/api/banking/pay/workbench/v2/capability',
    '/api/banking/pay/workbench/v2/session/:id/candidates',
    '/api/banking/pay/workbench/v2/session/:id/candidate/:candidateId/selection',
    '/api/banking/pay/workbench/v2/session/:id/candidate/:candidateId/ready-selection',
    '/api/banking/pay/workbench/v2/session/:id/candidate/:candidateId/group-selection',
    '/api/banking/pay/workbench/v2/session/:id/candidate/:candidateId/ready',
    '/api/banking/pay/workbench/v2/session/:id/action-required',
    '/api/banking/pay/workbench/v2/session/:id/action-required/:taskKey',
    '/api/banking/pay/workbench/v2/session/:id/blocked',
    '/api/banking/pay/workbench/v2/session/:id/blocked/:blockerKey',
    '/api/banking/pay/workbench/v2/session/:id/candidate/:candidateId/selected-ready-timesheets'
  ]) assert.match(worker, new RegExp(route.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  assert.match(worker, /BANKING_PAY_MODAL_STRUCTURE_V2/);
  assert.match(worker, /limit[^\n]+100/);
  assert.doesNotMatch(worker, /selected_display_amount\s*[+\-*/]/);
  assert.match(router, /dispatchBankingPayModalV2Request/);
});
