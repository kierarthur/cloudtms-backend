const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = (name) => fs.readFileSync(path.join(root, 'supabase', 'repeatable', name), 'utf8');

const requestStart = read('04082026_1207_pay_payment_correction_request_start.sql');
const statusPage = read('04082026_1146_pay_batch_payment_status_page_v1.sql');
const selection = read('04082026_1147_pay_payment_correction_selection_prepare_chunk_v1.sql');
const processChunk = read('04082026_1209_pay_payment_correction_process_chunk.sql');
const checker = read('04082026_1148_pay_payment_correction_integrity_check_v1.sql');
const expand = read('04082026_1208_pay_payment_correction_expand_work.sql');
const preBank = read('04082026_1158_pay_pre_bank_cancel_apply_work_item.sql');
const noMoney = read('04082026_1158_pay_no_money_unwind_apply_work_item.sql');
const acl = read('04082026_2035_banking_pay_correction_helper_acl.sql');
const eventIngest = read('04082026_1210_pay_bank_event_ingest.sql');
const installedFunctions = read('26052026_2100HRS_NEW_FUNCTIONS.sql');

test('planning start uses a constant-size version fence and creates no whole-scope JSON', () => {
  const fence = requestStart.slice(
    requestStart.indexOf('v_active_scope_hash :='),
    requestStart.indexOf('v_expected_snapshot_token :=')
  );
  assert.match(fence, /banking_pay_batch_change_signals/);
  assert.match(fence, /scope_version_authority/);
  assert.doesNotMatch(fence, /jsonb_agg|array_agg|candidate_count|active_item_count|provider_event_count/);
  assert.ok(requestStart.indexOf("INSERT INTO public.pay_payment_correction_requests") > 0);
  assert.ok(requestStart.indexOf("public.banking_pay_operation_start(") > 0);
});

test('status detail rollups are restricted to at most 100 page keys', () => {
  const pageKeys = statusPage.indexOf('page_keys AS MATERIALIZED');
  for (const cte of ['item_rollup AS', 'correction_rollup AS', 'work_rollup AS',
    'membership_history AS', 'provider_rollup AS', 'transfer_rollup AS']) {
    assert.ok(statusPage.indexOf(cte) > pageKeys, cte);
  }
  assert.match(statusPage, /page_keys AS MATERIALIZED[\s\S]*LIMIT p_limit \+ 1/);
  assert.doesNotMatch(statusPage, /pay_payment_cancelability_diagnostic\s*\(/);
  assert.ok((statusPage.match(/JOIN page_keys ON/g) || []).length >= 6);
});

test('explicit selection is canonical, exact and cannot silently narrow', () => {
  assert.match(requestStart, /EXPLICIT_TOKEN_INVALID/);
  assert.match(requestStart, /v_unique_explicit_count <> v_requested_explicit_count/);
  assert.match(requestStart, /canonical_explicit_candidate_tokens/);
  assert.match(requestStart, /'chain', 'EXPLICIT_IDS'/);
  assert.match(selection, /EXCEPT[\s\S]*pay_payment_correction_request_candidates/);
  assert.match(selection, /EXPLICIT_SELECTION_MISMATCH/);
  assert.match(selection, /v_requested_explicit_hash IS DISTINCT FROM v_actual_explicit_hash/);
});

test('selection replay uses exact cursor sequence, prior digest and scope fence', () => {
  for (const field of ['page_sequence_no', 'last_scanned_candidate_id', 'prior_page_hash', 'scope_fence']) {
    assert.match(selection, new RegExp(field));
  }
  assert.match(selection, /cursor_hash/);
  assert.match(selection, /PAGE_DIGEST_MISMATCH/);
  assert.match(selection, /v_request\.status = 'PLANNED'[\s\S]*'replayed', true/);
  assert.doesNotMatch(selection, /MAX\(existing_member\.selection_ordinal\)/i);
});

test('selection preparation uses exact candidate diagnostics and Current Payment Status precedence', () => {
  assert.match(selection, /v_candidate_selection_json := pg_catalog\.jsonb_build_object\([\s\S]*?'scope_type',[\s\S]*?'CANDIDATES'[\s\S]*?'pay_batch_item_ids'/);
  assert.match(selection, /pay_payment_cancelability_diagnostic\([\s\S]*?v_candidate_selection_json/);
  assert.match(selection, /AS latest_work_status/);
  assert.match(selection, /latest_work_status = 'BLOCKED'[\s\S]*?THEN 'BLOCKED'/);
  assert.match(selection, /latest_work_status IN \('FAILED_FINAL', 'FAILED_RETRYABLE'\)[\s\S]*?THEN 'FAILED'/);
  assert.match(selection, /can_no_money_unwind'[\s\S]*?THEN 'NOT_PAID'/);
  assert.doesNotMatch(selection, /can_release_after_terminal_no_money/);
  assert.match(selection, /IS DISTINCT FROM v_effective_display_state/);
  assert.doesNotMatch(selection, /NOT IN \(v_eligibility_code, 'ACTIVE', 'NOT_PAID'\)/);
  assert.doesNotMatch(selection, /pay_batch_payment_status_page_v1\s*\(/);

  const diagnosticStart = installedFunctions.indexOf('CREATE OR REPLACE FUNCTION public.pay_payment_cancelability_diagnostic');
  const diagnosticEnd = installedFunctions.indexOf('\nCREATE OR REPLACE FUNCTION ', diagnosticStart + 10);
  const diagnostic = installedFunctions.slice(diagnosticStart, diagnosticEnd > diagnosticStart ? diagnosticEnd : installedFunctions.length);
  assert.ok(diagnosticStart >= 0, 'pay_payment_cancelability_diagnostic source missing');
  assert.match(diagnostic, /'can_no_money_unwind'/);
  assert.doesNotMatch(diagnostic, /'can_release_after_terminal_no_money'/);
});

test('automatic no-money start occurs only after exact lease validation', () => {
  const lease = processChunk.indexOf("COALESCE(v_operation.lease_owner, v_operation.locked_by) IS NULL");
  const autoStart = processChunk.indexOf('v_auto_start_result := public.pay_payment_correction_request_start');
  assert.ok(lease > 0 && autoStart > lease);
  assert.match(processChunk, /p_worker_id IS NULL OR pg_catalog\.btrim\(p_worker_id\) = ''/);
  assert.match(processChunk, /AUTO_START_EVIDENCE_STALE/);
});

test('automatic no-money authority is identical at producer, request and phase boundaries', () => {
  for (const source of [requestStart, processChunk]) {
    assert.match(source, /banking_pay_auto_unwind_terminal_no_money/);
    assert.match(source, /event_source NOT IN \('PROVIDER_WEBHOOK', 'PROVIDER_POLL', 'PROVIDER_RESPONSE'\)/);
    assert.match(source, /mapping_status IS DISTINCT FROM 'MATCHED'/);
    assert.match(source, /correction_disposition IS DISTINCT FROM 'AUTO_PROCESSING'/);
    assert.match(source, /safe_to_auto_apply/);
    assert.match(source, /terminal_no_money_evidence/);
    assert.match(source, /source_bank_event_id/);
    assert.match(source, /normalised_state NOT IN \('FAILED', 'REJECTED', 'CANCELLED'\)/);
    assert.doesNotMatch(source.slice(source.indexOf('v_auto_classification_result :='), source.indexOf("RAISE EXCEPTION 'PAYMENT_CORRECTION_AUTO", source.indexOf('v_auto_classification_result :='))), /CONFIRMED_NOT_PAID/);
  }
  for (const key of [
    'contract_version', 'requested_action', 'event_source', 'provider_event_key',
    'mapping_status', 'mapping_method', 'classification', 'safe_to_auto_apply',
    'correction_disposition', 'terminal_no_money_evidence', 'source_bank_event_id'
  ]) assert.match(eventIngest, new RegExp(`'${key}'`));
});

test('zero-claim contention yields a typed future database continuation', () => {
  assert.match(processChunk, /v_claimed_count = 0 AND v_nonterminal_count > 0/);
  assert.match(processChunk, /v_now \+ interval '5 seconds'/);
  assert.match(processChunk, /locked_at_utc \+ interval '120 seconds'/);
  assert.match(processChunk, /PAYMENT_CORRECTION_PROCESS_CONTENTION_RETRY/);
  assert.match(processChunk, /'run_after_utc', v_contention_retry_at/);
});

test('latest correction progress prefers an active request over terminal history', () => {
  const latestInto = statusPage.indexOf('INTO v_latest_correction_request');
  const latest = statusPage.slice(statusPage.lastIndexOf('SELECT pg_catalog.jsonb_strip_nulls', latestInto), statusPage.indexOf('LIMIT 1;', latestInto) + 8);
  assert.match(latest, /WHEN request_row\.status IN[\s\S]*'PLANNING'[\s\S]*THEN 0/);
  assert.match(latest, /request_row\.updated_at_utc DESC/);
  for (const field of ['correction_request_id', 'request_status', 'is_active', 'is_terminal', 'user_title', 'request_kind']) {
    assert.match(latest, new RegExp(`'${field}'`));
  }
  assert.doesNotMatch(latest, /'operation_id'/);
});

test('all correction mutation functions follow guard request batch operation ordering', () => {
  for (const source of [processChunk, expand, preBank, noMoney]) {
    const guard = source.indexOf('private.pay_payment_mutation_guard_v1');
    const request = source.indexOf('FROM public.pay_payment_correction_requests', guard);
    const batch = source.indexOf('FROM public.pay_batches', request);
    const operation = source.indexOf('FROM public.banking_pay_operations', batch);
    assert.ok(guard >= 0 && request > guard && batch > request && operation > batch);
  }
});

test('selection and finalisation reduce bounded durable summaries only', () => {
  assert.match(selection, /Totals are reduced from at most 200 durable page summaries/);
  assert.match(selection, /ordered_prepare_pages/);
  assert.match(processChunk, /LIMIT 100/);
  assert.match(processChunk, /result_json->>'active_net_amount_pence'/);
  assert.match(processChunk, /v_finalise_page_sequence > 100/);
  assert.doesNotMatch(processChunk, /unselected_candidates/);
  assert.match(checker, /WITH prepare_pages AS/);
  assert.match(checker, /WITH final_pages AS/);
  assert.doesNotMatch(checker, /jsonb_agg\([\s\S]{0,120}pay_batch_candidate_id/);
});

test('existing correction helpers have exact owner-only and service-only ACLs', () => {
  for (const fn of [
    '_pay_payment_correction_apply_accepted_finance_resolution',
    '_pay_payment_correction_mail_scope_match',
    '_pay_payment_correction_selected_items',
    '_pay_payment_correction_validate_accepted_finance_resolution',
  ]) {
    assert.match(acl, new RegExp(`ALTER FUNCTION public\\.${fn}`));
  }
  assert.equal((acl.match(/FROM PUBLIC, anon, authenticated, service_role;/g) || []).length, 6);
  assert.equal((acl.match(/GRANT EXECUTE[\s\S]*?TO service_role;/g) || []).length, 2);
});
