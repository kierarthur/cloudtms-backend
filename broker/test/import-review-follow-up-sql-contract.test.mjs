import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const lifecycle = readFileSync(new URL('../../supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql', import.meta.url), 'utf8');
const canonicalContract = readFileSync(new URL('../../supabase/repeatable/25072026_1615_banking_pay_canonical_correction_carrier.sql', import.meta.url), 'utf8');
const retirements = readFileSync(new URL('../../supabase/repeatable/21072026_1820_99_import_review_hard_cutover_retirements.sql', import.meta.url), 'utf8');
const tsfinSummary = readFileSync(new URL('../../supabase/repeatable/04022026_nhsp_hr_code.sql', import.meta.url), 'utf8');
const tsfinSettlement = readFileSync(new URL('../../supabase/repeatable/23072026_0011_import_review_tsfin_settlement_v1.sql', import.meta.url), 'utf8');
const invoiceCorrectionScope = readFileSync(new URL('../../supabase/repeatable/21072026_1235_10_invoice_correction_pair_scope_v1.sql', import.meta.url), 'utf8');
const invoiceOutboxGenerator = readFileSync(new URL('../../supabase/repeatable/21072026_1235_56_invoice_generate_from_outbox_batch.sql', import.meta.url), 'utf8');

function functionBody(source, name) {
  const marker = `create or replace function public.${name}(`;
  const start = source.toLowerCase().indexOf(marker.toLowerCase());
  assert.notEqual(start, -1, `${name} must be defined`);
  const end = source.toLowerCase().indexOf('$function$;', start);
  assert.notEqual(end, -1, `${name} must have a complete body`);
  return source.slice(start, end + '$function$;'.length);
}

test('database contract exposes the component-aware follow-up capability', () => {
  const body = functionBody(canonicalContract, 'import_review_contract_version_get_v1');
  assert.match(body, /follow_up_component_version'\s*,\s*'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_V1/);
  assert.match(body, /tsfin_follow_up_settlement_version'\s*,\s*'IMPORT_REVIEW_TSFIN_SETTLEMENT_V1/);
  assert.match(body, /canonical_correction_carrier_version'\s*,\s*v_canonical_contract_version/);
  assert.match(body, /targeted_family_materialisation_version'\s*,\s*v_targeted_family_materialisation_version/);
  assert.match(body, /legacy_contracts_supported'\s*,\s*false/);
  assert.doesNotMatch(lifecycle, /create or replace function public\.import_review_contract_version_get_v1\(/i);
});

test('component update is request-hash bound and stores independent EMAIL and TSFIN state', () => {
  const body = functionBody(lifecycle, 'import_review_follow_up_component_update_v1');
  assert.match(body, /p_request_hash text/);
  assert.match(body, /o\.request_hash is distinct from lower\(btrim\(p_request_hash\)\)/);
  assert.match(body, /v_component not in \('EMAIL','TSFIN'\)/);
  assert.match(body, /review_email_follow_up_status/);
  assert.match(body, /review_tsfin_follow_up_status/);
  assert.match(body, /v_current='FAILED_RETRYABLE' and v_new='PENDING'/);
  assert.match(body, /v_current='PENDING' and v_new in \('COMPLETE','FAILED_RETRYABLE'\)/);
});

test('aggregate reconciliation derives bounded errors from the failed components', () => {
  const body = functionBody(lifecycle, '_import_review_follow_up_reconcile_core_v1');
  assert.match(body, /review_email_follow_up_error_code/);
  assert.match(body, /review_tsfin_follow_up_error_code/);
  assert.match(body, /MULTIPLE_FOLLOW_UP_COMPONENTS_FAILED/);
  assert.match(body, /follow_up_error_code=v_error_code/);
  assert.match(body, /follow_up_error_message=v_error_message/);
});

test('the new mutation RPC is service-role-only and the superseded TSFIN-only RPC is retired', () => {
  assert.match(lifecycle, /revoke all on function public\.import_review_follow_up_component_update_v1\([^)]+\) from public,anon,authenticated;/);
  assert.match(lifecycle, /grant execute on function public\.import_review_follow_up_component_update_v1\([^)]+\) to service_role;/);
  assert.doesNotMatch(lifecycle, /create or replace function public\.import_review_follow_up_update_v1\(/);
  assert.match(retirements, /drop function if exists public\.import_review_follow_up_update_v1\(/);
});

test('read contracts expose bounded aggregate follow-up diagnostics for the frontend', () => {
  for (const name of ['import_review_get_v1', 'import_review_apply_status_get_v1']) {
    const body = functionBody(lifecycle, name);
    assert.match(body, /follow_up_error_code/);
    assert.match(body, /follow_up_error_message/);
    assert.match(body, /follow_up_retry_count/);
  }
});

test('TSFIN pending summary exposes a freshness fence without changing its signature', () => {
  const body = functionBody(tsfinSummary, 'tsfin_outbox_pending_summary');
  assert.match(body, /p_timesheet_ids uuid\[\]/);
  assert.match(body, /max\(o\.created_at\) as latest_created_at/);
  assert.match(body, /'latest_created_at', v_latest_created_at/);
});

test('TSFIN follow-up settlement proof is bounded, commit-fenced and service-role-only', () => {
  const body = functionBody(tsfinSettlement, 'tsfin_follow_up_target_summary_v1');
  assert.match(body, /p_timesheet_ids uuid\[\]/);
  assert.match(body, /p_not_before_utc timestamptz/);
  assert.match(body, /v_target_count > 5000/);
  assert.match(body, /tf\.timesheet_version is not distinct from ts\.version/);
  assert.match(body, /_ctms_import_correction_classify_v1\(ts\.timesheet_id\)/);
  assert.match(body, /tf\.policy_snapshot_json->'correction_financials_policy_envelope'/);
  assert.match(body, /is not distinct from ts\.candidate_hint_text->'correction_financials_policy_envelope'/);
  assert.match(body, /coalesce\(tf\.computed_at_utc, tf\.updated_at, tf\.created_at\) >= p_not_before_utc/);
  assert.match(body, /'all_targets_settled'/);
  assert.match(tsfinSettlement, /revoke all on function public\.tsfin_follow_up_target_summary_v1\(uuid\[\], timestamptz\) from public, anon, authenticated;/);
  assert.match(tsfinSettlement, /grant execute on function public\.tsfin_follow_up_target_summary_v1\(uuid\[\], timestamptz\) to service_role;/);
});

test('invoice correction scope follows the frozen TSFIN stream used by the generator', () => {
  const body = functionBody(invoiceCorrectionScope, 'invoice_correction_pair_scope_v1');
  assert.match(body, /tf.basis/);
  assert.match(body, /'NHSP','NHSP_ADJUSTMENT'/);
  assert.match(body, /'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'/);
  assert.match(body, /v_current_stream is not distinct from v_expected_stream/);
  assert.doesNotMatch(body, /v_current_stream:=case when coalesce(r.self_bill,false)/);
  assert.doesNotMatch(body, /case when coalesce(c.self_bill,false) then 'SELF_BILL'/);
});

test('NONE invoice consolidation keeps each correction unit atomic without merging ordinary timesheets', () => {
  const body = functionBody(invoiceOutboxGenerator, 'invoice_generate_from_outbox_batch');
  assert.match(body, /Mode NONE keeps ordinary timesheets separate/);
  assert.ok(body.includes('_ctms_import_correction_classify_v1(u.tid)'));
  assert.ok(body.includes('then coalesce(ts.correction_id::text,u.tid::text)'));
  assert.match(body, /else u.tid::text/);
  assert.ok(body.includes('array_agg(tid order by tid)::uuid[] as ts_ids'));
  assert.ok(body.includes("(e.value->>'timesheet_id')::uuid = any(g.ts_ids)"));
});

test('empty and exact-page review lists use typed cursors and emit a cursor only when more rows exist', () => {
  const body = functionBody(lifecycle, 'import_review_list_v1');
  assert.match(body, /v_last_updated_at timestamptz/);
  assert.match(body, /v_last_import_id uuid/);
  assert.match(body, /v_has_more boolean:=false/);
  assert.match(body, /select count\(\*\)>v_limit from page/);
  assert.match(body, /case when v_has_more then jsonb_build_object/);
  assert.doesNotMatch(body, /v_last record/);
});
