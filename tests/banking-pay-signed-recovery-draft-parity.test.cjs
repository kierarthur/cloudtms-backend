const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const sourcePath = path.join(
  root,
  'supabase',
  'repeatable',
  '01092026_1459_banking_pay_signed_recovery_draft_v1.sql'
);
const sql = fs.readFileSync(sourcePath, 'utf8');
const currentProjectionSql = fs.readFileSync(path.join(
  root,
  'supabase',
  'repeatable',
  '05092026_0655_banking_pay_active_draft_reservation_evidence_precedence_v1.sql'
), 'utf8');

function functionBody(schema, name, source = sql) {
  const marker = `CREATE OR REPLACE FUNCTION ${schema}.${name}`;
  const start = source.indexOf(marker);
  assert.ok(start >= 0, `${schema}.${name} must be present`);
  const end = source.indexOf('$function$;', start + marker.length);
  assert.ok(end > start, `${schema}.${name} must have a bounded body`);
  return source.slice(start, end + '$function$;'.length);
}

test('the signed recovery repair replaces only its classifier and the two existing Draft owners', () => {
  const definitions = [...sql.matchAll(/CREATE OR REPLACE FUNCTION\s+(?:public|private)\.([a-z0-9_]+)/gi)]
    .map((match) => match[1]);
  assert.deepEqual(definitions, [
    'pay_batch_signed_non_charge_recovery_evidence_v1',
    'pay_workbench_sealed_rate_component_projection_v1',
    'pay_batch_finalize_reservations_and_markers',
  ]);
  for (const forbidden of [
    'banking_pay_draft_create_step_v1',
    'pay_payment_execute',
    'pay_payment_settle',
    'pay_payment_cancel',
    'pay_provider',
  ]) {
    assert.doesNotMatch(
      sql,
      new RegExp(`CREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\s+(?:public|private)\\.${forbidden}\\b`, 'i')
    );
  }
});

test('the classifier accepts only one fully reconciled frozen non-charge return', () => {
  const body = functionBody('private', 'pay_batch_signed_non_charge_recovery_evidence_v1');
  for (const required of [
    'SIGNED_NON_CHARGE_RECOVERY_DRAFT_V1',
    'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID',
    'MATCHED_COMPONENT_CARDINALITY',
    'FROZEN_EVIDENCE_SHAPE',
    'FROZEN_EVIDENCE_RECONCILIATION',
    'WORKED_TIME_AMOUNT',
    'authoritative_truth_ex_vat',
    'authoritative_baseline_ex_vat',
    'authoritative_reserved_ex_vat',
    'authoritative_outstanding_ex_vat',
    'source_charge_ex_vat',
    'physical_bucket_digest',
    'financial_revision_digest',
    'target_authority_digest',
    'conversion_context_digest',
    'sealed_evidence_digest',
  ]) assert.match(body, new RegExp(required));

  assert.match(body, /v_outstanding\s*<>\s*ROUND\(v_truth\s*-\s*v_baseline\s*-\s*v_reserved,\s*2\)/i);
  assert.match(body, /v_component_amount\s*<>\s*v_outstanding/i);
  assert.match(body, /v_source_pay\s*<>\s*v_outstanding/i);
  assert.match(body, /v_source_charge\s*<>\s*0/i);
  assert.match(body, /sealed_evidence_version',\s*2/i);
  assert.match(body, /v_component->>'sealed_evidence_digest'\s+IS DISTINCT FROM\s+v_expected_digest/i);
  assert.doesNotMatch(body, /\bFROM\s+(?:public|private)\./i,
    'post-Draft classification must consume only the frozen item document');
});

test('the sealed projection preserves the positive return and its paired negative recovery without live fallback', () => {
  const body = functionBody('private', 'pay_workbench_sealed_rate_component_projection_v1', currentProjectionSql);
  assert.match(body, /reservation_totals[\s\S]*signed_recovery\.evidence_json[\s\S]*outstanding_ex_vat/i);
  assert.match(body, /sealed_parent_facts[\s\S]*FROZEN_SIGNED_NON_CHARGE_RECOVERY/i);
  assert.match(body, /is_signed_non_charge_recovery[\s\S]*allocative_parent_facts/i);
  assert.match(body, /WHERE parent\.is_signed_non_charge_recovery IS NOT TRUE/i);
  assert.match(body, /signed_non_charge_recovery_contract/i);
  assert.doesNotMatch(
    body,
    /signed_recovery\.evidence_json[\s\S]{0,800}(?:FROM|JOIN)\s+public\.(?:timesheets|timesheet_financials|pay_timesheet_financial)/i,
    'signed return authority must not fall back to current finance after Draft creation'
  );
});

test('the final reservation owner validates the frozen return and rejects another active Draft', () => {
  const body = functionBody('public', 'pay_batch_finalize_reservations_and_markers');
  for (const required of [
    'SIGNED_NON_CHARGE_RECOVERY_ITEM_AMOUNT_MISMATCH',
    'BANKING_PAY_SIGNED_NON_CHARGE_RECOVERY',
    'active_signed_reservations',
    'FROZEN_SIGNED_NON_CHARGE_RECOVERY',
    'PAY_BATCH_RESERVATION_OVERRUN',
  ]) assert.match(body, new RegExp(required));

  assert.match(
    body,
    /timesheet_ids[\s\S]*is_signed_non_charge_recovery IS NOT TRUE[\s\S]*_pay_outstanding_components/i,
    'ordinary current outstanding must not replace frozen signed-return evidence'
  );
  assert.match(
    body,
    /active_batch\.id\s*<>\s*p_pay_batch_id[\s\S]*_pay_batch_status_is_active_reservation[\s\S]*active_evidence\.evidence_json IS NOT NULL/i
  );
  assert.match(
    body,
    /active_reservation_count,\s*0[\s\S]*frozen_signed_outstanding_ex_vat[\s\S]*ELSE 0::numeric/i
  );
  assert.match(
    body,
    /WHEN scoped_component_rows\.is_signed_non_charge_recovery[\s\S]*requested_source_amount_ex_vat/i
  );
  assert.doesNotMatch(body, /pay_timesheet_summary_pay_state_refresh[\s\S]*signed_non_charge_recovery_evidence/i,
    'later summary refresh must not become the authority for the frozen reservation check');
});

test('the classifier remains private and the established finalizer ACL is preserved', () => {
  assert.match(
    sql,
    /REVOKE ALL ON FUNCTION private\.pay_batch_signed_non_charge_recovery_evidence_v1\(jsonb\)[\s\S]*FROM PUBLIC, anon, authenticated;[\s\S]*GRANT EXECUTE ON FUNCTION private\.pay_batch_signed_non_charge_recovery_evidence_v1\(jsonb\)[\s\S]*TO postgres;/i
  );
  assert.match(
    sql,
    /REVOKE ALL ON FUNCTION public\.pay_batch_finalize_reservations_and_markers\([\s\S]*FROM PUBLIC, anon, authenticated;[\s\S]*GRANT EXECUTE ON FUNCTION public\.pay_batch_finalize_reservations_and_markers\([\s\S]*TO postgres, service_role;/i
  );
});
