const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const historicalPath = path.join(root, 'supabase', 'repeatable', '21072026_1235_49_pay_batch_apply_finance_adjustments.sql');
const ownerPath = path.join(root, 'supabase', 'repeatable', '02092026_2320_banking_pay_draft_finance_row_transport_v8.sql');
const historical = fs.readFileSync(historicalPath, 'utf8').replace(/\r\n/g, '\n').trim();
const owner = fs.readFileSync(ownerPath, 'utf8').replace(/\r\n/g, '\n').trim();

const historicalHeader = [
  '-- CloudTMS reviewed direct replacement; review artifact only, not installed.',
  '-- Exact TEST baseline body MD5 prefix: d7366616e4f6.',
  '-- Ordinary and non-import-authoritative branches remain on the installed implementation.'
].join('\n');
const transportHeader = [
  '-- CloudTMS Banking Pay V8 row-backed transport replacement.',
  '-- Runtime authority is Miget TEST. The historical finance owner remains byte-identical.',
  '-- The only semantic diff is transport: Stage 16C0 reads the same canonical lines',
  '-- through the V8 row-backed helper when the certified Draft route is active.'
].join('\n');
const legacyRead = `      from jsonb_array_elements(coalesce(
        operation_scope.selected_canonical_preview_lines_json,
        '[]'::jsonb
      )) with ordinality as canonical_line(value,ordinality)`;
const rowBackedRead = `      from private.pay_workbench_draft_scope_line_rows_v8(
        operation_scope.id,
        operation_scope.selected_canonical_preview_lines_json,
        operation_scope.effective_canonical_preview_lines_json
      ) as canonical_line(value,ordinality)`;

test('finance replacement contains one exact function and one transport substitution', () => {
  assert.equal((owner.match(/CREATE OR REPLACE FUNCTION/g) || []).length, 1);
  assert.equal((owner.match(/private\.pay_workbench_draft_scope_line_rows_v8\(/g) || []).length, 1);
  assert.equal((owner.match(/selected_canonical_preview_lines_json/g) || []).length, 1);
  assert.doesNotMatch(owner, /from jsonb_array_elements\(coalesce\(\s*operation_scope\.selected_canonical_preview_lines_json/i);

  const reconstructedHistorical = owner
    .replace(transportHeader, historicalHeader)
    .replace(rowBackedRead, legacyRead);
  assert.equal(reconstructedHistorical, historical);
});

test('finance policy vocabulary and equations are byte-identical to the current owner', () => {
  const normalize = (sql) => sql
    .replace(transportHeader, historicalHeader)
    .replace(rowBackedRead, legacyRead);
  assert.equal(normalize(owner), historical);
  for (const token of [
    'PAYMENT_ADVANCE_REPAYMENT',
    'LOAN_REPAYMENT',
    'OVERPAYMENT_RECOVERY',
    'MANUAL_DEBT_RECOVERY',
    'LOAN_PAYOUT',
    'UNDERPAYMENT_PAYMENT',
    'MANUAL_CREDIT_ADJUSTMENT_PAYMENT',
    'MANUAL_CREDIT_PAYOUT'
  ]) {
    assert.equal((owner.match(new RegExp(token, 'g')) || []).length,
      (historical.match(new RegExp(token, 'g')) || []).length,
      `${token} occurrence count must be unchanged`);
  }
});

test('signature, metadata, privileges and timeout behavior are unchanged', () => {
  for (const pattern of [
    /CREATE OR REPLACE FUNCTION public\.pay_batch_apply_finance_adjustments\(/,
    /SECURITY DEFINER/,
    /SET search_path TO 'public'/,
    /REVOKE ALL ON FUNCTION public\.pay_batch_apply_finance_adjustments\([\s\S]*?FROM PUBLIC, anon, authenticated/,
    /GRANT EXECUTE ON FUNCTION public\.pay_batch_apply_finance_adjustments\([\s\S]*?TO service_role/
  ]) {
    assert.match(owner, pattern);
    assert.match(historical, pattern);
  }
  assert.equal(/ALTER FUNCTION public\.pay_batch_apply_finance_adjustments/i.test(owner),
    /ALTER FUNCTION public\.pay_batch_apply_finance_adjustments/i.test(historical));
  assert.doesNotMatch(owner, /SET\s+(?:LOCAL\s+)?(?:statement_timeout|lock_timeout)/i);
});

test('legacy and row-backed transport share the same match and provenance guards', () => {
  assert.match(owner, /select count\(\*\)::integer as match_count/);
  assert.match(owner, /nullif\(canonical_line\.value->>'row_key',''\)/);
  assert.match(owner, /nullif\(canonical_line\.value->>'source_ref',''\)/);
  assert.match(owner, /=batch_item\.source_ref/);
  assert.match(owner, /coalesce\(operation_line\.match_count,0\)=1/);
});
