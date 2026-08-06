const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const worker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
const statusSql = fs.readFileSync(
  path.join(root, 'supabase', 'repeatable', '04082026_1146_pay_batch_payment_status_page_v1.sql'),
  'utf8'
);
const eventSql = fs.readFileSync(
  path.join(root, 'supabase', 'repeatable', '04082026_1210_pay_bank_event_ingest.sql'),
  'utf8'
);

function functionBody(name) {
  const marker = `async function ${name}`;
  const start = worker.indexOf(marker);
  assert.notEqual(start, -1, `${name} is missing`);
  const open = worker.indexOf('{', start);
  let depth = 0;
  let quote = null;
  let escaped = false;
  for (let index = open; index < worker.length; index += 1) {
    const char = worker[index];
    if (quote) {
      if (escaped) escaped = false;
      else if (char === '\\') escaped = true;
      else if (char === quote) quote = null;
      continue;
    }
    if (char === '"' || char === "'" || char === '`') {
      quote = char;
      continue;
    }
    if (char === '{') depth += 1;
    if (char === '}') {
      depth -= 1;
      if (depth === 0) return worker.slice(open, index + 1);
    }
  }
  throw new Error(`${name} has no closing brace`);
}

test('status-page detailed projection carries resolution context authority into classification', () => {
  const baseStart = statusSql.indexOf('), base AS (');
  const classifiedStart = statusSql.indexOf('), classified AS (', baseStart);
  assert.ok(baseStart > 0 && classifiedStart > baseStart);
  const base = statusSql.slice(baseStart, classifiedStart);
  assert.match(base, /status_index\.has_resolution_context/);
  assert.match(statusSql.slice(classifiedStart), /base\.ambiguous\s+AND\s+base\.has_resolution_context/);
});

test('manual resolution keeps batch-membership and candidate identity domains separate', () => {
  const body = functionBody('handleBankingPayPaymentStatusResolveV1');
  assert.match(body, /payBatchCandidateId\s*=\s*bankingPayCorrectionUuid\(resolutionContext\.candidate_token/);
  assert.match(body, /currentCandidateId\s*=\s*bankingPayCorrectionUuid\(currentRow\.candidate_id/);
  assert.match(body, /pay_batch_candidate_ids:\s*\[payBatchCandidateId\]/);
  assert.match(body, /pay_batch_candidate_id',\s*`eq\.\$\{payBatchCandidateId\}`/);
  assert.match(body, /pay_batch_candidate_id:\s*payBatchCandidateId/);
  assert.match(body, /candidate_id:\s*currentCandidateId/);
  assert.doesNotMatch(body, /(?:^|\n)\s*candidate_id:\s*payBatchCandidateId/);
});

test('manual resolution submits the complete 1..128 instruction scope in one event RPC', () => {
  const body = functionBody('handleBankingPayPaymentStatusResolveV1');
  assert.match(body, /instructionScopeIds[^\n]+128/);
  assert.match(body, /pay_bank_transfer_ids:\s*instructionScopeIds/);
  assert.doesNotMatch(body, /instructionScopeIds\s*\[\s*0\s*\]/);
  assert.equal((body.match(/sbRpc\(env, 'pay_bank_event_ingest'/g) || []).length, 1);
});

test('database manual-scope branch validates everything before bounded atomic member ingestion', () => {
  assert.match(eventSql, /v_manual_scope_mode/);
  assert.match(eventSql, /v_manual_scope_count\s*<\s*1\s+OR\s+v_manual_scope_count\s*>\s*128/);
  assert.match(eventSql, /MANUAL_PAYMENT_STATUS_CANDIDATE_DOMAIN_MISMATCH/);
  assert.match(eventSql, /public\.pay_payment_cancelability_diagnostic/);
  assert.match(eventSql, /v_manual_scope_diagnostic_transfer_ids\s+IS DISTINCT FROM\s+v_manual_scope_transfer_ids/);
  assert.match(eventSql, /ORDER BY transfer_row\.id\s+FOR UPDATE/);
  assert.match(eventSql, /FOREACH v_pay_bank_transfer_id IN ARRAY v_manual_scope_transfer_ids/);
  assert.match(eventSql, /v_manual_scope_idempotency_base\s*\|\|\s*':'\s*\|\|\s*v_pay_bank_transfer_id::text/);
  assert.match(eventSql, /'scope_atomic', true/);
  assert.equal((eventSql.match(/'MANUAL_PAYMENT_STATUS_SCOPE_RECORDED'/g) || []).length, 1);
});

test('database scope replay does not create another batch signal', () => {
  assert.match(eventSql, /IF v_manual_scope_all_idempotent THEN[\s\S]+?'duplicate_scope', true[\s\S]+?ELSIF v_should_touch_signal THEN/);
});
