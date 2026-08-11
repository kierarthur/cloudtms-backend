const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const worker = fs.readFileSync(path.resolve(__dirname, '..', 'broker', 'src', 'index.js'), 'utf8');
const authSql = fs.readFileSync(path.resolve(__dirname, '..', 'supabase', 'repeatable', '04082026_1158_pay_batch_auth_start.sql'), 'utf8');

test('Worker rejects payment and scheduled dates before today in Europe/London', () => {
  const start = worker.indexOf('async function handleBankingPayBatchExecutePayment');
  const end = worker.indexOf('\nasync function ', start + 1);
  assert.ok(start >= 0 && end > start, 'execute-payment handler must exist');
  const body = worker.slice(start, end);
  assert.match(body, /toLocalParts\(new Date\(\)\.toISOString\(\), 'Europe\/London'\)/);
  assert.match(body, /PAYMENT_DATE_IN_PAST/);
  assert.match(body, /SCHEDULE_DATE_IN_PAST/);
  assert.match(body, /The payment date must be today or in the future\./);
});

test('database authorisation owner independently enforces the UK date boundary', () => {
  assert.match(authSql, /v_payment_date < \(v_now AT TIME ZONE 'Europe\/London'\)::date/);
  assert.match(authSql, /'code', 'PAYMENT_DATE_IN_PAST'/);
  assert.match(authSql, /\(v_scheduled_at_utc AT TIME ZONE 'Europe\/London'\)::date < \(v_now AT TIME ZONE 'Europe\/London'\)::date/);
  assert.match(authSql, /'code', 'SCHEDULE_DATE_IN_PAST'/);
});
