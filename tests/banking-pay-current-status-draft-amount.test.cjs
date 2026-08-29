const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const sql = fs.readFileSync(path.resolve(__dirname, '..', 'supabase', 'repeatable', '04082026_1146_pay_batch_payment_status_page_v1.sql'), 'utf8');

test('Current Payment Status derives pre-execution PAYE bank value from the saved PAYE net input', () => {
  assert.match(sql, /FROM public\.pay_batch_paye_net_inputs AS paye_input/);
  assert.match(sql, /paye_input\.pay_batch_candidate_id = candidate_row\.id/);
  assert.match(sql, /ORDER BY paye_input\.imported_at_utc DESC, paye_input\.id DESC/);
  assert.match(sql, /status_index\.reviewed_payment_amount/);
});

test('mixed candidate bank value retains Umbrella payable value as well as saved PAYE net', () => {
  assert.match(sql, /pg_catalog\.sum\(umbrella_scope_item\.amount_inc_vat\)/);
  assert.match(sql, /umbrella_scope_item\.pay_batch_candidate_id = candidate_row\.id/);
  assert.match(sql, /pg_catalog\.upper\(pg_catalog\.btrim\(COALESCE\(umbrella_scope_item\.pay_channel, ''\)\)\) = 'UMBRELLA'/);
});
