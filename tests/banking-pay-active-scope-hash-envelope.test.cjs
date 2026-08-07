const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = (name) => fs.readFileSync(path.join(root, 'supabase', 'repeatable', name), 'utf8');

function activeScopeEnvelope(source, variableName) {
  const start = source.indexOf(`${variableName} := private.pay_payment_correction_sha256_v1(`);
  assert.notEqual(start, -1, `${variableName} assignment must exist`);
  const end = source.indexOf('\n  );', start);
  assert.notEqual(end, -1, `${variableName} assignment must be bounded`);
  return source.slice(start, end);
}

test('authorisation start, final approval, and scheduling hash the same freshness-bound active scope', () => {
  const start = activeScopeEnvelope(read('04082026_1158_pay_batch_auth_start.sql'), 'v_current_active_scope_hash');
  const apply = activeScopeEnvelope(read('04082026_1154_pay_batch_auth_apply_action.sql'), 'v_active_scope_hash');
  const schedule = activeScopeEnvelope(read('04082026_1158_pay_batch_schedule.sql'), 'v_current_active_scope_hash');

  assert.match(start, /'freshness_result_hash',\s*v_effective_freshness_result_hash/i);
  assert.match(start, /'freshness_scope_hash',\s*v_effective_freshness_scope_hash/i);
  for (const envelope of [apply, schedule]) {
    assert.match(envelope, /'freshness_result_hash',\s*v_execution_intent_json->>'freshness_result_hash'/i);
    assert.match(envelope, /'freshness_scope_hash',\s*v_execution_intent_json->>'freshness_scope_hash'/i);
    assert.match(envelope, /'active_items'/i);
  }
});

test('scope comparison remains fail-closed after the hash-envelope correction', () => {
  const apply = read('04082026_1154_pay_batch_auth_apply_action.sql');
  const schedule = read('04082026_1158_pay_batch_schedule.sql');
  assert.match(apply, /AUTH_SCOPE_STALE/i);
  assert.match(schedule, /REAUTHORISATION_REQUIRED/i);
  assert.match(apply, /is distinct from v_active_scope_hash/i);
  assert.match(schedule, /is distinct from v_current_active_scope_hash/i);
});
