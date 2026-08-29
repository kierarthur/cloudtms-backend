const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const readRepeatable = (name) => fs.readFileSync(
  path.join(root, 'supabase', 'repeatable', name),
  'utf8',
);

const canonical = readRepeatable('20072026_1228_allow_paye_group_destination_in_prepare.sql');
const legacyMonolith = readRepeatable('26052026_2100HRS_NEW_FUNCTIONS.sql');
const authStart = readRepeatable('04082026_1158_pay_batch_auth_start.sql');

test('legacy monolith cannot remove the canonical payment prepare authority', () => {
  assert.doesNotMatch(
    legacyMonolith,
    /DROP\s+FUNCTION\s+IF\s+EXISTS\s+public\.pay_batch_prepare\s*\(/i,
  );
  assert.match(
    legacyMonolith,
    /pay_batch_prepare is maintained in\s*\n-- 20072026_1228_allow_paye_group_destination_in_prepare\.sql/i,
  );
});

test('canonical payment prepare authority is unique and service-role only', () => {
  const bodies = canonical.match(
    /CREATE\s+OR\s+REPLACE\s+FUNCTION\s+public\.pay_batch_prepare\s*\(/gi,
  ) || [];
  assert.equal(bodies.length, 1);
  assert.match(canonical, /SECURITY\s+DEFINER/i);
  assert.match(
    canonical,
    /REVOKE\s+ALL\s+ON\s+FUNCTION\s+public\.pay_batch_prepare\(uuid, uuid, uuid, text\)\s+FROM\s+PUBLIC/i,
  );
  assert.match(
    canonical,
    /REVOKE\s+ALL\s+ON\s+FUNCTION\s+public\.pay_batch_prepare\(uuid, uuid, uuid, text\)\s+FROM\s+authenticated/i,
  );
  assert.match(
    canonical,
    /GRANT\s+EXECUTE\s+ON\s+FUNCTION\s+public\.pay_batch_prepare\(uuid, uuid, uuid, text\)\s+TO\s+service_role/i,
  );
});

test('authorisation start retains the canonical prepare proof dependency', () => {
  assert.match(
    authStart,
    /v_prepare_json\s*:=\s*public\.pay_batch_prepare\s*\(/i,
  );
});
