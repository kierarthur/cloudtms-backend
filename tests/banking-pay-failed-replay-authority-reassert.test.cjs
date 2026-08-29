const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const repeatableDir = path.join(root, 'supabase', 'repeatable');
const reassertName = '29082026_0540_banking_pay_failed_replay_authority_reassert.sql';
const certifiedName = '29082026_0326_banking_pay_release_authority_repair_v1.sql';
const source = fs.readFileSync(path.join(repeatableDir, reassertName), 'utf8')
  .replace(/\r\n/g, '\n');

test('failed historical replay reasserts only the certified Banking Pay authority closure', () => {
  const includes = [...source.matchAll(/^\\ir\s+([^\s]+\.sql)\s*$/gmi)]
    .map((match) => match[1]);

  assert.deepEqual(includes, [certifiedName]);
  assert.doesNotMatch(source, /26052026_2100HRS_NEW_FUNCTIONS\.sql/i);
  assert.doesNotMatch(source, /08082026_0902_reassert_authorities_after_legacy_monolith\.sql/i);
  assert.doesNotMatch(source, /\b(?:INSERT|UPDATE|DELETE|TRUNCATE)\b/i);
  assert.doesNotMatch(source, /plpgsql_check\./i);
});
