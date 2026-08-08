const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const repeatableDir = path.join(root, 'supabase', 'repeatable');
const monolithName = '26052026_2100HRS_NEW_FUNCTIONS.sql';
const reassertName = '08082026_0902_reassert_authorities_after_legacy_monolith.sql';
const monolith = fs.readFileSync(path.join(repeatableDir, monolithName), 'utf8');
const reassert = fs.readFileSync(path.join(repeatableDir, reassertName), 'utf8');

const dateKey = (name) => {
  const match = name.match(/^(\d{2})(\d{2})(\d{4})[_-]?(\d{2})?(\d{2})?/);
  if (!match) return '999999999999';
  return `${match[3]}${match[2]}${match[1]}${match[4] || '00'}${match[5] || '00'}`;
};

const createdFunctions = (source) => [...source.matchAll(
  /CREATE\s+OR\s+REPLACE\s+FUNCTION\s+(?:(public|private)\.)?([a-zA-Z0-9_]+)\s*\(/gi,
)].map((match) => `${(match[1] || 'public').toLowerCase()}.${match[2].toLowerCase()}`);

const touchedFunctions = (source) => [...source.matchAll(
  /(?:CREATE\s+OR\s+REPLACE|ALTER|DROP)\s+FUNCTION(?:\s+IF\s+EXISTS)?\s+(?:(public|private)\.)?([a-zA-Z0-9_]+)\s*\(/gi,
)].map((match) => `${(match[1] || 'public').toLowerCase()}.${match[2].toLowerCase()}`);

test('legacy monolith changes force a complete later-authority replay', () => {
  const monolithHash = crypto.createHash('sha256').update(monolith).digest('hex');
  assert.match(reassert, new RegExp(`legacy_monolith_sha256:\\s*\\n-- ${monolithHash}`));

  const monolithFunctions = new Set(createdFunctions(monolith));
  const expectedFiles = fs.readdirSync(repeatableDir)
    .filter((name) => name.endsWith('.sql'))
    .filter((name) => name !== monolithName && name !== reassertName)
    .filter((name) => dateKey(name) > dateKey(monolithName))
    .filter((name) => touchedFunctions(
      fs.readFileSync(path.join(repeatableDir, name), 'utf8'),
    ).some((identity) => monolithFunctions.has(identity)))
    .sort((left, right) => dateKey(left).localeCompare(dateKey(right)) || left.localeCompare(right));

  const includedFiles = [...reassert.matchAll(/^\\ir\s+([^\s]+\.sql)\s*$/gmi)]
    .map((match) => match[1]);
  assert.deepEqual(includedFiles, expectedFiles);
  assert.equal(new Set(includedFiles).size, includedFiles.length);
  assert.doesNotMatch(reassert, /\\ir\s+26052026_2100HRS_NEW_FUNCTIONS\.sql/i);
});
