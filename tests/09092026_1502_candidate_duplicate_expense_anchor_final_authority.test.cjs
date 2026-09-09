const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');
const sha256 = (value) => crypto.createHash('sha256').update(value).digest('hex');

const historicalPath = 'supabase/repeatable/04092026_1952_candidate_expense_history_anchor_recovery_v1.sql';
const currentOwnerPath = 'supabase/repeatable/08092026_0631_candidate_duplicate_expense_anchor_inclusion_v1.sql';
const finalClosurePath = 'supabase/repeatable/09092026_1500_candidate_duplicate_expense_anchor_final_authority_v1.sql';
const verifierPath = 'supabase/verification/09092026_1501_candidate_duplicate_expense_anchor_final_authority_verification.sql';

test('the final closure restores only the exact current duplicate-expense owner', () => {
  const historical = read(historicalPath);
  const currentOwner = read(currentOwnerPath);
  const finalClosure = read(finalClosurePath);

  assert.equal(sha256(historical), '639b53dfaf72b629e6c6064c9e816af88a63e67b0247117c1b4881aa27ea752a');
  assert.equal(sha256(currentOwner), 'fb09f450e5e54fa2b2678446aa9473270a673d14434b42d21d16d5c06177bf9e');
  assert.match(finalClosure, /^\\set ON_ERROR_STOP on$/m);
  assert.match(finalClosure, /^\\ir 08092026_0631_candidate_duplicate_expense_anchor_inclusion_v1\.sql$/m);
  assert.equal((finalClosure.match(/^\\ir /gm) || []).length, 1);
  assert.doesNotMatch(finalClosure, /create\s+(?:or\s+replace\s+)?function/i);
  assert.doesNotMatch(finalClosure, /\b(insert|update|delete|merge|truncate)\b/i);
});

test('the final authority is later than both the replaying historical closure and current owner', async () => {
  const { sqlDateKey } = await import('../scripts/cloudtms-db-release-lib.mjs');
  assert.ok(sqlDateKey(finalClosurePath) > sqlDateKey(historicalPath));
  assert.ok(sqlDateKey(finalClosurePath) > sqlDateKey(currentOwnerPath));
});

test('the verifier freezes the intended definition, metadata and private ACL', () => {
  const verifier = read(verifierPath);
  assert.match(verifier, /366fbaae56a5db9c628126f50ce078763f877c611dfae6cc26c587b565990ec8/);
  assert.doesNotMatch(verifier, /249757a20e512e182e6e3a4c8f6c17f638afd5aa1164f7238d1f1e78a38659be/);
  assert.match(verifier, /prosecdef[\s\S]*provolatile[\s\S]*proparallel[\s\S]*search_path=pg_catalog, public, private, pg_temp/i);
  assert.match(verifier, /aclexplode[\s\S]*grantee=0[\s\S]*privilege_type='EXECUTE'/i);
  for (const role of ['anon', 'authenticated', 'service_role']) {
    assert.match(verifier, new RegExp(`has_function_privilege\\(\\s*'${role}'`, 'i'));
  }
  assert.match(verifier, /begin;[\s\S]*rollback;/i);
  assert.doesNotMatch(verifier, /\bcommit\s*;/i);
});

test('both release modes run the final-authority verifier', () => {
  const release = JSON.parse(read('supabase/release/current-release.json'));
  for (const field of ['verificationFiles', 'newVerificationFiles']) {
    assert.equal(release[field].filter((entry) => entry === verifierPath).length, 1, field);
  }
});
