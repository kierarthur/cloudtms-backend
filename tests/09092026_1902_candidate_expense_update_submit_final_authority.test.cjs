const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');
const sha256 = (value) => crypto.createHash('sha256').update(value).digest('hex');

const replayRootPath = 'supabase/repeatable/06092026_1636_candidate_advanced_expense_component_policy_v1.sql';
const currentOwnerPath = 'supabase/repeatable/08092026_0918_candidate_expense_update_root_snapshot_validation_v1.sql';
const finalClosurePath = 'supabase/repeatable/09092026_1900_candidate_expense_update_submit_final_authority_v1.sql';
const verifierPath = 'supabase/verification/09092026_1901_candidate_expense_update_submit_final_authority_verification.sql';

test('the final closure replays the changed root then the exact established submit owner', () => {
  const replayRoot = read(replayRootPath);
  const currentOwner = read(currentOwnerPath);
  const finalClosure = read(finalClosurePath);

  assert.equal(sha256(replayRoot), 'f8f415a641f7ecdece1068d672f407370962e9aaeecc4f790ce832f791ce6908');
  assert.equal(sha256(currentOwner), 'cec8f0bcab33138ee33781dc8ac28edc8be4bd65818d93b9a97915f37b72a553');
  assert.match(finalClosure, /^\\set ON_ERROR_STOP on$/m);
  assert.match(finalClosure, /^\\ir 06092026_1636_candidate_advanced_expense_component_policy_v1\.sql$/m);
  assert.match(finalClosure, /^\\ir 08092026_0918_candidate_expense_update_root_snapshot_validation_v1\.sql$/m);
  assert.equal((finalClosure.match(/^\\ir /gm) || []).length, 2);
  assert.ok(finalClosure.indexOf('06092026_1636') < finalClosure.indexOf('08092026_0918'));
  assert.doesNotMatch(finalClosure, /create\s+(?:or\s+replace\s+)?function/i);
  assert.doesNotMatch(finalClosure, /\b(insert|update|delete|merge|truncate)\b/i);
});

test('the final closure is the last canonical owner after both conflicting sources', async () => {
  const { sqlDateKey, closureFor } = await import('../scripts/cloudtms-db-release-lib.mjs');
  assert.ok(sqlDateKey(finalClosurePath) > sqlDateKey(replayRootPath));
  assert.ok(sqlDateKey(finalClosurePath) > sqlDateKey(currentOwnerPath));
  const closure = closureFor(finalClosurePath);
  assert.ok(closure.paths.includes(replayRootPath));
  assert.equal(closure.paths.at(-1), currentOwnerPath);
});

test('the verifier freezes the exact established definition, metadata and service-only ACL', () => {
  const verifier = read(verifierPath);
  assert.match(verifier, /3be046c5da95b7d076cb8e4459abf46301a407579de30197f644394ae7938ea3/);
  assert.doesNotMatch(verifier, /d3ddbf4c68b49bf0d9baa449f2765dd64f88d96eef5f137f67d553c38aa822d9/);
  assert.match(verifier, /prosecdef[\s\S]*provolatile[\s\S]*proparallel[\s\S]*search_path=pg_catalog, public, private, pg_temp/i);
  assert.match(verifier, /aclexplode[\s\S]*grantee=0[\s\S]*privilege_type='EXECUTE'/i);
  for (const role of ['anon', 'authenticated']) {
    assert.match(verifier, new RegExp(`has_function_privilege\\(\\s*'${role}'`, 'i'));
  }
  assert.match(verifier, /not pg_catalog\.has_function_privilege\(\s*'service_role'/i);
  assert.match(verifier, /begin;[\s\S]*rollback;/i);
  assert.doesNotMatch(verifier, /\bcommit\s*;/i);
});

test('both release modes run the final-authority verifier', () => {
  const release = JSON.parse(read('supabase/release/current-release.json'));
  for (const field of ['verificationFiles', 'newVerificationFiles']) {
    assert.equal(release[field].filter((entry) => entry === verifierPath).length, 1, field);
  }
});
