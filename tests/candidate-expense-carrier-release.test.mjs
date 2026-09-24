import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { checkSource, unwrap, installationSql, verificationSql, manifest, run } from '../scripts/cloudtms-db-expense-carrier-release.mjs';

test('exact ten-routine generated component source and hashes agree', () => {
  assert.equal(checkSource(), true);
  assert.equal(manifest.beforeRoutines.length, 7);
  assert.equal(manifest.afterRoutines.length, 10);
  assert.equal(manifest.afterRoutines.filter(r => !manifest.beforeRoutines.some(b => b.identity === r.identity && b.schema === r.schema)).length, 3);
});
test('embedded verification never commits or rolls back the outer installation', () => {
  const sql = verificationSql();
  assert.doesNotMatch(sql, /^\s*(?:begin|commit|rollback);\s*$/mi);
  assert.equal((sql.match(/savepoint component_check_\d+;/g) || []).length, manifest.verifiers.length * 3);
  for (const bad of ['begin;\nselect 1;\ncommit;', 'begin;\nrollback;\nrollback;', '\\i unknown.sql']) {
    assert.throws(() => unwrap(bad, 'verifier'));
  }
  assert.equal(unwrap('begin;\nselect 1;\nrollback;', 'verifier').trim(), 'select 1;');
});
test('transaction compares all unrelated catalogue objects and exact routine security', () => {
  const sql = installationSql();
  assert.match(sql, /EXPENSE_COMPONENT_UNRELATED_CATALOGUE_CHANGE/);
  assert.match(sql, /EXPENSE_COMPONENT_ROUTINE_CONTRACT_MISMATCH/);
  assert.match(sql, /'relations'/);
  assert.match(sql, /'policies'/);
  assert.match(sql, /'default_acls'/);
  assert.match(sql, /"security_definer":true/);
  assert.match(sql, /"grantee":"service_role"/);
});
test('cannot execute a release outside explicit TEST; workflow uses protected exact branch', () => {
  const before = process.env.CLOUDTMS_ENVIRONMENT;
  try {
    process.env.CLOUDTMS_ENVIRONMENT = 'LIVE';
    assert.throws(() => run('apply'), /TEST only/);
  } finally {
    if (before === undefined) delete process.env.CLOUDTMS_ENVIRONMENT;
    else process.env.CLOUDTMS_ENVIRONMENT = before;
  }
  const yaml = fs.readFileSync(new URL('../.github/workflows/candidate-expense-carrier-release.yml', import.meta.url), 'utf8');
  assert.match(yaml, /environment: database-test/);
  assert.match(yaml, /refs\/heads\/test/);
  assert.match(yaml, /kierarthur\/cloudtms-backend/);
  assert.match(yaml, /group: cloudtms-database-release-TEST/);
  assert.doesNotMatch(yaml, /MIGET_DATABASE_URL_LIVE/);
});
