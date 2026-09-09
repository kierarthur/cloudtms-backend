const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const closurePath = 'supabase/repeatable/09092026_1548_banking_pay_restored_authority_canonicalisation_v1.sql';
const bulkAndManualPath = 'supabase/repeatable/29082026_0326_banking_pay_release_authority_repair_v1.sql';
const weeklyPath = 'supabase/repeatable/27082026_2205_candidate_weekly_manager_finalisation_authority_v1.sql';
const replayPath = 'supabase/repeatable/29082026_0613_banking_pay_replaced_candidate_owner_repair_v1.sql';
const selectionPath = 'supabase/repeatable/04092026_2355_banking_pay_workbench_selection_owner_reassert_v1.sql';
const qrPath = 'supabase/repeatable/30082026_1232_candidate_qr_document_revision_order_v1.sql';
const verifierPath = 'supabase/verification/31082026_0014_banking_pay_dirty_apply_family_authority_repair_verification.sql';
const closureSql = fs.readFileSync(path.join(root, closurePath), 'utf8');

const finalOwners = new Map([
  ['bulk_authorise_dataset_v1', bulkAndManualPath],
  ['contract_week_manual_upsert_atomic', weeklyPath],
  ['pay_workbench_session_replay_replaced_queue_v1', replayPath],
  ['pay_workbench_session_set_selected_rows', selectionPath],
  ['timesheet_qr_send_enqueue_v1', qrPath],
]);

test('restored authority closure is include-only and later than every exact owner', async () => {
  const { closureFor, sqlDateKey } = await import('../scripts/cloudtms-db-release-lib.mjs');
  const closure = closureFor(closurePath);
  const includes = [...closureSql.matchAll(/^\\ir\s+([^\s;]+)\s*;?\s*$/gm)]
    .map(match => `supabase/repeatable/${match[1]}`);

  assert.deepEqual(includes, [bulkAndManualPath, weeklyPath, replayPath, selectionPath, qrPath]);
  for (const ownerPath of includes) {
    assert.ok(sqlDateKey(ownerPath).localeCompare(sqlDateKey(closurePath)) < 0, ownerPath);
    assert.ok(closure.paths.includes(ownerPath), ownerPath);
  }
  assert.doesNotMatch(closureSql, /^\s*CREATE(?: OR REPLACE)?\s+FUNCTION\b/gmi);
  assert.doesNotMatch(closureSql, /^\s*(?:BEGIN|COMMIT|ROLLBACK)\s*;/gmi);
  assert.match(
    closureSql,
    /changes no[\s\S]*selection, eligibility, amount, gross\/net, tax, VAT, channel, payment,[\s\S]*provider, cancellation or settlement policy/i,
  );
});

test('closure leaves each guarded routine at its exact declared final owner', async () => {
  const { closureFor } = await import('../scripts/cloudtms-db-release-lib.mjs');
  const paths = closureFor(closurePath).paths;

  for (const [routineName, expectedOwnerPath] of finalOwners) {
    const definingPaths = paths.filter(relativePath => {
      const sql = fs.readFileSync(path.join(root, relativePath), 'utf8');
      return new RegExp(
        `CREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\s+public\\.${routineName}\\s*\\(`,
        'i',
      ).test(sql);
    });
    assert.ok(definingPaths.length > 0, routineName);
    assert.equal(definingPaths.at(-1), expectedOwnerPath, routineName);
  }
});

test('no later repeatable silently supersedes a guarded current owner', async () => {
  const { sqlDateKey, sqlFiles } = await import('../scripts/cloudtms-db-release-lib.mjs');
  const closureKey = sqlDateKey(closurePath);

  for (const relativePath of sqlFiles('supabase/repeatable')) {
    if (sqlDateKey(relativePath).localeCompare(closureKey) <= 0) continue;
    const sql = fs.readFileSync(path.join(root, relativePath), 'utf8');
    for (const routineName of finalOwners.keys()) {
      assert.doesNotMatch(
        sql,
        new RegExp(`CREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\s+public\\.${routineName}\\s*\\(`, 'i'),
        `${relativePath} silently supersedes ${routineName}`,
      );
    }
  }
});

test('existing final verifier remains mandatory in NEW and UPGRADE', () => {
  const release = JSON.parse(fs.readFileSync(path.join(root, 'supabase/release/current-release.json'), 'utf8'));
  assert.equal(release.verificationFiles.filter(file => file === verifierPath).length, 1);
  assert.equal(release.newVerificationFiles.filter(file => file === verifierPath).length, 1);

  const verifier = fs.readFileSync(path.join(root, verifierPath), 'utf8');
  for (const expectedHash of [
    '88880fc433528df687f6eb83983679ad0afc0cc8e6913971de7848874e3a4e71',
    'ac3a122f00af03e35bb2c40e82ddb114571f7252a6ac31d9bfec23d7cb3afc19',
    '363aeab20aed70b8396793808f9a2263766e984d66914317bdf0a767e6e0f360',
    '7d622194f7bca877bf8420cb6f10f9ad46a69bad118c5f8fb9ed16810492d98c',
    '090fcbd7a66ade81f107635c360a038a514a5c26358c0b4aa716bdea91245347',
  ]) {
    assert.match(verifier, new RegExp(expectedHash));
  }
});
