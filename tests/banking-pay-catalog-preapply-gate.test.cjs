const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const generator = path.join(root, 'supabase', 'verification', 'generate_banking_pay_catalog_preapply_check.mjs');
const workflowPath = path.join(root, '.github', 'workflows', 'supabase-migrate.yml');

test('catalog-owned pending repeatables are rehearsed in a rollback-only transaction', () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'banking-pay-catalog-preapply-'));
  const output = path.join(tempDir, 'preapply.sql');
  try {
    const result = spawnSync(process.execPath, [
      generator,
      output,
      'supabase/repeatable/07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql',
      'supabase/repeatable/13082026_1912_pay_workbench_sealed_rate_component_projection_v1.sql',
    ], { cwd: root, encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr);

    const sql = fs.readFileSync(output, 'utf8');
    assert.match(sql, /^\\set ON_ERROR_STOP on\nBEGIN;/);
    assert.match(sql, /07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1\.sql/);
    assert.match(sql, /13082026_1912_pay_workbench_sealed_rate_component_projection_v1\.sql/);
    assert.match(sql, /BANKING_PAY_CATALOG_PREAPPLY_DEFINITION_MISMATCH: private\.pay_sync_overpayments_from_workbench_workspace_v1/);
    assert.match(sql, /BANKING_PAY_CATALOG_PREAPPLY_DEFINITION_MISMATCH: private\.pay_workbench_sealed_rate_component_projection_v1/);
    assert.match(sql, /6545152d1cb26ddfb71453803d5c5d3f5682a02176c1b3eacc424917ca16478f/);
    assert.match(sql, /a3e3a35101070382fb2e9957bc007ef31f9801afca165b2391ae0179adf6da0e/);
    assert.match(sql, /pg_catalog\.min\(p\.oid::text\)::oid/);
    assert.match(sql, /ROLLBACK;\n$/);
    assert.doesNotMatch(sql, /COMMIT;/);
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
});

test('migration workflow runs the rollback-only catalog rehearsal before repeatable application', () => {
  const workflow = fs.readFileSync(workflowPath, 'utf8');
  const collectIndex = workflow.indexOf('REP_FILES_PENDING=()');
  const generatorIndex = workflow.indexOf('generate_banking_pay_catalog_preapply_check.mjs');
  const applyIndex = workflow.indexOf('echo "APPLY new/changed repeatable: $base"');

  assert.ok(collectIndex >= 0, 'workflow must collect pending repeatables');
  assert.ok(generatorIndex > collectIndex, 'workflow must generate the pre-apply rehearsal after collecting pending repeatables');
  assert.ok(applyIndex > generatorIndex, 'workflow must pass the pre-apply rehearsal before applying repeatables');
  assert.match(workflow, /psql "\$DB_URL" -v ON_ERROR_STOP=1 -X -q -f "\$\{CATALOG_PREFLIGHT_SQL\}"/);
});
