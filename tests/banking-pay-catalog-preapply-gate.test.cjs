const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const test = require('node:test');
const { pathToFileURL } = require('node:url');

const root = path.resolve(__dirname, '..');
const generator = path.join(root, 'supabase', 'verification', 'generate_banking_pay_catalog_preapply_check.mjs');
const ownerAdapter = path.join(root, 'supabase', 'verification', 'catalog_logical_owner_adapter.mjs');
const releaseEnginePath = path.join(root, 'scripts', 'cloudtms-db-release.mjs');

test('catalog-owned pending repeatables are rehearsed in a rollback-only transaction', () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'banking-pay-catalog-preapply-'));
  const output = path.join(tempDir, 'preapply.sql');
  try {
    const result = spawnSync(process.execPath, [
      generator,
      output,
      'supabase/repeatable/07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql',
      'supabase/repeatable/13082026_1912_pay_workbench_sealed_rate_component_projection_v1.sql',
      'supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql',
      'supabase/repeatable/08082026_0313_pay_workbench_fail_job_authority.sql',
    ], { cwd: root, encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr);

    const sql = fs.readFileSync(output, 'utf8');
    assert.match(sql, /^\\set ON_ERROR_STOP on\nBEGIN;/);
    assert.match(sql, /07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1\.sql/);
    assert.match(sql, /13082026_1912_pay_workbench_sealed_rate_component_projection_v1\.sql/);
    assert.match(sql, /04082026_1219_pay_workbench_fail_job\.sql/);
    assert.match(sql, /08082026_0313_pay_workbench_fail_job_authority\.sql/);
    assert.match(sql, /BEGIN EXACT EXPANDED RELATIVE INCLUDE supabase\/repeatable\/04082026_1219_pay_workbench_fail_job\.sql/);
    assert.match(sql, /BANKING_PAY_CATALOG_PREAPPLY_DEFINITION_MISMATCH: private\.pay_sync_overpayments_from_workbench_workspace_v1/);
    assert.match(sql, /BANKING_PAY_CATALOG_PREAPPLY_DEFINITION_MISMATCH: private\.pay_workbench_sealed_rate_component_projection_v1/);
    assert.match(sql, /BANKING_PAY_CATALOG_PREAPPLY_DEFINITION_MISMATCH: public\.pay_workbench_fail_job/);
    assert.match(sql, /6545152d1cb26ddfb71453803d5c5d3f5682a02176c1b3eacc424917ca16478f/);
    assert.match(sql, /a3e3a35101070382fb2e9957bc007ef31f9801afca165b2391ae0179adf6da0e/);
    assert.match(sql, /baef72fe071ed7bb0ee3a48cc82acf33053c2a5f0bba0bb8816bccbbb34abb49/);
    assert.match(sql, /pg_catalog\.min\(p\.oid::text\)::oid/);
    assert.match(sql, /ALTER FUNCTION public\.pay_workbench_fail_job[\s\S]*OWNER TO CURRENT_USER;/);
    assert.doesNotMatch(sql, /ALTER FUNCTION [^;]+ OWNER TO postgres;/i);
    assert.doesNotMatch(sql, /^\\i(?:r)?\s+/m);
    assert.match(sql, /ROLLBACK;\n$/);
    assert.doesNotMatch(sql, /COMMIT;/);
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
});

test('logical postgres ownership and exact relative includes are mapped only in rollback rehearsal SQL', async () => {
  const {
    adaptCatalogLogicalOwnerForRehearsal,
    expandCatalogRepeatableIncludesForRehearsal,
  } = await import(pathToFileURL(ownerAdapter).href);
  const source = [
    "SELECT 'ALTER FUNCTION public.fake() OWNER TO postgres;';",
    '-- ALTER FUNCTION public.fake() OWNER TO postgres;',
    'ALTER FUNCTION public.real_one(uuid, text)',
    '  OWNER TO postgres;',
    'ALTER PROCEDURE private.real_two(uuid) OWNER TO postgres;',
    'GRANT EXECUTE ON FUNCTION public.real_one(uuid,text) TO postgres;',
  ].join('\n');
  const adapted = adaptCatalogLogicalOwnerForRehearsal(source);
  assert.equal(adapted.mode, 'MAPPED_LOGICAL_POSTGRES_TO_CURRENT_USER');
  assert.equal(adapted.mappedIdentities.length, 2);
  assert.match(adapted.sourceSql, /ALTER FUNCTION public\.real_one\(uuid, text\)[\s\S]*OWNER TO CURRENT_USER;/);
  assert.match(adapted.sourceSql, /ALTER PROCEDURE private\.real_two\(uuid\) OWNER TO CURRENT_USER;/);
  assert.match(adapted.sourceSql, /SELECT 'ALTER FUNCTION public\.fake\(\) OWNER TO postgres;';/);
  assert.match(adapted.sourceSql, /GRANT EXECUTE ON FUNCTION public\.real_one\(uuid,text\) TO postgres;/);

  assert.throws(
    () => adaptCatalogLogicalOwnerForRehearsal('ALTER TABLE public.unsafe OWNER TO postgres;'),
    /only exact ALTER FUNCTION\/PROCEDURE\/ROUTINE/,
  );
  assert.throws(
    () => adaptCatalogLogicalOwnerForRehearsal('ALTER FUNCTION public.unsafe() OWNER TO service_role;'),
    /unexpected logical owner service_role/,
  );
  assert.throws(
    () => adaptCatalogLogicalOwnerForRehearsal('\\ir nested.sql\nALTER FUNCTION public.unsafe() OWNER TO postgres;'),
    /psql meta-commands are not permitted/,
  );
  assert.throws(
    () => adaptCatalogLogicalOwnerForRehearsal('\\echo unsafe\nALTER FUNCTION public.unsafe() OWNER TO postgres;'),
    /psql meta-commands are not permitted/,
  );
  assert.throws(
    () => adaptCatalogLogicalOwnerForRehearsal('SET ROLE postgres;'),
    /role-changing SQL is not permitted/,
  );

  const sources = new Map([
    ['root.sql', '\\ir child.sql\nSELECT 1;'],
    ['child.sql', 'ALTER FUNCTION public.child() OWNER TO postgres;'],
  ]);
  const expanded = expandCatalogRepeatableIncludesForRehearsal({
    sourceSql: sources.get('root.sql'),
    sourcePath: 'root.sql',
    resolveInclude(_current, includeReference) {
      return { sourcePath: includeReference, sourceSql: sources.get(includeReference) };
    },
  });
  assert.match(expanded.sourceSql, /BEGIN EXACT EXPANDED RELATIVE INCLUDE child\.sql/);
  assert.doesNotMatch(expanded.sourceSql, /^\\ir\s+/m);
  assert.throws(
    () => expandCatalogRepeatableIncludesForRehearsal({
      sourceSql: '\\i child.sql',
      sourcePath: 'root.sql',
      resolveInclude() { return { sourcePath: 'child.sql', sourceSql: 'SELECT 1;' }; },
    }),
    /unsupported psql meta-command/,
  );
  assert.throws(
    () => expandCatalogRepeatableIncludesForRehearsal({
      sourceSql: '\\ir child.sql',
      sourcePath: 'root.sql',
      resolveInclude() { return { sourcePath: 'root.sql', sourceSql: '\\ir child.sql' }; },
    }),
    /cyclic relative include/,
  );
});

test('release engine runs the rollback-only catalog rehearsal before changed repeatables', () => {
  const engine = fs.readFileSync(releaseEnginePath, 'utf8');
  const collectIndex = engine.indexOf('const pendingRepeatables = current.repeatables.filter');
  const generatorIndex = engine.indexOf('runBankingPayCatalogPreapply(pendingRepeatables.map');
  const applyIndex = engine.indexOf('for (const item of pendingRepeatables)');

  assert.ok(collectIndex >= 0, 'release engine must collect changed repeatables');
  assert.ok(generatorIndex > collectIndex, 'release engine must run the catalog rehearsal after collection');
  assert.ok(applyIndex > generatorIndex, 'catalog rehearsal must pass before applying changed repeatables');
  assert.match(engine, /generate_banking_pay_catalog_preapply_check\.mjs/);
  assert.match(engine, /psql\(\{ file: output \}\)/);
});
