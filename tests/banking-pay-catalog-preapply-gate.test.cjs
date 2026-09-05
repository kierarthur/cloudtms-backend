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
const envelopeParser = path.join(root, 'supabase', 'verification', 'catalog_outer_transaction_envelope.mjs');
const releaseEnginePath = path.join(root, 'scripts', 'cloudtms-db-release.mjs');

test('catalog-owned pending repeatables are rehearsed in a rollback-only transaction', () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'banking-pay-catalog-preapply-'));
  const output = path.join(tempDir, 'preapply.sql');
  try {
    const result = spawnSync(process.execPath, [
      generator,
      output,
      'supabase/repeatable/07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql',
      'supabase/repeatable/01092026_1459_banking_pay_signed_recovery_draft_v1.sql',
      'supabase/repeatable/04092026_1330_banking_pay_manual_carry_forward_selection_authority_v1.sql',
      'supabase/repeatable/05092026_0655_banking_pay_active_draft_reservation_evidence_precedence_v1.sql',
      'supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql',
      'supabase/repeatable/08082026_0313_pay_workbench_fail_job_authority.sql',
    ], { cwd: root, encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr);

    const sql = fs.readFileSync(output, 'utf8');
    assert.match(sql, /^\\set ON_ERROR_STOP on\nBEGIN;/);
    assert.match(sql, /07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1\.sql/);
    assert.match(sql, /declared dependency of supabase\/repeatable\/05092026_0655_banking_pay_active_draft_reservation_evidence_precedence_v1\.sql[\s\S]*01092026_1459_banking_pay_signed_recovery_draft_v1\.sql/);
    assert.match(sql, /05092026_0655_banking_pay_active_draft_reservation_evidence_precedence_v1\.sql/);
    assert.ok(
      sql.indexOf('declared dependency of supabase/repeatable/05092026_0655_banking_pay_active_draft_reservation_evidence_precedence_v1.sql inlined')
        < sql.indexOf('catalog-owned pending repeatable inlined after exact transaction-envelope, relative-include, and logical-owner validation (PLAIN): supabase/repeatable/05092026_0655_banking_pay_active_draft_reservation_evidence_precedence_v1.sql'),
      'the signed-recovery dependency must be rehearsed before the current helper owner',
    );
    assert.doesNotMatch(sql, /13082026_1912_pay_workbench_sealed_rate_component_projection_v1\.sql/);
    assert.match(sql, /04082026_1219_pay_workbench_fail_job\.sql/);
    assert.match(sql, /08082026_0313_pay_workbench_fail_job_authority\.sql/);
    assert.match(sql, /BEGIN EXACT EXPANDED RELATIVE INCLUDE supabase\/repeatable\/04082026_1219_pay_workbench_fail_job\.sql/);
    assert.match(sql, /BANKING_PAY_CATALOG_PREAPPLY_DEFINITION_MISMATCH: private\.pay_sync_overpayments_from_workbench_workspace_v1/);
    assert.match(sql, /BANKING_PAY_CATALOG_PREAPPLY_DEFINITION_MISMATCH: private\.pay_workbench_sealed_rate_component_projection_v1/);
    assert.match(sql, /BANKING_PAY_CATALOG_PREAPPLY_DEFINITION_MISMATCH: public\.pay_workbench_fail_job/);
    assert.match(sql, /6545152d1cb26ddfb71453803d5c5d3f5682a02176c1b3eacc424917ca16478f/);
    assert.match(sql, /7f454ee064f08480e9da8ea9c6b378fda3ed4833b965d65ea991874ab4560d4b/);
    assert.match(sql, /0b51f2d023da22e1cb8ce838f24954c95d25db51906183a025ac1a84e68a6715/);
    assert.doesNotMatch(sql, /IF v_actual_sha256 IS DISTINCT FROM '3aa94ba810ad14a48926ebef2787909f6870df83724abb8a3aad3eb58ec0e25b'/);
    assert.doesNotMatch(sql, /a3e3a35101070382fb2e9957bc007ef31f9801afca165b2391ae0179adf6da0e/);
    assert.match(sql, /85dee116f81fee1f0634d79f7562d2f6a97f3be9f1d3c40aef80238b13483656/);
    assert.match(sql, /pg_catalog\.min\(p\.oid::text\)::oid/);
    assert.match(sql, /ALTER FUNCTION public\.pay_workbench_fail_job[\s\S]*OWNER TO CURRENT_USER;/);
    assert.doesNotMatch(sql, /ALTER FUNCTION [^;]+ OWNER TO postgres;/i);
    assert.doesNotMatch(sql, /^\\i(?:r)?\s+/m);
    assert.match(sql, /ROLLBACK;\n$/);
    assert.doesNotMatch(sql, /^\s*COMMIT\s*;/im);
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
});

test('exact outer transaction parser preserves inner bytes and kills boundary mutations', async () => {
  const { prepareCatalogOwnedSourceForRehearsal } = await import(pathToFileURL(envelopeParser).href);
  const inner = [
    '',
    "SELECT 'BEGIN; COMMIT; ROLLBACK; START TRANSACTION; \\ir hidden.sql';",
    "SELECT E'escaped quote \\' COMMIT; ROLLBACK; START TRANSACTION; \\ir hidden.sql';",
    'SELECT "BEGIN; COMMIT; ROLLBACK; START TRANSACTION;";',
    '-- COMMIT; after quoted text is still only a comment',
    'DO $body$',
    'BEGIN',
    "  RAISE NOTICE 'COMMIT; ROLLBACK; \\i hidden.sql';",
    'END',
    '$body$;',
    '/* nested comment: BEGIN; /* COMMIT; */ ROLLBACK; */',
    '',
  ].join('\r\n');
  const wrapped = `-- permitted leading comment\r\n\\set ON_ERROR_STOP on\r\nBEGIN;${inner}COMMIT;\r\n-- permitted trailing comment\r\n`;
  const prepared = prepareCatalogOwnedSourceForRehearsal(wrapped);
  assert.equal(prepared.mode, 'EXACT_OUTER_TRANSACTION_ENVELOPE');
  assert.equal(prepared.innerSql, inner);
  assert.equal(prepared.outerBegin.toUpperCase(), 'BEGIN;');
  assert.equal(prepared.outerCommit.toUpperCase(), 'COMMIT;');

  const plain = prepareCatalogOwnedSourceForRehearsal([
    '-- BEGIN; COMMIT; ROLLBACK;',
    "SELECT 'START TRANSACTION; \\ir hidden.sql';",
    "DO $body$ BEGIN RAISE NOTICE 'COMMIT;'; END $body$;",
    '\\ir ordinary-plain-include.sql',
  ].join('\n'));
  assert.equal(plain.mode, 'PLAIN');

  const invalid = [
    ['missing terminal COMMIT', 'BEGIN; SELECT 1;'],
    ['COMMIT not terminal', 'BEGIN; SELECT 1; COMMIT; SELECT 2;'],
    ['second BEGIN', 'BEGIN; BEGIN; SELECT 1; COMMIT;'],
    ['second COMMIT', 'BEGIN; SELECT 1; COMMIT; COMMIT;'],
    ['explicit ROLLBACK', 'BEGIN; SELECT 1; ROLLBACK;'],
    ['START TRANSACTION', 'START TRANSACTION; SELECT 1; COMMIT;'],
    ['include', 'BEGIN;\n\\ir nested.sql\nSELECT 1;\nCOMMIT;'],
    ['meta command after COMMIT', 'BEGIN; SELECT 1; COMMIT;\n\\echo unsafe'],
    ['non-exact BEGIN', 'BEGIN WORK; SELECT 1; COMMIT;'],
    ['non-exact COMMIT', 'BEGIN; SELECT 1; COMMIT AND CHAIN;'],
    ['empty envelope', 'BEGIN; COMMIT;'],
    ['unterminated escape string', "BEGIN; SELECT E'escaped \\' COMMIT;"],
  ];
  for (const [label, source] of invalid) {
    assert.throws(
      () => prepareCatalogOwnedSourceForRehearsal(source),
      /transaction envelope is not exact/,
      label,
    );
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
    "ALTER FUNCTION public.real_one(uuid, text) SET plpgsql_check.mode TO 'disabled';",
    "SET plpgsql_check.profiler = 'off';",
    "SELECT 'ALTER FUNCTION public.real_one(uuid, text) SET plpgsql_check.mode TO ''disabled'';';",
    "SELECT 'REVOKE EXECUTE ON FUNCTION public.fake() FROM PUBLIC, authenticator;';",
    '-- REVOKE EXECUTE ON FUNCTION public.fake() FROM PUBLIC, authenticator;',
    'REVOKE ALL ON FUNCTION public.real_one(uuid,text) FROM PUBLIC, anon, authenticator, service_role;',
    'GRANT EXECUTE ON FUNCTION public.real_one(uuid,text) TO postgres;',
  ].join('\n');
  const adapted = adaptCatalogLogicalOwnerForRehearsal(source);
  assert.equal(adapted.mode, 'MAPPED_PROVIDER_PORTABLE_AUTHORITY');
  assert.equal(adapted.mappedIdentities.length, 2);
  assert.match(adapted.sourceSql, /ALTER FUNCTION public\.real_one\(uuid, text\)[\s\S]*OWNER TO CURRENT_USER;/);
  assert.match(adapted.sourceSql, /ALTER PROCEDURE private\.real_two\(uuid\) OWNER TO CURRENT_USER;/);
  assert.match(adapted.sourceSql, /SELECT 'ALTER FUNCTION public\.fake\(\) OWNER TO postgres;';/);
  assert.doesNotMatch(adapted.sourceSql, /^ALTER FUNCTION public\.real_one\(uuid, text\) SET plpgsql_check\.mode/m);
  assert.doesNotMatch(adapted.sourceSql, /^SET plpgsql_check\.profiler/m);
  assert.match(adapted.sourceSql, /SELECT 'ALTER FUNCTION public\.real_one\(uuid, text\) SET plpgsql_check\.mode TO ''disabled'';';/);
  assert.match(adapted.sourceSql, /SELECT 'REVOKE EXECUTE ON FUNCTION public\.fake\(\) FROM PUBLIC, authenticator;';/);
  assert.match(adapted.sourceSql, /-- REVOKE EXECUTE ON FUNCTION public\.fake\(\) FROM PUBLIC, authenticator;/);
  assert.match(adapted.sourceSql, /REVOKE ALL ON FUNCTION public\.real_one\(uuid,text\) FROM PUBLIC, anon, service_role;/);
  assert.doesNotMatch(adapted.sourceSql, /^REVOKE[^;]*\bauthenticator\b/im);
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
  assert.throws(
    () => adaptCatalogLogicalOwnerForRehearsal('REVOKE ALL ON FUNCTION public.unsafe() FROM authenticator;'),
    /authenticator may be omitted only from an exact comma-delimited REVOKE role list/,
  );
  assert.throws(
    () => adaptCatalogLogicalOwnerForRehearsal("ALTER FUNCTION public.unsafe() SET plpgsql_check.mode TO 'enabled';"),
    /only the seven exact disabled\/off plpgsql_check settings are portable/,
  );
  assert.throws(
    () => adaptCatalogLogicalOwnerForRehearsal("SET plpgsql_check.unknown = 'off';"),
    /only the seven exact disabled\/off plpgsql_check settings are portable/,
  );

  const createHeader = [
    'CREATE OR REPLACE FUNCTION public.header_setting() RETURNS integer',
    'LANGUAGE plpgsql',
    "SET plpgsql_check.mode TO 'disabled'",
    "SET plpgsql_check.profiler TO 'off'",
    'AS $body$',
    'BEGIN',
    "  PERFORM 'SET plpgsql_check.mode TO ''disabled''' ;",
    '  RETURN 1;',
    'END;',
    '$body$;',
  ].join('\n');
  const headerAdapted = adaptCatalogLogicalOwnerForRehearsal(createHeader);
  assert.doesNotMatch(headerAdapted.sourceSql, /^SET plpgsql_check\./m);
  assert.match(headerAdapted.sourceSql, /PERFORM 'SET plpgsql_check\.mode TO ''disabled'''/);

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
