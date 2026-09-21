import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import test from 'node:test';
import {
  WEEKLY_SOURCE_DATABASE_GROUPS,
  WEEKLY_SOURCE_COMPONENT_HANDOVER2_PENDING_FILES,
  WEEKLY_SOURCE_SERIAL_GROUP,
  databaseGroupIds,
} from './database-scenario-adapter.mjs';
import { localUrl as wp16cLocalUrl } from '../wp16c/proof-runner.mjs';

// ADOPTED CONSEQUENCE (WP-16c). Every Weekly Source database verifier on disk
// must be assigned to exactly one executable group. This is deliberately a
// census rather than a historical count: a newly added verifier makes the test
// fail until the adapter executes it, so release coverage cannot silently lag.

test('the database groups cover every Weekly Source verifier exactly once', () => {
  const files = WEEKLY_SOURCE_DATABASE_GROUPS.flatMap((group) => group.files);
  assert.equal(WEEKLY_SOURCE_DATABASE_GROUPS.length, 9);
  assert.equal(files.length, 39);
  assert.equal(new Set(files).size, files.length);

  const onDisk = readdirSync(new URL('../../../supabase/verification/', import.meta.url))
    .filter((file) => file.endsWith('.sql') && file.includes('weekly_source'))
    .sort((left, right) => left.localeCompare(right, 'en'));
  assert.deepEqual([...files].sort((left, right) => left.localeCompare(right, 'en')), onDisk);
  const release = JSON.parse(readFileSync(new URL('../../../supabase/release/current-release.json', import.meta.url), 'utf8'));
  const registered = new Set([...release.verificationFiles, ...release.newVerificationFiles]
    .map((file) => file.split('/').at(-1)));
  assert.deepEqual(onDisk.filter((file) => !registered.has(file)), [],
    'every Weekly Source verifier must run in the repository release gate');

  const completedPack = WEEKLY_SOURCE_DATABASE_GROUPS[6];
  assert.equal(completedPack.id, 'completed-pack-copy');
  assert.deepEqual(completedPack.files, ['19092026_1645_weekly_source_completed_pack_copy_v1.sql']);

  const ordinaryLifecycle = WEEKLY_SOURCE_DATABASE_GROUPS[7];
  assert.equal(ordinaryLifecycle.id, 'ordinary-lifecycle-protection');
  assert.deepEqual(ordinaryLifecycle.files, [
    '20092026_2300_weekly_source_ordinary_unauthorise_family_invoice_census_v1.sql',
  ]);
  assert.deepEqual(ordinaryLifecycle.protectedIds, ['PROT-UNAUTH-001', 'PROT-ROTATION-001']);

  const planSixTwo = WEEKLY_SOURCE_DATABASE_GROUPS[8];
  assert.equal(planSixTwo.id, 'plan62-owners');
  assert.equal(planSixTwo.files.length, 15);
  assert(planSixTwo.files.every((file) => /^17092026_.+\.sql$/.test(file)));
});

test('the component boundary partitions all 39 verifiers into 32 executable and 7 exact HANDOVER 2 dependencies', () => {
  assert.deepEqual(WEEKLY_SOURCE_COMPONENT_HANDOVER2_PENDING_FILES, [
    '17092026_0300_weekly_source_entitlement_publication_v1.sql',
    '17092026_0610_weekly_source_withdrawal_supersession_v1.sql',
    '17092026_0700_weekly_source_pending_entitlement_release_v1.sql',
    '17092026_0800_weekly_source_workbench_seams_v1.sql',
    '17092026_0900_weekly_source_installed_writer_census_v1.sql',
    '17092026_1000_weekly_source_settlement_allocation_v1.sql',
    '17092026_1400_weekly_source_ordinary_authorisation_guard_v1.sql',
  ]);
  const all = WEEKLY_SOURCE_DATABASE_GROUPS.flatMap((group) => group.files);
  assert(WEEKLY_SOURCE_COMPONENT_HANDOVER2_PENDING_FILES.every((file) => all.includes(file)));
  assert.equal(new Set(WEEKLY_SOURCE_COMPONENT_HANDOVER2_PENDING_FILES).size, 7);
  assert.equal(all.filter((file) => !WEEKLY_SOURCE_COMPONENT_HANDOVER2_PENDING_FILES.includes(file)).length, 32);
});

test('every group id set is sorted, unique and non-empty where it is declared', () => {
  const fields = ['acceptanceIds', 'protectedIds', 'modelIds', 'spiIds', 'issIds',
    'uiStateIds', 'ftiIds', 'xsgIds', 'h2Ids', 'proofIds'];
  for (const group of WEEKLY_SOURCE_DATABASE_GROUPS) {
    for (const field of fields) {
      if (!(field in group)) continue;
      const ids = group[field];
      assert(Array.isArray(ids), `${group.id}.${field} must be an array`);
      assert(ids.length > 0, `${group.id}.${field} is declared but empty; omit it instead`);
      assert.equal(new Set(ids).size, ids.length, `${group.id}.${field} repeats an id`);
      assert(ids.every((id) => typeof id === 'string' && id.trim() === id && id.length > 0),
        `${group.id}.${field} carries a blank or padded id`);
    }
  }
});

// This is the whole point of contract item G12-3: before it, all five groups
// emitted empty arrays and the Weekly Source verifiers contributed zero rows to
// the coverage gate, which is why coverage read 23 of 918.
test('the adapter emits only ids the database verifiers prove', () => {
  assert(databaseGroupIds('acceptanceIds').length > 0, 'acceptance ids must not be empty');
  assert(databaseGroupIds('proofIds').length > 0, 'R1-R44 proof ids must not be empty');
  assert(databaseGroupIds('uiStateIds').length === 22, 'all 22 UI lifecycle states are asserted');
  assert.deepEqual(databaseGroupIds('issIds'), [],
    'database-only evidence must not claim the cross-surface issue journeys');
  assert(databaseGroupIds('ftiIds').includes('FTI-020'),
    'the complete local invoice-discounting lifecycle must be credited by its real verifier');
  assert(!databaseGroupIds('ftiIds').includes('FTI-024'),
    'FTI-024 still needs its complete cross-surface route proof');
  assert(!databaseGroupIds('xsgIds').includes('XSG-017'),
    'XSG-017 needs the real asynchronous issue route');
  assert(databaseGroupIds('xsgIds').includes('XSG-030'),
    'the signed invoice-discounting gap must be credited by its real verifier');
  assert(databaseGroupIds('h2Ids').length > 0, 'H2 ids must not be empty');
  // Sorted and deduplicated across groups, so two groups naming the same id
  // cannot inflate a count.
  const proofIds = databaseGroupIds('proofIds');
  assert.deepEqual(proofIds, [...new Set(proofIds)].sort());
});

test('the serial concurrency group names the suites and the rows that need it', () => {
  assert.equal(WEEKLY_SOURCE_SERIAL_GROUP.requiresNamedSessions, true);
  assert(WEEKLY_SOURCE_SERIAL_GROUP.minimumSessions >= 2);
  assert(WEEKLY_SOURCE_SERIAL_GROUP.suites.includes('tests/weekly-source/wp16c/una-suite.mjs'));
  assert(WEEKLY_SOURCE_SERIAL_GROUP.suites.includes('tests/weekly-source/wp16c/rot-suite.mjs'));
  assert(WEEKLY_SOURCE_SERIAL_GROUP.suites.includes('tests/weekly-source/wp16c/release-suite.mjs'));
  assert.equal(WEEKLY_SOURCE_SERIAL_GROUP.suites.length, 3);
  for (const id of ['R12', 'R15', 'R37', 'R42', 'ROT-002', 'ROT-003', 'ROT-012', 'UNA-014']) {
    assert(WEEKLY_SOURCE_SERIAL_GROUP.servesProofIds.includes(id), `${id} needs named sessions`);
  }
});

test('the serial proof suites inherit the task-owned dynamic PostgreSQL port', () => {
  const previous = process.env.WP16C_PORT;
  process.env.WP16C_PORT = '61234';
  try {
    assert.equal(wp16cLocalUrl('banking_modal_v2_test'),
      'postgresql://postgres@127.0.0.1:61234/banking_modal_v2_test');
    process.env.WP16C_PORT = 'not-a-port';
    assert.throws(() => wp16cLocalUrl('banking_modal_v2_test'), (error) => error.code === 'WP16C_PORT_INVALID');
  } finally {
    if (previous === undefined) delete process.env.WP16C_PORT;
    else process.env.WP16C_PORT = previous;
  }
});

test('database residue checks reject leaked clients without treating PostgreSQL workers as test sessions', () => {
  const source = readFileSync(new URL('./database-scenario-adapter.mjs', import.meta.url), 'utf8');
  assert.match(source, /backend_type\s*=\s*'client backend'/,
    'connection isolation must count escaped verifier clients');
  assert.doesNotMatch(source,
    /select count\(\*\) from pg_stat_activity where datname = current_database\(\) and pid <> pg_backend_pid\(\);/,
    'connection isolation must not count autovacuum or other PostgreSQL-owned workers');
});

test('component proof writes a bounded SPI envelope without claiming release or HANDOVER 2 evidence', () => {
  const source = readFileSync(new URL('./database-scenario-adapter.mjs', import.meta.url), 'utf8');
  assert.match(source, /scenarioId:\s*`WS-DATABASE-COMPONENT-\$\{mode\}-PG17-001`/);
  assert.match(source, /releaseEvidenceEligible:\s*false/);
  assert.match(source, /h2Ids:\s*\[\]/);
  assert.match(source, /proofIds:\s*\[\]/);
  assert.match(source, /protectedIds:\s*componentProtectedIds/);
  assert.match(source, /database-component-\$\{mode\.toLowerCase\(\)\}-pg17-exact\.json/);
  assert.match(source, /componentResultEnvelopeDigest/);
});
