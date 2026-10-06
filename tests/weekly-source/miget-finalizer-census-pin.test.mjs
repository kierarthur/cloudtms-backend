import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const source = fs.readFileSync(path.join(root,
  'supabase/verification/17092026_0900_weekly_source_installed_writer_census_v1.sql'), 'utf8');

// The managed release validates its exact physical target before verification.
// This fallback is provider-neutral, not name-neutral financial authority: its
// complete closed conjunction accepts only this one audited TEST definition.
const expectedException = `
if observed_sha <> inventory_row.definition_sha256
  and not (
    inventory_row.schema_name = 'public'
    and inventory_row.routine_name = 'pay_batch_finalize_reservations_and_markers'
    and inventory_row.identity_arguments = 'p_pay_batch_id uuid, p_pay_channel_scope text, p_actor_user_id uuid, p_pay_date date, p_week_start date, p_operation_id uuid, p_candidate_scope_ids jsonb'
    and inventory_row.definition_sha256 = '49fbfaa7e3fbcefe7fde5e6937f2ac2b772fc62528460ae22246f98f7ad01d08'
    and observed_sha = '0449fe0daa2d6e1b23aa7d991bfce84d84fce30d7d123dff6804cbe528ddc4a4'
    and exists (
      select 1 from private.cloudtms_database_identity as identity_row
      where identity_row.singleton and identity_row.environment = 'TEST'
    )
    and exists (
      select 1
      from pg_catalog.pg_proc as mapped_proc
      join pg_catalog.pg_namespace as mapped_namespace on mapped_namespace.oid = mapped_proc.pronamespace
      where mapped_namespace.nspname = inventory_row.schema_name
        and mapped_proc.proname = inventory_row.routine_name
        and pg_catalog.pg_get_function_identity_arguments(mapped_proc.oid) = inventory_row.identity_arguments
        and mapped_proc.proconfig = array['search_path=public']::text[]
    )
  ) then`;

function assertExceptionContract(sql) {
  const conditions = [...sql.matchAll(/if observed_sha <> inventory_row\.definition_sha256\s+and not \([\s\S]*?\) then/g)];
  assert.equal(conditions.length, 1, 'one unique closed provider-mapped exception');
  const normalise = (value) => value.replace(/\s+/g, ' ').trim();
  assert.equal(normalise(conditions[0][0]), normalise(expectedException));
}

test('writer census admits only the complete exact provider-mapped TEST definition', () => {
  assertExceptionContract(source);
});

const refusedMutations = [
  ['OR instead of AND', "and inventory_row.routine_name = 'pay_batch_finalize_reservations_and_markers'", "or inventory_row.routine_name = 'pay_batch_finalize_reservations_and_markers'"],
  ['wrong schema', "inventory_row.schema_name = 'public'", "inventory_row.schema_name = 'private'"],
  ['different routine', "inventory_row.routine_name = 'pay_batch_finalize_reservations_and_markers'", "inventory_row.routine_name = 'another_writer'"],
  ['different signature', 'p_candidate_scope_ids jsonb', 'p_candidate_scope_ids text'],
  ['different canonical hash', '49fbfaa7e3fbcefe7fde5e6937f2ac2b772fc62528460ae22246f98f7ad01d08', 'unreviewed-canonical'],
  ['different observed hash', '0449fe0daa2d6e1b23aa7d991bfce84d84fce30d7d123dff6804cbe528ddc4a4', 'unreviewed-observed'],
  ['LIVE identity', "identity_row.environment = 'TEST'", "identity_row.environment = 'LIVE'"],
  ['missing singleton', 'identity_row.singleton and ', ''],
  ['missing TEST guard', "and identity_row.environment = 'TEST'", ''],
  ['missing signature guard', "and inventory_row.identity_arguments = 'p_pay_batch_id uuid, p_pay_channel_scope text, p_actor_user_id uuid, p_pay_date date, p_week_start date, p_operation_id uuid, p_candidate_scope_ids jsonb'", ''],
  ['missing canonical hash guard', "and inventory_row.definition_sha256 = '49fbfaa7e3fbcefe7fde5e6937f2ac2b772fc62528460ae22246f98f7ad01d08'", ''],
  ['missing observed hash guard', "and observed_sha = '0449fe0daa2d6e1b23aa7d991bfce84d84fce30d7d123dff6804cbe528ddc4a4'", ''],
  ['missing configuration guard', "and mapped_proc.proconfig = array['search_path=public']::text[]", ''],
  ['different configuration', "array['search_path=public']::text[]", "array['search_path=public,private']::text[]"],
  ['missing closing guard', ') then', ') or true then'],
];
for (const [name, before, after] of refusedMutations) {
  test(`static exception contract rejects ${name}`, () => {
    // Mutate the exact condition, not an unrelated earlier inventory entry.
    const changed = expectedException.replace(before, after);
    assert.notEqual(changed, expectedException, 'mutation must actually change the condition');
    assert.throws(() => assertExceptionContract(changed), assert.AssertionError);
  });
}

test('managed release retains independent exact physical-target preflight', () => {
  const driver = fs.readFileSync(path.join(root, 'scripts/cloudtms-db-release.mjs'), 'utf8');
  const library = fs.readFileSync(path.join(root, 'scripts/cloudtms-db-release-lib.mjs'), 'utf8');
  assert.match(driver, /const actualDatabase = psql\(\{ sql: 'select pg_catalog\.current_database\(\);' \}\);/);
  assert.match(driver, /if \(actualDatabase !== expectedDatabase\)/);
  assert.match(driver, /assertCurrentDatabase\(expectedDatabase\);/);
  assert.match(library, /if \(!expectedDatabase\) throw new Error\('CLOUDTMS_EXPECTED_DATABASE is required for NEW and UPGRADE'\)/);
  assert.match(library, /databasePath !== expectedDatabase/);
  assert.match(library, /process\.env\.CLOUDTMS_ALLOW_LOCAL !== '1'/);
  assert.match(library, /if \(!locator\.includes\(expectedTarget\)\)/);
});
