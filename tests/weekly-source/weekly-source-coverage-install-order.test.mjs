import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { closureFor, executableSqlFile, inventory, mapLogicalPostgresOwnerSql, repoRoot } from '../../scripts/cloudtms-db-release-lib.mjs';

const fragment = 'supabase/repeatable/05102026_1417_weekly_source_coverage_support/qualified_coverage.inc';
const owners = [
  'supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql',
  'supabase/repeatable/05102026_0308_weekly_source_pay_query_gate_v1.sql'
];
const read = (file) => readFileSync(path.join(repoRoot, file), 'utf8').replaceAll('\r\n', '\n');

test('one real transaction-neutral coverage body belongs to every atomic reader closure', () => {
  const source = read(fragment);
  assert.doesNotMatch(source, /^\s*(?:begin;|commit;|rollback;|\\ir|\\set)\s*$/gmi);
  for (const name of ['weekly_source_pay_query_facts_v1', 'weekly_source_covered_hours_incident_v1']) {
    assert.equal(source.split(`create or replace function private.${name}(`).length - 1, 1);
    assert.match(source, new RegExp(`revoke all on function private\\.${name}\\(uuid\\) from public,anon,authenticated,service_role;`));
  }
  for (const owner of owners) {
    const sql = read(owner);
    const include = '\\ir 05102026_1417_weekly_source_coverage_support/qualified_coverage.inc';
    assert.equal(sql.split(include).length - 1, 1);
    assert(sql.indexOf('begin;') < sql.indexOf(include));
    assert(sql.indexOf(include) < sql.indexOf('create or replace function'));
    assert(sql.lastIndexOf('commit;') > sql.indexOf(include));
    assert(closureFor(owner).paths.includes(fragment));
    assert.doesNotMatch(sql, /create or replace function private\.(?:weekly_source_pay_query_facts_v1|weekly_source_covered_hours_incident_v1)\(/);
  }
  assert(!inventory().repeatables.some((item) => item.path === fragment), 'pure include is not a standalone repeatable');
});

test('manager disagreement covers both system-correct and did-not-work confirmations', () => {
  const sql = read(owners[0]);
  const branch = sql.match(/if v_kind in \('SYSTEM_CORRECT','CANDIDATE_DID_NOT_WORK'\)[\s\S]*? then/);
  assert(branch, 'actual manager resolution branch');
  assert.match(branch[0], /and not \(v_incident\.candidate_action_state='RESPONDED'/);
  assert.match(branch[0], /candidate_answer\.issue_episode=v_incident\.episode_number/);
  assert.match(branch[0], /candidate_answer\.event_kind='CANDIDATE_RESPONDED'/);
  assert.match(branch[0], /candidate_answer\.bounded_payload_json->>'choice'='CANDIDATE_CORRECT'/);
  assert.doesNotMatch(branch[0], /and not \(v_kind=/);
});

test('generated-owner executable tree retains and maps the non-standalone support include', () => {
  const previous = process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
  process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER = 'CURRENT_USER';
  try {
    const mappedFragment = readFileSync(executableSqlFile(fragment), 'utf8');
    assert.equal(mappedFragment, mapLogicalPostgresOwnerSql(readFileSync(path.join(repoRoot, fragment), 'utf8')));
    assert.equal((mappedFragment.match(/owner to CURRENT_USER;/gi) || []).length, 2);
    assert.doesNotMatch(mappedFragment, /owner to postgres;/i);
    assert.equal((mappedFragment.match(/set search_path to 'pg_catalog','pg_temp'/gi) || []).length, 2);
    for (const name of ['weekly_source_pay_query_facts_v1', 'weekly_source_covered_hours_incident_v1']) {
      assert.match(mappedFragment, new RegExp(`revoke all on function private\\.${name}\\(uuid\\) from public,anon,authenticated,service_role;`));
    }
    for (const owner of owners) {
      const mappedOwner = executableSqlFile(owner);
      const include = readFileSync(mappedOwner, 'utf8').match(/^\\ir (.+qualified_coverage\.inc)$/m);
      assert(include);
      assert.equal(readFileSync(path.resolve(path.dirname(mappedOwner), include[1].trim()), 'utf8'), mappedFragment);
    }
    assert(!inventory().repeatables.some((item) => item.path === fragment));
  } finally {
    if (previous === undefined) delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
    else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER = previous;
  }
});
