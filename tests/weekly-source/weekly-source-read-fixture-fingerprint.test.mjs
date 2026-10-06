import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql = readFileSync(new URL('../../supabase/verification/15092026_1534_weekly_source_read_projections_v1.sql', import.meta.url), 'utf8');
const helperSource = readFileSync(new URL('../../supabase/verification/support/06102026_1410_source_full_row_fingerprints.sql', import.meta.url), 'utf8');
const helper = helperSource.slice(helperSource.indexOf('create or replace function'));

test('read fixture hashes every complete economic row before aggregation, with counts and multiplicity', () => {
  assert.match(helper, /pg_catalog\.to_jsonb\(row_value\)::text,'sha256'/);
  assert.match(helper, /'count',pg_catalog\.count\(\*\)/);
  assert.match(helper, /pg_catalog\.string_agg\(row_digest,'' order by row_digest\)/);
  assert.doesNotMatch(helper, /distinct|limit|jsonb_agg/i);
  assert.match(helper, /returns text language plpgsql set search_path=''/);
  assert.ok(sql.indexOf('support/06102026_1410_source_full_row_fingerprints.sql') < sql.indexOf('savepoint g9_asserted_legacy_negative_fixture'));
});

test('all five economic before/after comparisons use the same bounded-payload fingerprint', () => {
  assert.equal((sql.match(/v_hash:=pg_temp\.ws_verify_full_relation_fingerprint\(v_relation::regclass\)/g) || []).length, 5);
  assert.doesNotMatch(sql, /md5\(coalesce\(jsonb_agg\(to_jsonb\([xt]\)/);
  for (const guard of ['PRESENTATION_EXISTING_REQUEST_BOUNDARY_NON_METADATA_DRIFT', 'PRESENTATION_EXISTING_BANK_ROW_DRIFT']) {
    assert.ok(sql.includes(guard), guard);
  }
  assert.ok(sql.includes('rollback;'));
});

test('existing job snapshots retain complete rows individually and compare exact IDs and all non-target digests', () => {
  const capsule = sql.slice(sql.indexOf('-- BEGIN GENUINE CERTIFIED PRESENTATION CAPSULE'), sql.indexOf('create temporary table g9_candidate_variant_restore'));
  assert.match(capsule, /bpspv_existing_jobs_before\(id uuid primary key,row_json jsonb not null\)/);
  assert.match(capsule, /select j\.id,to_jsonb\(j\) from public\.banking_pay_workbench_jobs j/);
  assert.match(capsule, /v_jobs_before is distinct from v_jobs_after/);
  assert.match(capsule, /prior\.row_json as old_row/);
  assert.match(capsule, /where to_jsonb\(j\) is distinct from old_row/);
  assert.match(capsule, /select id from pg_temp\.bpspv_existing_jobs_after_ids/);
  assert.match(capsule, /ws_verify_other_jobs_fingerprint\(array\[\]::uuid\[\]\)/);
  assert.equal((capsule.match(/ws_verify_other_jobs_fingerprint\(v_owned\)/g) || []).length, 3);
  assert.doesNotMatch(capsule, /jsonb_agg\(to_jsonb\(j\) order by j\.id\)/);
  assert.doesNotMatch(sql, /jsonb_agg\(to_jsonb\(j\) order by j\.id\)/);
  assert.equal((sql.match(/'jobs',pg_temp\.ws_verify_other_jobs_fingerprint\(array\[\]::uuid\[\]\)/g) || []).length, 3);
});

test('ordinary-pay companion uses the same complete economic fingerprint for all three comparisons', () => {
  const ordinary = readFileSync(new URL('../../supabase/verification/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql', import.meta.url), 'utf8');
  assert.match(ordinary, /support\/06102026_1410_source_full_row_fingerprints\.sql/);
  assert.equal((ordinary.match(/ws_verify_full_relation_fingerprint\(v_relation::regclass\)/g) || []).length, 3);
  assert.doesNotMatch(ordinary, /md5\(coalesce\(jsonb_agg\(to_jsonb\(t\)/);
  assert.match(ordinary, /PAID_FIXTURE_TWELVE_ECONOMIC_ROW_DRIFT/);
});

test('ordinary-pay economic failures identify changed relations without disclosing rows or relaxing the guard', () => {
  const ordinary = readFileSync(new URL('../../supabase/verification/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql', import.meta.url), 'utf8');
  for (const guard of ['PAID_FIXTURE_TWELVE_ECONOMIC_ROW_DRIFT', 'PAID_FIXTURE_FANOUT_ECONOMIC_ROW_DRIFT']) {
    const at = ordinary.indexOf(`message='${guard}'`);
    assert.ok(at > 0);
    const diagnostic = ordinary.slice(at, ordinary.indexOf(';', at));
    assert.match(diagnostic, /detail=\(select string_agg\(expected\.key,',' order by expected\.key\)/);
    assert.match(diagnostic, /where expected\.value is distinct from v_after->expected\.key/);
    assert.doesNotMatch(diagnostic, /string_agg\(expected\.value|limit|sample/i);
  }
  assert.equal((ordinary.match(/if v_after is distinct from v_before then/g) || []).length, 2);
});

for (const filename of ['17092026_1100_weekly_source_candidate_view_producer_v1.sql', '17092026_1200_weekly_source_audit_and_export_v1.sql']) {
  const source = readFileSync(new URL(`../../supabase/verification/${filename}`, import.meta.url), 'utf8');
  test(`${filename}: complete historical job and economic checks remain bounded`, () => {
    assert.match(source, /support\/06102026_1410_source_full_row_fingerprints\.sql/);
    assert.match(source, /select j\.id,to_jsonb\(j\) from public\.banking_pay_workbench_jobs j/);
    assert.match(source, /v_jobs_before is distinct from v_jobs_after/);
    assert.match(source, /prior\.row_json as old_row/);
    assert.match(source, /select id from pg_temp\.bpspv_existing_jobs_after_ids/);
    assert.equal((source.match(/ws_verify_other_jobs_fingerprint\(v_owned\)/g) || []).length, 3);
    assert.equal((source.match(/ws_verify_full_relation_fingerprint\(v_relation::regclass\)/g) || []).length, 5);
    assert.doesNotMatch(source, /jsonb_agg\(to_jsonb\(j\) order by j\.id\)|md5\(coalesce\(jsonb_agg\(to_jsonb\([xt]\)/);
    for (const guard of ['PRESENTATION_EXISTING_REQUEST_BOUNDARY_NON_METADATA_DRIFT', 'PRESENTATION_EXISTING_BANK_ROW_DRIFT', 'PAID_FIXTURE_TWELVE_CHILD_OR_NON_TARGET_DRIFT', 'PAID_FIXTURE_TWELVE_ECONOMIC_ROW_DRIFT', 'PAID_FIXTURE_OTHER_JOB_ROW_DRIFT']) assert.ok(source.includes(guard), guard);
    assert.ok(source.includes('rollback;'));
  });
}
