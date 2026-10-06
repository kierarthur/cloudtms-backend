import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const read = path => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8');
const support = read('supabase/verification/support/06102026_1117_source_workbench_fixture_isolation.sql');
const paths = [
  '15092026_1534_weekly_source_ordinary_pay_projection_v1.sql',
  '15092026_1534_weekly_source_read_projections_v1.sql',
  '17092026_1100_weekly_source_candidate_view_producer_v1.sql',
  '17092026_1200_weekly_source_audit_and_export_v1.sql',
];

test('fixture support guards all twelve real workbench tables without raw history aggregation', () => {
  assert.match(support, /\\ir 22092026_1850_source_fixture_capture\.sql/);
  assert.equal((support.match(/'public\.banking_pay_workbench_\w+'::regclass/g) ?? []).length, 12);
  assert.match(support, /perform pg_temp\.ws_verify_watch\(v_relation\)/);
  assert.match(support, /string_agg\(row_digest,'' order by row_digest\)/);
  assert.match(support, /jsonb_build_object\('count',pg_catalog\.count\(\*\),'sha256'/);
  assert.doesNotMatch(support, /jsonb_agg|disable trigger|session_replication_role|delete from public|truncate public/i);
});

for (const path of paths) test(`${path} preserves real-worker and non-target guards`, () => {
  const sql = read(`supabase/verification/${path}`);
  assert.match(sql, /\\ir support\/06102026_1117_source_workbench_fixture_isolation\.sql/);
  const block = sql.slice(sql.indexOf('do $paid_root_owned_setup_fanout$'), sql.indexOf('$paid_root_owned_setup_fanout$;', sql.indexOf('do $paid_root_owned_setup_fanout$')));
  assert.doesNotMatch(block, /exists\(select 1 from public\.banking_pay_workbench_sessions\)/);
  assert.match(block, /banking_pay_workbench_session_scope where candidate_id=/);
  assert.match(block, /banking_pay_workbench_sessions where status='OPEN' and discarded_at_utc is null/);
  assert.match(block, /banking_pay_workbench_candidate_source_lines where candidate_id=v_candidate/);
  assert.match(block, /where status='RUNNING'/);
  assert.match(block, /where status in \('RUNNING','PROCESSING','IN_PROGRESS'\)/);
  assert.match(block, /v_all_ids is distinct from v_expected_all_ids/);
  assert.match(block, /v_other_jobs_after is distinct from v_other_jobs/);
  assert.match(block, /v_bank_after is distinct from v_bank_before/);
  assert.match(block, /for v_call in 1\.\.3 loop/);
  assert.match(block, /12,v_claim_now,null::uuid,v_candidate,'SOURCE_PAID_FIXTURE_SETUP_TWELVE',180/);
  assert.match(block, /recovered_stale_count' is distinct from '0'/);
  assert.match(block, /PAID_FIXTURE_NAMED_FINALIZER_POSTURE_NOT_EXACT/);
  assert.match(block, /PAID_FIXTURE_TWELVE_ECONOMIC_ROW_DRIFT/);
  assert.match(block, /where j\.id=any\(v_owned\) and j\.status in \('QUEUED','RUNNING'\)/);
  assert.doesNotMatch(block, /update public\.banking_pay_workbench_jobs|delete from public\.banking_pay_workbench/i);
});

test('ordinary fixture retains pre-existing namespace plus exactly twelve genuinely due owners', () => {
  const sql = read(`supabase/verification/${paths[0]}`);
  assert.ok(sql.indexOf('ws_paid_existing_job_ids') < sql.indexOf('\\ir 15092026_1534_weekly_source_finalisation_v1.sql'));
  assert.match(sql, /PAID_FIXTURE_CANDIDATE_NAMESPACE_COLLISION/);
  assert.match(sql, /select id from pg_temp\.ws_paid_existing_job_ids\s+union all select unnest\(v_owned\)/);
  assert.match(sql, /cardinality\(v_ordinary\) is distinct from 8/);
  assert.match(sql, /cardinality\(v_owned\)<>12/);
  assert.match(sql, /run_at_utc<=v_claim_now\)<>12/);
});
