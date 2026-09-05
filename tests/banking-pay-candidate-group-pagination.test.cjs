const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const test=require('node:test');
const root=path.resolve(__dirname,'..');
const sql=fs.readFileSync(path.join(root,'supabase/repeatable/31082026_1408_banking_pay_candidate_group_pagination_v2.sql'),'utf8');

test('Candidate Banking pages complete server groups before selecting representatives',()=>{
  assert.match(sql,/presentation_groups AS MATERIALIZED/);
  assert.match(sql,/GROUP BY presentation_group_kind,presentation_group_key/);
  assert.match(sql,/limited_rows AS MATERIALIZED/);
  assert.match(sql,/'total_count',\(SELECT count\(\*\) FROM presentation_groups\)/);
  assert.match(sql,/'ready_row_count',\(SELECT count\(\*\) FROM scoped_rows\)/);
  assert.match(sql,/'presentation_group_row_count',r\.presentation_group_row_count/);
  assert.doesNotMatch(sql,/\b(?:UPDATE|INSERT INTO|DELETE FROM)\s+public\./i);
});

test('large payment groups use one bounded read-only detail contract',()=>{
  assert.match(sql,/pay_workbench_session_get_candidate_ready_group_page_v1/);
  assert.match(sql,/p_limit integer DEFAULT 10/);
  assert.match(sql,/p_limit NOT BETWEEN 1 AND 25/);
  assert.match(sql,/'page_offset'/);
  assert.match(sql,/SET search_path TO '' SET statement_timeout TO '3s' SET lock_timeout TO '1s'/);
  assert.match(sql,/REVOKE ALL ON FUNCTION public\.pay_workbench_session_get_candidate_ready_group_page_v1[^;]+FROM PUBLIC,anon,authenticated/);
  assert.match(sql,/GRANT EXECUTE ON FUNCTION public\.pay_workbench_session_get_candidate_ready_group_page_v1[^;]+TO service_role/);
});

test('mandatory release evidence contains 10/2 groups and 10/10/8 detail chunks',()=>{
  const runtime=fs.readFileSync(path.join(root,'tests/31082026_1437_banking_pay_candidate_group_pagination_runtime_verification.sql'),'utf8');
  const release=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-release.json'),'utf8'));
  for(const file of ['supabase/verification/31082026_1436_banking_pay_candidate_group_pagination_v2_verification.sql',
    'tests/31082026_1437_banking_pay_candidate_group_pagination_runtime_verification.sql']){
    assert.ok(release.verificationFiles.includes(file));assert.ok(release.newVerificationFiles.includes(file));
  }
  assert.match(runtime,/40 Ready rows form 12 complete/);assert.match(runtime,/detail 10\/10\/8 paging failed/);
  assert.match(runtime,/set local cloudtms\.rollback_fixture_scope='BANKING_PAY_CANDIDATE_GROUP_PAGINATION_V2'/i);
  assert.match(runtime,/\:\{\?cloudtms_release_fixture_context\}/);
  assert.match(runtime,/set_config\(\s*'cloudtms\.release_verifier_context'/);
  const collisionIndex=runtime.indexOf('ROLLBACK_FIXTURE_ID_COLLISION');
  const includeIndex=runtime.indexOf('\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql');
  assert.ok(collisionIndex>=0&&collisionIndex<includeIndex);
  const fixture=fs.readFileSync(path.join(root,'tests/fixtures/28082026_1429_banking_pay_selection_setup.sql'),'utf8');
  assert.match(fixture,/current_database\(\)<>'banking_modal_v2_test'/);
  assert.doesNotMatch(fixture,/cloudtms_test_clone/);
  assert.match(fixture,/current_setting\('cloudtms\.release_verifier_context',true\)/);
  assert.match(fixture,/v_context->>'expected_database'.*current_database\(\)/);
  assert.match(fixture,/v_applying_count<>1 OR v_release_match_count<>1/);
});
