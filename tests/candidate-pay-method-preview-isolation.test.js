import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const read = p => readFileSync(new URL('../'+p,import.meta.url),'utf8');
const scope=read('supabase/repeatable/04082026_1219_candidate_pay_method_change_refresh_scope_v1.sql');
const apply=read('supabase/repeatable/20260221_01_add_tms_ref_num_and_admin_rpcs.sql');

test('preview cannot invalidate its own source sequence; only a new confirmed operation invalidates',()=>{
  const invalidation=scope.indexOf('v_scope_invalidation_result:=private.pay_workbench_scope_invalidate_v1(');
  assert.equal(scope.split('v_scope_invalidation_result:=private.pay_workbench_scope_invalidate_v1(').length,2);
  for(const guard of ['IF v_route_operation_id_text IS NOT NULL OR v_route_actor_user_id_text IS NOT NULL THEN',
    'CANDIDATE_PAY_METHOD_CHANGE_ADMIN_REQUIRED','CANDIDATE_PAY_METHOD_CHANGE_TARGET_NOT_COMMITTED',
    'IF v_route_job_id IS NOT NULL THEN','CANDIDATE_PAY_METHOD_CHANGE_DURABLE_QUEUE_SET_MISMATCH']){
    assert.ok(scope.indexOf(guard)>0&&scope.indexOf(guard)<invalidation,guard);
  }
  assert.match(scope.slice(0,invalidation),/ELSE\s+-- Preview and existing-operation checks are reads\.[\s\S]*?validated\.\s*$/);
  assert.ok(invalidation<scope.indexOf("PERFORM public._change_bump('pay_candidate:'"));
});

test('business conflicts remain strict but are not retried as PostgreSQL serialization failures',()=>{
  assert.doesNotMatch(scope+apply,/ERRCODE\s*=\s*'40001'/);
  assert.equal((scope.match(/ERRCODE = 'PT409'/g)||[]).length,2);
  assert.equal((apply.match(/ERRCODE = 'PT409'/g)||[]).length,8);
  assert.match(apply,/IF v_preview_source_change_seq <> v_current_source_change_seq THEN\s+RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_PREVIEW_STALE'\s+USING ERRCODE = 'PT409'/);
  assert.match(apply,/IF v_post_targeted_timesheet_ids IS DISTINCT FROM v_expected_targeted_timesheet_ids THEN\s+RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_CANONICAL_SCOPE_CHANGED'\s+USING ERRCODE = 'PT409'/);
  for(const code of ['ADMIN_REQUIRED','OPERATION_ID_CONFLICT','QUEUE_SET_MISMATCH','DURABLE_QUEUE_MISSING'])assert.ok(apply.includes('CANDIDATE_PAY_METHOD_CHANGE_'+code));
});

test('no schema-qualified conditional syntax or UUID min/max is introduced',()=>{
  assert.doesNotMatch(scope+apply,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(scope+apply,/\b(?:min|max)\(\s*\w+(?:\.\w+)?\s*::\s*uuid\s*\)/i);
});
