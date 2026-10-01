import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

test('combined finalise preserves the prepared NHSP scope and does not include checking-only work', {
  skip: !process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
}, () => {
  const fixture = readFileSync('supabase/verification/15092026_1534_weekly_source_read_projections_v1.sql','utf8');
  const marker = 'do $test$';
  assert.equal(fixture.split(marker).length, 2);
  const sql = fixture.slice(0,fixture.indexOf(marker)) + `
do $combined$
declare value jsonb; single jsonb; sort_key text; request jsonb:=jsonb_build_object(
  'actor_user_id','d1000000-0000-4000-8000-000000000001',
  'source_group_id','d5000000-0000-4000-8000-000000000003');
begin
  value:=public.weekly_source_combined_finalise_workspace_v1(request);
  perform pg_temp.assert_true(value->>'contract'='WEEKLY_SOURCE_COMBINED_FINALISE_V1','combined contract');
  perform pg_temp.assert_true(jsonb_array_length(value->'scopes')=1,'one prepared Trust scope');
  perform pg_temp.assert_true(value#>>'{counts,ready}'='1','prepared row appears');
  perform pg_temp.assert_true(value#>>'{rows,0,candidate}'='Taylor Nurse','candidate identity retained');
  perform pg_temp.assert_true(value#>>'{rows,0,invoice_charge}'='£100.00','canonical source display retained');
  perform pg_temp.assert_true(value#>>'{rows,0,candidate_sort}'='Nurse','surname sort key');
  foreach sort_key in array array['client','candidate','day_date','system_hours','movement','commission','total_cost','invoice_charge','status','finalised_at'] loop
    single:=public.weekly_source_combined_finalise_workspace_v1(request||jsonb_build_object('sort_key',sort_key));
    perform pg_temp.assert_true(single->>'sort_key'=sort_key and jsonb_array_length(single->'rows')=1,
      'each visible data column sorts through the server without losing the report');
  end loop;
  single:=public.weekly_source_office_workspace_v1(jsonb_build_object(
    'actor_user_id',request->>'actor_user_id','tab','finalise',
    'source_group_id',request->>'source_group_id','source_cycle_id','d6000000-0000-4000-8000-000000000004',
    'client_id','d2000000-0000-4000-8000-000000000004',
    'report_scope_id','dc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_true(value#>'{scopes,0,finalise_payload}'=single#>'{finalise,finalise_payload}',
    'combined action retains the exact single-report stale and identity guards');
  value:=public.weekly_source_combined_finalise_workspace_v1(request||jsonb_build_object('list','complete'));
  perform pg_temp.assert_true(jsonb_array_length(value->'rows')=0,'prepared is not falsely complete');
  value:=public.weekly_source_combined_finalise_workspace_v1(request||jsonb_build_object('sort_key','candidate','seek','Nur','limit',1));
  perform pg_temp.assert_true(value#>>'{rows,0,candidate}'='Taylor Nurse' and not (value->>'has_more')::boolean,
    'server surname seek and EOF');
  value:=public.weekly_source_combined_finalise_workspace_v1(request||jsonb_build_object(
    'source_group_id','d5000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_true(jsonb_array_length(value->'rows')=0,'checking file stays out of finalisation');
  value:=public.weekly_source_combined_review_workspace_v1(request||jsonb_build_object('tab','imports','section','current'));
  perform pg_temp.assert_true(value->>'contract'='WEEKLY_SOURCE_COMBINED_REVIEW_V1','combined imports contract');
  perform pg_temp.assert_true(jsonb_array_length(value->'rows')>0,'prepared file remains visible in combined imports');
  single:=public.weekly_source_upload_detail_v1(jsonb_build_object(
    'actor_user_id',request->>'actor_user_id','upload_id',value#>>'{rows,0,row_key}','limit',1));
  perform pg_temp.assert_true(single->>'contract'='WEEKLY_SOURCE_UPLOAD_DETAIL_V1','on-demand file detail contract');
  perform pg_temp.assert_true(single->>'purpose'='Finalisation report','file detail preserves purpose');
  perform pg_temp.assert_true(jsonb_array_length(single->'shifts')=1,'file detail returns bounded actual shifts');
  perform pg_temp.assert_true(single#>>'{shifts,0,candidate}' is not null,'file detail shows candidate identity');
  perform pg_temp.assert_true(not has_function_privilege('authenticated','public.weekly_source_upload_detail_v1(jsonb)','EXECUTE'),'file detail is not a browser RPC');
  value:=public.weekly_source_combined_review_workspace_v1(request||jsonb_build_object('tab','queries','section','checks'));
  perform pg_temp.assert_true(value->>'contract'='WEEKLY_SOURCE_COMBINED_REVIEW_V1','combined Office checks contract');
end;
$combined$;
rollback;`;
  const result=spawnSync(process.env.CLOUDTMS_TEST_PSQL||'psql',['-X','-h','127.0.0.1','-p',
    process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,'-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
    { input:sql,encoding:'utf8',env:{...process.env,PGOPTIONS:'-c jit=off'} });
  assert.equal(result.status,0,result.stderr||result.error?.message);
});
