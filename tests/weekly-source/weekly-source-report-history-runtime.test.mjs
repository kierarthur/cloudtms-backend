import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

test('completed report history uses durable reports and zero returns, with bounded immutable details', {
  skip: !process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
}, () => {
  let fixture = readFileSync('supabase/verification/15092026_1534_weekly_source_read_projections_v1.sql','utf8');
  const zeroProof = `do $history_zero$
declare result jsonb;
begin
  result:=public.weekly_source_report_history_v1(jsonb_build_object('actor_user_id','d1000000-0000-4000-8000-000000000001',
    'source_group_id','d5000000-0000-4000-8000-000000000002'));
  perform pg_temp.assert_true(result->>'total_count'='2','both no-shifts receipts appear');
  perform pg_temp.assert_true(result#>>'{rows,0,completion_kind}'='NO_SHIFTS_TO_IMPORT','zero receipt is labelled');
end; $history_zero$;
rollback;`;
  fixture=fixture.replace(/^rollback;\r?$/m,zeroProof);
  const marker=fixture.lastIndexOf('rollback;');
  assert.ok(marker>0);
  const proof=`do $history_report$
declare result jsonb; detail jsonb; request jsonb:=jsonb_build_object(
  'actor_user_id','d1000000-0000-4000-8000-000000000001','tab','history',
  'source_group_id','f5000000-0000-4000-8000-000000000001'); refused boolean:=false;
begin
  result:=public.weekly_source_combined_review_workspace_v1(request);
  perform pg_temp.assert_true(result->>'contract'='WEEKLY_SOURCE_REPORT_HISTORY_V1','history dispatch contract');
  perform pg_temp.assert_true(result->>'total_count'='1','one finalised report for selected group, not uploaded files');
  perform pg_temp.assert_true(result#>>'{rows,0,report_key}'='FINAL:f0200000-0000-4000-8000-000000000001','exact durable manifest');
  perform pg_temp.assert_true(not has_function_privilege('authenticated','public.weekly_source_report_history_v1(jsonb)','EXECUTE'),
    'history cannot be queried as a browser database role');
  detail:=public.weekly_source_combined_review_workspace_v1(request||jsonb_build_object('report_key',result#>>'{rows,0,report_key}','limit',1));
  perform pg_temp.assert_true(detail->>'contract'='WEEKLY_SOURCE_COMPLETED_REPORT_V1','exact report detail contract');
  perform pg_temp.assert_true(jsonb_array_length(detail->'shifts')=1 and jsonb_array_length(detail->'movements')=1,'bounded report details');
  perform pg_temp.assert_true(detail#>>'{shifts,0,break_minutes}' is not null,'finalised break retained');
  perform pg_temp.assert_true(detail#>>'{report,finalised_at}'='10 Sep 2026, 11:00','committed finalisation time');
  perform pg_temp.assert_true((detail->>'has_more')::boolean,'further report details available');
  detail:=public.weekly_source_combined_review_workspace_v1(request||jsonb_build_object('report_key',result#>>'{rows,0,report_key}',
    'limit',1,'cursor',detail->>'next_cursor'));
  perform pg_temp.assert_true(jsonb_array_length(detail->'movements')=1,'report details continue');
  begin
    perform public.weekly_source_report_history_v1(request-'tab'||jsonb_build_object('report_key','FINAL:f0200000-0000-4000-8000-000000000001',
      'client_id','ffffffff-ffff-4fff-8fff-ffffffffffff'));
  exception when sqlstate '22023' then refused:=true; end;
  perform pg_temp.assert_true(refused,'foreign filtered report refused');
  insert into public.weekly_source_finalisation_pay_runs(
    final_revision_id,source_cycle_id,requested_by_user_id,orchestration_key,
    final_revision_manifest_hash,task_manifest_hash,task_count,action_required_task_count,state,run_hash)
  select revision.id,revision.source_cycle_id,'d1000000-0000-4000-8000-000000000001',
    'report-history-local-follow-up',revision.manifest_hash,decode(repeat('ab',32),'hex'),
    1,1,'ACTION_REQUIRED',decode(repeat('cd',32),'hex')
  from public.weekly_source_final_revisions revision where revision.id='f1000000-0000-4000-8000-000000000001';
  result:=public.weekly_source_combined_review_workspace_v1(request||jsonb_build_object('tab','queries','section','checks'));
  perform pg_temp.assert_true(exists(select 1 from jsonb_array_elements(result->'rows') item
    where item#>>'{status,text}'='Approved hours need attention' and item->'follow_up_scope' is not null),
    'source completion does not hide the separate approved-hours follow-up from Queries');
  result:=public.weekly_source_combined_finalise_workspace_v1(request-'tab');
  perform pg_temp.assert_true(jsonb_array_length(result->'scopes')=0,
    'completed report is absent from pending Finalise even when approved-hours follow-up remains');
  result:=public.weekly_source_combined_review_workspace_v1(request||jsonb_build_object('tab','imports','section','current'));
  perform pg_temp.assert_true(not exists(select 1 from jsonb_array_elements(result->'rows') item
    where item->>'final_source'='Finalised' or item->>'state'<>'CURRENT'),
    'Imports excludes completed, superseded and rejected files');
end; $history_report$;
rollback;`;
  fixture=fixture.slice(0,marker)+proof+fixture.slice(marker+'rollback;'.length);
  const result=spawnSync(process.env.CLOUDTMS_TEST_PSQL||'psql',['-X','-h','127.0.0.1','-p',
    process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,'-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
    {input:fixture,encoding:'utf8',env:{...process.env,PGOPTIONS:'-c jit=off'},windowsHide:true});
  assert.equal(result.status,0,result.stderr||result.error?.message);
});
