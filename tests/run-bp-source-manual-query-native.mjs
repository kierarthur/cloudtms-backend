// Source-owned local PG17 query command proof, one unconditional rollback.
// Uses genuine upload/projection owners and bounded parser/economic fixture
// inputs; not a browser/parser/calculator/Banking application/deployment proof.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import path from 'node:path';

const root=path.resolve(import.meta.dirname,'..');
const banking=path.resolve(root,'../banking-pay-reset-implementation-20261003');
const container='codex-bpay-reset-release-pg17-20261003';
const database='source_local_joined_20261005';
const finalReportProof=process.argv.includes('--final-report-boundary');
assert(process.argv.slice(2).every(a=>a==='--final-report-boundary'),'closed native arguments');
const read=(name)=>readFileSync(path.join(root,name),'utf8');
const fixture=readFileSync(path.join(banking,'tests/fixtures/bpay-next-source-approved-basis-app-joined-real.sql'),'utf8');
assert.equal(createHash('sha256').update(fixture).digest('hex').toUpperCase(),
  'D7558104EAA4337D93FCC634796B9ACACC147A6EE418C93590DF9A76D14160CE','reviewed factual import fixture');
const setup=fixture.slice(fixture.indexOf('-- Unchanged bounded factual setup'),fixture.indexOf('-- One bounded real upload/finalisation.'));
assert.ok(setup.length>3000);
const importStart=fixture.indexOf('create function pg_temp.bpsc_import(');
const importEnd=fixture.indexOf('end $f$;',importStart)+'end $f$;'.length;
let importer=fixture.slice(importStart,importEnd);
assert.ok(importer.length>10000);
// Test-only stop after the genuine CURRENT projection. No owner is replaced
// or bypassed: this does not create a Final or initial finance certificate.
importer=importer.replace('p_session uuid default null)', 'p_session uuid default null,p_finalise boolean default true)');
const stop="  if p_session is not null then return v_result||jsonb_build_object('cycle_id',v_cycle,'upload_id',v_upload,'publication_id',v_publication);end if;";
assert.equal(importer.split(stop).length,2);
importer=importer.replace(stop,stop+"\n  if not p_finalise then return v_result||jsonb_build_object('cycle_id',v_cycle,'upload_id',v_upload,'publication_id',v_publication);end if;");
const strip=(sql)=>sql.replace(/^\\set.*$/gm,'').replace(/^begin;\r?$/gmi,'').replace(/^commit;\r?$/gmi,'');
const admission=readFileSync(path.join(banking,'supabase/repeatable/05102026_0044_bpay_next_source_pay_query_admission_v2.sql'),'utf8');
const helper=[...admission.matchAll(/create or replace function private\.weekly_source_pay_query_admit_v2\([\s\S]*?\$function\$;/gi)];
assert.equal(helper.length,1);
let finalEngine='';
if(finalReportProof){
  const source=readFileSync(path.join(banking,
    'supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql'),'utf8');
  const engines=[...source.matchAll(/create or replace function private\.weekly_source_finalise_engine_v1\([\s\S]*?\$function\$;/gi)];
  assert.equal(engines.length,1,'one actual integrated Final owner');
  assert.equal(engines[0][0].split('perform private.weekly_source_manual_reviews_finalised_v1(v_revision_id,v_actor);').length,2,
    'the exact actual Final caller owns query resolution');
  finalEngine=engines[0][0];
}
const checks=`
do $proof$
declare
  v_import jsonb; v_row uuid; v_source jsonb; v_request jsonb; v_reply jsonb;
  v_review uuid; v_cycle uuid; v_scope uuid; v_money_before jsonb; v_code text;
  v_actor constant uuid:='b8550000-0000-4000-8000-000000000001';
begin
  v_import:=pg_temp.bpsc_import(true,'${finalReportProof?'2026-09-06':'2026-09-20'}','[{"key":"manual-query","date":"${finalReportProof?'2026-08-31':'2026-09-08'}","end":"17:00","minutes":480,"break":0,"expense":0}]'::jsonb,'SOURCE_QUERY_NATIVE',null,false);
  v_cycle:=(v_import->>'cycle_id')::uuid;
  select r.id into strict v_row from public.weekly_source_upload_rows r where r.upload_id=(v_import->>'upload_id')::uuid;
  select p.report_scope_id into strict v_scope from public.weekly_source_projection_publications p where p.id=(v_import->>'publication_id')::uuid and p.projection_generation is null and p.state='CURRENT';
  v_source:=private.weekly_source_manual_review_source_v2(v_row);
  perform pg_temp.bpsc_assert(v_source is not null and v_source->>'projection_publication_id'=v_import->>'publication_id','ordinary NULL generation uses actual scope version');
  select jsonb_build_array((select count(*) from public.weekly_source_entitlement_heads),
    (select count(*) from private.bpay_next_work_revision),(select count(*) from private.bpay_next_financial_effect),
    (select count(*) from public.timesheets_financials)) into v_money_before;
  v_request:=jsonb_build_object('actor_user_id',v_actor,'source_row_id',v_row,'reason','Should be an extra hour here','command_id','b8550000-0000-4000-8000-000000000901');
  v_reply:=public.weekly_source_manual_review_open_v1(v_request);
  v_review:=(v_reply->>'review_id')::uuid;
  perform pg_temp.bpsc_assert(v_reply->>'ok'='true' and v_reply->>'already_open'='false','real query OPEN');
  perform pg_temp.bpsc_assert(public.weekly_source_manual_review_open_v1(v_request)=v_reply||jsonb_build_object('idempotent_replay',true),'exact OPEN replay');
  begin
    perform public.weekly_source_manual_review_open_v1(v_request||jsonb_build_object('reason','different'));
    raise exception 'collision was accepted';
  exception when unique_violation then
    get stacked diagnostics v_code=message_text;
    perform pg_temp.bpsc_assert(v_code='WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_COLLISION','same command cannot adopt new reason');
  end;
  begin
    update public.weekly_source_report_scopes set current_projection_publication_id=null where id=v_scope;
    perform pg_temp.bpsc_assert(private.weekly_source_manual_review_source_v2(v_row) is null,'contradictory current projection pointer refuses Source');
    raise exception 'FAULT_PROOF_ROLLBACK' using errcode='22023';
  exception when invalid_parameter_value then
    get stacked diagnostics v_code=message_text;
    perform pg_temp.bpsc_assert(v_code='FAULT_PROOF_ROLLBACK','restore actual pointer after negative');
  end;
  v_request:=jsonb_build_object('actor_user_id',v_actor,'review_id',v_review,'resolution_kind','OFFICE_ACCEPTED_SOURCE',
    'expected_current_row_hash',v_source->>'source_row_hash','command_id','b8550000-0000-4000-8000-000000000902');
  v_reply:=public.weekly_source_manual_review_resolve_v1(v_request);
  perform pg_temp.bpsc_assert(v_reply->>'ok'='true' and (select state='RESOLVED' from private.weekly_source_manual_reviews where id=v_review),'real provisional Source acceptance');
  perform pg_temp.bpsc_assert(public.weekly_source_manual_review_resolve_v1(v_request)=v_reply||jsonb_build_object('idempotent_replay',true),'exact RESOLVE replay');
  perform pg_temp.bpsc_assert((public.weekly_source_manual_review_open_v1(jsonb_build_object('actor_user_id',v_actor,
    'source_row_id',v_row,'reason','A new query after acceptance','command_id','b8550000-0000-4000-8000-000000000903'))->>'review_id')::uuid<>v_review,
    'genuine new command opens a new query after resolution');
  perform pg_temp.bpsc_assert(public.weekly_source_manual_review_resolve_v1(v_request)=v_reply||jsonb_build_object('idempotent_replay',true)
    and (select count(*)=1 from private.weekly_source_manual_reviews where state='OPEN'),'old RESOLVE replay cannot clear newly reopened query');
  begin
    update private.weekly_source_manual_review_commands set result_json='{}'::jsonb;
    raise exception 'immutable reply update accepted';
  exception when object_not_in_prerequisite_state then
    get stacked diagnostics v_code=message_text;
    perform pg_temp.bpsc_assert(v_code='WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_IMMUTABLE','retained command replies immutable');
  end;
  begin
    truncate private.weekly_source_manual_review_commands;
    raise exception 'immutable reply truncate accepted';
  exception when object_not_in_prerequisite_state then
    get stacked diagnostics v_code=message_text;
    perform pg_temp.bpsc_assert(v_code='WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_IMMUTABLE','retained command replies cannot be truncated');
  end;
  perform pg_temp.bpsc_assert(v_money_before=jsonb_build_array((select count(*) from public.weekly_source_entitlement_heads),
    (select count(*) from private.bpay_next_work_revision),(select count(*) from private.bpay_next_financial_effect),
    (select count(*) from public.timesheets_financials)),'query commands do not write financial authority');
  perform pg_temp.bpsc_assert((select count(*)=3 from private.weekly_source_manual_review_commands),'three retained command replies only');
  perform pg_temp.bpsc_assert(not has_function_privilege('service_role','private.weekly_source_manual_reviews_finalised_v1(uuid,uuid)','EXECUTE')
    and not has_function_privilege('authenticated','public.weekly_source_manual_review_open_v1(jsonb)','EXECUTE'),'private hook/browser ACLs closed');
end $proof$;
${finalReportProof?read('tests/fixtures/bp-source-manual-query-final-report-boundary.sql'):''}
set constraints all immediate;
select 'SOURCE_MANUAL_QUERY_NATIVE_PASS';
rollback;
`;
const batch=`begin; set local statement_timeout='120s'; set local lock_timeout='5s'; set local request.jwt.claim.role='service_role';
create function pg_temp.bpsc_assert(p_ok boolean,p_message text) returns void language plpgsql as $f$
begin if p_ok is distinct from true then raise exception 'SOURCE_MANUAL_QUERY_ASSERT: %',p_message; end if; end $f$;
select pg_temp.bpsc_assert(current_database()='${database}' and not exists(select 1 from public.weekly_source_uploads) and not exists(select 1 from private.weekly_source_manual_reviews),'own empty local clone');
${helper[0][0]}
revoke all on function private.weekly_source_pay_query_admit_v2() from public,anon,authenticated,service_role;
${strip(read('supabase/migrations/05102026_0428_weekly_source_manual_review_commands.sql'))}
${strip(read('supabase/repeatable/05102026_0428_weekly_source_manual_review_commands_v2.sql'))}
${finalEngine}
${setup}
${importer}
${checks}`;
const result=spawnSync('docker',['exec','-i',container,'psql','-X','-qtA','-v','ON_ERROR_STOP=1','-U','postgres','-d',database],{input:batch,encoding:'utf8',maxBuffer:3_000_000,timeout:180_000});
assert.equal(result.status,0,result.stderr);
assert.match(result.stdout,/SOURCE_MANUAL_QUERY_NATIVE_PASS/);
console.log('PASS actual provisional import, query OPEN/RESOLVE/replays/collision/current-pointer negative/no-financial-write/ACL/constraints');
if(finalReportProof) assert.match(result.stdout,/SOURCE_MANUAL_QUERY_FINAL_REPORT_BOUNDARY_PASS/);
if(finalReportProof) console.log('PASS actual Final owner: opening report/replay/omitted report retain query; new report with unchanged shift clears only at Final');
const fresh=spawnSync('docker',['exec',container,'psql','-X','-qtA','-v','ON_ERROR_STOP=1','-U','postgres','-d',database,'-c',
  "select to_regclass('private.weekly_source_manual_review_commands') is null and not exists(select 1 from public.weekly_source_uploads) and not exists(select 1 from private.weekly_source_manual_reviews);"],{encoding:'utf8'});
assert.equal(fresh.status,0,fresh.stderr);assert.equal(fresh.stdout.trim(),'t');
console.log('PASS independent fresh connection: candidate command table absent and fixture uploads/reviews absent after rollback');
