import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

test('publishing and replacing one non-NHSP client file preserves the other client publication', {
  skip: !process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
}, () => {
  const fixture=readFileSync('supabase/verification/15092026_1534_weekly_source_upload_publication_v1.sql','utf8');
  const begin=fixture.slice(fixture.indexOf('create function pg_temp.roster_begin('),fixture.indexOf('create function pg_temp.roster_stage_rows('))
    .replace('pg_temp.roster_begin(','pg_temp.scoped_roster_begin(p_cycle uuid,p_client uuid,')
    .replaceAll("'90000000-0000-4000-8000-000000000011'",'p_cycle')
    .replaceAll("'90000000-0000-4000-8000-000000000020'",'p_client');
  const sql=fixture.slice(0,fixture.indexOf('do $verification$'))+begin+`
insert into public.clients(id,name) values('90000000-0000-4000-8000-000000000022','Second isolated roster client');
insert into public.weekly_source_group_clients(source_group_id,client_id,valid_from,created_by_user_id)
values('90000000-0000-4000-8000-000000000010','90000000-0000-4000-8000-000000000022','2026-01-01','90000000-0000-4000-8000-000000000001');
insert into public.weekly_source_client_policies(source_group_id,client_id,effective_from,authority_mode,
  document_mode,self_bill_enabled,self_bill_correction_presentation,source_fixed_expenses_enabled,
  source_expense_vat_enabled,weekly_rate_classification_method,created_by_user_id)
select '90000000-0000-4000-8000-000000000010',id,'2026-01-01','SOURCE_AUTHORITY','CHECK_ONLY',
  true,'FULL_REVERSAL_REPLACEMENT',false,false,'SPLIT_RATE_WINDOWS','90000000-0000-4000-8000-000000000001'
from public.clients where id in ('90000000-0000-4000-8000-000000000020','90000000-0000-4000-8000-000000000022');
do $isolation$
declare actor uuid:='90000000-0000-4000-8000-000000000001'; a uuid; b uuid;
  client_a uuid:='90000000-0000-4000-8000-000000000020'; client_b uuid:='90000000-0000-4000-8000-000000000022';
  uploaded uuid; old_a uuid; upload_b uuid; publication uuid; publication_b uuid; response jsonb; target uuid; selected_client uuid;
begin
  a:=(public.weekly_source_client_cycle_resolve_atomic_v1(jsonb_build_object('actor_user_id',actor,
    'source_cycle_id','90000000-0000-4000-8000-000000000011','client_id',client_a))->>'source_cycle_id')::uuid;
  b:=(public.weekly_source_client_cycle_resolve_atomic_v1(jsonb_build_object('actor_user_id',actor,
    'source_cycle_id','90000000-0000-4000-8000-000000000011','client_id',client_b))->>'source_cycle_id')::uuid;
  perform pg_temp.assert_true(a<>b,'clients own separate cycles');
  for n in 1..3 loop
    target:=case when n=2 then b else a end; selected_client:=case when n=2 then client_b else client_a end;
    response:=pg_temp.scoped_roster_begin(target,selected_client,repeat(n::text,64),'client-'||n||'.csv');
    uploaded:=(response->>'logical_upload_id')::uuid;
    perform pg_temp.roster_stage_rows(uploaded,'ISOLATION-'||n);
    response:=public.weekly_source_upload_seal_atomic_v1(jsonb_build_object('actor_user_id',actor,'upload_id',uploaded));
    perform pg_temp.assert_true(response->>'status'='CURRENT','client file sealed');
    response:=public.weekly_source_projection_begin_atomic_v1(jsonb_build_object('actor_user_id',actor,
      'upload_id',uploaded,'expected_authority_scope_version',case when n=3 then 2 else 1 end));
    publication:=(response->>'publication_id')::uuid;
    perform pg_temp.complete_projection(publication);
    response:=public.weekly_source_projection_publish_atomic_v1(jsonb_build_object('actor_user_id',actor,'publication_id',publication));
    perform pg_temp.assert_true(response->>'status'='CURRENT','client projection published');
    if n=1 then old_a:=uploaded; end if;
    if n=2 then upload_b:=uploaded; publication_b:=publication; end if;
  end loop;
  perform pg_temp.assert_true((select state='SUPERSEDED' from public.weekly_source_uploads where id=old_a),'A replacement retires old A only');
  perform pg_temp.assert_true((select current_complete_upload_id=upload_b and current_projection_publication_id=publication_b and version=1
    from public.weekly_source_cycles where id=b),'B pointers and version unchanged');
  perform pg_temp.assert_true((select state='CURRENT' from public.weekly_source_uploads where id=upload_b),'B file remains current');
  perform pg_temp.assert_true((select state='CURRENT' from public.weekly_source_projection_publications where id=publication_b),'B projection remains current');
end;
$isolation$;
rollback;`;
  const result=spawnSync(process.env.CLOUDTMS_TEST_PSQL||'psql',['-X','-h','127.0.0.1','-p',
    process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,'-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
    {input:sql,encoding:'utf8',env:{...process.env,PGOPTIONS:'-c jit=off'}});
  assert.equal(result.status,0,result.stderr||result.error?.message);
});
