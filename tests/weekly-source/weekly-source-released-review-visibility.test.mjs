import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {spawn,spawnSync} from 'node:child_process';

const root='09102026_1130_weekly_source_released_review_visibility_v1.sql';
const sql=readFileSync('supabase/repeatable/'+root,'utf8');
const verification=readFileSync('supabase/verification/'+root,'utf8');
const before=readFileSync('supabase/repeatable/07102026_2215_weekly_source_same_file_recheck_recovery_v1.sql','utf8');
test('released review adds only bounded eligible current checking-file row clients',()=>{
  for(const bound of ["if v_tab='queries' then", "profile.profile_code='NHSP_PREFINAL_RELEASED_V1'",
    "profile.row_finalisation_capability='CHECKING_ONLY'",'not profile.single_client_required',
    "upload.state='CURRENT' and upload.report_scope_id is null",'publication.source_cycle_id=cycle.id',
    "publication.authority_scope_kind='CYCLE'",'publication.report_scope_id is null',
    'publication.authority_scope_version=cycle.version',
    'cycle.current_projection_publication_id=publication.id',
    'private.weekly_source_upload_client_eligible_v1(upload.id,resolution.client_id,source_row.work_date)',
    'resolution.generation=case',"'VIEW_SOURCE_PROGRESS',v_released_scope.source_group_id"])
    assert.ok(sql.includes(bound),bound);
});
test('review-only scopes never become final-report membership or financial authority',()=>{
  assert.match(sql,/'review_only',true,'prepared',false,'completed',false/);
  assert.doesNotMatch(sql,/\b(?:insert into|update|delete from) public\./i);
  assert.equal((sql.match(/CREATE OR REPLACE FUNCTION /g)||[]).length,1);
  assert.match(sql,/revoke all on function public\.weekly_source_combined_review_workspace_v1\(jsonb\) from public,anon,authenticated/);
  assert.match(sql,/grant execute on function public\.weekly_source_combined_review_workspace_v1\(jsonb\) to service_role/);
});
test('existing reads, filter guards, deduplication and actions are unchanged',()=>{
  const body=value=>value.match(/\bas \$function\$([\s\S]*?)\$function\$/i)[1].trim();
  const original=before.slice(before.indexOf('create or replace function public.weekly_source_combined_review_workspace_v1'));
  const amended=body(sql).replace('  v_released_scope record;\n','')
    .replace(/\n  -- Checking-only released NHSP files[\s\S]*?\n  -- A cycle publication is shared/,'\n  -- A cycle publication is shared');
  const normal=value=>value.replace(/\r\n/g,'\n').replace(/\n\s*\n/g,'\n');
  assert.equal(normal(amended),normal(body(original)));
});
test('mandatory NEW and UPGRADE verifications include contract, hours and charge continuation',()=>{
  const manifest=JSON.parse(readFileSync('supabase/release/current-release.json','utf8'));
  for(const key of ['verificationFiles','newVerificationFiles'])
    assert.ok(manifest[key].includes('supabase/verification/'+root),key);
  for(const assertion of ['released outside-group missing contract disappeared',
    'unresolved contract incorrectly created an Hours question',
    'eligible resolved contract did not expose missing candidate hours',
    'resolved charge mismatch disappeared','charge warning hid the independent missing-timesheet question',
    'signed matching hours incorrectly cleared a charge warning',
    'non-NHSP outside-group client acquired released review authority',
    'released review scope changed finalisation availability'])
    assert.ok(verification.includes(assertion),assertion);
  assert.doesNotMatch(verification,/disable trigger|session_replication_role/i);
  assert.match(verification,/rollback;\s*$/);
});
test('real PostgreSQL contract, missing-hours and charge visibility stages',{
  skip:!process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
},()=>{
  const result=spawnSync(process.env.CLOUDTMS_TEST_PSQL||'psql',['-X','-h','127.0.0.1','-p',
    process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,'-U',process.env.CLOUDTMS_TEST_PG_USER||'postgres','-d','banking_modal_v2_test',
    '-v','ON_ERROR_STOP=1','-f','supabase/verification/'+root],
    {encoding:'utf8',windowsHide:true,env:{...process.env,PGOPTIONS:'-c jit=off'}});
  assert.equal(result.status,0,result.stderr||result.error?.message);
});

test('shared counter conflict is reproduced and the pre-snapshot lock prevents it',{
  skip:!process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
},async()=>{
  const bin=process.env.CLOUDTMS_TEST_PSQL||'psql';
  const args=['-X','-h','127.0.0.1','-p',process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
    '-U',process.env.CLOUDTMS_TEST_PG_USER||'postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'];
  const run=sql=>spawnSync(bin,[...args,'-c',sql],{encoding:'utf8',windowsHide:true});
  assert.equal(run("insert into public.app_change_counters(entity_key,seq) values('clients',0) on conflict do nothing").status,0);
  for(const locked of [false,true]){
    const child=spawn(bin,[...args,'-At','-c',`begin isolation level repeatable read;
      set local lock_timeout='3s';
      ${locked?'lock table public.app_change_counters in share row exclusive mode;':''}
      select 'SNAPSHOT_READY';`,'-c',`select pg_sleep(0.8);
      insert into public.clients(id,name) values('e9200000-0000-4000-8000-000000000001','Counter race proof');
      rollback;`],{windowsHide:true});
    let out='',err='';child.stdout.on('data',b=>{out+=b;});child.stderr.on('data',b=>{err+=b;});
    const exited=new Promise(resolve=>child.on('close',resolve));
    await new Promise((resolve,reject)=>{
      child.stdout.on('data',()=>{if(out.includes('SNAPSHOT_READY'))resolve();});
      child.on('error',reject);child.on('close',()=>{if(!out.includes('SNAPSHOT_READY'))reject(Error(err));});
    });
    const writer=run("update public.app_change_counters set seq=seq+1 where entity_key='clients'");
    assert.equal(writer.status,0,writer.stderr);
    const status=await exited;
    if(locked)assert.equal(status,0,err);
    else {assert.notEqual(status,0);assert.match(err,/could not serialize access due to concurrent update/);}
  }
});
