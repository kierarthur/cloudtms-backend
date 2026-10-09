import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {spawnSync} from 'node:child_process';

const name='09102026_1250_weekly_source_nhsp_client_setup_v1.sql';
const sql=readFileSync('supabase/repeatable/'+name,'utf8');
const verifier=readFileSync('supabase/verification/'+name,'utf8');
test('reader and save derive only effective dedicated NHSP mode before contracts',()=>{
  assert.equal((sql.match(/if v_contract_count=0 and coalesce/g)||[]).length,2);
  for(const bound of ['settings.is_nhsp','not coalesce(settings.requires_hr,false)',
    'not coalesce(settings.autoprocess_hr,false)','not coalesce(settings.no_timesheet_required,false)',
    'settings.effective_from desc nulls last,settings.updated_at desc,settings.id desc',
    'v_scope_date','v_effective_from','WEEKLY_SOURCE_CLIENT_CONTRACT_POLICY_INCONSISTENT',
    'WEEKLY_SOURCE_CLIENT_READ_ONLY_POLICY_MISMATCH','WEEKLY_SOURCE_SETTINGS_STALE'])
    assert.ok(sql.includes(bound),bound);
  assert.doesNotMatch(sql,/\b(?:insert into|update|delete from) public\.(?:contracts|timesheets|invoices)\b/i);
  assert.equal((sql.match(/CREATE OR REPLACE FUNCTION /g)||[]).length,2);
});
test('all existing function bodies remain unchanged outside the bounded fallback',()=>{
  const previous=readFileSync('supabase/repeatable/15092026_1534_weekly_source_settings_admin_v1.sql','utf8');
  const body=(s,name)=>s.slice(s.toLowerCase().indexOf('create or replace function '+name))
    .match(/\bas \$function\$([\s\S]*?)\$function\$/i)[1];
  const normalize=s=>s.replace(/\r\n/g,'\n').replace(/\n\s*\n/g,'\n');
  for(const name of ['private._weekly_source_settings_client_shape_v1','public.weekly_source_client_settings_save_atomic_v1']){
    let changed=body(sql,name).replace('  v_client_nhsp_basis boolean:=false;\n','')
      .replace(/\n  -- Client setup precedes contracts\.[\s\S]*?  end if;\n/,'\n')
      .replace('v_eligible:=(v_contract_count>0 or v_client_nhsp_basis) and','v_eligible:=v_contract_count>0 and')
      .replace('if (v_contract_count=0 and not v_client_nhsp_basis) or','if v_contract_count=0 or');
    assert.equal(normalize(changed),normalize(body(previous,name)),name);
  }
});
test('both release modes include rollback and counter-safe proofs',()=>{
  const manifest=JSON.parse(readFileSync('supabase/release/current-release.json','utf8'));
  for(const key of ['verificationFiles','newVerificationFiles'])assert.ok(manifest[key].includes('supabase/verification/'+name));
  for(const assertion of ['NHSP before-contract settings not visible','NHSP first save or historical baseline failed',
    'NHSP subsequent save without contracts failed','Disabled effective NHSP mode accepted',
    'Ordinary client acquired NHSP eligibility','Caller overrode NHSP authority','Stale first-save version accepted',
    'NHSP setup changed contracts, timesheets or invoices','NHSP setup ACL boundary changed'])assert.ok(verifier.includes(assertion));
  assert.ok(verifier.indexOf('lock table public.app_change_counters')<verifier.indexOf('snapshot_guard.sql'));
  assert.doesNotMatch(verifier,/disable trigger|session_replication_role/i);
});
test('real PostgreSQL NHSP first save, repeat save and negative guards',{
  skip:!process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
},()=>{
  const r=spawnSync(process.env.CLOUDTMS_TEST_PSQL||'psql',['-X','-h','127.0.0.1','-p',process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
    '-U',process.env.CLOUDTMS_TEST_PG_USER||'postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','supabase/verification/'+name],
    {encoding:'utf8',windowsHide:true,env:{...process.env,PGOPTIONS:'-c jit=off'}});
  assert.equal(r.status,0,r.stderr||r.error?.message);
});

