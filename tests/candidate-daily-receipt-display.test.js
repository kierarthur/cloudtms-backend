import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const read=p=>fs.readFileSync(new URL('../'+p,import.meta.url),'utf8').replace(/\r\n/g,'\n');
const sql=read('supabase/repeatable/28082026_2236_candidate_daily_receipt_display_v1.sql');
const definition=(text,name)=>{
  const start=text.indexOf('create or replace function '+name+'(');
  const end=text.indexOf('$function$;',start);
  assert.ok(start>=0&&end>start);
  return text.slice(start,end+11).replace(/^  --.*\n/gm,'');
};
test('Daily receipt display changes no financial writer or other read behaviour',()=>{
  const name='private._candidate_daily_read_projection_v1';
  const before=definition(read('supabase/repeatable/28082026_2000_candidate_daily_read_context_v1.sql'),name);
  const after=definition(sql,name);
  const changed=after.match(/  v_hours:=case[\s\S]*? end;/)?.[0];
  assert.ok(changed);
  assert.match(changed,/candidate_first_receipt/);
  assert.match(changed,/office_resolution_pending/);
  assert.match(changed,/and not v_import and not v_protected/);
  assert.equal(after.replace(changed,'  v_hours:=coalesce(v_fin.total_hours,v_ts.worked_minutes::numeric/60,0);'),before);
});
test('Daily list reuses the same exact manager decision fallback as detail',()=>{
  const name='public.candidate_app_timesheet_page_v1';
  const before=definition(read('supabase/repeatable/28082026_2017_candidate_daily_timesheet_list_v1.sql'),name);
  const after=definition(sql,name);
  const old="'manager_approval_state',case d.detail#>>'{manager_approval,state}' when 'APPROVED' then 'MANAGER_APPROVED'\n        else d.detail#>>'{manager_approval,state}' end,";
  const replacement="'manager_approval_state',case coalesce(d.detail#>>'{manager_approval,state}',\n          d.detail#>>'{manager_review,manager_approval_state}')\n        when 'APPROVED' then 'MANAGER_APPROVED'\n        else coalesce(d.detail#>>'{manager_approval,state}',\n          d.detail#>>'{manager_review,manager_approval_state}') end,";
  assert.equal(after,before.replace(old,replacement));
});
test('first-use proof rejects null hours/approval and includes pending snapshot without read writes',()=>{
  const proof=read('supabase/verification/28082026_1858_candidate_daily_booked_source_verification.sql');
  assert.match(proof,/manager_approval_state' is distinct from 'MANAGER_APPROVED'/);
  assert.match(proof,/total_hours'\)::numeric is distinct from 10::numeric/);
  assert.match(proof,/'processing_status','UNASSIGNED'/);
  assert.match(proof,/v_documents_after is distinct from v_documents_before/);
  assert.match(proof,/DAILY_PENDING_SNAPSHOT_ROLLBACK/);
  for(const list of Object.values(JSON.parse(read('supabase/release/current-release.json'))).filter(Array.isArray)) {
    if(list.includes('supabase/verification/28082026_1858_candidate_daily_booked_source_verification.sql')) assert.ok(list.length>0);
  }
});
test('replacement preserves closed read ACLs and conditional syntax',()=>{
  assert.doesNotMatch(sql,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.match(sql,/revoke all on function private\._candidate_daily_read_projection_v1\([^;]+from public,anon,authenticated,service_role;/);
  assert.match(sql,/revoke all on function public\.candidate_app_timesheet_page_v1\([^;]+from public,anon,authenticated;/);
  assert.doesNotMatch(sql,/\b(?:insert into|update public\.|delete from|create trigger|alter table)\b/i);
});
