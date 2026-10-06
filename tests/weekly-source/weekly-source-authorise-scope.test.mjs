import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { weeklySourceAuthoriseRouting, WEEKLY_SOURCE_AUTHORISE_PROBE_RPC } from '../../broker/src/weekly-source/authorise-routing.mjs';

const actor='81000000-0000-4000-8000-000000000001';
const root='81000000-0000-4000-8000-000000000002';
const scope=(applicable)=>({contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable});

test('Source and ordinary routing use only the permission-qualified scope owner',async()=>{
  for(const applicable of [true,false]){
    const calls=[];
    const result=await weeklySourceAuthoriseRouting(async(name,args,options)=>{
      calls.push({name,args,options});
      return [{[name]:scope(applicable)}];
    },root,actor);
    assert.equal(result.bound,applicable);
    assert.equal(result.refusal,null);
    assert.deepEqual(calls,[{name:'weekly_source_office_authorise_scope_v1',
      args:{p_request:{actor_user_id:actor,timesheet_id:root}},options:{timeoutMs:12000}}]);
  }
});

test('missing or malformed applicability cannot silently route a Source root to ordinary authorisation',async()=>{
  for(const response of [null,{}, {applicable:true},scope(null),scope('false'),
    {contract:'WEEKLY_SOURCE_OFFICE_PRESENTATION_V1',applicable:false},
    {...scope(false),extra:true},[scope(false),scope(false)]]){
    const result=await weeklySourceAuthoriseRouting(async()=>response,root,actor);
    assert.equal(result.bound,false);
    assert.equal(result.refusal?.error_code,'WEEKLY_SOURCE_AUTHORISE_ROUTING_UNAVAILABLE');
  }
});

test('permission, scope and dependency failures remain refusals; absent current root stays on ordinary refusal',async()=>{
  for(const message of ['42501 OFFICE_FORBIDDEN','55000 WEEKLY_SOURCE_AUTHORISE_SCOPE_AMBIGUOUS','dependency unavailable']){
    const result=await weeklySourceAuthoriseRouting(async()=>{throw new Error(message);},root,actor);
    assert.equal(result.refusal?.status,503);
  }
  const missing=await weeklySourceAuthoriseRouting(async()=>{throw new Error('WEEKLY_SOURCE_TIMESHEET_NOT_FOUND');},root,actor);
  assert.deepEqual(missing,{bound:false,refusal:null});
});

test('scope reader is service-only and has no payment-information or financial mutation dependency',async()=>{
  assert.equal(WEEKLY_SOURCE_AUTHORISE_PROBE_RPC,'weekly_source_office_authorise_scope_v1');
  const sql=await readFile(new URL('../../supabase/repeatable/05102026_1210_weekly_source_office_authorise_scope_v1.sql',import.meta.url),'utf8');
  assert.match(sql,/weekly_source_query_require_service_v1\(\)/);
  assert.match(sql,/v_actor,'VIEW_SOURCE_PROGRESS',\s*v_group,v_contract.client_id,v_root.week_ending_date/);
  assert.match(sql,/stable security definer/);
  assert.match(sql,/revoke all[\s\S]*from public,anon,authenticated,service_role/);
  assert.match(sql,/grant execute[\s\S]*to service_role/);
  assert.match(sql,/notify pgrst/);
  assert.doesNotMatch(sql,/weekly_source_office_timesheet_presentation|settlement_allocation|paid_evidence|bpay_next|timesheets_financials|entitlement_heads|\b(insert into|update public|delete from)\b/i);
});
