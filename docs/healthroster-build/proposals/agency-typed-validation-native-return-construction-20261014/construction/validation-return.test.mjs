import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
import {decodeAgencyReturnStep} from './55_native_return_reply_codec.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const expected={request_id:'11111111-1111-4111-8111-111111111111',actor_user_id:'22222222-2222-4222-8222-222222222222',timesheet_id:'33333333-3333-4333-8333-333333333333',timesheet_version:1};
const held=()=>({schema_version:'WEEKLY_SOURCE_AGENCY_NATIVE_RETURN_STEP_RESULT_V1',request_id:expected.request_id,request_version:14,phase:'AUTHORISED',view_actor_user_id:expected.actor_user_id,timesheet_id:expected.timesheet_id,timesheet_version:1,completed:false,last_refusal_code:'AGENCY_CHANGE_CURRENT_NATIVE_WEEK_VALIDATION_REQUIRED',completed_actor_user_id:null,return_invoice_no:null,completion_sha256:null});
const complete=()=>({...held(),phase:'COMPLETED',completed:true,last_refusal_code:null,completed_actor_user_id:'44444444-4444-4444-8444-444444444444',return_invoice_no:'INV-TEST-1',completion_sha256:'a'.repeat(64)});
test('UNIT_ONLY transport: truthful native validation hold never becomes a complete return',()=>{const value=decodeAgencyReturnStep(held(),expected);assert.equal(value.completed,false);assert.equal(value.phase,'AUTHORISED');assert.equal(value.return_invoice_no,null);});
test('UNIT_ONLY transport: completed replay retains its real step actor independently from the current viewer',()=>{const value=decodeAgencyReturnStep(complete(),expected);assert.notEqual(value.completed_actor_user_id,value.view_actor_user_id);assert.equal(value.return_invoice_no,'INV-TEST-1');});
test('UNIT_ONLY transport: genuine no-original-draft completion permits a null invoice number',()=>{const value=complete();value.return_invoice_no=null;assert.equal(decodeAgencyReturnStep(value,expected).return_invoice_no,null);});
const mutations=[
 ['foreign request',v=>v.request_id='55555555-5555-4555-8555-555555555555'],
 ['foreign actor',v=>v.view_actor_user_id='55555555-5555-4555-8555-555555555555'],
 ['foreign physical target',v=>v.timesheet_id='55555555-5555-4555-8555-555555555555'],
 ['stale physical version',v=>v.timesheet_version=2],
 ['numeric version string',v=>v.request_version='14'],
 ['unknown phase',v=>v.phase='WAITING'],
 ['queue success relabeled completion',v=>{v.phase='AUTHORISED';v.completed=true;}],
 ['completed with causal refusal',v=>v.last_refusal_code='WAITING'],
 ['raw private frame',v=>v.financial_frame={}],
 ['invoice choice',v=>v.invoice_id=expected.request_id],
 ['array completion actor',v=>v.completed_actor_user_id=[v.completed_actor_user_id]],
 ['array invoice number',v=>v.return_invoice_no=['INV-TEST-1']],
 ['array completion hash',v=>v.completion_sha256=[v.completion_sha256]],
 ['absent completion hash',v=>v.completion_sha256=null],
 ['oversized invoice text',v=>v.return_invoice_no='x'.repeat(201)],
 ['empty completed actor',v=>v.completed_actor_user_id=''],
 ['wrong completed type',v=>v.completed='true']
];
for(const [name,mutate] of mutations)test(`UNIT_ONLY actual decoder refuses ${name}`,()=>{const value=complete();mutate(value);assert.throws(()=>decodeAgencyReturnStep(value,expected),e=>e.code==='AGENCY_CHANGE_NATIVE_RETURN_RESULT_UNCONFIRMED');});
test('UNIT_ONLY actual decoder refuses scalar-array caller binding rather than coercing UUIDs',()=>{for(const key of ['request_id','actor_user_id','timesheet_id'])assert.throws(()=>decodeAgencyReturnStep(complete(),{...expected,[key]:[expected[key]]}));});
test('UNIT_ONLY actual decoder requires a closed causal hold and no stored completion fields',()=>{for(const patch of [{last_refusal_code:null},{completion_sha256:'b'.repeat(64)},{completed_actor_user_id:expected.actor_user_id},{return_invoice_no:'INV-OTHER'}])assert.throws(()=>decodeAgencyReturnStep({...held(),...patch},expected));});
test('SOURCE whole wrapper reverse preserves prior actor, original-D1, unrelated-lines and native REMOVE guards',()=>{
 const reverse=JSON.parse(fs.readFileSync(path.join(here,'SAME_DRAFT_UNCHANGED_EDITOR_FULL_REVERSE.json')));
 let value=fs.readFileSync(path.join(here,'57_complete_same_draft_with_unchanged_editor.sqlinc'),'utf8');
 for(const h of [...reverse.hunks].reverse()){assert.equal(value.split(h.after).length,2);value=value.replace(h.after,()=>h.before);}
 assert.equal(value,fs.readFileSync(path.join(here,'../native/05_agency_draft_round_trip.sqlinc'),'utf8'));
});
test('SOURCE Return delegates money to the unchanged editor and verifies complete actual native segment locks',()=>{
 const value=fs.readFileSync(path.join(here,'57_complete_same_draft_with_unchanged_editor.sqlinc'),'utf8');
 assert.match(value,/v_result:=public\.invoice_apply_edits\(v_invoice\.id,jsonb_build_object\('add_timesheet_ids',jsonb_build_array\(v_request\.current_timesheet_id\)\),p_actor_user_id\)/);
 assert.doesNotMatch(value,/agency_invoice_line_plan_v1|round\(|vat_rate|charge_amount\s*\*/);
 assert.match(value,/s->>'invoice_locked_invoice_id' is distinct from v_invoice\.id::text/);
 assert.match(value,/meta_json,tsfin_id/);assert.match(value,/AGENCY_DRAFT_ROUND_TRIP_NATIVE_COMPONENT_SUBSET_REFUSED/);
});
test('SOURCE typed discriminator preserves legacy UUID/FK and cannot admit a null Agency facts hash',()=>{
 const value=fs.readFileSync(path.join(here,'52_same_validation_store_typed_source.sqlinc'),'utf8');
 assert.match(value,/validation_source_kind text not null default 'LEGACY'/);
 assert.match(value,/validation_source_facts_sha256 is not null/);
 assert.match(value,/validation_source_kind='AGENCY_INVOICE_WEEK' and last_source is null/);
 assert.doesNotMatch(value,/alter column last_source|drop constraint|insert into|update public|create table|grant /i);
});
test('SOURCE completion requires rederived whole-week facts and the full genuine native validation-recompute cut',()=>{
 const value=fs.readFileSync(path.join(here,'53_agency_same_draft_return_step.sqlinc'),'utf8');
 assert.match(value,/private\.weekly_source_agency_week_complete_v1\(ts\.timesheet_id\)/);
 assert.match(value,/week_facts\.complete is distinct from true/);assert.match(value,/financial_row_sha256/);
 assert.match(value,/observed_outbox_id/);assert.match(value,/validation_source_actor_user_id/);
 assert.match(value,/AGENCY_CHANGE_NATIVE_WEEK_PRODUCER_UNAVAILABLE/);assert.match(value,/if error_state='55P03' then raise/);
 assert.ok(value.indexOf("'RETURN',p_actual_actor_user_id")<value.indexOf("set state='COMPLETED'"));
});
