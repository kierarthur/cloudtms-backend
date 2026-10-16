import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync, readdirSync } from 'node:fs';
import { createHash, webcrypto } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const base=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const root=path.resolve(base,'../../../..');
const recipe=JSON.parse(readFileSync(path.join(base,'SOURCE_RECIPE.json')));
const sha=s=>createHash('sha256').update(s).digest('hex');
const source=file=>readFileSync(path.join(base,file),'utf8');
const rendererSource=execFileSync('git',['show',`${recipe.base_commit}:broker/src/weekly-source/manager-email.js`],{cwd:root,encoding:'utf8'});
// Exact complete published real renderer; no renderer replacement or provider.
const renderWeeklyManagerQueryEmail=new Function('crypto',rendererSource.replace(/\bexport\s+(?=(?:const|async function|function))/g,'')+
 '\nreturn renderWeeklyManagerQueryEmail;')(webcrypto);
let runtime=source('runtime/delivery-runtime.mjs').replace(/^import[\s\S]*?from '[^']+';\r?\n/gm,'');
runtime=runtime.slice(0,runtime.indexOf('async function prepareDispatchTargets'));
const api=new Function('crypto','renderWeeklyManagerQueryEmail',runtime+'\nreturn {renderDueIntents,renderManagerIntent,managerEmailModel};')(webcrypto,renderWeeklyManagerQueryEmail);
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const hash='a'.repeat(64);
const due={message_intent_id:id(1),projection_publication_id:id(2)};
const env={WEEKLY_SOURCE_ENVIRONMENT:'TEST',WEEKLY_SOURCE_MANAGER_TOKEN_SECRET:'synthetic-test-only-token-key-000000',
 MYTMS_MANAGER_ROUTE_HMAC_SECRET:'synthetic-test-only-route-key-000000'};
function row(name='Alex',n=3){return {clientId:id(20),clientName:'Ward One',candidateId:id(n),displayName:name,
 issueId:id(n+10),workDate:'2026-10-09',sourceStartInstant:'2026-10-09T07:00:00',start:'07:00',end:'15:00',breakMinutes:30,
 systemStart:'07:00',systemEnd:'15:00',systemBreakMinutes:0,systemAbsent:false,issueFamily:'HOURS_DIFFER',candidateRequested:true};}
function fixture(rows=[row()]){
 const calls=[];let staged;
 const input={ok:true,audience_kind:'MANAGER',tranche_kind:'MANAGER_EARLY_ALL',membership_hash:hash,
  shift_count:rows.length,client_count:new Set(rows.map(x=>x.clientId)).size,candidate_count:new Set(rows.map(x=>x.candidateId)).size,rows};
 const preparation={ok:true,manager_route_preparation_id:id(30),review_batch_id:id(31),recipient_generation_id:id(32),
  credential_generation:1,membership_hash:hash,issued_at_utc:'2026-10-16T10:00:00Z',expires_at_utc:'2026-10-23T10:00:00Z',
  environment:'TEST',agency_id:id(33),manager_address:'ward@example.invalid'};
 const dependencies={
  rpc:async(name,args)=>{calls.push({name,args});
   if(name==='weekly_source_message_render_due_list_v1')return {due_count:1,intents:[due]};
   if(name==='weekly_source_message_render_input_v1')return input;
   if(name==='weekly_source_manager_route_prepare_atomic_v1')return preparation;
   if(name==='weekly_source_message_render_stage_atomic_v1'){staged=args.p_request;return {ok:true};}
   throw Error('UNEXPECTED_RPC');},
  managerControlRpc:async(_schema,name,args)=>{calls.push({name,args});
   if(name==='weekly_query_manager_route_register_v1')return {manager_route_ticket_id:id(40)};
   if(name==='weekly_query_manager_route_resolve_v1')return {registry_binding_key:'synthetic-binding',route_version:1};
   if(name==='manager_review_origin_resolve_v1')return {manager_review_public_origin:'https://manager.example.invalid'};
   throw Error('UNEXPECTED_CONTROL_RPC');},
  sendManagerEmail:()=>{throw Error('PROVIDER_FORBIDDEN');},candidatePushFetch:()=>{throw Error('PUSH_FORBIDDEN');}
 };
 return {input,preparation,calls,dependencies,get staged(){return staged;}};
}

test('real current renderer renders only selected Alex; no Taylor content/link scope',async()=>{
 const f=fixture();const result=await api.renderDueIntents(env,f.dependencies,10);
 assert.deepEqual(result,{due:1,rendered:1,deferred:0,failed:0});
 assert.equal(f.staged.subject_text,'Timesheet queries requiring your review - 1 shift');
 assert.match(f.staged.html_body,/Alex/);assert.doesNotMatch(f.staged.html_body,/Taylor/);
 assert.equal(f.staged.membership_hash,hash);assert.equal(f.staged.agency_receipt_id,id(31));
 const registration=f.calls.find(x=>x.name==='weekly_query_manager_route_register_v1').args.p_registration;
 assert.equal(registration.original_membership_hash_hex,hash);
 assert.match(f.staged.html_body,new RegExp(id(31)));
 assert.equal(f.calls.some(x=>/push|quiet|calendar|provider/i.test(x.name)),false);
});
test('same actual preparation produces identical body, link and credential on retry',async()=>{
 const f=fixture();await api.renderDueIntents(env,f.dependencies,10);const first=structuredClone(f.staged);
 await api.renderDueIntents(env,f.dependencies,10);assert.deepEqual(f.staged,first);
});
test('recomposed Taylor subset gets its own genuine preparation/link; Alex is absent',async()=>{
 const f=fixture([row('Taylor',4)]);f.preparation.review_batch_id=id(41);f.preparation.manager_route_preparation_id=id(42);
 f.preparation.membership_hash=f.input.membership_hash='b'.repeat(64);
 await api.renderDueIntents(env,f.dependencies,10);assert.match(f.staged.html_body,/Taylor/);assert.doesNotMatch(f.staged.html_body,/Alex/);
 assert.match(f.staged.html_body,new RegExp(id(41)));assert.equal(f.staged.membership_hash,'b'.repeat(64));
});
test('native empty subset defers without a manager preparation or link registration',async()=>{
 const f=fixture();const original=f.dependencies.rpc;
 f.dependencies.rpc=async(name,...args)=>{if(name==='weekly_source_message_render_input_v1')throw Error('WEEKLY_SOURCE_MANAGER_RENDER_EMPTY');return original(name,...args);};
 assert.deepEqual(await api.renderDueIntents(env,f.dependencies,10),{due:1,rendered:0,deferred:1,failed:0});
 assert.equal(f.calls.length,1);assert.equal(f.staged,undefined);
});
test('native worker pause defers without a push or secure link',async()=>{
 const f=fixture();const original=f.dependencies.rpc;
 f.dependencies.rpc=async(name,...args)=>{if(name==='weekly_source_message_render_input_v1')throw Error('CONTACT_SOURCE_RENDER_PAUSED');return original(name,...args);};
 assert.equal((await api.renderDueIntents(env,f.dependencies,10)).deferred,1);assert.equal(f.calls.length,1);
});
test('membership changes between native input and prepare refuse before control registration',async()=>{
 const f=fixture();f.preparation.membership_hash='b'.repeat(64);
 await assert.rejects(api.renderManagerIntent(env,f.dependencies,due,f.input),/WEEKLY_SOURCE_RENDER_MEMBERSHIP_STALE/);
 assert.equal(f.calls.length,1);assert.equal(f.staged,undefined);
});
for(const key of ['shift_count','client_count','candidate_count'])test(`incorrect native ${key} refuses before route registration`,async()=>{
 const f=fixture();f.input[key]++;
 await assert.rejects(api.renderManagerIntent(env,f.dependencies,due,f.input),/WEEKLY_SOURCE_MANAGER_RENDER_INVALID/);
 assert.equal(f.calls.length,1);assert.equal(f.staged,undefined);
});
test('actual renderer handles multi-worker count from the current subset',async()=>{
 const f=fixture([row('Alex',3),row('Taylor',4)]);await api.renderDueIntents(env,f.dependencies,10);
 assert.equal(f.staged.subject_text,'Timesheet queries requiring your review - 2 shifts');assert.match(f.staged.html_body,/Alex/);assert.match(f.staged.html_body,/Taylor/);
});
test('unrecognised native refusal remains a failure, not an empty or paused answer',async()=>{
 const f=fixture();f.dependencies.rpc=async(name)=>{if(name==='weekly_source_message_render_due_list_v1')return {due_count:1,intents:[due]};throw Error('UNEXPECTED_CURRENT_FACT');};
 const original=console.warn;console.warn=()=>{};
 try{assert.equal((await api.renderDueIntents(env,f.dependencies,10)).failed,1);}finally{console.warn=original;}
});

test('published complete input pins and every emitted source byte match the recipe',()=>{
 for(const item of recipe.inputs){const bytes=execFileSync('git',['show',`${recipe.base_commit}:${item.path}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');assert.equal(sha(bytes),item.sha256,item.path);}
 for(const item of recipe.outputs)assert.equal(sha(readFileSync(path.join(base,item.path))),item.sha256,item.path);
 assert.equal(sha(source(recipe.runtime.path)),recipe.runtime.sha256);
});
test('one native member projection owns input and stage; route obtains that same native input',()=>{
 assert.match(source('forward/02_native_render_input.sqlinc'),/v_membership:=private.weekly_contact_source_manager_members_v1\(v_intent.id\)/);
 assert.match(source('forward/04_native_render_stage.sqlinc'),/v_membership:=private.weekly_contact_source_manager_members_v1\(v_intent.id\)/);
 assert.match(source('forward/03_native_route_prepare.sqlinc'),/v_input:=public.weekly_source_message_render_input_v1/);
});
test('generation uses exact same eligibility for hash projection and inserted native membership',()=>{
 assert.equal(source('forward/05_native_manager_generation.sqlinc').split('private.weekly_contact_source_manager_member_eligible_v1(incident.id,v_route.id,v_contact_at)').length-1,2);
});
test('native manager clocks retain current producer expressions and exact prior due provenance',()=>{
 const s=source('forward/05_native_manager_generation.sqlinc');
 assert.match(s,/v_candidate_generation.started_at_utc\+v_settings.manager_partial_digest_after/);
 assert.match(s,/v_due:=v_candidate_generation.deadline_at_utc/);
 assert.match(s,/v_settings.manager_first_delay_value,v_settings.manager_first_delay_unit/);
 assert.match(s,/if found then v_due:=v_contact_due.due_at_utc/);
 assert.match(s,/then v_contact_due.cohort_started_at_utc/);
 assert.doesNotMatch(s,/min\(activation.activated_at_utc\)/);
});
test('every native mutation carries explicit fixed search path, owner and role ACL',()=>{
 const acl=source('forward/99_acl.sqlinc');let count=0;
 for(const file of readdirSync(path.join(base,'forward')).filter(x=>x.endsWith('.sqlinc')&&!x.endsWith('_acl.sqlinc'))){
  const sql=source('forward/'+file);for(const m of sql.matchAll(/create or replace function ((public|private)\.[a-z0-9_]+)\(([\s\S]*?)\)\s*returns/gi)){
   count++;const sig=m[1]+'('+m[3].split(',').map(x=>x.trim().split(/\s+/)[1]).join(',')+')';
   assert.ok(acl.includes(`alter function ${sig} owner to postgres;`));
   assert.ok(acl.includes(`revoke all on function ${sig} from public,anon,authenticated,service_role;`));
   assert.equal(acl.includes(`grant execute on function ${sig} to service_role;`),m[2]==='public');
  }
 }
 assert.equal(count,17);
});
test('finite scheduler successor stays inside actual selected native due-route batch',()=>{
 const s=source('forward/08_native_scheduler.sqlinc');
 assert.match(s,/order by pg_catalog.min\(event.due_at_utc\),route.id,event.event_kind\s+limit v_limit/);
 assert.doesNotMatch(s,/CONTACT_SOURCE_ROUTE_CAPACITY_EXCEEDED|v_contact_route_count/);
 assert.equal(s.split('perform private.weekly_source_query_manager_generation_v1(v_route.route_id').length-1,1);
});
test('source check does not claim native qualification or add a queue/manual ordinal',()=>{
 assert.equal(recipe.native_status,'REQUIRED_NOT_RUN');
 const sql=recipe.outputs.map(x=>source(x.path)).join('\n');
 assert.doesNotMatch(sql,/create table|insert into public.weekly_contact_recheck_tasks|send_ordinal\s*[:=]\s*.*max/i);
 assert.match(sql,/CONTACT_SOURCE_NATIVE_ORDINAL_UNPROVIDED/);
});
