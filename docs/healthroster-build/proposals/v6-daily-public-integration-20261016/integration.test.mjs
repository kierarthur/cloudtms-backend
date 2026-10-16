import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {dispatchDailyRateCorrectionRequest,createDailyRateCorrectionRpc} from './existing-exact/broker/src/daily-rate-correction.js';
import {ACTION_VARIANTS} from './existing-exact/broker/src/contract-correction-v3.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
const read=f=>fs.readFileSync(path.join(here,f),'utf8'),assembly=JSON.parse(read('ASSEMBLY.json'));
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const sql=read('forward/sql/01_public_services.sqlinc'),acl=read('forward/sql/99_acl.sqlinc');
const fn=name=>{const start=sql.indexOf('create or replace function '+name+'(');assert.ok(start>=0);const end=sql.indexOf('$function$;',start);return sql.slice(start,end+11);};
const id=n=>`00000000-0000-0000-0000-${String(n).padStart(12,'0')}`;
test('exact four Health owners, no fresh composer or Banking body supplied',()=>{
 assert.equal([...sql.matchAll(/create or replace function /g)].length,4);
 assert.ok(!/create or replace function (?:private\.bpay_|private\.daily_rate_correction_(?:review|accept|result)_v1)/i.test(sql));
 assert.ok(!/\b(?:insert into|delete from|update public\.|update private\.)/i.test(sql));
 assert.equal(assembly.freshEconomicsComplete,false);assert.equal(assembly.currentNativeDefinitionCount,15);
});
test('existing canonical Daily calculator, DTO and HTTP bodies are byte-identical',()=>{
 for(const row of assembly.existingExactModules){assert.equal(sha(fs.readFileSync(path.join(root,row.file))),row.sha256);assert.equal(sha(fs.readFileSync(path.join(here,'existing-exact',row.file))),row.sha256);assert.equal(row.mode,'VERIFY_EXISTING_EXACT_NO_OVERWRITE');}
 assert.equal(assembly.generatedOfficeDto.wholeGenerationExact,true);assert.equal(assembly.generatedOfficeDto.currentBrokerEquivalent,true);
});
test('generated SQL action variants exactly equal sole current V3 decoder',()=>{
 const match=sql.match(/jsonb_array_elements\('([^']+)'::jsonb\) v where/);assert.ok(match);assert.deepEqual(JSON.parse(match[1]),ACTION_VARIANTS);
 assert.ok(!sql.includes('__ACTION_VARIANTS_JSON__'));
});
test('saved preview replay precedes missing fresh composer and preparation gates',()=>{
 const body=fn('public.timesheet_daily_rate_correction_preview_v1');
 assert.ok(body.indexOf('preview_replay_v1(p_args)')<body.indexOf("to_regprocedure('private.daily_rate_correction_review_v1(jsonb)')"));
 assert.ok(body.includes("p_args->'calculation_snapshot'='null'::jsonb"));
 assert.ok(body.includes("and o.from_contract_id is null and o.to_contract_id is null"));
 assert.ok(body.includes("result:=private.daily_rate_correction_public_assert_v1(result,'PREVIEW')"));
});
test('accepted replay binds exact request/actor/purpose before current role-safe result, no new acceptance journal',()=>{
 const body=fn('public.timesheet_daily_rate_correction_accept_v1');
 for(const text of ["saved.request_kind is distinct from 'ACCEPT'","saved.request_hash is distinct from private.daily_rate_correction_request_hash_v1(p_args,'ACCEPT')",'o.created_by_user_id is distinct from actor','result:=private.daily_rate_correction_result_v1(operation,actor)'])assert.ok(body.includes(text));
 assert.ok(body.indexOf('result:=private.daily_rate_correction_result_v1(operation,actor)')<body.indexOf('result:=private.daily_rate_correction_accept_v1(p_args)'));
 assert.ok(body.includes("result#>>'{placement,original_timesheet_id}' is distinct from o.from_timesheet_id::text"));
});
test('all new public RPCs are service only; helper stays owner only; old ACL not rewritten',()=>{
 assert.equal([...acl.matchAll(/grant execute/g)].length,3);
 for(const kind of ['preview','accept','result'])assert.ok(acl.includes(`revoke all on function public.timesheet_daily_rate_correction_${kind}_v1(jsonb) from public,anon,authenticated,service_role;`));
 assert.ok(acl.includes('revoke all on function private.daily_rate_correction_public_assert_v1(jsonb,text) from public,anon,authenticated,service_role;'));
 assert.equal([...acl.matchAll(/alter function /g)].length,4);
});
test('bounded actual index parent edits reverse to complete current preimage',()=>{
 const before=fs.readFileSync(path.join(root,assembly.parent.file),'utf8');assert.equal(sha(before),assembly.parent.beforeSha256);
 let after=before;
 for(const c of assembly.parent.changes){assert.equal(after.split(c.anchor).length,2);after=after.replace(c.anchor,c.appliedText);}
 assert.equal(sha(after),assembly.parent.proposedAfterSha256);
 for(const c of [...assembly.parent.changes].reverse())after=after.replace(c.appliedText,c.anchor);
 assert.equal(after,before);assert.ok(before.includes('if (isBpayNextLegacyFinancialRoute(req.method, p))'));
});
test('real host leaves every unrelated path untouched and does not instantiate transport',async()=>{
 const AsyncFunction=Object.getPrototypeOf(async function(){}).constructor;
 const run=new AsyncFunction('req','env','ctx','dispatchDailyRateCorrectionRequest','createDailyRateCorrectionRpc','requireUser','sbHeaders','toLocalParts','subtractBreak','classifyMinutes','resolveEffectivePayChannel','withCORS','p',read('forward/host/daily-rate-correction.dispatch.jsinc')+'return null;');
 for(const p of ['/api/timesheets/a/process-daily-manual','/api/weekly-source/reports','/api/banking/pay','/api/timesheets/x/daily-rate-correction/preview/extra']){
  const forbidden=()=>{throw Error('UNRELATED_ROUTE_SIDE_EFFECT');};
  assert.equal(await run(new Request('https://unit.invalid'+p),{},null,forbidden,forbidden,forbidden,forbidden,forbidden,forbidden,forbidden,forbidden,forbidden,p),null);
 }
});
test('actual existing legacy fence does not seize new exact Daily API paths',async()=>{
 const {isBpayNextLegacyFinancialRoute}=await import(pathToFileURL(path.join(root,'broker/src/bpay-next-legacy-fence.js')).href);
 for(const [method,url] of [['POST',`/api/timesheets/${id(1)}/daily-rate-correction/preview`],['POST',`/api/timesheets/${id(1)}/daily-rate-correction/accept`],['GET',`/api/timesheets/daily-rate-correction/operations/${id(2)}`]])assert.equal(isBpayNextLegacyFinancialRoute(method,url),false);
});
test('missing native result composer gives truthful retryable dependency response, no invented DTO',async()=>{
 const req=new Request('https://unit.invalid/api/timesheets/daily-rate-correction/operations/'+id(2));
 let calls=0;
 const response=await dispatchDailyRateCorrectionRequest(req,{},null,{requireUser:async()=>({id:id(3),is_active:true,role:'admin'}),rpc:async(name,args)=>{calls++;assert.equal(name,'timesheet_daily_rate_correction_result_v1');assert.deepEqual(args,{p_args:{version:'DAILY_RATE_CORRECTION_V1',operation_id:id(2),actor_user_id:id(3)}});return new Response(JSON.stringify({code:'55000',message:'private details'}),{status:500,headers:{'content-type':'application/json'}});}});
 assert.equal(calls,1);assert.equal(response.status,503);assert.deepEqual(await response.json(),{ok:false,error_code:'DAILY_RATE_NATIVE_DEPENDENCY_UNAVAILABLE',retryable:true});
});
