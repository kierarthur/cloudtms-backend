import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {createHash} from 'node:crypto';
import {matchCorrectionEntry,handleCorrectionEntry} from '../broker/src/correction-entry.mjs';
const hash=value=>createHash('sha256').update(value).digest('hex');
const proof=JSON.parse(fs.readFileSync('docs/healthroster-build/v6-owner-delivery-20261016/CORRECTION_ENTRY_ROOT_MOUNT_20261017.json'));
const raw=fs.readFileSync('broker/src/index.js','utf8');
const branch=proof.hunks.find(row=>row.before==='    const p = url.pathname;').after;
const execute=new (Object.getPrototypeOf(async function(){}).constructor)('req','env','matchCorrectionEntry','handleCorrectionEntry','bankingPayCorrectionTenantAuthority','requireUser','sbRpc','withCORS',`const url=new URL(req.url);${branch}\nreturn null;`);
const ts='10000000-0000-4000-8000-000000000001',actor='10000000-0000-4000-8000-000000000002',agency='8467e881-9691-4181-a738-f9834922d747';
const result={version:'TIMESHEET_CORRECTION_ENTRY_V1',requested_timesheet_id:ts,current_timesheet_id:ts,placement_revision:'9007199254740993',purpose:'CONTRACT_CORRECTION',may_open_review:false,disabled_reason:'REVIEW_UNAVAILABLE'};
test('complete Worker parent reverses only the two reviewed entry contributions',()=>{
 assert.equal(hash(raw),proof.workerParent.outputSha256);let before=raw;
 for(const {before:old,after} of [...proof.hunks].reverse()){assert.equal(before.split(after).length,2);before=before.replace(after,()=>old);}
 assert.equal(hash(before),proof.workerParent.inputSha256);
 assert(raw.indexOf(branch)<raw.indexOf('const nextRoute',raw.indexOf(branch))||raw.indexOf('const nextRoute',raw.indexOf(branch))===-1);
});
test('actual entry branch injects genuine Office authority into one fixed bounded RPC',async()=>{
 const env={},calls=[];const req=new Request(`https://test.invalid/api/timesheets/${ts}/correction-entry`);
 const response=await execute(req,env,matchCorrectionEntry,handleCorrectionEntry,()=>agency,async(e,r)=>{assert.equal(e,env);assert.equal(r,req);return {id:actor,tenant_id:agency,is_active:true,role:'user',payment_authoriser:true};},async(...args)=>{calls.push(args);return result;},(e,r,value)=>value);
 assert.equal(response.status,200);assert.equal(response.headers.get('cache-control'),'no-store');assert.deepEqual(await response.json(),result);
 assert.deepEqual(calls,[[env,'timesheet_correction_entry_v1',{p_request:{version:'TIMESHEET_CORRECTION_ENTRY_REQUEST_V1',timesheet_id:ts,actor_user_id:actor}},{timeoutMs:12000,allowAuthRefreshRetry:false}]]);
});
test('ordinary routes bypass entry and missing deployment authority reveals no subject',async()=>{
 const deps=[matchCorrectionEntry,handleCorrectionEntry,()=>{throw Error('missing');},()=>assert.fail('No actor read'),()=>assert.fail('No RPC'),(e,r,value)=>value];
 assert.equal(await execute(new Request('https://test.invalid/api/timesheets/'+ts),{},...deps),null);
 const response=await execute(new Request('https://test.invalid/api/timesheets/'+ts+'/correction-entry'),{},...deps);
 assert.equal(response.status,503);assert.deepEqual(await response.json(),{error:'CORRECTION_ENTRY_UNAVAILABLE'});
});
