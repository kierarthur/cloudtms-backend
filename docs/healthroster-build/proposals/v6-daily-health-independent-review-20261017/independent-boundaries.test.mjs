// Offline characterization of real pinned Worker and SQL source. Mocked RPC
// replies prove transport behavior only, never PostgreSQL roles/native effects.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {normalizeReason,VERSIONS} from '../../../../../healthroster-install2-14102026/broker/src/contract-correction-v3.mjs';
import {handleDailyRateCorrectionGet,handleDailyRateCorrectionAccept} from '../../../../../healthroster-install2-14102026/broker/src/daily-rate-correction.js';
const prefix='Applying updated rates';
const source=fs.readFileSync(new URL('../../../../../healthroster-checklist-native-14102026/docs/healthroster-build/proposals/daily-health-composition-20261017/17102026_0120_daily_health_composition.sql',import.meta.url),'utf8');
// Character-level model of the exact SQL left/substr inverse. This deliberately
// uses code points (PG UTF-8 text), not JavaScript UTF-16 slice offsets.
assert(source.includes("left(p_stored_reason,char_length(prefix)+3)=prefix||' — '"));
assert(source.includes('note:=substr(p_stored_reason,char_length(prefix)+4);'));
function sourceInverse(stored){
 if(typeof stored!=='string')throw Error('invalid');
 const chars=[...stored],n=[...prefix].length;
 const note=stored===prefix?'':chars.slice(0,n+3).join('')===prefix+' — '?chars.slice(n+3).join(''):null;
 if(note===null||normalizeReason(note,'DAILY_RATE_CORRECTION')!==stored)throw Error('invalid');
 return note;
}
const trimCodes=[9,10,11,12,13,32,160,5760,8192,8193,8194,8195,8196,8197,8198,8199,8200,8201,8202,8232,8233,8239,8287,12288,65279];
test('all native TrimString boundary codepoints retain the same canonical Details identity',()=>{
 for(const cp of trimCodes){
  const whitespace=String.fromCodePoint(cp),raw=whitespace+'details'+whitespace;
  const stored=normalizeReason(raw,'DAILY_RATE_CORRECTION');
  assert.equal(stored,prefix+' — details');assert.equal(sourceInverse(stored),'details');
  assert.equal(normalizeReason(sourceInverse(stored),'DAILY_RATE_CORRECTION'),normalizeReason(raw,'DAILY_RATE_CORRECTION'));
 }
});
test('Unicode full1969 boundary and exact single prefix inverse preserve codepoints',()=>{
 for(const note of ['x'.repeat(1969),'😀'.repeat(1969),'e\u0301'.repeat(984)+'z',prefix+' — '+prefix,'\u200bdetails\u200b']){
  const stored=normalizeReason(note,'DAILY_RATE_CORRECTION');
  assert.equal(sourceInverse(stored),note);
  assert.equal(normalizeReason(sourceInverse(stored),'DAILY_RATE_CORRECTION'),stored);
 }
 assert.throws(()=>normalizeReason('😀'.repeat(1970),'DAILY_RATE_CORRECTION'));
 for(const bad of [null,prefix+' — ',prefix+' — details ',prefix+' — '+'😀'.repeat(1970),' '+prefix,prefix+' - details'])assert.throws(()=>sourceInverse(bad));
});
const id=n=>`00000000-0000-0000-0000-${String(n).padStart(12,'0')}`;
const result=()=>({version:VERSIONS.DAILY,purpose:'DAILY_RATE_CORRECTION',operation_id:id(1),state:'COMPLETE',state_revision:'2',placement:{status:'COMMITTED',original_timesheet_id:id(2),current_timesheet_id:id(2),placement_revision:'1',receipt_id:id(3)},financial:{status:'REGISTERED',command_id:null,cut_id:null,summary:{visibility:'REDACTED'}},billing:{status:'DRAFTS_READY',revision:'1',group_id:null,visible_summary:{visibility:'REDACTED'}},next_actions:[],links:[]});
const get=()=>new Request('https://unit.invalid/api/timesheets/daily-rate-correction/operations/'+id(1));
test('actual GET admits current active nonAdmin and forwards only operation and authenticated actor',async()=>{
 const calls=[],value=result();
 const response=await handleDailyRateCorrectionGet(get(),{},{requireUser:async()=>({id:id(4),is_active:true,role:'viewer'}),rpc:async(name,args)=>{calls.push({name,args});return Response.json(value);}});
 assert.equal(response.status,200);assert.deepEqual(await response.json(),value);
 assert.deepEqual(calls,[{name:'timesheet_daily_rate_correction_result_v1',args:{p_args:{version:VERSIONS.DAILY,operation_id:id(1),actor_user_id:id(4)}}}]);
});
test('actual GET refuses inactive reader before RPC; mutation still refuses active nonAdmin',async()=>{
 let calls=0;const deps={requireUser:async()=>({id:id(4),is_active:false,role:'admin'}),rpc:async()=>{calls++;throw Error('unexpected');}};
 assert.equal((await handleDailyRateCorrectionGet(get(),{},deps)).status,401);assert.equal(calls,0);
 deps.requireUser=async()=>({id:id(4),is_active:true,role:'viewer'});
 const body={version:VERSIONS.DAILY,request_id:id(5),operation_id:id(1),review_id:id(6),review_digest:'a'.repeat(64),expected_state_revision:'1'};
 const request=new Request('https://unit.invalid/api/timesheets/'+id(2)+'/daily-rate-correction/accept',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
 assert.equal((await handleDailyRateCorrectionAccept(request,{},deps)).status,403);assert.equal(calls,0);
});
test('actual GET strict public decoder rejects raw receipt/unknown command fields',async()=>{
 for(const extra of [{placement_receipt:{command_id:id(7),cut_id:id(8)}},{financial_command_id:id(7)}]){
  const response=await handleDailyRateCorrectionGet(get(),{},{requireUser:async()=>({id:id(4),is_active:true,role:'viewer'}),rpc:async()=>Response.json({...result(),...extra})});
  assert.equal(response.status,502);const body=await response.json();assert.equal(body.error_code,'DAILY_RATE_RESPONSE_INVALID');assert(!JSON.stringify(body).includes(id(7)));
 }
});
