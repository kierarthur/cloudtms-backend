import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {decodeCandidateContractMove} from '../forward/candidate-contract-move.mjs';
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const target=n=>({identity_kind:'TIMESHEET',id:id(n),path:`/candidate-app/v1/timesheets/${id(n)}`});
function move(overrides={}){return {version:'CANDIDATE_CONTRACT_MOVE_V1',operation_id:id(1),family_root_id:id(2),placement_revision:'2',original_timesheet_id:id(2),current_timesheet_id:id(4),original_contract_label:'Client · Original',current_contract_label:'Client · Current',moved_at_utc:'2026-10-16T13:14:15.123456+00:00',evidence_origin_timesheet_id:id(2),current_mutable_timesheet_id:id(4),inherited_evidence:[{kind:'CANDIDATE_SIGNATURE',id:id(10),version:'1',sha256:'a'.repeat(64)}],original_detail_target:target(2),current_detail_target:target(4),unchanged_facts_sha256:'b'.repeat(64),current_state:'PENDING',is_history:false,...overrides};}
test('exact 17 native fields retain older source generation and original/current targets',()=>{
 const value=move();const result=decodeCandidateContractMove(value);assert.equal(Object.keys(result).length,17);assert.equal(result.inherited_evidence[0].version,'1');assert.notEqual(result,value);assert.notEqual(result.inherited_evidence,value.inherited_evidence);
});
test('history B between immutable A and current C is valid without inventing requested-id field',()=>assert.equal(decodeCandidateContractMove(move({is_history:true})).is_history,true));
test('uint63 revision rejects JS-number coercion, zero, sign, overflow and whitespace',()=>{for(const placement_revision of [2,'0','+2',' 2','9223372036854775808'])assert.throws(()=>decodeCandidateContractMove(move({placement_revision})));assert.equal(decodeCandidateContractMove(move({placement_revision:'9223372036854775807'})).placement_revision,'9223372036854775807');});
test('unknown Office/Bank payloads and target substitution are refused',()=>{
 for(const patch of [{reason:'Office only'},{current_detail_target:{...target(4),path:`/candidate-app/v1/timesheets/${id(2)}`}},{current_detail_target:{identity_kind:'timesheet',id:id(4)}},{current_mutable_timesheet_id:null},{current_state:'FINALISED'}])assert.throws(()=>decodeCandidateContractMove(move(patch)));
});
test('source refs require real closed source hash, string version and unique actual ID',()=>{
 for(const inherited_evidence of [[{kind:'CANDIDATE_SIGNATURE',id:id(10),version:1,sha256:'a'.repeat(64)}],[{kind:'B_PDF_INFERRED',id:id(10),version:null,sha256:'a'.repeat(64)}],[{kind:'CANDIDATE_SIGNATURE',id:id(10),version:null,sha256:'A'.repeat(64)}],[...move().inherited_evidence,...move().inherited_evidence]])assert.throws(()=>decodeCandidateContractMove(move({inherited_evidence})));
 assert.equal(decodeCandidateContractMove(move({inherited_evidence:[{...move().inherited_evidence[0],version:null}]})).inherited_evidence[0].version,null);
});
const input=JSON.parse(fs.readFileSync(new URL('../SOURCE_INPUTS.json',import.meta.url)));
const nativeBroker=fs.readFileSync(`${input.root}/broker/src/candidate-app-backend.js`,'utf8');
const helperStart=nativeBroker.indexOf('function managerSubmissionType(');
const managerHelper=nativeBroker.slice(helperStart,nativeBroker.indexOf('\n}\n',helperStart)+3);
const source=managerHelper+'\n'+fs.readFileSync(new URL('../forward/11_broker_native_contexts.mjsinc',import.meta.url),'utf8');
function compiled(deps){return Function(...Object.keys(deps),`${source};return {managerStartResult,candidatePaperPackContext};`)(...Object.values(deps));}
class HttpError extends Error{constructor(status,code){super(code);this.status=status;}}
function scalarDeps(){return {decodeCandidateContractMove,CandidateHttpError:HttpError,text:v=>v==null?'':String(v),upper:v=>String(v??'').toUpperCase(),UUID_RE:/^[0-9a-f-]{36}$/,SHA256_RE:/^[a-f0-9]{64}$/,normaliseMediaType:v=>v,requireUuid:v=>v,requireInteger:v=>v,requireSha256:v=>v};}
test('real manager result retains original signed request/page facts and only adds verified move metadata',()=>{
 const {managerStartResult}=compiled(scalarDeps());const raw={workflow_id:id(20),workflow_generation:2,approval_request_id:id(21),method:'PHONE',workflow_kind:'CONTRACT_HOURS',expires_at_utc:'2026-10-17T00:00:00Z',manifest_sha256:'c'.repeat(64),ordered_components:[{component_id:id(22),ordinal:1,component_kind:'HOURS_TIMESHEET',media_type:'application/pdf',byte_size:50,content_sha256:'d'.repeat(64),viewed:true}],reviewed_count:1,can_approve:true,can_refuse:true};
 const before=managerStartResult(raw,{});const after=managerStartResult({...raw,contract_move:move()},{});assert.deepEqual(Object.fromEntries(Object.entries(after).filter(([k])=>k!=='contract_move')),before);assert.equal(after.workflow_id,id(20));assert.equal(after.ordered_components[0].content_sha256,'d'.repeat(64));assert.throws(()=>managerStartResult({...raw,contract_move:{...move(),provider:'forbidden'}},{}));
});
async function paperRun(dto,requested=id(4),deny=false){const reads=[];const calls=[];const row={timesheet_id:dto?id(2):requested,version:1,sheet_scope:'WEEKLY',qr_token:'original-token',qr_status:'READY',document_state:'READY',current_document_version_id:id(30)};const workflow={id:id(20),generation:2,workflow_kind:'CONTRACT_HOURS',target_timesheet_id:id(2)};const pack={ready:true,key:'original-immutable-pack',sha256:'e'.repeat(64)};
 const deps={...scalarDeps(),verifyCandidateAccess:async()=>{calls.push('auth');return {};},candidateRpcArgs:(a,e,p)=>p,rpcCall:async(d,n,p)=>{calls.push(['detail',p]);if(deny)throw new HttpError(403,'CANDIDATE_DETAIL_NOT_FOUND');return dto?{contract_move:dto}:{};},restOne:async(e,r,q)=>{reads.push({r,q});return r==='timesheets'?row:{id:id(30),r2_key:'original-doc',sha256:'f'.repeat(64)};},activePaperWorkflowsForTimesheet:async(e,i)=>{calls.push(['workflow',i]);return [workflow];},requireCandidatePaperOutbox:async()=>({id:id(31)}),readyPaperPackReceiptFromOutbox:async()=>pack,readyPaperPackReceipt:async()=>pack,candidatePaperExecutionState:()=>({state:'READY'}),knownErrorCode:e=>e.message};
 const result=await compiled(deps).candidatePaperPackContext({}, {}, {}, requested);return {result,reads,calls};}
test('real paper context authorises requested B first, then resolves exact original source/pack with no new identity',async()=>{
 const {result,reads,calls}=await paperRun(move());assert.equal(calls[0],'auth');assert.deepEqual(calls[1],['detail',{p_timesheet_id:id(4),p_contract_week_id:null,p_workflow_id:null}]);assert.equal(result.id,id(2));assert.equal(result.workflow.id,id(20));assert.equal(result.key,'original-immutable-pack');assert.match(reads[0].q,new RegExp(id(2)));assert.doesNotMatch(reads[0].q,/is_current/);assert.match(reads[1].q,new RegExp(`entity_id=eq.${id(2)}`));
});
test('ordinary paper context preserves current-only native read',async()=>assert.match((await paperRun(null)).reads[0].q,/is_current=eq.true/));
test('native detail denial cannot fall through to any source asset read',async()=>await assert.rejects(paperRun(move(),id(4),true),/CANDIDATE_DETAIL_NOT_FOUND/));
test('repeated-move History B resolves immutable original pack without guessing a new workflow',async()=>{const {result}=await paperRun(move({is_history:true}),id(3));assert.equal(result.id,id(2));assert.equal(result.workflow.id,id(20));});
