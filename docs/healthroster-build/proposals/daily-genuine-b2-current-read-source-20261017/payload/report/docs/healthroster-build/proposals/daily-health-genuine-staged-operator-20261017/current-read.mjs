// Finite B2 evidence protocol. No connector, SQL executor, browser, credentials or filesystem.
import {check,exact,sha,requestSha} from './contract.mjs';
import {buildReadInput,readEnvelope} from './native-read.mjs';
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b),hash=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
const stages=['S1','S1a','S2','S3','S4'],iso=v=>typeof v==='string'&&Number.isFinite(Date.parse(v))&&new Date(Date.parse(v)).toISOString()===v;
export const currentFiles=stage=>{check(stages.includes(stage),'FIXTURE_CURRENT_STAGE_REQUIRED');return {input:stage+'_CURRENT_INPUT.json',binding:stage+'_CURRENT_BINDING.json',native:stage+'_CURRENT_NATIVE.json',timing:stage+'_CURRENT_TIMING.json',consumption:stage+'_CURRENT_CONSUMPTION.json',evidence:'docs/healthroster-build/v6-owner-delivery-20261016/DAILY_GENUINE_'+stage+'_CURRENT_READ_20261017.json',consumptionEvidence:'docs/healthroster-build/v6-owner-delivery-20261016/DAILY_GENUINE_'+stage+'_CURRENT_CONSUMPTION_20261017.json'};};
export const jsonBytes=v=>Buffer.from(JSON.stringify(v)+'\n');
const text=bytes=>new TextDecoder('utf-8',{fatal:true}).decode(bytes);
const label=(stage,nonce)=>'HR_DAILY_CURRENT_'+stage.toUpperCase()+'_'+nonce.toUpperCase();
const bindingKeys=['version','stage','nonce','identity','operatorSourceCommit','actorBindingSha256','beforeInputSha256','beforeEnvelopeSha256','entrySha256','effectAdmissionSha256','requestSha256','sqlSha256','verificationSqlSha256','generated','admittedAt','readyAt'];
function bindingShape(binding){
 check(exact(binding,bindingKeys)&&binding.version==='DAILY_GENUINE_CURRENT_BINDING_V1'&&stages.includes(binding.stage)&&/^[a-f0-9]{32}$/.test(binding.nonce),'FIXTURE_CURRENT_BINDING_REQUIRED');
 const i=binding.identity;check(exact(i,['physicalDatabase','databaseOid','backendPid','turnId','key','takenAt','task'])&&i.physicalDatabase==='cloudtms_test_clone'&&Number.isSafeInteger(i.databaseOid)&&i.databaseOid>0&&Number.isSafeInteger(i.backendPid)&&i.backendPid>0&&/^hr_turn_[a-f0-9]{32}$/.test(i.turnId)&&same(i.key,[20261013,1])&&iso(i.takenAt)&&i.task==='DAILY_GENUINE_'+binding.stage+'_STAGED_ONLY','FIXTURE_CURRENT_ORIGINAL_SEVEN_FIELD_TURN_REQUIRED');
 check(/^[a-f0-9]{40}$/.test(binding.operatorSourceCommit)&&['actorBindingSha256','beforeInputSha256','beforeEnvelopeSha256','entrySha256','effectAdmissionSha256','requestSha256','sqlSha256','verificationSqlSha256'].every(k=>hash(binding[k]))
  &&exact(binding.generated,['settingsId','rateId','timesheetId'])&&Object.values(binding.generated).every(v=>v===null||typeof v==='string'&&/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(v))&&iso(binding.admittedAt)&&iso(binding.readyAt)&&Date.parse(binding.readyAt)>=Date.parse(binding.admittedAt),'FIXTURE_CURRENT_BINDING_SHAPE_REQUIRED');
}
export function assertCurrentBinding(binding,{beforeBuilt,beforeInputBytes,beforeBytes,entry,entrySha256}){
 bindingShape(binding);const i=binding.identity;
 check(same(JSON.parse(text(beforeInputBytes)),beforeBuilt)&&beforeBuilt.receipt.stage===binding.stage&&beforeBuilt.receipt.phase==='BEFORE'&&same(i,beforeBuilt.receipt.identity),'FIXTURE_CURRENT_BASELINE_INPUT_REQUIRED');
 check(/^[a-f0-9]{40}$/.test(binding.operatorSourceCommit)&&binding.operatorSourceCommit===entry.sourceCommit&&binding.actorBindingSha256===beforeBuilt.receipt.actorBindingSha256
  &&binding.beforeInputSha256===sha(beforeInputBytes)&&binding.beforeEnvelopeSha256===sha(beforeBytes)&&binding.entrySha256===entrySha256&&binding.effectAdmissionSha256===entry.effectAdmissionSha256
  &&binding.requestSha256===requestSha(binding.stage)&&binding.sqlSha256===beforeBuilt.receipt.sqlSha256&&binding.verificationSqlSha256===beforeBuilt.receipt.verificationSqlSha256
  &&same(binding.generated,beforeBuilt.receipt.generated)&&['actorBindingSha256','beforeInputSha256','beforeEnvelopeSha256','entrySha256','effectAdmissionSha256','requestSha256','sqlSha256','verificationSqlSha256'].every(k=>hash(binding[k])),'FIXTURE_CURRENT_BASELINE_BINDING_CHANGED');
 check(iso(binding.admittedAt)&&iso(binding.readyAt)&&Date.parse(binding.readyAt)>=Date.parse(binding.admittedAt),'FIXTURE_CURRENT_ADMISSION_ORDER_REQUIRED');return binding;
}
export function buildCurrent({stage,identity,actorId,generated,beforeBuilt,beforeInputBytes,beforeBytes,entry,entrySha256,admittedAt,readyAt,nonce}){
 const built=buildReadInput({stage,phase:'BEFORE',identity,actorId,settingsId:generated.settingsId??null,rateId:generated.rateId??null,timesheetId:generated.timesheetId??null});
 check(same(built,beforeBuilt),'FIXTURE_CURRENT_REGENERATED_INPUT_CHANGED');
 const binding={version:'DAILY_GENUINE_CURRENT_BINDING_V1',stage,nonce,identity,operatorSourceCommit:entry.sourceCommit,actorBindingSha256:built.receipt.actorBindingSha256,beforeInputSha256:sha(beforeInputBytes),beforeEnvelopeSha256:sha(beforeBytes),entrySha256,effectAdmissionSha256:entry.effectAdmissionSha256,requestSha256:requestSha(stage),sqlSha256:built.receipt.sqlSha256,verificationSqlSha256:built.receipt.verificationSqlSha256,generated:built.receipt.generated,admittedAt,readyAt};
 assertCurrentBinding(binding,{beforeBuilt,beforeInputBytes,beforeBytes,entry,entrySha256});
 // Only the tool envelope label is distinct. Both SQL strings and the native BEFORE phase stay exact.
 built.input.label=built.receipt.label=label(stage,nonce);
 const ready={type:'CURRENT_NATIVE_READY',binding,inputSha256:sha(jsonBytes(built)),bindingSha256:sha(jsonBytes(binding))};
 assertCurrentReady(ready);return {built,binding,ready};
}
export function assertCurrentReady(ready,{stage,source}={}){
 check(exact(ready,['type','binding','inputSha256','bindingSha256'])&&ready.type==='CURRENT_NATIVE_READY'&&exact(ready.binding,bindingKeys)&&ready.binding.version==='DAILY_GENUINE_CURRENT_BINDING_V1'
  &&stages.includes(ready.binding.stage)&&(!stage||ready.binding.stage===stage)&&(!source||ready.binding.operatorSourceCommit===source)&&/^[a-f0-9]{32}$/.test(ready.binding.nonce)
  &&hash(ready.inputSha256)&&ready.bindingSha256===sha(jsonBytes(ready.binding))&&jsonBytes(ready).length<=4096,'FIXTURE_CURRENT_CLOSED_READY_REQUIRED');bindingShape(ready.binding);return ready;
}
function pair(beforeBuilt,currentBuilt,binding){
 check(exact(currentBuilt,['input','receipt'])&&currentBuilt.input.label===label(binding.stage,binding.nonce)&&currentBuilt.receipt.label===currentBuilt.input.label,'FIXTURE_CURRENT_UNIQUE_LABEL_REQUIRED');
 const copy=structuredClone(currentBuilt);copy.input.label=beforeBuilt.input.label;copy.receipt.label=beforeBuilt.receipt.label;
 check(same(copy,beforeBuilt)&&sha(currentBuilt.input.sql)===binding.sqlSha256&&sha(currentBuilt.input.verification_sql)===binding.verificationSqlSha256,'FIXTURE_CURRENT_EXACT_SQL_RECEIPT_REQUIRED');
}
export function validateCurrentEvidence(record,{beforeBuilt,beforeInputBytes,beforeBytes,entry,entrySha256,expectedBinding=null,now=Date.now(),currentMaxAgeMs=30000}){
 check(currentMaxAgeMs===30000||currentMaxAgeMs===180000,'FIXTURE_CURRENT_FIXED_CLOCK_BOUND_REQUIRED');
 check(exact(record,['version','binding','inputText','nativeResultText','timing'])&&record.version==='DAILY_GENUINE_ROOT_CURRENT_READ_V1'&&typeof record.inputText==='string'&&typeof record.nativeResultText==='string'
  &&Buffer.byteLength(record.inputText)<=2e6&&Buffer.byteLength(record.nativeResultText)<=2e6,'FIXTURE_CURRENT_CLOSED_EVIDENCE_REQUIRED');
 const binding=assertCurrentBinding(record.binding,{beforeBuilt,beforeInputBytes,beforeBytes,entry,entrySha256});
 check(!expectedBinding||same(binding,expectedBinding),'FIXTURE_CURRENT_PENDING_BINDING_CHANGED');
 const t=record.timing;check(exact(t,['startedAt','finishedAt'])&&iso(t.startedAt)&&iso(t.finishedAt)&&Date.parse(t.startedAt)>=Date.parse(binding.readyAt)&&Date.parse(t.finishedAt)>=Date.parse(t.startedAt)&&Date.parse(t.finishedAt)<=now,'FIXTURE_CURRENT_ACTUAL_CALL_ORDER_REQUIRED');
 const built=JSON.parse(record.inputText);pair(beforeBuilt,built,binding);check(Buffer.from(record.inputText).equals(jsonBytes(built)),'FIXTURE_CURRENT_EXACT_CHILD_INPUT_BYTES_REQUIRED');
 const before=readEnvelope(JSON.parse(text(beforeBytes)),beforeBuilt.receipt,{now,maxAgeMs:180000}),result=JSON.parse(record.nativeResultText),frame=readEnvelope(result,built.receipt,{now,maxAgeMs:currentMaxAgeMs});
 check(sha(Buffer.from(record.nativeResultText))!==sha(beforeBytes),'FIXTURE_CURRENT_REUSED_BASELINE_REFUSED');
 for(const k of ['scope','facts','ids','catalogueSha256'])check(same(before[k],frame[k]),'FIXTURE_CURRENT_BASELINE_STATE_CHANGED');
 return {binding,built,result,frame,before,inputSha256:sha(Buffer.from(record.inputText)),envelopeSha256:sha(Buffer.from(record.nativeResultText))};
}
export function makeCurrentEvidence({binding,builtBytes,nativeBytes,timing,...baseline}){
 const record={version:'DAILY_GENUINE_ROOT_CURRENT_READ_V1',binding,inputText:text(builtBytes),nativeResultText:text(nativeBytes),timing};
 validateCurrentEvidence(record,{...baseline,expectedBinding:binding});return record;
}
export function currentAttempt(binding,evidenceSha256){
 check(hash(evidenceSha256),'FIXTURE_CURRENT_EVIDENCE_HASH_REQUIRED');return {entrySha256:binding.entrySha256,beforeEnvelopeSha256:binding.beforeEnvelopeSha256,currentBindingSha256:sha(jsonBytes(binding)),currentEvidenceSha256:evidenceSha256,currentNonce:binding.nonce};
}
export function attemptHash(binding,evidenceSha256){return sha(jsonBytes({version:'DAILY_GENUINE_ONESHOT_ATTEMPT_V1',stage:binding.stage,requestSha256:binding.requestSha256,...currentAttempt(binding,evidenceSha256)}));}
export function makeConsumption(binding,evidenceSha256,now=Date.now()){
 return {version:'DAILY_GENUINE_CURRENT_CONSUMPTION_V1',stage:binding.stage,nonce:binding.nonce,operatorSourceCommit:binding.operatorSourceCommit,currentBindingSha256:sha(jsonBytes(binding)),currentEvidenceSha256:evidenceSha256,attemptBindingSha256:attemptHash(binding,evidenceSha256),requestSha256:binding.requestSha256,checkedAt:new Date(now).toISOString(),boundary:'ELIGIBILITY_ONLY_BEFORE_ORIGINAL_ROUTE_FETCH'};
}
export function validateCurrentAfter({currentEvidenceBytes,consumptionBytes,beforeBuilt,beforeInputBytes,beforeBytes,entry,entrySha256,response,now=Date.now()}){
 const record=JSON.parse(text(currentEvidenceBytes)),evidenceSha256=sha(currentEvidenceBytes),v=validateCurrentEvidence(record,{beforeBuilt,beforeInputBytes,beforeBytes,entry,entrySha256,now,currentMaxAgeMs:180000}),c=JSON.parse(text(consumptionBytes));
 check(exact(c,Object.keys(makeConsumption(v.binding,evidenceSha256)))&&iso(c.checkedAt)&&same(c,makeConsumption(v.binding,evidenceSha256,Date.parse(c.checkedAt)))
  &&Date.parse(c.checkedAt)>=Date.parse(record.timing.finishedAt)&&Date.parse(c.checkedAt)<=now&&response&&iso(response.finishedAt)&&Date.parse(response.finishedAt)>=Date.parse(c.checkedAt),'FIXTURE_CURRENT_CONSUMPTION_REQUIRED');
 // At AFTER it may legitimately be >30s old. The exact native envelopes must have been current at consumption.
 readEnvelope(v.result,v.built.receipt,{now:Date.parse(c.checkedAt),maxAgeMs:30000});
 return {version:'DAILY_GENUINE_B2_CONSUMED_BINDING_V1',stage:v.binding.stage,operatorSourceCommit:v.binding.operatorSourceCommit,nonce:v.binding.nonce,identity:v.binding.identity,beforeEnvelopeSha256:v.binding.beforeEnvelopeSha256,entrySha256:v.binding.entrySha256,currentReadEvidence:currentFiles(v.binding.stage).evidence,currentConsumptionEvidence:currentFiles(v.binding.stage).consumptionEvidence,currentEvidenceSha256:evidenceSha256,currentEnvelopeSha256:v.envelopeSha256,consumptionSha256:sha(consumptionBytes),attemptBindingSha256:c.attemptBindingSha256,checkedAt:c.checkedAt};
}
export function assertCurrentPrefix(v,stage){
 check(exact(v,['version','stage','operatorSourceCommit','nonce','identity','beforeEnvelopeSha256','entrySha256','currentReadEvidence','currentConsumptionEvidence','currentEvidenceSha256','currentEnvelopeSha256','consumptionSha256','attemptBindingSha256','checkedAt'])
  &&v.version==='DAILY_GENUINE_B2_CONSUMED_BINDING_V1'&&v.stage===stage&&stages.includes(stage)&&/^[a-f0-9]{40}$/.test(v.operatorSourceCommit)&&/^[a-f0-9]{32}$/.test(v.nonce)
  &&['beforeEnvelopeSha256','entrySha256','currentEvidenceSha256','currentEnvelopeSha256','consumptionSha256','attemptBindingSha256'].every(k=>hash(v[k]))&&iso(v.checkedAt)
  &&v.currentReadEvidence===currentFiles(stage).evidence&&v.currentConsumptionEvidence===currentFiles(stage).consumptionEvidence,'FIXTURE_CURRENT_QUALIFIED_PREFIX_REQUIRED');
 const i=v.identity;check(exact(i,['physicalDatabase','databaseOid','backendPid','turnId','key','takenAt','task'])&&i.physicalDatabase==='cloudtms_test_clone'&&Number.isSafeInteger(i.databaseOid)&&i.databaseOid>0&&Number.isSafeInteger(i.backendPid)&&i.backendPid>0&&/^hr_turn_[a-f0-9]{32}$/.test(i.turnId)&&same(i.key,[20261013,1])&&iso(i.takenAt)&&i.task==='DAILY_GENUINE_'+stage+'_STAGED_ONLY','FIXTURE_CURRENT_PREFIX_TURN_REQUIRED');return v;
}
