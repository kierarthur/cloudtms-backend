import fs from 'node:fs';
import path from 'node:path';
import {check,exact,sha,STAGES,nextStage,requestSha,entryText} from './contract.mjs';
import {scopes} from './native-read.mjs';
import {assertCurrentPrefix} from './current-read.mjs';
export function assertBefore(stage,before,previous){
 const index=STAGES.indexOf(stage);check(index>=0,'FIXTURE_STAGE_REQUIRED');
 const qualified=previous?.qualified??[];check(nextStage(qualified)===stage,'FIXTURE_STAGE_ORDER_REFUSED');
 const expected={clients:index>=2?1:0,settings:index>=2?1:0,mappings:index>=3?1:0,candidates:index>=4?1:0,candidateJobs:0,rates:index>=5?1:0,overrides:0,timesheets:0,financials:0,outbox:0,audit:index>=2?1:0};
 // S0 and S1 both require complete reserved-scope vacancy. S0 never performs a write.
 for(const k of scopes)check(before.scope[k].count===expected[k],'FIXTURE_VACANCY_OR_PREFIX_REFUSED');
 if(index>=2)check(before.facts.settings===true,'FIXTURE_RETAINED_SETTINGS_REFUSED');
 if(index>=3)check(before.facts.clientMapping===true,'FIXTURE_GENUINE_CLIENT_MAPPING_REQUIRED');
 if(index>=4)check(before.facts.candidate===true,'FIXTURE_RETAINED_CANDIDATE_REFUSED');
 if(index>=5)check(before.facts.rate===true,'FIXTURE_RETAINED_RATE_REFUSED');
 if(previous?.after){
  check(previous.after.catalogueSha256===before.catalogueSha256,'FIXTURE_NATIVE_METADATA_DRIFT');
  for(const k of scopes)check(JSON.stringify(previous.after.scope[k])===JSON.stringify(before.scope[k]),'FIXTURE_INTERSTAGE_CHANGE_REFUSED');
 }
 return true;
}
export function assertAfter(stage,before,after,projection){
 const allowed={S0:[],S1:['clients','settings','audit'],S1a:['mappings'],S2:['candidates'],S3:['rates'],S4:['timesheets','financials','audit']}[stage];
 check(allowed&&before.catalogueSha256===after.catalogueSha256,'FIXTURE_NATIVE_METADATA_DRIFT');
 for(const k of scopes)if(!allowed.includes(k))check(JSON.stringify(before.scope[k])===JSON.stringify(after.scope[k]),'FIXTURE_UNAPPROVED_SCOPE_CHANGE');
 const one=(k,fact)=>check(after.scope[k].count===1&&after.facts[fact]===true,'FIXTURE_NATIVE_STAGE_READBACK_REFUSED');
 if(stage==='S1'){
  check(after.scope.clients.count===1&&after.scope.audit.count===1&&after.facts.audit===true,'FIXTURE_NATIVE_CLIENT_REQUIRED');one('settings','settings');
  check(JSON.stringify(after.ids.settings)==JSON.stringify([projection.settingsId]),'FIXTURE_SETTINGS_ID_MISMATCH');
 }
 if(stage==='S1a')one('mappings','clientMapping');
 if(stage==='S2')one('candidates','candidate');
 if(stage==='S3'){
  one('rates','rate');check(JSON.stringify(after.ids.rates)===JSON.stringify([projection.rateId]),'FIXTURE_RATE_ID_MISMATCH');
 }
 if(stage==='S4'){
  one('timesheets','timesheet');one('financials','financial');
  check(JSON.stringify(after.ids.timesheets)===JSON.stringify([projection.timesheetId])&&after.ids.financials?.length===1,'FIXTURE_GENERATED_ID_MISMATCH');
  check(after.scope.outbox.count===0&&[1,2].includes(after.scope.audit.count)&&after.facts.audit===true,'FIXTURE_NATIVE_OUTBOX_OR_AUDIT_REFUSED');
 }
 return true;
}
// Each append is bound to the exact committed root source, observed before envelope and reviewed effect dossier.
// This is a local operator protocol, never a replacement native receipt or product ABI.
export function assertSetupEntry(manifestText,entry,stage,beforeEnvelope,beforeReceipt,effectBytes){
 check(exact(entry,['sourceCommit','beforeSqlSha256','verificationSqlSha256','beforeEnvelopeSha256','effectAdmissionSha256']),'FIXTURE_EXACT_ENTRY_BINDING_REQUIRED');
 check(entry.beforeSqlSha256===beforeReceipt.sqlSha256&&entry.verificationSqlSha256===beforeReceipt.verificationSqlSha256
  &&entry.beforeEnvelopeSha256===sha(beforeEnvelope)&&entry.effectAdmissionSha256===sha(effectBytes),'FIXTURE_MANIFEST_NATIVE_SOURCE_MISMATCH');
 const text=entryText(stage,entry);check(manifestText.split(text).length===2,'FIXTURE_EXACT_COMMITTED_ENTRY_REQUIRED');
 return sha(text);
}
export function assertEffectAdmission(effect,stage,before,entry){
 // Root must author and commit this dossier after native/source/deployed-owner qualification.
 // The historical audit explicitly cannot supply it. A metadata hash alone is insufficient.
 check(exact(effect,['version','stage','operatorSourceCommit','catalogueSha256','workerSourceSha256','nativeOwnerEvidence','schemaEffectEvidence','canonicalBeforeEvidence','reviewRecord'])
  &&effect.version==='DAILY_GENUINE_ROOT_EFFECT_ADMISSION_V1'&&effect.stage===stage&&effect.operatorSourceCommit===entry.sourceCommit
  &&effect.catalogueSha256===before.catalogueSha256&&/^[0-9a-f]{64}$/.test(effect.workerSourceSha256),'FIXTURE_CURRENT_EFFECT_ADMISSION_REQUIRED');
 for(const k of ['nativeOwnerEvidence','schemaEffectEvidence','canonicalBeforeEvidence','reviewRecord'])
  check(typeof effect[k]==='string'&&/^docs\/healthroster-build\/[A-Za-z0-9_./-]+$/.test(effect[k])&&!effect[k].split('/').includes('..'),'FIXTURE_ACTUAL_ROOT_EVIDENCE_REQUIRED');
 return effect;
}
export function acquireSession(directory){
 const lock=path.join(directory,'OPERATOR_EXCLUSIVE.lock');
 let handle;try{handle=fs.openSync(lock,'wx',0o600);}catch{throw Error('FIXTURE_EXCLUSIVE_SESSION_HELD');}
 fs.writeSync(handle,'OWNED_STAGED_OPERATOR\n');fs.fsyncSync(handle);
 let closed=false;return ()=>{if(!closed){closed=true;fs.closeSync(handle);fs.unlinkSync(lock);}};
}
export function readJournal(directory){
 const file=path.join(directory,'QUALIFIED.json');if(!fs.existsSync(file))return {qualified:[],after:null,generated:{}};
 const j=JSON.parse(fs.readFileSync(file,'utf8'));check(Array.isArray(j.qualified)&&j.qualified.every((s,i)=>s===STAGES[i]),'FIXTURE_QUALIFIED_PREFIX_REQUIRED');if(j.qualified.length>1)assertCurrentPrefix(j.currentBefore,j.qualified.at(-1));return j;
}
export function persistAttempt(directory,stage,binding){
 // A file exists before network dispatch. It is NEVER removed, including after 4xx/500/timeout/kill.
 for(const s of STAGES)if(fs.existsSync(path.join(directory,s+'_ATTEMPT.json'))&&!fs.existsSync(path.join(directory,s+'_QUALIFIED.json')))
  throw Error('FIXTURE_UNRESOLVED_ATTEMPT_RECONCILIATION_REQUIRED');
 const file=path.join(directory,stage+'_ATTEMPT.json');check(!fs.existsSync(file),'FIXTURE_STAGE_ALREADY_ATTEMPTED');
 const fd=fs.openSync(file,'wx',0o600);
 try{fs.writeSync(fd,JSON.stringify({version:'DAILY_GENUINE_ONESHOT_ATTEMPT_V1',stage,requestSha256:stage==='S0'?null:requestSha(stage),...binding})+'\n');fs.fsyncSync(fd);}finally{fs.closeSync(fd);}
 return file;
}
export function writeClosed(directory,name,value){
 const file=path.join(directory,name);const fd=fs.openSync(file,'wx',0o600);
 try{fs.writeSync(fd,JSON.stringify(value)+'\n');fs.fsyncSync(fd);}finally{fs.closeSync(fd);}
}
export function saveQualification(directory,stage,before,after,projection,previous,currentBefore=null){
 if(stage!=='S0')assertCurrentPrefix(currentBefore,stage);
 assertAfter(stage,before,after,projection);
 const qualified=[...previous.qualified,stage];check(qualified.every((s,i)=>s===STAGES[i]),'FIXTURE_STAGE_ORDER_REFUSED');
 writeClosed(directory,stage+'_QUALIFIED.json',{stage,after,projection,...stage==='S0'?{}:{currentBefore}});
 const value={qualified,after,generated:{...previous.generated,...projection},...stage==='S0'?{}:{currentBefore}};
 const next=path.join(directory,'QUALIFIED.next');check(!fs.existsSync(next),'FIXTURE_JOURNAL_TEMP_EXISTS');
 const fd=fs.openSync(next,'wx',0o600);try{fs.writeSync(fd,JSON.stringify(value)+'\n');fs.fsyncSync(fd);}finally{fs.closeSync(fd);}
 fs.renameSync(next,path.join(directory,'QUALIFIED.json'));
 return value;
}
