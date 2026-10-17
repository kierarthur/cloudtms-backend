import fs from 'node:fs';import cp from 'node:child_process';import crypto from 'node:crypto';import {pathToFileURL} from 'node:url';
const i='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',r='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026',p='docs/healthroster-build/v6-owner-delivery-20261016/',s='docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/',phase=process.env.HR_ROOT_PHASE,out=process.env.HR_ROOT_OUT;
if(!/^D:\/Development\/GitHub\/\.codex-worktrees\/healthroster-install2-14102026\/\.codex-tmp\/daily-genuine-stages-20261017-[a-f0-9]{32}$/.test(out??''))throw Error('OWNED_PATH');
const g=(root,...a)=>cp.execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','core.autocrlf=false','-c','core.longpaths=true','-C',root,...a],{maxBuffer:32e6,stdio:['ignore','pipe','pipe']});
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),read=f=>fs.readFileSync(out+'/'+f),imp=f=>import(pathToFileURL(r+'/'+s+f).href),source=g(r,'rev-parse','HEAD').toString().trim();
if(!/^[a-f0-9]{40}$/.test(process.env.HR_OPERATOR_COMMIT??'')||source!==process.env.HR_OPERATOR_COMMIT)throw Error('FIXED_REPORT_HEAD');
if(!['BEFORE','SETUP','AFTER'].includes(phase))throw Error('EXACT_ROOT_PHASE_REQUIRED');
if(g(i,'diff','--cached','--name-only','-z').length!==0)throw Error('UNRELATED_STAGED_PATHS');
const files=[],write=(f,v)=>{
 if(f===p+'DAILY_GENUINE_S0_EFFECT_ADMISSION_20261017.json'){
  const old=fs.readFileSync(i+'/'+f);
  if(sha(old)!=='4e72e9f72bb8888b15cd51edd947242aa9e25789957e67b4455b0c8d5d4f6bfd'||![g(i,'show','HEAD:'+f),g(i,'show',':'+f),g(i,'show','ccdf098eb7720f282ee1309ad95dc42a26927652:'+f)].every(b=>b.equals(old)))throw Error('EXACT_R4_EFFECT_PARENT');
  fs.writeFileSync(i+'/'+f,JSON.stringify(v,null,2)+'\n');
 }else fs.writeFileSync(i+'/'+f,JSON.stringify(v,null,2)+'\n',{flag:'wx'});
 files.push(f);
};
if(phase==='SETUP'){
 const file='docs/healthroster-build/TEST_SETUP_MANIFEST.md',old=fs.readFileSync(i+'/'+file),entry=read('S0_SETUP_ENTRY.md');
 if(old.includes(entry)||!old.equals(g(i,'show','HEAD:'+file))||!old.equals(g(i,'show',':'+file)))throw Error('MANIFEST');
 fs.appendFileSync(i+'/'+file,entry);if(!fs.readFileSync(i+'/'+file).subarray(0,old.length).equals(old))throw Error('PREFIX');files.push(file);
}else{
 const {readEnvelope}=await imp('native-read.mjs'),{assertBefore,assertAfter}=await imp('stages.mjs');
 const built=JSON.parse(read('S0_'+phase+'_INPUT.json')),bytes=read('S0_'+phase+'_NATIVE.json'),result=JSON.parse(bytes),frame=readEnvelope(result,built.receipt);
 if(built.receipt.stage!=='S0'||built.receipt.phase!==phase)throw Error('EXACT_S0_NATIVE_PHASE_REQUIRED');
 assertBefore('S0',frame,{qualified:[]});
 for(const v of Object.values(frame.scope))if(v.count!==0||v.sha256!==sha('[]'))throw Error('VACANCY');
 const f=p+'DAILY_GENUINE_S0_R5_'+phase+'_NATIVE_QUALIFICATION_20261017.json';
 const evidence={version:'DAILY_GENUINE_S0_'+phase+'_NATIVE_QUALIFICATION_V1',stage:'S0',phase,operatorSourceCommit:source,actualFrame:frame,nativeResultSha256:sha(bytes),actualInputSqlSha256:built.receipt.sqlSha256,actualVerificationSqlSha256:built.receipt.verificationSqlSha256,accepted:result.accepted,rolledBack:result.rolledBack,connectedFresh:result.rollbackVerification.connectedFresh,reservedScopeCount:11,reservedScopesVacant:true,businessRequests:0,fullCanonicalRestorationClaimed:false,globalWeakMapClaimed:false};
 if(phase==='BEFORE'){
  write(f,evidence);write(p+'DAILY_GENUINE_S0_EFFECT_ADMISSION_20261017.json',{version:'DAILY_GENUINE_ROOT_EFFECT_ADMISSION_V1',stage:'S0',operatorSourceCommit:source,catalogueSha256:frame.catalogueSha256,workerSourceSha256:sha(g(i,'show','f743b81fe537830d81b1de93ec4f9cb0331532a7:broker/src/index.js')),nativeOwnerEvidence:p+'DAILY_S0_CATALOGUE_ACTUAL_ROOT_QUALIFICATION_20261017.json',schemaEffectEvidence:p+'DAILY_GENUINE_S0_R5_ROOT_EXECUTION_PLAN_20261017.json',canonicalBeforeEvidence:f,reviewRecord:p+'DAILY_AUTH_R8_SOURCE_ROOT_REVIEW_20261017.json'});
 }else if(phase==='AFTER'){
  const beforeBytes=read('S0_BEFORE_NATIVE.json'),b=JSON.parse(read('S0_BEFORE_INPUT.json')),before=readEnvelope(JSON.parse(beforeBytes),b.receipt,{maxAgeMs:180000}),response=JSON.parse(read('S0_RESPONSE.json'));
  if(b.receipt.stage!=='S0'||b.receipt.phase!=='BEFORE')throw Error('EXACT_S0_BEFORE_RECEIPT_REQUIRED');
  if(JSON.stringify(b.receipt.identity)!==JSON.stringify(built.receipt.identity)||response.kind!=='NO_BUSINESS_REQUEST'||response.browserClosed!==true||JSON.stringify(response.projection)!=='{}'||Date.parse(frame.observedAt)<Date.parse(response.finishedAt))throw Error('AFTER_ORDER');
  assertBefore('S0',before,{qualified:[]});assertAfter('S0',before,frame,response.projection);
  write(f,{...evidence,responseProjectionSha256:sha(JSON.stringify(response.projection)),exactApprovedChanges:[],observedReservedScopeChanges:0,backgroundAttribution:'No business request forwarded; all eleven reserved scopes and catalogue unchanged. No assertion about unread unrelated background rows.'});
  write(p+'DAILY_GENUINE_S0_AFTER_EFFECT_QUALIFICATION_20261017.json',{version:'DAILY_GENUINE_ROOT_AFTER_EFFECT_QUALIFICATION_V1',stage:'S0',beforeEnvelopeSha256:sha(beforeBytes),afterEnvelopeSha256:sha(bytes),responseProjectionSha256:sha(JSON.stringify(response.projection)),canonicalAfterEvidence:f,exactApprovedChangesEvidence:f,backgroundAttributionEvidence:f});
 }else throw Error('PHASE');
}
g(i,'add','--',...files);for(const f of files)if(!fs.readFileSync(i+'/'+f).equals(g(i,'show',':'+f)))throw Error('INDEX');g(i,'diff','--cached','--check');const staged=g(i,'diff','--cached','--name-only','-z').toString().split('\0').filter(Boolean).sort();if(JSON.stringify(staged)!==JSON.stringify([...files].sort()))throw Error('EXACT_OWNED_INDEX_REQUIRED');g(i,'commit','-m','Record actual S0 '+phase+' qualification','--only','--',...files);g(i,'push','origin','HEAD:refs/heads/codex/healthroster-install2-14102026');
const commit=g(i,'rev-parse','HEAD').toString().trim();if(g(i,'ls-remote','origin','refs/heads/codex/healthroster-install2-14102026').toString().split(/\s/)[0]!==commit)throw Error('REMOTE');
console.log(JSON.stringify({status:'ROOT_ACTUAL_'+phase+'_COMMITTED',commit,files}));
