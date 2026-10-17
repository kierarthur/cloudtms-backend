// Pure root plan composition. No files, Git, auth, private journal, native/API or mutation.
import {createHash} from 'node:crypto';
import {currentFiles} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/current-read.mjs';
import {exactPlan,previousWitness,publicPath,publicBase} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-genuine-stage-root-bookkeeper-20261017/core.mjs';
import {assertBefore,assertAfter,assertEffectAdmission} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/stages.mjs';
const must=(v,c)=>{if(!v)throw Error('STAGE_PLAN_'+c)},same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const sha=b=>createHash('sha256').update(b).digest('hex'),stages=['S0','S1','S1a','S2','S3','S4'];
const json=v=>Buffer.from(JSON.stringify(v,null,2)+'\n'),parse=b=>{must(b instanceof Uint8Array&&b.byteLength>0&&b.byteLength<=2e6,'BOUNDED_COMMITTED_PUBLIC_BYTES_REQUIRED');return JSON.parse(new TextDecoder('utf8',{fatal:true}).decode(b));};
const keys=(v,k)=>v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).sort().join('|')===[...k].sort().join('|');
export const names=stage=>{must(stages.slice(1).includes(stage),'ORIGINAL_STAGE_REQUIRED');return{plan:publicBase+'DAILY_GENUINE_'+stage+'_B2_PLAN_20261017.json',before:publicBase+'DAILY_GENUINE_'+stage+'_B2_BEFORE_NATIVE_QUALIFICATION_20261017.json',after:publicBase+'DAILY_GENUINE_'+stage+'_B2_AFTER_NATIVE_QUALIFICATION_20261017.json',completed:publicBase+'DAILY_GENUINE_'+stage+'_B2_COMPLETED_PREFIX_20261017.json',changes:publicBase+'DAILY_GENUINE_'+stage+'_B2_ACTUAL_CHANGES_20261017.json',background:publicBase+'DAILY_GENUINE_'+stage+'_B2_BACKGROUND_OBSERVATIONS_20261017.json',predecision:publicBase+'DAILY_GENUINE_'+stage+'_B2_PREDECISION_20261017.json'};};
export function composeStage({stage,source,s0PrefixFile,readPublic}){
 must(stages.slice(1).includes(stage)&&/^[a-f0-9]{40}$/.test(source??'')&&typeof readPublic==='function','ACTUAL_ADMITTED_SOURCE_AND_READER_REQUIRED');
 // The caller must supply the original committed reader under its real source/HEAD guard.
 // Historical public S0 is read here; no private QUALIFIED journal is opened or emitted.
 publicPath(s0PrefixFile);const prefixBytes=readPublic(s0PrefixFile),prefix=parse(prefixBytes);
 previousWitness({qualifiedPrefix:['S0'],operatorSourceCommit:source},prefix);
 must(prefix.beforeEvidence!==prefix.afterEvidence&&prefix.stage==='S0'&&prefix.qualified.length===1&&same(prefix.qualified,['S0']),'GENUINE_NEW_S0_PREFIX_REQUIRED');
 for(const f of[prefix.beforeEvidence,prefix.afterEvidence])publicPath(f);
 const before=parse(readPublic(prefix.beforeEvidence)),after=parse(readPublic(prefix.afterEvidence));
 must(before.stage==='S0'&&before.phase==='BEFORE'&&after.stage==='S0'&&after.phase==='AFTER'&&[before,after].every(v=>v.operatorSourceCommit===source&&v.accepted===true&&v.rolledBack===true&&v.connectedFresh===true)&&before.nativeResultSha256===prefix.beforeEnvelopeSha256&&after.nativeResultSha256===prefix.afterEnvelopeSha256&&same(after.actualFrame,prefix.afterFrame),'ACTUAL_COMMITTED_S0_PROOFS_REQUIRED');
 assertBefore('S0',before.actualFrame,{qualified:[]});assertAfter('S0',before.actualFrame,after.actualFrame,{});
 const effect=parse(readPublic(publicBase+'DAILY_GENUINE_S0_EFFECT_ADMISSION_20261017.json'));
 assertEffectAdmission(effect,'S0',prefix.afterFrame,{sourceCommit:source});
 must(effect.canonicalBeforeEvidence===prefix.beforeEvidence,'S0_EFFECT_SOURCE_AND_CATALOGUE_REQUIRED');
 const n=names(stage),decision=parse(readPublic(n.predecision));
 must(keys(decision,['version','stage','operatorSourceCommit','decision','workerSourceSha256','nativeOwnerEvidence','schemaEffectEvidence','sourceEvidence','expectedEffects','reconciliation','limitations'])
  &&decision.version==='DAILY_GENUINE_ROOT_STAGE_PREDECISION_V1'&&decision.stage===stage&&decision.operatorSourceCommit===source&&decision.decision==='ROOT_REVIEWED_EXACT_EXISTING_STAGE_EFFECTS','MANUAL_ROOT_PREDECISION_REQUIRED');
 // The S0 hash identifies its own read producer; it is never inherited by a business stage.
 const bindingFile=stage==='S1a'?'docs/healthroster-build/proposals/daily-health-genuine-hospital-mapping-stage-20261017/SOURCE_PINS.json':publicBase+'DAILY_PRODUCERS_ACTIVE_RUNTIME_SOURCE_BINDING_20261017.json';
 must(/^[a-f0-9]{64}$/.test(decision.workerSourceSha256??'')&&Array.isArray(decision.sourceEvidence)&&decision.sourceEvidence.includes(bindingFile),'MANUAL_STAGE_PRODUCER_HASH_AND_BINDING_REQUIRED');
 const binding=parse(readPublic(bindingFile));let producer;
 if(stage==='S1a'){
  must(binding.version==='DAILY_GENUINE_S1A_CURRENT_PUBLISHED_SOURCE_PINS_V1'&&binding.route?.file==='reference/worker/hospital-route.js.txt'&&binding.route.rawPublishedEqual===true&&binding.route.sha256===binding.route.publishedSha256,'ACTUAL_HOSPITAL_ROUTE_BINDING_REQUIRED');producer=binding.route.sha256;
 }else{
  const name={S1:'handleCreateClient',S2:'handleCreateCandidate',S3:'handleUpsertClientRate',S4:'handleTimesheetCreateManualDaily'}[stage],matches=binding.selectedFunctions?.filter(v=>v.name===name);
  must(binding.version==='DAILY_PRODUCERS_CURRENT_ACTIVE_RUNTIME_SOURCE_BINDING_V1'&&binding.allSelectedFunctionsRawEqualPublishedOnce===true&&matches?.length===1&&matches[0].exactPublishedOccurrences===1,'ACTUAL_STAGE_PRODUCER_BINDING_REQUIRED');producer=matches[0].sha256;
 }
 must(decision.workerSourceSha256===producer,'ACTUAL_STAGE_PRODUCER_HASH_MISMATCH');
 const refs=[decision.nativeOwnerEvidence,decision.schemaEffectEvidence,...(Array.isArray(decision.sourceEvidence)?decision.sourceEvidence:[])];
 must(Array.isArray(decision.sourceEvidence)&&decision.sourceEvidence.length>0&&new Set(refs).size===refs.length,'SEPARATE_SUBSTANTIVE_EVIDENCE_REQUIRED');
 for(const f of refs){publicPath(f);must(f!==n.predecision&&f!==n.plan,'NO_SELF_EVIDENCE_REQUIRED');const b=readPublic(f);must(b instanceof Uint8Array&&b.byteLength>0&&b.byteLength<=2e6,'SUBSTANTIVE_PUBLIC_EVIDENCE_REQUIRED');}
 for(const k of['expectedEffects','reconciliation','limitations'])must(typeof decision[k]==='string'&&decision[k].trim().length>=20&&!/\b(?:NOT_PROVIDED|TODO|PLACEHOLDER)\b/.test(decision[k]),'MANUAL_SUBSTANTIVE_REVIEW_REQUIRED');
 const index=stages.indexOf(stage),prior=index===1?s0PrefixFile:names(stages[index-1]).completed;
 const plan={version:'DAILY_GENUINE_ROOT_STAGE_BOOKKEEPER_PLAN_V2',stage,currentReadEvidence:currentFiles(stage).evidence,currentConsumptionEvidence:currentFiles(stage).consumptionEvidence,operatorSourceCommit:source,qualifiedPrefix:stages.slice(0,index),previousEvidence:prior,beforeEvidence:n.before,afterEvidence:n.after,completedEvidence:n.completed,beforeEffectDecision:decision.decision,beforeReviewedEvidence:[decision.nativeOwnerEvidence,decision.schemaEffectEvidence,n.predecision],afterEffectDecision:null,afterReviewedEvidence:[n.after,n.changes,n.background]};
 exactPlan(plan,stage,source);
 const admission={version:'DAILY_GENUINE_ROOT_EFFECT_ADMISSION_V1',stage,operatorSourceCommit:source,catalogueSha256:prefix.afterFrame.catalogueSha256,workerSourceSha256:decision.workerSourceSha256,nativeOwnerEvidence:decision.nativeOwnerEvidence,schemaEffectEvidence:decision.schemaEffectEvidence,canonicalBeforeEvidence:n.before,reviewRecord:n.predecision};
 assertEffectAdmission(admission,stage,prefix.afterFrame,{sourceCommit:source});
 // Future paths are references only. No earlier stage or after result is manufactured.
 return{records:[{file:n.plan,bytes:json(plan)},{file:publicBase+'DAILY_GENUINE_'+stage+'_EFFECT_ADMISSION_20261017.json',bytes:json(admission)}],source,stage,s0PrefixSha256:sha(prefixBytes),rootPredecisionSha256:sha(readPublic(n.predecision)),priorPrefixCreated:false,afterDecisionProvided:false,financialSuccessClaimed:false};
}
export function composeAll(a){
 const all=stages.slice(1).map(stage=>composeStage({...a,stage})),records=all.flatMap(v=>v.records);
 must(records.length===10&&new Set(records.map(r=>r.file)).size===10,'EXACT_TEN_RECORDS_REQUIRED');return{records,stages:all};
}

