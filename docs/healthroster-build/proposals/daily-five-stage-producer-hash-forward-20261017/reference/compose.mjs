// Pure root plan composition. No files, Git, auth, private journal, native/API or mutation.
import {createHash} from 'node:crypto';
import {exactPlan,previousWitness,publicPath,publicBase} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-genuine-stage-root-bookkeeper-20261017/core.mjs';
import {assertBefore,assertAfter,assertEffectAdmission} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/stages.mjs';
const must=(v,c)=>{if(!v)throw Error('STAGE_PLAN_'+c)},same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const sha=b=>createHash('sha256').update(b).digest('hex'),stages=['S0','S1','S1a','S2','S3','S4'];
const json=v=>Buffer.from(JSON.stringify(v,null,2)+'\n'),parse=b=>{must(b instanceof Uint8Array&&b.byteLength>0&&b.byteLength<=2e6,'BOUNDED_COMMITTED_PUBLIC_BYTES_REQUIRED');return JSON.parse(new TextDecoder('utf8',{fatal:true}).decode(b));};
const keys=(v,k)=>v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).sort().join('|')===[...k].sort().join('|');
export const names=stage=>{must(stages.slice(1).includes(stage),'ORIGINAL_STAGE_REQUIRED');return{plan:publicBase+'DAILY_GENUINE_'+stage+'_R10_PLAN_20261017.json',before:publicBase+'DAILY_GENUINE_'+stage+'_R10_BEFORE_NATIVE_QUALIFICATION_20261017.json',after:publicBase+'DAILY_GENUINE_'+stage+'_R10_AFTER_NATIVE_QUALIFICATION_20261017.json',completed:publicBase+'DAILY_GENUINE_'+stage+'_R10_COMPLETED_PREFIX_20261017.json',changes:publicBase+'DAILY_GENUINE_'+stage+'_R10_ACTUAL_CHANGES_20261017.json',background:publicBase+'DAILY_GENUINE_'+stage+'_R10_BACKGROUND_OBSERVATIONS_20261017.json',predecision:publicBase+'DAILY_GENUINE_'+stage+'_R10_PREDECISION_20261017.json'};};
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
 must(keys(decision,['version','stage','operatorSourceCommit','decision','nativeOwnerEvidence','schemaEffectEvidence','sourceEvidence','expectedEffects','reconciliation','limitations'])
  &&decision.version==='DAILY_GENUINE_ROOT_STAGE_PREDECISION_V1'&&decision.stage===stage&&decision.operatorSourceCommit===source&&decision.decision==='ROOT_REVIEWED_EXACT_EXISTING_STAGE_EFFECTS','MANUAL_ROOT_PREDECISION_REQUIRED');
 const refs=[decision.nativeOwnerEvidence,decision.schemaEffectEvidence,...(Array.isArray(decision.sourceEvidence)?decision.sourceEvidence:[])];
 must(Array.isArray(decision.sourceEvidence)&&decision.sourceEvidence.length>0&&new Set(refs).size===refs.length,'SEPARATE_SUBSTANTIVE_EVIDENCE_REQUIRED');
 for(const f of refs){publicPath(f);must(f!==n.predecision&&f!==n.plan,'NO_SELF_EVIDENCE_REQUIRED');const b=readPublic(f);must(b instanceof Uint8Array&&b.byteLength>0&&b.byteLength<=2e6,'SUBSTANTIVE_PUBLIC_EVIDENCE_REQUIRED');}
 for(const k of['expectedEffects','reconciliation','limitations'])must(typeof decision[k]==='string'&&decision[k].trim().length>=20&&!/\b(?:NOT_PROVIDED|TODO|PLACEHOLDER)\b/.test(decision[k]),'MANUAL_SUBSTANTIVE_REVIEW_REQUIRED');
 const index=stages.indexOf(stage),prior=index===1?s0PrefixFile:names(stages[index-1]).completed;
 const plan={version:'DAILY_GENUINE_ROOT_STAGE_BOOKKEEPER_PLAN_V1',stage,operatorSourceCommit:source,qualifiedPrefix:stages.slice(0,index),previousEvidence:prior,beforeEvidence:n.before,afterEvidence:n.after,completedEvidence:n.completed,beforeEffectDecision:decision.decision,beforeReviewedEvidence:[decision.nativeOwnerEvidence,decision.schemaEffectEvidence,n.predecision],afterEffectDecision:null,afterReviewedEvidence:[n.after,n.changes,n.background]};
 exactPlan(plan,stage,source);
 const admission={version:'DAILY_GENUINE_ROOT_EFFECT_ADMISSION_V1',stage,operatorSourceCommit:source,catalogueSha256:prefix.afterFrame.catalogueSha256,workerSourceSha256:effect.workerSourceSha256,nativeOwnerEvidence:decision.nativeOwnerEvidence,schemaEffectEvidence:decision.schemaEffectEvidence,canonicalBeforeEvidence:n.before,reviewRecord:n.predecision};
 assertEffectAdmission(admission,stage,prefix.afterFrame,{sourceCommit:source});
 // Future paths are references only. No earlier stage or after result is manufactured.
 return{records:[{file:n.plan,bytes:json(plan)},{file:publicBase+'DAILY_GENUINE_'+stage+'_EFFECT_ADMISSION_20261017.json',bytes:json(admission)}],source,stage,s0PrefixSha256:sha(prefixBytes),rootPredecisionSha256:sha(readPublic(n.predecision)),priorPrefixCreated:false,afterDecisionProvided:false,financialSuccessClaimed:false};
}
export function composeAll(a){
 const all=stages.slice(1).map(stage=>composeStage({...a,stage})),records=all.flatMap(v=>v.records);
 must(records.length===10&&new Set(records.map(r=>r.file)).size===10,'EXACT_TEN_RECORDS_REQUIRED');return{records,stages:all};
}

