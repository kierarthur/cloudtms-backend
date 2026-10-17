// Root-local bookkeeping only. Does not dispatch a worker, browser or native call.
export const stages=Object.freeze(['S0','S1','S1a','S2','S3','S4']);
export const publicBase='docs/healthroster-build/v6-owner-delivery-20261016/';
export const must=(v,c)=>{if(!v)throw Error(c);};
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const keys=(v,k)=>v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).sort().join('|')===[...k].sort().join('|');
export function publicPath(v){must(typeof v==='string'&&/^docs\/healthroster-build\/[A-Za-z0-9_./-]+$/.test(v)&&!v.split('/').includes('..')&&!/PRIVATE_BINDING|storage-state|QUALIFIED(?:\.json|\.next)|_ATTEMPT/.test(v),'BOOKKEEPER_EXACT_PUBLIC_EVIDENCE_REQUIRED');return v;}
export function exactPlan(plan,stage,source){
 must(keys(plan,[...stage==='S0'?[]:['currentReadEvidence','currentConsumptionEvidence'],'version','stage','operatorSourceCommit','qualifiedPrefix','previousEvidence','beforeEvidence','afterEvidence','completedEvidence','beforeEffectDecision','beforeReviewedEvidence','afterEffectDecision','afterReviewedEvidence']),'BOOKKEEPER_CLOSED_ROOT_PLAN_REQUIRED');
 must(plan.version===(stage==='S0'?'DAILY_GENUINE_ROOT_STAGE_BOOKKEEPER_PLAN_V1':'DAILY_GENUINE_ROOT_STAGE_BOOKKEEPER_PLAN_V2')&&plan.stage===stage&&plan.operatorSourceCommit===source&&stages.includes(stage),'BOOKKEEPER_ROOT_PLAN_IDENTITY_REQUIRED');
 if(stage!=='S0')must(plan.currentReadEvidence===publicBase+'DAILY_GENUINE_'+stage+'_CURRENT_READ_20261017.json'&&plan.currentConsumptionEvidence===publicBase+'DAILY_GENUINE_'+stage+'_CURRENT_CONSUMPTION_20261017.json','BOOKKEEPER_EXACT_CURRENT_PATHS_REQUIRED');
 const prefix=stages.slice(0,stages.indexOf(stage));must(same(plan.qualifiedPrefix,prefix),'BOOKKEEPER_EXACT_PRIOR_PREFIX_REQUIRED');
 must(prefix.length?typeof plan.previousEvidence==='string':plan.previousEvidence===null,'BOOKKEEPER_ACTUAL_PRIOR_EVIDENCE_REQUIRED');
 for(const k of ['beforeEvidence','afterEvidence','completedEvidence'])publicPath(plan[k]);if(prefix.length)publicPath(plan.previousEvidence);
 must(new Set([plan.beforeEvidence,plan.afterEvidence,plan.completedEvidence,...prefix.length?[plan.previousEvidence]:[]]).size===3+(prefix.length?1:0),'BOOKKEEPER_DISTINCT_EVIDENCE_REQUIRED');
 for(const k of ['beforeReviewedEvidence','afterReviewedEvidence'])must(Array.isArray(plan[k])&&plan[k].length===3&&plan[k].every(v=>publicPath(v)),'BOOKKEEPER_SEPARATE_ROOT_REVIEW_REQUIRED');
 must(plan.beforeEffectDecision==='ROOT_REVIEWED_EXACT_EXISTING_STAGE_EFFECTS','BOOKKEEPER_ROOT_EFFECT_REVIEW_NOT_PROVIDED');
 must(plan.afterEffectDecision===null||plan.afterEffectDecision==='ROOT_REVIEWED_ACTUAL_COMPLETE_AFTER_EFFECTS','BOOKKEEPER_ROOT_AFTER_REVIEW_NOT_PROVIDED');return plan;
}
export function previousWitness(plan,prior){
 if(!plan.qualifiedPrefix.length)return {qualified:[],after:null};
 must(prior?.version==='DAILY_GENUINE_PUBLIC_COMPLETED_PREFIX_V1'&&prior.operatorSourceCommit===plan.operatorSourceCommit&&same(prior.qualified,plan.qualifiedPrefix)&&prior.stage===plan.qualifiedPrefix.at(-1)
  &&prior.originalQualifyResult?.status==='PASS'&&prior.originalQualifyResult.code==='FIXTURE_STAGE_NATIVE_QUALIFIED'&&prior.originalQualifyResult.browserClosed===true&&prior.originalQualifyResult.phase==='qualify'&&prior.originalQualifyResult.stage===prior.stage
  &&prior.coordinatorReleased===true&&prior.afterFrame?.stage===prior.stage&&prior.afterFrame.phase==='AFTER','BOOKKEEPER_GENUINE_PRECEDING_PREFIX_REQUIRED');
 if(prior.stage!=='S0')must(prior.currentBefore?.version==='DAILY_GENUINE_B2_CONSUMED_BINDING_V1'&&prior.currentBefore.stage===prior.stage&&prior.currentBefore.operatorSourceCommit===plan.operatorSourceCommit,'BOOKKEEPER_CURRENT_PREFIX_REQUIRED');
 return {qualified:plan.qualifiedPrefix,after:prior.afterFrame};
}
export function beforeProof({stage,source,plan,prior,built,bytes,result,effect,readPublic,now},o){
 exactPlan(plan,stage,source);must(stage!=='S0','BOOKKEEPER_ORIGINAL_S0_OWNER_REQUIRED');
 must(built.receipt.stage===stage&&built.receipt.phase==='BEFORE','BOOKKEEPER_ACTUAL_BEFORE_PHASE_REQUIRED');
 const frame=o.readEnvelope(result,built.receipt,{now,maxAgeMs:30000}),previous=previousWitness(plan,prior);o.assertBefore(stage,frame,previous);
 must(effect.canonicalBeforeEvidence===plan.beforeEvidence,'BOOKKEEPER_EXACT_BEFORE_EVIDENCE_REQUIRED');
 o.assertEffectAdmission(effect,stage,frame,{sourceCommit:source});
 const refs=['nativeOwnerEvidence','schemaEffectEvidence','reviewRecord'].map(k=>effect[k]);must(same(refs,plan.beforeReviewedEvidence),'BOOKKEEPER_SEPARATE_ROOT_REVIEW_REQUIRED');for(const f of refs)must(readPublic(f).length>0,'BOOKKEEPER_ACTUAL_EFFECT_EVIDENCE_REQUIRED');
 return {version:'DAILY_GENUINE_STAGE_NATIVE_QUALIFICATION_V1',stage,phase:'BEFORE',operatorSourceCommit:source,validatedAt:new Date(now).toISOString(),actualFrame:frame,nativeResultSha256:o.sha(bytes),actualInputSqlSha256:built.receipt.sqlSha256,actualVerificationSqlSha256:built.receipt.verificationSqlSha256,actorBindingSha256:built.receipt.actorBindingSha256,accepted:result.accepted,rolledBack:result.rolledBack,connectedFresh:result.rollbackVerification.connectedFresh,reservedScopeCount:11,effectQualification:'ROOT_SUPPLIED_SEPARATELY_REVIEWED_NOT_INFERRED_FROM_HASH',originalChildQualification:'NOT_YET_RUN',fullCanonicalRestorationClaimed:false};
}
export function afterProof({stage,source,plan,prior,beforeBuilt,beforeBytes,beforeResult,built,bytes,result,response,afterEffect,readPublic,now,entry,entrySha256,beforeInputBytes,currentEvidenceBytes,consumptionBytes},o){
 exactPlan(plan,stage,source);must(stage!=='S0','BOOKKEEPER_ORIGINAL_S0_OWNER_REQUIRED');
 must(beforeBuilt.receipt.stage===stage&&beforeBuilt.receipt.phase==='BEFORE'&&built.receipt.stage===stage&&built.receipt.phase==='AFTER','BOOKKEEPER_EXACT_STAGE_PHASE_REQUIRED');
 must(same(beforeBuilt.receipt.identity,built.receipt.identity)&&beforeBuilt.receipt.actorBindingSha256===built.receipt.actorBindingSha256,'BOOKKEEPER_SAME_ACTUAL_TURN_ACTOR_REQUIRED');
 const before=o.readEnvelope(beforeResult,beforeBuilt.receipt,{now,maxAgeMs:180000}),after=o.readEnvelope(result,built.receipt,{now,maxAgeMs:30000});
 must(response.kind==='REAL_HTTP_RESULT'&&response.status===200&&response.projection!==null&&response.browserClosed===true,'BOOKKEEPER_UNCERTAIN_RESPONSE_RECONCILIATION_REQUIRED');
 must(Number.isFinite(Date.parse(response.finishedAt))&&Date.parse(after.observedAt)>=Date.parse(response.finishedAt),'BOOKKEEPER_ACTUAL_RESPONSE_AFTER_ORDER_REQUIRED');
 o.assertBefore(stage,before,previousWitness(plan,prior));o.assertAfter(stage,before,after,response.projection);
 must(plan.afterEffectDecision==='ROOT_REVIEWED_ACTUAL_COMPLETE_AFTER_EFFECTS','BOOKKEEPER_ROOT_AFTER_REVIEW_NOT_PROVIDED');
 must(afterEffect?.version==='DAILY_GENUINE_ROOT_AFTER_EFFECT_QUALIFICATION_V1'&&afterEffect.stage===stage&&afterEffect.beforeEnvelopeSha256===o.sha(beforeBytes)&&afterEffect.afterEnvelopeSha256===o.sha(bytes)&&afterEffect.responseProjectionSha256===o.sha(JSON.stringify(response.projection)),'BOOKKEEPER_COMPLETE_ROOT_AFTER_EFFECT_REQUIRED');
 const currentBefore=o.validateCurrentAfter({currentEvidenceBytes,consumptionBytes,beforeBuilt,beforeInputBytes,beforeBytes,entry,entrySha256,response,now});
 must(afterEffect.currentReadEvidence===plan.currentReadEvidence&&afterEffect.currentConsumptionEvidence===plan.currentConsumptionEvidence&&afterEffect.currentEvidenceSha256===o.sha(currentEvidenceBytes)&&afterEffect.currentConsumptionSha256===o.sha(consumptionBytes),'BOOKKEEPER_CURRENT_AFTER_EFFECT_REQUIRED');
 const refs=['canonicalAfterEvidence','exactApprovedChangesEvidence','backgroundAttributionEvidence'].map(k=>afterEffect[k]);
 must(refs[0]===plan.afterEvidence&&same(refs,plan.afterReviewedEvidence),'BOOKKEEPER_EXACT_AFTER_REVIEW_REQUIRED');
 // canonicalAfterEvidence is this actual proof, written once by this action.
 // The other two records must already contain root's substantive actual review.
 for(const f of refs.slice(1))must(readPublic(f).length>0,'BOOKKEEPER_ACTUAL_AFTER_EFFECT_EVIDENCE_REQUIRED');
 return {version:'DAILY_GENUINE_STAGE_NATIVE_QUALIFICATION_V1',stage,phase:'AFTER',operatorSourceCommit:source,validatedAt:new Date(now).toISOString(),actualFrame:after,nativeResultSha256:o.sha(bytes),actualInputSqlSha256:built.receipt.sqlSha256,actualVerificationSqlSha256:built.receipt.verificationSqlSha256,actorBindingSha256:built.receipt.actorBindingSha256,accepted:result.accepted,rolledBack:result.rolledBack,connectedFresh:result.rollbackVerification.connectedFresh,responseProjectionSha256:o.sha(JSON.stringify(response.projection)),beforeEnvelopeSha256:o.sha(beforeBytes),reservedScopeCount:11,effectQualification:'ROOT_SUPPLIED_SEPARATELY_REVIEWED_NOT_INFERRED_FROM_HASH',originalChildQualification:'PENDING_ORIGINAL_QUALIFY',currentBefore,fullCanonicalRestorationClaimed:false};
}
export function setupAppend({stage,source,plan,entry,beforeBuilt,beforeBytes,effectBytes,manifest,addition},o){
 exactPlan(plan,stage,source);must(stage!=='S0'&&entry.effectFile===publicBase+'DAILY_GENUINE_'+stage+'_EFFECT_ADMISSION_20261017.json'&&entry.entry.sourceCommit===source,'BOOKKEEPER_EXACT_ENTRY_REQUIRED');
 const expected=o.entryText(stage,entry.entry);must(addition===expected&&!manifest.includes(expected),'BOOKKEEPER_EXACT_GENERATED_SETUP_REQUIRED');
 o.assertSetupEntry(manifest+addition,entry.entry,stage,beforeBytes,beforeBuilt.receipt,effectBytes);return manifest+addition;
}
export function publishPrefix({stage,source,plan,prior,beforeProof:before,afterProof:after,qualify,summary,beforeBytes,afterBytes,response,currentEvidenceBytes,consumptionBytes,beforeBuilt,beforeInputBytes,entry,entrySha256},o){
 exactPlan(plan,stage,source);previousWitness(plan,prior);
 must(before.stage===stage&&before.phase==='BEFORE'&&after.stage===stage&&after.phase==='AFTER'&&before.operatorSourceCommit===source&&after.operatorSourceCommit===source&&[before,after].every(v=>v.accepted===true&&v.rolledBack===true&&v.connectedFresh===true),'BOOKKEEPER_REAL_COMMITTED_NATIVE_PROOFS_REQUIRED');
 must(before.nativeResultSha256===o.sha(beforeBytes)&&after.nativeResultSha256===o.sha(afterBytes)&&after.responseProjectionSha256===o.sha(JSON.stringify(response.projection)),'BOOKKEEPER_PUBLIC_NATIVE_BINDING_REQUIRED');
 must(keys(qualify,['status','code','phase','stage','browserClosed'])&&qualify.status==='PASS'&&qualify.code==='FIXTURE_STAGE_NATIVE_QUALIFIED'&&qualify.phase==='qualify'&&qualify.stage===stage&&qualify.browserClosed===true,'BOOKKEEPER_ORIGINAL_QUALIFY_PASS_REQUIRED');
 must(summary.stage===stage&&summary.operatorSourceCommit===source&&summary.status===(stage==='S0'?'S0_NATIVE_QUALIFIED_OWN_TURN_RELEASED':'GENUINE_STAGE_NATIVE_QUALIFIED_OWN_TURN_RELEASED')&&(stage==='S0'?summary.businessRequests===0:summary.oneBusinessRequestQualified===true),'BOOKKEEPER_ORIGINAL_RELEASED_SUMMARY_REQUIRED');
 must((stage==='S0'?response.kind==='NO_BUSINESS_REQUEST'&&same(response.projection,{}):response.kind==='REAL_HTTP_RESULT'&&response.status===200&&response.projection!==null)&&response.browserClosed===true,'BOOKKEEPER_UNCERTAIN_RESPONSE_RECONCILIATION_REQUIRED');
 must(same(before.actualFrame,JSON.parse(beforeBytes).rollbackVerification.statements[0].rows[0].fixture_read)&&same(after.actualFrame,JSON.parse(afterBytes).rollbackVerification.statements[0].rows[0].fixture_read),'BOOKKEEPER_ACTUAL_HISTORICAL_FRAME_REQUIRED');
 if(stage!=='S0'){
  o.assertCurrentPrefix(after.currentBefore,stage);
  const currentBefore=o.validateCurrentAfter({currentEvidenceBytes,consumptionBytes,beforeBuilt,beforeInputBytes,beforeBytes,entry,entrySha256,response,now:Date.parse(after.validatedAt)});
  must(after.currentBefore.operatorSourceCommit===source&&same(currentBefore,after.currentBefore),'BOOKKEEPER_CURRENT_PUBLISH_BINDING_REQUIRED');
 }
 return {version:'DAILY_GENUINE_PUBLIC_COMPLETED_PREFIX_V1',...stage==='S0'?{}:{currentBefore:after.currentBefore},stage,operatorSourceCommit:source,qualified:[...plan.qualifiedPrefix,stage],beforeEvidence:plan.beforeEvidence,afterEvidence:plan.afterEvidence,afterFrame:after.actualFrame,originalQualifyResult:qualify,coordinatorReleased:true,originalCoordinatorSummary:summary,beforeEnvelopeSha256:o.sha(beforeBytes),afterEnvelopeSha256:o.sha(afterBytes),responseProjectionSha256:o.sha(JSON.stringify(response.projection)),qualification:'ORIGINAL_CHILD_AND_ROOT_EFFECT_QUALIFIED_STAGE_ONLY_NOT_PUBLIC_FINANCIAL_SUCCESS',fullCanonicalRestorationClaimed:false};
}
