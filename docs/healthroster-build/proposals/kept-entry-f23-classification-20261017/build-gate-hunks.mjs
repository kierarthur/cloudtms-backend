// Produces an exact finite forward/reverse recipe; never edits the integration.
import fs from'node:fs';import assert from'node:assert/strict';import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';import{fileURLToPath}from'node:url';import path from'node:path';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',target='scripts/bpay-next-source-gates.mjs',before=fs.readFileSync(root+'/'+target,'utf8'),sha=x=>createHash('sha256').update(x).digest('hex');
const recipe=[];let after=before;
function change(before,afterText,purpose){assert.equal(after.split(before).length,2);after=after.replace(before,()=>afterText);recipe.push({before,after:afterText,purpose});}
change("import {reviewedSourceClassifierDelegates} from './healthroster-source-classifier-delegation-review.mjs';","import {createReviewedKeptEntryClassifier} from './healthroster-kept-entry-type-review.mjs';\nimport {reviewedSourceClassifierDelegates} from './healthroster-source-classifier-delegation-review.mjs';",'Add one finite source classifier.');
change("import {reviewedHealthRowtypeHazards} from './healthroster-rowtype-type-review.mjs';","import {reviewedHealthRowtypeHazards,provesSingleTableTuple} from './healthroster-rowtype-type-review.mjs';",'Reuse the actual original same-table tuple proof.');
const initializer=`const reviewedKeptEntryHazards=createReviewedKeptEntryClassifier(repoRoot,{
  tupleProof:provesSingleTableTuple,hazardCensus:specialConstructViolations,
  classifyPrior:(source,file,initial=new Map())=>{
    const violations=specialConstructViolations(source,file),parts=executableParts(source,file);
    let result=new Map(initial);
    for(const review of [reviewedHealthRowtypeHazards,reviewedHealthUuidHazards,reviewedHealthAggregateHazards,reviewedJoinedNonDynamicHazards,reviewedHealthDynamicHazards])
      result=review(source,file,result,violations,parts);
    return result;
  }
});
`;
change('export const FROZEN_BASE=',initializer+'\nexport const FROZEN_BASE=','Retain original exact statement/type evidence through complete owner reverses; no new prior allowance.');
const line=after.split('\n').find(x=>x.includes('} else assertSpecialConstructs(source,file,reviewedSource13Hazards('));assert(line);
const start=line.indexOf('assertSpecialConstructs('),end=line.indexOf(';assertFrozenAuthority',start),call=line.slice(start,end),prefix='assertSpecialConstructs(source,file,';assert(call.startsWith(prefix)&&call.endsWith(')'));
const baseline=call.slice(prefix.length,-1),next=prefix+'reviewedKeptEntryHazards(source,file,'+baseline+',specialConstructViolations(source,file),executableParts(source,file)))';
change(call,next,'Wrap only the existing type/construction allowance map, leaving all other gates and reviews intact.');
let reversed=after;for(const h of [...recipe].reverse()){assert.equal(reversed.split(h.after).length,2);reversed=reversed.replace(h.after,()=>h.before);}assert.equal(reversed,before);
execFileSync(process.execPath,['--input-type=module','--check'],{input:after,encoding:'utf8',windowsHide:true});
fs.writeFileSync(here+'/GATE_EDITS.json',JSON.stringify({version:'FINITE_KEPT_ENTRY_F23_GATE_RECIPE_V1',integrationCheckpoint:'24c57e6c7af4a8fb07d5e3126ab9ac76e9448560',target,inputSha256:sha(before),outputSha256:sha(after),hunks:recipe,fullReverseByteEquality:true,syntax:'PASS_NODE_CHECK_ONLY',fullGateRun:false,native:'NOT_RUN',newModule:'scripts/healthroster-kept-entry-type-review.mjs',newReview:'docs/healthroster-build/proposals/kept-entry-f23-classification-20261017/CLASSIFICATION.json'},null,2)+'\n');console.log(JSON.stringify({hunks:recipe.length,fullReverse:true,syntax:'PASS',rootWrites:0}));
