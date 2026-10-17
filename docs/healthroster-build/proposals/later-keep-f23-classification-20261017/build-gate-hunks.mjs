// Finite source gate recipe only; never writes the integration gate.
import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';import{fileURLToPath}from'node:url';import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(here,'../../../../../healthroster-install2-14102026'),target='scripts/bpay-next-source-gates.mjs',before=fs.readFileSync(root+'/'+target,'utf8'),sha=x=>createHash('sha256').update(x).digest('hex');
assert(fs.realpathSync(root).replaceAll('\\','/').startsWith('D:/Development/GitHub/'));const hunks=[];let after=before;
function change(old,next,purpose){assert.equal(after.split(old).length,2,'ONE_EXACT_GATE_ANCHOR');after=after.replace(old,()=>next);hunks.push({before:old,after:next,purpose});}
change("import {createReviewedKeptEntryClassifier} from './healthroster-kept-entry-type-review.mjs';","import {createReviewedLaterKeepClassifier} from './healthroster-later-keep-type-review.mjs';\nimport {createReviewedKeptEntryClassifier} from './healthroster-kept-entry-type-review.mjs';",'Add separate finite Later/Keep review without changing original nineteen-file proof.');
const initializer=`const reviewedLaterKeepHazards=createReviewedLaterKeepClassifier(repoRoot,{
  tupleProof:provesSingleTableTuple,hazardCensus:specialConstructViolations,
  classifyPrior:(source,file,initial=new Map())=>{
    const violations=specialConstructViolations(source,file),parts=executableParts(source,file);
    let result=new Map(initial);
    for(const review of [reviewedContactFiniteHazards,reviewedHealthRowtypeHazards,reviewedHealthUuidHazards,reviewedHealthAggregateHazards,reviewedHealthDynamicHazards,reviewedJoinedNonDynamicHazards,reviewedPrebodyDynamicHazards,reviewedSource13Hazards])
      result=review(source,file,result,violations,parts);
    return result;
  }
});
`;
change('export const FROZEN_BASE=',initializer+'\nexport const FROZEN_BASE=','Invoke existing original statement/type classifiers only after complete current-parent reversal.');
const line=after.split('\n').find(x=>x.includes('} else assertSpecialConstructs(source,file,reviewedKeptEntryHazards('));assert(line);const at=line.indexOf('assertSpecialConstructs('),end=line.indexOf(';assertFrozenAuthority',at),old=line.slice(at,end),start='assertSpecialConstructs(source,file,';assert(old.startsWith(start)&&old.endsWith(')'));const prior=old.slice(start.length,-1);
change(old,start+'reviewedLaterKeepHazards(source,file,'+prior+',specialConstructViolations(source,file),executableParts(source,file)))','Compose on the unchanged Kept/Entry allowance map, preserving every existing gate.');
let reversed=after;for(const h of [...hunks].reverse()){assert.equal(reversed.split(h.after).length,2);reversed=reversed.replace(h.after,()=>h.before);}assert.equal(reversed,before);execFileSync(process.execPath,['--input-type=module','--check'],{input:after,windowsHide:true});
fs.writeFileSync(here+'/GATE_EDITS.json',JSON.stringify({version:'FINITE_LATER_KEEP_GATE_RECIPE_V1',target,inputSha256:sha(before),outputSha256:sha(after),hunks,fullReverseByteEquality:true,syntax:'PASS_NODE_CHECK_ONLY',newModule:'scripts/healthroster-later-keep-type-review.mjs',newReview:'docs/healthroster-build/proposals/later-keep-f23-classification-20261017/CLASSIFICATION.json',native:'NOT_RUN',rootWrites:0},null,2)+'\n');console.log(JSON.stringify({hunks:hunks.length,completeReverse:true,syntax:'PASS',rootWrites:0}));
