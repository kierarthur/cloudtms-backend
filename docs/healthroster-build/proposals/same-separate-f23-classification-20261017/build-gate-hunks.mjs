// Exact source-only recipe; no integration write or gate execution.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',target='scripts/bpay-next-source-gates.mjs';
const sha=x=>createHash('sha256').update(x).digest('hex'),before=fs.readFileSync(root+'/'+target,'utf8'),hunks=[];let after=before;
assert.equal(sha(before),'2798cc6178aca812b82194659a6618bd94f12fdb404621783b5822b53a6a965f');
function edit(old,next,purpose){assert.equal(after.split(old).length,2,'EXACT_UNIQUE_GATE_ANCHOR');after=after.replace(old,()=>next);hunks.push({before:old,after:next,purpose});}
edit("import {createReviewedLaterKeepClassifier} from './healthroster-later-keep-type-review.mjs';",
 "import {createReviewedSameSeparateClassifier} from './healthroster-same-separate-type-review.mjs';\nimport {createReviewedLaterKeepClassifier} from './healthroster-later-keep-type-review.mjs';",
 'Add independent finite Same/Separate proof; original modules and pins unchanged.');
const init=`const reviewedSameSeparateHazards=createReviewedSameSeparateClassifier(repoRoot,{
  tupleProof:provesSingleTableTuple,hazardCensus:specialConstructViolations,statementScan:scanManagedSql,
  classifyPrior:(source,file,initial=new Map())=>{
    const violations=specialConstructViolations(source,file),parts=executableParts(source,file);
    let result=sortedStoredCreditHazards(source,file,firstInstallIncomingHazards(source,file,new Map(initial)));
    for(const review of [reviewedContactFiniteHazards,reviewedHealthRowtypeHazards,reviewedHealthUuidHazards,reviewedHealthAggregateHazards,reviewedHealthDynamicHazards,reviewedJoinedNonDynamicHazards,reviewedPrebodyDynamicHazards,reviewedSource13Hazards])
      result=review(source,file,result,violations,parts);
    result=reviewedKeptEntryHazards(source,file,result,violations,parts);
    return reviewedLaterKeepHazards(source,file,result,violations,parts);
  }
});
`;
edit('export const FROZEN_BASE=',init+'\nexport const FROZEN_BASE=','Transfer only exact full-parent statements through every actual prior classifier.');
const line=after.split('\n').find(x=>x.includes('} else assertSpecialConstructs(source,file,reviewedLaterKeepHazards('));assert(line);
const at=line.indexOf('assertSpecialConstructs('),end=line.indexOf(';assertFrozenAuthority',at),old=line.slice(at,end),start='assertSpecialConstructs(source,file,';assert(old.startsWith(start)&&old.endsWith(')'));
edit(old,start+'reviewedSameSeparateHazards(source,file,'+old.slice(start.length,-1)+',specialConstructViolations(source,file),executableParts(source,file)))',
 'Compose on the entire unchanged existing allowance chain; retain every gate and frozen-authority check.');
let reversed=after;for(const h of [...hunks].reverse()){assert.equal(reversed.split(h.after).length,2);reversed=reversed.replace(h.after,()=>h.before);}assert.equal(reversed,before);
execFileSync(process.execPath,['--input-type=module','--check'],{input:after,windowsHide:true});
fs.writeFileSync(here+'/GATE_EDITS.json',JSON.stringify({version:'SAME_SEPARATE_FINITE_GATE_RECIPE_V1',target,inputSha256:sha(before),outputSha256:sha(after),hunks,
 completeRawByteReverse:true,nodeSyntax:'PASS',rootWrites:0,newModule:'scripts/healthroster-same-separate-type-review.mjs',
 newReview:'docs/healthroster-build/proposals/same-separate-f23-classification-20261017/CLASSIFICATION.json',native:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({hunks:hunks.length,completeRawByteReverse:true,rootWrites:0,nodeSyntax:'PASS'}));
