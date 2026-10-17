// Only public original source images. No actual stage records, private paths, verifier or bookkeeping.
import fs from 'node:fs';import cp from 'node:child_process';import assert from 'node:assert/strict';
import {blobImages} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/git-image-batch.mjs';
const r='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026',i='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',f='docs/healthroster-build/proposals/daily-genuine-root-stage-bookkeeper-20261017.mjs';
const git=(root,...args)=>cp.execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{windowsHide:true,maxBuffer:32e6,stdio:['ignore','pipe','pipe'],env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
const pins=JSON.parse(fs.readFileSync(r+'/docs/healthroster-build/proposals/daily-genuine-stage-root-bookkeeper-20261017/PARENTS.json')).fixedRuntime,head=git(i,'rev-parse','HEAD').toString().trim(),source=git(r,'rev-parse','HEAD').toString().trim(),reportRows=pins.map(row=>({ref:row.commit,file:row.file})),rootRows=[head,null,'8971881e6347f4597a8547aabeb404fe578011c0'].map(ref=>({ref,file:f})),samples=[];
for(let n=0;n<2;n++){
 const begin=performance.now(),old=[...reportRows.map(q=>git(r,'show',q.ref+':'+q.file)),...rootRows.map(q=>git(i,'show',(q.ref??'')+':'+q.file))],priorMs=performance.now()-begin;
 const next=performance.now(),report=blobImages(r,reportRows),root=blobImages(i,rootRows),after=[...reportRows.map(q=>report.get(q.ref,q.file)),...rootRows.map(q=>root.get(q.ref,q.file))],batchMs=performance.now()-next;
 assert.equal(old.length,15);for(let j=0;j<old.length;j++)assert(old[j].equals(after[j]));samples.push({prior15ShowsMs:Math.round(priorMs),twoBatchesMs:Math.round(batchMs),savedMs:Math.round(priorMs-batchMs),all15ImagesRawEqual:true});
}
assert.equal(git(i,'rev-parse','HEAD').toString().trim(),head);assert.equal(git(r,'rev-parse','HEAD').toString().trim(),source);
console.log(JSON.stringify({scope:'PUBLIC_FIXED_SOURCE_PINS_AND_HELPER_TRANSPORT_ONLY',rootHead:head,reportHead:source,samples,perActualPublicReadAdditionalProcessReduction:'2 Git shows to1 fresh batch',wholeStageTimingGuarantee:false,nativeActualStageRecordPrivateAclAccess:'NONE'},null,2));
