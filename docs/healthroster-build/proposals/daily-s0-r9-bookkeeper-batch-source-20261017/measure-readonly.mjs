// Existing committed public R8 tuple transport only. Never invokes R9 or any bookkeeper/preflight.
import fs from 'node:fs';import assert from 'node:assert/strict';import cp from 'node:child_process';
import {blobImages} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/git-image-batch.mjs';
const i='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',r='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026',p='docs/healthroster-build/v6-owner-delivery-20261016/',pinFile='docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/SOURCE_PINS.json';
const git=(root,...a)=>cp.execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...a],{maxBuffer:32e6,windowsHide:true,stdio:['ignore','pipe','pipe'],env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
const head=git(i,'rev-parse','HEAD').toString().trim(),source=git(r,'rev-parse','HEAD').toString().trim(),observed=JSON.parse(fs.readFileSync(r+'/'+pinFile)).authObservation,reviewFile=p+'DAILY_AUTH_R10_SOURCE_RECIPE_ROOT_REVIEW_20261017.json',review=git(i,'log','-1','--format=%H','--',reviewFile).toString().trim(),planFile=p+'DAILY_GENUINE_S0_R8_ROOT_EXECUTION_PLAN_20261017.json';
const reportRequests=[source,null].map(ref=>({ref,file:pinFile})),rootRequests=[[reviewFile,review],[planFile,head],[observed.file,observed.commit]].flatMap(([file,hist])=>[head,null,hist].map(ref=>({ref,file}))),samples=[];
for(let run=0;run<2;run++){
 const start=performance.now(),prior=[...reportRequests.map(q=>git(r,'show',(q.ref??'')+':'+q.file)),...rootRequests.map(q=>git(i,'show',(q.ref??'')+':'+q.file))],oldMs=performance.now()-start;
 const next=performance.now(),report=blobImages(r,reportRequests),root=blobImages(i,rootRequests),after=[...reportRequests.map(q=>report.get(q.ref,q.file)),...rootRequests.map(q=>root.get(q.ref,q.file))],newMs=performance.now()-next;
 for(let n=0;n<prior.length;n++)assert(prior[n].equals(after[n]));
 for(const [repo,requests,images]of[[r,reportRequests,report],[i,rootRequests,root]])for(const q of requests)assert(fs.readFileSync(repo+'/'+q.file).equals(images.get(q.ref,q.file)));
 samples.push({run:run+1,priorShowProcesses:11,newBatchProcesses:2,priorMs:Math.round(oldMs),batchMs:Math.round(newMs),savedMs:Math.round(oldMs-newMs),all11LogicalImagesRawEqual:true});
}
assert.equal(git(i,'rev-parse','HEAD').toString().trim(),head);assert.equal(git(r,'rev-parse','HEAD').toString().trim(),source);
console.log(JSON.stringify({kind:'READONLY_EXISTING_R8_PUBLIC_TUPLE_TRANSPORT_ONLY',rootHead:head,reportHead:source,reviewCommit:review,receiptCommit:observed.commit,existingPlan:planFile,logicalImages:11,batches:2,samples,r9PlanOrRuntime:'NOT_EXECUTED',nativePrivateAuthWrites:'NONE',wholeStageTimingGuarantee:false},null,2));
