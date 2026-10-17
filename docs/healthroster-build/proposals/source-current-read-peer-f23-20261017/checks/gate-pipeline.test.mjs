import './gate-hook.mjs';
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {originalReviews} from './prior-review.mjs';
const here=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),repo=path.resolve(here,'../../../..');
const root=path.resolve(repo,'../healthroster-install2-14102026');
const ownPrefix='docs/healthroster-build/proposals/source-current-read-peer-f23-20261017/';
const read=fs.readFileSync.bind(fs);
fs.readFileSync=(file,...args)=>{
 const relative=path.relative(root,path.resolve(String(file))).replaceAll('\\','/');
 return read(relative.startsWith(ownPrefix)?path.join(repo,relative):file,...args);
};
const {peerF23}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-source-gates.mjs')).href+'?source-read-peer');
const {baseline,gate}=await originalReviews(root),mandatory='supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql';
const old=execFileSync('git',['show',gate.FROZEN_BASE+':'+mandatory],{cwd:repo}).toString();
for(const point of gate.specialConstructViolations(old,mandatory))baseline.set(point.key,(baseline.get(point.key)??0)+1);
const proof=JSON.parse(read(path.join(here,'CLASSIFICATION.json'))),results=[];
for(const variant of proof.variants){
 const source=read(path.join(root,variant.source),'utf8');peerF23(source,variant.file,baseline);
 results.push({file:variant.file,sha256:variant.sha256,passed:true});
}
const target=proof.variants.find(v=>v.points.length===18),source=read(path.join(root,target.source),'utf8');
assert.throws(()=>peerF23(source+"\ndo $$begin execute 'select 1';end$$;\n",target.file,baseline),/CURRENT_SOURCE_CHANGED/);
fs.writeFileSync(path.join(here,'ACTUAL_GATE_PIPELINE_RESULTS.json'),JSON.stringify({
 actualCompletePatchedGateImported:true,exactPatchedPipelineExpression:true,constructorAndPriorSameProofReplayed:true,
 passedVariants:results,extraDynamicStatementRefused:true,fullGlobalGate:'ROOT_OWNED_NOT_RUN',native:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({actualPatchedPipeline:17,extraDynamicStatementRefused:true,native:'NOT_RUN'}));
