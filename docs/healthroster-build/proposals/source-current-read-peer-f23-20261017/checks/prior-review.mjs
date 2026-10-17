// Reuse actual original classifiers; no new approval or Banking body inspection.
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
export async function originalReviews(root){
 const load=file=>import(pathToFileURL(path.join(root,'scripts',file)).href);
 const gate=await load('bpay-next-source-gates.mjs'),tuple=await load('healthroster-rowtype-type-review.mjs');
 const descriptions=[['healthroster-contact-finite-review.mjs','reviewedContactFiniteHazards'],
  ['healthroster-rowtype-type-review.mjs','reviewedHealthRowtypeHazards'],['healthroster-uuid-cardinality-review.mjs','reviewedHealthUuidHazards'],
  ['healthroster-aggregate-type-review.mjs','reviewedHealthAggregateHazards'],['healthroster-dynamic-source-review.mjs','reviewedHealthDynamicHazards'],
  ['healthroster-joined-nondynamic-review.mjs','reviewedJoinedNonDynamicHazards'],['healthroster-prebody-dynamic-review.mjs','reviewedPrebodyDynamicHazards'],
  ['healthroster-source13-finite-review.mjs','reviewedSource13Hazards']];
 const reviews=[];for(const[file,name]of descriptions)reviews.push((await load(file))[name]);
 const classifyOriginal=(source,file,initial=new Map())=>{
  const points=gate.specialConstructViolations(source,file),parts=gate.executableParts(source,file);let result=new Map(initial);
  result=gate.firstInstallIncomingHazards(source,file,result);result=gate.sortedStoredCreditHazards(source,file,result);
  for(const review of reviews)result=review(source,file,result,points,parts);return result;
 };
 const {createReviewedKeptEntryClassifier}=await load('healthroster-kept-entry-type-review.mjs'),
  {createReviewedLaterKeepClassifier}=await load('healthroster-later-keep-type-review.mjs');
 const options={tupleProof:tuple.provesSingleTableTuple,hazardCensus:gate.specialConstructViolations,classifyPrior:classifyOriginal};
 const kept=createReviewedKeptEntryClassifier(root,options),later=createReviewedLaterKeepClassifier(root,options);
 const classifyPrior=(source,file,initial=new Map())=>{
  const points=gate.specialConstructViolations(source,file),parts=gate.executableParts(source,file);
  return later(source,file,kept(source,file,classifyOriginal(source,file,initial),points,parts),points,parts);
 };
 // The exact historical Health evidence used by the prior93328 proof is
 // retained, including explicit missing-file records that grant no allowance.
 const evidence=JSON.parse(fs.readFileSync(path.join(root,'docs/healthroster-build/proposals/later-keep-f23-classification-20261017/TEST_RESULTS.json'),'utf8')).baselineEvidence;
 const invoiceFile='supabase/repeatable/15092026_1534_weekly_source_invoice_admission_v1.sql',invoiceCommit='a6f7423eb71e6e7cd173e705c2290e5408a1d077';
 if(gate.FROZEN_BASE!==invoiceCommit)throw Error('ORIGINAL_FROZEN_AUTHORITY_CHANGED');
 const invoiceBytes=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,'show',invoiceCommit+':'+invoiceFile],{windowsHide:true,maxBuffer:4*1024*1024});
 evidence.push({file:invoiceFile,commit:invoiceCommit,sha256:createHash('sha256').update(invoiceBytes).digest('hex'),
  provenance:'Additional exact original frozen Health invoice owner; same gate FROZEN_BASE, no new allowance'});
 const baseline=new Map();
 for(const entry of evidence){if(entry.state==='NOT_PRESENT_NO_ALLOWANCE')continue;
  const source=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,'show',entry.commit+':'+entry.file],{encoding:'utf8',windowsHide:true,maxBuffer:4*1024*1024});
  const counts=new Map();for(const point of gate.specialConstructViolations(source,entry.file))counts.set(point.key,(counts.get(point.key)??0)+1);
  for(const[key,count]of counts)baseline.set(key,Math.max(baseline.get(key)??0,count));
 }
 return {gate,tupleProof:tuple.provesSingleTableTuple,classifyPrior,baseline,baselineEvidence:evidence,descriptions};
}
