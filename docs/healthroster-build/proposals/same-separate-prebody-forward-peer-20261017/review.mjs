// Independent source-only boundary review. No database or executor imports.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(here,'../../../..');
const root=path.resolve(own,'../healthroster-install2-14102026');
for(const p of [process.cwd(),own,root])assert.match(fs.realpathSync.native(p).replaceAll('\\','/'),/^D:\/Development\/GitHub(?:\/|$)/);
const sha=b=>createHash('sha256').update(b).digest('hex'),canon=b=>b.toString().replaceAll('\r\n','\n');
const read=f=>fs.readFileSync(path.join(root,f));
const git=(ref,f)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,'show',ref+':'+f],{maxBuffer:8*1024*1024});
const delivery='docs/healthroster-build/v6-owner-delivery-20261016/';
const records=['SAME_SEPARATE_PREBODY_DEPENDENCY_FORWARD_20261017.json','SAME_SEPARATE_F23_PREBODY_BINDING_FORWARD_20261017.json','SAME_SEPARATE_VERIFIER_TEST_PARENT_FORWARD_20261017.json'];
const proofs=records.map(name=>({name,...JSON.parse(read(delivery+name))}));
const checks=[],check=(name,fn)=>{fn();checks.push({name,status:'PASS'});};
const sourceCheck='4c74f5fe8aed042945eb663b6a0baa0d7cac3644';
const frozen='37fb0a383750930d428cd760a80d6849e8e2313b';
check('all three forward records reverse complete source bytes to independent committed parents',()=>{
 for(const p of proofs){const current=read(p.file).toString();assert.equal(sha(current),p.afterSha256);assert.equal(current.split(p.hunk.after).length,2);const old=current.replace(p.hunk.after,()=>p.hunk.before);assert.equal(sha(old),p.beforeSha256);
  const original=p.file==='scripts/healthroster-same-separate-type-review.mjs'?git(frozen,'docs/healthroster-build/proposals/same-separate-f23-classification-20261017/payload/'+p.file):git(sourceCheck,p.file);
  assert.equal(old,original.toString());
 }
});
const prebody=await import(pathToFileURL(root+'/scripts/healthroster-prebody-dynamic-review.mjs').href);
const same=await import(pathToFileURL(root+'/scripts/healthroster-same-separate-type-review.mjs').href);
const {scanManagedSql}=await import(pathToFileURL(root+'/scripts/cloudtms-managed-release-sql.mjs').href);
const preReview=JSON.parse(read(prebody.reviewPath)),sameReview=JSON.parse(read(same.reviewPath));
check('historical classification bytes and all 95 complete dependency bindings remain exact',()=>{
 assert.equal(sha(read(prebody.reviewPath)),prebody.reviewSha256);assert.deepEqual(read(prebody.reviewPath),git(sourceCheck,prebody.reviewPath));assert.equal(preReview.dependencies.length,95);
 for(const p of preReview.dependencies){const bytes=read(p.file);assert.equal(sha(prebody.prebodyDependencySource(p,bytes)),p.sha256,p.file);if(p.preservedRawSha256)assert.equal(sha(bytes),p.preservedRawSha256);}
 assert.deepEqual(read(preReview.priorReview.path),git(sourceCheck,preReview.priorReview.path));
 assert.equal(sha(read(same.reviewPath)),'f338e3eaf7f3ce2ff027cd2728cc5f16f0724ea4ce7eeea139f5217ee949766d');assert.deepEqual(read(same.reviewPath),git(frozen,same.reviewPath));
});
check('three proxy edits reconstruct exact genuine committed complete predecessor',()=>{
 const p=proofs[0];let old=canon(read(p.targetAdapter));assert.equal(sha(old),p.adapterAfterSha256);
 for(const[before,after]of p.adapterLiteralEdits){assert.equal(old.split(after).length,2);old=old.replace(after,()=>before);}
 assert.equal(sha(old),p.adapterBeforeSha256);assert.equal(old,canon(git(sourceCheck,p.targetAdapter)));
 const input={file:p.targetAdapter,sha256:p.adapterBeforeSha256};assert.equal(prebody.prebodyDependencySource(input,read(p.targetAdapter)),old);assert.equal(prebody.prebodyDependencySource(input,old),old);
 for(const changed of [canon(read(p.targetAdapter))+'\n// changed',canon(read(p.targetAdapter)).replace(p.adapterLiteralEdits[0][1],()=>p.adapterLiteralEdits[0][0]),canon(read(p.targetAdapter))+p.adapterLiteralEdits[2][1]])assert.throws(()=>prebody.prebodyDependencySource(input,changed),/BOUND_SOURCE_CHANGED/);
 for(const changed of [{...input,file:input.file+'.copy'},{...input,sha256:'0'.repeat(64)}])assert.notEqual(sha(prebody.prebodyDependencySource(changed,read(p.targetAdapter))),input.sha256);
});
// Only this copied receipt is read through fs by the new binding. All SQL,
// schema and owner source remains a read-only virtual view of actual I2.
const fixture=fs.mkdtempSync(path.join(here,'.peer-fixture-'));
const receipt=delivery+records[0],receiptPath=path.join(fixture,receipt),receiptBytes=read(receipt);
fs.mkdirSync(path.dirname(receiptPath),{recursive:true});fs.writeFileSync(receiptPath,receiptBytes);
const selected=sameReview.files.find(f=>f.points.length===0),baseline=new Map([['independent-baseline',2]]);
assert(selected);
function classify(transform=(file,bytes)=>bytes){const reader=file=>transform(file,read(file));const fn=same.createReviewedSameSeparateClassifier(fixture,{readFile:reader,statementScan:scanManagedSql});return fn(canon(read(selected.file)),selected.file,baseline,[],[]);}
try{
 check('actual new dependency binding admits only after whole historical module reconstruction',()=>assert.deepEqual(classify(),baseline));
 check('genuine old dependency remains usable without the forward receipt',()=>{
  fs.unlinkSync(receiptPath);try{assert.deepEqual(classify((file,b)=>file===proofs[0].file?git(sourceCheck,file):b),baseline);}finally{fs.writeFileSync(receiptPath,receiptBytes);}
 });
 check('same process refuses outside-hunk prebody drift and every other dependency drift',()=>{
  for(const p of sameReview.dependencies)assert.throws(()=>classify((file,b)=>file===p.path?Buffer.concat([b,Buffer.from('\n// peer drift')]):b),/DEPENDENCY_CHANGED/,p.path);
 });
 check('absent, malformed and edited forward evidence refuses instead of trusting a source hash',()=>{
  fs.unlinkSync(receiptPath);assert.throws(()=>classify(),/ENOENT/);
  for(const bytes of [Buffer.from('{'),Buffer.concat([receiptBytes,Buffer.from(' ')]),Buffer.from(receiptBytes.toString().replace('4606dc7e','00000000'))]){fs.writeFileSync(receiptPath,bytes);assert.throws(()=>classify(),/PREBODY_FORWARD_REVIEW_CHANGED/);}
  fs.writeFileSync(receiptPath,receiptBytes);assert.deepEqual(classify(),baseline);
 });
 check('frozen classification edits cannot create a foreign dependency allowance',()=>{
  for(const transform of [j=>j.dependencies[0].path+='copy',j=>j.dependencies.find(p=>p.path===proofs[0].file).sha256=proofs[0].afterSha256,j=>j.native='PASS'])assert.throws(()=>classify((f,b)=>{if(f!==same.reviewPath)return b;const j=JSON.parse(b);transform(j);return Buffer.from(JSON.stringify(j));}),/REVIEW_CHANGED/);
 });
}finally{
 const resolved=fs.realpathSync.native(fixture);assert.equal(path.dirname(resolved),fs.realpathSync.native(here));assert(path.basename(resolved).startsWith('.peer-fixture-'));fs.rmSync(resolved,{recursive:true});
}
check('four original raw/mapped packet records and complete producers remain committed bytes',()=>{
 assert.equal(preReview.files.length,2);assert.equal(preReview.files.reduce((n,f)=>n+f.variants.length,0),4);
 for(const f of preReview.files){assert.deepEqual(read(f.path),git(sourceCheck,f.path));assert.deepEqual(read(f.program.file),git(sourceCheck,f.program.file));for(const p of f.points)assert.deepEqual(read(p.bodyFile),git(sourceCheck,p.bodyFile));}
});
const result={version:'SAME_SEPARATE_PREBODY_FORWARD_INDEPENDENT_PEER_V1',sourceCheckpoint:sourceCheck,ownFrozenClassifier:frozen,checks,reviewedFiles:proofs.map(p=>({file:p.file,beforeSha256:p.beforeSha256,afterSha256:p.afterSha256})),sourceRecords:records.map(file=>({file:delivery+file,sha256:sha(read(delivery+file))})),historicalDependencies:95,originalPacketVariants:4,frozenClassificationSha256:sha(read(same.reviewPath)),runtimeSQLChangedByTheseForwards:false,newHazardAllowances:0,bankingBodiesRead:0,databaseConnections:0,native:'NOT_RUN'};
fs.writeFileSync(path.join(here,'REVIEW_RESULTS.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({checks:checks.length,status:'PASS',native:'NOT_RUN'}));
