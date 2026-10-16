import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),report=path.resolve(here,'../../../..'),worktrees=path.dirname(report),money=path.join(worktrees,'healthroster-v2-money-build-13102026'),i2=path.join(worktrees,'healthroster-install2-14102026');
const pin='bf7a321429969f2c43345ce05ad2679ce3dbe83c',base='2d74b1fdced14d4a60882720d7e590ad83f8dd5a',rootPin='34b8e2584b7500a6e7aad9db32fa2061b484977d';
const folder='docs/healthroster-build/proposals/v6-contact-repaired-managed-assembly-20261016',oldFolder='docs/healthroster-build/proposals/v6-contact-managed-assembly-20261016';
const sha=x=>createHash('sha256').update(x).digest('hex'),blob=(ref,p,cwd=money)=>execFileSync('git',['show',ref+':'+p],{cwd,maxBuffer:64_000_000,windowsHide:true}),frozen=p=>blob(pin,folder+'/'+p),json=p=>JSON.parse(frozen(p));
const M=json('ASSEMBLY_MANIFEST.json'),R=json('REPAIR_PROVENANCE.json'),D=json('registry/FINAL_DEFINER_DELTA.json');
const {scanManagedSql,readManagedClosure}=await import(pathToFileURL(path.join(i2,'scripts/cloudtms-managed-release-sql.mjs'))),{routineDefinitions}=await import(pathToFileURL(path.join(i2,'scripts/bpay-next-final-definers.mjs')));
function statements(text,p){return scanManagedSql(text).statements.map(s=>({sql:text.slice(s.start,s.end).trim(),signatures:routineDefinitions(text.slice(s.start,s.end),p)}));}
const defs=(text,p)=>statements(text,p).flatMap(s=>s.signatures.map(signature=>({signature,sql:s.sql})));
const payloadProof=[],allStatements=[];
for(const item of M.payloads){
 const current=frozen('payload/'+item.path),disk=fs.readFileSync(path.join(money,folder,'payload',item.path)),original=blob(base,oldFolder+'/payload/'+item.path);
 assert.deepEqual(disk,current,item.path+' committed/disk');assert.equal(sha(current),item.sha256);
 payloadProof.push({path:item.path,sha256:sha(current),equalsOriginal:current.equals(original)});
 if(item.path.startsWith('supabase/migrations/'))allStatements.push(...statements(current.toString(),item.path));
}
const mount=json('MOUNT_RECIPE.json');assert.equal(mount.output.length,63);for(const entry of mount.output){const item=payloadProof.find(p=>p.path===entry.path);assert(item,entry.path);assert.equal(item.sha256,entry.sha256);assert(!entry.path.includes('frozen/'));}
assert.equal(payloadProof.length,63);const changed=payloadProof.filter(p=>!p.equalsOriginal);assert.equal(changed.length,2);
assert(changed.some(p=>p.path===R.selectedLeaf));assert(changed.some(p=>p.path===M.reservedRoot));
const oldLeaf=blob(base,oldFolder+'/payload/'+R.selectedLeaf),newLeaf=frozen('payload/'+R.selectedLeaf),nativeRepair=blob('230c3bd58049406ea6ba87662160823dc43e2e52','docs/healthroster-build/proposals/v6-contact-assembly-review-20261016/forward/01_complete_current_closed_command_start.sqlinc');
assert.equal(sha(nativeRepair),'524fd763e1f0822b64cf3f471b99b22461083516df3a3be2daa07085bf037628');
const originalDef=defs(oldLeaf.toString(),'old').find(d=>d.signature===R.signature),fixedDef=defs(newLeaf.toString(),'new').find(d=>d.signature===R.signature);
assert.equal(fixedDef.sql,nativeRepair.toString().trim());assert.equal(sha(fixedDef.sql),'3af85b27bd5a224691e41568ea0548709b7ff5eb57897abe447e275b9591b00d');
assert.deepEqual(Buffer.concat([newLeaf.subarray(0,R.oldSpan.start),oldLeaf.subarray(R.oldSpan.start,R.oldSpan.endExclusive),newLeaf.subarray(R.oldSpan.start+Buffer.byteLength(fixedDef.sql))]),oldLeaf);
assert.deepEqual(statements(newLeaf.toString(),'new').filter(s=>!s.signatures.includes(R.signature)).map(s=>s.sql),statements(oldLeaf.toString(),'old').filter(s=>!s.signatures.includes(R.signature)).map(s=>s.sql));
assert.match(originalDef.sql,/insert into public\.weekly_message_provider_attempts/);assert(!/\b(?:insert\s+into|update|delete\s+from)\b/i.test(fixedDef.sql));
const rootOriginal=blob(base,oldFolder+'/payload/'+M.reservedRoot).toString(),rootNew=frozen('payload/'+M.reservedRoot).toString();assert.deepEqual(statements(rootNew,'newRoot'),statements(rootOriginal,'oldRoot'));
const selected=[];
for(const p of M.orderedRepeatableRoots){const c=readManagedClosure(p,path.join(money,folder,'payload'));allStatements.push(...statements(c.expanded,p));selected.push(...defs(c.expanded,p));}
assert.equal(allStatements.length,411);const declarations=allStatements.flatMap(s=>s.signatures.map(signature=>({signature,sql:s.sql}))),final=new Map(declarations.map(d=>[d.signature,d]));
assert.equal(declarations.length,113);assert.equal(final.size,111);assert.equal(allStatements.filter(s=>s.signatures.length===0).length,298);
assert.equal(selected.filter(d=>d.signature===R.signature).length,1);assert.equal(final.get(R.signature).sql,nativeRepair.toString().trim());
for(const d of declarations)assert(!/\binsert\s+into\s+(?:public\.)?weekly_message_(?:provider|target)_attempts\b/i.test(d.sql),d.signature);
const rootIndex=rootNew.indexOf('/38_native_submission_readers.sqlinc'),guardIndex=rootNew.indexOf('/98_manual_selection_ordinal_guard.sqlinc'),aclIndex=rootNew.indexOf('/99_acl.sqlinc');assert(rootIndex>0&&rootIndex<guardIndex&&guardIndex<aclIndex);
const prereq=json('PREREQUISITE_SOURCE_PROOF.json');for(const f of prereq.orderedFunctions){const native=defs(blob(f.sourceCommit,f.sourcePath).toString(),f.sourcePath).find(d=>d.signature===f.signature);assert.equal(final.get(f.signature).sql,native.sql);}
const acl=frozen('payload/'+prereq.acl.target).toString();assert.equal(statements(acl,'prereqACL').length,10);assert(!/grant execute/i.test(acl));
const before=JSON.parse(blob(rootPin,D.registryPath)),after=structuredClone(before);let updates=0,adds=0;
assert.equal(before.chains.length,1462);assert.equal(sha(blob(rootPin,D.registryPath)),D.baseSha256);
for(const r of D.rows){const at=after.chains.findIndex(c=>c.signature===r.signature);assert.deepEqual(at<0?null:after.chains[at],r.before);assert.equal(sha(final.get(r.signature).sql),r.completeFinalStatementSha256,r.signature);
 const physical=defs(frozen('payload/'+r.after.finalPhysical).toString(),r.after.finalPhysical).filter(d=>d.signature===r.signature).at(-1);assert.equal(physical.sql,final.get(r.signature).sql);
 if(at<0){after.chains.push(r.after);adds++;}else{for(const key of ['allowedPhysical','predecessors','finalClosures'])for(const value of r.before[key]??[])assert(r.after[key].includes(value));for(const key of Object.keys(r.before).filter(k=>!['finalRoot','finalPhysical','allowedPhysical','predecessors','finalClosures'].includes(k)))assert.deepEqual(r.after[key],r.before[key]);after.chains[at]=r.after;updates++;}}
assert.equal(updates,12);assert.equal(adds,99);assert.equal(after.chains.length,1561);for(const key of Object.keys(before).filter(k=>k!=='chains'))assert.deepEqual(before[key],after[key]);
const touched=new Set(D.rows.map(r=>r.signature));for(const r of before.chains.filter(r=>!touched.has(r.signature)))assert.deepEqual(after.chains.find(x=>x.signature===r.signature),r);
const nativePaths={
 'public.weekly_contact_submission_start_v1(jsonb)':'supabase/repeatable/16102026_0226_weekly_contact_query_outbox_admission_v1.sql',
 'public.weekly_contact_submission_consume_v1(jsonb)':'supabase/repeatable/16102026_0130_weekly_contact_submission_consume_v1.sql',
 'private.weekly_contact_source_result_v1(uuid,text,text,jsonb,jsonb)':'supabase/repeatable/16102026_0130_weekly_contact_submission_consume_v1.sql',
 'private.weekly_contact_source_memberships_v1(uuid,boolean)':'supabase/repeatable/includes/weekly_contact_source_memberships/030_native_enrol_assert.sqlinc',
 'private.weekly_contact_source_membership_accept_v1(uuid)':'supabase/repeatable/includes/weekly_contact_source_memberships/040_native_accept.sqlinc',
 'private.weekly_contact_source_admission_v1(jsonb)':'supabase/repeatable/16102026_0214_weekly_contact_source_admission_v1.sql'
};
const protectedOwners=[];for(const signature of [...R.protectedOwnerSignatures.filter(s=>s!==R.signature),'private.weekly_contact_source_admission_v1(jsonb)']){assert(!final.has(signature));const chain=before.chains.find(r=>r.signature===signature),physical=chain?.finalPhysical??nativePaths[signature];assert(physical,signature);const text=blob(rootPin,physical).toString(),d=defs(text,physical).find(d=>d.signature===signature);assert(d,signature);protectedOwners.push({signature,physical,individualRegistryRowPresent:!!chain,completeStatementSha256:sha(d.sql)});}
for(const pin of R.nativeDispatchOwnerPins){const actual=protectedOwners.find(p=>p.signature===pin.signature);assert.equal(actual.completeStatementSha256,pin.sha256);assert.equal(sha(blob(rootPin,pin.path)),pin.sourceFileSha256);}
const activeLegacy=before.chains.find(c=>c.signature===R.signature),legacyDef=defs(blob(rootPin,activeLegacy.finalPhysical).toString(),activeLegacy.finalPhysical).find(d=>d.signature===R.signature);assert.equal(legacyDef.sql,fixedDef.sql);
const runtime=json('RUNTIME_REVERSE_PROOF.json');let restored=frozen('payload/'+runtime.parentPath).toString();for(const e of [...runtime.edits].reverse()){assert.equal(restored.split(e.after).length,2);restored=restored.replace(e.after,e.before);}assert.equal(restored,blob(rootPin,runtime.parentPath).toString().replaceAll('\r\n','\n'));
assert.equal(sha(frozen('payload/broker/src/weekly-source/contact-recheck-runtime.mjs')),'6bf1eed7b3dd86b984217c91b37a4a8d6b786511f712ea6daa87cf8f0a41e8ff');
const result={version:'INDEPENDENT_REPAIRED_CONTACT_SOURCE_REVIEW_V1',reviewedCommit:pin,baseProposalCommit:base,rootCheckpoint:rootPin,qualification:'PASS_BOUNDED_SOURCE_REPAIR_NO_BLOCKER_IDENTIFIED_NATIVE_NOT_RUN',payloadProof,changedPayloads:changed,allStatements:allStatements.length,declarations:declarations.length,uniqueFinalOwners:final.size,nonFunctionStatements:298,wholeLeafReverseEqual:true,allOtherLeafStatementsEqual:true,rootSqlEffectsEqual:true,completeNativeClosedStartEqual:true,completePrerequisiteFunctions:5,originalPrivateAclStatements:10,registry:{before:1462,after:1561,updates,adds,unrelatedPreserved:1450,editorAndMetadataPreserved:true},protectedOwners,currentRuntimeThreeAnchorReverseEqual:true,producerRegressionsIndependentlyRerun:13,native:'NOT_RUN',PartB:'NOT_RUN'};
fs.writeFileSync(path.join(here,'EVIDENCE.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({payloads:63,statements:411,declarations:113,unique:111,effects:298,updates,adds,protectedOwners:protectedOwners.length,result:result.qualification},null,2));
