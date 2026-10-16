import{test}from'node:test';import assert from'node:assert/strict';import fs from'node:fs';import path from'node:path';import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';import{fileURLToPath,pathToFileURL}from'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),i2=path.resolve(repo,'../healthroster-install2-14102026'),read=p=>fs.readFileSync(path.join(here,p),'utf8'),json=p=>JSON.parse(read(p)),sha=x=>createHash('sha256').update(x).digest('hex');
const M=json('ASSEMBLY_MANIFEST.json'),R=json('REPAIR_PROVENANCE.json'),D=json('registry/FINAL_DEFINER_DELTA.json');
const{scanManagedSql,readManagedClosure,exactAtomicBody}=await import(pathToFileURL(path.join(i2,'scripts/cloudtms-managed-release-sql.mjs'))),{routineDefinitions}=await import(pathToFileURL(path.join(i2,'scripts/bpay-next-final-definers.mjs')));
const blob=(ref,p)=>execFileSync('git',['show',ref+':'+p],{cwd:repo,maxBuffer:64_000_000,windowsHide:true});
const defs=(text,file)=>scanManagedSql(text).statements.flatMap(s=>{const sql=text.slice(s.start,s.end).trim();return routineDefinitions(sql,file).map(signature=>({signature,sql}));});
const roots=M.orderedRepeatableRoots.map(p=>readManagedClosure(p,path.join(here,'payload'))),selected=roots.flatMap(c=>defs(c.expanded,c.path));
function assertClosedAuthority(definitions){
 const all=definitions.filter(d=>d.signature===R.signature);assert.equal(all.length,1,'Exactly one complete legacy START owner required');
 assert.equal(sha(all[0].sql),R.newCompleteStatementSha256,'Legacy START resurrection or closed-owner drift');
 assert.match(all[0].sql,/perform private\.weekly_source_query_require_service_v1\(\);\s*raise exception 'WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED' using errcode='55000';/i);
 assert(!/\b(?:insert\s+into|update|delete\s+from)\b/i.test(all[0].sql));
 for(const d of definitions)assert(!/\binsert\s+into\s+(?:public\.)?weekly_message_(?:provider|target)_attempts\b/i.test(d.sql),'Contact producer must not reintroduce alternate attempt allocation');
}
test('complete63 payload411 statements113 declarations111 unique and explicit five prerequisites are retained',()=>{
 assert.equal(M.payloads.length,63);assert.equal(M.counts.allManagedStatements,411);assert.equal(M.counts.totalFunctionDeclarations,113);assert.equal(M.counts.allUniqueFunctions,111);assert.equal(selected.length,112);assert.equal(new Set(selected.map(r=>r.signature)).size,111);
 for(const c of roots)exactAtomicBody(c.expanded);
 for(const p of M.payloads){const b=fs.readFileSync(path.join(here,'payload',p.path));assert.equal(sha(b),p.sha256);assert.equal(b.length,p.bytes);assert(!b.includes(Buffer.from('\r\n')));}
 assertClosedAuthority(selected);
});
test('one full native START substitution reverses the entire leaf; every other complete function and statement is unchanged',()=>{
 const old=fs.readFileSync(path.join(here,'frozen/38_native_submission_readers.DO_NOT_INSTALL.sqlinc')),now=fs.readFileSync(path.join(here,'payload',R.selectedLeaf));
 assert.equal(sha(old),R.oldLeafSha256);assert.equal(sha(now),R.newLeafSha256);
 const repair=read('forward/01_complete_current_closed_command_start.sqlinc');assert.equal(sha(repair),'524fd763e1f0822b64cf3f471b99b22461083516df3a3be2daa07085bf037628');
 const oldSpan=old.subarray(R.oldSpan.start,R.oldSpan.endExclusive),length=Buffer.byteLength(repair.trim());
 const reverse=Buffer.concat([now.subarray(0,R.oldSpan.start),oldSpan,now.subarray(R.oldSpan.start+length)]);assert.deepEqual(reverse,old);
 const oldFunctions=defs(old.toString(),R.selectedLeaf),newFunctions=defs(now.toString(),R.selectedLeaf);assert.equal(oldFunctions.length,newFunctions.length);
 for(const d of oldFunctions){const n=newFunctions.find(n=>n.signature===d.signature);assert(n);if(d.signature!==R.signature)assert.equal(n.sql,d.sql);}
 assert.deepEqual(old,blob(M.originalProposalCommit,'docs/healthroster-build/proposals/v6-contact-managed-assembly-20261016/payload/'+R.selectedLeaf));
});
test('regression actually rejects the frozen unsafe START and an attempted later resurrection',()=>{
 const unsafe=defs(read('frozen/38_native_submission_readers.DO_NOT_INSTALL.sqlinc'),'unsafe').find(d=>d.signature===R.signature);
 assert.match(unsafe.sql,/insert into public\.weekly_message_provider_attempts/i);
 assert.throws(()=>assertClosedAuthority(selected.map(d=>d.signature===R.signature?unsafe:d)),/resurrection|drift/);
 assert.throws(()=>assertClosedAuthority([...selected,unsafe]),/Exactly one/);
 const modified={...selected.find(d=>d.signature===R.signature),sql:selected.find(d=>d.signature===R.signature).sql.replace("raise exception 'WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED' using errcode='55000';","return '{}'::jsonb;")};
 assert.throws(()=>assertClosedAuthority(selected.map(d=>d.signature===R.signature?modified:d)),/resurrection|drift/);
});
test('all other61 managed/runtime payloads equal2d74 and three complete schema inputs reverse unchanged',()=>{
 assert.equal(M.payloads.filter(p=>p.equalsOriginal2d74).length,61);
 for(const p of M.payloads.filter(p=>p.equalsOriginal2d74))assert.deepEqual(fs.readFileSync(path.join(here,'payload',p.path)),blob(M.originalProposalCommit,'docs/healthroster-build/proposals/v6-contact-managed-assembly-20261016/payload/'+p.path));
 for(const p of M.sourcePins.filter(p=>p.phase==='schema')){const b=fs.readFileSync(path.join(here,'payload',p.target));assert.deepEqual(b.subarray(p.sourceByteOffset,p.sourceByteOffset+p.sourceBytes),blob(p.sourceCommit,p.sourcePath));}
 assert.equal(M.rawFrozenPayloadsUnchanged,55);assert.equal(M.rawFrozenPayloadsRetainedForHistory,56);
});
test('guard650f and final original ACL stay complete and original corrected metadata changes only recorded fields',()=>{
 const guard=M.payloads.find(p=>p.path.endsWith('/98_manual_selection_ordinal_guard.sqlinc'));assert.equal(guard.sha256,'650f736b80474576dcfc72ffba5eecbbb0aac954b893e3fb2e1cc81c3d83d796');
 const correction=json('metadata/ORDINAL_GUARD_CORRECTION.json'),frozen=blob(correction.source.commit,correction.source.path).toString(),old=defs(frozen,'guard').find(d=>d.signature===correction.source.signature);
 assert.equal(read('payload/'+guard.path).trim().replace(correction.correction.new,correction.correction.old),old.sql);
 assert(M.orderedRepeatableLeaves.at(-2).endsWith('/98_manual_selection_ordinal_guard.sqlinc'));const acl=M.orderedRepeatableLeaves.at(-1);assert(acl.endsWith('/99_acl.sqlinc'));assert.equal(sha(read('payload/'+acl)),'007857838e847fc6c51f61a778f7a2c7c0ca523ae960f42d0c766237d3f7d197');
 assert.match(read('payload/'+acl),/grant execute on function public\.weekly_source_message_dispatch_submission_start_atomic_v1\(jsonb\) to service_role;/i);
 const originalBase='docs/healthroster-build/proposals/source-contact-producer-continuation-20261016/';const recipe=JSON.parse(blob(M.producerCommit,originalBase+'SOURCE_COMPLETION_RECIPE.json')),recipeNow=json('metadata/CONTACT_SOURCE_COMPLETION_RECIPE_REVIEWED_SUCCESSOR.json');recipeNow.assembly.repeatable.at(-1).sha256=recipe.assembly.repeatable.at(-1).sha256;assert.deepEqual(recipeNow,recipe);
 const runtime=JSON.parse(blob(M.producerCommit,originalBase+'RUNTIME_CONTRIBUTION.json')),runtimeNow=json('metadata/RUNTIME_CONTRIBUTION_REVIEWED_SUCCESSOR.json');assert.equal(runtimeNow.leaf.sha256,'6bf1eed7b3dd86b984217c91b37a4a8d6b786511f712ea6daa87cf8f0a41e8ff');runtimeNow.leaf.sha256=runtime.leaf.sha256;assert.deepEqual(runtimeNow,runtime);
});
test('five genuine full18f helpers/private ACL remain exact with no legacy START dependency',()=>{
 const proof=json('PREREQUISITE_SOURCE_PROOF.json'),actual=defs(read('payload/'+proof.targetFunctionPath),'prerequisites');assert.equal(actual.length,5);
 for(const p of proof.orderedFunctions){const old=defs(blob(p.sourceCommit,p.sourcePath).toString(),'native').find(d=>d.signature===p.signature),now=actual.find(d=>d.signature===p.signature);assert.equal(now.sql,old.sql);assert.equal(sha(now.sql),p.completeFunctionStatementSha256);assert(!now.sql.includes('weekly_source_message_dispatch_submission_start_atomic_v1'));assert(!/\binsert\s+into\b/i.test(now.sql));}
 const acl=read('payload/'+proof.acl.target),old=blob(proof.sourceCommit,proof.acl.sourcePath).toString();assert.equal(scanManagedSql(acl).statements.length,10);for(const s of scanManagedSql(acl).statements)assert(old.includes(acl.slice(s.start,s.end).trim()));assert(!/grant execute/i.test(acl));
 const rel=json('PREREQUISITE_RELATION_REVIEW.json');assert.equal(rel.rootCommit,M.rootPrerequisiteCheckpoint);assert.deepEqual(rel.missingColumns,[]);assert.equal(rel.relations.length,21);
});
test('current1462 registry receives only12updates99adds and preserves every Source chain and unrelated/editor field',()=>{
 const before=JSON.parse(blob(D.rootCommit,D.registryPath));assert.equal(sha(blob(D.rootCommit,D.registryPath)),D.baseSha256);assert.equal(before.chains.length,1462);assert.equal(D.rows.length,111);assert.equal(D.updated,12);assert.equal(D.added,99);assert.equal(D.fullRegistryReplacementProvided,false);
 const after=structuredClone(before),changed=new Set(D.rows.map(r=>r.signature));for(const r of D.rows){const at=after.chains.findIndex(c=>c.signature===r.signature);assert.deepEqual(at<0?null:after.chains[at],r.before);if(at<0)after.chains.push(r.after);else after.chains[at]=r.after;}
 assert.equal(after.chains.length,1561);for(const k of Object.keys(before).filter(k=>k!=='chains'))assert.deepEqual(after[k],before[k]);
 for(const c of before.chains.filter(c=>!changed.has(c.signature)))assert.deepEqual(after.chains.find(a=>a.signature===c.signature),c);
 for(const r of D.rows.filter(r=>r.before)){for(const field of ['allowedPhysical','predecessors','finalClosures'])for(const value of r.before[field]??[])assert(r.after[field].includes(value));for(const k of Object.keys(r.before).filter(k=>!['finalRoot','finalPhysical','allowedPhysical','predecessors','finalClosures'].includes(k)))assert.deepEqual(r.after[k],r.before[k]);}
 const start=D.rows.find(r=>r.signature===R.signature);assert.equal(start.completeFinalStatementSha256,R.newCompleteStatementSha256);assert.equal(start.after.finalPhysical,R.selectedLeaf);
 assert.equal(json('registry/DELTA_APPLICATION_PROOF.json').resultCanonicalJsonSha256,sha(JSON.stringify(after)));assert(!fs.existsSync(path.join(here,'registry/final-definer-closures.proposed.json')));
});
test('full557 current-source callee rows and actual SQL creation order retain truthful native holds',()=>{
 const dependencies=json('DEPENDENCY_REVIEW.json');assert.equal(dependencies.rows.length,557);assert.deepEqual(dependencies.unresolved,[]);assert.equal(dependencies.rootCommit,M.rootPrerequisiteCheckpoint);
 const start=dependencies.rows.filter(r=>r.caller===R.signature);assert.deepEqual(start.map(r=>r.calleeName),['private.weekly_source_query_require_service_v1']);
 const first=new Map();for(const[i,d]of selected.entries())if(!first.has(d.signature.split('(')[0]))first.set(d.signature.split('(')[0],i);
 for(const[i,d]of selected.entries())if(/\blanguage\s+sql\b/i.test(d.sql.split(/\bas\s+\$/i)[0]))for(const call of dependencies.rows.filter(r=>r.caller===d.signature&&r.proposalSignatures.length))assert(first.get(call.calleeName)<i);
 assert.equal(M.native.compile,'NOT_RUN');assert.equal(M.native.provider,'NOT_RUN');assert(M.releaseHolds.some(h=>h.includes('Banking')));
});
test('current complete per-target START/common admission/RESULT owners are retained; proposal shadows none',()=>{
 for(const p of R.protectedOwnerSignatures.filter(p=>p!==R.signature))assert(!selected.some(d=>d.signature===p));
 for(const p of R.nativeDispatchOwnerPins){const bytes=blob(M.rootPrerequisiteCheckpoint,p.path),native=defs(bytes.toString(),p.path).find(d=>d.signature===p.signature);assert(native);assert.equal(sha(native.sql),p.sha256);assert.equal(sha(bytes),p.sourceFileSha256);assert(!selected.some(d=>d.signature===p.signature));}
 const start=read('evidence/public_weekly_source_message_dispatch_target_start_atomic_v1.sqlinc');assert(start.indexOf('public.weekly_contact_submission_start_v1(')<start.indexOf('insert into public.weekly_message_target_attempts'));assert.match(start,/private\.weekly_contact_source_permit_v1\(/);assert.match(start,/'call_once',false/);
 const result=read('evidence/public_weekly_source_message_dispatch_target_result_atomic_v1.sqlinc');assert(result.indexOf('private.weekly_contact_source_result_v1(')<result.indexOf('update public.weekly_message_target_attempts'));assert.match(result,/private\.weekly_source_delivery_aggregate_command_v1\(v_target\.dispatch_command_id\)/);
 const commandResult=read('evidence/public_weekly_source_message_dispatch_result_atomic_v1.sqlinc');assert.match(commandResult,/WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED/);assert(!selected.some(d=>d.signature===R.legacyCommandResultRetained.chain.signature));
});
const uuid='00000000-0000-4000-8000-000000000001',deployment={agency_id:uuid,environment:'TEST'};
function actualDeliver(outcome,start){const text=read('payload/broker/src/weekly-source/delivery-runtime.mjs'),a=text.indexOf('async function deliverTargets('),b=text.indexOf('\nexport async function runWeeklySourceDelivery',a);assert(a>=0&&b>a);const body=text.slice(a,b),calls=[],sends=[];
 const fn=new Function('rpc','safeUuid','upper','weeklySourceDeploymentContext','safeErrorCode','deliverCandidateTarget','deliverManagerTarget','console',body+'\nreturn deliverTargets;')(
  async(_deps,name,request)=>{calls.push({name,request});if(name.endsWith('_target_claim_v1'))return{claimed_count:1,targets:[{dispatch_target_id:uuid,lease_token:uuid,channel:'EMAIL'}]};if(name.endsWith('_target_start_atomic_v1')){if(start instanceof Error)throw start;return start;}if(name.endsWith('_target_result_atomic_v1'))return{ok:true};throw Error('Unexpected authority '+name);},
  v=>typeof v==='string'&&/^[0-9a-f-]{36}$/i.test(v)?v:null,v=>String(v??'').toUpperCase(),()=>deployment,()=> 'TEST_REFUSAL',async()=>{throw Error('Unexpected PUSH');},async()=>{sends.push('EMAIL');return outcome;},{warn(){}});
 return{calls,sends,run:()=>fn({}, {},'source-review-worker',2)};
}
test('actual contributed delivery body calls actual per-target START then per-target RESULT exactly once',async()=>{
 const t=actualDeliver({outcome:'ACCEPTED',provider_message_id:'review-mock-only',bounded_provider_receipt:{}},{ok:true,provider_attempt_id:uuid,call_once:true});const result=await t.run();assert.equal(result.accepted,1);assert.deepEqual(t.sends,['EMAIL']);assert.deepEqual(t.calls.map(c=>c.name),['weekly_source_message_dispatch_target_claim_v1','weekly_source_message_dispatch_target_start_atomic_v1','weekly_source_message_dispatch_target_result_atomic_v1']);assert.equal(t.calls[1].request.dispatch_target_id,uuid);assert.equal(t.calls[2].request.provider_attempt_id,uuid);
});
test('actual delivery refusal/raised START/replayed original attempt causes zero provider sends and zero result calls',async()=>{
 for(const start of [{ok:false,reason:'CONTACT_PAUSED'},Error('CONTACT_REFUSED'),{ok:true,provider_attempt_id:uuid,call_once:true,replay:true},{ok:true,provider_attempt_id:uuid,call_once:false}]){const t=actualDeliver({outcome:'ACCEPTED'},start),result=await t.run();assert.equal(t.sends.length,0);assert.equal(t.calls.filter(c=>c.name.endsWith('_target_result_atomic_v1')).length,0);assert.equal(result.accepted,0);}
});
test('actual current runtime parent still has only three bounded recheck contributions and reverses completely',()=>{
 const proof=json('RUNTIME_REVERSE_PROOF.json');assert.deepEqual(proof.anchorCounts,[1,1,1]);let text=read('payload/'+proof.parentPath);assert.equal(sha(text),proof.resultCanonicalLfSha256);for(const e of [...proof.edits].reverse()){assert.equal(text.split(e.after).length,2);text=text.replace(e.after,e.before);}assert.equal(sha(text),proof.parentCanonicalLfSha256);assert.equal(text,blob(proof.rootCommit,proof.parentPath).toString().replaceAll('\r\n','\n'));
});
test('all reviewed historical manifests/reviews remain pinned; no DB/native/provider/release assertion is smuggled in',()=>{
 for(const p of [...R.historicalSourcePins,...R.reviewPins])assert.equal(sha(blob(p.commit,p.path)),p.sha256);
 for(const name of ['database','locks','docker','release'])assert.equal(M.native[name],false);for(const name of ['compile','acl','concurrency','api','partB','provider'])assert.equal(M.native[name],'NOT_RUN');
});
