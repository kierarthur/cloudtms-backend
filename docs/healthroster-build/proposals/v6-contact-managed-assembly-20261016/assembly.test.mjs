import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const i2=path.resolve(repo,'../healthroster-install2-14102026');
const sha=value=>createHash('sha256').update(value).digest('hex');
const read=file=>fs.readFileSync(path.join(here,file),'utf8');
const json=file=>JSON.parse(read(file));
const manifest=json('ASSEMBLY_MANIFEST.json'),dependencies=json('DEPENDENCY_REVIEW.json');
const {scanManagedSql,readManagedClosure,exactAtomicBody}=await import(pathToFileURL(path.join(i2,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(i2,'scripts/bpay-next-final-definers.mjs')));
function blob(commit,file){const r=spawnSync('git',['show',`${commit}:${file}`],{cwd:repo,maxBuffer:10_000_000,windowsHide:true});assert.equal(r.status,0,r.stderr.toString());return r.stdout;}
function functionStatements(text,file){return scanManagedSql(text).statements.flatMap(row=>{
 const sql=text.slice(row.start,row.end).trim();return routineDefinitions(sql,file).map(signature=>({signature,sql}));
});}
const roots=manifest.orderedRepeatableRoots.map(file=>readManagedClosure(file,path.join(here,'payload')));
test('complete managed closure retains all frozen106 functions/effects and explicit five genuine prerequisites',()=>{
 assert.equal(manifest.rawFrozenPayloadsUnchanged,56);assert.equal(manifest.counts.frozenRepeatableLeaves,53);
 assert.equal(manifest.counts.allUniqueFunctions,111);assert.equal(manifest.counts.repeatableFunctionDeclarations,112);
 assert.equal(manifest.counts.totalFunctionDeclarations,113);assert.equal(manifest.counts.allManagedStatements,411);
 const definitions=roots.flatMap(row=>{exactAtomicBody(row.expanded);return functionStatements(row.expanded,row.path);});
 assert.equal(definitions.length,112);assert.equal(new Set(definitions.map(row=>row.signature)).size,111);
 for(const row of manifest.payloads){const bytes=fs.readFileSync(path.join(here,'payload',row.path));assert.equal(sha(bytes),row.sha256);assert.equal(bytes.length,row.bytes);assert(!bytes.includes(Buffer.from('\r\n')));}
});
test('all56 complete selected raw payloads reverse exactly to frozen e466 and transaction wrappers only surround schema',()=>{
 for(const row of manifest.sourcePins.filter(row=>row.sourceCommit===manifest.producerCommit)){
  const source=blob(row.sourceCommit,row.sourcePath),payload=fs.readFileSync(path.join(here,'payload',row.target));
  assert.equal(sha(source),row.sourceSha256);const extracted=row.phase==='schema'?payload.subarray(row.sourceByteOffset,row.sourceByteOffset+row.sourceBytes):payload;
  assert.deepEqual(extracted,source,row.sourcePath);
 }
});
test('complete guard successor has exactly the reviewed kind addition and is followed by final original ACL',()=>{
 const correction=json('metadata/ORDINAL_GUARD_CORRECTION.json'),row=manifest.sourcePins.find(item=>item.phase==='complete-successor');
 const complete=read('payload/'+row.target),frozen=blob(correction.source.commit,correction.source.path).toString();
 const old=functionStatements(frozen,'old').find(item=>item.signature===correction.source.signature).sql;
 assert.equal(sha(complete),'650f736b80474576dcfc72ffba5eecbbb0aac954b893e3fb2e1cc81c3d83d796');
 assert.equal(complete.trim().replace(correction.correction.new,correction.correction.old),old);
 assert(manifest.orderedRepeatableLeaves.at(-2).endsWith('/98_manual_selection_ordinal_guard.sqlinc'));
 assert(manifest.orderedRepeatableLeaves.at(-1).endsWith('/99_acl.sqlinc'));
});
test('five prerequisite functions and ten ACL statements equal complete original18f source selections',()=>{
 const proof=json('PREREQUISITE_SOURCE_PROOF.json'),actual=functionStatements(read('payload/'+proof.targetFunctionPath),'prerequisite');
 assert.equal(actual.length,5);assert.equal(proof.initialDirectMissing,3);assert.equal(proof.additionalTransitiveMissing,2);
 for(const row of proof.orderedFunctions){const text=blob(row.sourceCommit,row.sourcePath).toString();
  assert.equal(sha(text),row.wholeFileSha256);const old=functionStatements(text,'original').find(item=>item.signature===row.signature);
  const saved=actual.find(item=>item.signature===row.signature);assert.equal(saved.sql,old.sql);assert.equal(sha(saved.sql),row.completeFunctionStatementSha256);
 }
 const originalAcl=blob(proof.sourceCommit,proof.acl.sourcePath).toString();
 const acl=read('payload/'+proof.acl.target),statements=scanManagedSql(acl).statements;
 assert.equal(statements.length,10);for(const row of statements)assert(originalAcl.includes(acl.slice(row.start,row.end).trim()));
 assert(!/\bgrant\s+execute\b/i.test(acl));
});
test('actual full qualified callee census and manually reviewed prerequisite source columns have no unresolved dependency',()=>{
 assert.deepEqual(dependencies.unresolved,[]);assert(dependencies.rows.length>500);
 const relations=json('PREREQUISITE_RELATION_REVIEW.json');assert.deepEqual(relations.missingColumns,[]);
 assert.equal(relations.relations.length,21);for(const row of relations.relations)for(const column of row.columns)assert(column.sourceEvidence.length>0);
 const definitions=roots.flatMap(root=>functionStatements(root.expanded,root.path)),firstByName=new Map();
 for(const [index,row]of definitions.entries()){const name=row.signature.split('(')[0];if(!firstByName.has(name))firstByName.set(name,index);}
 for(const [index,definition]of definitions.entries()){
  const header=definition.sql.split(/\bas\s+\$/i)[0];if(!/\blanguage\s+sql\b/i.test(header))continue;
  for(const row of dependencies.rows.filter(item=>item.caller===definition.signature&&item.proposalSignatures.length))
   assert(firstByName.get(row.calleeName)<index,`CREATE-TIME_ORDER ${definition.signature} -> ${row.calleeName}`);
 }
});
test('full registry retains all prior chains, companions, editor fields and selects1209 with changed1210',()=>{
 const delta=json('registry/FINAL_DEFINER_DELTA.json'),old=JSON.parse(blob(delta.rootCommit,delta.registryPath)),proposed=json('registry/final-definer-closures.proposed.json');
 assert.equal(sha(blob(delta.rootCommit,delta.registryPath)),delta.baseSha256);assert.equal(delta.rows.length,111);
 assert.equal(delta.updated,12);assert.equal(delta.added,99);assert.equal(proposed.chains.length,1487);
 for(const key of Object.keys(old).filter(key=>key!=='chains'))assert.deepEqual(proposed[key],old[key]);
 const changed=new Set(delta.rows.map(row=>row.signature));for(const before of old.chains){const after=proposed.chains.find(row=>row.signature===before.signature);assert(after);
  if(!changed.has(before.signature)){assert.deepEqual(after,before);continue;}
  for(const field of ['allowedPhysical','predecessors','finalClosures'])for(const item of before[field]??[])assert(after[field].includes(item));
  for(const key of Object.keys(before).filter(key=>!['finalRoot','finalPhysical','allowedPhysical','predecessors','finalClosures'].includes(key)))assert.deepEqual(after[key],before[key]);
 }
 for(const row of delta.rows.filter(row=>row.after.finalRoot.includes('_1209_')))assert(row.after.predecessors.includes(manifest.reservedRoot));
});
test('both corrected metadata documents reverse with only their exact one-field hash substitution',()=>{
 const recipe=JSON.parse(blob(manifest.producerCommit,'docs/healthroster-build/proposals/source-contact-producer-continuation-20261016/SOURCE_COMPLETION_RECIPE.json'));
 const corrected=json('metadata/CONTACT_SOURCE_COMPLETION_RECIPE_REVIEWED_SUCCESSOR.json');assert.equal(corrected.assembly.repeatable.at(-1).sha256,'007857838e847fc6c51f61a778f7a2c7c0ca523ae960f42d0c766237d3f7d197');
 corrected.assembly.repeatable.at(-1).sha256=recipe.assembly.repeatable.at(-1).sha256;assert.deepEqual(corrected,recipe);
 const prior=JSON.parse(blob(manifest.producerCommit,'docs/healthroster-build/proposals/source-contact-producer-continuation-20261016/RUNTIME_CONTRIBUTION.json'));
 const runtime=json('metadata/RUNTIME_CONTRIBUTION_REVIEWED_SUCCESSOR.json');assert.equal(runtime.leaf.sha256,'6bf1eed7b3dd86b984217c91b37a4a8d6b786511f712ea6daa87cf8f0a41e8ff');
 runtime.leaf.sha256=prior.leaf.sha256;assert.deepEqual(runtime,prior);
});
test('complete actual current delivery parent reverses exactly after only the three finite edits',()=>{
 const proof=json('RUNTIME_REVERSE_PROOF.json');assert.deepEqual(proof.anchorCounts,[1,1,1]);
 let parent=read('payload/'+proof.parentPath);assert.equal(sha(parent),'b5b7109136fe51b26b643a4b4381d411e56efc8009a7ff0ce67e6b7c763cdaa5');
 for(const edit of [...proof.edits].reverse()){assert.equal(parent.split(edit.after).length,2);parent=parent.replace(edit.after,edit.before);}
 assert.equal(sha(parent),'cda0983c5292f6f86c6c812cccd176057ce8dc34059753e3009d25bcf398ea75');
 assert.equal(parent,blob(proof.rootCommit,proof.parentPath).toString().replaceAll('\r\n','\n'));
});
const {runWeeklySourceContactRecheck}=await import(pathToFileURL(path.join(here,'payload/broker/src/weekly-source/contact-recheck-runtime.mjs')));
const id='00000000-0000-4000-8000-000000000010';
function actualFunction(events){const source=read('payload/broker/src/weekly-source/delivery-runtime.mjs');
 const start=source.indexOf('export async function runWeeklySourceDelivery('),end=source.indexOf('\nexport async function handleWeeklySourceDeliveryRuntime',start);assert(start>=0&&end>start);
 const body=source.slice(start,end).replace(/^export /,''),mark=name=>async()=>{events.push(name);return {ok:true};};
 return new Function('text','canonicalServerUtc','rpc','renderDueIntents','prepareDispatchTargets','deliverTargets','runWeeklySourceCompletedPackCopies',
  'runPendingEntitlementRelease','safeErrorCode','weeklySourceDeploymentContext','runWeeklySourceContactRecheck',body+'\nreturn runWeeklySourceDelivery;')(
  value=>String(value??'').trim(),value=>new Date(value).toISOString(),async(deps,name,request)=>{events.push(name);return deps.rpc(name,{p_request:request});},
  mark('render'),mark('prepare'),mark('deliver'),mark('pack'),mark('release'),()=> 'SAFE_FAULT',()=>({agency_id:id,environment:'TEST'}),runWeeklySourceContactRecheck);
}
test('actual contributed current parent runs one bounded recheck between original scheduler and original rendering/delivery/release',async()=>{
 const events=[],run=actualFunction(events),result=await run({}, {rpc:async(name,args,options)=>{
  if(name==='weekly_source_contact_recheck_batch_v1'){events.push(name);assert.equal(args.p_request.limit,3);assert.equal(options.timeoutMs,30_000);
   return {ok:true,version:'CONTACT_RECHECK_BATCH_RESULT_V1',processed:0,held:0,selection_complete:false,results:[]};}return {ok:true};}},
 {workerId:'source-test-worker',limit:3,scheduledStartedAtUtc:'2026-10-16T12:00:00.000Z'});
 assert.deepEqual(events,['weekly_source_query_scheduler_tick_v1','weekly_source_contact_recheck_batch_v1','render','prepare','deliver','pack','release']);
 assert.equal(result.contact_recheck.selection_complete,false);assert.equal(result.delivery.ok,true);
});
test('actual contributed current parent isolates thrown and malformed recheck replies while preserving all original owners',async()=>{
 for(const mode of ['throw','malformed']){const events=[],run=actualFunction(events),result=await run({}, {rpc:async name=>{
  if(name==='weekly_source_contact_recheck_batch_v1'){events.push(name);if(mode==='throw')throw Error('private detail');return {ok:true};}return {ok:true};}},
  {workerId:'source-test-worker',limit:3,scheduledStartedAtUtc:'2026-10-16T12:00:00.000Z'});
  assert.deepEqual(result.contact_recheck,{ok:false,error_code:'CONTACT_RECHECK_UNAVAILABLE',processed:0,held:0,results:[]});
  assert.deepEqual(events.slice(-5),['render','prepare','deliver','pack','release']);
 }
});
test('all qualifications remain source-only and original416e review is unchanged',()=>{
 for(const file of ['REVIEW.md','FROZEN_SOURCE_REVIEW.json','SOURCE_MANIFEST.json','audit-frozen-contact.mjs']){
  const relative='docs/healthroster-build/proposals/v6-contact-producer-review-20261016/'+file;
  assert.deepEqual(fs.readFileSync(path.join(repo,relative)),blob(manifest.reviewCommit,relative));
 }
 assert.equal(manifest.native.database,false);assert.equal(manifest.native.locks,false);assert.equal(manifest.native.docker,false);
 for(const field of ['compile','acl','concurrency','api','partB'])assert.equal(manifest.native[field],'NOT_RUN');
});
