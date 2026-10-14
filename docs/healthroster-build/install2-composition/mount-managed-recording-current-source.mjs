// One-time source composition; no release, recorder, SQL session or source-tree capture is invoked.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';
const input='b5c1ea1b002b2d6b756707c020b6a170cc9fea80',commit='b70ef1b331a3654a0c7e001fa74825cd25ed2f8c';
const sha=b=>createHash('sha256').update(b).digest('hex');
const object=file=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0','show',commit+':'+file],{maxBuffer:64*1024*1024,stdio:['ignore','pipe','pipe']});
assert.equal(execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),input);
const recipeFile='docs/healthroster-build/MONEY_MANAGED_RECORDING_FINAL_MOUNT_RECIPE_14102026.json';
const recipeBytes=object(recipeFile);assert.equal(sha(recipeBytes),'52fd126efe47b2b2f9264c24cc12a38e52271595fe170a6d465e216cb3a89703');
const recipe=JSON.parse(recipeBytes);assert.equal(recipe.current_root_source_commit,input);
const prepared=[],replacements=[];
function prepare(file,bytes,sourceFile,expected=sha(bytes)){
 assert.ok(!file.split('/').includes('..'));assert.equal(sha(bytes),expected);assert.ok(!fs.existsSync(file),'NEW_TARGET_EXISTS '+file);
 prepared.push({file,bytes,sha256:expected,sourceCommit:commit,sourceFile});
}
for(const p of recipe.reviewed_packets){
 const bytes=object(p.manifest_file);assert.equal(sha(bytes),p.manifest_sha256);const m=JSON.parse(bytes);assert.equal(m.files.length,p.artifacts);
 for(const row of m.files){const raw=object(row.file);assert.equal(raw.length,row.bytes);prepare(row.file,raw,row.file,row.sha256);}
 prepare(p.manifest_file,bytes,p.manifest_file,p.manifest_sha256);
}
const base='docs/healthroster-build/proposals/';
const json=file=>JSON.parse(object(base+file));
const current=json('managed-qualification-current-root-install2-forward/COMPLETE_CURRENT_SOURCE_REVERSE.json');
const writer=json('managed-qualification-setup-mode-successor-20261014/COMPLETE_CURRENT_MANAGED_REVERSE.json');
const entry=json('protected-record-admission-successor-20261014/COMPLETE_PROTECTED_ENTRY_REVERSE.json');
const originalWorkflow=json('protected-record-evidence-current-root-install2-forward/COMPLETE_CURRENT_WORKFLOW_REVERSE.json');
const p10=json('p10-same-descriptor-successor-20261014/COMPLETE_P10_DESCRIPTOR_REVERSE.json');
const undo=(bytes,hunks)=>{let text=bytes.toString('utf8');for(const h of [...hunks].reverse()){assert.equal(text.split(h.after).length,2,'REVERSE_NOT_UNIQUE');if(h.before==='START_OF_MODULE')assert.ok(text.startsWith(h.after),'START_OF_MODULE_ANCHOR_MOVED');text=text.replace(h.after,()=>h.before==='START_OF_MODULE'?'':h.before);}return Buffer.from(text);};
let wholeHunks=0;
for(const row of recipe.only_five_complete_current_composed_or_new_source_outputs){
 const after=object(row.source_file);assert.equal(after.length,row.source_bytes);assert.equal(sha(after),row.source_sha256);
 const before=row.actual_current_input.absent?null:fs.readFileSync(row.destination);
 if(before){assert.equal(before.length,row.actual_current_input.bytes);assert.equal(sha(before),row.actual_current_input.sha256);}
 else assert.ok(!fs.existsSync(row.destination));
 let reverse;
 if(row.name==='P10'){
  reverse=undo(undo(after,p10.hunks),p10.current_bounded_hunks);wholeHunks+=p10.hunks.length+p10.current_bounded_hunks.length;
 }else if(row.name==='MANAGED_WRITER'){
  reverse=undo(undo(undo(after,[writer.setup_mode_hunk]),[writer.prerequisite_key_hunk]),writer.original_current_hunks);wholeHunks+=2+writer.original_current_hunks.length;
 }else if(row.name==='CURRENT_RELEASE_CALLER'){
  const proof=current.proofs.find(p=>p.destination===row.destination);assert.ok(proof);reverse=undo(after,proof.hunks);wholeHunks+=proof.hunks.length;
 }else if(row.name==='PROTECTED_RECORD_WORKFLOW'){
  reverse=undo(undo(undo(after,entry.workflow_hunks),originalWorkflow.recording_hunks),originalWorkflow.managed_existing_hunks);wholeHunks+=entry.workflow_hunks.length+originalWorkflow.recording_hunks.length+originalWorkflow.managed_existing_hunks.length;
 }else if(row.name==='PROTECTED_RECORD_ENTRY'){
  const predecessor=undo(after,[entry.entry_hunk]);assert.equal(sha(predecessor),entry.entry_before_sha256);wholeHunks++;
 }else throw Error('UNREVIEWED_OUTPUT');
 if(before){assert.ok(reverse.equals(before),'WHOLE_CURRENT_REVERSE_FAILED '+row.destination);replacements.push({file:row.destination,bytes:after,sha256:row.source_sha256,beforeSha256:sha(before),sourceCommit:commit,sourceFile:row.source_file,wholeCurrentReverse:true});}
 else prepare(row.destination,after,row.source_file,row.source_sha256);
}
for(const row of recipe.existing_actual_current_dependency_inputs){assert.equal(sha(fs.readFileSync(row.file)),row.sha256);}
for(const file of [recipeFile,'docs/healthroster-build/MONEY_A0_LATE_POSITION_DISJOINT_14102026.json','docs/healthroster-build/MONEY_MANAGED_RECORDING_HANDBACK_14102026.md','docs/healthroster-build/MONEY_SOURCE_TEST_LEFTOVER_14102026.json'])prepare(file,object(file),file);
prepare('docs/healthroster-build/install2-composition/MONEY_LANE_PROGRESS_AT_b70ef1b.md',object('docs/healthroster-build/PROGRESS.md'),'docs/healthroster-build/PROGRESS.md');
for(const row of [...prepared,...replacements]){fs.mkdirSync(path.dirname(row.file),{recursive:true});fs.writeFileSync(row.file,row.bytes);}
const receipt={sourceOnly:true,status:'MOUNTED_CONCRETE_PROTECTED_RECORDING_SOURCE_NATIVE_ENTRY_AND_SEAL_HELD',priorHeldSteps:[{stage:'WHOLE_CURRENT_REVERSE',cause:'Root mount reader initially treated the reviewed START_OF_MODULE symbolic anchor as literal source.',sourceInputsWritten:false,correction:'Recognise only the exact symbolic module-start anchor, require its inserted prefix at offset zero, and reverse to empty bytes.'}],rootInputCommit:input,publication:commit,recipeSha256:sha(recipeBytes),reviewedPackets:recipe.reviewed_packets,completeCurrentOwnerHunks:wholeHunks,onlyFiveSelectedSourceOutputs:true,existingDependenciesPreserved:recipe.existing_actual_current_dependency_inputs.length,receiptExceptions:3,recorderRequestFields:9,recorderResponseFields:13,noSqlOrRuntimeOwnerChange:true,files:prepared.map(({bytes,...row})=>row),replacements:replacements.map(({bytes,...row})=>row),noReleaseRecorderSqlOrWholeTreeCaptureInvocation:true};
fs.writeFileSync('docs/healthroster-build/install2-composition/MANAGED_RECORDING_SOURCE_COMPOSITION_14102026.json',JSON.stringify(receipt,null,2)+'\n');
console.log(JSON.stringify({sourceFiles:prepared.length,replacedCurrentOwners:replacements.length,wholeCurrentHunks:wholeHunks,selectedOutputs:5,existingDependenciesPreserved:receipt.existingDependenciesPreserved,nativeInvocation:false}));
