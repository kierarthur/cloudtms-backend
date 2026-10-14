// One-time source composition only. No database, release, recorder or qualification is invoked.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';
const input='eb744b66809685c61a2db60249d617eb818115a4',publication=process.argv[2];
assert.match(publication??'',/^[0-9a-f]{40}$/);
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{maxBuffer:32*1024*1024,stdio:['ignore','pipe','pipe']});
const object=file=>git(['show',publication+':'+file]);
assert.equal(git(['rev-parse','HEAD']).toString().trim(),input);
const recipeFile='docs/healthroster-build/MONEY_FINAL_CLASSIFICATION_MOUNT_RECIPE_14102026.json';
const recipeBytes=object(recipeFile);assert.equal(sha(recipeBytes),'6f3ca2a145839cb499e900a09fb18c5629c9f011bc873a61e01c9cfc50038698');
const recipe=JSON.parse(recipeBytes);assert.equal(recipe.current_root_selected_input_commit,input);
const manifestBytes=object(recipe.review_manifest.file);assert.equal(sha(manifestBytes),recipe.review_manifest.sha256);
const manifest=JSON.parse(manifestBytes);assert.equal(manifest.files.length,30);
const prepared=[],replacements=[];
function prepare(file,bytes,sourceFile,expected=sha(bytes)){
 assert.ok(!file.split('/').includes('..'));assert.ok(!fs.existsSync(file),'NEW_TARGET_EXISTS '+file);assert.equal(sha(bytes),expected);
 prepared.push({file,bytes,sha256:expected,sourceCommit:publication,sourceFile});
}
for(const row of manifest.files){const bytes=object(row.file);assert.equal(bytes.length,row.bytes);prepare(row.file,bytes,row.file,row.sha256);}
prepare(recipe.review_manifest.file,manifestBytes,recipe.review_manifest.file,recipe.review_manifest.sha256);
const proofFile='docs/healthroster-build/proposals/current-final-classification-union-20261014/COMPLETE_FORWARD_AND_REVERSE_PROOFS.json';
const proofs=JSON.parse(object(proofFile));let hunks=0;
const undo=(bytes,rows)=>{let text=bytes.toString();for(const row of [...rows].reverse()){assert.equal(text.split(row.after).length,2,'REVERSE_NOT_UNIQUE');text=text.replace(row.after,()=>row.before);}return Buffer.from(text);};
const rootFile='supabase/repeatable/14102026_0709_healthroster_current_literal_classification_v1.sql';
const leaf='supabase/repeatable/14102026_0709_healthroster_current_literal_classification/01_complete_current_literal_classification.sqlinc';
let prefixLengths=null;
for(const row of recipe.selected){
 const before=fs.readFileSync(row.consumed_actual_current_input.file),after=object(row.selected_forward.file);
 assert.equal(before.length,row.consumed_actual_current_input.bytes);assert.equal(sha(before),row.consumed_actual_current_input.sha256);
 assert.equal(after.length,row.selected_forward.bytes);assert.equal(sha(after),row.selected_forward.sha256);
 const proof=proofs.proofs.find(p=>p.input_file===row.consumed_actual_current_input.file);assert.ok(proof);
 if(row.operation==='APPEND_ONLY_REQUIRED_VERIFIER_LIST_DELTA_PRESERVE_LATER_ROOT_RELEASE_ID_CONTRACT_FIELDS'){
  assert.equal(row.reverse_hunks,0);assert.equal(proof.hunks,undefined);
  const previous=JSON.parse(before),next=JSON.parse(after),remainingPrevious={...previous},remainingNext={...next};
  const lengths={};for(const key of ['verificationFiles','newVerificationFiles']){
   assert.deepEqual(next[key].slice(0,previous[key].length),previous[key]);assert.equal(next[key].length-previous[key].length,15);assert.equal(new Set(next[key]).size,next[key].length);
   lengths[key]={before:previous[key].length,after:next[key].length};delete remainingPrevious[key];delete remainingNext[key];
  }assert.deepEqual(remainingNext,remainingPrevious);prefixLengths=lengths;
 }else{assert.equal(proof.hunks.length,row.reverse_hunks);assert.ok(undo(after,proof.hunks).equals(before),'WHOLE_CURRENT_REVERSE_FAILED');hunks+=proof.hunks.length;}
 if(row.destination==='NEW_UK_MINUTE_FINAL_REPEATABLE_CONTAINED_SUPPORT_AFTER_ALL_NATIVE_A0_REPORT')prepare(leaf,after,row.selected_forward.file,row.selected_forward.sha256);
 else replacements.push({file:row.destination,bytes:after,sha256:row.selected_forward.sha256,beforeSha256:sha(before),sourceCommit:publication,sourceFile:row.selected_forward.file});
}
for(const row of recipe.preserved_verifier_input_pins){assert.equal(sha(fs.readFileSync(row.file)),row.sha256);}
prepare(rootFile,Buffer.from("-- Reviewed current-source classification checkpoint; future native seams require a final census.\nbegin;\n\\ir 14102026_0709_healthroster_current_literal_classification/01_complete_current_literal_classification.sqlinc\ncommit;\n"),'ROOT_CONTAINED_ATOMIC_WRAPPER');
for(const file of [recipeFile,'docs/healthroster-build/MONEY_FINAL_CLASSIFICATION_HANDBACK_14102026.md','docs/healthroster-build/MONEY_A0_RPC_ISOLATION_SOURCE_BOUNDARY_14102026.json','docs/healthroster-build/MONEY_A0_RPC_ISOLATION_SOURCE_BOUNDARY_14102026.md'])prepare(file,object(file),file);
for(const row of [...prepared,...replacements]){fs.mkdirSync(path.dirname(row.file),{recursive:true});fs.writeFileSync(row.file,row.bytes);}
const receipt={sourceOnly:true,status:'CURRENT_SOURCE_CLASSIFICATION_CHECKPOINT_FINAL_IMPLEMENTATION_CENSUS_AND_NATIVE_GATES_HELD',priorHeldCheck:{cause:'Root mount reader initially expected a text-hunk array on the separately reviewed semantic append-only JSON proof.',sourceInputsWritten:false,correction:'Require exactly the reviewed zero-hunk append operation, compare both full ordered prefixes and all unrelated JSON fields; keep literal whole-file reverse checks on the three SQL owners.'},inputCommit:input,publication,rootFile,leaf,wholeCurrentHunks:hunks,verifierPrefixes:prefixLengths,preservedVerifierInputs:recipe.preserved_verifier_input_pins.length,scopedOwnerOnly:545,unchangedScopedService:142,privateService:8,scopedBrowser:0,publicTableClasses:105,lifecycleColumns:67,privateTableFacts:14,compiledContract:null,nativeCalls:0,futureSeams:'Planned Checklist/Undo/original-read and any accepted A0 isolation fix require fresh final source/ACL/verifier census before sealing.',files:prepared.map(({bytes,...row})=>({...row,bytes:bytes.length})),replacements:replacements.map(({bytes,...row})=>({...row,bytes:bytes.length}))};
fs.writeFileSync('docs/healthroster-build/install2-composition/CURRENT_CLASSIFICATION_SOURCE_COMPOSITION_14102026.json',JSON.stringify(receipt,null,2)+'\n');
console.log(JSON.stringify({archivedAndNew:prepared.length,replacements:replacements.length,hunks,prefixLengths,rootFile,nativeCalls:0}));
