// One-time reviewed source mount; no database, release or qualification action.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';
const input='74a9c10f7370cd3641ee95d31f6702ecadd900a7',publication='6606b5c2617b0e8af7c07d33ac48bbb60ec6e32d';
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{maxBuffer:16*1024*1024,stdio:['ignore','pipe','pipe']});
const object=file=>git(['show',publication+':'+file]);
assert.equal(git(['rev-parse','HEAD']).toString().trim(),input);
const recipeFile='docs/healthroster-build/MONEY_FIN4_REBILL_RECORD_FIELDS_MOUNT_RECIPE_14102026.json',recipeBytes=object(recipeFile);
assert.equal(sha(recipeBytes),'7d5f14409d5aecdc7ab3e76bbedaba1162634955c4d9c3c02b27663eb2867b38');
const recipe=JSON.parse(recipeBytes),manifestBytes=object(recipe.manifest_path);assert.equal(sha(manifestBytes),recipe.manifest_sha256);
const manifest=JSON.parse(manifestBytes);assert.equal(manifest.artifact_count,8);assert.equal(manifest.artifacts.length,8);
const prepared=[];
function prepare(file,bytes,sourceFile,expected=sha(bytes)){assert.ok(!file.split('/').includes('..'));assert.ok(!fs.existsSync(file),'TARGET_EXISTS '+file);assert.equal(sha(bytes),expected);prepared.push({file,bytes,sourceFile,sha256:expected});}
for(const row of manifest.artifacts){const file=recipe.proposal_root+'/'+row.file,bytes=object(file);assert.equal(bytes.length,row.bytes);prepare(file,bytes,file,row.sha256);}
prepare(recipe.manifest_path,manifestBytes,recipe.manifest_path,recipe.manifest_sha256);
for(const file of [recipeFile,'docs/healthroster-build/MONEY_FIN4_REBILL_RECORD_FIELDS_HANDBACK_14102026.md'])prepare(file,object(file),file);
const before=fs.readFileSync(recipe.source_root_current_owner),after=object(recipe.selected_complete_forward);
assert.equal(sha(before),recipe.source_root_current_owner_sha256);assert.equal(sha(after),recipe.selected_complete_forward_sha256);
const proof=JSON.parse(object(recipe.proposal_root+'/COMPLETE_CURRENT_OWNER_AND_REVERSE.json'));assert.equal(proof.hunks.length,1);
let reverse=after.toString();for(const h of [...proof.hunks].reverse()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,()=>h.before);}assert.ok(Buffer.from(reverse).equals(before));
const rootFile='supabase/repeatable/14102026_0821_healthroster_fin4_rebill_record_fields_v1.sql',leaf='supabase/repeatable/14102026_0821_healthroster_fin4_rebill_record_fields/01_complete_fin4.sqlinc';
prepare(leaf,after,recipe.selected_complete_forward,recipe.selected_complete_forward_sha256);
prepare(rootFile,Buffer.from("-- Complete reviewed FIN4 successor; four record-field accesses only.\n-- Source packaging is not compiled or native qualification.\n\\set ON_ERROR_STOP on\nbegin;\n\\ir 14102026_0821_healthroster_fin4_rebill_record_fields/01_complete_fin4.sqlinc\nalter function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text) owner to postgres;\nrevoke all on function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text) from public,anon,authenticated,service_role;\nnotify pgrst, 'reload schema';\ncommit;\n"),'ROOT_ATOMIC_PRIVATE_WRAPPER');
for(const row of prepared){fs.mkdirSync(path.dirname(row.file),{recursive:true});fs.writeFileSync(row.file,row.bytes,{flag:'wx'});}
const receipt={sourceOnly:true,inputCommit:input,publication,status:'FIN4_RECORD_FIELD_SOURCE_FIX_NATIVE_AND_FINAL_COUPLING_HELD',rootFile,leaf,wholeCurrentReversibleHunks:1,changedRecordAccesses:4,signature:recipe.signature,signatureDelta:0,serviceOrBrowserGrantDelta:0,priorOwnerFile:recipe.source_root_current_owner,priorOwnerSha256:sha(before),mountedOwnerSha256:sha(after),compiledContract:null,nativeCalls:0,finalVerifierCatalogueAndQualification:'HOLD',files:prepared.map(({bytes,...r})=>({...r,bytes:bytes.length}))};
fs.writeFileSync('docs/healthroster-build/install2-composition/FIN4_RECORD_FIELDS_SOURCE_COMPOSITION_14102026.json',JSON.stringify(receipt,null,2)+'\n',{flag:'wx'});console.log(JSON.stringify({pins:prepared.length,rootFile,leaf,reversibleHunks:1,nativeCalls:0}));
