// One-time reviewed source mount. No native call, feature change, recording or release is invoked.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';
const input='2339ecb74851ebe05d7188a60c6b42e0ec83b3ca',publication='83e228be1bf39d36705e55ae9dbf74da23a54649',base='docs/healthroster-build/proposals/';
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{maxBuffer:32*1024*1024,stdio:['ignore','pipe','pipe']});
const object=file=>git(['show',publication+':'+file]);
assert.equal(git(['rev-parse','HEAD']).toString().trim(),input);
const recipeFile=base+'install2-a0-current-mount-checkpoint-20261014/FINAL_CURRENT_MOUNT_REVERSE_AND_PINS.json',recipeBytes=object(recipeFile);
assert.equal(sha(recipeBytes),'23b061cc119ec06feb9c3ae8999c113d10c381cf22df8e0f33a4f99861436db1');
const recipe=JSON.parse(recipeBytes);assert.equal(recipe.current_root_commit,'61ce56ff5a0df2e4e4a85fdbb9efc25267d3c966');assert.equal(recipe.mounts.length,4);
const prepared=[],replacements=[];
function prepare(file,bytes,sourceFile,expected=sha(bytes)){assert.ok(!file.split('/').includes('..'));assert.equal(sha(bytes),expected);assert.ok(!fs.existsSync(file),'NEW_TARGET_EXISTS '+file);prepared.push({file,bytes,sha256:expected,sourceCommit:publication,sourceFile});}
const added=git(['diff-tree','--no-commit-id','--name-only','-r',publication]).toString().trim().split('\n');assert.equal(added.length,40);
const allowed=['install2-a0-scoped-pay-permission-successor-20261014/','install2-office-correction-context-20261014/','install2-a0-current-mount-checkpoint-20261014/'].map(f=>base+f);
for(const file of added){assert.ok(allowed.some(prefix=>file.startsWith(prefix)));prepare(file,object(file),file);}
for(const freeze of recipe.preserved_freezes){const folder=base+freeze.folder+'/',manifestFile=folder+'REVIEW_MANIFEST.json',manifestBytes=object(manifestFile);assert.equal(sha(manifestBytes),freeze.sha256);const manifest=JSON.parse(manifestBytes);assert.equal(manifest.artifacts.length+1,freeze.artifacts);for(const row of manifest.artifacts){const file=folder+row.path,bytes=object(file);assert.equal(bytes.length,row.bytes);assert.equal(sha(bytes),row.sha256);if(fs.existsSync(file))assert.ok(fs.readFileSync(file).equals(bytes));else assert.ok(prepared.some(p=>p.file===file&&p.bytes.equals(bytes)));}if(fs.existsSync(manifestFile))assert.ok(fs.readFileSync(manifestFile).equals(manifestBytes));}
const undo=(bytes,hunks)=>{let text=bytes.toString();for(const h of [...hunks].reverse()){assert.equal(text.split(h.replacement).length,2,'REVERSE_NOT_UNIQUE');text=text.replace(h.replacement,()=>h.original);}return Buffer.from(text);};
let hunks=0;
for(const row of recipe.mounts){const after=object(base+row.origin);assert.equal(after.length,row.bytes);assert.equal(sha(after),row.sha256);const reversed=undo(after,row.hunks);hunks+=row.hunks.length;
 if(row.before_state==='ABSENT_CURRENT'){
  assert.ok(!fs.existsSync(row.destination));
  if(row.destination.endsWith('/timesheet-correction-office-routes.mjs'))assert.ok(reversed.equals(object(base+'install2-a0-runtime-route-wiring-20261014/forward/broker/src/timesheet-correction-office-routes.mjs')),'NEW_ROUTE_PREDECESSOR_REVERSE_FAILED');
  else assert.equal(row.hunks.length,0);
  prepare(row.destination,after,base+row.origin,row.sha256);
 }else{const before=fs.readFileSync(row.destination);assert.equal(sha(before),row.before_sha256);assert.ok(reversed.equals(before),'WHOLE_CURRENT_REVERSE_FAILED '+row.destination);replacements.push({file:row.destination,bytes:after,sha256:row.sha256,beforeSha256:row.before_sha256,sourceCommit:publication,sourceFile:base+row.origin});}
}
assert.equal(hunks,33);
for(const row of [...recipe.source_current_native_pins,...recipe.source_current_permission_pins])assert.equal(sha(fs.readFileSync(row.path)),row.sha256);
for(const row of recipe.final_literal_import_graph){const composed=[...prepared,...replacements].find(p=>p.file===row.path),bytes=composed?.bytes??fs.readFileSync(row.path);assert.equal(bytes.length,row.bytes);assert.equal(sha(bytes),row.sha256);}
for(const row of [...prepared,...replacements]){fs.mkdirSync(path.dirname(row.file),{recursive:true});fs.writeFileSync(row.file,row.bytes);}
const receipt={sourceOnly:true,status:'SCOPED_CORRECTION_AND_CONTEXT_CURRENT_SOURCE_MOUNTED_FUTURE_IMPLEMENTATION_AND_NATIVE_GATES_HELD',rootInputCommit:input,selectedOwnersReviewedAgainst:recipe.current_root_commit,noWholeRootEquivalenceClaim:true,publication,recipeFile,recipeSha256:sha(recipeBytes),selectedSourceOutputs:4,wholeUniqueReversals:hunks,nativePins:27,permissionPins:9,importObjects:134,preservedFreezeArtifacts:77,nativeCalls:0,compiledNativeSeal:null,featureChanges:0,futureSeams:'Primary R239 preset T0 request/recovery, full U3/Undo, original eligibility, kept/rekey, hidden-RR and final census remain incomplete.',files:prepared.map(({bytes,...row})=>({...row,bytes:bytes.length})),replacements:replacements.map(({bytes,...row})=>({...row,bytes:bytes.length}))};
fs.writeFileSync('docs/healthroster-build/install2-composition/SCOPED_CORRECTION_CURRENT_SOURCE_COMPOSITION_14102026.json',JSON.stringify(receipt,null,2)+'\n');console.log(JSON.stringify({newFiles:prepared.length,replacements:replacements.length,hunks,nativePins:27,permissionPins:9,importObjects:134,nativeCalls:0}));
