// One-time immutable source archive. No executable root, SQL, configuration or native state is changed.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
const input='efc03dc5f540ac128838b34376790a9aa49fc166';
const publication='126e25d0d9b1acebc331603cbd5a22970b559342';
const base='docs/healthroster-build/proposals/';
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{maxBuffer:16*1024*1024,stdio:['ignore','pipe','pipe']});
const object=file=>git(['show',publication+':'+file]);
assert.equal(git(['rev-parse','HEAD']).toString().trim(),input);
const recipeFile=base+'install2-a0-publication-checkpoint-20261014/FINAL_A0_MOUNT_PINS.json';
const recipe=JSON.parse(object(recipeFile));
assert.equal(recipe.total_frozen_artifacts,40);
const files=git(['diff-tree','--no-commit-id','--name-only','-r',publication]).toString().trim().split('\n');
assert.equal(files.length,47);
const allowed=[...recipe.preserved_freezes.map(p=>base+p.folder+'/'),base+'install2-a0-publication-checkpoint-20261014/',base+'install2-office-consumer-forward-20261014/'];
const prepared=files.map(file=>{assert.ok(allowed.some(prefix=>file.startsWith(prefix)));assert.ok(!file.split('/').includes('..'));assert.ok(!fs.existsSync(file),'TARGET_EXISTS '+file);const bytes=object(file);return {file,bytes,sha256:sha(bytes)};});
for(const freeze of recipe.preserved_freezes){
 const folder=base+freeze.folder+'/';const manifest=object(folder+'REVIEW_MANIFEST.json');assert.equal(sha(manifest),freeze.sha256);
 const rows=JSON.parse(manifest).artifacts;assert.equal(rows.length+1,freeze.artifacts);
 for(const row of rows){const file=folder+row.path;const bytes=object(file);assert.equal(bytes.length,row.bytes);assert.equal(sha(bytes),row.sha256);assert.ok(prepared.some(p=>p.file===file));}
}
for(const row of recipe.mounts){
 const bytes=object(base+row.origin);assert.equal(sha(bytes),row.sha256);
 if(row.before_state==='ABSENT_CURRENT')assert.ok(!fs.existsSync(row.destination));
 else assert.equal(sha(fs.readFileSync(row.destination)),row.before_sha256);
}
for(const row of recipe.preserved_modules){
 const file=row.file??row.path??row.destination;assert.ok(file,'MISSING_MODULE_PATH');assert.equal(sha(fs.readFileSync(file)),row.sha256);
}
for(const row of prepared){fs.mkdirSync(path.dirname(row.file),{recursive:true});fs.writeFileSync(row.file,row.bytes);}
const receipt={sourceOnly:true,status:'ARCHIVED_ACCEPTED_SOURCE_RUNTIME_MOUNT_HELD_FOR_SCOPED_PERMISSION_SUCCESSOR',rootInputCommit:input,authorPublication:publication,files:prepared.map(({bytes,...row})=>({...row,bytes:bytes.length})),frozenArtifacts:40,additionalSourceRecords:7,rootExecutableChanges:0,nativeCalls:0,configurationChanges:0,hold:'Scoped AUTHORISE/LIFECYCLE permission successor and concrete Office/native reader consumers remain under construction. This archive does not mount or expose an older route.',whitespace:'Exact frozen snapshots retain inherited whitespace; no raw whitespace PASS is claimed.'};
fs.writeFileSync('docs/healthroster-build/install2-composition/A0_ROUTE_SOURCE_ARCHIVE_14102026.json',JSON.stringify(receipt,null,2)+'\n');
console.log(JSON.stringify({archived:prepared.length,frozenArtifacts:40,rootExecutableChanges:0,nativeCalls:0}));
