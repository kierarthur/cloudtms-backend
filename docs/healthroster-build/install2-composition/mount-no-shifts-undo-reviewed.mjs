// One-time reviewed SOURCE mount only. Never runs SQL or a release.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {digest,canonical,scanManagedSql} from '../../../scripts/cloudtms-managed-release-sql.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../../..');
const author='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026';
const beforeCommit='87b9908370d238d1af4da910b97018d19a3fd033',authorCommit='d59255c626bbad7f2ee5db02dc2c36ad58fcdd48';
const git=(args,cwd=root)=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd,windowsHide:true,maxBuffer:16000000});
assert.equal(git(['rev-parse','HEAD']).toString().trim(),beforeCommit);
assert.equal(git(['ls-remote','origin','refs/heads/codex/healthroster-v2-report-build-13102026'],author).toString().trim().split(/\s/)[0],authorCommit);
const packets=[
 {name:'native',folder:'install2-no-shifts-undo-native-20261014',hash:'d97bd1909b77353905ec19d8e988bbc9ce1f010e41386aee18a0d05c94db1337',count:44},
 {name:'state',folder:'install2-no-shifts-undo-state-successor-20261014',hash:'ecf9fbe50130511a860943f3e4b975f62163820c3d7e899849c483d170198ec4',count:13}
];
const archiveRoot='docs/healthroster-build/install2-composition/reviewed-no-shifts-undo-14102026';
assert.ok(!fs.existsSync(path.join(root,archiveRoot)),'ONE_TIME_MOUNT_ALREADY_PRESENT');
const writes=[],pins=[];
for(const packet of packets){
 const relative='docs/healthroster-build/proposals/'+packet.folder,manifestFile=relative+'/REVIEW_MANIFEST.json';
 const raw=fs.readFileSync(path.join(author,manifestFile));assert.equal(digest(raw),packet.hash);
 assert.deepEqual(raw,git(['show',authorCommit+':'+manifestFile],author));
 const manifest=JSON.parse(raw);assert.equal(manifest.entries.length+1,packet.count);assert.equal(manifest.artifact_count,packet.count);
 for(const entry of [...manifest.entries,{path:'REVIEW_MANIFEST.json',sha256:digest(raw),bytes:raw.length}]){
  assert.ok(!entry.path.split('/').includes('..')&&!path.isAbsolute(entry.path));
  const source=relative+'/'+entry.path,bytes=fs.readFileSync(path.join(author,source));
  assert.equal(digest(bytes),entry.sha256);assert.equal(bytes.length,entry.bytes);assert.deepEqual(bytes,git(['show',authorCommit+':'+source],author));
  const destination=archiveRoot+'/'+packet.name+'/'+entry.path;assert.ok(!fs.existsSync(path.join(root,destination)));
  writes.push({file:destination,bytes});pins.push({file:destination,sha256:digest(bytes),publishedSource:source});
 }
}
const native=path.join(author,'docs/healthroster-build/proposals/'+packets[0].folder),state=path.join(author,'docs/healthroster-build/proposals/'+packets[1].folder);
const reverse=JSON.parse(fs.readFileSync(path.join(native,'CURRENT_UNDO_FULL_REVERSE.json')));
const route=reverse.changed_owner.path,before=fs.readFileSync(path.join(root,route)),after=fs.readFileSync(path.join(native,'forward',route));
assert.equal(digest(before),reverse.changed_owner.before_sha256);assert.equal(digest(after),reverse.changed_owner.after_sha256);
assert.deepEqual(before,git(['show',beforeCommit+':'+route]));
let reconstructed=after.toString();for(const h of [...reverse.changed_owner.hunks].reverse()){
 assert.equal(reconstructed.split(h.after).length,2);reconstructed=reconstructed.replace(h.after,h.before);
}assert.deepEqual(Buffer.from(reconstructed),before);
const prefix='14102026_0953_healthroster_no_shifts_undo';
const wrapperFile='supabase/repeatable/'+prefix+'_v1.sql',leafFile='supabase/repeatable/'+prefix+'/01_no_shifts_undo_atomic.sqlinc';
const oldWrapper=fs.readFileSync(path.join(native,'forward/supabase/repeatable/14102026_0900_healthroster_no_shifts_undo_v1.sql'),'utf8');
const wrapper=Buffer.from(canonical(oldWrapper).replaceAll('14102026_0900_healthroster_no_shifts_undo',prefix));
const rawLeaf=fs.readFileSync(path.join(state,'forward/supabase/repeatable/14102026_0900_healthroster_no_shifts_undo/01_no_shifts_undo_atomic.sqlinc'));
assert.equal(digest(rawLeaf),'0abb53f2ec202aac215476a45aee8ce70b6acfcbbd9806c285cb7359bad2267e');
const leaf=Buffer.from(canonical(rawLeaf.toString()));
assert.ok(!fs.existsSync(path.join(root,wrapperFile))&&!fs.existsSync(path.join(root,leafFile)));
assert.equal(scanManagedSql(leaf.toString()).statements.length,4);assert.ok(wrapper.toString().includes("notify pgrst, 'reload schema'"));
writes.push({file:route,bytes:after},{file:wrapperFile,bytes:wrapper},{file:leafFile,bytes:leaf});
for(const write of writes){fs.mkdirSync(path.dirname(path.join(root,write.file)),{recursive:true});fs.writeFileSync(path.join(root,write.file),write.bytes);}
const receipt={status:'SOURCE_MOUNT_NOT_NATIVE',beforeCommit,authorCommit,archivePins:pins,
 owners:[{file:route,beforeSha256:digest(before),sha256:digest(after),reverseHunks:reverse.changed_owner.hunks.length},
  {file:wrapperFile,sha256:digest(wrapper),sourceNameOnlyChanged:true,sqlCanonicalLF:true},
  {file:leafFile,sha256:digest(leaf),rawSourceSha256:digest(rawLeaf),sqlCanonicalLF:true}],
 sourcePacketsPreserved:true,contractSeal:null,signature:'public.weekly_source_no_shifts_undo_atomic_v1(jsonb)',
 finalClassificationVerifierCouplingRequired:true,nativeFirstUse:false,installation:false};
fs.writeFileSync(path.join(root,'docs/healthroster-build/install2-composition/NO_SHIFTS_UNDO_MOUNT_14102026.json'),JSON.stringify(receipt,null,2)+'\n');
console.log(JSON.stringify({status:receipt.status,archiveFiles:pins.length,owners:receipt.owners}));
