// Public frozen source bytes and complete Health/shared target images only.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),author='D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026',root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',folder='docs/healthroster-build/proposals/contact-background-array-cse-forward-20261017',commit='fe2366f44c1514f9074e4c1413b7043330a3f4d7',sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(repo,...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true','-C',repo,...args],{windowsHide:true,maxBuffer:16*1024*1024});
assert(!fs.existsSync(here+'/MANIFEST.json'),'FROZEN_HISTORY');
const manifestRaw=fs.readFileSync(author+'/'+folder+'/MANIFEST.json');assert.equal(sha(manifestRaw),'4af20def35c5a76a0a6c5fd45c9d5c4694ccc992ba4e26dd439fb05230c5374c');
const m=JSON.parse(manifestRaw),source=[];
for(const row of [...m.files,{file:'MANIFEST.json',bytes:manifestRaw.length,sha256:sha(manifestRaw)}]){
 const relative=folder+'/'+row.file,b=fs.readFileSync(author+'/'+relative);assert.equal(b.length,row.bytes);assert.equal(sha(b),row.sha256);
 for(const rev of [commit,'HEAD',''])assert(b.equals(git(author,'show',rev+':'+relative)),relative);
 const out=here+'/reference/'+row.file;fs.mkdirSync(path.dirname(out),{recursive:true});fs.writeFileSync(out,b,{flag:'wx'});source.push(row);
}
const selection=JSON.parse(fs.readFileSync(here+'/reference/PAYLOAD_SELECTION.json')),parents=[];
assert.equal(selection.files.length,54);assert.equal(selection.parent,'22d82c88b7a28ff9b17e0352f33a4639299b2bdb');
for(const row of selection.files){const b=git(root,'show',selection.parent+':'+row.target);assert.equal(b.length,row.parentBytes);assert.equal(sha(b),row.parentSha256);
 assert(b.equals(fs.readFileSync(root+'/'+row.target)),row.target);for(const rev of ['HEAD',''])assert(b.equals(git(root,'show',rev+':'+row.target)),row.target);
 parents.push({file:row.target,bytes:b.length,sha256:sha(b),changed:row.changed});
}
const result={sourceCommit:commit,sourceFolder:author+'/'+folder,sourceFiles:source,sourceManifestSha256:sha(manifestRaw),completeTargetParent:selection.parent,currentRootHead:git(root,'rev-parse','HEAD').toString().trim(),targetParents:parents,payloads:parents.filter(r=>r.changed).length,unchanged:parents.filter(r=>!r.changed).length,allSavedIndexHeadHistoricalEqual:true,BankSemanticBodiesRead:false,native:'NOT_RUN'};
fs.writeFileSync(here+'/CAPTURE.json',JSON.stringify(result,null,2)+'\n',{flag:'wx'});console.log(JSON.stringify({sourceFiles:source.length,parentImages:parents.length,payloads:result.payloads,unchanged:result.unchanged,native:'NOT_RUN'}));
