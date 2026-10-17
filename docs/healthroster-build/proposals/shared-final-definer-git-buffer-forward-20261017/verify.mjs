// Read-only frozen source/admission verification. Does not import or execute the source gate.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const read=file=>fs.readFileSync(path.join(here,file));
const json=file=>JSON.parse(read(file));
const git=(root,...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true','-C',root,...args],{windowsHide:true,maxBuffer:8*1024*1024});
const parent=json('CURRENT_PARENT.json'),forward=json('FULL_PARENT_FORWARD.json'),manifest=json('MANIFEST.json');
assert.equal(parent.parent,'69dde78cd4d26d863cb7aa1f3aeb53d61bbc088f');
assert.equal(parent.target,'scripts/bpay-next-final-definers.mjs');
assert.equal(forward.parent,parent.parent);assert.equal(forward.target,parent.target);
const walk=(dir,relative='')=>fs.readdirSync(dir,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(dir,e.name),relative+e.name+'/'):[relative+e.name]).sort();
assert.deepEqual(manifest.files.map(r=>r.file).sort(),walk(here).filter(f=>f!=='MANIFEST.json'));
for(const row of manifest.files){const b=read(row.file);assert.equal(b.length,row.bytes,row.file);assert.equal(sha(b),row.sha256,row.file);}
assert.deepEqual(manifest.targets,[{target:parent.target,payload:'payload/'+parent.target,sha256:forward.afterSha256}]);
const before=read('current/'+parent.target),after=read('payload/'+parent.target);
assert.equal(sha(before),forward.beforeSha256);assert.equal(sha(after),forward.afterSha256);
assert.equal(forward.hunks.length,1);const hunk=forward.hunks[0];
assert.equal(before.toString().split(hunk.before).length,2);assert.equal(after.toString().split(hunk.after).length,2);
assert.equal(after.toString().replace(hunk.after,hunk.before),before.toString());
assert.equal(before.toString().replace(hunk.before,hunk.after),after.toString());
assert.equal(hunk.after,hunk.before.replace("encoding: 'utf8', windowsHide", "encoding: 'utf8', maxBuffer: 8 * 1024 * 1024, windowsHide"));
assert(before.equals(git(parent.root,'show',parent.parent+':'+parent.target)));
// Before-mount admission: real saved, index and current HEAD must still equal the reviewed parent.
assert(before.equals(fs.readFileSync(parent.root+'/'+parent.target)));
assert(before.equals(git(parent.root,'show',':'+parent.target)));
assert(before.equals(git(parent.root,'show','HEAD:'+parent.target)));
git(parent.root,'merge-base','--is-ancestor',parent.parent,'HEAD');
for(const row of parent.unchanged){
  const b=fs.readFileSync(parent.root+'/'+row.file);assert.equal(b.length,row.bytes);assert.equal(sha(b),row.sha256,row.file);
  for(const rev of [parent.parent,'HEAD',''])assert(b.equals(git(parent.root,'show',rev+':'+row.file)),row.file);
}
const registry=JSON.parse(fs.readFileSync(parent.root+'/docs/bpay-next/final-definer-closures.json'));
assert.equal(registry.releaseBaseCommit,forward.unchangedReleaseBaseCommit);
assert.equal(forward.unchangedReleaseBaseCommit,'a6f7423eb71e6e7cd173e705c2290e5408a1d077');
assert.equal(forward.newMaxBufferBytes,8*1024*1024);assert.equal(forward.sourceChanges,1);assert.equal(forward.sqlRuntimePolicyChanges,0);
const results=json('REGRESSION_RESULTS.json');
assert(results.fixture.stdoutBytes>1024*1024);assert(results.fixture.stdoutBytes<forward.newMaxBufferBytes);
assert.equal(results.fixture.actualUnionPaths,8194);assert.equal(results.fixture.originalError,'ENOBUFS');
for(const key of ['completeExactGeneratedSet','ancestryRefusal','invalidBaseRefusal','partialAndOverflowFailClosed'])assert.equal(results.fixture[key],true);
assert.equal(results.ownedFixtureRemoved,true);assert.equal(results.actualRoot.originalFourInputs.length,4);
assert.equal(results.actualRoot.releaseBaseCommit,registry.releaseBaseCommit);
assert.equal(results.fullGlobalGate,'ROOT_AFTER_REVIEW_NOT_RUN_THIS_LANE');assert.equal(results.native,'NOT_RUN');
if(process.argv.includes('--committed')){
  const repo=git(here,'rev-parse','--show-toplevel').toString().trim(),prefix=path.relative(repo,here).replaceAll('\\','/')+'/';
  for(const file of [...manifest.files.map(r=>r.file),'MANIFEST.json']){
    const b=read(file);for(const revision of ['HEAD',''])assert(b.equals(git(repo,'show',revision+':'+prefix+file)),file);
  }
}
console.log(JSON.stringify({files:manifest.files.length+1,targets:1,sourceLineChanges:1,wholeRawInverse:true,unchangedCompanions:parent.unchanged.length,actualFixturePaths:results.fixture.actualUnionPaths,actualFixtureStdoutBytes:results.fixture.stdoutBytes,releaseBaseCommit:registry.releaseBaseCommit,gateExecution:'NOT_RUN',native:'NOT_RUN'}));
