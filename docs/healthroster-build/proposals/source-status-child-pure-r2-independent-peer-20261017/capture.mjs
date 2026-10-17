// Immutable Health-only Source R2 capture. No database or native holder calls.
import fs from 'node:fs';import path from 'node:path';import cp from 'node:child_process';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),author='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(...args)=>{const r=cp.spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',author,...args],{maxBuffer:20e6});assert.equal(r.status,0,r.stderr.toString());return r.stdout;};
const selections=[{name:'source-status-child-pure-r2-alias-forward-20261017',commit:'d56db402e8b415a1303b0ae5956ceab2ec6822ca',manifest:'3807087a10214f5734e76b149c1ff954521eb37b53ad919526c84165b18d8275',count:51},{name:'source-status-child-pure-rollback-adapter-20261017',commit:'5931ae9e2f1ab11de810996f5aea6a02570b7ce9',manifest:'1c19cc71647619081a736d44cd794cdb3941b9d761e4bb9f88485e00bd3051a9',count:46}];
const captures=[];
for(const selection of selections){
 const folder='docs/healthroster-build/proposals/'+selection.name,manifestBytes=git('show',selection.commit+':'+folder+'/FREEZE_MANIFEST.json');
 assert.equal(sha(manifestBytes),selection.manifest);const manifest=JSON.parse(manifestBytes),files=[];
 for(const row of [...manifest.files,{file:'FREEZE_MANIFEST.json',bytes:manifestBytes.length,sha256:sha(manifestBytes)}]){
  const bytes=git('show',selection.commit+':'+folder+'/'+row.file),saved=fs.readFileSync(path.join(author,folder,row.file));
  assert.equal(bytes.length,row.bytes);assert.equal(sha(bytes),row.sha256);assert(bytes.equals(saved),'saved/Git differs '+row.file);
  const target=path.join(here,'frozen',selection.name,row.file);fs.mkdirSync(path.dirname(target),{recursive:true});assert(!fs.existsSync(target),'capture already exists');fs.writeFileSync(target,bytes);
  files.push({...row,savedGitEqual:true});
 }
 assert.equal(files.length,selection.count);captures.push({...selection,folder,files});
}
fs.writeFileSync(path.join(here,'CAPTURE.json'),JSON.stringify({version:'SOURCE_R2_INDEPENDENT_CAPTURE_V1',selections:captures,actualCwd:process.cwd(),author,ownWorktree:'D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026',native:'NOT_RUN',rootWrites:0},null,2)+'\n');
console.log(JSON.stringify({selections:captures.map(c=>({commit:c.commit,files:c.files.length})),native:'NOT_RUN'}));
