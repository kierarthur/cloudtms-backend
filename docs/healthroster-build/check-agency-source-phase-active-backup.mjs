import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';
import {execFileSync,spawnSync} from 'node:child_process';import {fileURLToPath,pathToFileURL} from 'node:url';
const repo=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../..');
const rel='docs/healthroster-build/proposals/agency-source-phase-active-capture-forward-20261015',dir=path.join(repo,rel);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...a],{cwd:repo,maxBuffer:64e6});
const m=fs.readFileSync(path.join(dir,'REVIEW_MANIFEST.json'));assert.equal(sha(m),'f20c66ebe63043035e86f191385044b54bc50577d45b1e8c86878d04085086ac');
const entries=JSON.parse(m).entries;assert.equal(entries.length,28);
const secret=/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|\b(?:ghp_|github_pat_)[A-Za-z0-9_]{30,}\b|\bAKIA[A-Z0-9]{16}\b|\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\b/;
const files=[];for(const e of [...entries,{path:'REVIEW_MANIFEST.json',sha256:sha(m),bytes:m.length}]){
 const p=rel+'/'+e.path,b=fs.readFileSync(path.join(repo,p));assert.equal(b.length,e.bytes,p);assert.equal(sha(b),e.sha256,p);assert.ok(!secret.test(b.toString()),p);files.push(p);}
const {scanManagedSql}=await import(pathToFileURL(path.join(dir,'reference/packaging-parser.mjs')).href);
const SQL=entries.filter(e=>/^\d\d_.*\.sqlinc$/.test(e.path)).map(e=>({path:e.path,statements:scanManagedSql(fs.readFileSync(path.join(dir,e.path),'utf8')).statements.length}));assert.equal(SQL.length,7);
const testRun=spawnSync(process.execPath,['--test','--test-reporter=tap',path.join(dir,'source-phase-admission.test.mjs')],{cwd:repo,encoding:'utf8',timeout:60000});
assert.equal(testRun.status,0,testRun.stdout+testRun.stderr);assert.match(testRun.stdout,/# pass 20/);
for(const p of ['source-phase-admission.test.mjs','reference/packaging-parser.mjs']){
 const syntax=spawnSync(process.execPath,['--check',path.join(dir,p)],{encoding:'utf8',timeout:30000});assert.equal(syntax.status,0,syntax.stderr);}
let preserved=0;for(const name of ['agency-source-dependency-current-reader-forward-20261014','agency-business-entry-inline-source-forward-20261014','agency-source-selector-precision-forward-20261014']){
 const prefix='docs/healthroster-build/proposals/'+name,manifest=fs.readFileSync(path.join(repo,prefix,'REVIEW_MANIFEST.json'));
 for(const e of [...JSON.parse(manifest).entries,{path:'REVIEW_MANIFEST.json',sha256:sha(manifest)}]){const p=prefix+'/'+e.path,b=fs.readFileSync(path.join(repo,p));
  assert.equal(sha(b),e.sha256,p);assert.ok(b.equals(git('cat-file','blob','b9100ea9d9b8f84098c593d5c6b80ead373f1e2b:'+p)));preserved++;}}
const self='docs/healthroster-build/check-agency-source-phase-active-backup.mjs';files.push(self);assert.ok(!secret.test(fs.readFileSync(path.join(repo,self),'utf8')));
assert.equal(git('diff','--name-only').toString().trim(),'');
if(process.argv.includes('--staged')){assert.deepEqual(git('diff','--cached','--name-only','-z').toString().split('\0').filter(Boolean).sort(),files.sort());
 for(const p of files)assert.ok(fs.readFileSync(path.join(repo,p)).equals(git('show',':'+p)),p);}
if(process.argv.includes('--committed')){assert.deepEqual(git('diff-tree','--no-commit-id','--name-only','-r','HEAD','-z').toString().split('\0').filter(Boolean).sort(),files.sort());
 for(const p of files)assert.ok(fs.readFileSync(path.join(repo,p)).equals(git('cat-file','blob','HEAD:'+p)),p);}
console.log(JSON.stringify({source_only:true,native:false,mount_ready:false,files:files.length,saved_mismatches:0,credential_findings:0,source20_oneclick30_selector12_preserved:preserved,
 tests:20,syntax:2,SQL_lexical_only:SQL,staged:process.argv.includes('--staged'),committed:process.argv.includes('--committed')}));
