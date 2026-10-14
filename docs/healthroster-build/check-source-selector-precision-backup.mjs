import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';
import {execFileSync,spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const repo=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../..'),rel='docs/healthroster-build/proposals/agency-source-selector-precision-forward-20261014';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...a],{cwd:repo,maxBuffer:96e6});
const m=fs.readFileSync(path.join(repo,rel,'REVIEW_MANIFEST.json'));assert.equal(sha(m),'c1c8341b9a910609393e2290bbc9085259975ca7b05aadbac3cf794c153df79b');
const e=JSON.parse(m).entries;assert.equal(e.length,11);const files=[],secret=/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|\b(?:ghp_|github_pat_)[A-Za-z0-9_]{30,}\b|\bAKIA[A-Z0-9]{16}\b|\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\b/;
for(const a of [...e,{path:'REVIEW_MANIFEST.json',sha256:sha(m),bytes:m.length}]){const p=rel+'/'+a.path,b=fs.readFileSync(path.join(repo,p));
 assert.equal(b.length,a.bytes);assert.equal(sha(b),a.sha256);assert.ok(!secret.test(b.toString()),p);files.push(p);}
const checker='docs/healthroster-build/check-source-selector-precision-backup.mjs';files.push(checker);assert.ok(!secret.test(fs.readFileSync(path.join(repo,checker),'utf8')));
let preserved=0;for(const name of ['agency-source-dependency-current-reader-forward-20261014','agency-business-entry-inline-source-forward-20261014']){
 const p='docs/healthroster-build/proposals/'+name,m=fs.readFileSync(path.join(repo,p,'REVIEW_MANIFEST.json'));
 for(const a of [...JSON.parse(m).entries,{path:'REVIEW_MANIFEST.json',sha256:sha(m)}]){const n=p+'/'+a.path,b=fs.readFileSync(path.join(repo,n));
  assert.equal(sha(b),a.sha256);assert.ok(b.equals(git('show','4c26565f68521fa868b211ce36ba22a9471053c0:'+n)));preserved++;}}
const t=spawnSync(process.execPath,['--test',path.join(repo,rel,'construction/source-selector-precision.test.mjs')],{encoding:'utf8'});
assert.equal(t.status,0,t.stdout+t.stderr);assert.match(t.stdout,/pass 19/);
assert.equal(git('diff','--name-only').toString().trim(),'');
if(process.argv.includes('--staged')){assert.deepEqual(git('diff','--cached','--name-only','-z').toString().split('\0').filter(Boolean).sort(),[...files].sort());
 for(const p of files)assert.ok(fs.readFileSync(path.join(repo,p)).equals(git('show',':'+p)));}
if(process.argv.includes('--committed')){for(const p of files)assert.ok(fs.readFileSync(path.join(repo,p)).equals(git('show','HEAD:'+p)));
 assert.deepEqual(git('diff','--name-only','HEAD^','HEAD','-z').toString().split('\0').filter(Boolean).sort(),[...files].sort());}
console.log(JSON.stringify({source_only:true,native:false,files:files.length,saved_mismatches:0,credential_findings:0,
 source20_and_one_click30_unchanged:preserved,tests:19,staged_exact:process.argv.includes('--staged'),committed_exact:process.argv.includes('--committed'),compiled_contract_sha256:null}));
