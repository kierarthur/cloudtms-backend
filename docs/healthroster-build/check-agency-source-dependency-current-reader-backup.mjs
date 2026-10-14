import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';
import {execFileSync,spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const repo=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../..'),rel='docs/healthroster-build/proposals/agency-source-dependency-current-reader-forward-20261014';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...a],{cwd:repo,maxBuffer:64e6});
const patterns=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/\b(?:ghp_|github_pat_)[A-Za-z0-9_]{30,}\b/,/\bAKIA[A-Z0-9]{16}\b/,/\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\b/];
const manifest=fs.readFileSync(path.join(repo,rel,'REVIEW_MANIFEST.json'));assert.equal(sha(manifest),'ada4acd2235b44406d65e59fce61664f075dcfaa1984dbabb1af4d5865eb3019');
const entries=JSON.parse(manifest).entries;assert.equal(entries.length,19);const files=[];
for(const e of [...entries,{path:'REVIEW_MANIFEST.json',bytes:manifest.length,sha256:sha(manifest)}]){
 const p=rel+'/'+e.path,b=fs.readFileSync(path.join(repo,p));assert.equal(b.length,e.bytes);assert.equal(sha(b),e.sha256);
 assert.ok(patterns.every(r=>!r.test(b.toString())),p);files.push(p);
}
const checker='docs/healthroster-build/check-agency-source-dependency-current-reader-backup.mjs';files.push(checker);
assert.ok(patterns.every(r=>!r.test(fs.readFileSync(path.join(repo,checker),'utf8'))));
let preserved=0;for(const name of ['agency-native-whole-week-validation-runtime-forward-20261014','agency-validation-marker-override-source-successor-20261014',
 'ppv2-report-d10-query-population-source-forward-20261014','agency-mode-a-source-support-issue-construction-20261014',
 'agency-mode-a-preset-policy-source-successor-20261014','agency-issued-credit-dependency-source-successor-20261014']){
 const p='docs/healthroster-build/proposals/'+name,m=fs.readFileSync(path.join(repo,p,'REVIEW_MANIFEST.json'));
 for(const e of [...JSON.parse(m).entries,{path:'REVIEW_MANIFEST.json',bytes:m.length,sha256:sha(m)}]){
  const name=p+'/'+e.path,b=fs.readFileSync(path.join(repo,name));assert.equal(sha(b),e.sha256);assert.ok(b.equals(git('show','28a39f4108940bc83df103c321ddf988ac9188ce:'+name)));preserved++;
 }
}
const test=spawnSync(process.execPath,['--test',path.join(repo,rel,'construction/source-dependencies.test.mjs')],{encoding:'utf8'});
assert.equal(test.status,0,test.stdout+test.stderr);assert.match(test.stdout,/pass 50/);
if(process.argv.includes('--staged')){assert.deepEqual(git('diff','--cached','--name-only','-z').toString().split('\0').filter(Boolean).sort(),[...files].sort());
 for(const p of files)assert.ok(fs.readFileSync(path.join(repo,p)).equals(git('show',':'+p)));}
if(process.argv.includes('--committed'))for(const p of files)assert.ok(fs.readFileSync(path.join(repo,p)).equals(git('show','HEAD:'+p)));
console.log(JSON.stringify({source_only:true,native:false,files:files.length,saved_mismatches:0,credential_scans:files.length,credential_findings:0,
 predecessor_files_unchanged:preserved,tests:50,staged_exact:process.argv.includes('--staged'),committed_exact:process.argv.includes('--committed'),native_seal:null}));
