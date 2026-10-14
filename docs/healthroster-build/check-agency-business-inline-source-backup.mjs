import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';
import {execFileSync,spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const repo=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../..'),rel='docs/healthroster-build/proposals/agency-business-entry-inline-source-forward-20261014';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...a],{cwd:repo,maxBuffer:96e6});
const m=fs.readFileSync(path.join(repo,rel,'REVIEW_MANIFEST.json'));
assert.equal(sha(m),'2440a8c130ffc85f872f0e3f6ae1818d3b4e3b7693c8a8c0b72421a4a955f5d4');
const entries=JSON.parse(m).entries;assert.equal(entries.length,29);const files=[];
const secret=/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|\b(?:ghp_|github_pat_)[A-Za-z0-9_]{30,}\b|\bAKIA[A-Z0-9]{16}\b|\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\b/;
for(const e of [...entries,{path:'REVIEW_MANIFEST.json',sha256:sha(m),bytes:m.length}]){
 const p=rel+'/'+e.path,b=fs.readFileSync(path.join(repo,p));assert.equal(b.length,e.bytes);assert.equal(sha(b),e.sha256);assert.ok(!secret.test(b.toString()),p);files.push(p);}
const checker='docs/healthroster-build/check-agency-business-inline-source-backup.mjs';files.push(checker);assert.ok(!secret.test(fs.readFileSync(path.join(repo,checker),'utf8')));
let prior=0;for(const name of ['agency-native-whole-week-validation-runtime-forward-20261014','agency-validation-marker-override-source-successor-20261014',
 'ppv2-report-d10-query-population-source-forward-20261014','agency-mode-a-source-support-issue-construction-20261014',
 'agency-mode-a-preset-policy-source-successor-20261014','agency-issued-credit-dependency-source-successor-20261014','agency-source-dependency-current-reader-forward-20261014']){
 const p='docs/healthroster-build/proposals/'+name,m=fs.readFileSync(path.join(repo,p,'REVIEW_MANIFEST.json'));
 for(const e of [...JSON.parse(m).entries,{path:'REVIEW_MANIFEST.json',sha256:sha(m),bytes:m.length}]){
  const n=p+'/'+e.path,b=fs.readFileSync(path.join(repo,n));assert.equal(sha(b),e.sha256);assert.ok(b.equals(git('show','63965738af6f148a75cd1bea5052600e4b007da5:'+n)));prior++;
 }}
const test=spawnSync(process.execPath,['--test',path.join(repo,rel,'construction/business-inline.test.mjs')],{encoding:'utf8'});
assert.equal(test.status,0,test.stdout+test.stderr);assert.match(test.stdout,/pass 58/);
const reverses=JSON.parse(fs.readFileSync(path.join(repo,rel,'construction/BUSINESS_ENTRY_CURRENT_REVERSES.json'))).reverses;
let newWhitespace=0;for(const e of reverses)for(const h of e.hunks)newWhitespace+=(h.after.match(/[\t ]+(?=\r?$)/gm)||[]).length;
assert.equal(newWhitespace,0,'No introduced hunk trailing whitespace');
assert.equal(git('diff','--name-only').toString().trim(),'','Preserve all unrelated tracked work');
if(process.argv.includes('--staged')){assert.deepEqual(git('diff','--cached','--name-only','-z').toString().split('\0').filter(Boolean).sort(),[...files].sort());for(const p of files)assert.ok(fs.readFileSync(path.join(repo,p)).equals(git('show',':'+p)));}
if(process.argv.includes('--committed')){for(const p of files)assert.ok(fs.readFileSync(path.join(repo,p)).equals(git('show','HEAD:'+p)));assert.deepEqual(git('diff','--name-only','HEAD^','HEAD','-z').toString().split('\0').filter(Boolean).sort(),[...files].sort());}
const mode=process.argv.includes('--staged')?['--cached']:process.argv.includes('--committed')?['HEAD^','HEAD']:[];
const w=spawnSync('git',['diff','--check',...mode],{cwd:repo,encoding:'utf8'});assert.ok([0,1,2].includes(w.status));
console.log(JSON.stringify({source_only:true,native:false,files:files.length,saved_mismatches:0,credential_findings:0,
 predecessor_files_unchanged:prior,tests:58,staged_exact:process.argv.includes('--staged'),committed_exact:process.argv.includes('--committed'),
 raw_whitespace_pass:mode.length?w.status===0:false,raw_whitespace_findings:mode.length?(w.stdout.match(/trailing whitespace|new blank line at EOF/g)||[]).length:null,
 introduced_hunk_trailing_whitespace:0,native_seal:null}));
