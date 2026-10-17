// Public source only. Never reads an auth state, environment file or native business payload.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import assert from 'node:assert/strict';
export const here=path.dirname(fileURLToPath(import.meta.url));
const author='D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026';
const packet='docs/healthroster-build/proposals/daily-health-current-genuine-producer-audit-20261017';
const commit='e1c13ecb123224aea0fa95beff3727cd4c58bbb2';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(root,...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{maxBuffer:32e6});
const physical=p=>fs.realpathSync.native(p).replaceAll('\\','/');
const save=(file,bytes)=>{const to=path.join(here,file);assert(!fs.existsSync(to));fs.mkdirSync(path.dirname(to),{recursive:true});fs.writeFileSync(to,bytes,{flag:'wx'});};
assert(physical(process.cwd()).startsWith('D:/Development/GitHub'));
assert(physical(author)===author);
assert(physical(git(author,'rev-parse','--absolute-git-dir').toString().trim())==='D:/Development/GitHub/cloudtms-backend/.git/worktrees/healthroster-checklist-native-14102026');
assert(physical(path.resolve(author,git(author,'rev-parse','--git-common-dir').toString().trim()))==='D:/Development/GitHub/cloudtms-backend/.git');
const final=fs.readFileSync(author+'/'+packet+'/FINAL_MANIFEST.json');
assert.equal(sha(final),'0b2e8a621f3673948396c10dc1da9d799638381f7436ad7a1261b5b7ab62ead7');
const manifest=JSON.parse(final),rows=[];
for(const r of [...manifest.files,{file:'FINAL_MANIFEST.json',bytes:final.length,sha256:sha(final)}]){
 const b=fs.readFileSync(author+'/'+packet+'/'+r.file);
 assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);
 assert(b.equals(git(author,'show',commit+':'+packet+'/'+r.file)));
 save('reference/audit/'+r.file,b);rows.push({...r,captured:'reference/audit/'+r.file});
}
const recipe='docs/healthroster-build/proposals/daily-health-genuine-fixture-recipe-20261017/RECIPE.json';
const b=git(author,'show','ad02698589fa28b1a73af26390fad7cf4c0466f9:'+recipe);
save('reference/daily-health-genuine-fixture-recipe-20261017/RECIPE.json',b);
save('AUDIT_CAPTURE.json',JSON.stringify({version:'DAILY_GENUINE_PRODUCER_INDEPENDENT_CAPTURE_V1',author,commit,packet,manifestSha256:sha(final),files:rows,recipe:{commit:'ad02698589fa28b1a73af26390fad7cf4c0466f9',file:recipe,sha256:sha(b)},native:'NOT_RUN',businessRowsRead:0,bankBodiesRead:0},null,2)+'\n');
console.log(JSON.stringify({publicAuditFiles:rows.length,originalRecipeCaptured:true,native:'NOT_RUN'}));
