import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),old=path.resolve(here,'../cn-a0-native-normalisation');
const root=process.argv[2];assert.ok(root,'read-only current complete root path required');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const expectedHead='dd06f9245f8a20615cd2edcea523a28dc5285f68';
const git=spawnSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8',windowsHide:true});
assert.equal(git.status,0);assert.equal(git.stdout.trim(),expectedHead,'Root checkpoint changed: reread and review, never overwrite it.');
const previous=JSON.parse(fs.readFileSync(path.join(old,'A0_SOURCE_MANIFEST.json')));
const census=JSON.parse(fs.readFileSync(path.join(here,'WRITER_LOCK_CENSUS.json'))),inputs=new Map();
for(const f of [...previous.inputs,...census.inputs]){
 const b=fs.readFileSync(path.join(root,f.path));assert.equal(sha(b),f.sha256,f.path);
 inputs.set(f.path,{path:f.path,sha256:sha(b),bytes:b.length});
}
const retainedCopies=['timesheet-correction-orchestrator.mjs','ADAPTER_ROOT.diff','ADAPTER_SOURCE_GUARD.json',
 'canonical-input-assert.sqlinc','exact-request-a0-weekly-clock-arm.mjs','daily-canonical-authority.native-input.js',
 'WORKER_ROOT.diff','WORKER_SOURCE_GUARD.json','NATIVE_WORKER_BINDING_HUNKS.mjsfrag'];
for(const file of retainedCopies){
 const b=fs.readFileSync(path.join(old,file));const frozen=previous.files.find(f=>f.path.endsWith('/'+file));assert.ok(frozen,file);
 assert.equal(sha(b),frozen.sha256,file);fs.writeFileSync(path.join(here,file),b);
}
const target='supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_approval.sqlinc';
const proposed='13102026_2034_approval.complete-proposed.sqlinc';
const diff=spawnSync('git',['diff','--no-index','--',path.join(root,target),path.join(here,proposed)],
 {encoding:'utf8',windowsHide:true,maxBuffer:3*1024*1024});assert.equal(diff.status,1);
const lines=diff.stdout.split('\n'),hunks=lines.slice(lines.findIndex(l=>l.startsWith('@@'))).join('\n');
fs.writeFileSync(path.join(here,'COMBINED_APPROVAL_ROOT.diff'),'--- a/'+target+'\n+++ b/'+target+'\n'+hunks);
const all=[...inputs.values()].sort((a,b)=>a.path.localeCompare(b.path));
fs.writeFileSync(path.join(here,'CURRENT_ROOT_INPUT_GUARD.json'),JSON.stringify({
 contract:'HEALTHROSTER_A0_LOCK_CURRENT_COMPLETE_ROOT_GUARD_V1',status:'REVIEW_PROPOSAL_UNMOUNTED',
 rootInputGitHead:expectedHead,rootInputBranch:'codex/healthroster-v2-backend-13102026',
 rootInputPath:path.resolve(root).replaceAll('\\','/'),inputs:all,
 priorA0ManifestSha256:sha(fs.readFileSync(path.join(old,'A0_SOURCE_MANIFEST.json'))),
 retainedCopies:retainedCopies.map(file=>({file,sha256:sha(fs.readFileSync(path.join(here,file)))})),
 combinedApproval:{path:target,beforeSha256:inputs.get(target).sha256,output:proposed,
  afterSha256:sha(fs.readFileSync(path.join(here,proposed))),hunkFile:'COMBINED_APPROVAL_ROOT.diff'},
 safety:'Complete saved source inputs checked, not installed/runtime authority. Later Report/Upload changes remain in root; no copies replace unrelated shared bodies. Root integrates only exact reviewed hunks/additive owners after new input review.'
},null,2)+'\n');
console.log(JSON.stringify({rootInputGitHead:expectedHead,inputs:all.length,retainedCopies:retainedCopies.length,
 combinedApprovalSha256:sha(fs.readFileSync(path.join(here,proposed)))}));
