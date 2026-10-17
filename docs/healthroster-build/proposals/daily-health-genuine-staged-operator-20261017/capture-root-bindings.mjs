import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import {createHash} from 'node:crypto';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',commit='c276aa49eb4db86ff42e184011a09bf2b59f253e';
const sha=b=>createHash('sha256').update(b).digest('hex');const git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...a],{maxBuffer:32e6});
const names=['NORMAL_TEST_WORKER_DEPLOYMENTS_READONLY_20261017.json','NORMAL_TEST_WORKER_ACTIVE_VERSION_METADATA_20261017.json','DAILY_PRODUCERS_ACTIVE_RUNTIME_SOURCE_BINDING_20261017.json','DAILY_GENUINE_AUDIT_RUNTIME_ROOT_REVIEW_20261017.json'];
const pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json'));pins.rootRuntimeObservations=[];
for(const name of names){const file='docs/healthroster-build/v6-owner-delivery-20261016/'+name,b=git('show',commit+':'+file);assert(b.equals(fs.readFileSync(root+'/'+file)));fs.writeFileSync(here+'/reference/'+name,b,{flag:'wx'});pins.rootRuntimeObservations.push({commit,file,bytes:b.length,sha256:sha(b),captured:'reference/'+name});}
const control='scripts/healthroster-turn-control.mjs',b=git('show',commit+':'+control);assert(b.equals(fs.readFileSync(root+'/'+control)));fs.writeFileSync(here+'/reference/healthroster-turn-control.mjs',b,{flag:'wx'});pins.turnControl={commit,file:control,bytes:b.length,sha256:sha(b),captured:'reference/healthroster-turn-control.mjs'};
const parent='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026',relative='docs/healthroster-build/proposals/test-auth-refresh-suppressed-startup-forward-20261017';
pins.authReusableParents=[];
for(const name of ['run-auth-refresh.mjs','policy.mjs']){const b=fs.readFileSync(parent+'/'+relative+'/'+name);pins.authReusableParents.push({root:parent,commit:'462c196c503c39731fe331f4b6899f3e72b35b9c',file:relative+'/'+name,bytes:b.length,sha256:sha(b)});fs.writeFileSync(here+'/reference/'+name,b,{flag:'wx'});}
fs.writeFileSync(here+'/SOURCE_PINS.json',JSON.stringify(pins,null,2)+'\n');
console.log(JSON.stringify({rootRuntimeObservationFiles:names.length,originalTurnControlCaptured:true,authFullParents:2,privateFilesRead:0}));
