// Public source capture only. Never execute the captured root helper.
import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import {createHash} from 'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url)),report='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026',root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',operator='docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017';
const sha=b=>createHash('sha256').update(b).digest('hex'),git=(repo,...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',repo,...a],{maxBuffer:32e6,windowsHide:true,env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
if(fs.existsSync(here+'/MANIFEST.json'))throw Error('FROZEN_HISTORY');
const heads=new Map([report,root].map(r=>[r,git(r,'rev-parse','HEAD').toString().trim()])),rows=[],fixedRuntime=[];
function capture(repo,file,captured,fixed=false){const commit=heads.get(repo),b=fs.readFileSync(repo+'/'+file);if(![commit,''].every(ref=>b.equals(git(repo,'show',ref+':'+file))))throw Error('BOOKKEEPER_ACTUAL_RAW_PARENT_REQUIRED');fs.mkdirSync(path.dirname(here+'/'+captured),{recursive:true});fs.writeFileSync(here+'/'+captured,b);const row={root:repo,commit,file,captured,bytes:b.length,sha256:sha(b)};rows.push(row);if(fixed)fixedRuntime.push({file,commit,bytes:b.length,sha256:sha(b)});}
for(const f of ['stages.mjs','native-read.mjs','worker.mjs','source.mjs','git-image-batch.mjs'])capture(report,operator+'/'+f,'reference/operator/'+f,true);
capture(report,operator+'/contract.mjs','reference/operator/contract.mjs');
for(const f of ['stage.mjs','sequence.mjs','coordinate.mjs'])capture(report,'docs/healthroster-build/proposals/daily-health-genuine-single-stage-coordinator-20261017/'+f,'reference/continuation/'+f,true);
for(const f of ['phase-parent.mjs','owned-worker.mjs','coordinate.mjs','sequence.mjs'])capture(report,'docs/healthroster-build/proposals/daily-health-s0-stage-coordinator-20261017/'+f,'reference/s0/'+f,true);
capture(root,'docs/healthroster-build/proposals/daily-genuine-root-s0-r6-bookkeeper-20261017.mjs','reference/root-s0-r6-bookkeeper.mjs');
capture(root,'scripts/healthroster-rehearsal-turn.mjs','reference/healthroster-rehearsal-turn.mjs');
fs.writeFileSync(here+'/PARENTS.json',JSON.stringify({version:'ROOT_STAGE_BOOKKEEPER_ACTUAL_SOURCE_PINS_V1',reportSourceCommit:heads.get(report),rootSourceCommit:heads.get(root),rows,fixedRuntime,actualFutureK4:'NOT_PROVIDED',actualS0Prefix:'NOT_PROVIDED',sourceOrRuntimeEdits:0},null,2)+'\n');
for(const [repo,head]of heads)if(git(repo,'rev-parse','HEAD').toString().trim()!==head)throw Error('BOOKKEEPER_PARENT_HEAD_MOVED');console.log(JSON.stringify({parents:rows.length,fixedRuntime:fixedRuntime.length,rootWrites:0,privateAccess:'NONE'}));
