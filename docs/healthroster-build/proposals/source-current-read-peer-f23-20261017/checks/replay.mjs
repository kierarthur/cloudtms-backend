import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {spawnSync} from 'node:child_process';
import assert from 'node:assert/strict';
const here=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const repo=path.resolve(here,'../../../..'),money=path.resolve(repo,'../healthroster-v2-money-build-13102026');
const bootstrap=pathToFileURL(path.join(here,'checks/read-only-bootstrap.mjs')).href;
const jobs=[
 ['source-read-identity-portability-forward-20261017','tests.mjs'],
 ['source-read-identity-portability-forward-20261017','source-tests.mjs'],
 ['source-read-identity-portability-forward-20261017','run-regressions.mjs'],
 ['source-read-identity-portability-forward-20261017','portability-tests.mjs'],
 ['source-current-read-health-successor-20261017','source-tests.mjs'],
 ['source-complete-classification-current-successor-20261017','tests.mjs']
],results=[];
for(const [packet,file]of jobs){
 const child=spawnSync(process.execPath,[path.join(money,'docs/healthroster-build/proposals',packet,file)],{
   cwd:repo,encoding:'utf8',windowsHide:true,maxBuffer:8000000,
   env:{...process.env,NODE_OPTIONS:'--import='+bootstrap}});
 assert.equal(child.status,0,packet+'/'+file+'\n'+child.stdout+'\n'+child.stderr);
 results.push({packet,file,status:child.status,stdout:child.stdout.trim()});
 console.log(packet+'/'+file+': PASS');
}
fs.writeFileSync(path.join(here,'REPLAY_RESULTS.json'),JSON.stringify({results,frozenWrites:0,
 receiptWrites:'OWN_REPORT_REPLAYED_ONLY',networkCalls:0,dbConnections:0,native:'NOT_RUN'},null,2)+'\n');
