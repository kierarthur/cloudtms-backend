// Portable source checks only; children receive no service/database credentials.
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { execFileSync, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { sourceCheckEnvironment } from '../../../scripts/healthroster-source-check-environment.mjs';
const folder=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(folder,'../../..');
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const gitDir=execFileSync('git',['rev-parse','--absolute-git-dir'],{cwd:root,encoding:'utf8'}).trim();
const finalDefinerBase=JSON.parse(fs.readFileSync(path.join(root,'docs/bpay-next/final-definer-closures.json'))).releaseBaseCommit;
const env=sourceCheckEnvironment(process.env,{gitDir,workTree:root,finalDefinerBase});
const npm='C:/Program Files/nodejs/node_modules/npm/bin/npm-cli.js';
const lockFile=path.join(root,'package-lock.json'),lockBefore=hash(fs.readFileSync(lockFile));
const receipt={sourceOnly:true,beganAt:new Date().toISOString(),status:'RUNNING',
 initialUnprovisionedCheck:{tests:16,pass:12,failedFiles:4,reason:'xlsx/pdf-lib packages absent; exact lock provisioning required',nativeCalls:0},steps:[]};
const outputFile=path.join(folder,'RUNTIME_SOURCE_CHECKS_14102026.json');
const save=()=>fs.writeFileSync(outputFile,JSON.stringify(receipt,null,2)+'\n');
for(const args of [
 [npm,'run','security:toolchain'],
 [npm,'ci','--engine-strict','--ignore-scripts','--no-audit','--no-fund'],
 ['--test','tests/weekly-source/weekly-source-pay-auth-transport.test.mjs','tests/weekly-source/weekly-source-office-presentation-integration.test.mjs','tests/weekly-source/weekly-source-settings-routes.test.mjs','tests/candidate-correction-v1.test.js','tests/weekly-source/protected-action-orchestrator.test.mjs']
]){
 const begin=Date.now(),result=spawnSync(process.execPath,args,{cwd:root,env,encoding:'utf8',windowsHide:true,maxBuffer:8_000_000});
 const text=(result.stdout??'')+(result.stderr??'');
 const step={args:args.map(arg=>arg===npm?'npm':arg),exitCode:result.status,elapsedMs:Date.now()-begin,outputSha256:hash(text),
  tests:Number(text.match(/(?:#|ℹ) tests (\d+)/)?.[1]??0),pass:Number(text.match(/(?:#|ℹ) pass (\d+)/)?.[1]??0),fail:Number(text.match(/(?:#|ℹ) fail (\d+)/)?.[1]??0),
  diagnosticCodes:[...new Set(text.match(/\bERR_[A-Z0-9_]+\b/g)??[])]};
 receipt.steps.push(step);save();console.log(JSON.stringify(step));
 if(result.status!==0||result.error){receipt.status='SOURCE_CHECK_HOLD';receipt.endedAt=new Date().toISOString();save();process.exitCode=1;break;}
}
receipt.lockfile={sha256:lockBefore,unchanged:hash(fs.readFileSync(lockFile))===lockBefore,installerHooks:false};
if(receipt.status==='RUNNING')receipt.status=receipt.lockfile.unchanged?'SOURCE_CHECK_PASS':'SOURCE_CHECK_HOLD_LOCK_CHANGED';
receipt.endedAt=new Date().toISOString();save();console.log(JSON.stringify({status:receipt.status,lockfile:receipt.lockfile,outputFile}));
