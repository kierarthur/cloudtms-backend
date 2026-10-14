// Source-only checkpoint. No database, secret-file read, release or deployment.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {execFileSync,spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {digest} from './cloudtms-managed-release-sql.mjs';
import {sourceCheckEnvironment} from './healthroster-source-check-environment.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const predecessor='807ffce239f86f9f1e104221e4b9400343ed19d2';
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,windowsHide:true,maxBuffer:16000000});
assert.equal(git(['rev-parse','HEAD']).toString().trim(),predecessor);
const files=['scripts/healthroster-full-rehearsal-protocol.mjs','scripts/healthroster-install1-archive-preflight.mjs',
 'scripts/healthroster-install1-full-rehearsal.mjs','scripts/healthroster-install1-native-coordinator.mjs',
 'tests/healthroster-install1-full-rehearsal.test.mjs','scripts/healthroster-live-test-restoration.mjs',
 'tests/healthroster-live-test-restoration.test.mjs','scripts/healthroster-live-test-restoration-source-check.mjs',
 'docs/healthroster-build/install1-rehearsal/LIVE_TEST_RESTORATION_RULING_14102026.md'];
const pins=files.map(file=>{
 const bytes=fs.readFileSync(path.join(root,file));
 let previous=null;try{previous=digest(git(['show',predecessor+':'+file]));}catch{}
 return {file,sha256:digest(bytes),bytes:bytes.length,previousSha256:previous};
});
const registry=JSON.parse(fs.readFileSync(path.join(root,'docs/bpay-next/final-definer-closures.json'),'utf8'));
const env=sourceCheckEnvironment(process.env,{gitDir:git(['rev-parse','--absolute-git-dir']).toString().trim(),workTree:root,finalDefinerBase:registry.releaseBaseCommit});
const checks=[];
const run=(file,args,input)=>{
 const began=Date.now(),r=spawnSync(file,args,{cwd:root,env,windowsHide:true,encoding:'utf8',input,maxBuffer:8000000});
 const output=(r.stdout??'')+(r.stderr??'');
 checks.push({command:path.basename(file),args,exitCode:r.status,elapsedMs:Date.now()-began,outputSha256:digest(output),
  tests:Number(output.match(/(?:#|ℹ) tests (\d+)/)?.[1]??0),pass:Number(output.match(/(?:#|ℹ) pass (\d+)/)?.[1]??0),fail:Number(output.match(/(?:#|ℹ) fail (\d+)/)?.[1]??0)});
 assert.equal(r.status,0,'SOURCE_CHECK_FAILED: '+args.join(' '));
};
for(const file of files.filter(f=>f.endsWith('.mjs')))run(process.execPath,['--check',file]);
run(process.execPath,['--test','tests/healthroster-install1-full-rehearsal.test.mjs','tests/healthroster-install1-native-coordinator.test.mjs','tests/healthroster-full-rehearsal-protocol.test.mjs','tests/healthroster-live-test-restoration.test.mjs']);
run(process.execPath,['scripts/cloudtms-db-release.mjs','check']);
run(process.execPath,['scripts/verify-database-contract-coupling.mjs']);
run(process.execPath,['scripts/verify-source-secret-safety.mjs']);
run('git',['diff','--check']);
const diff=git(['diff','--binary',predecessor,'--',...files.filter((_,i)=>pins[i].previousSha256)]);
run('git',['apply','--reverse','--check','-'],diff.toString());
assert.equal(digest(JSON.stringify(JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-contract.json'),'utf8')))),
 '7e3c00b70366c2c036cfea419adf32c77df5b65070771591e75168b68f3336d6');
for(const pin of pins)assert.equal(digest(fs.readFileSync(path.join(root,pin.file))),pin.sha256,'CHECKPOINT_MOVED');
const receipt={status:'SOURCE_ONLY_PASS',predecessor,checkedAt:new Date().toISOString(),pins,checks,completeTrackedDiffSha256:digest(diff),
 contractUnchanged:true,nativeFirstUse:false,installation:false,backgroundJobsPaused:false,
 repairHistory:['Draft final gate initially trusted a status label; independent P1 corrected by private validator WeakMap bound to complete snapshots and proof.',
  'First branding edit had a duplicate const declaration; syntax check failed before any native operation or publication, then declaration was corrected.',
  'Both full snapshot epochs/Xip are now checked; retry timing follows unchanged native fail/dequeue formulas and last_error remains stable.'],
 limitations:['Committed archive and independent source verdict required next.','Third native rehearsal and fresh rollback attribution not yet executed.']};
const destination=path.join(root,'docs/healthroster-build/install1-rehearsal/LIVE_TEST_RESTORATION_SOURCE_CHECKS_14102026.json');
fs.writeFileSync(destination,JSON.stringify(receipt,null,2)+'\n');
console.log(JSON.stringify({status:receipt.status,checks:checks.length,tests:checks.reduce((n,r)=>n+r.tests,0),pins:pins.length,receipt:destination}));
