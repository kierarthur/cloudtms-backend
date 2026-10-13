import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
const root=process.argv[2],here=path.dirname(fileURLToPath(import.meta.url));
const file='broker/src/index.js',bytes=fs.readFileSync(path.join(root,file)),s=bytes.toString('utf8').replace(/\r\n/g,'\n');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
assert.equal(sha(bytes),'a1cb181fa8588813637598873606438e6dd0a10ab57d286a210a253c63fd6a86','controlling native caller source changed');
const code=fs.readFileSync(path.join(here,'NATIVE_WORKER_BINDING_HUNKS.mjsfrag'),'utf8');
const imports=code.match(/^import[^\n]+$/gm).join('\n')+'\n';
const body=code.slice(code.indexOf('// Complete caller-side producer binding.'));
const anchor='async function runTsfinWorkerOnce(env, { limit = 50, onlyTimesheetIds = null, forceUnprocessedTimesheetIds = null } = {}) {';
assert.equal(s.split(anchor).length,2);assert.ok(!s.includes('function createTimesheetCorrectionNativeOrchestrator('));
const line=s.slice(0,s.indexOf(anchor)).split('\n').length;
const importCount=imports.trimEnd().split('\n').length;
const patch='--- a/'+file+'\n+++ b/'+file+'\n@@ -1,1 +1,'+(importCount+1)+' @@\n'+imports.trimEnd().split('\n').map(x=>'+'+x).join('\n')+'\n '+s.split('\n')[0]+'\n'+
 '@@ -'+line+',1 +'+(line+importCount)+','+(body.trimEnd().split('\n').length+2)+' @@\n'+body.trimEnd().split('\n').map(x=>'+'+x).join('\n')+'\n+\n '+anchor+'\n';
fs.writeFileSync(path.join(here,'WORKER_ROOT.diff'),patch);
fs.writeFileSync(path.join(here,'WORKER_SOURCE_GUARD.json'),JSON.stringify({status:'UNMOUNTED_FACTORY_PROPOSAL',input:{file,sha256:sha(bytes)},hunks:[{anchor:'START_OF_MODULE',replacement:imports},{anchor,replacement:body+'\n'+anchor}],
 retainedNativeOwnerReferences:['rpcTsfinLoadContextBatch','sbRpc','sbFetch','rpcTsfinResolveRatesBatch','buildWeeklyScheduleSegmentsSnapshot','buildNhspWeeklySnapshotCached','rebuildFromExistingSegmentsEvidence','enrichTsfinWithHrCrosscheck','deriveCanonicalDailyFinancialContext','buildCanonicalDailyFinancialSnapshot','toLocalParts','subtractBreak','classifyMinutes','resolveEffectivePayChannel','anyMissingRates'],
 boundary:'No route mount. Root must review and supply existing qualified actor/RPC/cleanup/ordinary adapters; public/private Candidate transport and manager authorities are unchanged.'},null,2)+'\n');
fs.writeFileSync(path.join(here,'worker-binding.syntax-proposed.mjs'),imports+body);
console.log(JSON.stringify({inputSha256:sha(bytes),hunks:2}));
