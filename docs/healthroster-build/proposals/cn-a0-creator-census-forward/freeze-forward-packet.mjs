import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),lane=path.resolve(here,'../../../..'),root=process.argv[2];assert.ok(root);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),read=p=>JSON.parse(fs.readFileSync(path.join(here,p)));
const census=read('FIRST_CREATOR_CENSUS.json'),audit=read('CREATOR_SCHEMA_AND_SOURCE_AUDIT.json'),fixtures=read('OBSERVED_REGRESSION_FIXTURES.json');
const inputs=new Map([...census.inputs,...audit.inputs,...fixtures.inputs].map(p=>[p.path,p]));for(const p of inputs.values())assert.equal(sha(fs.readFileSync(path.join(root,p.path))),p.sha256,p.path);
const head=spawnSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8',windowsHide:true});assert.equal(head.status,0);assert.equal(head.stdout.trim(),'cea15289e0283e3383e71f0acb8b18ce184647c2');
const priorRootPin='cdaf300c62a0d94b0fd0f5a57b41a42d1c7f2b83';
const changed=spawnSync('git',['diff','--name-only',priorRootPin+'..'+head.stdout.trim()],{cwd:root,encoding:'utf8',windowsHide:true});assert.equal(changed.status,0);
const changedPaths=changed.stdout.trim().split('\n').filter(Boolean);assert.ok(changedPaths.every(p=>p.startsWith('docs/')||[
 'scripts/healthroster-full-rehearsal-protocol.mjs','tests/healthroster-full-rehearsal-protocol.test.mjs','tests/healthroster-cn-a0-phantom-writers.test.mjs',
 'supabase/verification/13102026_2206_weekly_source_automatic_native_delta_v1.sql'].includes(p)),changed.stdout);
fs.writeFileSync(path.join(here,'ROOT_SOURCE_PIN_FORWARD.json'),JSON.stringify({contract:'HEALTHROSTER_A0_CENSUS_ROOT_PIN_FORWARD_V1',requestedRootInputGitHead:priorRootPin,
 currentRootInputGitHead:head.stdout.trim(),changedPaths,all1319SavedSourceInputsExact:true,native25HelperBodiesUnchanged:true,firstCreator30BodiesUnchanged:true,
 scope:'No root edit by lane. Updated root source/evidence/rehearsal/verifier pin only; no CN/A0/native writer change or operational qualification.',rootUnrelatedDirtyFilesPreserved:true},null,2)+'\n');
const frozen=[['docs/healthroster-build/PROTECTION_SOURCE_MANIFEST.json','2d0947db59f875da1fd4aeed4b49c6566174b19cc244c41785670248c3fd1070'],
 ['docs/healthroster-build/proposals/f4-ai/F4_AI_SOURCE_MANIFEST.json','6de00fa2d72625b8b3d39b38b14dd4765ab76989ae297abfcf2c97b7ae5010a8'],
 ['docs/healthroster-build/proposals/cn-a0-native-normalisation/A0_SOURCE_MANIFEST.json','f04bd3656846b5ba1e395095b64689fef839ada1e4dcc0102aa7f030cb16709e'],
 ['docs/healthroster-build/proposals/cn-a0-lock-fence/LOCK_SOURCE_MANIFEST.json','0c1c69391c772ec5fda1b2945094fdd14128159e6514fa02772fcdc71383ec1d'],
 ['docs/healthroster-build/proposals/cn-a0-phantom-writers/CREATOR_SOURCE_MANIFEST.json','762f65ac35f11f1c33b779799854c1373eb0ccf9c624e44e6cebc70266cdb33e']];
let retained=0;for(const [p,h] of frozen){const b=fs.readFileSync(path.join(lane,p));assert.equal(sha(b),h,p);for(const f of JSON.parse(b).files){assert.equal(sha(fs.readFileSync(path.join(lane,f.path))),f.sha256,f.path);retained++;}}assert.equal(retained,189);
const old=JSON.parse(fs.readFileSync(path.resolve(here,'../cn-a0-phantom-writers/FIRST_CREATOR_CENSUS.json'))),liveIds=new Set(census.controllingWriterDefinitions.map(d=>d.id));
const removed=old.controllingWriterDefinitions.filter(d=>!liveIds.has(d.name+'('+d.inputTypes+')')).map(d=>({id:d.name+'('+d.inputTypes+')',source:d.file}));assert.equal(removed.length,3);
assert.equal(census.controllingWriterDefinitions.length,70);assert.equal(census.firstCreators.length,30);assert.equal(census.nativeOwners.length,25);assert.equal(audit.triggerReview.length,16);
assert.ok(audit.triggerOrderChecks.every(t=>t.pass));assert.equal(audit.triggerOrderChecks.length,28);assert.equal(audit.observedOrderCounterexample.failedChecks.length,2);
assert.equal(census.catalogueReconciliation.filter(c=>c.catalogue).length,67);assert.equal(census.catalogueReconciliation.filter(c=>!c.catalogue).length,3);
const suites=['tests/healthroster-cn-a0-callbacks.test.mjs','tests/healthroster-cn-a0-producers.test.mjs','tests/healthroster-cn-a0-clock-scope.test.mjs','tests/healthroster-cn-a0-lock-fence.test.mjs',
 'tests/healthroster-cn-a0-phantom-writers.test.mjs','tests/healthroster-cn-a0-creator-census-forward.test.mjs'];
const result=spawnSync(process.execPath,['--test',...suites],{cwd:lane,encoding:'utf8',windowsHide:true,maxBuffer:4*1024*1024});assert.equal(result.status,0,result.stdout+result.stderr);
for(const s of ['tests 112','pass 112','fail 0'])assert.ok(result.stdout.includes(s));
const syntax=fs.readdirSync(here).filter(p=>p.endsWith('.mjs'));for(const p of syntax){const r=spawnSync(process.execPath,['--check',path.join(here,p)],{encoding:'utf8',windowsHide:true});assert.equal(r.status,0,r.stderr);}
const reconciliation={contract:'HEALTHROSTER_A0_CENSUS_FORWARD_RECONCILIATION_V1',status:'SOURCE_EVIDENCE_FIXED_NATIVE_HELD',rootInputGitHead:head.stdout.trim(),
 previous:{controllingWriters:73,firstCreators:30,historicalWriterOccurrences:233},corrected:{controllingWriters:70,firstCreators:30,historicalWriterOccurrences:233,
 liveTopLevelFunctionIdentities:census.liveFunctionCount,lifecycleEvents:census.lifecycleEvents.length,actualMapRemovals:census.removedFunctionIdentities.length},removedStaleWriterEntries:removed,
 cataloguePresent:census.catalogueReconciliation.filter(c=>c.catalogue).map(c=>({id:c.id,source:c.file})),catalogueMissingPendingSource:census.catalogueReconciliation.filter(c=>!c.catalogue),
 unresolvedSourceExecution:census.unresolved,conditionalScope:'Explicit top-level function events are applied. DO/dynamic/psql conditions are recorded, not executed or promoted to physical installed truth.',
 identityNoStaleClaim:'No observed comment-bearing identity or explicitly removed two-arg writer remains in the final map; all67 pre-existing controlling signatures reconcile to saved catalogue. This is not an installed/native body/admission claim.',
 nativeBodies:fixtures.nativeOwnerPreservation,firstCreatorBodies:fixtures.firstCreatorBodyPreservation,triggerOrderChecks:audit.triggerOrderChecks,
 triggerCounterexample:audit.observedOrderCounterexample,holds:audit.holds,isolation:audit.isolation};
fs.writeFileSync(path.join(here,'CENSUS_RECONCILIATION.json'),JSON.stringify(reconciliation,null,2)+'\n');
fs.writeFileSync(path.join(here,'SOURCE_CHECKS.json'),JSON.stringify({contract:'HEALTHROSTER_A0_CENSUS_FORWARD_SOURCE_CHECKS_V1',status:'PASS_SOURCE_ONLY_NATIVE_HELD',
 rootInputGitHead:head.stdout.trim(),inputs:{count:inputs.size,allExact:true},tests:{suites,tests:112,pass:112,fail:0,new:20},syntax:{files:syntax,count:syntax.length,pass:true},
 preservation:{priorOutputs:170,frozenCreatorOutputs:19,frozenCreatorManifest:true,all189OutputsByteEqual:true,fiveManifestPinsByteEqual:true,nativeHelperBodies:25,firstCreatorBodies:30},
 critic:{sha256:'f2887142f109c35c0861eaed4e43a3b46429dcae5ec3eebba6a87e0812e81b9f',byteEqual:true},
 correctedEvidence:{liveTopLevelFunctions:census.liveFunctionCount,controllingWriters:70,firstCreators:30,historicalWriterOccurrences:233,removedStaleWriters:3,beforeTriggerOwners:16,trueTriggerOrderChecks:28,counterexampleFailures:2},
 noRootEdits:true,noSqlChanges:true,noSqlCompile:true,noDatabaseCalls:true,noRuntimeBuildDeploy:true,noNativeAcceptance:true,holds:audit.holds,isolation:audit.isolation
},null,2)+'\n');
const paths=fs.readdirSync(here).filter(p=>p!=='CENSUS_FORWARD_SOURCE_MANIFEST.json').map(p=>path.join(here,p));paths.push(path.join(lane,'tests/healthroster-cn-a0-creator-census-forward.test.mjs'));
const files=paths.map(p=>{const b=fs.readFileSync(p);return {path:path.relative(lane,p).replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};}).sort((a,b)=>a.path.localeCompare(b.path));
fs.writeFileSync(path.join(here,'CENSUS_FORWARD_SOURCE_MANIFEST.json'),JSON.stringify({contract:'HEALTHROSTER_A0_CREATOR_CENSUS_FORWARD_SOURCE_PACKET_V1',status:'FROZEN_EVIDENCE_SUCCESSOR_UNMOUNTED_NATIVE_HELD',
 rootInputGitHead:head.stdout.trim(),requestedPriorRootPin:priorRootPin,rootInputPath:root,branch:'codex/healthroster-v2-protection-build-13102026',repository:'kierarthur/cloudtms-backend',
 inputs:[...inputs.values()].sort((a,b)=>a.path.localeCompare(b.path)),critic:{sha256:'f2887142f109c35c0861eaed4e43a3b46429dcae5ec3eebba6a87e0812e81b9f',copiedByteExact:true},
 priorFrozenPackets:frozen.map(([path,sha256])=>({path,sha256})),files,
 scope:'Only evidence tooling, corrected generated census/schema/order evidence, exact regression fixtures/tests and preserved independent critic. No root SQL/Worker/adapter/index/registries/contract/runtime changes.',
 acceptedP2Fixes:['SQL-aware declaration comments plus ordered explicit DROP/RENAME/SET SCHEMA identity map','Actual CREATE TRIGGER names distinct from routine owners; event-scoped ordering and observed earlier-name failure'],
 counts:{inputs:inputs.size,migrationRoots:census.migrationRoots,repeatableRoots:census.repeatableRoots,liveTopLevelFunctions:census.liveFunctionCount,controllingWriters:70,firstCreators:30,historicalWriters:233,
 cataloguePresentWriters:67,catalogueMissingPendingWriters:3,nativeHelpers:25,beforeTriggerOwners:16,trueOrderChecks:28},
 checks:{tests:112,pass:112,fail:0,newTests:20,jsSyntax:syntax.length,frozenPrior170:true,frozenCreator20:true,all189OutputsPlusFiveManifestsByteEqual:true},
 unresolvedSourceExecution:census.unresolved,allPriorHoldsRetained:true,nativeHolds:audit.holds,isolation:audit.isolation,
 nativeAcceptance:'NONE. No database/SQL/native/compiler/two-session/financial/document/NEXT/phone/API/role/activation proof. Normal RC/hidden-P0 RR and full native graph remain held. Source backup is not TEST promotion.'
},null,2)+'\n');
console.log(JSON.stringify({outputs:files.length,bytes:files.reduce((n,f)=>n+f.bytes,0),inputs:inputs.size,tests:112,syntax:syntax.length,preservedOutputs:189,
 manifestSha256:sha(fs.readFileSync(path.join(here,'CENSUS_FORWARD_SOURCE_MANIFEST.json')))}));
