import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),lane=path.resolve(here,'../../../..'),root=process.argv[2];assert.ok(root);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const rootGuard=JSON.parse(fs.readFileSync(path.join(here,'CURRENT_ROOT_INPUT_GUARD.json')));
for(const f of rootGuard.inputs)assert.equal(sha(fs.readFileSync(path.join(root,f.path))),f.sha256,f.path);
const frozen=[['docs/healthroster-build/PROTECTION_SOURCE_MANIFEST.json','2d0947db59f875da1fd4aeed4b49c6566174b19cc244c41785670248c3fd1070'],
 ['docs/healthroster-build/proposals/f4-ai/F4_AI_SOURCE_MANIFEST.json','6de00fa2d72625b8b3d39b38b14dd4765ab76989ae297abfcf2c97b7ae5010a8'],
 ['docs/healthroster-build/proposals/cn-a0-native-normalisation/A0_SOURCE_MANIFEST.json','f04bd3656846b5ba1e395095b64689fef839ada1e4dcc0102aa7f030cb16709e']];
let retainedFiles=0;for(const [file,expected] of frozen){const b=fs.readFileSync(path.join(lane,file));assert.equal(sha(b),expected,file);
 for(const f of JSON.parse(b).files){assert.equal(sha(fs.readFileSync(path.join(lane,f.path))),f.sha256,f.path);retainedFiles++;}}
const suites=['tests/healthroster-cn-a0-callbacks.test.mjs','tests/healthroster-cn-a0-producers.test.mjs',
 'tests/healthroster-cn-a0-clock-scope.test.mjs','tests/healthroster-cn-a0-lock-fence.test.mjs'];
const run=spawnSync(process.execPath,['--test',...suites],{cwd:lane,encoding:'utf8',windowsHide:true,maxBuffer:2*1024*1024});
assert.equal(run.status,0,run.stdout+run.stderr);assert.match(run.stdout,/tests 75/);assert.match(run.stdout,/pass 75/);assert.match(run.stdout,/fail 0/);
const syntax=fs.readdirSync(here).filter(n=>n.endsWith('.mjs'));
for(const f of syntax){const r=spawnSync(process.execPath,['--check',path.join(here,f)],{encoding:'utf8',windowsHide:true});assert.equal(r.status,0,f+' '+r.stderr);}
const patches=['COMBINED_APPROVAL_ROOT.diff','ADAPTER_ROOT.diff','WORKER_ROOT.diff'];
for(const f of patches){const r=spawnSync('git',['apply','--check',path.join(here,f)],{cwd:root,encoding:'utf8',windowsHide:true});assert.equal(r.status,0,f+' '+r.stderr);}
const census=JSON.parse(fs.readFileSync(path.join(here,'WRITER_LOCK_CENSUS.json'))),schema=JSON.parse(fs.readFileSync(path.join(here,'SCHEMA_SOURCE_CENSUS.json')));
assert.equal(census.routines.length,41);assert.equal(schema.exactFkSourceChecks.length,16);assert.equal(census.uncovered.length,5);
fs.writeFileSync(path.join(here,'SOURCE_CHECKS.json'),JSON.stringify({contract:'HEALTHROSTER_A0_LOCK_SOURCE_CHECKS_V1',
 status:'PASS_SOURCE_ONLY_NATIVE_HELD',tests:{suites,tests:75,pass:75,fail:0,new:9},syntax:{files:syntax,count:syntax.length,pass:true},
 rootApplyCheck:{patches,pass:true,mutatedRoot:false},rootInputs:{count:rootGuard.inputs.length,allExact:true},
 frozenPackets:{files:retainedFiles,allByteIdentical:true,manifests:frozen.map(([path,sha256])=>({path,sha256}))},
 schema:{explicitRelations:schema.explicitNativeFieldChecks.length,explicitFields:62,namedFkSources:16,missing:0,native:false},
 writerCensus:{exactBodies:41,workerEntries:4,foreignKeys:75,triggers:130,uncovered:census.uncovered},
 noNativeTests:true,noSqlCompile:true,noDatabaseCalls:true,noBuildDeploy:true,noHostedSideEffects:true,
 retainedGates:'All five unsupported writer/late-hook holds and all prior signed/source/rule/claim/agency/financial/official-document/NEXT/phone/installed-role gates. Source tests are not financial/concurrency readiness.'
},null,2)+'\n');
const files=fs.readdirSync(here).filter(n=>n!=='LOCK_SOURCE_MANIFEST.json').map(n=>path.join(here,n));
files.push(path.join(lane,'tests/healthroster-cn-a0-lock-fence.test.mjs'));
const outputs=files.map(f=>{const b=fs.readFileSync(f);return {path:path.relative(lane,f).replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};}).sort((a,b)=>a.path.localeCompare(b.path));
fs.writeFileSync(path.join(here,'LOCK_SOURCE_MANIFEST.json'),JSON.stringify({
 contract:'HEALTHROSTER_A0_LOCK_ADJACENT_SOURCE_PACKET_V1',status:'FROZEN_REVIEW_PROPOSAL_UNMOUNTED_NATIVE_HELD',
 branch:'codex/healthroster-v2-protection-build-13102026',repository:'kierarthur/cloudtms-backend',
 rootInputGitHead:rootGuard.rootInputGitHead,rootInputPath:rootGuard.rootInputPath,inputs:rootGuard.inputs,
 priorFrozenPackets:frozen.map(([path,sha256])=>({path,sha256})),files:outputs,
 completeRepositoryBoundary:'Own feature branch retains the complete repository and all frozen135 prior outputs. Root actual complete repository and65 exact saved inputs control this guarded proposal; no standalone build copy or root overwrite is authority.',
 newPrivateSignatures:['private.timesheet_correction_a0_native_input_witness_v1(uuid)','private.timesheet_correction_a0_native_lock_fence_v1(uuid,uuid)'],
 retainedPrivateSignatures:['private.timesheet_correction_canonical_input_assert_v1(jsonb,uuid,integer,uuid,uuid)','private.timesheet_correction_a0_clock_scope_v1(uuid)'],
 noNewStore:true,noPublicOperation:true,noRouteMounted:true,noBusinessRulingChange:true,
 nativeHolds:census.uncovered,allPriorGatesRetained:true,
 checks:{tests:75,pass:75,fail:0,syntax:syntax.length,rootPatchApplyChecks:3,frozenPriorFiles:retainedFiles},
 nativeAcceptance:'NONE: no database/compiler/API/two-session/financial/document/NEXT/phone proof in this lane. Root integration, new catalogue/ACL/role/RI proofs and closure of every exact writer hold precede eventual exposure. No fake readiness or historical/current pricing fallback.'
},null,2)+'\n');
console.log(JSON.stringify({outputs:outputs.length,bytes:outputs.reduce((n,f)=>n+f.bytes,0),tests:75,syntax:syntax.length,
 frozenPriorFiles:retainedFiles,manifestSha256:sha(fs.readFileSync(path.join(here,'LOCK_SOURCE_MANIFEST.json')))}));
