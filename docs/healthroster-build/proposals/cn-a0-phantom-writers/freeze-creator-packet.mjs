import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),lane=path.resolve(here,'../../../..'),root=process.argv[2];assert.ok(root);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const census=JSON.parse(fs.readFileSync(path.join(here,'FIRST_CREATOR_CENSUS.json'))),audit=JSON.parse(fs.readFileSync(path.join(here,'CREATOR_SCHEMA_AND_SOURCE_AUDIT.json')));
const inputs=new Map([...census.inputs,...audit.inputs].map(f=>[f.path,f]));
for(const f of inputs.values())assert.equal(sha(fs.readFileSync(path.join(root,f.path))),f.sha256,f.path+' changed after audit');
const rootHead=spawnSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8',windowsHide:true});assert.equal(rootHead.status,0);
assert.equal(rootHead.stdout.trim(),'b65df0997ac69188eadf4de2a88f70226fa99532','Re-review changed root checkpoint');
const frozen=[['docs/healthroster-build/PROTECTION_SOURCE_MANIFEST.json','2d0947db59f875da1fd4aeed4b49c6566174b19cc244c41785670248c3fd1070'],
 ['docs/healthroster-build/proposals/f4-ai/F4_AI_SOURCE_MANIFEST.json','6de00fa2d72625b8b3d39b38b14dd4765ab76989ae297abfcf2c97b7ae5010a8'],
 ['docs/healthroster-build/proposals/cn-a0-native-normalisation/A0_SOURCE_MANIFEST.json','f04bd3656846b5ba1e395095b64689fef839ada1e4dcc0102aa7f030cb16709e'],
 ['docs/healthroster-build/proposals/cn-a0-lock-fence/LOCK_SOURCE_MANIFEST.json','0c1c69391c772ec5fda1b2945094fdd14128159e6514fa02772fcdc71383ec1d']];
let retained=0;for(const [p,h] of frozen){const b=fs.readFileSync(path.join(lane,p));assert.equal(sha(b),h,p);for(const f of JSON.parse(b).files){assert.equal(sha(fs.readFileSync(path.join(lane,f.path))),f.sha256,f.path);retained++;}}
assert.equal(retained,170);
const suites=['tests/healthroster-cn-a0-callbacks.test.mjs','tests/healthroster-cn-a0-producers.test.mjs','tests/healthroster-cn-a0-clock-scope.test.mjs',
 'tests/healthroster-cn-a0-lock-fence.test.mjs','tests/healthroster-cn-a0-phantom-writers.test.mjs'];
const run=spawnSync(process.execPath,['--test',...suites],{cwd:lane,encoding:'utf8',windowsHide:true,maxBuffer:4*1024*1024});assert.equal(run.status,0,run.stdout+run.stderr);
for(const v of ['tests 92','pass 92','fail 0'])assert.ok(run.stdout.includes(v),v);
const syntax=fs.readdirSync(here).filter(n=>n.endsWith('.mjs'));
for(const p of syntax){const r=spawnSync(process.execPath,['--check',path.join(here,p)],{encoding:'utf8',windowsHide:true});assert.equal(r.status,0,p+' '+r.stderr);}
const patch='REQUEST_ROOT.diff',apply=spawnSync('git',['apply','--check',path.join(here,patch)],{cwd:root,encoding:'utf8',windowsHide:true});assert.equal(apply.status,0,apply.stderr);
assert.equal(audit.holds.length,5);assert.equal(audit.triggerReview.length,16);assert.deepEqual(audit.schemaChecks.flatMap(r=>r.missing),[]);
const successor=JSON.parse(fs.readFileSync(path.join(here,'SUCCESSOR_GUARD.json')));
for(const [file,g] of [['13102026_2153_request.complete-proposed.sqlinc',successor.request],['13102026_2153_a0_native_lock_fence.complete-proposed.sqlinc',successor.lockFence]]){
 const s=fs.readFileSync(path.join(here,file),'utf8');assert.equal(sha(s),g.afterSha256);assert.equal(s.split(g.replacement).length,2);assert.equal(sha(s.replace(g.replacement,g.anchor)),g.beforeSha256);
}
fs.writeFileSync(path.join(here,'SOURCE_CHECKS.json'),JSON.stringify({contract:'HEALTHROSTER_A0_CREATOR_SOURCE_CHECKS_V1',status:'PASS_SOURCE_ONLY_NATIVE_HELD',
 rootInputGitHead:rootHead.stdout.trim(),rootInputs:{count:inputs.size,allExact:true},tests:{suites,tests:92,pass:92,fail:0,new:17},
 syntax:{files:syntax,count:syntax.length,pass:true},rootApplyCheck:{patch,pass:true,mutatedRoot:false},reverse:{successors:2,byteExact:true},
 frozenPriorFiles:{count:retained,allByteIdentical:true,manifests:frozen.map(([path,sha256])=>({path,sha256}))},
 schema:{relations:7,fields:35,missing:0,beforeTriggers:16,compiled:false},sourceCensus:{migrationRoots:census.migrationRoots,repeatableRoots:census.repeatableRoots,
  controllingLiteralWriters:census.controllingWriterDefinitions.length,firstCreators:census.firstCreators.length,nativeOwners:census.nativeOwners.length},
 noDatabaseCalls:true,noSqlCompile:true,noRuntimeOrApiCalls:true,noBuildDeploy:true,noHostedSideEffects:true,
 isolation:audit.isolation,holds:audit.holds,nativeAcceptance:'NONE. Source checks do not qualify absent-row/RR/concurrency, native financial/document/NEXT/phone or installed ACL/trigger proof.'
},null,2)+'\n');
const files=fs.readdirSync(here).filter(n=>n!=='CREATOR_SOURCE_MANIFEST.json').map(n=>path.join(here,n));files.push(path.join(lane,'tests/healthroster-cn-a0-phantom-writers.test.mjs'));
const outputs=files.map(p=>{const b=fs.readFileSync(p);return {path:path.relative(lane,p).replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};}).sort((a,b)=>a.path.localeCompare(b.path));
fs.writeFileSync(path.join(here,'CREATOR_SOURCE_MANIFEST.json'),JSON.stringify({contract:'HEALTHROSTER_A0_CREATOR_ADJACENT_SOURCE_PACKET_V1',
 status:'FROZEN_REVIEW_PROPOSAL_UNMOUNTED_NATIVE_HELD',branch:'codex/healthroster-v2-protection-build-13102026',repository:'kierarthur/cloudtms-backend',
 rootInputGitHead:rootHead.stdout.trim(),rootInputPath:root,inputs:[...inputs.values()].sort((a,b)=>a.path.localeCompare(b.path)),
 priorFrozenPackets:frozen.map(([path,sha256])=>({path,sha256})),files:outputs,successorGuard:successor,
 newPrivateSignatures:['private.timesheet_correction_a0_signed_precursor_v1(uuid)','private.timesheet_correction_a0_writer_admit_v1(uuid[],text[],text)',
 'private.timesheet_correction_a0_reference_before_v1()','private.timesheet_correction_a0_booking_before_v1()',
 'private.timesheet_correction_a0_candidate_before_v1()','private.timesheet_correction_a0_send_family_fence_v1(uuid)'],
 rootRegistriesRetained:'Latest full root b65 checkpoint:105 tables/232 private/142 service, UPGRADE170/NEW171. This packet only proposes six additional owner-only private helpers; root owns complete union/ACL/contract.',
 completeRepositoryBoundary:'Own complete feature repository with all170 prior frozen outputs retained. Current complete root saved304 migration/835 repeatable inventory and nested includes are read-only inputs. No temporary partial source or mounted runtime is authority.',
 noNewStore:true,noNewLockIdentity:true,noPublicOperation:true,noRouteMounted:true,noBusinessRulingChange:true,allPriorGatesRetained:true,
 checks:{tests:92,pass:92,fail:0,newTests:17,jsSyntax:syntax.length,rootPatchApplyChecks:1,reverseByteExact:2,frozenPriorFiles:retained},
 isolation:audit.isolation,nativeHolds:audit.holds,
 nativeAcceptance:'NONE. Root must qualify actual supported RC/native visibility, hidden-P0 RR boundary, complete trigger/caller/late-hook graph, actual normalisation/financial/document/NEXT/phone/role proof before exposure. Existing parent row intents are not parent data-version changes.'
},null,2)+'\n');
console.log(JSON.stringify({outputs:outputs.length,bytes:outputs.reduce((n,f)=>n+f.bytes,0),inputs:inputs.size,tests:92,syntax:syntax.length,
 frozenPriorFiles:retained,manifestSha256:sha(fs.readFileSync(path.join(here,'CREATOR_SOURCE_MANIFEST.json')))}));
