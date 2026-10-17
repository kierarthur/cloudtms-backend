// Bounded source/AST/test peer only. Never calls a reader, native child or source-admission entry.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),sha=b=>crypto.createHash('sha256').update(b).digest('hex'),read=f=>fs.readFileSync(here+'/'+f),json=f=>JSON.parse(read(f));
const record=process.argv.includes('--record');assert(process.argv.slice(2).every(x=>x==='--record'));if(record)assert(!fs.existsSync(here+'/MANIFEST.json'),'FROZEN_HISTORY');
const capture=json('CAPTURE.json'),manifest=json('reference/MANIFEST.json'),selection=json('reference/PAYLOAD_SELECTION.json'),proof=json('reference/VERIFY_PROOF.json');
assert.equal(capture.sourceCommit,'fe2366f44c1514f9074e4c1413b7043330a3f4d7');assert.equal(sha(read('reference/MANIFEST.json')),'4af20def35c5a76a0a6c5fd45c9d5c4694ccc992ba4e26dd439fb05230c5374c');
assert.equal(sha(read('reference/PAYLOAD_SELECTION.json')),'619c55985e361d356819914be105b0795e32055f25f3f0ee8074c666c4615bd4');assert.equal(sha(read('reference/VERIFY_PROOF.json')),'9174ae26faf73f1e13f1b431a3edc3fc6d2063993a9cf32dcd5298fce587c288');
assert.equal(manifest.files.length+1,27);assert.equal(capture.sourceFiles.length,27);assert.equal(capture.targetParents.length,54);assert.equal(capture.allSavedIndexHeadHistoricalEqual,true);
for(const row of capture.sourceFiles){const b=read('reference/'+row.file);assert.equal(b.length,row.bytes);assert.equal(sha(b),row.sha256);}
const inverse=json('reference/FULL_PARENT_INVERSES.json');assert.equal(inverse.edits.length,5);let hunks=0;
for(const edit of inverse.edits){assert.equal(sha(edit.before),edit.beforeSha256);assert.equal(sha(edit.after),edit.afterSha256);const row=selection.files.find(r=>r.target===edit.file);assert(row?.payload);assert.equal(sha(read('reference/'+row.payload)),edit.afterSha256);assert.equal(row.parentSha256,edit.beforeSha256);let back=edit.after;for(const h of edit.hunks.toReversed()){assert.equal(back.split(h.after).length,2);back=back.replace(h.after,()=>h.before);hunks++;}assert.equal(back,edit.before);}
assert.equal(selection.files.filter(r=>r.changed).length,5);assert.equal(selection.files.filter(r=>!r.changed).length,49);
const prefix='docs/healthroster-build/proposals/contact-qt-positive-native-proposal-20261017/';
const oldBatch=inverse.edits.find(e=>e.file===prefix+'batch-source.mjs'),oldReader=inverse.edits.find(e=>e.file===prefix+'fixed-metadata-reader.mjs');
const rawJoin=s=>s.slice(s.indexOf('// Retain complete native JSON'));
assert.equal(rawJoin(oldBatch.before),rawJoin(oldBatch.after));
const range=(s,start,end)=>s.slice(s.indexOf(start),s.indexOf(end,s.indexOf(start)));
assert.equal(range(oldReader.before,'export class BoundedMemoryCapture','function assertPins('),range(oldReader.after,'export class BoundedMemoryCapture','function assertPins('));
assert.equal(range(oldReader.before,'async function runFixedChild(','export async function createFixedMetadataReader('),range(oldReader.after,'async function runFixedChild(','export async function createFixedMetadataReader('));
assert.equal(oldReader.before.replace('afterInput.indexOf(built.backgroundQuery)','afterInput.indexOf(built.finalQuery)'),oldReader.after);
for(const file of ['scripts/healthroster-install1-full-rehearsal.mjs','scripts/healthroster-live-test-restoration.mjs'])assert.equal(selection.files.find(r=>r.target===file).payload,null);
assert.equal(proof.native,'NOT_RUN');assert.equal(proof.restorationQualified,false);assert.equal(proof.actualPostimageSourceAdmission,'NOT_RUN_UNMOUNTED');assert.equal(proof.raw21MemberJoinRawUnchanged,true);
const opts={cwd:process.cwd(),encoding:'utf8',stdio:['ignore','pipe','pipe'],windowsHide:true,maxBuffer:4*1024*1024,timeout:60000,env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'}};
const initial=json('INITIAL_REPLAY.json');assert.equal(initial.exitCode,0);assert.equal(initial.stdout.tests,43);assert.equal(initial.stdout.stageAstCases,2);assert.equal(initial.stdout.astNegatives,21);
// Preserve real admission: the approved Git wrapper now differs from one old
// Contact pin. Do not overlay old bytes, update the pin, or infer current PASS.
let currentReplay;
try{execFileSync(process.execPath,[capture.sourceFolder+'/verify.mjs'],opts);throw Error('EXPECTED_CURRENT_PIN_HOLD_NOT_OBSERVED');}
catch(error){assert.match(error.stderr?.toString()??'',/LOCAL_METADATA_REVIEWED_SOURCE_CHANGED/);
 const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',pins=JSON.parse(fs.readFileSync(root+'/'+prefix+'SOURCE_PINS.json')),changed=pins.rootInputs.flatMap(r=>{const b=fs.readFileSync(root+'/'+r.file);return sha(b)===r.sha256?[]:[{file:r.file,reviewed:r.sha256,current:sha(b)}];});
 assert.deepEqual(changed,[{file:'scripts/bpay-next-final-definers.mjs',reviewed:'51ab7fd6d5c582ee7f4cab612502c4fc7ac11088cb1a996dd2cf1e6561468d09',current:'b6a8143f2bae077a4b72857419b4f8fab9f7652181f6ad3bf1fdcfbd4b77bafa'}]);
 const b=fs.readFileSync(root+'/'+changed[0].file);for(const revision of ['51177b9abcdc9f089e680c9e7e9ef5285f09ff57','HEAD',''])assert(b.equals(execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,'show',revision+':'+changed[0].file],{windowsHide:true,maxBuffer:1024*1024})));
 currentReplay={status:'HOLD_APPROVED_MECHANICAL_SOURCE_PIN_DRIFT',code:'LOCAL_METADATA_REVIEWED_SOURCE_CHANGED',rootSourceCommit:'51177b9abcdc9f089e680c9e7e9ef5285f09ff57',mismatches:changed,native:'NOT_RUN'};
}
const own=JSON.parse(execFileSync('C:/Users/KierArthur/AppData/Local/Programs/Python/Python312/python.exe',[here+'/adversarial.py'],opts));assert.equal(own.actualFinalQueryAstRefusals.length,8);assert.equal(own.syntheticBagVectors.length,6);assert.equal(own.twoNativeCountStarNoCast,true);
const result={verdict:'ACCEPT_BOUNDED_SOURCE_EQUIVALENCE_CURRENT_ADMISSION_HOLD',sourceCommit:capture.sourceCommit,sourceManifestSha256:capture.sourceManifestSha256,sourceFiles:27,targets:54,changed:5,unchanged:49,wholeRawInverses:5,literalHunks:hunks,all54OriginalParentsSavedIndexHeadHistoricalEqualAtCapture:true,rawCaptureAndPrivateChildAndRaw21JoinUnchanged:true,originalSharedOwnersAndWeakMapUnchanged:true,readerSingleFinalQueryAnchorEdit:true,initialUnamendedReplay:initial.stdout,currentUnamendedReplay:currentReplay,independent:own,sourceAssertions:{completeAllRowArrays:true,allTwelveBusinessScopes:true,bigintCountStar:true,sameRowHashTextLineages:true,pipeDelimiterAndAscendingDefaultCollationAndNullEmptyRules:true,catalogueTextExporterAndGuardPrefixAndRollbackUnchanged:true,base15Background6Disjoint:true,observationsAtActualNewExecutionPoints:true},algorithmFindings:[],integrationRequirements:['Finite exact approved mechanical Git-wrapper pin and historical source-binding successor before actual admission'],native:'NOT_RUN',performance:'NOT_MEASURED',actualPostimageSourceAdmission:'HOLD_PENDING_EXACT_APPROVED_PIN_FORWARD',restorationQualified:false,BankSemanticBodiesRead:false};
if(record)fs.writeFileSync(here+'/RESULTS.json',JSON.stringify(result,null,2)+'\n',{flag:'wx'});else assert.deepEqual(json('RESULTS.json'),result);
console.log(JSON.stringify({verdict:result.verdict,sourceFiles:27,targets:54,wholeRawInverses:5,unchanged:49,originalTests:43,originalAstNegatives:21,independentValidSqlAstRefusals:8,independentBagVectors:6,native:'NOT_RUN',performance:'NOT_MEASURED'}));
