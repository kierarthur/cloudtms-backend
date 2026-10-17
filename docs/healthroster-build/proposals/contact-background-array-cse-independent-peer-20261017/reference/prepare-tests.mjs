// Literal source-only test/verification adaptation, preserving prior test files.
import fs from'node:fs';import assert from'node:assert/strict';import{here,put,json,sha}from'./prepare.mjs';
const old='D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/docs/healthroster-build/proposals/contact-same-rr-native-batch-forward-20261017',records=[];
function adapt(file,hunks){const before=fs.readFileSync(old+'/'+file,'utf8');let after=before;for(const h of hunks){assert.equal(after.split(h.before).length,2,file+':'+h.before);after=after.replace(h.before,()=>h.after);}put(file,after);records.push({file,oldSource:old+'/'+file,beforeSha256:sha(before),afterSha256:sha(after),hunks});}
adapt('batch.test.mjs',[
 {before:"import './batch-overlay.mjs';",after:"import './overlay.mjs';"},
 {before:"assert(!r.batch.backgroundQuery.includes(\"'backgroundWriterRoutines',\"));",after:"assert(!r.batch.parts.bgFields.includes(\"'backgroundWriterRoutines',\"));assert.equal(r.batch.descriptor.completeBackgroundArraysMaterializedOnce,true);"},
 {before:'assert.equal(r.parts.other.length+1,12);',after:'assert.equal(r.parts.other.length+1,10);assert.equal(r.parts.backgroundBusiness.length,2);'}
]);
adapt('coordinator.test.mjs',[{before:"import './batch-overlay.mjs';",after:"import './overlay.mjs';"}]);
adapt('verify.mjs',[
 {before:'assert.equal(unchanged,48);assert.equal(newCount,1);',after:'assert.equal(unchanged,49);assert.equal(newCount,0);'},
 {before:"let child=coordinator(after);child=child.replace('diagnosticMode=null,batch=false','diagnosticMode=null').replace('new BoundedMemoryCapture(phase,batch)','new BoundedMemoryCapture(phase)');assert.equal(child,coordinator(before));",after:'assert.equal(coordinator(after),coordinator(before));const captures=s=>s.slice(s.indexOf(\'export class BoundedMemoryCapture\'),s.indexOf(\'function assertPins(\'));assert.equal(captures(after),captures(before));const oldBatch=old(ADAPTER+\'/batch-source.mjs\').toString(),newBatch=proposed(ADAPTER+\'/batch-source.mjs\').toString();assert.equal(newBatch.slice(newBatch.indexOf(\'// Retain complete native JSON\')),oldBatch.slice(oldBatch.indexOf(\'// Retain complete native JSON\')));'},
 {before:"assert(proposed(ADAPTER+'/batch-source.mjs').equals(read('batch-source.mjs')));",after:"assert.equal(sha(proposed(ADAPTER+'/batch-source.mjs')),selection.files.find(r=>r.target===ADAPTER+'/batch-source.mjs').sha256);"},
 {before:'assert.equal(actual.length,6);',after:'assert.equal(actual.length,5);'},
 {before:"pathToFileURL(here+'/batch-overlay.mjs').href",after:"pathToFileURL(here+'/overlay.mjs').href"},
 {before:"here+'/coordinator.test.mjs',ROOT",after:"here+'/coordinator.test.mjs',here+'/array.test.mjs',ROOT"},
 {before:'# pass 37\\b',after:'# pass 43\\b'},
 {before:'assert.equal(ast.counterexamples.length,17);',after:'assert.equal(ast.counterexamples.length,21);'},
 {before:"version:'CONTACT_SAME_RR_NATIVE_BATCH_FINITE_SOURCE_PROOF_V1'",after:"version:'CONTACT_BACKGROUND_ARRAY_CSE_FINITE_SOURCE_PROOF_V1'"},
 {before:'changedExisting:5,newFiles:1,unchangedParentReferences:48,actualPayloadFiles:6',after:'changedExisting:5,newFiles:0,unchangedParentReferences:49,actualPayloadFiles:5'},
 {before:'privateChildControlRawUnchangedExceptCaptureSelection:true',after:'privateChildCaptureAndControlRawUnchanged:true,raw21MemberJoinRawUnchanged:true,backgroundCompleteArraysMaterializedOnce:true,businessBagRecords:[1,9],businessArrayRecords:2'},
 {before:'tests:37,originalContactTestSha256',after:'tests:43,originalContactTestSha256'},
 {before:'payloadFiles:6,wholeReverses:5,unchanged:48,tests:37,stageAstCases:2,astNegatives:17',after:'payloadFiles:5,wholeReverses:5,unchanged:49,tests:43,stageAstCases:2,astNegatives:21'}
]);
adapt('freeze.mjs',[
 {before:"version:'CONTACT_SAME_RR_NATIVE_BATCH_FREEZE_V1'",after:"version:'CONTACT_BACKGROUND_ARRAY_CSE_FREEZE_V1'"},
 {before:'actualPayloadFiles:6,wholeParentInverses:5,unchangedParentReferences:48,tests:37',after:'actualPayloadFiles:5,wholeParentInverses:5,unchangedParentReferences:49,tests:43'}
]);
put('TEST_PARENT_EDITS.json',json(records));console.log(JSON.stringify({adapted:4,oldTestHistoryPreserved:true}));
