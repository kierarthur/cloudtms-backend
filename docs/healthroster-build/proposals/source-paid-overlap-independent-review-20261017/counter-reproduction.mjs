// Frozen functional defect reproduction only. No native or network calls.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(here,'../../../..');
assert(fs.realpathSync.native(own).replaceAll('\\','/').startsWith('D:/Development/GitHub/'));
const pin='9c791fa971c97a198c09c4e3579c01b589486c14',parent='ef64f43909db9811efaf1021d19f2fddc4bd44e6';
const base='docs/healthroster-build/proposals/same-separate-health-current-successor-20261017/payload/broker/src/weekly-source/';
const blob=(commit,file)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',commit+':'+file],{cwd:own,windowsHide:true,maxBuffer:1024*1024});
const data=s=>'data:text/javascript;base64,'+Buffer.from(s).toString('base64');
const timing=blob(parent,'broker/src/weekly-source/push-timing-office.mjs').toString();
const command=blob(pin,base+'same-separate-command.mjs').toString().replace("'./push-timing-office.mjs'",JSON.stringify(data(timing)));
const bytes=blob(pin,base+'correct-final-source-orchestrator.mjs');
assert.equal(createHash('sha256').update(bytes).digest('hex'),'905e51fa2c36587650b6afea2246d6cd5039abda075531a5673e7acb9585a8a5');
const {reviewWeeklyCorrectFinalPaidMatchRecheck}=await import(data(bytes.toString().replace("'./same-separate-command.mjs'",JSON.stringify(data(command)))));
const id='ffffffff-ffff-ffff-ffff-ffffffffffff',h='a'.repeat(64);
const request={request_id:id,actor_user_id:id,correction_session_id:id,upload_id:id,expected_authority_scope_version:'3',expected_row_manifest_hash:h};
const frame={schema_version:'WEEKLY_SOURCE_PAID_MATCH_CORRECTION_RECHECK_CONTEXT_V1',correction_session_id:id,source_cycle_id:id,expected_current_final_revision_id:id,expected_final_manifest_hash:h};
const begun={purpose:'FINAL_SOURCE_CORRECTION',correction_session_id:id,upload_id:id,publication_id:id,correction_review_identity:frame};
const rows=[];
for(const [name,authority,version,expectedCode]of [
 ['NATIVE_SMALL_NUMBER_VERSUS_REQUEST_STRING',3,7,'WEEKLY_SOURCE_CORRECTION_REBUILD_IDENTITY_MISMATCH'],
 ['CANONICAL_INT63_READY_SESSION_ABOVE_2_POW_53','3','9007199254740993','WEEKLY_SOURCE_CORRECTION_VERSION_INVALID']]){
 let calls=0,error;
 try{await reviewWeeklyCorrectFinalPaidMatchRecheck({dataRpc:async()=>{calls++;throw Error('UNEXPECTED_RPC_CALL');}},request,begun,{ok:true,status:'CORRECTION_READY',upload_id:id,publication_id:id,authority_scope_version:authority,ready_session_version:version,comparison_manifest_hash:h,issue_set_hash:h});}catch(e){error=e;}
 assert.equal(error?.code,expectedCode);assert.equal(calls,0);
 rows.push({name,requestAuthority:'3',nativeAuthority:authority,nativeReadySession:version,observedCode:error.code,status:error.status,rpcCalls:calls,defectReproduced:true});
}
let calls=0;
const scopeLost=await reviewWeeklyCorrectFinalPaidMatchRecheck({dataRpc:async(name,args)=>{calls++;assert.equal(name,'weekly_source_correct_final_review_atomic_v1');assert.equal(args.p_request.correction_session_id,id);return{ok:true,status:'READY_FOR_CONFIRMATION',correction_session_id:id,prior_final_revision_id:id,upload_id:id,version:8,office_preview:{changes:[],blockers:[],confirmation_text:'Confirm actual replacement report',preview_hash:h}};}},request,begun,{ok:true,status:'CORRECTION_READY',upload_id:id,publication_id:id,authority_scope_version:'3',ready_session_version:7,comparison_manifest_hash:h,issue_set_hash:h});
assert.equal(calls,1);assert.equal(scopeLost.review_context.authority_scope_kind,'CYCLE');assert.equal(scopeLost.review_context.report_scope_id,null);assert.deepEqual(scopeLost.review_context,scopeLost.apply_context);
rows.push({name:'FIVE_FIELD_NATIVE_FRAME_CANNOT_PRESERVE_NHSP_REPORT_SCOPE',nativeFrameKeys:Object.keys(frame),observedKind:scopeLost.review_context.authority_scope_kind,observedReportId:scopeLost.review_context.report_scope_id,rpcCalls:calls,defectReproduced:true,qualification:'The five-field native006 frame has identical shape for CYCLE and NHSP_REPORT_SCOPE; no report identity can survive this callback. The controlled RPC confirms the actual output contexts, not native execution.'});
fs.writeFileSync(path.join(here,'COUNTER_FINDING_REPRODUCTION.json'),JSON.stringify({functionalCommit:pin,sourceSha256:'905e51fa2c36587650b6afea2246d6cd5039abda075531a5673e7acb9585a8a5',cases:rows,native:'NOT_RUN',databaseConnections:0},null,2)+'\n');
console.log(JSON.stringify({frozenDefectReproduced:rows.length,nativeCalls:0}));
