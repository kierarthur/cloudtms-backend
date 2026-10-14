import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
import {caseIds,assertionsForCase,buildNativeComparisons,assertNativeComparisons,buildQualification,schema,recordPostVerifiedReportArtifacts,
 assertPostVerifiedReportReceipt,postVerifiedReceiptKeys,loadReportFinInputs} from './weekly-source-report-fin-ab-evidence.mjs';
import {domainHash} from './weekly-source-protected-release-qualification.mjs';
import {sourceInventory} from './weekly-source-protected-execution-tree.mjs';
import childProcess from 'node:child_process';import {syncBuiltinESMExports} from 'node:module';
import {preparePostVerifiedReportRequest,recordPostVerifiedReportEvidence} from './weekly-source-report-fin-ab-evidence.mjs';
import {unitRawInput} from './unit-raw-collector-fixture.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const hash='a'.repeat(64),id='10000000-0000-4000-8000-000000000001';
const execution={release_id:'UNIT_ONLY_R1_NOT_NATIVE_EVIDENCE',source_commit:'a'.repeat(40),contract_sha256:hash,installation_id:id,
 environment:'TEST',database:'cloudtms_test_clone',managed_manifest_sha256:hash,installed_catalogue_jsonb_sha256:hash};
const scope={agency_id:id,source_group_id:id,rule_set_version_id:id,rule_set_values_sha256:hash,
 effective_from_week_ending:'2026-10-18',effective_to_week_ending:null};
const fixture=()=>caseIds.flatMap(case_id=>['A','B'].map(variant=>{
 const row={schema_version:'WEEKLY_SOURCE_REPORT_FIN_NATIVE_VARIANT_V1',execution,scope,case_id,variant,
  source_fixture_sha256:hash,original_report_sha256:hash,report_row_manifest_sha256:hash,binding_authority_sha256:hash,
  native_tool_receipt:{accepted:true,database:'cloudtms_test_clone',rolledBack:true,connectedFresh:true,tool_receipt_sha256:hash,journey_id:'REPORT_FIN_'+case_id+'_'+variant},
  native_result:{source_cycle_id:id,report_scope_id:id,final_revision_id:id,client_manifest_id:id,invoice_id:id,
   fin_result_sha256:hash,admission_receipt_sha256:hash,validation_receipt_sha256:hash,rendered_document_sha256:hash,pay_isolation_receipt_sha256:hash},
  comparison:{schema_version:'WEEKLY_SOURCE_REPORT_FIN_COMPARISON_V1',net_pence:'100',vat_pence:'20',gross_pence:'120',
   rounding_pence:case_id==='D_POSITIVE'?'19':case_id==='D_NEGATIVE'?'-15':'0',report_lines_sha256:hash,
   invoice_target_sha256:hash,pay_isolation_sha256:hash,render_semantics_sha256:hash},assertions:Object.fromEntries(assertionsForCase(case_id).map(k=>[k,true]))};
 const receipt_bytes=JSON.stringify(row);return {receipt_id:'UNIT_ONLY/'+case_id+'/'+variant,receipt_bytes,receipt_sha256:sha(receipt_bytes)};
}));
const mutate=(inputs,index,fn)=>{const copy=structuredClone(inputs),row=JSON.parse(copy[index].receipt_bytes);fn(row);
 copy[index].receipt_bytes=JSON.stringify(row);copy[index].receipt_sha256=sha(copy[index].receipt_bytes);return copy;};
// These fabricated strings exercise only the codec. They are not native
// receipts, qualification, money/render proof, or inputs to a managed release.
test('metadata codec accepts exactly fourteen closed UNIT_ONLY envelopes',()=>assert.equal(buildNativeComparisons(execution,fixture()).length,14));
test('one variant alone is only partial measured evidence, never strategy qualification',()=>{
 const partial=buildNativeComparisons(execution,fixture().filter((_,i)=>i%2===0));assert.equal(partial.length,7);
 assert.throws(()=>assertNativeComparisons(execution,partial,{complete:true}),/CASE_CENSUS/);});
test('repeated case cannot replace a missing native journey',()=>{const f=fixture();f[2]=f[0];assert.throws(()=>buildNativeComparisons(execution,f),/CASE_CENSUS/);});
test('raw receipt bytes are bound before parsing',()=>{const f=fixture();f[0].receipt_bytes+=' ';assert.throws(()=>buildNativeComparisons(execution,f),/ARTIFACT/);});
test('compile-only or false gate is refused',()=>assert.throws(()=>buildNativeComparisons(execution,mutate(fixture(),0,r=>r.assertions.native_fin_atomic=false)),/ASSERTION_FAILED/));
test('missing renderer proof is refused',()=>assert.throws(()=>buildNativeComparisons(execution,mutate(fixture(),0,r=>delete r.native_result.rendered_document_sha256)),/RESULT_INVALID/));
test('different R1 catalogue is refused',()=>assert.throws(()=>buildNativeComparisons(execution,mutate(fixture(),0,r=>r.execution.installed_catalogue_jsonb_sha256='b'.repeat(64))),/EXECUTION_CHANGED/));
test('different exact report/export basis is refused',()=>assert.throws(()=>buildNativeComparisons(execution,mutate(fixture(),1,r=>r.binding_authority_sha256='b'.repeat(64))),/NOT_SAME_NATIVE_BASIS/));
test('different normalized invoice or pay comparison is refused',()=>assert.throws(()=>buildNativeComparisons(execution,mutate(fixture(),1,r=>r.comparison.pay_isolation_sha256='b'.repeat(64))),/NOT_SAME_NATIVE_BASIS/));
test('rounding test grain is enforced',()=>{const f=fixture(),index=f.findIndex(x=>JSON.parse(x.receipt_bytes).case_id==='D_NEGATIVE');
 assert.throws(()=>buildNativeComparisons(execution,mutate(f,index,r=>r.comparison.rounding_pence='0')),/CASE_INVALID/);});
test('native fresh rollback is mandatory',()=>assert.throws(()=>buildNativeComparisons(execution,mutate(fixture(),0,r=>r.native_tool_receipt.connectedFresh=false)),/TOOL_RECEIPT_INVALID/));
test('caller amount/strategy fields never enter the closed native receipt',()=>assert.throws(()=>buildNativeComparisons(execution,mutate(fixture(),0,r=>r.selected_variant='A')),/RECEIPT_INVALID/));
test('retained comparison ordering and bodies cannot drift',()=>{const rows=buildNativeComparisons(execution,fixture());assertNativeComparisons(execution,rows);
 const bad=structuredClone(rows);bad.reverse();assert.throws(()=>assertNativeComparisons(execution,bad),/BODY_CHANGED/);});
test('missing or default decision refuses before a certificate exists',()=>{const rows=buildNativeComparisons(execution,fixture());
 assert.throws(()=>buildQualification({...execution,release_id:'UNIT_ONLY_R2'},execution,rows,null,null,null),/DECISION_INVALID/);});
test('different installed execution cannot masquerade as the same installed qualification',()=>{const rows=buildNativeComparisons(execution,fixture()),decision={schema_version:'WEEKLY_SOURCE_REPORT_FIN_REVIEWED_DECISION_V1',selected_variant:'B',
 native_comparison_sha256:domainHash('WEEKLY_SOURCE_REPORT_FIN_NATIVE_COMPARISONS_V1',rows),reason:'UNIT_ONLY metadata vector; not qualification'};
 assert.throws(()=>buildQualification({...execution,release_id:'UNIT_DIFFERENT_RELEASE'},execution,rows,decision,null,null,'b'.repeat(40)),/INSTALLED_EXECUTION_CHANGED/);});
test('every complete successor reverses to its pinned original',()=>{const receipt=JSON.parse(read('FULL_REVERSE_RECEIPT.json'));
 for(const owner of receipt.owners){let source=read(owner.output);assert.equal(sha(source),owner.output_sha256);
  for(const hunk of [...owner.hunks].reverse()){assert.equal(source.split(hunk.new).length,2);source=source.replace(hunk.new,()=>hunk.old);}
  assert.equal(sha(source),owner.input_sha256);assert.equal(owner.full_reverse,true);}});
test('native admission and full ordinary I1 ladder keep their exact order',()=>{const s=read('04_complete_fin.sqlinc');
 assert.ok(s.indexOf('WEEKLY_SOURCE_FINALISE_REQUEST_UNKNOWN_FIELD')<s.indexOf('perform private.weekly_source_pay_query_admit_v2();'));
 assert.ok(s.indexOf('perform private.weekly_source_pay_query_admit_v2();')<s.indexOf('return private.weekly_source_report_fin_dispatch_v1(p_request);'));
 assert.ok(s.indexOf('return private.weekly_source_report_fin_dispatch_v1(p_request);')<s.indexOf('for v_lock_phase in 1..2 loop'));
 assert.equal((s.match(/perform private.weekly_source_pay_query_admit_v2\(\)/g)||[]).length,1);});
test('Report delegate rechecks qualified strategy after G1 and rolls back false native result',()=>{const s=read('03_operational_dispatch.sqlinc');
 const group=s.indexOf('from public.weekly_source_groups'),cycle=s.indexOf('from public.weekly_source_cycles'),report=s.indexOf('from public.weekly_source_report_scopes');
 assert.ok(group<cycle&&cycle<report);assert.ok(report<s.indexOf('after_value:=private.weekly_source_report_fin_strategy_v1'));
 assert.ok(s.indexOf('before_value is distinct from after_value')<s.indexOf('result:=private.weekly_source_self_bill_report_finalise_v1'));
 assert.match(s,/result->'invoice_only' is distinct from 'true'::jsonb/);assert.match(s,/raise exception 'WEEKLY_SOURCE_REPORT_FIN_NATIVE_RESULT_INVALID'/);
 assert.doesNotMatch(s,/set_config|total_pay_ex_vat|tsfin|ordinary_pay_projection_apply/);});
test('F1 receives no browser selector and preserves native admin/CAS/date guards',()=>{const s=read('05_complete_feature_setup.sqlinc');
 const closed=s.slice(s.indexOf('assert_request_v1'),s.indexOf('WEEKLY_SOURCE_RULE_FEATURE_REQUEST_INVALID'));assert.doesNotMatch(closed,/variant|report_fin/);
 assert.match(s,/_weekly_source_settings_require_admin_v1\(v_actor\)/);assert.match(s,/WEEKLY_SOURCE_RULE_ASSIGNMENT_STALE/);
 assert.match(s,/WEEKLY_SOURCE_RULE_FUTURE_WEEK_REQUIRED/);assert.match(s,/report_fin_qualification/);});
test('private ACL addition is exactly seven, with no service/browser grant',()=>{const sig=JSON.parse(read('PRIVATE_SIGNATURES.json'));assert.equal(sig.signatures.length,7);
 assert.equal(new Set(sig.signatures).size,7);const acl=read('07_owner_acl.sqlinc');assert.doesNotMatch(acl,/grant\s+execute/i);
 for(const s of sig.signatures)assert.ok(acl.includes('revoke all on function '+s+' from public,anon,authenticated,service_role;'));});
test('later strict complete Git parser retains every raw path and only existing bundle exceptions',()=>{const s=read('weekly-source-protected-execution-tree.mjs');
 assert.doesNotMatch(s,/\.filter\(row=>selected\(row\.path\)\)/);assert.match(s,/fatal/);assert.match(s,/weekly-source-protected-operational\.json/);
 assert.doesNotMatch(s,/report-fin-ab\.json|qualification-decisions/);});
test('native capture delegates only to the actual post-VERIFIED owner, never a direct ledger writer',()=>{const s=read('weekly-source-report-fin-ab-evidence.mjs');
 assert.match(s,/weekly_source_post_verified_evidence_record_v1/);assert.match(s,/expected_evidence_sha256/);assert.match(s,/receipt_input_commit/);
 assert.doesNotMatch(s,/update private\.cloudtms_database_releases|fetch\(|provider\.request|smtp|sendEmail|set_config\(/);});
test('Report-only receipt bundle never fabricates BULK/AUTO qualification',()=>{const s=read('09_complete_operational_evidence.mjs');
 assert.match(s,/bundle\.report_fin_qualifications/);assert.match(s,/return bundle\.qualifications\.map/);assert.doesNotMatch(s,/capabilities:.*REPORT/);});
test('absent Report binding supplies no bootstrap authority and cannot dispatch operationally',()=>{const binding=read('02_release_feature_strategy.sqlinc');
 assert.match(binding,/if p_version is null then return null; end if/);
 assert.match(binding,/self_bill_source' is distinct from 'SEPARATE_FINAL_REPORT' then return null/);
 assert.match(binding,/if p_operational is null then/);assert.match(binding,/WEEKLY_SOURCE_REPORT_FIN_OPERATIONAL_GATES_REQUIRED/);
 assert.match(binding,/receipt->'report_fin_qualification' is distinct from qualified/);
 assert.match(read('README.md'),/ordinary unqualified feature enablement is not an executable R1 path/i);
 const strategy=binding.slice(binding.indexOf('create or replace function private.weekly_source_report_fin_strategy_v1'));
 assert.doesNotMatch(strategy,/return null|coalesce.*selected_variant/);});
test('normal install writer/CLI remain exact; post-evidence never runs another install',()=>{
 for(const output of ['06_complete_managed_release.mjs','10_complete_current_release_caller.mjs']){
 const owner=JSON.parse(read('FULL_REVERSE_RECEIPT.json')).owners.find(r=>r.output===output);
 assert.equal(owner.hunks.length,0);assert.equal(owner.input_sha256,owner.output_sha256);}
 assert.doesNotMatch(read('weekly-source-report-fin-ab-evidence.mjs'),/applyManagedRelease\(/);});
test('non-NEXT measured evidence is truthful while NEXT qualification remains held',()=>{
 const rows=buildNativeComparisons(execution,fixture().filter(input=>JSON.parse(input.receipt_bytes).case_id!=='NEXT_ACTIVE'));
 assert.equal(rows.length,12);assert.ok(rows.every(row=>!Object.hasOwn(row.assertions,'next_active_no_expense_observation')));
 assert.throws(()=>assertNativeComparisons(execution,rows,{complete:true}),/CASE_CENSUS/);});
test('whole retained comparison body hash includes raw source, tool, and native outputs',()=>{
 const rows=buildNativeComparisons(execution,fixture());const corrupt=structuredClone(rows);corrupt[0].native_result.invoice_id='20000000-0000-4000-8000-000000000001';
 assert.throws(()=>assertNativeComparisons(execution,corrupt),/ARTIFACT_INVALID/);});
test('slot validators distinguish partial measurement from complete strategy and exact retained subset',()=>{
 const slots=read('13_slot_validators.sqlinc'),certificate=read('01_certificate.sqlinc');
 assert.match(slots,/jsonb_array_length\(p_artifacts\) not between 1 and 14/);assert.match(slots,/case_id'='NEXT_ACTIVE'/);
 assert.match(certificate,/cardinality\(required_cases\)\*2/);assert.match(certificate,/where retained_case=certificate_case/);
 assert.match(certificate,/execution is distinct from installation/);assert.match(certificate,/receipt_input_commit/);});
const receiptCommit='b'.repeat(40);
function unitTreeProof(){
 const body={schema_version:'WEEKLY_SOURCE_COMMITTED_EXECUTION_TREE_EQUIVALENCE_V2',execution_source_commit:execution.source_commit,
  installation_source_commit:receiptCommit,inventory_schema:'WEEKLY_SOURCE_NATIVE_RELEASE_TOOLCHAIN_VERIFIER_INVENTORY_V2',
  files:sourceInventory.required.map(file=>({path:file,git_mode:'100644',git_type:'blob',execution_blob_oid:'a'.repeat(40),
   installation_blob_oid:'a'.repeat(40),execution_sha256:hash,installation_sha256:hash,
   comparison:file===sourceInventory.manifest?'RELEASE_AUTHORITY_EXCEPT_CLOSED_RECEIPT_DESCRIPTORS':'EXACT_GIT_OBJECT',comparison_sha256:hash})),
  receipt_bindings:[]};
 return {...body,source_equivalence_sha256:domainHash(body.schema_version,body)};
}
const recordRequest=()=>({schema_version:'WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECORD_V1',expected_execution:execution,
 expected_evidence_sha256:hash,receipt_input_commit:receiptCommit,receipt_tree_proof:unitTreeProof(),
 artifact_slot:'weekly_source_report_fin_native_comparisons',artifacts:buildNativeComparisons(execution,fixture().slice(0,2)),
 reason:'UNIT_ONLY return binding vector, never a retained native artifact',turn_backend_pid:123});
function unitRecorderReceipt(request,{replayed=false}={}){
 const body={schema_version:'WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECEIPT_V1',execution:request.expected_execution,
  receipt_input_commit:request.receipt_input_commit,receipt_tree_sha256:request.receipt_tree_proof.source_equivalence_sha256,
  artifact_slot:request.artifact_slot,artifacts_sha256:domainHash('WEEKLY_SOURCE_POST_VERIFIED_ARTIFACT_SET_V1',request.artifacts),
  prior_evidence_sha256:request.expected_evidence_sha256,evidence_sha256:replayed?request.expected_evidence_sha256:'c'.repeat(64),
  actor_user_id:id,recorded_at_utc:'2026-10-14T23:40:01.123456+00:00',replayed,operational_settings_changed:false};
 return {...body,receipt_sha256:domainHash('WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECEIPT_V1',body)};
}
function rehashReceipt(receipt){const {receipt_sha256,...body}=receipt;
 return {...body,receipt_sha256:domainHash('WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECEIPT_V1',body)};}
test('actual recorder adapter sends only one closed owner call and binds all thirteen returned fields',async()=>{
 const request=recordRequest(),expected=unitRecorderReceipt(request);let calls=0;
 const literal=value=>"'"+value.replaceAll("'","''")+"'";
 const result=await recordPostVerifiedReportArtifacts({actorUserId:id,expectedExecution:execution,expectedEvidenceSha256:hash,
  receiptInputCommit:receiptCommit,receiptTreeProof:request.receipt_tree_proof,artifactSlot:request.artifact_slot,artifacts:request.artifacts,
  reason:request.reason,turnBackendPid:request.turn_backend_pid,literal,
  execute:async sql=>{calls++;assert.equal(sql,'select private.weekly_source_post_verified_evidence_record_v1('+literal(id)+'::uuid,'+
    literal(JSON.stringify(request))+'::jsonb)::text;');return JSON.stringify(expected);}});
 assert.equal(calls,1);assert.equal(postVerifiedReceiptKeys.length,13);assert.deepEqual(result,expected);
});
test('exact append replay preserves the evidence hash and cannot claim an operational change',()=>{
 const request=recordRequest(),receipt=unitRecorderReceipt(request,{replayed:true});assertPostVerifiedReportReceipt(receipt,request,id);
 receipt.operational_settings_changed=true;assert.throws(()=>assertPostVerifiedReportReceipt(rehashReceipt(receipt),request,id),/RECEIPT_INVALID/);
});
for(const [label,change] of [
 ['other execution',r=>r.execution={...execution,source_commit:'d'.repeat(40)}],
 ['other receipt commit',r=>r.receipt_input_commit='d'.repeat(40)],
 ['other measured tree',r=>r.receipt_tree_sha256='d'.repeat(64)],
 ['other artifact slot',r=>r.artifact_slot='weekly_source_report_fin_qualifications'],
 ['other artifact set',r=>r.artifacts_sha256='d'.repeat(64)],
 ['other prior evidence CAS',r=>r.prior_evidence_sha256='d'.repeat(64)],
 ['other actor',r=>r.actor_user_id='20000000-0000-4000-8000-000000000001'],
 ['false append replay',r=>r.replayed=true],
 ['string replay flag',r=>r.replayed='false'],
 ['invalid time',r=>r.recorded_at_utc='2026-02-30T23:40:01+00:00']
])test('correctly rehashed receipt still refuses '+label,()=>{const request=recordRequest(),receipt=unitRecorderReceipt(request);change(receipt);
 assert.throws(()=>assertPostVerifiedReportReceipt(rehashReceipt(receipt),request,id),/RECEIPT_INVALID/);});
test('receipt closure and domain cannot be replaced by schema-only success',()=>{
 const request=recordRequest();assert.throws(()=>assertPostVerifiedReportReceipt({schema_version:'WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECEIPT_V1'},request,id),/RECEIPT_INVALID/);
 const receipt=unitRecorderReceipt(request);receipt.receipt_sha256=hash;assert.throws(()=>assertPostVerifiedReportReceipt(receipt,request,id),/RECEIPT_INVALID/);
 receipt.unreviewed='extra';assert.throws(()=>assertPostVerifiedReportReceipt(rehashReceipt(receipt),request,id),/RECEIPT_INVALID/);
});
test('native returned failures, malformed JSON and evidence conflicts never become successful records',async()=>{
 const request=recordRequest(),options={actorUserId:id,expectedExecution:execution,expectedEvidenceSha256:hash,receiptInputCommit:receiptCommit,
  receiptTreeProof:request.receipt_tree_proof,artifactSlot:request.artifact_slot,artifacts:request.artifacts,reason:request.reason,
  turnBackendPid:request.turn_backend_pid,literal:value=>"'"+value.replaceAll("'","''")+"'"};
 for(const returned of ['null','{bad JSON',JSON.stringify({ok:false,error:'WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_CHANGED'})])
  await assert.rejects(recordPostVerifiedReportArtifacts({...options,execute:async()=>returned}),/RECEIPT_INVALID/);
 const conflict=Object.assign(Error('WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_CHANGED'),{code:'40001'});
 await assert.rejects(recordPostVerifiedReportArtifacts({...options,execute:async()=>{throw conflict;}}),error=>error===conflict);
});
test('invalid turn and mixed execution refuse before the recorder callback runs',async()=>{
 const request=recordRequest();let calls=0;const options={actorUserId:id,expectedExecution:execution,expectedEvidenceSha256:hash,receiptInputCommit:receiptCommit,
  receiptTreeProof:request.receipt_tree_proof,artifactSlot:request.artifact_slot,artifacts:request.artifacts,reason:request.reason,
  turnBackendPid:request.turn_backend_pid,literal:value=>JSON.stringify(value),execute:async()=>{calls++;return 'null';}};
 await assert.rejects(recordPostVerifiedReportArtifacts({...options,turnBackendPid:0}),/INPUT_INVALID/);
 await assert.rejects(recordPostVerifiedReportArtifacts({...options,expectedExecution:{...execution,source_commit:'d'.repeat(40)}}),/EXECUTION_TREE/);
 assert.equal(calls,0);
});
test('native SQL checks array types before SRFs and validates the same closed scope lexical facts',()=>{
 const certificate=read('01_certificate.sqlinc'),slots=read('13_slot_validators.sqlinc');
 assert.ok(certificate.indexOf("jsonb_typeof(p_certificate->'native_comparisons')")<certificate.indexOf('jsonb_array_elements(p_certificate'));
 assert.ok(slots.indexOf("jsonb_typeof(p_artifacts)")<slots.indexOf('jsonb_array_length(p_artifacts)'));
 assert.match(slots,/effective_from_week_ending'\)::date::text/);assert.match(slots,/invalid_datetime_format/);
});
test('Report strategy requires the same full D9 receipt and native-to-future window owner',()=>{
 const binding=read('02_release_feature_strategy.sqlinc'),feature=read('05_complete_feature_setup.sqlinc');
 assert.match(binding,/weekly_source_protected_operational_certificate_assert_v1\(p_operational,p_group,p_from,p_to/);
 assert.match(binding,/assignment\.effective_to_week_ending,receipt->'protected_capability_qualification'/);
 assert.match(binding,/fixture_rule_set_version_id' is distinct from p_version::text/);
 assert.match(binding,/fixture_rule_values_sha256' is distinct from activation->>'rule_values_sha256'/);
 assert.match(binding,/effective_from_week_ending}'=native_from::text/);
 assert.match(binding,/certificate_assert_v1\(result,p_group,p_version,native_from,native_to\)/);
 assert.match(feature,/effective_to_week_ending,v_qualification\)/);
 assert.doesNotMatch(binding,/update |insert |set_config|CLIENT_HTTP.*true|MYTMS_PHONE.*true/);
});
test('receipt bundle lookup never silently uses the installed source as receipt data',()=>{
 assert.throws(()=>loadReportFinInputs('UNUSED_UNIT_ONLY_PATH',{}, {gitCommit:execution.source_commit}),/RECEIPT_INPUT_COMMIT_REQUIRED/);
 const source=read('weekly-source-report-fin-ab-evidence.mjs');
 assert.match(source,/loadCommittedBundle\(root,release,\{\.\.\.context,gitCommit:context\.receiptInputCommit\}/);
 assert.match(source,/'rev-parse',context\.receiptInputCommit\+':'\+file/);
});
// Built-in I/O is intercepted only for UNIT_ONLY_ROOT. No Git, filesystem,
// database or network mutation occurs; these bytes are not native evidence.
async function withUnitCommittedBundle(t,change,run){
 const unitRoot='UNIT_ONLY_ROOT',bundlePath=sourceInventory.receiptBundles.weeklySourceProtectedOperational;
 const captured=['A','B'].map(variant=>unitRawInput(execution,variant));
 const bundle={schema_version:'WEEKLY_SOURCE_PROTECTED_OPERATIONAL_INPUT_BUNDLE_V1',qualifications:[],
  report_fin_native_inputs:captured.map(row=>row.input),report_fin_native_artifacts:captured.map(row=>row.artifact)};
 change?.(bundle);
 const bundleBytes=Buffer.from(JSON.stringify(bundle));
 const beforeManifest=Buffer.from(JSON.stringify({releaseId:execution.release_id,untouched_native_authority:'UNIT_ONLY'}));
 const afterManifest=Buffer.from(JSON.stringify({releaseId:execution.release_id,untouched_native_authority:'UNIT_ONLY',
  weeklySourceProtectedOperational:{path:bundlePath,sha256:sha(bundleBytes)}}));
 const blobs=new Map(),object=bytes=>{const oid=crypto.createHash('sha1').update(bytes).digest('hex');blobs.set(oid,bytes);return oid;};
 const native=sourceInventory.required.map(file=>({path:file,mode:'100644',type:'blob',
  blob:object(file===sourceInventory.manifest?beforeManifest:Buffer.from('UNIT_ONLY_NATIVE '+file))}));
 const after=native.map(row=>row.path===sourceInventory.manifest?{...row,blob:object(afterManifest)}:row);
 after.push({path:bundlePath,mode:'100644',type:'blob',blob:object(bundleBytes)});
 const tree=rows=>Buffer.from(rows.map(row=>row.mode+' '+row.type+' '+row.blob+'\t'+row.path+'\0').join(''));
 const originalRead=fs.readFileSync,originalGit=childProcess.execFileSync;
 const fsMock=t.mock.method(fs,'readFileSync',(file,...args)=>{
  const prefix=unitRoot+path.sep;if(typeof file==='string'&&file.startsWith(prefix)){
   const relative=file.slice(prefix.length).replaceAll(path.sep,'/');const bytes=relative===sourceInventory.manifest?afterManifest:relative===bundlePath?bundleBytes:null;
   if(!bytes)throw Error('UNIT_ONLY_UNEXPECTED_DISK_READ '+relative);return args[0]==='utf8'?bytes.toString('utf8'):bytes;}
  return originalRead(file,...args);
 });
 const gitMock=t.mock.method(childProcess,'execFileSync',(command,args,options)=>{
  if(command!=='git'||options?.cwd!==unitRoot)return originalGit(command,args,options);
  const query=args.slice(4);
  if(query[0]==='ls-tree')return tree(query.at(-1)===execution.source_commit?native:after);
  if(query[0]==='cat-file')return Buffer.concat(options.input.trim().split('\n').map(oid=>{
   const bytes=blobs.get(oid);assert.ok(bytes);return Buffer.concat([Buffer.from(oid+' blob '+bytes.length+'\n'),bytes,Buffer.from('\n')]);}));
  if(query[0]==='show'||query[0]==='rev-parse'){
   const [revision,...parts]=query[1].split(':'),file=parts.join(':'),row=(revision===execution.source_commit?native:after).find(row=>row.path===file);
   assert.ok(row);return query[0]==='show'?blobs.get(row.blob):row.blob+'\n';}
  throw Error('UNIT_ONLY_UNEXPECTED_GIT_CALL '+query.join(' '));
 });syncBuiltinESMExports();
 try{return await run(unitRoot);}finally{gitMock.mock.restore();fsMock.mock.restore();syncBuiltinESMExports();}
}
const producerOptions=root=>({root,expectedExecution:execution,actorUserId:id,expectedEvidenceSha256:hash,receiptInputCommit:receiptCommit,
 artifactSlot:'weekly_source_report_fin_native_comparisons',reason:recordRequest().reason,turnBackendPid:123,
 execute:async()=>{throw Error('UNIT_ONLY_NATIVE_PREPARATION_MUST_NOT_QUERY_DB');},literal:value=>"'"+value.replaceAll("'","''")+"'"});
test('default committed producer derives partial artifacts from exact bound raw bytes without SQL',async t=>{
 await withUnitCommittedBundle(t,null,async root=>{const request=await preparePostVerifiedReportRequest(producerOptions(root));
  assert.equal(request.artifacts.length,2);assert.equal(request.receipt_input_commit,receiptCommit);
  assert.equal(request.expected_execution.source_commit,execution.source_commit);
  assert.deepEqual(request.artifacts,buildNativeComparisons(execution,['A','B'].map(variant=>{const {input}=unitRawInput(execution,variant);
   return {receipt_id:input.receipt_id,receipt_bytes:input.receipt,receipt_sha256:input.receipt_sha256};})));
  assert.ok(request.receipt_tree_proof.receipt_bindings.some(row=>row.path===sourceInventory.receiptBundles.weeklySourceProtectedOperational));
 });
});
test('correct bundle descriptor cannot hide changed raw native bytes or extra caller fields',async t=>{
 for(const change of [bundle=>bundle.report_fin_native_inputs[0].receipt+=' ',bundle=>bundle.report_fin_native_inputs[0].variant='A'])
  await withUnitCommittedBundle(t,change,root=>assert.rejects(preparePostVerifiedReportRequest(producerOptions(root)),/ARTIFACT_INVALID|INPUT_INVALID|NOT_REDERIVED/));
});
test('owner session commits only after exact returned receipt validation',async t=>{
 await withUnitCommittedBundle(t,null,async root=>{const request=await preparePostVerifiedReportRequest(producerOptions(root)),calls=[];
  const session={execute:async sql=>{calls.push(sql);
   if(sql.startsWith('select pg_try_advisory_lock'))return 't';
   if(sql.startsWith('select jsonb_build_object'))return JSON.stringify({execution,evidence_sha256:hash});
   if(sql.startsWith('select private.weekly_source_post_verified_evidence_record_v1'))return JSON.stringify(unitRecorderReceipt(request));
   return '';
  }};
  await recordPostVerifiedReportEvidence({session,root,actorUserId:id,receiptInputCommit:receiptCommit,
   artifactSlot:request.artifact_slot,reason:request.reason,turnBackendPid:123});
  assert.equal(calls.filter(sql=>sql==='commit;').length,1);assert.ok(calls.indexOf('commit;')>calls.findIndex(sql=>sql.includes('evidence_record_v1')));
  assert.ok(calls.at(-1).startsWith('select pg_advisory_unlock'));assert.ok(!calls.includes('rollback;'));
 });
});
test('owner session rolls back malformed returns and partially begun authority failures',async t=>{
 for(const kind of ['RETURN_FAILURE','BEGIN_AUTHORITY_FAILURE'])await withUnitCommittedBundle(t,null,async root=>{
  const calls=[],failure=Error('UNIT_ONLY_NATIVE_AUTHORITY_FAILURE');const session={execute:async sql=>{calls.push(sql);
   if(sql.startsWith('select pg_try_advisory_lock'))return 't';
   if(sql.startsWith('begin;')&&kind==='BEGIN_AUTHORITY_FAILURE')throw failure;
   if(sql.startsWith('select jsonb_build_object'))return JSON.stringify({execution,evidence_sha256:hash});
   if(sql.startsWith('select private.weekly_source_post_verified_evidence_record_v1'))return JSON.stringify({ok:false});return '';
  }};
  await assert.rejects(recordPostVerifiedReportEvidence({session,root,actorUserId:id,receiptInputCommit:receiptCommit,
   artifactSlot:'weekly_source_report_fin_native_comparisons',reason:recordRequest().reason,turnBackendPid:123}),/AUTHORITY_FAILURE|RECEIPT_INVALID/);
  assert.ok(calls.includes('rollback;'));assert.ok(!calls.includes('commit;'));assert.ok(calls.at(-1).startsWith('select pg_advisory_unlock'));
 });
});
test('busy writer admission never begins, reads artifacts, rolls back, or unlocks an unowned mutex',async()=>{
 const calls=[];await assert.rejects(recordPostVerifiedReportEvidence({session:{execute:async sql=>{calls.push(sql);return 'f';}},
  root:'UNIT_ONLY_UNUSED',actorUserId:id,receiptInputCommit:receiptCommit,artifactSlot:'weekly_source_report_fin_native_comparisons',
  reason:recordRequest().reason,turnBackendPid:123}),/MANAGED_WRITER_BUSY/);
 assert.equal(calls.length,1);
});
