import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
import {collectorMaxBytes,deriveReportNativeReceipt,assertReportNativeArtifacts,collectReportNativeVariant,collectReportNativePair,reportFrozenSemantics,
 reportNativeBasisSql,reportNativeFinancialSql,reportNativeIsolationSql} from './weekly-source-report-fin-native-collector.mjs';
import {unitRawTrace,unitRawInput} from './unit-raw-collector-fixture.mjs';
import {sha256,domainHash} from './weekly-source-protected-release-qualification.mjs';
const id='10000000-0000-4000-8000-000000000001',hash='a'.repeat(64),execution={release_id:'UNIT_ONLY_NOT_NATIVE',source_commit:'a'.repeat(40),
 contract_sha256:hash,installation_id:id,environment:'TEST',database:'cloudtms_test_clone',managed_manifest_sha256:hash,installed_catalogue_jsonb_sha256:hash};
const mutateRaw=(trace,key,change)=>{const value=JSON.parse(trace[key]);change(value);trace[key]=JSON.stringify(value);};
test('fabricated raw vector exercises rederivation but is never saved as native evidence',()=>{
 const trace=unitRawTrace(execution),result=deriveReportNativeReceipt(JSON.stringify(trace));
 assert.equal(result.status,'MEASURED_NATIVE_VARIANT');assert.equal(JSON.parse(result.receipt).native_tool_receipt.tool_receipt_sha256,sha256(JSON.stringify(trace)));
});
test('actual missing document/native mapper produces WAITING without a recordable receipt',()=>{
 const result=deriveReportNativeReceipt(JSON.stringify(unitRawTrace(execution,{complete:false})));
 assert.equal(result.status,'WAITING_NATIVE_GATES');assert.equal(result.receipt,null);assert.ok(result.waiting.includes('native_frozen_render'));
});
for(const [label,key,change] of [
 ['FIN refused','fin_bytes',value=>value.ok=false],['unrelated FIN scope','fin_bytes',value=>value.report_scope_id='20000000-0000-4000-8000-000000000001'],
 ['invoice validator refused','financial_bytes',value=>value.validation.ok=false],['admission refused','admission_bytes',value=>value.status='WAITING'],
 ['nonzero invoice pay','financial_bytes',value=>value.movements[0].canonical_pay_vector_json.pay=1],
 ['forbidden pay origin','financial_bytes',value=>value.movements[0].source_facts_json.row_resolution_id=id],
 ['changed Request leading zeros','financial_bytes',value=>value.movements[0].source_facts_json.request_id='7'],
 ['changed literal charge','financial_bytes',value=>value.net_pence='101'],['unknown amount','financial_bytes',value=>value.net_pence=null],
 ['unexpected report transition','financial_bytes',value=>value.transition_count='1'],['changed pay fingerprint','after_bytes',value=>value.pay='b'.repeat(64)],
 ['fresh connection reused','fresh_bytes',value=>value.backend_pid='123'],['fresh source not restored','fresh_basis_bytes',value=>value.scope.current_final_revision_id=id],
 ['foreign current release','fresh_execution_bytes',value=>value.source_commit='b'.repeat(40)],
 ['different genuine original file','basis_bytes',value=>{value.report.content_sha256='\\x'+'b'.repeat(64);value.upload.content_sha256=value.report.content_sha256;}],
 ['changed publication CAS','basis_bytes',value=>value.publication.comparison_manifest_hash='\\x'+'b'.repeat(64)],
 ['foreign source row upload','basis_bytes',value=>value.rows[0].upload_id='20000000-0000-4000-8000-000000000001'],
 ['different native request reference','reference_bytes',value=>value.native_request_sha256='b'.repeat(64)],
 ['foreign financial revision scope','financial_bytes',value=>value.revision.report_scope_id='20000000-0000-4000-8000-000000000001'],
 ['unbound membership hash','financial_bytes',value=>value.membership[0].movement_hash='\\x'+'b'.repeat(64)],
 ['absent audit-owned OFF frame','financial_bytes',value=>value.off_audit_frames.pop()],
 ['wrong OFF native result hash','financial_bytes',value=>{const frame=JSON.parse(value.off_audit_frames[2].after_bytes);frame.native_result_sha256='b'.repeat(64);value.off_audit_frames[2].after_bytes=JSON.stringify(frame);}],
 ['unbound native presentation','financial_bytes',value=>value.presentation_lines[0].billing_movement_id='20000000-0000-4000-8000-000000000001'],
 ['changed frozen worker snapshot','financial_bytes',value=>value.presentation_lines[0].candidate_display_snapshot='OTHER_UNIT_ONLY']
])test('raw native derivation refuses '+label,()=>{
 const trace=unitRawTrace(execution);mutateRaw(trace,key,change);assert.throws(()=>deriveReportNativeReceipt(JSON.stringify(trace)));
});
test('a closed schema or caller PASS does not substitute for native gate observations',()=>{
 const trace=unitRawTrace(execution);trace.native_gate_facts.native_candidate_i1={ok:true};
 assert.throws(()=>deriveReportNativeReceipt(JSON.stringify(trace)),/GATE_FACTS_INVALID/);
});
test('rehashed native observation with wrong execution refuses',()=>{
 const trace=unitRawTrace(execution),gate=trace.native_gate_facts.origin_authority,result=JSON.parse(gate.result_bytes);
 result.execution={...execution,source_commit:'b'.repeat(40)};gate.result_bytes=JSON.stringify(result);
 assert.throws(()=>deriveReportNativeReceipt(JSON.stringify(trace)),/GATE_EXECUTION_CHANGED/);
});
test('native observation missing required origin cannot become a financial PASS',()=>{
 const trace=unitRawTrace(execution),gate=trace.native_gate_facts.origin_authority,result=JSON.parse(gate.result_bytes);
 result.observations.pop();result.native_result_sha256=domainHash('WEEKLY_SOURCE_REPORT_FIN_NATIVE_GATE_OBSERVATIONS_V1',result.observations);gate.result_bytes=JSON.stringify(result);
 assert.throws(()=>deriveReportNativeReceipt(JSON.stringify(trace)),/GATE_FACTS_INVALID/);
});
test('render semantic bytes must be rederived from the entire real frozen model and native membership',()=>{
 const trace=unitRawTrace(execution);trace.document.semantic_bytes='{}';
 assert.throws(()=>deriveReportNativeReceipt(JSON.stringify(trace)),/DOCUMENT_SEMANTICS_CHANGED/);
 const f=JSON.parse(trace.financial_bytes),model=JSON.parse(trace.document.frozen_model_bytes);model.lines[0].quantity='2';
 assert.notEqual(reportFrozenSemantics(JSON.stringify(model),f),reportFrozenSemantics(trace.document.frozen_model_bytes,f));
});
test('render bytes and render receipt hashes are measured independently',()=>{
 const trace=unitRawTrace(execution);trace.document.document_bytes_base64=Buffer.from('%PDF-CHANGED_UNIT_ONLY').toString('base64');
 assert.throws(()=>deriveReportNativeReceipt(JSON.stringify(trace)),/DOCUMENT_INVALID/);
});
test('closed collector bundle binds each input and refuses missing, duplicate, changed or oversized traces',()=>{
 const captured=unitRawInput(execution);assertReportNativeArtifacts([captured.input],[captured.artifact]);
 assert.throws(()=>assertReportNativeArtifacts([captured.input],[]),/CENSUS_INVALID/);
 assert.throws(()=>assertReportNativeArtifacts([captured.input],[{...captured.artifact,caller_assertions:true}]),/ARTIFACT_INVALID/);
 assert.throws(()=>assertReportNativeArtifacts([captured.input],[{...captured.artifact,collector_bytes:captured.artifact.collector_bytes+' '}]),/ARTIFACT_INVALID/);
 assert.throws(()=>assertReportNativeArtifacts([captured.input],[{...captured.artifact,collector_bytes:'x'.repeat(collectorMaxBytes+1)}]),/ARTIFACT_INVALID/);
 assert.throws(()=>assertReportNativeArtifacts([captured.input],[{...captured.artifact,collector_bytes:JSON.stringify(unitRawTrace(execution,{complete:false})),
  collector_sha256:sha256(JSON.stringify(unitRawTrace(execution,{complete:false})))}]),/NOT_REDERIVED/);
});
function unitTransport(trace,{turn=true,writer=true,failAt=null,failVariant=null,reusedFresh=false,pairTraces=null}={}){
 const calls=[],failure=Error('UNIT_ONLY_NATIVE_REFUSAL');let isolation=0,active=trace;
 const session={execute:async sql=>{calls.push(sql);if(failAt&&sql.includes(failAt)&&(!failVariant||active.variant===failVariant))throw failure;
  if(sql.startsWith('select exists(select 1 from pg_locks'))return turn?'t':'f';
  if(sql.startsWith('select pg_try_advisory_xact_lock_shared'))return writer?'t':'f';
  if(sql.startsWith('begin;')||sql==='rollback;')return '';
  if(sql.startsWith('select 1 from public.weekly_source_groups'))return '1';
  if(sql.startsWith('savepoint ')||sql.startsWith('rollback to savepoint '))return '';
  if(sql.startsWith('select private.weekly_source_protected_qualification_release_v1'))return JSON.stringify(execution);
  if(sql.startsWith("select jsonb_build_object('cycle'"))return trace.basis_bytes;
  if(sql===reportNativeIsolationSql)return isolation++===0?trace.before_bytes:trace.after_bytes;
  if(sql.startsWith('select private.weekly_source_rule_report_fixture_bind_v1')){
   if(pairTraces)active=pairTraces.find(item=>sql.includes('"variant":"'+item.variant+'"'));return active.reference_bytes;}
  if(sql.startsWith('select public.weekly_source_group_rule_feature_set_atomic_v1'))return active.fin_bytes;
  if(sql.startsWith("select jsonb_build_object('id'"))return JSON.stringify({id,sha256:hash});
  if(sql.startsWith('select public.weekly_source_invoice_admit_atomic_v1'))return active.admission_bytes;
  if(sql.startsWith("select jsonb_build_object('revision'"))return active.financial_bytes;
  throw Error('UNIT_ONLY_UNEXPECTED_QUERY');},freshExecute:async sql=>{calls.push(sql);
   const fresh=JSON.parse(trace.fresh_bytes);if(reusedFresh)fresh.backend_pid='123';
   return JSON.stringify({isolation_bytes:JSON.stringify(fresh),basis_bytes:trace.fresh_basis_bytes,execution_bytes:trace.fresh_execution_bytes,
    original_transaction_closed:true,new_financial_rows:'0'});}};
 return {session,calls,failure};
}
const options=(session,trace)=>({session,expectedExecution:execution,nativeRequest:trace.native_request,caseId:trace.case_id,variant:trace.variant,
 fixtureManifestSha256:trace.fixture_manifest_sha256,originalReportBytes:Buffer.from('UNIT_ONLY source'),turnBackendPid:125});
test('owned rollback transport enters the actual audited OFF Report command and confirms fresh restoration',async()=>{
 const trace=unitRawTrace(execution),{session,calls}=unitTransport(trace),result=await collectReportNativeVariant({...options(session,trace),
  captureFrozenDocument:async()=>trace.document,captureNativeGateFacts:async()=>trace.native_gate_facts});
 assert.equal(result.status,'MEASURED_NATIVE_VARIANT');assert.equal(calls.filter(sql=>sql==='rollback;').length,1);
 assert.ok(calls.some(sql=>sql.includes('REPORT_FIN_AB_FINALISE')));assert.ok(!calls.some(sql=>sql.includes('select private.weekly_source_finalise_engine_v1(')));
 assert.ok(calls.at(-1).startsWith("select jsonb_build_object('isolation_bytes'"));assert.ok(!calls.includes('commit;'));
});
test('unsupported native document/gate transport truthfully waits after rollback',async()=>{
 const trace=unitRawTrace(execution),{session,calls}=unitTransport(trace),result=await collectReportNativeVariant(options(session,trace));
 assert.equal(result.status,'WAITING_NATIVE_GATES');assert.equal(result.receipt,null);assert.ok(calls.includes('rollback;'));
});
test('missing SEND7 turn refuses before native reads or BEGIN',async()=>{
 const trace=unitRawTrace(execution),{session,calls}=unitTransport(trace,{turn:false});
 await assert.rejects(collectReportNativeVariant(options(session,trace)),/SEND7_REQUIRED/);assert.equal(calls.length,1);
});
test('native command refusal rolls back once and never composes or claims a receipt',async()=>{
 const trace=unitRawTrace(execution),{session,calls,failure}=unitTransport(trace,{failAt:'select public.weekly_source_group_rule_feature_set_atomic_v1'});
 await assert.rejects(collectReportNativeVariant(options(session,trace)),error=>error===failure);
 assert.equal(calls.filter(sql=>sql==='rollback;').length,1);assert.ok(!calls.some(sql=>sql.includes('invoice_admit_atomic')));
});
test('a busy managed writer refuses before native source reads and rolls back the collector transaction',async()=>{
 const trace=unitRawTrace(execution),{session,calls}=unitTransport(trace,{writer:false});
 await assert.rejects(collectReportNativeVariant(options(session,trace)),/MANAGED_WRITER_BUSY/);
 assert.equal(calls.filter(sql=>sql==='rollback;').length,1);assert.ok(!calls.some(sql=>sql.includes('qualification_release_v1')));
});
test('paired native collection uses two isolated savepoints and one genuine outer transaction/fresh restoration',async()=>{
 const pairTraces=['A','B'].map(variant=>unitRawTrace(execution,{variant})),{session,calls}=unitTransport(pairTraces[0],{pairTraces});
 const result=await collectReportNativePair({...options(session,pairTraces[0]),
  captureFrozenDocument:async({variant})=>pairTraces.find(item=>item.variant===variant).document,
  captureNativeGateFacts:async({variant})=>pairTraces.find(item=>item.variant===variant).native_gate_facts});
 assert.equal(result.status,'MEASURED_NATIVE_PAIR');assert.deepEqual(result.variants.map(item=>JSON.parse(item.collector_bytes).variant),['A','B']);
 assert.equal(calls.filter(sql=>sql.startsWith('begin;')).length,1);assert.equal(calls.filter(sql=>sql==='rollback;').length,1);
 assert.equal(calls.filter(sql=>sql.startsWith('savepoint ')).length,2);assert.equal(calls.filter(sql=>sql.startsWith('rollback to savepoint ')).length,2);
 assert.equal(calls.filter(sql=>sql.startsWith("select jsonb_build_object('isolation_bytes'")).length,1);
 assert.ok(!calls.some(sql=>/p_now|set.*timestamp|commit;/i.test(sql)));
});
test('a failed B command rolls back the whole pair and returns no partially accepted pair',async()=>{
 const pairTraces=['A','B'].map(variant=>unitRawTrace(execution,{variant})),{session,calls,failure}=unitTransport(pairTraces[0],
  {pairTraces,failAt:'select public.weekly_source_group_rule_feature_set_atomic_v1',failVariant:'B'});
 await assert.rejects(collectReportNativePair(options(session,pairTraces[0])),error=>error===failure);
 assert.equal(calls.filter(sql=>sql==='rollback;').length,1);assert.ok(!calls.some(sql=>sql.startsWith("select jsonb_build_object('isolation_bytes'")));
});
test('only the genuine generated invoice/remittance identity is remapped while actual document dates remain exact',()=>{
 const trace=unitRawTrace(execution),financial=JSON.parse(trace.financial_bytes),model=JSON.parse(trace.document.frozen_model_bytes);
 const before=reportFrozenSemantics(JSON.stringify(model),financial);
 financial.invoice.invoice_no='UNIT_ONLY_OTHER_GENERATED_NUMBER';model.invoice_number=financial.invoice.invoice_no;model.payment.remittance_reference=financial.invoice.invoice_no;
 assert.equal(reportFrozenSemantics(JSON.stringify(model),financial),before);
 model.payment.remittance_reference='UNBOUND_CALLER_REFERENCE';assert.throws(()=>reportFrozenSemantics(JSON.stringify(model),financial),/DOCUMENT_BINDING_INVALID/);
 model.payment.remittance_reference=financial.invoice.invoice_no;model.preview_date='2026-10-14T12:00:00Z';
 assert.notEqual(reportFrozenSemantics(JSON.stringify(model),financial),before);
});
test('fixed SQL collectors read full fixture/financial facts and invoke no ledger/payment/provider/queue writer',()=>{
 const trace=unitRawTrace(execution),sql=[reportNativeBasisSql(trace.native_request),reportNativeFinancialSql(id,id),reportNativeIsolationSql].join('\n');
 assert.match(sql,/weekly_source_invoice_allocation_assert_v1/);assert.match(sql,/weekly_source_invoice_issue_validate_v1/);
 assert.match(sql,/weekly_source_state_transitions/);assert.doesNotMatch(sql,/insert into|update |delete |ledger|provider|mail_outbox|queue/i);
 const source=fs.readFileSync(new URL('./weekly-source-report-fin-native-collector.mjs',import.meta.url),'utf8');
 assert.doesNotMatch(source,/fetch\(|execFile|spawn\(|process\.env|createConnection|set_config|COMMIT;/i);
});
for(const name of ['REPORT_ORDER_REVERSE.json','REPORT_ADMISSION_REVERSE.json'])test('complete native successor fully reverses '+name,()=>{
 const proof=JSON.parse(fs.readFileSync(new URL('./'+name,import.meta.url),'utf8'));
 let source=fs.readFileSync(new URL('./'+proof.output,import.meta.url),'utf8');assert.equal(sha256(source),proof.output_sha256);
 for(const hunk of [...proof.hunks].reverse()){assert.equal(source.split(hunk.new).length,2);source=source.replace(hunk.new,()=>hunk.old);}
 assert.equal(sha256(source),proof.input_sha256);assert.equal(proof.input_git_saved_equal,true);assert.equal(proof.full_reverse,true);
});
test('B ordering reaches the actual native frozen array through full source membership, without hash or timestamp fabrication',()=>{
 const source=fs.readFileSync(new URL('./14_complete_native_snapshot_assembler.sqlinc',import.meta.url),'utf8');
 assert.match(source,/WEEKLY_SOURCE_REPORT_PRESENTATION_ORDER_UNQUALIFIED/);assert.match(source,/movement\.self_bill_report_row_id=row_value\.id\)<>1/);
 assert.match(source,/row_value\.source_row_ordinal::bigint/);assert.match(source,/max\(last_row\.source_row_ordinal\)::bigint\+1/);
 assert.match(source,/order by report_order\.report_source_ordinal nulls last,il\.created_at,il\.id/);
 assert.match(source,/then c\.base_display_order end nulls last,c\.created_at,c\.id,c\.base_display_order/);
 assert.doesNotMatch(source,/update .*created_at|movement_economic_hash.*order|SOURCE_SHADOW|REPORT.*READY.*true/);
});
test('native Report admission uses genuine pointer/full rows/zero PAY and stored bound snapshot, preserving ordinary arms',()=>{
 const source=fs.readFileSync(new URL('./15_complete_native_report_admission.sqlinc',import.meta.url),'utf8');
 assert.match(source,/not v_is_report and not exists/);assert.match(source,/WEEKLY_SOURCE_REPORT_INVOICE_MANIFEST_INVALID/);
 assert.match(source,/canonical_pay_vector_json is distinct from private\.weekly_source_invoice_only_vector_v1\('PAY',0\)/);
 assert.match(source,/bound_pay_snapshot_line_id/);assert.match(source,/SELF_BILL_REPORT_STANDALONE_CREDIT/);
 assert.match(source,/WEEKLY_SOURCE_REPORT_INVOICE_BOUND_PRESENTATION_REQUIRED/);
 assert.match(source,/weekly_source_invoice_rounding_receipt_set_v1/);assert.match(source,/weekly_source_invoice_allocation_assert_v1/);
});
test('an open original transaction or surviving native invoice rows cannot claim fresh rollback success',()=>{
 for(const change of [r=>r.original_transaction_closed=false,r=>r.new_financial_rows='1']){
  const trace=unitRawTrace(execution);change(trace.session_receipt);assert.throws(()=>deriveReportNativeReceipt(JSON.stringify(trace)),/SESSION_RECEIPT_INVALID/);
 }
});
