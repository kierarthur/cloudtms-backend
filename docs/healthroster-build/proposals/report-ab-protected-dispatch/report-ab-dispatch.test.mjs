import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
import {caseIds,assertions,buildNativeComparisons,assertNativeComparisons,buildQualification,schema} from './weekly-source-report-fin-ab-evidence.mjs';
import {domainHash} from './weekly-source-protected-release-qualification.mjs';
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
   invoice_target_sha256:hash,pay_isolation_sha256:hash,render_semantics_sha256:hash},assertions:Object.fromEntries(assertions.map(k=>[k,true]))};
 const receipt_bytes=JSON.stringify(row);return {receipt_id:'UNIT_ONLY/'+case_id+'/'+variant,receipt_bytes,receipt_sha256:sha(receipt_bytes)};
}));
const mutate=(inputs,index,fn)=>{const copy=structuredClone(inputs),row=JSON.parse(copy[index].receipt_bytes);fn(row);
 copy[index].receipt_bytes=JSON.stringify(row);copy[index].receipt_sha256=sha(copy[index].receipt_bytes);return copy;};
// These fabricated strings exercise only the codec. They are not native
// receipts, qualification, money/render proof, or inputs to a managed release.
test('metadata codec accepts exactly fourteen closed UNIT_ONLY envelopes',()=>assert.equal(buildNativeComparisons(execution,fixture()).length,14));
test('one variant alone cannot select a strategy',()=>assert.throws(()=>buildNativeComparisons(execution,fixture().filter((_,i)=>i%2===0)),/CASE_CENSUS/));
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
test('R1 cannot masquerade as R2',()=>{const rows=buildNativeComparisons(execution,fixture()),decision={schema_version:'WEEKLY_SOURCE_REPORT_FIN_REVIEWED_DECISION_V1',selected_variant:'B',
 native_comparison_sha256:domainHash('WEEKLY_SOURCE_REPORT_FIN_NATIVE_COMPARISONS_V1',rows),reason:'UNIT_ONLY metadata vector; not qualification'};
 assert.throws(()=>buildQualification(execution,execution,rows,decision,null,null),/R1_R2_IDENTITY_CHANGED/);});
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
test('private ACL addition is exactly five, with no service/browser grant',()=>{const sig=JSON.parse(read('PRIVATE_SIGNATURES.json'));assert.equal(sig.signatures.length,5);
 assert.equal(new Set(sig.signatures).size,5);const acl=read('07_owner_acl.sqlinc');assert.doesNotMatch(acl,/grant\s+execute/i);
 for(const s of sig.signatures)assert.ok(acl.includes('revoke all on function '+s+' from public,anon,authenticated,service_role;'));});
test('later strict complete Git parser retains every raw path and only existing bundle exceptions',()=>{const s=read('weekly-source-protected-execution-tree.mjs');
 assert.doesNotMatch(s,/\.filter\(row=>selected\(row\.path\)\)/);assert.match(s,/fatal/);assert.match(s,/weekly-source-protected-operational\.json/);
 assert.doesNotMatch(s,/report-fin-ab\.json|qualification-decisions/);});
test('native capture is append-only under the actual writer, not a journey runner',()=>{const s=read('weekly-source-report-fin-ab-evidence.mjs');
 assert.match(s,/old is not null and old is distinct from/);assert.match(s,/assertWriterSql/);assert.match(s,/r\.status<>'VERIFIED'/);
 assert.doesNotMatch(s,/fetch\(|provider\.request|smtp|sendEmail|set_config\(/);});
test('Report-only receipt bundle never fabricates BULK/AUTO qualification',()=>{const s=read('09_complete_operational_evidence.mjs');
 assert.match(s,/bundle\.report_fin_qualifications/);assert.match(s,/return bundle\.qualifications\.map/);assert.doesNotMatch(s,/capabilities:.*REPORT/);});
test('absent Report binding supplies no bootstrap authority and cannot dispatch operationally',()=>{const binding=read('02_release_feature_strategy.sqlinc');
 assert.match(binding,/if certificates is null then return null; end if/);assert.match(binding,/if matching_count=0 then return null; end if/);
 assert.match(binding,/receipt->'report_fin_qualification' is distinct from qualified/);
 assert.match(read('README.md'),/ordinary unqualified feature enablement is not an executable R1 path/);
 const strategy=binding.slice(binding.indexOf('create or replace function private.weekly_source_report_fin_strategy_v1'));
 assert.doesNotMatch(strategy,/return null|coalesce.*selected_variant/);});
test('writer and CLI use only closed committed Report input, no selector/default',()=>{const writer=read('06_complete_managed_release.mjs'),cli=read('10_complete_current_release_caller.mjs');
 assert.match(writer,/prepareReportFinQualifications/);assert.match(cli,/reportFinQualifications:loadReportFinInputs/);
 assert.ok(writer.indexOf('prepareReportFinQualifications({')<writer.indexOf("set status='VERIFIED'"));
 assert.ok(writer.indexOf('retainNativeReportFinComparisons({context')>writer.indexOf("set status='VERIFIED'"));
 assert.doesNotMatch(writer,/p_report_vat_rounding_variant.*['"]A['"]|selected_variant.*\?\?.*['"]A['"]/);});
