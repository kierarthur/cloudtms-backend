// UNIT_ONLY. Import only the complete production mapper and its pure codecs.
// No connection, session implementation, native query, or seal is supplied.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const mapperSource=fs.readFileSync(path.join(here,'forward/06_complete_native_mapper.mjs'),'utf8');
const imported=mapperSource.replace(/from '(\.\/[^']+)'/g,(_,p)=>'from '+JSON.stringify(pathToFileURL(path.join(here,'reference',p)).href));
const mapper=await import('data:text/javascript;base64,'+Buffer.from(imported).toString('base64'));
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),clone=v=>structuredClone(v),id=n=>'00000000-0000-4000-8000-'+String(n).padStart(12,'0'),h='a'.repeat(64),bytea='\\x'+h;
const witnessKeys=['bound_pay_snapshot_line_id','bound_pay_final_revision_id','bound_pay_projection_receipt_id','published_timesheet_financial_id','invoice_timesheet_id','candidate_id','client_id','contract_id','work_event_id'];
function sample(){
 const b={id:id(1),report_row_id:id(2),export_upload_row_id:id(3),bound_row_resolution_id:id(4),export_projection_publication_id:id(5),work_event_id:id(6)};
 const s={id:id(7),final_revision_id:id(8),upload_row_id:b.export_upload_row_id,row_resolution_id:b.bound_row_resolution_id,
  work_event_id:b.work_event_id,candidate_id:id(9),client_id:id(10),contract_id:id(11),pay_effect:'NONE',snapshot_line_hash:bytea,paid_overlap_choice_id:42};
 const p={id:id(12),final_revision_id:id(13),row_resolution_id:id(14),work_event_id:s.work_event_id,candidate_id:s.candidate_id,client_id:s.client_id,contract_id:s.contract_id,pay_effect:'NORMAL'};
 // Deliberate PostgreSQL-like key order differs from the JS declaration order.
 const invoiceRow={upload_row_id:s.upload_row_id,snapshot_line_id:s.id,snapshot_line_hash:h,paid_overlap_choice_id:42};
 const basis={schema_version:'WEEKLY_SOURCE_PAID_MATCH_RETAINED_PAY_BASIS_V1',transition_id:id(15),final_revision_id:s.final_revision_id,work_event_id:b.work_event_id,
  snapshot_line_id:p.id,movement_id:id(16),row_resolution_id:p.row_resolution_id,lineage_id:id(17),economic_snapshot_id:id(18),actual_present_invoice_rows:[invoiceRow]};
 const current={schema_version:'WEEKLY_SOURCE_PAID_OVERLAP_CURRENT_PAY_SNAPSHOT_V1',state:'CURRENT',snapshot_line_id:p.id,work_event_id:b.work_event_id,movement_id:basis.movement_id,
  native_current_selections:[{snapshot_line_id:p.id,scope_id:id(19),root_timesheet_id:id(20),selected_transition_id:basis.transition_id,
   selected_order_key:'2026-10-15 12:00:00+00|native-order',source_hash:h,selected_final_revision_id:s.final_revision_id,source_export_present:true,exclude_cancel:false}]};
 const t={id:basis.transition_id,final_revision_id:s.final_revision_id,work_event_id:b.work_event_id,pay_basis_kind:'RETAINED_PAID_MATCH',outcome:'NO_CHANGE',previous_present:true,new_present:true,exclude_cancel:false,
  previous_snapshot_line_id:p.id,new_snapshot_line_id:p.id,prior_state_fingerprint:bytea,new_state_fingerprint:bytea,transition_fingerprint:bytea,paid_match_source_rows:basis.actual_present_invoice_rows};
 const rr={id:s.row_resolution_id,upload_row_id:s.upload_row_id,pay_effect:'NONE',paid_overlap_choice_id:s.paid_overlap_choice_id,mapping_state:'RESOLVED',work_event_id:s.work_event_id,candidate_id:s.candidate_id,client_id:s.client_id,contract_id:s.contract_id};
 const a=Object.fromEntries(mapper.reportPayAuthorityKeys.map(k=>[k,null]));
 Object.assign(a,{bound_pay_snapshot_line_id:p.id,bound_pay_final_revision_id:p.final_revision_id,bound_pay_projection_receipt_id:id(21),published_timesheet_financial_id:null,
  invoice_timesheet_id:id(20),candidate_id:s.candidate_id,client_id:s.client_id,contract_id:s.contract_id,work_event_id:s.work_event_id,
  schema_version:'WEEKLY_SOURCE_REPORT_PAY_AUTHORITY_V1',binding_id:b.id,bound_row_resolution_id:b.bound_row_resolution_id,bound_export_projection_publication_id:b.export_projection_publication_id,
  invoice_ready:true,native_pay_position:'PUBLISHED_SOURCE_HEAD',unavailable_reason:null,prepared_amount_known:false,withheld_amount_known:false,active_amount_scope:'ROOT_FAMILY',
  bound_export_snapshot_line_id:s.id,bound_export_final_revision_id:s.final_revision_id,bound_export_upload_row_id:s.upload_row_id,bound_export_snapshot_line_hash:h,
  bound_export_pay_effect:'NONE',bound_export_paid_overlap_choice_id:s.paid_overlap_choice_id,retained_pay_basis:basis,retained_native_current_pay_selection:current});
 const o={export_snapshot:s,pay_snapshot:p,transition:t,export_row:{id:s.upload_row_id,row_finalisation_state:'SOURCE_WORKED'},export_resolution:rr,
  retained_basis_bytes:JSON.stringify(basis),current_selection_bytes:JSON.stringify(current)};
 return {a,b,o};
}
function signed(a){const {authority_sha256:ignored,...base}=a;const bytes=JSON.stringify(base);return {raw:{...base,authority_sha256:sha('WEEKLY_SOURCE_REPORT_PAY_AUTHORITY_V1\x1f'+bytes)},bytes};}
function check(v){const s=signed(v.a);return mapper.assertReportPayAuthority(s.raw,s.bytes,v.b,v.o);}
test('actual production decoder accepts distinct B vs retained NORMAL with native JSON key order',()=>assert.deepEqual(check(sample()),{waiting:['native_prepared_amount','native_withheld_amount']}));
test('every full source owner reverses exactly with unique native anchors',()=>{const meta=JSON.parse(fs.readFileSync(path.join(here,'FULL_REVERSES.json')));for(const o of meta.owners){const before=fs.readFileSync(path.join(here,'before',o.file),'utf8'),after=fs.readFileSync(path.join(here,'forward',o.file),'utf8');assert.equal(sha(before),o.before_sha256);assert.equal(sha(after),o.after_sha256);let reversed=after;for(const h of [...o.hunks].reverse()){assert.equal(reversed.split(h.after).length,2);reversed=reversed.replace(h.after,()=>h.before);}assert.equal(reversed,before);}});
test('ordinary strict DTO remains old shape and its current unknown amounts remain unknown',()=>{const {a,b}=sample();for(const k of Object.keys(a))if(!mapper.reportPayAuthorityKeys.includes(k))delete a[k];const v=signed(a);assert.deepEqual(mapper.assertReportPayAuthority(v.raw,v.bytes,b),{waiting:['native_prepared_amount','native_withheld_amount']});assert.equal(Object.keys(v.raw).length,mapper.reportPayAuthorityKeys.length);});
for(const [name,mutate] of [
 ['B substituted into compact PAY9',v=>v.a.bound_pay_snapshot_line_id=v.a.bound_export_snapshot_line_id],
 ['B pretends NORMAL',v=>v.o.export_snapshot.pay_effect='NORMAL'],
 ['real PAY snapshot changed to NONE',v=>v.o.pay_snapshot.pay_effect='NONE'],
 ['foreign event in B',v=>v.o.export_snapshot.work_event_id=id(800)],
 ['foreign Candidate on real PAY',v=>v.o.pay_snapshot.candidate_id=id(801)],
 ['foreign Contract resolution',v=>v.o.export_resolution.contract_id=id(802)],
 ['current B row absent from retained complete census',v=>{v.a.retained_pay_basis.actual_present_invoice_rows=[];v.o.retained_basis_bytes=JSON.stringify(v.a.retained_pay_basis);}],
 ['caller changes rederived retained helper bytes',v=>v.o.retained_basis_bytes='{}'],
 ['caller changes rederived current selector bytes',v=>v.o.current_selection_bytes='{}'],
 ['actual transition uses CANCEL',v=>v.o.transition.outcome='CANCEL'],
 ['excluded missing row masquerades as B present',v=>v.o.transition.exclude_cancel=true],
 ['transition chooses B instead of retained basis',v=>v.o.transition.new_snapshot_line_id=v.a.bound_export_snapshot_line_id],
 ['current selector is unavailable',v=>{v.a.retained_native_current_pay_selection.state='UNAVAILABLE';v.o.current_selection_bytes=JSON.stringify(v.a.retained_native_current_pay_selection);}],
 ['current selector chooses wrong actual revision',v=>{v.a.retained_native_current_pay_selection.native_current_selections[0].selected_final_revision_id=id(803);v.o.current_selection_bytes=JSON.stringify(v.a.retained_native_current_pay_selection);}],
 ['old producing S1 substituted for selected retained S2',v=>{v.o.pay_snapshot.id=id(804);}],
 ['raw choice identity rotated',v=>v.o.export_resolution.paid_overlap_choice_id=43],
 ['bound raw export row rotated',v=>v.b.export_upload_row_id=id(805)],
 ['snapshot hash changed',v=>v.o.export_snapshot.snapshot_line_hash='\\x'+'b'.repeat(64)],
 ['unknown prepared amount fabricated zero',v=>v.a.prepared_pay_ex_vat='0'],
 ['unready native authority labelled invoice ready by caller',v=>v.a.invoice_ready=false],
 ['extra caller native authority key',v=>v.a.approved_by_caller=true],
 ['array choice scalar coercion',v=>v.a.bound_export_paid_overlap_choice_id=[42]]
])test('refuses '+name,()=>{const v=sample();mutate(v);assert.throws(()=>check(v),/REPORT_NATIVE_/);});
test('NONE without the fixed actual native observation is refused',()=>{const {a,b}=sample(),v=signed(a);assert.throws(()=>mapper.assertReportPayAuthority(v.raw,v.bytes,b),/REPORT_NATIVE_NONE_OBSERVATION_INVALID/);});
test('native owned complete census retains two B raw rows for the SAME event',()=>{const v=sample(),extra={upload_row_id:id(900),snapshot_line_id:id(901),snapshot_line_hash:'b'.repeat(64),paid_overlap_choice_id:43};v.a.retained_pay_basis.actual_present_invoice_rows.push(extra);v.o.retained_basis_bytes=JSON.stringify(v.a.retained_pay_basis);assert.deepEqual(check(v).waiting,['native_prepared_amount','native_withheld_amount']);});
test('ordinary authority refuses unrelated NONE observations',()=>{const {a,b,o}=sample();for(const k of Object.keys(a))if(!mapper.reportPayAuthorityKeys.includes(k))delete a[k];const v=signed(a);assert.throws(()=>mapper.assertReportPayAuthority(v.raw,v.bytes,b,o),/REPORT_NATIVE_NONE_OBSERVATION_UNEXPECTED/);});
test('literal native query derives B/raw/true PAY/retained/current selection separately',()=>{const q=mapper.reportNativeMapperQueries({actor_user_id:id(1),source_cycle_id:id(2),report_scope_id:id(3)},{fixture_manifest:{contract_ids:[id(4)]}});assert.match(q.pay_authorities,/left join public.weekly_source_final_snapshot_lines es/);assert.match(q.pay_authorities,/left join public.weekly_source_final_snapshot_lines ps/);assert.match(q.pay_authorities,/weekly_source_paid_match_retained_transition_assert_v1\(t.id\)/);assert.match(q.pay_authorities,/weekly_source_paid_overlap_current_snapshot_v1/);assert.match(q.pay_authorities,/public\._pay_timesheet_rotation_scope/);});
test('actual mapper owner seal must additionally bind both exact native dependency signatures',()=>{assert.ok(mapper.reportFinNativeOwnerSignatures.includes('private.weekly_source_paid_match_retained_transition_assert_v1(uuid)'));assert.ok(mapper.reportFinNativeOwnerSignatures.includes('private.weekly_source_paid_overlap_current_snapshot_v1(uuid,uuid,uuid,uuid,uuid,uuid,uuid[])'));assert.match(fs.readFileSync(path.join(here,'reference/weekly-source-report-fin-native-seal.mjs'),'utf8'),/reportFinNativeSeal = null/);});
test('NONE confirmation cannot bypass the unchanged full receipt/run or head proof',()=>{const s=fs.readFileSync(path.join(here,'forward/01_complete_report_pay_authority.sqlinc'),'utf8');assert.equal((s.match(/ready:=true/g)||[]).length,(fs.readFileSync(path.join(here,'before/01_complete_report_pay_authority.sqlinc'),'utf8').match(/ready:=true/g)||[]).length);assert.match(s,/private.weekly_source_report_projection_receipt_assert_v1\(receipt\)/);assert.match(s,/run.task_manifest_hash is distinct from/);assert.match(s,/head.source_generation_digest=private.weekly_source_publication_request_digest_v1/);assert.match(s,/pay_origin_revision:=export_fr/);assert.match(s,/withheld_amount_known',false/);});
test('new current head revision does not change historical receipt/run identity',()=>{const s=fs.readFileSync(path.join(here,'forward/01_complete_report_pay_authority.sqlinc'),'utf8');assert.match(s,/r.final_revision_id=fr.id and r.root_timesheet_id=lineage.timesheet_id/);assert.match(s,/prepared_context->>'final_revision_id' is distinct from fr.id::text/);assert.match(s,/native_origin->>'final_revision_id'=pay_origin_revision.id::text/);assert.match(s,/bound_pay_snapshot_line_id',fs.id,'bound_pay_final_revision_id',fr.id/);});
test('movement stores only closed B provenance in addition to unchanged PAY9',()=>{const s=fs.readFileSync(path.join(here,'forward/02_complete_report_movement.sqlinc'),'utf8');assert.match(s,/jsonb_object_agg\(key,value\)/);assert.match(s,/jsonb_each\(prior.source_facts_json\)/);assert.match(s,/coalesce\(v_pay_witness,'\{\}'::jsonb\)\|\|v_export_facts/);assert.ok(!s.includes('retained_native_current_pay_selection'));assert.match(s,/weekly_source_invoice_only_vector_v1\('PAY',0\)/);});
test('Report workspace and three alternative complete admission paths choose B only with exact retained proof',()=>{const ws=fs.readFileSync(path.join(here,'forward/03_complete_report_workspace.sqlinc'),'utf8');assert.match(ws,/v_pay_authority->>'bound_export_snapshot_line_id'/);assert.match(ws,/row_resolution_id=row_fact.bound_row_resolution_id/);assert.match(ws,/'export_snapshot',jsonb_build_object/);assert.match(ws,/'pay_snapshot',jsonb_build_object/);for(const name of ['04_complete_root_report_admission.sqlinc','05_complete_money_report_none_admission_join.sqlinc','07_complete_money_expense_report_none_admission_join.sqlinc']){const s=fs.readFileSync(path.join(here,'forward',name),'utf8');assert.equal((s.match(/snapshot.pay_effect='NONE'/g)||[]).length,2);assert.equal((s.match(/retained_pay_basis,actual_present_invoice_rows/g)||[]).length,2);assert.match(s,/bound_export_snapshot_line_hash/);}});
function priorSample(){
 const {a,b,o}=sample(),oldRow={id:id(950),external_source_key:'REAL_REQUEST',source_shift_charge_pence:100},row={id:id(951),external_source_key:'REAL_REQUEST',source_shift_charge_pence:-100};
 const report={id:id(952),source_group_id:id(953),client_id:a.client_id,invoice_date:'2026-10-08'},revision={id:id(954),state:'CURRENT',authority_scope_kind:'SELF_BILL_REPORT_SCOPE',report_scope_id:id(955)};
 const binding={...b,report_row_id:oldRow.id,self_bill_report_id:report.id};
 const facts={report_line_binding_id:binding.id,report_scope_id:revision.report_scope_id,...Object.fromEntries(witnessKeys.map(k=>[k,a[k]]))};
 for(const k of ['bound_export_snapshot_line_id','bound_export_final_revision_id','bound_export_upload_row_id','bound_export_snapshot_line_hash','bound_export_pay_effect','bound_export_paid_overlap_choice_id','retained_pay_basis'])facts[k]=a[k];
 const prior={id:id(956),self_bill_report_row_id:oldRow.id,final_revision_id:revision.id,source_line_kind:'SELF_BILL_REPORT_LINE',source_profile_kind:'SELF_BILL_REPORT',movement_role:'POSITIVE',
  actual_client_id:a.client_id,candidate_id:a.candidate_id,contract_id:a.contract_id,work_event_id:a.work_event_id,invoice_timesheet_id:a.invoice_timesheet_id,source_facts_json:facts};
 const negative={...prior,self_bill_report_row_id:row.id,id:id(957),movement_role:'REVERSAL',prior_movement_id:prior.id,source_facts_json:clone(facts)};
 const entry={report_row_id:row.id,prior_bytes:JSON.stringify({prior_movement_id:prior.id,row_id:binding.export_upload_row_id,resolution_id:binding.bound_row_resolution_id,publication_id:binding.export_projection_publication_id,work_event_id:binding.work_event_id}),
  prior_movement:prior,prior_binding:binding,prior_upload_row:oldRow,prior_report:report,prior_revision:revision,pay_snapshot:o.pay_snapshot,
  pay_revision:{id:a.bound_pay_final_revision_id},pay_receipt:{id:a.bound_pay_projection_receipt_id,final_revision_id:a.bound_pay_final_revision_id,root_timesheet_id:a.invoice_timesheet_id,client_id:a.client_id,published_timesheet_financial_id:null},
  pay_financial_identity:{id:null,timesheet_id:null,client_id:null,candidate_id:null}};
 return {entry,context:{basis:{rows:[row],scope:{client_id:a.client_id,source_group_id:report.source_group_id},report:{invoice_date:'2026-10-15'},authorization:{fixture_manifest:{candidate_ids:[a.candidate_id],contract_ids:[a.contract_id]}}},financial:{movements:[negative]}}};
}
test('actual negative decoder preserves real historical B and independent NORMAL PAY witness',()=>{const v=priorSample();assert.equal(mapper.validateNegativeProvenance([v.entry],v.context),true);});
test('actual negative decoder refuses rotated historical B without altering real PAY9',()=>{const v=priorSample();v.context.financial.movements[0].source_facts_json.bound_export_snapshot_line_id=id(999);assert.throws(()=>mapper.validateNegativeProvenance([v.entry],v.context),/REPORT_NATIVE_REVERSAL_EXPORT_WITNESS_CHANGED/);});
