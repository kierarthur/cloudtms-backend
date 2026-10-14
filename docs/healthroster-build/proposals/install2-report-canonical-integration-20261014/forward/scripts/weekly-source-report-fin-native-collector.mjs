// Root-owned native transport only. Importing this file opens no connection,
// creates no fixture and writes no file. UNIT_ONLY traces are never evidence.
import {exact,sha256,domainHash,pgJsonbText} from './weekly-source-protected-release-qualification.mjs';
import {validateReportNativeGateFacts,deriveReportCaseGrain,captureReportNativeGateBefore,captureReportNativeGateAfter} from './weekly-source-report-fin-native-mapper.mjs';
const deployed=import.meta.url.endsWith('/scripts/weekly-source-report-fin-native-collector.mjs'),
 proposal=import.meta.url.endsWith('/docs/healthroster-build/proposals/report-ab-protected-dispatch/weekly-source-report-fin-native-collector.mjs');
if(!deployed&&!proposal)throw Error('REPORT_NATIVE_COLLECTOR_LOCATION_INVALID');
const {validateFrozenPresentationModel}=await import(new URL(deployed?'../broker/src/invoice-presentation-contract.js':
 '../../../../broker/src/invoice-presentation-contract.js',import.meta.url));

const fail=code=>{throw Error(code);},uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
 hex=/^[0-9a-f]{64}$/,commit=/^[0-9a-f]{40}$/;
export const collectorSchema='WEEKLY_SOURCE_REPORT_FIN_RAW_COLLECTOR_V1';
export const collectorMaxBytes=256*1024,collectorMaxBundleBytes=2*1024*1024;
const executionKeys=['release_id','source_commit','contract_sha256','installation_id','environment','database',
 'managed_manifest_sha256','installed_catalogue_jsonb_sha256'];
const cases=['BOUND_NEGATIVE','D_NEGATIVE','D_POSITIVE','D_ZERO','NEXT_ACTIVE','ORDINARY_NHSP_REGRESSION','STANDALONE_NEGATIVE'];
const keys=['actor_user_id','source_cycle_id','authority_scope_kind','report_scope_id','upload_id','projection_publication_id',
 'expected_authority_scope_version','expected_row_manifest_hash','expected_comparison_manifest_hash','expected_issue_set_hash',
 'exclude_unfinalised_acknowledged'];
const quote=value=>"'"+String(value).replaceAll("'","''")+"'",json=value=>quote(JSON.stringify(value))+'::jsonb';
const canonical=value=>JSON.stringify(value,(_key,item)=>{
 if(typeof item==='number'&&(!Number.isFinite(item)||(Number.isInteger(item)&&!Number.isSafeInteger(item))))fail('REPORT_NATIVE_UNSAFE_NUMBER');
 return item&&typeof item==='object'&&!Array.isArray(item)?Object.fromEntries(Object.entries(item).sort(([a],[b])=>a.localeCompare(b))):item;
});
const same=(a,b)=>canonical(a)===canonical(b);
const byteaHex=value=>typeof value==='string'&&/^\\x[0-9a-f]{64}$/.test(value)?value.slice(2):null;
function rawJson(bytes,code){
 if(typeof bytes!=='string'||Buffer.byteLength(bytes)>collectorMaxBytes)fail(code);
 try {const decoded=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(Buffer.from(bytes));
  if(decoded!==bytes)fail(code);return JSON.parse(bytes);}catch{fail(code);}
}
function execution(value){
 exact(value,executionKeys,'REPORT_NATIVE_EXECUTION_INVALID');
 if(value.environment!=='TEST'||value.database!=='cloudtms_test_clone'||typeof value.release_id!=='string'||!value.release_id
  ||!commit.test(value.source_commit)||!uuid.test(value.installation_id)
  ||!executionKeys.filter(k=>k.endsWith('_sha256')).every(k=>hex.test(value[k])))fail('REPORT_NATIVE_EXECUTION_INVALID');
}
function request(value){
 exact(value,keys,'REPORT_NATIVE_REQUEST_INVALID');
 if(value.authority_scope_kind!=='SELF_BILL_REPORT_SCOPE'||!keys.filter(k=>k.endsWith('_id')).every(k=>uuid.test(value[k]))
  ||!keys.filter(k=>k.endsWith('_hash')).every(k=>hex.test(value[k]))
  ||!Number.isSafeInteger(value.expected_authority_scope_version)||value.expected_authority_scope_version<1
  ||typeof value.exclude_unfinalised_acknowledged!=='boolean')fail('REPORT_NATIVE_REQUEST_INVALID');
}
// The closed native FIN request has one safe integer version and no money.
// Retain the native JSONB scalar/key codec; do not serialize economic numbers.
export function reportNativeRequestHash(value,domain='WEEKLY_SOURCE_REPORT_FIN_FIXTURE_REQUEST_V1'){
 if(!['WEEKLY_SOURCE_REPORT_FIN_FIXTURE_REQUEST_V1','WEEKLY_SOURCE_OFF_RULE_NATIVE_REQUEST_V1'].includes(domain))fail('REPORT_NATIVE_REQUEST_HASH_DOMAIN_INVALID');
 request(value);const text='{'+Object.keys(value).sort((a,b)=>Buffer.byteLength(a)-Buffer.byteLength(b)||Buffer.compare(Buffer.from(a),Buffer.from(b)))
  .map(key=>JSON.stringify(key)+': '+(key==='expected_authority_scope_version'?String(value[key]):pgJsonbText(value[key]))).join(', ')+'}';
 return sha256(domain+'\x1f'+text);
}
export function reportNativeBasisSql(nativeRequest){
 request(nativeRequest);const r=nativeRequest;
 return `select jsonb_build_object('cycle',to_jsonb(c),'scope',to_jsonb(s),'report',to_jsonb(h),'upload',to_jsonb(u),
 'publication',to_jsonb(p),'authorization',private.weekly_source_rule_qualification_authorization_v1(${quote(r.source_cycle_id)}::uuid,${quote(r.actor_user_id)}::uuid,'SOURCE_FIXTURE_SETUP'),
 'rows',coalesce((select jsonb_agg(to_jsonb(row) order by row.source_row_ordinal) from public.weekly_source_upload_rows row where row.upload_id=u.id),'[]'::jsonb),
 'bindings',coalesce((select jsonb_agg(to_jsonb(b) order by b.id) from public.weekly_source_self_bill_report_line_bindings b where b.self_bill_report_id=h.id),'[]'::jsonb))::text
 from public.weekly_source_cycles c join public.weekly_source_report_scopes s on s.source_cycle_id=c.id
 join public.weekly_source_self_bill_reports h on h.id=s.self_bill_report_id join public.weekly_source_uploads u on u.id=s.current_complete_upload_id
 join public.weekly_source_projection_publications p on p.id=s.current_projection_publication_id
 where c.id=${quote(r.source_cycle_id)}::uuid and s.id=${quote(r.report_scope_id)}::uuid and u.id=${quote(r.upload_id)}::uuid and p.id=${quote(r.projection_publication_id)}::uuid;`;
}
export function reportNativeFinancialSql(finalRevisionId,invoiceId){
 if(!uuid.test(finalRevisionId)||!uuid.test(invoiceId))fail('REPORT_NATIVE_FINANCIAL_ID_INVALID');
 return `select jsonb_build_object('revision',to_jsonb(fr),'manifest',to_jsonb(cm),'invoice',to_jsonb(i),
 'movements',coalesce((select jsonb_agg(to_jsonb(m) order by mm.manifest_ordinal) from public.weekly_source_manifest_movements mm join public.weekly_source_billing_movements m on m.id=mm.billing_movement_id where mm.client_manifest_id=cm.id),'[]'::jsonb),
 'membership',coalesce((select jsonb_agg(to_jsonb(mm) order by mm.manifest_ordinal) from public.weekly_source_manifest_movements mm where mm.client_manifest_id=cm.id),'[]'::jsonb),
 'invoice_lines',coalesce((select jsonb_agg(to_jsonb(line) order by line.id) from public.invoice_lines line where line.invoice_id=i.id),'[]'::jsonb),
 'invoice_bindings',coalesce((select jsonb_agg(to_jsonb(b) order by b.id) from public.weekly_source_invoice_line_bindings b where b.invoice_id=i.id and b.state='CURRENT'),'[]'::jsonb),
 'presentation_lines',coalesce((select jsonb_agg(to_jsonb(presentation) order by presentation.id) from public.weekly_source_invoice_presentation_lines presentation
  where presentation.id in(select binding.presentation_line_id from public.weekly_source_invoice_line_bindings binding where binding.invoice_id=i.id and binding.state='CURRENT')),'[]'::jsonb),
 'off_audit_frames',coalesce((select jsonb_agg(jsonb_build_object('id',audit.id,'object_type',audit.object_type,'object_id_text',audit.object_id_text,
   'action',audit.action,'event_sequence',audit.event_sequence::text,'event_sequence_is_authoritative',audit.event_sequence_is_authoritative,
   'actor_user_id',audit.actor_user_id,'before_bytes',audit.before_json::text,'after_bytes',audit.after_json::text,
   'frame_base_bytes',case when audit.action='WEEKLY_SOURCE_REPORT_FIN_FIXTURE_BOUND' then (audit.after_json-'frame_sha256')::text
     when audit.action='WEEKLY_SOURCE_OFF_RULE_COMMAND_ADMITTED' then (audit.after_json-'command_sha256')::text end) order by audit.event_sequence)
  from public.audit_events audit where audit.event_sequence_is_authoritative and
   ((audit.object_type='weekly_source_off_rule_report_reference' and audit.action='WEEKLY_SOURCE_REPORT_FIN_FIXTURE_BOUND'
     and audit.after_json->>'transaction_id'=pg_current_xact_id_if_assigned()::text and audit.after_json->>'backend_pid'=pg_backend_pid()::text
     and audit.after_json#>>'{reference,report_scope_id}'=fr.report_scope_id::text)
    or (audit.object_type='weekly_source_off_rule_command' and audit.action in ('WEEKLY_SOURCE_OFF_RULE_COMMAND_ADMITTED','WEEKLY_SOURCE_OFF_RULE_COMMAND_COMPLETED')
     and coalesce(audit.after_json->>'transaction_id',audit.before_json->>'transaction_id')=pg_current_xact_id_if_assigned()::text
     and coalesce(audit.after_json->>'backend_pid',audit.before_json->>'backend_pid')=pg_backend_pid()::text
     and coalesce(audit.after_json->>'source_cycle_id',audit.before_json->>'source_cycle_id')=fr.source_cycle_id::text))),'[]'::jsonb),
 'allocation',private.weekly_source_invoice_allocation_assert_v1(i.id),'validation',private.weekly_source_invoice_issue_validate_v1(i.id),
 'net_pence',(i.subtotal_ex_vat*100)::bigint::text,'vat_pence',(i.vat_amount*100)::bigint::text,'gross_pence',(i.total_inc_vat*100)::bigint::text,
 'rounding_pence',coalesce((select sum(m.vat_amount*100) from public.weekly_source_billing_movements m where m.final_revision_id=fr.id and m.source_line_kind='INVOICE_VAT_ROUNDING'),0)::bigint::text,
 'snapshot_count',(select count(*)::text from public.weekly_source_final_snapshot_lines fs where fs.final_revision_id=fr.id),
 'transition_count',(select count(*)::text from public.weekly_source_state_transitions t where t.final_revision_id=fr.id))::text
 from public.weekly_source_final_revisions fr join public.weekly_source_client_manifests cm on cm.final_revision_id=fr.id
 join public.invoices i on i.id=${quote(invoiceId)}::uuid where fr.id=${quote(finalRevisionId)}::uuid;`;
}
// Whole-table isolation fingerprints retain no unrelated business payload.
// New owners/root observations are additional required native gates, never
// replaced by these digests or by current TSFIN amounts.
export const reportNativeIsolationSql=`select jsonb_build_object('database',current_database(),'backend_pid',pg_backend_pid()::text,
 'pay',encode(private.weekly_source_sha256_jsonb_v1('REPORT_NATIVE_PAY_ISOLATION_V1',jsonb_build_object(
 'timesheets',(select coalesce(jsonb_agg(to_jsonb(t) order by t.timesheet_id),'[]'::jsonb) from public.timesheets t),
 'financials',(select coalesce(jsonb_agg(to_jsonb(t) order by t.id),'[]'::jsonb) from public.timesheets_financials t),
 'ordinary_receipts',(select coalesce(jsonb_agg(to_jsonb(t) order by t.id),'[]'::jsonb) from public.weekly_source_ordinary_pay_projection_receipts t),
 'snapshots',(select coalesce(jsonb_agg(to_jsonb(t) order by t.id),'[]'::jsonb) from public.weekly_source_final_snapshot_lines t),
 'transitions',(select coalesce(jsonb_agg(to_jsonb(t) order by t.id),'[]'::jsonb) from public.weekly_source_state_transitions t))),'hex'),
 'expenses',encode(private.weekly_source_sha256_jsonb_v1('REPORT_NATIVE_EXPENSE_ISOLATION_V1',
 (select coalesce(jsonb_agg(to_jsonb(t) order by t.id),'[]'::jsonb) from public.weekly_source_row_expense_policy_snapshots t)),'hex'))::text;`;

function assertBasis(basis,r,expectedExecution,fixtureSha){
 exact(basis,['cycle','scope','report','upload','publication','authorization','rows','bindings'],'REPORT_NATIVE_BASIS_INVALID');
 const a=basis.authorization,s=basis.scope,h=basis.report,u=basis.upload,p=basis.publication,c=basis.cycle;
 if(a?.schema_version!=='WEEKLY_SOURCE_OFF_RULE_TEST_AUTHORIZATION_V1'||a.purpose!=='SOURCE_FIXTURE_SETUP'
  ||a.release_id!==expectedExecution.release_id||a.source_commit!==expectedExecution.source_commit||a.contract_sha256!==expectedExecution.contract_sha256
  ||a.source_cycle_id!==r.source_cycle_id||a.journey_actor_user_id!==r.actor_user_id||!hex.test(a.authorization_sha256)
  ||c?.id!==r.source_cycle_id||c.source_group_id!==a.source_group_id||c.finalisation_week_ending!==a.week_ending
  ||s?.id!==r.report_scope_id||s.source_cycle_id!==c.id||s.source_group_id!==a.source_group_id
  ||h?.id!==s.self_bill_report_id||h.source_cycle_id!==c.id||h.source_group_id!==s.source_group_id||h.client_id!==s.client_id
  ||u?.id!==r.upload_id||u.source_cycle_id!==c.id||u.report_scope_id!==s.id||u.state!=='CURRENT'
  ||byteaHex(u.content_sha256)===null||u.content_sha256!==h.content_sha256||byteaHex(h.row_manifest_hash)===null
  ||byteaHex(u.row_manifest_hash)!==r.expected_row_manifest_hash
  ||p?.id!==r.projection_publication_id||p.upload_id!==u.id||p.report_scope_id!==s.id||p.source_cycle_id!==c.id
  ||p.authority_scope_kind!=='SELF_BILL_REPORT_SCOPE'||p.authority_scope_version!==s.version||p.state!=='CURRENT'
  ||byteaHex(p.comparison_manifest_hash)!==r.expected_comparison_manifest_hash||byteaHex(p.issue_set_hash)!==r.expected_issue_set_hash
  ||s.current_final_revision_id!==null||s.current_complete_upload_id!==u.id||s.current_projection_publication_id!==p.id
  ||s.projection_state!=='CURRENT'||s.version!==r.expected_authority_scope_version
  ||!Array.isArray(basis.rows)||!basis.rows.length||basis.rows.length>10000||!Array.isArray(basis.bindings)
  ||!a.fixture_manifest?.client_ids?.includes(s.client_id)||sha256(Buffer.from(canonical(a.fixture_manifest)))!==fixtureSha)
  fail('REPORT_NATIVE_BASIS_CHANGED');
 if(basis.rows.length!==h.line_count||new Set(basis.rows.map(row=>row.id)).size!==basis.rows.length
  ||basis.rows.some((row,index)=>!uuid.test(row.id)||row.upload_id!==u.id||row.row_finalisation_state!=='SOURCE_REPORT_LINE'
    ||!Number.isSafeInteger(row.source_row_ordinal)||row.source_row_ordinal<1
    ||index>0&&row.source_row_ordinal<=basis.rows[index-1].source_row_ordinal
    ||!['source_shift_charge_pence','source_vat_pence','source_gross_pence'].every(k=>Number.isSafeInteger(row[k]))
    ||row.source_shift_charge_pence+row.source_vat_pence!==row.source_gross_pence))fail('REPORT_NATIVE_BASIS_ROW_INVALID');
 // Row payload may only belong to the dedicated authorization. The real owner
 // proves the Client/Contract/Candidate refs and disabled assignment itself.
 for(const b of basis.bindings){
  if(b.self_bill_report_id!==h.id||b.client_id!==undefined&&b.client_id!==s.client_id
   ||b.candidate_id!==null&&b.candidate_id!==undefined&&!a.fixture_manifest.candidate_ids.includes(b.candidate_id)
   ||b.contract_id!==null&&b.contract_id!==undefined&&!a.fixture_manifest.contract_ids.includes(b.contract_id))fail('REPORT_NATIVE_FOREIGN_FIXTURE');
 }
}
// Four concrete native slot mappers replace the retired generic equality gate.
const nativeGates=validateReportNativeGateFacts;
const integer=value=>typeof value==='string'&&/^(0|-?[1-9][0-9]{0,14})$/.test(value);
const isZero=value=>value!==null&&value!==undefined&&(typeof value==='object'?Object.values(value).every(isZero):
 typeof value==='number'?value===0:typeof value==='string'?/^-?0(?:\.0+)?$/.test(value):false);
function zeroPayVector(vector){
 try{exact(vector,['schema_version','kind','source_mode','rate_method','row_sign','paid_minutes','break_minutes','hours','rates','total_pence'],'REPORT_NATIVE_PAY_ENTITLEMENT_INVALID');
  for(const key of ['hours','rates'])exact(vector[key],['day','night','sat','sun','bh'],'REPORT_NATIVE_PAY_ENTITLEMENT_INVALID');
  return vector.schema_version==='WEEKLY_SOURCE_RATE_VECTOR_V1'&&vector.kind==='PAY'&&vector.source_mode==='HEALTHROSTER_WEEKLY'
   &&vector.rate_method==='SPLIT_RATE_WINDOWS'&&vector.row_sign===1
   &&['paid_minutes','break_minutes','hours','rates','total_pence'].every(k=>isZero(vector[k]));
 }catch{return false;}
}
export function reportFrozenSemantics(frozenModelBytes,financial){
 const model=rawJson(frozenModelBytes,'REPORT_NATIVE_DOCUMENT_INVALID');validateFrozenPresentationModel('INVOICE_CORE',model);
 const copied=structuredClone(model),invoice=financial.invoice;
 // Replace only freshly allocated identity fields through their exact native
 // row binding. Every other frozen field, including money, quantities, source
 // references, dates, currency/sign, attachments and ordering, is retained.
 if(copied.invoice_id!==undefined){if(copied.invoice_id!==invoice.id)fail('REPORT_NATIVE_DOCUMENT_BINDING_INVALID');copied.invoice_id='REPORT_AB_SAME_NATIVE_INVOICE';}
 if(copied.invoice_number!==undefined){if(copied.invoice_number!==invoice.invoice_no)fail('REPORT_NATIVE_DOCUMENT_BINDING_INVALID');copied.invoice_number='REPORT_AB_SAME_NATIVE_INVOICE_NUMBER';}
 if(copied.payment&&Object.hasOwn(copied.payment,'remittance_reference')){
  if(copied.payment.remittance_reference!==invoice.invoice_no)fail('REPORT_NATIVE_DOCUMENT_BINDING_INVALID');
  copied.payment.remittance_reference='REPORT_AB_SAME_NATIVE_INVOICE_NUMBER';
 }
 for(const line of copied.lines){
  const original=financial.invoice_lines.find(row=>row.id===line.source_invoice_line_id);
  const bound=financial.invoice_bindings.filter(row=>row.invoice_line_id===original?.id);
  if(!original||bound.length!==1||line.source_key!==original.source_key)fail('REPORT_NATIVE_DOCUMENT_BINDING_INVALID');
  const movement=financial.movements.find(row=>row.id===bound[0].billing_movement_id);
  const presentation=financial.presentation_lines?.find(row=>row.id===bound[0].presentation_line_id);
  if(!movement||!presentation||presentation.billing_movement_id!==movement.id||!Object.hasOwn(line,'worker')||!Object.hasOwn(line,'reference')
   ||line.worker!==presentation.candidate_display_snapshot||line.reference!==presentation.booking_reference_snapshot)
   fail('REPORT_NATIVE_DOCUMENT_BINDING_INVALID');
  const stable=movement.source_line_kind==='INVOICE_VAT_ROUNDING'?'ROUND':movement.self_bill_report_row_id;
  if(stable!=='ROUND'&&!uuid.test(stable))fail('REPORT_NATIVE_DOCUMENT_BINDING_INVALID');
  const rowKey=line.row_key,sourceKey=line.source_key;
  if(typeof rowKey!=='string'||!rowKey.startsWith(sourceKey+':'))fail('REPORT_NATIVE_DOCUMENT_BINDING_INVALID');
  line.source_invoice_line_id=stable;line.row_key=stable+rowKey.slice(sourceKey.length);line.source_key=stable;
  if(stable==='ROUND')for(const field of ['worker','reference','reference_required','reference_scope','reference_source','reference_source_row_keys','week_ending_date']){
   if(Object.hasOwn(line,field))line[field]='APPROVED_ROUND_IDENTITY_PRESENTATION_TRANSPORT';
  }
 }
 return canonical(copied);
}
function derive(trace){
 exact(trace,['schema_version','execution','case_id','variant','native_request','fixture_manifest_sha256','original_report_sha256',
  'basis_bytes','reference_bytes','fin_bytes','admission_bytes','financial_bytes','before_bytes','after_bytes','fresh_bytes','fresh_basis_bytes','fresh_execution_bytes',
  'document','native_gate_facts','session_receipt'],'REPORT_NATIVE_TRACE_INVALID');
 if(trace.schema_version!==collectorSchema||!cases.includes(trace.case_id)||!['A','B'].includes(trace.variant))fail('REPORT_NATIVE_TRACE_INVALID');
 execution(trace.execution);request(trace.native_request);
 if(!hex.test(trace.fixture_manifest_sha256)||!hex.test(trace.original_report_sha256))fail('REPORT_NATIVE_TRACE_INVALID');
 const r=trace.native_request,b=rawJson(trace.basis_bytes,'REPORT_NATIVE_BASIS_INVALID');assertBasis(b,r,trace.execution,trace.fixture_manifest_sha256);
 if(trace.original_report_sha256!==byteaHex(b.report.content_sha256))fail('REPORT_NATIVE_ORIGINAL_REPORT_CHANGED');
 const ref=rawJson(trace.reference_bytes,'REPORT_NATIVE_REFERENCE_INVALID'),fin=rawJson(trace.fin_bytes,'REPORT_NATIVE_FIN_INVALID'),
  admit=rawJson(trace.admission_bytes,'REPORT_NATIVE_ADMISSION_INVALID'),f=rawJson(trace.financial_bytes,'REPORT_NATIVE_FINANCIAL_INVALID'),
  before=rawJson(trace.before_bytes,'REPORT_NATIVE_ISOLATION_INVALID'),after=rawJson(trace.after_bytes,'REPORT_NATIVE_ISOLATION_INVALID'),fresh=rawJson(trace.fresh_bytes,'REPORT_NATIVE_ISOLATION_INVALID');
 if(ref.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_FIXTURE_REFERENCE_V1'||ref.case_id!==trace.case_id||ref.variant!==trace.variant
  ||ref.source_cycle_id!==r.source_cycle_id||ref.report_scope_id!==r.report_scope_id||ref.rule_set_version_id!==b.authorization.rule_set_version_id
  ||ref.client_id!==b.scope.client_id||ref.rule_set_values_sha256!==b.authorization.rule_values_sha256||!hex.test(ref.source_census_sha256)
  ||ref.native_request_sha256!==reportNativeRequestHash(r))fail('REPORT_NATIVE_REFERENCE_INVALID');
 if(fin.ok!==true||fin.status!=='FINALISED'||fin.invoice_only!==true||fin.idempotent!==false||!uuid.test(fin.final_revision_id)
  ||fin.source_cycle_id!==r.source_cycle_id||fin.report_scope_id!==r.report_scope_id
  ||admit.ok!==true||admit.status!=='ADMITTED'||!Array.isArray(admit.invoice_ids)||admit.invoice_ids.length!==1
  ||!uuid.test(admit.invoice_ids[0])||f.revision?.id!==fin.final_revision_id||f.manifest?.id!==admit.client_manifest_id
  ||f.revision.authority_scope_kind!=='SELF_BILL_REPORT_SCOPE'||f.revision.report_scope_id!==r.report_scope_id
  ||f.revision.source_cycle_id!==r.source_cycle_id||f.revision.upload_id!==r.upload_id||f.revision.rule_set_version_id!==b.authorization.rule_set_version_id
  ||f.manifest.final_revision_id!==f.revision.id||f.manifest.client_id!==b.scope.client_id||f.manifest.source_cycle_id!==r.source_cycle_id
  ||f.manifest.source_group_id!==b.scope.source_group_id||f.invoice?.id!==admit.invoice_ids[0]||f.invoice.client_id!==b.scope.client_id
  ||f.invoice.header_snapshot_json?.meta?.final_revision_id!==f.revision.id
  ||f.allocation?.ok!==true||f.allocation.is_source_invoice!==true||f.allocation.invoice_id!==f.invoice.id
  ||f.validation?.ok!==true||f.validation.is_source_invoice!==true||f.validation.invoice_id!==f.invoice.id)
  fail('REPORT_NATIVE_FINANCIAL_INVALID');
 if(!Array.isArray(f.movements)||!f.movements.length||!Array.isArray(f.membership)||f.membership.length!==f.movements.length
  ||!Array.isArray(f.invoice_bindings)||f.invoice_bindings.length!==f.movements.length
  ||!Array.isArray(f.invoice_lines)||!f.invoice_lines.length||!Array.isArray(f.presentation_lines)||f.presentation_lines.length!==f.invoice_lines.length
  ||f.snapshot_count!=='0'||f.transition_count!=='0'
  ||!['net_pence','vat_pence','gross_pence','rounding_pence'].every(k=>integer(f[k]))
  ||BigInt(f.net_pence)+BigInt(f.vat_pence)!==BigInt(f.gross_pence))fail('REPORT_NATIVE_FINANCIAL_INVALID');
 if(f.manifest.movement_count!==f.movements.length||f.invoice_lines.length!==f.movements.length
  ||f.movements.some(m=>!uuid.test(m.id)||byteaHex(m.movement_economic_hash)===null
    ||m.final_revision_id!==f.revision.id||m.finalisation_cycle_id!==r.source_cycle_id||m.actual_client_id!==b.scope.client_id
    ||m.source_profile_kind!=='SELF_BILL_REPORT')
  ||new Set(f.movements.map(m=>m.id)).size!==f.movements.length||new Set(f.invoice_lines.map(line=>line.id)).size!==f.invoice_lines.length
  ||f.membership.some(member=>member.client_manifest_id!==f.manifest.id
    ||f.movements.filter(m=>m.id===member.billing_movement_id&&m.movement_economic_hash===member.movement_hash).length!==1)
  ||new Set(f.membership.map(member=>member.billing_movement_id)).size!==f.movements.length
  ||f.invoice_bindings.some(binding=>binding.invoice_id!==f.invoice.id||binding.state!=='CURRENT'
    ||f.invoice_lines.filter(line=>line.id===binding.invoice_line_id&&line.invoice_id===f.invoice.id).length!==1
    ||f.presentation_lines.filter(p=>p.id===binding.presentation_line_id&&p.billing_movement_id===binding.billing_movement_id
      &&p.client_manifest_id===f.manifest.id&&p.final_revision_id===f.revision.id).length!==1
    ||f.movements.filter(m=>m.id===binding.billing_movement_id).length!==1)
  ||new Set(f.invoice_bindings.map(binding=>binding.invoice_line_id)).size!==f.invoice_lines.length
  ||new Set(f.invoice_bindings.map(binding=>binding.billing_movement_id)).size!==f.movements.length)
  fail('REPORT_NATIVE_MEMBERSHIP_INVALID');
 const lines=f.movements.filter(m=>m.source_line_kind==='SELF_BILL_REPORT_LINE'),round=f.movements.filter(m=>m.source_line_kind==='INVOICE_VAT_ROUNDING');
 if(lines.length!==b.rows.length||round.length>1||lines.length+round.length!==f.movements.length
  ||f.movements.some(m=>!zeroPayVector(m.canonical_pay_vector_json)||!isZero(m.total_pay_ex_vat))
  ||lines.some(m=>m.source_facts_json?.row_resolution_id!==undefined))fail('REPORT_NATIVE_PAY_ENTITLEMENT_INVALID');
 const last=b.rows.at(-1),lastMovement=lines.find(m=>m.self_bill_report_row_id===last.id);
 if(!lastMovement||new Set(lines.map(m=>m.self_bill_report_row_id)).size!==b.rows.length
  ||b.rows.some(row=>!lines.some(m=>m.self_bill_report_row_id===row.id)))fail('REPORT_NATIVE_MEMBERSHIP_INVALID');
 let sourceNet=0n,sourceVat=0n,sourceGross=0n;
 for(const row of b.rows){const m=lines.find(value=>value.self_bill_report_row_id===row.id),facts=m.source_facts_json;
  for(const [fact,source] of [['request_id','external_source_key'],['source_row_ordinal','source_row_ordinal'],['work_date','work_date'],
   ['start_at_local','start_at_local'],['end_at_local','end_at_local'],['source_net_pence','source_shift_charge_pence'],
   ['source_vat_pence','source_vat_pence'],['source_gross_pence','source_gross_pence']])if(!same(facts?.[fact],row[source]))fail('REPORT_NATIVE_SOURCE_FACT_CHANGED');
  if(m.actual_client_id!==b.scope.client_id||m.final_revision_id!==f.revision.id||m.finalisation_cycle_id!==r.source_cycle_id
   ||m.source_profile_kind!=='SELF_BILL_REPORT'||facts.schema_version!=='WEEKLY_SOURCE_SELF_BILL_REPORT_FACTS_V1'
   ||facts.report_scope_id!==r.report_scope_id||facts.self_bill_report_id!==b.report.id||facts.upload_row_id!==row.id
   ||facts.rule_set_version_id!==b.authorization.rule_set_version_id||m.invoice_presentation_charge_pence!==row.source_shift_charge_pence)
   fail('REPORT_NATIVE_SOURCE_FACT_CHANGED');
  if(m.movement_role!=='STANDALONE_NEGATIVE'&&!['candidate_id','contract_id','work_event_id','invoice_timesheet_id'].every(k=>uuid.test(m[k])))fail('REPORT_NATIVE_SOURCE_FACT_CHANGED');
  sourceNet+=BigInt(row.source_shift_charge_pence);sourceVat+=BigInt(row.source_vat_pence);sourceGross+=BigInt(row.source_gross_pence);
 }
 if(sourceNet!==BigInt(b.report.net_pence)||sourceVat!==BigInt(b.report.vat_pence)||sourceGross!==BigInt(b.report.gross_pence)
  ||BigInt(f.net_pence)!==sourceNet||BigInt(f.vat_pence)!==sourceVat+BigInt(f.rounding_pence)
  ||BigInt(f.gross_pence)!==sourceNet+sourceVat+BigInt(f.rounding_pence))fail('REPORT_NATIVE_INVOICE_TARGET_CHANGED');
 if(round.length){const rounding=round[0],identity=['candidate_id','contract_id','work_event_id','invoice_timesheet_id'];
  if(rounding.vat_rounding_revision_id!==f.revision.id||rounding.invoice_presentation_charge_pence!==0
   ||rounding.prior_movement_id!==(trace.variant==='A'?null:lastMovement.id)
   ||identity.some(k=>trace.variant==='A'?rounding[k]!==null:rounding[k]!==lastMovement[k]))fail('REPORT_NATIVE_ROUND_IDENTITY_INVALID');}
 for(const value of [before,after,fresh]){exact(value,['database','backend_pid','pay','expenses'],'REPORT_NATIVE_ISOLATION_INVALID');
  if(value.database!=='cloudtms_test_clone'||!/^([1-9][0-9]*)$/.test(value.backend_pid)||!hex.test(value.pay)||!hex.test(value.expenses))fail('REPORT_NATIVE_ISOLATION_INVALID');}
 if(before.pay!==after.pay||before.expenses!==after.expenses||before.pay!==fresh.pay||before.expenses!==fresh.expenses
  ||before.backend_pid!==after.backend_pid||before.backend_pid===fresh.backend_pid)fail('REPORT_NATIVE_ISOLATION_CHANGED');
 if(!Array.isArray(f.off_audit_frames)||f.off_audit_frames.length!==3)fail('REPORT_NATIVE_OFF_AUDIT_INVALID');
 for(const audit of f.off_audit_frames){exact(audit,['id','object_type','object_id_text','action','event_sequence','event_sequence_is_authoritative',
   'actor_user_id','before_bytes','after_bytes','frame_base_bytes'],'REPORT_NATIVE_OFF_AUDIT_INVALID');
  if(!uuid.test(audit.id)||!uuid.test(audit.object_id_text)||audit.actor_user_id!==r.actor_user_id||audit.event_sequence_is_authoritative!==true
   ||!/^[1-9][0-9]*$/.test(audit.event_sequence))fail('REPORT_NATIVE_OFF_AUDIT_INVALID');}
 if(new Set(f.off_audit_frames.map(audit=>audit.id)).size!==3
  ||f.off_audit_frames.some((audit,index)=>index>0&&BigInt(audit.event_sequence)<=BigInt(f.off_audit_frames[index-1].event_sequence)))fail('REPORT_NATIVE_OFF_AUDIT_INVALID');
 const [referenceAudit,commandAudit,completedAudit]=f.off_audit_frames;
 const referenceFrame=rawJson(referenceAudit.after_bytes,'REPORT_NATIVE_OFF_AUDIT_INVALID'),commandFrame=rawJson(commandAudit.after_bytes,'REPORT_NATIVE_OFF_AUDIT_INVALID'),
  completedFrame=rawJson(completedAudit.after_bytes,'REPORT_NATIVE_OFF_AUDIT_INVALID');
 const {frame_sha256:referenceHash,...referenceBase}=referenceFrame,{command_sha256:commandHash,...commandBase}=commandFrame;
 if(!same(rawJson(referenceAudit.frame_base_bytes,'REPORT_NATIVE_OFF_AUDIT_INVALID'),referenceBase)
  ||!same(rawJson(commandAudit.frame_base_bytes,'REPORT_NATIVE_OFF_AUDIT_INVALID'),commandBase)||completedAudit.frame_base_bytes!==null
  ||sha256('WEEKLY_SOURCE_REPORT_FIN_FIXTURE_FRAME_V1\x1f'+referenceAudit.frame_base_bytes)!==referenceHash
  ||sha256('WEEKLY_SOURCE_OFF_RULE_COMMAND_V1\x1f'+commandAudit.frame_base_bytes)!==commandHash)fail('REPORT_NATIVE_OFF_AUDIT_INVALID');
 if(referenceAudit.object_type!=='weekly_source_off_rule_report_reference'||referenceAudit.action!=='WEEKLY_SOURCE_REPORT_FIN_FIXTURE_BOUND'
  ||referenceFrame.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_FIXTURE_FRAME_V1'||!same(referenceFrame.reference,ref)
  ||referenceFrame.authorization_sha256!==b.authorization.authorization_sha256||referenceFrame.scope_id!==b.authorization.scope_id
  ||referenceFrame.source_cas?.source_cas_sha256!==ref.source_census_sha256||!hex.test(referenceFrame.frame_sha256)
  ||referenceFrame.operational_ready!==false||referenceFrame.backend_pid!==before.backend_pid
  ||commandAudit.object_type!=='weekly_source_off_rule_command'||commandAudit.action!=='WEEKLY_SOURCE_OFF_RULE_COMMAND_ADMITTED'
  ||commandFrame.schema_version!=='WEEKLY_SOURCE_OFF_RULE_COMMAND_V1'||commandFrame.command_id!==commandAudit.object_id_text
  ||commandFrame.source_cycle_id!==r.source_cycle_id||commandFrame.actor_user_id!==r.actor_user_id||!same(commandFrame.authorization,b.authorization)
  ||commandFrame.backend_pid!==before.backend_pid||commandFrame.transaction_id!==referenceFrame.transaction_id||!hex.test(commandFrame.command_sha256)
  ||commandFrame.native_operation!=='REPORT_FIN_AB_FINALISE'||commandFrame.operational_ready!==false
  ||commandFrame.native_request_sha256!==reportNativeRequestHash(r,'WEEKLY_SOURCE_OFF_RULE_NATIVE_REQUEST_V1')
  ||completedAudit.object_type!=='weekly_source_off_rule_command'||completedAudit.action!=='WEEKLY_SOURCE_OFF_RULE_COMMAND_COMPLETED'
  ||completedAudit.object_id_text!==commandAudit.object_id_text||completedAudit.before_bytes!==commandAudit.after_bytes
  ||completedFrame.command_id!==commandFrame.command_id||completedFrame.command_sha256!==commandFrame.command_sha256
  ||completedFrame.native_result_sha256!==sha256('WEEKLY_SOURCE_OFF_RULE_NATIVE_RESULT_V1\x1f'+trace.fin_bytes)
  ||completedFrame.operational_ready!==false)fail('REPORT_NATIVE_OFF_AUDIT_INVALID');
 if(trace.fresh_basis_bytes!==trace.basis_bytes||!same(rawJson(trace.fresh_execution_bytes,'REPORT_NATIVE_EXECUTION_INVALID'),trace.execution))
  fail('REPORT_NATIVE_FRESH_RESTORE_CHANGED');
 const receipt=trace.session_receipt;exact(receipt,['database','backend_pid','fresh_backend_pid','rolled_back','turn_backend_pid',
  'original_transaction_closed','new_financial_rows','turn_lock_result_bytes','managed_writer_lock_result_bytes'],'REPORT_NATIVE_SESSION_RECEIPT_INVALID');
 if(receipt.database!=='cloudtms_test_clone'||receipt.backend_pid!==before.backend_pid||receipt.fresh_backend_pid!==fresh.backend_pid
  ||receipt.rolled_back!==true||receipt.original_transaction_closed!==true||receipt.new_financial_rows!=='0'
  ||!Number.isSafeInteger(receipt.turn_backend_pid)||receipt.turn_backend_pid<1||String(receipt.turn_backend_pid)===before.backend_pid
  ||!['t','true'].includes(receipt.turn_lock_result_bytes)||!['t','true'].includes(receipt.managed_writer_lock_result_bytes))fail('REPORT_NATIVE_SESSION_RECEIPT_INVALID');
 deriveReportCaseGrain(trace.case_id,b,f);
 const gates=nativeGates(trace.native_gate_facts,{execution:trace.execution,native_request:r,invoice_id:f.invoice.id,variant:trace.variant,case_id:trace.case_id,
  basis:b,financial:f,backend_pid:before.backend_pid,transaction_id:commandFrame.transaction_id});
 let document=null;const waiting=[...gates.waiting];
 if(trace.document===null)waiting.push('native_frozen_render');else{
  exact(trace.document,['invoice_id','document_bytes_base64','document_sha256','frozen_model_bytes','semantic_bytes','render_receipt_bytes'],'REPORT_NATIVE_DOCUMENT_INVALID');
  document=trace.document;const bytes=Buffer.from(document.document_bytes_base64,'base64');
  if(document.invoice_id!==f.invoice.id||bytes.toString('base64')!==document.document_bytes_base64||!bytes.subarray(0,5).equals(Buffer.from('%PDF-'))
   ||sha256(bytes)!==document.document_sha256)fail('REPORT_NATIVE_DOCUMENT_INVALID');
  if(document.semantic_bytes!==reportFrozenSemantics(document.frozen_model_bytes,f))fail('REPORT_NATIVE_DOCUMENT_SEMANTICS_CHANGED');
  const model=rawJson(document.frozen_model_bytes,'REPORT_NATIVE_DOCUMENT_INVALID');
  const ordered=model.lines;
  if(ordered.some((line,index)=>index>0&&Number(line.display_order)<=Number(ordered[index-1].display_order)))
   fail('REPORT_NATIVE_DOCUMENT_ARRAY_ORDER_INVALID');
  const actualLast=ordered.map(line=>f.invoice_bindings.find(binding=>binding.invoice_line_id===line.source_invoice_line_id))
   .map(binding=>f.movements.find(m=>m.id===binding?.billing_movement_id)).filter(m=>m?.source_line_kind==='SELF_BILL_REPORT_LINE').at(-1);
  if(round.length&&trace.variant==='B'&&(!actualLast||['candidate_id','contract_id','work_event_id','invoice_timesheet_id']
   .some(k=>round[0][k]!==actualLast[k])))fail('REPORT_NATIVE_ROUND_LAST_INVOICE_LINE_INVALID');
  const rendered=rawJson(document.render_receipt_bytes,'REPORT_NATIVE_DOCUMENT_INVALID');
  exact(rendered,['invoice_id','document_sha256','frozen_model_sha256','semantic_sha256'],'REPORT_NATIVE_DOCUMENT_INVALID');
  if(rendered.invoice_id!==f.invoice.id||rendered.document_sha256!==document.document_sha256
   ||rendered.frozen_model_sha256!==sha256(document.frozen_model_bytes)||rendered.semantic_sha256!==sha256(document.semantic_bytes))fail('REPORT_NATIVE_DOCUMENT_INVALID');
 }
 return {basis:b,fin,admit,financial:f,before,after,fresh,document,waiting};
}
export function validateReportNativeCollectorTrace(collectorBytes){
 const trace=rawJson(collectorBytes,'REPORT_NATIVE_TRACE_INVALID');return {trace,...derive(trace)};
}
export function deriveReportNativeReceipt(collectorBytes){
 const {trace,basis,fin,admit,financial:f,before,document,waiting}=validateReportNativeCollectorTrace(collectorBytes);
 if(waiting.length)return {status:'WAITING_NATIVE_GATES',waiting,receipt:null};
 // Semantic bytes come from the root's native frozen-document mapper. It must
 // retain every invoice/source/pay fact; only approved ROUND identity/display
 // transport and explicit generated-ID remapping may differ between variants.
 const scope={agency_id:basis.authorization.agency_id,source_group_id:basis.authorization.source_group_id,
  rule_set_version_id:basis.authorization.rule_set_version_id,rule_set_values_sha256:basis.authorization.rule_values_sha256,
  effective_from_week_ending:basis.authorization.week_ending,effective_to_week_ending:basis.authorization.week_ending};
 const row={schema_version:'WEEKLY_SOURCE_REPORT_FIN_NATIVE_VARIANT_V1',execution:trace.execution,scope,case_id:trace.case_id,variant:trace.variant,
  source_fixture_sha256:trace.fixture_manifest_sha256,original_report_sha256:trace.original_report_sha256,
  report_row_manifest_sha256:String(basis.report.row_manifest_hash).replace(/^\\x/,''),binding_authority_sha256:sha256(canonical(basis.bindings)),
  native_tool_receipt:{accepted:true,database:'cloudtms_test_clone',rolledBack:true,connectedFresh:true,
   tool_receipt_sha256:sha256(collectorBytes),journey_id:'REPORT_FIN_'+trace.case_id+'_'+trace.variant},
  native_result:{source_cycle_id:trace.native_request.source_cycle_id,report_scope_id:trace.native_request.report_scope_id,
   final_revision_id:fin.final_revision_id,client_manifest_id:admit.client_manifest_id,invoice_id:f.invoice.id,
   fin_result_sha256:sha256(trace.fin_bytes),admission_receipt_sha256:sha256(trace.admission_bytes),
   validation_receipt_sha256:sha256(canonical({allocation:f.allocation,validation:f.validation})),rendered_document_sha256:document.document_sha256,
   pay_isolation_receipt_sha256:sha256(canonical({before:trace.before_bytes,after:trace.after_bytes,fresh:trace.fresh_bytes}))},
  comparison:{schema_version:'WEEKLY_SOURCE_REPORT_FIN_COMPARISON_V1',net_pence:f.net_pence,vat_pence:f.vat_pence,gross_pence:f.gross_pence,
   rounding_pence:f.rounding_pence,report_lines_sha256:sha256(canonical(basis.rows)),
   invoice_target_sha256:sha256(document.semantic_bytes),pay_isolation_sha256:sha256(canonical({pay:before.pay,expenses:before.expenses})),
   render_semantics_sha256:sha256(document.semantic_bytes)},
  assertions:Object.fromEntries(['actual_report_and_export_binding','bound_export_pay_finalised','native_fin_atomic','native_invoice_admission',
   'native_invoice_validation','native_frozen_render','invoice_target_matches_original','rounding_identity_and_last_line_checked',
   'zero_invoice_only_pay_vectors','no_report_pay_orchestrator','no_row_resolution_pay_entitlement','native_candidate_i1_hooks_retained',
   'no_provider_settlement_email_network','rollback_fresh_restored',...(trace.case_id==='NEXT_ACTIVE'?['next_active_no_expense_observation']:[]),
   ...(trace.case_id==='ORDINARY_NHSP_REGRESSION'?['ordinary_nhsp_unchanged']:[])].map(key=>[key,true]))};
 const receipt=JSON.stringify(row);return {status:'MEASURED_NATIVE_VARIANT',waiting:[],receipt,receipt_sha256:sha256(receipt)};
}
export function assertReportNativeArtifacts(inputs,artifacts){
 if(!Array.isArray(inputs)||!inputs.length||inputs.length>14||!Array.isArray(artifacts)||artifacts.length!==inputs.length)fail('REPORT_NATIVE_ARTIFACT_CENSUS_INVALID');
 if(new Set(inputs.map(input=>input.receipt_id)).size!==inputs.length)fail('REPORT_NATIVE_ARTIFACT_CENSUS_INVALID');
 for(const input of inputs){exact(input,['receipt_id','receipt','receipt_sha256'],'REPORT_AB_COMMITTED_NATIVE_INPUT_INVALID');
  if(typeof input.receipt!=='string'||Buffer.byteLength(input.receipt)>collectorMaxBytes||sha256(input.receipt)!==input.receipt_sha256)fail('REPORT_NATIVE_ARTIFACT_INVALID');}
 const seen=new Set();let bytes=0;
 for(const artifact of artifacts){
  exact(artifact,['receipt_id','collector_bytes','collector_sha256'],'REPORT_NATIVE_ARTIFACT_INVALID');
  if(typeof artifact.receipt_id!=='string'||!/^[A-Za-z0-9_./:@-]{1,256}$/.test(artifact.receipt_id)||seen.has(artifact.receipt_id)
   ||typeof artifact.collector_bytes!=='string'||Buffer.byteLength(artifact.collector_bytes)>collectorMaxBytes
   ||sha256(artifact.collector_bytes)!==artifact.collector_sha256)fail('REPORT_NATIVE_ARTIFACT_INVALID');
  bytes+=Buffer.byteLength(artifact.collector_bytes);if(bytes>collectorMaxBundleBytes)fail('REPORT_NATIVE_ARTIFACT_BYTES_EXCEEDED');seen.add(artifact.receipt_id);
  const input=inputs.find(value=>value.receipt_id===artifact.receipt_id),derived=deriveReportNativeReceipt(artifact.collector_bytes);
  if(!input||derived.status!=='MEASURED_NATIVE_VARIANT'||input.receipt!==derived.receipt||input.receipt_sha256!==derived.receipt_sha256)
   fail('REPORT_NATIVE_RECEIPT_NOT_REDERIVED');
 }
 return artifacts;
}

// The session is an already-authorized root-owned transport. execute returns
// exact one-row UTF8 JSONB bytes; freshExecute uses a different physical PG
// connection only after the confirmed rollback. No connection/credential code.
async function collectNativeBatch({session,expectedExecution,nativeRequest,caseId,fixtureManifestSha256,
 originalReportBytes,turnBackendPid,captureFrozenDocument,nativeRegressionFrames},variants){
 execution(expectedExecution);request(nativeRequest);
 if(!session||typeof session.execute!=='function'||typeof session.freshExecute!=='function'
  ||!cases.includes(caseId)||!Array.isArray(variants)||variants.length<1||variants.length>2||new Set(variants).size!==variants.length
  ||variants.some(variant=>!['A','B'].includes(variant))||!hex.test(fixtureManifestSha256)
  ||!Buffer.isBuffer(originalReportBytes)||!originalReportBytes.length||!Number.isSafeInteger(turnBackendPid)||turnBackendPid<1
  ||(captureFrozenDocument!==undefined&&typeof captureFrozenDocument!=='function')
  ||(nativeRegressionFrames!==undefined&&(nativeRegressionFrames===null||typeof nativeRegressionFrames!=='object'||Array.isArray(nativeRegressionFrames))))fail('REPORT_NATIVE_SESSION_REQUIRED');
 let begun=false,rolledBack=false;
 try{
  const turn=await session.execute(`select exists(select 1 from pg_locks where locktype='advisory' and mode='ExclusiveLock'
   and database=(select oid from pg_database where datname=current_database()) and classid=20261013 and objid=1 and objsubid=2
   and granted and pid=${turnBackendPid} and pid<>pg_backend_pid())::text;`);
  if(turn.trim()!=='true'&&turn.trim()!=='t')fail('REPORT_NATIVE_SEND7_REQUIRED');
  begun=true;await session.execute("begin; set local jit=off; set local pg_show_plans.is_enabled=off; set local statement_timeout=0; select private.weekly_source_protected_qualification_owner_v1();");
  const writer=await session.execute("select pg_try_advisory_xact_lock_shared(hashtextextended('cloudtms_database_release_admission_v1',0))::text;");
  if(writer.trim()!=='true'&&writer.trim()!=='t')fail('REPORT_NATIVE_MANAGED_WRITER_BUSY');
  const actual=rawJson(await session.execute("select private.weekly_source_protected_qualification_release_v1()::text;"),'REPORT_NATIVE_EXECUTION_INVALID');
  if(!same(actual,expectedExecution))fail('REPORT_NATIVE_EXECUTION_CHANGED');
  if(variants.length===2)await session.execute("select 1 from public.weekly_source_groups where id=(select source_group_id from public.weekly_source_cycles where id="
   +quote(nativeRequest.source_cycle_id)+"::uuid) for update; select 1 from public.weekly_source_cycles where id="+quote(nativeRequest.source_cycle_id)
   +"::uuid for update; select 1 from public.weekly_source_report_scopes where id="+quote(nativeRequest.report_scope_id)+"::uuid for update;");
  const basisBytes=await session.execute(reportNativeBasisSql(nativeRequest));
  assertBasis(rawJson(basisBytes,'REPORT_NATIVE_BASIS_INVALID'),nativeRequest,expectedExecution,fixtureManifestSha256);
  const beforeBytes=await session.execute(reportNativeIsolationSql);
  if(rawJson(beforeBytes,'REPORT_NATIVE_ISOLATION_INVALID').backend_pid===String(turnBackendPid))fail('REPORT_NATIVE_SEND7_SEPARATE_SESSION_REQUIRED');
  const basis=rawJson(basisBytes,'REPORT_NATIVE_BASIS_INVALID'),captured=[];
  for(const variant of variants){
  if(variants.length===2)await session.execute('savepoint report_native_variant;');
  const gateBefore=await captureReportNativeGateBefore({session,nativeRequest,basis,expectedExecution});
  const ref={actor_user_id:nativeRequest.actor_user_id,source_cycle_id:nativeRequest.source_cycle_id,case_id:caseId,variant,native_request:nativeRequest};
  const referenceBytes=await session.execute('select private.weekly_source_rule_report_fixture_bind_v1('+json(ref)+')::text;');
  const command={actor_user_id:nativeRequest.actor_user_id,agency_id:basis.authorization.agency_id,environment:'TEST',
   qualification_action:'OFF_RULE_COMMAND',source_cycle_id:nativeRequest.source_cycle_id,native_operation:'REPORT_FIN_AB_FINALISE',native_request:nativeRequest};
  const finBytes=await session.execute('select public.weekly_source_group_rule_feature_set_atomic_v1('+json(command)+')::text;');
  const fin=rawJson(finBytes,'REPORT_NATIVE_FIN_INVALID');if(!uuid.test(fin.final_revision_id))fail('REPORT_NATIVE_FIN_INVALID');
  const manifest=rawJson(await session.execute("select jsonb_build_object('id',id,'sha256',encode(manifest_hash,'hex'))::text from public.weekly_source_client_manifests where final_revision_id="+quote(fin.final_revision_id)+'::uuid;'),'REPORT_NATIVE_MANIFEST_INVALID');
  exact(manifest,['id','sha256'],'REPORT_NATIVE_MANIFEST_INVALID');if(!uuid.test(manifest.id)||!hex.test(manifest.sha256))fail('REPORT_NATIVE_MANIFEST_INVALID');
  const admissionBytes=await session.execute('select public.weekly_source_invoice_admit_atomic_v1('+json({actor_user_id:nativeRequest.actor_user_id,
   client_manifest_id:manifest.id,expected_manifest_hash:manifest.sha256})+')::text;');
  const admitted=rawJson(admissionBytes,'REPORT_NATIVE_ADMISSION_INVALID');if(admitted.invoice_ids?.length!==1||!uuid.test(admitted.invoice_ids[0]))fail('REPORT_NATIVE_ADMISSION_INVALID');
  const invoiceId=admitted.invoice_ids[0];
  // Root supplies actual frozen render/native gate captures. Absent transport
  // never becomes an invented renderer/I1/origin/NEXT result.
  const document=captureFrozenDocument?await captureFrozenDocument({session,invoiceId,nativeRequest,caseId,variant,expectedExecution}):null;
  const facts=await captureReportNativeGateAfter({session,nativeRequest,basis,expectedExecution,before:gateBefore,nativeRegressionFrames,caseId,variant,invoiceId});
  const financialBytes=await session.execute(reportNativeFinancialSql(fin.final_revision_id,invoiceId));
  const afterBytes=await session.execute(reportNativeIsolationSql);
  captured.push({variant,referenceBytes,finBytes,admissionBytes,financialBytes,document,facts,afterBytes,
   revisionId:fin.final_revision_id,manifestId:manifest.id,invoiceId});
  if(variants.length===2){await session.execute('rollback to savepoint report_native_variant; release savepoint report_native_variant;');
   if(await session.execute(reportNativeBasisSql(nativeRequest))!==basisBytes)fail('REPORT_NATIVE_PAIR_BASIS_CHANGED');}
  }
  await session.execute('rollback;');rolledBack=true;
  const before=rawJson(beforeBytes,'REPORT_NATIVE_ISOLATION_INVALID');
  if(!/^[1-9][0-9]*$/.test(before.backend_pid))fail('REPORT_NATIVE_ISOLATION_INVALID');
  const freshState=rawJson(await session.freshExecute("select jsonb_build_object('isolation_bytes',("+reportNativeIsolationSql.slice(0,-1)+"),"
   +"'basis_bytes',("+reportNativeBasisSql(nativeRequest).slice(0,-1)+"),'execution_bytes',private.weekly_source_protected_qualification_release_v1()::text,"
   +"'original_transaction_closed',not exists(select 1 from pg_stat_activity where pid="+before.backend_pid+" and xact_start is not null),"
   +"'new_financial_rows',((select count(*) from public.weekly_source_final_revisions where id=any(array["+captured.map(item=>quote(item.revisionId)).join(',')+"]::uuid[]))"
   +"+(select count(*) from public.weekly_source_client_manifests where id=any(array["+captured.map(item=>quote(item.manifestId)).join(',')+"]::uuid[]))"
   +"+(select count(*) from public.invoices where id=any(array["+captured.map(item=>quote(item.invoiceId)).join(',')+"]::uuid[])))::text)::text;"),'REPORT_NATIVE_FRESH_STATE_INVALID');
  exact(freshState,['isolation_bytes','basis_bytes','execution_bytes','original_transaction_closed','new_financial_rows'],'REPORT_NATIVE_FRESH_STATE_INVALID');
  const freshBytes=freshState.isolation_bytes,fresh=rawJson(freshBytes,'REPORT_NATIVE_ISOLATION_INVALID');
  return captured.map(({variant,referenceBytes,finBytes,admissionBytes,financialBytes,document,facts,afterBytes})=>{
  const trace={schema_version:collectorSchema,execution:expectedExecution,case_id:caseId,variant,native_request:nativeRequest,
   fixture_manifest_sha256:fixtureManifestSha256,original_report_sha256:sha256(originalReportBytes),basis_bytes:basisBytes,
   reference_bytes:referenceBytes,fin_bytes:finBytes,admission_bytes:admissionBytes,financial_bytes:financialBytes,
   before_bytes:beforeBytes,after_bytes:afterBytes,fresh_bytes:freshBytes,fresh_basis_bytes:freshState.basis_bytes,
   fresh_execution_bytes:freshState.execution_bytes,document,native_gate_facts:facts,
   session_receipt:{database:'cloudtms_test_clone',backend_pid:before.backend_pid,fresh_backend_pid:fresh.backend_pid,rolled_back:true,turn_backend_pid:turnBackendPid,
    original_transaction_closed:freshState.original_transaction_closed,new_financial_rows:freshState.new_financial_rows,
    turn_lock_result_bytes:turn.trim(),managed_writer_lock_result_bytes:writer.trim()}};
  const collectorBytes=JSON.stringify(trace);if(Buffer.byteLength(collectorBytes)>collectorMaxBytes)fail('REPORT_NATIVE_ARTIFACT_BYTES_EXCEEDED');
  return {collector_bytes:collectorBytes,collector_sha256:sha256(collectorBytes),...deriveReportNativeReceipt(collectorBytes)};});
 }finally{if(begun&&!rolledBack)await session.execute('rollback;');}
}
export async function collectReportNativeVariant(options){return (await collectNativeBatch(options,[options.variant]))[0];}
// Pair collection keeps the genuine native transaction clock common. A/B each
// use an isolated savepoint; their native reference/audits never coexist. No
// receipt is returned before outer rollback and one fresh restoration check.
export async function collectReportNativePair(options){
 const variants=await collectNativeBatch(options,['A','B']);
 return {status:variants.every(item=>item.status==='MEASURED_NATIVE_VARIANT')?'MEASURED_NATIVE_PAIR':'WAITING_NATIVE_GATES',variants};
}
