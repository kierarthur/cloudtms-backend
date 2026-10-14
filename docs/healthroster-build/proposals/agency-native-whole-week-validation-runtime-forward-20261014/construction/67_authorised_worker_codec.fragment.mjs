// Native authorised mode is a separate closed frame. It retains the real
// approval and cannot be decoded by changing it to a fictional UNA context.
export function decodeAgencyAuthorisedValidationContext(value,requestId){
 const code='AGENCY_CHANGE_AUTHORISED_VALIDATION_CONTEXT_INVALID';id(requestId,code);
 exact(value,['schema_version','request_id','request_sha256','request_version','phase','phase_actor_user_id',
  'current_row_signature','source_cas','source_schedule','outbox_id','outbox_reason','outbox_row','context',
  'weekly_context','weekly_validation','nhsp_shifts','week_facts_sha256','week_complete','validation_actor_user_id','context_sha256'],code);
 bounded(value,2097152,code);
 if(value.schema_version!=='WEEKLY_SOURCE_AGENCY_AUTHORISED_VALIDATION_CONTEXT_V1'||value.request_id!==requestId
  ||value.phase!=='AUTHORISED'||!Number.isSafeInteger(value.request_version)||value.request_version<1
  ||typeof value.request_sha256!=='string'||!SHA.test(value.request_sha256)
  ||typeof value.context_sha256!=='string'||!SHA.test(value.context_sha256)
  ||typeof value.week_facts_sha256!=='string'||!SHA.test(value.week_facts_sha256)
  ||typeof value.week_complete!=='boolean'||typeof value.current_row_signature!=='string'
  ||value.current_row_signature.length<1||value.current_row_signature.length>512)fail(code);
 id(value.phase_actor_user_id,code);id(value.validation_actor_user_id,code);id(value.outbox_id,code);
 exact(value.source_cas,['schema_version','source_cycle_id','client_ids','source_cas_sha256'],code);
 if(value.source_cas.schema_version!=='WEEKLY_SOURCE_BULK_SOURCE_CAS_V1'
  ||!Array.isArray(value.source_cas.client_ids)||value.source_cas.client_ids.length!==1
  ||typeof value.source_cas.source_cas_sha256!=='string'||!SHA.test(value.source_cas.source_cas_sha256))fail(code);
 id(value.source_cas.source_cycle_id,code);id(value.source_cas.client_ids[0],code);
 exact(value.context,['effective_timesheet_id','out_timesheet','out_cur_fin','out_candidate','out_umbrella',
  'out_client_id','out_effective_flags','out_policy'],code);
 exact(value.weekly_context,['timesheet_id','out_cw','out_contract'],code);
 const c=value.context,w=value.weekly_context;
 if(!plain(c.out_timesheet)||!plain(c.out_cur_fin)||!plain(c.out_candidate)||!plain(c.out_effective_flags)||!plain(c.out_policy)
  ||!plain(w.out_cw)||!plain(w.out_contract))fail(code);
 const t=c.out_timesheet,f=c.out_cur_fin,ts=id(t.timesheet_id,code);
 id(f.id,code);id(w.out_cw.id,code);id(w.out_contract.id,code);id(w.out_contract.candidate_id,code);id(w.out_contract.client_id,code);
 if(c.effective_timesheet_id!==ts||w.timesheet_id!==ts||w.out_cw.timesheet_id!==ts
  ||w.out_cw.contract_id!==w.out_contract.id||t.contract_id!==w.out_contract.id
  ||f.timesheet_id!==ts||f.timesheet_version!==t.version||!Number.isSafeInteger(t.version)||t.version<1
  ||t.sheet_scope!=='WEEKLY'||t.line_type!=='HOURS'||t.is_current!==true||t.revoked_at!==null
  ||typeof t.authorised_at_server!=='string'||!Number.isFinite(Date.parse(t.authorised_at_server))
  ||f.authorised_at_utc!==t.authorised_at_server||f.paid_at_utc!==null||f.locked_by_invoice_id!==null
  ||c.out_candidate.id!==w.out_contract.candidate_id||c.out_client_id!==w.out_contract.client_id
  ||value.source_cas.client_ids[0]!==w.out_contract.client_id)fail(code);
 id(f.authorised_by_user_id,code);
 if(!Array.isArray(value.source_schedule)||value.source_schedule.length>100
  ||!Array.isArray(value.nhsp_shifts)||value.nhsp_shifts.length>100)fail(code);
 const used=new Set();for(const s of value.source_schedule){
  exact(s,['date','start','end','end_date','break_minutes','source_shift_id','work_event_id'],code);
  if(typeof s.date!=='string'||typeof s.end_date!=='string'||typeof s.start!=='string'||typeof s.end!=='string'
   ||!DATE.test(s.date)||!DATE.test(s.end_date)||!TIME.test(s.start)||!TIME.test(s.end)
   ||!Number.isSafeInteger(s.break_minutes)||s.break_minutes<0||s.break_minutes>1440
   ||s.source_shift_id!==s.work_event_id||used.has(s.work_event_id))fail(code);
  id(s.work_event_id,code);used.add(s.work_event_id);
 }
 if(JSON.stringify(t.actual_schedule_json)!==JSON.stringify(value.source_schedule)
  ||JSON.stringify(f.actual_schedule_json)!==JSON.stringify(value.source_schedule))fail(code);
 const v=value.weekly_validation;
 exact(v,['id','timesheet_id','booking_id','status','reason_code','hr_request_id','validated_at_utc',
  'override_requested_at_utc','override_requested_by','override_confirmed_at_utc','override_confirmed_by','override_reason',
  'override_cleared_at_utc','last_source','created_at','updated_at','hr_request_source','hr_request_set_by',
  'hr_request_set_at_utc','pre_validated','validation_source_kind','validation_source_publication_id',
  'validation_source_facts_sha256','validation_source_actor_user_id'],code);
 id(v.id,code);id(v.validation_source_publication_id,code);id(v.validation_source_actor_user_id,code);
 if(v.timesheet_id!==ts||v.status!==(value.week_complete?'VALIDATION_OK':'VALIDATION_ERROR')
  ||v.validation_source_kind!=='AGENCY_INVOICE_WEEK'||v.last_source!==null||v.pre_validated!==false
  ||v.validation_source_actor_user_id!==value.validation_actor_user_id
  ||typeof v.validation_source_facts_sha256!=='string'||v.validation_source_facts_sha256!=='\\x'+value.week_facts_sha256
  ||v.override_requested_at_utc!==null||v.override_requested_by!==null||v.override_confirmed_at_utc!==null
  ||v.override_confirmed_by!==null||v.override_reason!==null)fail(code);
 exact(value.outbox_row,['id','timesheet_id','reason','attempt_count','next_attempt_at','last_error','created_at'],code);
 const q=value.outbox_row;
 if(value.outbox_reason!=='CONTEXT_CHANGED'||q.reason!==value.outbox_reason||q.id!==value.outbox_id
  ||q.timesheet_id!==ts||!Number.isSafeInteger(q.attempt_count)||q.attempt_count<1
  ||typeof q.next_attempt_at!=='string'||!Number.isFinite(Date.parse(q.next_attempt_at)))fail(code);
 return clone(value);
}
// The actual shared sbRpc preserves PostgREST's parsed error in error.json.
// It sets error.code only for Banking busy. Keep that owner unchanged; retain
// only a closed native causal code here, never its raw body/details/frame.
export function agencyNativeCausalCode(error,fallback){
 for(const candidate of [error?.code,error?.json?.message])if(typeof candidate==='string'&&/^[A-Z][A-Z0-9_]{0,199}$/.test(candidate))return candidate;
 return fallback;
}
export async function runAgencyLeasedAuthorisedValidation({requestId,readContext,buildWeeklySnapshot,applySnapshot,leasedRows}){
 const code='AGENCY_CHANGE_NATIVE_VALIDATION_RESULT_UNCONFIRMED';
 if(typeof readContext!=='function'||typeof buildWeeklySnapshot!=='function'||typeof applySnapshot!=='function')fail(code);
 const frame=decodeAgencyAuthorisedValidationContext(await readContext(requestId),requestId);
 if(!Array.isArray(leasedRows)||leasedRows.length<1||leasedRows.length>100)fail('AGENCY_CHANGE_NATIVE_OUTBOX_NOT_LEASED');
 const ids=new Set();let matches=0;for(const q of leasedRows){exact(q,['outbox_id','timesheet_id','reason'],code);
  id(q.outbox_id,code);id(q.timesheet_id,code);if(ids.has(q.outbox_id))fail(code);ids.add(q.outbox_id);
  if(q.outbox_id===frame.outbox_id){if(q.timesheet_id!==frame.context.out_timesheet.timesheet_id||q.reason!=='CONTEXT_CHANGED')fail(code);matches++;}}
 if(matches!==1)fail('AGENCY_CHANGE_NATIVE_OUTBOX_NOT_LEASED');
 const c=frame.context,w=frame.weekly_context,t=c.out_timesheet;
 const built=await buildWeeklySnapshot(t,w.out_cw,w.out_contract,c.out_cur_fin,{write_now:false,outbox_id:frame.outbox_id,
  policy_override:c.out_policy,hr_eff_flags:c.out_effective_flags,hr_preloaded_shifts:frame.nhsp_shifts,
  hr_validation_status:frame.weekly_validation.status,hr_validation_reason_code:frame.weekly_validation.reason_code,
  hr_validation_source_kind:frame.weekly_validation.validation_source_kind});
 if(!plain(built)||built.ok!==true||!plain(built.snapshot))fail('AGENCY_CHANGE_CANONICAL_BUILD_UNAVAILABLE');
 const snapshot=retainAgencySourceEventProvenance(built.snapshot,frame.source_schedule);
 if(snapshot.timesheet_id!==t.timesheet_id||snapshot.timesheet_version!==t.version
  ||snapshot.candidate_id!==w.out_contract.candidate_id||snapshot.client_id!==w.out_contract.client_id)fail(code);
 const value=await applySnapshot({p_request_id:requestId,p_expected_request_version:frame.request_version,
  p_expected_context_sha256:frame.context_sha256,p_snapshot:snapshot});
 exact(value,['schema_version','request_id','request_version','phase','phase_actor_user_id','timesheet_id','timesheet_version',
  'outbox_id','financial_id','native_write_observed','native_queue_completed','validation_receipt'],code);bounded(value,16384,code);
 if(value.schema_version!=='WEEKLY_SOURCE_AGENCY_VALIDATION_APPLY_RESULT_V1'||value.request_id!==requestId
  ||value.phase!=='AUTHORISED'||value.phase_actor_user_id!==frame.phase_actor_user_id||value.timesheet_id!==t.timesheet_id
  ||value.timesheet_version!==t.version||!Number.isSafeInteger(value.request_version)||value.request_version<=frame.request_version
  ||value.outbox_id!==frame.outbox_id||value.native_write_observed!==true||value.native_queue_completed!==true)fail(code);
 id(value.financial_id,code);if(value.financial_id===c.out_cur_fin.id)fail(code);
 const r=value.validation_receipt;
 exact(r,['schema_version','request_id','timesheet_id','timesheet_version','financial_id','prior_financial_id','observed_outbox_id',
  'publication_id','facts_sha256','validation_actor_user_id','phase_actor_user_id','financial_row_sha256','transaction_id',
  'backend_pid','native_ok_count','native_fail_count'],code);
 if(r.schema_version!=='WEEKLY_SOURCE_AGENCY_NATIVE_VALIDATION_RECOMPUTE_V1'||r.request_id!==requestId
  ||r.timesheet_id!==t.timesheet_id||r.timesheet_version!==t.version||r.financial_id!==value.financial_id
  ||r.prior_financial_id!==c.out_cur_fin.id||r.observed_outbox_id!==frame.outbox_id
  ||r.publication_id!==frame.weekly_validation.validation_source_publication_id||r.facts_sha256!==frame.week_facts_sha256
  ||r.validation_actor_user_id!==frame.validation_actor_user_id||r.phase_actor_user_id!==frame.phase_actor_user_id
  ||typeof r.financial_row_sha256!=='string'||!SHA.test(r.financial_row_sha256)
  ||typeof r.transaction_id!=='string'||!/^[1-9][0-9]{0,19}$/.test(r.transaction_id)
  ||!Number.isSafeInteger(r.backend_pid)||r.backend_pid<1||r.backend_pid>2147483647
  ||r.native_ok_count!==1||r.native_fail_count!==0)fail(code);
 return clone(value);
}
