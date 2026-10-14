// Concrete internal TSFIN Worker seam. Native RPCs rederive every authority;
// this codec neither grants a financial permission nor records qualification.
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SHA=/^[0-9a-f]{64}$/;
const DATE=/^\d{4}-\d{2}-\d{2}$/;
const TIME=/^(?:[01]\d|2[0-3]):[0-5]\d$/;
const PHASES=new Set(['CAPTURED','DRAFT_REMOVED','UNAUTHORISED','SCHEDULE_APPLIED',
  'RECOMPUTE_PENDING','FINANCIAL_CURRENT','AUTHORISED']);
const plain=x=>!!x&&typeof x==='object'&&!Array.isArray(x)
  &&(Object.getPrototypeOf(x)===Object.prototype||Object.getPrototypeOf(x)===null);
const fail=code=>{throw Object.assign(new Error(code),{code});};
const exact=(x,keys,code)=>{if(!plain(x)||Object.keys(x).sort().join(',')!==[...keys].sort().join(','))fail(code);};
const id=(x,code)=>{if(typeof x!=='string'||!UUID.test(x))fail(code);return x;};
const bounded=(x,max,code)=>{let json;try{json=JSON.stringify(x);}catch{fail(code);}
  if(typeof json!=='string'||new TextEncoder().encode(json).byteLength>max)fail(code);};
const clone=x=>JSON.parse(JSON.stringify(x));
export function decodeAgencyWorkerTargets(value,effectiveIds){
  const code='AGENCY_CHANGE_WORKER_TARGETS_INVALID';
  if(!Array.isArray(effectiveIds)||effectiveIds.length<1||effectiveIds.length>100
    ||new Set(effectiveIds).size!==effectiveIds.length)fail(code);
  effectiveIds.forEach(x=>id(x,code));
  if(!Array.isArray(value)||value.length>effectiveIds.length)fail(code);bounded(value,65536,code);
  const requested=new Set(effectiveIds),out=new Map(),requests=new Set();
  for(const row of value){exact(row,['request_id','timesheet_id','phase','outbox_id'],code);
    id(row.request_id,code);id(row.timesheet_id,code);
    if(!requested.has(row.timesheet_id)||out.has(row.timesheet_id)||requests.has(row.request_id)||!PHASES.has(row.phase))fail(code);
    if(row.outbox_id!==null)id(row.outbox_id,code);
    if(row.phase==='RECOMPUTE_PENDING'&&row.outbox_id===null)fail(code);
    requests.add(row.request_id);out.set(row.timesheet_id,clone(row));}
  return out;
}
export function decodeAgencyWorkerContext(value,requestId,expectedPhase){
  const code='AGENCY_CHANGE_WORKER_CONTEXT_INVALID';
  id(requestId,code);exact(value,['schema_version','request_id','request_sha256','request_version','phase',
    'phase_actor_user_id','current_row_signature','source_cas','source_schedule','outbox_id','outbox_reason',
    'outbox_row','context','weekly_context','weekly_validation','nhsp_shifts','context_sha256'],code);
  bounded(value,2097152,code);
  if(value.schema_version!=='WEEKLY_SOURCE_AGENCY_CANONICAL_WORKER_CONTEXT_V1'||value.request_id!==requestId
    ||value.phase!==expectedPhase||!['UNAUTHORISED','RECOMPUTE_PENDING'].includes(expectedPhase)
    ||typeof value.request_sha256!=='string'||!SHA.test(value.request_sha256)
    ||typeof value.context_sha256!=='string'||!SHA.test(value.context_sha256)
    ||!Number.isSafeInteger(value.request_version)||value.request_version<1
    ||typeof value.current_row_signature!=='string'||value.current_row_signature.length<1||value.current_row_signature.length>512)fail(code);
  id(value.phase_actor_user_id,code);
  exact(value.source_cas,['schema_version','source_cycle_id','client_ids','source_cas_sha256'],code);
  if(value.source_cas.schema_version!=='WEEKLY_SOURCE_BULK_SOURCE_CAS_V1'
    ||typeof value.source_cas.source_cas_sha256!=='string'||!SHA.test(value.source_cas.source_cas_sha256)
    ||!Array.isArray(value.source_cas.client_ids)||value.source_cas.client_ids.length!==1)fail(code);
  id(value.source_cas.source_cycle_id,code);id(value.source_cas.client_ids[0],code);
  exact(value.context,['effective_timesheet_id','out_timesheet','out_cur_fin','out_candidate','out_umbrella',
    'out_client_id','out_effective_flags','out_policy'],code);
  exact(value.weekly_context,['timesheet_id','out_cw','out_contract'],code);
  const c=value.context,w=value.weekly_context;
  if(!plain(c.out_timesheet)||!plain(c.out_cur_fin)||!plain(c.out_candidate)||!plain(c.out_effective_flags)||!plain(c.out_policy)
    ||!plain(w.out_cw)||!plain(w.out_contract))fail(code);
  const ts=id(c.out_timesheet.timesheet_id,code);id(c.out_cur_fin.id,code);
  id(w.out_cw.id,code);id(w.out_contract.id,code);id(w.out_contract.candidate_id,code);id(w.out_contract.client_id,code);
  if(c.effective_timesheet_id!==ts||w.timesheet_id!==ts||w.out_cw.timesheet_id!==ts
    ||w.out_cw.contract_id!==w.out_contract.id||c.out_timesheet.contract_id!==w.out_contract.id
    ||c.out_cur_fin.timesheet_id!==ts||c.out_cur_fin.timesheet_version!==c.out_timesheet.version
    ||!Number.isSafeInteger(c.out_timesheet.version)||c.out_timesheet.version<1
    ||c.out_timesheet.sheet_scope!=='WEEKLY'||c.out_timesheet.line_type!=='HOURS'
    ||c.out_timesheet.is_current!==true||c.out_timesheet.authorised_at_server!==null
    ||c.out_cur_fin.locked_by_invoice_id!==null||value.source_cas.client_ids[0]!==w.out_contract.client_id
    ||c.out_client_id!==w.out_contract.client_id||c.out_candidate.id!==w.out_contract.candidate_id)fail(code);
  if(!Array.isArray(value.source_schedule)||value.source_schedule.length>100)fail(code);
  bounded(value.source_schedule,65536,code);const events=new Set();
  for(const s of value.source_schedule){exact(s,['date','start','end','end_date','break_minutes','source_shift_id','work_event_id'],code);
    if(typeof s.date!=='string'||typeof s.end_date!=='string'||typeof s.start!=='string'||typeof s.end!=='string'
      ||!DATE.test(s.date)||!DATE.test(s.end_date)||!TIME.test(s.start)||!TIME.test(s.end)
      ||!Number.isSafeInteger(s.break_minutes)||s.break_minutes<0||s.break_minutes>1440
      ||s.source_shift_id!==s.work_event_id||events.has(s.work_event_id))fail(code);
    id(s.work_event_id,code);events.add(s.work_event_id);}
  if(!Array.isArray(value.nhsp_shifts)||value.nhsp_shifts.length>100)fail(code);
  if(value.weekly_validation!==null){exact(value.weekly_validation,
    ['timesheet_id','status','reason_code','validated_at_utc','updated_at','validation_source_kind'],code);
    if(value.weekly_validation.timesheet_id!==ts||typeof value.weekly_validation.status!=='string'
      ||!['LEGACY','AGENCY_INVOICE_WEEK'].includes(value.weekly_validation.validation_source_kind))fail(code);}
  if(expectedPhase==='UNAUTHORISED'){
    if(value.outbox_id!==null||value.outbox_reason!==null||value.outbox_row!==null)fail(code);
  }else{
    id(value.outbox_id,code);exact(value.outbox_row,
      ['id','timesheet_id','reason','attempt_count','next_attempt_at','last_error','created_at'],code);
    if(value.outbox_reason!=='MANUAL'||value.outbox_row.reason!=='MANUAL'||value.outbox_row.id!==value.outbox_id
      ||value.outbox_row.timesheet_id!==ts||!Number.isSafeInteger(value.outbox_row.attempt_count)
      ||value.outbox_row.attempt_count<1||typeof value.outbox_row.next_attempt_at!=='string')fail(code);
  }
  return clone(value);
}
export function retainAgencySourceEventProvenance(snapshot,schedule){
  const code='AGENCY_CHANGE_CANONICAL_SOURCE_PROVENANCE_INVALID';
  if(!plain(snapshot)||!Array.isArray(schedule)||schedule.length>100)fail(code);
  bounded(snapshot,1048576,code);
  const out=clone(snapshot),segments=out.invoice_breakdown_json?.segments;
  if(out.invoice_breakdown_json?.mode!=='SEGMENTS'||!Array.isArray(segments)||segments.length!==schedule.length)fail(code);
  const used=new Set(),segmentIds=new Set();
  for(const segment of segments){
    if(!plain(segment)||typeof segment.segment_id!=='string'||!segment.segment_id
      ||segmentIds.has(segment.segment_id)||segment.is_reversal===true||segment.invoice_locked_invoice_id)fail(code);
    segmentIds.add(segment.segment_id);
    const matches=schedule.filter(s=>s.date===segment.date&&s.start===segment.start&&s.end===segment.end
      &&s.break_minutes===segment.break_mins);
    if(matches.length!==1||used.has(matches[0].work_event_id))fail(code);
    const source=matches[0];id(source.work_event_id,code);
    if(source.source_shift_id!==source.work_event_id)fail(code);
    if(segment.work_event_id!=null&&segment.work_event_id!==source.work_event_id)fail(code);
    if(segment.source_shift_id!=null&&segment.source_shift_id!==source.work_event_id)fail(code);
    segment.work_event_id=source.work_event_id;segment.source_shift_id=source.work_event_id;
    used.add(source.work_event_id);
  }
  out.actual_schedule_json=clone(schedule);bounded(out,1048576,code);return out;
}
function decodeResult(value,frame,nextPhase){
  const code='AGENCY_CHANGE_NATIVE_WORKER_RESULT_UNCONFIRMED';
  exact(value,['schema_version','request_id','request_version','phase','phase_actor_user_id','timesheet_id',
    'timesheet_version','current_financial_id','outbox_id','last_refusal_code','native_write_observed','native_queue_completed'],code);
  bounded(value,16384,code);
  if(value.schema_version!=='WEEKLY_SOURCE_AGENCY_NATIVE_WORKER_RESULT_V1'||value.request_id!==frame.request_id
    ||!Number.isSafeInteger(value.request_version)||value.request_version<=frame.request_version
    ||value.phase_actor_user_id!==frame.phase_actor_user_id||value.timesheet_id!==frame.context.out_timesheet.timesheet_id
    ||value.timesheet_version!==frame.context.out_timesheet.version||typeof value.native_write_observed!=='boolean'
    ||typeof value.native_queue_completed!=='boolean'||!PHASES.has(value.phase))fail(code);
  id(value.current_financial_id,code);if(value.outbox_id!==null)id(value.outbox_id,code);
  if(value.last_refusal_code!==null){
    if(typeof value.last_refusal_code!=='string'||!/^[A-Z][A-Z0-9_]{0,199}$/.test(value.last_refusal_code)
      ||value.phase!==frame.phase||value.native_write_observed||value.native_queue_completed)fail(code);
    fail(value.last_refusal_code);
  }
  if(value.phase!==nextPhase||value.native_write_observed!==true
    ||value.native_queue_completed!==(nextPhase==='FINANCIAL_CURRENT')||value.outbox_id===null
    ||(nextPhase==='FINANCIAL_CURRENT'&&value.outbox_id!==frame.outbox_id)
    ||value.current_financial_id===frame.context.out_cur_fin.id)fail(code);
  return clone(value);
}
async function execute({requestId,readContext,buildWeeklySnapshot,applySnapshot,leasedRows},phase){
  if(typeof readContext!=='function'||typeof buildWeeklySnapshot!=='function'||typeof applySnapshot!=='function')
    fail('AGENCY_CHANGE_CANONICAL_WORKER_DEPENDENCY_UNAVAILABLE');
  const frame=decodeAgencyWorkerContext(await readContext(requestId),requestId,phase);
  if(phase==='RECOMPUTE_PENDING'){
    if(!Array.isArray(leasedRows)||leasedRows.length<1||leasedRows.length>100)fail('AGENCY_CHANGE_NATIVE_OUTBOX_NOT_LEASED');
    const ids=new Set();let matches=0;
    for(const row of leasedRows){exact(row,['outbox_id','timesheet_id','reason'],'AGENCY_CHANGE_NATIVE_LEASE_INVALID');
      id(row.outbox_id,'AGENCY_CHANGE_NATIVE_LEASE_INVALID');id(row.timesheet_id,'AGENCY_CHANGE_NATIVE_LEASE_INVALID');
      if(ids.has(row.outbox_id))fail('AGENCY_CHANGE_NATIVE_LEASE_INVALID');ids.add(row.outbox_id);
      if(row.outbox_id===frame.outbox_id){
        if(row.timesheet_id!==frame.context.out_timesheet.timesheet_id||row.reason!==frame.outbox_reason)
          fail('AGENCY_CHANGE_NATIVE_OUTBOX_NOT_LEASED');matches++;}}
    if(matches!==1)fail('AGENCY_CHANGE_NATIVE_OUTBOX_NOT_LEASED');
  }
  const c=frame.context,w=frame.weekly_context;
  const ts={...c.out_timesheet,actual_schedule_json:clone(frame.source_schedule)};
  const built=await buildWeeklySnapshot(ts,w.out_cw,w.out_contract,c.out_cur_fin,{
    write_now:false,outbox_id:frame.outbox_id,policy_override:c.out_policy,
    hr_eff_flags:c.out_effective_flags,hr_preloaded_shifts:frame.nhsp_shifts,
    hr_validation_status:frame.weekly_validation?.status??null,
    hr_validation_reason_code:frame.weekly_validation?.reason_code??null,
    hr_validation_source_kind:frame.weekly_validation?.validation_source_kind??null});
  if(!plain(built)||built.ok!==true||!plain(built.snapshot))fail('AGENCY_CHANGE_CANONICAL_BUILD_UNAVAILABLE');
  const snapshot=retainAgencySourceEventProvenance(built.snapshot,frame.source_schedule);
  if(snapshot.timesheet_id!==ts.timesheet_id||snapshot.timesheet_version!==ts.version
    ||snapshot.candidate_id!==w.out_contract.candidate_id||snapshot.client_id!==w.out_contract.client_id)
    fail('AGENCY_CHANGE_CANONICAL_TARGET_CHANGED');
  return decodeResult(await applySnapshot({p_request_id:requestId,p_expected_request_version:frame.request_version,
    p_expected_context_sha256:frame.context_sha256,p_snapshot:snapshot}),frame,
    phase==='UNAUTHORISED'?'RECOMPUTE_PENDING':'FINANCIAL_CURRENT');
}
export const runAgencyCanonicalManual=options=>execute(options,'UNAUTHORISED');
export const runAgencyLeasedCanonicalRecompute=options=>execute(options,'RECOMPUTE_PENDING');
// Codec for one literal native phase RPC. It cannot qualify an economic gate.
export function decodeAgencyAuthoriseStepResult(value,prior){
  const code='AGENCY_CHANGE_NATIVE_AUTHORISE_RESULT_UNCONFIRMED';
  exact(value,['schema_version','request_id','request_version','phase','view_actor_user_id','timesheet_id',
    'timesheet_version','financial_id','last_refusal_code','authorise_receipt','completed_receipt'],code);
  bounded(value,32768,code);
  if(!plain(prior)||value.schema_version!=='WEEKLY_SOURCE_AGENCY_NATIVE_AUTHORISE_STEP_RESULT_V1'
    ||value.request_id!==prior.request_id||value.timesheet_id!==prior.timesheet_id
    ||value.timesheet_version!==prior.timesheet_version||value.view_actor_user_id!==prior.phase_actor_user_id
    ||!Number.isSafeInteger(value.request_version)||value.request_version<prior.request_version
    ||!['FINANCIAL_CURRENT','AUTHORISED'].includes(value.phase)||value.completed_receipt!==null)fail(code);
  id(value.financial_id,code);if(value.financial_id!==prior.current_financial_id)fail(code);
  id(value.view_actor_user_id,code);
  if(value.phase==='FINANCIAL_CURRENT'){
    if(typeof value.last_refusal_code!=='string'||!/^[A-Z][A-Z0-9_]{0,199}$/.test(value.last_refusal_code)
      ||value.authorise_receipt!==null)fail(code);
    return clone(value); // Genuine causal hold; no Authorise/Return/completion assertion.
  }
  if(value.last_refusal_code!==null)fail(code);
  const receipt=value.authorise_receipt;
  exact(receipt,['schema_version','request_id','phase_actor_user_id','timesheet_id','timesheet_version','financial_id',
    'authorised_at_server','result','pay_purpose_sha256','transaction_id','backend_pid'],code);
  if(receipt.schema_version!=='WEEKLY_SOURCE_AGENCY_NATIVE_AUTHORISE_V1'||receipt.request_id!==value.request_id
    ||receipt.timesheet_id!==value.timesheet_id||receipt.timesheet_version!==value.timesheet_version
    ||receipt.financial_id!==value.financial_id||typeof receipt.pay_purpose_sha256!=='string'||!SHA.test(receipt.pay_purpose_sha256)
    ||typeof receipt.authorised_at_server!=='string'||!Number.isFinite(Date.parse(receipt.authorised_at_server))
    ||typeof receipt.transaction_id!=='string'||!/^[1-9][0-9]{0,19}$/.test(receipt.transaction_id)
    ||!Number.isSafeInteger(receipt.backend_pid)||receipt.backend_pid<1||receipt.backend_pid>2147483647)fail(code);
  id(receipt.phase_actor_user_id,code); // Stored real approval actor; replay does not relabel it to the reader.
  const n=receipt.result;
  exact(n,['ok','success','operation','timesheet_id','current_timesheet_id','requested_timesheet_id','contract_week_id',
    'booking_id','candidate_id','authorised_at_server','revoked_at','authorised_at_utc','advance_state_refresh','current_version',
    'was_stale','previous_processing_status','processing_status','new_processing_status','validation_status','validation_pre_validated',
    'hr_validation_required_for_invoice','backend_row_signature','row_signature','affected_rows','cache_invalidation_hints'],code);
  if(n.ok!==true||n.success!==true||n.operation!=='timesheet_authorise'||n.timesheet_id!==value.timesheet_id
    ||n.current_timesheet_id!==value.timesheet_id||n.requested_timesheet_id!==value.timesheet_id
    ||n.current_version!==value.timesheet_version||n.was_stale!==false||n.revoked_at!==null
    ||n.authorised_at_server!==receipt.authorised_at_server||n.authorised_at_utc!==receipt.authorised_at_server
    ||!['READY_FOR_HR','READY_FOR_INVOICE'].includes(n.processing_status)||n.new_processing_status!==n.processing_status
    ||typeof n.backend_row_signature!=='string'||n.backend_row_signature.length<1||n.backend_row_signature.length>512
    ||n.row_signature!==n.backend_row_signature||typeof n.booking_id!=='string'||n.booking_id.length<1||n.booking_id.length>200
    ||typeof n.validation_pre_validated!=='boolean'||typeof n.hr_validation_required_for_invoice!=='boolean'
    ||!plain(n.advance_state_refresh)||!plain(n.cache_invalidation_hints)||!Array.isArray(n.affected_rows)||n.affected_rows.length!==1)fail(code);
  id(n.contract_week_id,code);id(n.candidate_id,code);
  return clone(value);
}

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
