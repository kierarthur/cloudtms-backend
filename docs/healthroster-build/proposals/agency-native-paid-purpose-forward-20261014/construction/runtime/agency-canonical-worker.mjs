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
    if(!DATE.test(s.date)||!DATE.test(s.end_date)||!TIME.test(s.start)||!TIME.test(s.end)
      ||!Number.isSafeInteger(s.break_minutes)||s.break_minutes<0||s.break_minutes>1440
      ||s.source_shift_id!==s.work_event_id||events.has(s.work_event_id))fail(code);
    id(s.work_event_id,code);events.add(s.work_event_id);}
  if(!Array.isArray(value.nhsp_shifts)||value.nhsp_shifts.length>100)fail(code);
  if(value.weekly_validation!==null){exact(value.weekly_validation,
    ['timesheet_id','status','reason_code','validated_at_utc','updated_at'],code);
    if(value.weekly_validation.timesheet_id!==ts||typeof value.weekly_validation.status!=='string')fail(code);}
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
    hr_validation_reason_code:frame.weekly_validation?.reason_code??null});
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
