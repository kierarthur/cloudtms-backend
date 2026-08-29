-- Factual Daily receipt only. No rate calculation, TSFIN write, processing or
-- Office authorisation is performed here. Existing financial owners are unchanged.
begin;

create or replace function private._candidate_daily_factual_receipt_v1(
  p_workflow_id uuid,
  p_generation integer,
  p_input_sha256_hex text,
  p_electronic_patch jsonb,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_context jsonb;
  v_input jsonb;
  v_patch jsonb;
  v_start timestamptz;
  v_end timestamptz;
  v_break_start timestamptz;
  v_break_end timestamptz;
  v_break_minutes integer;
  v_minutes integer;
  v_candidate_signature public.candidate_submission_components%rowtype;
  v_manager_signature public.candidate_submission_components%rowtype;
begin
  select * into v_workflow from public.candidate_submission_workflows
    where id=p_workflow_id and generation=p_generation for update;
  if not found or v_workflow.workflow_kind<>'DAILY' or v_workflow.scope<>'DAILY'
     or v_workflow.route<>'PHONE' or v_workflow.state<>'READY_TO_FINALISE'
     or v_workflow.contract_week_id is not null or v_workflow.manager_approved_at_utc is null then
    raise exception 'CANDIDATE_DAILY_RECEIPT_NOT_READY' using errcode='55000';
  end if;
  v_context:=private._candidate_daily_receipt_context_v1(
    v_workflow.environment,v_workflow.candidate_id,v_workflow.target_timesheet_id,true,p_now_utc);
  if not coalesce((v_context->>'candidate_first_receipt')::boolean,false)
     or not coalesce((v_context->>'office_resolution_pending')::boolean,false) then
    raise exception 'CANDIDATE_DAILY_RECEIPT_CONTEXT_CHANGED' using errcode='40001';
  end if;
  select * into v_timesheet from public.timesheets
    where timesheet_id=v_workflow.target_timesheet_id and is_current for update;
  if v_timesheet.timesheet_id is null or v_timesheet.authorised_at_server is not null then
    raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
  end if;
  v_input:=private._candidate_daily_canonical_save_input_v1(v_workflow.id,v_workflow.generation);
  if p_input_sha256_hex is null or p_input_sha256_hex!~'^[a-f0-9]{64}$'
     or encode(private._candidate_sha256_jsonb_v1(v_input),'hex')<>p_input_sha256_hex then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_MISMATCH' using errcode='40001';
  end if;
  v_patch:=v_input->'timesheet_patch_json';
  v_start:=nullif(v_patch->>'worked_start_iso','')::timestamptz;
  v_end:=nullif(v_patch->>'worked_end_iso','')::timestamptz;
  v_break_start:=nullif(v_patch->>'break_start_iso','')::timestamptz;
  v_break_end:=nullif(v_patch->>'break_end_iso','')::timestamptz;
  v_break_minutes:=nullif(v_patch->>'break_minutes','')::integer;
  if v_start is null or v_end is null or not isfinite(v_start) or not isfinite(v_end)
     or v_end<=v_start or (v_start at time zone 'Europe/London')::date<>v_workflow.work_date
     or (v_end at time zone 'Europe/London')::date not between v_workflow.work_date and v_workflow.work_date+1
     or extract(second from v_start)<>0 or extract(second from v_end)<>0
     or (v_break_start is null)<>(v_break_end is null)
     or (v_break_start is not null and (
       not isfinite(v_break_start) or not isfinite(v_break_end)
       or v_break_start<v_start or v_break_end>v_end or v_break_end<=v_break_start
       or extract(second from v_break_start)<>0 or extract(second from v_break_end)<>0)) then
    raise exception 'CANDIDATE_DAILY_HOURS_INVALID' using errcode='22023';
  end if;
  if v_break_start is not null then
    if v_break_minutes is not null and v_break_minutes<>
        (extract(epoch from (v_break_end-v_break_start))/60)::integer then
      raise exception 'CANDIDATE_DAILY_BREAK_INVALID' using errcode='22023';
    end if;
    v_break_minutes:=(extract(epoch from (v_break_end-v_break_start))/60)::integer;
  end if;
  if v_break_minutes is null or v_break_minutes<0
     or v_break_minutes>=extract(epoch from (v_end-v_start))/60
     or jsonb_typeof(v_patch->'actual_schedule_json') is distinct from 'array'
     or jsonb_array_length(v_patch->'actual_schedule_json')<>1
     or exists(select 1 from jsonb_array_elements(v_patch->'actual_schedule_json') d
       where d->>'date' is distinct from v_workflow.work_date::text) then
    raise exception 'CANDIDATE_DAILY_BREAK_INVALID' using errcode='22023';
  end if;
  v_minutes:=(extract(epoch from (v_end-v_start))/60)::integer-v_break_minutes;
  select * into v_candidate_signature from public.candidate_submission_components
    where id=v_workflow.candidate_signature_component_id and workflow_id=v_workflow.id
      and document_role='CANDIDATE_SIGNATURE' and state='IMMUTABLE';
  select * into v_manager_signature from public.candidate_submission_components
    where id=v_workflow.manager_signature_component_id and workflow_id=v_workflow.id
      and document_role='MANAGER_SIGNATURE' and state='IMMUTABLE';
  if v_candidate_signature.id is null or v_manager_signature.id is null
     or v_candidate_signature.source_content_sha256 is distinct from v_workflow.candidate_signature_sha256
     or v_manager_signature.source_content_sha256 is distinct from v_workflow.manager_signature_sha256
     or not exists(select 1 from public.candidate_approval_requests a
       where a.workflow_id=v_workflow.id and a.workflow_generation=v_workflow.generation
         and a.id=v_manager_signature.approval_request_id and a.method='PHONE' and a.state='APPROVED'
         and a.review_manifest_sha256=v_workflow.review_manifest_sha256)
     or p_electronic_patch is distinct from jsonb_build_object(
       'submission_mode','ELECTRONIC','auth_name',v_workflow.manager_name,
       'auth_job_title',v_workflow.manager_position,
       'r2_nurse_key',v_candidate_signature.storage_key,'r2_auth_key',v_manager_signature.storage_key,
       'img_sha256_nurse',encode(v_candidate_signature.source_content_sha256,'hex'),
       'img_sha256_auth',encode(v_manager_signature.source_content_sha256,'hex'),
       'candidate_workflow_id',v_workflow.id,'candidate_workflow_generation',v_workflow.generation,
       'candidate_manager_approved_at_utc',v_workflow.manager_approved_at_utc) then
    raise exception 'ELECTRONIC_SIGNATURE_PAIR_INCOMPLETE' using errcode='55000';
  end if;

  -- Store only what the Candidate and manager attested. Normal Timesheet
  -- triggers remain enabled and retain their ordinary derived queue work.
  update public.timesheets set
    worked_start_iso=v_start,worked_end_iso=v_end,
    break_start_iso=v_break_start,break_end_iso=v_break_end,
    break_minutes=v_break_minutes,worked_minutes=v_minutes,
    actual_schedule_json=v_patch->'actual_schedule_json',
    submission_mode='ELECTRONIC',candidate_submission_route_intent=null,
    auth_name=v_workflow.manager_name,auth_job_title=v_workflow.manager_position,
    r2_nurse_key=v_candidate_signature.storage_key,r2_auth_key=v_manager_signature.storage_key,
    img_sha256_nurse=encode(v_candidate_signature.source_content_sha256,'hex'),
    img_sha256_auth=encode(v_manager_signature.source_content_sha256,'hex'),
    candidate_workflow_id=v_workflow.id,candidate_workflow_generation=v_workflow.generation,
    candidate_manager_approved_at_utc=v_workflow.manager_approved_at_utc,
    updated_at=p_now_utc
  where timesheet_id=v_timesheet.timesheet_id and is_current and authorised_at_server is null;
  if not found then
    raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
  end if;
  return jsonb_build_object('ok',true,
    'contract_version','CANDIDATE_DAILY_FACTUAL_RECEIPT_V1',
    'timesheet_id',v_timesheet.timesheet_id,'office_resolution_pending',true,
    'financial_processing_performed',false,'auto_authorised',false);
end;
$function$;

alter function private._candidate_daily_factual_receipt_v1(uuid,integer,text,jsonb,timestamptz) owner to postgres;
revoke all on function private._candidate_daily_factual_receipt_v1(uuid,integer,text,jsonb,timestamptz)
  from public,anon,authenticated,service_role;
commit;
