-- Narrow Office read of the current signed candidate-hours PDF. Protected
-- Weekly Source facts remain unavailable to the Worker by direct table reads.
\set ON_ERROR_STOP on

begin;

create or replace function public.weekly_source_candidate_hours_evidence_v1(p_request jsonb)
returns jsonb
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_timesheet_id uuid;
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_group_id uuid;
  v_group_count integer;
  v_policy jsonb;
  v_event public.weekly_completed_pack_copy_events%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_mail public.mail_outbox%rowtype;
  v_mail_count integer;
  v_attachment jsonb;
  v_bytes bigint;
  v_sha text;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
    or exists (select 1 from pg_catalog.jsonb_object_keys(p_request) key
      where key not in ('actor_user_id','timesheet_id'))
    or not pg_catalog.pg_input_is_valid(coalesce(p_request->>'actor_user_id',''),'uuid')
    or not pg_catalog.pg_input_is_valid(coalesce(p_request->>'timesheet_id',''),'uuid') then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_EVIDENCE_REQUEST_INVALID' using errcode='22023';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_timesheet_id:=(p_request->>'timesheet_id')::uuid;
  perform private.weekly_source_office_authority_v1(
    v_actor,'VIEW_SOURCE_PROGRESS',null,null,current_date);
  select * into v_timesheet from public.timesheets
    where timesheet_id=v_timesheet_id and is_current
      and revoked_at is null and archived_at_utc is null;
  if not found or v_timesheet.contract_id is null
    or v_timesheet.sheet_scope<>'WEEKLY'
    or v_timesheet.line_type<>'HOURS' then
    return pg_catalog.jsonb_build_object('available',false);
  end if;
  select * into strict v_contract from public.contracts where id=v_timesheet.contract_id;
  select pg_catalog.count(*)::integer,
    pg_catalog.min(source_group.id::text)::uuid
    into v_group_count,v_group_id
  from public.weekly_source_group_clients membership
  join public.weekly_source_groups source_group
    on source_group.id=membership.source_group_id
  where membership.client_id=v_contract.client_id and source_group.active
    and v_timesheet.week_ending_date between membership.valid_from
      and coalesce(membership.valid_to,'infinity'::date);
  if v_group_count<>1 then
    return pg_catalog.jsonb_build_object('available',false);
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'VIEW_SOURCE_PROGRESS',v_group_id,v_contract.client_id,
    v_timesheet.week_ending_date);
  v_policy:=private._weekly_source_effective_policy_v1(
    v_contract.client_id,v_contract.id,v_timesheet.week_ending_date);
  if v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
    or v_timesheet.r2_nurse_key is null
    or v_timesheet.img_sha256_nurse is null then
    return pg_catalog.jsonb_build_object('available',false);
  end if;

  select * into v_event from public.weekly_completed_pack_copy_events
    where timesheet_id=v_timesheet_id and state in ('READY','SENT')
      and document_mode='CHECK_ONLY'
    order by created_at_utc desc,id desc limit 1;
  if not found then return pg_catalog.jsonb_build_object('available',false); end if;
  select * into v_workflow from public.candidate_submission_workflows
    where id=v_timesheet.candidate_workflow_id
      and workflow_kind='CONTRACT_HOURS' and state='WORKER_SUBMITTED'
      and contract_id=v_contract.id and generation=v_event.completion_generation
      and generation=v_timesheet.candidate_workflow_generation
      and coalesce(target_timesheet_id,anchor_timesheet_id)=v_timesheet_id
      and candidate_signed_at_utc is not null;
  if not found then return pg_catalog.jsonb_build_object('available',false); end if;
  select pg_catalog.count(*)::integer into v_mail_count
    from public.mail_outbox outbox
    where outbox.context_kind='timesheets' and outbox.context_id=v_timesheet_id
      and outbox.email_type='WEEKLY_COMPLETED_TIMESHEET_COPY'
      and outbox.payment_scope_json->>'completed_pack_copy_event_id'=v_event.id::text;
  if v_mail_count<>1 then return pg_catalog.jsonb_build_object('available',false); end if;
  select * into strict v_mail from public.mail_outbox outbox
    where outbox.context_kind='timesheets' and outbox.context_id=v_timesheet_id
      and outbox.email_type='WEEKLY_COMPLETED_TIMESHEET_COPY'
      and outbox.payment_scope_json->>'completed_pack_copy_event_id'=v_event.id::text;
  if pg_catalog.jsonb_typeof(v_mail.attachments)<>'array'
    or pg_catalog.jsonb_array_length(v_mail.attachments)<>1
    or not coalesce(v_mail.attachments_ready,false) then
    return pg_catalog.jsonb_build_object('available',false);
  end if;
  v_attachment:=v_mail.attachments->0;
  v_sha:=pg_catalog.encode(v_event.final_document_hash,'hex');
  if coalesce(v_attachment->>'size_bytes','') !~ '^[1-9][0-9]{0,7}$' then
    return pg_catalog.jsonb_build_object('available',false);
  end if;
  v_bytes:=(v_attachment->>'size_bytes')::bigint;
  if v_bytes>15728640
    or pg_catalog.btrim(coalesce(v_attachment->>'r2_key',''))=''
    or v_attachment->>'content_type'<>'application/pdf'
    or pg_catalog.lower(v_attachment->>'sha256')<>v_sha
    or v_attachment->>'completed_pack_copy_event_id'<>v_event.id::text
    or v_attachment->>'candidate_workflow_id'<>v_workflow.id::text
    or v_attachment->>'candidate_workflow_generation'<>v_workflow.generation::text
    or v_mail.attachment_total_bytes<>v_bytes
    or v_mail.payment_scope_json->>'candidate_workflow_id'<>v_workflow.id::text
    or v_mail.payment_scope_json->>'candidate_workflow_generation'<>v_workflow.generation::text
    or v_mail.payment_scope_json->>'timesheet_id'<>v_timesheet_id::text
    or v_mail.payment_scope_json->>'document_mode'<>v_event.document_mode
    or pg_catalog.lower(pg_catalog.btrim(v_mail."to"))
       <>pg_catalog.lower(pg_catalog.btrim(v_event.recipient_snapshot)) then
    return pg_catalog.jsonb_build_object('available',false);
  end if;
  return pg_catalog.jsonb_build_object(
    'available',true,'event_id',v_event.id,'timesheet_id',v_timesheet_id,
    'r2_key',v_attachment->>'r2_key','sha256',v_sha,'size_bytes',v_bytes,
    'page_count',v_attachment->'page_count',
    'filename',coalesce(nullif(v_attachment->>'filename',''),'candidate-submitted-hours.pdf'),
    'created_at_utc',v_event.created_at_utc);
end;
$function$;

alter function public.weekly_source_candidate_hours_evidence_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_candidate_hours_evidence_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_candidate_hours_evidence_v1(jsonb)
  to service_role;
notify pgrst, 'reload schema';

commit;
