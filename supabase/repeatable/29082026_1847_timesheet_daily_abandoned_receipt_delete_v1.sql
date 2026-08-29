-- Office-only permanent removal authority for a financially clean Candidate
-- Daily receipt which never obtained manager approval. This adapter removes
-- only the retaining Candidate lifecycle artefacts, then delegates actual
-- Timesheet/financial deletion to the existing standard authority unchanged.
\set ON_ERROR_STOP on
begin;

create or replace function public.timesheet_daily_abandoned_receipt_delete_preview_v1(
  p_timesheet_id uuid,p_actor_user_id uuid,p_expected_timesheet_id uuid default null,
  p_expected_row_signature text default null
) returns jsonb language plpgsql stable security definer
set search_path='public','pg_temp' as $function$
declare
  v_requested public.timesheets%rowtype;
  v_current public.timesheets%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_standard jsonb:='{}'::jsonb;
  v_blockers jsonb:='[]'::jsonb;
  v_workflow_count integer:=0;
  v_decision text:='BLOCKED';
  v_r2_keys text[]:=array[]::text[];
begin
  if p_timesheet_id is null then raise exception using message='TIMESHEET_ID_REQUIRED'; end if;
  if p_actor_user_id is null then raise exception using message='ACTOR_USER_ID_REQUIRED'; end if;
  if not exists(select 1 from public.tms_users u where u.id=p_actor_user_id and u.is_active=true)
    then raise exception using message='ACTOR_NOT_FOUND_OR_INACTIVE'; end if;

  select t.* into v_requested from public.timesheets t where t.timesheet_id=p_timesheet_id;
  if not found then return jsonb_build_object(
    'ok',true,'applicable',false,'kind','DAILY_ABANDONED_RECEIPT_DELETE',
    'decision','BLOCKED','eligible',false,
    'blockers',jsonb_build_array(jsonb_build_object('code','TARGET_NOT_FOUND')),
    'blocked_reasons',jsonb_build_array(jsonb_build_object('code','TARGET_NOT_FOUND')),
    'timesheet_ids','[]'::jsonb,'contract_week_ids','[]'::jsonb,'nhsp_shift_ids','[]'::jsonb,
    'preserved_source_timesheet_ids','[]'::jsonb,'preserved_source_contract_week_ids','[]'::jsonb
  ); end if;
  select t.* into v_current from public.timesheets t
  where t.booking_id=v_requested.booking_id and t.is_current=true
  order by t.version desc nulls last,t.updated_at desc nulls last,t.created_at desc nulls last,t.timesheet_id desc limit 1;
  if not found then return jsonb_build_object(
    'ok',true,'applicable',false,'kind','DAILY_ABANDONED_RECEIPT_DELETE',
    'decision','BLOCKED','eligible',false,
    'blockers',jsonb_build_array(jsonb_build_object('code','CURRENT_TIMESHEET_NOT_FOUND')),
    'blocked_reasons',jsonb_build_array(jsonb_build_object('code','CURRENT_TIMESHEET_NOT_FOUND')),
    'timesheet_ids','[]'::jsonb,'contract_week_ids','[]'::jsonb,'nhsp_shift_ids','[]'::jsonb,
    'preserved_source_timesheet_ids','[]'::jsonb,'preserved_source_contract_week_ids','[]'::jsonb
  ); end if;

  select count(*) into v_workflow_count from public.candidate_submission_workflows w
  where w.workflow_kind='DAILY' and w.scope='DAILY'
    and (w.id=v_current.candidate_workflow_id or w.target_timesheet_id=v_current.timesheet_id or w.anchor_timesheet_id=v_current.timesheet_id);
  select w.* into v_workflow from public.candidate_submission_workflows w
  where w.workflow_kind='DAILY' and w.scope='DAILY'
    and (w.id=v_current.candidate_workflow_id or w.target_timesheet_id=v_current.timesheet_id or w.anchor_timesheet_id=v_current.timesheet_id)
  order by w.updated_at_utc desc,w.id desc limit 1;
  if v_current.sheet_scope<>'DAILY'::public.timesheet_scope_enum or v_workflow_count=0 then
    return jsonb_build_object('ok',true,'applicable',false,'kind','DAILY_ABANDONED_RECEIPT_DELETE',
      'decision','NOT_APPLICABLE','eligible',false,'current_timesheet_id',v_current.timesheet_id);
  end if;

  v_standard:=public.timesheet_standard_delete_preview_v1(
    p_timesheet_id,p_actor_user_id,p_expected_timesheet_id,p_expected_row_signature);
  v_blockers:=coalesce(v_standard->'blockers','[]'::jsonb);
  if v_workflow_count<>1 then v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object(
    'code','DAILY_RECEIPT_WORKFLOW_SET_AMBIGUOUS','workflow_count',v_workflow_count)); end if;
  if v_current.timesheet_id is distinct from v_workflow.target_timesheet_id
    or v_current.timesheet_id is distinct from v_workflow.anchor_timesheet_id
    or v_current.candidate_workflow_id is distinct from v_workflow.id
    or v_current.candidate_workflow_generation is distinct from v_workflow.generation then
    v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','DAILY_RECEIPT_WORKFLOW_LINK_INVALID'));
  end if;
  if v_current.authorised_at_server is not null or v_current.candidate_manager_approved_at_utc is not null then
    v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','DAILY_RECEIPT_ALREADY_AUTHORISED'));
  end if;
  if v_workflow.state in ('MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE','RECEIVED','FINALISED')
    or v_workflow.manager_approved_at_utc is not null or v_workflow.manager_signature_component_id is not null
    or v_workflow.manager_signature_sha256 is not null or v_workflow.canonical_saved_at_utc is not null
    or v_workflow.canonical_save_financials_id is not null then
    v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','DAILY_RECEIPT_MANAGER_OR_FINAL_AUTHORITY_EXISTS'));
  end if;
  if v_workflow.state in ('REFUSED','REJECTED') then
    v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','DAILY_RECEIPT_REPLACEMENT_REQUIRED'));
  end if;
  if v_workflow.replacement_of_workflow_id is not null or exists(
    select 1 from public.candidate_submission_workflows r where r.replacement_of_workflow_id=v_workflow.id) then
    v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','DAILY_RECEIPT_REPLACEMENT_HISTORY_EXISTS'));
  end if;
  if exists(select 1 from public.candidate_approval_requests r where r.workflow_id=v_workflow.id and
    (r.state='APPROVED' or r.approved_at_utc is not null or r.signature_component_id is not null)) then
    v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','DAILY_RECEIPT_MANAGER_APPROVAL_EXISTS'));
  end if;
  if exists(select 1 from public.candidate_submission_components c where c.workflow_id=v_workflow.id and
    (c.component_kind='MANAGER_SIGNATURE' or c.manager_approved_at_utc is not null
      or c.final_signed_storage_key is not null or c.final_signed_generated_at_utc is not null
      or c.final_signed_render_state='READY')) then
    v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','DAILY_RECEIPT_FINAL_DOCUMENT_EXISTS'));
  end if;
  if exists(select 1 from public.timesheet_evidence e where e.timesheet_id=v_current.timesheet_id and
    (e.candidate_component_id is null or not exists(select 1 from public.candidate_submission_components c
      where c.id=e.candidate_component_id and c.workflow_id=v_workflow.id))) then
    v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','DAILY_RECEIPT_OFFICE_EVIDENCE_EXISTS'));
  end if;
  select coalesce(array_agg(key_value order by convert_to(key_value,'UTF8')),array[]::text[]) into v_r2_keys
  from (select distinct key_value from public.candidate_submission_components c cross join lateral unnest(array[
    c.storage_key,c.review_storage_key,c.final_signed_storage_key,c.paper_return_page_key]) key_value
    where c.workflow_id=v_workflow.id and nullif(btrim(key_value),'') is not null) keys;
  if jsonb_array_length(v_blockers)=0 and upper(coalesce(v_standard->>'decision','BLOCKED'))='PERMANENT_DELETE'
    then v_decision:='PERMANENT_DELETE';
  elsif upper(coalesce(v_standard->>'decision','BLOCKED'))='ARCHIVE_REQUIRED' then v_decision:='ARCHIVE_REQUIRED'; end if;
  return v_standard||jsonb_build_object(
    'ok',true,'applicable',true,'kind','DAILY_ABANDONED_RECEIPT_DELETE','decision',v_decision,
    'eligible',v_decision='PERMANENT_DELETE','workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
    'workflow_state',v_workflow.state,'blockers',v_blockers,'blocked_reasons',v_blockers,
    'candidate_component_r2_keys',to_jsonb(v_r2_keys),
    'delete_items',jsonb_build_array(jsonb_build_object('timesheet_id',v_current.timesheet_id,
      'booking_id',v_current.booking_id,'week_ending_date',v_current.week_ending_date,'work_date',v_workflow.work_date,
      'status',v_current.status,'display_role','DAILY_ABANDONED_RECEIPT')));
end;$function$;

alter function public.timesheet_daily_abandoned_receipt_delete_preview_v1(uuid,uuid,uuid,text) owner to postgres;
revoke all on function public.timesheet_daily_abandoned_receipt_delete_preview_v1(uuid,uuid,uuid,text) from public;
do $acl$ begin
  if exists(select 1 from pg_catalog.pg_roles where rolname='anon') then revoke all on function public.timesheet_daily_abandoned_receipt_delete_preview_v1(uuid,uuid,uuid,text) from anon; end if;
  if exists(select 1 from pg_catalog.pg_roles where rolname='authenticated') then revoke all on function public.timesheet_daily_abandoned_receipt_delete_preview_v1(uuid,uuid,uuid,text) from authenticated; end if;
  if exists(select 1 from pg_catalog.pg_roles where rolname='service_role') then grant execute on function public.timesheet_daily_abandoned_receipt_delete_preview_v1(uuid,uuid,uuid,text) to service_role; end if;
end;$acl$;

create or replace function public.timesheet_daily_abandoned_receipt_delete_apply_v1(
  p_timesheet_id uuid,p_actor_user_id uuid,p_expected_timesheet_id uuid,p_expected_row_signature text
) returns jsonb language plpgsql volatile security definer
set search_path='public','pg_temp' as $function$
declare
  v_preview jsonb;
  v_recheck jsonb;
  v_workflow_id uuid;
  v_component_r2 text[]:=array[]::text[];
  v_standard_signature text;
  v_standard jsonb;
  v_standard_r2 text[]:=array[]::text[];
  v_all_r2 text[]:=array[]::text[];
begin
  v_preview:=public.timesheet_daily_abandoned_receipt_delete_preview_v1(
    p_timesheet_id,p_actor_user_id,p_expected_timesheet_id,p_expected_row_signature);
  if coalesce((v_preview->>'applicable')::boolean,false) is not true
    or upper(coalesce(v_preview->>'decision','BLOCKED'))<>'PERMANENT_DELETE' then
    return v_preview||jsonb_build_object('apply_performed',false);
  end if;
  v_workflow_id:=nullif(v_preview->>'workflow_id','')::uuid;

  perform 1 from public.timesheets t where t.timesheet_id=p_expected_timesheet_id for update;
  perform 1 from public.candidate_submission_workflows w where w.id=v_workflow_id for update;
  perform 1 from public.candidate_approval_requests r where r.workflow_id=v_workflow_id for update;
  perform 1 from public.candidate_submission_components c where c.workflow_id=v_workflow_id for update;
  perform 1 from public.candidate_manager_email_route_receipts r where r.workflow_id=v_workflow_id for update;
  perform 1 from public.mail_outbox m
    where m.context_kind='CANDIDATE_WORKFLOW' and m.context_id=v_workflow_id for update;
  perform 1 from public.candidate_notifications n
    where n.workflow_id=v_workflow_id or n.timesheet_id=p_expected_timesheet_id for update;
  perform 1 from public.timesheet_evidence e where e.timesheet_id=p_expected_timesheet_id for update;

  v_recheck:=public.timesheet_daily_abandoned_receipt_delete_preview_v1(
    p_timesheet_id,p_actor_user_id,p_expected_timesheet_id,p_expected_row_signature);
  if upper(coalesce(v_recheck->>'decision','BLOCKED'))<>'PERMANENT_DELETE'
    or v_recheck->>'workflow_id' is distinct from v_workflow_id::text
    or v_recheck->>'current_row_signature' is distinct from p_expected_row_signature then
    raise exception using message='DAILY_RECEIPT_DELETE_TARGET_CHANGED';
  end if;
  select coalesce(array_agg(value order by convert_to(value,'UTF8')),array[]::text[]) into v_component_r2
  from jsonb_array_elements_text(coalesce(v_recheck->'candidate_component_r2_keys','[]'::jsonb)) item(value);

  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,reason
  ) values (
    p_actor_user_id,'candidate_submission_workflows',v_workflow_id::text,
    'CANDIDATE_DAILY_ABANDONED_RECEIPT_DELETE_APPLIED',
    jsonb_build_object(
      'timesheet_id',p_expected_timesheet_id,
      'workflow_id',v_workflow_id,
      'workflow_generation',(v_recheck->>'workflow_generation')::integer,
      'workflow_state',v_recheck->>'workflow_state'
    ),
    jsonb_build_object('deleted',true),
    'UNAPPROVED_FINANCIALLY_CLEAN_DAILY_RECEIPT'
  );

  -- Break only this exact workflow's retaining cycles. All ordinary Timesheet,
  -- invoice, financial and Banking triggers remain enabled.
  -- A mail already sent remains immutable audit history, but deleting its
  -- approval request below invalidates the manager link. Only unsent queued
  -- work is terminally failed so that a deleted submission cannot be emailed.
  update public.mail_outbox set status='FAILED'::public.mail_status_enum,
    last_error='CANCELLED_DAILY_ABANDONED_RECEIPT_DELETE',failed_at=now(),
    next_attempt_at_utc=null,attempt_lease_token=null,attempt_leased_at_utc=null,
    attempt_lease_expires_at_utc=null
  where context_kind='CANDIDATE_WORKFLOW' and context_id=v_workflow_id
    and status='QUEUED'::public.mail_status_enum and sent_at is null;
  update public.candidate_approval_requests set current_manager_route_receipt_id=null,
    signature_component_id=null,manager_review_timesheet_component_id=null,updated_at_utc=now()
  where workflow_id=v_workflow_id;
  delete from public.candidate_manager_email_route_receipts where workflow_id=v_workflow_id;
  update public.candidate_submission_components set approval_request_id=null,source_component_id=null
  where workflow_id=v_workflow_id;
  update public.candidate_submission_workflows set candidate_signature_component_id=null,
    manager_signature_component_id=null,updated_at_utc=now() where id=v_workflow_id;
  update public.timesheet_evidence set candidate_component_id=null
  where timesheet_id=p_expected_timesheet_id and candidate_component_id in
    (select c.id from public.candidate_submission_components c where c.workflow_id=v_workflow_id);
  update public.timesheets set candidate_workflow_id=null,candidate_workflow_generation=null,updated_at=now()
  where timesheet_id=p_expected_timesheet_id and candidate_workflow_id=v_workflow_id;
  delete from public.candidate_notifications
  where workflow_id=v_workflow_id or timesheet_id=p_expected_timesheet_id;
  delete from public.candidate_approval_requests where workflow_id=v_workflow_id;
  delete from public.candidate_submission_components where workflow_id=v_workflow_id;
  delete from public.candidate_submission_workflows where id=v_workflow_id;

  select nullif(btrim(coalesce(s.value->>'backend_row_signature',s.value->>'row_signature',s.value->>'signature','')),'')
  into v_standard_signature
  from public.timesheet_lifecycle_guard_signature_v1(p_expected_timesheet_id,null,false) as s(value);
  if v_standard_signature is null then raise exception using message='DAILY_RECEIPT_DELETE_SIGNATURE_UNAVAILABLE'; end if;
  v_standard:=public.timesheet_standard_delete_apply_v1(
    p_expected_timesheet_id,p_actor_user_id,p_expected_timesheet_id,v_standard_signature);
  if coalesce((v_standard->>'apply_performed')::boolean,false) is not true
    or coalesce((v_standard->>'deleted')::boolean,false) is not true
    or coalesce((v_standard->>'database_commit_confirmed')::boolean,false) is not true then
    raise exception using message='DAILY_RECEIPT_STANDARD_DELETE_NOT_PROVEN';
  end if;
  select coalesce(array_agg(value order by convert_to(value,'UTF8')),array[]::text[]) into v_standard_r2
  from jsonb_array_elements_text(coalesce(v_standard->'r2_cleanup_keys','[]'::jsonb)) item(value);
  select coalesce(array_agg(distinct value order by value),array[]::text[]) into v_all_r2
  from unnest(v_component_r2||v_standard_r2) item(value) where nullif(btrim(value),'') is not null;
  return v_standard||jsonb_build_object('kind','DAILY_ABANDONED_RECEIPT_DELETE',
    'current_row_signature',p_expected_row_signature,'deleted_workflow_id',v_workflow_id,
    'r2_cleanup_keys',to_jsonb(v_all_r2),'r2_cleanup_required',cardinality(v_all_r2)>0);
exception when lock_not_available or deadlock_detected then
  return jsonb_build_object('ok',false,'kind','DAILY_ABANDONED_RECEIPT_DELETE',
    'decision','BLOCKED','apply_performed',false,'error_code','LOCK_TIMEOUT',
    'message','The Timesheet is currently being changed. Refresh and try again.');
end;$function$;

alter function public.timesheet_daily_abandoned_receipt_delete_apply_v1(uuid,uuid,uuid,text) owner to postgres;
revoke all on function public.timesheet_daily_abandoned_receipt_delete_apply_v1(uuid,uuid,uuid,text) from public;
do $acl$ begin
  if exists(select 1 from pg_catalog.pg_roles where rolname='anon') then revoke all on function public.timesheet_daily_abandoned_receipt_delete_apply_v1(uuid,uuid,uuid,text) from anon; end if;
  if exists(select 1 from pg_catalog.pg_roles where rolname='authenticated') then revoke all on function public.timesheet_daily_abandoned_receipt_delete_apply_v1(uuid,uuid,uuid,text) from authenticated; end if;
  if exists(select 1 from pg_catalog.pg_roles where rolname='service_role') then grant execute on function public.timesheet_daily_abandoned_receipt_delete_apply_v1(uuid,uuid,uuid,text) to service_role; end if;
end;$acl$;

notify pgrst,'reload schema';
commit;
