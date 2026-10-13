create or replace function private.timesheet_correction_actor_allowed_v1(p_actor_user_id uuid)
returns boolean language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_allowed boolean:=false;
begin
  if p_actor_user_id is null or not exists(select 1 from public.tms_users u
    where u.id=p_actor_user_id and coalesce(u.is_active,false)) then return false; end if;
  if to_regprocedure('private.weekly_source_pay_actor_allowed_v1(uuid)') is null then return false; end if;
  execute 'select private.weekly_source_pay_actor_allowed_v1($1)' into v_allowed using p_actor_user_id;
  return v_allowed is true;
end;
$function$;

-- Preserve source terms and signature/business authority across later normal
-- authorisation and document bookkeeping. Unknown future fields stay frozen.
create or replace function private.timesheet_correction_timesheet_authority_hash_v1(p_timesheet jsonb)
returns text language sql immutable security definer
set search_path to 'pg_catalog','extensions','pg_temp'
as $function$
  select encode(extensions.digest((p_timesheet-array['authorised_at_server','updated_at',
    'document_revision','document_state','current_document_version_id','active_document_operation_id','last_document_error_json',
    'generated_pdf_at_utc','generated_pdf_refs_sig','generated_pdf_refs_snapshot_json','generated_pdf_refs_captured_at_utc'])::text,
    'sha256'),'hex');
$function$;

create or replace function private.timesheet_correction_actor_assert_v1(p_actor_user_id uuid)
returns void language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_allowed boolean:=false;
begin
  perform 1 from public.tms_users u where u.id=p_actor_user_id for share;
  if p_actor_user_id is null or not exists(select 1 from public.tms_users u
    where u.id=p_actor_user_id and coalesce(u.is_active,false)) then
    raise exception 'CORRECTION_ACTOR_NOT_ALLOWED' using errcode='42501';
  end if;
  if to_regprocedure('private.weekly_source_pay_actor_allowed_v1(uuid)') is null then
    raise exception 'CORRECTION_PAY_ACTOR_HELPER_UNAVAILABLE' using errcode='55000';
  end if;
  execute 'select private.weekly_source_pay_actor_allowed_v1($1)' into v_allowed using p_actor_user_id;
  if v_allowed is distinct from true then
    raise exception 'CORRECTION_ACTOR_NOT_ALLOWED' using errcode='42501';
  end if;
end;
$function$;

-- Economic source identity is separate from later authorisation, invoice
-- placement and cash/notice bookkeeping. All monetary/policy/component facts
-- and every undeclared future field remain fingerprinted.
create or replace function private.timesheet_correction_financial_authority_hash_v1(p_snapshot jsonb)
returns text language sql immutable security definer
set search_path to 'pg_catalog','extensions','pg_temp'
as $function$
  select encode(extensions.digest((
    (p_snapshot-array['is_current','is_stale','stale_reason','computed_at_utc','locked_by_invoice_id','locked_at_utc',
      'unlocked_by_credit_note_id','created_at','updated_at','processing_status','candidate_assignment','paid_at_utc',
      'paid_by_user_id','payment_reference','remittance_last_sent_at_utc','remittance_send_count','processed_by_user_id',
      'processed_at_utc','authorised_by_user_id','authorised_at_utc','pay_on_hold','pay_on_hold_reason','pay_on_hold_since_utc'])
      -array['hr_crosscheck_status','hr_crosscheck_issues','external_source_rows_json']
    ||jsonb_build_object('invoice_breakdown_json',case when jsonb_typeof(p_snapshot#>'{invoice_breakdown_json,segments}')='array'
      then jsonb_set(p_snapshot->'invoice_breakdown_json','{segments}',coalesce((select jsonb_agg(
        s.value-array['invoice_locked_invoice_id','invoice_locked_at_utc','pay_locked','pay_locked_at_utc'] order by s.ordinality)
        from jsonb_array_elements(p_snapshot#>'{invoice_breakdown_json,segments}') with ordinality s(value,ordinality)),'[]'::jsonb),true)
      else p_snapshot->'invoice_breakdown_json' end)
  )::text,'sha256'),'hex');
$function$;

create or replace function private.timesheet_correction_origin_v1(p_timesheet_id uuid)
returns text language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','extensions','pg_temp'
as $function$
declare
  v_agency boolean:=false;
  v_signed boolean:=false;
begin
  if to_regprocedure('private.weekly_source_agency_invoice_check_only_v1(uuid)') is not null then
    execute 'select private.weekly_source_agency_invoice_check_only_v1($1)' into v_agency using p_timesheet_id;
    if v_agency is true then return 'AGENCY_INVOICE_CORRECTION'; end if;
  end if;
  if to_regprocedure('private.weekly_source_signed_check_only_v1(uuid)') is not null then
    execute 'select private.weekly_source_signed_check_only_v1($1)' into v_signed using p_timesheet_id;
    if v_signed is true then return 'SIGNED_TIMESHEET_CORRECTION'; end if;
  end if;
  return null;
exception when sqlstate '55000' or sqlstate '22023' then return null;
end;
$function$;

-- Signature authority is the immutable approval/component pair, never Office
-- authorisation text, a worker signature alone, paper/manual key or QR field.
create or replace function private.timesheet_correction_signed_witness_v1(p_timesheet_id uuid,p_lock_rows boolean default false)
returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare
  v_ts public.timesheets%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_hours public.candidate_submission_components%rowtype;
  v_candidate public.candidate_submission_components%rowtype;
  v_manager public.candidate_submission_components%rowtype;
begin
  select t.* into v_ts from public.timesheets t where t.timesheet_id=p_timesheet_id;
  if v_ts.timesheet_id is null or v_ts.submission_mode::text<>'ELECTRONIC'
    or v_ts.candidate_workflow_id is null or v_ts.candidate_workflow_generation is null then
    raise exception 'CORRECTION_MANAGER_SIGNED_TIMESHEET_REQUIRED' using errcode='55000';
  end if;
  if p_lock_rows then
    perform 1 from public.candidate_submission_workflows w where w.id=v_ts.candidate_workflow_id for update;
    perform 1 from public.candidate_approval_requests a where a.workflow_id=v_ts.candidate_workflow_id
      order by a.id for update;
    perform 1 from public.candidate_submission_components c where c.workflow_id=v_ts.candidate_workflow_id
      and c.workflow_generation=v_ts.candidate_workflow_generation order by c.id for update;
  end if;
  select w.* into v_workflow from public.candidate_submission_workflows w
    where w.id=v_ts.candidate_workflow_id and w.generation=v_ts.candidate_workflow_generation
      and w.route in('EMAIL','PHONE') and w.state='FINALISED'
      and w.workflow_kind in('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY')
      and w.manager_approved_at_utc is not null;
  if (select count(*) from public.candidate_approval_requests a
    where a.workflow_id=v_workflow.id and a.workflow_generation=v_workflow.generation and a.state='APPROVED')<>1
    or (select count(*) from public.candidate_submission_components c
      where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
        and c.component_kind='HOURS_TIMESHEET' and c.required and c.state='IMMUTABLE')<>1 then
    raise exception 'CORRECTION_MANAGER_SIGNATURE_WITNESS_AMBIGUOUS' using errcode='55000';
  end if;
  select a.* into v_approval from public.candidate_approval_requests a
    where a.workflow_id=v_workflow.id and a.workflow_generation=v_workflow.generation
      and a.state='APPROVED' order by a.id limit 1;
  select c.* into v_hours from public.candidate_submission_components c
    where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
      and c.component_kind='HOURS_TIMESHEET' and c.required and c.state='IMMUTABLE';
  select c.* into v_candidate from public.candidate_submission_components c
    where c.id=v_workflow.candidate_signature_component_id and c.workflow_id=v_workflow.id
      and c.workflow_generation=v_workflow.generation and c.document_role='CANDIDATE_SIGNATURE' and c.state='IMMUTABLE';
  select c.* into v_manager from public.candidate_submission_components c
    where c.id=v_workflow.manager_signature_component_id and c.workflow_id=v_workflow.id
      and c.workflow_generation=v_workflow.generation and c.document_role='MANAGER_SIGNATURE'
      and c.state='IMMUTABLE' and c.approval_request_id=v_approval.id;
  if v_workflow.id is null or v_approval.id is null or v_hours.id is null or v_candidate.id is null or v_manager.id is null
    or nullif(btrim(v_candidate.storage_key),'') is null or nullif(btrim(v_manager.storage_key),'') is null
    or octet_length(v_candidate.source_content_sha256) is distinct from 32
    or octet_length(v_manager.source_content_sha256) is distinct from 32
    or v_hours.review_render_state is distinct from 'READY' or v_hours.final_signed_render_state is distinct from 'READY'
    or nullif(btrim(v_hours.final_signed_storage_key),'') is null
    or octet_length(v_hours.final_signed_content_sha256) is distinct from 32
    or coalesce(v_hours.final_signed_page_count,0)<=0 or coalesce(v_hours.final_signed_byte_size,0)<=0
    or v_hours.review_render_input_sha256 is distinct from v_hours.final_signed_render_input_sha256
    or v_candidate.source_content_sha256 is distinct from v_workflow.candidate_signature_sha256
    or v_manager.source_content_sha256 is distinct from v_workflow.manager_signature_sha256
    or v_ts.r2_nurse_key is distinct from v_candidate.storage_key or v_ts.r2_auth_key is distinct from v_manager.storage_key
    or v_ts.img_sha256_nurse is distinct from encode(v_candidate.source_content_sha256,'hex')
    or v_ts.img_sha256_auth is distinct from encode(v_manager.source_content_sha256,'hex') then
    raise exception 'CORRECTION_MANAGER_SIGNATURE_WITNESS_INVALID' using errcode='55000';
  end if;
  return jsonb_build_object('workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
    'approval_request_id',v_approval.id,'hours_component_id',v_hours.id,
    'candidate_component_id',v_candidate.id,'manager_component_id',v_manager.id,
    'candidate_sha256',encode(v_candidate.source_content_sha256,'hex'),
    'manager_sha256',encode(v_manager.source_content_sha256,'hex'),
    'manager_approved_at_utc',v_workflow.manager_approved_at_utc,
    'document_version_id',v_ts.current_document_version_id);
end;
$function$;
