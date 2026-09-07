-- Replacement authority: printed/QR preparation is offered only after the
-- electronic review pack reaches READY_FOR_MANAGER_APPROVAL.  This target
-- preparation routine must accept that exact submitted state so it can create
-- or reuse the one durable weekly Timesheet.  The separate atomic PAPER
-- adapter remains the only authority that changes the workflow state and
-- invokes the established PAPER transition.

\set ON_ERROR_STOP on

begin;

-- A submitted weekly Candidate workflow can legitimately have no current
-- Timesheet after a prior withdrawal/refusal returned its Contract Week to the
-- editable planned state.  Printed/QR preparation needs a durable Timesheet
-- identity before the existing QR/document authority can build and bind the
-- official pack.  Materialise that one unapproved current record through the
-- existing canonical weekly owner; never create a parallel financial path.

create or replace function public.candidate_weekly_paper_target_prepare_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_context jsonb;
  v_office_context jsonb;
  v_office_actor_user_id uuid;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_week public.contract_weeks%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_input jsonb;
  v_hours_input jsonb;
  v_create jsonb;
  v_patch jsonb;
  v_week_patch jsonb;
  v_snapshot jsonb;
  v_route_authority jsonb;
  v_result jsonb;
  v_timesheet_id uuid;
  v_system_actor uuid;
  v_client_id uuid;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_workflow_id is null or coalesce(p_expected_generation,0)<1 then
    raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';
  end if;

  if p_session_id is null then
    begin
      v_office_context:=nullif(current_setting(
        'cloudtms.office_candidate_context',true
      ),'')::jsonb;
    exception when others then
      v_office_context:=null;
    end;
    v_office_actor_user_id:=nullif(v_office_context->>'actor_user_id','')::uuid;
    if not private._candidate_office_service_context_valid_v1(
      v_environment,v_office_actor_user_id,'PAPER_PREPARE'
    ) then
      raise exception 'CANDIDATE_OFFICE_SERVICE_CONTEXT_INVALID' using errcode='28000';
    end if;
    select workflow.candidate_id into v_candidate_id
    from public.candidate_submission_workflows workflow
    where workflow.id=p_workflow_id and workflow.environment=v_environment;
  else
    v_context:=private._candidate_session_context_v1(
      p_session_id,v_environment,null,p_now_utc,true
    );
    v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  end if;
  if v_candidate_id is null then
    raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
  end if;

  perform pg_advisory_xact_lock(hashtextextended(
    'candidate-weekly-paper-target|'||v_environment||'|'||p_workflow_id::text,
    0
  ));
  select workflow_row.* into v_workflow
  from public.candidate_submission_workflows workflow_row
  where workflow_row.id=p_workflow_id
    and workflow_row.environment=v_environment
    and workflow_row.candidate_id=v_candidate_id
  for update;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.generation is distinct from p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;
  if v_workflow.scope<>'WEEKLY'
     or v_workflow.workflow_kind not in ('CONTRACT_HOURS','CONTRACT_COMBINED')
     or v_workflow.contract_week_id is null
     or v_workflow.contract_id is null
     or v_workflow.week_ending_date is null then
    raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000';
  end if;
  if v_workflow.state not in ('READY_FOR_MANAGER_APPROVAL','WORKER_SUBMITTED','AWAITING_PAPER_RETURN') then
    raise exception 'CANDIDATE_WORKFLOW_TRANSITION_INVALID' using errcode='55000';
  end if;

  select week_row.* into v_week
  from public.contract_weeks week_row
  where week_row.id=v_workflow.contract_week_id
    and week_row.contract_id=v_workflow.contract_id
    and week_row.week_ending_date=v_workflow.week_ending_date
  for update;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_WEEK_NOT_FOUND' using errcode='P0002';
  end if;
  select contract_row.client_id into v_client_id
  from public.contracts contract_row
  where contract_row.id=v_workflow.contract_id
    and contract_row.candidate_id=v_candidate_id;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
  end if;

  v_route_authority:=private._candidate_route_family_v1(
    coalesce(v_workflow.target_timesheet_id,v_week.timesheet_id),v_week.id
  );
  if not coalesce(
    (v_route_authority->>'candidate_paper_submission_allowed')::boolean,false
  ) then
    raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED'
      using errcode='55000',detail=v_route_authority::text;
  end if;

  if v_workflow.target_timesheet_id is not null then
    if v_week.timesheet_id is distinct from v_workflow.target_timesheet_id then
      raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
    end if;
    select timesheet_row.* into v_timesheet
    from public.timesheets timesheet_row
    where timesheet_row.timesheet_id=v_workflow.target_timesheet_id
      and timesheet_row.is_current=true
      and timesheet_row.archived_at_utc is null
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
    end if;
    return jsonb_build_object(
      'ok',true,'created',false,'idempotent_replay',true,
      'workflow_id',v_workflow.id,'generation',v_workflow.generation,
      'timesheet_id',v_timesheet.timesheet_id
    );
  end if;

  v_input:=v_workflow.immutable_submission_json;
  if jsonb_typeof(v_input) is distinct from 'object'
     or private._candidate_sha256_jsonb_v1(v_input)
          is distinct from v_workflow.immutable_submission_sha256 then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_MISMATCH' using errcode='40001';
  end if;
  v_hours_input:=case
    when v_workflow.workflow_kind='CONTRACT_COMBINED'
      then v_input->'hours_submission'
    else v_input
  end;
  if jsonb_typeof(v_hours_input) is distinct from 'object'
     or v_hours_input->>'contract_version'
          is distinct from 'CANDIDATE_WEEKLY_CANONICAL_AUTHORITY_V1' then
    raise exception 'CANDIDATE_CANONICAL_SUBMISSION_REQUIRED' using errcode='22023';
  end if;

  v_create:=v_hours_input->'timesheet_create_json';
  v_patch:=coalesce(v_hours_input->'timesheet_patch_json','{}'::jsonb);
  v_week_patch:=coalesce(v_hours_input->'contract_week_patch_json','{}'::jsonb);
  v_snapshot:=v_hours_input->'canonical_tsfin_snapshot';
  if jsonb_typeof(v_create) is distinct from 'object'
     or jsonb_typeof(v_patch) is distinct from 'object'
     or jsonb_typeof(v_week_patch) is distinct from 'object'
     or jsonb_typeof(v_snapshot) is distinct from 'object'
     or coalesce(v_create->>'timesheet_id','')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
    raise exception 'CANDIDATE_CANONICAL_SUBMISSION_REQUIRED' using errcode='22023';
  end if;
  v_timesheet_id:=(v_create->>'timesheet_id')::uuid;
  if v_create ?| array[
       'authorised_at_server','auth_name','auth_job_title','r2_auth_key',
       'img_sha256_auth','candidate_manager_approved_at_utc'
     ] then
    raise exception 'CANDIDATE_PAPER_TARGET_AUTHORITY_FORBIDDEN' using errcode='22023';
  end if;
  if nullif(v_create->>'contract_id','')::uuid is distinct from v_workflow.contract_id
     or nullif(v_create->>'week_ending_date','')::date is distinct from v_workflow.week_ending_date
     or upper(coalesce(v_create->>'sheet_scope',''))<>'WEEKLY'
     or upper(coalesce(v_create->>'submission_mode',''))<>'MANUAL'
     or upper(coalesce(v_create->>'line_type','')) not in ('HOURS','EXPENSES')
     or nullif(v_snapshot->>'timesheet_id','')::uuid is distinct from v_timesheet_id
     or nullif(v_snapshot->>'candidate_id','')::uuid is distinct from v_candidate_id
     or nullif(v_snapshot->>'client_id','')::uuid is distinct from v_client_id then
    raise exception 'CANDIDATE_CANONICAL_SUBMISSION_REQUIRED' using errcode='22023';
  end if;

  if v_week.timesheet_id is not null then
    if v_week.timesheet_id is distinct from v_timesheet_id then
      raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
    end if;
    select timesheet_row.* into v_timesheet
    from public.timesheets timesheet_row
    where timesheet_row.timesheet_id=v_timesheet_id
      and timesheet_row.is_current=true
      and timesheet_row.archived_at_utc is null
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
    end if;
  else
    select settings_row.candidate_app_system_actor_user_id
    into v_system_actor
    from public.settings_defaults settings_row
    where settings_row.id=1;
    if v_system_actor is null
       or not exists(select 1 from public.tms_users actor where actor.id=v_system_actor) then
      raise exception 'CANDIDATE_SYSTEM_ACTOR_REQUIRED' using errcode='55000';
    end if;

    v_result:=public.contract_week_manual_upsert_atomic(
      p_week_id=>v_week.id,
      p_expected_timesheet_id=>null,
      p_timesheet_create_json=>v_create,
      p_timesheet_patch_json=>v_patch,
      p_contract_week_patch_json=>v_week_patch,
      p_tsfin_snapshot_json=>v_snapshot,
      p_rotation_json=>null,
      p_actor_user_id=>v_system_actor,
      p_materialise_staged_evidence=>false,
      p_now_utc=>p_now_utc,
      p_expected_row_signature=>null,
      p_queue_timesheet_materialisation_json=>jsonb_build_object(
        'suppress_timesheet_evidence_materialisation',true
      )
    );
    if not coalesce((v_result->>'ok')::boolean,false) then
      raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY'
        using errcode='55000',detail=v_result::text;
    end if;
    if coalesce(
         nullif(v_result->>'timesheet_id','')::uuid,
         nullif(v_result#>>'{timesheet,timesheet_id}','')::uuid
       ) is distinct from v_timesheet_id then
      raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
    end if;
    select timesheet_row.* into v_timesheet
    from public.timesheets timesheet_row
    where timesheet_row.timesheet_id=v_timesheet_id
      and timesheet_row.is_current=true
      and timesheet_row.archived_at_utc is null
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
    end if;
  end if;

  if v_timesheet.authorised_at_server is not null then
    raise exception 'CANDIDATE_PAPER_TARGET_AUTHORITY_FORBIDDEN' using errcode='55000';
  end if;
  if v_timesheet.contract_id is distinct from v_workflow.contract_id
     or v_timesheet.week_ending_date is distinct from v_workflow.week_ending_date
     or v_timesheet.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum then
    raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
  end if;
  select financial_row.* into v_fin
  from public.timesheets_financials financial_row
  where financial_row.timesheet_id=v_timesheet.timesheet_id
    and financial_row.is_current=true
  order by financial_row.updated_at desc,financial_row.id desc
  limit 1
  for update;
  if not found
     or v_fin.authorised_at_utc is not null
     or v_fin.paid_at_utc is not null
     or v_fin.locked_by_invoice_id is not null then
    raise exception 'CANDIDATE_PAPER_TARGET_AUTHORITY_FORBIDDEN' using errcode='55000';
  end if;

  update public.candidate_submission_workflows set
    target_timesheet_id=v_timesheet.timesheet_id,
    updated_at_utc=p_now_utc
  where id=v_workflow.id
    and generation=v_workflow.generation
    and target_timesheet_id is null;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
  end if;
  update public.candidate_submission_components set
    timesheet_id=v_timesheet.timesheet_id
  where workflow_id=v_workflow.id
    and workflow_generation=v_workflow.generation
    and timesheet_id is null
    and state<>'SUPERSEDED';

  perform private._candidate_audit_v1(
    'candidate_submission_workflow',v_workflow.id::text,
    'CANDIDATE_WEEKLY_PAPER_TARGET_PREPARED',
    jsonb_build_object('target_timesheet_id',null,'contract_week_id',v_week.id),
    jsonb_build_object('target_timesheet_id',v_timesheet.timesheet_id,'contract_week_id',v_week.id),
    null,v_system_actor,null,p_now_utc
  );
  return jsonb_build_object(
    'ok',true,'created',v_result is not null,'idempotent_replay',false,
    'workflow_id',v_workflow.id,'generation',v_workflow.generation,
    'timesheet_id',v_timesheet.timesheet_id
  );
end;
$function$;

revoke all on function public.candidate_weekly_paper_target_prepare_v1(
  uuid,text,uuid,integer,timestamptz
) from public,anon,authenticated;
grant execute on function public.candidate_weekly_paper_target_prepare_v1(
  uuid,text,uuid,integer,timestamptz
) to service_role;

commit;
