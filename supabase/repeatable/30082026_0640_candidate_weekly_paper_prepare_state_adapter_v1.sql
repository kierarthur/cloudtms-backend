-- The Candidate UI can offer Printed Documents after the electronic review
-- document has reached READY_FOR_MANAGER_APPROVAL.  The established PAPER
-- transition was originally written for the earlier WORKER_SUBMITTED state.
-- Move that one exact, already-submitted weekly state back to the PAPER entry
-- state and invoke the existing authoritative transition in the same database
-- transaction.  Any failure therefore rolls the state change back.

create or replace function public.candidate_weekly_paper_prepare_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_action text,
  p_expected_generation integer,
  p_payload jsonb default '{}'::jsonb,
  p_idempotency_key text default null,
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
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_timesheet_id uuid;
  v_family_key text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if upper(btrim(coalesce(p_action,'')))<>'PAPER_PREPARE'
     or p_workflow_id is null
     or coalesce(p_expected_generation,0)<1
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';
  end if;

  v_context:=private._candidate_session_context_v1(
    p_session_id,v_environment,null,p_now_utc,true
  );
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then
    raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
  end if;

  select workflow_row.* into v_workflow
  from public.candidate_submission_workflows workflow_row
  where workflow_row.id=p_workflow_id
    and workflow_row.environment=v_environment
    and workflow_row.candidate_id=v_candidate_id;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.generation is distinct from p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;
  if v_workflow.scope<>'WEEKLY'
     or v_workflow.workflow_kind not in (
       'CONTRACT_HOURS','CONTRACT_COMBINED','CONTRACT_EXPENSE'
     ) then
    raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000';
  end if;
  v_timesheet_id:=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id);
  if v_timesheet_id is null then
    raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
  end if;

  -- Match the canonical PAPER lock order: family advisory lock, current
  -- Timesheet, then workflow.  The unlocked identity is rechecked after both
  -- rows are held so concurrent withdrawal/replacement cannot cross this gate.
  v_family_key:='CANDIDATE_PAPER_FAMILY:'||v_workflow.environment||':'
    ||coalesce(v_workflow.contract_id::text,'-')||':'
    ||coalesce(v_workflow.week_ending_date::text,v_workflow.work_date::text,'-');
  perform pg_advisory_xact_lock(hashtextextended(v_family_key,0));
  select timesheet_row.* into v_timesheet
  from public.timesheets timesheet_row
  where timesheet_row.timesheet_id=v_timesheet_id
    and timesheet_row.is_current=true
    and timesheet_row.archived_at_utc is null
  for update;
  if not found then
    raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
  end if;
  select workflow_row.* into v_workflow
  from public.candidate_submission_workflows workflow_row
  where workflow_row.id=p_workflow_id
    and workflow_row.environment=v_environment
    and workflow_row.candidate_id=v_candidate_id
  for update;
  if not found
     or coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
          is distinct from v_timesheet_id then
    raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
  end if;
  if v_workflow.generation is distinct from p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;
  if v_timesheet.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
     or v_timesheet.submission_mode<>'MANUAL'::public.submission_mode_enum
     or v_timesheet.authorised_at_server is not null
     or v_timesheet.qr_scanned_at is not null
     or nullif(btrim(coalesce(v_timesheet.qr_signed_hash,'')),'') is not null
     or v_timesheet.qr_signed_at_utc is not null
     or upper(coalesce(v_timesheet.qr_status::text,'')) not in ('','PENDING') then
    raise exception 'CANDIDATE_PAPER_TARGET_AUTHORITY_FORBIDDEN' using errcode='55000';
  end if;

  update public.timesheets
  set qr_status='PENDING'::public.timesheet_qr_status_enum,
      updated_at=p_now_utc
  where timesheet_id=v_timesheet_id
    and is_current=true;

  if v_workflow.state='READY_FOR_MANAGER_APPROVAL' then
    update public.candidate_submission_workflows
    set state='WORKER_SUBMITTED',updated_at_utc=p_now_utc
    where id=v_workflow.id
      and generation=v_workflow.generation
      and state='READY_FOR_MANAGER_APPROVAL';
    if not found then
      raise exception 'CANDIDATE_WORKFLOW_TRANSITION_INVALID' using errcode='55000';
    end if;
  elsif v_workflow.state not in ('WORKER_SUBMITTED','AWAITING_PAPER_RETURN') then
    raise exception 'CANDIDATE_WORKFLOW_TRANSITION_INVALID' using errcode='55000';
  end if;

  return public.candidate_workflow_transition_atomic_v1(
    p_session_id=>p_session_id,
    p_environment=>v_environment,
    p_workflow_id=>p_workflow_id,
    p_action=>'PAPER_PREPARE',
    p_expected_generation=>p_expected_generation,
    p_payload=>coalesce(p_payload,'{}'::jsonb),
    p_idempotency_key=>p_idempotency_key,
    p_now_utc=>p_now_utc
  );
end;
$function$;

revoke all on function public.candidate_weekly_paper_prepare_atomic_v1(
  uuid,text,uuid,text,integer,jsonb,text,timestamptz
) from public,anon,authenticated;
grant execute on function public.candidate_weekly_paper_prepare_atomic_v1(
  uuid,text,uuid,text,integer,jsonb,text,timestamptz
) to service_role;
