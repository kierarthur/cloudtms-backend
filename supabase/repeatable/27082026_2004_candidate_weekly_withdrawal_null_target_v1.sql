-- A planned weekly submission can reach manager refusal without a canonical
-- Timesheet row. The immutable workflow/review components remain the audit
-- history, while the contract_week is the editable business record. Permit
-- that exact null-target state to reopen the week without weakening the
-- existing current-Timesheet, finance and invoice protection gates.

-- The installed TEST catalogue exposed two older Candidate definitions after
-- later edits to the legacy 0708 closure re-ran out of source order. Reassert
-- the already-reviewed later authorities in this final workflow closure so a
-- clean rebuild and an incremental UPGRADE finish with the same definitions.
-- This also keeps the no-work and manager-refusal routes on their intended
-- electronic resubmission semantics before the withdrawal correction below.
\ir 26082026_0659_candidate_no_work_weekly_chain_v1.sql
\ir 27082026_0423_candidate_electronic_rejection_resubmission_v1.sql

create or replace function private._candidate_weekly_withdrawal_reset_v1(
  p_workflow_id uuid,
  p_reason text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_source public.timesheets%rowtype;
  v_current public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_week public.contract_weeks%rowtype;
  v_current_count integer:=0;
  v_current_timesheet_id uuid;
  v_source_timesheet_id uuid;
  v_has_source boolean:=false;
  v_system_actor_user_id uuid;
  v_weekly_source_guard jsonb;
begin
  if p_workflow_id is null or nullif(btrim(coalesce(p_reason,'')),'') is null then
    raise exception 'CANDIDATE_WITHDRAWAL_RESET_INVALID' using errcode='22023';
  end if;

  select workflow_row.* into v_workflow
  from public.candidate_submission_workflows workflow_row
  where workflow_row.id=p_workflow_id
  for update;
  if not found
     or v_workflow.scope<>'WEEKLY'
     or v_workflow.workflow_kind not in ('CONTRACT_HOURS','CONTRACT_COMBINED')
     or v_workflow.contract_week_id is null then
    raise exception 'CANDIDATE_WITHDRAWAL_SCOPE_UNSUPPORTED' using errcode='55000';
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

  v_source_timesheet_id:=coalesce(
    v_workflow.target_timesheet_id,
    v_workflow.anchor_timesheet_id,
    v_week.timesheet_id
  );
  if v_source_timesheet_id is not null then
    select source_row.* into v_source
    from public.timesheets source_row
    where source_row.timesheet_id=v_source_timesheet_id;
    if not found then
      raise exception 'CANDIDATE_WORKFLOW_TARGET_NOT_FOUND' using errcode='P0002';
    end if;
    v_has_source:=true;
  end if;

  if v_has_source then
    select count(distinct current_row.timesheet_id)::integer,
      min(current_row.timesheet_id::text)::uuid
    into v_current_count,v_current_timesheet_id
    from public.timesheets current_row
    where current_row.is_current=true
      and current_row.archived_at_utc is null
      and current_row.contract_id is not distinct from v_source.contract_id
      and current_row.week_ending_date is not distinct from v_source.week_ending_date
      and (
        (
          nullif(btrim(coalesce(v_source.booking_id,'')),'') is not null
          and nullif(btrim(coalesce(current_row.booking_id,'')),'')
            =nullif(btrim(v_source.booking_id),'')
        )
        or (
          nullif(btrim(coalesce(v_source.booking_id,'')),'') is null
          and current_row.timesheet_id=v_source.timesheet_id
        )
      );
    if v_current_count<>1 or v_current_timesheet_id is null then
      raise exception 'CANDIDATE_WORKFLOW_TARGET_NOT_CURRENT' using errcode='40001';
    end if;

    select current_row.* into v_current
    from public.timesheets current_row
    where current_row.timesheet_id=v_current_timesheet_id
    for update;
    if v_week.timesheet_id is distinct from v_current.timesheet_id then
      raise exception 'CANDIDATE_WORKFLOW_WEEK_NOT_FOUND' using errcode='P0002';
    end if;

    select financial_row.* into v_fin
    from public.timesheets_financials financial_row
    where financial_row.timesheet_id=v_current.timesheet_id
      and financial_row.is_current=true
    order by financial_row.updated_at desc,financial_row.id desc
    limit 1
    for update;
    if v_current.authorised_at_server is not null
       or v_fin.authorised_at_utc is not null
       or v_fin.paid_at_utc is not null
       or v_fin.locked_by_invoice_id is not null then
      raise exception 'CANDIDATE_WITHDRAWAL_PROTECTED_HISTORY' using errcode='55000';
    end if;

    -- Plan 6.2 G6-11 (proof/34 section 3, entry point E10): refuse for an
    -- authorised Weekly-Source-managed root, before this owner's first write
    -- and while it holds the workflow, contract-week, current timesheet and
    -- TSFIN row locks taken above.  An unmanaged family is unaffected.
    v_weekly_source_guard:=private.weekly_source_managed_root_guard_v1(
      v_current.timesheet_id
    );
    -- HANDOVER 2 round-5 ruling B3 and A4 (18 September 2026).  The refusal is
    -- NARROWED: it applies to a Weekly-Source managed root, to a bound or
    -- protected family whose identity cannot be resolved, to a family carrying
    -- protected pay evidence but no authorisation row
    -- (PROTECTED_ROOT_AUTHORITY_MISSING), and to the live-record-on-an-
    -- unauthorised-Timesheet contradiction, which must never be allowed to
    -- continue merely because managed is false.  An unrelated, UNBOUND ordinary
    -- family -- including a malformed one -- keeps exactly the behaviour it had
    -- before this feature was installed.  Absent, null and non-boolean take the
    -- unsafe value at every read (Part 1 addendum rule 4).
    if (coalesce((v_weekly_source_guard->>'managed')::boolean, true)
         and (coalesce((v_weekly_source_guard->>'ok')::boolean, true)
              or coalesce((v_weekly_source_guard->>'weekly_source_bound')::boolean, true)))
       or coalesce((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
       or (v_weekly_source_guard->>'protected_target_ownership_state') is not null then
      raise exception 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
        using errcode='55000',detail=jsonb_build_object(
          'code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
          'entry_point','E10:private._candidate_weekly_withdrawal_reset_v1',
          'block_reason','WEEKLY_SOURCE_MANAGED_ROOT',
          'refusal_basis', case
            -- HANDOVER 2 round-5 Part E: the trim-equivalent split family is a
            -- canonical booking-reference collision and must be named as one.
            -- WP-03 handoff N20: accept BOTH the installed token and the ruled name for one
            -- release, so the order of this edit and WP-03's rename cannot open a gap in
            -- which Office stops seeing the ruled name.
            when v_weekly_source_guard->>'reason' in (
                   'FAMILY_SPLIT_BY_WHITESPACE','BOOKING_REFERENCE_CANONICAL_COLLISION')
              then 'BOOKING_REFERENCE_CANONICAL_COLLISION'
            when coalesce((v_weekly_source_guard->>'managed')::boolean, true) and coalesce((v_weekly_source_guard->>'ok')::boolean, true)
              then 'WEEKLY_SOURCE_MANAGED_ROOT'
            when coalesce((v_weekly_source_guard->>'managed')::boolean, true)
              then 'WEEKLY_SOURCE_BOUND_OR_PROTECTED_ROOT_UNRESOLVABLE'
            when coalesce((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
              then 'AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET'
            else 'PROTECTED_ROOT_AUTHORITY_MISSING' end,
          'integrity_failure', not coalesce((v_weekly_source_guard->>'ok')::boolean,false),
          'timesheet_id',v_current.timesheet_id,
          'reason',v_weekly_source_guard->>'reason'
        )::text;
    end if;

    update public.timesheets set
      is_current=false,
      status='REVOKED',
      revoked_reason=btrim(p_reason),
      revoked_by='CANDIDATE',
      updated_at=p_now_utc
    where timesheet_id=v_current.timesheet_id and is_current=true;

    update public.timesheet_evidence set processing_state='SUPERSEDED'
    where timesheet_id=v_current.timesheet_id and processing_state<>'SUPERSEDED';
  elsif v_week.timesheet_id is not null then
    -- A non-null week pointer must always resolve to a real Timesheet. Never
    -- silently reinterpret a dangling pointer as the valid planned-week case.
    raise exception 'CANDIDATE_WORKFLOW_TARGET_NOT_FOUND' using errcode='P0002';
  end if;

  update public.contract_weeks set
    timesheet_id=null,
    status='OPEN',
    day_entries_json='[]'::jsonb,
    totals_json='{}'::jsonb,
    updated_at=p_now_utc
  where id=v_week.id
    and timesheet_id is not distinct from (
      case when v_has_source then v_current.timesheet_id else null end
    );
  if not found then
    raise exception 'CANDIDATE_WITHDRAWAL_WEEK_MOVED' using errcode='40001';
  end if;

  select settings_row.candidate_app_system_actor_user_id
  into v_system_actor_user_id
  from public.settings_defaults settings_row
  where settings_row.id=1;
  if v_system_actor_user_id is null
     or not exists(
       select 1 from public.tms_users actor_row
       where actor_row.id=v_system_actor_user_id
     ) then
    raise exception 'CANDIDATE_SYSTEM_ACTOR_REQUIRED' using errcode='55000';
  end if;

  perform private._candidate_audit_v1(
    case when v_has_source then 'timesheet' else 'contract_week' end,
    case when v_has_source then v_current.timesheet_id::text else v_week.id::text end,
    'CANDIDATE_SUBMISSION_WITHDRAWN_TO_CONTRACT_WEEK',
    jsonb_build_object(
      'old_timesheet_id',case when v_has_source then v_current.timesheet_id else null end,
      'old_version',case when v_has_source then v_current.version else null end,
      'workflow_id',v_workflow.id,
      'previous_submission_mode',case when v_has_source then v_current.submission_mode else null end
    ),
    jsonb_build_object(
      'new_timesheet_id',null,
      'contract_week_id',v_week.id,
      'draft_submission_mode',null,
      'effective_submission_mode',v_week.submission_mode_snapshot
    ),
    btrim(p_reason),v_system_actor_user_id,null,p_now_utc
  );
  return jsonb_build_object(
    'reset',true,
    'old_timesheet_id',case when v_has_source then v_current.timesheet_id else null end,
    'current_timesheet_id',null,
    'contract_week_id',v_week.id,
    'timesheet_version',null,
    'draft_submission_mode',null,
    'effective_submission_mode',v_week.submission_mode_snapshot
  );
end;
$function$;

revoke all on function private._candidate_weekly_withdrawal_reset_v1(
  uuid,text,timestamptz
) from public,anon,authenticated;
grant execute on function private._candidate_weekly_withdrawal_reset_v1(
  uuid,text,timestamptz
) to service_role;
