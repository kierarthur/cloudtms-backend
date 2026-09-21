begin;

-- The browser chooses only an Office action and, for approve/amend, factual
-- shift times.  This owner opens every later protected-hours action against
-- the already-created Candidate + Contract + week family.  It accepts no pay,
-- charge, rate, residual, C1, Draft, invoice or Banking facts.
create or replace function public.weekly_exceptional_pay_prepare_action_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','family_id','source_cycle_id','work_event_id',
    'action','expected_family_bound_version','protected_schedule','reason',
    'idempotency_key'
  ];
  v_schedule_keys constant text[]:=array[
    'work_date','start_at_local','end_at_local','break_minutes'
  ];
  v_actor uuid;
  v_family_id uuid;
  v_cycle_id uuid;
  v_event_id uuid;
  v_expected_version bigint;
  v_action text;
  v_reason text;
  v_key text;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_contract public.contracts%rowtype;
  v_event public.weekly_work_events%rowtype;
  v_latest_event public.weekly_exceptional_pay_family_events%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_request_hash bytea;
  v_before bytea;
  v_source_mode text;
  v_policy jsonb;
  v_schedule jsonb;
  v_work_date date;
  v_start timestamp without time zone;
  v_end timestamp without time zone;
  v_break integer;
  v_replay boolean:=false;
begin
  if pg_catalog.jsonb_typeof(p_request)<>'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_ACTION_PREPARE_V1' then
    raise exception 'WEEKLY_PROTECTED_ACTION_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_family_id:=(p_request->>'family_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_event_id:=(p_request->>'work_event_id')::uuid;
    v_expected_version:=(p_request->>'expected_family_bound_version')::bigint;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_ACTION_REQUEST_INVALID' using errcode='22023';
  end;
  v_action:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'action','')));
  v_reason:=pg_catalog.btrim(coalesce(p_request->>'reason',''));
  v_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  v_request_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_PREPARE_ACTION_REQUEST_V1',p_request
  );
  if v_action not in ('AMEND','WITHDRAW','WAIT','RECONCILE','RECORD_NOT_WORKED')
     or v_expected_version<1
     or pg_catalog.char_length(v_reason) not between 1 and 1000
     or pg_catalog.char_length(v_key) not between 16 and 200 then
    raise exception 'WEEKLY_PROTECTED_ACTION_REQUEST_INVALID' using errcode='22023';
  end if;

  -- Share the same global idempotency lock as first approval.  Concurrent
  -- first attempts for one key therefore become one write plus one exact
  -- replay instead of leaking the ledger's unique constraint.
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'weekly-exceptional-orchestration|'||v_key,0
  ));

  select run.* into v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.idempotency_key=v_key for update;
  v_replay:=found;

  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_family_id for update;
  select cycle.* into strict v_cycle
  from public.weekly_source_cycles cycle where cycle.id=v_cycle_id for share;
  select source_group.* into strict v_group
  from public.weekly_source_groups source_group
  where source_group.id=v_cycle.source_group_id and source_group.active for share;
  select contract.* into strict v_contract
  from public.contracts contract where contract.id=v_family.contract_id for share;
  select work_event.* into strict v_event
  from public.weekly_work_events work_event
  where work_event.id=v_event_id
    and work_event.candidate_id=v_family.candidate_id
    and work_event.client_id=v_contract.client_id
    and work_event.work_date between v_family.week_start_date and v_family.week_ending_date
  for share;
  select family_event.* into strict v_latest_event
  from public.weekly_exceptional_pay_family_events family_event
  where family_event.family_id=v_family.id
    and family_event.durable_work_event_id=v_event.id
  order by family_event.event_sequence desc
  limit 1
  for share;

  if v_action='AMEND' then
    if pg_catalog.jsonb_typeof(p_request->'protected_schedule')<>'object'
       or not private.weekly_exceptional_json_keys_exact_v1(
         p_request->'protected_schedule',v_schedule_keys
       )
       or not ((p_request->'protected_schedule') ?& v_schedule_keys) then
      raise exception 'WEEKLY_PROTECTED_ACTION_SCHEDULE_INVALID' using errcode='22023';
    end if;
    v_schedule:=p_request->'protected_schedule';
    begin
      v_work_date:=(v_schedule->>'work_date')::date;
      v_start:=(v_schedule->>'start_at_local')::timestamp without time zone;
      v_end:=(v_schedule->>'end_at_local')::timestamp without time zone;
      v_break:=(v_schedule->>'break_minutes')::integer;
    exception when others then
      raise exception 'WEEKLY_PROTECTED_ACTION_SCHEDULE_INVALID' using errcode='22023';
    end;
    if v_work_date<>v_event.work_date or v_start::date<>v_work_date
       or v_end<=v_start or v_break<0
       or v_break>=extract(epoch from (v_end-v_start))/60 then
      raise exception 'WEEKLY_PROTECTED_ACTION_SCHEDULE_INVALID' using errcode='22023';
    end if;
  else
    if p_request->'protected_schedule'<>'null'::jsonb then
      raise exception 'WEEKLY_PROTECTED_ACTION_SCHEDULE_FORBIDDEN' using errcode='22023';
    end if;
    v_schedule:=pg_catalog.jsonb_build_object(
      'work_date',v_latest_event.work_date,
      'start_at_local',v_latest_event.start_at_local,
      'end_at_local',v_latest_event.end_at_local,
      'break_minutes',v_latest_event.break_minutes
    );
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'APPROVE_PROTECTED_PAY',v_group.id,v_contract.client_id,v_event.work_date
  );
  v_policy:=private._weekly_source_effective_policy_v1(
    v_contract.client_id,v_contract.id,v_event.work_date
  );
  v_source_mode:=v_policy->>'c1_source_mode';
  if v_family.agency_id is distinct from v_group.agency_id
     or v_family.root_timesheet_id is null
     or v_family.ownership_state<>'TARGET_MANAGED'
     or v_contract.candidate_id is distinct from v_family.candidate_id
     or not exists(
       select 1 from public.weekly_source_group_clients membership
       where membership.source_group_id=v_group.id
         and membership.client_id=v_contract.client_id
         and v_event.work_date between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)
     )
     or v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
     or coalesce((v_policy->>'self_bill_enabled')::boolean,false) is not true
     or v_source_mode not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
     or (v_group.source_family='NHSP') is distinct from (v_source_mode='NHSP_WEEKLY')
     or v_latest_event.id is null then
    raise exception 'WEEKLY_PROTECTED_ACTION_SCOPE_INVALID' using errcode='55000';
  end if;

  if v_replay then
    if v_run.family_id is distinct from v_family.id
       or v_run.request_kind is distinct from v_action
       or v_run.requested_by_user_id is distinct from v_actor
       or v_run.request_fingerprint is distinct from v_request_hash then
      raise exception 'WEEKLY_PROTECTED_ACTION_IDEMPOTENCY_COLLISION' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'outcome','PREPARED','family_id',v_family.id,
      'orchestration_run_id',v_run.id,'source_cycle_id',v_cycle.id,
      'source_group_id',v_group.id,'agency_id',v_group.agency_id,
      'candidate_id',v_family.candidate_id,'client_id',v_contract.client_id,
      'contract_id',v_family.contract_id,'root_timesheet_id',v_family.root_timesheet_id,
      'work_event_id',v_event.id,'week_start_date',v_family.week_start_date,
      'week_ending_date',v_family.week_ending_date,'source_mode',v_source_mode,
      'family_bound_version',v_expected_version,'request_kind',v_action,
      'protected_schedule',v_schedule,'run_state',v_run.state,
      'idempotent_replay',true
    );
  end if;

  if v_family.current_generation_id is null
     or v_family.current_lifecycle_state not in (
       'PROTECTED','WAITING_SOURCE','READY_TO_RECONCILE','RECONCILED','NOT_WORKED','ACTION_REQUIRED'
     )
     or v_family.c1_publication_state in ('PENDING','PUBLISHING')
     or v_family.bound_version<>v_expected_version then
    raise exception 'WEEKLY_PROTECTED_ACTION_STALE' using errcode='40001';
  end if;

  v_before:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_ACTION_PREPARE_BEFORE_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'family_bound_version',v_family.bound_version,
      'current_generation_id',v_family.current_generation_id,
      'current_generation_number',v_family.current_generation_number,
      'current_target_vector_hash',pg_catalog.encode(v_family.current_complete_target_vector_hash,'hex'),
      'source_cycle_id',v_cycle.id,'source_group_id',v_group.id,
      'work_event_id',v_event.id,'action',v_action,'actor_user_id',v_actor,
      'protected_schedule',v_schedule,'reason',v_reason
    )
  );
  insert into public.weekly_exceptional_orchestration_runs(
    family_id,request_kind,idempotency_key,requested_by_user_id,state,
    request_fingerprint,before_state_fingerprint
  ) values (
    v_family.id,v_action,v_key,v_actor,'RUNNING',v_request_hash,v_before
  ) returning * into v_run;
  return pg_catalog.jsonb_build_object(
    'ok',true,'outcome','PREPARED','family_id',v_family.id,
    'orchestration_run_id',v_run.id,'source_cycle_id',v_cycle.id,
    'source_group_id',v_group.id,'agency_id',v_group.agency_id,
    'candidate_id',v_family.candidate_id,'client_id',v_contract.client_id,
    'contract_id',v_family.contract_id,'root_timesheet_id',v_family.root_timesheet_id,
    'work_event_id',v_event.id,'week_start_date',v_family.week_start_date,
    'week_ending_date',v_family.week_ending_date,'source_mode',v_source_mode,
    'family_bound_version',v_family.bound_version,'request_kind',v_action,
    'protected_schedule',v_schedule,'run_state',v_run.state,
    'idempotent_replay',v_replay
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_ACTION_SCOPE_INVALID' using errcode='55000';
end;
$function$;

-- Return the bounded server authority required to compose one complete target.
-- Every money/rate/provider/source fact is loaded here after the Office action
-- has been authorised; none is accepted from the browser request.
create or replace function public.weekly_exceptional_pay_action_context_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','family_id','orchestration_run_id',
    'source_cycle_id','work_event_id','protected_schedule','evidence_timesheet_id'
  ];
  v_schedule_keys constant text[]:=array[
    'work_date','start_at_local','end_at_local','break_minutes'
  ];
  v_actor uuid;
  v_family_id uuid;
  v_run_id uuid;
  v_cycle_id uuid;
  v_event_id uuid;
  v_evidence_id uuid;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_contract public.contracts%rowtype;
  v_contract_week public.contract_weeks%rowtype;
  v_root public.timesheets%rowtype;
  -- WP-30 (WP-27 sweep finding N4), standing rule 3.  The expense-authority
  -- selection below decides which SOURCE_EXPENSE authorities belong to this
  -- protected root through the source-row lineage binding, which is a fact
  -- about the Timesheet FAMILY.  Keyed on the physical root id a rotated
  -- family's authorities were SILENTLY DROPPED from the projection rather than
  -- refused — fail open by omission.  EXECUTED on a rotated family: 0
  -- authorities selected by the physical id against 4 by the family.
  v_root_family uuid[];
  v_fin public.timesheets_financials%rowtype;
  v_event public.weekly_work_events%rowtype;
  v_schedule jsonb;
  v_work_date date;
  v_start timestamp without time zone;
  v_end timestamp without time zone;
  v_break integer;
  v_policy jsonb;
  v_settings jsonb;
  v_source_mode text;
  v_source_segments jsonb:='[]'::jsonb;
  v_client_sources jsonb:='[]'::jsonb;
  v_decisions jsonb:='[]'::jsonb;
  v_source_expenses jsonb:='[]'::jsonb;
  v_current_final uuid;
  v_comparison uuid;
  v_signed jsonb;
  v_candidate_submission jsonb;
  v_selected_source_present boolean:=false;
  v_selected_source_minutes integer:=0;
  v_selected_source_revision text;
  v_selected_source_hash text;
  v_source_head_revision uuid;
  v_source_head_count integer:=0;
  v_ambiguous_event_count integer:=0;
  v_fin_hash text;
  v_provider_hash text;
  v_rate_refs jsonb;
begin
  if pg_catalog.jsonb_typeof(p_request)<>'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_ACTION_CONTEXT_V1'
     or pg_catalog.jsonb_typeof(p_request->'protected_schedule')<>'object'
     or not private.weekly_exceptional_json_keys_exact_v1(
       p_request->'protected_schedule',v_schedule_keys
     )
     or not ((p_request->'protected_schedule') ?& v_schedule_keys) then
    raise exception 'WEEKLY_PROTECTED_ACTION_CONTEXT_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_family_id:=(p_request->>'family_id')::uuid;
    v_run_id:=(p_request->>'orchestration_run_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_event_id:=(p_request->>'work_event_id')::uuid;
    v_evidence_id:=nullif(p_request->>'evidence_timesheet_id','')::uuid;
    v_schedule:=p_request->'protected_schedule';
    v_work_date:=(v_schedule->>'work_date')::date;
    v_start:=(v_schedule->>'start_at_local')::timestamp without time zone;
    v_end:=(v_schedule->>'end_at_local')::timestamp without time zone;
    v_break:=(v_schedule->>'break_minutes')::integer;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_ACTION_CONTEXT_INVALID' using errcode='22023';
  end;
  if v_end<=v_start or v_start::date<>v_work_date or v_break<0
     or v_break>=extract(epoch from (v_end-v_start))/60 then
    raise exception 'WEEKLY_PROTECTED_ACTION_CONTEXT_INVALID' using errcode='22023';
  end if;

  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_family_id for share;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_run_id and run.family_id=v_family.id for share;
  select cycle.* into strict v_cycle
  from public.weekly_source_cycles cycle where cycle.id=v_cycle_id for share;
  select source_group.* into strict v_group
  from public.weekly_source_groups source_group
  where source_group.id=v_cycle.source_group_id and source_group.active for share;
  select contract.* into strict v_contract
  from public.contracts contract where contract.id=v_family.contract_id for share;
  select contract_week.* into strict v_contract_week
  from public.contract_weeks contract_week
  where contract_week.contract_id=v_family.contract_id
    and contract_week.week_ending_date=v_family.week_ending_date
    and contract_week.additional_seq=0 for share;
  -- WP-30, standing rule 3.  The family is resolved and its rows are locked
  -- BEFORE the family-scoped state below is read, in the SAME mode and the same
  -- timesheet_id order the installed code here already uses (`for share`), and
  -- before the root's own `for share` so no new lock ordering is introduced.
  v_root_family:=private.weekly_source_invoice_family_timesheet_ids_v1(
    v_family.root_timesheet_id
  );
  -- Standing rule 3's fail-closed branch: an explicit cardinality test, never a
  -- `limit`.  This owner already refuses on every other unprovable scope fact.
  if v_root_family is null or pg_catalog.cardinality(v_root_family)=0 then
    raise exception 'WEEKLY_PROTECTED_ACTION_CONTEXT_FAMILY_UNRESOLVED'
      using errcode='55000';
  end if;
  perform 1 from public.timesheets lock_row
  where lock_row.timesheet_id=any(v_root_family)
  order by lock_row.timesheet_id for share;
  select timesheet.* into strict v_root
  from public.timesheets timesheet
  where timesheet.timesheet_id=v_family.root_timesheet_id for share;
  select financial.* into v_fin
  from public.timesheets_financials financial
  where financial.timesheet_id=v_root.timesheet_id and financial.is_current
  order by financial.computed_at_utc desc nulls last,
           financial.updated_at desc nulls last,financial.id desc limit 1;
  select work_event.* into strict v_event
  from public.weekly_work_events work_event
  where work_event.id=v_event_id
    and work_event.candidate_id=v_family.candidate_id
    and work_event.client_id=v_contract.client_id for share;

  perform private.weekly_source_office_authority_v1(
    v_actor,'APPROVE_PROTECTED_PAY',v_group.id,v_contract.client_id,v_event.work_date
  );
  v_policy:=private._weekly_source_effective_policy_v1(
    v_contract.client_id,v_contract.id,v_event.work_date
  );
  v_settings:=(private._timesheet_settings_authority_frozen_v1(v_root.timesheet_id)->'values')
    -'resolved_at_utc';
  v_source_mode:=v_policy->>'c1_source_mode';
  if v_run.requested_by_user_id is distinct from v_actor
     or v_run.state not in ('RUNNING','COMPLETE')
     or v_run.request_kind not in ('APPROVE','AMEND','WITHDRAW','WAIT','RECONCILE','RECORD_NOT_WORKED')
     or v_family.agency_id is distinct from v_group.agency_id
     or v_contract.candidate_id is distinct from v_family.candidate_id
     or v_contract_week.timesheet_id is distinct from v_root.timesheet_id
     or v_root.contract_id is distinct from v_contract.id
     or v_root.week_ending_date is distinct from v_family.week_ending_date
     or not v_root.is_current or v_root.is_adjustment
     or v_root.revoked_at is not null or v_root.archived_at_utc is not null
     or v_event.work_date<>v_work_date
     or v_work_date not between v_family.week_start_date and v_family.week_ending_date
     or v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
     or coalesce((v_policy->>'self_bill_enabled')::boolean,false) is not true
     or v_source_mode not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
     or (v_group.source_family='NHSP') is distinct from (v_source_mode='NHSP_WEEKLY') then
    raise exception 'WEEKLY_PROTECTED_ACTION_CONTEXT_SCOPE_INVALID' using errcode='55000';
  end if;

  if v_source_mode='HEALTHROSTER_WEEKLY' then
    with ranked as (
      select transition.*,source_cycle.finalisation_week_ending,
        revision.finalised_at_utc,revision.revision_number,
        coalesce(current_line.contract_id,prior_line.contract_id) as resolved_contract_id,
        coalesce(current_line.candidate_id,prior_line.candidate_id) as resolved_candidate_id,
        coalesce(current_line.work_date,prior_line.work_date) as resolved_work_date,
        pg_catalog.row_number() over (
          partition by transition.work_event_id
          order by source_cycle.finalisation_week_ending desc,
                   revision.finalised_at_utc desc,revision.revision_number desc,
                   transition.created_at_utc desc,transition.id desc
        ) as event_rank
      from public.weekly_source_state_transitions transition
      join public.weekly_source_final_revisions revision
        on revision.id=transition.final_revision_id and revision.state='CURRENT'
      join public.weekly_source_cycles source_cycle on source_cycle.id=revision.source_cycle_id
      left join public.weekly_source_final_snapshot_lines current_line
        on current_line.id=transition.new_snapshot_line_id
      left join public.weekly_source_final_snapshot_lines prior_line
        on prior_line.id=transition.previous_snapshot_line_id
      where transition.source_profile_kind in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS')
        and source_cycle.source_group_id=v_group.id
    ), current_state as (
      select ranked.*,snapshot.start_at_local,snapshot.end_at_local,
             snapshot.break_minutes,snapshot.actual_net_minutes,
             snapshot.snapshot_line_hash
      from ranked
      left join public.weekly_source_final_snapshot_lines snapshot
        on snapshot.id=ranked.new_snapshot_line_id
      where ranked.event_rank=1
        and ranked.resolved_contract_id=v_family.contract_id
        and ranked.resolved_candidate_id=v_family.candidate_id
        and ranked.resolved_work_date between v_family.week_start_date and v_family.week_ending_date
    )
    select
      coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'work_event_id',state.work_event_id,'date',state.resolved_work_date,
        'start',pg_catalog.to_char(state.start_at_local,'HH24:MI'),
        'end',pg_catalog.to_char(state.end_at_local,'HH24:MI'),
        'break_mins',state.break_minutes
      ) order by state.resolved_work_date,state.start_at_local,state.work_event_id)
        filter (where state.new_present),'[]'::jsonb),
      coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'source_system','CLOUDTMS_WEEKLY_FINAL_SOURCE',
        'external_identity',state.work_event_id::text,
        'external_revision',state.final_revision_id::text,
        'document_sha256',pg_catalog.encode(coalesce(
          state.snapshot_line_hash,state.transition_fingerprint
        ),'hex'),
        'work_date',state.resolved_work_date,'client_source_id',state.id,
        'source_complete',true,'source_present',state.new_present,
        'approved_minutes',case when state.new_present then state.actual_net_minutes else 0 end
      ) order by state.resolved_work_date,state.work_event_id),'[]'::jsonb),
      (pg_catalog.array_agg(state.final_revision_id order by
        state.finalisation_week_ending desc,state.finalised_at_utc desc,
        state.revision_number desc,state.id desc)
        filter (where state.work_event_id=v_event_id))[1],
      coalesce(bool_or(state.work_event_id=v_event_id and state.new_present),false),
      coalesce(max(state.actual_net_minutes) filter (where state.work_event_id=v_event_id and state.new_present),0),
      (pg_catalog.array_agg(state.final_revision_id::text order by
        state.finalisation_week_ending desc,state.finalised_at_utc desc,
        state.revision_number desc,state.id desc)
        filter (where state.work_event_id=v_event_id))[1],
      max(pg_catalog.encode(coalesce(state.snapshot_line_hash,state.transition_fingerprint),'hex'))
        filter (where state.work_event_id=v_event_id)
    into v_source_segments,v_client_sources,v_current_final,
         v_selected_source_present,v_selected_source_minutes,
         v_selected_source_revision,v_selected_source_hash
    from current_state state;
  else
    -- WP-57 (WP-52 handoff N1).  WHICH SOURCE POSITION IS LIVE ON AN NHSP WORK
    -- EVENT IS DECIDED BY FACTS, NEVER BY WHICH PHYSICAL ROW THE TRUST LISTED
    -- FIRST.  The single owner of that decision is
    -- private.weekly_source_ordinary_projection_active_movements_v1, installed
    -- by WP-52 in
    -- supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql:3413.
    -- This surface READS it and keeps no second copy of the rule, so the
    -- proposal Office is shown and the pay the Candidate receives cannot drift
    -- apart.  APPLY-ORDER DEPENDENCY: that repeatable must be installed before
    -- this one.  It is, under the release runner's DDMMYYYY -> YYYYMMDD re-key
    -- followed by filename order (`…_ordinary_pay_projection_v1` sorts before
    -- `…_protected_action_orchestration_v1`), and plpgsql resolves the call at
    -- run time in any case.
    --
    -- WHAT WAS HERE AND WHY IT WAS WRONG.  A
    -- `row_number() over (partition by movement.work_event_id
    --   order by …, movement.created_at_utc desc, movement.id desc)`
    -- kept rank 1 per work event and read `source_present` and
    -- `approved_minutes` off it.  Since WP-37 an NHSP reversal and its re-issue
    -- resolve to ONE work event, so inside one report that ranking fell through
    -- to `created_at_utc` - which is `transaction_timestamp()` (schema
    -- 15092026_1534_weekly_source_plan6_schema.sql:1093) and therefore
    -- IDENTICAL for every movement of one finalisation - and then to
    -- `movement.id`, a random UUID.  EXECUTED on a build from empty: the same
    -- corrected shift answered `source_present=true, 540 approved minutes` on
    -- one run and `source_present=false, 0` on the next, with the Client
    -- invoiced GBP 180.00 either way; and with `source_present=false` the
    -- RECORD_NOT_WORKED guard at the foot of this owner stopped guarding, so
    -- Office could record a worked, re-issued shift as not worked.  Split
    -- across reports it was deterministically wrong: a later reversal of the
    -- ORIGINAL 8 h line wiped a re-issued 9 h shift.
    --
    -- WP-30, standing rule 3: the movements are read over the whole Timesheet
    -- FAMILY (`v_root_family`), never the physical root id alone.
    --
    -- The revision-ladder head is stated as a fact - "no CURRENT peer carrying
    -- this family's movements stands later" - not as `order by … limit 1`, and
    -- an ambiguous head fails CLOSED.
    with ladder as (
      select revision.id,source_cycle.finalisation_week_ending,
             revision.finalised_at_utc,revision.revision_number
      from public.weekly_source_final_revisions revision
      join public.weekly_source_cycles source_cycle
        on source_cycle.id=revision.source_cycle_id
      where revision.state='CURRENT'
        and exists(
          select 1
          from public.weekly_source_billing_movements movement
          where movement.final_revision_id=revision.id
            and movement.invoice_timesheet_id=any(v_root_family)
            and movement.source_line_kind in (
              'NHSP_PHYSICAL_POSITIVE','NHSP_PHYSICAL_FULL_NEGATIVE')
            and movement.candidate_id=v_family.candidate_id
            and movement.contract_id=v_family.contract_id
            and movement.actual_client_id=v_contract.client_id
        )
    )
    -- The UUID representative is taken as `min(...::text)::uuid` per AGENTS.md
    -- (PostgreSQL has no `min(uuid)`), and only after the exact cardinality
    -- proof on the next line.
    select pg_catalog.count(*)::integer,pg_catalog.min(head.id::text)::uuid
      into v_source_head_count,v_source_head_revision
    from ladder head
    where not exists(
      select 1 from ladder peer
      where (peer.finalisation_week_ending,peer.finalised_at_utc,peer.revision_number)
            >(head.finalisation_week_ending,head.finalised_at_utc,head.revision_number)
    );
    if v_source_head_count>1 then
      raise exception 'WEEKLY_PROTECTED_AMBIGUOUS_SOURCE_LADDER_HEAD'
        using errcode='55000',
        detail=pg_catalog.jsonb_build_object(
          'family_id',v_family.id,'work_event_id',v_event_id,
          'current_ladder_head_count',v_source_head_count)::text;
    end if;

    if v_source_head_revision is not null then
      with scope as (
        -- Which work events this family week has ANY NHSP source row for,
        -- positive or negative.  This is a scope question, not a money
        -- question: it fixes the rows that appear in the evidence, and it
        -- carries no ranking.
        select distinct movement.work_event_id,
               (movement.source_facts_json->>'work_date')::date as work_date
        from public.weekly_source_billing_movements movement
        join public.weekly_source_final_revisions revision
          on revision.id=movement.final_revision_id and revision.state='CURRENT'
        where movement.invoice_timesheet_id=any(v_root_family)
          and movement.source_line_kind in (
            'NHSP_PHYSICAL_POSITIVE','NHSP_PHYSICAL_FULL_NEGATIVE')
          and movement.candidate_id=v_family.candidate_id
          and movement.contract_id=v_family.contract_id
          and movement.actual_client_id=v_contract.client_id
          and (movement.source_facts_json->>'work_date')::date
              between v_family.week_start_date and v_family.week_ending_date
      ), live as (
        -- The single owner's answer, over every physical member of the family.
        -- Nothing is re-filtered here that could change WHICH position is
        -- live; the predicates below are identity and week scope only.  A
        -- movement voided by Correct Final Source cannot appear: the only
        -- writer of `VOIDED_BY_CORRECT_FINAL` in the tree sets it in the same
        -- atomic block that moves its revision CURRENT -> SUPERSEDED
        -- (15092026_1534_weekly_source_correct_final_source_v1.sql:1864-1873),
        -- and the owner reads CURRENT revisions only.
        select live_movement.*
        from pg_catalog.unnest(v_root_family) as family_member(root_timesheet_id)
        cross join lateral
          private.weekly_source_ordinary_projection_active_movements_v1(
            family_member.root_timesheet_id,v_source_head_revision) live_movement
        where live_movement.source_line_kind in (
            'NHSP_PHYSICAL_POSITIVE','NHSP_PHYSICAL_FULL_NEGATIVE')
          and live_movement.candidate_id=v_family.candidate_id
          and live_movement.contract_id=v_family.contract_id
          and live_movement.actual_client_id=v_contract.client_id
          and (live_movement.source_facts_json->>'work_date')::date
              between v_family.week_start_date and v_family.week_ending_date
      ), row_state as (
        select scope.work_event_id,scope.work_date,
               live.id as movement_id,live.final_revision_id,
               live.movement_economic_hash,live.source_facts_json,
               (live.id is not null) as source_present,
               pg_catalog.count(live.id) over (
                 partition by scope.work_event_id) as live_count
        from scope
        left join live on live.work_event_id=scope.work_event_id
      )
      -- Every `order by` below is inside a `jsonb_agg` and fixes DISPLAY order
      -- only; no money value is selected by one.  The `max(...) filter (...)`
      -- expressions run over a group whose cardinality is one, proved by
      -- `v_ambiguous_event_count` immediately after this statement: when two
      -- contradictory live positions survive on one work event this owner
      -- REFUSES, and none of these values reaches the caller.
      select
        coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'work_event_id',row_state.work_event_id,'date',row_state.work_date,
          'start',pg_catalog.to_char(
            (row_state.source_facts_json->>'start_at_local')::timestamp,'HH24:MI'),
          'end',pg_catalog.to_char(
            (row_state.source_facts_json->>'end_at_local')::timestamp,'HH24:MI'),
          'break_mins',(row_state.source_facts_json->>'break_minutes')::integer
        ) order by row_state.work_date,row_state.work_event_id)
          filter (where row_state.source_present),'[]'::jsonb),
        coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'source_system','NHSP_BACKING_REPORT',
          'external_identity',row_state.work_event_id::text,
          'external_revision',coalesce(
            row_state.final_revision_id::text,v_cycle.id::text),
          'document_sha256',case when row_state.source_present
            then pg_catalog.encode(row_state.movement_economic_hash,'hex')
            -- A work event the Trust has fully reversed has no live position.
            -- Its evidence is the same server-derived absence statement this
            -- owner already builds for a work event the source never mentioned
            -- (below), so nothing has to choose between several negatives.
            else pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
              'WEEKLY_PROTECTED_SOURCE_ABSENCE_V1',
              pg_catalog.jsonb_build_object(
                'family_id',v_family.id,'source_cycle_id',v_cycle.id,
                'work_event_id',row_state.work_event_id,
                'source_mode',v_source_mode,'source_present',false
              )),'hex') end,
          'work_date',row_state.work_date,
          'client_source_id',coalesce(row_state.movement_id,row_state.work_event_id),
          'source_complete',true,'source_present',row_state.source_present,
          'approved_minutes',case when row_state.source_present
            then (row_state.source_facts_json->>'actual_net_minutes')::integer
            else 0 end
        ) order by row_state.work_date,row_state.work_event_id),'[]'::jsonb),
        (pg_catalog.max(row_state.final_revision_id::text)
          filter (where row_state.work_event_id=v_event_id
            and row_state.source_present))::uuid,
        coalesce(pg_catalog.bool_or(row_state.work_event_id=v_event_id
          and row_state.source_present),false),
        coalesce(pg_catalog.max(
          (row_state.source_facts_json->>'actual_net_minutes')::integer)
          filter (where row_state.work_event_id=v_event_id
            and row_state.source_present),0),
        pg_catalog.max(row_state.final_revision_id::text)
          filter (where row_state.work_event_id=v_event_id
            and row_state.source_present),
        pg_catalog.max(pg_catalog.encode(row_state.movement_economic_hash,'hex'))
          filter (where row_state.work_event_id=v_event_id
            and row_state.source_present),
        coalesce(pg_catalog.count(distinct row_state.work_event_id)
          filter (where row_state.live_count>1),0)::integer
      into v_source_segments,v_client_sources,v_current_final,
           v_selected_source_present,v_selected_source_minutes,
           v_selected_source_revision,v_selected_source_hash,
           v_ambiguous_event_count
      from row_state;
      if v_ambiguous_event_count>0 then
        -- 14 s4.2.8.  Two live positions of different economics on one shift
        -- are two contradictory statements about it.  Standing rule 5: an
        -- explicit cardinality check, and the unexpected case goes to the
        -- fail-closed branch with a stated reason - never to whichever row the
        -- Trust listed last.
        raise exception 'WEEKLY_PROTECTED_AMBIGUOUS_SOURCE_HEAD'
          using errcode='55000',
          detail=pg_catalog.jsonb_build_object(
            'family_id',v_family.id,'work_event_id',v_event_id,
            'ambiguous_work_event_count',v_ambiguous_event_count)::text;
      end if;
    end if;
  end if;

  if not exists(
    select 1 from pg_catalog.jsonb_array_elements(v_client_sources) source_row(value)
    where source_row.value->>'external_identity'=v_event.id::text
  ) then
    v_selected_source_hash:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_PROTECTED_SOURCE_ABSENCE_V1',
      pg_catalog.jsonb_build_object(
        'family_id',v_family.id,'source_cycle_id',v_cycle.id,
        'work_event_id',v_event.id,'source_mode',v_source_mode,'source_present',false
      )
    ),'hex');
    v_selected_source_revision:=v_cycle.id::text;
    v_client_sources:=v_client_sources||pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'source_system',case when v_source_mode='NHSP_WEEKLY'
          then 'NHSP_BACKING_REPORT' else 'CLOUDTMS_WEEKLY_FINAL_SOURCE' end,
        'external_identity',v_event.id::text,'external_revision',v_selected_source_revision,
        'document_sha256',v_selected_source_hash,'work_date',v_event.work_date,
        'client_source_id',v_event.id,'source_complete',true,
        'source_present',false,'approved_minutes',0
      )
    );
  end if;

  -- A selected source observation may legitimately come from an older cycle.
  -- The approval audit pointer is populated only when that CURRENT revision
  -- belongs to this action's cycle; stage validation must never be handed a
  -- cross-cycle revision identifier.
  if v_current_final is not null and not exists(
    select 1 from public.weekly_source_final_revisions revision
    where revision.id=v_current_final
      and revision.source_cycle_id=v_cycle.id
      and revision.state='CURRENT'
  ) then
    v_current_final:=null;
  end if;

  with latest as (
    select family_event.*,
      pg_catalog.row_number() over (
        partition by family_event.durable_work_event_id
        order by family_event.event_sequence desc
      ) as event_rank
    from public.weekly_exceptional_pay_family_events family_event
    where family_event.family_id=v_family.id
  ), decisions as (
    select latest.durable_work_event_id as work_event_id,
      case when latest.durable_work_event_id=v_event.id then case
        when v_run.request_kind in ('APPROVE','AMEND','WAIT') then 'WAIT'
        when v_run.request_kind in ('WITHDRAW','RECONCILE') then 'ACCEPTED_SOURCE'
        else 'NOT_WORKED' end
      else latest.state end as decision_state,
      case when latest.durable_work_event_id=v_event.id
             and v_run.request_kind in ('APPROVE','AMEND','WAIT')
        then v_schedule
        else pg_catalog.jsonb_build_object(
          'work_date',latest.work_date,'start_at_local',latest.start_at_local,
          'end_at_local',latest.end_at_local,'break_minutes',latest.break_minutes
        ) end as fixed_schedule
    from latest where latest.event_rank=1
  )
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'work_event_id',decision.work_event_id,'state',decision.decision_state,
    'fixed_schedule',case when decision.decision_state='WAIT' then pg_catalog.jsonb_build_object(
      'date',decision.fixed_schedule->>'work_date',
      'start',pg_catalog.to_char((decision.fixed_schedule->>'start_at_local')::timestamp,'HH24:MI'),
      'end',pg_catalog.to_char((decision.fixed_schedule->>'end_at_local')::timestamp,'HH24:MI'),
      'break_mins',(decision.fixed_schedule->>'break_minutes')::integer
    ) else null end
  ) order by decision.work_event_id),'[]'::jsonb)
  into v_decisions from decisions decision;
  if not exists(
    select 1 from pg_catalog.jsonb_array_elements(v_decisions) d(value)
    where (d.value->>'work_event_id')::uuid=v_event.id
  ) then
    v_decisions:=v_decisions||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'work_event_id',v_event.id,
      'state',case when v_run.request_kind in ('APPROVE','AMEND','WAIT') then 'WAIT'
        when v_run.request_kind in ('WITHDRAW','RECONCILE') then 'ACCEPTED_SOURCE'
        else 'NOT_WORKED' end,
      'fixed_schedule',case when v_run.request_kind in ('APPROVE','AMEND','WAIT')
        then pg_catalog.jsonb_build_object(
          'date',v_work_date,'start',pg_catalog.to_char(v_start,'HH24:MI'),
          'end',pg_catalog.to_char(v_end,'HH24:MI'),'break_mins',v_break
        ) else null end
    ));
  end if;

  if v_run.request_kind='RECORD_NOT_WORKED' and v_selected_source_present then
    raise exception 'WEEKLY_PROTECTED_NOT_WORKED_SOURCE_PRESENT' using errcode='55000';
  end if;

  select comparison.id into v_comparison
  from public.weekly_issue_comparison_revisions comparison
  join public.weekly_discrepancy_incidents incident on incident.id=comparison.incident_id
  where incident.work_event_id=v_event.id and incident.candidate_id=v_family.candidate_id
    and incident.client_id=v_contract.client_id
  order by comparison.created_at_utc desc,comparison.id desc limit 1;

  if v_evidence_id is null then
    select approval.evidence_timesheet_id into v_evidence_id
    from public.weekly_exceptional_payment_approvals approval
    where approval.pay_target_family_id=v_family.id
      and approval.work_event_id=v_event.id
    order by approval.approved_at_utc desc,approval.id desc limit 1;
  end if;
  if v_evidence_id is not null then
    v_signed:=private.weekly_exceptional_candidate_signed_evidence_v1(v_evidence_id);
    v_candidate_submission:=pg_catalog.jsonb_build_object(
      'source_system','CLOUDTMS_CANDIDATE_TIMESHEET',
      'external_identity',v_evidence_id::text,
      'external_revision',v_signed->>'timesheet_version',
      'document_sha256',v_signed->>'signature_sha256',
      'work_date',v_event.work_date,'submitted_by_candidate_id',v_family.candidate_id,
      'submission_id',v_evidence_id,'submitted_at_utc',v_signed->>'signed_at_utc',
      'submitted_minutes',extract(epoch from (v_end-v_start))::integer/60-v_break
    );
  end if;

  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'expense_code','SOURCE_SUPPLIED:'||authority.work_event_id::text,
    'authority_kind','SOURCE_EXPENSE','source_expense_id',authority.id,
    'document_sha256',pg_catalog.encode(authority.authority_hash,'hex'),
    'pay_ex_vat',authority.candidate_reimbursement_ex_vat::text,
    'charge_ex_vat',authority.client_charge_ex_vat::text
  ) order by authority.work_event_id),'[]'::jsonb)
  into v_source_expenses
  from (
    select authority.*,
      pg_catalog.row_number() over (
        partition by authority.work_event_id order by source_cycle.finalisation_week_ending desc,
          revision.finalised_at_utc desc,authority.generation desc,authority.id desc
      ) as event_rank
    from public.weekly_expense_authority_generations authority
    join public.weekly_source_final_revisions revision
      on revision.id=authority.final_revision_id and revision.state='CURRENT'
    join public.weekly_source_cycles source_cycle on source_cycle.id=revision.source_cycle_id
    where authority.contract_id=v_family.contract_id and authority.state='CURRENT'
      and source_cycle.source_group_id=v_group.id
      and authority.source_expense_pence>0
      and exists(
        select 1 from public.weekly_source_row_timesheet_lineages lineage
        where lineage.work_event_id=authority.work_event_id
          and lineage.contract_id=authority.contract_id
          and lineage.timesheet_id=any(v_root_family)
      )
  ) authority where authority.event_rank=1;

  v_fin_hash:=case when v_fin.id is null then null else pg_catalog.encode(
    private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_PROTECTED_ROOT_FINANCIAL_V1',pg_catalog.to_jsonb(v_fin)
    ),'hex') end;
  v_provider_hash:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_PROVIDER_V1',
    pg_catalog.jsonb_build_object(
      'contract_id',v_contract.id,'pay_method',v_contract.pay_method_snapshot,
      'umbrella_id',pg_catalog.to_jsonb(v_contract)->>'umbrella_id',
      'candidate_id',v_contract.candidate_id
    )
  ),'hex');
  v_rate_refs:=pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_TARGET_MANAGED_ZERO_RATE_SOURCE_V1',
    'source_cycle_id',v_cycle.id,'source_group_id',v_group.id,
    'source_family',v_group.source_family,'source_mode',v_source_mode,
    'source_profile_domain',case when v_source_mode='NHSP_WEEKLY'
      then 'NHSP_TRUST_BACKING_REPORT' else 'ROSTER_FINAL_AUTHORITY' end,
    'target_family_id',v_family.id,'root_timesheet_id',v_root.timesheet_id,
    'effective_policy_sha256',v_policy->>'policy_sha256'
  );

  return pg_catalog.jsonb_build_object(
    'ok',true,'contract','WEEKLY_PROTECTED_ACTION_CONTEXT_V1',
    'action',v_run.request_kind,'family_id',v_family.id,
    'orchestration_run_id',v_run.id,'family_bound_version',v_family.bound_version,
    'current_target_vector_sha256',case
      when v_family.current_complete_target_vector_hash is null then null
      else pg_catalog.encode(v_family.current_complete_target_vector_hash,'hex') end,
    'source_cycle_id',v_cycle.id,'source_group_id',v_group.id,
    'agency_id',v_group.agency_id,'candidate_id',v_family.candidate_id,
    'client_id',v_contract.client_id,'contract_id',v_contract.id,
    'contract_week_id',v_contract_week.id,'root_timesheet_id',v_root.timesheet_id,
    'work_event_id',v_event.id,'week_ending_date',v_family.week_ending_date,
    'source_mode',v_source_mode,'policy',v_settings,
    'timesheet',pg_catalog.to_jsonb(v_root),'contract_week',pg_catalog.to_jsonb(v_contract_week),
    'contract_record',pg_catalog.to_jsonb(v_contract),
    'current_financial',case when v_fin.id is null then null else pg_catalog.to_jsonb(v_fin) end,
    'requires_zero_financial',v_fin.id is null,
    'protected_schedule',v_schedule,'source_segments',v_source_segments,
    'protected_decisions',v_decisions,'source_proposal',pg_catalog.jsonb_build_object(
      'selected_work_event_id',v_event.id,'source_present',v_selected_source_present,
      'source_minutes',v_selected_source_minutes,'source_revision',v_selected_source_revision,
      'source_hash',v_selected_source_hash,'source_segments',v_source_segments
    ),
    'client_sources',v_client_sources,'source_expenses',v_source_expenses,
    'candidate_submission',v_candidate_submission,
    'current_comparison_revision_id',v_comparison,
    'current_final_revision_id',v_current_final,
    'root_financial',case when v_fin.id is null then null else pg_catalog.jsonb_build_object(
      'source_system','CLOUDTMS_TIMESHEET_FINANCIAL','external_identity',v_fin.id::text,
      'external_revision',v_fin.timesheet_version::text,'document_sha256',v_fin_hash,
      'financial_row_id',v_fin.id,'root_version',v_root.version,
      'financial_timesheet_version',v_fin.timesheet_version,
      'financial_revision_digest',v_fin_hash
    ) end,
    'provider',pg_catalog.jsonb_build_object(
      'source_system','CLOUDTMS_CONTRACT_PROVIDER','external_identity',v_contract.id::text,
      'external_revision',v_root.version::text,'document_sha256',v_provider_hash,
      'source_pay_method',v_contract.pay_method_snapshot,
      'umbrella_id',nullif(pg_catalog.to_jsonb(v_contract)->>'umbrella_id','')::uuid,
      'provider_authority_sha256',v_provider_hash,
      'target_pay_method',v_contract.pay_method_snapshot,
      'target_umbrella_id',nullif(pg_catalog.to_jsonb(v_contract)->>'umbrella_id','')::uuid,
      'target_enabled',true,'target_vat_chargeable',null
    ),
    'expected_head_revision',coalesce((
      select request.c1_head_revision
      from public.weekly_exceptional_c1_publication_requests request
      where request.family_id=v_family.id and request.state='PUBLISHED'
      order by request.request_sequence desc limit 1
    ),0)::text,
    'request_sequence',(v_family.current_generation_number+1)::text,
    'zero_rate_source_refs',v_rate_refs
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_ACTION_CONTEXT_SCOPE_INVALID' using errcode='55000';
end;
$function$;

-- Resolve an already-staged publication from its immutable orchestration run.
-- The service uses this before any recomputation: a replay returns the exact
-- saved request, and an UNKNOWN result can enter only the explicit recovery
-- path.  No browser-visible identifier is accepted as C1 authority.
create or replace function public.weekly_exceptional_pay_action_publication_status_v1(
  p_request jsonb
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','family_id','orchestration_run_id'
  ];
  v_actor uuid;
  v_family_id uuid;
  v_run_id uuid;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_publication public.weekly_exceptional_c1_publication_requests%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_contract public.contracts%rowtype;
begin
  if pg_catalog.jsonb_typeof(p_request)<>'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_ACTION_PUBLICATION_STATUS_V1' then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_family_id:=(p_request->>'family_id')::uuid;
    v_run_id:=(p_request->>'orchestration_run_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_INVALID' using errcode='22023';
  end;
  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_family_id;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_run_id and run.family_id=v_family.id;
  select contract.* into strict v_contract
  from public.contracts contract where contract.id=v_family.contract_id;
  if v_run.requested_by_user_id is distinct from v_actor then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_SCOPE_INVALID' using errcode='55000';
  end if;

  select request.* into v_publication
  from public.weekly_exceptional_c1_publication_requests request
  where request.family_id=v_family.id
    and request.orchestration_run_id=v_run.id
  order by request.created_at_utc desc,request.id desc
  limit 1;
  if not found then
    perform 1 from public.tms_users office_user
    where office_user.id=v_actor and office_user.is_active
      and (office_user.payment_authoriser or office_user.payment_golden_key);
    if not found then
      raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_SCOPE_INVALID' using errcode='55000';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'staged',false,'family_id',v_family.id,
      'orchestration_run_id',v_run.id,'run_state',v_run.state
    );
  end if;

  select approval.* into strict v_approval
  from public.weekly_exceptional_payment_approvals approval
  where approval.creation_orchestration_run_id=v_run.id
    and approval.pay_target_family_id=v_family.id
  order by approval.approved_at_utc desc,approval.id desc
  limit 1;
  select cycle.* into strict v_cycle
  from public.weekly_source_cycles cycle where cycle.id=v_approval.source_cycle_id;
  select source_group.* into strict v_group
  from public.weekly_source_groups source_group
  where source_group.id=v_cycle.source_group_id;
  perform private.weekly_source_office_authority_v1(
    v_actor,'APPROVE_PROTECTED_PAY',v_group.id,v_contract.client_id,
    v_approval.protected_work_date
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'staged',true,'family_id',v_family.id,
    'orchestration_run_id',v_run.id,'run_state',v_run.state,
    'publication_request_id',v_publication.id,
    'request_sha256',pg_catalog.encode(v_publication.request_sha256,'hex'),
    'publication_state',v_publication.state
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_SCOPE_INVALID' using errcode='55000';
end;
$function$;

-- WAIT is an audited no-economic-change decision.  It advances neither the
-- C1 entitlement nor the ordinary TSFIN and is allowed only when the caller
-- proves the current complete target vector is unchanged.
create or replace function public.weekly_exceptional_pay_wait_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','family_id','orchestration_run_id',
    'source_cycle_id','work_event_id','expected_family_bound_version',
    'expected_target_vector_sha256','source_proposal','protected_schedule',
    'reason','idempotency_key'
  ];
  v_actor uuid;
  v_family_id uuid;
  v_run_id uuid;
  v_cycle_id uuid;
  v_event_id uuid;
  v_expected_version bigint;
  v_expected_hash bytea;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_prior_event public.weekly_exceptional_pay_family_events%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_sequence bigint;
  v_prior_hash bytea;
  v_event_hash bytea;
  v_target_sequence bigint;
  v_prior_target_hash bytea;
  v_target_hash bytea;
  v_source_hash bytea;
  v_after bytea;
  v_key text;
begin
  if pg_catalog.jsonb_typeof(p_request)<>'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_WAIT_V1'
     or pg_catalog.jsonb_typeof(p_request->'source_proposal')<>'object'
     or pg_catalog.jsonb_typeof(p_request->'protected_schedule')<>'object' then
    raise exception 'WEEKLY_PROTECTED_WAIT_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_family_id:=(p_request->>'family_id')::uuid;
    v_run_id:=(p_request->>'orchestration_run_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_event_id:=(p_request->>'work_event_id')::uuid;
    v_expected_version:=(p_request->>'expected_family_bound_version')::bigint;
    v_expected_hash:=private.weekly_exceptional_hex_sha256_v1(
      p_request->>'expected_target_vector_sha256'
    );
  exception when others then
    raise exception 'WEEKLY_PROTECTED_WAIT_REQUEST_INVALID' using errcode='22023';
  end;
  v_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_family_id for update;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_run_id and run.family_id=v_family.id for update;
  if v_run.state='COMPLETE' then
    return pg_catalog.jsonb_build_object(
      'ok',true,'outcome','WAITING_FOR_SOURCE','idempotent_replay',true,
      'family_id',v_family.id,'family_bound_version',v_family.bound_version
    );
  end if;
  if v_run.request_kind<>'WAIT' or v_run.state<>'RUNNING'
     or v_run.requested_by_user_id is distinct from v_actor
     or v_family.bound_version<>v_expected_version
     or v_family.current_complete_target_vector_hash is distinct from v_expected_hash
     or v_family.current_generation_id is null
     or pg_catalog.char_length(v_key) not between 16 and 240 then
    raise exception 'WEEKLY_PROTECTED_WAIT_SCOPE_INVALID' using errcode='55000';
  end if;
  select approval.* into strict v_approval
  from public.weekly_exceptional_payment_approvals approval
  where approval.pay_target_family_id=v_family.id and approval.work_event_id=v_event_id
  order by approval.approved_at_utc desc,approval.id desc limit 1;
  select family_event.* into strict v_prior_event
  from public.weekly_exceptional_pay_family_events family_event
  where family_event.family_id=v_family.id
    and family_event.durable_work_event_id=v_event_id
  order by family_event.event_sequence desc limit 1;
  select generation.* into strict v_generation
  from public.weekly_exceptional_pay_generations generation
  where generation.id=v_family.current_generation_id
    and generation.family_id=v_family.id
    and generation.lifecycle_state='PUBLISHED' for share;
  v_source_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1',p_request->'source_proposal'
  );
  select coalesce(max(event_sequence),0)+1,
         (array_agg(event_hash order by event_sequence desc))[1]
    into v_sequence,v_prior_hash
  from public.weekly_exceptional_pay_family_events where family_id=v_family.id;
  v_event_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_FAMILY_EVENT_V1',pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'event_sequence',v_sequence,
      'work_event_id',v_event_id,'approval_id',v_approval.id,
      'schedule',p_request->'protected_schedule',
      'source_proposal_hash',pg_catalog.encode(v_source_hash,'hex'),
      'target_vector_hash',pg_catalog.encode(v_expected_hash,'hex'),
      'prior_event_hash',case when v_prior_hash is null then null else pg_catalog.encode(v_prior_hash,'hex') end
    )
  );
  insert into public.weekly_exceptional_pay_family_events(
    family_id,event_sequence,durable_work_event_id,evidence_approval_id,
    work_date,start_at_local,end_at_local,break_minutes,rate_classification_json,
    source_proposal_snapshot_json,source_proposal_hash,fixed_office_target_snapshot_json,
    fixed_office_target_hash,state,current_comparison_revision_id,
    current_final_revision_id,office_actor_user_id,office_reason,
    prior_event_hash,event_hash
  ) values (
    v_family.id,v_sequence,v_event_id,v_approval.id,
    (p_request#>>'{protected_schedule,work_date}')::date,
    (p_request#>>'{protected_schedule,start_at_local}')::timestamp,
    (p_request#>>'{protected_schedule,end_at_local}')::timestamp,
    (p_request#>>'{protected_schedule,break_minutes}')::integer,
    v_prior_event.rate_classification_json,p_request->'source_proposal',v_source_hash,
    p_request->'protected_schedule',v_prior_event.fixed_office_target_hash,'WAIT',
    v_prior_event.current_comparison_revision_id,v_prior_event.current_final_revision_id,
    v_actor,p_request->>'reason',v_prior_hash,v_event_hash
  );
  select coalesce(max(event_sequence),0)+1,
         (array_agg(event_hash order by event_sequence desc))[1]
    into v_target_sequence,v_prior_target_hash
  from public.weekly_exceptional_pay_target_events where family_id=v_family.id;
  v_target_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_TARGET_EVENT_V1',pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'event_sequence',v_target_sequence,
      'approval_id',v_approval.id,'generation_id',null,
      'prior_vector_hash',pg_catalog.encode(v_expected_hash,'hex'),
      'next_vector_hash',pg_catalog.encode(v_expected_hash,'hex'),
      'reason','WAIT','actor_user_id',v_actor
    )
  );
  insert into public.weekly_exceptional_pay_target_events(
    family_id,approval_id,event_sequence,triggering_comparison_revision_id,
    triggering_final_revision_id,prior_event_fingerprint,fixed_target_component_snapshot,
    current_source_proposal_snapshot,complete_prior_family_vector_fingerprint,
    complete_next_family_vector_fingerprint,reason,resulting_lifecycle_state,
    financial_generation_id,actor_user_id,event_hash,idempotency_key
  ) values (
    v_family.id,v_approval.id,v_target_sequence,v_prior_event.current_comparison_revision_id,
    v_prior_event.current_final_revision_id,v_prior_target_hash,
    v_generation.complete_next_vector_json,p_request->'source_proposal',
    v_expected_hash,v_expected_hash,'WAIT','WAITING_SOURCE',null,
    v_actor,v_target_hash,v_key||':target-event'
  );
  update public.weekly_exceptional_pending_reconciliation_targets target
  set state='SUPERSEDED',completed_at_utc=pg_catalog.statement_timestamp()
  where target.family_id=v_family.id and target.state='ACTIVE';
  insert into public.weekly_exceptional_pending_reconciliation_targets(
    family_id,approval_id,durable_work_event_id,incident_id,
    current_final_revision_id,intended_outcome,
    source_action_policy_target_fingerprint,state
  ) values (
    v_family.id,v_approval.id,v_event_id,
    (select incident.id from public.weekly_discrepancy_incidents incident
      where incident.work_event_id=v_event_id
        and incident.candidate_id=v_family.candidate_id
      order by incident.episode_number desc limit 1),
    v_prior_event.current_final_revision_id,'WAIT',v_source_hash,'ACTIVE'
  );
  insert into public.weekly_exceptional_payment_events(
    family_id,approval_id,event_kind,lifecycle_view,bounded_payload_json,idempotency_key
  ) values (
    v_family.id,v_approval.id,'WAIT','WAITING_SOURCE',
    pg_catalog.jsonb_build_object('work_event_id',v_event_id,'source_cycle_id',v_cycle_id),
    v_key||':payment-event'
  );
  update public.weekly_exceptional_pay_target_families
  set current_source_proposal_hash=v_source_hash,
      current_lifecycle_state='WAITING_SOURCE',bound_version=bound_version+1
  where id=v_family.id;
  v_after:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_WAIT_AFTER_V1',pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'event_hash',pg_catalog.encode(v_event_hash,'hex'),
      'target_event_hash',pg_catalog.encode(v_target_hash,'hex'),
      'target_vector_hash',pg_catalog.encode(v_expected_hash,'hex')
    )
  );
  update public.weekly_exceptional_orchestration_runs
  set state='COMPLETE',after_state_fingerprint=v_after,
      completed_at_utc=pg_catalog.statement_timestamp()
  where id=v_run.id;
  return pg_catalog.jsonb_build_object(
    'ok',true,'outcome','WAITING_FOR_SOURCE','idempotent_replay',false,
    'family_id',v_family.id,'family_bound_version',v_expected_version+1,
    'target_vector_sha256',pg_catalog.encode(v_expected_hash,'hex')
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_WAIT_SCOPE_INVALID' using errcode='55000';
end;
$function$;

-- ---------------------------------------------------------------------------
-- G2-5.  The two later-change Office decisions of 24 section 4.2 and 25
-- section 1, per root, as ONE complete entitlement decision:
--
--   APPROVE_UPDATED_HOURS          publishes the new complete entitlement as
--                                  LOCKED_FINAL_SOURCE;
--   KEEP_CURRENTLY_APPROVED_HOURS  preserves the complete existing entitlement
--                                  as PROTECTED.
--
-- Pack erratum E-5 applies: these two replace the obsolete
-- `Accept match and reconcile` wording of 27 section 4 step 2.  The
-- protected-shift editor actions (AMEND, WITHDRAW, RECONCILE,
-- RECORD_NOT_WORKED) and WAIT stay exactly where they are: they are per work
-- event against a target family, NOT per root over the complete entitlement,
-- so they are deliberately not renamed into these two.
--
-- The browser supplies no money, no hours, no head id and no actor authority.
-- Everything economic is rebuilt here from the server's own composer and the
-- immutable PROPOSED decision bundle.  Publication itself belongs entirely to
-- the Gate 5 coordinator: the serial gate, the rotation lock set, the complete
-- family freeze census, the FROZEN -> pending-bundle branch, the single
-- Workbench invalidation and the one immutable receipt are all its work, and
-- this owner makes exactly one call into it.
create or replace function public.weekly_source_later_change_decide_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'actor_user_id','bundle_revision','decision','decision_bundle_id',
    'final_revision_id','idempotency_key','root_timesheet_id','schema_version'
  ];
  v_actual_keys text[];
  v_expected_keys text[];
  v_actor uuid;
  v_decision text;
  v_bundle_id uuid;
  v_bundle_revision bigint;
  v_root_timesheet_id uuid;
  v_final_revision_id uuid;
  v_idempotency_key text;
  v_identity jsonb;
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_revision public.weekly_source_final_revisions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_bundle public.weekly_source_entitlement_decision_bundles%rowtype;
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_head_found boolean:=false;
  v_head_family uuid[];
  v_head_count integer;
  v_authority_kind text;
  v_components jsonb;
  v_request jsonb;
  v_result jsonb;
  v_published boolean:=false;
  v_request_hash bytea;
  v_request_hash_hex text;
  v_audit public.audit_events%rowtype;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_LATER_CHANGE_REQUEST_INVALID' using errcode='22023';
  end if;
  select pg_catalog.array_agg(key order by key) into v_actual_keys
  from pg_catalog.jsonb_object_keys(p_request) key;
  select pg_catalog.array_agg(key order by key) into v_expected_keys
  from pg_catalog.unnest(v_keys) key;
  if v_actual_keys is distinct from v_expected_keys
     or p_request->>'schema_version'<>'WEEKLY_SOURCE_LATER_CHANGE_DECISION_V1' then
    raise exception 'WEEKLY_SOURCE_LATER_CHANGE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_bundle_id:=(p_request->>'decision_bundle_id')::uuid;
    v_bundle_revision:=(p_request->>'bundle_revision')::bigint;
    v_root_timesheet_id:=(p_request->>'root_timesheet_id')::uuid;
    v_final_revision_id:=(p_request->>'final_revision_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_LATER_CHANGE_REQUEST_INVALID' using errcode='22023';
  end;
  v_decision:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'decision','')));
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  if v_actor is null or v_bundle_id is null or v_root_timesheet_id is null
     or v_final_revision_id is null or coalesce(v_bundle_revision,0)<1
     or v_decision not in ('APPROVE_UPDATED_HOURS','KEEP_CURRENTLY_APPROVED_HOURS')
     or pg_catalog.char_length(v_idempotency_key) not between 16 and 200 then
    raise exception 'WEEKLY_SOURCE_LATER_CHANGE_REQUEST_INVALID' using errcode='22023';
  end if;

  -- Exact replay, in the shape this file already uses for the other Office
  -- actions.  An Office decision is a money act: submitting it twice must
  -- return the first result, not take it again.  The advisory lock makes two
  -- concurrent first attempts one write plus one replay rather than two writes.
  v_request_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_LATER_CHANGE_DECISION_V1',p_request-'idempotency_key'
  );
  v_request_hash_hex:=pg_catalog.encode(v_request_hash,'hex');
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_LATER_CHANGE_DECISION|'||v_idempotency_key,0
  ));
  select audit_row.* into v_audit
  from public.audit_events audit_row
  where audit_row.action='WEEKLY_SOURCE_LATER_CHANGE_DECIDED'
    and audit_row.after_json->>'idempotency_key'=v_idempotency_key
  order by audit_row.ts_utc,audit_row.id
  limit 1;
  if found then
    if v_audit.after_json->>'request_hash' is distinct from v_request_hash_hex then
      raise exception 'WEEKLY_SOURCE_LATER_CHANGE_IDEMPOTENCY_COLLISION'
        using errcode='22023';
    end if;
    return coalesce(v_audit.after_json->'result','{}'::jsonb)
      ||pg_catalog.jsonb_build_object('idempotent_replay',true);
  end if;

  -- The root must be the canonical current member of its own family before any
  -- decision is even considered (proof/34 section 5; WB-016).
  v_identity:=private.weekly_source_resolve_root_identity_v1(v_root_timesheet_id);
  if coalesce((v_identity->>'ok')::boolean,false) is not true
     or (v_identity->>'canonical_timesheet_id')::uuid is distinct from v_root_timesheet_id then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,
      'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'detail',coalesce(v_identity,'{}'::jsonb)
    );
  end if;

  select * into strict v_timesheet from public.timesheets
  where timesheet_id=v_root_timesheet_id;
  select * into strict v_contract from public.contracts
  where id=v_timesheet.contract_id;
  select * into strict v_revision from public.weekly_source_final_revisions
  where id=v_final_revision_id;
  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_revision.source_cycle_id;
  select * into strict v_group from public.weekly_source_groups
  where id=v_cycle.source_group_id;

  -- Office authority.  A later-change decision is a pay decision, so it uses
  -- the existing APPROVE_PROTECTED_PAY operation, which already requires an
  -- active admin who is a payment authoriser.  No new whitelist entry.
  perform private.weekly_source_office_authority_v1(
    v_actor,'APPROVE_PROTECTED_PAY',v_group.id,v_contract.client_id,
    v_timesheet.week_ending_date
  );

  select * into v_bundle
  from public.weekly_source_entitlement_decision_bundles bundle_row
  where bundle_row.decision_bundle_id=v_bundle_id
    and bundle_row.bundle_revision=v_bundle_revision
  for update;
  if not found then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,
      'code','WEEKLY_SOURCE_DECISION_BUNDLE_NOT_FOUND','retryable',false,
      'detail',pg_catalog.jsonb_build_object(
        'decision_bundle_id',v_bundle_id,'bundle_revision',v_bundle_revision)
    );
  end if;
  -- Only a live PROPOSED bundle may be decided.  A COMMITTED bundle's decision
  -- has already been published, and admitting it would let an Office user
  -- re-decide published money while the actor stamp and the ABANDONED update
  -- (both `where state='PROPOSED'`) silently matched zero rows.
  if v_bundle.state<>'PROPOSED'
     or v_bundle.source_root_timesheet_id is distinct from v_root_timesheet_id then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,
      'code','WEEKLY_SOURCE_DECISION_BUNDLE_NOT_PROPOSED','retryable',false,
      'detail',pg_catalog.jsonb_build_object('state',v_bundle.state)
    );
  end if;

  -- WP-30 (WP-27 sweep finding N4, second site), standing rule 3.  The
  -- committed current head is a fact about the FAMILY: the relation's own
  -- uniqueness after S8 is one head per trimmed family booking id, and the
  -- physical-root index is a second, weaker key.  Keyed on the physical id a
  -- rotated family's head was invisible, so `v_head_found` was false and the
  -- KEEP-CURRENT branch below abandoned the decision and reported
  -- `current_head_id=null, authority_kind=null` for a family that demonstrably
  -- holds a live LOCKED_FINAL_SOURCE head.
  --
  -- The cardinality is tested EXPLICITLY and contradictory evidence is refused;
  -- neither unique index is treated as making more than one impossible
  -- (Part 1 rule 5), and no `limit` decides anything.
  --
  -- No row lock is taken over the family here, deliberately.  This owner takes
  -- an advisory lock on its idempotency key and `for update` on the decision
  -- bundle, and takes NO lock at all on public.timesheets; adding family row
  -- locks would inject a new lock class, in a different order from the family-
  -- ordered lockers, into a path that has none.  The residual race is therefore
  -- real and stated rather than claimed closed: a rotation that commits between
  -- private.weekly_source_resolve_root_identity_v1 above (which already
  -- requires the root to be the canonical current member) and this read is not
  -- seen.
  v_head_family:=private.weekly_source_invoice_family_timesheet_ids_v1(
    v_root_timesheet_id
  );
  if v_head_family is null or pg_catalog.cardinality(v_head_family)=0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,
      'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'detail',pg_catalog.jsonb_build_object(
        'reason','FAMILY_IDENTITY_UNRESOLVED',
        'root_timesheet_id',v_root_timesheet_id)
    );
  end if;
  select pg_catalog.count(*)::integer into v_head_count
  from public.weekly_source_entitlement_heads head
  where head.root_timesheet_id=any(v_head_family)
    and head.state='COMMITTED_CURRENT';
  if v_head_count>1 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,
      'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'detail',pg_catalog.jsonb_build_object(
        'reason','MULTIPLE_COMMITTED_CURRENT_HEADS_FOR_THE_FAMILY',
        'root_timesheet_id',v_root_timesheet_id,
        'committed_current_head_count',v_head_count)
    );
  end if;
  if v_head_count=1 then
    select head.* into strict v_head
    from public.weekly_source_entitlement_heads head
    where head.root_timesheet_id=any(v_head_family)
      and head.state='COMMITTED_CURRENT';
  end if;
  v_head_found:=(v_head_count=1);

  if v_decision='APPROVE_UPDATED_HOURS' then
    -- The complete proposed entitlement, rebuilt from the server's own
    -- snapshot builders at the declared current source revision.
    v_authority_kind:='LOCKED_FINAL_SOURCE';
    v_components:=private.weekly_source_entitlement_components_v1(
      private.weekly_source_ordinary_projection_current_segments_v1(
        v_root_timesheet_id,v_final_revision_id),
      private.weekly_source_ordinary_projection_current_expenses_v1(
        v_root_timesheet_id,v_final_revision_id)
    );
  else
    -- 24 section 4.2 and section 6.4: `Keep currently approved hours`
    -- PRESERVES the current complete entitlement.  The money never moves.
    v_authority_kind:='PROTECTED';
    if not v_head_found or v_head.authority_kind='PROTECTED' then
      -- Nothing to publish: what is currently approved stays exactly as it is
      -- (26 Gate 2: "keep-current publishes/retains complete PROTECTED").  The
      -- later proposal is abandoned, the old position remains current, and no
      -- head, receipt, invalidation or Banking Pay row is written.
      update public.weekly_source_entitlement_decision_bundles
      set state='ABANDONED'
      where decision_bundle_id=v_bundle.decision_bundle_id
        and bundle_revision=v_bundle.bundle_revision
        and state='PROPOSED';
      v_result:=pg_catalog.jsonb_build_object(
        'ok',true,'published',false,'retained',true,'decision',v_decision,
        'decision_bundle_id',v_bundle.decision_bundle_id,
        'bundle_revision',v_bundle.bundle_revision,
        'current_head_id',v_head.id,
        'authority_kind',case when v_head_found then v_head.authority_kind else null end
      );
      perform public._audit_insert(
        'weekly_source_entitlement_decision_bundles',v_bundle.decision_bundle_id::text,
        'WEEKLY_SOURCE_LATER_CHANGE_DECIDED',null,
        pg_catalog.jsonb_build_object(
          'decision',v_decision,'outcome','RETAINED',
          'bundle_revision',v_bundle.bundle_revision,
          'root_timesheet_id',v_root_timesheet_id,
          'final_revision_id',v_final_revision_id,
          'current_head_id',v_head.id,
          'idempotency_key',v_idempotency_key,
          'request_hash',v_request_hash_hex,
          'result',v_result
        ),'WEEKLY_SOURCE_KEEP_CURRENTLY_APPROVED_HOURS',v_actor
      );
      return v_result||pg_catalog.jsonb_build_object('idempotent_replay',false);
    end if;
    -- A committed LOCKED_FINAL_SOURCE head becomes PROTECTED with EXACTLY the
    -- same complete component set: the authority kind moves, the money does not.
    select coalesce(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'component_ordinal',component.component_ordinal,
        'component_id',component.component_id,
        'component_kind',component.component_kind,
        'economic_key_type',component.economic_key_type,
        'economic_key_value',component.economic_key_value,
        'component_member_identity',component.component_member_identity,
        'segment_id',component.segment_id,
        'segment_key',component.segment_key,
        'segment_stable_key',component.segment_stable_key,
        'work_date',component.work_date,
        'reference_number',component.reference_number,
        'hours_day',case when component.hours_day is null then null
          else pg_catalog.to_char(component.hours_day,'FM9999999999990.000000') end,
        'hours_night',case when component.hours_night is null then null
          else pg_catalog.to_char(component.hours_night,'FM9999999999990.000000') end,
        'hours_sat',case when component.hours_sat is null then null
          else pg_catalog.to_char(component.hours_sat,'FM9999999999990.000000') end,
        'hours_sun',case when component.hours_sun is null then null
          else pg_catalog.to_char(component.hours_sun,'FM9999999999990.000000') end,
        'hours_bh',case when component.hours_bh is null then null
          else pg_catalog.to_char(component.hours_bh,'FM9999999999990.000000') end,
        'additional_code_raw',component.additional_code_raw,
        'unit_count',case when component.unit_count is null then null
          else pg_catalog.to_char(component.unit_count,'FM9999999999990.000000') end,
        'unit_pay_rate',case when component.unit_pay_rate is null then null
          else pg_catalog.to_char(component.unit_pay_rate,'FM9999999999990.000000') end,
        'unit_charge_rate',case when component.unit_charge_rate is null then null
          else pg_catalog.to_char(component.unit_charge_rate,'FM9999999999990.000000') end,
        'expense_code',component.expense_code,
        'pay_ex_vat',pg_catalog.to_char(component.pay_ex_vat,'FM9999999999990.00'),
        'charge_ex_vat',case when component.charge_ex_vat is null then null
          else pg_catalog.to_char(component.charge_ex_vat,'FM9999999999990.00') end,
        'exclude_from_pay',component.exclude_from_pay,
        'origin',component.origin,
        -- Copy the committed values, do NOT null them: this arm promises the
        -- byte-identical component set, and a head that did carry movement ids
        -- must keep them.
        'movement_id',component.movement_id,
        'movement_group_id',component.movement_group_id
      ) order by component.component_ordinal
    ),'[]'::jsonb) into v_components
    from public.weekly_source_entitlement_head_components component
    where component.head_id=v_head.id;
  end if;

  v_request:=private.weekly_source_entitlement_proposal_request_v1(
    v_root_timesheet_id,v_final_revision_id,v_authority_kind,
    v_bundle.decision_bundle_id,v_bundle.bundle_revision,
    v_bundle.proposed_head_ids[1],v_bundle.decision_id,v_components
  );

  -- proof/32 section 2: the coordinator copies the actor "once from the
  -- immutable accepted Office decision" and never from the caller, so the real
  -- Office actor is stamped on the bundle row before the coordinator reads it.
  -- decision_id is a digest field and is deliberately NOT changed.
  update public.weekly_source_entitlement_decision_bundles
  set decided_by_user_id=v_actor
  where decision_bundle_id=v_bundle.decision_bundle_id
    and bundle_revision=v_bundle.bundle_revision
    and state='PROPOSED';

  v_result:=private.weekly_source_entitlement_publish_immediate_v1(v_request);
  v_published:=coalesce((v_result->>'published')::boolean,false);
  v_result:=v_result||pg_catalog.jsonb_build_object(
    'decision',v_decision,'retained',false,
    'decision_bundle_id',v_bundle.decision_bundle_id,
    'bundle_revision',v_bundle.bundle_revision,
    'authority_kind',v_authority_kind
  );

  perform public._audit_insert(
    'weekly_source_entitlement_decision_bundles',v_bundle.decision_bundle_id::text,
    'WEEKLY_SOURCE_LATER_CHANGE_DECIDED',null,
    pg_catalog.jsonb_build_object(
      'decision',v_decision,
      'outcome',case when v_published then 'PUBLISHED' else 'NOT_PUBLISHED' end,
      'bundle_revision',v_bundle.bundle_revision,
      'root_timesheet_id',v_root_timesheet_id,
      'final_revision_id',v_final_revision_id,
      'authority_kind',v_authority_kind,
      'component_count',pg_catalog.jsonb_array_length(v_components),
      'coordinator_code',v_result->>'code',
      'idempotency_key',v_idempotency_key,
      'request_hash',v_request_hash_hex,
      'result',v_result
    ),case when v_decision='APPROVE_UPDATED_HOURS'
      then 'WEEKLY_SOURCE_APPROVE_UPDATED_HOURS'
      else 'WEEKLY_SOURCE_KEEP_CURRENTLY_APPROVED_HOURS' end,v_actor
  );

  return v_result||pg_catalog.jsonb_build_object('idempotent_replay',false);
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_SOURCE_LATER_CHANGE_SCOPE_INVALID' using errcode='55000';
end;
$function$;

alter function public.weekly_exceptional_pay_prepare_action_v1(jsonb) owner to postgres;
alter function public.weekly_exceptional_pay_action_context_v1(jsonb) owner to postgres;
alter function public.weekly_source_later_change_decide_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_exceptional_pay_action_publication_status_v1(jsonb)
  owner to postgres;
alter function public.weekly_exceptional_pay_wait_atomic_v1(jsonb) owner to postgres;

revoke all on function public.weekly_exceptional_pay_prepare_action_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_exceptional_pay_action_context_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_exceptional_pay_action_publication_status_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_exceptional_pay_wait_atomic_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_later_change_decide_atomic_v1(jsonb)
  from public,anon,authenticated,service_role;
grant execute on function public.weekly_exceptional_pay_prepare_action_v1(jsonb)
  to service_role;
grant execute on function public.weekly_exceptional_pay_action_context_v1(jsonb)
  to service_role;
grant execute on function public.weekly_exceptional_pay_action_publication_status_v1(jsonb)
  to service_role;
grant execute on function public.weekly_exceptional_pay_wait_atomic_v1(jsonb)
  to service_role;
grant execute on function public.weekly_source_later_change_decide_atomic_v1(jsonb)
  to service_role;

comment on function public.weekly_exceptional_pay_prepare_action_v1(jsonb) is
  'Service-only start/replay owner for protected-hours AMEND, WITHDRAW, WAIT, RECONCILE and RECORD_NOT_WORKED. Accepts no financial or C1 facts.';
comment on function public.weekly_exceptional_pay_action_context_v1(jsonb) is
  'Service-only bounded source/root/provider context for the protected target composer and established Weekly calculator.';
comment on function public.weekly_exceptional_pay_action_publication_status_v1(jsonb) is
  'Service-only resolver for an orchestration run already staged into the durable C1 publication journal. It returns no financial facts.';
comment on function public.weekly_exceptional_pay_wait_atomic_v1(jsonb) is
  'Service-only audited WAIT decision. Proves the complete protected entitlement is unchanged and performs no C1, TSFIN, invoice, Draft or Banking write.';
comment on function public.weekly_source_later_change_decide_atomic_v1(jsonb) is
  'Plan 6.2 Gate 2 (G2-5; 24 section 4.2; 25 section 1; pack erratum E-5). The two later-change Office decisions, per root, as ONE complete entitlement decision: APPROVE_UPDATED_HOURS publishes the new complete entitlement as LOCKED_FINAL_SOURCE, KEEP_CURRENTLY_APPROVED_HOURS preserves the complete existing entitlement as PROTECTED. Service-only. It accepts no money, hours, head id or contract choice from the caller: everything economic is rebuilt server-side from the locked PROPOSED decision bundle and the root Timesheet. It makes exactly one call into the Gate 5 coordinator, which owns the serial gate, the rotation locks, the freeze census, the FROZEN pending-bundle branch, the single Workbench invalidation and the one immutable receipt. It writes no Banking Pay, Draft, invoice, reservation or residual row and never sets a Workbench session setting.';

notify pgrst, 'reload schema';

commit;
