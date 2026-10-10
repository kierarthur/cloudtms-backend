-- SP-06 mandatory WAIT permission only; full original no-economic-change/replay owner retained.
\set ON_ERROR_STOP on
begin;
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
  v_family_event_id uuid;
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
  v_completed_witness public.weekly_exceptional_orchestration_runs%rowtype;
  v_completed_replay boolean:=false;
  v_pending public.weekly_exceptional_pending_reconciliation_targets%rowtype;
  v_incident uuid;
  v_next_wait boolean;
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
  -- SP-06: actual payment authority is rechecked in this final transaction, including replay.
  perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY');
  -- Prepare is a separate RPC. A fresh Wait must independently enter the
  -- shared admission boundary before acquiring any business-row locks.
  -- Only an already completed, no-write return may bypass that boundary.
  select run.* into v_completed_witness
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_run_id and run.family_id=v_family_id
    and run.requested_by_user_id=v_actor and run.request_kind='WAIT'
    and run.state='COMPLETE' and run.completed_at_utc is not null
    and pg_catalog.isfinite(run.completed_at_utc)
    and pg_catalog.octet_length(run.after_state_fingerprint)=32
    and run.request_fingerprint is not null
    and run.before_state_fingerprint is not null;
  v_completed_replay:=found;
  if not v_completed_replay then
    perform private.weekly_source_pay_query_admit_v2();
  end if;
  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_family_id for update;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_run_id and run.family_id=v_family.id for update;
  if v_completed_replay and v_run is distinct from v_completed_witness then
    -- Never fall through to a write after skipping fresh admission.
    raise exception 'WEEKLY_PROTECTED_WAIT_SCOPE_INVALID' using errcode='55000';
  end if;
  if v_run.state='COMPLETE' then
    if v_run.request_kind<>'WAIT' or v_run.requested_by_user_id is distinct from v_actor
       or v_run.completed_at_utc is null or not pg_catalog.isfinite(v_run.completed_at_utc)
       or pg_catalog.octet_length(v_run.after_state_fingerprint) is distinct from 32
       or v_run.request_fingerprint is null or v_run.before_state_fingerprint is null then
      raise exception 'WEEKLY_PROTECTED_WAIT_SCOPE_INVALID' using errcode='55000';
    end if;
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
  select event_sequence,event_hash into v_sequence,v_prior_hash
  from public.weekly_exceptional_pay_family_events where family_id=v_family.id
  order by event_sequence desc limit 1;
  v_sequence:=coalesce(v_sequence,0)+1;
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
  ) returning id into v_family_event_id;
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.bpay_next_protected_current_decision_capture_v1(v_family_event_id);
  end if;
  select event_sequence,event_hash into v_target_sequence,v_prior_target_hash
  from public.weekly_exceptional_pay_target_events where family_id=v_family.id
  order by event_sequence desc limit 1;
  v_target_sequence:=coalesce(v_target_sequence,0)+1;
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
  v_next_wait:=v_family.target_domain_version='NEXT_V1';
  if v_next_wait then
    select incident.id into v_incident from public.weekly_discrepancy_incidents incident
      where incident.work_event_id=v_event_id and incident.candidate_id=v_family.candidate_id
        and incident.source_group_id=(select cycle.source_group_id from public.weekly_source_cycles cycle where cycle.id=v_cycle_id)
      order by incident.episode_number desc limit 1;
    select target.* into v_pending from public.weekly_exceptional_pending_reconciliation_targets target
      where target.family_id=v_family.id and target.approval_id=v_approval.id
        and target.source_action_policy_target_fingerprint=v_source_hash for update;
    if found and (v_pending.state<>'ACTIVE' or v_pending.completed_at_utc is not null
        or v_pending.durable_work_event_id<>v_event_id or v_pending.incident_id is distinct from v_incident
        or v_pending.current_final_revision_id is distinct from v_prior_event.current_final_revision_id
        or v_pending.intended_outcome<>'WAIT'
        or v_generation.complete_next_vector_hash<>v_expected_hash
        or v_prior_event.fixed_office_target_snapshot_json is distinct from p_request->'protected_schedule'
        or v_prior_event.source_proposal_snapshot_json is distinct from p_request->'source_proposal') then
      raise exception 'WEEKLY_PROTECTED_WAIT_PENDING_CONFLICT' using errcode='23514';
    end if;
    -- The existing partial UNIQUE(family_id,approval_id) WHERE ACTIVE bounds
    -- this transition to one row. Keep an exact existing ACTIVE key itself;
    -- do not erase audit history or disturb another protected event's wait.
    update public.weekly_exceptional_pending_reconciliation_targets target
      set state='SUPERSEDED',completed_at_utc=pg_catalog.statement_timestamp()
      where target.family_id=v_family.id and target.approval_id=v_approval.id and target.state='ACTIVE'
        and (v_pending.id is null or target.id<>v_pending.id);
    if v_pending.id is null then
      insert into public.weekly_exceptional_pending_reconciliation_targets(
        family_id,approval_id,durable_work_event_id,incident_id,current_final_revision_id,
        intended_outcome,source_action_policy_target_fingerprint,state)
      values(v_family.id,v_approval.id,v_event_id,v_incident,v_prior_event.current_final_revision_id,'WAIT',v_source_hash,'ACTIVE');
    end if;
  else
    update public.weekly_exceptional_pending_reconciliation_targets target
    set state='SUPERSEDED',completed_at_utc=pg_catalog.statement_timestamp()
    -- Wait is per existing work identity, not a decision for every sibling.
    where target.family_id=v_family.id and target.durable_work_event_id=v_event_id
      and target.state='ACTIVE'
      and not (target.approval_id=v_approval.id
        and target.source_action_policy_target_fingerprint=v_source_hash);
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
    )
    -- A newly requested Wait for the same proposal retains the same pending
    -- identity. Replays still return above; a new command gets its own audit.
    -- Only retain an exact already-active row. Never reopen any retired decision
    -- (including superseded), or reuse another work/incident/revision's row.
    on conflict (family_id,approval_id,source_action_policy_target_fingerprint)
    do update set state='ACTIVE',completed_at_utc=null
    where weekly_exceptional_pending_reconciliation_targets.durable_work_event_id=v_event_id
      and weekly_exceptional_pending_reconciliation_targets.incident_id
        is not distinct from excluded.incident_id
      and weekly_exceptional_pending_reconciliation_targets.intended_outcome='WAIT'
      and weekly_exceptional_pending_reconciliation_targets.current_final_revision_id
        is not distinct from v_prior_event.current_final_revision_id
      and weekly_exceptional_pending_reconciliation_targets.state='ACTIVE'
      and weekly_exceptional_pending_reconciliation_targets.completed_at_utc is null;
    if not found then
      raise exception 'WEEKLY_PROTECTED_WAIT_SCOPE_INVALID' using errcode='55000';
    end if;
  end if;
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

alter function public.weekly_exceptional_pay_wait_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_exceptional_pay_wait_atomic_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_wait_atomic_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
