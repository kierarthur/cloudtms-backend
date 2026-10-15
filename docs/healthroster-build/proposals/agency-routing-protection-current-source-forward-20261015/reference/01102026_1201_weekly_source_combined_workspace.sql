-- Repeatable CloudTMS function/view authority: weekly_source_combined_workspace
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Allocate only an independently owned source cycle. No source, Timesheet,
-- financial, query, notification or completion facts are manufactured here.
create or replace function public.weekly_source_client_cycle_resolve_atomic_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_client uuid;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_result uuid;
  v_mode text;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or jsonb_typeof(p_request)<>'object'
    or exists(select 1 from jsonb_object_keys(p_request) key
      where key not in ('actor_user_id','source_cycle_id','client_id')) then
    raise exception 'WEEKLY_SOURCE_CLIENT_CYCLE_REQUEST_INVALID' using errcode='22023';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_client:=(p_request->>'client_id')::uuid;
  select cycle.* into strict v_cycle from public.weekly_source_cycles cycle
    where cycle.id=(p_request->>'source_cycle_id')::uuid;
  select source_group.* into strict v_group from public.weekly_source_groups source_group
    where source_group.id=v_cycle.source_group_id;
  perform private.weekly_source_office_authority_v1(v_actor,'UPLOAD_SOURCE',
    v_group.id,v_client,v_cycle.finalisation_week_ending);
  select policy.authority_mode into v_mode from public.weekly_source_client_policies policy
    where policy.source_group_id=v_group.id and policy.client_id=v_client
      and v_cycle.finalisation_week_ending between policy.effective_from
        and coalesce(policy.effective_to,'infinity'::date)
    order by policy.effective_from desc,policy.id desc limit 1;
  if v_group.source_family<>'ROSTER' or v_mode is distinct from 'SOURCE_AUTHORITY' then
    if v_cycle.scope_client_id is not null then
      raise exception 'WEEKLY_SOURCE_CLIENT_CYCLE_AUTHORITY_MISMATCH' using errcode='22023';
    end if;
    return jsonb_build_object('ok',true,'source_cycle_id',v_cycle.id,'client_id',v_client);
  end if;
  if v_client is null then
    raise exception 'WEEKLY_SOURCE_CLIENT_SELECTION_REQUIRED' using errcode='22023';
  end if;
  perform pg_advisory_xact_lock(hashtextextended(
    'weekly-source-client-cycle:'||v_group.id::text||':'||v_client::text||':'||v_cycle.finalisation_week_ending::text,0));
  insert into public.weekly_source_cycles(source_group_id,finalisation_week_ending,
    cutoff_at_utc,scope_client_id)
  values(v_group.id,v_cycle.finalisation_week_ending,v_cycle.cutoff_at_utc,v_client)
  on conflict on constraint weekly_source_cycles_group_week_client_uq do nothing;
  select cycle.id into strict v_result from public.weekly_source_cycles cycle
    where cycle.source_group_id=v_group.id
      and cycle.finalisation_week_ending=v_cycle.finalisation_week_ending
      and cycle.scope_client_id=v_client;
  return jsonb_build_object('ok',true,'source_cycle_id',v_result,'client_id',v_client);
end;
$function$;

alter function public.weekly_source_client_cycle_resolve_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_client_cycle_resolve_atomic_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_client_cycle_resolve_atomic_v1(jsonb) to service_role;

-- Read-only editor authority. Contract qualification is independent of an
-- uploaded row, a Candidate submission, or an already-created Timesheet.
create or replace function public.weekly_source_protected_editor_context_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_client uuid;
  v_candidate uuid;
  v_date date;
  v_group_id uuid;
  v_groups integer;
  v_cycle uuid;
  v_contracts jsonb;
  v_client_name text;
  v_candidate_name text;
  v_events jsonb;
  v_event jsonb;
  v_event_id uuid;
  v_signed jsonb;
  v_final_source jsonb;
  v_history jsonb:='[]'::jsonb;
  v_pending boolean:=false;
  v_resume jsonb;
  v_original jsonb;
  v_original_hash bytea;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_request,
    array['actor_user_id','client_id','candidate_id','work_date','source_group_id','work_event_id'],
    'WEEKLY_PROTECTED_EDITOR_REQUEST_INVALID');
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_client:=(p_request->>'client_id')::uuid;
  v_candidate:=(p_request->>'candidate_id')::uuid;
  v_date:=(p_request->>'work_date')::date;
  v_event_id:=nullif(p_request->>'work_event_id','')::uuid;
  if v_client is null or v_candidate is null or v_date is null then
    raise exception 'WEEKLY_PROTECTED_EDITOR_SELECTION_REQUIRED' using errcode='22023';
  end if;
  perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY');
  select count(distinct source_group.id),min(source_group.id::text)::uuid
    into v_groups,v_group_id
  from public.weekly_source_groups source_group
  join public.weekly_source_group_clients membership on membership.source_group_id=source_group.id
  join public.weekly_source_client_policies policy on policy.source_group_id=source_group.id
    and policy.client_id=membership.client_id
  where source_group.active and membership.client_id=v_client
    and v_date between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)
    and v_date between policy.effective_from and coalesce(policy.effective_to,'infinity'::date)
    and policy.authority_mode='SOURCE_AUTHORITY'
    and (nullif(p_request->>'source_group_id','') is null
      or source_group.id=(p_request->>'source_group_id')::uuid);
  if v_groups<>1 then
    raise exception 'WEEKLY_PROTECTED_EDITOR_SOURCE_SCOPE_REQUIRED' using errcode='22023';
  end if;
  perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',v_group_id,v_client,v_date);
  select client.name into strict v_client_name from public.clients client where client.id=v_client;
  select coalesce(nullif(candidate.display_name,''),
    nullif(concat_ws(' ',candidate.first_name,candidate.last_name),''),candidate.tms_ref,'Candidate')
    into strict v_candidate_name from public.candidates candidate where candidate.id=v_candidate;
  select coalesce(jsonb_agg(jsonb_build_object(
      'id',contract.id,
      'label',concat_ws(' · ',nullif(contract.role,''),nullif(contract.band,''),nullif(contract.display_site,''),
        nullif(contract.ward_hint,''),to_char(contract.start_date,'DD Mon YYYY')||' – '||to_char(contract.end_date,'DD Mon YYYY')),
      'week_ending_date',v_date+((contract.week_ending_weekday_snapshot-extract(dow from v_date)::integer+7)%7)
    ) order by contract.role,contract.start_date,contract.id),'[]'::jsonb)
    into v_contracts
  from public.contracts contract
  cross join lateral (select private._weekly_source_effective_policy_v1(v_client,contract.id,v_date) value) effective
  join public.weekly_source_groups source_group on source_group.id=v_group_id
  where contract.client_id=v_client and contract.candidate_id=v_candidate
    and v_date between contract.start_date and contract.end_date
    and contract.week_ending_weekday_snapshot between 0 and 6
    and effective.value->>'authority_mode'='SOURCE_AUTHORITY'
    and coalesce((effective.value->>'self_bill_enabled')::boolean,false)
    and effective.value->>'c1_source_mode' in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
    and (source_group.source_family='NHSP')=(effective.value->>'c1_source_mode'='NHSP_WEEKLY');
  select cycle.id into v_cycle from public.weekly_source_cycles cycle
    where cycle.source_group_id=v_group_id
      and (cycle.scope_client_id is null or cycle.scope_client_id=v_client)
    order by (cycle.state<>'FINALISED') desc,cycle.finalisation_week_ending desc,cycle.scope_client_id nulls last,cycle.id limit 1;
  -- Read each distinct work identity, not one arbitrary same-day shift. The
  -- Office must choose the existing identity before changing its protected pay.
  select coalesce(jsonb_agg(jsonb_build_object(
    'work_event_id',event.id,'identity_kind',event.identity_kind,
    'source_hours',case when source_row.id is not null then
      to_char(source_row.start_at_local,'HH24:MI')||'–'||to_char(source_row.end_at_local,'HH24:MI')
        ||' · '||source_row.break_minutes::text||' min break' end,
    'candidate_hours',case when comparison.candidate_start_at_local is not null then
      to_char(comparison.candidate_start_at_local,'HH24:MI')||'–'||to_char(comparison.candidate_end_at_local,'HH24:MI')
        ||' · '||comparison.candidate_break_minutes::text||' min break' end,
    'candidate_timesheet_id',comparison.candidate_timesheet_id,
    'booking_reference',source_row.external_source_key,
    'candidate_sort',(select coalesce(nullif(candidate.last_name,''),candidate.display_name)
      from public.candidates candidate where candidate.id=v_candidate),
    'pay_query_open',query_location.source_cycle_id is not null,
    'query_source_group_id',case when query_location.source_cycle_id is not null then v_group_id end,
    'query_week_ending',query_cycle.finalisation_week_ending,
    'final_report_key',case when final_report.manifest_id is not null then 'FINAL:'||final_report.manifest_id::text end,
    'final_report_week_ending',final_report.week_ending,
    'finalise_source_cycle_id',case when source_row.id is not null
      and (source_profile.profile_code='NHSP_FINAL_BACKING_V1'
        or source_upload.file_metadata_json->>'import_use'='PREPARE_FINALISATION')
      then source_upload.source_cycle_id end,
    'finalise_report_scope_id',case when source_row.id is not null
      and (source_profile.profile_code='NHSP_FINAL_BACKING_V1'
        or source_upload.file_metadata_json->>'import_use'='PREPARE_FINALISATION')
      then source_upload.report_scope_id end,
    'finalise_week_ending',case when source_row.id is not null
      and (source_profile.profile_code='NHSP_FINAL_BACKING_V1'
        or source_upload.file_metadata_json->>'import_use'='PREPARE_FINALISATION')
      then source_cycle.finalisation_week_ending end,
    'family_id',family.id,'contract_id',coalesce(family.contract_id,resolution.contract_id,comparison.contract_id),
    'expected_family_bound_version',family.bound_version::text,
    'protected_state',protected.state,
    'start',to_char(coalesce(protected.start_at_local,comparison.candidate_start_at_local,source_row.start_at_local),'HH24:MI'),
    'end',to_char(coalesce(protected.end_at_local,comparison.candidate_end_at_local,source_row.end_at_local),'HH24:MI'),
    'source_start',to_char(source_row.start_at_local,'HH24:MI'),
    'source_end',to_char(source_row.end_at_local,'HH24:MI'),
    'candidate_start',to_char(comparison.candidate_start_at_local,'HH24:MI'),
    'candidate_end',to_char(comparison.candidate_end_at_local,'HH24:MI'),
    'protected_start',to_char(protected.start_at_local,'HH24:MI'),
    'protected_end',to_char(protected.end_at_local,'HH24:MI'),
    'break_minutes',coalesce(protected.break_minutes,comparison.candidate_break_minutes,source_row.break_minutes)
  ) order by event.id),'[]'::jsonb) into v_events
  from public.weekly_work_events event
  left join lateral (
    select item.* from public.weekly_work_event_source_links item
    join public.weekly_source_row_resolutions resolved on resolved.id=item.row_resolution_id
    join public.weekly_source_upload_rows linked_row on linked_row.id=item.upload_row_id
    join public.weekly_source_projection_publications publication on publication.upload_id=linked_row.upload_id
      and coalesce(publication.projection_generation,publication.authority_scope_version::integer)=resolved.generation
    where item.work_event_id=event.id and publication.state='CURRENT'
      and (exists(select 1 from public.weekly_source_cycles cycle
        where cycle.current_projection_publication_id=publication.id)
        or exists(select 1 from public.weekly_source_report_scopes scope
          where scope.current_projection_publication_id=publication.id))
    order by item.created_at_utc desc,item.id desc limit 1
  ) link on true
  left join public.weekly_source_upload_rows source_row on source_row.id=link.upload_row_id
  left join public.weekly_source_uploads source_upload on source_upload.id=source_row.upload_id
  left join public.weekly_source_cycles source_cycle on source_cycle.id=source_upload.source_cycle_id
  left join public.weekly_source_format_profiles source_profile on source_profile.id=source_upload.source_format_profile_id
  left join public.weekly_source_row_resolutions resolution on resolution.id=link.row_resolution_id
  left join lateral (
    select coalesce(
      (select review.source_cycle_id from private.weekly_source_manual_reviews review
        where review.source_group_id=v_group_id and review.work_event_id=event.id and review.state='OPEN'
        order by review.opened_at_utc desc,review.id desc limit 1),
      (select incident.source_cycle_id from public.weekly_discrepancy_incidents incident
        join public.weekly_source_cycles incident_cycle on incident_cycle.id=incident.source_cycle_id
        where incident_cycle.source_group_id=v_group_id and incident.work_event_id=event.id
          and incident.state='OPEN'
        order by incident.created_at_utc desc,incident.id desc limit 1)) source_cycle_id
  ) query_location on true
  left join public.weekly_source_cycles query_cycle on query_cycle.id=query_location.source_cycle_id
  left join lateral (
    select manifest.id manifest_id,manifest.finalisation_week_ending week_ending
    from public.weekly_source_client_manifests manifest
    join public.weekly_source_final_revisions revision on revision.id=manifest.final_revision_id
    where manifest.source_group_id=v_group_id and manifest.client_id=v_client
      and revision.state='CURRENT'
      and (exists(select 1 from public.weekly_source_final_snapshot_lines snapshot
            where snapshot.final_revision_id=revision.id and snapshot.work_event_id=event.id)
        or exists(select 1 from public.weekly_source_billing_movements movement
            where movement.final_revision_id=revision.id and movement.work_event_id=event.id))
    order by revision.finalised_at_utc desc,manifest.id desc limit 1
  ) final_report on true
  left join lateral (
    select revision.* from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions revision on revision.id=incident.current_comparison_revision_id
    where incident.work_event_id=event.id
    order by revision.created_at_utc desc,revision.id desc limit 1
  ) comparison on true
  left join lateral (
    select item.* from public.weekly_exceptional_pay_family_events item
    where item.durable_work_event_id=event.id
    order by item.occurred_at_utc desc,item.event_sequence desc,item.id desc limit 1
  ) protected on true
  left join public.weekly_exceptional_pay_target_families family on family.id=protected.family_id
  where event.candidate_id=v_candidate and event.client_id=v_client and event.work_date=v_date
    and event.first_source_group_id=v_group_id;
  if v_event_id is not null then
    select item into v_event from jsonb_array_elements(v_events) item where item->>'work_event_id'=v_event_id::text;
    if v_event is null then
      raise exception 'WEEKLY_PROTECTED_EDITOR_SHIFT_STALE' using errcode='PT409';
    end if;
    select family.current_generation_id is null
      and family.c1_publication_state in ('PENDING','PUBLISHING') into v_pending
    from public.weekly_exceptional_pay_target_families family
    where family.id=(v_event->>'family_id')::uuid;
    if v_pending then
      -- Resume only a retained first approval, never a guessed amendment or
      -- reconstructed financial target. The original actor and exact request
      -- fingerprint remain the existing preparation owner's authority.
      select approval.* into v_approval
      from public.weekly_exceptional_payment_approvals approval
      join public.weekly_exceptional_pay_family_events recorded
        on recorded.evidence_approval_id=approval.id
      where recorded.family_id=(v_event->>'family_id')::uuid
        and recorded.durable_work_event_id=v_event_id
        and recorded.event_sequence=1 and approval.withdrawn_at_utc is null;
      if found then
        select run.* into v_run from public.weekly_exceptional_orchestration_runs run
        where run.id=v_approval.creation_orchestration_run_id
          and run.family_id=(v_event->>'family_id')::uuid
          and run.request_kind='APPROVE' and run.state='RUNNING'
          and run.requested_by_user_id=v_actor
          and v_approval.approved_by_user_id=v_actor
          and v_approval.candidate_id=v_candidate and v_approval.client_id=v_client
          and v_approval.protected_work_date=v_date
          and exists(select 1 from public.weekly_source_cycles original_cycle
            where original_cycle.id=v_approval.source_cycle_id
              and original_cycle.source_group_id=v_group_id)
          and 1=(select count(*) from public.weekly_exceptional_c1_publication_requests publication
            where publication.orchestration_run_id=run.id and publication.family_id=run.family_id
              and publication.state='READY');
        if found then
          v_original:=jsonb_build_object('actor_user_id',v_actor,
            'source_cycle_id',v_approval.source_cycle_id,'candidate_id',v_candidate,
            'client_id',v_client,'contract_id',v_approval.contract_id,
            'week_ending_date',v_approval.week_ending::text,'work_event_id',v_event_id,
            'work_date',v_date::text,
            'start_at_local',to_char(v_approval.protected_start_at_local,'YYYY-MM-DD HH24:MI:SS'),
            'end_at_local',to_char(v_approval.protected_end_at_local,'YYYY-MM-DD HH24:MI:SS'),
            'break_minutes',v_approval.protected_break_minutes,
            'evidence_timesheet_id',v_approval.evidence_timesheet_id,
            'reason',v_approval.approval_reason,'idempotency_key',v_run.idempotency_key);
          v_original_hash:=private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_PROTECTED_PREPARE_FAMILY_REQUEST_V1',v_original);
          if v_original_hash is distinct from v_run.request_fingerprint then
            -- A source-absent first approval originally requested creation of
            -- its work identity. Only an exact hash permits this null variant.
            v_original:=v_original||jsonb_build_object('work_event_id',null);
            v_original_hash:=private.weekly_source_sha256_jsonb_v1(
              'WEEKLY_PROTECTED_PREPARE_FAMILY_REQUEST_V1',v_original);
          end if;
          if v_original_hash=v_run.request_fingerprint then
            perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',
              v_group_id,v_client,(select finalisation_week_ending from public.weekly_source_cycles
                where id=v_approval.source_cycle_id));
            v_resume:=(v_original-array['actor_user_id','start_at_local','end_at_local'])
              ||jsonb_build_object('start',to_char(v_approval.protected_start_at_local,'HH24:MI'),
                'end',to_char(v_approval.protected_end_at_local,'HH24:MI'));
          end if;
        end if;
      end if;
    end if;
    if nullif(v_event->>'family_id','') is not null and v_cycle is not null then
      v_final_source:=private.weekly_source_protected_final_source_context_v1(
        (v_event->>'family_id')::uuid,v_cycle,v_event_id);
    end if;
    -- Immutable approvals only: never reconstruct an Office action from an
    -- imported row or a candidate's unsigned schedule.
    select coalesce(jsonb_agg(jsonb_build_object(
      'event_id',history.id,'sequence',history.event_sequence,
      'at',history.occurred_at_utc,
      'by',coalesce(nullif(actor.display_name,''),'Office user'),
      'reason',history.office_reason,'state',history.state,
      'before',history.previous_schedule,
      'after',jsonb_build_object('start',to_char(history.start_at_local,'HH24:MI'),
        'end',to_char(history.end_at_local,'HH24:MI'),'break_minutes',history.break_minutes)
      ) order by history.event_sequence desc),'[]'::jsonb) into v_history
    from (
      select entry.*,lag(jsonb_build_object('start',to_char(entry.start_at_local,'HH24:MI'),
        'end',to_char(entry.end_at_local,'HH24:MI'),'break_minutes',entry.break_minutes))
        over(order by entry.event_sequence) previous_schedule
      from public.weekly_exceptional_pay_family_events entry
      where entry.family_id=(v_event->>'family_id')::uuid
        and entry.durable_work_event_id=v_event_id
    ) history
    left join public.tms_users actor on actor.id=history.office_actor_user_id;
    if nullif(v_event->>'candidate_timesheet_id','') is not null then
      begin
        v_signed:=private.weekly_exceptional_candidate_signed_evidence_v1((v_event->>'candidate_timesheet_id')::uuid);
      exception when sqlstate '55000' then v_signed:=null; end;
    end if;
  end if;
  return jsonb_build_object('ok',true,'contract','WEEKLY_PROTECTED_EDITOR_V1',
    'allowed',true,'source_group_id',v_group_id,'source_cycle_id',v_cycle,
    'source_family',(select source_family from public.weekly_source_groups where id=v_group_id),
    'client_id',v_client,'candidate_id',v_candidate,'work_date',v_date,
    'client',v_client_name,'candidate',v_candidate_name,'contracts',v_contracts,
    'events',v_events,'work_event_id',v_event_id,
    'family_id',v_event->>'family_id','expected_family_bound_version',v_event->>'expected_family_bound_version',
    'protected_state',v_event->>'protected_state','shift_contract_id',v_event->>'contract_id',
    'pending_approval',coalesce(v_pending,false),'resume_request',v_resume,
    'can_reconcile',coalesce((v_final_source->>'source_observed')::boolean,false),
    'final_source_proposal',v_final_source->'source_proposal',
    'history',v_history,
    'candidate_hours',coalesce(v_event->>'candidate_hours',case when v_event_id is null and jsonb_array_length(v_events)>0 then 'Choose a shift to inspect its submission' else 'No candidate hours recorded for this shift' end),
    'source_hours',coalesce(v_event->>'source_hours',case when v_event_id is null and jsonb_array_length(v_events)>0 then 'Choose a shift to inspect its source' else 'Not present in the current import' end),
    'evidence_timesheet_id',v_signed->>'timesheet_id','signed_evidence_available',v_signed is not null,
    'current_schedule',jsonb_build_object('start',v_event->>'start','end',v_event->>'end','break_minutes',v_event->'break_minutes'),
    'create_contract_seed',jsonb_build_object('client_id',v_client,'candidate_id',v_candidate));
end;
$function$;

-- Explicit editor preparation allocates only source-cycle context. It neither
-- creates a Timesheet nor approves pay; those remain with the existing owner.
create or replace function public.weekly_source_protected_editor_prepare_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_context jsonb;
  v_cycle uuid;
  v_scoped jsonb;
begin
  v_context:=public.weekly_source_protected_editor_context_v1(p_request);
  if coalesce((v_context->>'pending_approval')::boolean,false) then
    if v_context->'resume_request' is null or v_context->'resume_request'='null'::jsonb then
      raise exception 'WEEKLY_PROTECTED_PENDING_REQUEST_UNAVAILABLE' using errcode='PT409';
    end if;
    -- A replay must retain its original period and key. Allocating a newer
    -- open cycle here changes the fingerprint and cannot resume that Save.
    return v_context||jsonb_build_object('source_cycle_id',v_context->'resume_request'->>'source_cycle_id');
  end if;
  v_cycle:=private._weekly_source_settings_ensure_open_cycle_v1((v_context->>'source_group_id')::uuid);
  v_scoped:=public.weekly_source_client_cycle_resolve_atomic_v1(jsonb_build_object(
    'actor_user_id',p_request->>'actor_user_id','source_cycle_id',v_cycle,'client_id',p_request->>'client_id'));
  return v_context||jsonb_build_object('source_cycle_id',v_scoped->>'source_cycle_id');
end;
$function$;

alter function public.weekly_source_protected_editor_context_v1(jsonb) owner to postgres;
alter function public.weekly_source_protected_editor_prepare_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_protected_editor_context_v1(jsonb) from public,anon,authenticated;
revoke all on function public.weekly_source_protected_editor_prepare_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_protected_editor_context_v1(jsonb) to service_role;
grant execute on function public.weekly_source_protected_editor_prepare_v1(jsonb) to service_role;

-- Completed display reads immutable finalisation evidence, never the latest
-- provisional rows. No financial calculation or permission is introduced.
create or replace function private.weekly_source_completed_rows_v1(
  p_cycle_id uuid,p_client_id uuid default null,p_report_scope_id uuid default null
) returns jsonb language sql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
  with completed as (
    select snapshot.id,revision.id final_revision_id,snapshot.candidate_id,snapshot.client_id,
      snapshot.work_date,snapshot.start_at_local,snapshot.end_at_local,snapshot.break_minutes,
      revision.finalised_at_utc,'Finalised'::text outcome
    from public.weekly_source_final_revisions revision
    join public.weekly_source_final_snapshot_lines snapshot on snapshot.final_revision_id=revision.id
    where revision.source_cycle_id=p_cycle_id and revision.state='CURRENT'
      and revision.authority_scope_kind='CYCLE'
      and (p_client_id is null or snapshot.client_id=p_client_id)
      and p_report_scope_id is null
    union all
    select movement.id,revision.id,movement.candidate_id,movement.actual_client_id,
      source_row.work_date,source_row.start_at_local,source_row.end_at_local,source_row.break_minutes,
      revision.finalised_at_utc,'Finalised'
    from public.weekly_source_final_revisions revision
    join public.weekly_source_billing_movements movement on movement.final_revision_id=revision.id
    join public.weekly_source_upload_rows source_row on source_row.id=movement.nhsp_upload_row_id
    where revision.source_cycle_id=p_cycle_id and revision.state='CURRENT'
      and revision.authority_scope_kind='NHSP_REPORT_SCOPE'
      and (p_client_id is null or movement.actual_client_id=p_client_id)
      and (p_report_scope_id is null or revision.report_scope_id=p_report_scope_id)
  )
  select jsonb_build_object('rows',coalesce(jsonb_agg(jsonb_build_object(
    'row_key','completed-'||completed.id::text,'final_revision_id',completed.final_revision_id,
    'candidate',coalesce(candidate.display_name,candidate.tms_ref,'Candidate'),
    'candidate_sort',coalesce(nullif(candidate.last_name,''),candidate.display_name,candidate.tms_ref,''),
    'work_date',completed.work_date,
    'client',client.name,'day_date',to_char(completed.work_date,'Dy FMDD Mon YYYY'),
    'system_hours',to_char(completed.start_at_local,'HH24:MI')||'–'||to_char(completed.end_at_local,'HH24:MI')
      ||' · '||completed.break_minutes::text||' min break',
    'finalised_at',to_char(completed.finalised_at_utc at time zone 'Europe/London','FMDD Mon YYYY, HH24:MI'),
    'finalised_at_utc',completed.finalised_at_utc,
    'status',jsonb_build_object('text',completed.outcome,'tone','positive'),'actions','[]'::jsonb
    ) order by client.name,candidate.last_name,candidate.first_name,completed.work_date,completed.id),'[]'::jsonb),
    'total_count',count(*),'next_cursor','','has_more',false,'stale',false)
  from completed
  join public.clients client on client.id=completed.client_id
  join public.candidates candidate on candidate.id=completed.candidate_id;
$function$;
alter function private.weekly_source_completed_rows_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_completed_rows_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

-- Identity suggestions are factual only. Different paid hours do not reject a
-- match. More than one plausible protected shift requires Office selection.
create or replace function private.weekly_source_protected_match_candidates_v1(
  p_upload_row_id uuid,p_candidate_id uuid,p_client_id uuid
) returns jsonb language sql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
  select coalesce(jsonb_agg(jsonb_build_object(
    'work_event_id',event.id,'contract_id',family.contract_id,
    'start',to_char(protected.start_at_local,'HH24:MI'),'end',to_char(protected.end_at_local,'HH24:MI'),
    'break_minutes',protected.break_minutes,'protected_state',protected.state,
    'schedule_compatible',protected.start_at_local<source_row.end_at_local
      and source_row.start_at_local<protected.end_at_local,
    'retained_source_identity',exists(
      select 1 from public.weekly_work_event_source_links retained
      join public.weekly_source_upload_rows previous on previous.id=retained.upload_row_id
      join public.weekly_source_uploads previous_upload on previous_upload.id=previous.upload_id
      where retained.work_event_id=event.id
        and previous_upload.source_format_profile_id=upload.source_format_profile_id
        and previous.work_date=source_row.work_date
        and nullif(previous.external_source_key,'')=nullif(source_row.external_source_key,'')
    )
  ) order by protected.start_at_local,event.id),'[]'::jsonb)
  from public.weekly_source_upload_rows source_row
  join public.weekly_source_uploads upload on upload.id=source_row.upload_id
  join public.weekly_source_cycles cycle on cycle.id=upload.source_cycle_id
  join public.weekly_work_events event on event.candidate_id=p_candidate_id and event.client_id=p_client_id
    and event.work_date=source_row.work_date and event.first_source_group_id=cycle.source_group_id
  join lateral (select item.* from public.weekly_exceptional_pay_family_events item
    where item.durable_work_event_id=event.id
    order by item.occurred_at_utc desc,item.event_sequence desc,item.id desc limit 1) protected on true
  join public.weekly_exceptional_pay_target_families family on family.id=protected.family_id
  join public.contracts contract on contract.id=family.contract_id
    and contract.candidate_id=p_candidate_id and contract.client_id=p_client_id
    and source_row.work_date between contract.start_date and coalesce(contract.end_date,'infinity'::date)
  where source_row.id=p_upload_row_id
    and (private._weekly_source_effective_policy_v1(p_client_id,contract.id,source_row.work_date)->>'authority_mode')='SOURCE_AUTHORITY';
$function$;
alter function private.weekly_source_protected_match_candidates_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_protected_match_candidates_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

create or replace function private.weekly_source_protected_query_rows_v1(
  p_group_id uuid,p_client_id uuid,p_can_act boolean
) returns jsonb language sql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
  with items as (
    select distinct on (event.family_id,event.durable_work_event_id)
      event.*,family.contract_id,family.candidate_id,family.bound_version,contract.client_id,
      candidate.display_name candidate_name,client.name client_name
    from public.weekly_exceptional_pay_family_events event
    join public.weekly_exceptional_pay_target_families family on family.id=event.family_id
    join public.contracts contract on contract.id=family.contract_id
    join public.candidates candidate on candidate.id=family.candidate_id
    join public.clients client on client.id=contract.client_id
    join public.weekly_work_events work on work.id=event.durable_work_event_id
    where work.first_source_group_id=p_group_id and (p_client_id is null or contract.client_id=p_client_id)
    order by event.family_id,event.durable_work_event_id,event.event_sequence desc
  ), rows as (
      select jsonb_build_object('row_key','protected-'||item.family_id::text||':'||item.durable_work_event_id::text,
        'family_id',item.family_id,'work_event_id',item.durable_work_event_id,
        'requires_attention',decision.needs_review,'pay_blocking',false,
        'client_id',item.client_id,'work_date',item.work_date,
        'candidate_sort',(select coalesce(nullif(person.last_name,''),person.display_name) from public.candidates person where person.id=item.candidate_id),
      'candidate',item.candidate_name,'client',item.client_name,'day_date',to_char(item.work_date,'Dy FMDD Mon YYYY'),
      'protected_hours',to_char(item.start_at_local,'HH24:MI')||'–'||to_char(item.end_at_local,'HH24:MI')||' · '||item.break_minutes::text||' min break',
      'status',jsonb_build_object('text',case when decision.needs_review
        then 'Ready to reconcile' else 'Protected pay — awaiting source' end,'tone','warning'),
      'actions',jsonb_build_array(jsonb_build_object('label','Change protected shift','enabled',p_can_act,'payload',payload.value),
        jsonb_build_object('label',case when decision.needs_review then 'Review and reconcile'
          else 'Review protected pay' end,'enabled',p_can_act,'payload',payload.value))
    ) value from items item
    cross join lateral (select jsonb_build_object('source_group_id',p_group_id,
      'client_id',item.client_id,'candidate_id',item.candidate_id,'work_date',item.work_date,
      'family_id',item.family_id,'work_event_id',item.durable_work_event_id,
      'expected_family_bound_version',item.bound_version::text,'contract_id',item.contract_id) value) payload
    left join lateral (
      select cycle.id from public.weekly_source_cycles cycle
      where cycle.source_group_id=p_group_id and (cycle.scope_client_id is null or cycle.scope_client_id=item.client_id)
      order by (cycle.state<>'FINALISED') desc,cycle.finalisation_week_ending desc,cycle.scope_client_id nulls last,cycle.id limit 1
    ) action_cycle on true
    left join lateral (select private.weekly_source_protected_final_source_context_v1(
      item.family_id,action_cycle.id,item.durable_work_event_id) value
      where action_cycle.id is not null and item.state='WAIT') source on true
    cross join lateral (select coalesce((source.value->>'source_observed')::boolean,false)
      and (source.value#>'{source_proposal,selected_work_event_id}'
        is distinct from item.source_proposal_snapshot_json->'selected_work_event_id'
        or source.value#>'{source_proposal,source_present}'
          is distinct from item.source_proposal_snapshot_json->'source_present'
        or source.value#>'{source_proposal,source_revision}'
          is distinct from item.source_proposal_snapshot_json->'source_revision'
        or source.value#>'{source_proposal,source_hash}'
          is distinct from item.source_proposal_snapshot_json->'source_hash') needs_review) decision
    where item.state='WAIT'
  ) select jsonb_build_object('rows',coalesce(jsonb_agg(value),'[]'::jsonb),'total_count',count(*),
    'next_cursor','','has_more',false,'stale',false) from rows;
$function$;
alter function private.weekly_source_protected_query_rows_v1(uuid,uuid,boolean) owner to postgres;
revoke all on function private.weekly_source_protected_query_rows_v1(uuid,uuid,boolean) from public,anon,authenticated,service_role;

-- Enumerate independent client/period authorities before rendering a combined
-- view. This does not choose a client for an upload or confer finalise authority.
create or replace function public.weekly_source_workspace_scopes_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_group uuid;
  v_client uuid;
  v_week date;
  v_limit integer:=100;
  v_offset integer:=0;
  v_cursor jsonb;
  v_rows jsonb;
  v_version text;
  v_total integer;
  v_missing integer;
  v_pending integer;
  v_cutoffs jsonb;
  v_monday date:=(date_trunc('week',statement_timestamp() at time zone 'Europe/London'))::date;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_request,
    array['actor_user_id','source_group_id','client_id','week_ending','cursor','limit'],
    'WEEKLY_SOURCE_SCOPE_REQUEST_INVALID');
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_group:=nullif(p_request->>'source_group_id','')::uuid;
  v_client:=nullif(p_request->>'client_id','')::uuid;
  v_week:=nullif(p_request->>'week_ending','')::date;
  v_limit:=coalesce((p_request->>'limit')::integer,100);
  if v_limit not between 1 and 100 then
    raise exception 'WEEKLY_SOURCE_SCOPE_REQUEST_INVALID' using errcode='22023';
  end if;
  perform private.weekly_source_office_authority_v1(v_actor,'VIEW_SOURCE_PROGRESS',v_group);
  if nullif(p_request->>'cursor','') is not null then
    begin
      v_cursor:=convert_from(decode(p_request->>'cursor','base64'),'UTF8')::jsonb;
      v_offset:=(v_cursor->>'offset')::integer;
      if v_offset is null or v_offset<0 then raise exception 'invalid offset'; end if;
    exception when others then
      raise exception 'WEEKLY_SOURCE_WORKSPACE_CURSOR_STALE' using errcode='40001';
    end;
  end if;
  with candidates as (
    select distinct on (source_group.id,membership.client_id,cycle.finalisation_week_ending)
      source_group.id group_id,source_group.display_name source_name,source_group.source_family,
      membership.client_id,client.name client_name,cycle.id cycle_id,
      cycle.scope_client_id,
      cycle.finalisation_week_ending,cycle.cutoff_at_utc,cycle.version cycle_version,
      cycle.current_complete_upload_id,cycle.current_projection_publication_id
    from public.weekly_source_groups source_group
    join public.weekly_source_cycles cycle on cycle.source_group_id=source_group.id
    join public.weekly_source_group_clients membership on membership.source_group_id=source_group.id
      and cycle.finalisation_week_ending between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)
      and (cycle.scope_client_id is null or cycle.scope_client_id=membership.client_id)
    join public.clients client on client.id=membership.client_id
    where source_group.active and (v_group is null or source_group.id=v_group)
      and (v_client is null or membership.client_id=v_client)
      and (v_week is null or cycle.finalisation_week_ending=v_week)
      and exists(select 1 from public.weekly_source_client_policies policy
        where policy.source_group_id=source_group.id and policy.client_id=membership.client_id
          and policy.authority_mode='SOURCE_AUTHORITY'
          and cycle.finalisation_week_ending between policy.effective_from and coalesce(policy.effective_to,'infinity'::date))
    order by source_group.id,membership.client_id,cycle.finalisation_week_ending,
      (cycle.scope_client_id is not null) desc,cycle.id
  ), scopes as (
    select candidate.*,report.id report_scope_id,report.version report_version,
      case when candidate.source_family='NHSP' then report.current_complete_upload_id
        when candidate.scope_client_id=candidate.client_id then candidate.current_complete_upload_id end upload_id,
      case when candidate.source_family='NHSP' then report.current_projection_publication_id
        when candidate.scope_client_id=candidate.client_id then candidate.current_projection_publication_id end publication_id,
      coalesce(final_report.id,completion.id) completion_id,
      case when final_report.id is not null then 'FINAL_SOURCE' else completion.completion_kind end completion_kind,
      coalesce(final_report.finalised_at_utc,completion.attested_at_utc) completed_at,
      encode(coalesce(final_report.manifest_hash,completion.completion_hash),'hex') completion_hash,
      coalesce(report.cutoff_at_utc,candidate.cutoff_at_utc) actual_cutoff
    from candidates candidate
    left join public.weekly_source_report_scopes report on report.source_cycle_id=candidate.cycle_id
      and report.client_id=candidate.client_id and candidate.source_family='NHSP'
    left join public.weekly_source_client_cycle_completions completion
      on completion.source_cycle_id=candidate.cycle_id and completion.client_id=candidate.client_id
      and completion.state='CURRENT' and completion.completion_kind='NO_SHIFTS_TO_IMPORT'
    -- Completion belongs to this exact client/report. A completed sibling
    -- report for the same client/week must not hide another pending report.
    left join lateral (
      select revision.id,revision.finalised_at_utc,revision.manifest_hash
      from public.weekly_source_final_revisions revision
      join public.weekly_source_client_manifests manifest on manifest.final_revision_id=revision.id
        and manifest.client_id=candidate.client_id
      where revision.source_cycle_id=candidate.cycle_id and revision.state='CURRENT'
        and revision.report_scope_id is not distinct from report.id
      order by revision.revision_number desc,revision.id limit 1
    ) final_report on true
  ), facts as (
    select scopes.*,private.weekly_source_import_is_prepared_v1(scopes.upload_id) prepared
    from scopes
  ), ordered as (
    select facts.*,bool_or(prepared) over(partition by cycle_id,client_id) client_has_prepared,
      row_number() over(order by private.weekly_source_query_ascii_fold_v1(client_name) collate "C",
      client_id,finalisation_week_ending desc,group_id,report_scope_id nulls first) ordinal
    from facts
  )
  select count(*)::integer,
    count(distinct (cycle_id,client_id)) filter(where completion_id is null and not client_has_prepared
      and actual_cutoff<(v_monday::timestamp at time zone 'Europe/London'))::integer,
    count(distinct (cycle_id,client_id)) filter(where completion_id is null and prepared)::integer,
    encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_WORKSPACE_SCOPES_V1',
      jsonb_build_object('filter',p_request-'cursor'-'limit','rows',coalesce(jsonb_agg(jsonb_build_array(
        cycle_id,client_id,cycle_version,report_scope_id,report_version,upload_id,publication_id,completion_hash,prepared)
        order by ordinal),'[]'::jsonb))),'hex'),
    coalesce(jsonb_agg(jsonb_build_object(
      'key',cycle_id::text||':'||client_id::text||':'||coalesce(report_scope_id::text,'CYCLE'),
      'source_group_id',group_id,'source_cycle_id',cycle_id,'report_scope_id',report_scope_id,
      'client_id',client_id,'client',client_name,'source',source_name,'source_family',source_family,
      'week_ending',finalisation_week_ending,'period',to_char(finalisation_week_ending,'FMDD Mon YYYY'),
      'cutoff',actual_cutoff,'upload_id',upload_id,'projection_publication_id',publication_id,
      'prepared',prepared,'completed',completion_id is not null,'completion_kind',completion_kind,
      'completed_at',completed_at,
      'missing_previous_report',completion_id is null and not client_has_prepared
        and actual_cutoff<(v_monday::timestamp at time zone 'Europe/London')
      ) order by ordinal) filter(where ordinal>v_offset and ordinal<=v_offset+v_limit),'[]'::jsonb)
    into v_total,v_missing,v_pending,v_version,v_rows from ordered;
  if v_cursor is not null and v_cursor->>'version' is distinct from v_version then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_CURSOR_STALE' using errcode='40001';
  end if;
  select coalesce(jsonb_agg(jsonb_build_object('source_group_id',source_group.id,
    'source',source_group.display_name,'cutoff',deadline.value,
    'passed',statement_timestamp()>=deadline.value) order by source_group.display_name,source_group.id),'[]'::jsonb)
  into v_cutoffs from public.weekly_source_groups source_group
  cross join lateral(select ((v_monday+((source_group.cutoff_weekday+6)%7))
    +source_group.cutoff_local_time) at time zone source_group.timezone value) deadline
  where source_group.active and (v_group is null or source_group.id=v_group)
    and exists(select 1 from public.weekly_source_group_clients membership
      join public.weekly_source_client_policies policy on policy.source_group_id=source_group.id
        and policy.client_id=membership.client_id and policy.authority_mode='SOURCE_AUTHORITY'
      where membership.source_group_id=source_group.id and (v_client is null or membership.client_id=v_client)
        and (statement_timestamp() at time zone source_group.timezone)::date between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)
        and (statement_timestamp() at time zone source_group.timezone)::date between policy.effective_from and coalesce(policy.effective_to,'infinity'::date));
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_WORKSPACE_SCOPES_V1',
    'rows',v_rows,'total_count',v_total,'version',v_version,
    'missing_previous_reports',v_missing,'reports_awaiting_finalisation',v_pending,
    'this_week_cutoffs',v_cutoffs,'has_more',v_offset+v_limit<v_total,
    'next_cursor',case when v_offset+v_limit<v_total then
      encode(convert_to(jsonb_build_object('offset',v_offset+v_limit,'version',v_version)::text,'UTF8'),'base64') else '' end);
end;
$function$;
alter function public.weekly_source_workspace_scopes_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_workspace_scopes_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_workspace_scopes_v1(jsonb) to service_role;

-- Combined finalisation is a read composition of existing per-report owners.
-- The batch UI returns each exact finalise payload to its existing mutator;
-- this reader cannot merge client economics or approve blocked siblings.
create or replace function public.weekly_source_combined_finalise_workspace_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_scope_request jsonb;
  v_scope_page jsonb;
  v_scopes jsonb:='[]'::jsonb;
  v_scope jsonb;
  v_workspace jsonb;
  v_plans jsonb:='[]'::jsonb;
  v_obligations jsonb:='[]'::jsonb;
  v_rows jsonb:='[]'::jsonb;
  v_page jsonb;
  v_versions jsonb:='[]'::jsonb;
  v_phase text;
  v_sort text;
  v_direction text;
  v_seek text;
  v_limit integer;
  v_offset integer:=0;
  v_cursor jsonb;
  v_version text;
  v_counts jsonb;
  v_total integer;
  v_meta jsonb;
  v_seek_base integer:=0;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_request,
    array['actor_user_id','source_group_id','client_id','week_ending','list','sort_key',
      'sort_direction','seek','cursor','limit'],'WEEKLY_SOURCE_COMBINED_REQUEST_INVALID');
  v_scope_request:=p_request-'list'-'sort_key'-'sort_direction'-'seek'-'cursor'-'limit';
  v_phase:=coalesce(nullif(p_request->>'list',''),'ready');
  v_sort:=coalesce(nullif(p_request->>'sort_key',''),case when nullif(p_request->>'client_id','') is null then 'client' else 'candidate' end);
  v_direction:=coalesce(nullif(p_request->>'sort_direction',''),'asc');
  v_seek:=private.weekly_source_query_ascii_fold_v1(coalesce(p_request->>'seek',''));
  v_limit:=coalesce((p_request->>'limit')::integer,50);
  if v_phase not in ('ready','blocked','complete') or v_sort not in
    ('client','source','period','candidate','day_date','status','system_hours','movement','commission','total_cost','invoice_charge','finalised_at')
    or v_direction not in ('asc','desc') or v_limit not between 1 and 100 or length(v_seek)>100 then
    raise exception 'WEEKLY_SOURCE_COMBINED_REQUEST_INVALID' using errcode='22023';
  end if;
  loop
    v_scope_page:=public.weekly_source_workspace_scopes_v1(v_scope_request);
    v_meta:=v_scope_page-'rows'-'next_cursor'-'has_more';
    v_scopes:=v_scopes||(v_scope_page->'rows');
    exit when not (v_scope_page->>'has_more')::boolean;
    v_scope_request:=v_scope_request||jsonb_build_object('cursor',v_scope_page->>'next_cursor');
  end loop;
  for v_scope in select item from jsonb_array_elements(v_scopes) item
  loop
    -- Completed report evidence lives in History; outstanding approved-hours
    -- work has its independent Office-check projection in Queries.
    if coalesce((v_scope->>'completed')::boolean,false) then continue; end if;
    v_workspace:=public.weekly_source_office_workspace_v1(jsonb_build_object(
      'actor_user_id',p_request->>'actor_user_id','tab','finalise',
      'source_group_id',v_scope->>'source_group_id','source_cycle_id',v_scope->>'source_cycle_id',
      'client_id',v_scope->>'client_id','report_scope_id',v_scope->>'report_scope_id'));
    v_versions:=v_versions||jsonb_build_array(jsonb_build_array(v_scope->>'key',v_workspace->>'workspace_version'));
    v_obligations:=v_obligations||jsonb_build_array(v_scope||jsonb_build_object('scope',v_workspace->'selected',
      'progress',coalesce((select tracker from jsonb_array_elements(v_workspace#>'{finalise,tracker,rows}') tracker
        where tracker->>'row_key'='tracker-'||(v_scope->>'client_id')),'{}'::jsonb)));
    if not (v_scope->>'prepared')::boolean and not (v_scope->>'completed')::boolean then continue; end if;
    v_plans:=v_plans||jsonb_build_array(jsonb_build_object(
      'key',v_scope->>'key','client',v_scope->>'client','period',v_scope->>'period',
      'week_ending',v_scope->>'week_ending',
      'missing_previous_report',v_scope->'missing_previous_report',
      'progress',coalesce((select tracker from jsonb_array_elements(v_workspace#>'{finalise,tracker,rows}') tracker
        where tracker->>'row_key'='tracker-'||(v_scope->>'client_id')),'{}'::jsonb),
      'source',v_scope->>'source','scope',v_workspace->'selected',
      'prepared',v_workspace#>'{finalise,prepared}','completed',v_scope->'completed',
      'finalise_enabled',v_workspace#>'{finalise,finalise_enabled}',
      'blocked_count',v_workspace#>'{finalise,blocked,total_count}',
      'ready_count',v_workspace#>'{finalise,ready,total_count}',
      'complete_count',v_workspace#>'{finalise,complete,total_count}',
      'finalise_payload',v_workspace#>'{finalise,finalise_payload}',
      'exclusion_confirmation',v_workspace#>'{finalise,exclusion_confirmation}',
      'excluded_rows',v_workspace#>'{finalise,excluded_rows}',
      'rate_warnings',v_workspace#>'{finalise,rate_warnings}',
      'approved_hours_follow_up',v_workspace#>'{finalise,approved_hours_follow_up}'));
    select v_rows||coalesce(jsonb_agg(item||jsonb_build_object(
      'scope_key',v_scope->>'key','client',v_scope->>'client','source',v_scope->>'source',
      'period',v_scope->>'period','profile_id',v_workspace#>>'{profile,id}')),'[]'::jsonb)
    into v_rows from jsonb_array_elements(coalesce(v_workspace#>array['finalise',v_phase,'rows'],'[]'::jsonb)) item;
  end loop;
  v_version:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_COMBINED_FINALISE_V1',
    jsonb_build_object('scopes_version',v_meta->>'version','reports',v_versions,
      'filter',p_request-'cursor'-'limit')),'hex');
  if nullif(p_request->>'cursor','') is not null then
    begin
      v_cursor:=convert_from(decode(p_request->>'cursor','base64'),'UTF8')::jsonb;
      v_offset:=(v_cursor->>'offset')::integer;
      if v_offset is null or v_offset<0 or v_cursor->>'version' is distinct from v_version then
        raise exception 'stale cursor';
      end if;
    exception when others then
      raise exception 'WEEKLY_SOURCE_WORKSPACE_CURSOR_STALE' using errcode='40001';
    end;
  end if;
  with keyed as (
    select item,private.weekly_source_query_ascii_fold_v1(coalesce(case v_sort
      when 'candidate' then item->>'candidate_sort' when 'client' then item->>'client'
      when 'day_date' then item->>'work_date' when 'status' then item#>>'{status,text}'
      when 'finalised_at' then item->>'finalised_at_utc' else item->>v_sort end,'')) sort_value,
      case when v_sort in ('commission','total_cost','invoice_charge')
        then nullif(regexp_replace(item->>v_sort,'[^0-9.\-]','','g'),'')::numeric end sort_amount
    from jsonb_array_elements(v_rows) item
  ), ordered as (
    select *,row_number() over(order by
      case when v_direction='asc' then sort_amount end asc nulls last,
      case when v_direction='desc' then sort_amount end desc nulls last,
      case when v_direction='asc' then sort_value end collate "C" asc,
      case when v_direction='desc' then sort_value end collate "C" desc,
      private.weekly_source_query_ascii_fold_v1(item->>'client') collate "C",
      private.weekly_source_query_ascii_fold_v1(item->>'candidate_sort') collate "C",
      item->>'work_date',item->>'scope_key',item->>'row_key') ordinal
    from keyed
  ), origin as (
    select coalesce(min(ordinal) filter(where v_seek<>'' and starts_with(sort_value,v_seek)),1)-1 base
    from ordered
  )
  select count(*)::integer,coalesce(max(origin.base),0)::integer,coalesce(jsonb_agg(item order by ordinal)
    filter(where ordinal>origin.base+v_offset and ordinal<=origin.base+v_offset+v_limit),'[]'::jsonb)
    into v_total,v_seek_base,v_page from ordered cross join origin;
  -- Counts remain whole-result counts even after a type-to-jump seek.
  select jsonb_build_object('ready',count(*) filter(where item->>'finalise_enabled'='true' and (item->>'blocked_count')::integer=0),
    'blocked',count(*) filter(where item->>'finalise_enabled' is distinct from 'true' or coalesce((item->>'blocked_count')::integer,0)>0),
    'complete',0) into v_counts
  from jsonb_array_elements(v_plans) item;
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_COMBINED_FINALISE_V1',
    'version',v_version,'scopes',v_plans,'obligations',v_obligations,'scope_options',v_scopes,'summary',v_meta,'counts',v_counts,'list',v_phase,
    'rows',v_page,'total_count',v_total,'sort_key',v_sort,'sort_direction',v_direction,
    'has_more',v_seek_base+v_offset+v_limit<v_total,
    'next_cursor',case when v_seek_base+v_offset+v_limit<v_total then
      encode(convert_to(jsonb_build_object('offset',v_offset+v_limit,'version',v_version)::text,'UTF8'),'base64') else '' end);
end;
$function$;
alter function public.weekly_source_combined_finalise_workspace_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_combined_finalise_workspace_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_combined_finalise_workspace_v1(jsonb) to service_role;

-- Combined checking is read-only. Exact actions and their version proofs are
-- retained from each existing cycle owner, never rebuilt from display labels.
create or replace function public.weekly_source_combined_review_workspace_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_request jsonb; v_scope_page jsonb; v_scopes jsonb:='[]'; v_scope jsonb;
  v_workspace jsonb; v_owner_request jsonb; v_rows jsonb:='[]'; v_item jsonb;
  v_manual_children jsonb; v_question_children jsonb; v_child jsonb;
  v_follow_workspace jsonb; v_follow_up jsonb;
  v_versions jsonb:='[]'; v_owners jsonb:='[]'; v_summary jsonb;
  v_tab text:=coalesce(nullif(p_request->>'tab',''),'queries');
  v_section text:=coalesce(nullif(p_request->>'section',''),'questions');
  v_sort text:=coalesce(nullif(p_request->>'sort_key',''),case when nullif(p_request->>'client_id','') is null then 'client' else 'candidate' end);
  v_direction text:=coalesce(nullif(p_request->>'sort_direction',''),'asc');
  v_seek text:=private.weekly_source_query_ascii_fold_v1(coalesce(p_request->>'seek',''));
  v_limit integer:=coalesce((p_request->>'limit')::integer,50);
  v_offset integer:=0; v_base integer:=0; v_total integer; v_page jsonb; v_counts jsonb;
  v_version text; v_cursor jsonb; v_client uuid; v_seen text[]:='{}'; v_key text; v_owner_key text;
  v_attention jsonb; v_attention_first boolean:=coalesce((p_request->>'attention_first')::boolean,false);
  v_attention_kind text:=coalesce(p_request->>'attention_kind','');
  v_pending record; v_pending_checks jsonb:='[]'::jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_request,
    array['actor_user_id','source_group_id','client_id','week_ending','tab','section',
      'sort_key','sort_direction','seek','cursor','limit','report_key','attention_first','attention_kind'],'WEEKLY_SOURCE_COMBINED_REQUEST_INVALID');
  if v_tab='history' then
    return public.weekly_source_report_history_v1(p_request-'tab'-'section');
  end if;
  if p_request ? 'report_key' then
    raise exception 'WEEKLY_SOURCE_COMBINED_REQUEST_INVALID' using errcode='22023';
  end if;
  if v_tab not in ('imports','queries') or v_section not in ('questions','checks','protected','current','archive')
    or v_sort not in ('client','candidate','day_date','status','file','uploaded')
    or v_direction not in ('asc','desc') or v_limit not between 1 and 100 or length(v_seek)>100
    or v_attention_kind not in ('','missing_source','questions','checks','protected')
    or (p_request ? 'attention_first' and jsonb_typeof(p_request->'attention_first')<>'boolean') then
    raise exception 'WEEKLY_SOURCE_COMBINED_REQUEST_INVALID' using errcode='22023';
  end if;
  v_client:=nullif(p_request->>'client_id','')::uuid;
  v_request:=p_request-'tab'-'section'-'sort_key'-'sort_direction'-'seek'-'cursor'-'limit'-'attention_first'-'attention_kind';
  loop
    v_scope_page:=public.weekly_source_workspace_scopes_v1(v_request);
    v_summary:=v_scope_page-'rows'-'next_cursor'-'has_more';
    v_scopes:=v_scopes||(v_scope_page->'rows');
    exit when not (v_scope_page->>'has_more')::boolean;
    v_request:=v_request||jsonb_build_object('cursor',v_scope_page->>'next_cursor');
  end loop;
  -- A cycle publication is shared across its client scopes. If it has no
  -- cycle publication, each NHSP client's current report scope can instead
  -- own distinct query work. Visit each such scope exactly once.
  for v_scope in select distinct on (item->>'source_cycle_id',
      case when v_tab='queries' and cycle.current_projection_publication_id is null then item->>'client_id' else '' end)
      item||jsonb_build_object('cycle_publication_id',cycle.current_projection_publication_id)
    from jsonb_array_elements(v_scopes) item
    join public.weekly_source_cycles cycle on cycle.id=(item->>'source_cycle_id')::uuid
    order by item->>'source_cycle_id',
      case when v_tab='queries' and cycle.current_projection_publication_id is null then item->>'client_id' else '' end,
      case when item->>'report_scope_id' is null then 1 else 0 end,
      item->>'cutoff' desc nulls last,item->>'key'
  loop
    v_owner_request:=jsonb_build_object('actor_user_id',p_request->>'actor_user_id',
      'tab',v_tab,'source_group_id',v_scope->>'source_group_id',
      'source_cycle_id',v_scope->>'source_cycle_id','limit',100);
    if v_scope->>'source_family'='ROSTER' or
      (v_tab='queries' and v_scope->>'source_family'='NHSP' and v_scope->>'cycle_publication_id' is null
        and v_scope->>'report_scope_id' is not null) then
      v_owner_request:=v_owner_request||jsonb_build_object('client_id',v_scope->>'client_id');
    end if;
    if v_tab='queries' and v_scope->>'source_family'='NHSP' and v_scope->>'cycle_publication_id' is null
      and v_scope->>'report_scope_id' is not null then
      v_owner_request:=v_owner_request||jsonb_build_object('report_scope_id',v_scope->>'report_scope_id');
    end if;
    loop
      v_workspace:=public.weekly_source_office_workspace_v1(v_owner_request);
      v_owner_key:=case when v_tab='imports' then v_scope->>'source_cycle_id'
        else (v_scope->>'source_cycle_id')||':'||
          coalesce(v_workspace#>>'{selected,projection_publication_id}','none') end;
      v_versions:=v_versions||jsonb_build_array(jsonb_build_array(v_scope->>'source_cycle_id',v_workspace->>'workspace_version'));
      if not (v_owner_request ? 'cursor') then
        v_owners:=v_owners||jsonb_build_array(jsonb_build_object('key',v_owner_key,
          'source',v_scope->>'source','period',v_scope->>'period','scope',v_workspace->'selected',
          'bulk_actions',v_workspace#>'{queries,bulk_actions}',
          'protected_pay_enabled',v_workspace#>'{queries,protected_pay_enabled}'));
      end if;
      for v_item in select item from jsonb_array_elements(coalesce(v_workspace#>array[v_tab,'rows'],'[]')) item
      loop
        v_key:=v_tab||':'||v_owner_key;
        v_key:=v_key||':'||(v_item->>'row_key');
        if v_key=any(v_seen) then continue; end if;
        v_seen:=array_append(v_seen,v_key);
        if nullif(v_item->>'client_id','') is not null and not exists(select 1
          from jsonb_array_elements(v_scopes) permitted where permitted->>'source_cycle_id'=v_scope->>'source_cycle_id'
            and permitted->>'client_id'=v_item->>'client_id') then continue; end if;
        if v_client is not null and nullif(v_item->>'client_id','') is not null
          and (v_item->>'client_id')::uuid<>v_client then continue; end if;
        if v_tab='imports' then
          -- Upload errors remain in their immediate review receipt, not an
          -- ever-growing archive. Retain only accepted current work here.
          if v_item->>'state' is distinct from 'CURRENT' then continue; end if;
          if exists(select 1 from public.weekly_source_final_revisions revision
            join public.weekly_source_client_manifests manifest on manifest.final_revision_id=revision.id
            where revision.upload_id=(v_item->>'row_key')::uuid
              and revision.state in ('CURRENT','SUPERSEDED')
              and (nullif(v_item->>'client_id','') is null
                or manifest.client_id=(v_item->>'client_id')::uuid))
            and not exists(select 1 from jsonb_array_elements(v_scopes) sibling
              where sibling->>'upload_id'=v_item->>'row_key'
                and not coalesce((sibling->>'completed')::boolean,false)) then continue; end if;
        end if;
        if v_tab='queries' then
          -- An Office-created pay review is an Office check, not a request for
          -- candidate/manager hours evidence. Split only those children so a
          -- mixed group can retain its genuine Hours questions independently.
          select coalesce(jsonb_agg(child.value order by child.ordinality)
              filter(where child.value ? 'manual_review_id'),'[]'::jsonb),
            coalesce(jsonb_agg(child.value order by child.ordinality)
              filter(where not (child.value ? 'manual_review_id')),'[]'::jsonb)
            into v_manual_children,v_question_children
          from jsonb_array_elements(coalesce(v_item->'children','[]'::jsonb))
            with ordinality child(value,ordinality);
          for v_child in select value from jsonb_array_elements(v_manual_children)
          loop
            v_rows:=v_rows||jsonb_build_array(jsonb_build_object(
              'combined_key','manual-check:'||(v_child->>'manual_review_id'),
              'row_key',v_child->>'row_key','section','checks',
              'scope_key',v_owner_key,
              'source',v_scope->>'source','source_family',v_scope->>'source_family',
              'period',v_scope->>'period',
              'client',v_item->>'client','client_id',v_item->>'client_id',
              'candidate',v_item->>'candidate','candidate_id',v_item->>'candidate_id',
              'day_date',v_child->>'day_date','system_hours',v_child->>'system_hours',
              'status',v_child->'status','manual_query',v_child->'manual_query',
              'problem','Accept current source hours or protect pay.',
              'pay_blocking',true,'actions',v_child->'actions'));
          end loop;
          if jsonb_array_length(v_question_children)=0 then continue; end if;
          if jsonb_array_length(v_manual_children)>0 then
            v_item:=jsonb_set(v_item,'{actions,0,payload,detail,shifts}',v_question_children,false)
              ||jsonb_build_object('children',v_question_children,
                'issues',greatest(0,coalesce((v_item->>'issues')::integer,0)
                  -jsonb_array_length(v_manual_children)));
          end if;
        end if;
        v_rows:=v_rows||jsonb_build_array(v_item||jsonb_build_object(
          'combined_key',v_key,'scope_key',v_owner_key,'source',v_scope->>'source',
          'source_family',v_scope->>'source_family',
          'period',v_scope->>'period','section',case when v_tab='queries' then 'questions' else 'current' end,
          'client',coalesce(nullif(v_item->>'client',''),(select name from public.clients
            where id=nullif(v_item->>'client_id','')::uuid),'Source-wide file')));
      end loop;
      if v_tab='queries' and not (v_owner_request ? 'cursor') then
        for v_item in
          select item||jsonb_build_object('section','checks') from jsonb_array_elements(coalesce(v_workspace#>'{queries,office_checks,rows}','[]')) item
          union all
          select item||jsonb_build_object('section','protected') from jsonb_array_elements(coalesce(v_workspace#>'{queries,protected_shifts,rows}','[]')) item
        loop
          v_key:=case when v_item->>'section'='protected' then
            'protected:'||(v_item->>'family_id')||':'||(v_item->>'work_event_id')
            else (v_item->>'section')||':'||v_owner_key||':'||(v_item->>'row_key') end;
          if v_key=any(v_seen) then continue; end if;
          v_seen:=array_append(v_seen,v_key);
          if nullif(v_item->>'client_id','') is not null and not exists(select 1
            from jsonb_array_elements(v_scopes) permitted where permitted->>'source_group_id'=v_scope->>'source_group_id'
              and permitted->>'client_id'=v_item->>'client_id') then continue; end if;
          if v_client is not null and nullif(v_item->>'client_id','') is not null
            and (v_item->>'client_id')::uuid<>v_client then continue; end if;
          v_rows:=v_rows||jsonb_build_array(v_item||jsonb_build_object('combined_key',v_key,
            'scope_key',v_owner_key,'source',v_scope->>'source','period',
              case when v_item->>'section'='protected' then 'Awaiting source period' else v_scope->>'period' end));
        end loop;
      end if;
      exit when not coalesce((v_workspace#>>array[v_tab,'has_more'])::boolean,false);
      v_owner_request:=v_owner_request||jsonb_build_object('cursor',v_workspace#>>array[v_tab,'next_cursor']);
    end loop;
  end loop;
  -- Finalisation does not complete its separately tracked approved-hours work.
  -- Keep that exact owner/action reachable from outstanding Office checks.
  if v_tab='queries' then
    for v_scope in select item from jsonb_array_elements(v_scopes) item
      where item->>'completion_kind'='FINAL_SOURCE'
    loop
      v_follow_workspace:=public.weekly_source_office_workspace_v1(jsonb_build_object(
        'actor_user_id',p_request->>'actor_user_id','tab','finalise',
        'source_group_id',v_scope->>'source_group_id','source_cycle_id',v_scope->>'source_cycle_id',
        'client_id',v_scope->>'client_id','report_scope_id',v_scope->>'report_scope_id'));
      v_follow_up:=v_follow_workspace#>'{finalise,approved_hours_follow_up}';
      if nullif(v_follow_up->>'title','') is null then continue; end if;
      v_key:='approved-hours:'||(v_scope->>'key');
      v_versions:=v_versions||jsonb_build_array(jsonb_build_array(v_key,v_follow_workspace->>'workspace_version',v_follow_up));
      v_rows:=v_rows||jsonb_build_array(jsonb_build_object(
        'combined_key',v_key,'row_key',v_key,'section','checks',
        'client',v_scope->>'client','client_id',v_scope->>'client_id',
        'source',v_scope->>'source','period',v_scope->>'period',
        'candidate','Report follow-up','requires_attention',
          v_follow_up->>'state' in ('ACTION_REQUIRED','RECOVERY_REQUIRED')
          or jsonb_typeof(v_follow_up->'action')='object',
        'status',jsonb_build_object('text',v_follow_up->>'title'),
        'problem',v_follow_up->>'body','follow_up_scope',v_follow_workspace->'selected','actions','[]'::jsonb));
    end loop;
  end if;
  -- Rechecks invalidate the old authority before the replacement is ready.
  -- Retain old unresolved rows as explicitly non-actionable history, with only
  -- the exact saved recheck available. Never treat a missing CURRENT pointer
  -- as proof that Office has no work remaining.
  if v_tab='queries' then
    for v_pending in
      select recheck.request_id,recheck.actor_user_id,recheck.request_json,
        recheck.upload_id,recheck.publication_id,source_row.id as upload_row_id,
        source_row.work_date,source_row.source_client_identity,source_row.source_candidate_identity,
        source_row.external_source_key,source_row.start_at_local,source_row.end_at_local,source_row.break_minutes,
        coalesce(candidate.display_name,source_row.bounded_raw_columns_json->>'worker_name',
          source_row.bounded_raw_columns_json->>'candidate',source_row.source_candidate_identity) as candidate_name,
        resolution.mapping_state,charge.phase_severity,cycle.source_group_id,cycle.finalisation_week_ending,
        (select item->>'source' from jsonb_array_elements(v_scopes) item
          where item->>'source_cycle_id'=cycle.id::text limit 1) as source_name
      from private.weekly_source_office_rechecks recheck
      join public.weekly_source_projection_publications publication on publication.id=recheck.publication_id
      join public.weekly_source_projection_publications prior_publication on prior_publication.id=recheck.prior_publication_id
      join public.weekly_source_uploads upload on upload.id=recheck.upload_id
      join public.weekly_source_cycles cycle on cycle.id=upload.source_cycle_id
      left join public.weekly_source_report_scopes scope on scope.id=upload.report_scope_id
      join public.weekly_source_upload_rows source_row on source_row.upload_id=upload.id
      join public.weekly_source_row_resolutions resolution on resolution.upload_row_id=source_row.id
        and resolution.generation=coalesce(prior_publication.projection_generation,prior_publication.authority_scope_version)
      left join public.weekly_source_charge_checks charge on charge.upload_row_id=source_row.id
        and charge.generation=resolution.generation
      left join public.candidates candidate on candidate.id=coalesce(
        (select choice.candidate_id from private.weekly_source_office_row_choices choice
          where choice.upload_row_id=source_row.id order by choice.id desc limit 1),resolution.candidate_id)
      where publication.state='BUILDING' and upload.state='CURRENT'
        and publication.authority_scope_version=case when upload.report_scope_id is null then cycle.version else scope.version end
        and upload.id=case when upload.report_scope_id is null then cycle.current_complete_upload_id else scope.current_complete_upload_id end
        and exists(select 1 from jsonb_array_elements(v_scopes) item where item->>'source_cycle_id'=cycle.id::text)
        and (v_client is null or resolution.client_id=v_client or scope.client_id=v_client)
      order by recheck.request_id,source_row.source_row_ordinal
    loop
      v_key:='recheck:'||v_pending.request_id::text||':'||v_pending.upload_row_id::text;
      v_pending_checks:=v_pending_checks||jsonb_build_array(jsonb_build_object(
        'combined_key',v_key,'row_key',v_key,'section','checks','recheck_pending',true,
        'client',v_pending.source_client_identity,'candidate',v_pending.candidate_name,
        'source_reference',v_pending.source_candidate_identity,'booking_reference',v_pending.external_source_key,
        'source',v_pending.source_name,'period',to_char(v_pending.finalisation_week_ending,'FMDD Mon YYYY'),
        'work_date',v_pending.work_date,'day_date',to_char(v_pending.work_date,'FMDD Mon YYYY'),
        'system_hours',to_char(v_pending.start_at_local,'HH24:MI')||'–'||to_char(v_pending.end_at_local,'HH24:MI')
          ||' · '||v_pending.break_minutes::text||' min break',
        'pay_blocking',v_pending.mapping_state<>'RESOLVED',
        'status',jsonb_build_object('text','Recheck incomplete'),
        'problem',case when v_pending.mapping_state<>'RESOLVED' then 'Previous linking check — selection saved; replacement check incomplete.'
          when v_pending.phase_severity in ('PROVISIONAL_WARNING','FINALISATION_BLOCKER') then 'Previous contract charge warning — replacement check incomplete.'
          else 'Previous check — replacement check incomplete.' end,
        'actions',jsonb_build_array(jsonb_build_object('label','Retry recheck',
          'enabled',v_pending.actor_user_id=(p_request->>'actor_user_id')::uuid,
          'command','RECHECK_SOURCE','payload',v_pending.request_json-'actor_user_id'))));
    end loop;
    v_rows:=v_rows||v_pending_checks;
    v_summary:=v_summary||jsonb_build_object('recheck_pending_count',jsonb_array_length(v_pending_checks));
  end if;
  -- Classify the complete permitted collection before counting or paging.
  -- A waiting signature alone is informational; red unresolved work and green
  -- charge/reconciliation decisions remain visible and count as Office work.
  select coalesce(jsonb_agg(item||jsonb_build_object(
    'attention_missing_source_count',case when item->>'section'='questions' then
      (select count(*) from jsonb_array_elements(coalesce(item->'children','[]')) child
        where child->'candidate_shift_absent_from_import'='true'::jsonb) else 0 end,
    'attention_question_count',case when item->>'section'='questions' then
      (select count(*) from jsonb_array_elements(coalesce(item->'children','[]')) child
        where child->>'issue' is distinct from 'Timesheet missing'
          and child->'candidate_shift_absent_from_import' is distinct from 'true'::jsonb) else 0 end,
    'requires_attention',case
    when item->>'section'='questions' then exists(select 1
      from jsonb_array_elements(coalesce(item->'children','[]')) child
      where child->>'issue' is distinct from 'Timesheet missing')
    when item->>'section'='checks' then case when item ? 'follow_up_scope'
      then coalesce((item->>'requires_attention')::boolean,false) else true end
    when item->>'section'='protected' then coalesce((item->>'requires_attention')::boolean,false)
    else false end)),'[]') into v_rows from jsonb_array_elements(v_rows) item;
  select jsonb_build_object('missing_source',coalesce(sum((item->>'attention_missing_source_count')::integer),0),
    'questions',coalesce(sum((item->>'attention_question_count')::integer),0),
    'checks',count(*) filter(where item->>'section'='checks'),
    'protected',count(*) filter(where item->>'section'='protected'),
    'total',coalesce(sum((item->>'attention_missing_source_count')::integer
      +(item->>'attention_question_count')::integer),0)
      +count(*) filter(where item->>'section' in ('checks','protected')),'complete',true) into v_attention
  from jsonb_array_elements(v_rows) item where item->'requires_attention'='true'::jsonb;
  select jsonb_build_object('questions',count(*) filter(where item->>'section'='questions'),
    'checks',count(*) filter(where item->>'section'='checks'),'protected',count(*) filter(where item->>'section'='protected'),
    'current',count(*) filter(where item->>'section'='current'),'archive',count(*) filter(where item->>'section'='archive'))
    into v_counts from jsonb_array_elements(v_rows) item;
  v_version:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_COMBINED_REVIEW_V1',
    jsonb_build_object('scopes',v_summary->>'version','owners',v_versions,'attention_rows',v_rows,
      'filters',p_request-'cursor'-'limit')),'hex');
  -- Attention is an exact view filter, not a tab-total alias. The full census
  -- above stays intact; only this page collection is narrowed. Never rewrite
  -- server-owned action selections or comparison/financial proof payloads.
  if v_attention_kind<>'' then
    select coalesce(jsonb_agg(item),'[]') into v_rows from jsonb_array_elements(v_rows) item
    where item->'requires_attention'='true'::jsonb and case v_attention_kind
      when 'missing_source' then item->>'section'='questions' and (item->>'attention_missing_source_count')::integer>0
      when 'questions' then item->>'section'='questions' and (item->>'attention_question_count')::integer>0
      when 'checks' then item->>'section'='checks'
      when 'protected' then item->>'section'='protected'
      else false end;
    if v_attention_kind in ('missing_source','questions') then
      for v_item in select item from jsonb_array_elements(v_rows) item
      loop
        select coalesce(jsonb_agg(child.value order by child.ordinality),'[]') into v_question_children
          from jsonb_array_elements(v_item->'children') with ordinality child(value,ordinality)
          where child.value->>'issue' is distinct from 'Timesheet missing' and case v_attention_kind
            when 'missing_source' then child.value->'candidate_shift_absent_from_import'='true'::jsonb
            else child.value->'candidate_shift_absent_from_import' is distinct from 'true'::jsonb end;
        v_item:=v_item||jsonb_build_object('children',v_question_children,'issues',jsonb_array_length(v_question_children),
          'actions',(select coalesce(jsonb_agg(case when action->>'label'='Open'
            then jsonb_set(action,'{payload,detail,shifts}',v_question_children,false) else action end),'[]')
            from jsonb_array_elements(coalesce(v_item->'actions','[]')) action));
        select coalesce(jsonb_agg(case when item->>'combined_key'=v_item->>'combined_key' then v_item else item end),'[]')
          into v_rows from jsonb_array_elements(v_rows) item;
      end loop;
    end if;
  end if;
  if nullif(p_request->>'cursor','') is not null then
    begin
      v_cursor:=convert_from(decode(p_request->>'cursor','base64'),'UTF8')::jsonb;
      v_offset:=(v_cursor->>'offset')::integer;
      if v_offset is null or v_offset<0 or v_cursor->>'version' is distinct from v_version then raise exception 'stale'; end if;
    exception when others then raise exception 'WEEKLY_SOURCE_WORKSPACE_CURSOR_STALE' using errcode='40001'; end;
  end if;
  with keyed as (
    select item,private.weekly_source_query_ascii_fold_v1(coalesce(case v_sort
      when 'candidate' then coalesce(item->>'candidate_sort',item->>'candidate') when 'client' then item->>'client'
      when 'day_date' then item->>'work_date' when 'file' then item->>'file' when 'uploaded' then item->>'uploaded_at'
      when 'status' then item#>>'{status,text}' end,'')) sort_value
    from jsonb_array_elements(v_rows) item where item->>'section'=v_section
  ), ordered as (
    select *,row_number() over(order by case when v_attention_first and v_seek='' then
      case v_attention_kind when 'missing_source' then ((item->>'attention_missing_source_count')::integer>0)::integer
        when 'questions' then ((item->>'attention_question_count')::integer>0)::integer
        else 0 end else 0 end desc,
      case when v_attention_first and v_seek=''
      then coalesce((item->>'requires_attention')::boolean,false)::integer else 0 end desc,
      case when v_direction='asc' then sort_value end collate "C" asc,
      case when v_direction='desc' then sort_value end collate "C" desc,
      private.weekly_source_query_ascii_fold_v1(item->>'client') collate "C",
      private.weekly_source_query_ascii_fold_v1(coalesce(item->>'candidate_sort',item->>'candidate')) collate "C",
      item->>'work_date',item->>'combined_key') ordinal from keyed
  ), origin as (select coalesce(min(ordinal) filter(where v_seek<>'' and starts_with(sort_value,v_seek)),1)-1 base from ordered)
  select count(*)::integer,coalesce(max(origin.base),0)::integer,coalesce(jsonb_agg(item order by ordinal)
    filter(where ordinal>origin.base+v_offset and ordinal<=origin.base+v_offset+v_limit),'[]')
    into v_total,v_base,v_page from ordered cross join origin;
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_COMBINED_REVIEW_V1','tab',v_tab,'section',v_section,
    'version',v_version,'rows',v_page,'total_count',v_total,'owners',v_owners,'scope_options',v_scopes,
    'summary',v_summary,'counts',v_counts,'attention',v_attention,
    'attention_kind',v_attention_kind,
    'sort_key',v_sort,'sort_direction',v_direction,
    'has_more',v_base+v_offset+v_limit<v_total,'next_cursor',case when v_base+v_offset+v_limit<v_total
      then encode(convert_to(jsonb_build_object('version',v_version,'offset',v_offset+v_limit)::text,'UTF8'),'base64') else '' end);
end;
$function$;
alter function public.weekly_source_combined_review_workspace_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_combined_review_workspace_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_combined_review_workspace_v1(jsonb) to service_role;

-- On-demand file inspection; never place every historical file's rows in the
-- combined list payload. The cursor is bound to this upload and its read state.
create or replace function public.weekly_source_upload_detail_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid; v_id uuid; v_offset integer:=0; v_limit integer:=50;
  v_upload public.weekly_source_uploads%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_client uuid; v_rows jsonb; v_count integer; v_version text; v_cursor jsonb;
  v_purpose text; v_finalised text; v_reason text;
begin
  perform private.weekly_source_query_require_service_v1();
  if jsonb_typeof(p_request)<>'object' or exists(select 1 from jsonb_object_keys(p_request) key
    where key not in ('actor_user_id','upload_id','cursor','limit')) then
    raise exception 'WEEKLY_SOURCE_UPLOAD_DETAIL_REQUEST_INVALID' using errcode='22023';
  end if;
  v_actor:=nullif(p_request->>'actor_user_id','')::uuid;
  v_id:=nullif(p_request->>'upload_id','')::uuid;
  v_limit:=least(100,greatest(1,coalesce((p_request->>'limit')::integer,50)));
  select * into strict v_upload from public.weekly_source_uploads where id=v_id;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_upload.source_cycle_id;
  select client_id into v_client from public.weekly_source_report_scopes where id=v_upload.report_scope_id;
  v_client:=coalesce(v_client,v_cycle.scope_client_id);
  perform private.weekly_source_office_authority_v1(v_actor,'VIEW_SOURCE_PROGRESS',
    v_cycle.source_group_id,v_client,v_cycle.finalisation_week_ending);
  select count(*) into v_count from public.weekly_source_upload_rows where upload_id=v_id;
  v_version:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_UPLOAD_DETAIL_V1',
    jsonb_build_object('upload',v_id,'state',v_upload.state,'manifest',encode(v_upload.row_manifest_hash,'hex'),
      'resolutions',(select max(resolution.created_at_utc) from public.weekly_source_row_resolutions resolution
        join public.weekly_source_upload_rows row on row.id=resolution.upload_row_id where row.upload_id=v_id))),'hex');
  if nullif(p_request->>'cursor','') is not null then
    begin
      v_cursor:=convert_from(decode(p_request->>'cursor','base64'),'UTF8')::jsonb;
      v_offset:=(v_cursor->>'offset')::integer;
      if v_offset is null or v_offset<0 or v_cursor->>'version' is distinct from v_version then raise exception 'stale'; end if;
    exception when others then raise exception 'WEEKLY_SOURCE_WORKSPACE_CURSOR_STALE' using errcode='40001'; end;
  end if;
  select case when profile.profile_code='NHSP_FINAL_BACKING_V1'
      or v_upload.file_metadata_json->>'import_use'='PREPARE_FINALISATION'
    then 'Finalisation report' else 'Checking hours' end into v_purpose
    from public.weekly_source_format_profiles profile where profile.id=v_upload.source_format_profile_id;
  select string_agg(to_char(revision.finalised_at_utc at time zone 'Europe/London','FMDD Mon YYYY, HH24:MI')
      ||case when revision.state='CURRENT' then '' else ' (replaced by a later finalisation)' end,', '
    order by revision.finalised_at_utc) into v_finalised
    from public.weekly_source_final_revisions revision where revision.upload_id=v_id;
  if v_upload.state='REJECTED' then
    select attempt.reason_code into v_reason from public.weekly_source_upload_attempts attempt
      where attempt.logical_upload_id=v_id and attempt.result in ('REJECTED','FAILED','CONFLICT','CORRUPT')
      order by attempt.attempted_at_utc desc,attempt.id desc limit 1;
  end if;
  select coalesce(jsonb_agg(jsonb_build_object(
    'source_row_id',case when resolution.mapping_state='RESOLVED'
      and row.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED')
      and (private.weekly_source_office_route_key_v1(v_cycle.id,resolution.candidate_id,
        resolution.client_id,resolution.contract_id,row.work_date))->>'authority_mode'='SOURCE_AUTHORITY'
      and (exists(select 1 from public.weekly_source_projection_publications publication
        where publication.upload_id=v_upload.id and publication.state='CURRENT')
      or exists(select 1 from public.weekly_source_final_revisions revision
          where revision.upload_id=v_upload.id and revision.state='CURRENT'))
      then row.id end,
    'pay_query_open',exists(select 1 from private.weekly_source_manual_reviews review
      where review.source_group_id=v_cycle.source_group_id
        and review.work_event_id=resolution.work_event_id and review.state='OPEN')
      or exists(select 1 from public.weekly_discrepancy_incidents incident
        join public.weekly_source_cycles incident_cycle on incident_cycle.id=incident.source_cycle_id
        where incident_cycle.source_group_id=v_cycle.source_group_id
          and incident.work_event_id=resolution.work_event_id and incident.state='OPEN'),
    'candidate',coalesce(candidate.display_name,nullif(row.bounded_raw_columns_json->>'worker_name',''),
      nullif(row.bounded_raw_columns_json->>'staff_name',''),nullif(row.bounded_raw_columns_json->>'candidate',''),row.source_candidate_identity),
    'client',coalesce(client.name,row.source_client_identity),'source_reference',row.source_candidate_identity,
    'booking_reference',row.external_source_key,'day_date',to_char(row.work_date,'FMDD Mon YYYY'),
    'system_hours',case when row.start_at_local is null or row.end_at_local is null then 'No confirmed shift times'
      else to_char(row.start_at_local,'HH24:MI')||'–'||to_char(row.end_at_local,'HH24:MI')||' · '
        ||coalesce(row.break_minutes::text,'Not recorded')||' min break' end,
    'status',case when resolution.id is null then 'Not yet checked'
      when resolution.mapping_state<>'RESOLVED' then 'Needs correction'
      when row.row_finalisation_state='SOURCE_UNFINALISED' then 'Not finalised in source'
      else 'Linked' end,
    'issue',case resolution.mapping_state
      when 'CANDIDATE_NOT_FOUND' then 'Candidate needs linking'
      when 'CLIENT_NOT_FOUND' then 'Client needs linking'
      when 'NO_ELIGIBLE_CONTRACT' then 'No eligible contract'
      when 'AMBIGUOUS_CONTRACT' then 'Several contracts match; choose the correct contract in Queries'
      when 'CONTRACT_SELECTION_REQUIRED' then 'Choose the contract in Queries'
      when 'SOURCE_ROW_BLOCKED' then 'Source details or contract need checking in Queries'
      else '' end
  ) order by row.source_row_ordinal,row.id),'[]') into v_rows
  from (select * from public.weekly_source_upload_rows where upload_id=v_id
    order by source_row_ordinal,id offset v_offset limit v_limit) row
  left join lateral (select item.* from public.weekly_source_row_resolutions item where item.upload_row_id=row.id
    order by item.generation desc,item.id desc limit 1) resolution on true
  left join public.candidates candidate on candidate.id=resolution.candidate_id
  left join public.clients client on client.id=resolution.client_id;
  return jsonb_build_object('contract','WEEKLY_SOURCE_UPLOAD_DETAIL_V1','upload_id',v_id,'version',v_version,
    'file',v_upload.original_filename,'purpose',v_purpose,
    'uploaded',to_char(v_upload.uploaded_at_utc at time zone 'Europe/London','FMDD Mon YYYY, HH24:MI'),
    'status',initcap(replace(v_upload.state,'_',' ')),'reason_code',v_reason,'rows',v_count,
    'coverage',case when v_upload.confirmed_coverage_start_local_date is null then 'Not confirmed'
      else to_char(v_upload.confirmed_coverage_start_local_date,'FMDD Mon YYYY')||' to '
        ||to_char(v_upload.confirmed_coverage_end_local_date,'FMDD Mon YYYY') end,
    'final_source',coalesce('Finalised: '||v_finalised,'Not finalised'),'shifts',v_rows,
    'has_more',v_offset+v_limit<v_count,'next_cursor',case when v_offset+v_limit<v_count
      then encode(convert_to(jsonb_build_object('version',v_version,'offset',v_offset+v_limit)::text,'UTF8'),'base64') else '' end);
end;
$function$;
alter function public.weekly_source_upload_detail_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_upload_detail_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_upload_detail_v1(jsonb) to service_role;

-- Completed reports are durable client manifests, not uploaded files and not
-- whatever happens to be the current checking projection. Corrections retain
-- their earlier report revisions; explicit zero returns retain their receipt.
create or replace function public.weekly_source_report_history_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid; v_group uuid; v_client uuid; v_week date;
  v_date_from date; v_date_to date;
  v_limit integer; v_offset integer:=0; v_cursor jsonb; v_version text;
  v_sort text; v_direction text; v_seek text;
  v_reports jsonb; v_options jsonb; v_page jsonb; v_report jsonb;
  v_total integer; v_base integer:=0; v_key text;
  v_revision uuid; v_shifts jsonb; v_movements jsonb;
  v_shift_count integer; v_movement_count integer;
  v_invoice_total bigint;
  v_final public.weekly_source_final_revisions%rowtype;
  v_profile text; v_actions jsonb:='[]'::jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_request,
    array['actor_user_id','source_group_id','client_id','week_ending','date_from','date_to','report_key',
      'sort_key','sort_direction','seek','cursor','limit'],'WEEKLY_SOURCE_HISTORY_REQUEST_INVALID');
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_group:=nullif(p_request->>'source_group_id','')::uuid;
  v_client:=nullif(p_request->>'client_id','')::uuid;
  v_week:=nullif(p_request->>'week_ending','')::date;
  v_date_from:=nullif(p_request->>'date_from','')::date;
  v_date_to:=nullif(p_request->>'date_to','')::date;
  v_key:=nullif(p_request->>'report_key','');
  v_limit:=coalesce((p_request->>'limit')::integer,50);
  v_sort:=coalesce(nullif(p_request->>'sort_key',''),'finalised_at');
  v_direction:=coalesce(nullif(p_request->>'sort_direction',''),'desc');
  v_seek:=private.weekly_source_query_ascii_fold_v1(coalesce(p_request->>'seek',''));
  if v_date_from>v_date_to or v_limit not between 1 and 100 or v_sort not in ('client','source','period','report','finalised_at')
    or v_direction not in ('asc','desc') or length(v_seek)>100 then
    raise exception 'WEEKLY_SOURCE_HISTORY_REQUEST_INVALID' using errcode='22023';
  end if;
  -- History includes retired source groups; the existing global Office read
  -- authority still requires an active administrator. No new financial right.
  perform private.weekly_source_office_authority_v1(v_actor,'VIEW_SOURCE_PROGRESS');
  with completed as (
    select 'FINAL:'||manifest.id::text report_key,manifest.source_group_id,
      manifest.source_cycle_id,manifest.client_id,manifest.finalisation_week_ending,
      manifest.final_revision_id,manifest.backing_report_number,
      revision.finalised_at_utc completed_at,revision.finalised_by_user_id actor_id,
      revision.revision_number,revision.state revision_state,revision.reason,
      encode(revision.manifest_hash,'hex') evidence_hash,'FINAL_SOURCE' completion_kind,
      null::text attestation
    from public.weekly_source_client_manifests manifest
    join public.weekly_source_final_revisions revision on revision.id=manifest.final_revision_id
    where revision.state in ('CURRENT','SUPERSEDED')
    union all
    select 'ZERO:'||completion.id::text,completion.source_group_id,completion.source_cycle_id,
      completion.client_id,cycle.finalisation_week_ending,null::uuid,null::text,
      completion.attested_at_utc,completion.attested_by_user_id,completion.completion_generation,
      completion.state,'NO_SHIFTS_TO_IMPORT',encode(completion.completion_hash,'hex'),
      completion.completion_kind,completion.attestation_text
    from public.weekly_source_client_cycle_completions completion
    join public.weekly_source_cycles cycle on cycle.id=completion.source_cycle_id
    where completion.completion_kind='NO_SHIFTS_TO_IMPORT'
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'report_key',completed.report_key,'source_group_id',completed.source_group_id,
    'source_cycle_id',completed.source_cycle_id,'client_id',completed.client_id,
    'client',client.name,'source',source_group.display_name,'source_family',source_group.source_family,
    'week_ending',completed.finalisation_week_ending,'period',to_char(completed.finalisation_week_ending,'FMDD Mon YYYY'),
    'final_revision_id',completed.final_revision_id,'revision_number',completed.revision_number,
    'revision_state',completed.revision_state,'reason',completed.reason,
    'report',coalesce(completed.backing_report_number,case when completed.completion_kind='NO_SHIFTS_TO_IMPORT'
      then 'No shifts to import' else 'Weekly finalisation' end),
    'finalised_at',to_char(completed.completed_at at time zone 'Europe/London','FMDD Mon YYYY, HH24:MI'),
    'finalised_at_utc',completed.completed_at,'finalised_by',coalesce(office_user.display_name,'Office'),
    'completion_kind',completed.completion_kind,'attestation',completed.attestation,
    'evidence_hash',completed.evidence_hash
  ) order by completed.completed_at desc,completed.report_key),'[]'::jsonb)
  into v_reports from completed
  join public.clients client on client.id=completed.client_id
  join public.weekly_source_groups source_group on source_group.id=completed.source_group_id
  left join public.tms_users office_user on office_user.id=completed.actor_id
  where (v_group is null or completed.source_group_id=v_group)
    and (v_client is null or completed.client_id=v_client)
    and (v_date_from is null or (completed.completed_at at time zone 'Europe/London')::date>=v_date_from)
    and (v_date_to is null or (completed.completed_at at time zone 'Europe/London')::date<=v_date_to);
  select coalesce(jsonb_agg(distinct jsonb_build_object('source_group_id',item->>'source_group_id',
    'source',item->>'source','client_id',item->>'client_id','client',item->>'client',
    'week_ending',item->>'week_ending','period',item->>'period')),'[]'::jsonb)
    into v_options from jsonb_array_elements(v_reports) item;
  v_version:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_REPORT_HISTORY_V1',
    jsonb_build_object('reports',v_reports,'filter',p_request-'cursor'-'limit')),'hex');
  if nullif(p_request->>'cursor','') is not null then
    begin
      v_cursor:=convert_from(decode(p_request->>'cursor','base64'),'UTF8')::jsonb;
      v_offset:=(v_cursor->>'offset')::integer;
      if v_offset is null or v_offset<0 or v_cursor->>'version' is distinct from v_version then raise exception 'stale'; end if;
    exception when others then raise exception 'WEEKLY_SOURCE_WORKSPACE_CURSOR_STALE' using errcode='40001'; end;
  end if;
  if v_key is not null then
    select item into v_report from jsonb_array_elements(v_reports) item where item->>'report_key'=v_key;
    if v_report is null then raise exception 'WEEKLY_SOURCE_COMPLETED_REPORT_NOT_FOUND' using errcode='22023'; end if;
    v_revision:=nullif(v_report->>'final_revision_id','')::uuid;
    v_client:=(v_report->>'client_id')::uuid;
    select revision.* into v_final from public.weekly_source_final_revisions revision where revision.id=v_revision;
    if v_final.state='CURRENT' and exists(select 1 from public.weekly_source_groups source_group
      join public.weekly_source_group_clients membership on membership.source_group_id=source_group.id
      where source_group.id=(v_report->>'source_group_id')::uuid and source_group.active
        and membership.client_id=v_client and (v_report->>'week_ending')::date
          between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)) then
      perform private.weekly_source_office_authority_v1(v_actor,'CORRECT_FINAL_SOURCE',
        (v_report->>'source_group_id')::uuid,v_client,(v_report->>'week_ending')::date);
      select profile.profile_code into v_profile from public.weekly_source_uploads upload
        join public.weekly_source_format_profiles profile on profile.id=upload.source_format_profile_id
        where upload.id=v_final.upload_id;
      v_actions:=jsonb_build_array(jsonb_build_object('label','Correct final source','enabled',true,
        'payload',jsonb_build_object('scope_label',v_report->>'client','current_label',v_report->>'report',
          'correction_payload',jsonb_build_object('source_cycle_id',v_final.source_cycle_id,
            'authority_scope_kind',v_final.authority_scope_kind,'report_scope_id',v_final.report_scope_id,
            'expected_current_final_revision_id',v_final.id,
            'expected_final_manifest_hash',encode(v_final.manifest_hash,'hex'),
            'idempotency_key','history-correction:'||gen_random_uuid()::text),
          'replacement_context',jsonb_build_object('source_group_id',v_report->>'source_group_id',
            'client_id',v_client,'profile_id',v_profile))));
    end if;
    with shifts as (
      select snapshot.id::text row_key,snapshot.upload_row_id source_row_id,
        snapshot.work_event_id,snapshot.candidate_id,snapshot.work_date,
        snapshot.start_at_local,snapshot.end_at_local,snapshot.break_minutes,snapshot.actual_net_minutes,
        snapshot.external_event_identity booking_reference,
        source_row.source_total_cost_pence,source_row.source_commission_pence,
        source_row.source_shift_charge_pence
      from public.weekly_source_final_snapshot_lines snapshot
      left join public.weekly_source_upload_rows source_row on source_row.id=snapshot.upload_row_id
      where snapshot.final_revision_id=v_revision and snapshot.client_id=v_client
      union all
      select source_row.id::text,source_row.id,movement.work_event_id,
        movement.candidate_id,source_row.work_date,source_row.start_at_local,
        source_row.end_at_local,source_row.break_minutes,source_row.actual_net_minutes,
        source_row.external_source_key,source_row.source_total_cost_pence,
        source_row.source_commission_pence,source_row.source_shift_charge_pence
      from public.weekly_source_billing_movements movement
      join public.weekly_source_upload_rows source_row on source_row.id=movement.nhsp_upload_row_id
      where movement.final_revision_id=v_revision and movement.actual_client_id=v_client
    ), ordered as (
      select shifts.*,coalesce(candidate.display_name,concat_ws(' ',candidate.first_name,candidate.last_name)) candidate_name,
        row_number() over(order by private.weekly_source_query_ascii_fold_v1(candidate.last_name),
          candidate.id,shifts.work_date,shifts.start_at_local,shifts.row_key) ordinal
      from shifts join public.candidates candidate on candidate.id=shifts.candidate_id
    )
    select count(*)::integer,coalesce(jsonb_agg(jsonb_build_object('row_key',row_key,
      'source_row_id',case when v_final.state='CURRENT' then source_row_id end,
      'pay_query_open',exists(select 1 from private.weekly_source_manual_reviews review
        where review.source_group_id=(v_report->>'source_group_id')::uuid
          and review.work_event_id=ordered.work_event_id and review.state='OPEN')
        or exists(select 1 from public.weekly_discrepancy_incidents incident
          join public.weekly_source_cycles incident_cycle on incident_cycle.id=incident.source_cycle_id
          where incident_cycle.source_group_id=(v_report->>'source_group_id')::uuid
            and incident.work_event_id=ordered.work_event_id and incident.state='OPEN'),
      'work_event_id',work_event_id,'candidate',candidate_name,
      'day_date',to_char(work_date,'Dy FMDD Mon YYYY'),'start',to_char(start_at_local,'HH24:MI'),
      'end',to_char(end_at_local,'HH24:MI'),'break_minutes',break_minutes,'net_minutes',actual_net_minutes,
      'booking_reference',booking_reference,
      'source_total_cost_pence',source_total_cost_pence,
      'source_commission_pence',source_commission_pence,
      'source_shift_charge_pence',source_shift_charge_pence) order by ordinal)
      filter(where ordinal>v_offset and ordinal<=v_offset+v_limit),'[]'::jsonb)
      into v_shift_count,v_shifts from ordered;
    with ordered as (
      select movement.*,coalesce(candidate.display_name,concat_ws(' ',candidate.first_name,candidate.last_name)) candidate_name,
        event.work_date,row_number() over(order by event.work_date,candidate.last_name,candidate.id,movement.id) ordinal
      from public.weekly_source_billing_movements movement
      join public.candidates candidate on candidate.id=movement.candidate_id
      join public.weekly_work_events event on event.id=movement.work_event_id
      where movement.final_revision_id=v_revision and movement.actual_client_id=v_client
    )
    select count(*)::integer,coalesce(jsonb_agg(jsonb_build_object('row_key',id,'candidate',candidate_name,
      'day_date',to_char(work_date,'Dy FMDD Mon YYYY'),'movement',initcap(replace(movement_role,'_',' ')),
      'source_line_kind',source_line_kind,'booking_reference',source_facts_json->>'booking_reference',
      'pay_ex_vat_pence',round(total_pay_ex_vat*100)::bigint,
      'invoice_charge_pence',invoice_presentation_charge_pence,
      'vat_pence',round(vat_amount*100)::bigint,
      'total_inc_vat_pence',round(total_inc_vat*100)::bigint) order by ordinal)
      filter(where ordinal>v_offset and ordinal<=v_offset+v_limit),'[]'::jsonb)
      into v_movement_count,v_movements from ordered;
    select coalesce(sum(invoice_presentation_charge_pence),0) into v_invoice_total
      from public.weekly_source_billing_movements where final_revision_id=v_revision and actual_client_id=v_client;
    return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_COMPLETED_REPORT_V1','report',v_report,
      'shifts',v_shifts,'shift_count',v_shift_count,'movements',v_movements,'movement_count',v_movement_count,'actions',v_actions,
      'invoice_charge_pence',v_invoice_total,'has_more',v_offset+v_limit<greatest(v_shift_count,v_movement_count),
      'next_cursor',case when v_offset+v_limit<greatest(v_shift_count,v_movement_count) then
        encode(convert_to(jsonb_build_object('version',v_version,'offset',v_offset+v_limit)::text,'UTF8'),'base64') else '' end);
  end if;
  with filtered as (
    select item,private.weekly_source_query_ascii_fold_v1(case v_sort when 'period' then item->>'week_ending'
      when 'finalised_at' then item->>'finalised_at_utc' else item->>v_sort end) sort_value
    from jsonb_array_elements(v_reports) item where v_week is null or (item->>'week_ending')::date=v_week
  ), ordered as (
    select item,sort_value,row_number() over(order by case when v_direction='asc' then sort_value end collate "C" asc,
      case when v_direction='desc' then sort_value end collate "C" desc,item->>'report_key') ordinal from filtered
  ), origin as (select coalesce(min(ordinal) filter(where v_seek<>'' and starts_with(sort_value,v_seek)),1)-1 base from ordered)
  select count(*)::integer,coalesce(max(origin.base),0)::integer,coalesce(jsonb_agg(item order by ordinal)
    filter(where ordinal>origin.base+v_offset and ordinal<=origin.base+v_offset+v_limit),'[]'::jsonb)
    into v_total,v_base,v_page from ordered cross join origin;
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_REPORT_HISTORY_V1','rows',v_page,
    'scope_options',v_options,'sort_key',v_sort,'sort_direction',v_direction,'total_count',v_total,
    'has_more',v_base+v_offset+v_limit<v_total,'next_cursor',case when v_base+v_offset+v_limit<v_total then
      encode(convert_to(jsonb_build_object('version',v_version,'offset',v_offset+v_limit)::text,'UTF8'),'base64') else '' end);
end;
$function$;
alter function public.weekly_source_report_history_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_report_history_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_report_history_v1(jsonb) to service_role;

notify pgrst, 'reload schema';

commit;
