-- Repeatable CloudTMS function/view authority: candidate_daily_hybrid_availability_authority_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

create or replace function public.candidate_daily_legacy_availability_apply_atomic_v1(
  p_internal_context jsonb,
  p_candidate_source_hmac text,
  p_request_id uuid,
  p_idempotency_key text,
  p_changes jsonb,
  p_correlation_id text
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_context jsonb;
  v_environment text;
  v_candidate_id uuid;
  v_scope private.candidate_daily_authority_scopes%rowtype;
  v_receipt public.candidate_daily_command_receipts%rowtype;
  v_request_hash text;
  v_change jsonb;
  v_preference text;
  v_outcomes jsonb := '[]'::jsonb;
  v_accepted jsonb := '[]'::jsonb;
  v_repairs jsonb := '[]'::jsonb;
  v_seen text[] := '{}';
  v_reason text;
  v_row public.candidate_daily_rota_days%rowtype;
  v_repair_preference text;
  v_repair_version bigint;
  v_repair_command_id uuid;
  v_new_version bigint;
  v_terminal jsonb;
begin
  v_context := private._candidate_daily_context_v1(p_internal_context,'LEGACY_COMPAT',false);
  v_environment := v_context->>'environment';
  v_candidate_id := private._candidate_daily_source_candidate_v1(v_environment,p_candidate_source_hmac);

  if p_request_id is null
     or p_idempotency_key !~ '^[A-Za-z0-9._~:+/-]{16,128}$'
     or jsonb_typeof(p_changes) <> 'array'
     or jsonb_array_length(p_changes) not between 1 and 14
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;

  v_request_hash := private._candidate_daily_json_sha256_v1(jsonb_build_object(
    'operation','LEGACY_AVAILABILITY_APPLY',
    'candidate_source_hmac',p_candidate_source_hmac,
    'request_id',p_request_id,
    'changes',p_changes
  ));

  insert into public.candidate_daily_command_receipts(
    command_id,environment,candidate_id,actor_class,command_class,idempotency_key,
    request_sha256,canonical_version_before,state,correlation_id
  )
  select
    p_request_id,v_environment,v_candidate_id,'LEGACY_ADAPTER','LEGACY_AVAILABILITY_APPLY',
    p_idempotency_key,v_request_hash,s.canonical_version,'IN_PROGRESS',p_correlation_id
  from private.candidate_daily_authority_scopes s
  where s.environment=v_environment and s.candidate_id=v_candidate_id
  on conflict do nothing;

  select * into v_receipt
  from public.candidate_daily_command_receipts
  where command_id=p_request_id
     or (environment=v_environment and candidate_id=v_candidate_id
       and actor_class='LEGACY_ADAPTER' and idempotency_key=p_idempotency_key)
  order by (command_id=p_request_id) desc
  limit 1
  for update;

  if v_receipt.command_id is null then
    raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
  end if;
  if v_receipt.candidate_id<>v_candidate_id or v_receipt.request_sha256<>v_request_hash then
    raise exception using errcode='23505',message='IDEMPOTENCY_KEY_REUSED';
  end if;
  if v_receipt.state<>'IN_PROGRESS' then
    return v_receipt.terminal_body_json||jsonb_build_object('_idempotent_replay',true);
  end if;

  select * into v_scope
  from private.candidate_daily_authority_scopes
  where environment=v_environment and candidate_id=v_candidate_id
  for update;

  for v_change in select value from jsonb_array_elements(p_changes) loop
    v_reason := null;
    v_preference := private._candidate_daily_preference_v1(v_change->>'availability');

    if (select count(*) from jsonb_object_keys(v_change))<>2
       or not(v_change ? 'date' and v_change ? 'availability')
       or v_preference is null
       or v_change->>'date' !~ '^\d{4}-\d{2}-\d{2}$' then
      v_reason := 'INVALID_VALUE';
    elsif (v_change->>'date')=any(v_seen) then
      v_reason := 'DUPLICATE_DATE';
    else
      v_seen := array_append(v_seen,v_change->>'date');
      select * into v_row
      from public.candidate_daily_rota_days
      where generation_id=v_scope.active_generation_id
        and rota_date=(v_change->>'date')::date;

      if v_row.generation_id is null then
        v_reason := 'OUTSIDE_WINDOW';
      elsif v_row.booked then
        v_reason := 'BOOKED';
      elsif v_row.system_blocked then
        v_reason := 'BLOCKED';
      elsif v_scope.transition_in_progress then
        v_reason := 'NOT_EDITABLE';
      elsif v_scope.authority_mode='SUPABASE_PRIMARY' then
        v_reason := 'MYTMS_PRIMARY';
      elsif v_scope.authority_mode not in ('GOOGLE_PRIMARY','ROLLBACK_PENDING') then
        v_reason := 'NOT_EDITABLE';
      end if;
    end if;

    if v_reason is null then
      v_accepted := v_accepted||jsonb_build_array(jsonb_build_object(
        'date',v_change->>'date','preference',v_preference
      ));
      v_outcomes := v_outcomes||jsonb_build_array(jsonb_build_object(
        'date',v_change->>'date','applied',true
      ));
    else
      if v_reason='MYTMS_PRIMARY' then
        v_repairs := v_repairs||jsonb_build_array(jsonb_build_object('date',v_change->>'date'));
      end if;
      v_outcomes := v_outcomes||jsonb_build_array(jsonb_build_object(
        'date',coalesce(v_change->>'date','1970-01-01'),
        'applied',false,
        'reason',v_reason
      ));
    end if;
  end loop;

  if jsonb_array_length(v_accepted)>0 then
    v_new_version := v_scope.canonical_version+1;
    update private.candidate_daily_authority_scopes
    set canonical_version=v_new_version,updated_at_utc=now()
    where environment=v_environment and candidate_id=v_candidate_id;

    for v_change in select value from jsonb_array_elements(v_accepted) loop
      insert into public.candidate_daily_availability_days(
        environment,candidate_id,availability_date,preference,availability_version,
        source_class,source_command_id,changed_by_class,row_hash
      )
      values(
        v_environment,v_candidate_id,(v_change->>'date')::date,v_change->>'preference',v_new_version,
        'LEGACY_ADAPTER',v_receipt.command_id,'LEGACY_ADAPTER',
        private._candidate_daily_json_sha256_v1(v_change)
      )
      on conflict(environment,candidate_id,availability_date) do update set
        preference=excluded.preference,
        availability_version=excluded.availability_version,
        source_class=excluded.source_class,
        source_command_id=excluded.source_command_id,
        changed_at_utc=now(),
        changed_by_class=excluded.changed_by_class,
        row_hash=excluded.row_hash;
    end loop;

    insert into private.candidate_daily_sync_state(
      environment,candidate_id,target,accepted_canonical_cursor,required_visible_cursor,
      delivered_visible_cursor,effective_visible_cursor,state,last_acknowledged_at_utc
    )
    values(
      v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',v_new_version,v_new_version,
      v_new_version,v_new_version,'READY',now()
    )
    on conflict(environment,candidate_id,target) do update set
      accepted_canonical_cursor=greatest(private.candidate_daily_sync_state.accepted_canonical_cursor,v_new_version),
      required_visible_cursor=greatest(private.candidate_daily_sync_state.required_visible_cursor,v_new_version),
      delivered_visible_cursor=greatest(private.candidate_daily_sync_state.delivered_visible_cursor,v_new_version),
      effective_visible_cursor=greatest(private.candidate_daily_sync_state.effective_visible_cursor,v_new_version),
      state='READY',
      last_acknowledged_at_utc=now(),
      updated_at_utc=now();
  else
    v_new_version := null;
  end if;

  -- The legacy browser remains operational during the transition, but it must
  -- never replace MyTMS authority. Reassert only the exact Candidate/date values
  -- that the legacy browser just touched. The normal bounded projection worker
  -- performs the Sheet write and preserves booked/system overlay checks.
  for v_change in select value from jsonb_array_elements(v_repairs) loop
    v_repair_preference := null;
    v_repair_version := null;
    v_repair_command_id := null;
    select a.preference,a.availability_version,a.source_command_id
    into v_repair_preference,v_repair_version,v_repair_command_id
    from public.candidate_daily_availability_days a
    where a.environment=v_environment
      and a.candidate_id=v_candidate_id
      and a.availability_date=(v_change->>'date')::date;

    v_repair_preference := coalesce(v_repair_preference,'PENDING');
    v_repair_version := coalesce(v_repair_version,v_scope.canonical_version);
    v_repair_command_id := coalesce(v_repair_command_id,v_receipt.command_id);

    if v_repair_version is null or v_repair_version<1 then
      raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
    end if;

    insert into public.candidate_daily_sheet_projection_outbox(
      environment,candidate_id,availability_date,availability_version,preference,
      command_id,correlation_id
    )
    values(
      v_environment,v_candidate_id,(v_change->>'date')::date,v_repair_version,
      v_repair_preference,v_repair_command_id,p_correlation_id
    )
    on conflict(environment,target,candidate_id,availability_date,availability_version,operation)
    do update set
      state=case
        when public.candidate_daily_sheet_projection_outbox.state='TERMINAL' then 'TERMINAL'
        else 'PENDING'
      end,
      next_available_at_utc=now(),
      overlay_generation_id=null,
      overlay_generation_version=null,
      overlay_source_row_hash=null,
      lease_owner=null,
      lease_token=null,
      lease_expires_at_utc=null,
      safe_error_code=null,
      completed_at_utc=null,
      updated_at_utc=now();
  end loop;

  if jsonb_array_length(v_repairs)>0 then
    insert into private.candidate_daily_sync_state(
      environment,candidate_id,target,accepted_canonical_cursor,required_visible_cursor,
      delivered_visible_cursor,effective_visible_cursor,state
    )
    values(
      v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',v_scope.canonical_version,
      v_scope.canonical_version,0,0,'PENDING'
    )
    on conflict(environment,candidate_id,target) do update set
      accepted_canonical_cursor=greatest(
        private.candidate_daily_sync_state.accepted_canonical_cursor,v_scope.canonical_version
      ),
      required_visible_cursor=greatest(
        private.candidate_daily_sync_state.required_visible_cursor,v_scope.canonical_version
      ),
      state='PENDING',
      updated_at_utc=now();
    perform private._candidate_daily_refresh_sync_state_v1(
      v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',now()
    );
  end if;

  v_terminal := jsonb_strip_nulls(jsonb_build_object(
    'request_receipt_id',v_receipt.command_id,
    'committed_version',v_new_version,
    'outcomes',v_outcomes
  ));

  update public.candidate_daily_command_receipts
  set canonical_version_after=coalesce(v_new_version,v_scope.canonical_version,canonical_version_before),
    state='COMPLETED',
    terminal_http_status=200,
    terminal_body_json=v_terminal,
    terminal_body_sha256=private._candidate_daily_json_sha256_v1(v_terminal),
    completed_at_utc=now(),
    updated_at_utc=now()
  where command_id=v_receipt.command_id;

  return v_terminal;
end;
$function$;

revoke all on function public.candidate_daily_legacy_availability_apply_atomic_v1(
  jsonb,text,uuid,text,jsonb,text
) from public;

do $grants$
begin
  if exists(select 1 from pg_roles where rolname='anon') then
    revoke all on function public.candidate_daily_legacy_availability_apply_atomic_v1(
      jsonb,text,uuid,text,jsonb,text
    ) from anon;
  end if;
  if exists(select 1 from pg_roles where rolname='authenticated') then
    revoke all on function public.candidate_daily_legacy_availability_apply_atomic_v1(
      jsonb,text,uuid,text,jsonb,text
    ) from authenticated;
  end if;
  if exists(select 1 from pg_roles where rolname='service_role') then
    grant execute on function public.candidate_daily_legacy_availability_apply_atomic_v1(
      jsonb,text,uuid,text,jsonb,text
    ) to service_role;
  end if;
end;
$grants$;

commit;
