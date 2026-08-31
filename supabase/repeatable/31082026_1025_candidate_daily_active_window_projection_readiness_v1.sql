-- Candidate Daily Rota readiness is governed by projection work for the active
-- published 14-day window. Historical outbox rows remain immutable audit
-- evidence but cannot permanently hide a later complete window.
\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_daily_refresh_sync_state_v1(
  p_environment text,
  p_candidate_id uuid,
  p_target text default 'MASTER_AVAILABILITY_SHEET',
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_scope private.candidate_daily_authority_scopes%rowtype;
  v_generation public.candidate_daily_rota_generations%rowtype;
  v_delivered bigint:=0;
  v_overlay bigint:=0;
  v_effective bigint:=0;
  v_pending integer:=0;
  v_retry integer:=0;
  v_deferred integer:=0;
  v_terminal integer:=0;
  v_state text:='LAGGING';
begin
  select s.* into v_scope
  from private.candidate_daily_authority_scopes s
  where s.environment=p_environment and s.candidate_id=p_candidate_id;
  if v_scope.candidate_id is null then
    raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
  end if;
  if v_scope.active_generation_id is not null then
    select g.* into v_generation
    from public.candidate_daily_rota_generations g
    where g.generation_id=v_scope.active_generation_id
      and g.environment=p_environment and g.candidate_id=p_candidate_id;
  end if;

  select
    coalesce(min(o.availability_version) filter(where o.state<>'DELIVERED')-1,v_scope.canonical_version),
    coalesce(max(o.availability_version) filter(where o.state='DEFERRED_OVERLAY'
      and o.overlay_generation_id=v_scope.active_generation_id
      and exists(
        select 1
        from public.candidate_daily_rota_generations g
        join public.candidate_daily_rota_days d on d.generation_id=g.generation_id
          and d.environment=o.environment and d.candidate_id=o.candidate_id
          and d.rota_date=o.availability_date
        where g.generation_id=v_scope.active_generation_id and g.state='ACTIVE'
          and g.generation_version=o.overlay_generation_version
          and (d.booked or d.system_blocked)
          and d.source_row_hash=o.overlay_source_row_hash
      )),0),
    coalesce(min(o.availability_version) filter(where not(
      o.state='DELIVERED'
      or (o.state='DEFERRED_OVERLAY'
        and o.overlay_generation_id=v_scope.active_generation_id
        and exists(
          select 1
          from public.candidate_daily_rota_generations g
          join public.candidate_daily_rota_days d on d.generation_id=g.generation_id
            and d.environment=o.environment and d.candidate_id=o.candidate_id
            and d.rota_date=o.availability_date
          where g.generation_id=v_scope.active_generation_id and g.state='ACTIVE'
            and g.generation_version=o.overlay_generation_version
            and (d.booked or d.system_blocked)
            and d.source_row_hash=o.overlay_source_row_hash
        ))) )-1,v_scope.canonical_version),
    count(*) filter(where o.state in ('PENDING','CLAIMED')),
    count(*) filter(where o.state='RETRY'),
    count(*) filter(where o.state='DEFERRED_OVERLAY'),
    count(*) filter(where o.state='TERMINAL')
  into v_delivered,v_overlay,v_effective,v_pending,v_retry,v_deferred,v_terminal
  from public.candidate_daily_sheet_projection_outbox o
  where o.environment=p_environment and o.candidate_id=p_candidate_id and o.target=p_target
    and (
      v_generation.generation_id is null
      or o.availability_date between v_generation.window_start and v_generation.window_end
    );

  v_delivered:=greatest(0,least(v_scope.canonical_version,coalesce(v_delivered,0)));
  v_overlay:=greatest(0,least(v_scope.canonical_version,coalesce(v_overlay,0)));
  v_effective:=greatest(0,least(v_scope.canonical_version,coalesce(v_effective,0)));
  if v_terminal>0 then v_state:='ERROR';
  elsif v_effective>=v_scope.canonical_version then v_state:='READY';
  else v_state:='LAGGING'; end if;

  insert into private.candidate_daily_sync_state(environment,candidate_id,target,
    accepted_canonical_cursor,required_visible_cursor,delivered_visible_cursor,overlay_proof_cursor,
    effective_visible_cursor,pending_count,retry_count,deferred_count,terminal_count,state,
    last_acknowledged_at_utc,updated_at_utc)
  values(p_environment,p_candidate_id,p_target,v_scope.canonical_version,v_scope.canonical_version,
    v_delivered,v_overlay,v_effective,v_pending,v_retry,v_deferred,v_terminal,v_state,p_now_utc,p_now_utc)
  on conflict(environment,candidate_id,target) do update set
    accepted_canonical_cursor=excluded.accepted_canonical_cursor,
    required_visible_cursor=excluded.required_visible_cursor,
    delivered_visible_cursor=excluded.delivered_visible_cursor,
    overlay_proof_cursor=excluded.overlay_proof_cursor,
    effective_visible_cursor=excluded.effective_visible_cursor,
    pending_count=excluded.pending_count,retry_count=excluded.retry_count,
    deferred_count=excluded.deferred_count,terminal_count=excluded.terminal_count,
    state=excluded.state,last_acknowledged_at_utc=excluded.last_acknowledged_at_utc,
    updated_at_utc=excluded.updated_at_utc;

  return jsonb_build_object('delivered_visible_cursor',v_delivered,
    'overlay_proof_cursor',v_overlay,'effective_visible_cursor',v_effective,
    'pending_count',v_pending,'retry_count',v_retry,'deferred_count',v_deferred,
    'terminal_count',v_terminal,'state',v_state);
end;
$function$;

alter function private._candidate_daily_refresh_sync_state_v1(text,uuid,text,timestamptz)
  owner to postgres;
revoke all on function private._candidate_daily_refresh_sync_state_v1(text,uuid,text,timestamptz)
  from public,anon,authenticated,service_role;

commit;
