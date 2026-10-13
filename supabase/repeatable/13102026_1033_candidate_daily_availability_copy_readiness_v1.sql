-- Separate canonical availability readiness from a committed Candidate's queued Sheet copy.
-- The unchanged current tiles reader is pinned here to repair the earlier upgrade
-- overlap: its complete definition is from the qualified clean PostgreSQL 17 build.
begin;

create or replace function private._candidate_daily_freshness_v1(
  p_environment text,p_candidate_id uuid,p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_scope private.candidate_daily_authority_scopes%rowtype;
  v_generation public.candidate_daily_rota_generations%rowtype;
  v_sync private.candidate_daily_sync_state%rowtype;
  v_oldest integer:=0; v_reasons jsonb:='[]'::jsonb; v_age integer:=0;
  v_today date:=(p_now_utc at time zone 'Europe/London')::date;
  v_deadline timestamptz; v_max_age integer:=1; v_days integer;
  v_first date; v_last date;
  v_background_copy_only boolean:=false;
begin
  select * into v_scope from private.candidate_daily_authority_scopes
    where environment=p_environment and candidate_id=p_candidate_id;
  if v_scope.active_generation_id is not null then
    select * into v_generation from public.candidate_daily_rota_generations
      where generation_id=v_scope.active_generation_id
        and environment=p_environment and candidate_id=p_candidate_id;
  end if;
  select * into v_sync from private.candidate_daily_sync_state
    where environment=p_environment and candidate_id=p_candidate_id and target='MASTER_AVAILABILITY_SHEET';
  select coalesce(greatest(0,extract(epoch from p_now_utc-min(o.created_at_utc))::integer),0)
    into v_oldest from public.candidate_daily_sheet_projection_outbox o
    where o.environment=p_environment and o.candidate_id=p_candidate_id
      and o.state in ('PENDING','RETRY','CLAIMED');
  if v_generation.generation_id is null then
    v_reasons:=v_reasons||'"GENERATION_MISSING"'::jsonb;
  else
    v_age:=greatest(0,extract(epoch from p_now_utc-v_generation.published_at_utc)::integer);
    select count(*),min(rota_date),max(rota_date) into v_days,v_first,v_last
      from public.candidate_daily_rota_days
      where generation_id=v_generation.generation_id
        and environment=p_environment and candidate_id=p_candidate_id;
    if v_generation.state<>'ACTIVE' or v_generation.expected_day_count<>14
       or v_generation.actual_day_count<>14 or v_days<>14
       or v_first is distinct from v_generation.window_start
       or v_last is distinct from v_generation.window_end
       or v_generation.window_end is distinct from v_generation.window_start+13 then
      v_reasons:=v_reasons||'"GENERATION_INCOMPLETE"'::jsonb;
    end if;
    -- No artificial re-dating or two-minute republishing of unchanged shifts.
    -- The expected refresh age remains diagnostic, not a visibility deadline.
    -- Retain the last complete window until its replacement is atomically published.
    v_deadline:=(v_generation.window_start+1)::timestamp at time zone 'Europe/London';
    v_max_age:=greatest(1,ceil(extract(epoch from v_deadline-v_generation.published_at_utc))::integer);
    if p_now_utc is null or v_generation.published_at_utc is null
       or v_generation.window_start>v_today then
      v_reasons:=v_reasons||'"GENERATION_STALE"'::jsonb;
    end if;
  end if;
  if coalesce(v_sync.effective_visible_cursor,0)<coalesce(v_sync.required_visible_cursor,0) then
    -- MyTMS is the availability authority. A completed Candidate command's
    -- queued Sheet copy is not a stale Rota and must not block the next edit.
    -- Retain the strict barrier for transitions, unknown/missing work, failed
    -- overlays, source reconciliation and every non-Candidate command.
    v_background_copy_only:=v_scope.authority_mode='SUPABASE_PRIMARY'
      and not v_scope.transition_in_progress
      and v_sync.accepted_canonical_cursor=v_scope.canonical_version
      and v_sync.required_visible_cursor=v_scope.canonical_version
      and coalesce(v_sync.terminal_count,0)=0
      and (select count(distinct o.availability_version)
        from public.candidate_daily_sheet_projection_outbox o
        where o.environment=p_environment and o.candidate_id=p_candidate_id
          and o.target='MASTER_AVAILABILITY_SHEET'
          and o.availability_version>v_sync.effective_visible_cursor
          and o.availability_version<=v_sync.required_visible_cursor)
        =v_sync.required_visible_cursor-v_sync.effective_visible_cursor
      and not exists(
        select 1 from public.candidate_daily_sheet_projection_outbox o
        left join public.candidate_daily_command_receipts c on c.command_id=o.command_id
          and c.environment=o.environment and c.candidate_id=o.candidate_id
        where o.environment=p_environment and o.candidate_id=p_candidate_id
          and o.target='MASTER_AVAILABILITY_SHEET'
          and o.availability_version>v_sync.effective_visible_cursor
          and (o.availability_version>v_sync.required_visible_cursor
            or o.state not in ('PENDING','CLAIMED','RETRY','DELIVERED')
            or c.actor_class is distinct from 'CANDIDATE'
            or c.command_class is distinct from 'AVAILABILITY_APPLY'
            or c.state is distinct from 'COMPLETED'
            or c.terminal_http_status is distinct from 200
            or c.canonical_version_after is distinct from o.availability_version));
    if v_background_copy_only is distinct from true then
      v_reasons:=v_reasons||'"PROJECTION_LAG"'::jsonb;
    end if;
  end if;
  if coalesce(v_sync.terminal_count,0)>0 then v_reasons:=v_reasons||'"TERMINAL_OUTBOX"'::jsonb; end if;
  if not exists(select 1 from private.candidate_daily_source_links l where l.environment=p_environment
    and l.candidate_id=p_candidate_id and l.state in ('PRIMARY','OVERLAP')) then
    v_reasons:=v_reasons||'"IDENTITY_NOT_READY"'::jsonb;
  end if;
  return jsonb_build_object(
    'generation_version',coalesce(v_generation.generation_version,1),
    'generation_published_at',coalesce(v_generation.published_at_utc,p_now_utc),
    'generation_age_seconds',v_age,
    'canonical_version',coalesce(v_scope.canonical_version,0),
    'accepted_canonical_cursor',coalesce(v_sync.accepted_canonical_cursor,0),
    'required_visible_cursor',coalesce(v_sync.required_visible_cursor,0),
    'delivered_visible_cursor',coalesce(v_sync.delivered_visible_cursor,0),
    'overlay_proof_cursor',coalesce(v_sync.overlay_proof_cursor,0),
    'effective_visible_cursor',coalesce(v_sync.effective_visible_cursor,0),
    'projection_oldest_pending_seconds',v_oldest,
    'generation_max_age_seconds',v_max_age,'projection_warning_seconds',120,
    'ready',jsonb_array_length(v_reasons)=0,'reasons',v_reasons
  );
end;
$function$;


CREATE OR REPLACE FUNCTION public.candidate_daily_tiles_get_v1(p_internal_context jsonb, p_from date DEFAULT NULL::date, p_days integer DEFAULT 14)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare
  v_context jsonb;
  v_environment text;
  v_candidate_id uuid;
  v_policy text;
  v_scope private.candidate_daily_authority_scopes%rowtype;
  v_generation public.candidate_daily_rota_generations%rowtype;
  v_tiles jsonb;
  v_from date;
  v_freshness jsonb;
  v_today date:=(pg_catalog.now() at time zone 'Europe/London')::date;
begin
  v_policy:=pg_catalog.upper(coalesce(
    nullif(p_internal_context->>'policy',''),'CANDIDATE_SURFACE'
  ));
  if v_policy='LEGACY_COMPAT' then
    v_context:=private._candidate_daily_context_v1(p_internal_context,'LEGACY_COMPAT',false);
    v_environment:=v_context->>'environment';
    v_candidate_id:=private._candidate_daily_source_candidate_v1(
      v_environment,p_internal_context->>'candidate_source_hmac'
    );
  elsif v_policy='CANDIDATE_SURFACE' then
    v_context:=private._candidate_daily_context_v1(p_internal_context,'CANDIDATE_SURFACE',true);
    v_environment:=v_context->>'environment';
    v_candidate_id:=(v_context->>'candidate_id')::uuid;
  else
    raise exception using errcode='22023',message='FORBIDDEN';
  end if;
  if p_days<>14 then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select * into v_scope from private.candidate_daily_authority_scopes s
  where s.environment=v_environment and s.candidate_id=v_candidate_id;
  select * into v_generation from public.candidate_daily_rota_generations g
  where g.generation_id=v_scope.active_generation_id and g.state='ACTIVE';
  if v_generation.generation_id is null then
    raise exception using errcode='55000',message='DAILY_GENERATION_UNAVAILABLE';
  end if;
  v_from:=coalesce(p_from,v_generation.window_start);
  -- A request for today may retain the last complete published window.
  -- Never shift its date labels or invent an unpublished fourteenth day.
  if v_from<>v_generation.window_start
     and not(v_policy='CANDIDATE_SURFACE' and v_from=v_today
       and v_generation.window_start<v_today) then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select pg_catalog.jsonb_agg(pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
    'date',d.rota_date,'display_day',pg_catalog.to_char(d.rota_date,'Dy'),
    'display_date',pg_catalog.to_char(d.rota_date,'DD/MM/YYYY'),
    'booked',d.booked,'system_blocked',d.system_blocked,
    'editable',not(d.booked or d.system_blocked)
      and (v_policy<>'CANDIDATE_SURFACE' or d.rota_date>=v_today),
    'status',case when d.booked then 'BOOKED' when d.system_blocked then 'BLOCKED'
      else coalesce(a.preference,'PENDING') end,
    'availability',coalesce(a.preference,'PENDING'),
    'shift_starts_at',d.shift_starts_at,'shift_ends_at',d.shift_ends_at,
    'shift_info',d.shift_info,'hospital',d.hospital,'ward',d.ward,
    'job_title',d.job_title,'booking_ref',d.booking_ref,'shift_type',d.shift_type,
    'booking_id',d.booking_id,'timesheet_authorised',d.timesheet_authorised,
    'timesheet_eligible',case when v_policy='CANDIDATE_SURFACE'
      then candidate_entry.eligible else d.timesheet_eligible end,
    'week_ending_date',case when d.booked then
      d.rota_date+mod(7-extract(dow from d.rota_date)::integer,7) else null end,
    'break_entry',case when v_policy='CANDIDATE_SURFACE' and d.booked
      and nullif(btrim(d.booking_id),'') is not null then
      case when current_daily.timesheet_id is not null then
        private._candidate_daily_read_projection_v1(v_environment,v_candidate_id,
          current_daily.timesheet_id,now())->'break_entry'
      else private._candidate_daily_break_entry_v1(d.booking_id,d.rota_date,null,null,
        candidate_entry.eligible) end else null end,
    'action_target',case
      when v_policy='CANDIDATE_SURFACE' and d.booked then
        case when current_daily.timesheet_id is not null then
          pg_catalog.jsonb_build_object('target_kind','TIMESHEET_DETAIL',
            'timesheet_id',current_daily.timesheet_id,
            'row_signature',public.timesheet_lifecycle_signature_v1(
              current_daily.timesheet_id,null,false)->>'row_signature')
        when candidate_entry.eligible
          and not exists(select 1 from public.timesheets previous where previous.booking_id=d.booking_id)
        then pg_catalog.jsonb_build_object('target_kind','BOOKED_DAILY_SHIFT',
          'source',pg_catalog.jsonb_build_object('generation_id',d.generation_id,
            'work_date',d.rota_date,'source_row_hash',d.source_row_hash,'booking_id',d.booking_id))
        else null end
      else case d.action_target_kind
      when 'TIMESHEET_DETAIL' then pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'target_kind',d.action_target_kind,'timesheet_id',d.action_timesheet_id,
        'workflow_id',d.action_workflow_id,'row_signature',d.action_row_signature))
      when 'CONTRACT_WEEK_DETAIL' then pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'target_kind',d.action_target_kind,'contract_week_id',d.action_contract_week_id,
        'timesheet_id',d.action_timesheet_id,'workflow_id',d.action_workflow_id,
        'row_signature',d.action_row_signature))
      when 'WORKFLOW_DETAIL' then pg_catalog.jsonb_build_object(
        'target_kind',d.action_target_kind,'workflow_id',d.action_workflow_id,
        'workflow_generation',d.action_workflow_generation,'row_signature',d.action_row_signature)
      else null end end
  )) order by d.rota_date) into v_tiles
  from public.candidate_daily_rota_days d
  cross join lateral (
    -- Do not inherit the legacy four-hour flag. A source-only entry is usable
    -- once started, until this exact complete generation is replaced.
    select coalesce(d.booked and not coalesce(d.timesheet_authorised,false)
      and d.shift_starts_at is not null and d.shift_ends_at>d.shift_starts_at
      and d.shift_starts_at<=pg_catalog.now()
      and (d.shift_starts_at at time zone 'Europe/London')::date=d.rota_date
      and nullif(btrim(d.booking_id),'') is not null
      and d.source_row_hash~'^[a-f0-9]{64}$',false) as eligible
  ) candidate_entry
  left join public.candidate_daily_availability_days a
    on a.environment=d.environment and a.candidate_id=d.candidate_id
    and a.availability_date=d.rota_date
  left join lateral (
    select t.timesheet_id from public.timesheets t
    join public.candidates c on c.id=v_candidate_id
      and upper(btrim(c.key_norm))=upper(btrim(t.occupant_key_norm))
    where v_policy='CANDIDATE_SURFACE' and d.booked and t.booking_id=d.booking_id
      and t.sheet_scope='DAILY' and t.is_current and t.archived_at_utc is null
      and not exists(select 1 from public.timesheets_financials f
        where f.timesheet_id=t.timesheet_id and f.is_current
          and f.candidate_id is not null and f.candidate_id<>v_candidate_id)
    limit 1
  ) current_daily on true
  where d.generation_id=v_generation.generation_id;
  if pg_catalog.jsonb_array_length(coalesce(v_tiles,'[]'::jsonb))<>14 then
    raise exception using errcode='55000',message='GENERATION_INCOMPLETE';
  end if;
  v_freshness:=private._candidate_daily_freshness_v1(v_environment,v_candidate_id,pg_catalog.now());
  if v_policy='CANDIDATE_SURFACE'
     and coalesce((v_freshness->>'ready')::boolean,false) is not true then
    raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
  end if;
  return pg_catalog.jsonb_build_object(
    'candidate_id',v_candidate_id,'window_start',v_generation.window_start,
    'window_end',v_generation.window_end,'generation_id',v_generation.generation_id,
    'generation_version',v_generation.generation_version,
    'availability_version',v_scope.canonical_version,'freshness',v_freshness,
    'cohorts','[]'::jsonb,'tiles',v_tiles
  );
end;
$function$;

alter function private._candidate_daily_freshness_v1(text,uuid,timestamptz) owner to postgres;
revoke all on function private._candidate_daily_freshness_v1(text,uuid,timestamptz) from public,anon,authenticated,service_role;
alter function public.candidate_daily_tiles_get_v1(jsonb,date,integer) owner to postgres;
revoke all on function public.candidate_daily_tiles_get_v1(jsonb,date,integer) from public,anon,authenticated;
grant execute on function public.candidate_daily_tiles_get_v1(jsonb,date,integer) to service_role;
commit;
