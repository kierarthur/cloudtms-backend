begin;

create or replace function private._candidate_home_announcement_normalize_v1(p_text text)
returns text
language plpgsql
immutable
set search_path=''
as $function$
declare
  v_text text:=pg_catalog.btrim(
    pg_catalog.replace(pg_catalog.replace(pg_catalog.coalesce(p_text,''),E'\r\n',E'\n'),E'\r',E'\n')
  );
begin
  if pg_catalog.char_length(v_text)>600
     or pg_catalog.position('<' in v_text)>0
     or pg_catalog.position('>' in v_text)>0
     or pg_catalog.regexp_replace(v_text,E'[\n\t]','','g')~'[[:cntrl:]]'
  then
    raise exception using errcode='22023',message='CANDIDATE_HOME_ANNOUNCEMENT_INVALID';
  end if;
  return v_text;
end;
$function$;

create or replace function public.candidate_home_announcement_settings_get_v1()
returns jsonb
language sql
stable
security definer
set search_path=''
as $function$
  select pg_catalog.jsonb_build_object(
    'ok',true,
    'announcement_text',s.candidate_home_announcement_text,
    'version',s.candidate_home_announcement_version,
    'semantic_sha256_hex',pg_catalog.encode(s.candidate_home_announcement_sha256,'hex'),
    'updated_at_utc',s.candidate_home_announcement_updated_at_utc
  )
  from public.settings_defaults s
  where s.id=1
$function$;

create or replace function public.candidate_home_announcement_settings_preview_v1(p_announcement_text text)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_text text:=private._candidate_home_announcement_normalize_v1(p_announcement_text);
  v_hash bytea;
begin
  v_hash:=extensions.digest(pg_catalog.convert_to(v_text,'UTF8'),'sha256');
  return pg_catalog.jsonb_build_object(
    'ok',true,'announcement_text',v_text,
    'semantic_sha256_hex',pg_catalog.encode(v_hash,'hex')
  );
end;
$function$;

create or replace function public.candidate_home_announcement_settings_set_v1(
  p_expected_version bigint,
  p_announcement_text text,
  p_actor_identity_hmac_hex text,
  p_idempotency_key text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_text text:=private._candidate_home_announcement_normalize_v1(p_announcement_text);
  v_hash bytea;
  v_actor bytea;
  v_existing public.candidate_home_announcement_versions%rowtype;
  v_version bigint;
begin
  if p_expected_version is null or p_expected_version<1
     or pg_catalog.coalesce(p_actor_identity_hmac_hex,'')!~'^[0-9a-f]{64}$'
     or pg_catalog.char_length(pg_catalog.btrim(pg_catalog.coalesce(p_idempotency_key,''))) not between 16 and 200
  then
    raise exception using errcode='22023',message='CANDIDATE_HOME_ANNOUNCEMENT_REQUEST_INVALID';
  end if;
  v_hash:=extensions.digest(pg_catalog.convert_to(v_text,'UTF8'),'sha256');
  v_actor:=pg_catalog.decode(p_actor_identity_hmac_hex,'hex');

  select * into v_existing
  from public.candidate_home_announcement_versions h
  where h.idempotency_key=p_idempotency_key;
  if found then
    if v_existing.semantic_sha256<>v_hash then
      raise exception using errcode='40001',message='CANDIDATE_HOME_ANNOUNCEMENT_IDEMPOTENCY_CONFLICT';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'announcement_text',v_existing.announcement_text,
      'version',v_existing.version,
      'semantic_sha256_hex',pg_catalog.encode(v_existing.semantic_sha256,'hex'),
      'updated_at_utc',v_existing.recorded_at_utc,'idempotent_replay',true
    );
  end if;

  update public.settings_defaults s set
    candidate_home_announcement_text=v_text,
    candidate_home_announcement_version=s.candidate_home_announcement_version+1,
    candidate_home_announcement_sha256=v_hash,
    candidate_home_announcement_updated_at_utc=p_now_utc,
    candidate_home_announcement_updated_by_hmac=v_actor
  where s.id=1 and s.candidate_home_announcement_version=p_expected_version
  returning s.candidate_home_announcement_version into v_version;
  if not found then
    raise exception using errcode='40001',message='MYTMS_SETTINGS_VERSION_CONFLICT';
  end if;

  insert into public.candidate_home_announcement_versions(
    version,announcement_text,semantic_sha256,actor_identity_hmac,
    idempotency_key,reason_code,recorded_at_utc
  ) values (
    v_version,v_text,v_hash,v_actor,p_idempotency_key,'OFFICE_SAVE',p_now_utc
  );
  return public.candidate_home_announcement_settings_get_v1()
    ||pg_catalog.jsonb_build_object('idempotent_replay',false);
end;
$function$;

create or replace function public.candidate_home_announcement_settings_reset_v1(
  p_expected_version bigint,
  p_actor_identity_hmac_hex text,
  p_idempotency_key text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_result jsonb;
begin
  v_result:=public.candidate_home_announcement_settings_set_v1(
    p_expected_version,'',p_actor_identity_hmac_hex,p_idempotency_key,p_now_utc
  );
  update public.candidate_home_announcement_versions h
  set reason_code='OFFICE_RESET'
  where h.idempotency_key=p_idempotency_key and h.reason_code='OFFICE_SAVE';
  return v_result;
end;
$function$;

create or replace function private._candidate_home_summary_v1(
  p_environment text,
  p_account_id uuid,
  p_candidate_id uuid,
  p_daily_capability jsonb,
  p_now_utc timestamptz
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_settings public.settings_defaults%rowtype;
  v_unread_count integer:=0;
  v_timesheet_attention_count integer:=0;
  v_next_shift jsonb:=null;
begin
  select * into strict v_settings from public.settings_defaults s where s.id=1;
  if p_account_id is not null then
    select pg_catalog.count(*)::integer into v_unread_count
    from public.candidate_notifications n
    where n.environment=pg_catalog.upper(p_environment)
      and n.account_id=p_account_id and n.state='UNREAD';
  end if;

  if p_candidate_id is not null then
    select pg_catalog.count(*)::integer into v_timesheet_attention_count
    from public.contract_weeks cw
    join public.contracts c on c.id=cw.contract_id and c.candidate_id=p_candidate_id
    left join public.timesheets t on t.timesheet_id=cw.timesheet_id
      and t.is_current=true and t.archived_at_utc is null
    left join lateral (
      select f.* from public.timesheets_financials f
      where f.timesheet_id=t.timesheet_id and f.is_current=true
      order by f.computed_at_utc desc nulls last,f.updated_at desc,f.id desc limit 1
    ) tf on true
    left join lateral (
      select cs.week_ending_weekday
      from public.client_settings cs
      where cs.client_id=c.client_id
        and cs.effective_from<=(p_now_utc at time zone 'Europe/London')::date
      order by cs.effective_from desc,cs.updated_at desc nulls last,cs.id desc limit 1
    ) effective_client on true
    cross join lateral (
      select (
        (p_now_utc at time zone 'Europe/London')::date
        +pg_catalog.mod(
          pg_catalog.coalesce(c.week_ending_weekday_snapshot,effective_client.week_ending_weekday,0)
          -pg_catalog.extract(dow from (p_now_utc at time zone 'Europe/London')::date)::integer+7,
          7
        )
      )::date as current_week_ending_date
    ) current_window
    cross join lateral (
      select private._candidate_record_capabilities_v1(t.timesheet_id,cw.id,'{}'::jsonb) as value
    ) capability
    where cw.week_ending_date<=current_window.current_week_ending_date
      and tf.paid_at_utc is null
      and pg_catalog.coalesce(capability.value->>'record_role','')<>'EXPENSE_ONLY'
      and (
        pg_catalog.coalesce((capability.value->>'can_edit_hours')::boolean,false)
        or pg_catalog.coalesce((capability.value->>'can_edit_expenses')::boolean,false)
        or exists (
          select 1 from public.candidate_submission_workflows w
          where w.candidate_id=p_candidate_id and w.contract_week_id=cw.id
            and w.state='REJECTED'
            and not private._candidate_rejection_replaced_v1(w.id)
        )
      );

    if pg_catalog.coalesce((p_daily_capability->>'enabled')::boolean,false) then
      select pg_catalog.jsonb_build_object(
        'date',d.rota_date,
        'starts_at',d.shift_starts_at,
        'ends_at',d.shift_ends_at,
        'hospital',d.hospital,
        'ward',d.ward,
        'job_title',d.job_title,
        'booking_ref',d.booking_ref
      ) into v_next_shift
      from private.candidate_daily_authority_scopes s
      join public.candidate_daily_rota_generations g
        on g.generation_id=s.active_generation_id and g.state='ACTIVE'
      join public.candidate_daily_rota_days d on d.generation_id=g.generation_id
      where s.environment=pg_catalog.upper(p_environment)
        and s.candidate_id=p_candidate_id
        and d.booked and d.shift_starts_at is not null and d.shift_ends_at>p_now_utc
      order by d.shift_starts_at,d.rota_date,d.booking_id
      limit 1;
    end if;
  end if;

  return pg_catalog.jsonb_build_object(
    'announcement',pg_catalog.jsonb_build_object(
      'text',v_settings.candidate_home_announcement_text,
      'version',v_settings.candidate_home_announcement_version,
      'updated_at_utc',v_settings.candidate_home_announcement_updated_at_utc
    ),
    'timesheets',pg_catalog.jsonb_build_object(
      'attention_count',v_timesheet_attention_count
    ),
    'notifications',pg_catalog.jsonb_build_object('unread_count',v_unread_count),
    'next_shift',v_next_shift
  );
end;
$function$;

create or replace function public.candidate_app_bootstrap_v1(
  p_session_id uuid,
  p_environment text,
  p_expected_rotation integer,
  p_now_utc timestamptz default pg_catalog.now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_context jsonb;
  v_candidate public.candidates%rowtype;
  v_flags jsonb;
  v_contract_enabled boolean:=false;
  v_daily_capability jsonb;
  v_home jsonb;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_reads');
  v_context:=private._candidate_session_context_v1(
    p_session_id,p_environment,p_expected_rotation,p_now_utc,false
  );
  if pg_catalog.nullif(v_context->>'selected_candidate_id','') is not null then
    select * into v_candidate from public.candidates c
    where c.id=(v_context->>'selected_candidate_id')::uuid;
    select exists(
      select 1
      from public.contract_weeks cw
      join public.contracts c on c.id=cw.contract_id and c.candidate_id=v_candidate.id
      left join lateral (
        select cs.week_ending_weekday
        from public.client_settings cs
        where cs.client_id=c.client_id
          and cs.effective_from<=(p_now_utc at time zone 'Europe/London')::date
        order by cs.effective_from desc,cs.updated_at desc nulls last,cs.id desc limit 1
      ) effective_client on true
      cross join lateral (
        select ((p_now_utc at time zone 'Europe/London')::date
          +pg_catalog.mod(
            pg_catalog.coalesce(c.week_ending_weekday_snapshot,effective_client.week_ending_weekday,0)
            -pg_catalog.extract(dow from (p_now_utc at time zone 'Europe/London')::date)::integer+7,
            7
          ))::date as current_week_ending_date
      ) entitlement_window
      left join public.timesheets t on t.timesheet_id=cw.timesheet_id
        and t.is_current=true and t.archived_at_utc is null
      left join public.timesheets_financials tf
        on tf.timesheet_id=t.timesheet_id and tf.is_current=true
      where cw.week_ending_date >= (entitlement_window.current_week_ending_date-interval '6 months')::date
        and cw.week_ending_date <= entitlement_window.current_week_ending_date
        and (
          cw.status in ('OPEN','SUBMITTED','AUTHORISED','INVOICED')
          or tf.processing_status in (
            'UNASSIGNED','CLIENT_UNRESOLVED','RATE_MISSING','PAY_CHANNEL_MISSING',
            'READY_FOR_HR','READY_FOR_INVOICE','PENDING_AUTH',
            'AWAITING_MANUAL_SIGNATURE','UNPROCESSED'
          )
          or cw.week_ending_date=entitlement_window.current_week_ending_date
        )
    ) into v_contract_enabled;
  end if;
  select s.candidate_app_feature_flags_json into v_flags
  from public.settings_defaults s where s.id=1;
  v_daily_capability:=private._candidate_daily_capability_v1(
    pg_catalog.upper(v_context->>'environment'),
    pg_catalog.nullif(v_context->>'selected_candidate_id','')::uuid,
    p_now_utc
  );
  v_home:=private._candidate_home_summary_v1(
    v_context->>'environment',(v_context->>'account_id')::uuid,
    pg_catalog.nullif(v_context->>'selected_candidate_id','')::uuid,
    v_daily_capability,p_now_utc
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,
    'feature_contract_version','candidate-app-private-v1',
    'environment',v_context->>'environment',
    'account_id',v_context->'account_id',
    'selected_candidate_id',v_context->'selected_candidate_id',
    'selection_required',pg_catalog.coalesce(
      (v_context#>>'{eligibility,selection_required}')::boolean,false
    ) and (v_context->>'selected_candidate_id') is null,
    'selectable_candidate_ids',case
      when pg_catalog.upper(v_context->>'environment')='TEST'
      then v_context#>'{eligibility,candidate_ids}' else '[]'::jsonb end,
    'entitlements',pg_catalog.jsonb_build_object(
      'contract',v_contract_enabled,
      'daily',pg_catalog.nullif(pg_catalog.btrim(pg_catalog.coalesce(v_candidate.key_norm,'')),'') is not null,
      'gck_present',pg_catalog.nullif(pg_catalog.btrim(pg_catalog.coalesce(v_candidate.key_norm,'')),'') is not null
    ),
    'notification_preferences',v_context->'notification_preferences',
    'feature_flags',pg_catalog.coalesce(v_flags,'{}'::jsonb),
    'capabilities',pg_catalog.jsonb_build_object('daily_availability',v_daily_capability),
    'home',v_home,
    'session',pg_catalog.jsonb_build_object(
      'rotation',(v_context->>'rotation')::integer,
      'session_version',(v_context->>'session_version')::bigint,
      'expires_at_utc',v_context->'expires_at_utc',
      'absolute_expires_at_utc',v_context->'absolute_expires_at_utc'
    )
  );
end;
$function$;

create or replace function public.candidate_daily_tiles_get_v1(
  p_internal_context jsonb,
  p_from date default null,
  p_days integer default 14
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
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
begin
  v_policy:=pg_catalog.upper(pg_catalog.coalesce(
    pg_catalog.nullif(p_internal_context->>'policy',''),'CANDIDATE_SURFACE'
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
  v_from:=pg_catalog.coalesce(p_from,v_generation.window_start);
  if v_from<>v_generation.window_start then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select pg_catalog.jsonb_agg(pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
    'date',d.rota_date,'display_day',pg_catalog.to_char(d.rota_date,'Dy'),
    'display_date',pg_catalog.to_char(d.rota_date,'DD/MM/YYYY'),
    'booked',d.booked,'system_blocked',d.system_blocked,
    'editable',not(d.booked or d.system_blocked),
    'status',case when d.booked then 'BOOKED' when d.system_blocked then 'BLOCKED'
      else pg_catalog.coalesce(a.preference,'PENDING') end,
    'availability',pg_catalog.coalesce(a.preference,'PENDING'),
    'shift_starts_at',d.shift_starts_at,'shift_ends_at',d.shift_ends_at,
    'shift_info',d.shift_info,'hospital',d.hospital,'ward',d.ward,
    'job_title',d.job_title,'booking_ref',d.booking_ref,'shift_type',d.shift_type,
    'booking_id',d.booking_id,'timesheet_authorised',d.timesheet_authorised,
    'timesheet_eligible',d.timesheet_eligible,
    'action_target',case d.action_target_kind
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
      else null end
  )) order by d.rota_date) into v_tiles
  from public.candidate_daily_rota_days d
  left join public.candidate_daily_availability_days a
    on a.environment=d.environment and a.candidate_id=d.candidate_id
    and a.availability_date=d.rota_date
  where d.generation_id=v_generation.generation_id;
  if pg_catalog.jsonb_array_length(pg_catalog.coalesce(v_tiles,'[]'::jsonb))<>14 then
    raise exception using errcode='55000',message='GENERATION_INCOMPLETE';
  end if;
  v_freshness:=private._candidate_daily_freshness_v1(v_environment,v_candidate_id,pg_catalog.now());
  if v_policy='CANDIDATE_SURFACE'
     and pg_catalog.coalesce((v_freshness->>'ready')::boolean,false) is not true then
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

alter function private._candidate_home_announcement_normalize_v1(text) owner to postgres;
alter function private._candidate_home_summary_v1(text,uuid,uuid,jsonb,timestamptz) owner to postgres;
alter function public.candidate_home_announcement_settings_get_v1() owner to postgres;
alter function public.candidate_home_announcement_settings_preview_v1(text) owner to postgres;
alter function public.candidate_home_announcement_settings_set_v1(bigint,text,text,text,timestamptz) owner to postgres;
alter function public.candidate_home_announcement_settings_reset_v1(bigint,text,text,timestamptz) owner to postgres;
alter function public.candidate_app_bootstrap_v1(uuid,text,integer,timestamptz) owner to postgres;
alter function public.candidate_daily_tiles_get_v1(jsonb,date,integer) owner to postgres;

revoke all on function private._candidate_home_announcement_normalize_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_home_summary_v1(text,uuid,uuid,jsonb,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_home_announcement_settings_get_v1()
  from public,anon,authenticated;
revoke all on function public.candidate_home_announcement_settings_preview_v1(text)
  from public,anon,authenticated;
revoke all on function public.candidate_home_announcement_settings_set_v1(bigint,text,text,text,timestamptz)
  from public,anon,authenticated;
revoke all on function public.candidate_home_announcement_settings_reset_v1(bigint,text,text,timestamptz)
  from public,anon,authenticated;
revoke all on function public.candidate_app_bootstrap_v1(uuid,text,integer,timestamptz)
  from public,anon,authenticated;
revoke all on function public.candidate_daily_tiles_get_v1(jsonb,date,integer)
  from public,anon,authenticated;

grant execute on function public.candidate_home_announcement_settings_get_v1() to service_role;
grant execute on function public.candidate_home_announcement_settings_preview_v1(text) to service_role;
grant execute on function public.candidate_home_announcement_settings_set_v1(bigint,text,text,text,timestamptz)
  to service_role;
grant execute on function public.candidate_home_announcement_settings_reset_v1(bigint,text,text,timestamptz)
  to service_role;
grant execute on function public.candidate_app_bootstrap_v1(uuid,text,integer,timestamptz) to service_role;
grant execute on function public.candidate_daily_tiles_get_v1(jsonb,date,integer) to service_role;

commit;
