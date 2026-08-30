-- Rollback-contained first-use proof for the Candidate Daily emergency window.
-- The regression was a composite-row assignment (`select d into %rowtype`) that
-- compiled successfully but failed on its first real booked shift.
\set ON_ERROR_STOP on

begin;

do $verification$
declare
  v_candidate_id uuid:=gen_random_uuid();
  v_generation_id uuid:=gen_random_uuid();
  v_batch_id uuid:=gen_random_uuid();
  v_link_group_id uuid:=gen_random_uuid();
  v_today date:='2026-08-30'::date;
  v_shift_start timestamptz:='2026-08-30 07:30:00+01'::timestamptz;
  v_shift_end timestamptz:='2026-08-30 20:00:00+01'::timestamptz;
  v_source_hash text:=repeat('a',64);
  v_token text;
  v_result jsonb;
  v_definition text;
  v_day integer;
begin
  select pg_get_functiondef(
    'private._candidate_daily_specialist_shift_v1(jsonb,text,timestamptz)'::regprocedure
  ) into v_definition;
  if position('select d.* into v_day' in lower(v_definition))=0
     or lower(v_definition) ~ 'select[[:space:]]+d[[:space:]]+into[[:space:]]+v_day'
     or v_definition~*'pg_catalog\.(coalesce|nullif|least|greatest)[[:space:]]*\('
  then
    raise exception 'CANDIDATE_DAILY_EMERGENCY_WINDOW_ROW_PROOF: row assignment is unsafe';
  end if;
  if not exists(
    select 1 from pg_proc p
    where p.oid='private._candidate_daily_specialist_shift_v1(jsonb,text,timestamptz)'::regprocedure
      and p.prosecdef and p.provolatile='s'
      and p.proconfig @> array['search_path=""']::text[]
  ) then
    raise exception 'CANDIDATE_DAILY_EMERGENCY_WINDOW_ROW_PROOF: function security changed';
  end if;
  if has_function_privilege(
       'service_role',
       'private._candidate_daily_specialist_shift_v1(jsonb,text,timestamptz)',
       'EXECUTE'
     )
     or has_function_privilege(
       'anon',
       'private._candidate_daily_specialist_shift_v1(jsonb,text,timestamptz)',
       'EXECUTE'
     )
     or has_function_privilege(
       'authenticated',
       'private._candidate_daily_specialist_shift_v1(jsonb,text,timestamptz)',
       'EXECUTE'
     )
  then
    raise exception 'CANDIDATE_DAILY_EMERGENCY_WINDOW_ROW_PROOF: private ACL opened';
  end if;

  update public.settings_defaults
  set candidate_app_feature_flags_json=
    candidate_app_feature_flags_json||'{"candidate_daily_enabled":true}'::jsonb
  where id=1;

  insert into public.candidates(id,email,display_name,phone,active)
  values(
    v_candidate_id,
    'emergency-window-'||v_candidate_id::text||'@example.invalid',
    'Emergency window proof','07000000000',true
  );
  insert into private.candidate_daily_authority_scopes(
    environment,candidate_id,authority_mode,canonical_version,transition_in_progress
  ) values('TEST',v_candidate_id,'SUPABASE_PRIMARY',1,false);
  insert into private.candidate_daily_entitlements(
    environment,candidate_id,enabled,reason,evidence_sha256
  ) values('TEST',v_candidate_id,true,'Emergency window proof',repeat('1',64));
  insert into private.candidate_daily_source_links(
    environment,candidate_id,source_system,canonicalization_version,
    link_group_id,identifier_hmac,hmac_key_version,state,evidence_sha256
  ) values(
    'TEST',v_candidate_id,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
    v_link_group_id,repeat('2',64),1,'PRIMARY',repeat('3',64)
  );
  insert into private.candidate_daily_batch_receipts(
    batch_receipt_id,environment,actor_class,operation_class,idempotency_key,
    request_hash,item_keys_json,item_count,state,terminal_http_status,
    terminal_response_body,terminal_response_sha256,correlation_id,completed_at_utc
  ) values(
    v_batch_id,'TEST','SIGNED_SYSTEM','ROTA_GENERATION_PUBLISH',
    'emergency-window-'||v_batch_id::text,repeat('4',64),
    jsonb_build_array(v_candidate_id::text),1,'COMPLETED',200,'{}'::jsonb,
    repeat('5',64),'01M18D00000000000000000000',clock_timestamp()
  );
  insert into public.candidate_daily_rota_generations(
    generation_id,environment,candidate_id,generation_version,window_start,
    window_end,state,expected_day_count,actual_day_count,source_system,
    source_event_id,source_revision,source_event_time,item_key,source_hash,
    generation_row_hash,batch_receipt_id,correlation_id,activated_at_utc,
    published_at_utc
  ) values(
    v_generation_id,'TEST',v_candidate_id,1,v_today,v_today+13,'ACTIVE',14,14,
    'MASTER_ROTA','emergency-window-source-'||v_generation_id::text,
    'emergency-window-v1',clock_timestamp()-interval '1 day',
    'emergency-window-item-'||v_generation_id::text,repeat('6',64),repeat('7',64),
    v_batch_id,'01M18D00000000000000000000',clock_timestamp()-interval '1 day',
    clock_timestamp()-interval '1 day'
  );
  for v_day in 0..13 loop
    insert into public.candidate_daily_rota_days(
      generation_id,environment,candidate_id,rota_date,booked,system_blocked,
      booking_id,shift_starts_at,shift_ends_at,shift_info,hospital,ward,
      job_title,booking_ref,shift_type,timesheet_authorised,timesheet_eligible,
      source_row_hash
    ) values(
      v_generation_id,'TEST',v_candidate_id,v_today+v_day,v_day=0,false,
      case when v_day=0 then 'emergency-window-booking' else null end,
      case when v_day=0 then v_shift_start else null end,
      case when v_day=0 then v_shift_end else null end,
      case when v_day=0 then 'Long Day 0730-2000hrs' else null end,
      case when v_day=0 then 'North General Hospital' else null end,
      case when v_day=0 then 'Ward 1' else null end,
      case when v_day=0 then 'Registered Nurse' else null end,
      case when v_day=0 then 'TEST-BOOKING' else null end,
      case when v_day=0 then 'Long Day' else null end,
      false,false,
      case when v_day=0 then v_source_hash
        else repeat(substr(md5(v_candidate_id::text||v_day::text),1,1),64) end
    );
  end loop;
  update private.candidate_daily_authority_scopes
  set active_generation_id=v_generation_id
  where environment='TEST' and candidate_id=v_candidate_id;

  v_token:=private._candidate_daily_emergency_token_v1(
    'TEST',v_candidate_id,v_generation_id,v_today,v_source_hash
  );
  v_result:=private._candidate_daily_specialist_shift_v1(
    jsonb_build_object(
      'policy','CANDIDATE_SURFACE','environment','TEST','candidate_id',v_candidate_id
    ),v_token,v_shift_start
  );
  if v_result->>'date' is distinct from v_today::text
     or (v_result->>'starts_at')::timestamptz is distinct from v_shift_start
     or (v_result->>'ends_at')::timestamptz is distinct from v_shift_end
     or not (v_result->'allowed_issues' ? 'RUNNING_LATE')
     or not (v_result->'allowed_issues' ? 'LEAVE_EARLY')
  then
    raise exception 'CANDIDATE_DAILY_EMERGENCY_WINDOW_ROW_PROOF: private first use is wrong: %',
      v_result;
  end if;

  v_result:=public.candidate_daily_specialist_read_v1(
    jsonb_build_object(
      'policy','CANDIDATE_SURFACE','environment','TEST','candidate_id',v_candidate_id
    ),'EMERGENCY_WINDOW','{}'::jsonb,v_shift_start,
    '01M18D00000000000000000000'
  );
  if v_result->>'eligible' is distinct from 'true'
     or jsonb_array_length(v_result->'shifts')<>1
     or v_result#>>'{shifts,0,date}' is distinct from v_today::text
     or v_result#>>'{shifts,0,display_label}' not like '%North General Hospital%'
     or (v_result#>'{shifts,0}') ? '_agency_payload'
  then
    raise exception 'CANDIDATE_DAILY_EMERGENCY_WINDOW_ROW_PROOF: public first use is wrong: %',
      v_result;
  end if;
end;
$verification$;

rollback;
