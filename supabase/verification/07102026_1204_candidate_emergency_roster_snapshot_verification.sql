-- Rollback-contained full Master roster / callable DNA / effect replay proof.
-- Includes stale, ambiguous and caller-supplied subject negative cases.
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
  v_subject jsonb;
  v_context jsonb;
  v_input jsonb;
  v_claim jsonb;
  v_roster jsonb;
  v_bad jsonb;
  v_case integer;
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
  -- Complete Master contacts are a separate signed-source projection, not
  -- dependent on the colleague being enrolled in MyTMS.
  v_result:=public.candidate_daily_specialist_read_v1(
    jsonb_build_object('policy','CANDIDATE_SURFACE','environment','TEST','candidate_id',v_candidate_id),
    'EMERGENCY_ROSTER_PUBLISH',jsonb_build_object('emergency_shift_token',v_token,'roster',jsonb_build_object(
      'emergency_shift_token',v_token,'captured_at',clock_timestamp(),'groups',jsonb_build_object(
        'current',jsonb_build_array(jsonb_build_object('display_name','Unenrolled colleague','role','RMN','callable_mobile','447111111111')),
        'previous','[]'::jsonb,'next','[]'::jsonb))),v_shift_start,'01M18D00000000000000000000');
  if v_result->>'accepted' is distinct from 'true' then raise exception 'Emergency roster publication failed'; end if;
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
     or not (v_result->'allowed_issues' ? 'DNA')
     or v_result#>>'{dna_subjects,0,callable_mobile}' is distinct from '447111111111'
     or coalesce(v_result#>>'{dna_subjects,0,subject_token}','') !~ '^[a-f0-9]{64}$'
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
  -- Service-only projection ACL and no enrolment requirement for DNA subject.
  if has_table_privilege('service_role','private.candidate_daily_emergency_roster_snapshots','SELECT,INSERT,UPDATE,DELETE')
    or has_table_privilege('anon','private.candidate_daily_emergency_roster_snapshots','SELECT,INSERT,UPDATE,DELETE')
    or has_table_privilege('authenticated','private.candidate_daily_emergency_roster_snapshots','SELECT,INSERT,UPDATE,DELETE') then
    raise exception 'Emergency roster direct table ACL opened';
  end if;
  v_subject:=v_result#>'{shifts,0,dna_subjects,0}';
  v_context:=jsonb_build_object('policy','CANDIDATE_SURFACE','environment','TEST','candidate_id',v_candidate_id);
  v_input:=jsonb_build_object('type','DNA','emergency_shift_token',v_token,
    'subject_token',v_subject->>'subject_token','tried_calling',false);
  begin
    perform public.candidate_daily_effect_claim_candidate_v1(v_context,'DNA',v_input,
      'emergency-roster-proof','emergency-dna-'||v_candidate_id::text,120,v_shift_start,'01M18D00000000000000000000');
    raise exception 'DNA accepted without tried calling';
  exception when sqlstate '22023' then
    if sqlerrm<>'SEMANTIC_REJECTION' then raise; end if;
  end;
  v_input:=v_input||'{"tried_calling":true}'::jsonb;
  v_claim:=public.candidate_daily_effect_claim_candidate_v1(v_context,'DNA',v_input,
    'emergency-roster-proof','emergency-dna-'||v_candidate_id::text,120,v_shift_start,'01M18D00000000000000000000');
  if v_claim->>'state' is distinct from 'CLAIMED'
    or v_claim#>>'{effect_payload,dna_subject,callable_mobile}' is distinct from '447111111111'
    or v_claim#>>'{effect_payload,dna_subject,display_name}' is distinct from 'Unenrolled colleague' then
    raise exception 'DNA exact callable identity not preserved';
  end if;
  -- A lost HTTP response must recover its exact key while the original executor
  -- is still running or its lease has expired, without another claim or send.
  v_result:=public.candidate_daily_specialist_read_v1(v_context,'EFFECT_REPLAY',
    jsonb_build_object('operation','DNA','input',v_input,'idempotency_key','emergency-dna-'||v_candidate_id::text),
    v_shift_start+interval '1 second','01M18D00000000000000000000');
  if v_result->>'state' is distinct from 'IN_PROGRESS'
    or v_result#>>'{safe_result,effect_key}' is distinct from v_claim->>'effect_key'
    or v_result#>>'{safe_result,status}' is distinct from 'IN_PROGRESS' then
    raise exception 'In-progress replay did not recover the original safe status';
  end if;
  v_result:=public.candidate_daily_specialist_read_v1(v_context,'EFFECT_REPLAY',
    jsonb_build_object('operation','DNA','input',v_input,'idempotency_key','emergency-dna-'||v_candidate_id::text),
    v_shift_start+interval '121 seconds','01M18D00000000000000000000');
  if v_result#>>'{safe_result,effect_key}' is distinct from v_claim->>'effect_key'
    or (select attempt_count from private.candidate_daily_external_effect_receipts
      where effect_receipt_id=(v_claim->>'effect_receipt_id')::uuid)<>1 then
    raise exception 'Expired pending replay replaced/reclaimed the original attempt';
  end if;
  -- No external executor is invoked. Complete the test-only receipt UNKNOWN.
  perform public.candidate_daily_effect_complete_candidate_v1(v_context,
    (v_claim->>'effect_receipt_id')::uuid,v_claim->>'lease_token','UNKNOWN',null,
    'Fixture only; no messages sent',v_shift_start+interval '1 second','01M18D00000000000000000000');
  delete from private.candidate_daily_emergency_roster_snapshots where environment='TEST' and candidate_id=v_candidate_id;
  v_result:=public.candidate_daily_specialist_read_v1(v_context,'EFFECT_REPLAY',
    jsonb_build_object('operation','DNA','input',v_input,'idempotency_key','emergency-dna-'||v_candidate_id::text),
    v_shift_end+interval '1 day','01M18D00000000000000000000');
  if v_result->>'state' is distinct from 'UNKNOWN' then raise exception 'Uncertain replay requires source/resend'; end if;
  begin
    perform public.candidate_daily_specialist_read_v1(v_context,'EFFECT_REPLAY',
      jsonb_build_object('operation','DNA','input',v_input||'{"reason_text":"different"}'::jsonb,
        'idempotency_key','emergency-dna-'||v_candidate_id::text),v_shift_start,'01M18D00000000000000000000');
    raise exception 'Changed payload accepted under existing request key';
  exception when sqlstate '23505' then if sqlerrm<>'SOURCE_EVENT_CONFLICT' then raise; end if; end;
  begin
    perform public.candidate_daily_specialist_read_v1(v_context,'EMERGENCY_WINDOW','{}',v_shift_start,'01M18D00000000000000000000');
    raise exception 'Missing roster treated as zero colleagues';
  exception when sqlstate '55000' then if sqlerrm<>'DEPENDENCY_UNAVAILABLE' then raise; end if; end;
  v_roster:=jsonb_build_object('emergency_shift_token',v_token,'captured_at',clock_timestamp(),'groups',
    jsonb_build_object('current',jsonb_build_array(jsonb_build_object(
      'display_name','Unenrolled colleague','role','RMN','callable_mobile','447111111111')),'previous','[]'::jsonb,'next','[]'::jsonb));
  for v_case in 1..8 loop
    v_bad:=v_roster;
    if v_case=1 then v_bad:=jsonb_set(v_bad,'{groups,current,0,callable_mobile}','"07111111111"'); end if;
    if v_case=2 then v_bad:=jsonb_set(v_bad,'{groups,current,0,callable_mobile}','"447000000000"'); end if;
    if v_case=3 then v_bad:=jsonb_set(v_bad,'{groups,current}',(v_roster#>'{groups,current}')||(v_roster#>'{groups,current}')); end if;
    if v_case=4 then v_bad:=v_bad-'groups'; end if;
    if v_case=5 then v_bad:=v_bad||jsonb_build_object('captured_at',clock_timestamp()-interval '2 minutes'); end if;
    if v_case=6 then v_bad:=v_bad||jsonb_build_object('captured_at',clock_timestamp()+interval '1 minute'); end if;
    if v_case=7 then v_bad:=v_bad||jsonb_build_object('emergency_shift_token',repeat('f',64)); end if;
    if v_case=8 then v_bad:=jsonb_set(v_bad,'{groups,current,0,subject_token}',to_jsonb(repeat('f',64)),true); end if;
    begin
      perform public.candidate_daily_specialist_read_v1(v_context,'EMERGENCY_ROSTER_PUBLISH',
        jsonb_build_object('emergency_shift_token',v_token,'roster',v_bad),v_shift_start,'01M18D00000000000000000000');
      raise exception 'Invalid roster case % accepted',v_case;
    exception when sqlstate '22023' or sqlstate '55000' then
      if sqlerrm not in ('VALIDATION_FAILED','DEPENDENCY_UNAVAILABLE') then raise; end if;
    end;
  end loop;
  perform public.candidate_daily_specialist_read_v1(v_context,'EMERGENCY_ROSTER_PUBLISH',
    jsonb_build_object('emergency_shift_token',v_token,'roster',v_roster),v_shift_start,'01M18D00000000000000000000');
  update private.candidate_daily_emergency_roster_snapshots set
    observed_at_utc=p.instant-interval '6 minutes',expires_at_utc=p.instant-interval '1 minute'
    from (select clock_timestamp() as instant) p where environment='TEST' and candidate_id=v_candidate_id;
  begin
    perform public.candidate_daily_specialist_read_v1(v_context,'EMERGENCY_WINDOW','{}',v_shift_start,'01M18D00000000000000000000');
    raise exception 'Expired roster accepted';
  exception when sqlstate '55000' then if sqlerrm<>'DEPENDENCY_UNAVAILABLE' then raise; end if; end;
  update public.candidate_daily_rota_days set source_row_hash=repeat('b',64)
    where generation_id=v_generation_id and rota_date=v_today;
  begin
    perform public.candidate_daily_specialist_read_v1(v_context,'EMERGENCY_ROSTER_PUBLISH',
      jsonb_build_object('emergency_shift_token',v_token,'roster',v_roster),v_shift_start,'01M18D00000000000000000000');
    raise exception 'Changed booking accepted the old shift token';
  exception when sqlstate '02000' then if sqlerrm<>'NOT_FOUND' then raise; end if; end;

end;
$verification$;

rollback;
