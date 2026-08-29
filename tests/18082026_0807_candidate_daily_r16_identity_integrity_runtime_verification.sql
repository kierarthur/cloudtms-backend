\set ON_ERROR_STOP on

begin;

do $test$
declare
  v_system jsonb:=jsonb_build_object(
    'policy','SIGNED_SYSTEM_SYNC','environment','TEST','system_auth_verified',true,
    'nonce_consumed',true,'environment_trusted',true,'stable_operation_identity',true,
    'approved_source_mapping',true,'source_scope_ready',true,'authority_mode_compatible',true,
    'transition_ready',true
  );
  v_days jsonb;
  v_case jsonb;
  v_item jsonb;
  v_result jsonb;
  v_before_links integer;
  v_after_links integer;
begin
  if not exists(
    select 1 from pg_indexes
    where schemaname='public' and indexname='candidates_active_normalized_cid1_uq'
  ) then
    raise exception 'Normalized active CID1 index is missing';
  end if;
  if not exists(
    select 1 from pg_indexes
    where schemaname='private' and indexname='candidate_daily_source_links_history_hmac_uq'
  ) then
    raise exception 'All-history source HMAC index is missing';
  end if;
  if not exists(
    select 1 from pg_trigger
    where tgname='candidate_daily_source_link_identity_history_guard_v1' and not tgisinternal
  ) then
    raise exception 'All-history source HMAC trigger is missing';
  end if;

  insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm) values
    ('00000000-0000-4000-8000-00000000f001','r16-normalized@example.invalid','R16 Normalized','R16','Normalized',true,'CID1-23456789ABCDEFGH'),
    ('00000000-0000-4000-8000-00000000f002','r16-normalized-inactive@example.invalid','R16 Normalized Inactive','R16','Normalized Inactive',false,'  cid1-23456789abcdefgh  ');

  begin
    insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
    values('00000000-0000-4000-8000-00000000f003','r16-normalized-conflict@example.invalid',
      'R16 Normalized Conflict','R16','Normalized Conflict',true,'cid1-23456789abcdefgh');
    raise exception 'Normalized case variant unexpectedly inserted as active';
  exception when unique_violation then
    null;
  end;

  begin
    update public.candidates set active=true where id='00000000-0000-4000-8000-00000000f002';
    raise exception 'Normalized spacing variant unexpectedly became active';
  exception when unique_violation then
    null;
  end;

  insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm) values
    ('00000000-0000-4000-8000-00000000f011','r16-owner-retired@example.invalid','R16 Owner Retired','R16','Owner Retired',true,'CID1-AAAAA23456789BCD'),
    ('00000000-0000-4000-8000-00000000f012','r16-target-retired@example.invalid','R16 Target Retired','R16','Target Retired',true,'CID1-3456789ABCDEFGHJ'),
    ('00000000-0000-4000-8000-00000000f013','r16-owner-rejected@example.invalid','R16 Owner Rejected','R16','Owner Rejected',true,'CID1-BBBBB23456789CDE'),
    ('00000000-0000-4000-8000-00000000f014','r16-target-rejected@example.invalid','R16 Target Rejected','R16','Target Rejected',true,'CID1-456789ABCDEFGHJK'),
    ('00000000-0000-4000-8000-00000000f015','r16-owner-expired@example.invalid','R16 Owner Expired','R16','Owner Expired',true,'CID1-CCCCC23456789DEF'),
    ('00000000-0000-4000-8000-00000000f016','r16-target-expired@example.invalid','R16 Target Expired','R16','Target Expired',true,'CID1-56789ABCDEFGHJKM'),
    ('00000000-0000-4000-8000-00000000f017','r16-owner-future@example.invalid','R16 Owner Future','R16','Owner Future',true,'CID1-DDDDD23456789EFG'),
    ('00000000-0000-4000-8000-00000000f018','r16-target-future@example.invalid','R16 Target Future','R16','Target Future',true,'CID1-6789ABCDEFGHJKMN'),
    ('00000000-0000-4000-8000-00000000f019','r16-same-retired@example.invalid','R16 Same Retired','R16','Same Retired',true,'CID1-789ABCDEFGHJKMNP');

  insert into private.candidate_daily_source_links(
    environment,candidate_id,source_system,canonicalization_version,link_group_id,
    identifier_hmac,hmac_key_version,state,valid_from_utc,valid_to_utc,evidence_sha256
  ) values
    ('TEST','00000000-0000-4000-8000-00000000f011','GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
      '00000000-0000-4000-8000-00000000f111',repeat('6',64),1,'RETIRED',now()-interval '3 days',now()-interval '2 days',repeat('e',64)),
    ('TEST','00000000-0000-4000-8000-00000000f013','GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
      '00000000-0000-4000-8000-00000000f112',repeat('7',64),1,'REJECTED',now()-interval '3 days',now()-interval '2 days',repeat('e',64)),
    ('TEST','00000000-0000-4000-8000-00000000f015','GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
      '00000000-0000-4000-8000-00000000f113',repeat('8',64),1,'PRIMARY',now()-interval '3 days',now()-interval '2 days',repeat('e',64)),
    ('TEST','00000000-0000-4000-8000-00000000f017','GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
      '00000000-0000-4000-8000-00000000f114',repeat('9',64),1,'PRIMARY',now()+interval '2 days',null,repeat('e',64)),
    ('TEST','00000000-0000-4000-8000-00000000f019','GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
      '00000000-0000-4000-8000-00000000f115',repeat('a',64),1,'RETIRED',now()-interval '3 days',now()-interval '2 days',repeat('e',64));

  select jsonb_agg(jsonb_build_object(
    'date',(date '2026-08-18'+n)::text,'booked',false,'system_blocked',false,
    'source_row_hash',lpad(to_hex(n+101),64,'0')
  ) order by n) into v_days from generate_series(0,13)n;

  for v_case in select value from jsonb_array_elements(jsonb_build_array(
    jsonb_build_object('label','retired-other','candidate_id','00000000-0000-4000-8000-00000000f012','global_key','CID1-3456789ABCDEFGHJ','hmac',repeat('6',64)),
    jsonb_build_object('label','rejected-other','candidate_id','00000000-0000-4000-8000-00000000f014','global_key','CID1-456789ABCDEFGHJK','hmac',repeat('7',64)),
    jsonb_build_object('label','expired-other','candidate_id','00000000-0000-4000-8000-00000000f016','global_key','CID1-56789ABCDEFGHJKM','hmac',repeat('8',64)),
    jsonb_build_object('label','future-other','candidate_id','00000000-0000-4000-8000-00000000f018','global_key','CID1-6789ABCDEFGHJKMN','hmac',repeat('9',64)),
    jsonb_build_object('label','retired-same','candidate_id','00000000-0000-4000-8000-00000000f019','global_key','CID1-789ABCDEFGHJKMNP','hmac',repeat('a',64))
  )) loop
    select count(*) into v_before_links
    from private.candidate_daily_source_links
    where environment='TEST' and candidate_id=(v_case->>'candidate_id')::uuid;

    v_item:=jsonb_build_object(
      'candidate_global_key',v_case->>'global_key',
      'candidate_source_hmac',v_case->>'hmac',
      'source_hmac_key_version',1,
      'source_event_id','rota-event-r16-'||(v_case->>'label'),
      'source_revision','r16-history-'||(v_case->>'label'),
      'source_hash',encode(extensions.digest(
        convert_to('r16-history-'||(v_case->>'label'),'UTF8'),'sha256'),'hex'),
      'window_start','2026-08-18','days',v_days,
      'source_event_time','2026-08-18T00:05:00Z',
      'item_key','rota-item-r16-'||(v_case->>'label')
    );
    v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(
      v_system,gen_random_uuid(),'candidate-r16-history-'||(v_case->>'label'),
      jsonb_build_array(v_item),'01K2ABCDEFGHJKMNPQRSTVWXYZ'
    );
    if v_result#>>'{outcomes,0,status}'<>'REJECTED'
       or v_result#>>'{outcomes,0,error_code}'<>'IDENTITY_LINK_CONFLICT' then
      raise exception 'Historical ownership case % did not reject: %',v_case->>'label',v_result;
    end if;
    select count(*) into v_after_links
    from private.candidate_daily_source_links
    where environment='TEST' and candidate_id=(v_case->>'candidate_id')::uuid;
    if v_after_links<>v_before_links
       or exists(select 1 from private.candidate_daily_authority_scopes
         where environment='TEST' and candidate_id=(v_case->>'candidate_id')::uuid)
       or exists(select 1 from public.candidate_daily_rota_generations
         where environment='TEST' and candidate_id=(v_case->>'candidate_id')::uuid) then
      raise exception 'Historical ownership case % left partial authority',v_case->>'label';
    end if;
  end loop;

  begin
    update private.candidate_daily_source_links
    set identifier_hmac=repeat('6',64)
    where link_group_id='00000000-0000-4000-8000-00000000f112';
    raise exception 'Identity-changing update unexpectedly reused historical HMAC';
  exception when unique_violation then
    null;
  end;
end;
$test$;

do $acl$
begin
  if has_function_privilege('public',
      'private._candidate_daily_source_candidate_bind_on_generation_v1(text,text,text,integer)',
      'execute') then
    raise exception 'PUBLIC can execute the first-generation binder';
  end if;
  if has_function_privilege('public',
      'private._candidate_daily_source_link_identity_history_guard_v1()',
      'execute') then
    raise exception 'PUBLIC can execute the source-history guard';
  end if;
  if exists(select 1 from pg_roles where rolname='anon') and has_function_privilege('anon',
      'private._candidate_daily_source_candidate_bind_on_generation_v1(text,text,text,integer)',
      'execute') then
    raise exception 'anon can execute the first-generation binder';
  end if;
  if exists(select 1 from pg_roles where rolname='authenticated') and has_function_privilege('authenticated',
      'private._candidate_daily_source_candidate_bind_on_generation_v1(text,text,text,integer)',
      'execute') then
    raise exception 'authenticated can execute the first-generation binder';
  end if;
  if exists(select 1 from pg_roles where rolname='service_role') then
    if has_function_privilege('service_role',
        'private._candidate_daily_source_candidate_bind_on_generation_v1(text,text,text,integer)',
        'execute') then
      raise exception 'service_role can execute the first-generation binder';
    end if;
    if not has_function_privilege('service_role',
        'public.candidate_daily_rota_generation_publish_atomic_v1(jsonb,uuid,text,jsonb,text)',
        'execute') then
      raise exception 'service_role cannot execute the public generation RPC';
    end if;
  end if;
end;
$acl$;

rollback;

select 'candidate daily R16 identity-integrity runtime verification passed' as result;
