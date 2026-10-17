-- Bounded rollback-only fixture. Never a durable bootstrap or live admin seed.
-- Run with the exact new QT migration/repeatable in one server-owned rollback.
do $block$
begin
  if pg_catalog.current_database()<>'cloudtms_test_clone'
    or not exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST')
    or exists(select 1 from public.tms_users where id='f7160000-0000-4000-8000-000000000001'
      or email='healthroster-qt-rollback-only@example.invalid') then
    raise exception 'QT_FIXTURE_IDENTITY_OR_VACANCY_REFUSED';
  end if;
end;
$block$;

insert into public.tms_users(id,email,role,is_active,password_hash)
values('f7160000-0000-4000-8000-000000000001','healthroster-qt-rollback-only@example.invalid',
       'admin',true,'!HEALTHROSTER_QT_ROLLBACK_ONLY_NOT_A_LOGIN!');

do $block$
declare
  v_policy jsonb:=jsonb_build_object('version','PUSH_TIMING_POLICY_V1','enabled',true,
    'timezone','Europe/London','normal_quiet_start','00:00','normal_quiet_end','07:00',
    'day_rules',jsonb_build_object('monday',jsonb_build_object('mode','INHERIT'),
      'tuesday',jsonb_build_object('mode','INHERIT'),'wednesday',jsonb_build_object('mode','INHERIT'),
      'thursday',jsonb_build_object('mode','INHERIT'),'friday',jsonb_build_object('mode','INHERIT'),
      'saturday',jsonb_build_object('mode','INHERIT'),'sunday',jsonb_build_object('mode','INHERIT')),
    'bank_holiday_rule',jsonb_build_object('mode','INHERIT'));
  v_get jsonb:=jsonb_build_object('version','PUSH_TIMING_GET_V1',
    'actor_user_id','f7160000-0000-4000-8000-000000000001',
    'agency_id','f7160000-0000-4000-8000-000000000019','environment','TEST');
  v_save jsonb; v_result jsonb; v_original jsonb; v_bad jsonb; v_value text; v_state text;
  v_user_before jsonb; v_global_before jsonb;
begin
  v_user_before:=(select to_jsonb(u)-'password_hash' from public.tms_users u
    where id='f7160000-0000-4000-8000-000000000001');
  v_global_before:=(select to_jsonb(s) from public.weekly_source_global_settings s where singleton);
  begin
    perform public.candidate_push_timing_get_v1(v_get);
    raise exception 'MISSING_DEPLOYMENT_ALLOWED';
  exception when sqlstate 'P0001' then
    if SQLERRM<>'PUSH_TIMING_DEPLOYMENT_UNAVAILABLE' then raise; end if;
  end;
  insert into public.candidate_push_timing_policy(environment,agency_id,revision,policy_json,policy_sha256,updated_at_utc)
    values('TEST','f7160000-0000-4000-8000-000000000019',1,v_policy,
      private.weekly_source_sha256_jsonb_v1('PUSH_TIMING_POLICY_V1',v_policy),clock_timestamp());
  v_result:=public.candidate_push_timing_get_v1(v_get);
  if v_result->>'revision'<>'1' or v_result->'policy' is distinct from v_policy
    or jsonb_typeof(v_result->'revision')<>'string' or v_result->>'can_edit'<>'true'
    or length(v_result->>'policy_sha256')<>64 then raise exception 'QT_FIRST_GET_MISMATCH'; end if;
  v_save:=(v_get-'version')||jsonb_build_object('version','PUSH_TIMING_SAVE_V1',
    'request_id','f7160000-0000-4000-8000-000000000002','expected_revision','1','policy',v_policy);
  v_result:=public.candidate_push_timing_save_v1(v_save);
  if v_result->>'revision'<>'1' then raise exception 'UNCHANGED_POLICY_INCREMENTED'; end if;
  if public.candidate_push_timing_save_v1(v_save) is distinct from v_result then raise exception 'EXACT_REPLAY_CHANGED'; end if;
  v_original:=v_result;
  begin
    perform public.candidate_push_timing_save_v1(jsonb_set(v_save,'{policy,normal_quiet_end}','"08:00"'));
    raise exception 'ALTERED_REPLAY_ALLOWED';
  exception when sqlstate 'P0001' then
    if SQLERRM<>'PUSH_TIMING_IDEMPOTENCY_CONFLICT' then raise; end if;
  end;
  v_save:=jsonb_set(v_save,'{request_id}','"f7160000-0000-4000-8000-000000000003"');
  v_save:=jsonb_set(v_save,'{policy,normal_quiet_end}','"08:00"');
  v_result:=public.candidate_push_timing_save_v1(v_save);
  if v_result->>'revision'<>'2' then raise exception 'CHANGED_POLICY_DID_NOT_INCREMENT_ONCE'; end if;
  if public.candidate_push_timing_save_v1(jsonb_set(jsonb_set(v_save,'{request_id}',
      '"f7160000-0000-4000-8000-000000000002"'),'{policy}',v_policy)) is distinct from v_original then
    raise exception 'OLD_EXACT_REPLAY_NOT_ORIGINAL_RECEIPT'; end if;
  begin
    perform public.candidate_push_timing_save_v1(jsonb_set(v_save,'{request_id}',
      '"f7160000-0000-4000-8000-000000000004"'));
    raise exception 'STALE_NEW_REQUEST_ALLOWED';
  exception when sqlstate 'P0001' then
    if SQLERRM<>'PUSH_TIMING_REVISION_CONFLICT' then raise; end if;
  end;
  -- Exact scalar boundary: no JSON number, float/exponent/sign/leading zeros,
  -- whitespace or bigint overflow is silently coerced.
  for v_bad in select value from jsonb_array_elements('[1,1.0,null,true,"01","+1","-1","1e3"," 1","9223372036854775808"]'::jsonb) loop
    begin
      perform private.weekly_contact_revision_v1(v_bad,true);
      raise exception 'NONCANONICAL_REVISION_ALLOWED';
    exception when sqlstate '22023' then
      if SQLERRM<>'PUSH_TIMING_REQUEST_INVALID' then raise; end if;
    end;
  end loop;
  if private.weekly_contact_revision_v1('"9007199254740993"',true)<>9007199254740993
    or private.weekly_contact_revision_v1('"9223372036854775807"',true)<>9223372036854775807 then
    raise exception 'LARGE_REVISION_LOST'; end if;
  for v_bad in select value from jsonb_array_elements(jsonb_build_array(
    v_policy||jsonb_build_object('extra',true),
    jsonb_set(v_policy,'{timezone}','"UTC"'),
    jsonb_set(v_policy,'{enabled}','null'),
    jsonb_set(v_policy,'{normal_quiet_end}','"00:00"'),
    jsonb_set(v_policy,'{normal_quiet_end}','"24:00"'),
    jsonb_set(v_policy,'{day_rules,monday}',jsonb_build_object('mode','INHERIT','start','08:00')),
    jsonb_set(v_policy,'{bank_holiday_rule}',jsonb_build_object('mode','ALLOW_WINDOW','start','23:00','end','07:00')),
    jsonb_set(v_policy,'{day_rules}',jsonb_build_object('monday',jsonb_build_object('mode','NO_PUSH_DAY'),
      'tuesday',jsonb_build_object('mode','NO_PUSH_DAY'),'wednesday',jsonb_build_object('mode','NO_PUSH_DAY'),
      'thursday',jsonb_build_object('mode','NO_PUSH_DAY'),'friday',jsonb_build_object('mode','NO_PUSH_DAY'),
      'saturday',jsonb_build_object('mode','NO_PUSH_DAY'),'sunday',jsonb_build_object('mode','NO_PUSH_DAY'))))) loop
    begin
      perform private.candidate_push_timing_policy_validate_v1(v_bad);
      raise exception 'INVALID_POLICY_ALLOWED';
    exception when sqlstate '22023' then
      if SQLERRM<>'PUSH_TIMING_REQUEST_INVALID' then raise; end if;
    end;
  end loop;
  begin
    perform public.candidate_push_timing_get_v1(v_get||jsonb_build_object('capability','ignored'));
    raise exception 'UNKNOWN_REQUEST_FIELD_ALLOWED';
  exception when sqlstate '22023' then
    if SQLERRM<>'PUSH_TIMING_REQUEST_INVALID' then raise; end if;
  end;
  begin
    perform public.candidate_push_timing_get_v1(jsonb_set(v_get,'{agency_id}',
      '"f7160000-0000-4000-8000-000000000020"'));
    raise exception 'FOREIGN_DEPLOYMENT_DISCLOSED';
  exception when sqlstate 'P0001' then
    if SQLERRM<>'PUSH_TIMING_DEPLOYMENT_UNAVAILABLE' then raise; end if;
  end;
  begin
    update public.candidate_push_timing_policy set agency_id='f7160000-0000-4000-8000-000000000020';
    raise exception 'DEPLOYMENT_IDENTITY_MUTABLE';
  exception when sqlstate '23514' then
    if SQLERRM<>'PUSH_TIMING_IMMUTABLE_AUTHORITY' then raise; end if;
  end;
  begin
    update public.candidate_push_timing_commands set response_json='{}';
    raise exception 'RECEIPT_MUTABLE';
  exception when sqlstate '23514' then
    if SQLERRM<>'PUSH_TIMING_IMMUTABLE_AUTHORITY' then raise; end if;
  end;
  -- Role loss must reject even the old exact accepted request.
  update public.tms_users set is_active=false where id='f7160000-0000-4000-8000-000000000001';
  begin
    perform public.candidate_push_timing_save_v1(v_save);
    raise exception 'LOST_PERMISSION_REPLAY_ALLOWED';
  exception when sqlstate '42501' then
    if SQLERRM<>'WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED' then raise; end if;
  end;
  update public.tms_users set is_active=true where id='f7160000-0000-4000-8000-000000000001';
  if (select count(*) from public.candidate_push_timing_commands)<>2 then
    raise exception 'FAILED_REQUEST_LEFT_RECEIPT_OR_DUPLICATED'; end if;
  if (select to_jsonb(s) from public.weekly_source_global_settings s where singleton) is distinct from v_global_before then
    raise exception 'UNRELATED_GLOBAL_SETTINGS_CHANGED'; end if;
  foreach v_value in array array['public.candidate_push_timing_policy','public.candidate_push_timing_commands'] loop
    foreach v_state in array array['anon','authenticated','service_role'] loop
      if has_table_privilege(v_state,v_value,'SELECT,INSERT,UPDATE,DELETE') then
        raise exception 'QT_DIRECT_TABLE_EXPOSED % %',v_state,v_value; end if;
    end loop;
  end loop;
  foreach v_state in array array['anon','authenticated'] loop
    if has_function_privilege(v_state,'public.candidate_push_timing_get_v1(jsonb)','EXECUTE')
      or has_function_privilege(v_state,'public.candidate_push_timing_save_v1(jsonb)','EXECUTE') then
      raise exception 'QT_BROWSER_RPC_EXPOSED'; end if;
  end loop;
  if not has_function_privilege('service_role','public.candidate_push_timing_get_v1(jsonb)','EXECUTE')
    or not has_function_privilege('service_role','public.candidate_push_timing_save_v1(jsonb)','EXECUTE')
    or has_function_privilege('service_role','private.candidate_push_timing_policy_validate_v1(jsonb)','EXECUTE') then
    raise exception 'QT_EXACT_SERVICE_ACL_MISMATCH'; end if;
end;
$block$;

select jsonb_build_object('scope','QT_ADMIN_ROLLBACK_FIXTURE_ONLY','assertions','PASS',
  'database',current_database(),'fixture_commands',count(*)) from public.candidate_push_timing_commands;
