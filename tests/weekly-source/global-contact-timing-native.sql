-- Define and invoke ONLY inside the integrator's server-owned TEST rollback.
-- Actual existing actor/Agency/Client/incident/generation identities are inputs.
-- No fake source rows, provider submission or permanent settings changes.
create or replace function pg_temp.contact_timing_settings_roundtrip_v1(p_actor uuid,p_agency uuid,p_environment text)
returns jsonb language plpgsql as $function$
declare context jsonb:=jsonb_build_object('actor_user_id',p_actor,'agency_id',p_agency,'environment',p_environment);
 before_state jsonb;settings jsonb;request jsonb;first_state jsonb;saved jsonb;replayed jsonb;unchanged jsonb;refused boolean;
begin
 if current_database()<>'cloudtms_test_clone' then raise exception 'TEST_DATABASE_REQUIRED';end if;
 before_state:=public.weekly_source_global_settings_get_v1(context);
 settings:=before_state->'settings'||jsonb_build_object('candidate_reminder_minutes',121,'candidate_response_deadline_minutes',763,
  'manager_digest_minutes',137,'manager_manual_resend_cooldown_minutes',11,'candidate_manual_reminder_cooldown_minutes',83,'manager_secure_link_days',9,
  'manager_first_delay_value',0,'manager_first_delay_unit','HOURS');
 first_state:=public.weekly_source_global_settings_save_atomic_v1(context||jsonb_build_object('request_id',gen_random_uuid(),
  'expected_settings_version',before_state->'settings_version','settings',settings));
 settings:=first_state->'settings'||jsonb_build_object('manager_first_delay_value',1,'manager_followups','[]'::jsonb);
 request:=context||jsonb_build_object('request_id',gen_random_uuid(),'expected_settings_version',first_state->'settings_version','settings',settings);
 saved:=public.weekly_source_global_settings_save_atomic_v1(request);
 replayed:=public.weekly_source_global_settings_save_atomic_v1(request);
 if saved is distinct from replayed or jsonb_typeof(saved->'settings_version')<>'string'
  or (saved->'settings')-array['manager_first_delay_value','manager_followups'] is distinct from
     (first_state->'settings')-array['manager_first_delay_value','manager_followups']
  or saved->'settings'->>'manager_digest_minutes'<>'137' or saved->'settings'->>'manager_manual_resend_cooldown_minutes'<>'11' then
  raise exception 'SETTINGS_ROUNDTRIP_OR_REPLAY_FAILED';end if;
 refused:=false;
 begin perform public.weekly_source_global_settings_save_atomic_v1(jsonb_set(request,'{settings,manager_first_delay_value}','2'));
 exception when serialization_failure then refused:=sqlerrm='WEEKLY_SOURCE_SETTINGS_REQUEST_REUSED';end;
 if not refused then raise exception 'ALTERED_REPLAY_NOT_REJECTED';end if;
 refused:=false;
 begin perform public.weekly_source_global_settings_save_atomic_v1(jsonb_set(request,'{request_id}',to_jsonb(gen_random_uuid())));
 exception when serialization_failure then refused:=sqlerrm='WEEKLY_SOURCE_SETTINGS_STALE';end;
 if not refused then raise exception 'STALE_CAS_NOT_REJECTED';end if;
 unchanged:=public.weekly_source_global_settings_save_atomic_v1(context||jsonb_build_object('request_id',gen_random_uuid(),
  'expected_settings_version',saved->'settings_version','settings',settings));
 if unchanged is distinct from saved then raise exception 'UNCHANGED_SAVE_ADVANCED_VERSION';end if;
 foreach settings in array array[
  jsonb_set(saved->'settings','{manager_first_delay_unit}','"DAYS"'),
  (saved->'settings')||jsonb_build_object('manager_partial_digest_minutes',360),
  (saved->'settings')||jsonb_build_object('manager_manual_send_cooldown_minutes',5),
  jsonb_set(saved->'settings','{manager_followups}','[{"sequence":2,"interval_value":1,"interval_unit":"HOURS","mode":"MANUAL"}]')
 ] loop
  refused:=false;
  begin perform public.weekly_source_global_settings_save_atomic_v1(context||jsonb_build_object('request_id',gen_random_uuid(),
   'expected_settings_version',saved->'settings_version','settings',settings));
  exception when invalid_parameter_value then refused:=sqlerrm='WEEKLY_SOURCE_GLOBAL_SETTINGS_VALUES_INVALID';end;
  if not refused then raise exception 'INVALID_SETTINGS_NOT_REJECTED';end if;
 end loop;
 return jsonb_build_object('ok',true,'version',saved->'settings_version','native_route_office_acceptance','REQUIRED_NOT_RUN');
end;
$function$;

-- The existing QT/calendar fixture supplies the actual rollback-only admin.
-- This is native SQL settings/receipt proof, not deployed Office acceptance.
do $fixture$
declare context jsonb:=jsonb_build_object('actor_user_id','f7160000-0000-4000-8000-000000000001','agency_id','f7160000-0000-4000-8000-000000000019','environment','TEST');
 before_state jsonb;request jsonb;receipt jsonb;refused boolean;role_name text;
begin
 if current_database()<>'cloudtms_test_clone' then raise exception 'GLOBAL_TIMING_TEST_REQUIRED';end if;
 update public.weekly_source_global_settings set version=9007199254740993 where singleton;
 perform pg_temp.contact_timing_settings_roundtrip_v1('f7160000-0000-4000-8000-000000000001','f7160000-0000-4000-8000-000000000019','TEST');
 before_state:=public.weekly_source_global_settings_get_v1(context);
 if jsonb_typeof(before_state->'settings_version')<>'string' or (before_state->>'settings_version')::bigint<=9007199254740992 then raise exception 'LARGE_REVISION_LOST';end if;
 request:=context||jsonb_build_object('request_id',gen_random_uuid(),'expected_settings_version',before_state->'settings_version','settings',before_state->'settings');
 receipt:=public.weekly_source_global_settings_save_atomic_v1(request);
 update public.tms_users set is_active=false where id=(context->>'actor_user_id')::uuid;
 refused:=false;
 begin perform public.weekly_source_global_settings_save_atomic_v1(request);exception when insufficient_privilege then refused:=sqlerrm='WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED';end;
 if not refused then raise exception 'PERMISSION_LOSS_REPLAY_ALLOWED';end if;
 update public.tms_users set is_active=true where id=(context->>'actor_user_id')::uuid;
 if public.weekly_source_global_settings_save_atomic_v1(request) is distinct from receipt then raise exception 'IMMUTABLE_SAVE_REPLAY_CHANGED';end if;
 refused:=false;
 begin update public.weekly_source_global_settings_receipts set response_json='{}' where request_id=(request->>'request_id')::uuid;exception when insufficient_privilege then refused:=sqlerrm='WEEKLY_SOURCE_SETTINGS_RECEIPT_IMMUTABLE';end;
 if not refused then raise exception 'RECEIPT_HISTORY_REWRITE_ALLOWED';end if;
 foreach role_name in array array['anon','authenticated','service_role'] loop
  if has_table_privilege(role_name,'public.weekly_source_global_settings_receipts','SELECT,INSERT,UPDATE,DELETE') then raise exception 'GLOBAL_RECEIPT_TABLE_EXPOSED';end if;
 end loop;
 if not exists(select 1 from pg_class where oid='public.weekly_source_global_settings_receipts'::regclass and relrowsecurity and relforcerowsecurity)
  or has_function_privilege('authenticated','public.weekly_source_global_settings_save_atomic_v1(jsonb)','EXECUTE')
  or has_function_privilege('anon','public.weekly_source_global_settings_get_v1(jsonb)','EXECUTE')
  or not has_function_privilege('service_role','public.weekly_source_global_settings_save_atomic_v1(jsonb)','EXECUTE') then raise exception 'GLOBAL_SETTINGS_RLS_RPC_ACL_INVALID';end if;
 raise notice 'GLOBAL_TIMING_NATIVE_SETTINGS_CAS_REPLAY_137_11_LARGE_REVISION_PERMISSION_ACL_PASS; SCHEDULER_OFFICE_PROVIDER_NOT_RUN';
end;$fixture$;
set constraints all immediate;
