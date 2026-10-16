-- Exact new checking-control owners only, after the QT/calendar fixtures.
-- No generation, clock activation, provider, payment, mail or drain owner runs.
do $fixture$
declare v_read jsonb;v_command jsonb;v_reply jsonb;v_original jsonb;v_before jsonb;v_bad jsonb;v_value text;
 v_until timestamptz;v_id uuid;v_count bigint;
begin
 if current_database()<>'cloudtms_test_clone' then raise exception 'CONTACT_FIXTURE_WRONG_DATABASE';end if;
 if exists(select 1 from public.weekly_contact_pause_scopes)
  or exists(select 1 from public.weekly_contact_pause_commands)
  or exists(select 1 from public.weekly_contact_pause_events)
  or exists(select 1 from public.weekly_contact_submission_permits)
  or exists(select 1 from public.weekly_contact_delivery_memberships)
  or exists(select 1 from public.weekly_contact_recheck_tasks) then raise exception 'CONTACT_FIXTURE_NEW_STORES_NOT_EMPTY';end if;
 select to_jsonb(s) into v_before from public.weekly_source_global_settings s where singleton;
 if private.weekly_contact_next_monday_v1('2026-10-15T08:00Z') is distinct from '2026-10-18T23:00Z'::timestamptz
  or private.weekly_contact_next_monday_v1('2026-10-18T23:00Z') is distinct from '2026-10-26T00:00Z'::timestamptz
  or private.weekly_contact_next_monday_v1('2026-03-27T09:00Z') is distinct from '2026-03-29T23:00Z'::timestamptz
  then raise exception 'STRICT_FUTURE_LONDON_MONDAY_DST_FAILED';end if;
 v_read:=jsonb_build_object('candidate_id','f7160000-0000-4000-8000-000000000010',
  'client_id','f7160000-0000-4000-8000-000000000011','actor_user_id','f7160000-0000-4000-8000-000000000001',
  'agency_id','f7160000-0000-4000-8000-000000000019','environment','TEST');
 perform set_config('request.jwt.claim.role','service_role',true);
 v_reply:=public.weekly_source_contact_pause_get_v1(v_read);
 if (v_reply#>>'{scope,next_pause_until_utc}')::timestamptz is distinct from private.weekly_contact_next_monday_v1(transaction_timestamp())
  or v_reply#>>'{scope,next_pause_until_local}' is distinct from
   to_char(private.weekly_contact_next_monday_v1(transaction_timestamp()) at time zone 'Europe/London','YYYY-MM-DD HH24:MI')||' Europe/London' then
  raise exception 'SERVER_MONDAY_PREVIEW_MISSING_OR_CHANGED';end if;
 if v_reply#>>'{scope,revision}' is distinct from '0' or v_reply#>'{scope,worker,paused}' is distinct from 'false'::jsonb
  or v_reply#>'{scope,manager,paused}' is distinct from 'false'::jsonb
  or exists(select 1 from public.weekly_contact_pause_scopes) then raise exception 'ABSENT_SCOPE_GET_MUTATED_OR_FALSE_CONFLICT';end if;
 v_command:=v_read||jsonb_build_object('request_id','f7160000-0000-4000-8000-000000000020','expected_revision','0','audience','WORKER','note','Fixture checking pause');
 v_original:=public.weekly_source_contact_pause_atomic_v1(v_command);v_reply:=v_original;
 if v_reply#>>'{scope,revision}' is distinct from '1' or v_reply#>'{scope,worker,paused}' is distinct from 'true'::jsonb
  or v_reply#>'{scope,manager,paused}' is distinct from 'false'::jsonb or v_reply->'changed' is distinct from 'true'::jsonb
  or v_reply->'automatic_send' is distinct from 'false'::jsonb then raise exception 'FIRST_WORKER_PAUSE_FAILED';end if;
 if public.weekly_source_contact_pause_atomic_v1(v_command) is distinct from v_original then raise exception 'EXACT_PAUSE_REPLAY_CHANGED';end if;
 v_until:=(v_reply#>>'{scope,worker,paused_until_utc}')::timestamptz;v_id:=(v_reply#>>'{scope,scope_id}')::uuid;
 v_reply:=private.weekly_contact_scope_guard_v1((v_read->>'candidate_id')::uuid,(v_read->>'client_id')::uuid,'WORKER',v_until);
 if v_reply->'allow' is distinct from 'true'::jsonb then raise exception 'EXACT_EXPIRY_NOT_LAZY_INACTIVE';end if;
 begin perform public.weekly_source_contact_pause_atomic_v1(jsonb_set(v_command,'{note}','"Changed payload"'));
  raise exception 'CHANGED_PAUSE_KEY_ALLOWED';
 exception when sqlstate '23505' then if SQLERRM<>'CONTACT_IDEMPOTENCY_CONFLICT' then raise;end if;end;
 begin perform public.weekly_source_contact_pause_atomic_v1(jsonb_set(v_command,'{request_id}','"f7160000-0000-4000-8000-000000000021"'));
  raise exception 'STALE_PAUSE_ALLOWED';
 exception when sqlstate '40001' then if SQLERRM<>'CONTACT_REVISION_CONFLICT' then raise;end if;end;
 v_reply:=public.weekly_source_contact_pause_atomic_v1(v_command||jsonb_build_object('request_id','f7160000-0000-4000-8000-000000000021','expected_revision','1','audience','MANAGER'));
 if v_reply#>>'{scope,revision}' is distinct from '2' or v_reply#>'{scope,worker,paused}' is distinct from 'true'::jsonb
  or v_reply#>'{scope,manager,paused}' is distinct from 'true'::jsonb then raise exception 'INDEPENDENT_MANAGER_PAUSE_FAILED';end if;
 v_reply:=public.weekly_source_contact_resume_atomic_v1(v_command||jsonb_build_object('request_id','f7160000-0000-4000-8000-000000000022','expected_revision','2'));
 if v_reply#>>'{scope,revision}' is distinct from '3' or v_reply#>'{scope,worker,paused}' is distinct from 'false'::jsonb
  or v_reply#>'{scope,manager,paused}' is distinct from 'true'::jsonb or v_reply->>'recheck_task_id' is null then
  raise exception 'PARTIAL_RESUME_CLEARED_OTHER_AUDIENCE';end if;
 v_reply:=public.weekly_source_contact_resume_atomic_v1(v_command||jsonb_build_object('request_id','f7160000-0000-4000-8000-000000000023','expected_revision','3','audience','BOTH'));
 if v_reply#>>'{scope,revision}' is distinct from '4' or v_reply#>'{scope,worker,paused}' is distinct from 'false'::jsonb
  or v_reply#>'{scope,manager,paused}' is distinct from 'false'::jsonb then raise exception 'BOTH_RESUME_FAILED';end if;
 v_reply:=public.weekly_source_contact_resume_atomic_v1(v_command||jsonb_build_object('request_id','f7160000-0000-4000-8000-000000000024','expected_revision','4','audience','BOTH'));
 if v_reply#>>'{scope,revision}' is distinct from '4' or v_reply->'changed' is distinct from 'false'::jsonb
  or v_reply->>'recheck_task_id' is null then raise exception 'NOOP_RESUME_REVISION_OR_WAKE_FAILED';end if;
 -- Replay remains the exact original accepted acknowledgement despite newer state.
 if public.weekly_source_contact_pause_atomic_v1(v_command) is distinct from v_original then raise exception 'LATER_REPLAY_REWROTE_HISTORY';end if;
 update public.tms_users set is_active=false where id=(v_read->>'actor_user_id')::uuid;
 begin perform public.weekly_source_contact_pause_atomic_v1(v_command);raise exception 'LOST_PERMISSION_PAUSE_REPLAY_ALLOWED';
 exception when sqlstate '42501' then if SQLERRM<>'WEEKLY_SOURCE_OFFICE_ACTOR_INACTIVE' then raise;end if;end;
 update public.tms_users set is_active=true where id=(v_read->>'actor_user_id')::uuid;
 for v_bad in select value from jsonb_array_elements('[0,1,"01","-1","+1","1e2","9223372036854775808",null]'::jsonb) loop
  begin perform public.weekly_source_contact_pause_atomic_v1(jsonb_set(v_command,'{expected_revision}',v_bad));raise exception 'BAD_CONTACT_REVISION_ALLOWED';
  exception when sqlstate '22023' then if SQLERRM<>'CONTACT_REQUEST_INVALID' then raise;end if;end;
 end loop;
 begin perform public.weekly_source_contact_pause_atomic_v1(v_command||'{"actor_id":"f7160000-0000-4000-8000-000000000001"}');
  raise exception 'UNKNOWN_CONTACT_FIELD_ALLOWED';
 exception when sqlstate '22023' then if SQLERRM<>'CONTACT_REQUEST_INVALID' then raise;end if;end;
 begin update public.weekly_contact_pause_events set reason='Rewrite';raise exception 'CONTACT_EVENT_MUTABLE';
 exception when sqlstate '23514' then if SQLERRM<>'CONTACT_IMMUTABLE_HISTORY' then raise;end if;end;
 begin update public.weekly_contact_pause_commands set response_json='{}';raise exception 'CONTACT_COMMAND_MUTABLE';
 exception when sqlstate '23514' then if SQLERRM<>'CONTACT_IMMUTABLE_HISTORY' then raise;end if;end;
 if(select count(*) from public.weekly_contact_pause_events)<>4 or(select count(*) from public.weekly_contact_pause_commands)<>5
  or(select count(*) from public.weekly_contact_recheck_tasks)<>5 then raise exception 'CONTACT_ONCE_ONLY_EVENT_COMMAND_OR_WAKE_FAILED';end if;
 if(select to_jsonb(s) from public.weekly_source_global_settings s where singleton) is distinct from v_before then raise exception 'CONTACT_EDITED_EXISTING_CLOCK_POLICY';end if;
 foreach v_value in array array['weekly_contact_pause_scopes','weekly_contact_pause_commands','weekly_contact_pause_events',
  'weekly_contact_submission_permits','weekly_contact_delivery_memberships','weekly_contact_recheck_tasks'] loop
  if has_table_privilege('anon','public.'||v_value,'SELECT,INSERT,UPDATE,DELETE')
   or has_table_privilege('authenticated','public.'||v_value,'SELECT,INSERT,UPDATE,DELETE')
   or has_table_privilege('service_role','public.'||v_value,'SELECT,INSERT,UPDATE,DELETE') then raise exception 'CONTACT_DIRECT_TABLE_EXPOSED';end if;
 end loop;
 if has_function_privilege('authenticated','public.weekly_source_contact_pause_atomic_v1(jsonb)','EXECUTE')
  or has_function_privilege('service_role','private.weekly_contact_scope_lock_v1(uuid,text,uuid,uuid)','EXECUTE') then raise exception 'CONTACT_RPC_ACL_EXPOSED';end if;
end;
$fixture$;
set constraints all immediate;
select jsonb_build_object('scope','CONTACT_PAUSE_CONTROL_ROLLBACK_ONLY','assertions','PASS','database',current_database(),
 'commands',(select count(*) from public.weekly_contact_pause_commands),'events',(select count(*) from public.weekly_contact_pause_events));
