-- Run after the exact administration fixture in one server-owned rollback.
-- Explicit instants below are pure function inputs, not business timestamps.
do $fixture$
declare v_policy jsonb;v_reply jsonb;v_holidays jsonb;v_expected jsonb;v_request jsonb;v_bad jsonb;v_role text;
 v_state_before jsonb;v_candidates_before bigint;v_clients_before bigint;
begin
 if current_database()<>'cloudtms_test_clone' then raise exception 'CALENDAR_FIXTURE_WRONG_DATABASE';end if;
 if exists(select 1 from public.candidates where id='f7160000-0000-4000-8000-000000000010'
   or email='healthroster-qt-calendar@example.invalid' or key_norm='HR_QT_CALENDAR_ROLLBACK')
  or exists(select 1 from public.clients where id='f7160000-0000-4000-8000-000000000011') then
  raise exception 'CALENDAR_FIXTURE_NOT_VACANT';end if;
 select policy_json into strict v_policy from public.candidate_push_timing_policy
  where environment='TEST' and agency_id='f7160000-0000-4000-8000-000000000019';
 v_policy:=jsonb_set(v_policy,'{normal_quiet_end}','"07:00"');
 v_reply:=private.weekly_contact_push_timing_decision_v1(v_policy,'2026-10-15T23:30Z','2026-10-17T12:00Z',null);
 if v_reply->>'reason' is distinct from 'QUIET' or v_reply->>'next_allowed_utc' is distinct from '2026-10-16T06:00:00.000000Z'
  or v_reply->'allowed_now' is distinct from 'false'::jsonb then raise exception 'SUMMER_NORMAL_QUIET_FAILED';end if;
 v_reply:=private.weekly_contact_push_timing_decision_v1(v_policy,'2026-10-16T06:00Z','2026-10-17T12:00Z',null);
 if v_reply->>'reason' is distinct from 'ALLOWED' or v_reply->'allowed_now' is distinct from 'true'::jsonb then
  raise exception 'QUIET_EXCLUSIVE_END_FAILED';end if;
 v_reply:=private.weekly_contact_push_timing_decision_v1(v_policy,'2026-10-15T23:30Z','2026-10-16T06:00Z',null);
 if v_reply->>'reason' is distinct from 'EXPIRED_TIMING' or v_reply->'next_allowed_utc' is distinct from 'null'::jsonb then
  raise exception 'EXPIRY_EXCLUSIVE_FAILED';end if;
 -- Missing spring-forward01:30 ends at the first real instant02:00BST.
 v_policy:=jsonb_set(v_policy,'{normal_quiet_end}','"01:30"');
 v_reply:=private.weekly_contact_push_timing_decision_v1(v_policy,'2026-03-29T00:45Z','2026-03-30T12:00Z',null);
 if v_reply->>'next_allowed_utc' is distinct from '2026-03-29T01:00:00.000000Z' then
  raise exception 'SPRING_GAP_END_FAILED';end if;
 -- Autumn01:30 occurs twice. Even first01:45 remains quiet until later01:30.
 v_reply:=private.weekly_contact_push_timing_decision_v1(v_policy,'2026-10-25T00:45Z','2026-10-26T12:00Z',null);
 if v_reply->>'next_allowed_utc' is distinct from '2026-10-25T01:30:00.000000Z' then
  raise exception 'AUTUMN_LATER_END_FAILED';end if;
 if private.weekly_contact_london_boundary_v1('2026-10-25','01:30',false) is distinct from '2026-10-25T00:30Z'::timestamptz
  or private.weekly_contact_london_boundary_v1('2026-10-25','01:30',true) is distinct from '2026-10-25T01:30Z'::timestamptz then
  raise exception 'AUTUMN_EXACT_BOUNDARY_FAILED';end if;
 v_policy:=jsonb_set(jsonb_set(v_policy,'{normal_quiet_start}','"22:00"'),'{normal_quiet_end}','"07:00"');
 v_reply:=private.weekly_contact_push_timing_decision_v1(v_policy,'2026-10-25T00:45Z','2026-10-26T12:00Z',null);
 if v_reply->>'next_allowed_utc' is distinct from '2026-10-25T07:00:00.000000Z' then
  raise exception 'WRAPPED_QUIET_DST_FAILED';end if;
 v_policy:=jsonb_set(v_policy,'{day_rules,saturday}','{"mode":"ALLOW_WINDOW","start":"09:00","end":"13:00"}');
 v_policy:=jsonb_set(v_policy,'{day_rules,sunday}','{"mode":"NO_PUSH_DAY"}');
 v_reply:=private.weekly_contact_push_timing_decision_v1(v_policy,'2026-10-17T06:00Z','2026-10-20T12:00Z',null);
 if v_reply->>'next_allowed_utc' is distinct from '2026-10-17T08:00:00.000000Z' then raise exception 'SATURDAY_WINDOW_FAILED';end if;
 v_reply:=private.weekly_contact_push_timing_decision_v1(v_policy,'2026-10-17T12:00Z','2026-10-20T12:00Z',null);
 if v_reply->>'next_allowed_utc' is distinct from '2026-10-19T06:00:00.000000Z' then raise exception 'WINDOW_END_AND_SUNDAY_FAILED';end if;
 v_policy:=jsonb_set(v_policy,'{bank_holiday_rule}','{"mode":"NO_PUSH_DAY"}');
 v_holidays:='{"2026-10-19":true,"2026-10-20":false}';
 v_reply:=private.weekly_contact_push_timing_decision_v1(v_policy,'2026-10-19T09:00Z','2026-10-20T12:00Z',v_holidays);
 if v_reply->>'next_allowed_utc' is distinct from '2026-10-20T06:00:00.000000Z' then raise exception 'HOLIDAY_PRECEDENCE_FAILED';end if;
 begin
  perform private.weekly_contact_push_timing_decision_v1(v_policy,'2026-10-19T09:00Z','2026-10-20T12:00Z',null);
  raise exception 'MISSING_REQUIRED_CALENDAR_ALLOWED';
 exception when sqlstate 'P0001' then if SQLERRM<>'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' then raise;end if;end;
 v_policy:=jsonb_set(v_policy,'{enabled}','false');
 v_reply:=private.weekly_contact_push_timing_decision_v1(v_policy,'2026-10-19T00:00Z','2026-10-20T12:00Z',null);
 if v_reply->>'reason' is distinct from 'ALLOWED' then raise exception 'DISABLED_QT_DEPENDS_ON_CALENDAR';end if;
 v_policy:=jsonb_set(v_policy,'{enabled}','true');
 select jsonb_object_agg(d::date::text,true) into v_holidays from generate_series('2026-10-19'::timestamp,'2026-11-15'::timestamp,'1 day')d;
 v_reply:=private.weekly_contact_push_timing_decision_v1(v_policy,'2026-10-19T09:00Z','2026-12-20T12:00Z',v_holidays);
 if v_reply->>'reason' is distinct from 'EXPIRED_TIMING' or v_reply->'allowed_now' is distinct from 'false'::jsonb then
  raise exception 'HORIZON_NOT_BOUNDED_28_DATES';end if;
 select jsonb_build_object('bh_list',s.bh_list,'bh_feed_list',s.bh_feed_list) into strict v_state_before
 from public.settings_defaults s where id=1;
 insert into public.candidates(id,email,display_name,active,key_norm) values
  ('f7160000-0000-4000-8000-000000000010','healthroster-qt-calendar@example.invalid','QT rollback calendar fixture',true,'HR_QT_CALENDAR_ROLLBACK');
 insert into public.clients(id,name) values('f7160000-0000-4000-8000-000000000011','QT rollback calendar fixture');
 insert into public.client_settings(client_id,effective_from,default_submission_mode,week_ending_weekday)
  values('f7160000-0000-4000-8000-000000000011','2026-01-01','ELECTRONIC',0);
 v_request:=jsonb_build_object('version','PUSH_CALENDAR_DECISION_V1',
  'candidate_id','f7160000-0000-4000-8000-000000000010','client_id',null,
  'agency_id','f7160000-0000-4000-8000-000000000019','environment','TEST',
  'local_dates',jsonb_build_array('2026-10-20','2026-10-19'));
 perform set_config('request.jwt.claim.role','service_role',true);
 v_reply:=public.weekly_source_push_calendar_decision_v1(v_request);
 if v_reply->>'timezone' is distinct from 'Europe/London' or v_reply->>'calendar_digest' is null
  or v_reply->>'calendar_digest' !~ '^[0-9a-f]{64}$'
  or jsonb_array_length(v_reply->'dates') is distinct from 2 or v_reply->'dates'->0->>'local_date' is distinct from '2026-10-19' then
  raise exception 'AGENCY_NULL_CLIENT_CALENDAR_FAILED';end if;
 v_expected:=public.weekly_source_push_calendar_decision_v1(jsonb_set(v_request,'{client_id}','"f7160000-0000-4000-8000-000000000011"'));
 if v_expected is distinct from v_reply then raise exception 'ACTUAL_CLIENT_CALENDAR_CHANGED_AUTHORITY';end if;
 for v_bad in select value from jsonb_array_elements(jsonb_build_array(
  v_request||'{"actor_user_id":"f7160000-0000-4000-8000-000000000001"}',
  jsonb_set(v_request,'{local_dates}','["2026-10-19","2026-10-19"]'),
  jsonb_set(v_request,'{local_dates}','["2026-02-30"]'),
  jsonb_set(v_request,'{local_dates}','["2026-2-3"]'),
  jsonb_set(v_request,'{local_dates}','[20261019]'),
  jsonb_set(v_request,'{local_dates}','[]'),
  jsonb_set(v_request,'{agency_id}','["f7160000-0000-4000-8000-000000000019"]')
 )) loop
  begin perform public.weekly_source_push_calendar_decision_v1(v_bad);raise exception 'BAD_CALENDAR_REQUEST_ALLOWED';
  exception when sqlstate '22023' then if SQLERRM<>'PUSH_TIMING_REQUEST_INVALID' then raise;end if;end;
 end loop;
 begin perform public.weekly_source_push_calendar_decision_v1(jsonb_set(v_request,'{agency_id}','"f7160000-0000-4000-8000-000000000099"'));
  raise exception 'FOREIGN_CALENDAR_DEPLOYMENT_ALLOWED';
 exception when sqlstate 'P0001' then if SQLERRM<>'PUSH_TIMING_DEPLOYMENT_UNAVAILABLE' then raise;end if;end;
 perform set_config('request.jwt.claim.role','authenticated',true);
 begin perform public.weekly_source_push_calendar_decision_v1(v_request);raise exception 'BROWSER_CALENDAR_ALLOWED';
 exception when sqlstate '42501' then if SQLERRM<>'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' then raise;end if;end;
 perform set_config('request.jwt.claim.role','service_role',true);
 foreach v_role in array array['anon','authenticated'] loop
  if has_function_privilege(v_role,'public.weekly_source_push_calendar_decision_v1(jsonb)','EXECUTE') then
   raise exception 'CALENDAR_BROWSER_ACL_EXPOSED';end if;
 end loop;
 if (select jsonb_build_object('bh_list',s.bh_list,'bh_feed_list',s.bh_feed_list) from public.settings_defaults s where id=1)
  is distinct from v_state_before then raise exception 'CALENDAR_WROTE_EXISTING_AUTHORITY';end if;
end;
$fixture$;
select jsonb_build_object('scope','QT_TIMING_AND_EXISTING_CALENDAR_ROLLBACK_ONLY',
 'assertions','PASS','database',current_database());
