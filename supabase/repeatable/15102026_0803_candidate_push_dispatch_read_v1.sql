-- Advisory saved-target timing. This grants no attempt/permit/provider call.
\set ON_ERROR_STOP on
begin;

create or replace function private.weekly_contact_push_dispatch_context_v1(
 p_target_id uuid,p_command_id uuid,p_provider text,p_snapshot_hash bytea,p_agency uuid,p_environment text
) returns jsonb
language plpgsql stable security definer set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_target public.weekly_message_dispatch_targets%rowtype;
 v_command public.weekly_message_dispatch_commands%rowtype;v_intent public.weekly_message_intents%rowtype;
 v_render public.weekly_message_renders%rowtype;v_generation public.weekly_candidate_outreach_generations%rowtype;
 v_safe jsonb;v_pairs jsonb;v_current boolean;v_notification_current boolean;
begin
 perform private.weekly_source_query_require_service_v1();
 select * into v_target from public.weekly_message_dispatch_targets where id=p_target_id;
 if not found or v_target.channel<>'PUSH' or v_target.target_kind<>'CANDIDATE_DEVICE'
  or v_target.dispatch_command_id is distinct from p_command_id or v_target.provider is distinct from p_provider
  or v_target.target_snapshot_hash is distinct from p_snapshot_hash then
  raise exception 'PUSH_TIMING_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 select * into v_command from public.weekly_message_dispatch_commands where id=v_target.dispatch_command_id;
 if not found or v_command.agency_id is distinct from p_agency or v_command.environment is distinct from p_environment
  or v_command.tranche_kind not in('CANDIDATE_INITIAL','CANDIDATE_REMINDER_6H','CANDIDATE_MANUAL_REMINDER',
    'TIMESHEET_SUBMISSION_INITIAL','TIMESHEET_SUBMISSION_REMINDER_6H') then
  raise exception 'PUSH_TIMING_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 select * into v_intent from public.weekly_message_intents where id=v_command.message_intent_id;
 if not found or v_intent.audience_kind<>'CANDIDATE' then raise exception 'PUSH_TIMING_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 select * into v_render from public.weekly_message_renders where id=v_command.message_render_id;
 if not found or v_render.message_intent_id is distinct from v_intent.id
  or v_target.rendered_content_hash is distinct from v_command.rendered_content_hash
  or v_target.rendered_content_hash is distinct from v_render.rendered_content_hash then
  raise exception 'PUSH_TIMING_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 select * into v_generation from public.weekly_candidate_outreach_generations where id=v_command.candidate_generation_id;
 if not found or v_generation.source_cycle_id is distinct from v_command.source_cycle_id then
  raise exception 'PUSH_TIMING_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 v_safe:=v_target.safe_target_snapshot_json;
 perform private.weekly_contact_assert_closed_v1(v_safe,
  array['control_plane_snapshot_id','snapshot_device_id','provider','target_revision_hash'],
  array['control_plane_snapshot_id','snapshot_device_id','provider','target_revision_hash'],'PUSH_TIMING_DISPATCH_SCOPE_INVALID');
 if v_safe->>'control_plane_snapshot_id' is distinct from v_command.control_plane_snapshot_id::text
  or v_safe->>'snapshot_device_id' is distinct from v_target.external_target_id::text
  or v_safe->>'provider' is distinct from v_target.provider
  or v_safe->>'target_revision_hash' is distinct from encode(v_target.target_snapshot_hash,'hex') then
  raise exception 'PUSH_TIMING_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 select coalesce(jsonb_agg(jsonb_build_object('candidate_id',x.candidate_id,'client_id',x.client_id)
  order by x.candidate_id,x.client_id),'[]'::jsonb) into v_pairs from(
  select distinct i.candidate_id,i.client_id from public.weekly_candidate_outreach_memberships m
   join public.weekly_discrepancy_incidents i on i.id=m.incident_id
   where m.candidate_generation_id=v_generation.id and m.state='ACTIONABLE' and i.state='OPEN'
    and i.candidate_id=v_generation.candidate_id
  union
  select distinct r.candidate_id,m.client_id from public.weekly_timesheet_submission_requests r
   join public.weekly_timesheet_submission_request_memberships m on m.submission_request_id=r.id
   where v_generation.request_kind='SUBMIT_TIMESHEET' and r.candidate_cohort_id=v_generation.candidate_cohort_id
    and r.candidate_id=v_generation.candidate_id and r.source_cycle_id=v_command.source_cycle_id
    and r.state in('ACTIVE','OVERDUE','PARTLY_SUBMITTED') and m.state='WAITING'
 )x;
 if jsonb_array_length(v_pairs)>500 then raise exception 'PUSH_TIMING_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 v_current:=v_target.state in('READY','LEASED','TRANSIENT_FAILURE') and v_intent.state='RENDERED'
  and v_render.state='CURRENT' and v_generation.state='ACTIVE' and jsonb_array_length(v_pairs)>0;
 if v_current then
  v_current:=private.weekly_source_candidate_generation_current_v1(v_generation.id);
  begin perform private.weekly_source_query_current_publication_v1(v_command.source_cycle_id,v_command.projection_publication_id);
  exception when sqlstate '55000' or no_data_found then v_current:=false;end;
 end if;
 select exists(select 1 from public.weekly_candidate_message_notifications mn
  join public.candidate_notifications n on n.id=mn.notification_id
  where mn.message_intent_id=v_intent.id and mn.candidate_generation_id=v_generation.id
   and mn.candidate_id=v_generation.candidate_id and n.state='UNREAD'
   and private.candidate_notification_visible_v1(n,statement_timestamp())) into v_notification_current;
 return jsonb_build_object('dispatch_target_id',v_target.id,'dispatch_command_id',v_command.id,
  'agency_id',v_command.agency_id,'environment',v_command.environment,'candidate_id',v_generation.candidate_id,
  'provider',v_target.provider,'target_snapshot_hash',encode(v_target.target_snapshot_hash,'hex'),
  'content_hash',encode(v_target.rendered_content_hash,'hex'),'safe_target_snapshot',v_safe,
  'scope_pairs',v_pairs,'expires_at_utc',v_generation.deadline_at_utc,
  'current',coalesce(v_current and v_notification_current,false));
end;
$function$;

create or replace function private.weekly_contact_push_calendar_witness_v1(
 p_context jsonb,p_policy jsonb,p_at_utc timestamptz
) returns jsonb
language plpgsql stable security definer set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_dates jsonb;v_date date;v_pair jsonb;v_calendar jsonb;v_map jsonb;v_digest text;v_next_digest text;
begin
 perform private.candidate_push_timing_policy_validate_v1(p_policy);
 if not(p_policy->>'enabled')::boolean or p_policy->'bank_holiday_rule'->>'mode'='INHERIT' then
  -- A cryptographic witness of NO calendar dependency, not a fabricated list
  -- or a claim that current holiday configuration was read. START proves this
  -- same policy branch. Missing calendar settings cannot block this branch.
  return jsonb_build_object('calendar_digest',encode(private.weekly_source_sha256_jsonb_v1(
   'PUSH_CALENDAR_NOT_REQUIRED_V1',jsonb_build_object('enabled',p_policy->'enabled','bank_holiday_rule',p_policy->'bank_holiday_rule')),'hex'),
   'dates',null,'dependency','NOT_REQUIRED_BY_POLICY');
 end if;
 select jsonb_agg(to_jsonb(d::text) order by d) into v_dates from(
  select (p_at_utc at time zone 'Europe/London')::date+n d from generate_series(0,27)n
 )dates where private.weekly_contact_london_boundary_v1(d,'00:00'::time,false)<(p_context->>'expires_at_utc')::timestamptz;
 if v_dates is null then
  return jsonb_build_object('calendar_digest',encode(private.weekly_source_sha256_jsonb_v1('PUSH_CALENDAR_EXPIRED_V1',
   jsonb_build_object('expires_at_utc',p_context->'expires_at_utc')),'hex'),'dates',null,'dependency','EXPIRED_BEFORE_LOOKUP');
 end if;
 for v_pair in select value from jsonb_array_elements(p_context->'scope_pairs') loop
  v_calendar:=public.weekly_source_push_calendar_decision_v1(jsonb_build_object('version','PUSH_CALENDAR_DECISION_V1',
   'candidate_id',p_context->'candidate_id','client_id',v_pair->'client_id','local_dates',v_dates,
   'agency_id',p_context->'agency_id','environment',p_context->'environment'));
  v_next_digest:=v_calendar->>'calendar_digest';
  if v_next_digest is null or v_next_digest !~ '^[0-9a-f]{64}$'
   or(v_digest is not null and v_digest<>v_next_digest) then
   raise exception 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' using errcode='P0001';end if;
  v_digest:=v_next_digest;
  select jsonb_object_agg(d->>'local_date',d->'is_holiday') into v_map from jsonb_array_elements(v_calendar->'dates')d;
 end loop;
 if v_digest is null or v_map is null then raise exception 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' using errcode='P0001';end if;
 return jsonb_build_object('calendar_digest',v_digest,'dates',v_map,'dependency','EXISTING_CONFIGURED_CALENDAR');
end;
$function$;

create or replace function public.candidate_push_timing_for_dispatch_v1(p_request jsonb) returns jsonb
language plpgsql stable security definer set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_keys text[]:=array['version','dispatch_target_id','dispatch_command_id','target_snapshot_hash','provider','agency_id','environment'];
 v_target uuid;v_command uuid;v_agency uuid;v_context jsonb;v_witness jsonb;v_decision jsonb;
 v_policy public.candidate_push_timing_policy%rowtype;v_at timestamptz:=statement_timestamp();
begin
 perform private.weekly_source_query_require_service_v1();
 perform private.weekly_contact_assert_closed_v1(p_request,v_keys,v_keys,'PUSH_TIMING_REQUEST_INVALID');
 if p_request->>'version' is distinct from 'PUSH_TIMING_DISPATCH_READ_V1'
  or jsonb_typeof(p_request->'target_snapshot_hash') is distinct from 'string'
  or p_request->>'target_snapshot_hash' !~ '^[0-9a-f]{64}$'
  or p_request->>'provider' is null or p_request->>'provider' not in('APNS','FCM')
  or p_request->>'environment' is null or p_request->>'environment' not in('TEST','LIVE') then
  raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';end if;
 begin v_target:=private.weekly_contact_uuid_v1(p_request->'dispatch_target_id');
  v_command:=private.weekly_contact_uuid_v1(p_request->'dispatch_command_id');v_agency:=private.weekly_contact_uuid_v1(p_request->'agency_id');
 exception when sqlstate '22023' then raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';end;
 v_context:=private.weekly_contact_push_dispatch_context_v1(v_target,v_command,p_request->>'provider',
  decode(p_request->>'target_snapshot_hash','hex'),v_agency,p_request->>'environment');
 select * into v_policy from public.candidate_push_timing_policy where agency_id=v_agency and environment=p_request->>'environment';
 if not found then raise exception 'PUSH_TIMING_DEPLOYMENT_UNAVAILABLE' using errcode='P0001';end if;
 perform private.candidate_push_timing_policy_validate_v1(v_policy.policy_json);
 if v_policy.policy_sha256 is distinct from private.weekly_source_sha256_jsonb_v1('PUSH_TIMING_POLICY_V1',v_policy.policy_json) then
  raise exception 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' using errcode='P0001';end if;
 if v_context->'current' is distinct from 'true'::jsonb then
  v_witness:=jsonb_build_object('calendar_digest',encode(private.weekly_source_sha256_jsonb_v1('PUSH_CALENDAR_OBSOLETE_V1',
   jsonb_build_object('dispatch_target_id',v_target)),'hex'));
  v_decision:=jsonb_build_object('allowed_now',false,'reason','OBSOLETE','next_allowed_utc',null,
   'expires_at_utc',to_char((v_context->>'expires_at_utc')::timestamptz at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
 else
  v_witness:=private.weekly_contact_push_calendar_witness_v1(v_context,v_policy.policy_json,v_at);
  v_decision:=private.weekly_contact_push_timing_decision_v1(v_policy.policy_json,v_at,
   (v_context->>'expires_at_utc')::timestamptz,v_witness->'dates');
 end if;
 return jsonb_build_object('ok',true,'version','PUSH_TIMING_DISPATCH_STATE_V1','dispatch_target_id',v_target,
  'policy_revision',v_policy.revision::text,'policy_sha256',encode(v_policy.policy_sha256,'hex'),
  'calendar_digest',v_witness->>'calendar_digest')||v_decision;
end;
$function$;

alter function private.weekly_contact_push_dispatch_context_v1(uuid,uuid,text,bytea,uuid,text) owner to postgres;
alter function private.weekly_contact_push_calendar_witness_v1(jsonb,jsonb,timestamptz) owner to postgres;
alter function public.candidate_push_timing_for_dispatch_v1(jsonb) owner to postgres;
revoke all on function private.weekly_contact_push_dispatch_context_v1(uuid,uuid,text,bytea,uuid,text),
 private.weekly_contact_push_calendar_witness_v1(jsonb,jsonb,timestamptz),public.candidate_push_timing_for_dispatch_v1(jsonb)
 from public,anon,authenticated,service_role;
grant execute on function public.candidate_push_timing_for_dispatch_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
