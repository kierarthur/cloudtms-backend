-- Repeatable CloudTMS function/view authority: weekly_contact_source_admission_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Source admission is private transaction work. The original native START
-- remains the only attempt allocator. The common public tagged entry and its
-- genuine legacy-outbox arm must be composed before dispatch activation.
create or replace function private.weekly_contact_source_membership_v1(
 p_target_id uuid,p_agency uuid,p_environment text
) returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_target public.weekly_message_dispatch_targets%rowtype;
 v_command public.weekly_message_dispatch_commands%rowtype;
 v_intent public.weekly_message_intents%rowtype;
 v_render public.weekly_message_renders%rowtype;
 v_generation public.weekly_candidate_outreach_generations%rowtype;
 v_batch public.weekly_manager_review_batches%rowtype;
 v_pairs jsonb;v_current boolean;v_push jsonb;v_audience text;
begin
 perform private.weekly_source_query_require_service_v1();
 select * into strict v_target from public.weekly_message_dispatch_targets where id=p_target_id;
 select * into strict v_command from public.weekly_message_dispatch_commands where id=v_target.dispatch_command_id;
 select * into strict v_intent from public.weekly_message_intents where id=v_command.message_intent_id;
 select * into strict v_render from public.weekly_message_renders where id=v_command.message_render_id;
 if v_command.agency_id is distinct from p_agency or v_command.environment is distinct from p_environment
  or v_intent.agency_id is distinct from p_agency or v_intent.environment is distinct from p_environment
  or v_command.source_cycle_id is distinct from v_intent.source_cycle_id
  or v_render.message_intent_id is distinct from v_intent.id
  or v_render.rendered_content_hash is distinct from v_command.rendered_content_hash
  or v_target.rendered_content_hash is distinct from v_command.rendered_content_hash
  or v_render.membership_hash is distinct from v_command.membership_hash
  or v_command.tranche_kind is distinct from v_intent.tranche_kind
  or v_command.tranche_sequence is distinct from v_intent.tranche_sequence then
  raise exception 'CONTACT_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 v_current:=v_target.state='LEASED' and v_intent.state='RENDERED' and v_render.state='CURRENT';
 if v_intent.audience_kind='CANDIDATE' and v_target.channel='PUSH' then
  v_audience:='WORKER';
  select * into strict v_generation from public.weekly_candidate_outreach_generations
   where id=v_command.candidate_generation_id;
  if v_intent.candidate_generation_id is distinct from v_generation.id
   or v_intent.candidate_cohort_id is distinct from v_generation.candidate_cohort_id
   or v_generation.source_cycle_id is distinct from v_command.source_cycle_id
   or v_generation.membership_hash is distinct from v_command.membership_hash then
   raise exception 'CONTACT_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
  v_push:=private.weekly_contact_push_dispatch_context_v1(v_target.id,v_command.id,v_target.provider,
   v_target.target_snapshot_hash,p_agency,p_environment);
  v_current:=v_current and v_push->'current'='true'::jsonb;
  if v_generation.request_kind='CHECK_HOURS' then
   -- Retain every member of the captured generation, including a subsequently
   -- answered/retired member. Never derive a smaller pause vector while still
   -- sending the original frozen body. Such a body is obsolete and must be
   -- recomposed by the original generation/render owners before another START.
   select coalesce(jsonb_agg(jsonb_build_object('candidate_id',x.candidate_id,'client_id',x.client_id)
    order by x.candidate_id,x.client_id),'[]'::jsonb) into v_pairs from(
    select distinct i.candidate_id,i.client_id from public.weekly_candidate_outreach_memberships m
     join public.weekly_discrepancy_incidents i on i.id=m.incident_id
     where m.candidate_generation_id=v_generation.id
   )x;
   v_current:=v_current and not exists(
    select 1 from public.weekly_candidate_outreach_memberships m
    join public.weekly_discrepancy_incidents i on i.id=m.incident_id
    where m.candidate_generation_id=v_generation.id and(
     i.candidate_id is distinct from v_generation.candidate_id
     or i.client_id is distinct from v_generation.client_id
     or i.source_cycle_id is distinct from v_command.source_cycle_id
     or m.state<>'ACTIONABLE' or i.state<>'OPEN'
     or i.candidate_action_state in('RESPONDED','NOT_REQUIRED')
     or m.comparison_revision_id is distinct from i.current_comparison_revision_id));
  elsif v_generation.request_kind='SUBMIT_TIMESHEET' then
   select coalesce(jsonb_agg(jsonb_build_object('candidate_id',x.candidate_id,'client_id',x.client_id)
    order by x.candidate_id,x.client_id),'[]'::jsonb) into v_pairs from(
    select distinct r.candidate_id,m.client_id from public.weekly_timesheet_submission_requests r
    join public.weekly_timesheet_submission_request_memberships m on m.submission_request_id=r.id
    where r.candidate_cohort_id=v_generation.candidate_cohort_id and r.candidate_id=v_generation.candidate_id
     and r.source_cycle_id=v_generation.source_cycle_id and r.membership_hash=v_generation.membership_hash
     and r.state in('ACTIVE','OVERDUE','PARTLY_SUBMITTED')
   )x;
   v_current:=v_current and not exists(
    select 1 from public.weekly_timesheet_submission_requests r
    join public.weekly_timesheet_submission_request_memberships m on m.submission_request_id=r.id
    where r.candidate_cohort_id=v_generation.candidate_cohort_id and r.candidate_id=v_generation.candidate_id
     and r.source_cycle_id=v_generation.source_cycle_id and r.membership_hash=v_generation.membership_hash
     and r.state in('ACTIVE','OVERDUE','PARTLY_SUBMITTED') and m.state<>'WAITING');
  else raise exception 'CONTACT_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 elsif v_intent.audience_kind='MANAGER' and v_target.channel='EMAIL' then
  v_audience:='MANAGER';
  if v_command.tranche_kind not in('MANAGER_EARLY_ALL','MANAGER_T6_RESPONDED','MANAGER_T12_REMAINDER',
   'MANAGER_MANUAL_SELECTED','MANAGER_MANUAL_RESEND') or v_target.target_kind<>'MANAGER_ADDRESS'
   or v_target.provider<>'POWER_AUTOMATE' or v_command.recipient_route_id is distinct from v_intent.recipient_route_id then
   raise exception 'CONTACT_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
  select * into strict v_batch from public.weekly_manager_review_batches where message_render_id=v_render.id;
  if v_batch.recipient_generation_id is distinct from v_intent.recipient_generation_id
   or v_batch.original_membership_hash is distinct from v_command.membership_hash
   or v_batch.sent_content_hash is distinct from v_render.rendered_content_hash then
   raise exception 'CONTACT_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
  -- Exact immutable batch items, never only the currently eligible subset.
  select coalesce(jsonb_agg(jsonb_build_object('candidate_id',x.candidate_id,'client_id',x.client_id)
   order by x.candidate_id,x.client_id),'[]'::jsonb) into v_pairs from(
   select distinct i.candidate_id,i.client_id from public.weekly_manager_review_items m
   join public.weekly_discrepancy_incidents i on i.id=m.incident_id where m.review_batch_id=v_batch.id
  )x;
  v_current:=v_current and v_batch.state='ACTIVE' and v_batch.expires_at_utc>clock_timestamp()
   and exists(select 1 from public.weekly_manager_recipient_generations g
    join public.weekly_manager_recipient_routes r on r.id=g.recipient_route_id
    where g.id=v_intent.recipient_generation_id and g.state='ACTIVE'
     and r.current_generation_id=g.id and r.id=v_intent.recipient_route_id)
   and not exists(select 1 from public.weekly_manager_review_items m
    join public.weekly_discrepancy_incidents i on i.id=m.incident_id
    join public.weekly_issue_comparison_revisions c on c.id=i.current_comparison_revision_id
    where m.review_batch_id=v_batch.id and(
     m.response_state<>'UNANSWERED' or i.state<>'OPEN' or i.manager_action_state='RESPONDED'
     or i.source_cycle_id is distinct from v_command.source_cycle_id
     or m.incident_episode is distinct from i.episode_number
     or m.sent_comparison_revision_id is distinct from i.current_comparison_revision_id
     or m.sent_comparison_fingerprint is distinct from c.material_comparison_fingerprint));
 else
  -- No unregistered future producer inherits checking or QT authority.
  raise exception 'CONTACT_DISPATCH_SCOPE_INVALID' using errcode='42501';
 end if;
 if jsonb_array_length(v_pairs)>500 then raise exception 'CONTACT_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 if v_current then
  begin perform private.weekly_source_query_current_publication_v1(v_command.source_cycle_id,v_command.projection_publication_id);
  exception when sqlstate '55000' or no_data_found then v_current:=false;end;
 end if;
 return jsonb_build_object('dispatch_target_id',v_target.id,'dispatch_command_id',v_command.id,
  'channel',v_target.channel,'audience',v_audience,'scope_pairs',v_pairs,
  'content_hash',encode(v_target.rendered_content_hash,'hex'),
  'recipient_hash',encode(v_target.keyed_target_fingerprint,'hex'),
  'current',coalesce(v_current and jsonb_array_length(v_pairs)>0,false));
end;
$function$;

create or replace function private.weekly_contact_source_admission_v1(p_request jsonb) returns jsonb
language plpgsql volatile security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_keys text[]:=array['version','owner_kind','dispatch_target_id','lease_token','worker_id','agency_id','environment'];
 v_target_id uuid;v_lease uuid;v_agency uuid;v_environment text;v_worker text;
 v_target public.weekly_message_dispatch_targets%rowtype;v_command public.weekly_message_dispatch_commands%rowtype;
 v_policy public.candidate_push_timing_policy%rowtype;v_context jsonb;v_before jsonb;v_after jsonb;
 v_pair jsonb;v_guard jsonb;v_scopes jsonb:='[]';v_scope uuid;v_until timestamptz;
 v_at timestamptz;v_witness jsonb;v_decision jsonb;v_admission jsonb;
begin
 perform private.weekly_source_query_require_service_v1();
 perform private.weekly_contact_assert_closed_v1(p_request,v_keys,v_keys,'CONTACT_REQUEST_INVALID');
 if p_request->>'version' is distinct from 'CONTACT_START_V1'
  or p_request->>'owner_kind' is distinct from 'WEEKLY_SOURCE_TARGET'
  or jsonb_typeof(p_request->'worker_id') is distinct from 'string'
  or char_length(p_request->>'worker_id') not between 1 and 200
  or p_request->>'environment' is null or p_request->>'environment' not in('TEST','LIVE') then
  raise exception 'CONTACT_REQUEST_INVALID' using errcode='22023';end if;
 v_target_id:=private.weekly_contact_uuid_v1(p_request->'dispatch_target_id');
 v_lease:=private.weekly_contact_uuid_v1(p_request->'lease_token');
 v_agency:=private.weekly_contact_uuid_v1(p_request->'agency_id');
 v_worker:=p_request->>'worker_id';v_environment:=p_request->>'environment';
 -- The preliminary reads discover native IDs only; they grant no admission.
 perform private.weekly_contact_deployment_lock_v1(v_agency,v_environment);
 perform private.weekly_contact_context_v1(v_agency,v_environment);
 select * into strict v_target from public.weekly_message_dispatch_targets where id=v_target_id;
 select * into strict v_command from public.weekly_message_dispatch_commands where id=v_target.dispatch_command_id;
 if v_command.agency_id is distinct from v_agency or v_command.environment is distinct from v_environment then
  raise exception 'CONTACT_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 if v_target.channel='PUSH' then
  select * into v_policy from public.candidate_push_timing_policy
   where agency_id=v_agency and environment=v_environment for update;
  if not found then raise exception 'PUSH_TIMING_DEPLOYMENT_UNAVAILABLE' using errcode='P0001';end if;
  perform private.candidate_push_timing_policy_validate_v1(v_policy.policy_json);
  if v_policy.policy_sha256 is distinct from private.weekly_source_sha256_jsonb_v1('PUSH_TIMING_POLICY_V1',v_policy.policy_json) then
   raise exception 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' using errcode='P0001';end if;
 end if;
 v_before:=private.weekly_contact_source_membership_v1(v_target_id,v_agency,v_environment);
 for v_pair in select value from jsonb_array_elements(v_before->'scope_pairs')
  order by value->>'candidate_id',value->>'client_id' loop
  perform private.weekly_contact_scope_lock_v1(v_agency,v_environment,
   (v_pair->>'candidate_id')::uuid,(v_pair->>'client_id')::uuid);
 end loop;
 if v_target.channel='PUSH' and (v_policy.policy_json->>'enabled')::boolean
  and v_policy.policy_json->'bank_holiday_rule'->>'mode'<>'INHERIT' then
  -- The existing effective owner derives holidays solely from this actual
  -- defaults row's bh_list UNION bh_feed_list. A stable row lock excludes its
  -- actual feed/editor writers; unrelated client/rate rows are not locked.
  perform 1 from public.settings_defaults where id=1 for share;
  if not found then raise exception 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' using errcode='P0001';end if;
 end if;
 -- Only now acquire the existing native relative target -> command locks.
 select * into strict v_target from public.weekly_message_dispatch_targets where id=v_target_id for update;
 select * into strict v_command from public.weekly_message_dispatch_commands where id=v_target.dispatch_command_id for update;
 v_after:=private.weekly_contact_source_membership_v1(v_target_id,v_agency,v_environment);
 if (v_before-'current') is distinct from (v_after-'current') then
  raise exception 'CONTACT_DISPATCH_MEMBERSHIP_CHANGED' using errcode='40001';end if;
 -- Re-read the genuine clock and current scope expiry after every possible wait.
 v_at:=clock_timestamp();
 if v_target.state<>'LEASED' or v_target.lease_token is distinct from v_lease
  or v_target.lease_owner is distinct from v_worker or v_target.lease_expires_at_utc<=v_at then
  raise exception 'WEEKLY_SOURCE_TARGET_LEASE_STALE' using errcode='40001';end if;
 if v_after->'current' is distinct from 'true'::jsonb then
  return jsonb_build_object('ok',false,'version','CONTACT_START_RESULT_V1','owner_kind','WEEKLY_SOURCE_TARGET',
   'channel',v_target.channel,'reason','OBSOLETE','dispatch_target_id',v_target.id,'next_eligible_at_utc',null);end if;
 for v_pair in select value from jsonb_array_elements(v_after->'scope_pairs')
  order by value->>'candidate_id',value->>'client_id' loop
  v_guard:=private.weekly_contact_scope_guard_v1((v_pair->>'candidate_id')::uuid,
   (v_pair->>'client_id')::uuid,v_after->>'audience',v_at);
  v_scope:=(v_guard->>'scope_id')::uuid;
  if v_scope is null or jsonb_typeof(v_guard->'allow') is distinct from 'boolean'
   or jsonb_typeof(v_guard->'revision') is distinct from 'string'
   or v_guard->>'revision' !~ '^(0|[1-9][0-9]*)$' then
   raise exception 'CONTACT_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
  v_scopes:=v_scopes||jsonb_build_array(jsonb_build_object('scope_id',v_scope,
   'candidate_id',v_pair->'candidate_id','client_id',v_pair->'client_id',
   'audience',v_after->>'audience','revision',v_guard->>'revision'));
  if v_guard->'allow' is distinct from 'true'::jsonb then
   if v_guard->>'next_allowed_utc' is null then
    raise exception 'CONTACT_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
   v_until:=greatest(v_until,(v_guard->>'next_allowed_utc')::timestamptz);end if;
 end loop;
 if v_until is not null then
  return jsonb_build_object('ok',false,'version','CONTACT_START_RESULT_V1','owner_kind','WEEKLY_SOURCE_TARGET',
   'channel',v_target.channel,'reason','NOT_STARTED_PAUSED','dispatch_target_id',v_target.id,
   'next_eligible_at_utc',to_char(v_until at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));end if;
 if not (private.weekly_contact_source_budget_v1(v_command.id)->>'allowed')::boolean then
  return jsonb_build_object('ok',false,'version','CONTACT_START_RESULT_V1','owner_kind','WEEKLY_SOURCE_TARGET',
   'channel',v_target.channel,'reason','OBSOLETE','dispatch_target_id',v_target.id,'next_eligible_at_utc',null);end if;
 v_admission:=jsonb_build_object('scope_revisions',v_scopes,'content_hash',v_after->>'content_hash',
  'admitted_at_utc',to_char(v_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
 if v_target.channel='PUSH' then
  v_context:=private.weekly_contact_push_dispatch_context_v1(v_target.id,v_command.id,v_target.provider,
   v_target.target_snapshot_hash,v_agency,v_environment);
  v_witness:=private.weekly_contact_push_calendar_witness_v1(v_context,v_policy.policy_json,v_at);
  v_at:=clock_timestamp();
  if v_target.lease_expires_at_utc<=v_at then
   raise exception 'WEEKLY_SOURCE_TARGET_LEASE_STALE' using errcode='40001';end if;
  v_decision:=private.weekly_contact_push_timing_decision_v1(v_policy.policy_json,v_at,
   (v_context->>'expires_at_utc')::timestamptz,v_witness->'dates');
  if v_decision->'allowed_now' is distinct from 'true'::jsonb then
   return jsonb_build_object('ok',false,'version','CONTACT_START_RESULT_V1','owner_kind','WEEKLY_SOURCE_TARGET',
    'channel','PUSH','reason',case v_decision->>'reason' when 'QUIET' then 'NOT_STARTED_QUIET' else v_decision->>'reason' end,
    'dispatch_target_id',v_target.id,'next_eligible_at_utc',v_decision->'next_allowed_utc');end if;
  v_admission:=v_admission||jsonb_build_object('policy_revision',v_policy.revision::text,
   'policy_sha256',encode(v_policy.policy_sha256,'hex'),'calendar_digest',v_witness->>'calendar_digest',
   'target_snapshot_hash',encode(v_target.target_snapshot_hash,'hex'),'provider',v_target.provider,
   'admitted_at_utc',to_char(v_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
   'consume_before_utc',to_char((v_at+interval '30 seconds') at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
 else
  v_admission:=v_admission||jsonb_build_object('recipient_hash',v_after->>'recipient_hash');
 end if;
 return jsonb_build_object('ok',true,'version','CONTACT_START_RESULT_V1','owner_kind','WEEKLY_SOURCE_TARGET',
  'channel',v_target.channel,'dispatch_target_id',v_target.id,'admission',v_admission);
end;
$function$;

alter function private.weekly_contact_source_membership_v1(uuid,uuid,text) owner to postgres;
alter function private.weekly_contact_source_admission_v1(jsonb) owner to postgres;
revoke all on function private.weekly_contact_source_membership_v1(uuid,uuid,text),
 private.weekly_contact_source_admission_v1(jsonb) from public,anon,authenticated,service_role;

-- Called by the original native START after it allocates its real attempt and
-- performs its existing state transitions. No caller-selected scope/body or
-- replacement attempt is accepted, and this private function is not callable
-- by service_role. Admission, native attempt and permit share one transaction.
create or replace function private.weekly_contact_source_permit_v1(
 p_target_id uuid,p_attempt_id uuid,p_admission jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_target public.weekly_message_dispatch_targets%rowtype;
 v_attempt public.weekly_message_target_attempts%rowtype;
 v_command public.weekly_message_dispatch_commands%rowtype;
 v_policy public.candidate_push_timing_policy%rowtype;
 v_scope public.weekly_contact_pause_scopes%rowtype;
 v_keys text[];v_member jsonb;v_seen uuid[]:='{}';v_scope_id uuid;
 v_at timestamptz:=clock_timestamp();v_admitted timestamptz;v_before timestamptz;v_permit uuid;
 v_guard jsonb;v_audience text;v_logical bytea;v_actual jsonb;v_supplied_pairs jsonb;
begin
 perform private.weekly_source_query_require_service_v1();
 select * into strict v_target from public.weekly_message_dispatch_targets where id=p_target_id;
 select * into strict v_attempt from public.weekly_message_target_attempts where id=p_attempt_id;
 select * into strict v_command from public.weekly_message_dispatch_commands where id=v_target.dispatch_command_id;
 if v_target.state<>'SUBMISSION_STARTED' or v_attempt.dispatch_target_id is distinct from v_target.id
  or v_attempt.attempt_number is distinct from v_target.attempt_count or v_attempt.completed_at_utc is not null
  or v_attempt.lease_owner is distinct from v_target.lease_owner
  or v_attempt.lease_token is distinct from v_target.lease_token or v_target.lease_expires_at_utc<=v_at then
  raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
 v_keys:=array['scope_revisions','content_hash','admitted_at_utc'];
 if v_target.channel='PUSH' then
  v_keys:=v_keys||array['policy_revision','policy_sha256','calendar_digest','target_snapshot_hash','provider','consume_before_utc'];
 else v_keys:=v_keys||array['recipient_hash'];end if;
 perform private.weekly_contact_assert_closed_v1(p_admission,v_keys,v_keys,'CONTACT_PERMIT_INVALID');
 if jsonb_typeof(p_admission->'scope_revisions') is distinct from 'array'
  or jsonb_array_length(p_admission->'scope_revisions') not between 1 and 500
  or p_admission->>'content_hash' is distinct from encode(v_target.rendered_content_hash,'hex')
  or jsonb_typeof(p_admission->'admitted_at_utc') is distinct from 'string' then
  raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
 begin v_admitted:=(p_admission->>'admitted_at_utc')::timestamptz;
 exception when others then raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end;
 if v_admitted is null or not isfinite(v_admitted) or v_admitted>v_at then
  raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
 select case when i.audience_kind='CANDIDATE' then 'WORKER'
  when i.audience_kind='MANAGER' then 'MANAGER' end into v_audience
 from public.weekly_message_intents i where i.id=v_command.message_intent_id;
 if v_audience is null then raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
 v_actual:=private.weekly_contact_source_membership_v1(v_target.id,v_command.agency_id,v_command.environment);
 select jsonb_agg(jsonb_build_object('candidate_id',m->'candidate_id','client_id',m->'client_id')
  order by m->>'candidate_id',m->>'client_id') into v_supplied_pairs
 from jsonb_array_elements(p_admission->'scope_revisions')m;
 if v_supplied_pairs is distinct from v_actual->'scope_pairs' then
  raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
 perform private.weekly_contact_context_v1(v_command.agency_id,v_command.environment);
 for v_member in select value from jsonb_array_elements(p_admission->'scope_revisions') loop
  perform private.weekly_contact_assert_closed_v1(v_member,
   array['scope_id','candidate_id','client_id','audience','revision'],
   array['scope_id','candidate_id','client_id','audience','revision'],'CONTACT_PERMIT_INVALID');
  v_scope_id:=private.weekly_contact_uuid_v1(v_member->'scope_id');
  perform private.weekly_contact_revision_v1(v_member->'revision',false);
  if v_scope_id=any(v_seen) then raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
  v_seen:=array_append(v_seen,v_scope_id);
  select * into strict v_scope from public.weekly_contact_pause_scopes where id=v_scope_id;
  if v_scope.agency_id is distinct from v_command.agency_id or v_scope.environment is distinct from v_command.environment
   or v_member->>'candidate_id' is distinct from v_scope.candidate_id::text
   or v_member->>'client_id' is distinct from v_scope.client_id::text
   or v_member->>'revision' is distinct from v_scope.revision::text
   or v_member->>'audience' is distinct from v_audience then
   raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
  -- These are already locked admission scopes. Do not acquire a lower-order
  -- deployment/policy/scope lock after the native target/command locks.
  v_guard:=private.weekly_contact_scope_guard_v1(v_scope.candidate_id,v_scope.client_id,v_audience,v_admitted);
  if v_guard->'allow' is distinct from 'true'::jsonb then
   raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
 end loop;
 if v_target.channel='PUSH' then
  perform private.weekly_contact_revision_v1(p_admission->'policy_revision',true);
  select * into strict v_policy from public.candidate_push_timing_policy
   where agency_id=v_command.agency_id and environment=v_command.environment;
  if p_admission->>'policy_revision' is distinct from v_policy.revision::text
   or p_admission->>'policy_sha256' is distinct from encode(v_policy.policy_sha256,'hex')
   or p_admission->>'target_snapshot_hash' is distinct from encode(v_target.target_snapshot_hash,'hex')
   or p_admission->>'provider' is distinct from v_target.provider
   or jsonb_typeof(p_admission->'calendar_digest') is distinct from 'string'
   or p_admission->>'calendar_digest' !~ '^[0-9a-f]{64}$'
   or jsonb_typeof(p_admission->'consume_before_utc') is distinct from 'string' then
   raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
  begin v_before:=(p_admission->>'consume_before_utc')::timestamptz;
  exception when others then raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end;
  if v_before is distinct from v_admitted+interval '30 seconds' or v_before<=v_at then
   raise exception 'CONTACT_PERMIT_EXPIRED' using errcode='P0001';end if;
 elsif p_admission->>'recipient_hash' is distinct from encode(v_target.keyed_target_fingerprint,'hex') then
  raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';
 end if;
 v_logical:=private.weekly_source_sha256_jsonb_v1('WEEKLY_CONTACT_SOURCE_DELIVERY_V1',
  jsonb_build_object('dispatch_target_id',v_target.id,'dispatch_command_id',v_command.id,
   'content_hash',encode(v_target.rendered_content_hash,'hex'),
   'recipient_hash',encode(v_target.keyed_target_fingerprint,'hex')));
 insert into public.weekly_contact_submission_permits(owner_kind,logical_delivery_key,environment,agency_id,
  dispatch_target_id,dispatch_command_id,provider_attempt_id,channel,provider,content_hash,recipient_hash,
  scope_revisions_json,target_snapshot_hash,safe_target_snapshot_json,policy_revision,policy_sha256,calendar_digest,
  lease_owner,lease_token,lease_expires_at_utc,status,admitted_at_utc,consume_before_utc,consumed_at_utc,
  provider_idempotency_identity)
 values('WEEKLY_SOURCE_TARGET',v_logical,v_command.environment,v_command.agency_id,
  v_target.id,v_command.id,v_attempt.id,v_target.channel,v_target.provider,v_target.rendered_content_hash,
  v_target.keyed_target_fingerprint,p_admission->'scope_revisions',
  case when v_target.channel='PUSH' then v_target.target_snapshot_hash end,
  case when v_target.channel='PUSH' then v_target.safe_target_snapshot_json end,
  case when v_target.channel='PUSH' then v_policy.revision end,
  case when v_target.channel='PUSH' then v_policy.policy_sha256 end,
  case when v_target.channel='PUSH' then decode(p_admission->>'calendar_digest','hex') end,
  v_attempt.lease_owner,v_attempt.lease_token::text,v_target.lease_expires_at_utc,'STARTED',v_admitted,v_before,
  case when v_target.channel='EMAIL' then v_admitted end,v_attempt.provider_idempotency_key)
 returning id into v_permit;
 perform private.weekly_contact_source_memberships_v1(v_permit,true);
 if v_target.channel='PUSH' then
  return jsonb_build_object('ok',true,'replay',false,'provider_attempt_id',v_attempt.id,
   'provider_idempotency_key',v_attempt.provider_idempotency_key,'permit_id',v_permit,
   'consume_before_utc',p_admission->'consume_before_utc','call_once',false);
 end if;
 return jsonb_build_object('ok',true,'replay',false,'provider_attempt_id',v_attempt.id,
  'provider_idempotency_key',v_attempt.provider_idempotency_key,'contact_permit_id',v_permit,'call_once',true);
end;
$function$;

alter function private.weekly_contact_source_permit_v1(uuid,uuid,jsonb) owner to postgres;
revoke all on function private.weekly_contact_source_permit_v1(uuid,uuid,jsonb) from public,anon,authenticated,service_role;

commit;
