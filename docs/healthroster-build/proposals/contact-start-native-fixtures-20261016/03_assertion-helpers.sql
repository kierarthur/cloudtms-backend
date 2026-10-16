CREATE OR REPLACE FUNCTION pg_temp.contact_assert_no_attempt_v1(p_kind text)
RETURNS void LANGUAGE plpgsql AS $function$
DECLARE v_id uuid;
BEGIN
 SELECT dispatch_target_id INTO STRICT v_id FROM pg_temp.contact_start_fixture_targets WHERE kind=p_kind;
 PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.weekly_message_target_attempts WHERE dispatch_target_id=v_id)
  AND NOT EXISTS(SELECT 1 FROM public.weekly_contact_submission_permits WHERE dispatch_target_id=v_id)
  AND NOT EXISTS(SELECT 1 FROM public.weekly_message_delivery_failures WHERE dispatch_target_id=v_id)
  AND EXISTS(SELECT 1 FROM public.weekly_message_dispatch_targets WHERE id=v_id AND attempt_count=0),
  'no-start must allocate no native attempt/permit/failure or retry count');
END;
$function$;
CREATE OR REPLACE FUNCTION pg_temp.contact_assert_started_v1(p_kind text,p_reply jsonb)
RETURNS void LANGUAGE plpgsql AS $function$
DECLARE v_target public.weekly_message_dispatch_targets%rowtype;v_command public.weekly_message_dispatch_commands%rowtype;
 v_attempt public.weekly_message_target_attempts%rowtype;v_permit public.weekly_contact_submission_permits%rowtype;
 v_members jsonb;v_member jsonb;v_keys text[];
BEGIN
 SELECT t.* INTO STRICT v_target FROM public.weekly_message_dispatch_targets t
 JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_target_id=t.id WHERE f.kind=p_kind;
 SELECT * INTO STRICT v_command FROM public.weekly_message_dispatch_commands WHERE id=v_target.dispatch_command_id;
 SELECT * INTO STRICT v_attempt FROM public.weekly_message_target_attempts WHERE id=(p_reply->>'provider_attempt_id')::uuid;
 SELECT * INTO STRICT v_permit FROM public.weekly_contact_submission_permits WHERE provider_attempt_id=v_attempt.id;
 v_keys:=ARRAY['ok','replay','provider_attempt_id','provider_idempotency_key','call_once']
  ||CASE WHEN p_kind='PUSH' THEN ARRAY['permit_id','consume_before_utc'] ELSE ARRAY['contact_permit_id'] END;
 PERFORM private.weekly_contact_assert_closed_v1(p_reply,v_keys,v_keys,'CONTACT_FIXTURE_START_REPLY_NOT_CLOSED');
 PERFORM pg_temp.contact_assert_v1(p_reply->'ok'='true'::jsonb AND p_reply->'replay'='false'::jsonb
  AND p_reply->'call_once'=to_jsonb(p_kind='EMAIL') AND v_target.channel=p_kind
  AND v_target.state='SUBMISSION_STARTED' AND v_target.attempt_count=1
  AND v_command.state='SUBMISSION_STARTED'
  AND EXISTS(SELECT 1 FROM public.weekly_message_renders WHERE id=v_command.message_render_id AND state='SUBMISSION_STARTED')
  AND v_attempt.dispatch_target_id=v_target.id AND v_attempt.attempt_number=1 AND v_attempt.completed_at_utc IS NULL
  AND v_attempt.lease_owner=v_target.lease_owner AND v_attempt.lease_token=v_target.lease_token
  AND v_attempt.provider_idempotency_key=p_reply->>'provider_idempotency_key'
  AND v_permit.owner_kind='WEEKLY_SOURCE_TARGET' AND v_permit.status='STARTED'
  AND v_permit.dispatch_target_id=v_target.id AND v_permit.dispatch_command_id=v_command.id
  AND v_permit.content_hash=v_target.rendered_content_hash AND v_permit.recipient_hash=v_target.keyed_target_fingerprint
  AND v_permit.lease_owner=v_attempt.lease_owner AND v_permit.lease_token=v_attempt.lease_token::text
  AND v_permit.admitted_at_utc=v_attempt.submission_started_at_utc
  AND v_permit.accepted_at_utc IS NULL AND v_permit.accepted_result_json IS NULL,
  'actual native attempt and permit must be bound atomically without provider result');
 v_members:=private.weekly_contact_source_membership_v1(v_target.id,v_command.agency_id,v_command.environment);
 PERFORM pg_temp.contact_assert_v1((SELECT jsonb_agg(jsonb_build_object('candidate_id',m->'candidate_id','client_id',m->'client_id')
  ORDER BY m->>'candidate_id',m->>'client_id') FROM jsonb_array_elements(v_permit.scope_revisions_json)m)=v_members->'scope_pairs',
  'permit must retain full original frozen membership');
 FOR v_member IN SELECT value FROM jsonb_array_elements(v_permit.scope_revisions_json) LOOP
  PERFORM pg_temp.contact_assert_v1(jsonb_typeof(v_member->'revision')='string'
   AND EXISTS(SELECT 1 FROM public.weekly_contact_pause_scopes s WHERE s.id=(v_member->>'scope_id')::uuid
    AND s.revision::text=v_member->>'revision' AND s.candidate_id::text=v_member->>'candidate_id'
    AND s.client_id::text=v_member->>'client_id' AND s.agency_id=v_command.agency_id AND s.environment=v_command.environment),
   'each scope revision and native candidate/client must match');
 END LOOP;
 IF p_kind='PUSH' THEN
  PERFORM pg_temp.contact_assert_v1(v_permit.id::text=p_reply->>'permit_id' AND v_permit.consumed_at_utc IS NULL
   AND v_permit.consume_before_utc=v_permit.admitted_at_utc+interval '30 seconds'
   AND v_permit.target_snapshot_hash=v_target.target_snapshot_hash
   AND v_permit.safe_target_snapshot_json=v_target.safe_target_snapshot_json
   AND EXISTS(SELECT 1 FROM public.candidate_push_timing_policy q WHERE q.agency_id=v_command.agency_id
    AND q.environment=v_command.environment AND q.revision=v_permit.policy_revision AND q.policy_sha256=v_permit.policy_sha256),
   'PUSH native START is unconsumed and seals real policy/captured target');
 ELSE
  PERFORM pg_temp.contact_assert_v1(v_permit.id::text=p_reply->>'contact_permit_id'
   AND v_permit.consumed_at_utc=v_permit.admitted_at_utc AND v_permit.consume_before_utc IS NULL
   AND v_permit.policy_revision IS NULL AND v_permit.policy_sha256 IS NULL AND v_permit.calendar_digest IS NULL
   AND v_permit.target_snapshot_hash IS NULL,'EMAIL must be consumed solely under actual pause authority');
 END IF;
END;
$function$;
CREATE OR REPLACE FUNCTION pg_temp.contact_pause_first_v1(p_kind text,p_audience text)
RETURNS jsonb LANGUAGE plpgsql AS $function$
DECLARE v_f record;v_pair jsonb;v_member jsonb;v_revision text;v_actor uuid;
BEGIN
 SELECT * INTO STRICT v_f FROM pg_temp.contact_start_fixture_targets WHERE kind=p_kind;
 SELECT actor_user_id INTO STRICT v_actor FROM pg_temp.contact_start_fixture_input WHERE kind=p_kind;
 v_member:=private.weekly_contact_source_membership_v1(v_f.dispatch_target_id,v_f.agency_id,v_f.environment);
 v_pair:=v_member->'scope_pairs'->0;
 SELECT revision::text INTO v_revision FROM public.weekly_contact_pause_scopes WHERE environment=v_f.environment
  AND agency_id=v_f.agency_id AND candidate_id=(v_pair->>'candidate_id')::uuid AND client_id=(v_pair->>'client_id')::uuid;
 RETURN public.weekly_source_contact_pause_atomic_v1(jsonb_build_object('actor_user_id',v_actor,
  'candidate_id',v_pair->'candidate_id','client_id',v_pair->'client_id','agency_id',v_f.agency_id,'environment',v_f.environment,
  'request_id',gen_random_uuid(),'expected_revision',coalesce(v_revision,'0'),'audience',p_audience,
  'note','Rollback-only contact START fixture'));
END;
$function$;
CREATE OR REPLACE FUNCTION pg_temp.contact_policy_v1(p_mode text)
RETURNS jsonb LANGUAGE plpgsql AS $function$
DECLARE v_policy jsonb;v_days jsonb;v_day text;v_allow text;v_now timestamp:=clock_timestamp() AT TIME ZONE 'Europe/London';
BEGIN
 v_days:=jsonb_build_object('monday',jsonb_build_object('mode','INHERIT'),'tuesday',jsonb_build_object('mode','INHERIT'),
  'wednesday',jsonb_build_object('mode','INHERIT'),'thursday',jsonb_build_object('mode','INHERIT'),
  'friday',jsonb_build_object('mode','INHERIT'),'saturday',jsonb_build_object('mode','INHERIT'),'sunday',jsonb_build_object('mode','INHERIT'));
 v_policy:=jsonb_build_object('version','PUSH_TIMING_POLICY_V1','enabled',p_mode<>'ALLOWED','timezone','Europe/London',
  'normal_quiet_start',to_char(v_now-interval '6 hours','HH24:MI'),'normal_quiet_end',to_char(v_now+interval '6 hours','HH24:MI'),
  'day_rules',v_days,'bank_holiday_rule',jsonb_build_object('mode','INHERIT'));
 IF p_mode='EXPIRED' THEN
  -- The real active question expires in <24h. First permissible weekday is
  -- four local dates away; no artificial timestamp or clock override is used.
  v_allow:=(ARRAY['monday','tuesday','wednesday','thursday','friday','saturday','sunday'])[extract(isodow FROM v_now+interval '4 days')::integer];
  FOR v_day IN SELECT jsonb_object_keys(v_days) LOOP
   v_days:=jsonb_set(v_days,ARRAY[v_day],jsonb_build_object('mode',CASE WHEN v_day=v_allow THEN 'INHERIT' ELSE 'NO_PUSH_DAY' END));
  END LOOP;
  v_policy:=jsonb_set(v_policy,'{day_rules}',v_days);
 ELSIF p_mode NOT IN('QUIET','ALLOWED') THEN RAISE EXCEPTION 'CONTACT_FIXTURE_POLICY_MODE_INVALID'; END IF;
 PERFORM private.candidate_push_timing_policy_validate_v1(v_policy); RETURN v_policy;
END;
$function$;
CREATE OR REPLACE FUNCTION pg_temp.contact_save_policy_v1(p_mode text)
RETURNS void LANGUAGE plpgsql AS $function$
DECLARE v_f record;v_actor uuid;v_revision text;
BEGIN
 SELECT * INTO STRICT v_f FROM pg_temp.contact_start_fixture_targets WHERE kind='PUSH';
 SELECT actor_user_id INTO STRICT v_actor FROM pg_temp.contact_start_fixture_input WHERE kind='PUSH';
 SELECT revision::text INTO STRICT v_revision FROM public.candidate_push_timing_policy WHERE environment=v_f.environment AND agency_id=v_f.agency_id;
 PERFORM public.candidate_push_timing_save_v1(jsonb_build_object('version','PUSH_TIMING_SAVE_V1','request_id',gen_random_uuid(),
  'actor_user_id',v_actor,'agency_id',v_f.agency_id,'environment',v_f.environment,
  'expected_revision',v_revision,'policy',pg_temp.contact_policy_v1(p_mode)));
END;
$function$;

-- Actual dynamic ID inventory, emitted before the outer rollback. Root's prefix
-- must add all its own native seed/build IDs to this same TEMP table.
CREATE OR REPLACE FUNCTION pg_temp.contact_case_ids_v1() RETURNS jsonb LANGUAGE sql AS $function$
 SELECT coalesce(jsonb_agg(jsonb_build_object('relation_name',x.relation_name,'column_name','id','object_id',x.id)), '[]') FROM(
  SELECT 'public.weekly_message_target_attempts' relation_name,a.id FROM public.weekly_message_target_attempts a
   JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_target_id=a.dispatch_target_id
  UNION ALL SELECT 'public.weekly_message_delivery_failures',a.id FROM public.weekly_message_delivery_failures a
   JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_target_id=a.dispatch_target_id
  UNION ALL SELECT 'public.weekly_contact_submission_permits',a.id FROM public.weekly_contact_submission_permits a
   WHERE a.agency_id='f7160000-0000-4000-8000-000000000019'
  UNION ALL SELECT 'public.weekly_contact_pause_scopes',a.id FROM public.weekly_contact_pause_scopes a
   WHERE a.agency_id='f7160000-0000-4000-8000-000000000019' AND a.candidate_id::text LIKE 'f716c000-%'
  UNION ALL SELECT 'public.weekly_contact_pause_events',a.id FROM public.weekly_contact_pause_events a
   JOIN public.weekly_contact_pause_scopes s ON s.id=a.scope_id WHERE s.candidate_id::text LIKE 'f716c000-%'
  UNION ALL SELECT 'public.weekly_contact_recheck_tasks',a.id FROM public.weekly_contact_recheck_tasks a
   JOIN public.weekly_contact_pause_scopes s ON s.id=a.scope_id WHERE s.candidate_id::text LIKE 'f716c000-%'
 )x;
$function$;
