-- Each success is deliberately rolled back at an inner subtransaction boundary.
-- Only its case-name/PASS row survives until the server-owned outer rollback.
-- No fake provider result is supplied; START/CONSUME rows are real native rows.
CREATE OR REPLACE FUNCTION pg_temp.contact_run_case_v1(p_case text)
RETURNS void LANGUAGE plpgsql AS $function$
DECLARE v_request jsonb;v_reply jsonb;v_replay jsonb;v_start jsonb;v_email jsonb;v_consume jsonb;v_bad jsonb;
 v_pause jsonb;v_before jsonb;v_after jsonb;v_clocks jsonb;v_target uuid;v_command uuid;v_kind text;v_id uuid;
 v_absence jsonb;v_object jsonb;v_key text;v_role text;v_reason text;v_wait numeric;v_at timestamptz;v_actor uuid;
BEGIN
 PERFORM set_config('request.jwt.claim.role','service_role',true);
 v_kind:=CASE WHEN p_case LIKE 'email_%' THEN 'EMAIL' ELSE 'PUSH' END;
 v_request:=pg_temp.contact_start_request_v1(v_kind);
 PERFORM pg_temp.contact_assert_v1(v_request IS NOT NULL,'native target prefix required');
 v_target:=(v_request->>'dispatch_target_id')::uuid;
 SELECT dispatch_command_id INTO STRICT v_command FROM pg_temp.contact_start_fixture_targets WHERE kind=v_kind;
 -- Question clocks remain the original native activation times, never shifted by START/QT/pause.
 SELECT jsonb_agg(to_jsonb(g) ORDER BY g.id) INTO v_clocks FROM public.weekly_candidate_outreach_generations g
 JOIN public.weekly_source_cycles c ON c.id=g.source_cycle_id
 WHERE c.source_group_id='f716c000-0000-4000-8000-000000000001';
 SELECT jsonb_build_object('targets',(SELECT jsonb_agg(to_jsonb(t) ORDER BY t.id) FROM public.weekly_message_dispatch_targets t
   JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_target_id=t.id),
  'permits',(SELECT jsonb_agg(to_jsonb(p) ORDER BY p.id) FROM public.weekly_contact_submission_permits p),
  'scopes',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) FROM public.weekly_contact_pause_scopes s)) INTO v_before;
 BEGIN
  CASE p_case
   WHEN 'email_missing_qt' THEN
    PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.candidate_push_timing_policy),'run missing-QT case before fixture bootstrap');
    v_reply:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    PERFORM pg_temp.contact_assert_started_v1('EMAIL',v_reply);
   WHEN 'push_missing_qt' THEN
    PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.candidate_push_timing_policy),'run missing-QT case before fixture bootstrap');
    BEGIN
     PERFORM public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
     RAISE EXCEPTION 'CONTACT_NATIVE_ASSERT: missing QT admitted PUSH';
    EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'PUSH_TIMING_DEPLOYMENT_UNAVAILABLE' THEN RAISE; END IF; END;
    PERFORM pg_temp.contact_assert_no_attempt_v1('PUSH');
   WHEN 'push_native_start_and_replay','email_native_start_and_replay' THEN
    v_start:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    PERFORM pg_temp.contact_assert_started_v1(v_kind,v_start);
    v_reply:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    PERFORM private.weekly_contact_assert_closed_v1(v_reply,
     ARRAY['ok','replay','provider_attempt_id','call_once','reason'],
     ARRAY['ok','replay','provider_attempt_id','call_once','reason'],'CONTACT_FIXTURE_REPLAY_NOT_CLOSED');
    PERFORM pg_temp.contact_assert_v1(v_reply=jsonb_build_object('ok',true,'replay',true,
     'provider_attempt_id',v_start->'provider_attempt_id','call_once',false,'reason','CHECK_ORIGINAL_ATTEMPT'),
     'exact replay must point to original attempt without permission');
    v_replay:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request||jsonb_build_object(
     'lease_token',gen_random_uuid(),'worker_id','contact-native-other-worker'));
    PERFORM pg_temp.contact_assert_v1(v_replay=v_reply,'unknown original owns across different lease/worker');
    PERFORM pg_temp.contact_assert_v1((SELECT count(*)=1 FROM public.weekly_message_target_attempts WHERE dispatch_target_id=v_target)
     AND(SELECT count(*)=1 FROM public.weekly_contact_submission_permits WHERE dispatch_target_id=v_target),'replay allocated another native row');
   WHEN 'push_once_consume','push_admitted_before_pause' THEN
    v_start:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    PERFORM pg_temp.contact_assert_started_v1('PUSH',v_start);
    IF p_case='push_admitted_before_pause' THEN
     v_pause:=pg_temp.contact_pause_first_v1('PUSH','WORKER');
     PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM jsonb_array_elements(v_pause#>'{scope,in_flight}')i
      WHERE i->>'provider_attempt_id'=v_start->>'provider_attempt_id' AND i->>'permit_id'=v_start->>'permit_id'
       AND i->>'status'='STARTED'),'pause must honestly report admitted original');
    END IF;
    v_consume:=pg_temp.contact_consume_request_v1(v_start);
    v_reply:=public.weekly_contact_submission_consume_v1(v_consume);
    PERFORM private.weekly_contact_assert_closed_v1(v_reply,
     ARRAY['ok','version','permit_id','provider_attempt_id','call_once','consumed_at_utc'],
     ARRAY['ok','version','permit_id','provider_attempt_id','call_once','consumed_at_utc'],'CONTACT_FIXTURE_CONSUME_NOT_CLOSED');
    PERFORM pg_temp.contact_assert_v1(v_reply->'ok'='true'::jsonb AND v_reply->'call_once'='true'::jsonb
     AND v_reply->>'version'='CONTACT_CONSUMED_V1' AND v_reply->'permit_id'=v_start->'permit_id'
     AND v_reply->'provider_attempt_id'=v_start->'provider_attempt_id','sole real consume must admit once');
    SELECT consumed_at_utc INTO STRICT v_at FROM public.weekly_contact_submission_permits WHERE id=(v_start->>'permit_id')::uuid;
    v_replay:=public.weekly_contact_submission_consume_v1(v_consume);
    PERFORM pg_temp.contact_assert_v1(v_replay=jsonb_build_object('ok',false,'reason','ALREADY_CONSUMED',
     'provider_attempt_id',v_start->'provider_attempt_id','call_once',false)
     AND(SELECT consumed_at_utc=v_at FROM public.weekly_contact_submission_permits WHERE id=(v_start->>'permit_id')::uuid),
     'consume replay must not renew time or authority');
   WHEN 'push_wrong_lease','push_expired_lease' THEN
    IF p_case='push_wrong_lease' THEN v_request:=v_request||jsonb_build_object('lease_token',gen_random_uuid());
    ELSE
     -- Synthetic negative lease fact on our actual native fixture target only.
     UPDATE public.weekly_message_dispatch_targets SET lease_expires_at_utc=clock_timestamp()-interval '1 second' WHERE id=v_target;
    END IF;
    BEGIN
     PERFORM public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
     RAISE EXCEPTION 'CONTACT_NATIVE_ASSERT: wrong/expired native lease admitted';
    EXCEPTION WHEN serialization_failure THEN IF SQLERRM<>'WEEKLY_SOURCE_TARGET_LEASE_STALE' THEN RAISE; END IF; END;
    PERFORM pg_temp.contact_assert_no_attempt_v1('PUSH');
   WHEN 'push_worker_pause','email_mixed_pause' THEN
    v_pause:=pg_temp.contact_pause_first_v1(v_kind,CASE WHEN v_kind='PUSH' THEN 'WORKER' ELSE 'MANAGER' END);
    v_reply:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    PERFORM pg_temp.contact_assert_v1(v_reply->'ok'='false'::jsonb AND v_reply->>'reason'='NOT_STARTED_PAUSED'
     AND v_reply->>'channel'=v_kind AND v_reply->>'next_eligible_at_utc' IS NOT NULL,'paused frozen membership must hold whole native target');
    PERFORM pg_temp.contact_assert_no_attempt_v1(v_kind);
    IF v_kind='EMAIL' THEN
     PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.weekly_message_dispatch_targets WHERE id=v_target AND state='RETIRED')
      AND EXISTS(SELECT 1 FROM public.weekly_manager_review_batches b JOIN public.weekly_message_dispatch_commands c ON c.message_render_id=b.message_render_id
       WHERE c.id=v_command AND b.state='REVOKED')
      AND NOT EXISTS(SELECT 1 FROM public.weekly_manager_review_items i JOIN public.weekly_manager_review_batches b ON b.id=i.review_batch_id
       JOIN public.weekly_message_dispatch_commands c ON c.message_render_id=b.message_render_id WHERE c.id=v_command AND i.response_state='UNANSWERED'),
      'mixed paused manager body/link must retire intact with no partial submission');
    ELSE
     PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.weekly_message_dispatch_targets WHERE id=v_target
      AND state='TRANSIENT_FAILURE' AND next_attempt_at_utc>clock_timestamp()
      AND lease_token IS NULL AND lease_owner IS NULL AND lease_expires_at_utc IS NULL),'worker pause must release native lease without attempt');
    END IF;
   WHEN 'push_manager_independent','email_worker_independent' THEN
    PERFORM pg_temp.contact_pause_first_v1(v_kind,CASE WHEN v_kind='PUSH' THEN 'MANAGER' ELSE 'WORKER' END);
    v_reply:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    PERFORM pg_temp.contact_assert_started_v1(v_kind,v_reply);
   WHEN 'push_quiet','email_quiet','push_expired_schedule' THEN
    PERFORM pg_temp.contact_save_policy_v1(CASE WHEN p_case='push_expired_schedule' THEN 'EXPIRED' ELSE 'QUIET' END);
    v_reply:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    IF v_kind='EMAIL' THEN PERFORM pg_temp.contact_assert_started_v1('EMAIL',v_reply);
    ELSE
     v_reason:=CASE WHEN p_case='push_expired_schedule' THEN 'EXPIRED_TIMING' ELSE 'NOT_STARTED_QUIET' END;
     PERFORM pg_temp.contact_assert_v1(v_reply->'ok'='false'::jsonb AND v_reply->>'reason'=v_reason,'native QT no-start disposition mismatch');
     PERFORM pg_temp.contact_assert_no_attempt_v1('PUSH');
     IF p_case='push_quiet' THEN
      PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.weekly_message_dispatch_targets WHERE id=v_target
       AND state='TRANSIENT_FAILURE' AND next_attempt_at_utc>clock_timestamp() AND lease_token IS NULL),'quiet must defer/release lease');
     ELSE
      PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.weekly_message_dispatch_targets WHERE id=v_target
       AND state='RETIRED' AND terminal_reason='EXPIRED_TIMING'),'no permissible date must retire only stale alert');
     END IF;
    END IF;
   WHEN 'email_broken_qt' THEN
    -- Real, rollback-contained schema dependency fault, not a replacement helper
    -- returning a fake failure. EMAIL must remain independent even on first use.
    ALTER TABLE public.candidate_push_timing_policy RENAME COLUMN policy_json TO contact_fixture_unavailable_policy_json;
    BEGIN
     EXECUTE 'SELECT policy_json FROM public.candidate_push_timing_policy';
     RAISE EXCEPTION 'CONTACT_NATIVE_ASSERT: broken QT fault was not established';
    EXCEPTION WHEN undefined_column THEN NULL; END;
    v_reply:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    PERFORM pg_temp.contact_assert_started_v1('EMAIL',v_reply);
   WHEN 'email_stale_member','push_stale_publication','email_stale_publication' THEN
    IF p_case='email_stale_member' THEN
     -- Negative fact on one original captured member, keeping the body/membership
     -- hash untouched. Native START must not shrink the original scope vector.
     UPDATE public.weekly_manager_review_items SET response_state='OBSOLETE' WHERE id=(
      SELECT i.id FROM public.weekly_manager_review_items i JOIN public.weekly_manager_review_batches b ON b.id=i.review_batch_id
      JOIN public.weekly_message_dispatch_commands c ON c.message_render_id=b.message_render_id
      WHERE c.id=v_command ORDER BY i.id LIMIT 1);
    ELSE
     UPDATE public.weekly_source_projection_publications SET state='STALE' WHERE id=(
      SELECT projection_publication_id FROM public.weekly_message_dispatch_commands WHERE id=v_command);
    END IF;
    v_reply:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    PERFORM pg_temp.contact_assert_v1(v_reply->'ok'='false'::jsonb AND v_reply->>'reason'='OBSOLETE','stale original native facts must reject');
    PERFORM pg_temp.contact_assert_no_attempt_v1(v_kind);
   WHEN 'push_unknown_crosslease','push_consumed_lost_unknown' THEN
    v_start:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    PERFORM pg_temp.contact_assert_started_v1('PUSH',v_start);
    IF p_case='push_consumed_lost_unknown' THEN
     PERFORM public.weekly_contact_submission_consume_v1(pg_temp.contact_consume_request_v1(v_start));
    END IF;
    -- Model lost START/consume response by not calling a provider or result.
    -- Expire only our native lease; the real native claim owner classifies it.
    UPDATE public.weekly_message_dispatch_targets SET lease_expires_at_utc=transaction_timestamp()-interval '1 second' WHERE id=v_target;
    v_reply:=public.weekly_source_message_dispatch_target_claim_v1(jsonb_build_object('worker_id','contact-native-reclaimer','limit',1,'lease_seconds',300));
    PERFORM pg_temp.contact_assert_v1(v_reply->>'claimed_count'='0'
     AND EXISTS(SELECT 1 FROM public.weekly_message_dispatch_targets WHERE id=v_target AND state='AMBIGUOUS'),
     'original native lease expiry must retain unknown ownership rather than grant retry');
    v_replay:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request||jsonb_build_object('lease_token',gen_random_uuid(),'worker_id','contact-native-reclaimer'));
    PERFORM pg_temp.contact_assert_v1(v_replay->>'provider_attempt_id'=v_start->>'provider_attempt_id'
     AND v_replay->'call_once'='false'::jsonb AND v_replay->>'reason'='CHECK_ORIGINAL_ATTEMPT'
     AND(SELECT count(*)=1 FROM public.weekly_message_target_attempts WHERE dispatch_target_id=v_target)
     AND EXISTS(SELECT 1 FROM public.weekly_message_target_attempts WHERE id=(v_start->>'provider_attempt_id')::uuid AND completed_at_utc IS NULL),
     'unknown original must survive cross-lease START without fabricated provider result');
   WHEN 'push_foreign_capture' THEN
    v_start:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    v_email:=public.weekly_source_message_dispatch_target_start_atomic_v1(pg_temp.contact_start_request_v1('EMAIL'));
    v_consume:=pg_temp.contact_consume_request_v1(v_start);
    FOR v_bad IN SELECT value FROM jsonb_array_elements(jsonb_build_array(
     v_consume||jsonb_build_object('permit_id',v_email->'contact_permit_id'),
     v_consume||jsonb_build_object('provider_attempt_id',v_email->'provider_attempt_id'),
     v_consume||jsonb_build_object('dispatch_target_id',pg_temp.contact_start_request_v1('EMAIL')->'dispatch_target_id'),
     v_consume||jsonb_build_object('dispatch_command_id',(SELECT dispatch_command_id FROM pg_temp.contact_start_fixture_targets WHERE kind='EMAIL')),
     v_consume||jsonb_build_object('target_snapshot_hash',repeat('00',32)),
     v_consume||jsonb_build_object('content_hash',repeat('00',32)),
     v_consume||jsonb_build_object('provider','APNS'))) LOOP
     BEGIN
      PERFORM public.weekly_contact_submission_consume_v1(v_bad);
      RAISE EXCEPTION 'CONTACT_NATIVE_ASSERT: foreign/captured tuple admitted';
     EXCEPTION WHEN insufficient_privilege THEN IF SQLERRM<>'PUSH_TIMING_DISPATCH_SCOPE_INVALID' THEN RAISE; END IF; END;
    END LOOP;
    BEGIN
     PERFORM public.weekly_source_message_dispatch_target_start_atomic_v1(v_request||jsonb_build_object('agency_id','f716c000-0000-4000-8000-000000000099'));
     RAISE EXCEPTION 'CONTACT_NATIVE_ASSERT: foreign native deployment replay admitted';
    EXCEPTION WHEN insufficient_privilege THEN IF SQLERRM<>'CONTACT_DISPATCH_SCOPE_INVALID' THEN RAISE; END IF; END;
    PERFORM pg_temp.contact_assert_v1((SELECT consumed_at_utc IS NULL FROM public.weekly_contact_submission_permits
     WHERE id=(v_start->>'permit_id')::uuid),'refused consume changed original permit');
   WHEN 'push_current_role_replay' THEN
    v_start:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    FOREACH v_role IN ARRAY ARRAY['anon','authenticated',''] LOOP
     PERFORM set_config('request.jwt.claim.role',v_role,true);
     BEGIN
      PERFORM public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
      RAISE EXCEPTION 'CONTACT_NATIVE_ASSERT: current service-role loss allowed original replay';
     EXCEPTION WHEN insufficient_privilege THEN IF SQLERRM<>'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' THEN RAISE; END IF; END;
    END LOOP;
    PERFORM set_config('request.jwt.claim.role','service_role',true);
    v_reply:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    PERFORM pg_temp.contact_assert_v1(v_reply->'call_once'='false'::jsonb AND v_reply->>'provider_attempt_id'=v_start->>'provider_attempt_id',
     'restored service may only check original');
   WHEN 'push_closed_native_request' THEN
    FOREACH v_key IN ARRAY ARRAY['channel','admission','scope_pairs','provider_attempt_id'] LOOP
     BEGIN
      PERFORM public.weekly_source_message_dispatch_target_start_atomic_v1(v_request||jsonb_build_object(v_key,'caller supplied'));
      RAISE EXCEPTION 'CONTACT_NATIVE_ASSERT: caller-selected scope/channel/authority admitted';
     EXCEPTION WHEN invalid_parameter_value THEN IF SQLERRM<>'WEEKLY_SOURCE_TARGET_START_REQUEST_INVALID' THEN RAISE; END IF; END;
    END LOOP;
    PERFORM pg_temp.contact_assert_no_attempt_v1('PUSH');
   WHEN 'push_permit_real_clock_expiry' THEN
    v_start:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request);
    PERFORM pg_temp.contact_assert_started_v1('PUSH',v_start);
    v_consume:=pg_temp.contact_consume_request_v1(v_start);
    v_wait:=greatest(0,extract(epoch FROM (v_start->>'consume_before_utc')::timestamptz-clock_timestamp()))+0.05;
    PERFORM pg_temp.contact_assert_v1(v_wait<=30.1,'bounded real 30-second permit expiry required');
    PERFORM pg_sleep(v_wait::double precision);
    BEGIN
     PERFORM public.weekly_contact_submission_consume_v1(v_consume);
     RAISE EXCEPTION 'CONTACT_NATIVE_ASSERT: expired permit granted a provider call';
    EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'CONTACT_PERMIT_EXPIRED' THEN RAISE; END IF; END;
    v_reply:=public.weekly_source_message_dispatch_target_start_atomic_v1(v_request||jsonb_build_object('lease_token',gen_random_uuid(),'worker_id','contact-native-late'));
    PERFORM pg_temp.contact_assert_v1(v_reply->'call_once'='false'::jsonb
     AND v_reply->>'provider_attempt_id'=v_start->>'provider_attempt_id'
     AND(SELECT consumed_at_utc IS NULL FROM public.weekly_contact_submission_permits WHERE id=(v_start->>'permit_id')::uuid),
     'expiry alone cannot fabricate a provider outcome or replacement attempt');
   ELSE RAISE EXCEPTION 'CONTACT_NATIVE_FIXTURE_UNKNOWN_CASE %',p_case;
  END CASE;
  SELECT jsonb_agg(to_jsonb(g) ORDER BY g.id) INTO v_after FROM public.weekly_candidate_outreach_generations g
   JOIN public.weekly_source_cycles c ON c.id=g.source_cycle_id
   WHERE c.source_group_id='f716c000-0000-4000-8000-000000000001';
  PERFORM pg_temp.contact_assert_v1(v_after IS NOT DISTINCT FROM v_clocks,'START/pause/QT shifted original native question clocks');
  -- No provider or result owner was called. Every actual attempt is unfinished;
  -- no permit/history may claim accepted delivery from fixture-only START.
  PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.weekly_message_target_attempts a
   JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_target_id=a.dispatch_target_id WHERE a.completed_at_utc IS NOT NULL)
   AND NOT EXISTS(SELECT 1 FROM public.weekly_contact_submission_permits WHERE status='ACCEPTED' OR accepted_at_utc IS NOT NULL),
   'fixture must never fabricate provider acceptance');
  v_absence:=pg_temp.contact_case_ids_v1();
  v_absence:=v_absence||coalesce((SELECT jsonb_agg(jsonb_build_object('relation_name','public.candidate_push_timing_commands',
   'column_name','request_id','object_id',c.request_id)) FROM public.candidate_push_timing_commands c
   WHERE c.agency_id='f7160000-0000-4000-8000-000000000019'),'[]'::jsonb);
  v_absence:=v_absence||coalesce((SELECT jsonb_agg(jsonb_build_object('relation_name','public.weekly_contact_pause_commands',
   'column_name','request_id','object_id',c.request_id)) FROM public.weekly_contact_pause_commands c
   JOIN public.weekly_contact_pause_scopes s ON s.id=c.scope_id WHERE s.candidate_id::text LIKE 'f716c000-%'),'[]'::jsonb);
  RAISE EXCEPTION USING ERRCODE='ZC001',MESSAGE='CONTACT_CASE_ROLLBACK_SUCCESS';
 EXCEPTION WHEN SQLSTATE 'ZC001' THEN
  IF SQLERRM<>'CONTACT_CASE_ROLLBACK_SUCCESS' THEN RAISE; END IF;
 END;
 SELECT jsonb_build_object('targets',(SELECT jsonb_agg(to_jsonb(t) ORDER BY t.id) FROM public.weekly_message_dispatch_targets t
   JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_target_id=t.id),
  'permits',(SELECT jsonb_agg(to_jsonb(p) ORDER BY p.id) FROM public.weekly_contact_submission_permits p),
  'scopes',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) FROM public.weekly_contact_pause_scopes s)) INTO v_after;
 PERFORM pg_temp.contact_assert_v1(v_after IS NOT DISTINCT FROM v_before,'case subtransaction did not restore native target/permit/scope baseline');
 FOR v_object IN SELECT value FROM jsonb_array_elements(v_absence) LOOP
  INSERT INTO pg_temp.contact_start_fixture_absence_ids VALUES(v_object->>'relation_name',v_object->>'column_name',
   (v_object->>'object_id')::uuid) ON CONFLICT DO NOTHING;
 END LOOP;
 INSERT INTO pg_temp.contact_start_fixture_results VALUES(p_case,'PASS');
END;
$function$;

-- Phase A deliberately precedes any QT bootstrap.
SELECT pg_temp.contact_run_case_v1('email_missing_qt');
SELECT pg_temp.contact_run_case_v1('push_missing_qt');
