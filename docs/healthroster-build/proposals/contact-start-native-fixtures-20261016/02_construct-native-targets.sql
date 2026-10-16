-- Consumes a genuine prefix. All business generations/intents/publication already
-- exist through their actual owners; this file does not INSERT or promote them.
-- Safe synthetic text and captured transport IDs below are fixture facts only.
-- No provider material, provider acceptance, signed approval or financial row is invented.
DO $construct$
DECLARE v_seed record; v_intent public.weekly_message_intents%rowtype;
 v_command public.weekly_message_dispatch_commands%rowtype; v_route public.weekly_manager_recipient_routes%rowtype;
 v_input jsonb;v_prepare jsonb;v_stage jsonb;v_request jsonb;v_claim jsonb;v_item jsonb;
 v_target_id uuid;v_target_snapshot uuid;v_external uuid;v_snapshot_hash text;
 v_worker text:='contact-native-fixture';v_member jsonb;v_pairs jsonb;v_group uuid;
BEGIN
 PERFORM set_config('request.jwt.claim.role','service_role',true);
 PERFORM pg_temp.contact_assert_v1(current_database() IN('cloudtms_test_clone','banking_modal_v2_test'),'wrong database');
 PERFORM pg_temp.contact_assert_v1(clock_timestamp()-transaction_timestamp()<interval '180 seconds',
  'native claim uses transaction time: prefix too old for genuine 300-second lease budget');
 PERFORM pg_temp.contact_assert_v1((SELECT count(*)=2 FROM pg_temp.contact_start_fixture_input),
  'NATIVE_SOURCE_PREFIX_REQUIRED: exactly one PUSH and one EMAIL DUE intent');
 PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.weekly_contact_submission_permits),
  'fresh temporary permit stores required');
 -- Do not use an old verification row inserted directly as CURRENT. The prefix
 -- is separately reviewed by the Source report/build owner and its native output
 -- receipt is recorded in the fixture input; current native readers rederive facts.
 FOR v_seed IN SELECT * FROM pg_temp.contact_start_fixture_input ORDER BY kind LOOP
  SELECT * INTO STRICT v_intent FROM public.weekly_message_intents WHERE id=v_seed.message_intent_id;
  PERFORM pg_temp.contact_assert_v1(v_intent.state='DUE' AND v_intent.environment='TEST'
   AND v_intent.agency_id='f7160000-0000-4000-8000-000000000019'::uuid,'prefix deployment or DUE intent mismatch');
  SELECT c.source_group_id INTO STRICT v_group FROM public.weekly_source_cycles c WHERE c.id=v_intent.source_cycle_id;
  PERFORM pg_temp.contact_assert_v1(v_group='f716c000-0000-4000-8000-000000000001'::uuid
   AND EXISTS(SELECT 1 FROM public.weekly_source_groups g WHERE g.id=v_group
    AND g.code='CONTACT_START_NATIVE_FIXTURE' AND g.environment='TEST' AND g.agency_id=v_intent.agency_id),
   'prefix must own reserved group and native deployment');
  PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.tms_users u WHERE u.id=v_seed.actor_user_id
   AND u.id::text LIKE 'f716c000-%' AND u.is_active AND u.role='admin'), 'fixture current Office admin required');
  IF v_seed.kind='PUSH' THEN
   PERFORM pg_temp.contact_assert_v1(v_intent.audience_kind='CANDIDATE'
    AND v_intent.tranche_kind='CANDIDATE_INITIAL'
    AND private.weekly_source_candidate_generation_current_v1(v_intent.candidate_generation_id),
    'actual active original CHECK_HOURS generation required');
   PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.weekly_candidate_outreach_generations g
    WHERE g.id=v_intent.candidate_generation_id AND g.request_kind='CHECK_HOURS'
     AND g.deadline_at_utc>clock_timestamp()+interval '5 minutes'
     AND g.deadline_at_utc<clock_timestamp()+interval '24 hours'), 'native active short question window required');
  ELSE
   PERFORM pg_temp.contact_assert_v1(v_intent.audience_kind='MANAGER', 'actual original manager intent required');
  END IF;
  v_request:=jsonb_build_object('message_intent_id',v_intent.id,
   'projection_publication_id',(SELECT c.current_projection_publication_id FROM public.weekly_source_cycles c
    WHERE c.id=v_intent.source_cycle_id));
  PERFORM private.weekly_source_query_current_publication_v1(v_intent.source_cycle_id,
   (v_request->>'projection_publication_id')::uuid);
  v_input:=public.weekly_source_message_render_input_v1(v_request);
  PERFORM pg_temp.contact_assert_v1(v_input->>'membership_hash' ~ '^[0-9a-f]{64}$','native render membership required');
  IF v_seed.kind='PUSH' THEN
   v_stage:=public.weekly_source_message_render_stage_atomic_v1(v_request||jsonb_build_object(
    'membership_hash',v_input->'membership_hash','policy_version','contact-native-fixture',
    'renderer_version','contact-native-fixture','plain_body','Rollback-only Timesheet check.'));
  ELSE
   PERFORM pg_temp.contact_assert_v1((v_input->>'shift_count')::integer BETWEEN 2 AND 500,
    'mixed manager native render must include at least two shifts');
   v_prepare:=public.weekly_source_manager_route_prepare_atomic_v1(v_request);
   v_stage:=public.weekly_source_message_render_stage_atomic_v1(v_request||jsonb_build_object(
    'membership_hash',v_input->'membership_hash','policy_version','1.8.0','renderer_version','1.4.0',
    'structure_version','1.0.0','subject_text',format('Timesheet queries requiring your review - %s shifts',v_input->>'shift_count'),
    'html_body','<p>Rollback-only Timesheet check. This fixture is never delivered.</p>',
    'plain_body','Rollback-only Timesheet check. This fixture is never delivered.',
    -- Synthetic non-secret transport witnesses, never a real issued secure link.
    'credential_hash',encode(private.weekly_source_sha256_jsonb_v1('CONTACT_FIXTURE_ONLY',to_jsonb(v_intent.id)),'hex'),
    'control_plane_ticket_id',gen_random_uuid(),'agency_receipt_id',gen_random_uuid(),
    'data_plane_identity','contact-native-fixture-invalid','route_version','v1','credential_version','v1',
    'manager_route_preparation_id',v_prepare->'manager_route_preparation_id'));
  END IF;
  PERFORM pg_temp.contact_assert_v1(v_stage->'ok'='true'::jsonb AND v_stage->'replay'='false'::jsonb,
   'native render constructor failed');
  SELECT * INTO STRICT v_command FROM public.weekly_message_dispatch_commands WHERE id=(v_stage->>'dispatch_command_id')::uuid;
  -- Claims have global maintenance side effects and no ID filter. Hold this
  -- bounded table fence and REFUSE any unrelated eligible or stale row.
  -- Root runs only with the common rehearsal lock; no background drain is called.
  LOCK TABLE public.weekly_message_dispatch_commands,public.weekly_message_dispatch_targets IN SHARE ROW EXCLUSIVE MODE;
  PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.weekly_message_dispatch_commands c
   WHERE c.id<>v_command.id AND c.target_set_state='NOT_PREPARED' AND (
    (c.state IN('READY','FAILED') AND coalesce(c.next_attempt_at_utc,'-infinity'::timestamptz)<=transaction_timestamp())
    OR(c.state='LEASED' AND c.lease_expires_at_utc<=transaction_timestamp()))),
   'UNRELATED_NATIVE_COMMAND_CLAIM_WORK_PRESENT: use isolated native fixture database');
  v_claim:=public.weekly_source_message_dispatch_claim_v1(jsonb_build_object('worker_id',v_worker,'limit',1,'lease_seconds',300));
  PERFORM pg_temp.contact_assert_v1(v_claim->>'claimed_count'='1'
   AND v_claim->'commands'->0->>'dispatch_command_id'=v_command.id::text,'native command claim must select only fixture');
  v_item:=v_claim->'commands'->0;
  IF v_seed.kind='PUSH' THEN
   v_target_snapshot:='f716c000-0000-4000-8000-000000000080';
   v_external:='f716c000-0000-4000-8000-000000000081';
   v_snapshot_hash:=encode(private.weekly_source_sha256_jsonb_v1('CONTACT_FIXTURE_DEVICE_ONLY',to_jsonb(v_external)),'hex');
   v_request:=jsonb_build_object('dispatch_command_id',v_command.id,'lease_token',v_item->'lease_token','worker_id',v_worker,
    'control_plane_snapshot_id',v_target_snapshot,'suppression_reason',null,'targets',jsonb_build_array(jsonb_build_object(
     'external_target_id',v_external,'target_fingerprint',repeat('71',32),'target_snapshot_hash',v_snapshot_hash,
     'target_version',1,'provider','FCM','safe_target_snapshot',jsonb_build_object('control_plane_snapshot_id',v_target_snapshot,
      'snapshot_device_id',v_external,'provider','FCM','target_revision_hash',v_snapshot_hash))));
  ELSE
   SELECT * INTO STRICT v_route FROM public.weekly_manager_recipient_routes WHERE id=v_command.recipient_route_id;
   PERFORM pg_temp.contact_assert_v1(v_route.protected_recipient_address LIKE '%@example.invalid', 'synthetic manager address required');
   v_request:=jsonb_build_object('dispatch_command_id',v_command.id,'lease_token',v_item->'lease_token','worker_id',v_worker,
    'control_plane_snapshot_id',null,'suppression_reason',null,'targets',jsonb_build_array(jsonb_build_object(
     'external_target_id',v_route.id,'target_fingerprint',encode(v_route.normalised_recipient_hash,'hex'),
     'target_snapshot_hash',encode(v_command.rendered_content_hash,'hex'),'target_version',1,'provider','POWER_AUTOMATE',
     'safe_target_snapshot',jsonb_build_object('recipient_route_id',v_route.id))));
  END IF;
  PERFORM public.weekly_source_message_targets_register_atomic_v1(v_request);
  SELECT id INTO STRICT v_target_id FROM public.weekly_message_dispatch_targets WHERE dispatch_command_id=v_command.id;
  -- The target claim also retires stale rows globally, so reject every such
  -- unrelated row, even if it would not be returned by the claim's limit.
  PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.weekly_message_dispatch_targets t
   JOIN public.weekly_message_dispatch_commands c ON c.id=t.dispatch_command_id
   JOIN public.weekly_message_intents i ON i.id=c.message_intent_id
   JOIN public.weekly_message_renders r ON r.id=c.message_render_id
   WHERE t.id<>v_target_id AND(
    (t.state='SUBMISSION_STARTED' AND t.lease_expires_at_utc<=transaction_timestamp())
    OR(t.state NOT IN('ACCEPTED','DEFINITELY_REJECTED','AMBIGUOUS','RETIRED','SKIPPED') AND(i.state='RETIRED' OR r.state='STALE'))
    OR(t.state IN('READY','TRANSIENT_FAILURE') AND coalesce(t.next_attempt_at_utc,'-infinity'::timestamptz)<=transaction_timestamp())
    OR(t.state='LEASED' AND t.lease_expires_at_utc<=transaction_timestamp()))),
   'UNRELATED_NATIVE_TARGET_CLAIM_WORK_PRESENT: use isolated native fixture database');
  v_claim:=public.weekly_source_message_dispatch_target_claim_v1(jsonb_build_object('worker_id',v_worker,'limit',1,'lease_seconds',300));
  PERFORM pg_temp.contact_assert_v1(v_claim->>'claimed_count'='1'
   AND v_claim->'targets'->0->>'dispatch_target_id'=v_target_id::text,'native target claim must select only fixture');
  INSERT INTO pg_temp.contact_start_fixture_targets VALUES(v_seed.kind,v_target_id,v_command.id,
   (v_claim->'targets'->0->>'lease_token')::uuid,v_worker,v_command.agency_id,v_command.environment);
  v_input:=private.weekly_contact_source_membership_v1(v_target_id,v_command.agency_id,v_command.environment);
  PERFORM pg_temp.contact_assert_v1(v_input->'current'='true'::jsonb,'native membership is not currently eligible');
  v_pairs:=v_input->'scope_pairs';
  PERFORM pg_temp.contact_assert_v1(jsonb_array_length(v_pairs) BETWEEN (CASE WHEN v_seed.kind='EMAIL' THEN 2 ELSE 1 END) AND 500,
   'manager fixture needs two distinct worker/client scopes, not duplicate shifts');
  FOR v_member IN SELECT value FROM jsonb_array_elements(v_pairs) LOOP
   PERFORM pg_temp.contact_assert_v1(v_member->>'candidate_id' LIKE 'f716c000-%'
    AND v_member->>'client_id' LIKE 'f716c000-%','nonfixture member rejected');
  END LOOP;
 END LOOP;
END;
$construct$;
