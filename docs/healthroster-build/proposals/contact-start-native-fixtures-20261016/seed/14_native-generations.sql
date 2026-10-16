-- Actual native query activation/generation/intent/notification owners only.
DO $generations$
DECLARE v_cycle uuid;v_pub uuid;v_actor uuid:='f716c000-0000-4000-8000-000000000003';v_ask jsonb;v_manager jsonb;
 v_ids jsonb;v_generation uuid;v_push uuid;v_email uuid;v_receipt jsonb;v_artifact jsonb;v_review jsonb;v_projection jsonb;r record;
BEGIN
 SELECT (value->>'source_cycle_id')::uuid INTO STRICT v_cycle FROM pg_temp.contact_seed_receipts WHERE name='native_cycle';
 SELECT (value->>'publication_id')::uuid INTO STRICT v_pub FROM pg_temp.contact_seed_receipts WHERE name='native_projection_publish';
 SELECT value INTO STRICT v_artifact FROM pg_temp.contact_native_seed_artifact;
 SELECT jsonb_agg(i.id ORDER BY i.id) INTO v_ids FROM public.weekly_discrepancy_incidents i
  WHERE i.source_cycle_id=v_cycle AND i.candidate_id='f716c000-0000-4000-8000-000000000010' AND i.state='OPEN'
   AND i.candidate_action_state NOT IN('RESPONDED','NOT_REQUIRED');
 PERFORM pg_temp.contact_assert_v1(jsonb_array_length(v_ids)=1,'one genuine missing-A incident');
 v_ask:=public.weekly_source_query_ask_candidate_atomic_v1(jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle,
  'projection_publication_id',v_pub,'candidate_id','f716c000-0000-4000-8000-000000000010',
  'client_id','f716c000-0000-4000-8000-000000000020','incident_ids',v_ids));
 PERFORM pg_temp.contact_assert_v1(v_ask->>'status' IN('CREATED','UNCHANGED'),'native ASK must activate/retain original current question');
 v_generation:=(v_ask->>'candidate_generation_id')::uuid;
 PERFORM pg_temp.contact_assert_v1(private.weekly_source_candidate_generation_current_v1(v_generation)
  AND EXISTS(SELECT 1 FROM public.weekly_candidate_outreach_generations WHERE id=v_generation AND request_kind='CHECK_HOURS'
   AND state='ACTIVE' AND deadline_at_utc>clock_timestamp()+interval '5 minutes' AND deadline_at_utc<clock_timestamp()+interval '24 hours'),
  'native active original CHECK_HOURS window');
 SELECT id INTO STRICT v_push FROM public.weekly_message_intents WHERE candidate_generation_id=v_generation
  AND tranche_kind='CANDIDATE_INITIAL' AND audience_kind='CANDIDATE' AND state='DUE' AND due_at_utc<=clock_timestamp();
 PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.weekly_candidate_message_notifications m
  JOIN public.candidate_notifications n ON n.id=m.notification_id WHERE m.message_intent_id=v_push
   AND n.state='UNREAD' AND n.candidate_id='f716c000-0000-4000-8000-000000000010'
   AND private.candidate_notification_visible_v1(n,clock_timestamp())), 'actual unread visible Candidate notification required');
 SELECT jsonb_agg(i.id ORDER BY i.id) INTO v_ids FROM public.weekly_discrepancy_incidents i
  JOIN public.weekly_issue_comparison_revisions c ON c.id=i.current_comparison_revision_id
  WHERE i.source_cycle_id=v_cycle AND i.candidate_id IN('f716c000-0000-4000-8000-000000000011','f716c000-0000-4000-8000-000000000012')
   AND i.state='OPEN' AND c.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' AND c.candidate_timesheet_id IS NOT NULL
   AND i.manager_potential_state='AVAILABLE';
 PERFORM pg_temp.contact_assert_v1(jsonb_array_length(v_ids)=2,'two actual zero-conflict worker/client manager scopes required');
 v_manager:=public.weekly_source_query_send_manager_now_atomic_v1(jsonb_build_object('actor_user_id',v_actor,
  'source_cycle_id',v_cycle,'projection_publication_id',v_pub,'incident_ids',v_ids));
 v_email:=(v_manager->>'message_intent_id')::uuid;
 PERFORM pg_temp.contact_assert_v1(v_email IS NOT NULL AND EXISTS(SELECT 1 FROM public.weekly_message_intents
  WHERE id=v_email AND audience_kind='MANAGER' AND state='DUE' AND due_at_utc<=clock_timestamp()),'native manual selected manager DUE intent');
 PERFORM pg_temp.contact_assert_v1(private.weekly_source_candidate_generation_current_v1(v_generation)
  AND EXISTS(SELECT 1 FROM public.weekly_message_intents WHERE id=v_push AND state='DUE'), 'manager-direct must retain separate worker question');
 PERFORM pg_temp.contact_assert_v1((SELECT count(DISTINCT c.candidate_id::text||':'||c.client_id::text)=2
  FROM public.weekly_manager_recipient_memberships m JOIN public.weekly_candidate_cohorts c ON c.id=m.candidate_cohort_id
  JOIN public.weekly_manager_cohort_due_events d ON d.recipient_generation_id=m.recipient_generation_id AND d.candidate_cohort_id=m.candidate_cohort_id
  JOIN public.weekly_message_intents i ON i.recipient_generation_id=m.recipient_generation_id AND d.id=ANY(i.sorted_due_event_ids)
  WHERE i.id=v_email),'actual frozen manager intent membership has two distinct worker/client pairs');
 PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.timesheets_financials f JOIN public.timesheets t ON t.timesheet_id=f.timesheet_id
  WHERE t.contract_id::text LIKE 'f716c000-%')
  AND NOT EXISTS(SELECT 1 FROM public.timesheets t WHERE t.contract_id::text LIKE 'f716c000-%'
   AND(t.authorised_at_server IS NOT NULL))
  AND NOT EXISTS(SELECT 1 FROM public.candidate_submission_workflows WHERE candidate_id::text LIKE 'f716c000-%'
   AND(manager_approved_at_utc IS NOT NULL OR manager_signature_component_id IS NOT NULL OR canonical_save_financials_id IS NOT NULL)),
  'entire prefix remains unsigned by manager, unapproved and nonfinancial');
 PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.banking_pay_workbench_jobs
  WHERE candidate_id::text LIKE 'f716c000-%'),'nonfinancial declaration must not enqueue Banking workbench work');
 PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.weekly_source_final_revisions WHERE source_cycle_id=v_cycle),
  'checking publication must not fabricate source financial finalisation');
 v_review:=public.weekly_source_combined_review_workspace_v1(jsonb_build_object('actor_user_id',v_actor,
  'source_group_id','f716c000-0000-4000-8000-000000000001','client_id','f716c000-0000-4000-8000-000000000020',
  'week_ending',v_artifact->'week_ending','tab','queries','section','questions','limit',100));
 SELECT jsonb_agg(jsonb_build_object('candidate_id',row->'candidate_id','client_id',row->'client_id',
  'scope_key',row->'scope_key') ORDER BY row->>'candidate_id') INTO v_projection FROM jsonb_array_elements(v_review->'rows') row
 WHERE row->>'candidate_id' IN('f716c000-0000-4000-8000-000000000010','f716c000-0000-4000-8000-000000000011','f716c000-0000-4000-8000-000000000012')
  AND nullif(row->>'scope_key','') IS NOT NULL AND EXISTS(SELECT 1 FROM jsonb_array_elements(v_review->'owners') owner
   WHERE owner->>'key'=row->>'scope_key' AND owner->'scope'->>'source_group_id'='f716c000-0000-4000-8000-000000000001'
    AND owner->'scope'->>'source_cycle_id'=v_cycle::text);
 PERFORM pg_temp.contact_assert_v1(jsonb_array_length(v_projection)=3,'genuine combined query rows must expose native owner scope_key');
 INSERT INTO pg_temp.contact_seed_receipts VALUES('native_query_projection',v_projection);
 INSERT INTO pg_temp.contact_seed_receipts VALUES('native_ask_candidate',v_ask),('native_manager_selection',v_manager);
 v_receipt:=jsonb_build_object('version','CONTACT_NATIVE_SOURCE_PREFIX_RECEIPT_V1','native_constructors_completed',true,
  'source_cycle_id',v_cycle,'projection_publication_id',v_pub,'candidate_generation_id',v_generation,
  'push_intent_id',v_push,'email_intent_id',v_email,'artifact_as_of',v_artifact->'as_of',
  'artifact_report_sha256',v_artifact->'report'->'sha256','artifact_signature_sha256',v_artifact->'signature'->'sha256',
  'restoration',(SELECT value FROM pg_temp.contact_seed_receipts WHERE name='before_shared_settings'),
  'native_query_projection',v_projection,
  'native_publication_receipt',(SELECT value FROM pg_temp.contact_seed_receipts WHERE name='native_projection_publish'),
  'native_comparison_audit',(SELECT value FROM pg_temp.contact_seed_receipts WHERE name='native_comparison_audit'),
  'synthetic_evidence_object_uploaded',false,'financial_or_manager_approval',false,'provider_called',false);
 INSERT INTO pg_temp.contact_start_fixture_input VALUES('PUSH',v_push,v_actor,v_receipt),('EMAIL',v_email,v_actor,v_receipt);
 -- Remove only our exact observers. Existing native triggers were never altered.
 FOR r IN SELECT * FROM pg_temp.contact_seed_observers ORDER BY relation_name LOOP
  EXECUTE format('DROP TRIGGER %I ON %s',r.trigger_name,r.relation_name::regclass);
 END LOOP;
 PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='contact_seed_insert_observer'), 'observer cleanup complete');
 PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM pg_temp.contact_start_fixture_absence_ids WHERE relation_name='public.audit_events')
  AND EXISTS(SELECT 1 FROM pg_temp.contact_start_fixture_absence_ids WHERE relation_name='public.timesheets')
  AND EXISTS(SELECT 1 FROM pg_temp.contact_start_fixture_absence_ids WHERE relation_name='public.candidate_submission_components')
  AND EXISTS(SELECT 1 FROM pg_temp.contact_start_fixture_absence_ids WHERE relation_name='public.weekly_source_uploads')
  AND EXISTS(SELECT 1 FROM pg_temp.contact_start_fixture_absence_ids WHERE relation_name='public.weekly_source_projection_publications')
  AND EXISTS(SELECT 1 FROM pg_temp.contact_start_fixture_absence_ids WHERE relation_name='public.weekly_message_intents')
  AND EXISTS(SELECT 1 FROM pg_temp.contact_start_fixture_absence_ids WHERE relation_name='public.candidate_notifications'), 'actual native UUID inventory is incomplete');
END;
$generations$;
SELECT kind,message_intent_id,source_prefix_receipt FROM pg_temp.contact_start_fixture_input ORDER BY kind;
