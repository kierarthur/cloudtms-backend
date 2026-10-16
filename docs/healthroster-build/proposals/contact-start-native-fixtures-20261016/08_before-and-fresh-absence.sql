-- Run before the genuine prefix AND on a fresh read-only connection after rollback.
-- New proposed stores may correctly be absent after staged DDL rolled back.
-- The generated exact UUID set from 07 is an ADDITIONAL independent fresh probe.
DO $absence$
DECLARE v_item record;v_found boolean;
BEGIN
 IF current_database() NOT IN('cloudtms_test_clone','banking_modal_v2_test') THEN RAISE EXCEPTION 'CONTACT_FIXTURE_ABSENCE_WRONG_DATABASE';END IF;
 FOR v_item IN SELECT * FROM(VALUES
  ('tms_users','id'),
  ('clients','id'),
  ('client_settings','client_id'),
  ('candidates','id'),
  ('contracts','id'),
  ('contracts','candidate_id'),
  ('timesheets','timesheet_id'),
  ('timesheets','contract_id'),
  ('weekly_source_groups','id'),
  ('weekly_source_cycles','source_group_id'),
  ('weekly_source_uploads','source_cycle_id'),
  ('weekly_source_projection_publications','source_cycle_id'),
  ('weekly_work_events','candidate_id'),
  ('weekly_discrepancy_incidents','candidate_id'),
  ('weekly_candidate_cohorts','candidate_id'),
  ('weekly_candidate_outreach_generations','candidate_id'),
  ('weekly_route_activations','candidate_id'),
  ('weekly_manager_recipient_routes','source_cycle_id'),
  ('candidate_notifications','candidate_id'),
  ('weekly_contact_pause_scopes','candidate_id'),
  ('weekly_message_dispatch_targets','external_target_id'))x(table_name,column_name) LOOP
  IF to_regclass('public.'||v_item.table_name) IS NOT NULL THEN
   EXECUTE format('SELECT EXISTS(SELECT 1 FROM public.%I WHERE %I::text LIKE %L)',
    v_item.table_name,v_item.column_name,'f716c000-%') INTO v_found;
   IF v_found THEN RAISE EXCEPTION 'CONTACT_FIXTURE_NOT_ABSENT public.%.%',v_item.table_name,v_item.column_name;END IF;
  END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM public.weekly_source_groups WHERE code='CONTACT_START_NATIVE_FIXTURE') THEN
  RAISE EXCEPTION 'CONTACT_FIXTURE_GROUP_CODE_REMAINS';END IF;
 FOR v_item IN SELECT unnest(ARRAY['candidate_push_timing_policy','candidate_push_timing_commands',
  'weekly_contact_submission_permits']) table_name LOOP
  IF to_regclass('public.'||v_item.table_name) IS NOT NULL THEN
   EXECUTE format('SELECT EXISTS(SELECT 1 FROM public.%I WHERE agency_id=%L::uuid)',v_item.table_name,
    'f7160000-0000-4000-8000-000000000019') INTO v_found;
   IF v_found THEN RAISE EXCEPTION 'CONTACT_FIXTURE_DEPLOYMENT_REMAINS %',v_item.table_name;END IF;
  END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('public.candidate_push_timing_policy')
  AND attname='contact_fixture_unavailable_policy_json' AND NOT attisdropped) THEN
  RAISE EXCEPTION 'CONTACT_FIXTURE_DEPENDENCY_FAULT_REMAINS';END IF;
END;
$absence$;
SELECT jsonb_build_object('fixture','CONTACT_START_RESERVED_ABSENCE','absent',true,'database',current_database());
