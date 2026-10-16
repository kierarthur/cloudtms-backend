-- Read-only native boundary assertions. Root executes only under its rollback lock.
-- No business fixture, provider, queue claim, approval or database deployment here.
DO $boundary$
DECLARE v_role text; v_table text; v_privilege text; v_signature text; v_request jsonb;
BEGIN
 IF current_database() NOT IN('cloudtms_test_clone','banking_modal_v2_test')
  OR NOT EXISTS(SELECT 1 FROM private.cloudtms_database_identity WHERE singleton AND environment='TEST') THEN
  RAISE EXCEPTION 'CONTACT_START_FIXTURE_WRONG_DATABASE'; END IF;
 FOREACH v_table IN ARRAY ARRAY[
  'weekly_contact_pause_scopes','weekly_contact_pause_commands','weekly_contact_pause_events',
  'weekly_contact_submission_permits','weekly_contact_delivery_memberships','weekly_contact_recheck_tasks',
  'candidate_push_timing_policy','candidate_push_timing_commands'] LOOP
  IF NOT EXISTS(SELECT 1 FROM pg_class WHERE oid=to_regclass('public.'||v_table)
   AND relrowsecurity AND relforcerowsecurity) THEN RAISE EXCEPTION 'CONTACT_FIXTURE_RLS_MISSING %',v_table; END IF;
  FOREACH v_role IN ARRAY ARRAY['anon','authenticated','service_role'] LOOP
   FOREACH v_privilege IN ARRAY ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER'] LOOP
    IF has_table_privilege(v_role,'public.'||v_table,v_privilege) THEN
     RAISE EXCEPTION 'CONTACT_FIXTURE_DIRECT_TABLE_GRANT % % %',v_role,v_table,v_privilege; END IF;
   END LOOP;
  END LOOP;
 END LOOP;
 FOREACH v_signature IN ARRAY ARRAY[
  'public.weekly_contact_submission_start_v1(jsonb)',
  'public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb)',
  'public.weekly_contact_submission_consume_v1(jsonb)'] LOOP
  IF NOT has_function_privilege('service_role',v_signature,'EXECUTE') THEN
   RAISE EXCEPTION 'CONTACT_FIXTURE_SERVICE_EXECUTE_MISSING %',v_signature; END IF;
  FOREACH v_role IN ARRAY ARRAY['anon','authenticated'] LOOP
   IF has_function_privilege(v_role,v_signature,'EXECUTE') THEN
    RAISE EXCEPTION 'CONTACT_FIXTURE_BROWSER_EXECUTE % %',v_role,v_signature; END IF;
  END LOOP;
 END LOOP;
 FOREACH v_signature IN ARRAY ARRAY[
  'private.weekly_contact_source_membership_v1(uuid,uuid,text)',
  'private.weekly_contact_source_admission_v1(jsonb)',
  'private.weekly_contact_source_permit_v1(uuid,uuid,jsonb)',
  'private.weekly_contact_query_admission_v1(jsonb)',
  'private.weekly_contact_query_content_v1(uuid,uuid)',
  'private.weekly_contact_scope_lock_v1(uuid,text,uuid,uuid)'] LOOP
  FOREACH v_role IN ARRAY ARRAY['anon','authenticated','service_role'] LOOP
   IF has_function_privilege(v_role,v_signature,'EXECUTE') THEN
    RAISE EXCEPTION 'CONTACT_FIXTURE_PRIVATE_EXECUTE % %',v_role,v_signature; END IF;
  END LOOP;
 END LOOP;
 -- Actual current JWT-role checks, including replay-independent ingress.
 -- Claim settings model the trusted service transport; this does not bypass a table ACL.
 FOREACH v_role IN ARRAY ARRAY['anon','authenticated',''] LOOP
  PERFORM set_config('request.jwt.claim.role',v_role,true);
  BEGIN
   PERFORM public.weekly_contact_submission_start_v1('{}');
   RAISE EXCEPTION 'CONTACT_FIXTURE_CURRENT_ROLE_BYPASS';
  EXCEPTION WHEN insufficient_privilege THEN
   IF SQLERRM<>'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' THEN RAISE; END IF;
  END;
  BEGIN
   PERFORM public.weekly_source_message_dispatch_target_start_atomic_v1('{}');
   RAISE EXCEPTION 'CONTACT_FIXTURE_NATIVE_CURRENT_ROLE_BYPASS';
  EXCEPTION WHEN insufficient_privilege THEN
   IF SQLERRM<>'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' THEN RAISE; END IF;
  END;
  BEGIN
   PERFORM public.weekly_contact_submission_consume_v1('{}');
   RAISE EXCEPTION 'CONTACT_FIXTURE_CONSUME_CURRENT_ROLE_BYPASS';
  EXCEPTION WHEN insufficient_privilege THEN
   IF SQLERRM<>'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' THEN RAISE; END IF;
  END;
 END LOOP;
 PERFORM set_config('request.jwt.claim.role','service_role',true);
 FOR v_request IN SELECT value FROM jsonb_array_elements('[null,[],{},
  {"version":"CONTACT_START_V1","owner_kind":"UNKNOWN"},
  {"version":"CONTACT_START_V1","owner_kind":"WEEKLY_SOURCE_TARGET","channel":"EMAIL"}]') LOOP
  BEGIN
   PERFORM public.weekly_contact_submission_start_v1(v_request);
   RAISE EXCEPTION 'CONTACT_FIXTURE_OPEN_START_SCHEMA';
  EXCEPTION WHEN invalid_parameter_value THEN
   IF SQLERRM<>'CONTACT_REQUEST_INVALID' THEN RAISE; END IF;
  END;
 END LOOP;
 -- Historical three-key callers must be upgraded; they receive no admission.
 BEGIN
  PERFORM public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb_build_object(
   'dispatch_target_id','f716c000-0000-4000-8000-000000000090',
   'lease_token','f716c000-0000-4000-8000-000000000091','worker_id','contact-native-fixture'));
  RAISE EXCEPTION 'CONTACT_FIXTURE_LEGACY_DEPLOYMENT_BYPASS';
 EXCEPTION WHEN invalid_parameter_value THEN
  IF SQLERRM<>'WEEKLY_SOURCE_TARGET_START_REQUEST_INVALID' THEN RAISE; END IF;
 END;
END;
$boundary$;
SELECT jsonb_build_object('fixture','CONTACT_START_SERVICE_BOUNDARIES','assertions','PASS',
 'qualification','NATIVE_WHEN_ROOT_EXECUTES','database',current_database());
