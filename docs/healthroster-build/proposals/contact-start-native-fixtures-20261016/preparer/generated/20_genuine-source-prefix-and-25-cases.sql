-- CONTACT_FIXTURE_COMPONENT 08_before-and-fresh-absence.sql SHA256 276454ef6c32ef82d7f07f2f6b23141aca4a02cf82bbad6ee2d674422341624c
-- Run before the genuine prefix AND on a fresh read-only connection after rollback.
-- New proposed stores may correctly be absent after staged DDL rolled back.
-- The generated exact UUID set from 07 is an ADDITIONAL independent fresh probe.
DO $absence$
DECLARE v_item record;v_found boolean;
BEGIN
 IF current_database() NOT IN('cloudtms_test_clone') THEN RAISE EXCEPTION 'CONTACT_FIXTURE_ABSENCE_WRONG_DATABASE';END IF;
 FOR v_item IN SELECT * FROM(VALUES
  ('tms_users','id'),
  ('clients','id'),
  ('client_settings','client_id'),
  ('candidates','id'),
  ('candidate_app_accounts','id'),
  ('candidate_app_sessions','id'),
  ('candidate_app_global_membership_links','membership_id'),
  ('candidate_submission_workflows','id'),
  ('candidate_submission_components','workflow_id'),
  ('contract_weeks','contract_id'),
  ('audit_events','actor_user_id'),
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
 IF EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='contact_seed_insert_observer') THEN
  RAISE EXCEPTION 'CONTACT_FIXTURE_OBSERVER_REMAINS';END IF;
 IF EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('public.candidate_push_timing_policy')
  AND attname='contact_fixture_unavailable_policy_json' AND NOT attisdropped) THEN
  RAISE EXCEPTION 'CONTACT_FIXTURE_DEPENDENCY_FAULT_REMAINS';END IF;
END;
$absence$;
SELECT jsonb_build_object('fixture','CONTACT_START_RESERVED_ABSENCE','absent',true,'database',current_database());

-- CONTACT_FIXTURE_COMPONENT 00_service-boundaries.sql SHA256 77fcf8e87bd16df89b7cf21abe8eb0d53d559996352e5b545e6f2d6e58f376b1
-- Read-only native boundary assertions. Root executes only under its rollback lock.
-- No business fixture, provider, queue claim, approval or database deployment here.
DO $boundary$
DECLARE v_role text; v_table text; v_privilege text; v_signature text; v_request jsonb;
BEGIN
 IF current_database() NOT IN('cloudtms_test_clone')
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
  'private.weekly_contact_scope_lock_v1(uuid,text,uuid,uuid)',
  'private.weekly_contact_office_rows_v1(jsonb,jsonb,uuid)'] LOOP
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

-- CONTACT_FIXTURE_COMPONENT 01_fixture-input.sql SHA256 d4df308febc10097af38238dcb0553282bee1a407759c8ead6cd461cdff48c23
-- TEMP-only fixture hand-off; never an API, deployed routine or persistence protocol.
-- Root's genuine native prefix runs after this file and inserts exactly two DUE intents.
-- Prefix ownership and required real constructors are in RUNBOOK.md. No fallback seed.
CREATE TEMP TABLE contact_start_fixture_input(
 kind text PRIMARY KEY CHECK(kind IN('PUSH','EMAIL')),
 message_intent_id uuid NOT NULL UNIQUE,
 actor_user_id uuid NOT NULL,
 source_prefix_receipt jsonb NOT NULL CHECK(jsonb_typeof(source_prefix_receipt)='object')
) ON COMMIT DROP;
CREATE TEMP TABLE contact_start_fixture_targets(
 kind text PRIMARY KEY CHECK(kind IN('PUSH','EMAIL')),
 dispatch_target_id uuid NOT NULL UNIQUE,
 dispatch_command_id uuid NOT NULL UNIQUE,
 lease_token uuid NOT NULL,
 worker_id text NOT NULL,
 agency_id uuid NOT NULL,
 environment text NOT NULL CHECK(environment='TEST')
) ON COMMIT DROP;
CREATE TEMP TABLE contact_start_fixture_absence_ids(
 relation_name text NOT NULL, column_name text NOT NULL, object_id uuid NOT NULL,
 PRIMARY KEY(relation_name,column_name,object_id)
) ON COMMIT DROP;
CREATE TEMP TABLE contact_start_fixture_results(case_name text PRIMARY KEY, assertions text NOT NULL CHECK(assertions='PASS')) ON COMMIT DROP;

CREATE OR REPLACE FUNCTION pg_temp.contact_assert_v1(p_ok boolean,p_message text)
RETURNS void LANGUAGE plpgsql AS $function$
BEGIN IF p_ok IS NOT TRUE THEN RAISE EXCEPTION 'CONTACT_NATIVE_ASSERT: %',p_message; END IF; END;
$function$;
CREATE OR REPLACE FUNCTION pg_temp.contact_start_request_v1(p_kind text)
RETURNS jsonb LANGUAGE sql AS $function$
 SELECT jsonb_build_object('dispatch_target_id',dispatch_target_id,'lease_token',lease_token,
  'worker_id',worker_id,'agency_id',agency_id,'environment',environment)
 FROM pg_temp.contact_start_fixture_targets WHERE kind=p_kind;
$function$;
CREATE OR REPLACE FUNCTION pg_temp.contact_consume_request_v1(p_start jsonb)
RETURNS jsonb LANGUAGE sql AS $function$
 SELECT jsonb_build_object('permit_id',p_start->'permit_id','provider_attempt_id',p_start->'provider_attempt_id',
  'dispatch_target_id',t.id,'dispatch_command_id',c.id,'target_snapshot_hash',encode(t.target_snapshot_hash,'hex'),
  'provider',t.provider,'content_hash',encode(t.rendered_content_hash,'hex'),'agency_id',c.agency_id,'environment',c.environment)
 FROM public.weekly_message_dispatch_targets t JOIN public.weekly_message_dispatch_commands c ON c.id=t.dispatch_command_id
 JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_target_id=t.id WHERE f.kind='PUSH';
$function$;

-- CONTACT_FIXTURE_COMPONENT seed/10_generated-artifact.sql SHA256 f9a93bec6d628e1d66c988a220edfe724ca2dcef61db192a2b34ca599ed56758
-- Generated through actual parser/adapter/projection exports. Synthetic fixture facts only.
CREATE TEMP TABLE contact_native_seed_artifact(value jsonb NOT NULL) ON COMMIT DROP;
INSERT INTO pg_temp.contact_native_seed_artifact VALUES ('{"version":"CONTACT_NATIVE_SOURCE_SEED_ARTIFACT_V1","as_of":"2026-10-16","week_start":"2026-10-05","week_ending":"2026-10-11","work_date":"2026-10-06","contract_start":"2026-08-12","contract_end":"2026-12-10","root_commit":"cae8715b349a4b78c34846bd46ecf967990058f5","synthetic_facts":true,"native_execution":"NOT_RUN","report":{"sha256":"d57f8453ba968e147f686c8c416b9fbb2f67a41133bcd793e467737f7288b345","byte_count":1685,"profile_id":"ROSTER_WEEKLY_SUMMARY_ACTUAL_V1"},"signature":{"sha256":"834de3ec90e69a100b1c10c36dd2289938613365600ffdaf3d27c774907a6a60","byte_count":125,"width":24,"height":8,"storage_key":"contact-native-fixture/TEST-declaration.png","object_uploaded":false},"begin_request":{"actor_user_id":"f716c000-0000-4000-8000-000000000003","environment":"TEST","agency_id":"f7160000-0000-4000-8000-000000000019","source_group_id":"f716c000-0000-4000-8000-000000000001","source_cycle_id":null,"report_scope_id":null,"client_id":"f716c000-0000-4000-8000-000000000020","original_filename":"contact-native-zero.csv","content_sha256":"d57f8453ba968e147f686c8c416b9fbb2f67a41133bcd793e467737f7288b345","byte_count":1685,"profile_code":"ROSTER_WEEKLY_SUMMARY_ACTUAL_V1","profile_version":"1","parser_version":"WEEKLY_SOURCE_STRICT_V1","normaliser_version":"ROSTER_WEEKLY_SUMMARY_ACTUAL_V1:1:WEEKLY_SOURCE_UPLOAD_ADAPTER_V1","workbook_part_and_sheet_fingerprint":null,"header_coordinate_map_json":{"grandParentBusinessUnitName":{"column_index":1,"column":"A","coordinate":"A1"},"parentBusinessUnitName":{"column_index":2,"column":"B","coordinate":"B1"},"businessUnitName":{"column_index":3,"column":"C","coordinate":"C1"},"clientWeekendDate":{"column_index":5,"column":"E","coordinate":"E1"},"weekendDate":{"column_index":6,"column":"F","coordinate":"F1"},"candidate":{"column_index":13,"column":"M","coordinate":"M1"},"payrollNumber":{"column_index":14,"column":"N","coordinate":"N1"},"bookingId":{"column_index":16,"column":"P","coordinate":"P1"},"timesheetId":{"column_index":17,"column":"Q","coordinate":"Q1"},"candidateUid":{"column_index":20,"column":"T","coordinate":"T1"},"candidateId":{"column_index":21,"column":"U","coordinate":"U1"},"expenses":{"column_index":34,"column":"AH","coordinate":"AH1"},"totalHours":{"column_index":35,"column":"AI","coordinate":"AI1"},"lineId":{"column_index":37,"column":"AK","coordinate":"AK1"},"bookingReference":{"column_index":56,"column":"BD","coordinate":"BD1"},"supplyType":{"column_index":58,"column":"BF","coordinate":"BF1"},"businessUnitId":{"column_index":60,"column":"BH","coordinate":"BH1"},"jobId":{"column_index":61,"column":"BI","coordinate":"BI1"},"jobType":{"column_index":62,"column":"BJ","coordinate":"BJ1"},"bookingStart":{"column_index":63,"column":"BK","coordinate":"BK1"},"bookingEnd":{"column_index":64,"column":"BL","coordinate":"BL1"}},"money_lexical_authority_version":null,"purpose":"ORDINARY","correction_session_id":null,"suggested_coverage_start_local_date":"2026-10-06","suggested_coverage_end_local_date":"2026-10-06","confirmed_coverage_start_local_date":"2026-10-05","confirmed_coverage_end_local_date":"2026-10-11","coverage_timezone":"Europe/London","coverage_confirmation_version":"WEEKLY_SOURCE_OFFICE_COVERAGE_CONFIRMATION_V1","coverage_confirmed_at_utc":"2026-10-16T02:33:58.452Z","coverage_shrink_acknowledged":false,"coverage_state":"COMPLETE","coverage_proof_kind":"OFFICE_COMPLETE_EXPORT_ATTESTATION","physical_row_count":3,"header_count":1,"trailer_count":0,"continuation_count":0,"accepted_count":2,"blocking_economic_duplicate_count":0,"malformed_count":0,"blocked_count":0,"file_metadata_json":{"client_id":"f716c000-0000-4000-8000-000000000020","import_use":"CHECKING","source_file_r2_key":null},"parser_summary_json":{"parser_version":"WEEKLY_SOURCE_STRICT_V1","adapter_version":"WEEKLY_SOURCE_UPLOAD_ADAPTER_V1","source_kind":"CSV","selected_worksheet":{"name":"CSV","index":0,"partPath":null,"physicalRowCount":3,"physicalOrdinalSpan":3,"physicalCellCount":195,"firstPhysicalRow":1,"lastPhysicalRow":3,"workbookPartAndSheetFingerprint":null},"row_counts":{"total":2,"sourceShifts":0,"sourceZero":2,"sourceExpensePresent":0,"sourceExpenseOmittedZero":2},"warning_count":0}},"stage_request":{"physical_rows":[{"source_row_ordinal":1,"bounded_raw_cells_json":{"1":{"coordinate":"R1C1","kind":"STRING","originalToken":"grand_parent_business_unit_name","decodedToken":"grand_parent_business_unit_name","tokenTruncated":false},"2":{"coordinate":"R1C2","kind":"STRING","originalToken":"parent_business_unit_name","decodedToken":"parent_business_unit_name","tokenTruncated":false},"3":{"coordinate":"R1C3","kind":"STRING","originalToken":"business_unit_name","decodedToken":"business_unit_name","tokenTruncated":false},"4":{"coordinate":"R1C4","kind":"STRING","originalToken":"Billing Group Name","decodedToken":"Billing Group Name","tokenTruncated":false},"5":{"coordinate":"R1C5","kind":"STRING","originalToken":"Client Weekend Date","decodedToken":"Client Weekend Date","tokenTruncated":false},"6":{"coordinate":"R1C6","kind":"STRING","originalToken":"Weekend Date","decodedToken":"Weekend Date","tokenTruncated":false},"7":{"coordinate":"R1C7","kind":"STRING","originalToken":"Custom Code 1","decodedToken":"Custom Code 1","tokenTruncated":false},"8":{"coordinate":"R1C8","kind":"STRING","originalToken":"Custom Code 2","decodedToken":"Custom Code 2","tokenTruncated":false},"9":{"coordinate":"R1C9","kind":"STRING","originalToken":"Custom Code 3","decodedToken":"Custom Code 3","tokenTruncated":false},"10":{"coordinate":"R1C10","kind":"STRING","originalToken":"Job Category","decodedToken":"Job Category","tokenTruncated":false},"11":{"coordinate":"R1C11","kind":"STRING","originalToken":"Vat Option","decodedToken":"Vat Option","tokenTruncated":false},"12":{"coordinate":"R1C12","kind":"STRING","originalToken":"Permanent Equivalent Rates","decodedToken":"Permanent Equivalent Rates","tokenTruncated":false},"13":{"coordinate":"R1C13","kind":"STRING","originalToken":"Candidate","decodedToken":"Candidate","tokenTruncated":false},"14":{"coordinate":"R1C14","kind":"STRING","originalToken":"Payroll Number","decodedToken":"Payroll Number","tokenTruncated":false},"15":{"coordinate":"R1C15","kind":"STRING","originalToken":"Vacancy Id","decodedToken":"Vacancy Id","tokenTruncated":false},"16":{"coordinate":"R1C16","kind":"STRING","originalToken":"Booking Id","decodedToken":"Booking Id","tokenTruncated":false},"17":{"coordinate":"R1C17","kind":"STRING","originalToken":"Timesheet Id","decodedToken":"Timesheet Id","tokenTruncated":false},"18":{"coordinate":"R1C18","kind":"STRING","originalToken":"Agency","decodedToken":"Agency","tokenTruncated":false},"19":{"coordinate":"R1C19","kind":"STRING","originalToken":"Agency Location","decodedToken":"Agency Location","tokenTruncated":false},"20":{"coordinate":"R1C20","kind":"STRING","originalToken":"Candidate Uid","decodedToken":"Candidate Uid","tokenTruncated":false},"21":{"coordinate":"R1C21","kind":"STRING","originalToken":"Candidate Id","decodedToken":"Candidate Id","tokenTruncated":false},"22":{"coordinate":"R1C22","kind":"STRING","originalToken":"TNA Reference","decodedToken":"TNA Reference","tokenTruncated":false},"23":{"coordinate":"R1C23","kind":"STRING","originalToken":"Approved By","decodedToken":"Approved By","tokenTruncated":false},"24":{"coordinate":"R1C24","kind":"STRING","originalToken":"Approved Date","decodedToken":"Approved Date","tokenTruncated":false},"25":{"coordinate":"R1C25","kind":"STRING","originalToken":"monday","decodedToken":"monday","tokenTruncated":false},"26":{"coordinate":"R1C26","kind":"STRING","originalToken":"tuesday","decodedToken":"tuesday","tokenTruncated":false},"27":{"coordinate":"R1C27","kind":"STRING","originalToken":"wednesday","decodedToken":"wednesday","tokenTruncated":false},"28":{"coordinate":"R1C28","kind":"STRING","originalToken":"thursday","decodedToken":"thursday","tokenTruncated":false},"29":{"coordinate":"R1C29","kind":"STRING","originalToken":"friday","decodedToken":"friday","tokenTruncated":false},"30":{"coordinate":"R1C30","kind":"STRING","originalToken":"saturday","decodedToken":"saturday","tokenTruncated":false},"31":{"coordinate":"R1C31","kind":"STRING","originalToken":"sunday","decodedToken":"sunday","tokenTruncated":false},"32":{"coordinate":"R1C32","kind":"STRING","originalToken":"Bonus","decodedToken":"Bonus","tokenTruncated":false},"33":{"coordinate":"R1C33","kind":"STRING","originalToken":"Bonus NI","decodedToken":"Bonus NI","tokenTruncated":false},"34":{"coordinate":"R1C34","kind":"STRING","originalToken":"Expenses","decodedToken":"Expenses","tokenTruncated":false},"35":{"coordinate":"R1C35","kind":"STRING","originalToken":"Total Hours","decodedToken":"Total Hours","tokenTruncated":false},"36":{"coordinate":"R1C36","kind":"STRING","originalToken":"Total Cost","decodedToken":"Total Cost","tokenTruncated":false},"37":{"coordinate":"R1C37","kind":"STRING","originalToken":"Line ID","decodedToken":"Line ID","tokenTruncated":false},"38":{"coordinate":"R1C38","kind":"STRING","originalToken":"STD Hours","decodedToken":"STD Hours","tokenTruncated":false},"39":{"coordinate":"R1C39","kind":"STRING","originalToken":"STD Unit Cost","decodedToken":"STD Unit Cost","tokenTruncated":false},"40":{"coordinate":"R1C40","kind":"STRING","originalToken":"STD Sub Cost","decodedToken":"STD Sub Cost","tokenTruncated":false},"41":{"coordinate":"R1C41","kind":"STRING","originalToken":"OT Hours","decodedToken":"OT Hours","tokenTruncated":false},"42":{"coordinate":"R1C42","kind":"STRING","originalToken":"OT Unit Cost","decodedToken":"OT Unit Cost","tokenTruncated":false},"43":{"coordinate":"R1C43","kind":"STRING","originalToken":"OT Sub Cost","decodedToken":"OT Sub Cost","tokenTruncated":false},"44":{"coordinate":"R1C44","kind":"STRING","originalToken":"SAT Hours","decodedToken":"SAT Hours","tokenTruncated":false},"45":{"coordinate":"R1C45","kind":"STRING","originalToken":"SAT Unit Cost","decodedToken":"SAT Unit Cost","tokenTruncated":false},"46":{"coordinate":"R1C46","kind":"STRING","originalToken":"SAT Sub Cost","decodedToken":"SAT Sub Cost","tokenTruncated":false},"47":{"coordinate":"R1C47","kind":"STRING","originalToken":"SUN Hours","decodedToken":"SUN Hours","tokenTruncated":false},"48":{"coordinate":"R1C48","kind":"STRING","originalToken":"SUN Unit Cost","decodedToken":"SUN Unit Cost","tokenTruncated":false},"49":{"coordinate":"R1C49","kind":"STRING","originalToken":"SUN Sub Cost","decodedToken":"SUN Sub Cost","tokenTruncated":false},"50":{"coordinate":"R1C50","kind":"STRING","originalToken":"BH Hours","decodedToken":"BH Hours","tokenTruncated":false},"51":{"coordinate":"R1C51","kind":"STRING","originalToken":"BH Unit Cost","decodedToken":"BH Unit Cost","tokenTruncated":false},"52":{"coordinate":"R1C52","kind":"STRING","originalToken":"BH Sub Cost","decodedToken":"BH Sub Cost","tokenTruncated":false},"53":{"coordinate":"R1C53","kind":"STRING","originalToken":"Nmw-Midweek-Adj/sat Hours","decodedToken":"Nmw-Midweek-Adj/sat Hours","tokenTruncated":false},"54":{"coordinate":"R1C54","kind":"STRING","originalToken":"Nmw-Midweek-Adj/sat Unit Cost","decodedToken":"Nmw-Midweek-Adj/sat Unit Cost","tokenTruncated":false},"55":{"coordinate":"R1C55","kind":"STRING","originalToken":"Nmw-Midweek-Adj/sat Sub Cost","decodedToken":"Nmw-Midweek-Adj/sat Sub Cost","tokenTruncated":false},"56":{"coordinate":"R1C56","kind":"STRING","originalToken":"Booking Reference","decodedToken":"Booking Reference","tokenTruncated":false},"57":{"coordinate":"R1C57","kind":"STRING","originalToken":"Booking Reason","decodedToken":"Booking Reason","tokenTruncated":false},"58":{"coordinate":"R1C58","kind":"STRING","originalToken":"Supply Type","decodedToken":"Supply Type","tokenTruncated":false},"59":{"coordinate":"R1C59","kind":"STRING","originalToken":"Type","decodedToken":"Type","tokenTruncated":false},"60":{"coordinate":"R1C60","kind":"STRING","originalToken":"Business Unit ID","decodedToken":"Business Unit ID","tokenTruncated":false},"61":{"coordinate":"R1C61","kind":"STRING","originalToken":"Job ID","decodedToken":"Job ID","tokenTruncated":false},"62":{"coordinate":"R1C62","kind":"STRING","originalToken":"Job Type","decodedToken":"Job Type","tokenTruncated":false},"63":{"coordinate":"R1C63","kind":"STRING","originalToken":"Booking Start","decodedToken":"Booking Start","tokenTruncated":false},"64":{"coordinate":"R1C64","kind":"STRING","originalToken":"Booking End","decodedToken":"Booking End","tokenTruncated":false},"65":{"coordinate":"R1C65","kind":"STRING","originalToken":"Invoice ID","decodedToken":"Invoice ID","tokenTruncated":false}},"classification":"HEADER"},{"source_row_ordinal":2,"bounded_raw_cells_json":{"1":{"coordinate":"R2C1","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"2":{"coordinate":"R2C2","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"3":{"coordinate":"R2C3","kind":"STRING","originalToken":"Contact Native Fixture Trust","decodedToken":"Contact Native Fixture Trust","tokenTruncated":false},"4":{"coordinate":"R2C4","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"5":{"coordinate":"R2C5","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"6":{"coordinate":"R2C6","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"7":{"coordinate":"R2C7","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"8":{"coordinate":"R2C8","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"9":{"coordinate":"R2C9","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"10":{"coordinate":"R2C10","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"11":{"coordinate":"R2C11","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"12":{"coordinate":"R2C12","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"13":{"coordinate":"R2C13","kind":"STRING","originalToken":"Contact Worker B","decodedToken":"Contact Worker B","tokenTruncated":false},"14":{"coordinate":"R2C14","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"15":{"coordinate":"R2C15","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"16":{"coordinate":"R2C16","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"17":{"coordinate":"R2C17","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"18":{"coordinate":"R2C18","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"19":{"coordinate":"R2C19","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"20":{"coordinate":"R2C20","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"21":{"coordinate":"R2C21","kind":"STRING","originalToken":"CONTACT-TEST-B","decodedToken":"CONTACT-TEST-B","tokenTruncated":false},"22":{"coordinate":"R2C22","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"23":{"coordinate":"R2C23","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"24":{"coordinate":"R2C24","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"25":{"coordinate":"R2C25","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"26":{"coordinate":"R2C26","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"27":{"coordinate":"R2C27","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"28":{"coordinate":"R2C28","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"29":{"coordinate":"R2C29","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"30":{"coordinate":"R2C30","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"31":{"coordinate":"R2C31","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"32":{"coordinate":"R2C32","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"33":{"coordinate":"R2C33","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"34":{"coordinate":"R2C34","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"35":{"coordinate":"R2C35","kind":"STRING","originalToken":"0","decodedToken":"0","tokenTruncated":false},"36":{"coordinate":"R2C36","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"37":{"coordinate":"R2C37","kind":"STRING","originalToken":"CONTACT-ZERO-B","decodedToken":"CONTACT-ZERO-B","tokenTruncated":false},"38":{"coordinate":"R2C38","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"39":{"coordinate":"R2C39","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"40":{"coordinate":"R2C40","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"41":{"coordinate":"R2C41","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"42":{"coordinate":"R2C42","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"43":{"coordinate":"R2C43","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"44":{"coordinate":"R2C44","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"45":{"coordinate":"R2C45","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"46":{"coordinate":"R2C46","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"47":{"coordinate":"R2C47","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"48":{"coordinate":"R2C48","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"49":{"coordinate":"R2C49","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"50":{"coordinate":"R2C50","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"51":{"coordinate":"R2C51","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"52":{"coordinate":"R2C52","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"53":{"coordinate":"R2C53","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"54":{"coordinate":"R2C54","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"55":{"coordinate":"R2C55","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"56":{"coordinate":"R2C56","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"57":{"coordinate":"R2C57","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"58":{"coordinate":"R2C58","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"59":{"coordinate":"R2C59","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"60":{"coordinate":"R2C60","kind":"STRING","originalToken":"f716c000-0000-4000-8000-000000000020","decodedToken":"f716c000-0000-4000-8000-000000000020","tokenTruncated":false},"61":{"coordinate":"R2C61","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"62":{"coordinate":"R2C62","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"63":{"coordinate":"R2C63","kind":"STRING","originalToken":"2026-10-06 09:00:00","decodedToken":"2026-10-06 09:00:00","tokenTruncated":false},"64":{"coordinate":"R2C64","kind":"STRING","originalToken":"2026-10-06 17:00:00","decodedToken":"2026-10-06 17:00:00","tokenTruncated":false},"65":{"coordinate":"R2C65","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false}},"classification":"ACCEPTED_SHIFT"},{"source_row_ordinal":3,"bounded_raw_cells_json":{"1":{"coordinate":"R3C1","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"2":{"coordinate":"R3C2","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"3":{"coordinate":"R3C3","kind":"STRING","originalToken":"Contact Native Fixture Trust","decodedToken":"Contact Native Fixture Trust","tokenTruncated":false},"4":{"coordinate":"R3C4","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"5":{"coordinate":"R3C5","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"6":{"coordinate":"R3C6","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"7":{"coordinate":"R3C7","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"8":{"coordinate":"R3C8","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"9":{"coordinate":"R3C9","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"10":{"coordinate":"R3C10","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"11":{"coordinate":"R3C11","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"12":{"coordinate":"R3C12","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"13":{"coordinate":"R3C13","kind":"STRING","originalToken":"Contact Worker C","decodedToken":"Contact Worker C","tokenTruncated":false},"14":{"coordinate":"R3C14","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"15":{"coordinate":"R3C15","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"16":{"coordinate":"R3C16","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"17":{"coordinate":"R3C17","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"18":{"coordinate":"R3C18","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"19":{"coordinate":"R3C19","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"20":{"coordinate":"R3C20","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"21":{"coordinate":"R3C21","kind":"STRING","originalToken":"CONTACT-TEST-C","decodedToken":"CONTACT-TEST-C","tokenTruncated":false},"22":{"coordinate":"R3C22","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"23":{"coordinate":"R3C23","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"24":{"coordinate":"R3C24","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"25":{"coordinate":"R3C25","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"26":{"coordinate":"R3C26","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"27":{"coordinate":"R3C27","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"28":{"coordinate":"R3C28","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"29":{"coordinate":"R3C29","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"30":{"coordinate":"R3C30","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"31":{"coordinate":"R3C31","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"32":{"coordinate":"R3C32","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"33":{"coordinate":"R3C33","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"34":{"coordinate":"R3C34","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"35":{"coordinate":"R3C35","kind":"STRING","originalToken":"0","decodedToken":"0","tokenTruncated":false},"36":{"coordinate":"R3C36","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"37":{"coordinate":"R3C37","kind":"STRING","originalToken":"CONTACT-ZERO-C","decodedToken":"CONTACT-ZERO-C","tokenTruncated":false},"38":{"coordinate":"R3C38","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"39":{"coordinate":"R3C39","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"40":{"coordinate":"R3C40","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"41":{"coordinate":"R3C41","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"42":{"coordinate":"R3C42","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"43":{"coordinate":"R3C43","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"44":{"coordinate":"R3C44","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"45":{"coordinate":"R3C45","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"46":{"coordinate":"R3C46","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"47":{"coordinate":"R3C47","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"48":{"coordinate":"R3C48","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"49":{"coordinate":"R3C49","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"50":{"coordinate":"R3C50","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"51":{"coordinate":"R3C51","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"52":{"coordinate":"R3C52","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"53":{"coordinate":"R3C53","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"54":{"coordinate":"R3C54","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"55":{"coordinate":"R3C55","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"56":{"coordinate":"R3C56","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"57":{"coordinate":"R3C57","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"58":{"coordinate":"R3C58","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"59":{"coordinate":"R3C59","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"60":{"coordinate":"R3C60","kind":"STRING","originalToken":"f716c000-0000-4000-8000-000000000020","decodedToken":"f716c000-0000-4000-8000-000000000020","tokenTruncated":false},"61":{"coordinate":"R3C61","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"62":{"coordinate":"R3C62","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false},"63":{"coordinate":"R3C63","kind":"STRING","originalToken":"2026-10-06 09:00:00","decodedToken":"2026-10-06 09:00:00","tokenTruncated":false},"64":{"coordinate":"R3C64","kind":"STRING","originalToken":"2026-10-06 17:00:00","decodedToken":"2026-10-06 17:00:00","tokenTruncated":false},"65":{"coordinate":"R3C65","kind":"BLANK","originalToken":"","decodedToken":"","tokenTruncated":false}},"classification":"ACCEPTED_SHIFT"}],"normalised_rows":[{"source_row_ordinal":2,"external_source_key":"CONTACT-ZERO-B","source_candidate_identity":"CONTACT-TEST-B","source_client_identity":"f716c000-0000-4000-8000-000000000020","work_date":"2026-10-06","start_at_local":"2026-10-06T09:00:00","end_at_local":"2026-10-06T17:00:00","break_minutes":480,"actual_net_minutes":0,"row_finalisation_state":"SOURCE_ABSENT_ZERO","finalised_by":null,"role_band_source":null,"source_commission_pence":null,"source_total_cost_pence":null,"source_shift_charge_pence":null,"source_money_parse_state":"NOT_APPLICABLE","source_qualification_profile_version":null,"source_expense_pence":"0","source_expense_parse_state":"OMITTED_ZERO","bounded_raw_columns_json":{"candidate":"Contact Worker B","candidate_uid":null,"candidate_id":"CONTACT-TEST-B","payroll_number":null,"business_unit_id":"f716c000-0000-4000-8000-000000000020","business_unit_name":"Contact Native Fixture Trust","booking_id":null,"timesheet_id":null,"booking_reference":null,"job_id":null,"job_type":null,"supply_type":null}},{"source_row_ordinal":3,"external_source_key":"CONTACT-ZERO-C","source_candidate_identity":"CONTACT-TEST-C","source_client_identity":"f716c000-0000-4000-8000-000000000020","work_date":"2026-10-06","start_at_local":"2026-10-06T09:00:00","end_at_local":"2026-10-06T17:00:00","break_minutes":480,"actual_net_minutes":0,"row_finalisation_state":"SOURCE_ABSENT_ZERO","finalised_by":null,"role_band_source":null,"source_commission_pence":null,"source_total_cost_pence":null,"source_shift_charge_pence":null,"source_money_parse_state":"NOT_APPLICABLE","source_qualification_profile_version":null,"source_expense_pence":"0","source_expense_parse_state":"OMITTED_ZERO","bounded_raw_columns_json":{"candidate":"Contact Worker C","candidate_uid":null,"candidate_id":"CONTACT-TEST-C","payroll_number":null,"business_unit_id":"f716c000-0000-4000-8000-000000000020","business_unit_name":"Contact Native Fixture Trust","booking_id":null,"timesheet_id":null,"booking_reference":null,"job_id":null,"job_type":null,"supply_type":null}}],"money_evidence":[],"expense_evidence":[{"source_row_ordinal":2,"source_column_index":34,"cell_coordinate":"R2C34","source_kind":"CSV_DECODED_TEXT","original_token":"","decoded_token":"","cell_type_marker":"BLANK","formula_present":false,"parse_state":"OMITTED_ZERO","parsed_pence":"0","source_file_sha256":"d57f8453ba968e147f686c8c416b9fbb2f67a41133bcd793e467737f7288b345","lexical_profile_version":"SOURCE_FIXED_EXPENSE_PENCE_V1"},{"source_row_ordinal":3,"source_column_index":34,"cell_coordinate":"R3C34","source_kind":"CSV_DECODED_TEXT","original_token":"","decoded_token":"","cell_type_marker":"BLANK","formula_present":false,"parse_state":"OMITTED_ZERO","parsed_pence":"0","source_file_sha256":"d57f8453ba968e147f686c8c416b9fbb2f67a41133bcd793e467737f7288b345","lexical_profile_version":"SOURCE_FIXED_EXPENSE_PENCE_V1"}]},"expected_context":{"profile_id":"ROSTER_WEEKLY_SUMMARY_ACTUAL_V1","authority_scope_kind":"CYCLE","rows":[{"source_row_ordinal":2,"external_source_key":"CONTACT-ZERO-B","work_date":"2026-10-06","start_at_local":"2026-10-06T09:00:00","end_at_local":"2026-10-06T17:00:00","break_minutes":480,"actual_net_minutes":0,"row_finalisation_state":"SOURCE_ABSENT_ZERO","candidate_match_count":1,"candidate_id":"f716c000-0000-4000-8000-000000000011","client_id":"f716c000-0000-4000-8000-000000000020","prior_work_event_id":null,"prior_accepted_contract_id":null,"protected_matches":[],"contracts":[{"contract_id":"f716c000-0000-4000-8000-000000000031","candidate_id":"f716c000-0000-4000-8000-000000000011","client_id":"f716c000-0000-4000-8000-000000000020","valid_from":"2026-08-12","valid_to":"2026-12-10","weekly_source_applicable":true,"schedule_compatible":true,"verified_role_band_match":false,"effective_policy":{"authority_mode":"SOURCE_AUTHORITY","c1_source_mode":"HEALTHROSTER_WEEKLY"}}]},{"source_row_ordinal":3,"external_source_key":"CONTACT-ZERO-C","work_date":"2026-10-06","start_at_local":"2026-10-06T09:00:00","end_at_local":"2026-10-06T17:00:00","break_minutes":480,"actual_net_minutes":0,"row_finalisation_state":"SOURCE_ABSENT_ZERO","candidate_match_count":1,"candidate_id":"f716c000-0000-4000-8000-000000000012","client_id":"f716c000-0000-4000-8000-000000000020","prior_work_event_id":null,"prior_accepted_contract_id":null,"protected_matches":[],"contracts":[{"contract_id":"f716c000-0000-4000-8000-000000000032","candidate_id":"f716c000-0000-4000-8000-000000000012","client_id":"f716c000-0000-4000-8000-000000000020","valid_from":"2026-08-12","valid_to":"2026-12-10","weekly_source_applicable":true,"schedule_compatible":true,"verified_role_band_match":false,"effective_policy":{"authority_mode":"SOURCE_AUTHORITY","c1_source_mode":"HEALTHROSTER_WEEKLY"}}]}]},"projection_templates":[{"candidate_id":"f716c000-0000-4000-8000-000000000011","client_id":"f716c000-0000-4000-8000-000000000020","prior_work_event_id":null,"qualifying_contract_ids":["f716c000-0000-4000-8000-000000000031"],"qualification_observations":[],"mapping_state":"RESOLVED","contract_id":"f716c000-0000-4000-8000-000000000031","contract_selection_method":"AUTO_UNIQUE","identity_kind":"PROFILE_EXTERNAL_KEY","profile_external_key":"CONTACT-ZERO-B","link_kind":"ZERO_SOURCE","source_row_ordinal":2},{"candidate_id":"f716c000-0000-4000-8000-000000000012","client_id":"f716c000-0000-4000-8000-000000000020","prior_work_event_id":null,"qualifying_contract_ids":["f716c000-0000-4000-8000-000000000032"],"qualification_observations":[],"mapping_state":"RESOLVED","contract_id":"f716c000-0000-4000-8000-000000000032","contract_selection_method":"AUTO_UNIQUE","identity_kind":"PROFILE_EXTERNAL_KEY","profile_external_key":"CONTACT-ZERO-C","link_kind":"ZERO_SOURCE","source_row_ordinal":3}]}'::jsonb);

-- CONTACT_FIXTURE_COMPONENT seed/11_observe-native-inserts.sql SHA256 407f5ba20ec4d4e5a3600c0086f50cba2eaecc70254782c1d9413ea16f6c75db
-- Observation only. No native result/authority/guard is replaced.
-- Outer server rollback owns these AFTER INSERT observers and all fixture data.
CREATE TEMP TABLE contact_seed_observers(relation_name text PRIMARY KEY,trigger_name text NOT NULL) ON COMMIT DROP;
CREATE TEMP TABLE contact_seed_receipts(name text PRIMARY KEY,value jsonb NOT NULL) ON COMMIT DROP;
CREATE OR REPLACE FUNCTION pg_temp.contact_seed_capture_insert_v1()
RETURNS trigger LANGUAGE plpgsql AS $capture$
DECLARE key text;v jsonb:=to_jsonb(NEW);
BEGIN
 FOREACH key IN ARRAY TG_ARGV LOOP
  IF v->>key IS NOT NULL THEN
   INSERT INTO pg_temp.contact_start_fixture_absence_ids VALUES(TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME,key,(v->>key)::uuid)
    ON CONFLICT DO NOTHING;
  END IF;
 END LOOP;
 RETURN NEW;
END;
$capture$;
DO $observers$
DECLARE r record;v_args text;v_count integer:=0;v_sig text;
BEGIN
 PERFORM pg_temp.contact_assert_v1(current_database() IN('cloudtms_test_clone'),'seed database');
 PERFORM set_config('request.jwt.claim.role','service_role',true);
 PERFORM pg_temp.contact_assert_v1((SELECT count(*)=1 FROM pg_temp.contact_native_seed_artifact)
  AND (SELECT value->>'as_of'=(clock_timestamp() AT TIME ZONE 'Europe/London')::date::text FROM pg_temp.contact_native_seed_artifact),
  'artifact date differs from actual London day; regenerate through real offline producer');
 FOREACH v_sig IN ARRAY ARRAY[
  'public._audit_insert(text,text,text,jsonb,jsonb,text,uuid)',
  'public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamp with time zone)',
  'public.candidate_component_prepare_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamp with time zone)',
  'public.weekly_source_candidate_self_submit_atomic_v1(uuid,text,jsonb,timestamp with time zone)',
  'private._weekly_source_settings_ensure_open_cycle_v1(uuid,timestamp with time zone)',
  'public.weekly_source_upload_context_v1(jsonb)',
  'public.weekly_source_upload_stage_begin_atomic_v1(jsonb)',
  'public.weekly_source_upload_stage_rows_atomic_v1(jsonb)',
  'public.weekly_source_upload_seal_atomic_v1(jsonb)',
  'public.weekly_source_projection_begin_atomic_v1(jsonb)',
  'public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb)',
  'public.weekly_source_projection_publish_atomic_v1(jsonb)',
  'public.weekly_source_query_ask_candidate_atomic_v1(jsonb)',
  'public.weekly_source_combined_review_workspace_v1(jsonb)',
  'private.weekly_contact_office_rows_v1(jsonb,jsonb,uuid)',
  'public.weekly_source_query_send_manager_now_atomic_v1(jsonb)',
  'public.weekly_source_global_settings_get_v1(jsonb)',
  'public.weekly_source_global_settings_save_atomic_v1(jsonb)'
 ] LOOP
  PERFORM pg_temp.contact_assert_v1(to_regprocedure(v_sig) IS NOT NULL,'missing genuine prerequisite '||v_sig);
 END LOOP;
 -- All UUID identity/unique columns in these bounded native domains are captured,
 -- including real audit/mutation receipts and children created by native triggers.
 -- No table scan, xmin guess, secret read, history rewrite or outcome injection.
 FOR r IN
  SELECT n.nspname,c.relname,array_agg(DISTINCT a.attname ORDER BY a.attname) columns
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  JOIN pg_constraint k ON k.conrelid=c.oid AND k.contype IN('p','u')
  JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=ANY(k.conkey) AND a.atttypid='uuid'::regtype
  WHERE c.relkind='r' AND n.nspname IN('public','private')
   AND(c.relname~'^(candidate_|weekly_|timesheet|contract_|summary_)'
    OR(n.nspname='public' AND c.relname IN('tms_users','clients','client_settings','candidates','contracts','audit_events')))
  GROUP BY n.nspname,c.relname ORDER BY n.nspname,c.relname
 LOOP
  v_count:=v_count+1;
  IF v_count>250 THEN RAISE EXCEPTION 'CONTACT_SEED_OBSERVER_DOMAIN_EXCEEDED';END IF;
  PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM pg_trigger t
   WHERE t.tgrelid=format('%I.%I',r.nspname,r.relname)::regclass AND t.tgname='contact_seed_insert_observer'),
   'observer name already exists');
  SELECT string_agg(quote_literal(x),',') INTO v_args FROM unnest(r.columns) x;
  EXECUTE format('CREATE TRIGGER contact_seed_insert_observer AFTER INSERT ON %I.%I FOR EACH ROW EXECUTE FUNCTION pg_temp.contact_seed_capture_insert_v1(%s)',r.nspname,r.relname,v_args);
  INSERT INTO pg_temp.contact_seed_observers VALUES(r.nspname||'.'||r.relname,'contact_seed_insert_observer');
 END LOOP;
 PERFORM pg_temp.contact_assert_v1(v_count BETWEEN 20 AND 250,'genuine native schema inventory required');
 INSERT INTO pg_temp.contact_seed_receipts VALUES('observer_catalog',jsonb_build_object('relations',v_count,'mode','AFTER_INSERT_UUID_OBSERVATION_ONLY'));
END;
$observers$;

-- CONTACT_FIXTURE_COMPONENT seed/12_declare-native-weeks.sql SHA256 6aba02d59f175d42f5d79ad533e37a3f7f058b7c8dd058ee74b3f16fe5a0f85f
-- Honest TEST base configuration/accounts/planned weeks, then actual native
-- Candidate workflow -> component prepare/complete -> SELF_SUBMIT constructors.
-- A PNG spelling TEST is local synthetic evidence; no R2/provider proof is claimed.
DO $declare$
DECLARE a jsonb;v_now timestamptz:=transaction_timestamp();v_week date;v_date date;i integer;
 v_candidate uuid;v_contract uuid;v_account uuid;v_session uuid;v_workflow uuid;v_cw uuid;v_component uuid;
 v_create jsonb;v_prepare jsonb;v_complete jsonb;v_submit jsonb;v_policy jsonb;v_days jsonb;v_cycle uuid;
 v_actor uuid:='f716c000-0000-4000-8000-000000000003';v_get jsonb;v_saved jsonb;
BEGIN
 SELECT value INTO STRICT a FROM pg_temp.contact_native_seed_artifact;
 v_week:=(a->>'week_ending')::date;v_date:=(a->>'work_date')::date;
 PERFORM pg_temp.contact_assert_v1(v_date<(clock_timestamp() AT TIME ZONE 'Europe/London')::date,'worked day must be complete');
 PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.settings_defaults WHERE id=1
  AND candidate_manager_email_templates_sha256 IS NOT NULL AND candidate_home_announcement_sha256 IS NOT NULL),
  'actual baseline settings/template authorities required; no fake template digest fallback');
 INSERT INTO pg_temp.contact_seed_receipts VALUES('before_shared_settings',jsonb_build_object(
  'settings_defaults_sha256',(SELECT encode(extensions.digest(convert_to(jsonb_build_object('candidate_app_feature_flags_json',x.candidate_app_feature_flags_json,'updated_at',to_jsonb(x)->'updated_at')::text,'UTF8'),'sha256'),'hex') FROM public.settings_defaults x WHERE id=1),
  'global_contact_settings_sha256',(SELECT encode(extensions.digest(convert_to((to_jsonb(x)-ARRAY['automatic_reconcile_actor_user_id','manager_first_delay_value','manager_first_delay_unit','manager_followups','manager_recent_completed_weeks','manager_max_work_age_days'])::text,'UTF8'),'sha256'),'hex') FROM public.weekly_source_global_settings x WHERE singleton)));
 UPDATE public.settings_defaults SET candidate_app_feature_flags_json=coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
  ||jsonb_build_object('candidate_app_reads',true,'candidate_app_writes',true) WHERE id=1;
 INSERT INTO public.tms_users(id,email,role,password_hash,display_name,is_active) VALUES
  (v_actor,'contact-native-office@example.invalid','admin','SYNTHETIC-NO-LOGIN','Contact native fixture admin',true);
 INSERT INTO public.clients(id,name,ts_queries_email) VALUES
  ('f716c000-0000-4000-8000-000000000020','Contact Native Fixture Trust','contact-manager@example.invalid');
 INSERT INTO public.client_settings(id,client_id,effective_from,default_submission_mode,week_ending_weekday,
  hr_validation_required,autoprocess_hr,self_bill_no_invoices_sent,no_timesheet_required,requires_hr,is_nhsp)
 VALUES('f716c000-0000-4000-8000-000000000021','f716c000-0000-4000-8000-000000000020',(a->>'contract_start')::date,
  'ELECTRONIC',0,true,true,true,true,false,false);
 INSERT INTO public.weekly_source_groups(id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time)
 VALUES('f716c000-0000-4000-8000-000000000001','TEST','f7160000-0000-4000-8000-000000000019',
  'CONTACT_START_NATIVE_FIXTURE','Contact native fixture Roster','ROSTER',extract(dow FROM v_now AT TIME ZONE 'Europe/London')::integer,'23:59');
 INSERT INTO public.weekly_source_group_clients(source_group_id,client_id,valid_from,created_by_user_id)
 VALUES('f716c000-0000-4000-8000-000000000001','f716c000-0000-4000-8000-000000000020',(a->>'contract_start')::date,v_actor);
 INSERT INTO public.weekly_source_client_policies(source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,candidate_queries_enabled,manager_queries_enabled,manager_query_recipient,created_by_user_id)
 VALUES('f716c000-0000-4000-8000-000000000001','f716c000-0000-4000-8000-000000000020',(a->>'contract_start')::date,
  'SOURCE_AUTHORITY','CHECK_ONLY',true,true,true,'contact-manager@example.invalid',v_actor);
 -- Actual native global CAS/receipt, scoped to this rollback. Preserve all other
 -- timing choices and use a real 60 minute worker question window, no clock edits.
 v_get:=public.weekly_source_global_settings_get_v1(jsonb_build_object('actor_user_id',v_actor,
  'agency_id','f7160000-0000-4000-8000-000000000019','environment','TEST'));
 v_saved:=public.weekly_source_global_settings_save_atomic_v1(jsonb_build_object('actor_user_id',v_actor,
  'agency_id','f7160000-0000-4000-8000-000000000019','environment','TEST','request_id',gen_random_uuid(),
  'expected_settings_version',v_get->'settings_version','settings',v_get->'settings'||jsonb_build_object(
   'candidate_reminder_minutes',10,'candidate_response_deadline_minutes',60,'automatic_reconcile_actor_user_id',v_actor)));
 INSERT INTO pg_temp.contact_seed_receipts VALUES('native_global_settings',v_saved);
 v_cycle:=private._weekly_source_settings_ensure_open_cycle_v1('f716c000-0000-4000-8000-000000000001',v_now);
 PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.weekly_source_cycles WHERE id=v_cycle
  AND finalisation_week_ending=v_week AND state='OPEN' AND projection_state='NONE'
  AND current_complete_upload_id IS NULL AND cutoff_at_utc>clock_timestamp()+interval '5 minutes'),
  'real open-cycle owner must allocate expected week with future cutoff');
 INSERT INTO pg_temp.contact_seed_receipts VALUES('native_cycle',jsonb_build_object('source_cycle_id',v_cycle));
 SELECT jsonb_agg(to_char(d,'YYYY-MM-DD') ORDER BY d) INTO v_days
 FROM generate_series((a->>'week_start')::date::timestamp,v_week::timestamp,interval '1 day') d WHERE d::date<>v_date;
 FOR i IN 0..2 LOOP
  v_candidate:=('f716c000-0000-4000-8000-'||lpad((10+i)::text,12,'0'))::uuid;
  v_contract:=('f716c000-0000-4000-8000-'||lpad((30+i)::text,12,'0'))::uuid;
  v_account:=('f716c000-0000-4000-8000-'||lpad((40+i)::text,12,'0'))::uuid;
  v_session:=('f716c000-0000-4000-8000-'||lpad((50+i)::text,12,'0'))::uuid;
  v_workflow:=('f716c000-0000-4000-8000-'||lpad((70+i)::text,12,'0'))::uuid;
  v_cw:=('f716c000-0000-4000-8000-'||lpad((90+i)::text,12,'0'))::uuid;
  INSERT INTO public.candidates(id,tms_ref,first_name,last_name,display_name,email,active,key_norm,opt_in_email,nhsp_hr_name_aliases)
  VALUES(v_candidate,'CONTACT-TEST-'||chr(65+i),'Contact Worker',chr(65+i),'Contact Worker '||chr(65+i),
   'contact-worker-'||lower(chr(65+i))||'@example.invalid',true,'CONTACT-NATIVE-'||chr(65+i),true,jsonb_build_array('Contact Worker '||chr(65+i)));
  INSERT INTO public.contracts(id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
   week_ending_weekday_snapshot,default_submission_mode,role)
  VALUES(v_contract,v_candidate,'f716c000-0000-4000-8000-000000000020',(a->>'contract_start')::date,(a->>'contract_end')::date,
   'PAYE','{}'::jsonb,0,'ELECTRONIC','RMN');
  -- Planned/open Contract Week is a synthetic scheduling fact, as in the actual
  -- mandatory SELF_SUBMIT verifier. No Timesheet, current owner, approval or finance is seeded.
  INSERT INTO public.contract_weeks(id,contract_id,week_ending_date,status,submission_mode_snapshot,day_entries_json,totals_json)
  VALUES(v_cw,v_contract,v_week,'OPEN','ELECTRONIC','[]'::jsonb,'{}'::jsonb);
  INSERT INTO public.candidate_app_accounts(id,environment,email_normalized,status,password_scheme,password_scheme_version,
   password_salt,password_digest,password_changed_at_utc)
  VALUES(v_account,'TEST','contact-worker-'||lower(chr(65+i))||'@example.invalid','ACTIVE','PBKDF2-HMAC-SHA256',1,
   extensions.digest(convert_to('SYNTHETIC-NO-LOGIN-SALT-'||i,'UTF8'),'sha256'),
   extensions.digest(convert_to('SYNTHETIC-NO-LOGIN-DIGEST-'||i,'UTF8'),'sha256'),v_now);
  INSERT INTO public.candidate_app_sessions(id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
   expires_at_utc,absolute_expires_at_utc)
  VALUES(v_session,v_account,'TEST',v_candidate,'ACTIVE',extensions.digest(convert_to('SYNTHETIC-NO-TOKEN-'||i,'UTF8'),'sha256'),
   v_now+interval '2 hours',v_now+interval '3 hours');
  INSERT INTO public.candidate_app_global_membership_links(membership_id,global_account_identity_hmac,account_id,candidate_id,
   candidate_code,membership_generation,state,linked_at_utc,updated_at_utc)
  VALUES(('f716c000-0000-4000-8000-'||lpad((60+i)::text,12,'0'))::uuid,
   extensions.digest(convert_to('SYNTHETIC-NO-CONTROL-PLANE-'||i,'UTF8'),'sha256'),v_account,v_candidate,'CONTACT-TEST-'||chr(65+i),1,'ACTIVE',v_now,v_now);
  v_policy:=private._weekly_source_effective_policy_v1('f716c000-0000-4000-8000-000000000020',v_contract,v_date);
  PERFORM pg_temp.contact_assert_v1(v_policy->>'authority_mode'='SOURCE_AUTHORITY' AND v_policy->>'document_mode'='CHECK_ONLY',
   'native effective source policy required');
  v_create:=public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_workflow,'CREATE',NULL,
   jsonb_build_object('workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','ELECTRONIC','contract_id',v_contract,
    'contract_week_id',v_cw,'week_ending_date',v_week,'input_snapshot','{}'::jsonb),'contact-native-create-'||i,v_now);
  PERFORM pg_temp.contact_assert_v1(v_create->>'state'='WORKER_DRAFT' AND v_create->>'generation'='1','native draft CREATE');
  v_prepare:=public.candidate_component_prepare_atomic_v1(v_session,'TEST',v_workflow,1,
   jsonb_build_object('component_kind','CANDIDATE_SIGNATURE','document_role','CANDIDATE_SIGNATURE','media_type','image/png',
    'byte_size',a->'signature'->'byte_count','storage_key',a->'signature'->>'storage_key'),'contact-native-component-'||i,v_now);
  v_component:=(v_prepare->>'component_id')::uuid;
  PERFORM pg_temp.contact_assert_v1(v_component IS NOT NULL AND v_prepare->>'state'='PENDING','native signature PREPARE');
  v_complete:=public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',1,
   jsonb_build_object('component_id',v_component,'source_content_sha256_hex',a->'signature'->>'sha256',
    'verified_byte_size',a->'signature'->'byte_count','verified_media_type','image/png',
    'verified_image_width',a->'signature'->'width','verified_image_height',a->'signature'->'height'),'contact-native-complete-'||i,v_now);
  PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.candidate_submission_components WHERE id=v_component
   AND state='IMMUTABLE' AND encode(source_content_sha256,'hex')=a->'signature'->>'sha256'), 'native COMPLETE immutable fixture bytes');
  v_submit:=public.weekly_source_candidate_self_submit_atomic_v1(v_session,'TEST',jsonb_build_object('workflow_id',v_workflow,
   'expected_workflow_generation',1,'candidate_signature_component_id',v_component,'candidate_signed_at_utc',v_now,
   'immutable_submission',jsonb_build_object('actual_schedule_json',jsonb_build_array(jsonb_build_object('row_key','contact-fixture-'||i,
    'date',v_date,'start','09:00','end','17:00','break_minutes',30)),'day_off_dates',v_days),
   'idempotency_key',gen_random_uuid()),v_now);
  PERFORM pg_temp.contact_assert_v1(v_submit->>'request_kind'='SELF_SUBMIT' AND v_submit->>'generation'='2'
   AND v_submit->>'timesheet_id' IS NOT NULL,'real signed whole-week declaration');
  PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.timesheets t WHERE t.timesheet_id=(v_submit->>'timesheet_id')::uuid
   AND t.contract_id=v_contract AND t.candidate_workflow_id=v_workflow AND t.authorised_at_server IS NULL
   AND t.r2_nurse_key=a->'signature'->>'storage_key' AND t.img_sha256_nurse=a->'signature'->>'sha256'
   AND t.is_current AND t.archived_at_utc IS NULL AND t.revoked_at IS NULL)
   AND NOT EXISTS(SELECT 1 FROM public.timesheets_financials WHERE timesheet_id=(v_submit->>'timesheet_id')::uuid),
   'actual declaration must be unapproved, signed and nonfinancial');
  INSERT INTO pg_temp.contact_seed_receipts VALUES('declaration_'||chr(65+i),jsonb_build_object(
   'create',v_create,'prepare',v_prepare-'storage_key','complete',v_complete,'self_submit',v_submit));
 END LOOP;
END;
$declare$;

-- CONTACT_FIXTURE_COMPONENT seed/13_native-publication.sql SHA256 0b1f2a4a1a2ab9cb3f36692aa7cf9edb1eb1eee3c379525498800224407d4df0
-- Native complete upload/seal/projection/current publication. No manual CURRENT,
-- comparison, incident, WorkEvent, generation, intent or audit INSERT is allowed here.
DO $publish$
DECLARE a jsonb;v_cycle uuid;v_actor uuid:='f716c000-0000-4000-8000-000000000003';
 v_begin jsonb;v_stage jsonb;v_seal jsonb;v_build jsonb;v_context jsonb;v_apply jsonb;v_publish jsonb;
 v_row jsonb;v_expected jsonb;v_contract jsonb;v_ec jsonb;v_template jsonb;v_rows jsonb:='[]'::jsonb;
 v_publication uuid;v_upload uuid;v_audit jsonb;v_candidate_ids uuid[];
BEGIN
 SELECT value INTO STRICT a FROM pg_temp.contact_native_seed_artifact;
 SELECT (value->>'source_cycle_id')::uuid INTO STRICT v_cycle FROM pg_temp.contact_seed_receipts WHERE name='native_cycle';
 v_begin:=public.weekly_source_upload_stage_begin_atomic_v1(a->'begin_request'||jsonb_build_object('source_cycle_id',v_cycle));
 PERFORM pg_temp.contact_assert_v1(v_begin->>'status'='STAGING','real upload BEGIN');
 v_upload:=(v_begin->>'logical_upload_id')::uuid;
 v_stage:=public.weekly_source_upload_stage_rows_atomic_v1(a->'stage_request'||jsonb_build_object('actor_user_id',v_actor,'upload_id',v_upload));
 v_seal:=public.weekly_source_upload_seal_atomic_v1(jsonb_build_object('actor_user_id',v_actor,'upload_id',v_upload));
 PERFORM pg_temp.contact_assert_v1(v_seal->>'status'='CURRENT' AND v_seal->>'row_manifest_hash'~'^[0-9a-f]{64}$','real immutable complete seal');
 v_build:=public.weekly_source_projection_begin_atomic_v1(jsonb_build_object('actor_user_id',v_actor,'upload_id',v_upload,
  'expected_authority_scope_version',v_seal->'authority_scope_version'));
 PERFORM pg_temp.contact_assert_v1(v_build->>'status'='BUILDING','native BUILDING allocation');
 v_publication:=(v_build->>'publication_id')::uuid;
 v_context:=public.weekly_source_upload_context_v1(jsonb_build_object('operation','BUILD_PROJECTION','actor_user_id',v_actor,
  'source_group_id','f716c000-0000-4000-8000-000000000001','source_cycle_id',v_cycle,
  'client_id','f716c000-0000-4000-8000-000000000020','upload_id',v_upload));
 PERFORM pg_temp.contact_assert_v1(v_context->>'profile_id'=a->'report'->>'profile_id'
  AND v_context->>'authority_scope_kind'='CYCLE' AND jsonb_array_length(v_context->'rows')=2,'real native zero context census');
 FOR v_row IN SELECT value FROM jsonb_array_elements(v_context->'rows') LOOP
  SELECT value INTO STRICT v_expected FROM jsonb_array_elements(a->'expected_context'->'rows')
   WHERE value->'source_row_ordinal'=v_row->'source_row_ordinal';
  -- Every fact the existing zero-row projection producer reads is compared
  -- with the actual native context. Hash and allocated row ID are bound only now.
  PERFORM pg_temp.contact_assert_v1(v_row->'candidate_match_count'=v_expected->'candidate_match_count'
   AND v_row->'candidate_id'=v_expected->'candidate_id' AND v_row->'client_id'=v_expected->'client_id'
   AND v_row->'external_source_key'=v_expected->'external_source_key' AND v_row->'work_date'=v_expected->'work_date'
   AND v_row->>'row_finalisation_state'='SOURCE_ABSENT_ZERO' AND (v_row->>'actual_net_minutes')::integer=0
   AND v_row->'start_at_local'=v_expected->'start_at_local' AND v_row->'end_at_local'=v_expected->'end_at_local'
   AND v_row->'break_minutes'=v_expected->'break_minutes'
   AND v_row->>'prior_work_event_id' IS NULL AND v_row->>'prior_accepted_contract_id' IS NULL
   AND v_row->>'office_selected_contract_id' IS NULL AND v_row->>'office_selected_work_event_id' IS NULL
   AND NOT coalesce((v_row->>'office_separate_shift')::boolean,false)
   AND NOT coalesce((v_row->>'prior_work_event_ambiguous')::boolean,false)
   AND v_row->'protected_matches'='[]'::jsonb AND jsonb_array_length(v_row->'contracts')=1,
   'actual native context differs from the zero-only expected fixture inputs');
  v_contract:=v_row->'contracts'->0;v_ec:=v_expected->'contracts'->0;
  PERFORM pg_temp.contact_assert_v1(v_contract->'contract_id'=v_ec->'contract_id'
   AND v_contract->'candidate_id'=v_ec->'candidate_id' AND v_contract->'client_id'=v_ec->'client_id'
   AND v_contract->'valid_from'=v_ec->'valid_from' AND v_contract->'valid_to'=v_ec->'valid_to'
   AND v_contract->'weekly_source_applicable'='true'::jsonb AND v_contract->'schedule_compatible'='true'::jsonb
   AND coalesce((v_contract->>'verified_role_band_match')::boolean,false)=false
   AND v_contract->'effective_policy'->>'authority_mode'='SOURCE_AUTHORITY'
   AND v_contract->'effective_policy'->>'c1_source_mode'='HEALTHROSTER_WEEKLY'
   AND v_contract->'effective_policy'->>'policy_sha256'~'^[0-9a-f]{64}$',
   'actual contract applicability/policy differs from existing projection producer inputs');
  SELECT value INTO STRICT v_template FROM jsonb_array_elements(a->'projection_templates')
   WHERE value->'source_row_ordinal'=v_row->'source_row_ordinal';
  v_rows:=v_rows||jsonb_build_array((v_template-'source_row_ordinal')||jsonb_build_object('upload_row_id',v_row->'upload_row_id',
   'effective_policy_fingerprint',v_contract->'effective_policy'->'policy_sha256'));
 END LOOP;
 v_apply:=public.weekly_source_projection_rows_apply_atomic_v1(v_actor,v_publication,v_rows);
 PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.weekly_source_row_resolutions r
  JOIN public.weekly_source_upload_rows u ON u.id=r.upload_row_id WHERE u.upload_id=v_upload
   AND(r.mapping_state<>'RESOLVED' OR r.paid_minutes<>0 OR r.rate_classifications_json IS NOT NULL)),'native mapping must remain resolved/nonfinancial');
 v_publish:=public.weekly_source_projection_publish_atomic_v1(jsonb_build_object('actor_user_id',v_actor,'publication_id',v_publication));
 PERFORM pg_temp.contact_assert_v1(v_publish->>'status'='CURRENT' AND v_publish->>'publication_fingerprint'~'^[0-9a-f]{64}$','real native CURRENT publication');
 PERFORM private.weekly_source_query_current_publication_v1(v_cycle,v_publication);
 SELECT jsonb_agg(jsonb_build_object('timesheet_id',after_json->>'timesheet_id','state',after_json->>'state',
  'refusal_code',after_json->>'refusal_code') ORDER BY after_json->>'timesheet_id') INTO v_audit
 FROM public.audit_events WHERE object_type='timesheets' AND action='WEEKLY_SOURCE_PUBLICATION_COMPARISON'
  AND after_json->>'projection_publication_id'=v_publication::text AND event_sequence_is_authoritative;
 PERFORM pg_temp.contact_assert_v1(jsonb_array_length(v_audit)=3 AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_audit) x
  WHERE x->>'state'<>'COMPARED' OR x->>'refusal_code' IS NOT NULL), 'all three genuine signed declarations must be COMPARED: '||coalesce(v_audit::text,'NO_NATIVE_AUDIT'));
 PERFORM pg_temp.contact_assert_v1((SELECT count(*)=3 FROM public.weekly_discrepancy_incidents i
  JOIN public.weekly_issue_comparison_revisions c ON c.id=i.current_comparison_revision_id
  WHERE i.source_cycle_id=v_cycle AND i.state='OPEN' AND c.projection_publication_id=v_publication
   AND c.candidate_timesheet_id IS NOT NULL AND(
    (i.candidate_id='f716c000-0000-4000-8000-000000000010' AND c.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED')
    OR(i.candidate_id IN('f716c000-0000-4000-8000-000000000011','f716c000-0000-4000-8000-000000000012')
     AND c.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED'))),
  'genuine missing-A/zero-B/C comparison topology required');
 INSERT INTO pg_temp.contact_seed_receipts VALUES('native_upload_begin',v_begin),('native_upload_stage',v_stage),
  ('native_upload_seal',v_seal),('native_projection_begin',v_build),('native_projection_apply',v_apply),
  ('native_projection_publish',v_publish),('native_comparison_audit',v_audit);
END;
$publish$;

-- CONTACT_FIXTURE_COMPONENT seed/14_native-generations.sql SHA256 bc61289a2b1a7fb59cf7e3f23599df34c14018705a6165f57ee0ab3c7795eff1
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
  'scope_key',row->'scope_key','contact_controls',row->'contact_controls') ORDER BY row->>'candidate_id') INTO v_projection FROM jsonb_array_elements(v_review->'rows') row
 WHERE row->>'candidate_id' IN('f716c000-0000-4000-8000-000000000010','f716c000-0000-4000-8000-000000000011','f716c000-0000-4000-8000-000000000012')
  AND nullif(row->>'scope_key','') IS NOT NULL AND EXISTS(SELECT 1 FROM jsonb_array_elements(v_review->'owners') owner
   WHERE owner->>'key'=row->>'scope_key' AND owner->'scope'->>'source_group_id'='f716c000-0000-4000-8000-000000000001'
    AND owner->'scope'->>'source_cycle_id'=v_cycle::text);
 PERFORM pg_temp.contact_assert_v1(jsonb_array_length(v_projection)=3,'genuine combined query rows must expose native owner scope_key');
 PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_projection) x
  WHERE x->'contact_controls' IS DISTINCT FROM jsonb_build_object('version','CONTACT_CONTROLS_ROW_V1',
   'candidate_id',x->'candidate_id','client_id',x->'client_id','scope_revision','0',
   'worker',jsonb_build_object('paused',false,'until_local',NULL),
   'manager',jsonb_build_object('paused',false,'until_local',NULL),
   'actions',jsonb_build_array(jsonb_build_object('operation','PAUSE','label','Pause messages'))))
  AND NOT EXISTS(SELECT 1 FROM public.weekly_contact_pause_scopes WHERE candidate_id::text LIKE 'f716c000-%'),
  '0319 must return exact scoped initial contact actions for all A/B/C without writing scope rows');
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

-- CONTACT_FIXTURE_COMPONENT 02_construct-native-targets.sql SHA256 a16cf5139c5c112e48e2b9f003fa5c50de6fec4e4917f3cd7ffddbe68f73f035
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
 PERFORM pg_temp.contact_assert_v1(current_database() IN('cloudtms_test_clone'),'wrong database');
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

-- CONTACT_FIXTURE_COMPONENT 03_assertion-helpers.sql SHA256 5e13a922ba6839a15af283591025085b2dfbd1426813a475526b4bbe46b411ae
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

-- CONTACT_FIXTURE_COMPONENT 04_native-cases.sql SHA256 e03eeaf317df27acdd5d742b09ab4e44291a89560202f857123f142caa9d4d1d
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

-- CONTACT_FIXTURE_COMPONENT 05_fixture-qt-bootstrap.sql SHA256 6eb97b2a4a48c12c7c34aa12de628c673032abd7556b0454b3366699a9fde77c
-- Synthetic rollback-only deployment authority. This is NOT a deployment seed.
-- The root must have mounted the real empty schema; do not replace an installed
-- authority or delete/relabel its row to fit this fixture.
DO $bootstrap$
DECLARE v_policy jsonb:=pg_temp.contact_policy_v1('ALLOWED');
BEGIN
 PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.candidate_push_timing_policy)
  AND NOT EXISTS(SELECT 1 FROM public.candidate_push_timing_commands),'fixture QT authority must start vacant');
 INSERT INTO public.candidate_push_timing_policy(environment,agency_id,revision,policy_json,policy_sha256,updated_at_utc)
 VALUES('TEST','f7160000-0000-4000-8000-000000000019',1,v_policy,
  private.weekly_source_sha256_jsonb_v1('PUSH_TIMING_POLICY_V1',v_policy),clock_timestamp());
END;
$bootstrap$;

-- CONTACT_FIXTURE_COMPONENT 06_run-native-cases.sql SHA256 2bea4ce96ad78e70831aa4f46471a9cb9651c538254357d21e23a62e6e74502c
-- Phase B: after the genuine prefix/constructors and temporary QT bootstrap.
SELECT pg_temp.contact_run_case_v1('push_native_start_and_replay');
SELECT pg_temp.contact_run_case_v1('email_native_start_and_replay');
SELECT pg_temp.contact_run_case_v1('push_once_consume');
SELECT pg_temp.contact_run_case_v1('push_admitted_before_pause');
SELECT pg_temp.contact_run_case_v1('push_wrong_lease');
SELECT pg_temp.contact_run_case_v1('push_expired_lease');
SELECT pg_temp.contact_run_case_v1('push_worker_pause');
SELECT pg_temp.contact_run_case_v1('email_mixed_pause');
SELECT pg_temp.contact_run_case_v1('push_manager_independent');
SELECT pg_temp.contact_run_case_v1('email_worker_independent');
SELECT pg_temp.contact_run_case_v1('push_quiet');
SELECT pg_temp.contact_run_case_v1('email_quiet');
SELECT pg_temp.contact_run_case_v1('email_broken_qt');
SELECT pg_temp.contact_run_case_v1('email_stale_member');
SELECT pg_temp.contact_run_case_v1('push_stale_publication');
SELECT pg_temp.contact_run_case_v1('email_stale_publication');
SELECT pg_temp.contact_run_case_v1('push_expired_schedule');
SELECT pg_temp.contact_run_case_v1('push_unknown_crosslease');
SELECT pg_temp.contact_run_case_v1('push_consumed_lost_unknown');
SELECT pg_temp.contact_run_case_v1('push_foreign_capture');
SELECT pg_temp.contact_run_case_v1('push_current_role_replay');
SELECT pg_temp.contact_run_case_v1('push_closed_native_request');
SELECT pg_temp.contact_run_case_v1('push_permit_real_clock_expiry');
SET CONSTRAINTS ALL IMMEDIATE;
SELECT pg_temp.contact_assert_v1((SELECT count(*)=25 FROM pg_temp.contact_start_fixture_results),'exact 25-case native matrix incomplete');
SELECT jsonb_build_object('fixture','CONTACT_START_NATIVE_SOURCE','assertions','PASS','cases',count(*),
 'database',current_database(),'provider_calls',0,'provider_results',0,
 'source_prefix_receipts',(SELECT jsonb_object_agg(kind,source_prefix_receipt) FROM pg_temp.contact_start_fixture_input))
 FROM pg_temp.contact_start_fixture_results;

-- CONTACT_FIXTURE_COMPONENT 07_capture-absence-set.sql SHA256 95ef20c2d8fd6e6ac504d52db91b9a4c810246cef64a8cbcbaf4d3af5d712f35
-- Prefix IDs must already have been recorded here by the real native prefix.
-- Add every original row allocated by this suffix (not only the root group).
INSERT INTO pg_temp.contact_start_fixture_absence_ids
 SELECT 'public.weekly_message_dispatch_targets','id',dispatch_target_id FROM pg_temp.contact_start_fixture_targets
 UNION ALL SELECT 'public.weekly_message_dispatch_commands','id',dispatch_command_id FROM pg_temp.contact_start_fixture_targets
 UNION ALL SELECT 'public.weekly_message_renders','id',c.message_render_id FROM public.weekly_message_dispatch_commands c
 JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_command_id=c.id
 UNION ALL SELECT 'public.weekly_manager_review_batches','id',b.id FROM public.weekly_manager_review_batches b
 JOIN public.weekly_message_dispatch_commands c ON c.message_render_id=b.message_render_id
 JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_command_id=c.id
 UNION ALL SELECT 'public.weekly_manager_review_items','id',i.id FROM public.weekly_manager_review_items i
 JOIN public.weekly_manager_review_batches b ON b.id=i.review_batch_id
 JOIN public.weekly_message_dispatch_commands c ON c.message_render_id=b.message_render_id
 JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_command_id=c.id
 UNION ALL SELECT 'public.weekly_manager_route_receipts','id',r.id FROM public.weekly_manager_route_receipts r
 JOIN public.weekly_manager_review_batches b ON b.id=r.review_batch_id
 JOIN public.weekly_message_dispatch_commands c ON c.message_render_id=b.message_render_id
 JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_command_id=c.id
 UNION ALL SELECT 'public.weekly_manager_route_preparations','id',r.id FROM public.weekly_manager_route_preparations r
 JOIN pg_temp.contact_start_fixture_input f ON f.message_intent_id=r.message_intent_id
 ON CONFLICT DO NOTHING;
-- No payload, recipient, credential, token or full body is returned.
SELECT jsonb_build_object('version','CONTACT_FIXTURE_ABSENCE_SET_V1','database',current_database(),
 'restoration',(SELECT source_prefix_receipt->'restoration' FROM pg_temp.contact_start_fixture_input WHERE kind='PUSH'),
 'source_prefix_receipts',(SELECT jsonb_object_agg(kind,source_prefix_receipt) FROM pg_temp.contact_start_fixture_input),
 'objects',(SELECT jsonb_agg(to_jsonb(x) ORDER BY relation_name,column_name,object_id)
  FROM pg_temp.contact_start_fixture_absence_ids x)) AS fixture_absence_set;

