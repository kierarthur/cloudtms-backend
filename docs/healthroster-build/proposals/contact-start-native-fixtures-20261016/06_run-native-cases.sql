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
