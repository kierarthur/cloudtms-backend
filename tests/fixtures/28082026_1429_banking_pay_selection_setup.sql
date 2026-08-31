-- Synthetic, rollback-only Banking selection data. Caller owns BEGIN/ROLLBACK.
-- No copied customer data, usable credentials, Draft or provider operation.
DO $local_only$ BEGIN
  IF current_database()<>'banking_modal_v2_test'
     AND NOT (
       current_database()='cloudtms_test_clone'
       AND current_setting('cloudtms.rollback_fixture_scope',true)='BANKING_PAY_CANDIDATE_GROUP_PAGINATION_V2'
     ) THEN
    RAISE EXCEPTION 'LOCAL_FIXTURE_ONLY';
  END IF;
END $local_only$;
INSERT INTO public.tms_users(id,email,password_hash,role,is_active)
VALUES ('10000000-0000-4000-8000-000000000001','selection-fixture@example.invalid','UNUSABLE_LOCAL_FIXTURE','admin',true);
INSERT INTO public.candidates(id,display_name,tms_ref)
VALUES ('10000000-0000-4000-8000-000000000002','Selection fixture A','SELECT-A'),
       ('10000000-0000-4000-8000-000000000003','Selection fixture B','SELECT-B');
INSERT INTO public.banking_pay_snapshot_runs(id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,eligibility_to_date)
VALUES ('10000000-0000-4000-8000-000000000004','2026-08-28','2026-08-30','2026-08-24','2026-08-01','2026-08-31');
INSERT INTO public.banking_pay_workbench_sessions(id,actor_user_id,pay_date,week_ending_cutoff,session_signature,source_snapshot_run_id,version,progress_counter_version,progress_json)
VALUES ('10000000-0000-4000-8000-000000000005','10000000-0000-4000-8000-000000000001','2026-08-28','2026-08-30',
 'synthetic candidate selection only','10000000-0000-4000-8000-000000000004',1,4,'{"ready":true}');
INSERT INTO public.banking_pay_workbench_session_scope(session_id,candidate_id,scope_ordinal,status,seeded,dirty,certified_preview_publication_attestation_json)
SELECT '10000000-0000-4000-8000-000000000005'::uuid,id,n,'READY',true,false,
 '{"attestation_version":"CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3","contract_version":"3","semantic_contract_version":"READY_TO_PAY_SEMANTIC_V2"}'::jsonb
FROM (VALUES ('10000000-0000-4000-8000-000000000002'::uuid,1),('10000000-0000-4000-8000-000000000003'::uuid,2)) f(id,n);
INSERT INTO public.banking_pay_workbench_preview_rows(id,session_id,candidate_id,section,row_key,row_ordinal,row_json,key_type,key_value,selected,selection_state,status,session_version)
SELECT ('10000000-0000-4000-8000-' || lpad((1000+n)::text,12,'0'))::uuid,
 '10000000-0000-4000-8000-000000000005'::uuid,
 CASE WHEN n<=107 THEN '10000000-0000-4000-8000-000000000002'::uuid ELSE '10000000-0000-4000-8000-000000000003'::uuid END,
 'canonical_preview_lines','selection-fixture:' || n,n,
 jsonb_build_object('candidate_name','Selection fixture','pay_channel',CASE WHEN n<=105 THEN 'PAYE' ELSE 'UMBRELLA' END,
 'amount_display','10.00','section_amount_display','10.00','amount_ex_vat','10.00','presentation_section','READY_TO_PAY',
 'line_type','TIMESHEET_PAYMENT','presentation_role','ALLOCATION_COMPONENT','selection_allowed',true,'draftable',true,'is_ready_for_draft',true),
 'SOURCE_REF','selection-fixture-' || n,true,'SELECTED','READY',1
FROM generate_series(1,109) n;
INSERT INTO public.banking_pay_workbench_preview_rows(id,session_id,candidate_id,section,row_key,row_ordinal,row_json,key_type,key_value,selected,selection_state,status,session_version)
SELECT ('10000000-0000-4000-8000-' || lpad((2000+n)::text,12,'0'))::uuid,
 '10000000-0000-4000-8000-000000000005'::uuid,
 CASE WHEN n<=2 THEN '10000000-0000-4000-8000-000000000002'::uuid ELSE '10000000-0000-4000-8000-000000000003'::uuid END,
 'blocked_for_pay','selection-recovery:' || n,200+n,
 jsonb_build_object('pay_channel',CASE WHEN n=1 THEN 'PAYE' ELSE 'UMBRELLA' END,
 'line_type','LOAN_REPAYMENT','finance_case_id',('10000000-0000-4000-8000-' || lpad((3000+n)::text,12,'0')),
 'nominal_due_amount_ex_vat','15.00','amount_ex_vat','0.00','blocked_reason_codes',jsonb_build_array('NO_PAY_HEADROOM'),
 'presentation_reason','NO_PAY_HEADROOM','presentation_section','BLOCKED_FOR_PAY','selection_user_override','UNSELECTED',
 'case_components',jsonb_build_array(jsonb_build_object('finance_component_id',('10000000-0000-4000-8000-' || lpad((4000+n)::text,12,'0')),
 'target_pay_ex_vat','15.00','source_pay_ex_vat','15.00','component_key_type','LOAN','component_key_value','fixture-' || n))),
 'SOURCE_REF','selection-recovery-' || n,false,'NOT_SELECTABLE','READY',1
FROM generate_series(1,3) n;
