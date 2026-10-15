-- Run fresh and read-only before and after root's rollback trial; compare the entire JSON value.
with wanted(signature) as(values
('private.weekly_source_actual_timesheet_submission_v1(uuid)'),
('private.weekly_source_signed_submission_read_context_v1(uuid,uuid)'),
('private.weekly_source_checklist_client_calendar_v1(uuid,date)'),
('private.weekly_source_check_only_query_rows_v1(uuid,uuid,jsonb)'),
('private.weekly_source_check_only_issue_filter_keys_v1(text,jsonb)'),
('private.weekly_source_check_only_query_groups_v1(uuid,uuid,jsonb)'),
('private.weekly_source_checklist_native_initial_certificate_core_v1(uuid,boolean)'),
('private.weekly_source_checklist_native_initial_bound_assert_core_v1(uuid,boolean)'),
('private.weekly_source_checklist_native_bound_snapshot_core_v1(uuid,bigint,boolean)'),
('private.weekly_source_checklist_native_members_core_v1(uuid,uuid,boolean)'),
('private.weekly_source_invoice_hold_release_context_core_v1(jsonb,boolean)'),
('private.weekly_source_invoice_hold_first_invoice_assert_v1(uuid)'),
('private.weekly_source_invoice_unreleased_vat_fit_movements_v1(uuid)'),
('private.bpay_next_work_period_proof_v1(uuid,uuid,date)'),
('private.weekly_source_checklist_legacy_pay_state_core_v1(uuid)'),
('private.weekly_source_checklist_legacy_read_frame_v1(uuid,jsonb,uuid)'),
('private.weekly_source_checklist_timesheet_hold_facts_v1(uuid,jsonb,uuid)'),
('private.weekly_source_checklist_information_hold_facts_v1(jsonb,jsonb,uuid)'),
('private.weekly_source_checklist_ordinary_next_hold_money_v1(uuid,jsonb,uuid)'),
('private.weekly_source_checklist_source_shift_query_money_v1(uuid,uuid,jsonb,uuid)'),
('private.weekly_source_checklist_source_invoice_block_items_v1(jsonb,uuid)'),
('private.weekly_source_checklist_source_invoice_decision_items_v1(jsonb,uuid)'),
('private.weekly_source_checklist_process_publication_v1(uuid)'),
('private.weekly_source_checklist_process_v1(uuid,uuid)'),
('private.weekly_source_checklist_page_v1(jsonb,jsonb,uuid,text,text,text)'),
('private.weekly_source_office_tracker_v1(uuid,uuid)'),
('private.weekly_source_healthroster_checklist_leaf_v1(jsonb)'),
('private.weekly_source_check_only_candidate_reminder_subject_v1(jsonb,text,uuid)'),
('private.weekly_source_check_only_candidate_reminder_context_v1(uuid,jsonb,text)'),
('public.weekly_source_check_only_candidate_reminder_atomic_v1(jsonb)'),
('private.weekly_source_checklist_submission_cycle_admit_v1(uuid,text,uuid)'),
('private._weekly_source_settings_ensure_open_cycle_v1(uuid,uuid,bytea,uuid)'),
('private.weekly_source_checklist_submission_cycle_prepare_v1(uuid,uuid)'),
('public.weekly_source_submitted_timesheet_prepare_atomic_v1(jsonb)'),
('private.weekly_source_checklist_historical_submission_descriptor_v1(uuid)'),
('public.weekly_source_checklist_submission_manifest_v1(jsonb)'),
('public.weekly_source_checklist_historical_prepare_atomic_v1(jsonb)'),
('public.weekly_source_checklist_v1(jsonb)'),
('private.bpay_next_workbench_refresh_work_core_v1(uuid)'),
('private.bpay_next_append_work_skip_v1(uuid,uuid,uuid)'),
('private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb)'),
('private.bpay_next_bound_source_snapshot_v1(uuid,bigint)'),
('private.bpay_next_initial_tsfin_certificate_v1(uuid)'),
('private.bpay_next_initial_tsfin_bound_assert_v1(uuid)'),
('private.bpay_next_workbench_work_members_v1(uuid,uuid)'),
('public.weekly_source_invoice_hold_release_atomic_v1(jsonb)'),
('public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)'),
('public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamp with time zone)'),
('private.candidate_notification_visible_v1(public.candidate_notifications,timestamp with time zone,text)'),
('private.weekly_source_checklist_legacy_hold_review_v1(uuid,jsonb,uuid)')
), routines as(
 select w.signature,p.oid::text oid,p.proowner::pg_catalog.regrole::text owner,p.provolatile::text volatility,
  p.prosecdef security_definer,p.proacl::text acl,p.proconfig::text configuration,
  case when p.oid is null then null else pg_catalog.md5(pg_catalog.pg_get_functiondef(p.oid)) end definition_md5
 from wanted w left join pg_catalog.pg_proc p on p.oid=pg_catalog.to_regprocedure(w.signature)
), triggers as(
 select n.nspname schema_name,c.relname table_name,t.tgname trigger_name,
  pg_catalog.md5(pg_catalog.pg_get_triggerdef(t.oid,true)) definition_md5,t.tgenabled enabled
 from pg_catalog.pg_trigger t join pg_catalog.pg_class c on c.oid=t.tgrelid
 join pg_catalog.pg_namespace n on n.oid=c.relnamespace
 where not t.tgisinternal and n.nspname in('private','public')
)
select pg_catalog.jsonb_build_object('contract','CHECKLIST_CONSTRUCTOR_ROUTINE_TRIGGER_FRAME_V1',
 'routines',(select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r) order by signature) from routines r),
 'triggers',(select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(t) order by schema_name,table_name,trigger_name) from triggers t));
