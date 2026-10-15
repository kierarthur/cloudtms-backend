-- Proposed final current Agency-specific signature closure. Not executed.
do $agency_owner_security$
declare entry record; oid_value oid; p record; config text;
begin
 for entry in select * from(values
   ('private.weekly_source_agency_submitted_claim_v1(uuid)',false),
   ('private.weekly_source_agency_native_pay_scope_v1(uuid)',false),
   ('private.weekly_source_agency_legacy_original_document_v1(uuid,uuid,uuid)',false),
   ('private.weekly_source_agency_original_pay_identity_v1(uuid,uuid,uuid,uuid)',false),
   ('private.weekly_source_agency_paid_write_purpose_v1(uuid)',false),
   ('private.weekly_source_agency_ordinary_collection_inputs_v1(uuid)',false),
   ('private.weekly_source_agency_before_collection_inputs_v1(uuid,uuid)',false),
   ('private.weekly_source_agency_candidate_admit_v1(uuid,uuid)',false),
   ('private.weekly_source_agency_physical_pay_inputs_v1(uuid,uuid)',false),
   ('private.weekly_source_agency_segment_flags_apply_v1(uuid)',false),
   ('private.weekly_source_agency_native_query_facts_v1(uuid)',false),
   ('private.weekly_source_agency_invoice_segment_selection_v1(uuid,uuid,uuid)',false),
   ('private.weekly_source_agency_draft_round_trip_v1(text,text,uuid)',false),
   ('private.weekly_source_agency_week_facts_v1(uuid)',false),
   ('private.weekly_source_agency_week_complete_v1(uuid)',false),
   ('public.weekly_source_agency_week_validation_refresh_v1(uuid,uuid)',true),
   ('private.weekly_source_agency_validation_source_guard_v1()',false),
   ('private.weekly_source_agency_validation_context_v1(uuid)',false),
   ('public.weekly_source_agency_validation_context_v1(uuid)',true),
   ('private.weekly_source_agency_validation_frame_open_v1(uuid,jsonb,uuid)',false),
   ('private.weekly_source_agency_validation_write_assert_v1(uuid,jsonb,uuid,uuid)',false),
   ('private.weekly_source_agency_validation_write_observe_v1(uuid,jsonb,uuid)',false),
   ('public.weekly_source_agency_validation_apply_v1(uuid,bigint,text,jsonb)',true),
   ('private.weekly_source_agency_financial_frame_open_v1(uuid,text,jsonb,uuid)',false),
   ('private.weekly_source_agency_financial_write_assert_v1(uuid,jsonb,uuid,uuid)',true),
   ('private.weekly_source_agency_financial_write_observe_v1(uuid,jsonb,uuid)',false),
   ('private.weekly_source_agency_segment_flags_v1(uuid)',false),
   ('private.weekly_source_agency_change_authorise_step_v1(uuid,uuid)',false),
   ('public.weekly_source_agency_change_authorise_step_v1(uuid,uuid)',true),
   ('private.weekly_source_agency_publication_validation_v1(uuid)',false),
   ('private.weekly_source_agency_authorise_validation_after_v1()',false),
   ('private.weekly_source_agency_legacy_import_validation_admit_v1(uuid)',false),
   ('public.weekly_source_agency_legacy_import_validation_admit_v1(uuid,uuid)',true),
   ('private.weekly_source_agency_change_return_step_v1(uuid,uuid)',false),
   ('public.weekly_source_agency_change_return_step_v1(uuid,uuid)',true),
   ('private.weekly_source_agency_snapshot_provenance_assert_v1(uuid,jsonb)',false),
   ('public.weekly_source_agency_change_worker_apply_v1(uuid,bigint,text,jsonb)',true),
   ('public.weekly_source_agency_change_worker_targets_v1(uuid[])',true),
   ('private.weekly_source_agency_change_worker_context_v1(uuid)',false),
   ('public.weekly_source_agency_change_worker_context_v1(uuid)',true),
   ('public.weekly_source_agency_timesheet_change_v1(uuid,text,text,text,uuid)',true),
   ('private.weekly_source_agency_crosscheck_context_v1(uuid)',false),
   ('public.weekly_source_agency_crosscheck_context_v1(uuid)',true),
   ('private.weekly_source_agency_issue_requirements_v1(uuid)',false),
   ('private.weekly_source_agency_issue_close_v1(uuid)',false),
   ('private.weekly_source_agency_mode_a_units_v1(uuid)',false),
   ('private.weekly_source_agency_change_identity_guard_v1()',false),
   ('private.weekly_source_agency_source_phase_admit_v1(uuid,uuid,uuid,uuid,uuid,jsonb,jsonb)',false),
   ('private.weekly_source_agency_source_mutation_basis_v1(text,jsonb)',false),
   ('private.weekly_source_agency_active_source_mutation_assert_v1(text,jsonb,jsonb,jsonb)',false),
   ('private.weekly_source_agency_change_capture_v1(uuid,uuid)',false),
   ('private.weekly_source_agency_change_request_sha256_v1(uuid,text,text,text,uuid,jsonb)',false),
   ('private.weekly_source_agency_original_claim_recheck_v1(uuid)',false),
   ('private.weekly_source_agency_change_request_assert_v1(uuid,text,text,text,uuid)',false),
   ('private.weekly_source_agency_change_begin_v1(uuid,text,text,text,uuid)',false),
   ('private.timesheet_correction_a0_source_raw_tokens_v1(text,jsonb)',false),
   ('private.timesheet_correction_a0_source_revision_tokens_v1(text,jsonb)',false),
   ('private.timesheet_correction_a0_source_policy_tokens_v1(text,jsonb)',false),
   ('private.timesheet_correction_a0_source_reference_before_v1()',false),
   ('private.timesheet_correction_a0_source_reference_final_before_v1()',false)
 ) selected(signature,service_execute) loop
  oid_value:=to_regprocedure(entry.signature);
  if oid_value is null then raise exception 'AGENCY_SOURCE_OWNER_MISSING: %',entry.signature;end if;
  select * into strict p from pg_proc where oid=oid_value;
  if not p.prosecdef or p.proowner<>(select oid from pg_roles where rolname=current_user)
   or not exists(select 1 from unnest(p.proconfig) setting where setting in(
    'search_path=pg_catalog, public, private, pg_temp','search_path=pg_catalog, pg_temp','search_path=pg_catalog, private, pg_temp'))
   or exists(select 1 from aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl where acl.grantee=0 and acl.privilege_type='EXECUTE')
   or has_function_privilege('anon',oid_value,'EXECUTE') or has_function_privilege('authenticated',oid_value,'EXECUTE')
   or has_function_privilege('service_role',oid_value,'EXECUTE') is distinct from entry.service_execute then
   raise exception 'AGENCY_SOURCE_OWNER_SECURITY_INVALID: %',entry.signature;
  end if;
 end loop;
end;
$agency_owner_security$;
