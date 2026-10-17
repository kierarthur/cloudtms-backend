\set ON_ERROR_STOP on
begin;
-- Independent literal81 security expectations, bound to original approved owner ACLs.
do $verify$
declare expected record;actual record;fn oid;
begin
 for expected in select * from (values
 ('private._candidate_expense_source_family_v1(uuid)',false,true,'s','pg_catalog,public,private,pg_temp'),
 ('private.weekly_exceptional_pay_complete_local_impl_v1(jsonb,uuid)',false,true,'v','public,private,pg_catalog,pg_temp'),
 ('private.weekly_exceptional_pay_publish_next_impl_v1(jsonb,uuid)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_exceptional_pay_stage_c1_request_impl_v1(jsonb,uuid)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_invoice_duty_scope_v1(uuid)',false,true,'s','pg_catalog,pg_temp'),
 ('private.weekly_source_invoice_duty_v1(uuid)',false,true,'s','pg_catalog,pg_temp'),
 ('private.weekly_source_local_preauthorisation_write_allowed_v1(uuid,jsonb)',true,true,'v','pg_catalog,pg_temp'),
 ('private.weekly_source_local_protected_pending_released_v1()',false,true,'v','pg_catalog,pg_temp'),
 ('private.weekly_source_local_publication_context_v2(uuid,uuid,uuid)',false,true,'s','pg_catalog,pg_temp'),
 ('private.weekly_source_local_saved_status_native_v1(uuid,uuid,uuid)',false,true,'s','pg_catalog,pg_temp'),
 ('private.weekly_source_local_saved_status_v1(uuid,uuid,uuid)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_lock_family_rows_v1(uuid[],uuid)',false,true,'v','public,private,pg_catalog,pg_temp'),
 ('private.weekly_source_pay_decision_application_commit_guard_v1()',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_application_finish_v1(uuid)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_assembly_assert_v1(uuid,uuid)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_assembly_commit_guard_v1()',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_bind_publication_v1(uuid)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_confirmation_context_v1(jsonb,text)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_contact_retire_v1(uuid)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_contact_suppressed_v1(uuid,uuid,jsonb)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_family_native_route_v1(uuid,uuid)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_incident_guard_v1()',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_local_head_owned_v1(uuid)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_local_release_proof_v1(uuid)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_metadata_guard_v1()',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_native_dependency_v1(uuid,jsonb)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_native_local_payload_validate_v1(jsonb,uuid)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_native_next_payload_validate_v1(jsonb,uuid)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_native_normal_gate_v1(uuid,uuid,uuid,text)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_native_request_scope_v1(uuid,jsonb,text,boolean)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_native_rollback_witness_v1(uuid)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_native_snapshot_assert_v1(jsonb,jsonb)',false,true,'i','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_public_actions_v1(uuid,jsonb,uuid)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_publication_adoption_guard_v1()',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_publication_adoptions_assert_v1(uuid)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_quantity_union_v1(jsonb,text)',false,true,'i','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_read_source_v1(uuid,uuid,uuid)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_record_native_adoption_v1(uuid,uuid,jsonb)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_request_v1(jsonb,text)',false,true,'i','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_resolved_scope_v1(jsonb,uuid,boolean)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_review_projection_v1(uuid)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_scope_access_v1(uuid,uuid,uuid,date)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_selection_identity_v1(jsonb)',false,false,'i','pg_catalog'),
 ('private.weekly_source_pay_decision_source_subject_v1(uuid)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_unprotect_application_v1(uuid)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_pay_decision_unprotect_apply_v1(uuid,uuid,text,uuid)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_pay_query_scope_v1(uuid)',false,true,'s','pg_catalog,pg_temp'),
 ('private.weekly_source_protected_action_facts_v1(jsonb,text)',false,true,'v','public,private,extensions,pg_catalog,pg_temp'),
 ('private.weekly_source_protected_action_placement_assert_v1(jsonb,jsonb)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_protected_current_placement_v1(uuid,uuid,boolean)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_protected_entitlement_projection_v1(jsonb)',false,false,'i','pg_catalog'),
 ('private.weekly_source_protected_local_stage_engine_v1(jsonb,uuid,boolean)',false,true,'v','public,private,extensions,pg_temp'),
 ('private.weekly_source_protected_next_engine_v1(jsonb,uuid,boolean)',false,true,'v','pg_catalog,private,public'),
 ('private.weekly_source_protected_root_financial_fact_v1(jsonb,integer)',false,true,'i','pg_catalog,private,public'),
 ('private.weekly_source_protected_saved_placement_v1(uuid,uuid,uuid)',false,true,'s','pg_catalog,private,public'),
 ('private.weekly_source_protected_signed_evidence_v1(uuid,uuid,uuid)',false,true,'s','public,private,extensions,pg_temp'),
 ('private.weekly_source_query_resolved_decision_matches_v1(uuid,uuid,jsonb)',false,true,'s','pg_catalog,pg_temp'),
 ('private.weekly_source_resolve_root_identity_v1(uuid)',false,true,'s','public,private,pg_catalog,pg_temp'),
 ('private.weekly_source_target_family_for_root_v1(uuid)',false,true,'s','public,private,pg_catalog,pg_temp'),
 ('private.weekly_source_unprotect_source_observation_v1(uuid,uuid,uuid,jsonb)',false,true,'s','pg_catalog,private,public'),
 ('public.weekly_exceptional_pay_action_context_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_exceptional_pay_complete_local_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_exceptional_pay_next_publication_status_v1(jsonb)',true,true,'s','pg_catalog,private,public'),
 ('public.weekly_exceptional_pay_prepare_action_v1(jsonb)',true,true,'v','public,private,extensions,pg_catalog,pg_temp'),
 ('public.weekly_exceptional_pay_publish_next_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_accept_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_confirmation_context_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_confirmation_prepare_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_preview_abandon_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_preview_begin_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_preview_quote_context_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_preview_quote_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_preview_ready_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_preview_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_read_source_v1(jsonb)',true,true,'s','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_unprotect_prepare_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_unprotect_quote_context_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_unprotect_review_context_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_pay_decision_unprotect_run_prepare_v1(jsonb)',true,true,'v','pg_catalog,private,public'),
 ('public.weekly_source_protected_current_financial_source_v1(jsonb)',true,true,'s','pg_catalog,private,public')
 ) literal(signature,service_execute,security_definer,volatility,search_path) loop
  fn:=pg_catalog.to_regprocedure(expected.signature);
  if fn is null then raise exception 'HEALTH_SOURCE_COMPLETE_ROUTINE_MISSING: %',expected.signature;end if;
  select p.* into strict actual from pg_catalog.pg_proc p where p.oid=fn;
  if actual.proowner<>(current_user::regrole)::oid or actual.prosecdef is distinct from expected.security_definer or actual.provolatile::text<>expected.volatility
   or not exists(select 1 from pg_catalog.unnest(actual.proconfig) setting where pg_catalog.replace(setting,' ','')='search_path='||expected.search_path)
   or exists(select 1 from pg_catalog.aclexplode(coalesce(actual.proacl,pg_catalog.acldefault('f',actual.proowner))) acl
    where acl.privilege_type='EXECUTE' and acl.grantee<>actual.proowner
     and (not expected.service_execute or acl.grantee<>('service_role'::regrole)::oid))
   or pg_catalog.has_function_privilege('anon',fn,'EXECUTE') or pg_catalog.has_function_privilege('authenticated',fn,'EXECUTE')
   or pg_catalog.has_function_privilege('service_role',fn,'EXECUTE') is distinct from expected.service_execute then
   raise exception 'HEALTH_SOURCE_COMPLETE_SECURITY_INVALID: %',expected.signature;end if;
 end loop;
end;
$verify$;
rollback;
