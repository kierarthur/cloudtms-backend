-- Portable NEW/UPGRADE catalogue, isolation and narrow input checks only.
-- No business fixtures/data changed. Root registers this in the final release.
-- Publication/contact/business/financial/concurrency acceptance is separate.
\set ON_ERROR_STOP on

do $verify$
declare routine record; definition text; v_role text:=current_setting('request.jwt.claim.role',true);
begin
  for routine in select expected.signature,expected.service_execute,proc.* from (values
    ('private.weekly_source_checking_client_scope_v1(uuid)',false),
    ('private.weekly_source_protected_wait_receipt_v1(uuid,uuid)',false),
    ('private.weekly_source_protected_wait_witness_v1(uuid,uuid)',false),
    ('private.weekly_source_pay_query_facts_v1(uuid)',false),
    ('private.weekly_source_covered_hours_incident_v1(uuid)',false),
    ('private.weekly_source_incident_contact_retire_v1(uuid)',false),
    ('private.weekly_source_protected_contact_retire_v1(uuid,uuid)',false),
    ('private.weekly_source_unprotected_not_finalised_close_v1(uuid,uuid)',false),
    ('public.weekly_source_global_settings_get_v1(jsonb)',true),
    ('public.weekly_source_global_settings_save_atomic_v1(jsonb)',true),
    ('private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamptz)',false),
    ('private.weekly_source_candidate_current_rows_v1(uuid,uuid,uuid,uuid,date)',false),
    ('private.weekly_source_publication_comparison_record_v1(uuid,uuid,text,text)',false),
    ('private.weekly_source_query_comparison_fingerprint_v1(uuid,integer,uuid,jsonb)',false),
    ('private.weekly_source_expected_week_unsigned_materialise_v1(uuid)',false),
    ('private.weekly_source_candidate_prefinal_publish_recheck_v1(uuid,uuid)',false),
    ('private.weekly_source_expected_week_question_v1(uuid)',false),
    ('private.weekly_source_manager_due_event_covers_v1(uuid,uuid)',false),
    ('private.weekly_source_query_manager_row_owned_v1(uuid,uuid,integer)',false),
    ('private.weekly_source_query_manager_generation_v1(uuid,text,timestamptz,boolean)',false),
    ('private.weekly_source_not_finalised_dispatch_v1(uuid)',false),
    ('private.weekly_source_import_outreach_v1(uuid)',false),
    ('private.weekly_source_protected_family_lock_v1(uuid)',false),
    ('private.weekly_source_protected_group_lock_v1(uuid)',false),
    ('public.weekly_exceptional_pay_prepare_family_v1(jsonb)',true),
    ('public.weekly_exceptional_pay_prepare_action_v1(jsonb)',true),
    ('public.weekly_exceptional_pay_action_context_v1(jsonb)',true),
    ('public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)',true),
    ('private.weekly_source_publication_signed_roots_v1(uuid)',false),
    ('private.weekly_source_publication_signed_family_fence_v1(uuid,jsonb)',false),
    ('private.weekly_source_office_query_groups_v1(uuid,uuid,jsonb)',false),
    ('private.weekly_source_bulk_signed_claim_v1(uuid)',false),
    ('private.weekly_source_protect_all_signed_selection_v1(uuid,uuid,jsonb)',false),
    ('private.weekly_source_not_finalised_due_scope_guard_v1()',false),
    ('private.weekly_source_bulk_source_cas_assert_v1(uuid,uuid[])',false),
    ('private.weekly_source_bulk_stage_replay_assert_v1(uuid,jsonb)',false),
    ('private.weekly_source_bulk_own_stage_v1(uuid,uuid,uuid,text,text,text)',false),
    ('private.weekly_source_bulk_own_stage_context_v1(uuid,uuid,text,text,text)',false),
    ('private.weekly_source_bulk_stage_proof_v1(jsonb,uuid)',false),
    ('public.weekly_source_protect_all_signed_atomic_v1(jsonb)',true),
    ('private.weekly_source_automatic_reconcile_capability_assert_v1(uuid)',false),
    ('private.weekly_source_automatic_reconcile_selection_v1(uuid,uuid)',false),
    ('private.weekly_source_automatic_reconcile_stage_replay_assert_v1(uuid,jsonb)',false),
    ('private.weekly_source_automatic_reconcile_stage_proof_v1(jsonb)',false),
    ('private.weekly_source_automatic_reconcile_certificate_v1(uuid,uuid,uuid)',false),
    ('public.weekly_source_automatic_reconcile_atomic_v1(jsonb)',true)
  ) expected(signature,service_execute) left join pg_proc proc on proc.oid=to_regprocedure(expected.signature)
  loop
    if routine.oid is null or not routine.prosecdef
      or routine.proowner<>(select oid from pg_roles where rolname=current_user)
      or not exists(select 1 from unnest(routine.proconfig) config where config like 'search_path=%')
      or has_function_privilege('anon',routine.oid,'EXECUTE')
      or has_function_privilege('authenticated',routine.oid,'EXECUTE')
      or has_function_privilege('service_role',routine.oid,'EXECUTE') is distinct from routine.service_execute then
      raise exception 'WEEKLY_SOURCE_PROTECTION_ROUTINE_ACL_INVALID:%',routine.signature;
    end if;
  end loop;
  if not exists(select 1 from pg_attribute where attrelid='public.weekly_source_global_settings'::regclass
    and attname='automatic_reconcile_actor_user_id' and atttypid='uuid'::regtype and not attnotnull and not attisdropped)
    or not exists(select 1 from pg_attribute where attrelid='public.weekly_manager_cohort_due_events'::regclass
    and attname='not_finalised_comparison_revision_id' and atttypid='uuid'::regtype and not attnotnull and not attisdropped)
    or not exists(select 1 from pg_trigger where tgrelid='public.weekly_manager_cohort_due_events'::regclass
      and tgname='weekly_source_not_finalised_due_scope_guard' and tgenabled='O'
      and tgfoid='private.weekly_source_not_finalised_due_scope_guard_v1()'::regprocedure) then
    raise exception 'WEEKLY_SOURCE_PROTECTION_EXISTING_STORE_EXTENSION_INVALID';
  end if;
  definition:=pg_get_functiondef('private.weekly_source_pay_query_facts_v1(uuid)'::regprocedure);
  if position('PAY_QUERY_STATE_V3' in definition)=0
    or position('weekly_source_is_import_authoritative_v1' in definition)=0 then
    raise exception 'WEEKLY_SOURCE_PROTECTION_V3_SCOPE_INVALID'; end if;
  definition:=pg_get_functiondef('public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)'::regprocedure);
  if position('bulk_signed_selection_proof' in definition)=0 or position('weekly_source_bulk_stage_proof_v1' in definition)=0
    or position('weekly_source_protected_family_lock_v1' in definition)=0
    or position('automatic_reconcile_selection_sha256' in definition)=0
    or position('weekly_source_automatic_reconcile_certificate_v1' in definition)=0 then
    raise exception 'WEEKLY_SOURCE_PROTECTION_NATIVE_STAGE_BOUNDARY_INVALID'; end if;
  perform set_config('request.jwt.claim.role','authenticated',true);
  begin
    perform public.weekly_source_protect_all_signed_atomic_v1('{}'::jsonb);
    raise exception 'WEEKLY_SOURCE_PROTECTION_BROWSER_CALL_ACCEPTED';
  exception when insufficient_privilege then
    if sqlerrm<>'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' then raise; end if;
  end;
  begin
    perform public.weekly_source_automatic_reconcile_atomic_v1('{}'::jsonb);
    raise exception 'WEEKLY_SOURCE_PROTECTION_BROWSER_AUTO_CALL_ACCEPTED';
  exception when insufficient_privilege then
    if sqlerrm<>'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' then raise; end if;
  end;
  perform set_config('request.jwt.claim.role','service_role',true);
  begin
    perform private.weekly_source_protected_wait_witness_v1(null,null);
    raise exception 'WEEKLY_SOURCE_PROTECTION_NULL_SCOPE_ACCEPTED';
  exception when invalid_parameter_value then
    if sqlerrm<>'WEEKLY_SOURCE_PROTECTED_WAIT_SCOPE_REQUIRED' then raise; end if;
  end;
  perform set_config('request.jwt.claim.role',coalesce(v_role,''),true);
end;
$verify$;
