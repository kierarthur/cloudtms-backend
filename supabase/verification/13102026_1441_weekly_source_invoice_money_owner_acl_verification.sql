-- Source-helper/ACL/closure proof only, root-owned hosted TEST rollback.
-- Financial, invoice renderer and pay isolation journeys have separate gates.
\set ON_ERROR_STOP on
begin;
create function pg_temp.f5_owner_assert(p_ok boolean,p_name text) returns void language plpgsql as $f$
begin if p_ok is not true then raise exception 'F5_OWNER_VERIFY_FAILED: %',p_name; end if; end; $f$;
do $verify$
declare signature text; browser_role text; definition text; state jsonb; vector jsonb; class_count integer;
begin
  state:=jsonb_build_object('transition_id','f5510000-0000-4000-8000-000000000999','source_facts',jsonb_build_object('legacy',true));
  perform pg_temp.f5_owner_assert(private.weekly_source_billing_movement_hash_v1(state)=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_BILLING_MOVEMENT_V1',state),'old movement domain and JSON byte identity');
  begin perform private.weekly_source_billing_movement_hash_v1(state||jsonb_build_object('unknown_origin',1));
    raise exception 'F5_EXPECTED_HASH_REFUSAL';
  exception when sqlstate '22023' then perform pg_temp.f5_owner_assert(sqlerrm='WEEKLY_SOURCE_BILLING_HASH_INPUT_INVALID','closed hash keys'); end;
  vector:=private.weekly_source_invoice_only_vector_v1('PAY',0);
  perform pg_temp.f5_owner_assert(vector->>'total_pence'='0' and vector->>'paid_minutes'='0' and vector->>'break_minutes'='0'
    and vector->'hours'='{"day":0,"night":0,"sat":0,"sun":0,"bh":0}'::jsonb,'full zero pay vector');
  begin perform private.weekly_source_invoice_only_vector_v1('PAY',1); raise exception 'F5_EXPECTED_PAY_REFUSAL';
  exception when sqlstate '22023' then perform pg_temp.f5_owner_assert(sqlerrm='WEEKLY_SOURCE_INVOICE_VECTOR_INVALID','nonzero pay refused'); end;
  perform pg_temp.f5_owner_assert(private.weekly_source_frozen_rule_values_v1('f5510000-0000-4000-8000-000000000999') is null,'absent immutable version has no current-setting fallback');
  perform pg_temp.f5_owner_assert(private.weekly_source_shift_on_self_bill_v1('f5510000-0000-4000-8000-000000000999','f5510000-0000-4000-8000-000000000998')
    ='{"on_accepted_report":false,"bound_to_finalised_export":false,"invoice_ready":false,"s25_pay_selected":false}'::jsonb,'unknown scope returns four strict false facts');
  foreach signature in array array[
    'private.weekly_source_billing_movement_hash_v1(jsonb)',
    'private.weekly_source_frozen_rule_values_v1(uuid)',
    'private.weekly_source_shift_on_self_bill_v1(uuid,uuid)',
    'private.weekly_source_invoice_only_vector_v1(text,bigint)',
    'private.weekly_source_invoice_line_amounts_v1(uuid,uuid)',
    'private.weekly_source_source_amount_acceptance_policy_fingerprint_v1()',
    'private.weekly_source_invoice_window_cycle_v1(uuid,uuid)',
    'private.weekly_source_invoice_correction_insert_v1(uuid,uuid,uuid)',
    'private.weekly_source_invoice_correction_unwind_v1(uuid,uuid,uuid)',
    'private.weekly_source_invoice_movement_money_v1(uuid,uuid)',
    'private.weekly_source_invoice_presentation_release_authority_v1(uuid[],uuid)',
    'private.weekly_source_invoice_presentation_source_hash_v1(uuid[],uuid)',
    'private.weekly_source_vat_rounding_movement_insert_v1(uuid,uuid,uuid)',
    'private.weekly_source_rebill_movement_insert_v1(uuid,uuid)',
    'private.weekly_source_invoice_source_amount_movement_valid_v1(uuid)',
    'private.weekly_source_source_amount_warnings_v1(uuid,uuid)',
    'private.weekly_source_invoice_placement_identity_guard_v1()'] loop
    perform pg_temp.f5_owner_assert(to_regprocedure(signature) is not null,'new private signature: '||signature);
    foreach browser_role in array array['anon','authenticated','service_role'] loop
      perform pg_temp.f5_owner_assert(not has_function_privilege(browser_role,signature,'EXECUTE'),'private owner-only effective EXECUTE: '||signature||'/'||browser_role);
    end loop;
  end loop;
  perform pg_temp.f5_owner_assert(has_function_privilege('service_role','public.weekly_source_invoice_hold_release_atomic_v1(jsonb)','EXECUTE'),'bounded service RPC');
  perform pg_temp.f5_owner_assert(not has_function_privilege('anon','public.weekly_source_invoice_hold_release_atomic_v1(jsonb)','EXECUTE')
    and not has_function_privilege('authenticated','public.weekly_source_invoice_hold_release_atomic_v1(jsonb)','EXECUTE'),'no browser RPC exposure');
  perform pg_temp.f5_owner_assert(has_function_privilege('service_role','private.weekly_source_pay_actor_allowed_v1(uuid)','EXECUTE'),'retained PP service predicate union');
  foreach browser_role in array array['anon','authenticated','service_role'] loop
    perform pg_temp.f5_owner_assert(not has_table_privilege(browser_role,'public.weekly_source_invoice_hold_releases','SELECT,INSERT,UPDATE,DELETE,TRUNCATE'),'hold table direct access revoked: '||browser_role);
  end loop;
  perform pg_temp.f5_owner_assert((select relrowsecurity and relforcerowsecurity from pg_class where oid='public.weekly_source_invoice_hold_releases'::regclass),'hold FORCE RLS');
  perform pg_temp.f5_owner_assert(exists(select 1 from pg_policy where polrelid='public.weekly_source_invoice_hold_releases'::regclass and polname='cloudtms_miget_service_owner_all'),'service-owner RLS policy');
  perform pg_temp.f5_owner_assert(exists(select 1 from pg_trigger where tgrelid='public.weekly_source_invoice_hold_releases'::regclass and tgname='weekly_source_immutable_record_guard' and not tgisinternal)
    and exists(select 1 from pg_trigger where tgrelid='public.weekly_source_invoice_hold_releases'::regclass and tgname='weekly_source_immutable_truncate_guard' and not tgisinternal),'all immutable operations guarded');
  select count(*) into class_count from private._weekly_source_acl_table_contract_v1() where table_name='weekly_source_invoice_hold_releases' and record_class='IMMUTABLE_APPEND_ONLY';
  perform pg_temp.f5_owner_assert(class_count=1,'one hold inventory entry');
  foreach signature in array array[
    'public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb)',
    'private.weekly_source_ordinary_projection_source_units_v1(uuid,uuid)',
    'private.weekly_source_ordinary_projection_active_movements_v1(uuid,uuid)',
    'private.weekly_source_selected_source_witness_v1(uuid,uuid,uuid)',
    'private.weekly_source_protected_final_source_context_v1(uuid,uuid,uuid)',
    'private.weekly_source_finalisation_pay_context_v1(uuid,uuid)',
    'public.weekly_source_finalisation_pay_open_atomic_v1(jsonb)',
    'private.weekly_source_correct_final_prepared_context_v1(uuid)'] loop
    definition:=pg_get_functiondef(to_regprocedure(signature));
    perform pg_temp.f5_owner_assert(position('SOURCE_REBILL_LINE' in definition)>0 and position('INVOICE_VAT_ROUNDING' in definition)>0
      and position('SOURCE_INVOICE_CORRECTION' in definition)>0 and position('SELF_BILL_REPORT_LINE' in definition)>0,'complete controlling pay exclusion: '||signature);
  end loop;
end;
$verify$;
rollback;
