-- Banking Pay signed recovery Draft parity.
-- A positive return of an earlier non-charge recovery is classified only from
-- its exact frozen item evidence.  Ordinary pay, current finance authority,
-- provider execution and settlement are outside this repair.
\set ON_ERROR_STOP on

do $catalogue$
declare
  helper_signature regprocedure :=
    'private.pay_batch_signed_non_charge_recovery_evidence_v1(jsonb)'::regprocedure;
  projection_signature regprocedure :=
    'private.pay_workbench_sealed_rate_component_projection_v1(uuid,uuid,uuid[])'::regprocedure;
  finalizer_signature regprocedure :=
    'public.pay_batch_finalize_reservations_and_markers(uuid,text,uuid,date,date,uuid,jsonb)'::regprocedure;
  helper_definition text;
  projection_definition text;
  finalizer_definition text;
begin
  select pg_get_functiondef(helper_signature) into strict helper_definition;
  select pg_get_functiondef(projection_signature) into strict projection_definition;
  select pg_get_functiondef(finalizer_signature) into strict finalizer_definition;

  if position('SIGNED_NON_CHARGE_RECOVERY_DRAFT_V1' in helper_definition)=0
     or position('MATCHED_COMPONENT_CARDINALITY' in helper_definition)=0
     or position('FROZEN_EVIDENCE_RECONCILIATION' in helper_definition)=0
     or position('sealed_evidence_version'', 2' in helper_definition)=0
     or position('FROZEN_SIGNED_NON_CHARGE_RECOVERY' in projection_definition)=0
     or position('signed_non_charge_recovery_contract' in projection_definition)=0
     or position('SIGNED_NON_CHARGE_RECOVERY_ITEM_AMOUNT_MISMATCH' in finalizer_definition)=0
     or position('BANKING_PAY_SIGNED_NON_CHARGE_RECOVERY|' in finalizer_definition)=0
     or position('active_signed_reservations' in finalizer_definition)=0
     or position('FROZEN_SIGNED_NON_CHARGE_RECOVERY' in finalizer_definition)=0 then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_DRAFT_DEFINITION_MISSING';
  end if;

  if helper_definition~*'\m(from|join)\M[[:space:]]+(public|private)\.' then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_LIVE_POST_DRAFT_FALLBACK_FOUND';
  end if;
  if has_function_privilege('anon',helper_signature,'EXECUTE')
     or has_function_privilege('authenticated',helper_signature,'EXECUTE')
     or has_function_privilege('service_role',helper_signature,'EXECUTE')
     or not has_function_privilege('postgres',helper_signature,'EXECUTE')
     or has_function_privilege('anon',finalizer_signature,'EXECUTE')
     or has_function_privilege('authenticated',finalizer_signature,'EXECUTE')
     or not has_function_privilege('service_role',finalizer_signature,'EXECUTE') then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_DRAFT_ACL_DRIFT';
  end if;
end;
$catalogue$;

begin;
set local statement_timeout='45s';
set local lock_timeout='5s';

do $first_use$
declare
  timesheet_id constant text := '10000000-0000-4000-8000-000000009001';
  component jsonb;
  item jsonb;
  nested_document jsonb;
  result jsonb;
  expected_seal text;
begin
  expected_seal := md5(jsonb_build_object(
    'sealed_evidence_version',2,
    'financial_revision_digest','financial-digest-fixture',
    'target_authority_digest','target-digest-fixture',
    'conversion_context_digest','conversion-digest-fixture',
    'physical_bucket_digest','physical-digest-fixture',
    'economic_key_type','TS_DAY',
    'economic_key_value','2026-03-13',
    'truth_ex_vat',round(0::numeric,2),
    'baseline_ex_vat',round(-37.39::numeric,2),
    'reserved_ex_vat',round(0::numeric,2)
  )::text);

  component := jsonb_build_object(
    'component_key_type','TS_DAY',
    'component_key_value','2026-03-13',
    'component_amount_ex_vat',37.39::numeric,
    'source_pay_ex_vat',37.39::numeric,
    'source_charge_ex_vat',0::numeric,
    'authoritative_truth_ex_vat',0::numeric,
    'authoritative_baseline_ex_vat',-37.39::numeric,
    'authoritative_reserved_ex_vat',0::numeric,
    'authoritative_outstanding_ex_vat',37.39::numeric,
    'physical_bucket_key','fixture-physical-bucket',
    'physical_bucket_digest','physical-digest-fixture',
    'financial_revision_digest','financial-digest-fixture',
    'target_authority_digest','target-digest-fixture',
    'conversion_context_digest','conversion-digest-fixture',
    'sealed_evidence_digest',expected_seal,
    'source_family_key','timesheet:'||timesheet_id,
    'source_pay_method','PAYE',
    'current_target_pay_method','PAYE',
    'source_basis_json',jsonb_build_object(
      'component_fallback','WORKED_TIME_AMOUNT',
      'work_date','2026-03-13'
    )
  );
  item := jsonb_build_object(
    'timesheet_id',timesheet_id,
    'item_type','SEGMENT_DELTA',
    'amount_ex_vat',37.39::numeric,
    'frozen_component_key_type','TS_DAY',
    'frozen_component_key_value','2026-03-13',
    'frozen_resolution_payload_json',jsonb_build_object(
      'case_components',jsonb_build_array(component)
    )
  );

  result := private.pay_batch_signed_non_charge_recovery_evidence_v1(item);
  if result->>'contract'<>'SIGNED_NON_CHARGE_RECOVERY_DRAFT_V1'
     or (result->>'outstanding_ex_vat')::numeric<>37.39
     or (result->>'source_charge_ex_vat')::numeric<>0
     or result->>'economic_key_type'<>'TS_DAY'
     or result->>'economic_key_value'<>'2026-03-13'
     or result->>'timesheet_id'<>timesheet_id
     or nullif(btrim(coalesce(result->>'evidence_digest','')),'') is null then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_VALID_EVIDENCE_REJECTED: %',result;
  end if;

  nested_document:=jsonb_build_object('pay_batch_item',item,'fact_family','RESERVATION_COMPONENT');
  if private.pay_batch_signed_non_charge_recovery_evidence_v1(nested_document)
       is distinct from result then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_NESTED_EVIDENCE_DRIFT';
  end if;

  if private.pay_batch_signed_non_charge_recovery_evidence_v1(
       item-'frozen_resolution_payload_json'
     ) is not null then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_ORDINARY_ITEM_MISCLASSIFIED';
  end if;

  begin
    perform private.pay_batch_signed_non_charge_recovery_evidence_v1(
      jsonb_set(item,'{frozen_resolution_payload_json,case_components}',
        jsonb_build_array(component,component),false)
    );
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_DUPLICATE_COMPONENT_ACCEPTED';
  exception when sqlstate '23514' then
    if sqlerrm<>'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID' then raise; end if;
  end;

  begin
    perform private.pay_batch_signed_non_charge_recovery_evidence_v1(
      jsonb_set(item,
        '{frozen_resolution_payload_json,case_components,0,source_charge_ex_vat}',
        '1'::jsonb,false)
    );
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_NONZERO_CHARGE_ACCEPTED';
  exception when sqlstate '23514' then
    if sqlerrm<>'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID' then raise; end if;
  end;

  begin
    perform private.pay_batch_signed_non_charge_recovery_evidence_v1(
      jsonb_set(item,
        '{frozen_resolution_payload_json,case_components,0,sealed_evidence_digest}',
        '"tampered"'::jsonb,false)
    );
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_TAMPERED_SEAL_ACCEPTED';
  exception when sqlstate '23514' then
    if sqlerrm<>'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID' then raise; end if;
  end;

  begin
    perform private.pay_batch_signed_non_charge_recovery_evidence_v1(
      jsonb_set(item,
        '{frozen_resolution_payload_json,case_components,0,current_target_pay_method}',
        '"UMBRELLA"'::jsonb,false)
    );
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_PAY_METHOD_MISMATCH_ACCEPTED';
  exception when sqlstate '23514' then
    if sqlerrm<>'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID' then raise; end if;
  end;
end;
$first_use$;

rollback;
