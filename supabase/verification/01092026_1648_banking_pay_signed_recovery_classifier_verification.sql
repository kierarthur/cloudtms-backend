-- Banking Pay signed-recovery classifier historical-shape compatibility.
-- Ordinary frozen rate/correction components may share an economic key; only
-- components carrying the complete signed non-charge signature participate in
-- the signed-recovery cardinality guard.
\set ON_ERROR_STOP on

do $catalogue$
declare
  helper_signature regprocedure :=
    'private.pay_batch_signed_non_charge_recovery_evidence_v1(jsonb)'::regprocedure;
  helper_definition text;
  signature_filter_position integer;
  cardinality_position integer;
begin
  select pg_get_functiondef(helper_signature) into strict helper_definition;

  signature_filter_position := position(
    'component_fallback' in helper_definition
  );
  cardinality_position := position(
    'MATCHED_COMPONENT_CARDINALITY' in helper_definition
  );
  if signature_filter_position=0
     or cardinality_position=0
     or signature_filter_position>=cardinality_position
     or position('authoritative_truth_ex_vat' in helper_definition)=0
     or position('authoritative_baseline_ex_vat' in helper_definition)=0
     or position('SIGNED_NON_CHARGE_RECOVERY_DRAFT_V1' in helper_definition)=0 then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_CLASSIFIER_ORDER_INVALID';
  end if;

  if helper_definition~*'\m(from|join)\M[[:space:]]+(public|private)\.' then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_CLASSIFIER_LIVE_FALLBACK_FOUND';
  end if;
  if has_function_privilege('anon',helper_signature,'EXECUTE')
     or has_function_privilege('authenticated',helper_signature,'EXECUTE')
     or has_function_privilege('service_role',helper_signature,'EXECUTE')
     or not has_function_privilege('postgres',helper_signature,'EXECUTE') then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_CLASSIFIER_ACL_DRIFT';
  end if;
end;
$catalogue$;

begin;
set local statement_timeout='45s';
set local lock_timeout='5s';

do $first_use$
declare
  timesheet_id constant text := '10000000-0000-4000-8000-000000009101';
  valid_component jsonb;
  ordinary_component_a jsonb;
  ordinary_component_b jsonb;
  base_item jsonb;
  ordinary_item jsonb;
  mixed_item jsonb;
  duplicate_signed_item jsonb;
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

  valid_component := jsonb_build_object(
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
  ordinary_component_a := jsonb_build_object(
    'component_key_type','TS_DAY',
    'component_key_value','2026-03-13',
    'component_amount_ex_vat',52.17::numeric,
    'source_pay_ex_vat',52.17::numeric,
    'source_charge_ex_vat',90::numeric,
    'source_pay_method','PAYE',
    'current_target_pay_method','PAYE',
    'source_basis_json',jsonb_build_object('work_date','2026-03-13')
  );
  ordinary_component_b := jsonb_build_object(
    'component_key_type','TS_DAY',
    'component_key_value','2026-03-13',
    'component_amount_ex_vat',-17.39::numeric,
    'source_pay_ex_vat',-17.39::numeric,
    'source_charge_ex_vat',-30::numeric,
    'source_pay_method','PAYE',
    'current_target_pay_method','PAYE',
    'source_basis_json',jsonb_build_object(
      'component_fallback','WORKED_TIME_AMOUNT',
      'work_date','2026-03-13'
    )
  );
  base_item := jsonb_build_object(
    'timesheet_id',timesheet_id,
    'item_type','SEGMENT_DELTA',
    'amount_ex_vat',37.39::numeric,
    'frozen_component_key_type','TS_DAY',
    'frozen_component_key_value','2026-03-13'
  );

  ordinary_item := base_item || jsonb_build_object(
    'frozen_resolution_payload_json',jsonb_build_object(
      'case_components',jsonb_build_array(
        ordinary_component_a,ordinary_component_b
      )
    )
  );
  if private.pay_batch_signed_non_charge_recovery_evidence_v1(ordinary_item)
       is not null then
    raise exception 'BANKING_PAY_ORDINARY_MULTI_COMPONENT_MISCLASSIFIED';
  end if;

  mixed_item := base_item || jsonb_build_object(
    'frozen_resolution_payload_json',jsonb_build_object(
      'case_components',jsonb_build_array(
        ordinary_component_a,valid_component,ordinary_component_b
      )
    )
  );
  result := private.pay_batch_signed_non_charge_recovery_evidence_v1(mixed_item);
  if result->>'contract'<>'SIGNED_NON_CHARGE_RECOVERY_DRAFT_V1'
     or (result->>'outstanding_ex_vat')::numeric<>37.39
     or result->>'economic_key_type'<>'TS_DAY'
     or result->>'economic_key_value'<>'2026-03-13' then
    raise exception 'BANKING_PAY_MIXED_SIGNED_RECOVERY_NOT_RECOGNISED: %',result;
  end if;

  duplicate_signed_item := base_item || jsonb_build_object(
    'frozen_resolution_payload_json',jsonb_build_object(
      'case_components',jsonb_build_array(
        ordinary_component_a,valid_component,ordinary_component_b,valid_component
      )
    )
  );
  begin
    perform private.pay_batch_signed_non_charge_recovery_evidence_v1(
      duplicate_signed_item
    );
    raise exception 'BANKING_PAY_DUPLICATE_SIGNED_RECOVERY_ACCEPTED';
  exception when sqlstate '23514' then
    if sqlerrm<>'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID' then
      raise;
    end if;
  end;
end;
$first_use$;

rollback;
