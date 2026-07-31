-- ============================================================================
-- 01_timesheet_paid_uninvoiced_rollover_v1.txt
--
-- Creates one new current, unauthorised, pending TSFIN shell while preserving
-- the paid TSFIN as non-current immutable financial history.
--
-- This function:
--   - does not amend timesheet hours/schedule;
--   - does not authorise or unauthorise a timesheet;
--   - does not delete any row;
--   - does not alter paid amounts, paid references, invoice evidence,
--     settlement evidence, or Banking Pay artifacts.
--
-- Required call order:
--   canonical unauthorise
--   -> this rollover
--   -> separate source/timesheet amendment
--   -> existing TSFIN calculator/writer
--   -> canonical reauthorisation
--
-- Limits:
--   6 arguments.
--   One timesheet and one current TSFIN.
--   Transaction-scoped try-lock; no unbounded waiting.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.timesheet_paid_uninvoiced_rollover_v1(
  p_timesheet_id uuid,
  p_actor_user_id uuid,
  p_operation_id uuid,
  p_expected_current_tsfin_id uuid,
  p_expected_preflight_fingerprint text,
  p_now_utc timestamptz DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_expected_preflight_fingerprint text :=
    NULLIF(BTRIM(COALESCE(p_expected_preflight_fingerprint, '')), '');

  v_operation public.import_apply_operations%ROWTYPE;
  v_timesheet public.timesheets%ROWTYPE;
  v_old_tsfin public.timesheets_financials%ROWTYPE;
  v_existing_new_tsfin public.timesheets_financials%ROWTYPE;
  v_new_tsfin public.timesheets_financials%ROWTYPE;

  v_chain jsonb;
  v_preflight jsonb;
  v_root_timesheet_id uuid;
  v_correction_financials_policy_envelope jsonb;
  v_correction_financials_policy_envelope_fingerprint text;
  v_actual_policy_envelope_fingerprint text;
  v_replacement_policy jsonb;
  v_operation_unit_count integer := 0;
  v_operation_contract jsonb;
  v_operation_contract_fingerprint text;
  v_operation_unit jsonb;
  v_request_unit jsonb;
  v_request_unit_count integer := 0;
  v_route text;
  v_is_ordinary_source boolean := false;
  v_replay boolean := false;
  v_old_paid_digest text;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_TIMESHEET_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_ACTOR_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_OPERATION_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_expected_current_tsfin_id IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_EXPECTED_TSFIN_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF v_expected_preflight_fingerprint IS NULL
     OR char_length(v_expected_preflight_fingerprint) > 256 THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_PREFLIGHT_FINGERPRINT_INVALID'
      USING ERRCODE = '22023';
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.import_apply_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0002',
            DETAIL = jsonb_build_object(
              'operation_id', p_operation_id::text
            )::text;
  END IF;

  IF v_operation.actor_user_id IS DISTINCT FROM p_actor_user_id THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_ACTOR_MISMATCH'
      USING ERRCODE = '42501',
            DETAIL = jsonb_build_object(
              'operation_id', p_operation_id::text,
              'expected_actor_user_id', v_operation.actor_user_id::text,
              'supplied_actor_user_id', p_actor_user_id::text
            )::text;
  END IF;

  IF v_operation.state <> 'PREPARED' THEN
    IF v_operation.state IN (
      'SOURCE_COMMITTED_TSFIN_PENDING',
      'FINANCIALISED_PENDING_FINALISATION',
      'COMPLETE'
    ) THEN
      SELECT current_financial.*
      INTO v_existing_new_tsfin
      FROM public.timesheets_financials AS current_financial
      WHERE current_financial.timesheet_id = p_timesheet_id
        AND current_financial.is_current = true
        AND current_financial.id <> p_expected_current_tsfin_id
      ORDER BY current_financial.computed_at_utc DESC, current_financial.id DESC
      LIMIT 1;

      IF FOUND THEN
        RETURN jsonb_build_object(
          'ok', true,
          'replay', true,
          'operation_id', p_operation_id::text,
          'timesheet_id', p_timesheet_id::text,
          'historical_paid_tsfin_id', p_expected_current_tsfin_id::text,
          'new_current_tsfin_id', v_existing_new_tsfin.id::text,
          'new_current_processing_status',
            v_existing_new_tsfin.processing_status::text,
          'operation_state', v_operation.state
        );
      END IF;
    END IF;

    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_OPERATION_STATE_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'operation_id', p_operation_id::text,
              'state', v_operation.state,
              'required_state', 'PREPARED'
            )::text;
  END IF;

  select count(*)::integer,min(request_unit::text)::jsonb
  into v_request_unit_count,v_request_unit
  from jsonb_array_elements(coalesce(
    v_operation.response_json#>'{request_envelope,reconciliation_units}','[]'::jsonb
  )) request_unit
  where request_unit->>'source_timesheet_id'=p_timesheet_id::text
     or coalesce(request_unit->'M_active_member_ids','[]'::jsonb) @> jsonb_build_array(p_timesheet_id);
  if v_request_unit_count<>1
     or v_request_unit->>'source_system' not in ('NHSP','HEALTHROSTER')
     or nullif(v_request_unit->>'action_id','') is null
     or nullif(v_request_unit->>'source_identity','') is null then
    raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_OPERATION_UNIT_INVALID'
      using errcode='P0001',detail=jsonb_build_object(
        'operation_id',p_operation_id,'timesheet_id',p_timesheet_id,
        'matching_unit_count',v_request_unit_count
      )::text;
  end if;
  v_route:=v_request_unit->>'route';
  v_is_ordinary_source:=v_route='AMEND_PAID_UNINVOICED_SOURCE';

  if v_is_ordinary_source then
    if jsonb_array_length(coalesce(v_request_unit->'B_effective_invoice_ids','[]'::jsonb))<>0
       or jsonb_array_length(coalesce(v_request_unit->'B_effective_invoice_line_ids','[]'::jsonb))<>0
       or coalesce((v_request_unit#>>'{B_hours,total_hours}')::numeric,0)<>0 then
      raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_OPERATION_UNIT_INVALID'
        using errcode='P0001';
    end if;
    v_root_timesheet_id:=p_timesheet_id;
    v_chain:=jsonb_build_object('valid',true,'root_timesheet_id',p_timesheet_id,
      'ordinary_source',true);
  else
    v_chain := public.timesheet_correction_chain_scope_v1(
      p_timesheet_id,
      true,
      32,
      100
    );

    IF COALESCE((v_chain ->> 'valid')::boolean, false) IS NOT TRUE THEN
      RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_CHAIN_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'chain', v_chain
              )::text;
    END IF;
    v_root_timesheet_id :=NULLIF(v_chain ->> 'root_timesheet_id', '')::uuid;
  end if;
  v_operation_contract:=v_operation.response_json#>'{correction_operation_contract}';
  if jsonb_typeof(v_operation_contract)<>'object'
     or v_operation_contract->>'schema_version'<>'IMPORT_CORRECTION_OPERATION_V2'
     or v_operation_contract->>'operation_id' is distinct from p_operation_id::text then
    raise exception 'PAID_TSFIN_ROLLOVER_OPERATION_CONTRACT_INVALID' using errcode='P0001';
  end if;
  v_operation_contract_fingerprint:=encode(extensions.digest(
    convert_to((v_operation_contract-'operation_contract_fingerprint')::text,'UTF8'),
    'sha256'::text
  ),'hex');
  if v_operation_contract->>'operation_contract_fingerprint'
     is distinct from v_operation_contract_fingerprint then
    raise exception 'PAID_TSFIN_ROLLOVER_OPERATION_CONTRACT_FINGERPRINT_INVALID'
      using errcode='P0001';
  end if;
  -- The paid source row predates the correction member, so read the one exact
  -- frozen unit from the durable operation contract rather than trusting a
  -- caller-supplied or obsolete top-level response field.
  select count(*)::integer,min(unit::text)::jsonb
  into v_operation_unit_count,v_operation_unit
  from jsonb_array_elements(
    case when jsonb_typeof(v_operation_contract->'correction_units')='array'
      then v_operation_contract->'correction_units'
      else '[]'::jsonb end
  ) unit
  where unit->>'action_id'=v_request_unit->>'action_id'
    and unit->>'root_timesheet_id'=v_root_timesheet_id::text;
  if v_operation_unit_count<>1 then
    raise exception 'PAID_TSFIN_ROLLOVER_OPERATION_UNIT_NOT_UNIQUE'
      using errcode='P0001',detail=jsonb_build_object(
        'operation_id',p_operation_id,'root_timesheet_id',v_root_timesheet_id,
        'matching_unit_count',v_operation_unit_count
      )::text;
  end if;
  if v_operation_unit->>'source_row_key' is distinct from v_request_unit->>'source_identity'
     or nullif(v_operation_unit->>'source_shift_id','') is distinct from nullif(v_request_unit->>'source_shift_id','')
     or v_operation_contract->>'source_system' is distinct from v_request_unit->>'source_system'
     or v_operation_unit#>>'{policy_envelope,classification,source_system}' is distinct from v_request_unit->>'source_system' then
    raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_POLICY_INVALID' using errcode='P0001';
  end if;
  v_correction_financials_policy_envelope:=v_operation_unit->'policy_envelope';
  v_correction_financials_policy_envelope_fingerprint := NULLIF(
    v_correction_financials_policy_envelope ->> 'envelope_fingerprint', ''
  );
  v_replacement_policy := case
    when v_correction_financials_policy_envelope->>'correction_shape'='REVERSAL_ONLY'
      then v_correction_financials_policy_envelope->'reversal'
    else v_correction_financials_policy_envelope->'replacement' end;

  IF jsonb_typeof(v_correction_financials_policy_envelope) <> 'object'
     OR v_correction_financials_policy_envelope_fingerprint IS NULL
     OR v_correction_financials_policy_envelope#>>'{operation,operation_id}'
        IS DISTINCT FROM p_operation_id::text
     OR v_operation_unit->>'policy_envelope_fingerprint'
        IS DISTINCT FROM v_correction_financials_policy_envelope_fingerprint
     OR coalesce((v_replacement_policy->>'applicable')::boolean,false) IS NOT TRUE THEN
    RAISE EXCEPTION '%',case when v_is_ordinary_source
        then 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_POLICY_INVALID'
        else 'PAID_TSFIN_ROLLOVER_POLICY_ENVELOPE_REQUIRED' end
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'root_timesheet_id', v_root_timesheet_id::text,
              'chain_errors', COALESCE(v_chain -> 'errors', '[]'::jsonb)
            )::text;
  END IF;

  v_actual_policy_envelope_fingerprint := encode(
    extensions.digest(
      convert_to(
        (v_correction_financials_policy_envelope - 'envelope_fingerprint')::text,
        'UTF8'
      ),
      'sha256'::text
    ),
    'hex'
  );

  IF v_actual_policy_envelope_fingerprint
       IS DISTINCT FROM v_correction_financials_policy_envelope_fingerprint THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_POLICY_ENVELOPE_FINGERPRINT_INVALID'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'stored_fingerprint',
                v_correction_financials_policy_envelope_fingerprint,
              'actual_fingerprint', v_actual_policy_envelope_fingerprint
            )::text;
  END IF;

  SELECT timesheet_row.*
  INTO v_timesheet
  FROM public.timesheets AS timesheet_row
  WHERE timesheet_row.timesheet_id = p_timesheet_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_TIMESHEET_NOT_FOUND'
      USING ERRCODE = 'P0002';
  END IF;

  IF COALESCE(v_timesheet.is_current, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_TIMESHEET_NOT_CURRENT'
      USING ERRCODE = 'P0001';
  END IF;

  IF v_timesheet.authorised_at_server IS NOT NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_REQUIRES_UNAUTHORISED_TIMESHEET'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'timesheet_id', p_timesheet_id::text,
              'authorised_at_server', v_timesheet.authorised_at_server
            )::text;
  END IF;

  SELECT financial_row.*
  INTO v_old_tsfin
  FROM public.timesheets_financials AS financial_row
  WHERE financial_row.id = p_expected_current_tsfin_id
    AND financial_row.timesheet_id = p_timesheet_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_EXPECTED_TSFIN_NOT_FOUND'
      USING ERRCODE = 'P0002';
  END IF;

  IF COALESCE(v_old_tsfin.is_current, false) IS NOT TRUE THEN
    SELECT current_financial.*
    INTO v_existing_new_tsfin
    FROM public.timesheets_financials AS current_financial
    WHERE current_financial.timesheet_id = p_timesheet_id
      AND current_financial.is_current = true
      AND current_financial.id <> v_old_tsfin.id
    ORDER BY current_financial.computed_at_utc DESC, current_financial.id DESC
    LIMIT 1;

    IF FOUND THEN
      RETURN jsonb_build_object(
        'ok', true,
        'replay', true,
        'operation_id', p_operation_id::text,
        'timesheet_id', p_timesheet_id::text,
        'historical_paid_tsfin_id', v_old_tsfin.id::text,
        'new_current_tsfin_id', v_existing_new_tsfin.id::text,
        'new_current_processing_status',
          v_existing_new_tsfin.processing_status::text,
        'operation_state', v_operation.state
      );
    END IF;

    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_EXPECTED_TSFIN_NOT_CURRENT'
      USING ERRCODE = 'P0001';
  END IF;

  IF v_old_tsfin.authorised_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_REQUIRES_UNAUTHORISED_TSFIN'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'timesheet_id', p_timesheet_id::text,
              'tsfin_id', v_old_tsfin.id::text,
              'authorised_at_utc', v_old_tsfin.authorised_at_utc
            )::text;
  END IF;

  IF v_old_tsfin.paid_at_utc IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_PAID_EVIDENCE_REQUIRED'
      USING ERRCODE = 'P0001';
  END IF;

  IF v_old_tsfin.locked_by_invoice_id IS NOT NULL
     OR EXISTS (
       SELECT 1
       FROM public.invoice_lines AS invoice_line
       WHERE invoice_line.timesheet_id = p_timesheet_id
     ) THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_INVOICE_EVIDENCE_BLOCKS'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'timesheet_id', p_timesheet_id::text,
              'locked_by_invoice_id', CASE
                WHEN v_old_tsfin.locked_by_invoice_id IS NULL THEN NULL
                ELSE v_old_tsfin.locked_by_invoice_id::text
              END
            )::text;
  END IF;

  v_preflight := public.import_timesheet_financial_preflight_v1(
    ARRAY[p_timesheet_id]::uuid[],
    'PAID_UNINVOICED_ROLLOVER',
    p_actor_user_id,
    case when v_is_ordinary_source then '{}'::jsonb else jsonb_build_object(
      'chain_fingerprints', jsonb_build_object(
        v_root_timesheet_id::text,
        v_chain ->> 'chain_fingerprint'
      ),
      'correction_financials_policy_envelope_fingerprints', jsonb_build_object(
        v_root_timesheet_id::text,
        v_correction_financials_policy_envelope_fingerprint
      )
    ) end,
    false,
    100
  );

  IF COALESCE((v_preflight ->> 'allowed')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION '%',case when v_is_ordinary_source
        then 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_PREFLIGHT_INVALID'
        else 'PAID_TSFIN_ROLLOVER_PREFLIGHT_BLOCKED' end
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'preflight', v_preflight
            )::text;
  END IF;

  IF v_preflight ->> 'preflight_fingerprint'
       IS DISTINCT FROM v_expected_preflight_fingerprint THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_PREFLIGHT_STALE'
      USING ERRCODE = '40001',
            DETAIL = jsonb_build_object(
              'expected_preflight_fingerprint',
                v_expected_preflight_fingerprint,
              'actual_preflight_fingerprint',
                v_preflight ->> 'preflight_fingerprint'
            )::text;
  END IF;

  if v_is_ordinary_source and (
       v_preflight->>'required_path' is distinct from 'PAID_UNINVOICED_ROLLOVER'
       or coalesce((v_preflight->>'input_count')::integer,0)<>1
       or coalesce((v_preflight->>'member_count')::integer,0)<>1
       or coalesce((v_preflight->>'paid_count')::integer,0)<>1
       or coalesce((v_preflight->>'invoice_lined_count')::integer,0)<>0
       or coalesce((v_preflight->>'blocking_batch_count')::integer,0)<>0
       or coalesce((v_preflight->>'stale_tsfin_count')::integer,0)<>0
       or jsonb_array_length(coalesce(v_preflight->'errors','[]'::jsonb))<>0
       or (select count(*) from jsonb_array_elements(coalesce(v_preflight->'members','[]'::jsonb)) member
           where member->>'timesheet_id'=p_timesheet_id::text
             and member->>'current_tsfin_id'=p_expected_current_tsfin_id::text
             and coalesce((member->>'paid')::boolean,false)
             and not coalesce((member->>'invoice_lined')::boolean,false))<>1
     ) then
    raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_PREFLIGHT_INVALID' using errcode='P0001';
  end if;

  v_old_paid_digest := encode(
    extensions.digest(
      convert_to(
        jsonb_build_object(
          'id', v_old_tsfin.id::text,
          'timesheet_id', v_old_tsfin.timesheet_id::text,
          'timesheet_version', v_old_tsfin.timesheet_version,
          'paid_at_utc', v_old_tsfin.paid_at_utc,
          'paid_by_user_id', CASE
            WHEN v_old_tsfin.paid_by_user_id IS NULL THEN NULL
            ELSE v_old_tsfin.paid_by_user_id::text
          END,
          'payment_reference', v_old_tsfin.payment_reference,
          'total_hours', v_old_tsfin.total_hours,
          'total_pay_ex_vat', v_old_tsfin.total_pay_ex_vat,
          'total_charge_ex_vat', v_old_tsfin.total_charge_ex_vat,
          'pay_vat_rate_pct_snapshot',
            v_old_tsfin.pay_vat_rate_pct_snapshot,
          'pay_vat_amount_snapshot',
            v_old_tsfin.pay_vat_amount_snapshot,
          'pay_total_inc_vat_snapshot',
            v_old_tsfin.pay_total_inc_vat_snapshot,
          'policy_snapshot_json', v_old_tsfin.policy_snapshot_json,
          'rate_source_refs_json', v_old_tsfin.rate_source_refs_json,
          'actual_schedule_json', v_old_tsfin.actual_schedule_json
        )::text,
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );

  UPDATE public.timesheets_financials AS historical_financial
  SET is_current = false
  WHERE historical_financial.id = v_old_tsfin.id
    AND historical_financial.is_current = true;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_CONCURRENT_CURRENT_CHANGE'
      USING ERRCODE = '40001';
  END IF;

  INSERT INTO public.timesheets_financials (
    timesheet_id,
    timesheet_version,
    basis,
    is_current,
    is_stale,
    stale_reason,
    candidate_id,
    client_id,
    role,
    band,
    pay_method,
    policy_snapshot_json,
    rate_source_refs_json,
    computed_at_utc,
    created_at,
    updated_at,
    occupant_key_norm,
    candidate_assignment,
    processing_status,
    processed_by_user_id,
    processed_at_utc,
    po_number,
    pay_on_hold,
    pay_on_hold_reason,
    pay_on_hold_since_utc,
    expenses_pay_ex_vat,
    expenses_charge_ex_vat,
    expenses_description,
    expenses_evidence_r2_key,
    expenses_evidence_manifest,
    mileage_units,
    mileage_pay_ex_vat,
    mileage_charge_ex_vat,
    mileage_pay_rate,
    mileage_charge_rate,
    mileage_evidence_r2_key,
    mileage_evidence_manifest,
    travel_pay_ex_vat,
    travel_charge_ex_vat,
    accommodation_pay_ex_vat,
    accommodation_charge_ex_vat,
    other_pay_ex_vat,
    other_charge_ex_vat,
    hr_crosscheck_status,
    hr_crosscheck_issues,
    external_source_rows_json,
    actual_schedule_json,
    additional_units_json,
    invoice_breakdown_json,
    nhsp_import_id,
    has_rate_issue,
    has_pay_channel_issue
  )
  VALUES (
    p_timesheet_id,
    v_timesheet.version,
    v_old_tsfin.basis,
    true,
    true,
    'IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION',
    v_old_tsfin.candidate_id,
    v_old_tsfin.client_id,
    v_old_tsfin.role,
    v_old_tsfin.band,
    v_old_tsfin.pay_method,
    jsonb_build_object(
      'import_apply_operation_id', p_operation_id::text,
      'import_authoritative_route',v_route,
      'import_authoritative_source_system',v_request_unit->>'source_system',
      'import_authoritative_source_identity',v_request_unit->>'source_identity',
      'import_authoritative_unit_fingerprint',v_request_unit->>'unit_fingerprint',
      'rollover_source_tsfin_id', v_old_tsfin.id::text,
      'rollover_source_paid_digest', v_old_paid_digest,
      'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
      'correction_financials_policy_envelope_fingerprint',
        v_correction_financials_policy_envelope_fingerprint,
      'requires_frozen_correction_policy', true,
      'correction_finance_override_fields', jsonb_build_array(
        'erni_pct',
        'apply_erni_to',
        'vat_rate_pct'
      ),
      'erni_pct', v_replacement_policy #> '{tsfin_policy,erni_pct}',
      'apply_erni_to',
        v_replacement_policy #>> '{tsfin_policy,apply_erni_to}',
      'vat_rate_pct',
        v_replacement_policy #> '{tsfin_policy,applied_pay_vat_rate_pct}',
      'pay_vat_rate_pct',
        v_replacement_policy #> '{tsfin_policy,applied_pay_vat_rate_pct}',
      'correction_leg_fingerprint',
        v_replacement_policy ->> 'leg_fingerprint',
      'correction_tsfin_policy',
        v_replacement_policy -> 'tsfin_policy',
      'correction_tsfin_policy_fingerprint',
        v_replacement_policy #>> '{tsfin_policy,tsfin_policy_fingerprint}',
      'correction_invoice_policy',
        v_replacement_policy -> 'invoice_policy',
      'correction_invoice_policy_fingerprint',
        v_replacement_policy #>> '{invoice_policy,invoice_policy_fingerprint}',
      'correction_invoice_stream',
        v_replacement_policy #>> '{invoice_policy,invoice_stream}'
    ),
    COALESCE(v_old_tsfin.rate_source_refs_json, '{}'::jsonb)
      || jsonb_build_object(
        'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
        'correction_financials_policy_envelope_fingerprint',
          v_correction_financials_policy_envelope_fingerprint,
        'rollover_source_tsfin_id', v_old_tsfin.id::text
      ),
    v_now,
    v_now,
    v_now,
    v_old_tsfin.occupant_key_norm,
    v_old_tsfin.candidate_assignment,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum,
    v_old_tsfin.processed_by_user_id,
    v_old_tsfin.processed_at_utc,
    v_old_tsfin.po_number,
    v_old_tsfin.pay_on_hold,
    v_old_tsfin.pay_on_hold_reason,
    v_old_tsfin.pay_on_hold_since_utc,
    v_old_tsfin.expenses_pay_ex_vat,
    v_old_tsfin.expenses_charge_ex_vat,
    v_old_tsfin.expenses_description,
    v_old_tsfin.expenses_evidence_r2_key,
    v_old_tsfin.expenses_evidence_manifest,
    v_old_tsfin.mileage_units,
    v_old_tsfin.mileage_pay_ex_vat,
    v_old_tsfin.mileage_charge_ex_vat,
    v_old_tsfin.mileage_pay_rate,
    v_old_tsfin.mileage_charge_rate,
    v_old_tsfin.mileage_evidence_r2_key,
    v_old_tsfin.mileage_evidence_manifest,
    v_old_tsfin.travel_pay_ex_vat,
    v_old_tsfin.travel_charge_ex_vat,
    v_old_tsfin.accommodation_pay_ex_vat,
    v_old_tsfin.accommodation_charge_ex_vat,
    v_old_tsfin.other_pay_ex_vat,
    v_old_tsfin.other_charge_ex_vat,
    v_old_tsfin.hr_crosscheck_status,
    v_old_tsfin.hr_crosscheck_issues,
    v_old_tsfin.external_source_rows_json,
    COALESCE(v_timesheet.actual_schedule_json, '[]'::jsonb),
    COALESCE(
      jsonb_build_object(
        'week', COALESCE(v_timesheet.additional_units_week, '{}'::jsonb),
        'per_day', COALESCE(
          v_timesheet.additional_units_per_day,
          '{}'::jsonb
        )
      ),
      '{}'::jsonb
    ),
    '{}'::jsonb,
    v_old_tsfin.nhsp_import_id,
    false,
    false
  )
  RETURNING *
  INTO v_new_tsfin;

  PERFORM public._inv_write_audit(
    p_actor_user_id,
    'IMPORT_PAID_TSFIN_ROLLED',
    jsonb_build_object(
      'operation_id', p_operation_id::text,
      'timesheet_id', p_timesheet_id::text,
      'historical_paid_tsfin_id', v_old_tsfin.id::text,
      'historical_paid_digest', v_old_paid_digest,
      'new_current_tsfin_id', v_new_tsfin.id::text,
      'new_current_processing_status',
        v_new_tsfin.processing_status::text,
      'correction_financials_policy_envelope_fingerprint',
        v_correction_financials_policy_envelope_fingerprint,
      'import_authoritative_route',v_route
    ),
    'timesheet_financials',
    v_new_tsfin.id::text,
    jsonb_build_object(
      'source_tsfin_id', v_old_tsfin.id::text,
      'source_is_current', true,
      'source_paid_at_utc', v_old_tsfin.paid_at_utc
    ),
    'Paid but uninvoiced TSFIN rollover before import amendment',
    NULL::text,
    NULL::text,
    'import-operation:' || p_operation_id::text
  );

  RETURN jsonb_build_object(
    'ok', true,
    'replay', v_replay,
    'operation_id', p_operation_id::text,
    'timesheet_id', p_timesheet_id::text,
    'historical_paid_tsfin_id', v_old_tsfin.id::text,
    'historical_paid_digest', v_old_paid_digest,
    'new_current_tsfin_id', v_new_tsfin.id::text,
    'new_current_processing_status',
      v_new_tsfin.processing_status::text,
    'new_current_is_stale', v_new_tsfin.is_stale,
    'new_current_stale_reason', v_new_tsfin.stale_reason,
    'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
    'correction_financials_policy_envelope_fingerprint',
      v_correction_financials_policy_envelope_fingerprint,
    'requires_frozen_correction_policy', true,
    'requires_calculation', true,
    'requires_reauthorisation', true
    ,'import_authoritative_route',v_route
    ,'ordinary_source',v_is_ordinary_source
  );
END;
$function$;

COMMENT ON FUNCTION public.timesheet_paid_uninvoiced_rollover_v1(
  uuid,
  uuid,
  uuid,
  uuid,
  text,
  timestamptz
)
IS 'Rotates one paid but uninvoiced current TSFIN into non-current retained history by changing only is_current, and creates one current pending-calculation shell carrying the root historical ERNI/VAT anchor.';

REVOKE ALL ON FUNCTION public.timesheet_paid_uninvoiced_rollover_v1(
  uuid,
  uuid,
  uuid,
  uuid,
  text,
  timestamptz
) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.timesheet_paid_uninvoiced_rollover_v1(
  uuid,
  uuid,
  uuid,
  uuid,
  text,
  timestamptz
) TO service_role;
