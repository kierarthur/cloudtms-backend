-- Banking Pay bounded-scope Version 1.2.8.
-- Page candidate finance movement items before component/case/frozen resolution.
-- Heavy ownership, transfer, reservation and correction joins therefore apply
-- to at most p_limit + 1 physical item rows once per build, not once per unit.

CREATE OR REPLACE FUNCTION private.pay_workbench_finance_item_authority_page_v1(
  p_build_id uuid,
  p_last_batch_candidate_id uuid DEFAULT NULL::uuid,
  p_last_item_id uuid DEFAULT NULL::uuid,
  p_limit integer DEFAULT 25
)
RETURNS TABLE(
  source_key text,
  pay_batch_candidate_id uuid,
  pay_batch_item_id uuid,
  resolved_timesheet_id uuid,
  resolved_candidate_id uuid,
  resolved_finance_case_id uuid,
  finance_component_id uuid,
  economic_key_type text,
  economic_key_value text,
  source_amount_ex_vat numeric,
  source_amount_inc_vat numeric,
  is_authoritative boolean,
  source_payload_json jsonb,
  financial_digest text,
  resolution_failure text
)
LANGUAGE sql
STABLE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
  WITH build AS (
    SELECT build_row.id,build_row.candidate_id
    FROM private.banking_pay_workbench_economic_builds build_row
    WHERE build_row.id=p_build_id
  ), scope_timesheet AS MATERIALIZED (
    SELECT scope_row.timesheet_id
    FROM private.banking_pay_workbench_economic_build_scope scope_row
    WHERE scope_row.build_id=p_build_id
  ), relevant_item_id AS MATERIALIZED (
    SELECT item.id
    FROM scope_timesheet scope_row
    JOIN public.pay_batch_items item ON item.timesheet_id=scope_row.timesheet_id
    WHERE item.item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
      AND COALESCE(item.is_voided,false) IS FALSE
    UNION
    SELECT item.id
    FROM scope_timesheet scope_row
    JOIN public.pay_finance_case_components component
      ON component.linked_timesheet_id=scope_row.timesheet_id
    JOIN public.pay_batch_items item ON item.finance_component_id=component.id
    WHERE item.item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
      AND COALESCE(item.is_voided,false) IS FALSE
    UNION
    SELECT item.id
    FROM scope_timesheet scope_row
    JOIN public.pay_advances finance_case
      ON finance_case.linked_timesheet_id=scope_row.timesheet_id
    JOIN public.pay_batch_items item ON item.finance_case_id=finance_case.id
    WHERE item.item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
      AND COALESCE(item.is_voided,false) IS FALSE
    UNION
    SELECT item.id
    FROM scope_timesheet scope_row
    JOIN public.pay_batch_items item ON (
      item.frozen_source_basis_json @> jsonb_build_object('linked_timesheet_id',scope_row.timesheet_id::text)
      OR item.frozen_source_basis_json @> jsonb_build_object('timesheet_id',scope_row.timesheet_id::text)
      OR item.frozen_source_basis_json @> jsonb_build_object('carrier_timesheet_id',scope_row.timesheet_id::text)
      OR item.frozen_source_basis_json @> jsonb_build_object(
        'economic_key',jsonb_build_object('timesheet_id',scope_row.timesheet_id::text))
      OR item.frozen_component_snapshot_json @> jsonb_build_object('linked_timesheet_id',scope_row.timesheet_id::text)
      OR item.frozen_component_snapshot_json @> jsonb_build_object('timesheet_id',scope_row.timesheet_id::text)
      OR item.frozen_component_snapshot_json @> jsonb_build_object('carrier_timesheet_id',scope_row.timesheet_id::text)
      OR item.frozen_component_snapshot_json @> jsonb_build_object('source_basis_json',
        jsonb_build_object('linked_timesheet_id',scope_row.timesheet_id::text))
      OR item.frozen_component_snapshot_json @> jsonb_build_object('source_basis_json',
        jsonb_build_object('timesheet_id',scope_row.timesheet_id::text))
      OR item.frozen_component_snapshot_json @> jsonb_build_object('source_basis_json',
        jsonb_build_object('carrier_timesheet_id',scope_row.timesheet_id::text))
    )
    WHERE item.item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
      AND COALESCE(item.is_voided,false) IS FALSE
  ), item_page AS MATERIALIZED (
    SELECT item.*,batch_candidate.candidate_id AS batch_candidate_candidate_id,
      batch_candidate.settlement_status,batch_candidate.settled_at_utc
    FROM build
    JOIN public.pay_batch_candidates batch_candidate
      ON batch_candidate.candidate_id=build.candidate_id
    JOIN relevant_item_id relevant ON true
    JOIN public.pay_batch_items item
      ON item.id=relevant.id AND item.pay_batch_candidate_id=batch_candidate.id
     AND item.item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
     AND COALESCE(item.is_voided,false) IS FALSE
    WHERE p_last_batch_candidate_id IS NULL
       OR (batch_candidate.id,item.id)>(p_last_batch_candidate_id,p_last_item_id)
    ORDER BY batch_candidate.id,item.id
    LIMIT LEAST(GREATEST(COALESCE(p_limit,25),1),25)+1
  ), enriched AS (
    SELECT item_page.*,
      component.linked_timesheet_id AS component_timesheet_id,
      component.candidate_id AS component_candidate_id,
      component.finance_case_id AS component_case_id,
      component.component_key_type,component.component_key_value,
      finance_case.linked_timesheet_id AS case_timesheet_id,
      finance_case.candidate_id AS case_candidate_id,
      transfer.status AS transfer_status,transfer.completed_at_utc,
      COALESCE(reservation.has_settled,false) AS reservation_has_settled,
      reservation.settled_source_amount,
      EXISTS(SELECT 1 FROM public.pay_payment_correction_items correction
        WHERE correction.pay_batch_item_id=item_page.id
          AND correction.status='APPLIED'
          AND correction.correction_item_kind IN
            ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')) AS is_reversed
    FROM item_page
    LEFT JOIN public.pay_finance_case_components component
      ON component.id=item_page.finance_component_id
    LEFT JOIN public.pay_advances finance_case
      ON finance_case.id=COALESCE(item_page.finance_case_id,component.finance_case_id)
    LEFT JOIN public.pay_bank_transfers transfer ON transfer.id=item_page.pay_bank_transfer_id
    LEFT JOIN LATERAL (
      SELECT BOOL_OR(UPPER(BTRIM(COALESCE(reservation_row.status,'')))='SETTLED'
          OR reservation_row.settled_at_utc IS NOT NULL) AS has_settled,
        ROUND(COALESCE(SUM(ABS(COALESCE(reservation_row.reserved_source_amount,
          reservation_row.reserved_amount,0))) FILTER(
            WHERE UPPER(BTRIM(COALESCE(reservation_row.status,'')))='SETTLED'
               OR reservation_row.settled_at_utc IS NOT NULL),0),2) AS settled_source_amount
      FROM public.pay_advance_reservations reservation_row
      WHERE reservation_row.pay_batch_item_id=item_page.id
    ) reservation ON true
  ), resolved AS (
    SELECT enriched.*,
      authority.owner_ids,authority.candidate_ids,authority.finance_case_ids,
      authority.resolved_timesheet_id,authority.resolved_candidate_id,
      authority.resolved_finance_case_id,authority.resolution_failure AS owner_failure,
      authority.evidence_json,
      ROUND(ABS(COALESCE(NULLIF(enriched.frozen_source_amount,0),
        NULLIF(enriched.settled_source_amount,0),NULLIF(enriched.amount_ex_vat,0),
        NULLIF(enriched.amount_inc_vat,0))),2) AS resolved_source_amount,
      (UPPER(BTRIM(COALESCE(enriched.settlement_status,'')))='SETTLED'
        OR enriched.settled_at_utc IS NOT NULL
        OR UPPER(BTRIM(COALESCE(enriched.transfer_status,'')))='COMPLETED'
        OR enriched.completed_at_utc IS NOT NULL
        OR enriched.reservation_has_settled) AND NOT enriched.is_reversed AS settled_authority
    FROM enriched
    CROSS JOIN LATERAL private.pay_workbench_financial_source_authority_v1(
      enriched.batch_candidate_candidate_id,
      ARRAY[enriched.timesheet_id,enriched.component_timesheet_id,enriched.case_timesheet_id],
      ARRAY[enriched.batch_candidate_candidate_id,enriched.component_candidate_id,
        enriched.case_candidate_id],
      ARRAY[enriched.finance_case_id,enriched.component_case_id],
      enriched.frozen_source_basis_json,enriched.frozen_component_snapshot_json,'FINANCE') authority
  ), classified AS (
    SELECT resolved.*,
      UPPER(BTRIM(COALESCE(resolved.frozen_component_key_type,
        resolved.component_key_type,''))) AS resolved_key_type,
      BTRIM(COALESCE(resolved.frozen_component_key_value,
        resolved.component_key_value,'')) AS resolved_key_value,
      CASE
        WHEN NOT resolved.settled_authority THEN NULL::text
        WHEN resolved.owner_failure IS NOT NULL THEN resolved.owner_failure
        WHEN resolved.resolved_source_amount IS NULL OR resolved.resolved_source_amount<=0
          THEN 'FROZEN_FINANCE_AMOUNT_INVALID'
        WHEN UPPER(BTRIM(COALESCE(resolved.frozen_component_key_type,
          resolved.component_key_type,''))) NOT IN
          ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE',
            'MANUAL_CARRY_FORWARD')
          OR NULLIF(BTRIM(COALESCE(resolved.frozen_component_key_value,
            resolved.component_key_value,'')),'') IS NULL
          THEN 'FROZEN_ECONOMIC_KEY_INVALID'
        WHEN UPPER(BTRIM(COALESCE(resolved.frozen_component_key_type,
          resolved.component_key_type,'')))='TS_DAY' AND NOT (
            BTRIM(COALESCE(resolved.frozen_component_key_value,
              resolved.component_key_value,'')) ~ '^\d{4}-\d{2}-\d{2}$'
            AND pg_input_is_valid(BTRIM(COALESCE(resolved.frozen_component_key_value,
              resolved.component_key_value,'')),'date')
            AND CASE WHEN pg_input_is_valid(BTRIM(COALESCE(
              resolved.frozen_component_key_value,resolved.component_key_value,'')),
              'date')
              THEN (BTRIM(COALESCE(resolved.frozen_component_key_value,
                resolved.component_key_value,''))::date)::text=
                BTRIM(COALESCE(resolved.frozen_component_key_value,
                  resolved.component_key_value,'')) ELSE false END)
          THEN 'FROZEN_ECONOMIC_KEY_INVALID'
      END AS final_failure
    FROM resolved
  )
  SELECT '05:'||classified.pay_batch_candidate_id::text||':'||classified.id::text,
    classified.pay_batch_candidate_id,classified.id,classified.resolved_timesheet_id,
    classified.resolved_candidate_id,classified.resolved_finance_case_id,
    classified.finance_component_id,classified.resolved_key_type,
    classified.resolved_key_value,
    ROUND(CASE WHEN UPPER(BTRIM(classified.item_type))='OVERPAYMENT_RECOVERY'
      THEN -1 ELSE 1 END*classified.resolved_source_amount,2),
    ROUND(CASE WHEN UPPER(BTRIM(classified.item_type))='OVERPAYMENT_RECOVERY'
      THEN -1 ELSE 1 END*COALESCE(NULLIF(ABS(classified.amount_inc_vat),0),
        classified.resolved_source_amount),2),
    classified.settled_authority,
    jsonb_build_object(
      'role','FINANCE_ITEM_AUTHORITY','pay_batch_item_id',classified.id,
      'pay_batch_candidate_id',classified.pay_batch_candidate_id,
      'item_type',UPPER(BTRIM(classified.item_type)),
      'settled_authority',classified.settled_authority,
      'owner_evidence',classified.evidence_json,
      'finance_component_id',classified.finance_component_id,
      'resolution_failure',classified.final_failure,
      'frozen_source_basis_json',classified.frozen_source_basis_json,
      'frozen_component_snapshot_json',classified.frozen_component_snapshot_json),
    md5(jsonb_build_object('pay_batch_item_id',classified.id,
      'pay_batch_candidate_id',classified.pay_batch_candidate_id,
      'resolved_timesheet_id',classified.resolved_timesheet_id,
      'resolved_candidate_id',classified.resolved_candidate_id,
      'resolved_finance_case_id',classified.resolved_finance_case_id,
      'finance_component_id',classified.finance_component_id,
      'key_type',classified.resolved_key_type,'key_value',classified.resolved_key_value,
      'amount_ex_vat',classified.resolved_source_amount,
      'settled_authority',classified.settled_authority)::text),
    classified.final_failure
  FROM classified
  ORDER BY classified.pay_batch_candidate_id,classified.id;
$function$;

ALTER FUNCTION private.pay_workbench_finance_item_authority_page_v1(
  uuid,uuid,uuid,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_finance_item_authority_page_v1(
  uuid,uuid,uuid,integer) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_finance_item_authority_page_v1(
  uuid,uuid,uuid,integer) TO postgres;
