-- Banking Pay bounded-scope Version 1.2.9.
-- Page the candidate's physical finance-movement items before any ownership,
-- scope, frozen-JSON, component or key filtering.  Every selected item is then
-- either authoritative in-scope evidence, explicit outside-scope evidence, or
-- a typed blocking authority failure.

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
LANGUAGE plpgsql
STABLE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
BEGIN
  RETURN QUERY
  WITH build AS (
    SELECT build_row.id,build_row.candidate_id
    FROM private.banking_pay_workbench_economic_builds build_row
    WHERE build_row.id=p_build_id
  ), scope_timesheet AS MATERIALIZED (
    SELECT scope_row.timesheet_id
    FROM private.banking_pay_workbench_economic_build_scope scope_row
    WHERE scope_row.build_id=p_build_id
  ), direct_owner_item AS MATERIALIZED (
    SELECT item.pay_batch_candidate_id,item.id AS item_id
    FROM build
    JOIN scope_timesheet scope_row ON true
    JOIN public.pay_batch_items item ON item.timesheet_id=scope_row.timesheet_id
    JOIN public.pay_batch_candidates batch_candidate
      ON batch_candidate.id=item.pay_batch_candidate_id
     AND batch_candidate.candidate_id=build.candidate_id
    WHERE item.item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
      AND COALESCE(item.is_voided,false) IS FALSE
      AND (p_last_batch_candidate_id IS NULL OR
        (item.pay_batch_candidate_id,item.id)>(p_last_batch_candidate_id,p_last_item_id))
    ORDER BY item.pay_batch_candidate_id,item.id
    LIMIT LEAST(GREATEST(COALESCE(p_limit,25),1),25)+1
  ), component_owner_item AS MATERIALIZED (
    SELECT item.pay_batch_candidate_id,item.id AS item_id
    FROM build
    JOIN scope_timesheet scope_row ON true
    JOIN public.pay_finance_case_components component
      ON component.linked_timesheet_id=scope_row.timesheet_id
    JOIN public.pay_batch_items item ON item.finance_component_id=component.id
    JOIN public.pay_batch_candidates batch_candidate
      ON batch_candidate.id=item.pay_batch_candidate_id
     AND batch_candidate.candidate_id=build.candidate_id
    WHERE item.item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
      AND COALESCE(item.is_voided,false) IS FALSE
      AND (p_last_batch_candidate_id IS NULL OR
        (item.pay_batch_candidate_id,item.id)>(p_last_batch_candidate_id,p_last_item_id))
    ORDER BY item.pay_batch_candidate_id,item.id
    LIMIT LEAST(GREATEST(COALESCE(p_limit,25),1),25)+1
  ), case_owner_item AS MATERIALIZED (
    SELECT item.pay_batch_candidate_id,item.id AS item_id
    FROM build
    JOIN scope_timesheet scope_row ON true
    JOIN public.pay_advances finance_case
      ON finance_case.linked_timesheet_id=scope_row.timesheet_id
    JOIN public.pay_batch_items item ON item.finance_case_id=finance_case.id
    JOIN public.pay_batch_candidates batch_candidate
      ON batch_candidate.id=item.pay_batch_candidate_id
     AND batch_candidate.candidate_id=build.candidate_id
    WHERE item.item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
      AND COALESCE(item.is_voided,false) IS FALSE
      AND (p_last_batch_candidate_id IS NULL OR
        (item.pay_batch_candidate_id,item.id)>(p_last_batch_candidate_id,p_last_item_id))
    ORDER BY item.pay_batch_candidate_id,item.id
    LIMIT LEAST(GREATEST(COALESCE(p_limit,25),1),25)+1
  ), frozen_owner_item AS MATERIALIZED (
    SELECT item.pay_batch_candidate_id,item.id AS item_id
    FROM build
    JOIN scope_timesheet scope_row ON true
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
        jsonb_build_object('carrier_timesheet_id',scope_row.timesheet_id::text)))
    JOIN public.pay_batch_candidates batch_candidate
      ON batch_candidate.id=item.pay_batch_candidate_id
     AND batch_candidate.candidate_id=build.candidate_id
    WHERE item.item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
      AND COALESCE(item.is_voided,false) IS FALSE
      AND (p_last_batch_candidate_id IS NULL OR
        (item.pay_batch_candidate_id,item.id)>(p_last_batch_candidate_id,p_last_item_id))
    ORDER BY item.pay_batch_candidate_id,item.id
    LIMIT LEAST(GREATEST(COALESCE(p_limit,25),1),25)+1
  ), candidate_settled_item AS MATERIALIZED (
    SELECT item.pay_batch_candidate_id,item.id AS item_id
    FROM build
    JOIN public.pay_batch_candidates batch_candidate
      ON batch_candidate.candidate_id=build.candidate_id
    JOIN public.pay_batch_items item ON item.pay_batch_candidate_id=batch_candidate.id
    WHERE item.item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
      AND COALESCE(item.is_voided,false) IS FALSE
      AND (UPPER(BTRIM(COALESCE(batch_candidate.settlement_status,'')))='SETTLED'
        OR batch_candidate.settled_at_utc IS NOT NULL)
      AND (p_last_batch_candidate_id IS NULL OR
        (item.pay_batch_candidate_id,item.id)>(p_last_batch_candidate_id,p_last_item_id))
    ORDER BY item.pay_batch_candidate_id,item.id
    LIMIT LEAST(GREATEST(COALESCE(p_limit,25),1),25)+1
  ), candidate_transfer_item AS MATERIALIZED (
    SELECT item.pay_batch_candidate_id,item.id AS item_id
    FROM build
    JOIN public.pay_batch_candidates batch_candidate
      ON batch_candidate.candidate_id=build.candidate_id
    JOIN public.pay_batch_items item ON item.pay_batch_candidate_id=batch_candidate.id
    JOIN public.pay_bank_transfers transfer ON transfer.id=item.pay_bank_transfer_id
    WHERE item.item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
      AND COALESCE(item.is_voided,false) IS FALSE
      AND (UPPER(BTRIM(COALESCE(transfer.status,'')))='COMPLETED'
        OR transfer.completed_at_utc IS NOT NULL)
      AND (p_last_batch_candidate_id IS NULL OR
        (item.pay_batch_candidate_id,item.id)>(p_last_batch_candidate_id,p_last_item_id))
    ORDER BY item.pay_batch_candidate_id,item.id
    LIMIT LEAST(GREATEST(COALESCE(p_limit,25),1),25)+1
  ), candidate_reservation_item AS MATERIALIZED (
    SELECT item.pay_batch_candidate_id,item.id AS item_id
    FROM build
    JOIN public.pay_batch_candidates batch_candidate
      ON batch_candidate.candidate_id=build.candidate_id
    JOIN public.pay_batch_items item ON item.pay_batch_candidate_id=batch_candidate.id
    JOIN public.pay_advance_reservations reservation
      ON reservation.pay_batch_item_id=item.id
    WHERE item.item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
      AND COALESCE(item.is_voided,false) IS FALSE
      AND (UPPER(BTRIM(COALESCE(reservation.status,'')))='SETTLED'
        OR reservation.settled_at_utc IS NOT NULL)
      AND (p_last_batch_candidate_id IS NULL OR
        (item.pay_batch_candidate_id,item.id)>(p_last_batch_candidate_id,p_last_item_id))
    ORDER BY item.pay_batch_candidate_id,item.id
    LIMIT LEAST(GREATEST(COALESCE(p_limit,25),1),25)+1
  ), candidate_item_key AS MATERIALIZED (
    SELECT * FROM direct_owner_item
    UNION ALL SELECT * FROM component_owner_item
    UNION ALL SELECT * FROM case_owner_item
    UNION ALL SELECT * FROM frozen_owner_item
    UNION ALL SELECT * FROM candidate_settled_item
    UNION ALL SELECT * FROM candidate_transfer_item
    UNION ALL SELECT * FROM candidate_reservation_item
  ), item_key AS MATERIALIZED (
    SELECT candidate_item_key.pay_batch_candidate_id,candidate_item_key.item_id
    FROM candidate_item_key
    GROUP BY candidate_item_key.pay_batch_candidate_id,candidate_item_key.item_id
    ORDER BY candidate_item_key.pay_batch_candidate_id,candidate_item_key.item_id
    LIMIT LEAST(GREATEST(COALESCE(p_limit,25),1),25)+1
  ), item_page AS MATERIALIZED (
    SELECT item.*,batch_candidate.candidate_id AS batch_candidate_candidate_id,
      batch_candidate.settlement_status,batch_candidate.settled_at_utc
    FROM item_key
    JOIN public.pay_batch_items item ON item.id=item_key.item_id
      AND item.pay_batch_candidate_id=item_key.pay_batch_candidate_id
    JOIN public.pay_batch_candidates batch_candidate
      ON batch_candidate.id=item_key.pay_batch_candidate_id
    ORDER BY item_key.pay_batch_candidate_id,item_key.item_id
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
      authority.finance_component_ids,authority.component_key_pairs,
      authority.resolved_timesheet_id,authority.resolved_candidate_id,
      authority.resolved_finance_case_id,authority.resolved_finance_component_id,
      authority.resolved_component_key_type,authority.resolved_component_key_value,
      authority.resolution_failure AS authority_failure,authority.evidence_json,
      ROUND(ABS(COALESCE(NULLIF(enriched.frozen_source_amount,0),
        NULLIF(enriched.settled_source_amount,0),NULLIF(enriched.amount_ex_vat,0),
        NULLIF(enriched.amount_inc_vat,0))),2) AS resolved_source_amount,
      (UPPER(BTRIM(COALESCE(enriched.settlement_status,'')))='SETTLED'
        OR enriched.settled_at_utc IS NOT NULL
        OR UPPER(BTRIM(COALESCE(enriched.transfer_status,'')))='COMPLETED'
        OR enriched.completed_at_utc IS NOT NULL
        OR enriched.reservation_has_settled) AND NOT enriched.is_reversed AS settled_authority
    FROM enriched
    CROSS JOIN LATERAL private.pay_workbench_financial_source_authority_v2(
      enriched.batch_candidate_candidate_id,
      ARRAY[enriched.timesheet_id,enriched.component_timesheet_id,enriched.case_timesheet_id],
      ARRAY[enriched.batch_candidate_candidate_id,enriched.component_candidate_id,
        enriched.case_candidate_id],
      ARRAY[enriched.finance_case_id,enriched.component_case_id],
      ARRAY[enriched.finance_component_id],
      jsonb_build_array(
        jsonb_build_object('source','PAY_BATCH_ITEM_FROZEN_COLUMNS',
          'key_type',enriched.frozen_component_key_type,
          'key_value',enriched.frozen_component_key_value),
        jsonb_build_object('source','FINANCE_COMPONENT',
          'key_type',enriched.component_key_type,
          'key_value',enriched.component_key_value)
      ),
      enriched.frozen_source_basis_json,enriched.frozen_component_snapshot_json,'FINANCE'
    ) authority
  ), classified AS (
    SELECT resolved.*,
      EXISTS(SELECT 1
        FROM private.banking_pay_workbench_economic_build_scope scope_row
        WHERE scope_row.build_id=p_build_id
          AND scope_row.timesheet_id=resolved.resolved_timesheet_id) AS owner_in_scope,
      CASE
        WHEN resolved.settled_authority AND resolved.authority_failure IS NOT NULL
          THEN resolved.authority_failure
        WHEN resolved.settled_authority
         AND (resolved.resolved_source_amount IS NULL OR resolved.resolved_source_amount<=0)
          THEN 'FROZEN_FINANCE_AMOUNT_INVALID'
        WHEN resolved.settled_authority
         AND (resolved.resolved_component_key_type NOT IN
            ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE',
              'MANUAL_CARRY_FORWARD')
          OR NULLIF(BTRIM(COALESCE(resolved.resolved_component_key_value,'')),'') IS NULL)
          THEN 'FROZEN_ECONOMIC_KEY_INVALID'
        WHEN resolved.settled_authority AND resolved.resolved_component_key_type='TS_DAY'
         AND NOT (
           resolved.resolved_component_key_value ~ '^\d{4}-\d{2}-\d{2}$'
           AND pg_input_is_valid(resolved.resolved_component_key_value,'date')
           AND CASE WHEN pg_input_is_valid(resolved.resolved_component_key_value,'date')
             THEN (resolved.resolved_component_key_value::date)::text=
               resolved.resolved_component_key_value ELSE false END)
          THEN 'FROZEN_ECONOMIC_KEY_INVALID'
      END AS final_failure
    FROM resolved
  ), projected AS (
    SELECT classified.*,
      classified.settled_authority AND classified.owner_in_scope
        AND classified.final_failure IS NULL AS authoritative_in_scope,
      md5(jsonb_build_object(
        'pay_batch_candidate_id',classified.pay_batch_candidate_id,
        'pay_batch_item_id',classified.id,'item_type',classified.item_type,
        'amount_ex_vat',classified.amount_ex_vat,'amount_inc_vat',classified.amount_inc_vat,
        'frozen_source_amount',classified.frozen_source_amount,
        'settlement_status',classified.settlement_status,
        'settled_at_utc',classified.settled_at_utc,
        'transfer_status',classified.transfer_status,
        'transfer_completed_at_utc',classified.completed_at_utc,
        'reservation_has_settled',classified.reservation_has_settled,
        'is_reversed',classified.is_reversed)::text) AS raw_physical_digest
    FROM classified
  )
  SELECT '05:'||projected.pay_batch_candidate_id::text||':'||projected.id::text,
    projected.pay_batch_candidate_id,projected.id,projected.resolved_timesheet_id,
    projected.resolved_candidate_id,projected.resolved_finance_case_id,
    projected.resolved_finance_component_id,projected.resolved_component_key_type,
    projected.resolved_component_key_value,
    ROUND(CASE WHEN UPPER(BTRIM(projected.item_type))='OVERPAYMENT_RECOVERY'
      THEN -1 ELSE 1 END*projected.resolved_source_amount,2),
    ROUND(CASE WHEN UPPER(BTRIM(projected.item_type))='OVERPAYMENT_RECOVERY'
      THEN -1 ELSE 1 END*COALESCE(NULLIF(ABS(projected.amount_inc_vat),0),
        projected.resolved_source_amount),2),
    projected.authoritative_in_scope,
    jsonb_build_object(
      'role','FINANCE_ITEM_AUTHORITY','pay_batch_item_id',projected.id,
      'pay_batch_candidate_id',projected.pay_batch_candidate_id,
      'item_type',UPPER(BTRIM(projected.item_type)),
      'settled_authority',projected.settled_authority,
      'authoritative_in_scope',projected.authoritative_in_scope,
      'owner_in_scope',projected.owner_in_scope,
      'evidence_only',NOT projected.authoritative_in_scope,
      'owner_evidence',projected.evidence_json,
      'finance_component_id',projected.resolved_finance_component_id,
      'source_timesheet_id',projected.resolved_timesheet_id,
      'raw_physical_source_key','05:'||projected.pay_batch_candidate_id::text||':'||projected.id::text,
      'raw_physical_digest',projected.raw_physical_digest,
      'raw_physical_amount_ex_vat',ROUND(CASE
        WHEN UPPER(BTRIM(projected.item_type))='OVERPAYMENT_RECOVERY' THEN -1 ELSE 1 END*
        ABS(COALESCE(NULLIF(projected.frozen_source_amount,0),
          NULLIF(projected.settled_source_amount,0),NULLIF(projected.amount_ex_vat,0),
          NULLIF(projected.amount_inc_vat,0),0)),2),
      'resolution_failure',projected.final_failure,
      'frozen_source_basis_json',projected.frozen_source_basis_json,
      'frozen_component_snapshot_json',projected.frozen_component_snapshot_json),
    md5(jsonb_build_object('pay_batch_item_id',projected.id,
      'pay_batch_candidate_id',projected.pay_batch_candidate_id,
      'resolved_timesheet_id',projected.resolved_timesheet_id,
      'resolved_candidate_id',projected.resolved_candidate_id,
      'resolved_finance_case_id',projected.resolved_finance_case_id,
      'resolved_finance_component_id',projected.resolved_finance_component_id,
      'key_type',projected.resolved_component_key_type,
      'key_value',projected.resolved_component_key_value,
      'amount_ex_vat',projected.resolved_source_amount,
      'settled_authority',projected.settled_authority,
      'authoritative_in_scope',projected.authoritative_in_scope,
      'raw_physical_digest',projected.raw_physical_digest)::text),
    projected.final_failure
  FROM projected
  ORDER BY projected.pay_batch_candidate_id,projected.id;
END;
$function$;

ALTER FUNCTION private.pay_workbench_finance_item_authority_page_v1(
  uuid,uuid,uuid,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_finance_item_authority_page_v1(
  uuid,uuid,uuid,integer) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_finance_item_authority_page_v1(
  uuid,uuid,uuid,integer) TO postgres;
