-- Banking Pay bounded-scope Version 1.2.16.
-- Page the candidate's physical finance-movement items before any ownership,
-- scope, frozen-JSON, component or key filtering.  Every selected item is then
-- either authoritative in-scope evidence, explicit outside-scope evidence, or
-- a typed blocking authority failure.
--
-- Settled Policy-X evidence may retain a frozen rotation-family owner after
-- mutable case/component projection has moved to another member.  That frozen
-- owner is accepted only when every asserted owner is already sealed in the
-- same dependency unit.  Truly ownerless legacy PAID_OFF cases are retained as
-- explicit terminal evidence only after proving that they have no component,
-- reservation, correction, snooze, protection or outstanding authority.

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
      finance_case.id AS linked_case_row_id,
      finance_case.linked_timesheet_id AS case_timesheet_id,
      finance_case.candidate_id AS case_candidate_id,
      finance_case.status::text AS case_status,
      finance_case.advance_kind::text AS case_advance_kind,
      finance_case.case_type::text AS case_type,
      finance_case.outstanding_amount AS case_outstanding_amount,
      finance_case.cleared_at_utc AS case_cleared_at_utc,
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
  ), scoped_authority AS (
    SELECT resolved.*,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(resolved.evidence_json->>'frozen_owner_id','')),'') IS NOT NULL
         AND pg_input_is_valid(resolved.evidence_json->>'frozen_owner_id','uuid')
          THEN (resolved.evidence_json->>'frozen_owner_id')::uuid
      END AS frozen_owner_id,
      cardinality(COALESCE(resolved.owner_ids,ARRAY[]::uuid[])) AS authority_owner_count,
      (SELECT COUNT(*)::integer
       FROM private.banking_pay_workbench_economic_build_scope owner_scope
       WHERE owner_scope.build_id=p_build_id
         AND owner_scope.timesheet_id=ANY(COALESCE(resolved.owner_ids,ARRAY[]::uuid[])))
        AS authority_owner_in_scope_count,
      (SELECT COUNT(DISTINCT owner_scope.dependency_unit_key)::integer
       FROM private.banking_pay_workbench_economic_build_scope owner_scope
       WHERE owner_scope.build_id=p_build_id
         AND owner_scope.timesheet_id=ANY(COALESCE(resolved.owner_ids,ARRAY[]::uuid[])))
        AS authority_dependency_unit_count
    FROM resolved
  ), resolution_policy AS (
    SELECT scoped_authority.*,
      (
        scoped_authority.settled_authority
        AND scoped_authority.authority_failure='FINANCE_OWNER_CONFLICT'
        AND scoped_authority.frozen_owner_id IS NOT NULL
        AND scoped_authority.authority_owner_count>=2
        AND scoped_authority.authority_owner_in_scope_count=
          scoped_authority.authority_owner_count
        AND scoped_authority.authority_dependency_unit_count=1
        AND EXISTS(SELECT 1
          FROM private.banking_pay_workbench_economic_build_scope frozen_scope
          WHERE frozen_scope.build_id=p_build_id
            AND frozen_scope.timesheet_id=scoped_authority.frozen_owner_id)
        AND cardinality(COALESCE(scoped_authority.candidate_ids,ARRAY[]::uuid[]))=1
        AND scoped_authority.candidate_ids[1]=scoped_authority.batch_candidate_candidate_id
        AND cardinality(COALESCE(scoped_authority.finance_case_ids,ARRAY[]::uuid[]))=1
        AND cardinality(COALESCE(scoped_authority.finance_component_ids,ARRAY[]::uuid[]))<=1
        AND cardinality(COALESCE(scoped_authority.component_key_pairs,ARRAY[]::text[]))=1
        AND COALESCE((scoped_authority.evidence_json->>
          'invalid_frozen_uuid_count')::integer,0)=0
        AND COALESCE((scoped_authority.evidence_json->>
          'invalid_finance_component_uuid_count')::integer,0)=0
        AND COALESCE((scoped_authority.evidence_json->>
          'incomplete_component_key_count')::integer,0)=0
      ) AS use_frozen_same_unit_owner,
      (
        scoped_authority.settled_authority
        AND scoped_authority.authority_failure='FINANCE_OWNER_UNRESOLVED'
        AND scoped_authority.authority_owner_count=0
        AND cardinality(COALESCE(scoped_authority.candidate_ids,ARRAY[]::uuid[]))=1
        AND scoped_authority.candidate_ids[1]=scoped_authority.batch_candidate_candidate_id
        AND cardinality(COALESCE(scoped_authority.finance_case_ids,ARRAY[]::uuid[]))=1
        AND scoped_authority.linked_case_row_id=scoped_authority.finance_case_ids[1]
        AND scoped_authority.timesheet_id IS NULL
        AND scoped_authority.component_timesheet_id IS NULL
        AND scoped_authority.case_timesheet_id IS NULL
        AND scoped_authority.finance_component_id IS NULL
        AND scoped_authority.component_case_id IS NULL
        AND scoped_authority.frozen_owner_id IS NULL
        AND COALESCE(scoped_authority.frozen_source_basis_json,'{}'::jsonb)='{}'::jsonb
        AND COALESCE(scoped_authority.frozen_component_snapshot_json,'{}'::jsonb)='{}'::jsonb
        AND NULLIF(BTRIM(COALESCE(scoped_authority.frozen_component_key_type,'')),'') IS NULL
        AND NULLIF(BTRIM(COALESCE(scoped_authority.frozen_component_key_value,'')),'') IS NULL
        AND NULLIF(BTRIM(COALESCE(scoped_authority.operation_source_key,'')),'') IS NULL
        AND scoped_authority.source_ref='advance:'||scoped_authority.linked_case_row_id::text
        AND UPPER(BTRIM(COALESCE(scoped_authority.case_status,'')))='PAID_OFF'
        AND UPPER(BTRIM(COALESCE(scoped_authority.case_advance_kind,'')))='OVERPAYMENT'
        AND UPPER(BTRIM(COALESCE(scoped_authority.case_type,'')))='OVERPAYMENT'
        AND ROUND(COALESCE(scoped_authority.case_outstanding_amount,0),2)=0
        AND NOT EXISTS(SELECT 1
          FROM public.pay_finance_case_components legacy_component
          WHERE legacy_component.finance_case_id=scoped_authority.linked_case_row_id)
        AND NOT EXISTS(SELECT 1
          FROM public.pay_advance_reservations legacy_reservation
          WHERE (legacy_reservation.pay_batch_item_id=scoped_authority.id
              OR legacy_reservation.finance_case_id=scoped_authority.linked_case_row_id)
            AND UPPER(BTRIM(COALESCE(legacy_reservation.status,'')))
              NOT IN ('RELEASED','SETTLED'))
        AND NOT EXISTS(SELECT 1
          FROM public.pay_payment_correction_items legacy_correction
          WHERE (legacy_correction.pay_batch_item_id=scoped_authority.id
              OR legacy_correction.finance_case_id=scoped_authority.linked_case_row_id)
            AND (legacy_correction.timesheet_id IS NOT NULL
              OR UPPER(BTRIM(COALESCE(legacy_correction.status,'')))<>'APPLIED'))
        AND NOT EXISTS(SELECT 1
          FROM public.pay_item_snoozes legacy_snooze
          WHERE legacy_snooze.source_ref='advance:'||scoped_authority.linked_case_row_id::text
            AND legacy_snooze.cleared_at_utc IS NULL
            AND legacy_snooze.cancelled_at_utc IS NULL)
        AND NOT EXISTS(SELECT 1
          FROM public.pay_finance_case_events legacy_event
          WHERE legacy_event.finance_case_id=scoped_authority.linked_case_row_id
            AND (
              UPPER(BTRIM(COALESCE(legacy_event.event_type,''))) LIKE '%WRITE%OFF%'
              OR UPPER(BTRIM(COALESCE(legacy_event.event_type,''))) IN (
                'MANUAL_ECONOMIC_OVERRIDE','MANUAL_WRITE_OFF',
                'CASE_MANUAL_ECONOMIC_OVERRIDE','COMPONENT_MANUAL_ECONOMIC_OVERRIDE')
              OR UPPER(BTRIM(COALESCE(legacy_event.reason,''))) IN (
                'MANUAL_ECONOMIC_OVERRIDE','WRITE_OFF','COMPONENT_MANUAL_ECONOMIC_OVERRIDE')
              OR (UPPER(BTRIM(COALESCE(legacy_event.event_type,'')))='CASE_CLEARED'
                AND LOWER(BTRIM(COALESCE(legacy_event.reason,'')))='rail_settlement')
              OR (UPPER(BTRIM(COALESCE(legacy_event.event_type,'')))='CLEARED'
                AND UPPER(BTRIM(COALESCE(legacy_event.reason,'')))='PREVIEW_FINANCE_SYNC')
            ))
      ) AS legacy_terminal_unowned_evidence
    FROM scoped_authority
  ), effective_authority AS (
    SELECT resolution_policy.*,
      CASE WHEN resolution_policy.use_frozen_same_unit_owner
        THEN resolution_policy.frozen_owner_id
        ELSE resolution_policy.resolved_timesheet_id END AS effective_timesheet_id,
      CASE
        WHEN resolution_policy.use_frozen_same_unit_owner
          OR resolution_policy.legacy_terminal_unowned_evidence THEN NULL
        ELSE resolution_policy.authority_failure
      END AS effective_authority_failure,
      CASE
        WHEN resolution_policy.use_frozen_same_unit_owner
          THEN 'FROZEN_OWNER_SELECTED_SAME_DEPENDENCY_UNIT'
        WHEN resolution_policy.legacy_terminal_unowned_evidence
          THEN 'LEGACY_TERMINAL_UNOWNED_EVIDENCE'
        WHEN resolution_policy.authority_failure IS NOT NULL THEN 'BLOCKED_AUTHORITY_FAILURE'
        WHEN resolution_policy.settled_authority THEN 'RESOLVED_SETTLED_AUTHORITY'
        ELSE 'NON_SETTLED_EVIDENCE'
      END AS authority_resolution_status
    FROM resolution_policy
  ), classified AS (
    SELECT effective_authority.*,
      EXISTS(SELECT 1
        FROM private.banking_pay_workbench_economic_build_scope scope_row
        WHERE scope_row.build_id=p_build_id
          AND scope_row.timesheet_id=effective_authority.effective_timesheet_id) AS owner_in_scope,
      CASE
        WHEN effective_authority.effective_authority_failure IS NOT NULL
          THEN effective_authority.effective_authority_failure
        WHEN effective_authority.settled_authority
         AND EXISTS(SELECT 1
           FROM private.banking_pay_workbench_economic_build_scope scope_row
           WHERE scope_row.build_id=p_build_id
             AND scope_row.timesheet_id=effective_authority.effective_timesheet_id)
         AND (effective_authority.resolved_source_amount IS NULL
           OR effective_authority.resolved_source_amount<=0)
          THEN 'FROZEN_FINANCE_AMOUNT_INVALID'
        WHEN effective_authority.settled_authority
         AND EXISTS(SELECT 1
           FROM private.banking_pay_workbench_economic_build_scope scope_row
           WHERE scope_row.build_id=p_build_id
             AND scope_row.timesheet_id=effective_authority.effective_timesheet_id)
         AND (effective_authority.resolved_component_key_type NOT IN
            ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE',
              'MANUAL_CARRY_FORWARD')
          OR NULLIF(BTRIM(COALESCE(effective_authority.resolved_component_key_value,'')),'') IS NULL)
          THEN 'FROZEN_ECONOMIC_KEY_INVALID'
        WHEN effective_authority.settled_authority
         AND EXISTS(SELECT 1
           FROM private.banking_pay_workbench_economic_build_scope scope_row
           WHERE scope_row.build_id=p_build_id
             AND scope_row.timesheet_id=effective_authority.effective_timesheet_id)
         AND effective_authority.resolved_component_key_type='TS_DAY'
         AND NOT (
           effective_authority.resolved_component_key_value ~ '^\d{4}-\d{2}-\d{2}$'
           AND pg_input_is_valid(effective_authority.resolved_component_key_value,'date')
           AND CASE WHEN pg_input_is_valid(effective_authority.resolved_component_key_value,'date')
             THEN (effective_authority.resolved_component_key_value::date)::text=
               effective_authority.resolved_component_key_value ELSE false END)
          THEN 'FROZEN_ECONOMIC_KEY_INVALID'
      END AS final_failure
    FROM effective_authority
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
    projected.pay_batch_candidate_id,projected.id,projected.effective_timesheet_id,
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
      'authority_resolution_status',projected.authority_resolution_status,
      'owner_evidence',projected.evidence_json,
      'finance_case_id',projected.resolved_finance_case_id,
      'finance_component_id',projected.resolved_finance_component_id,
      'source_timesheet_id',projected.effective_timesheet_id,
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
      'resolved_timesheet_id',projected.effective_timesheet_id,
      'resolved_candidate_id',projected.resolved_candidate_id,
      'resolved_finance_case_id',projected.resolved_finance_case_id,
      'resolved_finance_component_id',projected.resolved_finance_component_id,
      'key_type',projected.resolved_component_key_type,
      'key_value',projected.resolved_component_key_value,
      'amount_ex_vat',projected.resolved_source_amount,
      'settled_authority',projected.settled_authority,
      'authoritative_in_scope',projected.authoritative_in_scope,
      'authority_resolution_status',projected.authority_resolution_status,
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
