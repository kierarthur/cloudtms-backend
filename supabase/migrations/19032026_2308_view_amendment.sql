DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_type AS t
    JOIN pg_namespace AS n
      ON n.oid = t.typnamespace
    JOIN pg_enum AS e
      ON e.enumtypid = t.oid
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_finance_component_classification_enum'
      AND e.enumlabel = 'NET_PAY_FIXED_RECOVERY'
  ) THEN
    RAISE EXCEPTION
      'Enum value % is missing from public.pay_finance_component_classification_enum. Apply the enum migration first.',
      'NET_PAY_FIXED_RECOVERY';
  END IF;
END
$$;

CREATE OR REPLACE VIEW public.v_finance_cases_register AS
WITH reservation_rollup AS (
  SELECT
    r.finance_case_id,
    round(
      COALESCE(
        sum(
          CASE
            WHEN r.status = ANY (ARRAY['RESERVED'::text, 'COMMITTED'::text]) THEN r.reserved_amount
            ELSE 0::numeric
          END
        ),
        0::numeric
      ),
      2
    ) AS active_reserved_amount,
    round(
      COALESCE(
        sum(
          CASE
            WHEN r.status = 'RESERVED'::text THEN r.reserved_amount
            ELSE 0::numeric
          END
        ),
        0::numeric
      ),
      2
    ) AS reserved_amount,
    round(
      COALESCE(
        sum(
          CASE
            WHEN r.status = 'COMMITTED'::text THEN r.reserved_amount
            ELSE 0::numeric
          END
        ),
        0::numeric
      ),
      2
    ) AS committed_amount,
    round(
      COALESCE(
        sum(
          CASE
            WHEN r.status = 'SETTLED'::text THEN r.reserved_amount
            ELSE 0::numeric
          END
        ),
        0::numeric
      ),
      2
    ) AS settled_amount,
    round(
      COALESCE(
        sum(
          CASE
            WHEN r.status = 'RELEASED'::text THEN r.reserved_amount
            ELSE 0::numeric
          END
        ),
        0::numeric
      ),
      2
    ) AS released_amount,
    count(*) FILTER (
      WHERE r.status = ANY (ARRAY['RESERVED'::text, 'COMMITTED'::text])
    )::integer AS active_reservation_count,
    max(r.created_at_utc) AS latest_reservation_created_at_utc,
    max(r.committed_at_utc) AS latest_committed_at_utc,
    max(r.settled_at_utc) AS latest_settled_at_utc,
    max(r.released_at_utc) AS latest_released_at_utc
  FROM pay_advance_reservations AS r
  GROUP BY r.finance_case_id
),
latest_remittance AS (
  SELECT DISTINCT ON (x.finance_case_id)
    x.finance_case_id,
    x.pay_batch_id,
    x.pay_date,
    x.remittance_sent_at_utc,
    x.remittance_trigger_status,
    x.last_remittance_error
  FROM (
    SELECT
      COALESCE(pbi.finance_case_id, pa_fallback.id) AS finance_case_id,
      pbc.pay_batch_id,
      pb.pay_date,
      pbc.remittance_sent_at_utc,
      pbc.remittance_trigger_status,
      pbc.last_remittance_error
    FROM pay_batch_items AS pbi
    JOIN pay_batch_candidates AS pbc
      ON pbc.id = pbi.pay_batch_candidate_id
    JOIN pay_batches AS pb
      ON pb.id = pbc.pay_batch_id
    LEFT JOIN pay_advances AS pa_fallback
      ON pbi.finance_case_id IS NULL
     AND pbi.source_ref = ('advance:'::text || pa_fallback.id::text)
    WHERE pbi.finance_case_id IS NOT NULL
       OR (
            pbi.source_ref IS NOT NULL
        AND pbi.source_ref ~~ 'advance:%'::text
       )
  ) AS x
  WHERE x.finance_case_id IS NOT NULL
  ORDER BY
    x.finance_case_id,
    x.remittance_sent_at_utc DESC NULLS LAST,
    x.pay_date DESC NULLS LAST,
    x.pay_batch_id DESC
),
latest_recovery_batch AS (
  SELECT DISTINCT ON (x.finance_case_id)
    x.finance_case_id,
    x.pay_batch_id AS latest_recovery_pay_batch_id,
    x.pay_date AS latest_recovery_pay_date
  FROM (
    SELECT
      COALESCE(pbi.finance_case_id, pa_fallback.id) AS finance_case_id,
      pbc.pay_batch_id,
      pb.pay_date
    FROM pay_batch_items AS pbi
    JOIN pay_batch_candidates AS pbc
      ON pbc.id = pbi.pay_batch_candidate_id
    JOIN pay_batches AS pb
      ON pb.id = pbc.pay_batch_id
    LEFT JOIN pay_advances AS pa_fallback
      ON pbi.finance_case_id IS NULL
     AND pbi.source_ref = ('advance:'::text || pa_fallback.id::text)
    WHERE (
            pbi.item_type = ANY (
              ARRAY[
                'OVERPAYMENT_RECOVERY'::text,
                'LOAN_REPAYMENT'::text,
                'MANUAL_DEBT_RECOVERY'::text
              ]
            )
          )
      AND (
            pbi.finance_case_id IS NOT NULL
         OR (
              pbi.source_ref IS NOT NULL
          AND pbi.source_ref ~~ 'advance:%'::text
            )
          )
  ) AS x
  WHERE x.finance_case_id IS NOT NULL
  ORDER BY
    x.finance_case_id,
    x.pay_date DESC NULLS LAST,
    x.pay_batch_id DESC
),
active_snooze AS (
  SELECT DISTINCT ON (pa_1.id)
    pa_1.id AS finance_case_id,
    s_1.id AS snooze_id,
    s_1.snooze_kind,
    s_1.snooze_until_date,
    s_1.note,
    s_1.created_at_utc,
    s_1.updated_at_utc,
    s_1.created_by_user_id,
    s_1.updated_by_user_id
  FROM pay_advances AS pa_1
  JOIN pay_item_snoozes AS s_1
    ON s_1.source_ref = ('advance:'::text || pa_1.id::text)
   AND s_1.cleared_at_utc IS NULL
  ORDER BY
    pa_1.id,
    s_1.updated_at_utc DESC NULLS LAST,
    s_1.created_at_utc DESC,
    s_1.id DESC
),
component_rollup AS (
  SELECT
    pfc.finance_case_id,
    count(*) FILTER (
      WHERE pfc.closed_at_utc IS NULL
        AND pfc.remaining_source_amount > 0::numeric
        AND pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::pay_finance_component_classification_enum
    )::integer AS open_taxable_count,
    count(*) FILTER (
      WHERE pfc.closed_at_utc IS NULL
        AND pfc.remaining_source_amount > 0::numeric
        AND pfc.classification = 'REIMBURSEMENT_GROSS_FIXED'::pay_finance_component_classification_enum
    )::integer AS open_reimbursement_count,
    count(*) FILTER (
      WHERE pfc.closed_at_utc IS NULL
        AND pfc.remaining_source_amount > 0::numeric
        AND pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::pay_finance_component_classification_enum
        AND (
              pfc.is_resolution_stale
           OR pfc.saved_target_pay_method IS NULL
           OR pfc.saved_resolution_mode IS NULL
        )
    )::integer AS unresolved_taxable_count,
    count(*) FILTER (
      WHERE pfc.closed_at_utc IS NULL
        AND pfc.remaining_source_amount > 0::numeric
        AND pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::pay_finance_component_classification_enum
        AND pfc.is_resolution_stale
    )::integer AS stale_count
  FROM pay_finance_case_components AS pfc
  GROUP BY pfc.finance_case_id
)
SELECT
  pa.id AS finance_case_id,
  pa.case_type,
  pa.advance_kind,
  pa.reason,
  pa.candidate_id,
  c.tms_ref AS candidate_tms_ref,
  c.display_name AS candidate_display_name,
  c.first_name AS candidate_first_name,
  c.last_name AS candidate_last_name,
  c.pay_method,
  pa.client_id,
  cli.name AS client_name,
  pa.linked_timesheet_id,
  pa.linked_shift_date,
  pa.created_at,
  pa.created_by,
  pa.updated_at,
  pa.status,
  pa.payout_status,
  pa.payout_pay_batch_id,
  pa.payout_transfer_id,
  pa.original_amount,
  pa.outstanding_amount,
  pa.weekly_due,
  pa.weeks_total,
  pa.start_week_start,
  pa.next_due_week_start,
  pa.schedule_json,
  pa.adjustment_comment,
  pa.source_original_paid_amount,
  pa.source_corrected_paid_amount,
  pa.minimum_earnings_threshold,
  pa.take_home_floor_override,
  pa.baseline_signature,
  pa.best_guess_hours,
  pa.notes,
  pa.written_off_at_utc,
  pa.written_off_by_user_id,
  pa.write_off_reason,
  pa.cleared_at_utc,
  pa.cleared_by_user_id,
  rr.active_reserved_amount,
  rr.reserved_amount,
  rr.committed_amount,
  rr.settled_amount,
  rr.released_amount,
  rr.active_reservation_count,
  rr.latest_reservation_created_at_utc,
  rr.latest_committed_at_utc,
  rr.latest_settled_at_utc,
  rr.latest_released_at_utc,
  lrb.latest_recovery_pay_batch_id,
  lrb.latest_recovery_pay_date,
  lr.remittance_sent_at_utc AS latest_remittance_sent_at_utc,
  lr.remittance_trigger_status AS latest_remittance_trigger_status,
  lr.last_remittance_error,
  s.snooze_id AS active_snooze_id,
  s.snooze_kind AS active_snooze_kind,
  s.snooze_until_date AS active_snooze_until_date,
  s.note AS active_snooze_note,
  s.created_at_utc AS active_snooze_created_at_utc,
  s.updated_at_utc AS active_snooze_updated_at_utc,
  COALESCE(cr.open_taxable_count, 0) > 0 AND COALESCE(cr.open_reimbursement_count, 0) > 0 AS is_mixed_case,
  COALESCE(cr.open_taxable_count, 0) AS open_taxable_count,
  COALESCE(cr.open_reimbursement_count, 0) AS open_reimbursement_count,
  COALESCE(cr.unresolved_taxable_count, 0) AS unresolved_taxable_count,
  COALESCE(cr.stale_count, 0) AS stale_count,
  jsonb_build_object(
    'open_taxable_count', COALESCE(cr.open_taxable_count, 0),
    'open_reimbursement_count', COALESCE(cr.open_reimbursement_count, 0),
    'unresolved_taxable_count', COALESCE(cr.unresolved_taxable_count, 0),
    'stale_count', COALESCE(cr.stale_count, 0),
    'is_mixed_case', COALESCE(cr.open_taxable_count, 0) > 0 AND COALESCE(cr.open_reimbursement_count, 0) > 0
  ) AS component_resolution_summary_json
FROM pay_advances AS pa
JOIN candidates AS c
  ON c.id = pa.candidate_id
LEFT JOIN clients AS cli
  ON cli.id = pa.client_id
LEFT JOIN reservation_rollup AS rr
  ON rr.finance_case_id = pa.id
LEFT JOIN latest_remittance AS lr
  ON lr.finance_case_id = pa.id
LEFT JOIN latest_recovery_batch AS lrb
  ON lrb.finance_case_id = pa.id
LEFT JOIN active_snooze AS s
  ON s.finance_case_id = pa.id
LEFT JOIN component_rollup AS cr
  ON cr.finance_case_id = pa.id;
