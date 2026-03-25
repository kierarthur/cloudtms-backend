ALTER TABLE public.pay_advances
  ADD COLUMN IF NOT EXISTS taxability public.pay_finance_taxability_enum,
  ADD COLUMN IF NOT EXISTS routing_kind public.pay_finance_routing_kind_enum,
  ADD COLUMN IF NOT EXISTS oneoff_bank_details_required boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS public.pay_finance_case_oneoff_payout_bank_details (
  finance_case_id uuid PRIMARY KEY
    REFERENCES public.pay_advances(id)
    ON DELETE CASCADE,
  candidate_id uuid NOT NULL
    REFERENCES public.candidates(id)
    ON DELETE RESTRICT,
  beneficiary_name text NOT NULL,
  sort_code text NOT NULL,
  account_number text NOT NULL,
  bank_details_hash text NOT NULL,
  note text NULL,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  created_by_user_id uuid NULL,
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_by_user_id uuid NULL,
  CONSTRAINT pay_finance_case_oneoff_payout_bank_details_beneficiary_name_chk
    CHECK (nullif(btrim(beneficiary_name), '') IS NOT NULL),
  CONSTRAINT pay_finance_case_oneoff_payout_bank_details_sort_code_chk
    CHECK (nullif(btrim(sort_code), '') IS NOT NULL),
  CONSTRAINT pay_finance_case_oneoff_payout_bank_details_account_number_chk
    CHECK (nullif(btrim(account_number), '') IS NOT NULL),
  CONSTRAINT pay_finance_case_oneoff_payout_bank_details_bank_details_hash_chk
    CHECK (nullif(btrim(bank_details_hash), '') IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_pay_fin_case_oneoff_payout_bank_details_candidate
  ON public.pay_finance_case_oneoff_payout_bank_details(candidate_id);

CREATE INDEX IF NOT EXISTS idx_pay_fin_case_oneoff_payout_bank_details_hash
  ON public.pay_finance_case_oneoff_payout_bank_details(bank_details_hash);

ALTER TABLE public.pay_batch_items
  ADD COLUMN IF NOT EXISTS payout_instruction_snapshot_json jsonb;

ALTER TABLE public.pay_item_snoozes
  ADD COLUMN IF NOT EXISTS schedule_before_snooze_json jsonb,
  ADD COLUMN IF NOT EXISTS next_due_week_start_before_snooze date,
  ADD COLUMN IF NOT EXISTS schedule_after_snooze_json jsonb,
  ADD COLUMN IF NOT EXISTS next_due_week_start_after_snooze date,
  ADD COLUMN IF NOT EXISTS cancelled_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS cancelled_by_user_id uuid,
  ADD COLUMN IF NOT EXISTS cancel_reason text;


UPDATE public.pay_advances AS pa
SET
  taxability = CASE
    WHEN pa.taxability IS NOT NULL THEN pa.taxability
    WHEN pa.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
      THEN 'NON_TAXABLE'::public.pay_finance_taxability_enum
    ELSE pa.taxability
  END,
  routing_kind = CASE
    WHEN pa.routing_kind IS NOT NULL THEN pa.routing_kind
    WHEN pa.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
         AND upper(coalesce(c.pay_method, '')) = 'UMBRELLA'
      THEN 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum
    WHEN pa.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
         AND upper(coalesce(c.pay_method, '')) = 'PAYE'
      THEN 'NORMAL_PAY_ROUTE'::public.pay_finance_routing_kind_enum
    WHEN pa.case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
         AND upper(coalesce(c.pay_method, '')) = 'UMBRELLA'
      THEN 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum
    WHEN pa.case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
         AND upper(coalesce(c.pay_method, '')) = 'PAYE'
      THEN 'NORMAL_PAY_ROUTE'::public.pay_finance_routing_kind_enum
    ELSE pa.routing_kind
  END,
  oneoff_bank_details_required = CASE
    WHEN pa.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
         AND upper(coalesce(c.pay_method, '')) = 'UMBRELLA'
      THEN true
    ELSE pa.oneoff_bank_details_required
  END
FROM public.candidates AS c
WHERE c.id = pa.candidate_id
  AND (
    pa.taxability IS NULL
    OR pa.routing_kind IS NULL
    OR (
      pa.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
      AND upper(coalesce(c.pay_method, '')) = 'UMBRELLA'
      AND pa.oneoff_bank_details_required = false
    )
  );

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
  FROM public.pay_advance_reservations AS r
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
    FROM public.pay_batch_items AS pbi
    JOIN public.pay_batch_candidates AS pbc
      ON pbc.id = pbi.pay_batch_candidate_id
    JOIN public.pay_batches AS pb
      ON pb.id = pbc.pay_batch_id
    LEFT JOIN public.pay_advances AS pa_fallback
      ON pbi.finance_case_id IS NULL
     AND pbi.source_ref = ('advance:'::text || pa_fallback.id::text)
    WHERE pbi.finance_case_id IS NOT NULL
       OR (
         pbi.source_ref IS NOT NULL
         AND pbi.source_ref LIKE 'advance:%'
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
    FROM public.pay_batch_items AS pbi
    JOIN public.pay_batch_candidates AS pbc
      ON pbc.id = pbi.pay_batch_candidate_id
    JOIN public.pay_batches AS pb
      ON pb.id = pbc.pay_batch_id
    LEFT JOIN public.pay_advances AS pa_fallback
      ON pbi.finance_case_id IS NULL
     AND pbi.source_ref = ('advance:'::text || pa_fallback.id::text)
    WHERE pbi.item_type = ANY (
            ARRAY[
              'OVERPAYMENT_RECOVERY'::text,
              'LOAN_REPAYMENT'::text,
              'MANUAL_DEBT_RECOVERY'::text
            ]
          )
      AND (
        pbi.finance_case_id IS NOT NULL
        OR (
          pbi.source_ref IS NOT NULL
          AND pbi.source_ref LIKE 'advance:%'
        )
      )
  ) AS x
  WHERE x.finance_case_id IS NOT NULL
  ORDER BY
    x.finance_case_id,
    x.pay_date DESC NULLS LAST,
    x.pay_batch_id DESC
),
latest_finance_batch AS (
  SELECT DISTINCT ON (x.finance_case_id)
    x.finance_case_id,
    x.pay_batch_id AS latest_finance_pay_batch_id,
    x.pay_date AS latest_finance_pay_date,
    x.batch_status AS latest_finance_batch_status,
    x.batch_created_at_utc AS latest_finance_batch_created_at_utc,
    x.batch_completed_at_utc AS latest_finance_batch_completed_at_utc,
    x.batch_cancelled_at_utc AS latest_finance_batch_cancelled_at_utc,
    x.authoritative_payment_date AS latest_finance_authoritative_payment_date
  FROM (
    SELECT
      COALESCE(pbi.finance_case_id, pa_fallback.id) AS finance_case_id,
      pb.id AS pay_batch_id,
      pb.pay_date,
      pb.status AS batch_status,
      pb.created_at_utc AS batch_created_at_utc,
      pb.completed_at_utc AS batch_completed_at_utc,
      pb.cancelled_at_utc AS batch_cancelled_at_utc,
      pb.authoritative_payment_date
    FROM public.pay_batch_items AS pbi
    JOIN public.pay_batch_candidates AS pbc
      ON pbc.id = pbi.pay_batch_candidate_id
    JOIN public.pay_batches AS pb
      ON pb.id = pbc.pay_batch_id
    LEFT JOIN public.pay_advances AS pa_fallback
      ON pbi.finance_case_id IS NULL
     AND pbi.source_ref = ('advance:'::text || pa_fallback.id::text)
    WHERE pbi.finance_case_id IS NOT NULL
       OR (
         pbi.source_ref IS NOT NULL
         AND pbi.source_ref LIKE 'advance:%'
       )
  ) AS x
  WHERE x.finance_case_id IS NOT NULL
  ORDER BY
    x.finance_case_id,
    x.batch_created_at_utc DESC NULLS LAST,
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
  FROM public.pay_advances AS pa_1
  JOIN public.pay_item_snoozes AS s_1
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
        AND pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
    )::integer AS open_taxable_count,
    count(*) FILTER (
      WHERE pfc.closed_at_utc IS NULL
        AND pfc.remaining_source_amount > 0::numeric
        AND pfc.classification = 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
    )::integer AS open_reimbursement_count,
    count(*) FILTER (
      WHERE pfc.closed_at_utc IS NULL
        AND pfc.remaining_source_amount > 0::numeric
        AND pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        AND (
          pfc.is_resolution_stale
          OR pfc.saved_target_pay_method IS NULL
          OR pfc.saved_resolution_mode IS NULL
        )
    )::integer AS unresolved_taxable_count,
    count(*) FILTER (
      WHERE pfc.closed_at_utc IS NULL
        AND pfc.remaining_source_amount > 0::numeric
        AND pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        AND pfc.is_resolution_stale
    )::integer AS stale_count
  FROM public.pay_finance_case_components AS pfc
  GROUP BY pfc.finance_case_id
),
oneoff_bank_details AS (
  SELECT
    d.finance_case_id,
    d.candidate_id,
    d.beneficiary_name,
    d.sort_code,
    d.account_number,
    d.bank_details_hash,
    d.note,
    d.created_at_utc,
    d.created_by_user_id,
    d.updated_at_utc,
    d.updated_by_user_id
  FROM public.pay_finance_case_oneoff_payout_bank_details AS d
),
base_rows AS (
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
    ((COALESCE(cr.open_taxable_count, 0) > 0) AND (COALESCE(cr.open_reimbursement_count, 0) > 0)) AS is_mixed_case,
    COALESCE(cr.open_taxable_count, 0) AS open_taxable_count,
    COALESCE(cr.open_reimbursement_count, 0) AS open_reimbursement_count,
    COALESCE(cr.unresolved_taxable_count, 0) AS unresolved_taxable_count,
    COALESCE(cr.stale_count, 0) AS stale_count,
    jsonb_build_object(
      'open_taxable_count', COALESCE(cr.open_taxable_count, 0),
      'open_reimbursement_count', COALESCE(cr.open_reimbursement_count, 0),
      'unresolved_taxable_count', COALESCE(cr.unresolved_taxable_count, 0),
      'stale_count', COALESCE(cr.stale_count, 0),
      'is_mixed_case', ((COALESCE(cr.open_taxable_count, 0) > 0) AND (COALESCE(cr.open_reimbursement_count, 0) > 0))
    ) AS component_resolution_summary_json,

    pa.taxability,
    pa.routing_kind,
    pa.oneoff_bank_details_required,
    lfb.latest_finance_pay_batch_id,
    lfb.latest_finance_pay_date,
    lfb.latest_finance_batch_status,
    lfb.latest_finance_batch_created_at_utc,
    lfb.latest_finance_batch_completed_at_utc,
    lfb.latest_finance_batch_cancelled_at_utc,
    lfb.latest_finance_authoritative_payment_date,
    obd.finance_case_id AS oneoff_bank_finance_case_id,
    obd.candidate_id AS oneoff_bank_candidate_id,
    obd.beneficiary_name AS oneoff_bank_beneficiary_name,
    obd.sort_code AS oneoff_bank_sort_code,
    obd.account_number AS oneoff_bank_account_number,
    obd.bank_details_hash AS oneoff_bank_details_hash,
    obd.note AS oneoff_bank_note,
    obd.created_at_utc AS oneoff_bank_created_at_utc,
    obd.created_by_user_id AS oneoff_bank_created_by_user_id,
    obd.updated_at_utc AS oneoff_bank_updated_at_utc,
    obd.updated_by_user_id AS oneoff_bank_updated_by_user_id
  FROM public.pay_advances AS pa
  JOIN public.candidates AS c
    ON c.id = pa.candidate_id
  LEFT JOIN public.clients AS cli
    ON cli.id = pa.client_id
  LEFT JOIN reservation_rollup AS rr
    ON rr.finance_case_id = pa.id
  LEFT JOIN latest_remittance AS lr
    ON lr.finance_case_id = pa.id
  LEFT JOIN latest_recovery_batch AS lrb
    ON lrb.finance_case_id = pa.id
  LEFT JOIN latest_finance_batch AS lfb
    ON lfb.finance_case_id = pa.id
  LEFT JOIN active_snooze AS s
    ON s.finance_case_id = pa.id
  LEFT JOIN component_rollup AS cr
    ON cr.finance_case_id = pa.id
  LEFT JOIN oneoff_bank_details AS obd
    ON obd.finance_case_id = pa.id
),
derived_rows AS (
  SELECT
    br.*,
    CASE
      WHEN br.case_type = 'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum THEN 'CREDIT_ADJUSTMENTS'
      WHEN br.case_type = 'UNDERPAYMENT'::public.pay_finance_case_type_enum THEN 'CREDIT_ADJUSTMENTS'
      WHEN br.case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum THEN 'DEBIT_ADJUSTMENTS'
      WHEN br.case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum THEN 'DEBIT_ADJUSTMENTS'
      ELSE 'LOANS_PAYMENT_ADVANCES'
    END AS management_group,
    CASE
      WHEN br.latest_finance_batch_cancelled_at_utc IS NOT NULL
        OR upper(coalesce(br.latest_finance_batch_status, '')) = 'CANCELLED'
        OR upper(coalesce(br.payout_status::text, '')) = 'CANCELLED'
      THEN 'Cancelled'
      WHEN upper(coalesce(br.latest_finance_batch_status, '')) IN ('FAILED', 'ERROR')
      THEN 'Failed'
      WHEN (
        br.case_type IN (
          'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
          'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum,
          'UNDERPAYMENT'::public.pay_finance_case_type_enum
        )
        AND upper(coalesce(br.payout_status::text, '')) = 'PAID'
      )
      OR (
        br.case_type IN (
          'OVERPAYMENT'::public.pay_finance_case_type_enum,
          'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
        )
        AND (
          br.written_off_at_utc IS NOT NULL
          OR br.cleared_at_utc IS NOT NULL
          OR upper(coalesce(br.status::text, '')) IN ('PAID_OFF', 'CLEARED')
          OR coalesce(br.outstanding_amount, 0) <= 0::numeric
        )
      )
      THEN 'Paid'
      WHEN br.latest_finance_pay_batch_id IS NOT NULL
      THEN 'Drafted awaiting authorisation'
      ELSE 'Not processed yet'
    END AS lifecycle_status_display,
    (
      upper(coalesce(br.pay_method, '')) = 'UMBRELLA'
      AND br.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum
      AND br.case_type IN (
        'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
        'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
      )
      AND (
        br.case_type <> 'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
        OR br.taxability = 'NON_TAXABLE'::public.pay_finance_taxability_enum
      )
    ) AS is_candidate_directed_oneoff_payout,
    (
      br.routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum
    ) AS appears_on_umbrella_remittance,
    (
      upper(coalesce(br.pay_method, '')) = 'UMBRELLA'
      AND br.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum
      AND br.case_type IN (
        'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
        'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
      )
      AND (
        br.case_type <> 'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
        OR br.taxability = 'NON_TAXABLE'::public.pay_finance_taxability_enum
      )
    ) AS generates_candidate_payment_advice,
    (br.oneoff_bank_finance_case_id IS NOT NULL) AS oneoff_bank_details_present,
    (
      upper(coalesce(br.pay_method, '')) = 'UMBRELLA'
      AND br.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum
      AND br.case_type IN (
        'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
        'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
      )
      AND (
        br.case_type <> 'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
        OR br.taxability = 'NON_TAXABLE'::public.pay_finance_taxability_enum
      )
      AND (
        CASE
          WHEN br.latest_finance_batch_cancelled_at_utc IS NOT NULL
            OR upper(coalesce(br.latest_finance_batch_status, '')) = 'CANCELLED'
            OR upper(coalesce(br.payout_status::text, '')) = 'CANCELLED'
          THEN 'Cancelled'
          WHEN upper(coalesce(br.latest_finance_batch_status, '')) IN ('FAILED', 'ERROR')
          THEN 'Failed'
          WHEN (
            br.case_type IN (
              'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
              'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum,
              'UNDERPAYMENT'::public.pay_finance_case_type_enum
            )
            AND upper(coalesce(br.payout_status::text, '')) = 'PAID'
          )
          OR (
            br.case_type IN (
              'OVERPAYMENT'::public.pay_finance_case_type_enum,
              'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
            )
            AND (
              br.written_off_at_utc IS NOT NULL
              OR br.cleared_at_utc IS NOT NULL
              OR upper(coalesce(br.status::text, '')) IN ('PAID_OFF', 'CLEARED')
              OR coalesce(br.outstanding_amount, 0) <= 0::numeric
            )
          )
          THEN 'Paid'
          WHEN br.latest_finance_pay_batch_id IS NOT NULL
          THEN 'Drafted awaiting authorisation'
          ELSE 'Not processed yet'
        END
      ) = 'Not processed yet'
    ) AS oneoff_bank_details_editable,
    (
      br.case_type IN (
        'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
        'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
      )
      AND br.written_off_at_utc IS NULL
      AND br.cleared_at_utc IS NULL
      AND upper(coalesce(br.status::text, '')) NOT IN ('PAID_OFF', 'CLEARED')
      AND coalesce(br.outstanding_amount, 0) > 0::numeric
    ) AS snooze_allowed,
    (
      upper(coalesce(br.pay_method, '')) = 'UMBRELLA'
      AND br.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum
      AND br.case_type IN (
        'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
        'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
      )
      AND (
        br.case_type <> 'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
        OR br.taxability = 'NON_TAXABLE'::public.pay_finance_taxability_enum
      )
      AND (
        CASE
          WHEN br.latest_finance_batch_cancelled_at_utc IS NOT NULL
            OR upper(coalesce(br.latest_finance_batch_status, '')) = 'CANCELLED'
            OR upper(coalesce(br.payout_status::text, '')) = 'CANCELLED'
          THEN 'Cancelled'
          WHEN upper(coalesce(br.latest_finance_batch_status, '')) IN ('FAILED', 'ERROR')
          THEN 'Failed'
          WHEN (
            br.case_type IN (
              'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
              'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum,
              'UNDERPAYMENT'::public.pay_finance_case_type_enum
            )
            AND upper(coalesce(br.payout_status::text, '')) = 'PAID'
          )
          OR (
            br.case_type IN (
              'OVERPAYMENT'::public.pay_finance_case_type_enum,
              'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
            )
            AND (
              br.written_off_at_utc IS NOT NULL
              OR br.cleared_at_utc IS NOT NULL
              OR upper(coalesce(br.status::text, '')) IN ('PAID_OFF', 'CLEARED')
              OR coalesce(br.outstanding_amount, 0) <= 0::numeric
            )
          )
          THEN 'Paid'
          WHEN br.latest_finance_pay_batch_id IS NOT NULL
          THEN 'Drafted awaiting authorisation'
          ELSE 'Not processed yet'
        END
      ) = 'Not processed yet'
    ) AS edit_bank_details_allowed
  FROM base_rows AS br
)
SELECT
  dr.finance_case_id,
  dr.case_type,
  dr.advance_kind,
  dr.reason,
  dr.candidate_id,
  dr.candidate_tms_ref,
  dr.candidate_display_name,
  dr.candidate_first_name,
  dr.candidate_last_name,
  dr.pay_method,
  dr.client_id,
  dr.client_name,
  dr.linked_timesheet_id,
  dr.linked_shift_date,
  dr.created_at,
  dr.created_by,
  dr.updated_at,
  dr.status,
  dr.payout_status,
  dr.payout_pay_batch_id,
  dr.payout_transfer_id,
  dr.original_amount,
  dr.outstanding_amount,
  dr.weekly_due,
  dr.weeks_total,
  dr.start_week_start,
  dr.next_due_week_start,
  dr.schedule_json,
  dr.adjustment_comment,
  dr.source_original_paid_amount,
  dr.source_corrected_paid_amount,
  dr.minimum_earnings_threshold,
  dr.take_home_floor_override,
  dr.baseline_signature,
  dr.best_guess_hours,
  dr.notes,
  dr.written_off_at_utc,
  dr.written_off_by_user_id,
  dr.write_off_reason,
  dr.cleared_at_utc,
  dr.cleared_by_user_id,
  dr.active_reserved_amount,
  dr.reserved_amount,
  dr.committed_amount,
  dr.settled_amount,
  dr.released_amount,
  dr.active_reservation_count,
  dr.latest_reservation_created_at_utc,
  dr.latest_committed_at_utc,
  dr.latest_settled_at_utc,
  dr.latest_released_at_utc,
  dr.latest_recovery_pay_batch_id,
  dr.latest_recovery_pay_date,
  dr.latest_remittance_sent_at_utc,
  dr.latest_remittance_trigger_status,
  dr.last_remittance_error,
  dr.active_snooze_id,
  dr.active_snooze_kind,
  dr.active_snooze_until_date,
  dr.active_snooze_note,
  dr.active_snooze_created_at_utc,
  dr.active_snooze_updated_at_utc,
  dr.is_mixed_case,
  dr.open_taxable_count,
  dr.open_reimbursement_count,
  dr.unresolved_taxable_count,
  dr.stale_count,
  dr.component_resolution_summary_json,

  dr.taxability,
  dr.routing_kind,
  dr.oneoff_bank_details_present,
  dr.oneoff_bank_details_editable,
  dr.management_group,
  dr.lifecycle_status_display,
  dr.is_candidate_directed_oneoff_payout,
  dr.appears_on_umbrella_remittance,
  dr.generates_candidate_payment_advice,
  dr.snooze_allowed,
  dr.edit_bank_details_allowed
FROM derived_rows AS dr;

