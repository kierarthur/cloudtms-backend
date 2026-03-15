DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_finance_case_type_enum'
  )
  AND NOT EXISTS (
    SELECT 1
    FROM pg_enum e
    JOIN pg_type t ON t.oid = e.enumtypid
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_finance_case_type_enum'
      AND e.enumlabel = 'UNDERPAYMENT'
  ) THEN
    ALTER TYPE public.pay_finance_case_type_enum ADD VALUE 'UNDERPAYMENT';
  END IF;
END
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_advance_reason_enum'
  )
  AND NOT EXISTS (
    SELECT 1
    FROM pg_enum e
    JOIN pg_type t ON t.oid = e.enumtypid
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_advance_reason_enum'
      AND e.enumlabel = 'UNDERPAYMENT'
  ) THEN
    ALTER TYPE public.pay_advance_reason_enum ADD VALUE 'UNDERPAYMENT';
  END IF;
END
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_advance_kind_enum'
  )
  AND NOT EXISTS (
    SELECT 1
    FROM pg_enum e
    JOIN pg_type t ON t.oid = e.enumtypid
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_advance_kind_enum'
      AND e.enumlabel = 'UNDERPAYMENT'
  ) THEN
    ALTER TYPE public.pay_advance_kind_enum ADD VALUE 'UNDERPAYMENT';
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_finance_component_classification_enum'
  ) THEN
    CREATE TYPE public.pay_finance_component_classification_enum AS ENUM (
      'TAXABLE_CHANNEL_SENSITIVE',
      'REIMBURSEMENT_GROSS_FIXED'
    );
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_finance_component_resolution_mode_enum'
  ) THEN
    CREATE TYPE public.pay_finance_component_resolution_mode_enum AS ENUM (
      'SUGGESTED_EQUIVALENT_BASIS',
      'MANUAL_REPLACEMENT_RATE',
      'MANUAL_AMOUNT'
    );
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS public.pay_finance_case_components (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  finance_case_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  client_id uuid NULL,
  linked_timesheet_id uuid NULL,
  source_family_key text NOT NULL,
  component_key_type text NOT NULL,
  component_key_value text NOT NULL,
  classification public.pay_finance_component_classification_enum NOT NULL,
  source_pay_method text NOT NULL,
  source_basis_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_amount numeric(12,2) NOT NULL DEFAULT 0,
  remaining_source_amount numeric(12,2) NOT NULL DEFAULT 0,
  saved_target_pay_method text NULL,
  saved_resolution_mode public.pay_finance_component_resolution_mode_enum NULL,
  saved_resolution_payload_json jsonb NULL,
  saved_resolution_result_json jsonb NULL,
  resolution_fingerprint text NULL,
  is_resolution_stale boolean NOT NULL DEFAULT false,
  stale_reason text NULL,
  allocation_priority_group integer NOT NULL DEFAULT 0,
  allocation_priority_order integer NOT NULL DEFAULT 0,
  created_at_utc timestamp with time zone NOT NULL DEFAULT now(),
  updated_at_utc timestamp with time zone NOT NULL DEFAULT now(),
  resolved_at_utc timestamp with time zone NULL,
  closed_at_utc timestamp with time zone NULL,
  CONSTRAINT pay_finance_case_components_pkey PRIMARY KEY (id),
  CONSTRAINT pay_finance_case_components_source_amount_nonneg CHECK (source_amount >= 0),
  CONSTRAINT pay_finance_case_components_remaining_source_amount_nonneg CHECK (remaining_source_amount >= 0)
);

ALTER TABLE public.pay_finance_case_components
  ADD COLUMN IF NOT EXISTS candidate_id uuid,
  ADD COLUMN IF NOT EXISTS client_id uuid,
  ADD COLUMN IF NOT EXISTS linked_timesheet_id uuid,
  ADD COLUMN IF NOT EXISTS source_family_key text,
  ADD COLUMN IF NOT EXISTS component_key_type text,
  ADD COLUMN IF NOT EXISTS component_key_value text,
  ADD COLUMN IF NOT EXISTS classification public.pay_finance_component_classification_enum,
  ADD COLUMN IF NOT EXISTS source_pay_method text,
  ADD COLUMN IF NOT EXISTS source_basis_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS source_amount numeric(12,2) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS remaining_source_amount numeric(12,2) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS saved_target_pay_method text,
  ADD COLUMN IF NOT EXISTS saved_resolution_mode public.pay_finance_component_resolution_mode_enum,
  ADD COLUMN IF NOT EXISTS saved_resolution_payload_json jsonb,
  ADD COLUMN IF NOT EXISTS saved_resolution_result_json jsonb,
  ADD COLUMN IF NOT EXISTS resolution_fingerprint text,
  ADD COLUMN IF NOT EXISTS is_resolution_stale boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS stale_reason text,
  ADD COLUMN IF NOT EXISTS allocation_priority_group integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS allocation_priority_order integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS created_at_utc timestamp with time zone NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at_utc timestamp with time zone NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS resolved_at_utc timestamp with time zone,
  ADD COLUMN IF NOT EXISTS closed_at_utc timestamp with time zone;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_finance_case_components_finance_case_id_fkey'
      AND conrelid = 'public.pay_finance_case_components'::regclass
  ) THEN
    ALTER TABLE public.pay_finance_case_components
      ADD CONSTRAINT pay_finance_case_components_finance_case_id_fkey
      FOREIGN KEY (finance_case_id)
      REFERENCES public.pay_advances(id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_finance_case_components_candidate_id_fkey'
      AND conrelid = 'public.pay_finance_case_components'::regclass
  ) THEN
    ALTER TABLE public.pay_finance_case_components
      ADD CONSTRAINT pay_finance_case_components_candidate_id_fkey
      FOREIGN KEY (candidate_id)
      REFERENCES public.candidates(id);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_finance_case_components_client_id_fkey'
      AND conrelid = 'public.pay_finance_case_components'::regclass
  ) THEN
    ALTER TABLE public.pay_finance_case_components
      ADD CONSTRAINT pay_finance_case_components_client_id_fkey
      FOREIGN KEY (client_id)
      REFERENCES public.clients(id);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_finance_case_components_linked_timesheet_id_fkey'
      AND conrelid = 'public.pay_finance_case_components'::regclass
  ) THEN
    ALTER TABLE public.pay_finance_case_components
      ADD CONSTRAINT pay_finance_case_components_linked_timesheet_id_fkey
      FOREIGN KEY (linked_timesheet_id)
      REFERENCES public.timesheets(timesheet_id);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_finance_case_components_source_amount_nonneg'
      AND conrelid = 'public.pay_finance_case_components'::regclass
  ) THEN
    ALTER TABLE public.pay_finance_case_components
      ADD CONSTRAINT pay_finance_case_components_source_amount_nonneg
      CHECK (source_amount >= 0);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_finance_case_components_remaining_source_amount_nonneg'
      AND conrelid = 'public.pay_finance_case_components'::regclass
  ) THEN
    ALTER TABLE public.pay_finance_case_components
      ADD CONSTRAINT pay_finance_case_components_remaining_source_amount_nonneg
      CHECK (remaining_source_amount >= 0);
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_pay_finance_case_components_case_id
  ON public.pay_finance_case_components (finance_case_id);

CREATE INDEX IF NOT EXISTS idx_pay_finance_case_components_candidate_id
  ON public.pay_finance_case_components (candidate_id);

CREATE INDEX IF NOT EXISTS idx_pay_finance_case_components_linked_timesheet_id
  ON public.pay_finance_case_components (linked_timesheet_id);

CREATE INDEX IF NOT EXISTS idx_pay_finance_case_components_classification
  ON public.pay_finance_case_components (classification);

CREATE INDEX IF NOT EXISTS idx_pay_finance_case_components_stale_open
  ON public.pay_finance_case_components (finance_case_id, is_resolution_stale)
  WHERE closed_at_utc IS NULL AND remaining_source_amount > 0;

CREATE INDEX IF NOT EXISTS idx_pay_finance_case_components_open
  ON public.pay_finance_case_components (
    finance_case_id,
    allocation_priority_group,
    allocation_priority_order,
    created_at_utc
  )
  WHERE closed_at_utc IS NULL AND remaining_source_amount > 0;

CREATE UNIQUE INDEX IF NOT EXISTS uq_pay_finance_case_components_active_identity
  ON public.pay_finance_case_components (
    finance_case_id,
    source_family_key,
    component_key_type,
    component_key_value
  )
  WHERE closed_at_utc IS NULL;

ALTER TABLE public.pay_advance_reservations
  ADD COLUMN IF NOT EXISTS finance_component_id uuid,
  ADD COLUMN IF NOT EXISTS frozen_component_snapshot_json jsonb,
  ADD COLUMN IF NOT EXISTS frozen_component_key_type text,
  ADD COLUMN IF NOT EXISTS frozen_component_key_value text,
  ADD COLUMN IF NOT EXISTS frozen_component_classification public.pay_finance_component_classification_enum,
  ADD COLUMN IF NOT EXISTS frozen_source_basis_json jsonb,
  ADD COLUMN IF NOT EXISTS frozen_source_pay_method text,
  ADD COLUMN IF NOT EXISTS frozen_target_pay_method text,
  ADD COLUMN IF NOT EXISTS frozen_resolution_mode public.pay_finance_component_resolution_mode_enum,
  ADD COLUMN IF NOT EXISTS frozen_resolution_payload_json jsonb,
  ADD COLUMN IF NOT EXISTS frozen_resolution_result_json jsonb,
  ADD COLUMN IF NOT EXISTS reserved_source_amount numeric(12,2),
  ADD COLUMN IF NOT EXISTS frozen_rounded_target_amount numeric(12,2);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_advance_reservations_finance_component_id_fkey'
      AND conrelid = 'public.pay_advance_reservations'::regclass
  ) THEN
    ALTER TABLE public.pay_advance_reservations
      ADD CONSTRAINT pay_advance_reservations_finance_component_id_fkey
      FOREIGN KEY (finance_component_id)
      REFERENCES public.pay_finance_case_components(id)
      ON DELETE SET NULL;
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_pay_advance_reservations_finance_component_id
  ON public.pay_advance_reservations (finance_component_id);

CREATE INDEX IF NOT EXISTS idx_pay_advance_reservations_finance_component_status
  ON public.pay_advance_reservations (finance_component_id, status)
  WHERE finance_component_id IS NOT NULL;

ALTER TABLE public.pay_batch_items
  ADD COLUMN IF NOT EXISTS finance_component_id uuid,
  ADD COLUMN IF NOT EXISTS frozen_component_snapshot_json jsonb,
  ADD COLUMN IF NOT EXISTS frozen_component_key_type text,
  ADD COLUMN IF NOT EXISTS frozen_component_key_value text,
  ADD COLUMN IF NOT EXISTS frozen_component_classification public.pay_finance_component_classification_enum,
  ADD COLUMN IF NOT EXISTS frozen_source_basis_json jsonb,
  ADD COLUMN IF NOT EXISTS frozen_source_pay_method text,
  ADD COLUMN IF NOT EXISTS frozen_target_pay_method text,
  ADD COLUMN IF NOT EXISTS frozen_resolution_mode public.pay_finance_component_resolution_mode_enum,
  ADD COLUMN IF NOT EXISTS frozen_resolution_payload_json jsonb,
  ADD COLUMN IF NOT EXISTS frozen_resolution_result_json jsonb,
  ADD COLUMN IF NOT EXISTS frozen_source_amount numeric(12,2),
  ADD COLUMN IF NOT EXISTS frozen_target_amount_ex_vat numeric(12,2),
  ADD COLUMN IF NOT EXISTS frozen_target_amount_vat numeric(12,2),
  ADD COLUMN IF NOT EXISTS frozen_target_amount_inc_vat numeric(12,2);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_batch_items_finance_component_id_fkey'
      AND conrelid = 'public.pay_batch_items'::regclass
  ) THEN
    ALTER TABLE public.pay_batch_items
      ADD CONSTRAINT pay_batch_items_finance_component_id_fkey
      FOREIGN KEY (finance_component_id)
      REFERENCES public.pay_finance_case_components(id)
      ON DELETE SET NULL;
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_pay_batch_items_finance_component_id
  ON public.pay_batch_items (finance_component_id);

ALTER TABLE public.pay_finance_case_events
  ADD COLUMN IF NOT EXISTS finance_component_id uuid;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_finance_case_events_finance_component_id_fkey'
      AND conrelid = 'public.pay_finance_case_events'::regclass
  ) THEN
    ALTER TABLE public.pay_finance_case_events
      ADD CONSTRAINT pay_finance_case_events_finance_component_id_fkey
      FOREIGN KEY (finance_component_id)
      REFERENCES public.pay_finance_case_components(id)
      ON DELETE SET NULL;
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_pay_finance_case_events_component_at
  ON public.pay_finance_case_events (finance_component_id, event_at_utc DESC)
  WHERE finance_component_id IS NOT NULL;

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
  FROM public.pay_advance_reservations r
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
    FROM public.pay_batch_items pbi
    JOIN public.pay_batch_candidates pbc
      ON pbc.id = pbi.pay_batch_candidate_id
    JOIN public.pay_batches pb
      ON pb.id = pbc.pay_batch_id
    LEFT JOIN public.pay_advances pa_fallback
      ON pbi.finance_case_id IS NULL
     AND pbi.source_ref = ('advance:'::text || pa_fallback.id::text)
    WHERE pbi.finance_case_id IS NOT NULL
       OR (pbi.source_ref IS NOT NULL AND pbi.source_ref LIKE 'advance:%')
  ) x
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
    FROM public.pay_batch_items pbi
    JOIN public.pay_batch_candidates pbc
      ON pbc.id = pbi.pay_batch_candidate_id
    JOIN public.pay_batches pb
      ON pb.id = pbc.pay_batch_id
    LEFT JOIN public.pay_advances pa_fallback
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
        OR (pbi.source_ref IS NOT NULL AND pbi.source_ref LIKE 'advance:%')
      )
  ) x
  WHERE x.finance_case_id IS NOT NULL
  ORDER BY
    x.finance_case_id,
    x.pay_date DESC NULLS LAST,
    x.pay_batch_id DESC
),
active_snooze AS (
  SELECT DISTINCT ON (pa.id)
    pa.id AS finance_case_id,
    s.id AS snooze_id,
    s.snooze_kind,
    s.snooze_until_date,
    s.note,
    s.created_at_utc,
    s.updated_at_utc,
    s.created_by_user_id,
    s.updated_by_user_id
  FROM public.pay_advances pa
  JOIN public.pay_item_snoozes s
    ON s.source_ref = ('advance:'::text || pa.id::text)
   AND s.cleared_at_utc IS NULL
  ORDER BY
    pa.id,
    s.updated_at_utc DESC NULLS LAST,
    s.created_at_utc DESC,
    s.id DESC
),
component_rollup AS (
  SELECT
    pfc.finance_case_id,
    count(*) FILTER (
      WHERE pfc.closed_at_utc IS NULL
        AND pfc.remaining_source_amount > 0
        AND pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
    )::integer AS open_taxable_count,
    count(*) FILTER (
      WHERE pfc.closed_at_utc IS NULL
        AND pfc.remaining_source_amount > 0
        AND pfc.classification = 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
    )::integer AS open_reimbursement_count,
    count(*) FILTER (
      WHERE pfc.closed_at_utc IS NULL
        AND pfc.remaining_source_amount > 0
        AND pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        AND (
          pfc.is_resolution_stale
          OR pfc.saved_target_pay_method IS NULL
          OR pfc.saved_resolution_mode IS NULL
        )
    )::integer AS unresolved_taxable_count,
    count(*) FILTER (
      WHERE pfc.closed_at_utc IS NULL
        AND pfc.remaining_source_amount > 0
        AND pfc.is_resolution_stale
    )::integer AS stale_count
  FROM public.pay_finance_case_components pfc
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
  (COALESCE(cr.open_taxable_count, 0) > 0 AND COALESCE(cr.open_reimbursement_count, 0) > 0) AS is_mixed_case,
  COALESCE(cr.open_taxable_count, 0) AS open_taxable_count,
  COALESCE(cr.open_reimbursement_count, 0) AS open_reimbursement_count,
  COALESCE(cr.unresolved_taxable_count, 0) AS unresolved_taxable_count,
  COALESCE(cr.stale_count, 0) AS stale_count,
  jsonb_build_object(
    'open_taxable_count', COALESCE(cr.open_taxable_count, 0),
    'open_reimbursement_count', COALESCE(cr.open_reimbursement_count, 0),
    'unresolved_taxable_count', COALESCE(cr.unresolved_taxable_count, 0),
    'stale_count', COALESCE(cr.stale_count, 0),
    'is_mixed_case', (COALESCE(cr.open_taxable_count, 0) > 0 AND COALESCE(cr.open_reimbursement_count, 0) > 0)
  ) AS component_resolution_summary_json
FROM public.pay_advances pa
JOIN public.candidates c
  ON c.id = pa.candidate_id
LEFT JOIN public.clients cli
  ON cli.id = pa.client_id
LEFT JOIN reservation_rollup rr
  ON rr.finance_case_id = pa.id
LEFT JOIN latest_remittance lr
  ON lr.finance_case_id = pa.id
LEFT JOIN latest_recovery_batch lrb
  ON lrb.finance_case_id = pa.id
LEFT JOIN active_snooze s
  ON s.finance_case_id = pa.id
LEFT JOIN component_rollup cr
  ON cr.finance_case_id = pa.id;
