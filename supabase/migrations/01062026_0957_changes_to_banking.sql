-- ============================================================
-- CloudTMS Banking Pay safety foundation
-- Rerunnable Supabase/PostgreSQL migration
--
-- Implements the attached DB/schema foundation only:
-- - row-backed workbench scope / preview / line work
-- - row-backed operation/freshness units
-- - row-backed transfer-scope item membership
-- - provider queue/proof fields
-- - cached alert display summary
-- - backend-owned operation runner/lease fields
-- - provider attempt audit table
-- - hot-path statement/lock budget helper
--
-- Safe to rerun:
-- - CREATE TABLE IF NOT EXISTS
-- - ALTER TABLE ... ADD COLUMN IF NOT EXISTS
-- - CREATE INDEX IF NOT EXISTS
-- - constraint additions guarded through pg_constraint checks
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- Shared updated_at helper for new row-backed tables
-- ============================================================

CREATE OR REPLACE FUNCTION public._cloudtms_touch_updated_at_utc()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  NEW.updated_at_utc := now();
  RETURN NEW;
END;
$function$;

-- ============================================================
-- DB-1. pay_batch_display_summary
-- ============================================================

CREATE TABLE IF NOT EXISTS public.pay_batch_display_summary (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pay_batch_id uuid NOT NULL,
  batch_status text,
  batch_kind text,
  candidate_count integer NOT NULL DEFAULT 0,
  item_count integer NOT NULL DEFAULT 0,
  transfer_count integer NOT NULL DEFAULT 0,
  total_payable numeric(14,2) NOT NULL DEFAULT 0,
  total_bank_out numeric(14,2) NOT NULL DEFAULT 0,
  execution_commit_state text,
  rail_provider_label text,
  rail_env_label text,
  issue_summary_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
  latest_operation_id uuid,
  latest_operation_status text,
  latest_operation_phase text,
  display_label text,
  stale_summary_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  freshness_validation_status text,
  freshness_checked_at_utc timestamptz,
  freshness_result_hash text,
  summary_version bigint NOT NULL DEFAULT 1,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT pay_batch_display_summary_batch_fk
    FOREIGN KEY (pay_batch_id)
    REFERENCES public.pay_batches(id)
    ON DELETE CASCADE,
  CONSTRAINT pay_batch_display_summary_latest_op_fk
    FOREIGN KEY (latest_operation_id)
    REFERENCES public.banking_pay_operations(id)
    ON DELETE SET NULL
);

ALTER TABLE public.pay_batch_display_summary
  ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid();

ALTER TABLE public.pay_batch_display_summary
  ADD COLUMN IF NOT EXISTS pay_batch_id uuid;

ALTER TABLE public.pay_batch_display_summary
  ADD COLUMN IF NOT EXISTS batch_status text,
  ADD COLUMN IF NOT EXISTS batch_kind text,
  ADD COLUMN IF NOT EXISTS candidate_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS item_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS transfer_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS total_payable numeric(14,2) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS total_bank_out numeric(14,2) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS execution_commit_state text,
  ADD COLUMN IF NOT EXISTS rail_provider_label text,
  ADD COLUMN IF NOT EXISTS rail_env_label text,
  ADD COLUMN IF NOT EXISTS issue_summary_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS latest_operation_id uuid,
  ADD COLUMN IF NOT EXISTS latest_operation_status text,
  ADD COLUMN IF NOT EXISTS latest_operation_phase text,
  ADD COLUMN IF NOT EXISTS display_label text,
  ADD COLUMN IF NOT EXISTS stale_summary_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS freshness_validation_status text,
  ADD COLUMN IF NOT EXISTS freshness_checked_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS freshness_result_hash text,
  ADD COLUMN IF NOT EXISTS summary_version bigint NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS created_at_utc timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at_utc timestamptz NOT NULL DEFAULT now();

CREATE UNIQUE INDEX IF NOT EXISTS uq_pay_batch_display_summary_batch
  ON public.pay_batch_display_summary(pay_batch_id);

CREATE INDEX IF NOT EXISTS idx_pay_batch_display_summary_updated
  ON public.pay_batch_display_summary(updated_at_utc);

CREATE INDEX IF NOT EXISTS idx_pay_batch_display_summary_version
  ON public.pay_batch_display_summary(summary_version);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'trg_pay_batch_display_summary_touch_updated_at_utc'
  ) THEN
    CREATE TRIGGER trg_pay_batch_display_summary_touch_updated_at_utc
    BEFORE UPDATE ON public.pay_batch_display_summary
    FOR EACH ROW
    EXECUTE FUNCTION public._cloudtms_touch_updated_at_utc();
  END IF;
END $$;

-- Conservative missing-row backfill only.
-- This intentionally avoids deep diagnostics/provider/correction/mail scans.
INSERT INTO public.pay_batch_display_summary (
  pay_batch_id,
  batch_status,
  batch_kind,
  total_bank_out,
  execution_commit_state,
  rail_provider_label,
  rail_env_label,
  display_label,
  freshness_validation_status,
  freshness_checked_at_utc,
  freshness_result_hash,
  stale_summary_json
)
SELECT
  pb.id,
  pb.status,
  pb.batch_kind_fixed,
  COALESCE(pb.total_bank_out, 0),
  pb.execution_commit_state,
  pb.rail_provider_snapshot,
  pb.rail_env_snapshot,
  concat_ws(' · ', pb.batch_kind_fixed, pb.status, pb.pay_date::text),
  pb.freshness_validation_status,
  pb.freshness_checked_at_utc,
  pb.freshness_result_hash,
  jsonb_strip_nulls(jsonb_build_object(
    'freshness_validation_status', pb.freshness_validation_status,
    'freshness_checked_at_utc', pb.freshness_checked_at_utc,
    'freshness_result_hash', pb.freshness_result_hash
  ))
FROM public.pay_batches pb
ON CONFLICT (pay_batch_id) DO NOTHING;

-- Optional safe one-batch refresh helper.
-- This is deliberately summary-only and does not run alerts, provider diagnostics,
-- cancelability, correction planning, remittance scans, or mail scans.
CREATE OR REPLACE FUNCTION public.pay_batch_display_summary_refresh(
  p_pay_batch_id uuid
)
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO public.pay_batch_display_summary (
    pay_batch_id,
    batch_status,
    batch_kind,
    candidate_count,
    item_count,
    transfer_count,
    total_payable,
    total_bank_out,
    execution_commit_state,
    rail_provider_label,
    rail_env_label,
    latest_operation_id,
    latest_operation_status,
    latest_operation_phase,
    display_label,
    freshness_validation_status,
    freshness_checked_at_utc,
    freshness_result_hash,
    stale_summary_json,
    summary_version,
    updated_at_utc
  )
  SELECT
    pb.id,
    pb.status,
    pb.batch_kind_fixed,
    COALESCE(cand.candidate_count, 0),
    COALESCE(items.item_count, 0),
    COALESCE(transfers.transfer_count, 0),
    COALESCE(items.total_payable, 0),
    COALESCE(pb.total_bank_out, transfers.total_bank_out, 0),
    pb.execution_commit_state,
    pb.rail_provider_snapshot,
    pb.rail_env_snapshot,
    latest_op.id,
    latest_op.status,
    latest_op.phase,
    concat_ws(' · ', pb.batch_kind_fixed, pb.status, pb.pay_date::text),
    pb.freshness_validation_status,
    pb.freshness_checked_at_utc,
    pb.freshness_result_hash,
    jsonb_strip_nulls(jsonb_build_object(
      'freshness_validation_status', pb.freshness_validation_status,
      'freshness_checked_at_utc', pb.freshness_checked_at_utc,
      'freshness_result_hash', pb.freshness_result_hash
    )),
    1,
    now()
  FROM public.pay_batches pb
  LEFT JOIN LATERAL (
    SELECT count(*)::integer AS candidate_count
    FROM public.pay_batch_candidates pbc
    WHERE pbc.pay_batch_id = pb.id
  ) cand ON true
  LEFT JOIN LATERAL (
    SELECT
      count(*)::integer AS item_count,
      COALESCE(sum(
        COALESCE(
          pbi.amount_inc_vat,
          pbi.frozen_target_amount_inc_vat,
          pbi.amount_ex_vat,
          pbi.frozen_target_amount_ex_vat,
          0
        )
      ), 0)::numeric(14,2) AS total_payable
    FROM public.pay_batch_candidates pbc
    JOIN public.pay_batch_items pbi
      ON pbi.pay_batch_candidate_id = pbc.id
    WHERE pbc.pay_batch_id = pb.id
      AND COALESCE(pbi.is_voided, false) = false
  ) items ON true
  LEFT JOIN LATERAL (
    SELECT
      count(*)::integer AS transfer_count,
      COALESCE(sum(pbt.amount), 0)::numeric(14,2) AS total_bank_out
    FROM public.pay_bank_transfers pbt
    WHERE pbt.pay_batch_id = pb.id
  ) transfers ON true
  LEFT JOIN LATERAL (
    SELECT bpo.id, bpo.status, bpo.phase
    FROM public.banking_pay_operations bpo
    WHERE bpo.pay_batch_id = pb.id
    ORDER BY bpo.updated_at_utc DESC NULLS LAST, bpo.created_at_utc DESC NULLS LAST
    LIMIT 1
  ) latest_op ON true
  WHERE pb.id = p_pay_batch_id
  ON CONFLICT (pay_batch_id) DO UPDATE
  SET
    batch_status = EXCLUDED.batch_status,
    batch_kind = EXCLUDED.batch_kind,
    candidate_count = EXCLUDED.candidate_count,
    item_count = EXCLUDED.item_count,
    transfer_count = EXCLUDED.transfer_count,
    total_payable = EXCLUDED.total_payable,
    total_bank_out = EXCLUDED.total_bank_out,
    execution_commit_state = EXCLUDED.execution_commit_state,
    rail_provider_label = EXCLUDED.rail_provider_label,
    rail_env_label = EXCLUDED.rail_env_label,
    latest_operation_id = EXCLUDED.latest_operation_id,
    latest_operation_status = EXCLUDED.latest_operation_status,
    latest_operation_phase = EXCLUDED.latest_operation_phase,
    display_label = EXCLUDED.display_label,
    freshness_validation_status = EXCLUDED.freshness_validation_status,
    freshness_checked_at_utc = EXCLUDED.freshness_checked_at_utc,
    freshness_result_hash = EXCLUDED.freshness_result_hash,
    stale_summary_json = EXCLUDED.stale_summary_json,
    summary_version = public.pay_batch_display_summary.summary_version + 1,
    updated_at_utc = now();
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_batch_display_summary_touch(
  p_pay_batch_id uuid
)
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO public.pay_batch_display_summary (
    pay_batch_id,
    summary_version,
    updated_at_utc
  )
  VALUES (
    p_pay_batch_id,
    1,
    now()
  )
  ON CONFLICT (pay_batch_id) DO UPDATE
  SET
    summary_version = public.pay_batch_display_summary.summary_version + 1,
    updated_at_utc = now();
END;
$function$;

-- ============================================================
-- DB-2. banking_pay_workbench_session_scope
-- ============================================================

CREATE TABLE IF NOT EXISTS public.banking_pay_workbench_session_scope (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  scope_ordinal bigint NOT NULL,
  status text NOT NULL DEFAULT 'PENDING',
  pending_job_id uuid,
  seeded boolean NOT NULL DEFAULT false,
  dirty boolean NOT NULL DEFAULT false,
  error_json jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT bpay_wb_scope_session_fk
    FOREIGN KEY (session_id)
    REFERENCES public.banking_pay_workbench_sessions(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_wb_scope_candidate_fk
    FOREIGN KEY (candidate_id)
    REFERENCES public.candidates(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_wb_scope_pending_job_fk
    FOREIGN KEY (pending_job_id)
    REFERENCES public.banking_pay_workbench_jobs(id)
    ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_bpay_wb_scope_session_candidate
  ON public.banking_pay_workbench_session_scope(session_id, candidate_id);

CREATE INDEX IF NOT EXISTS idx_bpay_wb_scope_session_status_ordinal
  ON public.banking_pay_workbench_session_scope(session_id, status, scope_ordinal);

CREATE INDEX IF NOT EXISTS idx_bpay_wb_scope_session_ordinal
  ON public.banking_pay_workbench_session_scope(session_id, scope_ordinal);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'trg_bpay_wb_scope_touch_updated_at_utc'
  ) THEN
    CREATE TRIGGER trg_bpay_wb_scope_touch_updated_at_utc
    BEFORE UPDATE ON public.banking_pay_workbench_session_scope
    FOR EACH ROW
    EXECUTE FUNCTION public._cloudtms_touch_updated_at_utc();
  END IF;
END $$;

-- ============================================================
-- DB-3. banking_pay_workbench_preview_rows
-- ============================================================

CREATE TABLE IF NOT EXISTS public.banking_pay_workbench_preview_rows (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  section text NOT NULL,
  row_key text NOT NULL,
  row_ordinal bigint NOT NULL,
  row_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  timesheet_id uuid,
  key_type text,
  key_value text,
  selected boolean NOT NULL DEFAULT true,
  selection_state text NOT NULL DEFAULT 'SELECTED',
  status text NOT NULL DEFAULT 'READY',
  session_version bigint,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT bpay_wb_preview_rows_session_fk
    FOREIGN KEY (session_id)
    REFERENCES public.banking_pay_workbench_sessions(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_wb_preview_rows_candidate_fk
    FOREIGN KEY (candidate_id)
    REFERENCES public.candidates(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_wb_preview_rows_timesheet_fk
    FOREIGN KEY (timesheet_id)
    REFERENCES public.timesheets(timesheet_id)
    ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_bpay_wb_preview_session_section_candidate_row
  ON public.banking_pay_workbench_preview_rows(session_id, section, candidate_id, row_key);

CREATE INDEX IF NOT EXISTS idx_bpay_wb_preview_session_section_ordinal_id
  ON public.banking_pay_workbench_preview_rows(session_id, section, row_ordinal, id);

CREATE INDEX IF NOT EXISTS idx_bpay_wb_preview_session_candidate
  ON public.banking_pay_workbench_preview_rows(session_id, candidate_id);

CREATE INDEX IF NOT EXISTS idx_bpay_wb_preview_economic_key
  ON public.banking_pay_workbench_preview_rows(timesheet_id, key_type, key_value);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'trg_bpay_wb_preview_touch_updated_at_utc'
  ) THEN
    CREATE TRIGGER trg_bpay_wb_preview_touch_updated_at_utc
    BEFORE UPDATE ON public.banking_pay_workbench_preview_rows
    FOR EACH ROW
    EXECUTE FUNCTION public._cloudtms_touch_updated_at_utc();
  END IF;
END $$;

-- ============================================================
-- DB-4. banking_pay_workbench_candidate_line_work
-- ============================================================

CREATE TABLE IF NOT EXISTS public.banking_pay_workbench_candidate_line_work (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  timesheet_id uuid,
  line_key text NOT NULL,
  line_ordinal bigint NOT NULL,
  status text NOT NULL DEFAULT 'PENDING',
  work_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  result_row_json jsonb,
  error_json jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT bpay_wb_line_work_session_fk
    FOREIGN KEY (session_id)
    REFERENCES public.banking_pay_workbench_sessions(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_wb_line_work_candidate_fk
    FOREIGN KEY (candidate_id)
    REFERENCES public.candidates(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_wb_line_work_timesheet_fk
    FOREIGN KEY (timesheet_id)
    REFERENCES public.timesheets(timesheet_id)
    ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_bpay_wb_line_work_session_candidate_timesheet_line
  ON public.banking_pay_workbench_candidate_line_work(
    session_id,
    candidate_id,
    COALESCE(timesheet_id, '00000000-0000-0000-0000-000000000000'::uuid),
    line_key
  );

CREATE INDEX IF NOT EXISTS idx_bpay_wb_line_work_session_candidate_status_ordinal
  ON public.banking_pay_workbench_candidate_line_work(session_id, candidate_id, status, line_ordinal);

CREATE INDEX IF NOT EXISTS idx_bpay_wb_line_work_session_status
  ON public.banking_pay_workbench_candidate_line_work(session_id, status);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'trg_bpay_wb_line_work_touch_updated_at_utc'
  ) THEN
    CREATE TRIGGER trg_bpay_wb_line_work_touch_updated_at_utc
    BEFORE UPDATE ON public.banking_pay_workbench_candidate_line_work
    FOR EACH ROW
    EXECUTE FUNCTION public._cloudtms_touch_updated_at_utc();
  END IF;
END $$;

-- ============================================================
-- DB-5. banking_pay_operation_scope_units
-- ============================================================

CREATE TABLE IF NOT EXISTS public.banking_pay_operation_scope_units (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  operation_id uuid NOT NULL,
  pay_batch_id uuid,
  phase text NOT NULL,
  unit_type text NOT NULL,
  unit_key text NOT NULL,
  unit_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  unit_ordinal bigint NOT NULL,
  status text NOT NULL DEFAULT 'PENDING',
  chunk_id uuid,
  result_hash text,
  error_json jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT bpay_op_scope_units_operation_fk
    FOREIGN KEY (operation_id)
    REFERENCES public.banking_pay_operations(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_op_scope_units_batch_fk
    FOREIGN KEY (pay_batch_id)
    REFERENCES public.pay_batches(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_op_scope_units_chunk_fk
    FOREIGN KEY (chunk_id)
    REFERENCES public.banking_pay_operation_chunks(id)
    ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_bpay_op_scope_units_operation_phase_type_key
  ON public.banking_pay_operation_scope_units(operation_id, phase, unit_type, unit_key);

CREATE INDEX IF NOT EXISTS idx_bpay_op_scope_units_claim
  ON public.banking_pay_operation_scope_units(operation_id, phase, unit_type, status, unit_ordinal);

CREATE INDEX IF NOT EXISTS idx_bpay_op_scope_units_chunk
  ON public.banking_pay_operation_scope_units(operation_id, phase, chunk_id);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'trg_bpay_op_scope_units_touch_updated_at_utc'
  ) THEN
    CREATE TRIGGER trg_bpay_op_scope_units_touch_updated_at_utc
    BEFORE UPDATE ON public.banking_pay_operation_scope_units
    FOR EACH ROW
    EXECUTE FUNCTION public._cloudtms_touch_updated_at_utc();
  END IF;
END $$;

-- ============================================================
-- DB-6. banking_pay_operation_transfer_scope_items
-- ============================================================

CREATE TABLE IF NOT EXISTS public.banking_pay_operation_transfer_scope_items (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  operation_id uuid NOT NULL,
  pay_batch_id uuid NOT NULL,
  transfer_scope_id uuid NOT NULL,
  pay_batch_item_id uuid NOT NULL,
  pay_batch_candidate_id uuid,
  candidate_id uuid,
  item_amount numeric(14,2) NOT NULL DEFAULT 0,
  item_status text NOT NULL DEFAULT 'PENDING',
  item_ordinal bigint NOT NULL DEFAULT 0,
  rollup_status text NOT NULL DEFAULT 'PENDING',
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT bpay_op_transfer_items_operation_fk
    FOREIGN KEY (operation_id)
    REFERENCES public.banking_pay_operations(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_op_transfer_items_batch_fk
    FOREIGN KEY (pay_batch_id)
    REFERENCES public.pay_batches(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_op_transfer_items_scope_fk
    FOREIGN KEY (transfer_scope_id)
    REFERENCES public.banking_pay_operation_transfer_scope(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_op_transfer_items_item_fk
    FOREIGN KEY (pay_batch_item_id)
    REFERENCES public.pay_batch_items(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_op_transfer_items_batch_candidate_fk
    FOREIGN KEY (pay_batch_candidate_id)
    REFERENCES public.pay_batch_candidates(id)
    ON DELETE SET NULL,
  CONSTRAINT bpay_op_transfer_items_candidate_fk
    FOREIGN KEY (candidate_id)
    REFERENCES public.candidates(id)
    ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_bpay_op_transfer_items_scope_item
  ON public.banking_pay_operation_transfer_scope_items(operation_id, transfer_scope_id, pay_batch_item_id);

CREATE INDEX IF NOT EXISTS idx_bpay_op_transfer_items_rollup
  ON public.banking_pay_operation_transfer_scope_items(operation_id, transfer_scope_id, rollup_status, item_ordinal);

CREATE INDEX IF NOT EXISTS idx_bpay_op_transfer_items_batch_rollup
  ON public.banking_pay_operation_transfer_scope_items(operation_id, pay_batch_id, rollup_status);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'trg_bpay_op_transfer_items_touch_updated_at_utc'
  ) THEN
    CREATE TRIGGER trg_bpay_op_transfer_items_touch_updated_at_utc
    BEFORE UPDATE ON public.banking_pay_operation_transfer_scope_items
    FOR EACH ROW
    EXECUTE FUNCTION public._cloudtms_touch_updated_at_utc();
  END IF;
END $$;

-- ============================================================
-- DB-7. Extend banking_pay_operation_transfer_scope
-- ============================================================

ALTER TABLE public.banking_pay_operation_transfer_scope
  ADD COLUMN IF NOT EXISTS provider_submit_ready boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS provider_submit_state text NOT NULL DEFAULT 'NOT_READY',
  ADD COLUMN IF NOT EXISTS provider_submit_chunk_id uuid,
  ADD COLUMN IF NOT EXISTS provider_submit_claimed_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS provider_submit_attempt_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS provider_idempotency_key text,
  ADD COLUMN IF NOT EXISTS provider_request_id text,
  ADD COLUMN IF NOT EXISTS provider_transaction_id text,
  ADD COLUMN IF NOT EXISTS provider_request_prepared_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS provider_request_sending_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS provider_request_sent_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS provider_response_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS provider_submission_status text,
  ADD COLUMN IF NOT EXISTS provider_review_required boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS provider_unsafe_reason text,
  ADD COLUMN IF NOT EXISTS prepared_item_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS prepared_amount_total numeric(14,2) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS prepared_scope_hash text,
  ADD COLUMN IF NOT EXISTS prepared_result_hash text;

UPDATE public.banking_pay_operation_transfer_scope
SET provider_submit_state = 'NOT_READY'
WHERE provider_submit_state IS NULL;

ALTER TABLE public.banking_pay_operation_transfer_scope
  ALTER COLUMN provider_submit_ready SET DEFAULT false,
  ALTER COLUMN provider_submit_state SET DEFAULT 'NOT_READY',
  ALTER COLUMN provider_submit_attempt_count SET DEFAULT 0,
  ALTER COLUMN provider_review_required SET DEFAULT false,
  ALTER COLUMN prepared_item_count SET DEFAULT 0,
  ALTER COLUMN prepared_amount_total SET DEFAULT 0;

ALTER TABLE public.banking_pay_operation_transfer_scope
  ALTER COLUMN provider_submit_ready SET NOT NULL,
  ALTER COLUMN provider_submit_state SET NOT NULL,
  ALTER COLUMN provider_submit_attempt_count SET NOT NULL,
  ALTER COLUMN provider_review_required SET NOT NULL,
  ALTER COLUMN prepared_item_count SET NOT NULL,
  ALTER COLUMN prepared_amount_total SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_bpay_transfer_scope_provider_state'
      AND conrelid = 'public.banking_pay_operation_transfer_scope'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_operation_transfer_scope
      ADD CONSTRAINT chk_bpay_transfer_scope_provider_state
      CHECK (
        provider_submit_state IN (
          'NOT_READY',
          'READY',
          'CLAIMED',
          'REQUEST_PREPARING',
          'REQUEST_SENDING',
          'REQUEST_SENT_LOCAL',
          'PROVIDER_ACCEPTED',
          'PROVIDER_REJECTED',
          'PROVIDER_UNKNOWN',
          'REVIEW_REQUIRED',
          'CHUNK_FINALISED'
        )
      );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'bpay_transfer_scope_provider_chunk_fk'
      AND conrelid = 'public.banking_pay_operation_transfer_scope'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_operation_transfer_scope
      ADD CONSTRAINT bpay_transfer_scope_provider_chunk_fk
      FOREIGN KEY (provider_submit_chunk_id)
      REFERENCES public.banking_pay_operation_chunks(id)
      ON DELETE SET NULL;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_bpay_transfer_scope_provider_claim
  ON public.banking_pay_operation_transfer_scope(
    operation_id,
    pay_batch_id,
    provider_submit_ready,
    provider_submit_state,
    id
  );

CREATE INDEX IF NOT EXISTS idx_bpay_transfer_scope_provider_chunk
  ON public.banking_pay_operation_transfer_scope(
    operation_id,
    pay_batch_id,
    provider_submit_state,
    provider_submit_chunk_id
  );

CREATE INDEX IF NOT EXISTS idx_bpay_transfer_scope_provider_tx
  ON public.banking_pay_operation_transfer_scope(provider_transaction_id)
  WHERE provider_transaction_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_bpay_transfer_scope_provider_request
  ON public.banking_pay_operation_transfer_scope(provider_request_id)
  WHERE provider_request_id IS NOT NULL;

-- ============================================================
-- DB-8. banking_alert_display_summary
-- ============================================================

CREATE TABLE IF NOT EXISTS public.banking_alert_display_summary (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  actor_user_id uuid NOT NULL,
  alert_hash text,
  summary_hash text,
  unacknowledged_count integer NOT NULL DEFAULT 0,
  highest_severity text,
  highest_label text,
  summary_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_banking_alert_display_summary_actor
  ON public.banking_alert_display_summary(actor_user_id);

CREATE INDEX IF NOT EXISTS idx_banking_alert_display_summary_updated
  ON public.banking_alert_display_summary(updated_at_utc);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'trg_banking_alert_display_summary_touch_updated_at_utc'
  ) THEN
    CREATE TRIGGER trg_banking_alert_display_summary_touch_updated_at_utc
    BEFORE UPDATE ON public.banking_alert_display_summary
    FOR EACH ROW
    EXECUTE FUNCTION public._cloudtms_touch_updated_at_utc();
  END IF;
END $$;

-- Lightweight touch/upsert helper for cached alert signal rows.
-- This does not calculate live alerts.
CREATE OR REPLACE FUNCTION public.banking_alert_display_summary_touch(
  p_actor_user_id uuid,
  p_alert_hash text DEFAULT NULL,
  p_summary_hash text DEFAULT NULL,
  p_unacknowledged_count integer DEFAULT NULL,
  p_highest_severity text DEFAULT NULL,
  p_highest_label text DEFAULT NULL,
  p_summary_json jsonb DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO public.banking_alert_display_summary (
    actor_user_id,
    alert_hash,
    summary_hash,
    unacknowledged_count,
    highest_severity,
    highest_label,
    summary_json,
    updated_at_utc
  )
  VALUES (
    p_actor_user_id,
    p_alert_hash,
    p_summary_hash,
    COALESCE(p_unacknowledged_count, 0),
    p_highest_severity,
    p_highest_label,
    COALESCE(p_summary_json, '{}'::jsonb),
    now()
  )
  ON CONFLICT (actor_user_id) DO UPDATE
  SET
    alert_hash = COALESCE(EXCLUDED.alert_hash, public.banking_alert_display_summary.alert_hash),
    summary_hash = COALESCE(EXCLUDED.summary_hash, public.banking_alert_display_summary.summary_hash),
    unacknowledged_count = COALESCE(EXCLUDED.unacknowledged_count, public.banking_alert_display_summary.unacknowledged_count),
    highest_severity = COALESCE(EXCLUDED.highest_severity, public.banking_alert_display_summary.highest_severity),
    highest_label = COALESCE(EXCLUDED.highest_label, public.banking_alert_display_summary.highest_label),
    summary_json = COALESCE(EXCLUDED.summary_json, public.banking_alert_display_summary.summary_json),
    updated_at_utc = now();
END;
$function$;

-- ============================================================
-- DB-9. Extend banking_pay_operations for backend-owned runner
-- ============================================================

ALTER TABLE public.banking_pay_operations
  ADD COLUMN IF NOT EXISTS run_after_utc timestamptz,
  ADD COLUMN IF NOT EXISTS lease_owner text,
  ADD COLUMN IF NOT EXISTS lease_expires_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS heartbeat_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS last_advanced_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS attempt_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS max_attempts integer NOT NULL DEFAULT 20,
  ADD COLUMN IF NOT EXISTS requires_user_action boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS runner_state text NOT NULL DEFAULT 'IDLE',
  ADD COLUMN IF NOT EXISTS resume_reason text;

UPDATE public.banking_pay_operations
SET
  attempt_count = COALESCE(attempt_count, 0),
  max_attempts = COALESCE(max_attempts, 20),
  requires_user_action = COALESCE(requires_user_action, false),
  runner_state = COALESCE(runner_state, 'IDLE')
WHERE attempt_count IS NULL
   OR max_attempts IS NULL
   OR requires_user_action IS NULL
   OR runner_state IS NULL;

ALTER TABLE public.banking_pay_operations
  ALTER COLUMN attempt_count SET DEFAULT 0,
  ALTER COLUMN max_attempts SET DEFAULT 20,
  ALTER COLUMN requires_user_action SET DEFAULT false,
  ALTER COLUMN runner_state SET DEFAULT 'IDLE';

ALTER TABLE public.banking_pay_operations
  ALTER COLUMN attempt_count SET NOT NULL,
  ALTER COLUMN max_attempts SET NOT NULL,
  ALTER COLUMN requires_user_action SET NOT NULL,
  ALTER COLUMN runner_state SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_bpay_operations_status_run_after
  ON public.banking_pay_operations(status, run_after_utc);

CREATE INDEX IF NOT EXISTS idx_bpay_operations_type_status_run_after
  ON public.banking_pay_operations(operation_type, status, run_after_utc);

CREATE INDEX IF NOT EXISTS idx_bpay_operations_lease_expires
  ON public.banking_pay_operations(lease_expires_at_utc);

CREATE INDEX IF NOT EXISTS idx_bpay_operations_batch_type_status
  ON public.banking_pay_operations(pay_batch_id, operation_type, status);

-- ============================================================
-- DB-10. banking_pay_operation_provider_attempts
-- ============================================================

CREATE TABLE IF NOT EXISTS public.banking_pay_operation_provider_attempts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  operation_id uuid NOT NULL,
  pay_batch_id uuid,
  transfer_scope_id uuid,
  provider_chunk_id uuid,
  provider_idempotency_key text,
  provider_request_id text,
  provider_transaction_id text,
  previous_state text,
  new_state text,
  lease_owner text,
  compact_request_hash text,
  compact_response_hash text,
  compact_error_summary_json jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT bpay_provider_attempts_operation_fk
    FOREIGN KEY (operation_id)
    REFERENCES public.banking_pay_operations(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_provider_attempts_batch_fk
    FOREIGN KEY (pay_batch_id)
    REFERENCES public.pay_batches(id)
    ON DELETE CASCADE,
  CONSTRAINT bpay_provider_attempts_transfer_scope_fk
    FOREIGN KEY (transfer_scope_id)
    REFERENCES public.banking_pay_operation_transfer_scope(id)
    ON DELETE SET NULL,
  CONSTRAINT bpay_provider_attempts_chunk_fk
    FOREIGN KEY (provider_chunk_id)
    REFERENCES public.banking_pay_operation_chunks(id)
    ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_bpay_provider_attempts_operation_created
  ON public.banking_pay_operation_provider_attempts(operation_id, created_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_bpay_provider_attempts_transfer_created
  ON public.banking_pay_operation_provider_attempts(transfer_scope_id, created_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_bpay_provider_attempts_request
  ON public.banking_pay_operation_provider_attempts(provider_request_id)
  WHERE provider_request_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_bpay_provider_attempts_transaction
  ON public.banking_pay_operation_provider_attempts(provider_transaction_id)
  WHERE provider_transaction_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_bpay_provider_attempts_idempotency
  ON public.banking_pay_operation_provider_attempts(provider_idempotency_key)
  WHERE provider_idempotency_key IS NOT NULL;

-- ============================================================
-- DB-11. Hot-path statement and lock budget helper
-- ============================================================

CREATE OR REPLACE FUNCTION public.banking_pay_hot_path_budget_apply(
  p_route_class text DEFAULT 'DISPLAY'
)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
  v_route_class text := upper(trim(COALESCE(p_route_class, 'DISPLAY')));
BEGIN
  /*
    Local transaction budgets only.

    This is a circuit breaker, not the primary safety mechanism.
    The primary fix is row-backed, capped, chunked work.
  */

  IF v_route_class IN (
    'DISPLAY',
    'LIST',
    'BOOTSTRAP',
    'PROGRESS',
    'WATCH',
    'OVERVIEW',
    'OPERATION_PROGRESS',
    'PREVIEW_PROGRESS'
  ) THEN
    PERFORM set_config('statement_timeout', '3000', true);
    PERFORM set_config('lock_timeout', '750', true);
    PERFORM set_config('idle_in_transaction_session_timeout', '10000', true);

  ELSIF v_route_class IN (
    'PREVIEW_CHUNK',
    'WORKBENCH_CHUNK',
    'EXECUTION_CHUNK',
    'WORKER_CHUNK',
    'PROVIDER_CHUNK',
    'OPERATION_WORKER'
  ) THEN
    PERFORM set_config('statement_timeout', '15000', true);
    PERFORM set_config('lock_timeout', '1500', true);
    PERFORM set_config('idle_in_transaction_session_timeout', '30000', true);

  ELSIF v_route_class IN (
    'DIAGNOSTIC',
    'EXPLICIT_DIAGNOSTIC',
    'EXPORT',
    'ADMIN'
  ) THEN
    PERFORM set_config('statement_timeout', '30000', true);
    PERFORM set_config('lock_timeout', '3000', true);
    PERFORM set_config('idle_in_transaction_session_timeout', '60000', true);

  ELSE
    PERFORM set_config('statement_timeout', '5000', true);
    PERFORM set_config('lock_timeout', '1000', true);
    PERFORM set_config('idle_in_transaction_session_timeout', '15000', true);
  END IF;
END;
$function$;

COMMENT ON FUNCTION public.banking_pay_hot_path_budget_apply(text)
IS 'Applies local statement/lock budgets for Banking Pay hot paths. This is only a circuit breaker; row-backed capped work remains the main safety fix.';

-- ============================================================
-- Verification notes:
--
-- This migration intentionally does NOT:
-- - remove old JSON/array columns yet;
-- - rewrite pay_batches_list/pay_batch_get/etc.;
-- - rewrite frontend/backend execution ownership;
-- - change payment maths;
-- - change Policy X economic key logic.
--
-- Those changes should be applied in the function/backend/frontend
-- implementation phase after this foundation is present.
-- ============================================================
