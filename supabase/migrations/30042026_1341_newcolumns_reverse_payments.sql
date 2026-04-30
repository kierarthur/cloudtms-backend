BEGIN;

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS payment_remittance_send_timing text,
  ADD COLUMN IF NOT EXISTS payment_return_auto_reverse_timesheets boolean,
  ADD COLUMN IF NOT EXISTS payment_return_admin_recipient_role text,
  ADD COLUMN IF NOT EXISTS payment_return_admin_notice_quiet_minutes integer,
  ADD COLUMN IF NOT EXISTS payment_return_admin_notice_max_wait_minutes integer;

ALTER TABLE public.settings_defaults
  ALTER COLUMN payment_remittance_send_timing SET DEFAULT 'ON_EXECUTION',
  ALTER COLUMN payment_return_auto_reverse_timesheets SET DEFAULT false,
  ALTER COLUMN payment_return_admin_recipient_role SET DEFAULT 'ADMIN',
  ALTER COLUMN payment_return_admin_notice_quiet_minutes SET DEFAULT 10,
  ALTER COLUMN payment_return_admin_notice_max_wait_minutes SET DEFAULT 60;

WITH normalised_settings AS (
  SELECT
    id,
    CASE
      WHEN payment_return_admin_notice_quiet_minutes BETWEEN 0 AND 1440
        THEN payment_return_admin_notice_quiet_minutes
      ELSE 10
    END AS quiet_minutes
  FROM public.settings_defaults
)
UPDATE public.settings_defaults sd
SET
  payment_remittance_send_timing = CASE
    WHEN sd.payment_remittance_send_timing IN ('ON_EXECUTION', 'ON_PAYMENT_CONFIRMED')
      THEN sd.payment_remittance_send_timing
    ELSE 'ON_EXECUTION'
  END,
  payment_return_auto_reverse_timesheets = COALESCE(sd.payment_return_auto_reverse_timesheets, false),
  payment_return_admin_recipient_role = COALESCE(NULLIF(sd.payment_return_admin_recipient_role, ''), 'ADMIN'),
  payment_return_admin_notice_quiet_minutes = ns.quiet_minutes,
  payment_return_admin_notice_max_wait_minutes = CASE
    WHEN sd.payment_return_admin_notice_max_wait_minutes >= ns.quiet_minutes
      AND sd.payment_return_admin_notice_max_wait_minutes <= 1440
      THEN sd.payment_return_admin_notice_max_wait_minutes
    ELSE GREATEST(ns.quiet_minutes, 60)
  END
FROM normalised_settings ns
WHERE ns.id = sd.id;

ALTER TABLE public.settings_defaults
  ALTER COLUMN payment_remittance_send_timing SET NOT NULL,
  ALTER COLUMN payment_return_auto_reverse_timesheets SET NOT NULL,
  ALTER COLUMN payment_return_admin_recipient_role SET NOT NULL,
  ALTER COLUMN payment_return_admin_notice_quiet_minutes SET NOT NULL,
  ALTER COLUMN payment_return_admin_notice_max_wait_minutes SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.settings_defaults'::regclass
      AND conname = 'settings_defaults_payment_remittance_send_timing_chk'
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_payment_remittance_send_timing_chk
      CHECK (payment_remittance_send_timing IN ('ON_EXECUTION', 'ON_PAYMENT_CONFIRMED'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.settings_defaults'::regclass
      AND conname = 'settings_defaults_payment_return_admin_quiet_minutes_chk'
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_payment_return_admin_quiet_minutes_chk
      CHECK (
        payment_return_admin_notice_quiet_minutes >= 0
        AND payment_return_admin_notice_quiet_minutes <= 1440
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.settings_defaults'::regclass
      AND conname = 'settings_defaults_payment_return_admin_max_wait_minutes_chk'
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_payment_return_admin_max_wait_minutes_chk
      CHECK (
        payment_return_admin_notice_max_wait_minutes >= payment_return_admin_notice_quiet_minutes
        AND payment_return_admin_notice_max_wait_minutes <= 1440
      );
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.pay_bank_transfer_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pay_batch_id uuid NOT NULL REFERENCES public.pay_batches(id),
  pay_bank_transfer_id uuid NULL REFERENCES public.pay_bank_transfers(id),
  candidate_id uuid NULL REFERENCES public.candidates(id),
  umbrella_id uuid NULL REFERENCES public.umbrellas(id),
  provider_key text NULL,
  provider_event_id text NULL,
  provider_reference text NULL,
  provider_state text NULL,
  normalised_state text NOT NULL,
  event_source text NOT NULL,
  event_time_utc timestamptz NULL,
  received_at_utc timestamptz NOT NULL DEFAULT now(),
  amount numeric NULL,
  currency text NOT NULL DEFAULT 'GBP',
  mapping_status text NOT NULL,
  movement_classification text NULL,
  correction_disposition text NULL,
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  idempotency_key text NOT NULL,
  created_at_utc timestamptz NOT NULL DEFAULT now()
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_bank_transfer_events'::regclass
      AND conname = 'pay_bank_transfer_events_normalised_state_chk'
  ) THEN
    ALTER TABLE public.pay_bank_transfer_events
      ADD CONSTRAINT pay_bank_transfer_events_normalised_state_chk
      CHECK (normalised_state IN (
        'SUBMITTED',
        'PENDING',
        'PROCESSING',
        'COMPLETED',
        'FAILED',
        'DECLINED',
        'REJECTED',
        'CANCELLED',
        'RETURNED',
        'REVERTED',
        'UNKNOWN'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_bank_transfer_events'::regclass
      AND conname = 'pay_bank_transfer_events_event_source_chk'
  ) THEN
    ALTER TABLE public.pay_bank_transfer_events
      ADD CONSTRAINT pay_bank_transfer_events_event_source_chk
      CHECK (event_source IN (
        'PROVIDER_WEBHOOK',
        'PROVIDER_POLL',
        'MANUAL_CONFIRM',
        'MANUAL_EVIDENCE',
        'SYSTEM'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_bank_transfer_events'::regclass
      AND conname = 'pay_bank_transfer_events_mapping_status_chk'
  ) THEN
    ALTER TABLE public.pay_bank_transfer_events
      ADD CONSTRAINT pay_bank_transfer_events_mapping_status_chk
      CHECK (mapping_status IN (
        'MATCHED',
        'AMBIGUOUS',
        'UNMATCHED',
        'LEGACY_NO_ARTIFACT'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_bank_transfer_events'::regclass
      AND conname = 'pay_bank_transfer_events_movement_classification_chk'
  ) THEN
    ALTER TABLE public.pay_bank_transfer_events
      ADD CONSTRAINT pay_bank_transfer_events_movement_classification_chk
      CHECK (
        movement_classification IS NULL
        OR movement_classification IN (
          'PRE_BANK_CANCEL',
          'NO_MONEY_UNWIND',
          'TRUE_SETTLED_REVERSAL_REQUIRED',
          'AMBIGUOUS_REVIEW_REQUIRED'
        )
      );
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS pay_bank_transfer_events_idempotency_key_uidx
  ON public.pay_bank_transfer_events (idempotency_key);

CREATE INDEX IF NOT EXISTS pay_bank_transfer_events_batch_received_idx
  ON public.pay_bank_transfer_events (pay_batch_id, received_at_utc DESC);

CREATE INDEX IF NOT EXISTS pay_bank_transfer_events_transfer_received_idx
  ON public.pay_bank_transfer_events (pay_bank_transfer_id, received_at_utc DESC);

CREATE INDEX IF NOT EXISTS pay_bank_transfer_events_provider_event_idx
  ON public.pay_bank_transfer_events (provider_key, provider_event_id);

CREATE INDEX IF NOT EXISTS pay_bank_transfer_events_provider_reference_idx
  ON public.pay_bank_transfer_events (provider_reference);

CREATE INDEX IF NOT EXISTS pay_bank_transfer_events_normalised_state_idx
  ON public.pay_bank_transfer_events (normalised_state);

CREATE INDEX IF NOT EXISTS pay_bank_transfer_events_movement_class_idx
  ON public.pay_bank_transfer_events (movement_classification);

CREATE INDEX IF NOT EXISTS pay_bank_transfer_events_correction_disp_idx
  ON public.pay_bank_transfer_events (correction_disposition);

CREATE TABLE IF NOT EXISTS public.pay_payment_correction_requests (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pay_batch_id uuid NOT NULL REFERENCES public.pay_batches(id),
  correction_kind text NOT NULL,
  status text NOT NULL,
  requested_by_user_id uuid NULL REFERENCES public.tms_users(id),
  requested_at_utc timestamptz NOT NULL DEFAULT now(),
  required_quantity integer NOT NULL DEFAULT 1,
  approved_count integer NOT NULL DEFAULT 0,
  golden_key_used boolean NOT NULL DEFAULT false,
  golden_key_user_id uuid NULL REFERENCES public.tms_users(id),
  reason text NULL,
  selection_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  selection_hash text NOT NULL,
  plan_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  plan_hash text NOT NULL,
  accepted_resolution_json jsonb NULL,
  accepted_resolution_hash text NULL,
  source_bank_event_id uuid NULL REFERENCES public.pay_bank_transfer_events(id),
  auto_requested boolean NOT NULL DEFAULT false,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  authorised_at_utc timestamptz NULL,
  applied_at_utc timestamptz NULL,
  cancelled_at_utc timestamptz NULL,
  updated_at_utc timestamptz NOT NULL DEFAULT now()
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_requests'::regclass
      AND conname = 'pay_payment_correction_requests_kind_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_requests
      ADD CONSTRAINT pay_payment_correction_requests_kind_chk
      CHECK (correction_kind IN (
        'PRE_BANK_CANCEL',
        'NO_MONEY_UNWIND',
        'SETTLED_REVERSAL',
        'MANUAL_EVIDENCE_NO_MONEY',
        'MANUAL_EVIDENCE_SETTLED_RETURN'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_requests'::regclass
      AND conname = 'pay_payment_correction_requests_status_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_requests
      ADD CONSTRAINT pay_payment_correction_requests_status_chk
      CHECK (status IN (
        'REQUESTED',
        'AWAITING_AUTHORISATION',
        'AUTHORISED',
        'EXPANDED',
        'PROCESSING',
        'APPLIED',
        'APPLIED_WITH_BLOCKERS',
        'BLOCKED',
        'FAILED',
        'REJECTED',
        'CANCELLED'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_requests'::regclass
      AND conname = 'pay_payment_correction_requests_required_qty_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_requests
      ADD CONSTRAINT pay_payment_correction_requests_required_qty_chk
      CHECK (required_quantity >= 1);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_requests'::regclass
      AND conname = 'pay_payment_correction_requests_approved_count_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_requests
      ADD CONSTRAINT pay_payment_correction_requests_approved_count_chk
      CHECK (approved_count >= 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_requests'::regclass
      AND conname = 'pay_payment_correction_requests_selection_obj_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_requests
      ADD CONSTRAINT pay_payment_correction_requests_selection_obj_chk
      CHECK (jsonb_typeof(selection_json) = 'object');
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_requests'::regclass
      AND conname = 'pay_payment_correction_requests_plan_obj_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_requests
      ADD CONSTRAINT pay_payment_correction_requests_plan_obj_chk
      CHECK (jsonb_typeof(plan_json) = 'object');
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS pay_payment_correction_requests_batch_status_idx
  ON public.pay_payment_correction_requests (pay_batch_id, status);

CREATE INDEX IF NOT EXISTS pay_payment_correction_requests_status_created_idx
  ON public.pay_payment_correction_requests (status, created_at_utc);

CREATE INDEX IF NOT EXISTS pay_payment_correction_requests_source_event_idx
  ON public.pay_payment_correction_requests (source_bank_event_id);

CREATE UNIQUE INDEX IF NOT EXISTS pay_payment_correction_requests_open_scope_uidx
  ON public.pay_payment_correction_requests (pay_batch_id, selection_hash, correction_kind)
  WHERE status IN ('REQUESTED', 'AWAITING_AUTHORISATION', 'AUTHORISED', 'EXPANDED', 'PROCESSING');

CREATE TABLE IF NOT EXISTS public.pay_payment_correction_actions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  correction_request_id uuid NOT NULL REFERENCES public.pay_payment_correction_requests(id),
  pay_batch_id uuid NOT NULL REFERENCES public.pay_batches(id),
  actor_kind text NOT NULL,
  actor_user_id uuid NULL REFERENCES public.tms_users(id),
  action text NOT NULL,
  action_at_utc timestamptz NOT NULL DEFAULT now(),
  note text NULL,
  before_json jsonb NULL,
  after_json jsonb NULL,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_actions'::regclass
      AND conname = 'pay_payment_correction_actions_actor_kind_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_actions
      ADD CONSTRAINT pay_payment_correction_actions_actor_kind_chk
      CHECK (actor_kind IN ('USER', 'SYSTEM'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_actions'::regclass
      AND conname = 'pay_payment_correction_actions_action_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_actions
      ADD CONSTRAINT pay_payment_correction_actions_action_chk
      CHECK (action IN (
        'REQUEST',
        'AUTHORISE',
        'USE_GOLDEN_KEY',
        'REJECT',
        'CANCEL',
        'EXPAND_WORK',
        'APPLY',
        'APPLY_FAILED',
        'BLOCK',
        'RETRY'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_actions'::regclass
      AND conname = 'pay_payment_correction_actions_metadata_obj_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_actions
      ADD CONSTRAINT pay_payment_correction_actions_metadata_obj_chk
      CHECK (jsonb_typeof(metadata_json) = 'object');
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS pay_payment_correction_actions_request_action_idx
  ON public.pay_payment_correction_actions (correction_request_id, action_at_utc);

CREATE INDEX IF NOT EXISTS pay_payment_correction_actions_batch_action_idx
  ON public.pay_payment_correction_actions (pay_batch_id, action_at_utc);

CREATE INDEX IF NOT EXISTS pay_payment_correction_actions_actor_action_idx
  ON public.pay_payment_correction_actions (actor_user_id, action_at_utc);

CREATE TABLE IF NOT EXISTS public.pay_payment_correction_items (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  correction_request_id uuid NOT NULL REFERENCES public.pay_payment_correction_requests(id),
  pay_batch_id uuid NOT NULL REFERENCES public.pay_batches(id),
  pay_batch_candidate_id uuid NULL REFERENCES public.pay_batch_candidates(id),
  candidate_id uuid NULL REFERENCES public.candidates(id),
  pay_batch_item_id uuid NULL REFERENCES public.pay_batch_items(id),
  pay_bank_transfer_id uuid NULL REFERENCES public.pay_bank_transfers(id),
  timesheet_id uuid NULL,
  finance_case_id uuid NULL REFERENCES public.pay_advances(id),
  finance_component_id uuid NULL REFERENCES public.pay_finance_case_components(id),
  reservation_id uuid NULL REFERENCES public.pay_advance_reservations(id),
  item_type text NULL,
  correction_item_kind text NOT NULL,
  source_amount numeric NULL,
  amount_ex_vat numeric NULL,
  amount_vat numeric NULL,
  amount_inc_vat numeric NULL,
  economic_key_type text NULL,
  economic_key_value text NULL,
  before_snapshot_json jsonb NULL,
  after_snapshot_json jsonb NULL,
  status text NOT NULL DEFAULT 'APPLIED',
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  applied_at_utc timestamptz NULL
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_items'::regclass
      AND conname = 'pay_payment_correction_items_kind_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_items
      ADD CONSTRAINT pay_payment_correction_items_kind_chk
      CHECK (correction_item_kind IN (
        'PRE_BANK_CANCEL',
        'NO_MONEY_UNWIND',
        'SETTLED_REVERSAL'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_items'::regclass
      AND conname = 'pay_payment_correction_items_status_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_items
      ADD CONSTRAINT pay_payment_correction_items_status_chk
      CHECK (status IN (
        'PLANNED',
        'APPLIED',
        'SKIPPED',
        'BLOCKED',
        'FAILED'
      ));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS pay_payment_correction_items_batch_candidate_idx
  ON public.pay_payment_correction_items (pay_batch_id, pay_batch_candidate_id);

CREATE INDEX IF NOT EXISTS pay_payment_correction_items_item_idx
  ON public.pay_payment_correction_items (pay_batch_item_id);

CREATE INDEX IF NOT EXISTS pay_payment_correction_items_transfer_idx
  ON public.pay_payment_correction_items (pay_bank_transfer_id);

CREATE INDEX IF NOT EXISTS pay_payment_correction_items_candidate_idx
  ON public.pay_payment_correction_items (candidate_id);

CREATE INDEX IF NOT EXISTS pay_payment_correction_items_timesheet_key_idx
  ON public.pay_payment_correction_items (timesheet_id, economic_key_type, economic_key_value);

CREATE INDEX IF NOT EXISTS pay_payment_correction_items_finance_case_idx
  ON public.pay_payment_correction_items (finance_case_id);

CREATE INDEX IF NOT EXISTS pay_payment_correction_items_finance_component_idx
  ON public.pay_payment_correction_items (finance_component_id);

CREATE INDEX IF NOT EXISTS pay_payment_correction_items_reservation_idx
  ON public.pay_payment_correction_items (reservation_id);

CREATE INDEX IF NOT EXISTS pay_payment_correction_items_request_idx
  ON public.pay_payment_correction_items (correction_request_id);

CREATE UNIQUE INDEX IF NOT EXISTS pay_payment_correction_items_applied_item_kind_uidx
  ON public.pay_payment_correction_items (pay_batch_item_id, correction_item_kind)
  WHERE status = 'APPLIED' AND pay_batch_item_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.pay_payment_correction_work_items (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  correction_request_id uuid NOT NULL REFERENCES public.pay_payment_correction_requests(id),
  pay_batch_id uuid NOT NULL REFERENCES public.pay_batches(id),
  pay_batch_candidate_id uuid NULL REFERENCES public.pay_batch_candidates(id),
  pay_bank_transfer_id uuid NULL REFERENCES public.pay_bank_transfers(id),
  candidate_id uuid NULL REFERENCES public.candidates(id),
  umbrella_id uuid NULL REFERENCES public.umbrellas(id),
  work_kind text NOT NULL,
  selection_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  selection_hash text NOT NULL,
  status text NOT NULL DEFAULT 'PENDING',
  attempt_count integer NOT NULL DEFAULT 0,
  last_error text NULL,
  locked_at_utc timestamptz NULL,
  locked_by text NULL,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  processed_at_utc timestamptz NULL,
  result_json jsonb NOT NULL DEFAULT '{}'::jsonb
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_work_items'::regclass
      AND conname = 'pay_payment_correction_work_items_kind_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_work_items
      ADD CONSTRAINT pay_payment_correction_work_items_kind_chk
      CHECK (work_kind IN (
        'PRE_BANK_CANCEL',
        'NO_MONEY_UNWIND',
        'SETTLED_REVERSAL'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_work_items'::regclass
      AND conname = 'pay_payment_correction_work_items_status_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_work_items
      ADD CONSTRAINT pay_payment_correction_work_items_status_chk
      CHECK (status IN (
        'PENDING',
        'PROCESSING',
        'APPLIED',
        'SKIPPED',
        'BLOCKED',
        'FAILED_RETRYABLE',
        'FAILED_FINAL',
        'CANCELLED'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_work_items'::regclass
      AND conname = 'pay_payment_correction_work_items_attempt_count_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_work_items
      ADD CONSTRAINT pay_payment_correction_work_items_attempt_count_chk
      CHECK (attempt_count >= 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_work_items'::regclass
      AND conname = 'pay_payment_correction_work_items_selection_obj_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_work_items
      ADD CONSTRAINT pay_payment_correction_work_items_selection_obj_chk
      CHECK (jsonb_typeof(selection_json) = 'object');
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_work_items'::regclass
      AND conname = 'pay_payment_correction_work_items_result_obj_chk'
  ) THEN
    ALTER TABLE public.pay_payment_correction_work_items
      ADD CONSTRAINT pay_payment_correction_work_items_result_obj_chk
      CHECK (jsonb_typeof(result_json) = 'object');
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS pay_payment_correction_work_items_request_status_idx
  ON public.pay_payment_correction_work_items (correction_request_id, status);

CREATE INDEX IF NOT EXISTS pay_payment_correction_work_items_status_created_idx
  ON public.pay_payment_correction_work_items (status, created_at_utc);

CREATE INDEX IF NOT EXISTS pay_payment_correction_work_items_batch_idx
  ON public.pay_payment_correction_work_items (pay_batch_id);

CREATE INDEX IF NOT EXISTS pay_payment_correction_work_items_candidate_idx
  ON public.pay_payment_correction_work_items (candidate_id);

CREATE INDEX IF NOT EXISTS pay_payment_correction_work_items_transfer_idx
  ON public.pay_payment_correction_work_items (pay_bank_transfer_id);

CREATE INDEX IF NOT EXISTS pay_payment_correction_work_items_umbrella_idx
  ON public.pay_payment_correction_work_items (umbrella_id);

CREATE UNIQUE INDEX IF NOT EXISTS pay_payment_correction_work_items_scope_uidx
  ON public.pay_payment_correction_work_items (correction_request_id, work_kind, selection_hash);

CREATE TABLE IF NOT EXISTS public.pay_payment_return_notice_groups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pay_batch_id uuid NULL REFERENCES public.pay_batches(id),
  execution_commit_ref text NULL,
  provider_key text NULL,
  event_source text NULL,
  notice_kind text NOT NULL,
  status text NOT NULL DEFAULT 'OPEN',
  quiet_until_utc timestamptz NOT NULL,
  max_send_at_utc timestamptz NOT NULL,
  summary_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  mail_outbox_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  sent_at_utc timestamptz NULL
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_return_notice_groups'::regclass
      AND conname = 'pay_payment_return_notice_groups_kind_chk'
  ) THEN
    ALTER TABLE public.pay_payment_return_notice_groups
      ADD CONSTRAINT pay_payment_return_notice_groups_kind_chk
      CHECK (notice_kind IN (
        'BANK_FAILURE_DETECTED',
        'NO_MONEY_UNWIND_REQUIRED',
        'NO_MONEY_UNWIND_APPLIED',
        'SETTLED_RETURN_DETECTED',
        'SETTLED_REVERSAL_REQUIRED',
        'SETTLED_REVERSAL_APPLIED',
        'AUTO_CORRECTION_BLOCKED',
        'MANUAL_CORRECTION_APPLIED'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_return_notice_groups'::regclass
      AND conname = 'pay_payment_return_notice_groups_status_chk'
  ) THEN
    ALTER TABLE public.pay_payment_return_notice_groups
      ADD CONSTRAINT pay_payment_return_notice_groups_status_chk
      CHECK (status IN (
        'OPEN',
        'READY',
        'SENT',
        'CANCELLED',
        'FAILED'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_return_notice_groups'::regclass
      AND conname = 'pay_payment_return_notice_groups_send_window_chk'
  ) THEN
    ALTER TABLE public.pay_payment_return_notice_groups
      ADD CONSTRAINT pay_payment_return_notice_groups_send_window_chk
      CHECK (max_send_at_utc >= quiet_until_utc);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_return_notice_groups'::regclass
      AND conname = 'pay_payment_return_notice_groups_summary_obj_chk'
  ) THEN
    ALTER TABLE public.pay_payment_return_notice_groups
      ADD CONSTRAINT pay_payment_return_notice_groups_summary_obj_chk
      CHECK (jsonb_typeof(summary_json) = 'object');
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_return_notice_groups'::regclass
      AND conname = 'pay_payment_return_notice_groups_mail_ids_arr_chk'
  ) THEN
    ALTER TABLE public.pay_payment_return_notice_groups
      ADD CONSTRAINT pay_payment_return_notice_groups_mail_ids_arr_chk
      CHECK (jsonb_typeof(mail_outbox_ids) = 'array');
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS pay_payment_return_notice_groups_status_quiet_idx
  ON public.pay_payment_return_notice_groups (status, quiet_until_utc);

CREATE INDEX IF NOT EXISTS pay_payment_return_notice_groups_status_max_idx
  ON public.pay_payment_return_notice_groups (status, max_send_at_utc);

CREATE INDEX IF NOT EXISTS pay_payment_return_notice_groups_batch_idx
  ON public.pay_payment_return_notice_groups (pay_batch_id);

CREATE INDEX IF NOT EXISTS pay_payment_return_notice_groups_provider_commit_idx
  ON public.pay_payment_return_notice_groups (provider_key, execution_commit_ref);

COMMIT;
