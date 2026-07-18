-- Banking alert lifecycle repair.
-- Policy X: success alert amounts/counts are captured only from frozen pay batch,
-- pay batch display summary, and pay bank transfer artifacts. No live finance
-- component identity or economic-key fallback is introduced.

CREATE TABLE IF NOT EXISTS public.banking_alert_success_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pay_batch_id uuid NOT NULL REFERENCES public.pay_batches(id) ON DELETE CASCADE,
  alert_kind text NOT NULL,
  event_key text NOT NULL,
  payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  occurred_at_utc timestamptz NOT NULL DEFAULT now(),
  expires_at_utc timestamptz NOT NULL DEFAULT (now() + interval '365 days'),
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT banking_alert_success_events_kind_chk CHECK (
    UPPER(BTRIM(alert_kind)) IN ('BATCH_SCHEDULED_SUCCESS', 'BATCH_SETTLED_SUCCESS')
  ),
  CONSTRAINT banking_alert_success_events_event_key_chk CHECK (NULLIF(BTRIM(event_key), '') IS NOT NULL),
  CONSTRAINT banking_alert_success_events_expiry_chk CHECK (expires_at_utc > occurred_at_utc),
  CONSTRAINT banking_alert_success_events_batch_kind_key_uk UNIQUE (pay_batch_id, alert_kind, event_key)
);

CREATE INDEX IF NOT EXISTS idx_banking_alert_success_events_active
  ON public.banking_alert_success_events (expires_at_utc, occurred_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_banking_alert_success_events_batch
  ON public.banking_alert_success_events (pay_batch_id, alert_kind, occurred_at_utc DESC);

ALTER TABLE public.banking_alert_success_events ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.banking_alert_success_events FROM anon, authenticated;

ALTER TABLE public.banking_alert_user_preferences
  ALTER COLUMN include_success_alerts SET DEFAULT true;

-- Previous code forcibly stored false and exposed no success-alert choice, so
-- these values were not user-selected. Enable the newly operational lifecycle
-- alerts for existing TEST/user preference rows; users can opt out afterwards.
UPDATE public.banking_alert_user_preferences
SET
  include_success_alerts = true,
  updated_at_utc = now()
WHERE include_success_alerts IS DISTINCT FROM true;

CREATE OR REPLACE FUNCTION public.banking_alert_success_event_capture_pay_batch()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_status text := UPPER(BTRIM(COALESCE(NEW.status, '')));
  v_schedule_kind text := UPPER(BTRIM(COALESCE(NEW.schedule_kind, '')));
  v_execution_mode text := UPPER(BTRIM(COALESCE(
    NEW.execution_intent_json ->> 'execution_mode',
    NEW.bank_csv_export_json ->> 'execution_mode',
    NEW.settlement_confirmation_json ->> 'execution_mode',
    ''
  )));
  v_is_csv_settlement boolean := false;
  v_became_future_scheduled boolean := false;
  v_became_settled boolean := false;
  v_total_bank_out numeric(14,2) := 0::numeric;
  v_individual_payment_count integer := 0;
  v_event_key text := NULL::text;
  v_event_at_utc timestamptz := now();
  v_local_at timestamp without time zone := NULL::timestamp;
  v_local_date_label text := NULL::text;
  v_local_time_label text := NULL::text;
  v_amount_label text := '£0.00'::text;
  v_payment_count_label text := '0 individual payments'::text;
  v_user_label text := NULL::text;
  v_user_description text := NULL::text;
  v_alert_kind text := NULL::text;
BEGIN
  v_is_csv_settlement := v_execution_mode LIKE 'CSV%'
    OR COALESCE(jsonb_typeof(NEW.bank_csv_export_json), 'null') = 'object'
       AND NEW.bank_csv_export_json <> '{}'::jsonb;

  v_became_future_scheduled := v_status = 'SCHEDULED'
    AND v_schedule_kind = 'SCHEDULED'
    AND NEW.scheduled_at_utc IS NOT NULL
    AND NEW.scheduled_at_utc > now()
    AND v_is_csv_settlement IS NOT TRUE
    AND (
      TG_OP = 'INSERT'
      OR CASE WHEN TG_OP = 'UPDATE' THEN
        UPPER(BTRIM(COALESCE(OLD.status, ''))) IS DISTINCT FROM v_status
        OR UPPER(BTRIM(COALESCE(OLD.schedule_kind, ''))) IS DISTINCT FROM v_schedule_kind
        OR OLD.scheduled_at_utc IS DISTINCT FROM NEW.scheduled_at_utc
      ELSE false END
    );

  v_became_settled := v_status = 'SETTLED'
    AND (
      TG_OP = 'INSERT'
      OR CASE WHEN TG_OP = 'UPDATE' THEN
        UPPER(BTRIM(COALESCE(OLD.status, ''))) IS DISTINCT FROM 'SETTLED'
      ELSE false END
    );

  IF v_became_future_scheduled IS NOT TRUE AND v_became_settled IS NOT TRUE THEN
    RETURN NEW;
  END IF;

  SELECT
    COALESCE(
      NULLIF(NEW.total_bank_out, 0::numeric),
      NULLIF(display_summary.total_bank_out, 0::numeric),
      transfer_summary.total_bank_out,
      0::numeric
    )::numeric(14,2),
    COALESCE(
      NULLIF(display_summary.transfer_count, 0),
      transfer_summary.transfer_count,
      0
    )::integer
  INTO
    v_total_bank_out,
    v_individual_payment_count
  FROM (SELECT 1) AS one_row
  LEFT JOIN public.pay_batch_display_summary AS display_summary
    ON display_summary.pay_batch_id = NEW.id
  LEFT JOIN LATERAL (
    SELECT
      COUNT(*)::integer AS transfer_count,
      COALESCE(SUM(pay_bank_transfer.amount), 0::numeric)::numeric(14,2) AS total_bank_out
    FROM public.pay_bank_transfers AS pay_bank_transfer
    WHERE pay_bank_transfer.pay_batch_id = NEW.id
  ) AS transfer_summary ON true;

  v_amount_label := '£' || TO_CHAR(COALESCE(v_total_bank_out, 0::numeric), 'FM999,999,999,990.00');
  v_payment_count_label := COALESCE(v_individual_payment_count, 0)::text
    || CASE WHEN COALESCE(v_individual_payment_count, 0) = 1 THEN ' individual payment' ELSE ' individual payments' END;

  IF v_became_future_scheduled THEN
    v_alert_kind := 'BATCH_SCHEDULED_SUCCESS';
    v_event_at_utc := now();
    v_event_key := 'SCHEDULED:' || TO_CHAR(NEW.scheduled_at_utc AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISSUS');
    v_local_at := NEW.scheduled_at_utc AT TIME ZONE 'Europe/London';
    v_local_date_label := TO_CHAR(v_local_at, 'FMDD') || ' ' || TO_CHAR(v_local_at, 'FMMonth') || ' ' || TO_CHAR(v_local_at, 'YYYY');
    v_local_time_label := TO_CHAR(v_local_at, 'HH24:MI');
    v_user_label := 'Future payment batch scheduled';
    v_user_description := 'Payment batch scheduled successfully. '
      || v_amount_label || ' will be paid across ' || v_payment_count_label
      || ' on ' || v_local_date_label || ' at ' || v_local_time_label || ' (UK Time).';
  ELSE
    v_alert_kind := 'BATCH_SETTLED_SUCCESS';
    v_event_at_utc := COALESCE(NEW.completed_at_utc, now());
    v_event_key := 'SETTLED:' || TO_CHAR(v_event_at_utc AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISSUS');
    v_local_at := v_event_at_utc AT TIME ZONE 'Europe/London';
    v_local_date_label := TO_CHAR(v_local_at, 'FMDD') || ' ' || TO_CHAR(v_local_at, 'FMMonth') || ' ' || TO_CHAR(v_local_at, 'YYYY');
    v_local_time_label := TO_CHAR(v_local_at, 'HH24:MI');
    IF v_is_csv_settlement THEN
      v_user_label := 'CSV settlement recorded';
      v_user_description := 'CSV settlement recorded successfully. ' || v_amount_label || ' across '
        || v_payment_count_label || ' was marked settled on ' || v_local_date_label || ' at '
        || v_local_time_label || ' (UK Time). This records settlement only; CloudTMS did not transfer the money.';
    ELSE
      v_user_label := 'Payment batch settled';
      v_user_description := 'Payment batch settled successfully. ' || v_amount_label || ' across '
        || v_payment_count_label || ' completed on ' || v_local_date_label || ' at '
        || v_local_time_label || ' (UK Time).';
    END IF;
  END IF;

  INSERT INTO public.banking_alert_success_events (
    pay_batch_id,
    alert_kind,
    event_key,
    payload_json,
    occurred_at_utc,
    expires_at_utc
  )
  VALUES (
    NEW.id,
    v_alert_kind,
    v_event_key,
    jsonb_strip_nulls(jsonb_build_object(
      'alert_kind', v_alert_kind,
      'issue_kind', v_alert_kind,
      'alert_severity', 'info',
      'severity', 'info',
      'alert_candidate_is_success_only', true,
      'is_success_only', true,
      'stable_issue_key', NEW.id::text || ':' || v_alert_kind || ':' || v_event_key,
      'dedupe_key', NEW.id::text || ':' || v_alert_kind || ':' || v_event_key,
      'pay_batch_id', NEW.id::text,
      'entity_kind', 'pay_batch',
      'entity_id', NEW.id::text,
      'payment_lifecycle_state', CASE WHEN v_alert_kind = 'BATCH_SCHEDULED_SUCCESS' THEN 'SCHEDULED' ELSE 'SETTLED' END,
      'current_status', v_status,
      'schedule_kind', v_schedule_kind,
      'scheduled_at_utc', CASE WHEN NEW.scheduled_at_utc IS NULL THEN NULL::text ELSE NEW.scheduled_at_utc::text END,
      'settled_at_utc', CASE WHEN v_alert_kind = 'BATCH_SETTLED_SUCCESS' THEN v_event_at_utc::text ELSE NULL::text END,
      'execution_mode', NULLIF(v_execution_mode, ''),
      'csv_settlement', v_is_csv_settlement,
      'amount_gbp', v_total_bank_out,
      'individual_payment_count', v_individual_payment_count,
      'uk_date_label', v_local_date_label,
      'uk_time_label', v_local_time_label,
      'user_label', v_user_label,
      'user_description', v_user_description,
      'required_user_action', 'Review or clear this Banking alert.',
      'link_target', 'banking_pay_batch',
      'link_tab', 'current_payment_status',
      'policy_x_source', 'FROZEN_BATCH_ARTIFACTS'
    )),
    v_event_at_utc,
    v_event_at_utc + interval '365 days'
  )
  ON CONFLICT (pay_batch_id, alert_kind, event_key) DO NOTHING;

  RETURN NEW;
EXCEPTION
  WHEN OTHERS THEN
    RAISE WARNING 'BANKING_ALERT_SUCCESS_EVENT_CAPTURE_FAILED [%]', SQLSTATE;
    RETURN NEW;
END;
$function$;

DROP TRIGGER IF EXISTS trg_banking_alert_success_events_pay_batches_insert ON public.pay_batches;
CREATE TRIGGER trg_banking_alert_success_events_pay_batches_insert
AFTER INSERT ON public.pay_batches
FOR EACH ROW EXECUTE FUNCTION public.banking_alert_success_event_capture_pay_batch();

DROP TRIGGER IF EXISTS trg_banking_alert_success_events_pay_batches_update ON public.pay_batches;
CREATE TRIGGER trg_banking_alert_success_events_pay_batches_update
AFTER UPDATE OF status, schedule_kind, scheduled_at_utc, completed_at_utc, execution_intent_json, bank_csv_export_json, settlement_confirmation_json, total_bank_out
ON public.pay_batches
FOR EACH ROW EXECUTE FUNCTION public.banking_alert_success_event_capture_pay_batch();

DROP FUNCTION IF EXISTS public.banking_alerts_active_for_user(uuid, text, uuid, boolean, integer);

CREATE OR REPLACE FUNCTION public.banking_alerts_active_for_user(
  p_actor_user_id uuid,
  p_entity_kind text DEFAULT NULL::text,
  p_entity_id uuid DEFAULT NULL::uuid,
  p_include_acknowledged boolean DEFAULT false,
  p_limit integer DEFAULT 100,
  p_alert_context text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_entity_kind text := LOWER(NULLIF(BTRIM(COALESCE(p_entity_kind, '')), ''));
  v_limit integer := NULL::integer;
  v_result jsonb := '{}'::jsonb;
  v_alert_context text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_alert_context, '')), ''), '-', '_'));
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERTS_ACTIVE_FOR_USER_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERTS_ACTIVE_FOR_USER_ACTOR_REQUIRED')::text;
  END IF;

  IF v_alert_context IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERTS_ACTIVE_FOR_USER_CONTEXT_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERTS_ACTIVE_FOR_USER_CONTEXT_REQUIRED')::text;
  END IF;

  IF v_alert_context IN (
    'LIST','BATCH_LIST','PAY_BATCHES_LIST','BOOTSTRAP','BATCH_BOOTSTRAP','PAY_BATCH_GET_BOOTSTRAP_ONLY',
    'WATCH','WATCH_SIGNAL','LIVE_WATCH','PROGRESS','PROGRESS_POLLING','PREVIEW','PREVIEW_OPEN',
    'PREVIEW_PROGRESS','OPERATION_GET','OPERATION_PROGRESS','RPC_CHANGES_PING','CHANGES_PING'
  ) OR v_alert_context NOT IN (
    'ALERT_PANEL','ALERTS_PANEL','ALERT_MANAGEMENT','ALERT_REFRESH_JOB','EXPLICIT_ALERT_REFRESH','USER_TRIGGERED_ALERTS'
  ) THEN
    RAISE EXCEPTION 'BANKING_ALERTS_ACTIVE_FOR_USER_CONTEXT_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERTS_ACTIVE_FOR_USER_CONTEXT_NOT_ALLOWED', 'context', v_alert_context)::text;
  END IF;

  IF p_limit IS NULL THEN
    v_limit := 100;
  ELSIF p_limit = 0 THEN
    v_limit := NULL::integer;
  ELSE
    v_limit := LEAST(GREATEST(p_limit, 1), 500);
  END IF;

  WITH blocked_funds_base AS (
    SELECT
      blocked_funds_pay_batches.id AS pay_batch_id,
      blocked_funds_pay_batches.status AS batch_status,
      blocked_funds_pay_batches.execution_commit_state AS execution_commit_state,
      blocked_funds_pay_batches.execution_commit_ref AS execution_commit_ref,
      blocked_funds_pay_batches.execution_committed_at_utc AS execution_committed_at_utc,
      blocked_funds_pay_batches.last_funds_check_at_utc AS last_funds_check_at_utc,
      blocked_funds_pay_batches.last_funds_check_json AS last_funds_check_json,
      blocked_funds_pay_batches.funding_account_ref AS funding_account_ref,
      blocked_funds_pay_batches.rail_provider_snapshot AS rail_provider_snapshot,
      blocked_funds_pay_batches.rail_env_snapshot AS rail_env_snapshot,
      COALESCE(
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{required_gbp}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{required}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{required_amount_gbp}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{required_amount}'), '')
      ) AS required_gbp_text,
      COALESCE(
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{available_gbp}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{available}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{available_amount_gbp}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{available_amount}'), '')
      ) AS available_gbp_text,
      COALESCE(
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{funding_account_ref}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{account_ref}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{funding_account_id}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.funding_account_ref), '')
      ) AS resolved_funding_account_ref,
      COALESCE(
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{rail_provider}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{provider}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.rail_provider_snapshot), '')
      ) AS resolved_rail_provider,
      COALESCE(
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{rail_env}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{env}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.rail_env_snapshot), '')
      ) AS resolved_rail_env
    FROM public.pay_batches AS blocked_funds_pay_batches
    WHERE UPPER(BTRIM(COALESCE(blocked_funds_pay_batches.status, ''))) = 'BLOCKED_FUNDS'
      AND UPPER(BTRIM(COALESCE(blocked_funds_pay_batches.execution_commit_state, 'NOT_SUBMITTED'))) = 'NOT_SUBMITTED'
      AND NULLIF(BTRIM(COALESCE(blocked_funds_pay_batches.execution_commit_ref, '')), '') IS NULL
      AND blocked_funds_pay_batches.execution_committed_at_utc IS NULL
      AND blocked_funds_pay_batches.cancelled_at_utc IS NULL
      AND EXISTS (
        SELECT 1
        FROM public.pay_batch_candidates AS blocked_funds_candidate_exists
        JOIN public.pay_batch_items AS blocked_funds_item_exists
          ON blocked_funds_item_exists.pay_batch_candidate_id = blocked_funds_candidate_exists.id
        WHERE blocked_funds_candidate_exists.pay_batch_id = blocked_funds_pay_batches.id
          AND COALESCE(blocked_funds_item_exists.is_voided, false) = false
      )
  ),
  blocked_funds_alerts AS MATERIALIZED (
    SELECT
      'PROVIDER_OUTAGE_RETRY_LATER'::text AS alert_kind,
      'critical'::text AS severity,
      100::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      blocked_funds_base.pay_batch_id AS entity_id,
      blocked_funds_base.pay_batch_id AS pay_batch_id,
      NULL::text AS payload_source_kind,
      NULL::uuid AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'fingerprint_source_kind', 'pay_batch',
        'fingerprint_source_id', blocked_funds_base.pay_batch_id::text,
        'issue_kind', 'PROVIDER_OUTAGE_RETRY_LATER',
        'pay_batch_id', blocked_funds_base.pay_batch_id::text,
        'batch_status', UPPER(BTRIM(COALESCE(blocked_funds_base.batch_status, ''))),
        'execution_commit_state', UPPER(BTRIM(COALESCE(blocked_funds_base.execution_commit_state, 'NOT_SUBMITTED'))),
        'execution_commit_ref', NULLIF(BTRIM(COALESCE(blocked_funds_base.execution_commit_ref, '')), ''),
        'execution_committed_at_utc', CASE
          WHEN blocked_funds_base.execution_committed_at_utc IS NULL THEN NULL::text
          ELSE TO_CHAR(blocked_funds_base.execution_committed_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
        END,
        'last_funds_check_at_utc', CASE
          WHEN blocked_funds_base.last_funds_check_at_utc IS NULL THEN NULL::text
          ELSE TO_CHAR(blocked_funds_base.last_funds_check_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
        END,
        'required_gbp', CASE
          WHEN blocked_funds_base.required_gbp_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN blocked_funds_base.required_gbp_text::numeric
          ELSE NULL::numeric
        END,
        'available_gbp', CASE
          WHEN blocked_funds_base.available_gbp_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN blocked_funds_base.available_gbp_text::numeric
          ELSE NULL::numeric
        END,
        'funding_account_ref', blocked_funds_base.resolved_funding_account_ref,
        'rail_provider', blocked_funds_base.resolved_rail_provider,
        'rail_env', blocked_funds_base.resolved_rail_env
      )) AS fingerprint_payload_json,
      'Bank unavailable — unsent payments can be retried'::text AS label,
      'Bank unavailable — unsent payments can be retried'::text AS title,
      ('Bank unavailable — required '
        || COALESCE(NULLIF(BTRIM(blocked_funds_base.required_gbp_text), ''), '—')
        || ', available '
        || COALESCE(NULLIF(BTRIM(blocked_funds_base.available_gbp_text), ''), '—')
        || '. Payments were not sent to the bank/provider and can be retried from Overview.')::text AS description,
      'Open Banking Pay Overview and retry unsent payments.'::text AS action_guidance,
      blocked_funds_base.last_funds_check_at_utc AS sort_at_utc
    FROM blocked_funds_base
  ),
  bank_event_base AS MATERIALIZED (
    SELECT
      CASE
        WHEN (
            UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_failure_reason_group, ''))) = 'WEBHOOK_UNMATCHED'
            OR (
              UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.mapping_status, ''))) IN ('UNMATCHED','NO_MATCH')
              AND UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_event_transport, public.pay_bank_transfer_events.event_source, ''))) IN ('PROVIDER_WEBHOOK','FAILED_WEBHOOK_REPLAY','WEBHOOK')
            )
          )
          THEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.mapping_status, ''))) IN ('AMBIGUOUS','UNMATCHED','NO_MATCH','MULTIPLE_MATCHES')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.correction_disposition, ''))) IN ('AMBIGUOUS','ACTION_REQUIRED')
          THEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.correction_disposition, ''))) = 'FAILED'
          THEN 'PAYMENT_CORRECTION_FAILED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.correction_disposition, ''))) = 'BLOCKED'
          THEN 'PAYMENT_CORRECTION_BLOCKED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) IN ('RETURNED','REVERTED')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) IN ('RETURNED','REVERTED')
          THEN 'BANK_RETURNED_PAYMENT'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED')
          THEN 'BANK_REJECTED_PAYMENT'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) IN ('UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) LIKE 'CREATE_ERROR%'
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) IN ('UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) LIKE 'CREATE_ERROR%'
          THEN 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT'
        ELSE NULL::text
      END AS alert_kind,
      public.pay_bank_transfer_events.pay_batch_id AS pay_batch_id,
      public.pay_bank_transfer_events.id AS source_id,
      public.pay_bank_transfer_events.normalised_state AS normalised_state,
      public.pay_bank_transfer_events.provider_state AS provider_state,
      public.pay_bank_transfer_events.mapping_status AS mapping_status,
      public.pay_bank_transfer_events.correction_disposition AS correction_disposition,
      public.pay_bank_transfer_events.event_time_utc AS event_time_utc,
      public.pay_bank_transfer_events.received_at_utc AS received_at_utc,
      public.pay_bank_transfer_events.created_at_utc AS created_at_utc,
      COALESCE(NULLIF(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_key, '')), ''), NULLIF(BTRIM(COALESCE(bank_event_batches.rail_provider_snapshot, '')), ''), 'UNKNOWN_PROVIDER') AS provider_key,
      COALESCE(NULLIF(BTRIM(COALESCE(public.pay_bank_transfer_events.rail_env, '')), ''), NULLIF(BTRIM(COALESCE(bank_event_batches.rail_env_snapshot, '')), ''), 'PROD') AS rail_env,
      NULLIF(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_event_key, '')), '') AS provider_event_key,
      NULLIF(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_failure_reason_code, '')), '') AS provider_failure_reason_code,
      NULLIF(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_failure_reason_group, '')), '') AS provider_failure_reason_group,
      public.pay_bank_transfer_events.provider_webhook_receipt_id AS provider_webhook_receipt_id,
      COALESCE(public.pay_bank_transfer_events.event_time_utc, public.pay_bank_transfer_events.received_at_utc, public.pay_bank_transfer_events.created_at_utc) AS sort_at_utc
    FROM public.pay_bank_transfer_events
    JOIN public.pay_batches AS bank_event_batches
      ON bank_event_batches.id = public.pay_bank_transfer_events.pay_batch_id
    WHERE UPPER(BTRIM(COALESCE(bank_event_batches.status, ''))) NOT IN ('CANCELLED','CANCELED')
      AND (
        UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) LIKE 'CREATE_ERROR%'
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) LIKE 'CREATE_ERROR%'
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.mapping_status, ''))) IN ('AMBIGUOUS','UNMATCHED','NO_MATCH','MULTIPLE_MATCHES')
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.correction_disposition, ''))) IN ('AMBIGUOUS','ACTION_REQUIRED','BLOCKED','FAILED')
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_requests AS resolved_event_corrections
        WHERE resolved_event_corrections.source_bank_event_id = public.pay_bank_transfer_events.id
          AND UPPER(BTRIM(COALESCE(resolved_event_corrections.status, ''))) IN ('APPLIED','RESOLVED')
      )
  ),
  bank_event_alerts AS MATERIALIZED (
    SELECT
      bank_event_base.alert_kind,
      'critical'::text AS severity,
      CASE bank_event_base.alert_kind
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 96
        WHEN 'BANK_RETURNED_PAYMENT' THEN 95
        WHEN 'BANK_REJECTED_PAYMENT' THEN 94
        WHEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED' THEN 90
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 88
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 87
        ELSE 80
      END::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      bank_event_base.pay_batch_id AS entity_id,
      bank_event_base.pay_batch_id AS pay_batch_id,
      'pay_bank_transfer_event'::text AS payload_source_kind,
      bank_event_base.source_id AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'fingerprint_source_kind', 'pay_bank_transfer_event',
        'fingerprint_source_id', bank_event_base.source_id::text,
        'issue_kind', bank_event_base.alert_kind,
        'alert_kind', bank_event_base.alert_kind,
        'pay_batch_id', bank_event_base.pay_batch_id::text,
        'provider_key', bank_event_base.provider_key,
        'rail_provider', bank_event_base.provider_key,
        'rail_env', bank_event_base.rail_env,
        'provider_event_key', bank_event_base.provider_event_key,
        'provider_webhook_receipt_id', CASE WHEN bank_event_base.provider_webhook_receipt_id IS NULL THEN NULL ELSE bank_event_base.provider_webhook_receipt_id::text END,
        'provider_failure_reason_code', bank_event_base.provider_failure_reason_code,
        'provider_failure_reason_group', CASE WHEN bank_event_base.alert_kind = 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN COALESCE(bank_event_base.provider_failure_reason_group, 'WEBHOOK_UNMATCHED') ELSE bank_event_base.provider_failure_reason_group END,
        'normalised_state', NULLIF(BTRIM(COALESCE(bank_event_base.normalised_state, '')), ''),
        'provider_state', NULLIF(BTRIM(COALESCE(bank_event_base.provider_state, '')), ''),
        'mapping_status', NULLIF(BTRIM(COALESCE(bank_event_base.mapping_status, '')), ''),
        'correction_disposition', NULLIF(BTRIM(COALESCE(bank_event_base.correction_disposition, '')), ''),
        'event_time_utc', CASE WHEN bank_event_base.event_time_utc IS NULL THEN NULL::text ELSE TO_CHAR(bank_event_base.event_time_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END,
        'received_at_utc', CASE WHEN bank_event_base.received_at_utc IS NULL THEN NULL::text ELSE TO_CHAR(bank_event_base.received_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END
      )) AS fingerprint_payload_json,
      CASE bank_event_base.alert_kind
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Unmatched bank webhook'
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Provider outcome unknown — check provider'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'Failed payments — Rewind financials available'
        WHEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED' THEN 'Payment needs review'
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'Payment correction failed'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'Payment correction blocked'
        ELSE 'Rail submission needs review'
      END::text AS label,
      CASE bank_event_base.alert_kind
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Unmatched provider webhook needs review'
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Provider outcome unknown — check provider'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'Failed payments — Rewind financials available'
        WHEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED' THEN 'Payment event needs review'
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'Payment correction failed'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'Payment correction blocked'
        ELSE 'Rail submission needs review'
      END::text AS title,
      CASE bank_event_base.alert_kind
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'A verified provider webhook could not be matched safely and needs review.'
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Provider returned/reverted status requires checking before any financial correction.'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'The provider/bank outcome indicates no money moved. Open Current Payment Status and rewind financials where safe.'
        WHEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED' THEN 'A bank payment event could not be matched safely and needs review.'
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'CloudTMS could not complete a payment correction automatically.'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'A payment correction is blocked and needs review.'
        ELSE 'CloudTMS could not confirm the final bank submission state.'
      END::text AS description,
      CASE bank_event_base.alert_kind
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Review the unmatched provider webhook.'
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Open Current Payment Status and review the payment status.'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'Open Current Payment Status and rewind financials where no money moved.'
        WHEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED' THEN 'Review and resolve the ambiguous bank event.'
        ELSE 'Open Current Payment Status.'
      END::text AS action_guidance,
      bank_event_base.sort_at_utc
    FROM bank_event_base
    WHERE bank_event_base.alert_kind IS NOT NULL
  ),
  transfer_base AS MATERIALIZED (
    SELECT
      CASE
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) IN ('RETURNED','REVERTED')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) IN ('RETURNED','REVERTED') THEN 'BANK_RETURNED_PAYMENT'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT')
          OR NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.failed_reason, '')), '') IS NOT NULL THEN 'BANK_REJECTED_PAYMENT'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) IN ('UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) LIKE 'CREATE_ERROR%'
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) IN ('UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) LIKE 'CREATE_ERROR%' THEN 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT'
        ELSE NULL::text
      END AS alert_kind,
      public.pay_bank_transfers.pay_batch_id AS pay_batch_id,
      public.pay_bank_transfers.id AS source_id,
      public.pay_bank_transfers.status AS transfer_status,
      public.pay_bank_transfers.rail_state AS rail_state,
      public.pay_bank_transfers.failed_reason AS failed_reason,
      public.pay_bank_transfers.completed_at_utc AS completed_at_utc,
      public.pay_bank_transfers.created_at_utc AS created_at_utc,
      COALESCE(public.pay_bank_transfers.completed_at_utc, public.pay_bank_transfers.created_at_utc) AS sort_at_utc
    FROM public.pay_bank_transfers
    JOIN public.pay_batches AS transfer_batches
      ON transfer_batches.id = public.pay_bank_transfers.pay_batch_id
    WHERE UPPER(BTRIM(COALESCE(transfer_batches.status, ''))) NOT IN ('CANCELLED','CANCELED')
      AND (
        UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) LIKE 'CREATE_ERROR%'
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) LIKE 'CREATE_ERROR%'
        OR NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.failed_reason, '')), '') IS NOT NULL
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_bank_transfer_events AS transfer_resolution_events
        WHERE transfer_resolution_events.pay_bank_transfer_id = public.pay_bank_transfers.id
          AND UPPER(BTRIM(COALESCE(transfer_resolution_events.correction_disposition, ''))) IN ('NO_CORRECTION_REQUIRED','AUTO_APPLIED','APPLIED','RESOLVED')
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_work_items AS transfer_resolved_work_items
        JOIN public.pay_payment_correction_requests AS transfer_resolved_requests
          ON transfer_resolved_requests.id = transfer_resolved_work_items.correction_request_id
        WHERE transfer_resolved_work_items.pay_bank_transfer_id = public.pay_bank_transfers.id
          AND UPPER(BTRIM(COALESCE(transfer_resolved_requests.status, ''))) IN ('APPLIED','RESOLVED')
      )
  ),
  transfer_alerts AS MATERIALIZED (
    SELECT
      transfer_base.alert_kind,
      'critical'::text AS severity,
      CASE transfer_base.alert_kind
        WHEN 'BANK_RETURNED_PAYMENT' THEN 93
        WHEN 'BANK_REJECTED_PAYMENT' THEN 92
        ELSE 79
      END::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      transfer_base.pay_batch_id AS entity_id,
      transfer_base.pay_batch_id AS pay_batch_id,
      'pay_bank_transfer'::text AS payload_source_kind,
      transfer_base.source_id AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'fingerprint_source_kind', 'pay_bank_transfer',
        'fingerprint_source_id', transfer_base.source_id::text,
        'issue_kind', transfer_base.alert_kind,
        'pay_batch_id', transfer_base.pay_batch_id::text,
        'status', NULLIF(BTRIM(COALESCE(transfer_base.transfer_status, '')), ''),
        'rail_state', NULLIF(BTRIM(COALESCE(transfer_base.rail_state, '')), ''),
        'failed_reason_hash', CASE WHEN NULLIF(BTRIM(COALESCE(transfer_base.failed_reason, '')), '') IS NULL THEN NULL::text ELSE MD5(transfer_base.failed_reason) END,
        'completed_at_utc', CASE WHEN transfer_base.completed_at_utc IS NULL THEN NULL::text ELSE TO_CHAR(transfer_base.completed_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END,
        'created_at_utc', CASE WHEN transfer_base.created_at_utc IS NULL THEN NULL::text ELSE TO_CHAR(transfer_base.created_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END
      )) AS fingerprint_payload_json,
      CASE transfer_base.alert_kind
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Provider outcome unknown — check provider'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'Failed payments — Rewind financials available'
        ELSE 'Rail submission needs review'
      END::text AS label,
      CASE transfer_base.alert_kind
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Provider outcome unknown — check provider'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'Failed payments — Rewind financials available'
        ELSE 'Rail submission needs review'
      END::text AS title,
      CASE transfer_base.alert_kind
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Provider returned/reverted status requires checking before any financial correction.'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'The bank rejected a payment transfer.'
        ELSE 'CloudTMS could not confirm the final bank submission state.'
      END::text AS description,
      CASE transfer_base.alert_kind
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Open Current Payment Status and review the payment status.'
        ELSE 'Open Current Payment Status.'
      END::text AS action_guidance,
      transfer_base.sort_at_utc
    FROM transfer_base
    WHERE transfer_base.alert_kind IS NOT NULL
  ),
  correction_request_base AS MATERIALIZED (
    SELECT
      CASE
        WHEN UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) IN ('FAILED','FAILED_RETRYABLE','FAILED_FINAL') THEN 'PAYMENT_CORRECTION_FAILED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) IN ('BLOCKED','APPLIED_WITH_BLOCKERS') THEN 'PAYMENT_CORRECTION_BLOCKED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) IN ('REQUESTED','AWAITING_AUTHORISATION','AWAITING_AUTHORIZATION','PENDING_APPROVAL') THEN 'PAYMENT_CORRECTION_AWAITING_APPROVAL'
        ELSE NULL::text
      END AS alert_kind,
      public.pay_payment_correction_requests.pay_batch_id AS pay_batch_id,
      public.pay_payment_correction_requests.id AS source_id,
      public.pay_payment_correction_requests.status AS request_status,
      public.pay_payment_correction_requests.correction_kind AS correction_kind,
      public.pay_payment_correction_requests.selection_hash AS selection_hash,
      public.pay_payment_correction_requests.updated_at_utc AS updated_at_utc,
      public.pay_payment_correction_requests.created_at_utc AS created_at_utc,
      public.pay_payment_correction_requests.requested_at_utc AS requested_at_utc,
      COALESCE(public.pay_payment_correction_requests.updated_at_utc, public.pay_payment_correction_requests.created_at_utc, public.pay_payment_correction_requests.requested_at_utc) AS sort_at_utc
    FROM public.pay_payment_correction_requests
    JOIN public.pay_batches AS correction_request_batches
      ON correction_request_batches.id = public.pay_payment_correction_requests.pay_batch_id
    WHERE UPPER(BTRIM(COALESCE(correction_request_batches.status, ''))) NOT IN ('CANCELLED','CANCELED')
      AND UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) IN ('FAILED','FAILED_RETRYABLE','FAILED_FINAL','BLOCKED','APPLIED_WITH_BLOCKERS','REQUESTED','AWAITING_AUTHORISATION','AWAITING_AUTHORIZATION','PENDING_APPROVAL')
  ),
  correction_request_alerts AS MATERIALIZED (
    SELECT
      correction_request_base.alert_kind,
      'critical'::text AS severity,
      CASE correction_request_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 88
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 87
        ELSE 75
      END::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      correction_request_base.pay_batch_id AS entity_id,
      correction_request_base.pay_batch_id AS pay_batch_id,
      'pay_payment_correction_request'::text AS payload_source_kind,
      correction_request_base.source_id AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'fingerprint_source_kind', 'pay_payment_correction_request',
        'fingerprint_source_id', correction_request_base.source_id::text,
        'issue_kind', correction_request_base.alert_kind,
        'pay_batch_id', correction_request_base.pay_batch_id::text,
        'status', NULLIF(BTRIM(COALESCE(correction_request_base.request_status, '')), ''),
        'correction_kind', NULLIF(BTRIM(COALESCE(correction_request_base.correction_kind, '')), ''),
        'selection_hash', NULLIF(BTRIM(COALESCE(correction_request_base.selection_hash, '')), '')
      )) AS fingerprint_payload_json,
      CASE correction_request_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'Payment correction failed'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'Payment correction blocked'
        ELSE 'Payment correction awaiting approval'
      END::text AS label,
      CASE correction_request_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'Payment correction failed'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'Payment correction blocked'
        ELSE 'Payment correction awaiting approval'
      END::text AS title,
      CASE correction_request_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'CloudTMS could not complete a payment correction automatically.'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'A payment correction is blocked and needs review.'
        ELSE 'A payment correction is awaiting approval.'
      END::text AS description,
      CASE correction_request_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_AWAITING_APPROVAL' THEN 'Review and approve or reject the correction request.'
        ELSE 'Open Current Payment Status.'
      END::text AS action_guidance,
      correction_request_base.sort_at_utc
    FROM correction_request_base
    WHERE correction_request_base.alert_kind IS NOT NULL
  ),
  correction_work_base AS MATERIALIZED (
    SELECT
      CASE
        WHEN UPPER(BTRIM(COALESCE(public.pay_payment_correction_work_items.status, ''))) IN ('FAILED','FAILED_RETRYABLE','FAILED_FINAL') THEN 'PAYMENT_CORRECTION_FAILED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_payment_correction_work_items.status, ''))) IN ('BLOCKED','APPLIED_WITH_BLOCKERS') THEN 'PAYMENT_CORRECTION_BLOCKED'
        ELSE NULL::text
      END AS alert_kind,
      public.pay_payment_correction_work_items.pay_batch_id AS pay_batch_id,
      public.pay_payment_correction_work_items.id AS source_id,
      public.pay_payment_correction_work_items.status AS work_status,
      public.pay_payment_correction_work_items.work_kind AS work_kind,
      public.pay_payment_correction_work_items.selection_hash AS selection_hash,
      public.pay_payment_correction_work_items.processed_at_utc AS processed_at_utc,
      public.pay_payment_correction_work_items.created_at_utc AS created_at_utc,
      COALESCE(public.pay_payment_correction_work_items.processed_at_utc, public.pay_payment_correction_work_items.created_at_utc) AS sort_at_utc
    FROM public.pay_payment_correction_work_items
    JOIN public.pay_batches AS correction_work_batches
      ON correction_work_batches.id = public.pay_payment_correction_work_items.pay_batch_id
    WHERE UPPER(BTRIM(COALESCE(correction_work_batches.status, ''))) NOT IN ('CANCELLED','CANCELED')
      AND UPPER(BTRIM(COALESCE(public.pay_payment_correction_work_items.status, ''))) IN ('FAILED','FAILED_RETRYABLE','FAILED_FINAL','BLOCKED','APPLIED_WITH_BLOCKERS')
  ),
  correction_work_alerts AS MATERIALIZED (
    SELECT
      correction_work_base.alert_kind,
      'critical'::text AS severity,
      CASE correction_work_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 86
        ELSE 85
      END::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      correction_work_base.pay_batch_id AS entity_id,
      correction_work_base.pay_batch_id AS pay_batch_id,
      'pay_payment_correction_work_item'::text AS payload_source_kind,
      correction_work_base.source_id AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'fingerprint_source_kind', 'pay_payment_correction_work_item',
        'fingerprint_source_id', correction_work_base.source_id::text,
        'issue_kind', correction_work_base.alert_kind,
        'pay_batch_id', correction_work_base.pay_batch_id::text,
        'status', NULLIF(BTRIM(COALESCE(correction_work_base.work_status, '')), ''),
        'work_kind', NULLIF(BTRIM(COALESCE(correction_work_base.work_kind, '')), ''),
        'selection_hash', NULLIF(BTRIM(COALESCE(correction_work_base.selection_hash, '')), '')
      )) AS fingerprint_payload_json,
      CASE correction_work_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'Payment correction failed'
        ELSE 'Payment correction blocked'
      END::text AS label,
      CASE correction_work_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'Payment correction failed'
        ELSE 'Payment correction blocked'
      END::text AS title,
      CASE correction_work_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'CloudTMS could not complete a payment correction work item automatically.'
        ELSE 'A payment correction work item is blocked and needs review.'
      END::text AS description,
      'Open Current Payment Status.'::text AS action_guidance,
      correction_work_base.sort_at_utc
    FROM correction_work_base
    WHERE correction_work_base.alert_kind IS NOT NULL
  ),
  remittance_base AS MATERIALIZED (
    SELECT
      public.mail_outbox.context_id AS pay_batch_id,
      public.mail_outbox.id AS source_id,
      public.mail_outbox.reference AS reference,
      public.mail_outbox.failed_at AS failed_at,
      public.mail_outbox.created_at_utc AS created_at_utc,
      COALESCE(public.mail_outbox.failed_at, public.mail_outbox.created_at_utc) AS sort_at_utc
    FROM public.mail_outbox
    JOIN public.pay_batches AS remittance_batches
      ON remittance_batches.id = public.mail_outbox.context_id
    WHERE UPPER(BTRIM(COALESCE(remittance_batches.status, ''))) NOT IN ('CANCELLED','CANCELED')
      AND UPPER(BTRIM(COALESCE(public.mail_outbox.type, ''))) = 'REMITTANCE'
      AND LOWER(BTRIM(COALESCE(public.mail_outbox.context_kind, ''))) IN ('pay_batch','pay_batches')
      AND public.mail_outbox.context_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(public.mail_outbox.status::text, ''))) = 'FAILED'
      AND NOT EXISTS (
        SELECT 1
        FROM public.mail_outbox AS remittance_success_outbox
        WHERE UPPER(BTRIM(COALESCE(remittance_success_outbox.type, ''))) = 'REMITTANCE'
          AND LOWER(BTRIM(COALESCE(remittance_success_outbox.context_kind, ''))) IN ('pay_batch','pay_batches')
          AND remittance_success_outbox.context_id = public.mail_outbox.context_id
          AND COALESCE(NULLIF(BTRIM(remittance_success_outbox.reference), ''), remittance_success_outbox.id::text) = COALESCE(NULLIF(BTRIM(public.mail_outbox.reference), ''), public.mail_outbox.id::text)
          AND UPPER(BTRIM(COALESCE(remittance_success_outbox.status::text, ''))) = 'SENT'
          AND COALESCE(remittance_success_outbox.sent_at, remittance_success_outbox.created_at_utc) >= COALESCE(public.mail_outbox.failed_at, public.mail_outbox.created_at_utc)
      )
  ),
  remittance_alerts AS MATERIALIZED (
    SELECT
      'REMITTANCE_SEND_FAILED'::text AS alert_kind,
      'critical'::text AS severity,
      60::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      remittance_base.pay_batch_id AS entity_id,
      remittance_base.pay_batch_id AS pay_batch_id,
      'mail_outbox'::text AS payload_source_kind,
      remittance_base.source_id AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'fingerprint_source_kind', 'mail_outbox',
        'fingerprint_source_id', remittance_base.source_id::text,
        'issue_kind', 'REMITTANCE_SEND_FAILED',
        'pay_batch_id', remittance_base.pay_batch_id::text,
        'reference', NULLIF(BTRIM(COALESCE(remittance_base.reference, '')), ''),
        'failed_at_utc', CASE WHEN remittance_base.failed_at IS NULL THEN NULL::text ELSE TO_CHAR(remittance_base.failed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END
      )) AS fingerprint_payload_json,
      'Remittance failed'::text AS label,
      'Remittance send failed'::text AS title,
      'A remittance email failed to send and requires review.'::text AS description,
      'Review or resend the remittance from the batch.'::text AS action_guidance,
      remittance_base.sort_at_utc
    FROM remittance_base
  ),

  provider_submit_review_scope AS MATERIALIZED (
    SELECT DISTINCT
      provider_operation.id AS operation_id,
      provider_operation.pay_batch_id AS pay_batch_id,
      provider_operation.status AS operation_status,
      provider_operation.phase AS operation_phase,
      COALESCE(provider_operation.updated_at_utc, provider_operation.created_at_utc, now()) AS sort_at_utc
    FROM public.banking_pay_operations AS provider_operation
    JOIN public.pay_batches AS provider_batch
      ON provider_batch.id = provider_operation.pay_batch_id
    WHERE provider_operation.operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
      AND provider_operation.pay_batch_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(provider_batch.status, ''))) NOT IN ('CANCELLED', 'CANCELED')
      AND (
        UPPER(BTRIM(COALESCE(provider_operation.status, ''))) IN ('REVIEW_REQUIRED', 'FAILED')
        OR jsonb_typeof(provider_operation.progress_json->'provider_submit_diagnostic') = 'object'
        OR jsonb_typeof(provider_operation.result_json->'provider_submit_diagnostic') = 'object'
        OR jsonb_typeof(provider_operation.error_json->'provider_submit_diagnostic') = 'object'
      )
      AND (
        UPPER(BTRIM(COALESCE(provider_operation.phase, ''))) IN ('SUBMIT_PROVIDER_TRANSFERS', 'APPLY_RAIL_UPDATES', 'COMPLETE')
        OR jsonb_typeof(provider_operation.progress_json->'provider_submit_diagnostic') = 'object'
        OR jsonb_typeof(provider_operation.result_json->'provider_submit_diagnostic') = 'object'
        OR jsonb_typeof(provider_operation.error_json->'provider_submit_diagnostic') = 'object'
      )
  ),
  provider_submit_review_base AS MATERIALIZED (
    SELECT
      provider_submit_review_scope.pay_batch_id,
      provider_submit_review_scope.operation_id,
      provider_submit_review_scope.operation_status,
      provider_submit_review_scope.operation_phase,
      provider_submit_review_scope.sort_at_utc,
      provider_diagnostic.diagnostic_json AS diagnostic_result,
      COALESCE(provider_diagnostic.diagnostic_json->'provider_submit_diagnostic', '{}'::jsonb) AS provider_submit_diagnostic,
      UPPER(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'provider_submission_status', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) AS provider_submission_status,
      COALESCE(NULLIF(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'review_reason_code', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,review_reason_code}', '')), ''), 'PAYMENT_PROVIDER_SUBMIT_REVIEW') AS review_reason_code,
      NULLIF(BTRIM(COALESCE(provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,chunk_id}', '')), '') AS chunk_id_text,
      NULLIF(BTRIM(COALESCE(provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,transfer_id}', '')), '') AS transfer_id_text,
      NULLIF(BTRIM(COALESCE(provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,transfer_scope_id}', '')), '') AS transfer_scope_id_text,
      NULLIF(BTRIM(COALESCE(provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,auth_request_id}', '')), '') AS auth_request_id_text,
      lower(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'manual_resolution_required', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,manual_resolution_required}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS manual_resolution_required,
      lower(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'safe_retry_available', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,safe_retry_available}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS safe_retry_available,
      lower(BTRIM(COALESCE(provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_acceptance_evidence_present}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS provider_acceptance_evidence_present,
      lower(BTRIM(COALESCE(provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_response_present}', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_response_received}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS provider_response_present
    FROM provider_submit_review_scope
    CROSS JOIN LATERAL public.pay_provider_submit_diagnostic_get(
      p_pay_batch_id := provider_submit_review_scope.pay_batch_id,
      p_operation_id := provider_submit_review_scope.operation_id,
      p_transfer_id := NULL::uuid,
      p_chunk_id := NULL::uuid,
      p_counts_only := false
    ) AS provider_diagnostic(diagnostic_json)
    WHERE (
        (
          UPPER(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'provider_submission_status', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) IN (
            'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK',
            'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME',
            'PROVIDER_SUBMISSION_MALFORMED_RESPONSE',
            'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID',
            'PROVIDER_SUBMISSION_REJECTED'
          )
          AND UPPER(BTRIM(COALESCE(provider_submit_review_scope.operation_status, ''))) IN ('REVIEW_REQUIRED', 'FAILED')
        )
        OR (
          UPPER(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'provider_submission_status', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) = 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL'
          AND UPPER(BTRIM(COALESCE(provider_submit_review_scope.operation_status, ''))) IN ('REVIEW_REQUIRED', 'FAILED')
        )
        OR (
          UPPER(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'provider_submission_status', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) = 'PROVIDER_SUBMISSION_ACCEPTED'
          AND UPPER(BTRIM(COALESCE(provider_submit_review_scope.operation_status, ''))) = 'REVIEW_REQUIRED'
          AND NOT EXISTS (
            SELECT 1
            FROM public.pay_batches AS accepted_review_batch
            WHERE accepted_review_batch.id = provider_submit_review_scope.pay_batch_id
              AND (
                UPPER(BTRIM(COALESCE(accepted_review_batch.status, ''))) IN ('SETTLED','COMMITTED')
                OR UPPER(BTRIM(COALESCE(accepted_review_batch.execution_commit_state, ''))) = 'COMMITTED'
              )
              AND EXISTS (
                SELECT 1
                FROM public.pay_bank_transfers AS accepted_review_transfer
                WHERE accepted_review_transfer.pay_batch_id = accepted_review_batch.id
                  AND (
                    UPPER(BTRIM(COALESCE(accepted_review_transfer.status, ''))) IN ('COMPLETED','SETTLED','PAID','COMMITTED')
                    OR UPPER(BTRIM(COALESCE(accepted_review_transfer.rail_state, ''))) IN ('COMPLETED','SETTLED','PAID','COMMITTED')
                    OR accepted_review_transfer.completed_at_utc IS NOT NULL
                  )
                  AND NULLIF(BTRIM(COALESCE(accepted_review_transfer.rail_tx_id, '')), '') IS NOT NULL
              )
          )
        )
      )
      AND UPPER(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'provider_submission_status', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) <> 'MANUAL_RESOLVED_NO_PAYMENT_MADE'
  ),
  provider_submit_review_alerts AS MATERIALIZED (
    SELECT
      'PAYMENT_PROVIDER_SUBMIT_REVIEW'::text AS alert_kind,
      'critical'::text AS severity,
      91::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      provider_submit_review_base.pay_batch_id AS entity_id,
      provider_submit_review_base.pay_batch_id AS pay_batch_id,
      'banking_pay_operation'::text AS payload_source_kind,
      provider_submit_review_base.operation_id AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'alert_kind', 'PAYMENT_PROVIDER_SUBMIT_REVIEW',
        'issue_kind', 'PAYMENT_PROVIDER_SUBMIT_REVIEW',
        'pay_batch_id', provider_submit_review_base.pay_batch_id::text,
        'operation_id', provider_submit_review_base.operation_id::text,
        'chunk_id', provider_submit_review_base.chunk_id_text,
        'review_reason_code', provider_submit_review_base.review_reason_code,
        'provider_submission_status', provider_submit_review_base.provider_submission_status
      )) AS fingerprint_payload_json,
      CASE provider_submit_review_base.provider_submission_status
        WHEN 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' THEN 'Provider submission outcome unknown'
        WHEN 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' THEN 'Provider response missing'
        WHEN 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'Provider response unusable'
        WHEN 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID' THEN 'Provider acceptance evidence missing'
        WHEN 'PROVIDER_SUBMISSION_REJECTED' THEN 'Provider submission failed'
        WHEN 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'Provider was not called'
        WHEN 'PROVIDER_SUBMISSION_ACCEPTED' THEN 'Provider acceptance evidence present'
        ELSE 'Provider submission needs review'
      END::text AS label,
      CASE provider_submit_review_base.provider_submission_status
        WHEN 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' THEN 'Provider submission outcome unknown'
        WHEN 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' THEN 'Provider response missing'
        WHEN 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'Provider response unusable'
        WHEN 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID' THEN 'Provider acceptance evidence missing'
        WHEN 'PROVIDER_SUBMISSION_REJECTED' THEN 'Provider submission failed'
        WHEN 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'Provider was not called'
        WHEN 'PROVIDER_SUBMISSION_ACCEPTED' THEN 'Provider acceptance evidence present'
        ELSE 'Provider submission needs review'
      END::text AS title,
      CASE provider_submit_review_base.provider_submission_status
        WHEN 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' THEN 'Submit chunk became stale with no provider response, transfer event, rail transaction ID, or rail state.'
        WHEN 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' THEN 'A provider request may have been sent, but no usable provider response was recorded.'
        WHEN 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'Provider returned an unusable response. Manual reconciliation is required before retry.'
        WHEN 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID' THEN 'Provider response/status was recorded, but no usable external provider transaction/reference was stored.'
        WHEN 'PROVIDER_SUBMISSION_REJECTED' THEN 'Provider rejected the payment submission.'
        WHEN 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'Provider submission failed before the provider payment request was sent.'
        WHEN 'PROVIDER_SUBMISSION_ACCEPTED' THEN 'Provider acceptance evidence exists and retry is unsafe until reconciliation is complete.'
        ELSE 'Provider submission requires review before retry.'
      END::text AS description,
      COALESCE(NULLIF(BTRIM(provider_submit_review_base.provider_submit_diagnostic->>'recommended_action'), ''), 'Check Revolut/bank before retry. If no payment was made, record manual no-payment confirmation.')::text AS action_guidance,
      provider_submit_review_base.sort_at_utc
    FROM provider_submit_review_base
    WHERE NOT EXISTS (
        SELECT 1 FROM blocked_funds_alerts AS stronger_blocked
        WHERE stronger_blocked.pay_batch_id = provider_submit_review_base.pay_batch_id
      )
      AND NOT EXISTS (
        SELECT 1 FROM bank_event_alerts AS stronger_bank_event
        WHERE stronger_bank_event.pay_batch_id = provider_submit_review_base.pay_batch_id
          AND stronger_bank_event.alert_kind IN ('BANK_REJECTED_PAYMENT', 'BANK_RETURNED_PAYMENT', 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT')
      )
      AND NOT EXISTS (
        SELECT 1 FROM transfer_alerts AS stronger_transfer
        WHERE stronger_transfer.pay_batch_id = provider_submit_review_base.pay_batch_id
          AND stronger_transfer.alert_kind IN ('BANK_REJECTED_PAYMENT', 'BANK_RETURNED_PAYMENT', 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT')
      )
  ),

  grouped_banking_pay_diagnostic_pay_batch_scope AS MATERIALIZED (
    SELECT DISTINCT candidate_scope.pay_batch_id
    FROM (
      SELECT scoped_specific_batch.id AS pay_batch_id
      FROM public.pay_batches AS scoped_specific_batch
      WHERE p_entity_id IS NOT NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND scoped_specific_batch.id = p_entity_id

      UNION

      SELECT blocked_candidate_batch.id AS pay_batch_id
      FROM public.pay_batches AS blocked_candidate_batch
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND UPPER(BTRIM(COALESCE(blocked_candidate_batch.status, ''))) = 'BLOCKED_FUNDS'
        AND UPPER(BTRIM(COALESCE(blocked_candidate_batch.execution_commit_state, 'NOT_SUBMITTED'))) = 'NOT_SUBMITTED'
        AND blocked_candidate_batch.cancelled_at_utc IS NULL

      UNION

      SELECT event_candidate.pay_batch_id
      FROM public.pay_bank_transfer_events AS event_candidate
      JOIN public.pay_batches AS event_candidate_batch
        ON event_candidate_batch.id = event_candidate.pay_batch_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND event_candidate.pay_batch_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(event_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND (
          UPPER(BTRIM(COALESCE(event_candidate.normalised_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(event_candidate.normalised_state, ''))) LIKE 'CREATE_ERROR%'
          OR UPPER(BTRIM(COALESCE(event_candidate.provider_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(event_candidate.provider_state, ''))) LIKE 'CREATE_ERROR%'
          OR UPPER(BTRIM(COALESCE(event_candidate.mapping_status, ''))) IN ('AMBIGUOUS','UNMATCHED','NO_MATCH','MULTIPLE_MATCHES')
          OR UPPER(BTRIM(COALESCE(event_candidate.correction_disposition, ''))) IN ('AMBIGUOUS','ACTION_REQUIRED','BLOCKED','FAILED')
          OR NULLIF(BTRIM(COALESCE(event_candidate.provider_failure_reason_group, '')), '') IS NOT NULL
        )

      UNION

      SELECT transfer_candidate.pay_batch_id
      FROM public.pay_bank_transfers AS transfer_candidate
      JOIN public.pay_batches AS transfer_candidate_batch
        ON transfer_candidate_batch.id = transfer_candidate.pay_batch_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND transfer_candidate.pay_batch_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(transfer_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND (
          UPPER(BTRIM(COALESCE(transfer_candidate.status, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(transfer_candidate.status, ''))) LIKE 'CREATE_ERROR%'
          OR UPPER(BTRIM(COALESCE(transfer_candidate.rail_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(transfer_candidate.rail_state, ''))) LIKE 'CREATE_ERROR%'
          OR NULLIF(BTRIM(COALESCE(transfer_candidate.failed_reason, '')), '') IS NOT NULL
        )

      UNION

      SELECT correction_request_candidate.pay_batch_id
      FROM public.pay_payment_correction_requests AS correction_request_candidate
      JOIN public.pay_batches AS correction_request_candidate_batch
        ON correction_request_candidate_batch.id = correction_request_candidate.pay_batch_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND correction_request_candidate.pay_batch_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(correction_request_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND UPPER(BTRIM(COALESCE(correction_request_candidate.status, ''))) NOT IN ('APPLIED','RESOLVED','COMPLETE','COMPLETED','CANCELLED','CANCELED')

      UNION

      SELECT correction_work_candidate.pay_batch_id
      FROM public.pay_payment_correction_work_items AS correction_work_candidate
      JOIN public.pay_batches AS correction_work_candidate_batch
        ON correction_work_candidate_batch.id = correction_work_candidate.pay_batch_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND correction_work_candidate.pay_batch_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(correction_work_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND UPPER(BTRIM(COALESCE(correction_work_candidate.status, ''))) IN ('PENDING','PROCESSING','BLOCKED','FAILED','FAILED_RETRYABLE','FAILED_FINAL','APPLIED_WITH_BLOCKERS')

      UNION

      SELECT operation_candidate.pay_batch_id
      FROM public.banking_pay_operations AS operation_candidate
      JOIN public.pay_batches AS operation_candidate_batch
        ON operation_candidate_batch.id = operation_candidate.pay_batch_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND operation_candidate.pay_batch_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(operation_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND (
          UPPER(BTRIM(COALESCE(operation_candidate.operation_type, ''))) = 'PAYMENT_RETRY_BLOCKED_FUNDS'
          OR UPPER(BTRIM(COALESCE(operation_candidate.status, ''))) IN ('REVIEW_REQUIRED','FAILED')
          OR jsonb_typeof(operation_candidate.progress_json->'provider_submit_diagnostic') = 'object'
          OR jsonb_typeof(operation_candidate.result_json->'provider_submit_diagnostic') = 'object'
          OR jsonb_typeof(operation_candidate.error_json->'provider_submit_diagnostic') = 'object'
        )

      UNION

      SELECT remittance_candidate.context_id AS pay_batch_id
      FROM public.mail_outbox AS remittance_candidate
      JOIN public.pay_batches AS remittance_candidate_batch
        ON remittance_candidate_batch.id = remittance_candidate.context_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND remittance_candidate.context_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(remittance_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND UPPER(BTRIM(COALESCE(remittance_candidate.type, ''))) = 'REMITTANCE'
        AND LOWER(BTRIM(COALESCE(remittance_candidate.context_kind, ''))) IN ('pay_batch','pay_batches')
        AND UPPER(BTRIM(COALESCE(remittance_candidate.status::text, ''))) = 'FAILED'

      UNION

      SELECT carry_forward_candidate.source_pay_batch_id AS pay_batch_id
      FROM public.pay_manual_adjustment_carry_forwards AS carry_forward_candidate
      JOIN public.pay_batches AS carry_forward_candidate_batch
        ON carry_forward_candidate_batch.id = carry_forward_candidate.source_pay_batch_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND carry_forward_candidate.source_pay_batch_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(carry_forward_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND UPPER(BTRIM(COALESCE(carry_forward_candidate.status, ''))) = 'PENDING_CARRY_FORWARD'
    ) AS candidate_scope
    WHERE candidate_scope.pay_batch_id IS NOT NULL
  ),
  grouped_banking_pay_diagnostic_scope AS MATERIALIZED (
    SELECT
      scoped_pay_batches.id AS pay_batch_id,
      scoped_pay_batches.status AS batch_status,
      scoped_pay_batches.rail_provider_snapshot,
      scoped_pay_batches.rail_env_snapshot,
      GREATEST(
        COALESCE(scoped_pay_batches.last_status_checked_at_utc, '-infinity'::timestamptz),
        COALESCE(scoped_pay_batches.last_funds_check_at_utc, '-infinity'::timestamptz),
        COALESCE(scoped_pay_batches.execution_committed_at_utc, '-infinity'::timestamptz),
        COALESCE(scoped_pay_batches.created_at_utc, '-infinity'::timestamptz)
      ) AS sort_at_utc,
      COALESCE(
        public.pay_payment_cancelability_diagnostic(
          scoped_pay_batches.id,
          jsonb_build_object('scope_type', 'BATCH'),
          p_actor_user_id
        ),
        '{}'::jsonb
      ) AS diagnostic_json
    FROM grouped_banking_pay_diagnostic_pay_batch_scope AS diagnostic_scope
    JOIN public.pay_batches AS scoped_pay_batches
      ON scoped_pay_batches.id = diagnostic_scope.pay_batch_id
  ),
  grouped_banking_pay_correction_scope AS MATERIALIZED (
    SELECT
      public.pay_payment_correction_requests.pay_batch_id,
      public.pay_payment_correction_requests.id AS correction_request_id,
      UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.correction_kind, ''))) AS correction_kind,
      UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) AS correction_status,
      public.pay_payment_correction_requests.updated_at_utc,
      public.pay_payment_correction_requests.created_at_utc,
      COUNT(public.pay_payment_correction_work_items.id)::integer AS work_total,
      COUNT(public.pay_payment_correction_work_items.id) FILTER (
        WHERE UPPER(BTRIM(COALESCE(public.pay_payment_correction_work_items.status, ''))) IN ('APPLIED','DONE','COMPLETE','COMPLETED')
      )::integer AS work_completed
    FROM public.pay_payment_correction_requests
    LEFT JOIN public.pay_payment_correction_work_items
      ON public.pay_payment_correction_work_items.correction_request_id = public.pay_payment_correction_requests.id
    WHERE EXISTS (
      SELECT 1
      FROM grouped_banking_pay_diagnostic_scope
      WHERE grouped_banking_pay_diagnostic_scope.pay_batch_id = public.pay_payment_correction_requests.pay_batch_id
    )
      AND UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) NOT IN ('APPLIED','COMPLETE','COMPLETED','CANCELLED','CANCELED')
    GROUP BY
      public.pay_payment_correction_requests.pay_batch_id,
      public.pay_payment_correction_requests.id,
      public.pay_payment_correction_requests.correction_kind,
      public.pay_payment_correction_requests.status,
      public.pay_payment_correction_requests.updated_at_utc,
      public.pay_payment_correction_requests.created_at_utc
  ),
  grouped_banking_pay_carry_forward_scope AS MATERIALIZED (
    SELECT
      public.pay_manual_adjustment_carry_forwards.source_pay_batch_id AS pay_batch_id,
      COUNT(*)::integer AS carry_forward_count,
      MAX(grouped_banking_pay_diagnostic_scope.sort_at_utc) AS sort_at_utc
    FROM public.pay_manual_adjustment_carry_forwards
    JOIN grouped_banking_pay_diagnostic_scope
      ON grouped_banking_pay_diagnostic_scope.pay_batch_id = public.pay_manual_adjustment_carry_forwards.source_pay_batch_id
    WHERE public.pay_manual_adjustment_carry_forwards.source_pay_batch_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(public.pay_manual_adjustment_carry_forwards.status, ''))) = 'PENDING_CARRY_FORWARD'
    GROUP BY public.pay_manual_adjustment_carry_forwards.source_pay_batch_id
  ),
  grouped_banking_pay_alert_seeds AS MATERIALIZED (
    SELECT
      'PROVIDER_OUTAGE_RETRY_LATER'::text AS alert_kind,
      92::integer AS severity_rank,
      'ACTION_REQUIRED'::text AS severity,
      grouped_banking_pay_diagnostic_scope.pay_batch_id,
      NULL::text AS payload_source_kind,
      NULL::uuid AS payload_source_id,
      grouped_banking_pay_diagnostic_scope.sort_at_utc
    FROM grouped_banking_pay_diagnostic_scope
    WHERE COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'payment_lifecycle_state'), ''), '') = 'PROVIDER_OUTAGE_RETRY_LATER'
       OR COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'recommended_action'), ''), '') = 'RETRY_PROVIDER_LATER'
       OR LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'requires_retry_later', 'false'))) IN ('true','t','1','yes','y','on')

    UNION ALL
    SELECT
      'PROVIDER_OUTAGE_RETRY_LATER'::text AS alert_kind,
      92::integer AS severity_rank,
      'PROGRESS'::text AS severity,
      retry_operation.pay_batch_id,
      'banking_pay_operation'::text AS payload_source_kind,
      retry_operation.id AS payload_source_id,
      COALESCE(retry_operation.updated_at_utc, retry_operation.created_at_utc, now()) AS sort_at_utc
    FROM public.banking_pay_operations AS retry_operation
    WHERE UPPER(BTRIM(COALESCE(retry_operation.operation_type, ''))) = 'PAYMENT_RETRY_BLOCKED_FUNDS'
      AND UPPER(BTRIM(COALESCE(retry_operation.status, ''))) NOT IN ('COMPLETE','COMPLETED','SUCCEEDED','SUCCESS','FAILED','FAILED_FINAL','CANCELLED','CANCELED')
      AND EXISTS (
        SELECT 1
        FROM grouped_banking_pay_diagnostic_scope
        WHERE grouped_banking_pay_diagnostic_scope.pay_batch_id = retry_operation.pay_batch_id
      )

    UNION ALL
    SELECT
      'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
      94,
      'ACTION_REQUIRED',
      grouped_banking_pay_diagnostic_scope.pay_batch_id,
      NULL::text,
      NULL::uuid,
      grouped_banking_pay_diagnostic_scope.sort_at_utc
    FROM grouped_banking_pay_diagnostic_scope
    WHERE (
      COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'payment_lifecycle_state'), ''), '') = 'PROVIDER_OUTCOME_UNKNOWN'
       OR COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'recommended_action'), ''), '') = 'CHECK_PROVIDER_STATUS'
       OR LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'requires_bank_check', 'false'))) IN ('true','t','1','yes','y','on')
    )
      AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'clean_paid_or_settled_success', 'false'))) NOT IN ('true','t','1','yes','y','on')
      AND NOT (
        COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'payment_lifecycle_state'), ''), '') = 'PAID_OR_SETTLED'
        AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'has_actual_recovery_context', 'false'))) NOT IN ('true','t','1','yes','y','on')
        AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'has_no_money_context', 'false'))) NOT IN ('true','t','1','yes','y','on')
        AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'has_failed_remittance_context', 'false'))) NOT IN ('true','t','1','yes','y','on')
        AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'has_unmatched_or_ambiguous_event_context', 'false'))) NOT IN ('true','t','1','yes','y','on')
      )

    UNION ALL
    SELECT
      'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
      93,
      'ACTION_REQUIRED',
      grouped_banking_pay_diagnostic_scope.pay_batch_id,
      NULL::text,
      NULL::uuid,
      grouped_banking_pay_diagnostic_scope.sort_at_utc
    FROM grouped_banking_pay_diagnostic_scope
    WHERE COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'payment_lifecycle_state'), ''), '') IN ('PROVIDER_CANCELLED_NO_MONEY','PROVIDER_FAILED_NO_MONEY')
      AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'can_no_money_unwind', 'false'))) IN ('true','t','1','yes','y','on')
      AND NOT EXISTS (
        SELECT 1
        FROM grouped_banking_pay_correction_scope
        WHERE grouped_banking_pay_correction_scope.pay_batch_id = grouped_banking_pay_diagnostic_scope.pay_batch_id
          AND grouped_banking_pay_correction_scope.correction_kind IN ('NO_MONEY_UNWIND','MANUAL_EVIDENCE_NO_MONEY')
      )

    UNION ALL
    SELECT
      'PAID_SETTLED_RECOVERY_REQUIRED',
      94,
      'ACTION_REQUIRED',
      grouped_banking_pay_diagnostic_scope.pay_batch_id,
      NULL::text,
      NULL::uuid,
      grouped_banking_pay_diagnostic_scope.sort_at_utc
    FROM grouped_banking_pay_diagnostic_scope
    WHERE (
      COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'recommended_action'), ''), '') = 'AMEND_AND_RECOVER_OVERPAYMENT'
       OR LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'can_recover_overpayment', 'false'))) IN ('true','t','1','yes','y','on')
    )
      AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'has_actual_recovery_context', 'false'))) IN ('true','t','1','yes','y','on')

    UNION ALL
    SELECT
      'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
      94,
      'ACTION_REQUIRED',
      grouped_banking_pay_diagnostic_scope.pay_batch_id,
      NULL::text,
      NULL::uuid,
      grouped_banking_pay_diagnostic_scope.sort_at_utc
    FROM grouped_banking_pay_diagnostic_scope
    WHERE COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'blockers', '[]'::jsonb)::text LIKE '%CANCELLATION_RACED_WITH_PROVIDER_SUBMIT%'
       OR COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'blockers', '[]'::jsonb)::text LIKE '%PROVIDER_SUBMISSION_ALREADY_CLAIMED%'
       OR COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'blockers', '[]'::jsonb)::text LIKE '%PROVIDER_SUBMISSION_IN_PROGRESS%'

    UNION ALL
    SELECT
      'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
      91,
      'ACTION_REQUIRED',
      grouped_banking_pay_diagnostic_scope.pay_batch_id,
      NULL::text,
      NULL::uuid,
      grouped_banking_pay_diagnostic_scope.sort_at_utc
    FROM grouped_banking_pay_diagnostic_scope
    WHERE CASE
      WHEN jsonb_typeof(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'carry_forward_blockers', '[]'::jsonb)) = 'array'
        THEN jsonb_array_length(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'carry_forward_blockers', '[]'::jsonb))
      ELSE 0
    END > 0
       OR COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'blockers', '[]'::jsonb)::text LIKE '%SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS%'

    UNION ALL
    SELECT
      'AUTO_UNWIND_PROGRESS',
      85,
      'PROGRESS',
      grouped_banking_pay_correction_scope.pay_batch_id,
      'pay_payment_correction_request',
      grouped_banking_pay_correction_scope.correction_request_id,
      COALESCE(grouped_banking_pay_correction_scope.updated_at_utc, grouped_banking_pay_correction_scope.created_at_utc)
    FROM grouped_banking_pay_correction_scope
    WHERE grouped_banking_pay_correction_scope.correction_kind IN ('NO_MONEY_UNWIND','MANUAL_EVIDENCE_NO_MONEY')

    UNION ALL
    SELECT
      'WHOLE_BATCH_CANCELLATION_PROGRESS',
      85,
      'PROGRESS',
      grouped_banking_pay_correction_scope.pay_batch_id,
      'pay_payment_correction_request',
      grouped_banking_pay_correction_scope.correction_request_id,
      COALESCE(grouped_banking_pay_correction_scope.updated_at_utc, grouped_banking_pay_correction_scope.created_at_utc)
    FROM grouped_banking_pay_correction_scope
    WHERE grouped_banking_pay_correction_scope.correction_kind = 'PRE_BANK_CANCEL'

    UNION ALL
    SELECT
      'MANUAL_ADJUSTMENTS_CARRIED_FORWARD',
      70,
      'INFO',
      grouped_banking_pay_carry_forward_scope.pay_batch_id,
      NULL::text,
      NULL::uuid,
      grouped_banking_pay_carry_forward_scope.sort_at_utc
    FROM grouped_banking_pay_carry_forward_scope
    WHERE COALESCE(grouped_banking_pay_carry_forward_scope.carry_forward_count, 0) > 0
  ),
  grouped_banking_pay_alerts AS MATERIALIZED (
    SELECT
      grouped_banking_pay_alert_seeds.alert_kind,
      grouped_banking_pay_alert_seeds.severity,
      grouped_banking_pay_alert_seeds.severity_rank,
      'pay_batch'::text AS entity_kind,
      grouped_banking_pay_alert_seeds.pay_batch_id AS entity_id,
      grouped_banking_pay_alert_seeds.pay_batch_id,
      grouped_banking_pay_alert_seeds.payload_source_kind,
      grouped_banking_pay_alert_seeds.payload_source_id,
      jsonb_strip_nulls(
        jsonb_build_object(
          'payload_is_grouped', true,
          'alert_kind', grouped_banking_pay_alert_seeds.alert_kind,
          'issue_kind', grouped_banking_pay_alert_seeds.alert_kind,
          'legacy_alert_kind', grouped_banking_pay_alert_seeds.alert_kind,
          'pay_batch_id', grouped_banking_pay_alert_seeds.pay_batch_id::text,
          'link_target', 'banking_pay_batch',
          'link_tab', CASE
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'overview'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'overview'
            ELSE 'current_payment_status'
          END,
          'alert_severity', grouped_banking_pay_alert_seeds.severity,
          'severity', grouped_banking_pay_alert_seeds.severity
        )
        || jsonb_build_object(
          'stable_issue_key', CONCAT_WS(
            ':',
            grouped_banking_pay_alert_seeds.pay_batch_id::text,
            grouped_banking_pay_alert_seeds.alert_kind,
            CASE
              WHEN grouped_banking_pay_alert_seeds.alert_kind IN (
                'PROVIDER_OUTAGE_RETRY_LATER',
                'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
                'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
                'PAID_SETTLED_RECOVERY_REQUIRED',
                'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
                'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
                'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
              ) THEN 'GROUPED'
              ELSE COALESCE(NULLIF(BTRIM(grouped_banking_pay_alert_seeds.payload_source_kind), ''), 'BATCH')
            END,
            CASE
              WHEN grouped_banking_pay_alert_seeds.alert_kind IN (
                'PROVIDER_OUTAGE_RETRY_LATER',
                'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
                'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
                'PAID_SETTLED_RECOVERY_REQUIRED',
                'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
                'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
                'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
              ) THEN 'ACTIVE'
              ELSE COALESCE(grouped_banking_pay_alert_seeds.payload_source_id::text, 'NO_SOURCE')
            END
          ),
          'dedupe_key', CONCAT_WS(
            ':',
            grouped_banking_pay_alert_seeds.pay_batch_id::text,
            grouped_banking_pay_alert_seeds.alert_kind,
            CASE
              WHEN grouped_banking_pay_alert_seeds.alert_kind IN (
                'PROVIDER_OUTAGE_RETRY_LATER',
                'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
                'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
                'PAID_SETTLED_RECOVERY_REQUIRED',
                'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
                'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
                'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
              ) THEN 'GROUPED'
              ELSE COALESCE(grouped_banking_pay_alert_seeds.payload_source_id::text, 'NO_SOURCE')
            END
          ),
          'payload_source_kind', NULLIF(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.payload_source_kind, '')), ''),
          'payload_source_id', CASE WHEN grouped_banking_pay_alert_seeds.payload_source_id IS NULL THEN NULL::text ELSE grouped_banking_pay_alert_seeds.payload_source_id::text END
        )
        || jsonb_build_object(
          'user_label', CASE
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
              AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
              THEN 'Retrying unsent payments'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
              THEN 'Bank unavailable — unsent payments can be retried'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
              THEN 'Provider outcome unknown — check provider'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
              THEN 'Failed payments — Rewind financials available'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
              THEN 'Rewinding failed payments'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
              THEN 'Cancelling scheduled batch'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
              THEN 'Manual adjustments carried forward'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
              THEN 'Manual adjustment blockers — review required'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
              THEN 'Paid — recovery required'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
              THEN 'Cancellation conflict — provider submission already started'
            ELSE INITCAP(REPLACE(LOWER(grouped_banking_pay_alert_seeds.alert_kind), '_', ' '))
          END,
          'user_description', CASE
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
              AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
              THEN 'Retrying unsent payments is in progress. You can continue using CloudTMS while this runs.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
              THEN 'The bank/provider was unavailable before the payment request was sent. Retry unsent payments from Banking Pay Overview.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
              THEN 'A provider request may have been sent, but the outcome is not confirmed. Open Current Payment Status and check the provider outcome.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
              THEN 'The provider/bank outcome indicates no money moved. Open Current Payment Status and rewind financials where safe.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
              THEN 'Automatic no-money unwind is running and this alert updates grouped progress.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
              THEN 'Scheduled local cancellation is running in chunks and this alert updates grouped progress.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
              THEN 'Safe source-less manual adjustments were carried forward and will be included in the next pay run.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
              THEN 'One or more manual adjustments cannot be safely carried forward automatically.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
              THEN 'Money appears to have moved. Amend the timesheet and recover the overpayment in the next pay run rather than unwinding the payment.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
              THEN 'Cancellation could not proceed because provider submission had already started.'
            ELSE 'Open Banking Pay for details.'
          END,
          'required_user_action', CASE
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
              AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
              THEN 'Monitor retry progress from Banking Pay Overview.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
              THEN 'Retry unsent payments from Banking Pay Overview.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
              THEN 'Open Current Payment Status and check the provider outcome.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
              THEN 'Open Current Payment Status and rewind financials where no money moved.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
              THEN 'Monitor rewind progress.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
              THEN 'Monitor cancellation progress in Banking Pay Overview.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
              THEN 'Review carried-forward manual adjustments in the next pay run.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
              THEN 'Open Current Payment Status and review ambiguous manual adjustment blockers.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
              THEN 'Open Current Payment Status and recover overpayment in next pay run.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
              THEN 'Open Current Payment Status and check provider submission before continuing.'
            ELSE 'Open Banking Pay.'
          END
        )
      ) AS fingerprint_payload_json,
      CASE
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
          THEN 'Retrying unsent payments'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          THEN 'Bank unavailable — unsent payments can be retried'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
          THEN 'Provider outcome unknown — check provider'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
          THEN 'Failed payments — Rewind financials available'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
          THEN 'Rewinding failed payments'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
          THEN 'Cancelling scheduled batch'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
          THEN 'Manual adjustments carried forward'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
          THEN 'Manual adjustment blockers — review required'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
          THEN 'Paid — recovery required'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
          THEN 'Cancellation conflict — provider submission already started'
        ELSE INITCAP(REPLACE(LOWER(grouped_banking_pay_alert_seeds.alert_kind), '_', ' '))
      END::text AS label,
      CASE
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
          THEN 'Retrying unsent payments'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          THEN 'Bank unavailable — unsent payments can be retried'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
          THEN 'Provider outcome unknown — check provider'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
          THEN 'Failed payments — Rewind financials available'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
          THEN 'Rewinding failed payments'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
          THEN 'Cancelling scheduled batch'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
          THEN 'Manual adjustments carried forward'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
          THEN 'Manual adjustment blockers — review required'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
          THEN 'Paid — recovery required'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
          THEN 'Cancellation conflict — provider submission already started'
        ELSE INITCAP(REPLACE(LOWER(grouped_banking_pay_alert_seeds.alert_kind), '_', ' '))
      END::text AS title,
      CASE
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
          THEN 'Retrying unsent payments is in progress. You can continue using CloudTMS while this runs.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          THEN 'The bank/provider was unavailable before the payment request was sent. Retry unsent payments from Banking Pay Overview.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
          THEN 'A provider request may have been sent, but the outcome is not confirmed. Open Current Payment Status and check the provider outcome.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
          THEN 'The provider/bank outcome indicates no money moved. Open Current Payment Status and rewind financials where safe.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
          THEN 'Automatic no-money unwind is running and this alert updates grouped progress.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
          THEN 'Scheduled local cancellation is running in chunks and this alert updates grouped progress.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
          THEN 'Safe source-less manual adjustments were carried forward and will be included in the next pay run.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
          THEN 'One or more manual adjustments cannot be safely carried forward automatically.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
          THEN 'Money appears to have moved. Amend the timesheet and recover the overpayment in the next pay run rather than unwinding the payment.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
          THEN 'Cancellation could not proceed because provider submission had already started.'
        ELSE 'Open Banking Pay for details.'
      END::text AS description,
      CASE
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
          THEN 'Monitor retry progress from Banking Pay Overview.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          THEN 'Retry unsent payments from Banking Pay Overview.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
          THEN 'Open Current Payment Status and check the provider outcome.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
          THEN 'Open Current Payment Status and rewind financials where no money moved.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
          THEN 'Monitor rewind progress.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
          THEN 'Monitor cancellation progress in Banking Pay Overview.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
          THEN 'Review carried-forward manual adjustments in the next pay run.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
          THEN 'Open Current Payment Status and review ambiguous manual adjustment blockers.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
          THEN 'Open Current Payment Status and recover overpayment in next pay run.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
          THEN 'Open Current Payment Status and check provider submission before continuing.'
        ELSE 'Open Banking Pay.'
      END::text AS action_guidance,
      grouped_banking_pay_alert_seeds.sort_at_utc
    FROM grouped_banking_pay_alert_seeds
  ),
  latest_success_events AS MATERIALIZED (
    SELECT DISTINCT ON (success_event.pay_batch_id, success_event.alert_kind)
      success_event.id,
      success_event.pay_batch_id,
      success_event.alert_kind,
      success_event.event_key,
      COALESCE(success_event.payload_json, '{}'::jsonb) AS payload_json,
      success_event.occurred_at_utc
    FROM public.banking_alert_success_events AS success_event
    WHERE success_event.expires_at_utc > now()
      AND UPPER(BTRIM(COALESCE(success_event.alert_kind, ''))) IN (
        'BATCH_SCHEDULED_SUCCESS',
        'BATCH_SETTLED_SUCCESS'
      )
    ORDER BY
      success_event.pay_batch_id,
      success_event.alert_kind,
      success_event.occurred_at_utc DESC,
      success_event.id DESC
  ),
  success_event_alerts AS MATERIALIZED (
    SELECT
      UPPER(BTRIM(latest_success_events.alert_kind))::text AS alert_kind,
      'info'::text AS severity,
      10::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      latest_success_events.pay_batch_id AS entity_id,
      latest_success_events.pay_batch_id,
      'banking_alert_success_event'::text AS payload_source_kind,
      latest_success_events.id AS payload_source_id,
      jsonb_strip_nulls(
        latest_success_events.payload_json
        || jsonb_build_object(
          'alert_kind', UPPER(BTRIM(latest_success_events.alert_kind)),
          'issue_kind', UPPER(BTRIM(latest_success_events.alert_kind)),
          'stable_issue_key', COALESCE(
            NULLIF(BTRIM(latest_success_events.payload_json ->> 'stable_issue_key'), ''),
            latest_success_events.pay_batch_id::text || ':' || UPPER(BTRIM(latest_success_events.alert_kind)) || ':' || latest_success_events.event_key
          ),
          'dedupe_key', COALESCE(
            NULLIF(BTRIM(latest_success_events.payload_json ->> 'dedupe_key'), ''),
            latest_success_events.pay_batch_id::text || ':' || UPPER(BTRIM(latest_success_events.alert_kind)) || ':' || latest_success_events.event_key
          ),
          'payload_source_kind', 'banking_alert_success_event',
          'payload_source_id', latest_success_events.id::text,
          'pay_batch_id', latest_success_events.pay_batch_id::text,
          'alert_candidate_is_success_only', true,
          'is_success_only', true
        )
      ) AS fingerprint_payload_json,
      COALESCE(
        NULLIF(BTRIM(latest_success_events.payload_json ->> 'user_label'), ''),
        CASE WHEN UPPER(BTRIM(latest_success_events.alert_kind)) = 'BATCH_SCHEDULED_SUCCESS'
          THEN 'Future payment batch scheduled'
          ELSE 'Payment batch settled'
        END
      )::text AS label,
      COALESCE(
        NULLIF(BTRIM(latest_success_events.payload_json ->> 'user_label'), ''),
        CASE WHEN UPPER(BTRIM(latest_success_events.alert_kind)) = 'BATCH_SCHEDULED_SUCCESS'
          THEN 'Future payment batch scheduled'
          ELSE 'Payment batch settled'
        END
      )::text AS title,
      COALESCE(
        NULLIF(BTRIM(latest_success_events.payload_json ->> 'user_description'), ''),
        'Payment batch lifecycle completed successfully.'
      )::text AS description,
      COALESCE(
        NULLIF(BTRIM(latest_success_events.payload_json ->> 'required_user_action'), ''),
        'Review or clear this Banking alert.'
      )::text AS action_guidance,
      latest_success_events.occurred_at_utc AS sort_at_utc
    FROM latest_success_events
  ),
  raw_current_alerts AS MATERIALIZED (
    SELECT * FROM blocked_funds_alerts
    UNION ALL
    SELECT bank_event_alerts.*
    FROM bank_event_alerts
    WHERE NOT EXISTS (
      SELECT 1
      FROM grouped_banking_pay_alerts AS grouped_replacement_alerts
      WHERE grouped_replacement_alerts.pay_batch_id = bank_event_alerts.pay_batch_id
        AND (
          (
            bank_event_alerts.alert_kind IN ('BANK_REJECTED_PAYMENT')
            AND grouped_replacement_alerts.alert_kind IN (
              'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
              'AUTO_UNWIND_PROGRESS'
            )
          )
          OR (
            bank_event_alerts.alert_kind IN ('BANK_RETURNED_PAYMENT')
            AND grouped_replacement_alerts.alert_kind IN (
              'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
              'AUTO_UNWIND_PROGRESS',
              'PAID_SETTLED_RECOVERY_REQUIRED'
            )
          )
          OR (
            bank_event_alerts.alert_kind IN ('RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT')
            AND grouped_replacement_alerts.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
          )
          OR (
            bank_event_alerts.alert_kind IN ('AMBIGUOUS_PAYMENT_REVIEW_REQUIRED')
            AND grouped_replacement_alerts.alert_kind IN (
              'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
              'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
            )
          )
          OR (
            bank_event_alerts.alert_kind IN ('PAYMENT_CORRECTION_FAILED','PAYMENT_CORRECTION_BLOCKED')
            AND grouped_replacement_alerts.alert_kind IN (
              'AUTO_UNWIND_PROGRESS',
              'WHOLE_BATCH_CANCELLATION_PROGRESS',
              'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
              'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
              'PAID_SETTLED_RECOVERY_REQUIRED'
            )
          )
        )
    )
    UNION ALL
    SELECT transfer_alerts.*
    FROM transfer_alerts
    WHERE NOT EXISTS (
      SELECT 1
      FROM grouped_banking_pay_alerts AS grouped_replacement_alerts
      WHERE grouped_replacement_alerts.pay_batch_id = transfer_alerts.pay_batch_id
        AND (
          (
            transfer_alerts.alert_kind IN ('BANK_REJECTED_PAYMENT')
            AND grouped_replacement_alerts.alert_kind IN (
              'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
              'AUTO_UNWIND_PROGRESS'
            )
          )
          OR (
            transfer_alerts.alert_kind IN ('BANK_RETURNED_PAYMENT')
            AND grouped_replacement_alerts.alert_kind IN (
              'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
              'AUTO_UNWIND_PROGRESS',
              'PAID_SETTLED_RECOVERY_REQUIRED'
            )
          )
          OR (
            transfer_alerts.alert_kind IN ('RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT')
            AND grouped_replacement_alerts.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
          )
        )
    )
    UNION ALL
    SELECT correction_request_alerts.*
    FROM correction_request_alerts
    WHERE NOT EXISTS (
      SELECT 1
      FROM grouped_banking_pay_alerts AS grouped_replacement_alerts
      WHERE grouped_replacement_alerts.pay_batch_id = correction_request_alerts.pay_batch_id
        AND grouped_replacement_alerts.payload_source_kind = 'pay_payment_correction_request'
        AND grouped_replacement_alerts.payload_source_id = correction_request_alerts.payload_source_id
        AND grouped_replacement_alerts.alert_kind IN (
          'AUTO_UNWIND_PROGRESS',
          'WHOLE_BATCH_CANCELLATION_PROGRESS'
        )
    )
    UNION ALL
    SELECT correction_work_alerts.*
    FROM correction_work_alerts
    WHERE NOT EXISTS (
      SELECT 1
      FROM grouped_banking_pay_alerts AS grouped_replacement_alerts
      JOIN public.pay_payment_correction_work_items AS correction_work_group_lookup
        ON correction_work_group_lookup.id = correction_work_alerts.payload_source_id
       AND correction_work_group_lookup.correction_request_id = grouped_replacement_alerts.payload_source_id
      WHERE grouped_replacement_alerts.pay_batch_id = correction_work_alerts.pay_batch_id
        AND grouped_replacement_alerts.payload_source_kind = 'pay_payment_correction_request'
        AND grouped_replacement_alerts.alert_kind IN (
          'AUTO_UNWIND_PROGRESS',
          'WHOLE_BATCH_CANCELLATION_PROGRESS'
        )
    )
    UNION ALL
    SELECT * FROM remittance_alerts
    UNION ALL
    SELECT provider_submit_review_alerts.*
    FROM provider_submit_review_alerts
    WHERE NOT EXISTS (
      SELECT 1
      FROM grouped_banking_pay_alerts AS grouped_replacement_alerts
      WHERE grouped_replacement_alerts.pay_batch_id = provider_submit_review_alerts.pay_batch_id
        AND grouped_replacement_alerts.alert_kind IN (
          'PROVIDER_OUTAGE_RETRY_LATER',
          'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
          'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
        )
    )
    UNION ALL
    SELECT * FROM grouped_banking_pay_alerts
    UNION ALL
    SELECT * FROM success_event_alerts
  ),
  normalised_current_alerts AS MATERIALIZED (
    SELECT
      mapped_alerts.mapped_alert_kind AS alert_kind,
      raw_current_alerts.severity,
      raw_current_alerts.severity_rank,
      raw_current_alerts.entity_kind,
      raw_current_alerts.entity_id,
      raw_current_alerts.pay_batch_id,
      raw_current_alerts.payload_source_kind,
      raw_current_alerts.payload_source_id,
      COALESCE(raw_current_alerts.fingerprint_payload_json, '{}'::jsonb) || jsonb_build_object(
        'legacy_alert_kind', raw_current_alerts.alert_kind,
        'alert_kind', mapped_alerts.mapped_alert_kind
      ) AS fingerprint_payload_json,
      CASE mapped_alerts.mapped_alert_kind
        WHEN 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'Bank unavailable — unsent payments can be retried'
        WHEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN 'Provider outcome unknown — check provider'
        WHEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN 'Failed payments — Rewind financials available'
        WHEN 'AUTO_UNWIND_PROGRESS' THEN 'Rewinding failed payments'
        WHEN 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'Cancelling scheduled batch'
        WHEN 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN 'Manual adjustments carried forward'
        WHEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS' THEN 'Manual adjustment blockers — review required'
        WHEN 'PAID_SETTLED_RECOVERY_REQUIRED' THEN 'Paid — recovery required'
        WHEN 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT' THEN 'Cancellation conflict — provider submission already started'
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Unmatched bank webhook — review required'
        ELSE raw_current_alerts.label
      END AS label,
      CASE mapped_alerts.mapped_alert_kind
        WHEN 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'Bank unavailable — unsent payments can be retried'
        WHEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN 'Provider outcome unknown — check provider'
        WHEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN 'Failed payments — Rewind financials available'
        WHEN 'AUTO_UNWIND_PROGRESS' THEN 'Rewinding failed payments'
        WHEN 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'Cancelling scheduled batch'
        WHEN 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN 'Manual adjustments carried forward'
        WHEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS' THEN 'Manual adjustment blockers — review required'
        WHEN 'PAID_SETTLED_RECOVERY_REQUIRED' THEN 'Paid — recovery required'
        WHEN 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT' THEN 'Cancellation conflict — provider submission already started'
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Unmatched bank webhook — review required'
        ELSE raw_current_alerts.title
      END AS title,
      CASE mapped_alerts.mapped_alert_kind
        WHEN 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'The bank/provider was unavailable before the payment request was sent. Retry unsent payments from Banking Pay Overview.'
        WHEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN 'A provider request may have been sent, but the outcome is not confirmed. Open Current Payment Status and check the provider outcome.'
        WHEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN 'The provider/bank outcome indicates no money moved. Open Current Payment Status and rewind financials where safe.'
        WHEN 'AUTO_UNWIND_PROGRESS' THEN 'Automatic no-money unwind is running and this alert updates grouped progress.'
        WHEN 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'Scheduled local cancellation is running in chunks and this alert updates grouped progress.'
        WHEN 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN 'Safe source-less manual adjustments were carried forward and will be included in the next pay run.'
        WHEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS' THEN 'One or more manual adjustments cannot be safely carried forward automatically.'
        WHEN 'PAID_SETTLED_RECOVERY_REQUIRED' THEN 'Money appears to have moved. Amend the timesheet and recover the overpayment in the next pay run rather than unwinding the payment.'
        WHEN 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT' THEN 'Cancellation could not proceed because provider submission had already started.'
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'A verified provider webhook was received but could not be matched safely to a payment.'
        ELSE raw_current_alerts.description
      END AS description,
      CASE mapped_alerts.mapped_alert_kind
        WHEN 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'Open Banking Pay Overview and retry unsent payments.'
        WHEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN 'Open Current Payment Status and check the provider outcome.'
        WHEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN 'Open Current Payment Status and rewind financials where no money moved.'
        WHEN 'AUTO_UNWIND_PROGRESS' THEN 'Monitor rewind progress.'
        WHEN 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'Monitor cancellation progress in Banking Pay Overview.'
        WHEN 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN 'Review carried-forward manual adjustments in the next pay run.'
        WHEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS' THEN 'Open Current Payment Status and review ambiguous manual adjustment blockers.'
        WHEN 'PAID_SETTLED_RECOVERY_REQUIRED' THEN 'Open Current Payment Status and recover overpayment in next pay run.'
        WHEN 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT' THEN 'Open Current Payment Status and check provider submission before continuing.'
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Open Current Payment Status and review the unmatched provider webhook.'
        ELSE replace(COALESCE(raw_current_alerts.action_guidance, 'Open Banking Pay.'), 'Payment Issues', 'Current Payment Status')
      END AS action_guidance,
      raw_current_alerts.sort_at_utc
    FROM raw_current_alerts
    CROSS JOIN LATERAL (
      SELECT CASE raw_current_alerts.alert_kind
        WHEN 'BLOCKED_FUNDS' THEN 'PROVIDER_OUTAGE_RETRY_LATER'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
        WHEN 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
        WHEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
        WHEN 'PAYMENT_PROVIDER_SUBMIT_REVIEW' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
        WHEN 'PAYMENT_CORRECTION_AWAITING_APPROVAL' THEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
        ELSE raw_current_alerts.alert_kind
      END AS mapped_alert_kind
    ) AS mapped_alerts
  ),
  current_alert_identities AS MATERIALIZED (
    SELECT
      normalised_current_alerts.alert_kind,
      normalised_current_alerts.severity,
      normalised_current_alerts.severity_rank,
      normalised_current_alerts.entity_kind,
      normalised_current_alerts.entity_id,
      normalised_current_alerts.pay_batch_id,
      normalised_current_alerts.payload_source_kind,
      normalised_current_alerts.payload_source_id,
      normalised_current_alerts.fingerprint_payload_json,
      public.banking_alert_fingerprint(
        normalised_current_alerts.alert_kind,
        normalised_current_alerts.entity_kind,
        normalised_current_alerts.entity_id,
        normalised_current_alerts.fingerprint_payload_json
      ) AS alert_fingerprint,
      normalised_current_alerts.label,
      normalised_current_alerts.title,
      normalised_current_alerts.description,
      normalised_current_alerts.action_guidance,
      normalised_current_alerts.sort_at_utc
    FROM normalised_current_alerts
    WHERE normalised_current_alerts.alert_kind IS NOT NULL
      AND normalised_current_alerts.entity_id IS NOT NULL
  ),
  filtered_current_alert_identities AS MATERIALIZED (
    SELECT current_alert_identities.*
    FROM current_alert_identities
    WHERE (v_entity_kind IS NULL OR current_alert_identities.entity_kind = v_entity_kind)
      AND (p_entity_id IS NULL OR current_alert_identities.entity_id = p_entity_id)
      AND public._banking_alert_user_filter_allows(
        p_actor_user_id,
        jsonb_strip_nulls(
          COALESCE(current_alert_identities.fingerprint_payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'alert_kind', current_alert_identities.alert_kind,
            'alert_severity', current_alert_identities.severity,
            'severity', current_alert_identities.severity,
            'entity_kind', current_alert_identities.entity_kind,
            'entity_id', current_alert_identities.entity_id::text,
            'pay_batch_id', current_alert_identities.pay_batch_id::text,
            'alert_fingerprint', current_alert_identities.alert_fingerprint,
            'user_label', current_alert_identities.label,
            'user_description', current_alert_identities.description,
            'required_user_action', current_alert_identities.action_guidance
          )
        )
      )
  ),
  deduped_alerts AS MATERIALIZED (
    SELECT DISTINCT ON (filtered_current_alert_identities.alert_fingerprint)
      filtered_current_alert_identities.alert_kind,
      filtered_current_alert_identities.severity,
      filtered_current_alert_identities.severity_rank,
      filtered_current_alert_identities.entity_kind,
      filtered_current_alert_identities.entity_id,
      filtered_current_alert_identities.pay_batch_id,
      filtered_current_alert_identities.payload_source_kind,
      filtered_current_alert_identities.payload_source_id,
      filtered_current_alert_identities.fingerprint_payload_json,
      filtered_current_alert_identities.alert_fingerprint,
      filtered_current_alert_identities.label,
      filtered_current_alert_identities.title,
      filtered_current_alert_identities.description,
      filtered_current_alert_identities.action_guidance,
      filtered_current_alert_identities.sort_at_utc
    FROM filtered_current_alert_identities
    WHERE filtered_current_alert_identities.alert_fingerprint IS NOT NULL
    ORDER BY filtered_current_alert_identities.alert_fingerprint ASC,
             filtered_current_alert_identities.severity_rank DESC,
             filtered_current_alert_identities.sort_at_utc DESC NULLS LAST
  ),
  alert_rows AS MATERIALIZED (
    SELECT
      deduped_alerts.alert_kind,
      deduped_alerts.severity,
      deduped_alerts.severity_rank,
      deduped_alerts.entity_kind,
      deduped_alerts.entity_id,
      deduped_alerts.pay_batch_id,
      deduped_alerts.payload_source_kind,
      deduped_alerts.payload_source_id,
      deduped_alerts.fingerprint_payload_json,
      deduped_alerts.alert_fingerprint,
      deduped_alerts.label,
      deduped_alerts.title,
      deduped_alerts.description,
      deduped_alerts.action_guidance,
      deduped_alerts.sort_at_utc,
      alert_acknowledgements.id IS NOT NULL AS acknowledged_for_current_user,
      alert_acknowledgements.acknowledged_at_utc
    FROM deduped_alerts
    LEFT JOIN public.banking_alert_acknowledgements AS alert_acknowledgements
      ON alert_acknowledgements.acknowledged_by_user_id = p_actor_user_id
     AND UPPER(BTRIM(COALESCE(alert_acknowledgements.acknowledge_scope, 'USER'))) = 'USER'
     AND (
       alert_acknowledgements.alert_fingerprint = deduped_alerts.alert_fingerprint
       OR (
         UPPER(BTRIM(COALESCE(alert_acknowledgements.alert_kind, ''))) = UPPER(BTRIM(COALESCE(deduped_alerts.alert_kind, '')))
         AND LOWER(BTRIM(COALESCE(alert_acknowledgements.entity_kind, ''))) = LOWER(BTRIM(COALESCE(deduped_alerts.entity_kind, '')))
         AND alert_acknowledgements.entity_id = deduped_alerts.entity_id
         AND alert_acknowledgements.acknowledged_at_utc >= COALESCE(deduped_alerts.sort_at_utc, '-infinity'::timestamptz)
       )
     )
    WHERE COALESCE(p_include_acknowledged, false) = true
       OR alert_acknowledgements.id IS NULL
  ),
  counted_alerts AS MATERIALIZED (
    SELECT
      alert_rows.*,
      (COUNT(*) OVER ())::integer AS filtered_count,
      (COUNT(*) FILTER (WHERE alert_rows.acknowledged_for_current_user = false) OVER ())::integer AS unacknowledged_count
    FROM alert_rows
  ),
  limited_alerts AS MATERIALIZED (
    SELECT counted_alerts.*
    FROM counted_alerts
    ORDER BY counted_alerts.severity_rank DESC,
             counted_alerts.sort_at_utc DESC NULLS LAST,
             counted_alerts.alert_fingerprint ASC
    LIMIT v_limit
  ),
  detailed_alerts AS MATERIALIZED (
    SELECT
      limited_alerts.alert_kind,
      limited_alerts.severity,
      limited_alerts.severity_rank,
      limited_alerts.entity_kind,
      limited_alerts.entity_id,
      limited_alerts.pay_batch_id,
      limited_alerts.fingerprint_payload_json,
      limited_alerts.alert_fingerprint,
      limited_alerts.label,
      limited_alerts.title,
      limited_alerts.description,
      limited_alerts.action_guidance,
      limited_alerts.sort_at_utc,
      limited_alerts.acknowledged_for_current_user,
      limited_alerts.acknowledged_at_utc,
      limited_alerts.filtered_count,
      limited_alerts.unacknowledged_count,
      jsonb_strip_nulls(
        COALESCE(limited_alerts.fingerprint_payload_json, '{}'::jsonb)
        || jsonb_build_object(
          'alert_kind', limited_alerts.alert_kind,
          'issue_kind', limited_alerts.alert_kind,
          'pay_batch_id', limited_alerts.pay_batch_id::text,
          'entity_kind', limited_alerts.entity_kind,
          'entity_id', limited_alerts.entity_id::text,
          'alert_fingerprint', limited_alerts.alert_fingerprint,
          'user_label', limited_alerts.label,
          'user_description', limited_alerts.description,
          'required_user_action', limited_alerts.action_guidance,
          'link_target', 'banking_pay_batch',
          'link_tab', CASE
            WHEN limited_alerts.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'overview'
            WHEN limited_alerts.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'overview'
            ELSE 'current_payment_status'
          END
        )
      ) AS payload_json
    FROM limited_alerts
  ),
  signal_aggregate AS MATERIALIZED (
    SELECT
      (COUNT(*) FILTER (WHERE alert_rows.acknowledged_for_current_user = false))::integer AS unacknowledged_count,
      (ARRAY_AGG(alert_rows.severity ORDER BY alert_rows.severity_rank DESC, alert_rows.sort_at_utc DESC NULLS LAST, alert_rows.alert_fingerprint ASC) FILTER (WHERE alert_rows.acknowledged_for_current_user = false))[1] AS highest_severity,
      (ARRAY_AGG(alert_rows.label ORDER BY alert_rows.severity_rank DESC, alert_rows.sort_at_utc DESC NULLS LAST, alert_rows.alert_fingerprint ASC) FILTER (WHERE alert_rows.acknowledged_for_current_user = false))[1] AS highest_label,
      'banking_alert_signal:v3:' || MD5(COALESCE(STRING_AGG(
        CONCAT_WS('|',
          alert_rows.alert_fingerprint,
          alert_rows.alert_kind,
          alert_rows.entity_kind,
          alert_rows.entity_id::text,
          alert_rows.severity
        ),
        CHR(10)
        ORDER BY alert_rows.alert_fingerprint ASC
      ) FILTER (WHERE alert_rows.acknowledged_for_current_user = false), '')) AS alert_hash,
      'banking_alert_summary:v3:' || MD5(COALESCE(STRING_AGG(
        alert_rows.alert_fingerprint,
        CHR(10)
        ORDER BY alert_rows.alert_fingerprint ASC
      ) FILTER (WHERE alert_rows.acknowledged_for_current_user = false), '')) AS summary_signature
    FROM alert_rows
  ),
  aggregate_result AS MATERIALIZED (
    SELECT
      COALESCE(JSONB_AGG(
        jsonb_build_object(
          'alert_kind', detailed_alerts.alert_kind,
          'severity', detailed_alerts.severity,
          'severity_rank', detailed_alerts.severity_rank,
          'entity_kind', detailed_alerts.entity_kind,
          'entity_id', detailed_alerts.entity_id::text,
          'pay_batch_id', detailed_alerts.pay_batch_id::text,
          'alert_fingerprint', detailed_alerts.alert_fingerprint,
          'label', detailed_alerts.label,
          'title', detailed_alerts.title,
          'description', detailed_alerts.description,
          'action_guidance', detailed_alerts.action_guidance,
          'acknowledged_for_current_user', detailed_alerts.acknowledged_for_current_user,
          'requires_attention_for_current_user', NOT detailed_alerts.acknowledged_for_current_user,
          'acknowledged_at_utc', CASE WHEN detailed_alerts.acknowledged_at_utc IS NULL THEN NULL::text ELSE detailed_alerts.acknowledged_at_utc::text END,
          'sort_at_utc', CASE WHEN detailed_alerts.sort_at_utc IS NULL THEN NULL::text ELSE detailed_alerts.sort_at_utc::text END,
          'payload_json', detailed_alerts.payload_json
        )
        ORDER BY detailed_alerts.severity_rank DESC,
                 detailed_alerts.sort_at_utc DESC NULLS LAST,
                 detailed_alerts.alert_fingerprint ASC
      ), '[]'::jsonb) AS alerts_json,
      COALESCE(MAX(detailed_alerts.filtered_count), 0)::integer AS filtered_count,
      (ARRAY_AGG(detailed_alerts.severity ORDER BY detailed_alerts.severity_rank DESC, detailed_alerts.sort_at_utc DESC NULLS LAST, detailed_alerts.alert_fingerprint ASC))[1] AS highest_severity,
      (ARRAY_AGG(detailed_alerts.label ORDER BY detailed_alerts.severity_rank DESC, detailed_alerts.sort_at_utc DESC NULLS LAST, detailed_alerts.alert_fingerprint ASC))[1] AS highest_label
    FROM detailed_alerts
  )
  SELECT jsonb_build_object(
    'ok', true,
    'generated_at_utc', now()::text,
    'actor_user_id', p_actor_user_id::text,
    'alert_context', v_alert_context,
    'include_acknowledged', COALESCE(p_include_acknowledged, false),
    'limit', COALESCE(v_limit, 0),
    'alerts', aggregate_result.alerts_json,
    'banking_alert_hash', COALESCE(signal_aggregate.alert_hash, 'banking_alert_signal:v3:' || MD5('')),
    'banking_alert_summary_signature', COALESCE(signal_aggregate.summary_signature, 'banking_alert_summary:v3:' || MD5('')),
    'banking_unacknowledged_alert_count', COALESCE(signal_aggregate.unacknowledged_count, 0),
    'banking_highest_alert_severity', CASE WHEN COALESCE(signal_aggregate.unacknowledged_count, 0) > 0 THEN COALESCE(signal_aggregate.highest_severity, '') ELSE '' END,
    'banking_highest_alert_label', CASE WHEN COALESCE(signal_aggregate.unacknowledged_count, 0) > 0 THEN COALESCE(signal_aggregate.highest_label, '') ELSE '' END,
    'unacknowledged_count', COALESCE(signal_aggregate.unacknowledged_count, 0),
    'filtered_count', COALESCE(aggregate_result.filtered_count, 0),
    'highest_severity', CASE WHEN COALESCE(aggregate_result.filtered_count, 0) > 0 THEN aggregate_result.highest_severity ELSE NULL::text END,
    'highest_label', CASE WHEN COALESCE(aggregate_result.filtered_count, 0) > 0 THEN aggregate_result.highest_label ELSE NULL::text END
  )
  INTO v_result
  FROM aggregate_result
  CROSS JOIN signal_aggregate;

  RETURN COALESCE(v_result, jsonb_build_object(
    'ok', true,
    'generated_at_utc', now()::text,
    'actor_user_id', p_actor_user_id::text,
    'alert_context', v_alert_context,
    'include_acknowledged', COALESCE(p_include_acknowledged, false),
    'limit', COALESCE(v_limit, 0),
    'alerts', '[]'::jsonb,
    'banking_alert_hash', 'banking_alert_signal:v3:' || MD5(''),
    'banking_alert_summary_signature', 'banking_alert_summary:v3:' || MD5(''),
    'banking_unacknowledged_alert_count', 0,
    'banking_highest_alert_severity', '',
    'banking_highest_alert_label', '',
    'unacknowledged_count', 0,
    'filtered_count', 0,
    'highest_severity', NULL::text,
    'highest_label', NULL::text
  ));
END;
$function$;


CREATE OR REPLACE FUNCTION public.banking_alerts_refresh_for_user(
  p_actor_user_id uuid,
  p_alert_context text DEFAULT 'ALERT_PANEL'::text,
  p_limit integer DEFAULT 100
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_alert_context text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_alert_context, 'ALERT_PANEL')), ''), '-', '_'));
  v_active_json jsonb := '{}'::jsonb;
  v_alert_hash text := 'banking_alert_signal:v3:' || MD5('');
  v_summary_hash text := 'banking_alert_summary:v3:' || MD5('');
  v_unacknowledged_count integer := 0;
  v_highest_severity text := NULL::text;
  v_highest_label text := NULL::text;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERTS_REFRESH_FOR_USER_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERTS_REFRESH_FOR_USER_ACTOR_REQUIRED')::text;
  END IF;

  PERFORM public.banking_pay_hot_path_budget_apply(
    CASE WHEN v_alert_context = 'ALERT_REFRESH_JOB' THEN 'ALERT_REFRESH_JOB' ELSE 'ALERT_PANEL' END
  );

  v_active_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    LEAST(GREATEST(COALESCE(p_limit, 100), 0), 100),
    v_alert_context
  );

  v_alert_hash := COALESCE(NULLIF(BTRIM(v_active_json ->> 'banking_alert_hash'), ''), v_alert_hash);
  v_summary_hash := COALESCE(NULLIF(BTRIM(v_active_json ->> 'banking_alert_summary_signature'), ''), v_summary_hash);
  BEGIN
    v_unacknowledged_count := COALESCE((v_active_json ->> 'banking_unacknowledged_alert_count')::integer, 0);
  EXCEPTION WHEN OTHERS THEN
    v_unacknowledged_count := 0;
  END;
  v_highest_severity := NULLIF(BTRIM(COALESCE(v_active_json ->> 'banking_highest_alert_severity', v_active_json ->> 'highest_severity', '')), '');
  v_highest_label := NULLIF(BTRIM(COALESCE(v_active_json ->> 'banking_highest_alert_label', v_active_json ->> 'highest_label', '')), '');

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
    v_alert_hash,
    v_summary_hash,
    v_unacknowledged_count,
    v_highest_severity,
    v_highest_label,
    v_active_json,
    now()
  )
  ON CONFLICT (actor_user_id) DO UPDATE
  SET
    alert_hash = EXCLUDED.alert_hash,
    summary_hash = EXCLUDED.summary_hash,
    unacknowledged_count = EXCLUDED.unacknowledged_count,
    highest_severity = EXCLUDED.highest_severity,
    highest_label = EXCLUDED.highest_label,
    summary_json = EXCLUDED.summary_json,
    updated_at_utc = now();

  RETURN v_active_json;
END;
$function$;

CREATE OR REPLACE FUNCTION public.banking_alert_acknowledge(
  p_alert_fingerprint text,
  p_alert_kind text,
  p_entity_kind text,
  p_entity_id uuid,
  p_actor_user_id uuid,
  p_note text DEFAULT NULL::text,
  p_alert_payload_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_alert_fingerprint text := nullif(btrim(coalesce(p_alert_fingerprint, '')), '');
  v_alert_kind text := upper(nullif(btrim(coalesce(p_alert_kind, '')), ''));
  v_entity_kind text := lower(nullif(btrim(coalesce(p_entity_kind, '')), ''));
  v_alert_payload_json jsonb := '{}'::jsonb;
  v_active_json jsonb := '{}'::jsonb;
  v_remaining_json jsonb := '{}'::jsonb;
  v_active_alert_json jsonb := NULL::jsonb;
  v_current_alert_fingerprint text := NULL::text;
  v_existing_id uuid := NULL::uuid;
  v_existing_alert_kind text := NULL::text;
  v_existing_entity_kind text := NULL::text;
  v_existing_entity_id uuid := NULL::uuid;
  v_existing_alert_payload_json jsonb := '{}'::jsonb;
  v_ack_id uuid;
  v_ack_alert_fingerprint text;
  v_ack_alert_kind text;
  v_ack_entity_kind text;
  v_ack_entity_id uuid;
  v_acknowledged_by_user_id uuid;
  v_acknowledged_at_utc timestamptz;
  v_acknowledge_scope text;
  v_ack_note text;
  v_ack_alert_payload_json jsonb;
  v_ack_resolved_at_ack boolean;
  v_created boolean := false;
  v_upsert_inserted boolean := false;
  v_remaining_count integer := 0;
  v_signal_json jsonb := '{}'::jsonb;
BEGIN
  IF v_alert_fingerprint IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_FINGERPRINT_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_FINGERPRINT_REQUIRED')::text;
  END IF;

  IF v_alert_kind IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_KIND_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_KIND_REQUIRED')::text;
  END IF;

  IF v_entity_kind IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_ENTITY_KIND_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_ENTITY_KIND_REQUIRED')::text;
  END IF;

  IF p_entity_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_ENTITY_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_ENTITY_ID_REQUIRED')::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_ACTOR_REQUIRED')::text;
  END IF;

  IF p_alert_payload_json IS NOT NULL AND coalesce(jsonb_typeof(p_alert_payload_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_PAYLOAD_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANKING_ALERT_ACKNOWLEDGE_PAYLOAD_MUST_BE_OBJECT',
              'alert_fingerprint', v_alert_fingerprint
            )::text;
  END IF;

  PERFORM pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtext('banking_alert_acknowledge:' || v_alert_fingerprint),
    pg_catalog.hashtext(p_actor_user_id::text || ':USER')
  );

  v_alert_payload_json := coalesce(p_alert_payload_json, '{}'::jsonb);

  SELECT
    public.banking_alert_acknowledgements.id,
    public.banking_alert_acknowledgements.alert_kind,
    public.banking_alert_acknowledgements.entity_kind,
    public.banking_alert_acknowledgements.entity_id,
    COALESCE(public.banking_alert_acknowledgements.alert_payload_json, '{}'::jsonb)
  INTO
    v_existing_id,
    v_existing_alert_kind,
    v_existing_entity_kind,
    v_existing_entity_id,
    v_existing_alert_payload_json
  FROM public.banking_alert_acknowledgements
  WHERE public.banking_alert_acknowledgements.alert_fingerprint = v_alert_fingerprint
    AND public.banking_alert_acknowledgements.acknowledged_by_user_id = p_actor_user_id
    AND upper(btrim(coalesce(public.banking_alert_acknowledgements.acknowledge_scope, 'USER'))) = 'USER'
  ORDER BY public.banking_alert_acknowledgements.acknowledged_at_utc ASC, public.banking_alert_acknowledgements.id ASC
  LIMIT 1
  FOR UPDATE;

  IF v_existing_id IS NOT NULL THEN
    IF v_alert_payload_json = '{}'::jsonb THEN
      v_alert_payload_json := COALESCE(v_existing_alert_payload_json, '{}'::jsonb);
    END IF;
  ELSE
    v_active_json := public.banking_alerts_active_for_user(
      p_actor_user_id,
      v_entity_kind,
      p_entity_id,
      false,
      0,
      'ALERT_MANAGEMENT'
    );

    SELECT active_alert.alert_json
    INTO v_active_alert_json
    FROM jsonb_array_elements(coalesce(v_active_json -> 'alerts', '[]'::jsonb)) AS active_alert(alert_json)
    WHERE active_alert.alert_json ->> 'alert_fingerprint' = v_alert_fingerprint
      AND upper(btrim(coalesce(active_alert.alert_json ->> 'alert_kind', ''))) = v_alert_kind
      AND lower(btrim(coalesce(active_alert.alert_json ->> 'entity_kind', ''))) = v_entity_kind
      AND nullif(btrim(coalesce(active_alert.alert_json ->> 'entity_id', '')), '') IS NOT NULL
      AND (active_alert.alert_json ->> 'entity_id')::uuid = p_entity_id
    LIMIT 1;

    IF v_active_alert_json IS NULL THEN
      SELECT
        public.banking_alert_acknowledgements.id,
        public.banking_alert_acknowledgements.alert_kind,
        public.banking_alert_acknowledgements.entity_kind,
        public.banking_alert_acknowledgements.entity_id,
        COALESCE(public.banking_alert_acknowledgements.alert_payload_json, '{}'::jsonb)
      INTO
        v_existing_id,
        v_existing_alert_kind,
        v_existing_entity_kind,
        v_existing_entity_id,
        v_existing_alert_payload_json
      FROM public.banking_alert_acknowledgements
      WHERE public.banking_alert_acknowledgements.alert_fingerprint = v_alert_fingerprint
        AND public.banking_alert_acknowledgements.acknowledged_by_user_id = p_actor_user_id
        AND upper(btrim(coalesce(public.banking_alert_acknowledgements.acknowledge_scope, 'USER'))) = 'USER'
      ORDER BY public.banking_alert_acknowledgements.acknowledged_at_utc ASC, public.banking_alert_acknowledgements.id ASC
      LIMIT 1;

      IF v_existing_id IS NOT NULL THEN
        IF v_alert_payload_json = '{}'::jsonb THEN
          v_alert_payload_json := COALESCE(v_existing_alert_payload_json, '{}'::jsonb);
        END IF;
      ELSE
        SELECT active_alert.alert_json ->> 'alert_fingerprint'
        INTO v_current_alert_fingerprint
        FROM jsonb_array_elements(coalesce(v_active_json -> 'alerts', '[]'::jsonb)) AS active_alert(alert_json)
        WHERE upper(btrim(coalesce(active_alert.alert_json ->> 'alert_kind', ''))) = v_alert_kind
          AND lower(btrim(coalesce(active_alert.alert_json ->> 'entity_kind', ''))) = v_entity_kind
          AND nullif(btrim(coalesce(active_alert.alert_json ->> 'entity_id', '')), '') IS NOT NULL
          AND (active_alert.alert_json ->> 'entity_id')::uuid = p_entity_id
        ORDER BY active_alert.alert_json ->> 'alert_fingerprint'
        LIMIT 1;

        v_remaining_json := public.banking_alerts_active_for_user(
          p_actor_user_id,
          NULL::text,
          NULL::uuid,
          false,
          25,
          'ALERT_MANAGEMENT'
        );

        v_signal_json := public.banking_alert_signal_for_user(
          p_actor_user_id,
          NULL::text,
          'ALERT_MANAGEMENT'
        );

        v_remaining_json := v_remaining_json || jsonb_build_object(
          'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
          'banking_unacknowledged_alert_count', COALESCE((v_signal_json ->> 'banking_unacknowledged_alert_count')::integer, 0),
          'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
          'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
          'unacknowledged_count', COALESCE((v_signal_json ->> 'banking_unacknowledged_alert_count')::integer, 0),
          'highest_severity', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''), ''),
          'highest_label', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''), '')
        );

        RETURN jsonb_build_object(
          'ok', true,
          'created', false,
          'already_acknowledged', false,
          'acknowledged', false,
          'ignored', true,
          'ignored_count', 1,
          'reason', 'ALERT_NOT_CURRENT',
          'code', 'BANKING_ALERT_ACKNOWLEDGE_ALERT_NOT_ACTIVE',
          'alert_fingerprint', v_alert_fingerprint,
          'alert_kind', v_alert_kind,
          'entity_kind', v_entity_kind,
          'entity_id', p_entity_id::text,
          'current_alert_fingerprint', v_current_alert_fingerprint,
          'entity_alert_summary', v_active_json,
          'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
          'banking_unacknowledged_alert_count', COALESCE((v_signal_json ->> 'banking_unacknowledged_alert_count')::integer, 0),
          'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
          'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
          'remaining_alert_summary', v_remaining_json,
          'alert_summary', v_remaining_json
        );
      END IF;
    END IF;

    IF v_existing_id IS NULL AND v_alert_payload_json = '{}'::jsonb THEN
      v_alert_payload_json := coalesce(v_active_alert_json -> 'payload_json', '{}'::jsonb);
    END IF;
  END IF;

  INSERT INTO public.banking_alert_acknowledgements AS banking_alert_acknowledgement_upsert (
    alert_fingerprint,
    alert_kind,
    entity_kind,
    entity_id,
    acknowledged_by_user_id,
    acknowledged_at_utc,
    acknowledge_scope,
    note,
    alert_payload_json,
    resolved_at_ack
  )
  VALUES (
    v_alert_fingerprint,
    v_alert_kind,
    v_entity_kind,
    p_entity_id,
    p_actor_user_id,
    now(),
    'USER',
    nullif(btrim(coalesce(p_note, '')), ''),
    v_alert_payload_json,
    false
  )
  ON CONFLICT (alert_fingerprint, acknowledged_by_user_id, acknowledge_scope) DO UPDATE
  SET
    note = COALESCE(EXCLUDED.note, banking_alert_acknowledgement_upsert.note),
    alert_payload_json = CASE
      WHEN EXCLUDED.alert_payload_json = '{}'::jsonb THEN banking_alert_acknowledgement_upsert.alert_payload_json
      ELSE EXCLUDED.alert_payload_json
    END
  RETURNING
    banking_alert_acknowledgement_upsert.id,
    banking_alert_acknowledgement_upsert.alert_fingerprint,
    banking_alert_acknowledgement_upsert.alert_kind,
    banking_alert_acknowledgement_upsert.entity_kind,
    banking_alert_acknowledgement_upsert.entity_id,
    banking_alert_acknowledgement_upsert.acknowledged_by_user_id,
    banking_alert_acknowledgement_upsert.acknowledged_at_utc,
    banking_alert_acknowledgement_upsert.acknowledge_scope,
    banking_alert_acknowledgement_upsert.note,
    banking_alert_acknowledgement_upsert.alert_payload_json,
    banking_alert_acknowledgement_upsert.resolved_at_ack,
    (banking_alert_acknowledgement_upsert.xmax = 0)
  INTO
    v_ack_id,
    v_ack_alert_fingerprint,
    v_ack_alert_kind,
    v_ack_entity_kind,
    v_ack_entity_id,
    v_acknowledged_by_user_id,
    v_acknowledged_at_utc,
    v_acknowledge_scope,
    v_ack_note,
    v_ack_alert_payload_json,
    v_ack_resolved_at_ack,
    v_upsert_inserted;

  v_created := coalesce(v_upsert_inserted, false);

  v_remaining_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    25,
    'ALERT_MANAGEMENT'
  );

  v_signal_json := public.banking_alert_signal_for_user(
    p_actor_user_id,
    NULL::text,
    'ALERT_MANAGEMENT'
  );

  v_remaining_count := COALESCE((v_signal_json ->> 'banking_unacknowledged_alert_count')::integer, 0);

  v_remaining_json := v_remaining_json || jsonb_build_object(
    'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
    'banking_unacknowledged_alert_count', v_remaining_count,
    'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
    'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
    'unacknowledged_count', v_remaining_count,
    'highest_severity', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''), ''),
    'highest_label', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''), '')
  );

  RETURN jsonb_build_object(
    'ok', true,
    'created', v_created,
    'already_acknowledged', NOT v_created,
    'acknowledged', true,
    'ignored', false,
    'ignored_count', 0,
    'remaining_unacknowledged_count', v_remaining_count,
    'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
    'banking_unacknowledged_alert_count', v_remaining_count,
    'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
    'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
    'remaining_alert_summary', v_remaining_json,
    'alert_summary', v_remaining_json,
    'acknowledgement', jsonb_build_object(
      'id', v_ack_id::text,
      'alert_fingerprint', v_ack_alert_fingerprint,
      'alert_kind', v_ack_alert_kind,
      'entity_kind', v_ack_entity_kind,
      'entity_id', v_ack_entity_id::text,
      'acknowledged_by_user_id', v_acknowledged_by_user_id::text,
      'acknowledged_at_utc', v_acknowledged_at_utc::text,
      'acknowledge_scope', v_acknowledge_scope,
      'note', v_ack_note,
      'alert_payload_json', v_ack_alert_payload_json,
      'resolved_at_ack', v_ack_resolved_at_ack
    )
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.banking_alert_acknowledge_many(
  p_actor_user_id uuid,
  p_alerts_json jsonb,
  p_note text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_requested_count integer := 0;
  v_created_count integer := 0;
  v_already_acknowledged_count integer := 0;
  v_acknowledged_count integer := 0;
  v_ignored_count integer := 0;
  v_remaining_count integer := 0;
  v_active_json jsonb := '{}'::jsonb;
  v_remaining_json jsonb := '{}'::jsonb;
  v_acknowledged_alerts jsonb := '[]'::jsonb;
  v_signal_json jsonb := '{}'::jsonb;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_MANY_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_MANY_ACTOR_REQUIRED')::text;
  END IF;

  IF p_alerts_json IS NULL OR coalesce(jsonb_typeof(p_alerts_json), 'null') <> 'array' THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_MANY_ALERTS_ARRAY_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_MANY_ALERTS_ARRAY_REQUIRED')::text;
  END IF;

  PERFORM pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtext('banking_alert_acknowledge_many'),
    pg_catalog.hashtext(p_actor_user_id::text || ':USER')
  );

  WITH requested_fingerprints_for_count AS (
    SELECT DISTINCT
      nullif(btrim(coalesce(
        CASE
          WHEN jsonb_typeof(requested_alerts_for_count.requested_alert_json) = 'string'
            THEN trim(both '"' from requested_alerts_for_count.requested_alert_json::text)
          WHEN jsonb_typeof(requested_alerts_for_count.requested_alert_json) = 'object'
            THEN COALESCE(
              requested_alerts_for_count.requested_alert_json ->> 'alert_fingerprint',
              requested_alerts_for_count.requested_alert_json ->> 'fingerprint'
            )
          ELSE NULL::text
        END,
        ''
      )), '') AS alert_fingerprint
    FROM jsonb_array_elements(p_alerts_json) AS requested_alerts_for_count(requested_alert_json)
  )
  SELECT count(*)::integer
  INTO v_requested_count
  FROM requested_fingerprints_for_count
  WHERE requested_fingerprints_for_count.alert_fingerprint IS NOT NULL;

  v_active_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    0,
    'ALERT_MANAGEMENT'
  );

  WITH requested_fingerprints AS (
    SELECT DISTINCT
      nullif(btrim(coalesce(
        CASE
          WHEN jsonb_typeof(requested_alerts.requested_alert_json) = 'string'
            THEN trim(both '"' from requested_alerts.requested_alert_json::text)
          WHEN jsonb_typeof(requested_alerts.requested_alert_json) = 'object'
            THEN COALESCE(
              requested_alerts.requested_alert_json ->> 'alert_fingerprint',
              requested_alerts.requested_alert_json ->> 'fingerprint'
            )
          ELSE NULL::text
        END,
        ''
      )), '') AS alert_fingerprint
    FROM jsonb_array_elements(p_alerts_json) AS requested_alerts(requested_alert_json)
  ),
  active_alerts AS (
    SELECT
      active_alert.alert_json ->> 'alert_fingerprint' AS alert_fingerprint,
      active_alert.alert_json ->> 'alert_kind' AS alert_kind,
      active_alert.alert_json ->> 'entity_kind' AS entity_kind,
      (active_alert.alert_json ->> 'entity_id')::uuid AS entity_id,
      active_alert.alert_json AS alert_json,
      coalesce(active_alert.alert_json -> 'payload_json', '{}'::jsonb) AS payload_json
    FROM jsonb_array_elements(coalesce(v_active_json -> 'alerts', '[]'::jsonb)) AS active_alert(alert_json)
    WHERE nullif(btrim(coalesce(active_alert.alert_json ->> 'alert_fingerprint', '')), '') IS NOT NULL
      AND nullif(btrim(coalesce(active_alert.alert_json ->> 'alert_kind', '')), '') IS NOT NULL
      AND nullif(btrim(coalesce(active_alert.alert_json ->> 'entity_kind', '')), '') IS NOT NULL
      AND nullif(btrim(coalesce(active_alert.alert_json ->> 'entity_id', '')), '') IS NOT NULL
  ),
  matched_active_alerts AS (
    SELECT DISTINCT ON (active_alerts.alert_fingerprint)
      active_alerts.alert_fingerprint,
      upper(btrim(active_alerts.alert_kind)) AS alert_kind,
      lower(btrim(active_alerts.entity_kind)) AS entity_kind,
      active_alerts.entity_id,
      active_alerts.alert_json,
      coalesce(active_alerts.payload_json, '{}'::jsonb) AS payload_json
    FROM active_alerts
    JOIN requested_fingerprints
      ON requested_fingerprints.alert_fingerprint = active_alerts.alert_fingerprint
    WHERE requested_fingerprints.alert_fingerprint IS NOT NULL
    ORDER BY active_alerts.alert_fingerprint ASC
  ),
  upserted_acknowledgements AS (
    INSERT INTO public.banking_alert_acknowledgements AS banking_alert_acknowledgement_upsert (
      alert_fingerprint,
      alert_kind,
      entity_kind,
      entity_id,
      acknowledged_by_user_id,
      acknowledged_at_utc,
      acknowledge_scope,
      note,
      alert_payload_json,
      resolved_at_ack
    )
    SELECT
      matched_active_alerts.alert_fingerprint,
      matched_active_alerts.alert_kind,
      matched_active_alerts.entity_kind,
      matched_active_alerts.entity_id,
      p_actor_user_id,
      now(),
      'USER',
      nullif(btrim(coalesce(p_note, '')), ''),
      matched_active_alerts.payload_json,
      false
    FROM matched_active_alerts
    WHERE true
    ON CONFLICT (alert_fingerprint, acknowledged_by_user_id, acknowledge_scope) DO UPDATE
    SET
      note = COALESCE(EXCLUDED.note, banking_alert_acknowledgement_upsert.note),
      alert_payload_json = CASE
        WHEN EXCLUDED.alert_payload_json = '{}'::jsonb THEN banking_alert_acknowledgement_upsert.alert_payload_json
        ELSE EXCLUDED.alert_payload_json
      END
    RETURNING
      banking_alert_acknowledgement_upsert.alert_fingerprint,
      banking_alert_acknowledgement_upsert.alert_kind,
      banking_alert_acknowledgement_upsert.entity_kind,
      banking_alert_acknowledgement_upsert.entity_id,
      banking_alert_acknowledgement_upsert.acknowledged_at_utc,
      (banking_alert_acknowledgement_upsert.xmax = 0) AS created
  ),
  already_existing_requested_acknowledgements AS (
    SELECT DISTINCT ON (public.banking_alert_acknowledgements.alert_fingerprint)
      public.banking_alert_acknowledgements.alert_fingerprint,
      public.banking_alert_acknowledgements.alert_kind,
      public.banking_alert_acknowledgements.entity_kind,
      public.banking_alert_acknowledgements.entity_id,
      public.banking_alert_acknowledgements.acknowledged_at_utc,
      false AS created
    FROM public.banking_alert_acknowledgements
    JOIN requested_fingerprints
      ON requested_fingerprints.alert_fingerprint = public.banking_alert_acknowledgements.alert_fingerprint
    LEFT JOIN upserted_acknowledgements
      ON upserted_acknowledgements.alert_fingerprint = public.banking_alert_acknowledgements.alert_fingerprint
    WHERE requested_fingerprints.alert_fingerprint IS NOT NULL
      AND upserted_acknowledgements.alert_fingerprint IS NULL
      AND public.banking_alert_acknowledgements.acknowledged_by_user_id = p_actor_user_id
      AND upper(btrim(coalesce(public.banking_alert_acknowledgements.acknowledge_scope, 'USER'))) = 'USER'
    ORDER BY public.banking_alert_acknowledgements.alert_fingerprint ASC,
             public.banking_alert_acknowledgements.acknowledged_at_utc ASC,
             public.banking_alert_acknowledgements.id ASC
  ),
  changed_acknowledgements AS (
    SELECT
      upserted_acknowledgements.alert_fingerprint,
      upserted_acknowledgements.alert_kind,
      upserted_acknowledgements.entity_kind,
      upserted_acknowledgements.entity_id,
      upserted_acknowledgements.acknowledged_at_utc,
      upserted_acknowledgements.created
    FROM upserted_acknowledgements

    UNION ALL

    SELECT
      already_existing_requested_acknowledgements.alert_fingerprint,
      already_existing_requested_acknowledgements.alert_kind,
      already_existing_requested_acknowledgements.entity_kind,
      already_existing_requested_acknowledgements.entity_id,
      already_existing_requested_acknowledgements.acknowledged_at_utc,
      already_existing_requested_acknowledgements.created
    FROM already_existing_requested_acknowledgements
  )
  SELECT
    coalesce(count(*) FILTER (WHERE changed_acknowledgements.created = true), 0)::integer,
    coalesce(count(*) FILTER (WHERE changed_acknowledgements.created = false), 0)::integer,
    coalesce(count(*), 0)::integer,
    coalesce(jsonb_agg(jsonb_build_object(
      'alert_fingerprint', changed_acknowledgements.alert_fingerprint,
      'alert_kind', changed_acknowledgements.alert_kind,
      'entity_kind', changed_acknowledgements.entity_kind,
      'entity_id', changed_acknowledgements.entity_id::text,
      'acknowledged_at_utc', changed_acknowledgements.acknowledged_at_utc::text,
      'created', changed_acknowledgements.created,
      'already_acknowledged', NOT changed_acknowledgements.created
    ) ORDER BY changed_acknowledgements.acknowledged_at_utc DESC, changed_acknowledgements.alert_fingerprint ASC), '[]'::jsonb)
  INTO
    v_created_count,
    v_already_acknowledged_count,
    v_acknowledged_count,
    v_acknowledged_alerts
  FROM changed_acknowledgements;

  v_ignored_count := greatest(coalesce(v_requested_count, 0) - coalesce(v_acknowledged_count, 0), 0);

  v_remaining_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    25,
    'ALERT_MANAGEMENT'
  );

  v_signal_json := public.banking_alert_signal_for_user(
    p_actor_user_id,
    NULL::text,
    'ALERT_MANAGEMENT'
  );

  v_remaining_count := COALESCE((v_signal_json ->> 'banking_unacknowledged_alert_count')::integer, 0);

  v_remaining_json := v_remaining_json || jsonb_build_object(
    'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
    'banking_unacknowledged_alert_count', v_remaining_count,
    'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
    'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
    'unacknowledged_count', v_remaining_count,
    'highest_severity', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''), ''),
    'highest_label', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''), '')
  );

  RETURN jsonb_build_object(
    'ok', true,
    'requested_count', coalesce(v_requested_count, 0),
    'created_count', coalesce(v_created_count, 0),
    'already_acknowledged_count', coalesce(v_already_acknowledged_count, 0),
    'acknowledged_count', coalesce(v_acknowledged_count, 0),
    'remaining_unacknowledged_count', coalesce(v_remaining_count, 0),
    'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
    'banking_unacknowledged_alert_count', coalesce(v_remaining_count, 0),
    'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
    'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
    'ignored_count', coalesce(v_ignored_count, 0),
    'acknowledged_alerts', coalesce(v_acknowledged_alerts, '[]'::jsonb),
    'remaining_alert_summary', v_remaining_json,
    'alert_summary', v_remaining_json
  );
END;
$function$;

DROP FUNCTION IF EXISTS public.banking_alert_signal_for_user(uuid, text);

CREATE OR REPLACE FUNCTION public.banking_alert_signal_for_user(
  p_actor_user_id uuid,
  p_last_alert_hash text DEFAULT NULL::text,
  p_alert_context text DEFAULT 'CACHED'::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_last_alert_hash text := NULLIF(BTRIM(COALESCE(p_last_alert_hash, '')), '');
  v_alert_context text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_alert_context, 'CACHED')), ''), '-', '_'));
  v_alert_hash text := 'banking_alert_signal:v3:' || MD5('');
  v_summary_hash text := 'banking_alert_summary:v3:' || MD5('');
  v_unacknowledged_count integer := 0;
  v_highest_severity text := NULL::text;
  v_highest_label text := NULL::text;
  v_summary_json jsonb := '{}'::jsonb;
  v_live_json jsonb := NULL::jsonb;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_SIGNAL_FOR_USER_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_SIGNAL_FOR_USER_ACTOR_REQUIRED')::text;
  END IF;

  IF v_alert_context IN (
    'ALERT_PANEL',
    'ALERTS_PANEL',
    'ALERT_MANAGEMENT',
    'ALERT_REFRESH_JOB',
    'EXPLICIT_ALERT_REFRESH',
    'USER_TRIGGERED_ALERTS'
  ) THEN
    v_live_json := public.banking_alerts_refresh_for_user(
      p_actor_user_id,
      v_alert_context,
      100
    );

    v_alert_hash := COALESCE(NULLIF(BTRIM(COALESCE(v_live_json->>'banking_alert_hash', '')), ''), v_alert_hash);
    v_summary_hash := COALESCE(NULLIF(BTRIM(COALESCE(v_live_json->>'banking_alert_summary_signature', '')), ''), v_summary_hash);

    BEGIN
      v_unacknowledged_count := COALESCE((v_live_json->>'banking_unacknowledged_alert_count')::integer, 0);
    EXCEPTION WHEN OTHERS THEN
      v_unacknowledged_count := 0;
    END;

    v_highest_severity := NULLIF(BTRIM(COALESCE(v_live_json->>'banking_highest_alert_severity', v_live_json->>'highest_severity', '')), '');
    v_highest_label := NULLIF(BTRIM(COALESCE(v_live_json->>'banking_highest_alert_label', v_live_json->>'highest_label', '')), '');
    v_summary_json := COALESCE(v_live_json, '{}'::jsonb);
  ELSE
    SELECT
      COALESCE(alert_summary.alert_hash, v_alert_hash),
      COALESCE(alert_summary.summary_hash, v_summary_hash),
      COALESCE(alert_summary.unacknowledged_count, 0),
      alert_summary.highest_severity,
      alert_summary.highest_label,
      COALESCE(alert_summary.summary_json, '{}'::jsonb)
    INTO
      v_alert_hash,
      v_summary_hash,
      v_unacknowledged_count,
      v_highest_severity,
      v_highest_label,
      v_summary_json
    FROM public.banking_alert_display_summary AS alert_summary
    WHERE alert_summary.actor_user_id = p_actor_user_id;

    IF NOT FOUND THEN
      v_alert_hash := 'banking_alert_signal:v3:' || MD5('');
      v_summary_hash := 'banking_alert_summary:v3:' || MD5('');
      v_unacknowledged_count := 0;
      v_highest_severity := NULL::text;
      v_highest_label := NULL::text;
      v_summary_json := '{}'::jsonb;
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'generated_at_utc', now()::text,
    'actor_user_id', p_actor_user_id::text,
    'cached', v_live_json IS NULL,
    'alert_context', v_alert_context,
    'banking_alert_hash', v_alert_hash,
    'banking_alert_summary_signature', v_summary_hash,
    'banking_alert_summary_changed', COALESCE(v_last_alert_hash IS DISTINCT FROM v_alert_hash, true),
    'banking_unacknowledged_alert_count', COALESCE(v_unacknowledged_count, 0),
    'grouped_banking_unacknowledged_alert_count', COALESCE(v_unacknowledged_count, 0),
    'banking_highest_alert_severity', COALESCE(v_highest_severity, ''),
    'banking_highest_alert_label', COALESCE(v_highest_label, ''),
    'highest_severity', COALESCE(v_highest_severity, ''),
    'highest_label', COALESCE(v_highest_label, ''),
    'summary_is_grouped', true,
    'summary_json', COALESCE(v_summary_json, '{}'::jsonb)
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.banking_alert_acknowledge_all_current(
  p_actor_user_id uuid,
  p_note text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_active_json jsonb := '{}'::jsonb;
  v_remaining_json jsonb := '{}'::jsonb;
  v_active_count integer := 0;
  v_created_count integer := 0;
  v_already_acknowledged_count integer := 0;
  v_acknowledged_count integer := 0;
  v_remaining_count integer := 0;
  v_acknowledged_alerts jsonb := '[]'::jsonb;
  v_signal_json jsonb := '{}'::jsonb;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_ALL_CURRENT_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_ALL_CURRENT_ACTOR_REQUIRED')::text;
  END IF;

  PERFORM pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtext('banking_alert_acknowledge_all_current'),
    pg_catalog.hashtext(p_actor_user_id::text || ':USER')
  );

  v_active_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    0,
    'ALERT_MANAGEMENT'
  );

  WITH active_alert_rows AS (
    SELECT
      active_alert_element.alert_json ->> 'alert_fingerprint' AS active_alert_fingerprint,
      active_alert_element.alert_json ->> 'alert_kind' AS active_alert_kind,
      active_alert_element.alert_json ->> 'entity_kind' AS active_entity_kind,
      (active_alert_element.alert_json ->> 'entity_id')::uuid AS active_entity_id,
      COALESCE(active_alert_element.alert_json -> 'payload_json', '{}'::jsonb) AS active_payload_json
    FROM jsonb_array_elements(COALESCE(v_active_json -> 'alerts', '[]'::jsonb)) AS active_alert_element(alert_json)
    WHERE NULLIF(BTRIM(COALESCE(active_alert_element.alert_json ->> 'alert_fingerprint', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(active_alert_element.alert_json ->> 'alert_kind', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(active_alert_element.alert_json ->> 'entity_kind', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(active_alert_element.alert_json ->> 'entity_id', '')), '') IS NOT NULL
      AND COALESCE((active_alert_element.alert_json ->> 'acknowledged_for_current_user')::boolean, false) = false
  ),
  deduped_active_alert_rows AS (
    SELECT DISTINCT ON (active_alert_rows.active_alert_fingerprint)
      active_alert_rows.active_alert_fingerprint,
      upper(btrim(active_alert_rows.active_alert_kind)) AS active_alert_kind,
      lower(btrim(active_alert_rows.active_entity_kind)) AS active_entity_kind,
      active_alert_rows.active_entity_id,
      COALESCE(active_alert_rows.active_payload_json, '{}'::jsonb) AS active_payload_json
    FROM active_alert_rows
    ORDER BY active_alert_rows.active_alert_fingerprint ASC
  ),
  upserted_acknowledgements AS (
    INSERT INTO public.banking_alert_acknowledgements AS banking_alert_acknowledgement_upsert (
      alert_fingerprint,
      alert_kind,
      entity_kind,
      entity_id,
      acknowledged_by_user_id,
      acknowledged_at_utc,
      acknowledge_scope,
      note,
      alert_payload_json,
      resolved_at_ack
    )
    SELECT
      deduped_active_alert_rows.active_alert_fingerprint,
      deduped_active_alert_rows.active_alert_kind,
      deduped_active_alert_rows.active_entity_kind,
      deduped_active_alert_rows.active_entity_id,
      p_actor_user_id,
      now(),
      'USER',
      NULLIF(BTRIM(COALESCE(p_note, '')), ''),
      deduped_active_alert_rows.active_payload_json,
      false
    FROM deduped_active_alert_rows
    WHERE true
    ON CONFLICT (alert_fingerprint, acknowledged_by_user_id, acknowledge_scope) DO UPDATE
    SET
      note = COALESCE(EXCLUDED.note, banking_alert_acknowledgement_upsert.note),
      alert_payload_json = CASE
        WHEN EXCLUDED.alert_payload_json = '{}'::jsonb THEN banking_alert_acknowledgement_upsert.alert_payload_json
        ELSE EXCLUDED.alert_payload_json
      END
    RETURNING
      banking_alert_acknowledgement_upsert.alert_fingerprint,
      banking_alert_acknowledgement_upsert.alert_kind,
      banking_alert_acknowledgement_upsert.entity_kind,
      banking_alert_acknowledgement_upsert.entity_id,
      banking_alert_acknowledgement_upsert.acknowledged_at_utc,
      (banking_alert_acknowledgement_upsert.xmax = 0) AS created
  ),
  acknowledgement_summary AS (
    SELECT
      COALESCE(count(*) FILTER (WHERE upserted_acknowledgements.created = true), 0)::integer AS created_alert_count,
      COALESCE(count(*) FILTER (WHERE upserted_acknowledgements.created = false), 0)::integer AS already_acknowledged_alert_count,
      COALESCE(count(*), 0)::integer AS acknowledged_alert_count,
      COALESCE(jsonb_agg(jsonb_build_object(
        'alert_fingerprint', upserted_acknowledgements.alert_fingerprint,
        'alert_kind', upserted_acknowledgements.alert_kind,
        'entity_kind', upserted_acknowledgements.entity_kind,
        'entity_id', upserted_acknowledgements.entity_id::text,
        'acknowledged_at_utc', upserted_acknowledgements.acknowledged_at_utc::text,
        'created', upserted_acknowledgements.created,
        'already_acknowledged', NOT upserted_acknowledgements.created
      ) ORDER BY upserted_acknowledgements.acknowledged_at_utc DESC, upserted_acknowledgements.alert_fingerprint ASC), '[]'::jsonb) AS acknowledged_alerts_json
    FROM upserted_acknowledgements
  ),
  counted_active_alerts AS (
    SELECT COUNT(*)::integer AS active_alert_count
    FROM deduped_active_alert_rows
  )
  SELECT
    COALESCE(counted_active_alerts.active_alert_count, 0),
    COALESCE(acknowledgement_summary.created_alert_count, 0),
    COALESCE(acknowledgement_summary.already_acknowledged_alert_count, 0),
    COALESCE(acknowledgement_summary.acknowledged_alert_count, 0),
    COALESCE(acknowledgement_summary.acknowledged_alerts_json, '[]'::jsonb)
  INTO
    v_active_count,
    v_created_count,
    v_already_acknowledged_count,
    v_acknowledged_count,
    v_acknowledged_alerts
  FROM counted_active_alerts
  CROSS JOIN acknowledgement_summary;

  v_remaining_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    25,
    'ALERT_MANAGEMENT'
  );

  v_signal_json := public.banking_alert_signal_for_user(
    p_actor_user_id,
    NULL::text,
    'ALERT_MANAGEMENT'
  );

  v_remaining_count := COALESCE((v_signal_json ->> 'banking_unacknowledged_alert_count')::integer, 0);

  v_remaining_json := v_remaining_json || jsonb_build_object(
    'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
    'banking_unacknowledged_alert_count', v_remaining_count,
    'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
    'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
    'unacknowledged_count', v_remaining_count,
    'highest_severity', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''), ''),
    'highest_label', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''), '')
  );

  RETURN jsonb_build_object(
    'ok', true,
    'mode', 'clear_all',
    'active_count_before_acknowledge', COALESCE(v_active_count, 0),
    'requested_count', COALESCE(v_active_count, 0),
    'created_count', COALESCE(v_created_count, 0),
    'already_acknowledged_count', COALESCE(v_already_acknowledged_count, 0),
    'acknowledged_count', COALESCE(v_acknowledged_count, 0),
    'remaining_unacknowledged_count', COALESCE(v_remaining_count, 0),
    'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
    'banking_unacknowledged_alert_count', COALESCE(v_remaining_count, 0),
    'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
    'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
    'ignored_count', GREATEST(COALESCE(v_active_count, 0) - COALESCE(v_acknowledged_count, 0), 0),
    'acknowledged_alerts', COALESCE(v_acknowledged_alerts, '[]'::jsonb),
    'remaining_alert_summary', v_remaining_json,
    'alert_summary', v_remaining_json
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.banking_alert_preferences_get(p_user_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_preferences public.banking_alert_user_preferences%ROWTYPE;
  v_defaults_applied boolean := false;
  v_enabled boolean := true;
  v_alert_kind_allowlist jsonb := NULL::jsonb;
  v_alert_kind_blocklist jsonb := '[]'::jsonb;
  v_failure_reason_allowlist jsonb := NULL::jsonb;
  v_failure_reason_blocklist jsonb := '[]'::jsonb;
  v_include_action_required boolean := true;
  v_include_progress_alerts boolean := true;
  v_include_informational_alerts boolean := false;
  v_include_success_alerts boolean := true;
  v_severity_min text := 'ACTION_REQUIRED';
  v_muted_provider_keys jsonb := '[]'::jsonb;
  v_muted_pay_batch_ids jsonb := '[]'::jsonb;
  v_snoozed_until_utc_text text := NULL::text;
  v_mode text := 'ALL_ACTION_REQUIRED';
  v_failure_reason_groups jsonb := '[]'::jsonb;
  v_informational_alert_kinds jsonb := '[]'::jsonb;
  v_allowed_alert_kinds jsonb := to_jsonb(ARRAY[
    'PROVIDER_OUTAGE_RETRY_LATER',
    'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
    'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
    'AUTO_UNWIND_PROGRESS',
    'WHOLE_BATCH_CANCELLATION_PROGRESS',
    'MANUAL_ADJUSTMENTS_CARRIED_FORWARD',
    'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
    'PAID_SETTLED_RECOVERY_REQUIRED',
    'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
    'WEBHOOK_UNMATCHED_REVIEW_REQUIRED',
    'BATCH_SCHEDULED_SUCCESS',
    'BATCH_SETTLED_SUCCESS'
  ]::text[]);
  v_allowed_failure_reason_groups jsonb := to_jsonb(ARRAY[
    'INSUFFICIENT_FUNDS',
    'UNKNOWN_RECIPIENT',
    'INVALID_ACCOUNT',
    'ACCOUNT_CLOSED',
    'BANK_REJECTED',
    'PROVIDER_OUTAGE',
    'PROVIDER_UNKNOWN',
    'COMPLIANCE_REVIEW',
    'DUPLICATE_RISK',
    'PAID_RECOVERY_REQUIRED',
    'MANUAL_ADJUSTMENT_BLOCKER',
    'WEBHOOK_UNMATCHED',
    'PROVIDER_FAILED_UNSPECIFIED'
  ]::text[]);
  v_progress_alert_kinds jsonb := to_jsonb(ARRAY[
    'AUTO_UNWIND_PROGRESS',
    'WHOLE_BATCH_CANCELLATION_PROGRESS'
  ]::text[]);
  v_default_informational_alert_kinds jsonb := to_jsonb(ARRAY[
    'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
  ]::text[]);
  v_effective_options_json jsonb := '{}'::jsonb;
BEGIN
  IF p_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_GET_USER_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_GET_USER_REQUIRED')::text;
  END IF;

  SELECT preference_row.*
  INTO v_preferences
  FROM public.banking_alert_user_preferences AS preference_row
  WHERE preference_row.user_id = p_user_id;

  IF FOUND THEN
    v_enabled := COALESCE(v_preferences.enabled, true);
    v_alert_kind_allowlist := CASE WHEN jsonb_typeof(v_preferences.alert_kind_allowlist) = 'array' THEN v_preferences.alert_kind_allowlist ELSE NULL::jsonb END;
    v_alert_kind_blocklist := COALESCE(v_preferences.alert_kind_blocklist, '[]'::jsonb);
    v_failure_reason_allowlist := CASE WHEN jsonb_typeof(v_preferences.failure_reason_allowlist) = 'array' THEN v_preferences.failure_reason_allowlist ELSE NULL::jsonb END;
    v_failure_reason_blocklist := COALESCE(v_preferences.failure_reason_blocklist, '[]'::jsonb);
    v_include_action_required := COALESCE(v_preferences.include_action_required, true);
    v_include_progress_alerts := COALESCE(v_preferences.include_progress_alerts, true);
    v_include_informational_alerts := COALESCE(v_preferences.include_informational_alerts, false);
    v_include_success_alerts := COALESCE(v_preferences.include_success_alerts, true);
    v_severity_min := COALESCE(NULLIF(BTRIM(v_preferences.severity_min), ''), 'ACTION_REQUIRED');
    v_muted_provider_keys := COALESCE(v_preferences.muted_provider_keys, '[]'::jsonb);
    v_muted_pay_batch_ids := COALESCE(v_preferences.muted_pay_batch_ids, '[]'::jsonb);
    v_snoozed_until_utc_text := CASE WHEN v_preferences.snoozed_until_utc IS NULL THEN NULL ELSE v_preferences.snoozed_until_utc::text END;
  ELSE
    v_defaults_applied := true;
  END IF;

  v_failure_reason_groups := CASE
    WHEN jsonb_typeof(v_failure_reason_allowlist) = 'array' THEN v_failure_reason_allowlist
    ELSE '[]'::jsonb
  END;

  v_mode := CASE
    WHEN COALESCE(v_enabled, true) IS NOT TRUE THEN 'NO_BANKING_PAY_ALERTS'
    WHEN jsonb_typeof(v_failure_reason_allowlist) = 'array' AND jsonb_array_length(v_failure_reason_allowlist) > 0 THEN 'SELECTED_FAILURE_REASONS'
    ELSE 'ALL_ACTION_REQUIRED'
  END;

  v_informational_alert_kinds := '[]'::jsonb;
  IF COALESCE(v_include_progress_alerts, true) THEN
    v_informational_alert_kinds := v_informational_alert_kinds || v_progress_alert_kinds;
  END IF;
  IF COALESCE(v_include_informational_alerts, false) THEN
    v_informational_alert_kinds := v_informational_alert_kinds || v_default_informational_alert_kinds;
  END IF;

  v_effective_options_json := jsonb_build_object(
    'modes', to_jsonb(ARRAY['ALL_ACTION_REQUIRED','SELECTED_FAILURE_REASONS','NO_BANKING_PAY_ALERTS']::text[]),
    'alert_kinds', v_allowed_alert_kinds,
    'failure_reason_groups', v_allowed_failure_reason_groups,
    'progress_alert_kinds', v_progress_alert_kinds,
    'informational_alert_kinds', v_default_informational_alert_kinds,
        'success_alerts_available', true,
    'default_mode', 'ALL_ACTION_REQUIRED'
  );

  RETURN jsonb_build_object(
    'ok', true,
    'user_id', p_user_id::text,
    'enabled', v_enabled,
    'mode', v_mode,
    'alert_kind_allowlist', v_alert_kind_allowlist,
    'alert_kind_blocklist', v_alert_kind_blocklist,
    'failure_reason_allowlist', v_failure_reason_allowlist,
    'failure_reason_blocklist', v_failure_reason_blocklist,
    'failure_reason_groups', v_failure_reason_groups,
    'include_action_required', v_include_action_required,
    'include_progress_alerts', v_include_progress_alerts,
    'include_informational_alerts', v_include_informational_alerts,
    'include_success_alerts', v_include_success_alerts,
    'informational_alert_kinds', v_informational_alert_kinds,
    'severity_min', v_severity_min,
    'muted_provider_keys', v_muted_provider_keys,
    'muted_pay_batch_ids', v_muted_pay_batch_ids,
    'snoozed_until_utc', v_snoozed_until_utc_text,
    'effective_defaults_applied', v_defaults_applied,
    'effective_options_json', v_effective_options_json
  );
END;
$function$;



CREATE OR REPLACE FUNCTION public.banking_alert_preferences_update(p_user_id uuid, p_preferences_json jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_payload jsonb := '{}'::jsonb;
  v_current jsonb := '{}'::jsonb;
  v_enabled boolean := true;
  v_alert_kind_allowlist jsonb := NULL::jsonb;
  v_alert_kind_blocklist jsonb := '[]'::jsonb;
  v_failure_reason_allowlist jsonb := NULL::jsonb;
  v_failure_reason_blocklist jsonb := '[]'::jsonb;
  v_include_action_required boolean := true;
  v_include_progress_alerts boolean := true;
  v_include_informational_alerts boolean := false;
  v_include_success_alerts boolean := true;
  v_severity_min text := 'ACTION_REQUIRED';
  v_muted_provider_keys jsonb := '[]'::jsonb;
  v_muted_pay_batch_ids jsonb := '[]'::jsonb;
  v_snoozed_until_utc timestamptz := NULL::timestamptz;
  v_mode text := NULL::text;
  v_informational_alert_kinds jsonb := '[]'::jsonb;
  v_allowed_alert_kinds text[] := ARRAY[
    'PROVIDER_OUTAGE_RETRY_LATER',
    'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
    'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
    'AUTO_UNWIND_PROGRESS',
    'WHOLE_BATCH_CANCELLATION_PROGRESS',
    'MANUAL_ADJUSTMENTS_CARRIED_FORWARD',
    'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
    'PAID_SETTLED_RECOVERY_REQUIRED',
    'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
    'WEBHOOK_UNMATCHED_REVIEW_REQUIRED',
    'BATCH_SCHEDULED_SUCCESS',
    'BATCH_SETTLED_SUCCESS'
  ];
  v_allowed_failure_reasons text[] := ARRAY[
    'INSUFFICIENT_FUNDS',
    'UNKNOWN_RECIPIENT',
    'INVALID_ACCOUNT',
    'ACCOUNT_CLOSED',
    'BANK_REJECTED',
    'PROVIDER_OUTAGE',
    'PROVIDER_UNKNOWN',
    'COMPLIANCE_REVIEW',
    'DUPLICATE_RISK',
    'PAID_RECOVERY_REQUIRED',
    'MANUAL_ADJUSTMENT_BLOCKER',
    'WEBHOOK_UNMATCHED',
    'PROVIDER_FAILED_UNSPECIFIED'
  ];
  v_value_text text;
  v_preference_id uuid;
  v_result jsonb := '{}'::jsonb;
BEGIN
  IF p_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_UPDATE_USER_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_UPDATE_USER_REQUIRED')::text;
  END IF;

  IF p_preferences_json IS NOT NULL AND COALESCE(jsonb_typeof(p_preferences_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_UPDATE_PAYLOAD_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_UPDATE_PAYLOAD_MUST_BE_OBJECT')::text;
  END IF;

  v_payload := COALESCE(p_preferences_json, '{}'::jsonb);
  v_current := public.banking_alert_preferences_get(p_user_id);

  v_enabled := COALESCE((v_current ->> 'enabled')::boolean, true);
  v_alert_kind_allowlist := CASE WHEN jsonb_typeof(v_current -> 'alert_kind_allowlist') = 'array' THEN v_current -> 'alert_kind_allowlist' ELSE NULL::jsonb END;
  v_alert_kind_blocklist := CASE WHEN jsonb_typeof(v_current -> 'alert_kind_blocklist') = 'array' THEN v_current -> 'alert_kind_blocklist' ELSE '[]'::jsonb END;
  v_failure_reason_allowlist := CASE WHEN jsonb_typeof(v_current -> 'failure_reason_allowlist') = 'array' THEN v_current -> 'failure_reason_allowlist' ELSE NULL::jsonb END;
  v_failure_reason_blocklist := CASE WHEN jsonb_typeof(v_current -> 'failure_reason_blocklist') = 'array' THEN v_current -> 'failure_reason_blocklist' ELSE '[]'::jsonb END;
  v_include_action_required := COALESCE((v_current ->> 'include_action_required')::boolean, true);
  v_include_progress_alerts := COALESCE((v_current ->> 'include_progress_alerts')::boolean, true);
  v_include_informational_alerts := COALESCE((v_current ->> 'include_informational_alerts')::boolean, false);
  v_include_success_alerts := COALESCE((v_current ->> 'include_success_alerts')::boolean, true);
  v_severity_min := COALESCE(NULLIF(BTRIM(v_current ->> 'severity_min'), ''), 'ACTION_REQUIRED');
  v_muted_provider_keys := COALESCE(v_current -> 'muted_provider_keys', '[]'::jsonb);
  v_muted_pay_batch_ids := COALESCE(v_current -> 'muted_pay_batch_ids', '[]'::jsonb);

  IF NULLIF(BTRIM(COALESCE(v_current ->> 'snoozed_until_utc', '')), '') IS NOT NULL THEN
    BEGIN
      v_snoozed_until_utc := (v_current ->> 'snoozed_until_utc')::timestamptz;
    EXCEPTION WHEN OTHERS THEN
      v_snoozed_until_utc := NULL::timestamptz;
    END;
  END IF;

  IF v_payload ? 'mode' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'mode'), 'null') <> 'string' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_MODE_MUST_BE_STRING'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_MODE_MUST_BE_STRING')::text;
    END IF;

    v_mode := UPPER(NULLIF(BTRIM(COALESCE(v_payload ->> 'mode', '')), ''));
    IF v_mode = 'NONE' THEN
      v_mode := 'NO_BANKING_PAY_ALERTS';
    END IF;

    IF v_mode NOT IN ('NO_BANKING_PAY_ALERTS', 'ALL_ACTION_REQUIRED', 'SELECTED_FAILURE_REASONS') THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_MODE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_MODE', 'mode', COALESCE(v_payload ->> 'mode', NULL))::text;
    END IF;

    IF v_mode = 'NO_BANKING_PAY_ALERTS' THEN
      v_enabled := false;
    ELSIF v_mode = 'ALL_ACTION_REQUIRED' THEN
      v_enabled := true;
      v_include_action_required := true;
      IF NOT (v_payload ? 'failure_reason_allowlist') AND NOT (v_payload ? 'failure_reason_groups') THEN
        v_failure_reason_allowlist := NULL::jsonb;
      END IF;
    ELSIF v_mode = 'SELECTED_FAILURE_REASONS' THEN
      v_enabled := true;
      v_include_action_required := true;
      IF NOT (v_payload ? 'failure_reason_allowlist') AND NOT (v_payload ? 'failure_reason_groups') THEN
        v_failure_reason_allowlist := '[]'::jsonb;
      END IF;
    END IF;
  END IF;

  IF v_payload ? 'failure_reason_groups' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'failure_reason_groups'), 'null') <> 'array' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_REASON_GROUPS_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_REASON_GROUPS_MUST_BE_ARRAY')::text;
    END IF;

    SELECT COALESCE(jsonb_agg(to_jsonb(normalised_reason.failure_reason_group) ORDER BY normalised_reason.failure_reason_group), '[]'::jsonb)
    INTO v_failure_reason_allowlist
    FROM (
      SELECT DISTINCT CASE UPPER(BTRIM(reason_value.value))
        WHEN 'PROVIDER_OUTCOME_UNKNOWN' THEN 'PROVIDER_UNKNOWN'
        WHEN 'UNSPECIFIED_PROVIDER_FAILURE' THEN 'PROVIDER_FAILED_UNSPECIFIED'
        ELSE UPPER(BTRIM(reason_value.value))
      END AS failure_reason_group
      FROM jsonb_array_elements_text(v_payload -> 'failure_reason_groups') AS reason_value(value)
      WHERE NULLIF(BTRIM(reason_value.value), '') IS NOT NULL
    ) AS normalised_reason;
  END IF;

  IF v_payload ? 'informational_alert_kinds' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'informational_alert_kinds'), 'null') <> 'array' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INFORMATIONAL_KINDS_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INFORMATIONAL_KINDS_MUST_BE_ARRAY')::text;
    END IF;

    SELECT COALESCE(jsonb_agg(to_jsonb(UPPER(BTRIM(informational_kind.value))) ORDER BY UPPER(BTRIM(informational_kind.value))), '[]'::jsonb)
    INTO v_informational_alert_kinds
    FROM jsonb_array_elements_text(v_payload -> 'informational_alert_kinds') AS informational_kind(value)
    WHERE NULLIF(BTRIM(informational_kind.value), '') IS NOT NULL;

    v_include_informational_alerts := EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_informational_alert_kinds) AS informational_kind(value)
      WHERE informational_kind.value IN ('MANUAL_ADJUSTMENTS_CARRIED_FORWARD')
    );

    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_informational_alert_kinds) AS progress_kind(value)
      WHERE progress_kind.value IN ('AUTO_UNWIND_PROGRESS', 'WHOLE_BATCH_CANCELLATION_PROGRESS')
    ) THEN
      v_include_progress_alerts := true;
    END IF;
  END IF;

  FOR v_value_text IN
    SELECT informational_kind_entry.value
    FROM jsonb_array_elements_text(COALESCE(v_informational_alert_kinds, '[]'::jsonb)) AS informational_kind_entry(value)
  LOOP
    IF NOT (v_value_text = ANY(v_allowed_alert_kinds)) THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_INFORMATIONAL_ALERT_KIND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_INFORMATIONAL_ALERT_KIND', 'alert_kind', v_value_text)::text;
    END IF;
  END LOOP;

  IF v_payload ? 'enabled' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'enabled'), 'null') <> 'boolean' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_ENABLED_MUST_BE_BOOLEAN'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_ENABLED_MUST_BE_BOOLEAN')::text;
    END IF;
    v_enabled := (v_payload ->> 'enabled')::boolean;
  END IF;

  IF v_payload ? 'include_action_required' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'include_action_required'), 'null') <> 'boolean' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_ACTION_REQUIRED_MUST_BE_BOOLEAN'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_ACTION_REQUIRED_MUST_BE_BOOLEAN')::text;
    END IF;
    v_include_action_required := (v_payload ->> 'include_action_required')::boolean;
  END IF;

  IF v_payload ? 'include_progress_alerts' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'include_progress_alerts'), 'null') <> 'boolean' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_PROGRESS_MUST_BE_BOOLEAN'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_PROGRESS_MUST_BE_BOOLEAN')::text;
    END IF;
    v_include_progress_alerts := (v_payload ->> 'include_progress_alerts')::boolean;
  END IF;

  IF v_payload ? 'include_informational_alerts' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'include_informational_alerts'), 'null') <> 'boolean' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INFORMATIONAL_MUST_BE_BOOLEAN'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INFORMATIONAL_MUST_BE_BOOLEAN')::text;
    END IF;
    v_include_informational_alerts := (v_payload ->> 'include_informational_alerts')::boolean;
  END IF;

  IF v_payload ? 'include_success_alerts' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'include_success_alerts'), 'null') <> 'boolean' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_SUCCESS_MUST_BE_BOOLEAN'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_SUCCESS_MUST_BE_BOOLEAN')::text;
    END IF;
    v_include_success_alerts := (v_payload ->> 'include_success_alerts')::boolean;
  END IF;

  IF v_payload ? 'severity_min' THEN
    v_severity_min := UPPER(NULLIF(BTRIM(COALESCE(v_payload ->> 'severity_min', '')), ''));
    IF v_severity_min IS NULL OR v_severity_min NOT IN ('INFO','PROGRESS','ACTION_REQUIRED','CRITICAL') THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_SEVERITY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'BANKING_ALERT_PREFERENCES_INVALID_SEVERITY',
                'severity_min', COALESCE(v_payload ->> 'severity_min', NULL)
              )::text;
    END IF;
  END IF;

  IF v_payload ? 'alert_kind_allowlist' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'alert_kind_allowlist'), 'null') = 'null' THEN
      v_alert_kind_allowlist := NULL::jsonb;
    ELSIF jsonb_typeof(v_payload -> 'alert_kind_allowlist') = 'array' THEN
      SELECT COALESCE(jsonb_agg(to_jsonb(normalised_alert_kind.alert_kind) ORDER BY normalised_alert_kind.alert_kind), '[]'::jsonb)
      INTO v_alert_kind_allowlist
      FROM (
        SELECT DISTINCT UPPER(BTRIM(alert_kind_value.value)) AS alert_kind
        FROM jsonb_array_elements_text(v_payload -> 'alert_kind_allowlist') AS alert_kind_value(value)
        WHERE NULLIF(BTRIM(alert_kind_value.value), '') IS NOT NULL
      ) AS normalised_alert_kind;
    ELSE
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_ALERT_ALLOWLIST_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_ALERT_ALLOWLIST_MUST_BE_ARRAY')::text;
    END IF;
  END IF;

  IF v_payload ? 'alert_kind_blocklist' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'alert_kind_blocklist'), 'null') <> 'array' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_ALERT_BLOCKLIST_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_ALERT_BLOCKLIST_MUST_BE_ARRAY')::text;
    END IF;
    SELECT COALESCE(jsonb_agg(to_jsonb(normalised_alert_kind.alert_kind) ORDER BY normalised_alert_kind.alert_kind), '[]'::jsonb)
    INTO v_alert_kind_blocklist
    FROM (
      SELECT DISTINCT UPPER(BTRIM(alert_kind_value.value)) AS alert_kind
      FROM jsonb_array_elements_text(v_payload -> 'alert_kind_blocklist') AS alert_kind_value(value)
      WHERE NULLIF(BTRIM(alert_kind_value.value), '') IS NOT NULL
    ) AS normalised_alert_kind;
  END IF;

  FOR v_value_text IN
    SELECT alert_kind_entry.value
    FROM jsonb_array_elements_text(COALESCE(v_alert_kind_allowlist, '[]'::jsonb)) AS alert_kind_entry(value)
  LOOP
    IF NOT (v_value_text = ANY(v_allowed_alert_kinds)) THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_ALERT_KIND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_ALERT_KIND', 'alert_kind', v_value_text)::text;
    END IF;
  END LOOP;

  FOR v_value_text IN
    SELECT alert_kind_entry.value
    FROM jsonb_array_elements_text(COALESCE(v_alert_kind_blocklist, '[]'::jsonb)) AS alert_kind_entry(value)
  LOOP
    IF NOT (v_value_text = ANY(v_allowed_alert_kinds)) THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_ALERT_KIND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_ALERT_KIND', 'alert_kind', v_value_text)::text;
    END IF;
  END LOOP;

  IF v_payload ? 'failure_reason_allowlist' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'failure_reason_allowlist'), 'null') = 'null' THEN
      v_failure_reason_allowlist := NULL::jsonb;
    ELSIF jsonb_typeof(v_payload -> 'failure_reason_allowlist') = 'array' THEN
      SELECT COALESCE(jsonb_agg(to_jsonb(normalised_reason.failure_reason_group) ORDER BY normalised_reason.failure_reason_group), '[]'::jsonb)
      INTO v_failure_reason_allowlist
      FROM (
        SELECT DISTINCT CASE UPPER(BTRIM(reason_value.value))
        WHEN 'PROVIDER_OUTCOME_UNKNOWN' THEN 'PROVIDER_UNKNOWN'
        WHEN 'UNSPECIFIED_PROVIDER_FAILURE' THEN 'PROVIDER_FAILED_UNSPECIFIED'
        ELSE UPPER(BTRIM(reason_value.value))
      END AS failure_reason_group
        FROM jsonb_array_elements_text(v_payload -> 'failure_reason_allowlist') AS reason_value(value)
        WHERE NULLIF(BTRIM(reason_value.value), '') IS NOT NULL
      ) AS normalised_reason;
    ELSE
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_REASON_ALLOWLIST_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_REASON_ALLOWLIST_MUST_BE_ARRAY')::text;
    END IF;
  END IF;

  IF v_payload ? 'failure_reason_blocklist' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'failure_reason_blocklist'), 'null') <> 'array' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_REASON_BLOCKLIST_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_REASON_BLOCKLIST_MUST_BE_ARRAY')::text;
    END IF;
    SELECT COALESCE(jsonb_agg(to_jsonb(normalised_reason.failure_reason_group) ORDER BY normalised_reason.failure_reason_group), '[]'::jsonb)
    INTO v_failure_reason_blocklist
    FROM (
      SELECT DISTINCT CASE UPPER(BTRIM(reason_value.value))
        WHEN 'PROVIDER_OUTCOME_UNKNOWN' THEN 'PROVIDER_UNKNOWN'
        WHEN 'UNSPECIFIED_PROVIDER_FAILURE' THEN 'PROVIDER_FAILED_UNSPECIFIED'
        ELSE UPPER(BTRIM(reason_value.value))
      END AS failure_reason_group
      FROM jsonb_array_elements_text(v_payload -> 'failure_reason_blocklist') AS reason_value(value)
      WHERE NULLIF(BTRIM(reason_value.value), '') IS NOT NULL
    ) AS normalised_reason;
  END IF;

  FOR v_value_text IN
    SELECT reason_entry.value
    FROM jsonb_array_elements_text(COALESCE(v_failure_reason_allowlist, '[]'::jsonb)) AS reason_entry(value)
  LOOP
    IF NOT (v_value_text = ANY(v_allowed_failure_reasons)) THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_FAILURE_REASON'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_FAILURE_REASON', 'failure_reason_group', v_value_text)::text;
    END IF;
  END LOOP;

  FOR v_value_text IN
    SELECT reason_entry.value
    FROM jsonb_array_elements_text(COALESCE(v_failure_reason_blocklist, '[]'::jsonb)) AS reason_entry(value)
  LOOP
    IF NOT (v_value_text = ANY(v_allowed_failure_reasons)) THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_FAILURE_REASON'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_FAILURE_REASON', 'failure_reason_group', v_value_text)::text;
    END IF;
  END LOOP;

  IF v_payload ? 'muted_provider_keys' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'muted_provider_keys'), 'null') <> 'array' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_MUTED_PROVIDERS_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_MUTED_PROVIDERS_MUST_BE_ARRAY')::text;
    END IF;
    SELECT COALESCE(jsonb_agg(to_jsonb(provider_value.provider_key) ORDER BY provider_value.provider_key), '[]'::jsonb)
    INTO v_muted_provider_keys
    FROM (
      SELECT DISTINCT UPPER(BTRIM(muted_provider.value)) AS provider_key
      FROM jsonb_array_elements_text(v_payload -> 'muted_provider_keys') AS muted_provider(value)
      WHERE NULLIF(BTRIM(muted_provider.value), '') IS NOT NULL
    ) AS provider_value;
  END IF;

  IF v_payload ? 'muted_pay_batch_ids' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'muted_pay_batch_ids'), 'null') <> 'array' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_MUTED_BATCHES_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_MUTED_BATCHES_MUST_BE_ARRAY')::text;
    END IF;
    SELECT COALESCE(jsonb_agg(to_jsonb(batch_value.batch_id_text) ORDER BY batch_value.batch_id_text), '[]'::jsonb)
    INTO v_muted_pay_batch_ids
    FROM (
      SELECT DISTINCT LOWER(BTRIM(muted_batch.value)) AS batch_id_text
      FROM jsonb_array_elements_text(v_payload -> 'muted_pay_batch_ids') AS muted_batch(value)
      WHERE NULLIF(BTRIM(muted_batch.value), '') IS NOT NULL
    ) AS batch_value;
  END IF;

  FOR v_value_text IN
    SELECT muted_batch_entry.value
    FROM jsonb_array_elements_text(COALESCE(v_muted_pay_batch_ids, '[]'::jsonb)) AS muted_batch_entry(value)
  LOOP
    IF v_value_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_MUTED_BATCH_ID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_MUTED_BATCH_ID', 'pay_batch_id', v_value_text)::text;
    END IF;
  END LOOP;

  IF v_payload ? 'snoozed_until_utc' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'snoozed_until_utc'), 'null') = 'null' THEN
      v_snoozed_until_utc := NULL::timestamptz;
    ELSIF jsonb_typeof(v_payload -> 'snoozed_until_utc') = 'string' THEN
      BEGIN
        v_snoozed_until_utc := NULLIF(BTRIM(v_payload ->> 'snoozed_until_utc'), '')::timestamptz;
      EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_SNOOZE_TIMESTAMP'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_SNOOZE_TIMESTAMP')::text;
      END;
    ELSE
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_SNOOZE_MUST_BE_STRING_OR_NULL'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_SNOOZE_MUST_BE_STRING_OR_NULL')::text;
    END IF;

    IF v_snoozed_until_utc IS NOT NULL AND v_snoozed_until_utc > now() + interval '366 days' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_SNOOZE_TOO_FAR_IN_FUTURE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_SNOOZE_TOO_FAR_IN_FUTURE')::text;
    END IF;
  END IF;

  IF v_mode = 'NO_BANKING_PAY_ALERTS' THEN
    v_enabled := false;
  ELSIF v_mode = 'ALL_ACTION_REQUIRED' THEN
    v_enabled := true;
    v_include_action_required := true;
    IF NOT (v_payload ? 'failure_reason_allowlist') AND NOT (v_payload ? 'failure_reason_groups') THEN
      v_failure_reason_allowlist := NULL::jsonb;
    END IF;
  ELSIF v_mode = 'SELECTED_FAILURE_REASONS' THEN
    v_enabled := true;
    v_include_action_required := true;
    IF NOT (v_payload ? 'failure_reason_allowlist') AND NOT (v_payload ? 'failure_reason_groups') THEN
      v_failure_reason_allowlist := COALESCE(v_failure_reason_allowlist, '[]'::jsonb);
    END IF;
  END IF;

  INSERT INTO public.banking_alert_user_preferences AS preference_target (
    user_id,
    enabled,
    alert_kind_allowlist,
    alert_kind_blocklist,
    failure_reason_allowlist,
    failure_reason_blocklist,
    include_action_required,
    include_progress_alerts,
    include_informational_alerts,
    include_success_alerts,
    severity_min,
    muted_provider_keys,
    muted_pay_batch_ids,
    snoozed_until_utc,
    created_at_utc,
    updated_at_utc
  )
  VALUES (
    p_user_id,
    v_enabled,
    v_alert_kind_allowlist,
    v_alert_kind_blocklist,
    v_failure_reason_allowlist,
    v_failure_reason_blocklist,
    v_include_action_required,
    v_include_progress_alerts,
    v_include_informational_alerts,
    v_include_success_alerts,
    v_severity_min,
    v_muted_provider_keys,
    v_muted_pay_batch_ids,
    v_snoozed_until_utc,
    now(),
    now()
  )
  ON CONFLICT (user_id) DO UPDATE
  SET
    enabled = EXCLUDED.enabled,
    alert_kind_allowlist = EXCLUDED.alert_kind_allowlist,
    alert_kind_blocklist = EXCLUDED.alert_kind_blocklist,
    failure_reason_allowlist = EXCLUDED.failure_reason_allowlist,
    failure_reason_blocklist = EXCLUDED.failure_reason_blocklist,
    include_action_required = EXCLUDED.include_action_required,
    include_progress_alerts = EXCLUDED.include_progress_alerts,
    include_informational_alerts = EXCLUDED.include_informational_alerts,
    include_success_alerts = EXCLUDED.include_success_alerts,
    severity_min = EXCLUDED.severity_min,
    muted_provider_keys = EXCLUDED.muted_provider_keys,
    muted_pay_batch_ids = EXCLUDED.muted_pay_batch_ids,
    snoozed_until_utc = EXCLUDED.snoozed_until_utc,
    updated_at_utc = now()
  RETURNING id
  INTO v_preference_id;

  v_result := public.banking_alert_preferences_get(p_user_id);

  RETURN v_result || jsonb_build_object(
    'updated', true,
    'preference_id', v_preference_id::text,
    'nav_refresh_recommended', true
  );
END;
$function$;





CREATE OR REPLACE FUNCTION public._banking_alert_user_filter_allows(p_user_id uuid, p_alert_payload jsonb)
RETURNS boolean
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_payload jsonb := '{}'::jsonb;
  v_preferences jsonb := '{}'::jsonb;
  v_enabled boolean := true;
  v_alert_kind text := NULL::text;
  v_failure_reason_group text := NULL::text;
  v_provider_key text := NULL::text;
  v_pay_batch_id_text text := NULL::text;
  v_severity text := NULL::text;
  v_severity_min text := 'ACTION_REQUIRED';
  v_snoozed_until_utc timestamptz := NULL::timestamptz;
  v_success_only boolean := false;
  v_is_progress boolean := false;
  v_is_informational boolean := false;
  v_is_action_required boolean := false;
  v_allowlist_contains boolean := false;
  v_blocklist_contains boolean := false;
  v_reason_allowlist_contains boolean := false;
  v_reason_blocklist_contains boolean := false;
  v_provider_muted boolean := false;
  v_batch_muted boolean := false;
  v_payload_lifecycle text := NULL::text;
  v_required_user_action text := NULL::text;
BEGIN
  IF p_user_id IS NULL THEN
    RETURN false;
  END IF;

  IF p_alert_payload IS NULL OR COALESCE(jsonb_typeof(p_alert_payload), 'null') <> 'object' THEN
    RETURN false;
  END IF;

  v_payload := p_alert_payload;
  v_preferences := public.banking_alert_preferences_get(p_user_id);

  v_enabled := COALESCE((v_preferences ->> 'enabled')::boolean, true);
  IF v_enabled IS NOT TRUE THEN
    RETURN false;
  END IF;

  v_alert_kind := UPPER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'alert_kind',
    v_payload ->> 'kind',
    v_payload ->> 'alert_type',
    ''
  )), ''));

  v_failure_reason_group := UPPER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'provider_failure_reason_group',
    v_payload ->> 'failure_reason_group',
    v_payload ->> 'reason_group',
    ''
  )), ''));

  v_provider_key := UPPER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'provider_key',
    v_payload ->> 'provider',
    v_payload ->> 'rail_provider',
    v_payload ->> 'rail',
    ''
  )), ''));

  v_pay_batch_id_text := LOWER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'pay_batch_id',
    v_payload ->> 'entity_id',
    ''
  )), ''));

  v_required_user_action := UPPER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'required_user_action',
    v_payload ->> 'recommended_action',
    ''
  )), ''));

  v_payload_lifecycle := UPPER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'payment_lifecycle_state',
    v_payload ->> 'current_status',
    v_payload ->> 'status',
    ''
  )), ''));

  v_severity := UPPER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'alert_severity',
    v_payload ->> 'severity',
    ''
  )), ''));

  IF v_alert_kind IS NULL AND v_required_user_action IS NULL AND v_failure_reason_group IS NULL THEN
    RETURN false;
  END IF;

  IF v_alert_kind IN (
    'BATCH_SCHEDULED_SUCCESS',
    'BATCH_SETTLED_SUCCESS',
    'PAYMENT_COMPLETED',
    'PAYMENT_SUCCESS',
    'PAYMENT_SUCCEEDED',
    'PROVIDER_PAYMENT_COMPLETED',
    'PROVIDER_PAYMENT_SUCCESS',
    'SETTLEMENT_COMPLETED',
    'BANK_PAYMENT_SUCCESS',
    'SUCCESS',
    'COMPLETED'
  ) THEN
    v_success_only := true;
  END IF;

  IF v_severity IS NULL THEN
    IF v_alert_kind IN ('AUTO_UNWIND_PROGRESS', 'WHOLE_BATCH_CANCELLATION_PROGRESS') THEN
      v_severity := 'PROGRESS';
    ELSIF v_alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN
      v_severity := 'INFO';
    ELSE
      v_severity := 'ACTION_REQUIRED';
    END IF;
  END IF;

  v_severity_min := UPPER(NULLIF(BTRIM(COALESCE(v_preferences ->> 'severity_min', 'ACTION_REQUIRED')), ''));
  IF v_severity_min IS NULL OR v_severity_min NOT IN ('INFO','PROGRESS','ACTION_REQUIRED','CRITICAL') THEN
    v_severity_min := 'ACTION_REQUIRED';
  END IF;

  v_success_only := v_success_only OR lower(BTRIM(COALESCE(
    v_payload ->> 'alert_candidate_is_success_only',
    v_payload ->> 'is_success_only',
    v_payload ->> 'success_only',
    'false'
  ))) IN ('true','t','1','yes','y','on');

  IF v_success_only IS NOT TRUE
     AND v_alert_kind IS NULL
     AND v_required_user_action IS NULL
     AND v_payload_lifecycle IN ('PAID', 'PAID_OR_SETTLED', 'SETTLED', 'COMPLETED', 'SUCCESS', 'SUCCEEDED') THEN
    v_success_only := true;
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_preferences ->> 'snoozed_until_utc', '')), '') IS NOT NULL THEN
    BEGIN
      v_snoozed_until_utc := (v_preferences ->> 'snoozed_until_utc')::timestamptz;
    EXCEPTION WHEN OTHERS THEN
      v_snoozed_until_utc := NULL::timestamptz;
    END;
  END IF;

  IF v_snoozed_until_utc IS NOT NULL AND v_snoozed_until_utc > now() THEN
    RETURN false;
  END IF;

  IF COALESCE(jsonb_typeof(v_preferences -> 'alert_kind_blocklist'), 'null') = 'array' AND v_alert_kind IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_preferences -> 'alert_kind_blocklist') AS blocked_alert_kind(value)
      WHERE UPPER(BTRIM(blocked_alert_kind.value)) = v_alert_kind
    )
    INTO v_blocklist_contains;

    IF v_blocklist_contains THEN
      RETURN false;
    END IF;
  END IF;

  IF COALESCE(jsonb_typeof(v_preferences -> 'alert_kind_allowlist'), 'null') = 'array' THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_preferences -> 'alert_kind_allowlist') AS allowed_alert_kind(value)
      WHERE UPPER(BTRIM(allowed_alert_kind.value)) = v_alert_kind
    )
    INTO v_allowlist_contains;

    IF v_allowlist_contains IS NOT TRUE THEN
      RETURN false;
    END IF;
  END IF;

  IF v_success_only IS NOT TRUE
     AND COALESCE(jsonb_typeof(v_preferences -> 'failure_reason_blocklist'), 'null') = 'array'
     AND v_failure_reason_group IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_preferences -> 'failure_reason_blocklist') AS blocked_reason(value)
      WHERE UPPER(BTRIM(blocked_reason.value)) = v_failure_reason_group
    )
    INTO v_reason_blocklist_contains;

    IF v_reason_blocklist_contains THEN
      RETURN false;
    END IF;
  END IF;

  IF v_success_only IS NOT TRUE
     AND COALESCE(jsonb_typeof(v_preferences -> 'failure_reason_allowlist'), 'null') = 'array' THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_preferences -> 'failure_reason_allowlist') AS allowed_reason(value)
      WHERE UPPER(BTRIM(allowed_reason.value)) = v_failure_reason_group
    )
    INTO v_reason_allowlist_contains;

    IF v_reason_allowlist_contains IS NOT TRUE THEN
      RETURN false;
    END IF;
  END IF;

  IF COALESCE(jsonb_typeof(v_preferences -> 'muted_provider_keys'), 'null') = 'array' AND v_provider_key IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_preferences -> 'muted_provider_keys') AS muted_provider(value)
      WHERE UPPER(BTRIM(muted_provider.value)) = v_provider_key
    )
    INTO v_provider_muted;

    IF v_provider_muted THEN
      RETURN false;
    END IF;
  END IF;

  IF COALESCE(jsonb_typeof(v_preferences -> 'muted_pay_batch_ids'), 'null') = 'array' AND v_pay_batch_id_text IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_preferences -> 'muted_pay_batch_ids') AS muted_batch(value)
      WHERE LOWER(BTRIM(muted_batch.value)) = v_pay_batch_id_text
    )
    INTO v_batch_muted;

  IF v_batch_muted THEN
      RETURN false;
    END IF;
  END IF;

  IF v_success_only THEN
    RETURN COALESCE((v_preferences ->> 'include_success_alerts')::boolean, true);
  END IF;

  v_is_progress := v_alert_kind IN ('AUTO_UNWIND_PROGRESS', 'WHOLE_BATCH_CANCELLATION_PROGRESS') OR v_severity = 'PROGRESS';
  v_is_informational := v_alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' OR v_severity = 'INFO';
  v_is_action_required := v_severity IN ('ACTION_REQUIRED', 'CRITICAL') OR v_required_user_action IS NOT NULL;

  IF v_is_progress AND COALESCE((v_preferences ->> 'include_progress_alerts')::boolean, true) IS NOT TRUE THEN
    RETURN false;
  END IF;

  IF v_is_informational AND COALESCE((v_preferences ->> 'include_informational_alerts')::boolean, false) IS NOT TRUE THEN
    RETURN false;
  END IF;

  IF v_is_action_required AND COALESCE((v_preferences ->> 'include_action_required')::boolean, true) IS NOT TRUE THEN
    RETURN false;
  END IF;

  IF v_is_progress OR v_is_informational THEN
    RETURN true;
  END IF;

  IF v_severity_min = 'CRITICAL' AND v_severity <> 'CRITICAL' THEN
    RETURN false;
  ELSIF v_severity_min = 'ACTION_REQUIRED' AND v_severity NOT IN ('ACTION_REQUIRED', 'CRITICAL') THEN
    RETURN false;
  ELSIF v_severity_min = 'PROGRESS' AND v_severity NOT IN ('PROGRESS', 'ACTION_REQUIRED', 'CRITICAL') THEN
    RETURN false;
  END IF;

  RETURN true;
END;
$function$;

CREATE OR REPLACE FUNCTION public.banking_alert_display_summary_refresh_for_user(
  p_actor_user_id uuid,
  p_context text DEFAULT 'EXPLICIT_ALERT_REFRESH'::text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_context text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_context, 'EXPLICIT_ALERT_REFRESH')), ''), '-', '_'));
  v_active_json jsonb := '{}'::jsonb;
  v_alert_hash text := NULL::text;
  v_summary_hash text := NULL::text;
  v_unacknowledged_count integer := 0;
  v_highest_severity text := NULL::text;
  v_highest_label text := NULL::text;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_DISPLAY_SUMMARY_REFRESH_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANKING_ALERT_DISPLAY_SUMMARY_REFRESH_ACTOR_REQUIRED'
            )::text;
  END IF;

  IF v_context IN (
    'BATCH_LIST',
    'PAY_BATCH_LIST',
    'LIST',
    'BOOTSTRAP',
    'BATCH_BOOTSTRAP',
    'PAY_BATCH_BOOTSTRAP',
    'DISPLAY',
    'BATCH_OPEN',
    'MODAL_OPEN',
    'PAY_BATCH_GET',
    'PAY_BATCH_GET_BOOTSTRAP_ONLY',
    'SECTION_PAGE',
    'OVERVIEW',
    'OVERVIEW_PAGE',
    'OPERATION_PROGRESS',
    'PREVIEW_PROGRESS',
    'WATCH_SIGNAL',
    'LIVE_WATCH',
    'RPC_CHANGES_PING',
    'CHANGES_PING'
  ) THEN
    RAISE EXCEPTION 'BANKING_ALERT_DISPLAY_SUMMARY_REFRESH_CONTEXT_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANKING_ALERT_DISPLAY_SUMMARY_REFRESH_CONTEXT_NOT_ALLOWED',
              'context', v_context
            )::text;
  END IF;

  PERFORM public.banking_pay_hot_path_budget_apply(
    CASE WHEN v_context = 'ALERT_REFRESH_JOB' THEN 'ALERT_REFRESH_JOB' ELSE 'EXPLICIT_DIAGNOSTIC' END
  );

  v_active_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    100,
    v_context
  );

  v_alert_hash := COALESCE(
    NULLIF(BTRIM(COALESCE(v_active_json ->> 'banking_alert_hash', '')), ''),
    'banking_alert_signal:v3:' || MD5('')
  );

  v_summary_hash := COALESCE(
    NULLIF(BTRIM(COALESCE(v_active_json ->> 'banking_alert_summary_signature', '')), ''),
    'banking_alert_summary:v3:' || MD5('')
  );

  BEGIN
    v_unacknowledged_count := COALESCE((v_active_json ->> 'banking_unacknowledged_alert_count')::integer, 0);
  EXCEPTION
    WHEN OTHERS THEN
      v_unacknowledged_count := 0;
  END;

  v_highest_severity := NULLIF(BTRIM(COALESCE(v_active_json ->> 'banking_highest_alert_severity', v_active_json ->> 'highest_severity', '')), '');
  v_highest_label := NULLIF(BTRIM(COALESCE(v_active_json ->> 'banking_highest_alert_label', v_active_json ->> 'highest_label', '')), '');

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
    v_alert_hash,
    v_summary_hash,
    v_unacknowledged_count,
    v_highest_severity,
    v_highest_label,
    v_active_json,
    now()
  )
  ON CONFLICT (actor_user_id) DO UPDATE
  SET
    alert_hash = EXCLUDED.alert_hash,
    summary_hash = EXCLUDED.summary_hash,
    unacknowledged_count = EXCLUDED.unacknowledged_count,
    highest_severity = EXCLUDED.highest_severity,
    highest_label = EXCLUDED.highest_label,
    summary_json = EXCLUDED.summary_json,
    updated_at_utc = now();
END;
$function$;

-- Alert RPCs are Worker/service-role only. They accept an actor id and use
-- SECURITY DEFINER, so direct browser roles must not be able to impersonate a
-- different user through PostgREST.
REVOKE ALL ON FUNCTION public.banking_alert_success_event_capture_pay_batch() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.banking_alerts_active_for_user(uuid, text, uuid, boolean, integer, text) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.banking_alerts_refresh_for_user(uuid, text, integer) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.banking_alert_acknowledge(text, text, text, uuid, uuid, text, jsonb) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.banking_alert_acknowledge_many(uuid, jsonb, text) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.banking_alert_signal_for_user(uuid, text, text) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.banking_alert_acknowledge_all_current(uuid, text) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.banking_alert_preferences_get(uuid) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.banking_alert_preferences_update(uuid, jsonb) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public._banking_alert_user_filter_allows(uuid, jsonb) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.banking_alert_display_summary_refresh_for_user(uuid, text) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.banking_alerts_active_for_user(uuid, text, uuid, boolean, integer, text) TO service_role;
GRANT EXECUTE ON FUNCTION public.banking_alerts_refresh_for_user(uuid, text, integer) TO service_role;
GRANT EXECUTE ON FUNCTION public.banking_alert_acknowledge(text, text, text, uuid, uuid, text, jsonb) TO service_role;
GRANT EXECUTE ON FUNCTION public.banking_alert_acknowledge_many(uuid, jsonb, text) TO service_role;
GRANT EXECUTE ON FUNCTION public.banking_alert_signal_for_user(uuid, text, text) TO service_role;
GRANT EXECUTE ON FUNCTION public.banking_alert_acknowledge_all_current(uuid, text) TO service_role;
GRANT EXECUTE ON FUNCTION public.banking_alert_preferences_get(uuid) TO service_role;
GRANT EXECUTE ON FUNCTION public.banking_alert_preferences_update(uuid, jsonb) TO service_role;
GRANT EXECUTE ON FUNCTION public.banking_alert_display_summary_refresh_for_user(uuid, text) TO service_role;
