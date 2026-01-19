CREATE OR REPLACE FUNCTION public.enqueue_ts_financials(_timesheet_id uuid, _reason ts_fin_reason_enum)
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO public.ts_financials_outbox (timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
  VALUES (_timesheet_id, _reason, 0, NULL, NULL, now())
  ON CONFLICT (timesheet_id, reason)
  DO UPDATE SET
      attempt_count   = 0,
      next_attempt_at = NULL,
      last_error      = NULL,
      created_at      = now();
END;
$function$;

CREATE OR REPLACE FUNCTION public.tsfin_prepare_write(p_timesheet_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  -- Guard: if current snapshot is locked by an invoice, refuse writes
  IF EXISTS (
    SELECT 1 FROM public.timesheets_financials
    WHERE timesheet_id = p_timesheet_id
      AND is_current = true
      AND locked_by_invoice_id IS NOT NULL
  ) THEN
    RAISE EXCEPTION 'TSFIN_LOCKED';
  END IF;

  -- Make any current snapshots non-current (we keep history)
  UPDATE public.timesheets_financials
  SET is_current = false
  WHERE timesheet_id = p_timesheet_id
    AND is_current = true;
END;
$function$;

CREATE OR REPLACE FUNCTION public.tsfin_work_success(p_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  DELETE FROM public.ts_financials_outbox WHERE id = p_id;
END;
$function$;

CREATE OR REPLACE FUNCTION public.tsfin_work_fail(p_id uuid, p_error text)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
  v_attempt int;
  v_delay_minutes int;
BEGIN
  UPDATE public.ts_financials_outbox
  SET attempt_count = attempt_count + 1,
      last_error    = p_error
  WHERE id = p_id
  RETURNING attempt_count INTO v_attempt;

  v_delay_minutes := LEAST(60, GREATEST(1, (2 ^ GREATEST(0, v_attempt - 1))));
  UPDATE public.ts_financials_outbox
  SET next_attempt_at = now() + (v_delay_minutes || ' minutes')::interval
  WHERE id = p_id;
END;
$function$;

CREATE OR REPLACE FUNCTION public.tsfin_mark_revoked(p_timesheet_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  UPDATE public.timesheets_financials
  SET is_current = false
  WHERE timesheet_id = p_timesheet_id
    AND is_current = true;
END;
$function$;

