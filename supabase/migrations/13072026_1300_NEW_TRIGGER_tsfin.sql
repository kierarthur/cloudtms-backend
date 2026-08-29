CREATE OR REPLACE FUNCTION public.trg_tsfin_candidates_wakeup()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_enqueue_current_financials boolean := false;
  v_enqueue_alias_matches boolean := false;
BEGIN
  -- A candidate PAYE↔UMBRELLA status switch is prospective-only.
  -- Banking Pay is refreshed through the exact Workbench dirty-event path;
  -- existing Timesheet financial snapshots must not be repriced or rewritten.
  IF TG_OP = 'INSERT' THEN
    v_enqueue_current_financials := true;
    v_enqueue_alias_matches := true;
  ELSIF TG_OP = 'UPDATE' THEN
    v_enqueue_current_financials := NEW.pay_method IS NOT DISTINCT FROM OLD.pay_method;
    v_enqueue_alias_matches := NEW.key_norm IS DISTINCT FROM OLD.key_norm
      OR NEW.nhsp_hr_name_aliases IS DISTINCT FROM OLD.nhsp_hr_name_aliases;
  END IF;

  IF v_enqueue_current_financials IS TRUE THEN
    INSERT INTO public.ts_financials_outbox (timesheet_id, reason)
    SELECT financial_row.timesheet_id, 'CONTEXT_CHANGED'::public.ts_fin_reason_enum
    FROM public.timesheets_financials AS financial_row
    WHERE financial_row.candidate_id = NEW.id
      AND financial_row.is_current IS TRUE
      AND financial_row.locked_by_invoice_id IS NULL
      AND financial_row.paid_at_utc IS NULL
    ON CONFLICT (timesheet_id, reason)
    DO UPDATE SET next_attempt_at = NULL;
  END IF;

  -- Alias/key changes continue to refresh matching current Timesheets independently.
  IF v_enqueue_alias_matches IS TRUE THEN
    WITH alias_norms AS (
      SELECT LOWER(NEW.key_norm) AS norm
      WHERE NULLIF(BTRIM(COALESCE(NEW.key_norm, '')), '') IS NOT NULL
      UNION
      SELECT LOWER(alias_value.value)
      FROM jsonb_array_elements_text(
        COALESCE(NEW.nhsp_hr_name_aliases, '[]'::jsonb)
      ) AS alias_value(value)
      WHERE NULLIF(BTRIM(alias_value.value), '') IS NOT NULL
    )
    INSERT INTO public.ts_financials_outbox (timesheet_id, reason)
    SELECT DISTINCT current_timesheet.timesheet_id, 'CONTEXT_CHANGED'::public.ts_fin_reason_enum
    FROM public.timesheets AS current_timesheet
    JOIN alias_norms AS alias_norm
      ON current_timesheet.occupant_key_norm = alias_norm.norm
    LEFT JOIN public.timesheets_financials AS current_financial
      ON current_financial.timesheet_id = current_timesheet.timesheet_id
     AND current_financial.is_current IS TRUE
    WHERE current_timesheet.is_current IS TRUE
      AND current_timesheet.revoked_at IS NULL
      AND (
        current_financial.timesheet_id IS NULL
        OR (
          current_financial.locked_by_invoice_id IS NULL
          AND current_financial.paid_at_utc IS NULL
        )
      )
    ON CONFLICT (timesheet_id, reason)
    DO UPDATE SET next_attempt_at = NULL;
  END IF;

  RETURN NEW;
END;
$function$;

REVOKE ALL ON FUNCTION public.trg_tsfin_candidates_wakeup() FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.trg_tsfin_candidates_wakeup() TO service_role;
