-- Enforce a single shared OPEN banking pay workbench session globally.
-- Safe to rerun.
--
-- Verified against current schema:
--   public.banking_pay_workbench_sessions
--     id uuid primary key
--     status text not null default 'OPEN'
--     discarded_at_utc timestamptz null
--     updated_at_utc timestamptz not null default now()
--
-- Verified against current discard RPC:
--   public.pay_workbench_session_discard(...)
--     sets status = 'DISCARDED'
--     sets discarded_at_utc = coalesce(existing, now())
--
-- This migration:
--   1) deterministically keeps one canonical OPEN row, if any
--   2) marks all other OPEN rows as DISCARDED
--   3) adds a global partial unique index for status = 'OPEN'

BEGIN;

LOCK TABLE public.banking_pay_workbench_sessions IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
  v_keep_id uuid;
BEGIN
  /*
    Keep one canonical OPEN row if multiple exist.
    Deterministic choice:
      - latest updated_at_utc
      - then latest created_at_utc
      - then highest id
  */
  SELECT ws.id
  INTO v_keep_id
  FROM public.banking_pay_workbench_sessions AS ws
  WHERE ws.status = 'OPEN'
  ORDER BY
    ws.updated_at_utc DESC NULLS LAST,
    ws.created_at_utc DESC NULLS LAST,
    ws.id DESC
  LIMIT 1;

  /*
    Discard every other OPEN row.
    This matches the installed discard status model:
      status = 'DISCARDED'
      discarded_at_utc set if null
      updated_at_utc refreshed
  */
  IF v_keep_id IS NOT NULL THEN
    UPDATE public.banking_pay_workbench_sessions AS ws
    SET status = 'DISCARDED',
        discarded_at_utc = COALESCE(ws.discarded_at_utc, now()),
        updated_at_utc = now()
    WHERE ws.status = 'OPEN'
      AND ws.id <> v_keep_id;
  END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_workbench_sessions_single_open
  ON public.banking_pay_workbench_sessions ((true))
  WHERE status = 'OPEN';

COMMIT;
