BEGIN;

LOCK TABLE public.banking_pay_workbench_sessions IN ACCESS EXCLUSIVE MODE;

-- 1) Deduplicate existing OPEN sessions by shared Banking context.
--    Keep the most recently updated OPEN row per session_signature.
--    Discard older OPEN duplicates without merging their child decision rows.
WITH ranked_open AS (
  SELECT
    ws.id,
    ws.session_signature,
    ROW_NUMBER() OVER (
      PARTITION BY ws.session_signature
      ORDER BY
        ws.updated_at_utc DESC NULLS LAST,
        ws.created_at_utc DESC NULLS LAST,
        ws.id DESC
    ) AS rn
  FROM public.banking_pay_workbench_sessions AS ws
  WHERE ws.status = 'OPEN'
),
to_discard AS (
  SELECT ro.id
  FROM ranked_open AS ro
  WHERE ro.rn > 1
)
UPDATE public.banking_pay_workbench_sessions AS ws
SET
  status = 'DISCARDED',
  discarded_at_utc = COALESCE(ws.discarded_at_utc, NOW()),
  updated_at_utc = NOW()
WHERE ws.id IN (SELECT td.id FROM to_discard AS td)
  AND ws.status = 'OPEN';

-- 2) Remove the old actor-scoped OPEN uniqueness rule if it still exists.
DROP INDEX IF EXISTS public.ux_bpay_workbench_sessions_actor_signature_open;

-- 3) Create the new shared OPEN uniqueness rule.
CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_workbench_sessions_signature_open
ON public.banking_pay_workbench_sessions USING btree (session_signature)
WHERE (status = 'OPEN'::text);

COMMIT;
