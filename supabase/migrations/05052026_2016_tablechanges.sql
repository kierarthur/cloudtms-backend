BEGIN;

ALTER TABLE public.pay_payment_return_notice_groups
    ADD COLUMN IF NOT EXISTS alert_fingerprint text NULL;

UPDATE public.pay_payment_return_notice_groups AS pprng
SET alert_fingerprint = NULL
WHERE pprng.alert_fingerprint IS NOT NULL
  AND BTRIM(pprng.alert_fingerprint) = '';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS c
        WHERE c.conrelid = 'public.pay_payment_return_notice_groups'::regclass
          AND c.conname = 'pay_payment_return_notice_groups_alert_fingerprint_chk'
    ) THEN
        ALTER TABLE public.pay_payment_return_notice_groups
            ADD CONSTRAINT pay_payment_return_notice_groups_alert_fingerprint_chk
            CHECK (
                alert_fingerprint IS NULL
                OR BTRIM(alert_fingerprint) <> ''
            );
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_pay_payment_return_notice_groups_notice_kind_alert_fingerprint
ON public.pay_payment_return_notice_groups (
    notice_kind,
    alert_fingerprint
)
WHERE alert_fingerprint IS NOT NULL
  AND BTRIM(alert_fingerprint) <> '';

CREATE INDEX IF NOT EXISTS idx_pay_payment_return_notice_groups_alert_fingerprint
ON public.pay_payment_return_notice_groups (
    alert_fingerprint
)
WHERE alert_fingerprint IS NOT NULL
  AND BTRIM(alert_fingerprint) <> '';

COMMENT ON COLUMN public.pay_payment_return_notice_groups.alert_fingerprint IS
'Optional durable Banking alert fingerprint used to dedupe admin notice groups for exact alert states, such as BLOCKED_FUNDS funds-check failures. This is notice grouping metadata only and does not represent payment resolution.';

COMMIT;
