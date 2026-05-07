BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.banking_alert_acknowledgements AS baa
    LEFT JOIN public.tms_users AS tu
      ON tu.id = baa.acknowledged_by_user_id
    WHERE tu.id IS NULL
  ) THEN
    RAISE EXCEPTION 'Cannot change FK: banking_alert_acknowledgements contains acknowledged_by_user_id values not present in public.tms_users';
  END IF;
END $$;

ALTER TABLE public.banking_alert_acknowledgements
DROP CONSTRAINT IF EXISTS banking_alert_acknowledgements_ack_user_fkey;

ALTER TABLE public.banking_alert_acknowledgements
ADD CONSTRAINT banking_alert_acknowledgements_ack_user_fkey
FOREIGN KEY (acknowledged_by_user_id)
REFERENCES public.tms_users(id)
ON DELETE CASCADE;

COMMIT;
