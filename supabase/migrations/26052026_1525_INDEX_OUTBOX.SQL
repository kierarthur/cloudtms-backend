DO $migration$
DECLARE
  v_duplicate_key_count integer := 0;
  v_duplicate_key_samples jsonb := '[]'::jsonb;
BEGIN
  SELECT COUNT(*)::integer
  INTO v_duplicate_key_count
  FROM (
    SELECT mail_outbox_row.deterministic_outbox_key
    FROM public.mail_outbox AS mail_outbox_row
    WHERE mail_outbox_row.deterministic_outbox_key IS NOT NULL
    GROUP BY mail_outbox_row.deterministic_outbox_key
    HAVING COUNT(*) > 1
  ) AS duplicate_key_count_rows;

  SELECT COALESCE(
           jsonb_agg(duplicate_key_samples.deterministic_outbox_key ORDER BY duplicate_key_samples.deterministic_outbox_key),
           '[]'::jsonb
         )
  INTO v_duplicate_key_samples
  FROM (
    SELECT mail_outbox_row.deterministic_outbox_key
    FROM public.mail_outbox AS mail_outbox_row
    WHERE mail_outbox_row.deterministic_outbox_key IS NOT NULL
    GROUP BY mail_outbox_row.deterministic_outbox_key
    HAVING COUNT(*) > 1
    ORDER BY mail_outbox_row.deterministic_outbox_key
    LIMIT 25
  ) AS duplicate_key_samples;

  IF COALESCE(v_duplicate_key_count, 0) > 0 THEN
    RAISE EXCEPTION 'MAIL_OUTBOX_DETERMINISTIC_OUTBOX_KEY_DUPLICATES_EXIST'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'MAIL_OUTBOX_DETERMINISTIC_OUTBOX_KEY_DUPLICATES_EXIST',
              'duplicate_key_count', v_duplicate_key_count,
              'duplicate_key_samples', v_duplicate_key_samples
            )::text;
  END IF;
END;
$migration$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mail_outbox_deterministic_outbox_key_full
ON public.mail_outbox USING btree (deterministic_outbox_key);

CREATE INDEX IF NOT EXISTS idx_pay_bank_transfers_rail_tx_id_not_null
ON public.pay_bank_transfers USING btree (rail_tx_id)
WHERE rail_tx_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_banking_pay_operation_transfer_scope_operation_batch_transfer
ON public.banking_pay_operation_transfer_scope USING btree (operation_id, pay_batch_id, pay_bank_transfer_id)
WHERE pay_bank_transfer_id IS NOT NULL;
