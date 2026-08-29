CREATE OR REPLACE FUNCTION public.pay_workbench_mark_candidate_dirty()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_trigger_table text := lower(TG_TABLE_NAME);
  v_new_row jsonb := '{}'::jsonb;
  v_old_row jsonb := '{}'::jsonb;
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_id uuid;
  v_live_change_seq bigint := 0;
  v_reason text;
  v_payload_json jsonb;
  v_dedupe_key text;
  v_old_timesheet_id uuid := NULL::uuid;
  v_new_timesheet_id uuid := NULL::uuid;
  v_old_contract_id uuid := NULL::uuid;
  v_new_contract_id uuid := NULL::uuid;
  v_old_booking_id uuid := NULL::uuid;
  v_new_booking_id uuid := NULL::uuid;
  v_refresh_scope_kind text := NULL;
  v_targeted_timesheet_uuid_ids uuid[] := ARRAY[]::uuid[];
  v_linked_timesheet_uuid_ids uuid[] := ARRAY[]::uuid[];
  v_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_should_dirty boolean := false;
  v_old_authorised boolean := false;
  v_new_authorised boolean := false;
  v_old_has_settled boolean := false;
  v_new_has_settled boolean := false;
  v_old_has_linked_banking_effect boolean := false;
  v_new_has_linked_banking_effect boolean := false;
  v_tsfin_gate_changed boolean := false;
BEGIN
  IF TG_OP <> 'DELETE' THEN
    v_new_row := to_jsonb(NEW);
  END IF;

  IF TG_OP <> 'INSERT' THEN
    v_old_row := to_jsonb(OLD);
  END IF;

  IF v_trigger_table = 'timesheets' THEN
    v_old_timesheet_id := NULLIF(BTRIM(COALESCE(v_old_row->>'timesheet_id', '')), '')::uuid;
    v_new_timesheet_id := NULLIF(BTRIM(COALESCE(v_new_row->>'timesheet_id', '')), '')::uuid;
    v_old_contract_id := NULLIF(BTRIM(COALESCE(v_old_row->>'contract_id', '')), '')::uuid;
    v_new_contract_id := NULLIF(BTRIM(COALESCE(v_new_row->>'contract_id', '')), '')::uuid;
    v_old_booking_id := NULLIF(BTRIM(COALESCE(v_old_row->>'booking_id', '')), '')::uuid;
    v_new_booking_id := NULLIF(BTRIM(COALESCE(v_new_row->>'booking_id', '')), '')::uuid;

    SELECT array_cat(
             array_cat(
               ARRAY[
                 (
                   SELECT ct.candidate_id
                   FROM public.contracts AS ct
                   WHERE ct.id = v_old_contract_id
                   LIMIT 1
                 ),
                 (
                   SELECT ct.candidate_id
                   FROM public.contracts AS ct
                   WHERE ct.id = v_new_contract_id
                   LIMIT 1
                 )
               ]::uuid[],
               ARRAY[
                 (
                   SELECT tf.candidate_id
                   FROM public.timesheets_financials AS tf
                   WHERE tf.timesheet_id = v_old_timesheet_id
                     AND tf.is_current = true
                   ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                   LIMIT 1
                 ),
                 (
                   SELECT tf.candidate_id
                   FROM public.timesheets_financials AS tf
                   WHERE tf.timesheet_id = v_new_timesheet_id
                     AND tf.is_current = true
                   ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                   LIMIT 1
                 )
               ]::uuid[]
             ),
             ARRAY[]::uuid[]
           )
    INTO v_candidate_ids;

    v_old_authorised := (NULLIF(BTRIM(COALESCE(v_old_row->>'authorised_at_server', '')), '') IS NOT NULL)
                        AND (NULLIF(BTRIM(COALESCE(v_old_row->>'revoked_at', '')), '') IS NULL);
    v_new_authorised := (NULLIF(BTRIM(COALESCE(v_new_row->>'authorised_at_server', '')), '') IS NOT NULL)
                        AND (NULLIF(BTRIM(COALESCE(v_new_row->>'revoked_at', '')), '') IS NULL);

    SELECT EXISTS (
      SELECT 1
      FROM public.timesheet_pay_state AS tps
      WHERE tps.timesheet_id = v_old_timesheet_id
        AND (
          tps.last_settled_snapshot_json IS NOT NULL
          OR tps.last_settled_signature IS NOT NULL
          OR tps.last_settled_pay_batch_id IS NOT NULL
          OR tps.last_settled_at_utc IS NOT NULL
        )
    )
    INTO v_old_has_settled;

    SELECT EXISTS (
      SELECT 1
      FROM public.timesheet_pay_state AS tps
      WHERE tps.timesheet_id = v_new_timesheet_id
        AND (
          tps.last_settled_snapshot_json IS NOT NULL
          OR tps.last_settled_signature IS NOT NULL
          OR tps.last_settled_pay_batch_id IS NOT NULL
          OR tps.last_settled_at_utc IS NOT NULL
        )
    )
    INTO v_new_has_settled;

    SELECT EXISTS (
      SELECT 1
      FROM public.timesheet_payment_overrides AS tpo
      WHERE tpo.timesheet_id = v_old_timesheet_id
        AND tpo.cleared_at_utc IS NULL
        AND tpo.consumed_at_utc IS NULL
        AND tpo.consumed_by_pay_batch_id IS NULL
    )
    OR EXISTS (
      SELECT 1
      FROM public.pay_item_snoozes AS pis
      WHERE pis.cleared_at_utc IS NULL
        AND (
          pis.timesheet_id = v_old_timesheet_id
          OR (v_old_booking_id IS NOT NULL AND pis.booking_id = v_old_booking_id)
        )
    )
    OR EXISTS (
      SELECT 1
      FROM public.pay_advances AS pa
      WHERE pa.linked_timesheet_id = v_old_timesheet_id
    )
    OR EXISTS (
      SELECT 1
      FROM public.ts_pay_adjustments AS tpa
      WHERE tpa.timesheet_id = v_old_timesheet_id
    )
    OR EXISTS (
      SELECT 1
      FROM public.timesheets_financials AS tf
      WHERE tf.timesheet_id = v_old_timesheet_id
        AND tf.is_current = true
        AND (
          tf.locked_by_invoice_id IS NOT NULL
          OR tf.paid_at_utc IS NOT NULL
          OR tf.locked_at_utc IS NOT NULL
        )
    )
    INTO v_old_has_linked_banking_effect;

    SELECT EXISTS (
      SELECT 1
      FROM public.timesheet_payment_overrides AS tpo
      WHERE tpo.timesheet_id = v_new_timesheet_id
        AND tpo.cleared_at_utc IS NULL
        AND tpo.consumed_at_utc IS NULL
        AND tpo.consumed_by_pay_batch_id IS NULL
    )
    OR EXISTS (
      SELECT 1
      FROM public.pay_item_snoozes AS pis
      WHERE pis.cleared_at_utc IS NULL
        AND (
          pis.timesheet_id = v_new_timesheet_id
          OR (v_new_booking_id IS NOT NULL AND pis.booking_id = v_new_booking_id)
        )
    )
    OR EXISTS (
      SELECT 1
      FROM public.pay_advances AS pa
      WHERE pa.linked_timesheet_id = v_new_timesheet_id
    )
    OR EXISTS (
      SELECT 1
      FROM public.ts_pay_adjustments AS tpa
      WHERE tpa.timesheet_id = v_new_timesheet_id
    )
    OR EXISTS (
      SELECT 1
      FROM public.timesheets_financials AS tf
      WHERE tf.timesheet_id = v_new_timesheet_id
        AND tf.is_current = true
        AND (
          tf.locked_by_invoice_id IS NOT NULL
          OR tf.paid_at_utc IS NOT NULL
          OR tf.locked_at_utc IS NOT NULL
        )
    )
    INTO v_new_has_linked_banking_effect;

    IF TG_OP = 'DELETE' THEN
      IF v_old_authorised OR v_old_has_settled OR v_old_has_linked_banking_effect THEN
        v_should_dirty := true;
      END IF;
    ELSE
      IF (v_old_authorised IS DISTINCT FROM v_new_authorised)
         OR ((NULLIF(BTRIM(COALESCE(v_old_row->>'revoked_at', '')), '') IS NULL) IS DISTINCT FROM (NULLIF(BTRIM(COALESCE(v_new_row->>'revoked_at', '')), '') IS NULL)) THEN
        v_should_dirty := true;
      END IF;
    END IF;

    IF v_should_dirty THEN
      v_refresh_scope_kind := 'TARGETED_TIMESHEETS';
      v_targeted_timesheet_uuid_ids := array_cat(v_targeted_timesheet_uuid_ids, ARRAY[v_old_timesheet_id, v_new_timesheet_id]::uuid[]);
    END IF;

  ELSIF v_trigger_table = 'timesheets_financials' THEN
    v_old_timesheet_id := NULLIF(BTRIM(COALESCE(v_old_row->>'timesheet_id', '')), '')::uuid;
    v_new_timesheet_id := NULLIF(BTRIM(COALESCE(v_new_row->>'timesheet_id', '')), '')::uuid;

    v_candidate_ids := array_cat(
      v_candidate_ids,
      ARRAY[
        NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid,
        NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid
      ]::uuid[]
    );

    v_tsfin_gate_changed :=
      COALESCE(v_old_row->>'pay_on_hold', '') IS DISTINCT FROM COALESCE(v_new_row->>'pay_on_hold', '')
      OR COALESCE(v_old_row->>'has_rate_issue', '') IS DISTINCT FROM COALESCE(v_new_row->>'has_rate_issue', '')
      OR COALESCE(v_old_row->>'has_pay_channel_issue', '') IS DISTINCT FROM COALESCE(v_new_row->>'has_pay_channel_issue', '')
      OR COALESCE(v_old_row->>'processing_status', '') IS DISTINCT FROM COALESCE(v_new_row->>'processing_status', '')
      OR COALESCE(v_old_row->>'candidate_id', '') IS DISTINCT FROM COALESCE(v_new_row->>'candidate_id', '')
      OR COALESCE(v_old_row->>'client_id', '') IS DISTINCT FROM COALESCE(v_new_row->>'client_id', '')
      OR COALESCE(v_old_row->>'pay_method', '') IS DISTINCT FROM COALESCE(v_new_row->>'pay_method', '')
      OR COALESCE(v_old_row->>'is_current', '') IS DISTINCT FROM COALESCE(v_new_row->>'is_current', '');

    SELECT EXISTS (
      SELECT 1
      FROM public.timesheets AS ts
      WHERE ts.timesheet_id = COALESCE(v_new_timesheet_id, v_old_timesheet_id)
        AND (
          (ts.authorised_at_server IS NOT NULL AND ts.revoked_at IS NULL)
          OR EXISTS (
            SELECT 1
            FROM public.timesheet_pay_state AS tps
            WHERE tps.timesheet_id = ts.timesheet_id
              AND (
                tps.last_settled_snapshot_json IS NOT NULL
                OR tps.last_settled_signature IS NOT NULL
                OR tps.last_settled_pay_batch_id IS NOT NULL
                OR tps.last_settled_at_utc IS NOT NULL
              )
          )
          OR EXISTS (
            SELECT 1
            FROM public.timesheet_payment_overrides AS tpo
            WHERE tpo.timesheet_id = ts.timesheet_id
              AND tpo.cleared_at_utc IS NULL
              AND tpo.consumed_at_utc IS NULL
              AND tpo.consumed_by_pay_batch_id IS NULL
          )
        )
    )
    INTO v_should_dirty;

    v_should_dirty := v_should_dirty AND v_tsfin_gate_changed;

    IF v_should_dirty THEN
      v_refresh_scope_kind := 'TARGETED_TIMESHEETS';
      v_targeted_timesheet_uuid_ids := array_cat(v_targeted_timesheet_uuid_ids, ARRAY[v_old_timesheet_id, v_new_timesheet_id]::uuid[]);
    END IF;

  ELSIF v_trigger_table = 'timesheet_pay_state' THEN
    v_old_timesheet_id := NULLIF(BTRIM(COALESCE(v_old_row->>'timesheet_id', '')), '')::uuid;
    v_new_timesheet_id := NULLIF(BTRIM(COALESCE(v_new_row->>'timesheet_id', '')), '')::uuid;

    SELECT array_cat(
             array_cat(
               ARRAY[
                 (
                   SELECT tf.candidate_id
                   FROM public.timesheets_financials AS tf
                   WHERE tf.timesheet_id = v_old_timesheet_id
                     AND tf.is_current = true
                   ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                   LIMIT 1
                 ),
                 (
                   SELECT tf.candidate_id
                   FROM public.timesheets_financials AS tf
                   WHERE tf.timesheet_id = v_new_timesheet_id
                     AND tf.is_current = true
                   ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                   LIMIT 1
                 )
               ]::uuid[],
               ARRAY[
                 (
                   SELECT ct.candidate_id
                   FROM public.timesheets AS ts
                   JOIN public.contracts AS ct
                     ON ct.id = ts.contract_id
                   WHERE ts.timesheet_id = v_old_timesheet_id
                   LIMIT 1
                 ),
                 (
                   SELECT ct.candidate_id
                   FROM public.timesheets AS ts
                   JOIN public.contracts AS ct
                     ON ct.id = ts.contract_id
                   WHERE ts.timesheet_id = v_new_timesheet_id
                   LIMIT 1
                 )
               ]::uuid[]
             ),
             ARRAY[]::uuid[]
           )
    INTO v_candidate_ids;

    IF COALESCE(v_old_row->>'last_settled_snapshot_json', '') IS DISTINCT FROM COALESCE(v_new_row->>'last_settled_snapshot_json', '')
       OR COALESCE(v_old_row->>'last_settled_signature', '') IS DISTINCT FROM COALESCE(v_new_row->>'last_settled_signature', '')
       OR COALESCE(v_old_row->>'last_settled_pay_batch_id', '') IS DISTINCT FROM COALESCE(v_new_row->>'last_settled_pay_batch_id', '')
       OR COALESCE(v_old_row->>'last_settled_at_utc', '') IS DISTINCT FROM COALESCE(v_new_row->>'last_settled_at_utc', '') THEN
      v_should_dirty := true;
      v_refresh_scope_kind := 'TARGETED_TIMESHEETS';
      v_targeted_timesheet_uuid_ids := array_cat(v_targeted_timesheet_uuid_ids, ARRAY[v_old_timesheet_id, v_new_timesheet_id]::uuid[]);
    END IF;

  ELSIF v_trigger_table = 'timesheet_payment_overrides' THEN
    v_old_timesheet_id := NULLIF(BTRIM(COALESCE(v_old_row->>'timesheet_id', '')), '')::uuid;
    v_new_timesheet_id := NULLIF(BTRIM(COALESCE(v_new_row->>'timesheet_id', '')), '')::uuid;

    SELECT array_cat(
             array_cat(
               ARRAY[
                 NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid,
                 NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid
               ]::uuid[],
               ARRAY[
                 (
                   SELECT tf.candidate_id
                   FROM public.timesheets_financials AS tf
                   WHERE tf.timesheet_id = v_old_timesheet_id
                     AND tf.is_current = true
                   ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                   LIMIT 1
                 ),
                 (
                   SELECT tf.candidate_id
                   FROM public.timesheets_financials AS tf
                   WHERE tf.timesheet_id = v_new_timesheet_id
                     AND tf.is_current = true
                   ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                   LIMIT 1
                 )
               ]::uuid[]
             ),
             ARRAY[]::uuid[]
           )
    INTO v_candidate_ids;

    v_should_dirty := true;
    IF v_old_timesheet_id IS NOT NULL OR v_new_timesheet_id IS NOT NULL THEN
      v_refresh_scope_kind := 'TARGETED_TIMESHEETS';
      v_targeted_timesheet_uuid_ids := array_cat(v_targeted_timesheet_uuid_ids, ARRAY[v_old_timesheet_id, v_new_timesheet_id]::uuid[]);
    ELSE
      v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
    END IF;

  ELSIF v_trigger_table = 'pay_item_snoozes' THEN
    v_old_timesheet_id := NULLIF(BTRIM(COALESCE(v_old_row->>'timesheet_id', '')), '')::uuid;
    v_new_timesheet_id := NULLIF(BTRIM(COALESCE(v_new_row->>'timesheet_id', '')), '')::uuid;
    v_old_booking_id := NULLIF(BTRIM(COALESCE(v_old_row->>'booking_id', '')), '')::uuid;
    v_new_booking_id := NULLIF(BTRIM(COALESCE(v_new_row->>'booking_id', '')), '')::uuid;

    SELECT array_cat(
             array_cat(
               array_cat(
                 ARRAY[
                   NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid,
                   NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid
                 ]::uuid[],
                 ARRAY[
                   (
                     SELECT tf.candidate_id
                     FROM public.timesheets_financials AS tf
                     WHERE tf.timesheet_id = v_old_timesheet_id
                       AND tf.is_current = true
                     ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                     LIMIT 1
                   ),
                   (
                     SELECT tf.candidate_id
                     FROM public.timesheets_financials AS tf
                     WHERE tf.timesheet_id = v_new_timesheet_id
                       AND tf.is_current = true
                     ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                     LIMIT 1
                   )
                 ]::uuid[]
               ),
               ARRAY[
                 (
                   SELECT COALESCE(tf.candidate_id, ct.candidate_id)
                   FROM public.timesheets AS ts
                   LEFT JOIN public.timesheets_financials AS tf
                     ON tf.timesheet_id = ts.timesheet_id
                    AND tf.is_current = true
                   LEFT JOIN public.contracts AS ct
                     ON ct.id = ts.contract_id
                   WHERE ts.booking_id = v_old_booking_id
                   ORDER BY ts.updated_at DESC NULLS LAST, ts.timesheet_id DESC
                   LIMIT 1
                 ),
                 (
                   SELECT COALESCE(tf.candidate_id, ct.candidate_id)
                   FROM public.timesheets AS ts
                   LEFT JOIN public.timesheets_financials AS tf
                     ON tf.timesheet_id = ts.timesheet_id
                    AND tf.is_current = true
                   LEFT JOIN public.contracts AS ct
                     ON ct.id = ts.contract_id
                   WHERE ts.booking_id = v_new_booking_id
                   ORDER BY ts.updated_at DESC NULLS LAST, ts.timesheet_id DESC
                   LIMIT 1
                 )
               ]::uuid[]
             ),
             ARRAY[]::uuid[]
           )
    INTO v_candidate_ids;

    SELECT COALESCE(array_agg(DISTINCT ts.timesheet_id), ARRAY[]::uuid[])
    INTO v_linked_timesheet_uuid_ids
    FROM public.timesheets AS ts
    WHERE (v_old_booking_id IS NOT NULL AND ts.booking_id = v_old_booking_id)
       OR (v_new_booking_id IS NOT NULL AND ts.booking_id = v_new_booking_id);

    v_should_dirty := true;
    IF v_old_timesheet_id IS NOT NULL OR v_new_timesheet_id IS NOT NULL OR COALESCE(array_length(v_linked_timesheet_uuid_ids, 1), 0) > 0 THEN
      v_refresh_scope_kind := 'TARGETED_TIMESHEETS';
      v_targeted_timesheet_uuid_ids := array_cat(v_targeted_timesheet_uuid_ids, ARRAY[v_old_timesheet_id, v_new_timesheet_id]::uuid[]);
    ELSE
      v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
    END IF;

  ELSIF v_trigger_table = 'ts_pay_adjustments' THEN
    v_old_timesheet_id := NULLIF(BTRIM(COALESCE(v_old_row->>'timesheet_id', '')), '')::uuid;
    v_new_timesheet_id := NULLIF(BTRIM(COALESCE(v_new_row->>'timesheet_id', '')), '')::uuid;

    v_candidate_ids := array_cat(
      v_candidate_ids,
      ARRAY[
        NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid,
        NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid
      ]::uuid[]
    );

    v_should_dirty := true;
    IF v_old_timesheet_id IS NOT NULL OR v_new_timesheet_id IS NOT NULL THEN
      v_refresh_scope_kind := 'TARGETED_TIMESHEETS';
      v_targeted_timesheet_uuid_ids := array_cat(v_targeted_timesheet_uuid_ids, ARRAY[v_old_timesheet_id, v_new_timesheet_id]::uuid[]);
    ELSE
      v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
    END IF;

  ELSIF v_trigger_table = 'pay_advances' THEN
    v_old_timesheet_id := NULLIF(BTRIM(COALESCE(v_old_row->>'linked_timesheet_id', '')), '')::uuid;
    v_new_timesheet_id := NULLIF(BTRIM(COALESCE(v_new_row->>'linked_timesheet_id', '')), '')::uuid;

    v_candidate_ids := array_cat(
      v_candidate_ids,
      ARRAY[
        NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid,
        NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid
      ]::uuid[]
    );

    v_should_dirty := true;
    IF v_old_timesheet_id IS NOT NULL OR v_new_timesheet_id IS NOT NULL THEN
      v_refresh_scope_kind := 'TARGETED_TIMESHEETS';
      v_targeted_timesheet_uuid_ids := array_cat(v_targeted_timesheet_uuid_ids, ARRAY[v_old_timesheet_id, v_new_timesheet_id]::uuid[]);
    ELSE
      v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
    END IF;

  ELSIF v_trigger_table IN ('bank_name_checks', 'bank_payee_map') THEN
    SELECT array_cat(
             CASE
               WHEN upper(COALESCE(v_old_row->>'entity_kind', '')) = 'CANDIDATE'
                 THEN ARRAY[NULLIF(BTRIM(COALESCE(v_old_row->>'entity_id', '')), '')::uuid]
               WHEN upper(COALESCE(v_old_row->>'entity_kind', '')) = 'UMBRELLA'
                 THEN COALESCE(
                   (
                     SELECT array_agg(DISTINCT c.id)
                     FROM public.candidates AS c
                     WHERE c.umbrella_id = NULLIF(BTRIM(COALESCE(v_old_row->>'entity_id', '')), '')::uuid
                   ),
                   ARRAY[]::uuid[]
                 )
               ELSE ARRAY[]::uuid[]
             END,
             CASE
               WHEN upper(COALESCE(v_new_row->>'entity_kind', '')) = 'CANDIDATE'
                 THEN ARRAY[NULLIF(BTRIM(COALESCE(v_new_row->>'entity_id', '')), '')::uuid]
               WHEN upper(COALESCE(v_new_row->>'entity_kind', '')) = 'UMBRELLA'
                 THEN COALESCE(
                   (
                     SELECT array_agg(DISTINCT c.id)
                     FROM public.candidates AS c
                     WHERE c.umbrella_id = NULLIF(BTRIM(COALESCE(v_new_row->>'entity_id', '')), '')::uuid
                   ),
                   ARRAY[]::uuid[]
                 )
               ELSE ARRAY[]::uuid[]
             END
           )
    INTO v_candidate_ids;

    v_should_dirty := true;
    v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';

  ELSIF v_trigger_table = 'candidates' THEN
    v_candidate_ids := array_cat(
      v_candidate_ids,
      ARRAY[
        NULLIF(BTRIM(COALESCE(v_old_row->>'id', '')), '')::uuid,
        NULLIF(BTRIM(COALESCE(v_new_row->>'id', '')), '')::uuid
      ]::uuid[]
    );

    IF COALESCE(v_old_row->>'pay_method', '') IS DISTINCT FROM COALESCE(v_new_row->>'pay_method', '')
       OR COALESCE(v_old_row->>'umbrella_id', '') IS DISTINCT FROM COALESCE(v_new_row->>'umbrella_id', '')
       OR COALESCE(v_old_row->>'bank_details_hash', '') IS DISTINCT FROM COALESCE(v_new_row->>'bank_details_hash', '')
       OR COALESCE(v_old_row->>'account_holder', '') IS DISTINCT FROM COALESCE(v_new_row->>'account_holder', '')
       OR COALESCE(v_old_row->>'bank_name', '') IS DISTINCT FROM COALESCE(v_new_row->>'bank_name', '')
       OR COALESCE(v_old_row->>'sort_code', '') IS DISTINCT FROM COALESCE(v_new_row->>'sort_code', '')
       OR COALESCE(v_old_row->>'account_number', '') IS DISTINCT FROM COALESCE(v_new_row->>'account_number', '')
       OR COALESCE(v_old_row->>'min_take_home_wtd', '') IS DISTINCT FROM COALESCE(v_new_row->>'min_take_home_wtd', '') THEN
      v_should_dirty := true;
      v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
    END IF;

  ELSIF v_trigger_table = 'umbrellas' THEN
    SELECT COALESCE(array_agg(DISTINCT c.id), ARRAY[]::uuid[])
    INTO v_candidate_ids
    FROM public.candidates AS c
    WHERE c.umbrella_id IN (
      NULLIF(BTRIM(COALESCE(v_old_row->>'id', '')), '')::uuid,
      NULLIF(BTRIM(COALESCE(v_new_row->>'id', '')), '')::uuid
    );

    IF TG_OP IN ('INSERT', 'DELETE')
       OR COALESCE(v_old_row->>'enabled', '') IS DISTINCT FROM COALESCE(v_new_row->>'enabled', '')
       OR COALESCE(v_old_row->>'vat_chargeable', '') IS DISTINCT FROM COALESCE(v_new_row->>'vat_chargeable', '')
       OR COALESCE(v_old_row->>'bank_details_hash', '') IS DISTINCT FROM COALESCE(v_new_row->>'bank_details_hash', '')
       OR COALESCE(v_old_row->>'bank_name', '') IS DISTINCT FROM COALESCE(v_new_row->>'bank_name', '')
       OR COALESCE(v_old_row->>'sort_code', '') IS DISTINCT FROM COALESCE(v_new_row->>'sort_code', '')
       OR COALESCE(v_old_row->>'account_number', '') IS DISTINCT FROM COALESCE(v_new_row->>'account_number', '') THEN
      v_should_dirty := true;
      v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
    END IF;
  END IF;

  SELECT COALESCE(array_agg(DISTINCT x.candidate_id), ARRAY[]::uuid[])
  INTO v_candidate_ids
  FROM unnest(COALESCE(v_candidate_ids, ARRAY[]::uuid[])) AS x(candidate_id)
  WHERE x.candidate_id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT x.timesheet_id), ARRAY[]::uuid[])
  INTO v_targeted_timesheet_uuid_ids
  FROM unnest(COALESCE(v_targeted_timesheet_uuid_ids, ARRAY[]::uuid[])) AS x(timesheet_id)
  WHERE x.timesheet_id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT x.timesheet_id), ARRAY[]::uuid[])
  INTO v_linked_timesheet_uuid_ids
  FROM unnest(COALESCE(v_linked_timesheet_uuid_ids, ARRAY[]::uuid[])) AS x(timesheet_id)
  WHERE x.timesheet_id IS NOT NULL;

  SELECT COALESCE(jsonb_agg(to_jsonb(x.timesheet_id::text) ORDER BY x.timesheet_id::text), '[]'::jsonb)
  INTO v_targeted_timesheet_ids_json
  FROM unnest(COALESCE(v_targeted_timesheet_uuid_ids, ARRAY[]::uuid[])) AS x(timesheet_id);

  SELECT COALESCE(jsonb_agg(to_jsonb(x.timesheet_id::text) ORDER BY x.timesheet_id::text), '[]'::jsonb)
  INTO v_linked_timesheet_ids_json
  FROM unnest(COALESCE(v_linked_timesheet_uuid_ids, ARRAY[]::uuid[])) AS x(timesheet_id);

  IF COALESCE(array_length(v_candidate_ids, 1), 0) = 0 OR v_should_dirty = false THEN
    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    ELSE
      RETURN NEW;
    END IF;
  END IF;

  FOREACH v_candidate_id IN ARRAY v_candidate_ids
  LOOP
    PERFORM public._change_bump('pay_candidate:' || v_candidate_id::text);

    SELECT COALESCE(acc.seq, 0)
    INTO v_live_change_seq
    FROM public.app_change_counters AS acc
    WHERE acc.entity_key = 'pay_candidate:' || v_candidate_id::text;

    v_reason := 'DIRTY_TRIGGER:' || upper(v_trigger_table) || ':' || TG_OP;
    v_payload_json := jsonb_build_object(
      'trigger_table', v_trigger_table,
      'trigger_op', TG_OP,
      'scope_kind', 'CANDIDATE',
      'scope_id', v_candidate_id::text,
      'candidate_id', v_candidate_id::text,
      'reason', v_reason,
      'source_change_seq', v_live_change_seq,
      'refresh_scope_kind', v_refresh_scope_kind,
      'targeted_timesheet_ids', v_targeted_timesheet_ids_json,
      'linked_timesheet_ids', v_linked_timesheet_ids_json,
      'old_timesheet_id', CASE WHEN v_old_timesheet_id IS NULL THEN NULL ELSE v_old_timesheet_id::text END,
      'new_timesheet_id', CASE WHEN v_new_timesheet_id IS NULL THEN NULL ELSE v_new_timesheet_id::text END,
      'old_contract_id', CASE WHEN v_old_contract_id IS NULL THEN NULL ELSE v_old_contract_id::text END,
      'new_contract_id', CASE WHEN v_new_contract_id IS NULL THEN NULL ELSE v_new_contract_id::text END,
      'old_booking_id', CASE WHEN v_old_booking_id IS NULL THEN NULL ELSE v_old_booking_id::text END,
      'new_booking_id', CASE WHEN v_new_booking_id IS NULL THEN NULL ELSE v_new_booking_id::text END,
      'old_entity_kind', NULLIF(BTRIM(COALESCE(v_old_row->>'entity_kind', '')), ''),
      'new_entity_kind', NULLIF(BTRIM(COALESCE(v_new_row->>'entity_kind', '')), ''),
      'old_entity_id', NULLIF(BTRIM(COALESCE(v_old_row->>'entity_id', '')), ''),
      'new_entity_id', NULLIF(BTRIM(COALESCE(v_new_row->>'entity_id', '')), '')
    );
    v_dedupe_key := 'CONTRACT_CLIENT_DIRTY_FANOUT:CANDIDATE:' || v_candidate_id::text;

    INSERT INTO public.banking_pay_workbench_jobs (
      job_type,
      status,
      priority,
      run_at_utc,
      attempt_count,
      max_attempts,
      dedupe_key,
      snapshot_run_id,
      session_id,
      candidate_id,
      payload_json,
      created_at_utc,
      updated_at_utc,
      started_at_utc,
      completed_at_utc,
      failed_at_utc,
      last_error_json
    )
    VALUES (
      'CONTRACT_CLIENT_DIRTY_FANOUT',
      'QUEUED',
      200,
      v_now,
      0,
      8,
      v_dedupe_key,
      NULL,
      NULL,
      v_candidate_id,
      v_payload_json,
      v_now,
      v_now,
      NULL,
      NULL,
      NULL,
      NULL
    )
    ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
    DO UPDATE
    SET priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
        run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
        candidate_id = COALESCE(public.banking_pay_workbench_jobs.candidate_id, EXCLUDED.candidate_id),
        payload_json = public._pay_workbench_merge_targeted_scope_payload(
          COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb),
          COALESCE(EXCLUDED.payload_json, '{}'::jsonb)
        ),
        updated_at_utc = v_now;

    UPDATE public.banking_pay_workbench_session_candidate_state AS scs
    SET status = 'PENDING',
        source_change_seq = GREATEST(COALESCE(scs.source_change_seq, 0), v_live_change_seq),
        updated_at_utc = v_now,
        last_error_json = NULL
    FROM public.banking_pay_workbench_sessions AS ws
    WHERE ws.id = scs.session_id
      AND ws.status = 'OPEN'
      AND scs.candidate_id = v_candidate_id
      AND ws.scope_candidate_ids @> ARRAY[v_candidate_id]::uuid[];
  END LOOP;

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;
END;
$function$;


CREATE OR REPLACE FUNCTION public.pay_workbench_mark_finance_case_dirty()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_trigger_table text := lower(TG_TABLE_NAME);
  v_new_row jsonb := '{}'::jsonb;
  v_old_row jsonb := '{}'::jsonb;
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_id uuid;
  v_live_change_seq bigint := 0;
  v_reason text;
  v_payload_json jsonb;
  v_dedupe_key text;
BEGIN
  IF TG_OP <> 'DELETE' THEN
    v_new_row := to_jsonb(NEW);
  END IF;

  IF TG_OP <> 'INSERT' THEN
    v_old_row := to_jsonb(OLD);
  END IF;

  IF v_trigger_table = 'pay_finance_case_components' THEN
    SELECT array_cat(
             ARRAY[
               nullif(btrim(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid,
               nullif(btrim(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid,
               (
                 SELECT pa.candidate_id
                 FROM public.pay_advances pa
                 WHERE pa.id = nullif(btrim(COALESCE(v_old_row->>'finance_case_id', '')), '')::uuid
                 LIMIT 1
               ),
               (
                 SELECT pa.candidate_id
                 FROM public.pay_advances pa
                 WHERE pa.id = nullif(btrim(COALESCE(v_new_row->>'finance_case_id', '')), '')::uuid
                 LIMIT 1
               )
             ]::uuid[],
             ARRAY[]::uuid[]
           )
    INTO v_candidate_ids;

  ELSIF v_trigger_table = 'pay_finance_case_events' THEN
    SELECT array_cat(
             ARRAY[
               (
                 SELECT pa.candidate_id
                 FROM public.pay_advances pa
                 WHERE pa.id = nullif(btrim(COALESCE(v_old_row->>'finance_case_id', '')), '')::uuid
                 LIMIT 1
               ),
               (
                 SELECT pa.candidate_id
                 FROM public.pay_advances pa
                 WHERE pa.id = nullif(btrim(COALESCE(v_new_row->>'finance_case_id', '')), '')::uuid
                 LIMIT 1
               )
             ]::uuid[],
             ARRAY[]::uuid[]
           )
    INTO v_candidate_ids;
  END IF;

  SELECT COALESCE(array_agg(DISTINCT x.candidate_id), ARRAY[]::uuid[])
  INTO v_candidate_ids
  FROM unnest(COALESCE(v_candidate_ids, ARRAY[]::uuid[])) AS x(candidate_id)
  WHERE x.candidate_id IS NOT NULL;

  IF COALESCE(array_length(v_candidate_ids, 1), 0) = 0 THEN
    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    ELSE
      RETURN NEW;
    END IF;
  END IF;

  FOREACH v_candidate_id IN ARRAY v_candidate_ids
  LOOP
    PERFORM public._change_bump('pay_candidate:' || v_candidate_id::text);

    SELECT COALESCE(acc.seq, 0)
    INTO v_live_change_seq
    FROM public.app_change_counters acc
    WHERE acc.entity_key = 'pay_candidate:' || v_candidate_id::text;

    v_reason := 'DIRTY_TRIGGER:' || upper(v_trigger_table) || ':' || TG_OP;
    v_payload_json := jsonb_build_object(
      'trigger_table', v_trigger_table,
      'trigger_op', TG_OP,
      'scope_kind', 'CANDIDATE',
      'scope_id', v_candidate_id::text,
      'candidate_id', v_candidate_id::text,
      'reason', v_reason,
      'source_change_seq', v_live_change_seq,
      'old_finance_case_id', nullif(btrim(COALESCE(v_old_row->>'finance_case_id', '')), ''),
      'new_finance_case_id', nullif(btrim(COALESCE(v_new_row->>'finance_case_id', '')), ''),
      'old_row_id', nullif(btrim(COALESCE(v_old_row->>'id', '')), ''),
      'new_row_id', nullif(btrim(COALESCE(v_new_row->>'id', '')), '')
    );
    v_dedupe_key := 'CONTRACT_CLIENT_DIRTY_FANOUT:CANDIDATE:' || v_candidate_id::text;

    INSERT INTO public.banking_pay_workbench_jobs (
      job_type,
      status,
      priority,
      run_at_utc,
      attempt_count,
      max_attempts,
      dedupe_key,
      snapshot_run_id,
      session_id,
      candidate_id,
      payload_json,
      created_at_utc,
      updated_at_utc,
      started_at_utc,
      completed_at_utc,
      failed_at_utc,
      last_error_json
    )
    VALUES (
      'CONTRACT_CLIENT_DIRTY_FANOUT',
      'QUEUED',
      200,
      v_now,
      0,
      8,
      v_dedupe_key,
      NULL,
      NULL,
      v_candidate_id,
      v_payload_json,
      v_now,
      v_now,
      NULL,
      NULL,
      NULL,
      NULL
    )
    ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
    DO UPDATE
    SET priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
        run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
        candidate_id = COALESCE(public.banking_pay_workbench_jobs.candidate_id, EXCLUDED.candidate_id),
        payload_json = COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb) || COALESCE(EXCLUDED.payload_json, '{}'::jsonb),
        updated_at_utc = v_now;

    UPDATE public.banking_pay_workbench_session_candidate_state scs
    SET status = 'PENDING',
        source_change_seq = GREATEST(COALESCE(scs.source_change_seq, 0), v_live_change_seq),
        updated_at_utc = v_now,
        last_error_json = NULL
    FROM public.banking_pay_workbench_sessions ws
    WHERE ws.id = scs.session_id
      AND ws.status = 'OPEN'
      AND scs.candidate_id = v_candidate_id
      AND ws.scope_candidate_ids @> ARRAY[v_candidate_id]::uuid[];
  END LOOP;

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_candidate_refresh(
  p_snapshot_run_id uuid,
  p_candidate_id uuid,
  p_reason text DEFAULT NULL::text,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_payload_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_live_change_seq bigint := 0;
  v_dedupe_key text;
  v_job_id uuid;
  v_job_status text;
  v_job_was_inserted boolean := false;
BEGIN
  IF p_snapshot_run_id IS NULL THEN
    RAISE EXCEPTION 'snapshot_run_id is required';
  END IF;

  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'candidate_id is required';
  END IF;

  PERFORM 1
  FROM public.banking_pay_snapshot_runs AS sr
  WHERE sr.id = p_snapshot_run_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_snapshot_runs row % not found', p_snapshot_run_id;
  END IF;

  PERFORM 1
  FROM public.candidates AS c
  WHERE c.id = p_candidate_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'candidates row % not found', p_candidate_id;
  END IF;

  SELECT COALESCE(acc.seq, 0)
  INTO v_live_change_seq
  FROM public.app_change_counters AS acc
  WHERE acc.entity_key = 'pay_candidate:' || p_candidate_id::text;

  v_dedupe_key := 'SNAPSHOT_CANDIDATE_REFRESH:' || p_snapshot_run_id::text || ':' || p_candidate_id::text || ':s' || v_live_change_seq::text;

  INSERT INTO public.banking_pay_workbench_jobs (
    job_type,
    status,
    priority,
    run_at_utc,
    attempt_count,
    max_attempts,
    dedupe_key,
    snapshot_run_id,
    session_id,
    candidate_id,
    payload_json,
    created_at_utc,
    updated_at_utc,
    started_at_utc,
    completed_at_utc,
    failed_at_utc,
    last_error_json
  )
  VALUES (
    'SNAPSHOT_CANDIDATE_REFRESH',
    'QUEUED',
    50,
    v_now,
    0,
    8,
    v_dedupe_key,
    p_snapshot_run_id,
    NULL,
    p_candidate_id,
    COALESCE(p_payload_json, '{}'::jsonb)
      || jsonb_build_object(
        'reason', p_reason,
        'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'snapshot_run_id', p_snapshot_run_id::text,
        'candidate_id', p_candidate_id::text,
        'source_change_seq', v_live_change_seq,
        'job_type', 'SNAPSHOT_CANDIDATE_REFRESH'
      ),
    v_now,
    v_now,
    NULL,
    NULL,
    NULL,
    NULL
  )
  ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
  DO UPDATE
  SET priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
      run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
      payload_json = public._pay_workbench_merge_targeted_scope_payload(
        COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb),
        COALESCE(EXCLUDED.payload_json, '{}'::jsonb)
      ),
      updated_at_utc = v_now
  RETURNING public.banking_pay_workbench_jobs.id, public.banking_pay_workbench_jobs.status, (xmax = 0)
  INTO v_job_id, v_job_status, v_job_was_inserted;

  INSERT INTO public.banking_pay_snapshot_candidate_state (
    snapshot_run_id,
    candidate_id,
    status,
    candidate_fragment_json,
    summary_fragment_json,
    paye_candidate_json,
    non_paye_payee_json,
    payees_json,
    case_resolution_states_json,
    canonical_preview_lines_json,
    source_change_seq,
    created_at_utc,
    updated_at_utc,
    last_refreshed_at_utc,
    last_error_json
  )
  VALUES (
    p_snapshot_run_id,
    p_candidate_id,
    'PENDING',
    '{}'::jsonb,
    '{}'::jsonb,
    NULL,
    NULL,
    '[]'::jsonb,
    '[]'::jsonb,
    '[]'::jsonb,
    v_live_change_seq,
    v_now,
    v_now,
    NULL,
    NULL
  )
  ON CONFLICT (snapshot_run_id, candidate_id)
  DO UPDATE
  SET status = 'PENDING',
      source_change_seq = GREATEST(public.banking_pay_snapshot_candidate_state.source_change_seq, EXCLUDED.source_change_seq),
      updated_at_utc = v_now,
      last_error_json = NULL;

  UPDATE public.banking_pay_workbench_session_candidate_state AS scs
  SET status = 'PENDING',
      source_change_seq = GREATEST(scs.source_change_seq, v_live_change_seq),
      pending_job_id = v_job_id,
      updated_at_utc = v_now,
      last_error_json = NULL
  FROM public.banking_pay_workbench_sessions AS ws
  WHERE ws.id = scs.session_id
    AND ws.status = 'OPEN'
    AND ws.source_snapshot_run_id = p_snapshot_run_id
    AND scs.candidate_id = p_candidate_id
    AND ws.scope_candidate_ids @> ARRAY[p_candidate_id]::uuid[];

  PERFORM public._audit_insert(
    'banking_pay_workbench_job',
    v_job_id::text,
    CASE WHEN v_job_was_inserted THEN 'QUEUED' ELSE 'REUSED' END,
    NULL,
    jsonb_build_object(
      'id', v_job_id::text,
      'job_type', 'SNAPSHOT_CANDIDATE_REFRESH',
      'status', v_job_status,
      'snapshot_run_id', p_snapshot_run_id::text,
      'candidate_id', p_candidate_id::text,
      'dedupe_key', v_dedupe_key,
      'source_change_seq', v_live_change_seq
    ),
    'WORKBENCH_JOB_ENQUEUE',
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'job_id', v_job_id::text,
    'job_type', 'SNAPSHOT_CANDIDATE_REFRESH',
    'snapshot_run_id', p_snapshot_run_id::text,
    'candidate_id', p_candidate_id::text,
    'source_change_seq', v_live_change_seq,
    'dedupe_key', v_dedupe_key,
    'reason', p_reason
  );
END;
$function$;


BEGIN;
DROP TRIGGER IF EXISTS trg_pay_workbench_mark_candidate_dirty__timesheet_pay_state ON public.timesheet_pay_state;
COMMIT;


