-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 67dcc73bb4bb.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.contract_week_manual_unprocess_atomic(
  p_week_id uuid,
  p_expected_timesheet_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_now_utc timestamp with time zone DEFAULT now(),
  p_expected_row_signature text DEFAULT NULL::text
)
RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_week public.contract_weeks%ROWTYPE;
  v_pointer_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_existing_queue public.manual_timesheet_queue%ROWTYPE;
  v_contract public.contracts%ROWTYPE;

  v_booking_id text := NULL;
  v_all_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_deleted_timesheet_count integer := 0;
  v_deleted_tsfin_count integer := 0;
  v_deleted_evidence_count integer := 0;
  v_deleted_validation_count integer := 0;
  v_deleted_ts_pdf_outbox_count integer := 0;
  v_deleted_tsfin_outbox_count integer := 0;
  v_cleared_snooze_ids uuid[] := ARRAY[]::uuid[];

  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;

  v_paid_timesheet_id uuid := NULL;
  v_invoice_locked_timesheet_id uuid := NULL;
  v_invoice_segment_locked_timesheet_id uuid := NULL;

  v_seen_storage_keys text[] := ARRAY[]::text[];
  v_stage_items jsonb := '[]'::jsonb;
  v_stage_item jsonb := NULL;
  v_stage_item_kind text := NULL;
  v_stage_item_storage_key text := NULL;
  v_stage_item_display_name text := NULL;
  v_stage_item_created_at timestamp with time zone := NULL;
  v_stage_item_created_by uuid := NULL;
  v_stage_item_rotation integer := 0;
  v_stage_item_timesheet_id uuid := NULL;
  v_stage_item_evidence_id uuid := NULL;
  v_timesheet_stage_keys text[] := ARRAY[]::text[];
  v_active_timesheet_keys text[] := ARRAY[]::text[];
  v_active_timesheet_missing_key_id uuid := NULL;
  v_existing_active_key text := NULL;
  v_repaired_same_key_duplicate_count integer := 0;
  v_dematerialised_primary_timesheet_storage_key text := NULL;
  v_staged_count integer := 0;

  v_evidence_record record;
  v_queue_record record;
  v_queue_storage_key text := NULL;
  v_queue_kind text := NULL;
  v_duplicate_queue_ids uuid[] := ARRAY[]::uuid[];
  v_clean_meta jsonb := '{}'::jsonb;
  v_merged_meta jsonb := '{}'::jsonb;

  v_reopen_snapshot text := 'MANUAL';
  v_reopened_totals_json jsonb := '{}'::jsonb;
  v_reopened_day_entries_json jsonb := '{}'::jsonb;
  v_reopened_planned_schedule_json jsonb := NULL;
  v_additional_units_week_json jsonb := '{}'::jsonb;
  v_additional_units_per_day_json jsonb := '{}'::jsonb;
  v_existing_totals_json jsonb := '{}'::jsonb;
  v_expenses_draft_json jsonb := '{}'::jsonb;

  v_signature_after_text text := NULL;
  v_previous_contract_week_status text := NULL;
  v_previous_processing_status text := NULL;
  v_error_constraint text := NULL;
  v_history jsonb := '{}'::jsonb;
BEGIN

  if p_expected_timesheet_id is not null
     and coalesce((public._ctms_import_correction_classify_v1(p_expected_timesheet_id)
       ->> 'is_import_authoritative_correction')::boolean, false) then
    declare v_transition jsonb;
    begin
      v_transition := public.timesheet_correction_pair_transition_v1(
        p_expected_timesheet_id, 'UNPROCESS', p_actor_user_id,
        null::uuid, null::text, true, 100
      );
      if coalesce((v_transition ->> 'action_ready')::boolean, false) is not true
         or coalesce((v_transition ->> 'expected_member_count')::integer, 0) > 1 then
        return jsonb_build_object(
          'ok', false,
          'error_code', 'IMPORT_CORRECTION_UNIT_REQUIRES_ATOMIC_PROCESS_ORCHESTRATION',
          'transition', v_transition
        );
      end if;
    end;
  end if;
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_week_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_week_id')::text;
  END IF;

  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id')::text;
  END IF;

  IF NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EXPECTED_ROW_SIGNATURE_REQUIRED',
      DETAIL = jsonb_build_object(
        'contract_week_id', p_week_id,
        'expected_timesheet_id', p_expected_timesheet_id,
        'message', 'The current lifecycle signature is required. Refresh the timesheet and try again.'
      )::text;
  END IF;

  SELECT cw.*
    INTO v_week
  FROM public.contract_weeks AS cw
  WHERE cw.id = p_week_id
  FOR UPDATE;

  IF v_week.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;

  v_previous_contract_week_status := v_week.status::text;

  PERFORM pg_advisory_xact_lock(hashtext('contract_week_staged_timesheet:' || v_week.id::text));

  IF v_week.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_UNPROCESSED', DETAIL = jsonb_build_object('contract_week_id', v_week.id)::text;
  END IF;

  SELECT ts.*
    INTO v_pointer_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = v_week.timesheet_id
  FOR UPDATE;

  IF v_pointer_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'timesheet_id', v_week.timesheet_id)::text;
  END IF;

  v_booking_id := v_pointer_ts.booking_id;

  SELECT ts.*
    INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.booking_id = v_booking_id
    AND ts.is_current = true
  ORDER BY ts.version DESC, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('booking_id', v_booking_id, 'reason', 'current_timesheet_not_found')::text;
  END IF;

  IF v_current_ts.timesheet_id IS DISTINCT FROM p_expected_timesheet_id THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH',
      DETAIL = jsonb_build_object(
        'expected_timesheet_id', p_expected_timesheet_id,
        'current_timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', v_week.id
      )::text;
  END IF;

  IF v_week.status = 'INVOICED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id, 'lock_scope', 'contract_week_status')::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL OR v_week.status = 'AUTHORISED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_AUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id)::text;
  END IF;

  SELECT locked_ids.timesheet_ids
    INTO v_all_timesheet_ids
  FROM (
    SELECT COALESCE(array_agg(locked_ts.timesheet_id ORDER BY locked_ts.version ASC, locked_ts.created_at ASC, locked_ts.timesheet_id ASC), ARRAY[]::uuid[]) AS timesheet_ids
    FROM (
      SELECT ts.timesheet_id, ts.version, ts.created_at
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_booking_id
      FOR UPDATE
    ) AS locked_ts
  ) AS locked_ids;

  IF COALESCE(array_length(v_all_timesheet_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('booking_id', v_booking_id, 'reason', 'timesheet_series_not_found')::text;
  END IF;

  IF EXISTS (
    SELECT 1 FROM public.timesheets AS archived_guard
    WHERE archived_guard.timesheet_id = ANY(v_all_timesheet_ids)
      AND archived_guard.archived_at_utc IS NOT NULL
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_ids', to_jsonb(v_all_timesheet_ids), 'contract_week_id', v_week.id)::text;
  END IF;

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'current_timesheet_financials_not_found')::text;
  END IF;

  v_previous_processing_status := v_current_tsfin.processing_status::text;

  SELECT invoice_guard.timesheet_id
    INTO v_invoice_locked_timesheet_id
  FROM public.timesheets_financials AS invoice_guard
  WHERE invoice_guard.timesheet_id = ANY(v_all_timesheet_ids)
    AND invoice_guard.is_current = true
    AND invoice_guard.locked_by_invoice_id IS NOT NULL
  LIMIT 1
  FOR UPDATE;

  IF v_invoice_locked_timesheet_id IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_invoice_locked_timesheet_id, 'contract_week_id', v_week.id)::text;
  END IF;

  SELECT segment_guard.timesheet_id
    INTO v_invoice_segment_locked_timesheet_id
  FROM public.timesheets_financials AS segment_guard
  WHERE segment_guard.timesheet_id = ANY(v_all_timesheet_ids)
    AND segment_guard.is_current = true
    AND EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN segment_guard.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(segment_guard.invoice_breakdown_json) = 'array' THEN segment_guard.invoice_breakdown_json
          WHEN jsonb_typeof(segment_guard.invoice_breakdown_json) = 'object'
           AND jsonb_typeof(segment_guard.invoice_breakdown_json -> 'segments') = 'array' THEN segment_guard.invoice_breakdown_json -> 'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
    )
  LIMIT 1
  FOR UPDATE;

  IF v_invoice_segment_locked_timesheet_id IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_invoice_segment_locked_timesheet_id, 'contract_week_id', v_week.id, 'lock_scope', 'segment')::text;
  END IF;

  IF v_current_tsfin.authorised_at_utc IS NOT NULL OR v_current_tsfin.authorised_by_user_id IS NOT NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ALREADY_AUTHORISED',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id, 'lock_scope', 'timesheet_financials')::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_signature_v1(v_current_ts.timesheet_id, v_week.id, false);
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');

  IF COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ROW_SIGNATURE_MISMATCH',
      DETAIL = jsonb_build_object(
        'expected_row_signature', v_expected_row_signature,
        'current_row_signature', v_current_row_signature,
        'current_timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', v_week.id
      )::text;
  END IF;

  SELECT c.*
    INTO v_contract
  FROM public.contracts AS c
  WHERE c.id = v_week.contract_id
  LIMIT 1;

  FOR v_evidence_record IN
    SELECT
      ev.id AS evidence_id,
      ev.timesheet_id,
      ev.kind,
      ev.display_name,
      ev.storage_key,
      ev.created_at,
      ev.created_by,
      ts.manual_pdf_r2_key,
      ts.manual_pdf_rotation_degrees
    FROM public.timesheet_evidence AS ev
    JOIN public.timesheets AS ts ON ts.timesheet_id = ev.timesheet_id
    WHERE ev.timesheet_id = ANY(v_all_timesheet_ids)
    ORDER BY ev.created_at ASC NULLS LAST, ev.id ASC
    FOR UPDATE OF ev
  LOOP
    v_stage_item_storage_key := NULLIF(regexp_replace(BTRIM(COALESCE(v_evidence_record.storage_key, '')), '^/+', ''), '');
    IF v_stage_item_storage_key IS NULL OR v_stage_item_storage_key = ANY(v_seen_storage_keys) THEN
      CONTINUE;
    END IF;
    v_seen_storage_keys := array_append(v_seen_storage_keys, v_stage_item_storage_key);

    v_stage_item_kind := UPPER(COALESCE(NULLIF(BTRIM(v_evidence_record.kind), ''), 'OTHER'));
    IF v_stage_item_kind NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION','OTHER') THEN
      v_stage_item_kind := 'OTHER';
    END IF;

    v_stage_item_rotation := 0;
    IF v_stage_item_kind = 'TIMESHEET'
       AND NULLIF(regexp_replace(BTRIM(COALESCE(v_evidence_record.manual_pdf_r2_key, '')), '^/+', ''), '') = v_stage_item_storage_key THEN
      v_stage_item_rotation := COALESCE(v_evidence_record.manual_pdf_rotation_degrees, 0);
    END IF;

    v_stage_items := v_stage_items || jsonb_build_array(
      jsonb_build_object(
        'source', 'TIMESHEET_EVIDENCE',
        'evidence_id', v_evidence_record.evidence_id,
        'timesheet_id', v_evidence_record.timesheet_id,
        'kind', v_stage_item_kind,
        'storage_key', v_stage_item_storage_key,
        'display_name', COALESCE(NULLIF(BTRIM(v_evidence_record.display_name), ''), regexp_replace(v_stage_item_storage_key, '^.*/', '')),
        'created_at', COALESCE(v_evidence_record.created_at, v_now),
        'created_by', v_evidence_record.created_by,
        'rotation_deg', v_stage_item_rotation
      )
    );
  END LOOP;

  v_stage_item_storage_key := NULLIF(regexp_replace(BTRIM(COALESCE(v_current_ts.manual_pdf_r2_key, '')), '^/+', ''), '');
  IF v_stage_item_storage_key IS NOT NULL AND NOT (v_stage_item_storage_key = ANY(v_seen_storage_keys)) THEN
    v_seen_storage_keys := array_append(v_seen_storage_keys, v_stage_item_storage_key);
    v_stage_items := v_stage_items || jsonb_build_array(
      jsonb_build_object(
        'source', 'LEGACY_MANUAL_PDF_POINTER',
        'evidence_id', NULL,
        'timesheet_id', v_current_ts.timesheet_id,
        'kind', 'TIMESHEET',
        'storage_key', v_stage_item_storage_key,
        'display_name', COALESCE(NULLIF(regexp_replace(v_stage_item_storage_key, '^.*/', ''), ''), 'file'),
        'created_at', v_now,
        'created_by', p_actor_user_id,
        'rotation_deg', COALESCE(v_current_ts.manual_pdf_rotation_degrees, 0)
      )
    );
  END IF;

  SELECT COALESCE(array_agg(DISTINCT item_rows.storage_key ORDER BY item_rows.storage_key), ARRAY[]::text[])
    INTO v_timesheet_stage_keys
  FROM (
    SELECT NULLIF(BTRIM(item_value.value ->> 'storage_key'), '') AS storage_key
    FROM jsonb_array_elements(v_stage_items) AS item_value(value)
    WHERE UPPER(COALESCE(NULLIF(BTRIM(item_value.value ->> 'kind'), ''), 'OTHER')) = 'TIMESHEET'
  ) AS item_rows
  WHERE item_rows.storage_key IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT active_rows.storage_key ORDER BY active_rows.storage_key), ARRAY[]::text[])
    INTO v_active_timesheet_keys
  FROM (
    SELECT NULLIF(regexp_replace(COALESCE(
      NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'r2_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'storage_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'file_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'canonical_key', '')), ''),
      ''
    ), '^/+', ''), '') AS storage_key
    FROM public.manual_timesheet_queue AS mq
    WHERE mq.status = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
      AND UPPER(COALESCE(
        NULLIF(BTRIM(mq.meta_json ->> 'staged_kind'), ''),
        NULLIF(BTRIM(mq.meta_json ->> 'kind'), ''),
        NULLIF(BTRIM(mq.meta_json ->> 'attached_kind'), ''),
        'TIMESHEET'
      )) = 'TIMESHEET'
    FOR UPDATE OF mq
  ) AS active_rows
  WHERE active_rows.storage_key IS NOT NULL;

  SELECT mq.id
    INTO v_active_timesheet_missing_key_id
  FROM public.manual_timesheet_queue AS mq
  WHERE mq.status = 'STAGED'
    AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
    AND UPPER(COALESCE(
      NULLIF(BTRIM(mq.meta_json ->> 'staged_kind'), ''),
      NULLIF(BTRIM(mq.meta_json ->> 'kind'), ''),
      NULLIF(BTRIM(mq.meta_json ->> 'attached_kind'), ''),
      'TIMESHEET'
    )) = 'TIMESHEET'
    AND NULLIF(regexp_replace(COALESCE(
      NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'r2_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'storage_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'file_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'canonical_key', '')), ''),
      ''
    ), '^/+', ''), '') IS NULL
  LIMIT 1
  FOR UPDATE;

  IF COALESCE(array_length(v_timesheet_stage_keys, 1), 0) > 0 AND v_active_timesheet_missing_key_id IS NOT NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_TIMESHEET_EVIDENCE',
      DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_active_timesheet_missing_key_id, 'reason', 'missing_storage_key')::text;
  END IF;

  IF COALESCE(array_length(v_timesheet_stage_keys, 1), 0) > 1 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
      DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'dematerialised_storage_keys', to_jsonb(v_timesheet_stage_keys), 'timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF COALESCE(array_length(v_active_timesheet_keys, 1), 0) > 1 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
      DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'active_storage_keys', to_jsonb(v_active_timesheet_keys))::text;
  END IF;

  IF COALESCE(array_length(v_active_timesheet_keys, 1), 0) = 1 AND COALESCE(array_length(v_timesheet_stage_keys, 1), 0) = 1 THEN
    v_existing_active_key := v_active_timesheet_keys[1];
    IF v_existing_active_key IS DISTINCT FROM v_timesheet_stage_keys[1] THEN
      RAISE EXCEPTION USING
        MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
        DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'existing_storage_key', v_existing_active_key, 'dematerialised_storage_key', v_timesheet_stage_keys[1])::text;
    END IF;
  END IF;

  FOR v_queue_record IN
    SELECT
      mq.id,
      mq.r2_key,
      mq.meta_json,
      mq.uploaded_at_utc
    FROM public.manual_timesheet_queue AS mq
    WHERE mq.status = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
      AND UPPER(COALESCE(
        NULLIF(BTRIM(mq.meta_json ->> 'staged_kind'), ''),
        NULLIF(BTRIM(mq.meta_json ->> 'kind'), ''),
        NULLIF(BTRIM(mq.meta_json ->> 'attached_kind'), ''),
        'TIMESHEET'
      )) = 'TIMESHEET'
    ORDER BY mq.uploaded_at_utc ASC NULLS LAST, mq.id ASC
    FOR UPDATE
  LOOP
    v_queue_storage_key := NULLIF(regexp_replace(COALESCE(
      NULLIF(BTRIM(COALESCE(v_queue_record.r2_key, '')), ''),
      NULLIF(BTRIM(COALESCE(v_queue_record.meta_json ->> 'r2_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_queue_record.meta_json ->> 'storage_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_queue_record.meta_json ->> 'file_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_queue_record.meta_json ->> 'canonical_key', '')), ''),
      ''
    ), '^/+', ''), '');

    IF v_queue_storage_key IS NOT NULL
       AND v_queue_storage_key = ANY(v_timesheet_stage_keys)
       AND NOT (v_queue_record.id = ANY(v_duplicate_queue_ids)) THEN
      IF v_existing_active_key IS NULL THEN
        v_existing_active_key := v_queue_storage_key;
      ELSE
        v_duplicate_queue_ids := array_append(v_duplicate_queue_ids, v_queue_record.id);
      END IF;
    END IF;
  END LOOP;

  -- Complete the pre-mutation validation before the retention decision so an
  -- invoice, authorisation, Archived, stale, evidence-conflict, or invalid-actor
  -- blocker is never replaced by the financial-history explanation.
  IF p_actor_user_id IS NULL AND EXISTS (
    SELECT 1
    FROM jsonb_array_elements(v_stage_items) AS staged_item(value)
    WHERE NULLIF(BTRIM(COALESCE(staged_item.value ->> 'storage_key', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(staged_item.value ->> 'created_by', '')), '') IS NULL
  ) THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_PAYLOAD',
      DETAIL = jsonb_build_object(
        'field', 'p_actor_user_id',
        'reason', 'actor required to recreate staged evidence provenance'
      )::text;
  END IF;

  -- Use the same sticky retained-financial-history classifier as permanent Delete.
  -- Only its marker-backed archive_required result blocks Unprocess here; the
  -- earlier weekly validation continues to own invoice, authorisation, Archive,
  -- identity, evidence and stale-row errors.
  v_history := public.timesheet_removal_financial_history_v1(
    v_all_timesheet_ids,
    ARRAY[v_booking_id]::text[],
    ARRAY[v_week.id]::uuid[]
  );

  IF COALESCE((v_history ->> 'archive_required')::boolean, false) THEN
    RAISE EXCEPTION USING
      MESSAGE = 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
      DETAIL = jsonb_build_object(
        'message', 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.',
        'contract_week_id', v_week.id,
        'requested_timesheet_id', p_expected_timesheet_id,
        'current_timesheet_id', v_current_ts.timesheet_id,
        'timesheet_ids', to_jsonb(v_all_timesheet_ids),
        'current_row_signature', v_current_row_signature,
        'backend_row_signature', v_current_row_signature,
        'row_signature', v_current_row_signature,
        'has_retained_financial_history', true,
        'retention_reasons', COALESCE(v_history -> 'retention_reasons', '[]'::jsonb),
        'can_unprocess', false,
        'unprocess_block_reason', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
        'unprocess_action_visible', true,
        'row_patch', jsonb_build_object(
          'timesheet_id', v_current_ts.timesheet_id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'row_key', 'timesheet:' || v_current_ts.timesheet_id::text,
          'row_signature', v_current_row_signature,
          'backend_row_signature', v_current_row_signature,
          'has_retained_financial_history', true,
          'can_unprocess', false,
          'unprocess_block_reason', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
          'unprocess_action_visible', true
        ),
        'action_flags', jsonb_build_object(
          'has_retained_financial_history', true,
          'can_unprocess', false,
          'unprocess_block_reason', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
          'unprocess_action_visible', true
        )
      )::text;
  END IF;


  IF COALESCE(array_length(v_duplicate_queue_ids, 1), 0) > 0 THEN
    UPDATE public.manual_timesheet_queue AS mq
       SET status = 'DISCARDED',
           timesheet_id = NULL,
           meta_json = (
             COALESCE(mq.meta_json, '{}'::jsonb)
               - 'deferred_target_timesheet_id'
               - 'materialised_to_timesheet_id'
               - 'materialisation_deferred_to_backend'
               - 'materialisation_deferred_at_utc'
               - 'materialised_storage_key'
               - 'materialised_at_utc'
               - 'deferred_rotation_degrees'
               - 'duplicate_of_queue_item_id'
               - 'duplicate_timesheet_evidence_identity'
               - 'materialisation_noop_reason'
           ) || jsonb_build_object(
             'contract_week_id', v_week.id::text,
             'staged_kind', 'TIMESHEET',
             'duplicate_timesheet_evidence_identity', true,
             'materialisation_noop_reason', 'same_storage_key_duplicate',
             'same_storage_duplicate_deactivated_at_utc', v_now
           )
     WHERE mq.id = ANY(v_duplicate_queue_ids);

    GET DIAGNOSTICS v_repaired_same_key_duplicate_count = ROW_COUNT;
  END IF;

  FOR v_stage_item IN
    SELECT item_value.value
    FROM jsonb_array_elements(v_stage_items) AS item_value(value)
  LOOP
    v_stage_item_kind := UPPER(COALESCE(NULLIF(BTRIM(v_stage_item ->> 'kind'), ''), 'OTHER'));
    IF v_stage_item_kind NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION','OTHER') THEN
      v_stage_item_kind := 'OTHER';
    END IF;
    v_stage_item_storage_key := NULLIF(regexp_replace(BTRIM(COALESCE(v_stage_item ->> 'storage_key', '')), '^/+', ''), '');
    IF v_stage_item_storage_key IS NULL THEN
      CONTINUE;
    END IF;

    v_stage_item_display_name := COALESCE(NULLIF(BTRIM(v_stage_item ->> 'display_name'), ''), regexp_replace(v_stage_item_storage_key, '^.*/', ''), 'file');
    v_stage_item_created_at := COALESCE(NULLIF(v_stage_item ->> 'created_at', '')::timestamp with time zone, v_now);
    v_stage_item_created_by := NULLIF(v_stage_item ->> 'created_by', '')::uuid;
    v_stage_item_rotation := COALESCE(NULLIF(v_stage_item ->> 'rotation_deg', '')::integer, 0);
    IF v_stage_item_rotation NOT IN (0, 90, 180, 270) THEN
      v_stage_item_rotation := 0;
    END IF;
    v_stage_item_timesheet_id := NULLIF(v_stage_item ->> 'timesheet_id', '')::uuid;
    v_stage_item_evidence_id := NULLIF(v_stage_item ->> 'evidence_id', '')::uuid;

    IF COALESCE(v_stage_item_created_by, p_actor_user_id) IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id', 'reason', 'actor required to recreate staged evidence provenance')::text;
    END IF;

    v_existing_queue := NULL;
    SELECT existing_candidate.id,
           existing_candidate.r2_key,
           existing_candidate.original_filename,
           existing_candidate.mime_type,
           existing_candidate.content_hash,
           existing_candidate.uploaded_by_user_id,
           existing_candidate.uploaded_at_utc,
           existing_candidate.status,
           existing_candidate.timesheet_id,
           existing_candidate.last_rotation_deg,
           existing_candidate.meta_json
      INTO v_existing_queue
    FROM (
      SELECT mq.*, CASE
        WHEN mq.status = 'STAGED'
         AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text THEN 1
        ELSE 0
      END AS prefer_active_staged
      FROM public.manual_timesheet_queue AS mq
      WHERE (
          mq.timesheet_id = ANY(v_all_timesheet_ids)
          OR (
            mq.status = 'STAGED'
            AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
          )
        )
        AND NULLIF(regexp_replace(COALESCE(
          NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'r2_key', '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'storage_key', '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'file_key', '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'canonical_key', '')), ''),
          ''
        ), '^/+', ''), '') = v_stage_item_storage_key
      ORDER BY prefer_active_staged DESC, mq.uploaded_at_utc ASC NULLS LAST, mq.id ASC
      LIMIT 1
      FOR UPDATE
    ) AS existing_candidate;

    v_clean_meta := COALESCE(v_existing_queue.meta_json, '{}'::jsonb)
      - 'deferred_target_timesheet_id'
      - 'materialised_to_timesheet_id'
      - 'materialisation_deferred_to_backend'
      - 'materialisation_deferred_at_utc'
      - 'materialised_storage_key'
      - 'materialised_at_utc'
      - 'deferred_rotation_degrees'
      - 'duplicate_of_queue_item_id'
      - 'duplicate_timesheet_evidence_identity'
      - 'materialisation_noop_reason';

    v_merged_meta := v_clean_meta || jsonb_build_object(
      'contract_week_id', v_week.id::text,
      'staged_kind', v_stage_item_kind,
      'dematerialised_from_timesheet_id', CASE WHEN v_stage_item_timesheet_id IS NULL THEN NULL ELSE v_stage_item_timesheet_id::text END,
      'dematerialised_from_booking_id', v_booking_id,
      'dematerialised_at_utc', v_now
    );

    IF v_existing_queue.id IS NOT NULL THEN
      UPDATE public.manual_timesheet_queue AS mq
         SET status = 'STAGED',
             timesheet_id = NULL,
             r2_key = v_stage_item_storage_key,
             original_filename = v_stage_item_display_name,
             uploaded_by_user_id = COALESCE(v_existing_queue.uploaded_by_user_id, v_stage_item_created_by, p_actor_user_id),
             uploaded_at_utc = COALESCE(v_existing_queue.uploaded_at_utc, v_stage_item_created_at, v_now),
             last_rotation_deg = COALESCE(v_stage_item_rotation, v_existing_queue.last_rotation_deg, 0)::smallint,
             meta_json = v_merged_meta
       WHERE mq.id = v_existing_queue.id;
    ELSE
      INSERT INTO public.manual_timesheet_queue (
        r2_key,
        original_filename,
        mime_type,
        content_hash,
        uploaded_by_user_id,
        uploaded_at_utc,
        status,
        timesheet_id,
        last_rotation_deg,
        meta_json
      )
      VALUES (
        v_stage_item_storage_key,
        v_stage_item_display_name,
        NULL,
        'DEMATERIALISED:' || COALESCE(v_stage_item_evidence_id::text, v_stage_item_storage_key),
        COALESCE(v_stage_item_created_by, p_actor_user_id),
        COALESCE(v_stage_item_created_at, v_now),
        'STAGED',
        NULL,
        v_stage_item_rotation::smallint,
        v_merged_meta
      );
    END IF;

    IF v_stage_item_kind = 'TIMESHEET' AND v_dematerialised_primary_timesheet_storage_key IS NULL THEN
      v_dematerialised_primary_timesheet_storage_key := v_stage_item_storage_key;
    END IF;
  END LOOP;

  v_staged_count := jsonb_array_length(v_stage_items);

  DELETE FROM public.timesheet_evidence AS ev
  WHERE ev.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_evidence_count = ROW_COUNT;

  WITH cleared AS (
    UPDATE public.pay_item_snoozes AS ps
       SET cleared_at_utc = v_now,
           cleared_by_user_id = p_actor_user_id,
           updated_at_utc = v_now,
           updated_by_user_id = p_actor_user_id
     WHERE ps.cleared_at_utc IS NULL
       AND ps.source_ref IS NULL
       AND (
         ps.timesheet_id = ANY(v_all_timesheet_ids)
         OR (v_booking_id IS NOT NULL AND ps.booking_id = v_booking_id)
       )
     RETURNING ps.id
  )
  SELECT COALESCE(array_agg(cleared.id ORDER BY cleared.id), ARRAY[]::uuid[])
    INTO v_cleared_snooze_ids
  FROM cleared;

  DELETE FROM public.ts_pdfs_outbox AS tpo
  WHERE tpo.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_ts_pdf_outbox_count = ROW_COUNT;

  DELETE FROM public.ts_financials_outbox AS tfo
  WHERE tfo.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_tsfin_outbox_count = ROW_COUNT;

  DELETE FROM public.timesheet_validations AS tv
  WHERE tv.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_validation_count = ROW_COUNT;

  DELETE FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_tsfin_count = ROW_COUNT;

  DELETE FROM public.timesheets AS ts
  WHERE ts.booking_id = v_booking_id;
  GET DIAGNOSTICS v_deleted_timesheet_count = ROW_COUNT;

  v_reopen_snapshot := UPPER(COALESCE(NULLIF(BTRIM(v_current_ts.submission_mode::text), ''), NULLIF(BTRIM(v_week.submission_mode_snapshot::text), ''), 'MANUAL'));
  IF v_reopen_snapshot <> 'ELECTRONIC' THEN
    v_reopen_snapshot := 'MANUAL';
  END IF;

  IF v_week.totals_json IS NOT NULL AND jsonb_typeof(v_week.totals_json) = 'object' THEN
    v_existing_totals_json := v_week.totals_json;
  ELSE
    v_existing_totals_json := '{}'::jsonb;
  END IF;

  IF v_current_ts.additional_units_week IS NOT NULL AND jsonb_typeof(v_current_ts.additional_units_week) = 'object' THEN
    SELECT COALESCE(jsonb_object_agg(week_units.key, to_jsonb((week_units.value_text)::numeric)), '{}'::jsonb)
      INTO v_additional_units_week_json
    FROM (
      SELECT UPPER(BTRIM(week_entry.key)) AS key,
             BTRIM(week_entry.value #>> '{}') AS value_text
      FROM jsonb_each(v_current_ts.additional_units_week) AS week_entry(key, value)
      WHERE NULLIF(BTRIM(week_entry.key), '') IS NOT NULL
        AND NULLIF(BTRIM(week_entry.value #>> '{}'), '') ~ '^-?[0-9]+([.][0-9]+)?$'
        AND (week_entry.value #>> '{}')::numeric > 0
    ) AS week_units;
  END IF;

  IF v_current_ts.additional_units_per_day IS NOT NULL AND jsonb_typeof(v_current_ts.additional_units_per_day) = 'object' THEN
    SELECT COALESCE(jsonb_object_agg(per_code.code, per_code.day_values), '{}'::jsonb)
      INTO v_additional_units_per_day_json
    FROM (
      SELECT
        UPPER(BTRIM(code_entry.key)) AS code,
        COALESCE(jsonb_object_agg(SUBSTRING(day_entry.key FROM 1 FOR 10), to_jsonb((day_entry.value #>> '{}')::numeric)), '{}'::jsonb) AS day_values
      FROM jsonb_each(v_current_ts.additional_units_per_day) AS code_entry(key, value)
      JOIN LATERAL jsonb_each(CASE WHEN jsonb_typeof(code_entry.value) = 'object' THEN code_entry.value ELSE '{}'::jsonb END) AS day_entry(key, value) ON true
      WHERE NULLIF(BTRIM(code_entry.key), '') IS NOT NULL
        AND SUBSTRING(day_entry.key FROM 1 FOR 10) ~ '^\d{4}-\d{2}-\d{2}$'
        AND NULLIF(BTRIM(day_entry.value #>> '{}'), '') ~ '^-?[0-9]+([.][0-9]+)?$'
        AND (day_entry.value #>> '{}')::numeric > 0
      GROUP BY UPPER(BTRIM(code_entry.key))
    ) AS per_code
    WHERE jsonb_typeof(per_code.day_values) = 'object'
      AND per_code.day_values <> '{}'::jsonb;
  END IF;

  v_expenses_draft_json := jsonb_build_object(
    'mileage_units', round(COALESCE(v_current_tsfin.mileage_units, 0), 2),
    'travel_pay', round(COALESCE(v_current_tsfin.travel_pay_ex_vat, 0), 2),
    'travel_charge', round(COALESCE(v_current_tsfin.travel_charge_ex_vat, 0), 2),
    'accommodation_pay', round(COALESCE(v_current_tsfin.accommodation_pay_ex_vat, 0), 2),
    'accommodation_charge', round(COALESCE(v_current_tsfin.accommodation_charge_ex_vat, 0), 2),
    'other_pay', round(COALESCE(v_current_tsfin.other_pay_ex_vat, 0), 2),
    'other_charge', round(COALESCE(v_current_tsfin.other_charge_ex_vat, 0), 2),
    'note', COALESCE(NULLIF(BTRIM(v_current_tsfin.expenses_description), ''), NULLIF(BTRIM(v_existing_totals_json #>> '{expenses_draft,note}'), ''), NULLIF(BTRIM(v_existing_totals_json #>> '{expenses_draft,notes}'), ''), '')
  );

  v_reopened_totals_json := v_existing_totals_json
    || jsonb_build_object(
      'hours', jsonb_build_object(
        'day', COALESCE(v_current_tsfin.hours_day, 0),
        'night', COALESCE(v_current_tsfin.hours_night, 0),
        'sat', COALESCE(v_current_tsfin.hours_sat, 0),
        'sun', COALESCE(v_current_tsfin.hours_sun, 0),
        'bh', COALESCE(v_current_tsfin.hours_bh, 0)
      ),
      'additional_units_week', COALESCE(v_additional_units_week_json, '{}'::jsonb),
      'additional_units_per_day', COALESCE(v_additional_units_per_day_json, '{}'::jsonb),
      'expenses_draft', v_expenses_draft_json
    );

  IF v_current_ts.day_references_json IS NOT NULL AND jsonb_typeof(v_current_ts.day_references_json) = 'object' THEN
    v_reopened_day_entries_json := v_current_ts.day_references_json;
  ELSE
    v_reopened_day_entries_json := '{}'::jsonb;
  END IF;

  IF v_current_ts.actual_schedule_json IS NOT NULL AND jsonb_typeof(v_current_ts.actual_schedule_json) = 'array' THEN
    v_reopened_planned_schedule_json := v_current_ts.actual_schedule_json;
  ELSE
    v_reopened_planned_schedule_json := NULL;
  END IF;

  UPDATE public.contract_weeks AS cw
     SET timesheet_id = NULL,
         status = 'OPEN'::public.contract_week_status_enum,
         submission_mode_snapshot = v_reopen_snapshot::public.submission_mode_enum,
         uploaded_pdf_r2_key = v_dematerialised_primary_timesheet_storage_key,
         planned_schedule_json = v_reopened_planned_schedule_json,
         totals_json = v_reopened_totals_json,
         day_entries_json = v_reopened_day_entries_json,
         updated_at = v_now
   WHERE cw.id = v_week.id
   RETURNING cw.* INTO v_week;

  v_after_signature_json := public.timesheet_lifecycle_signature_v1(NULL::uuid, v_week.id, false);
  v_signature_after_text := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', '')), '');

  PERFORM public._audit_insert(
    'contract_week',
    v_week.id::text,
    'CONTRACT_WEEK_MANUAL_TIMESHEET_UNPROCESSED',
    jsonb_build_object(
      'contract_week_id', v_week.id,
      'timesheet_id', v_current_ts.timesheet_id,
      'booking_id', v_booking_id,
      'previous_contract_week_status', v_previous_contract_week_status,
      'previous_processing_status', v_previous_processing_status,
      'previous_row_signature', v_current_row_signature
    ),
    jsonb_build_object(
      'contract_week_id', v_week.id,
      'timesheet_id', NULL,
      'booking_id', v_booking_id,
      'new_contract_week_status', v_week.status::text,
      'new_row_signature', v_signature_after_text,
      'staged_count', v_staged_count,
      'primary_timesheet_storage_key', v_dematerialised_primary_timesheet_storage_key,
      'deleted_timesheet_count', v_deleted_timesheet_count,
      'deleted_tsfin_count', v_deleted_tsfin_count,
      'cleared_snooze_count', COALESCE(array_length(v_cleared_snooze_ids, 1), 0)
    ),
    'WEEKLY_MANUAL_UNPROCESS',
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'operation', 'weekly_unprocess',
    'contract_week_id', v_week.id,
    'previous_timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', NULL,
    'deleted_timesheet_ids', to_jsonb(v_all_timesheet_ids),
    'deleted_timesheet_count', v_deleted_timesheet_count,
    'previous_contract_week_status', v_previous_contract_week_status,
    'new_contract_week_status', v_week.status::text,
    'previous_processing_status', v_previous_processing_status,
    'new_processing_status', 'UNPROCESSED',
    'backend_row_signature', v_signature_after_text,
    'row_signature', v_signature_after_text,
    'affected_rows', jsonb_build_array(jsonb_build_object(
      'contract_week_id', v_week.id,
      'previous_timesheet_id', v_current_ts.timesheet_id,
      'timesheet_id', NULL,
      'booking_id', v_booking_id,
      'row_key', 'contract_week:' || v_week.id::text
    )),
    'staged_evidence_summary', jsonb_build_object(
      'staged_count', v_staged_count,
      'primary_timesheet_storage_key', v_dematerialised_primary_timesheet_storage_key,
      'repaired_same_key_duplicate_count', v_repaired_same_key_duplicate_count
    ),
    'cleanup_summary', jsonb_build_object(
      'deleted_evidence_count', v_deleted_evidence_count,
      'deleted_tsfin_count', v_deleted_tsfin_count,
      'deleted_validation_count', v_deleted_validation_count,
      'deleted_ts_pdf_outbox_count', v_deleted_ts_pdf_outbox_count,
      'deleted_tsfin_outbox_count', v_deleted_tsfin_outbox_count,
      'cleared_snooze_ids', to_jsonb(v_cleared_snooze_ids)
    ),
    'cache_invalidation_hints', jsonb_build_object(
      'changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks', 'timesheet_evidence', 'manual_timesheet_queue'),
      'contract_week_id', v_week.id,
      'previous_timesheet_id', v_current_ts.timesheet_id,
      'booking_id', v_booking_id
    )
  );
EXCEPTION
  WHEN unique_violation THEN
    GET STACKED DIAGNOSTICS v_error_constraint = CONSTRAINT_NAME;
    IF v_error_constraint = 'uq_manual_timesheet_queue_one_active_staged_timesheet_per_contr' THEN
      RAISE EXCEPTION USING
        MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
        DETAIL = jsonb_build_object(
          'contract_week_id', p_week_id,
          'expected_timesheet_id', p_expected_timesheet_id,
          'constraint_name', v_error_constraint,
          'reason', 'active_staged_timesheet_uniqueness_race'
        )::text;
    END IF;
    RAISE;
  WHEN lock_not_available THEN
    RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('contract_week_id', p_week_id, 'expected_timesheet_id', p_expected_timesheet_id)::text;
END;
$function$;
