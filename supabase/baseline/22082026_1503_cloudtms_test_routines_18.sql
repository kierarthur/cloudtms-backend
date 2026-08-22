-- Immutable CloudTMS TEST function snapshot, page 18.
-- Generated from pg_get_functiondef; definitions only, with function body checks deferred for forward references.
-- Do not edit an applied baseline page. Add or replace routine authority in supabase/repeatable.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- timesheet_archive_state_v1(uuid)
CREATE OR REPLACE FUNCTION public.timesheet_archive_state_v1(p_timesheet_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_requested public.timesheets%ROWTYPE;
  v_current public.timesheets%ROWTYPE;
  v_actor_display text := NULL;
  v_actor_role text := NULL;
  v_signature_payload jsonb := '{}'::jsonb;
  v_signature text := NULL;
  v_tools_stage text := NULL;
  v_summary_stage text := NULL;
  v_processing_status text := NULL;
  v_paid_at_utc timestamptz := NULL;
  v_locked_by_invoice_id uuid := NULL;
  v_invoice_segments_locked integer := 0;
  v_contract_week_id uuid := NULL;
  v_pair_ids uuid[] := ARRAY[]::uuid[];
  v_pair_members jsonb := '[]'::jsonb;
  v_pair_state text := NULL;
  v_pair_fingerprint text := NULL;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ID_REQUIRED';
  END IF;

  SELECT t.*
    INTO v_requested
  FROM public.timesheets AS t
  WHERE t.timesheet_id = p_timesheet_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok', false, 'error_code', 'TARGET_NOT_FOUND');
  END IF;

  IF UPPER(COALESCE(v_current.adjustment_origin,''))='IMPORT_CORRECTION'
     AND v_current.correction_kind IN ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
     AND v_current.correction_id IS NOT NULL THEN
    SELECT COALESCE(array_agg(t.timesheet_id ORDER BY t.timesheet_id),ARRAY[]::uuid[]),
      COALESCE(jsonb_agg(jsonb_build_object(
        'timesheet_id',t.timesheet_id,'correction_kind',t.correction_kind,
        'is_archived',t.archived_at_utc IS NOT NULL,
        'archived_at_utc',t.archived_at_utc,
        'authorised',t.authorised_at_server IS NOT NULL,
        'invoice_linked',EXISTS(SELECT 1 FROM public.invoice_lines il WHERE il.timesheet_id=t.timesheet_id))
        ORDER BY CASE t.correction_kind WHEN 'CHANGED_HOURS_REVERSAL' THEN 1 ELSE 2 END),'[]'::jsonb)
    INTO v_pair_ids,v_pair_members
    FROM public.timesheets t
    WHERE t.correction_id=v_current.correction_id AND t.is_current
      AND t.correction_kind IN ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT');
    IF cardinality(v_pair_ids)=2
       AND (SELECT count(*) FROM public.timesheets t WHERE t.timesheet_id=ANY(v_pair_ids)
            AND t.correction_kind='CHANGED_HOURS_REVERSAL')=1
       AND (SELECT count(*) FROM public.timesheets t WHERE t.timesheet_id=ANY(v_pair_ids)
            AND t.correction_kind='CHANGED_HOURS_REPLACEMENT')=1 THEN
      SELECT CASE count(*) FILTER(WHERE archived_at_utc IS NOT NULL)
        WHEN 0 THEN 'PAIR_ACTIVE' WHEN 2 THEN 'PAIR_ARCHIVED'
        ELSE 'PAIR_PARTIALLY_ARCHIVED_BLOCKED' END
      INTO v_pair_state FROM public.timesheets WHERE timesheet_id=ANY(v_pair_ids);
      v_pair_fingerprint:=public._import_review_hash_v1(concat_ws('|',
        'correction-pair-archive-state-v1',v_current.correction_id,
        array_to_string(v_pair_ids,','),v_pair_members::text));
    ELSE
      v_pair_state:='PAIR_MALFORMED_BLOCKED';
    END IF;
  END IF;

  SELECT t.*
    INTO v_current
  FROM public.timesheets AS t
  WHERE t.booking_id = v_requested.booking_id
    AND t.is_current = true
  ORDER BY t.version DESC NULLS LAST,
           t.updated_at DESC NULLS LAST,
           t.created_at DESC NULLS LAST,
           t.timesheet_id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'CURRENT_TIMESHEET_NOT_FOUND',
      'requested_timesheet_id', p_timesheet_id
    );
  END IF;

  SELECT cw.id
    INTO v_contract_week_id
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current.timesheet_id
  ORDER BY cw.id
  LIMIT 1;

  IF v_current.archived_by_user_id IS NOT NULL THEN
    SELECT
      COALESCE(
        NULLIF(BTRIM(actor.display_name), ''),
        NULLIF(BTRIM(actor.email), ''),
        'a CloudTMS administrator'
      ),
      actor.role
    INTO v_actor_display, v_actor_role
    FROM public.tms_users AS actor
    WHERE actor.id = v_current.archived_by_user_id;
  END IF;

  v_signature_payload := public.timesheet_lifecycle_guard_signature_v1(
    v_current.timesheet_id,
    v_contract_week_id,
    false
  );
  v_signature := NULLIF(BTRIM(COALESCE(
    v_signature_payload ->> 'backend_row_signature',
    v_signature_payload ->> 'row_signature',
    v_signature_payload ->> 'signature',
    ''
  )), '');

  SELECT
    source_row.tools_stage,
    source_row.summary_stage,
    source_row.processing_status::text,
    source_row.paid_at_utc,
    source_row.locked_by_invoice_id,
    COALESCE(source_row.invoice_segments_locked, 0)
  INTO
    v_tools_stage,
    v_summary_stage,
    v_processing_status,
    v_paid_at_utc,
    v_locked_by_invoice_id,
    v_invoice_segments_locked
  FROM public.bulk_timesheet_workbench_row_source_v1(
    jsonb_build_object('timesheet_ids', jsonb_build_array(v_current.timesheet_id))
  ) AS source_row
  WHERE source_row.timesheet_id = v_current.timesheet_id
  LIMIT 1;

  RETURN jsonb_build_object(
    'ok', true,
    'requested_timesheet_id', p_timesheet_id,
    'current_timesheet_id', v_current.timesheet_id,
    'was_stale', v_current.timesheet_id IS DISTINCT FROM p_timesheet_id,
    'is_archived', v_current.archived_at_utc IS NOT NULL,
    'archived_at_utc', v_current.archived_at_utc,
    'archived_by_user_id', v_current.archived_by_user_id,
    'archived_by_display', CASE
      WHEN v_current.archived_at_utc IS NULL THEN NULL
      ELSE COALESCE(v_actor_display, 'a CloudTMS administrator')
    END,
    'archived_by_role', CASE WHEN v_current.archived_at_utc IS NULL THEN NULL ELSE v_actor_role END,
    'archived_reason_code', v_current.archived_reason_code,
    'is_current', v_current.is_current,
    'contract_id', v_current.contract_id,
    'contract_week_id', v_contract_week_id,
    'booking_id', v_current.booking_id,
    'authorised_at_server', v_current.authorised_at_server,
    'tools_stage', COALESCE(v_tools_stage, CASE WHEN v_current.archived_at_utc IS NOT NULL THEN 'ARCHIVED' ELSE NULL END),
    'summary_stage', v_summary_stage,
    'processing_status', v_processing_status,
    'paid_at_utc', v_paid_at_utc,
    'locked_by_invoice_id', v_locked_by_invoice_id,
    'invoice_segments_locked', v_invoice_segments_locked,
    'correction_pair',v_pair_state IS NOT NULL,
    'correction_id',CASE WHEN v_pair_state IS NOT NULL THEN v_current.correction_id END,
    'pair_state',v_pair_state,
    'pair_timesheet_ids',to_jsonb(v_pair_ids),
    'pair_members',v_pair_members,
    'pair_fingerprint',v_pair_fingerprint,
    'backend_row_signature', v_signature,
    'row_signature', v_signature
  );
END;
$function$;

-- timesheet_archive_transition_v1(uuid,text,text,uuid,uuid,text,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.timesheet_archive_transition_v1(p_timesheet_id uuid, p_action text, p_removal_kind text DEFAULT 'STANDARD_DELETE'::text, p_actor_user_id uuid DEFAULT NULL::uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_expected_row_signature text DEFAULT NULL::text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_action text := UPPER(NULLIF(BTRIM(COALESCE(p_action, '')), ''));
  v_kind text := UPPER(NULLIF(BTRIM(COALESCE(p_removal_kind, 'STANDARD_DELETE')), ''));
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_preview jsonb := '{}'::jsonb;
  v_recheck jsonb := '{}'::jsonb;
  v_decision text := NULL;
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_advance jsonb := '{}'::jsonb;
  v_archive_before jsonb := '{}'::jsonb;
  v_target_count integer := 0;
  v_archived_count integer := 0;
  v_unarchived_count integer := 0;
  v_already_archived_count integer := 0;
  v_actor_display text := NULL;
  v_actor_role text := NULL;
  v_current_timesheet_id uuid := NULL;
  v_primary_contract_week_id uuid := NULL;
  v_signature_payload jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_archive_capability_token uuid := NULL;
  v_remaining_capability_count integer := 0;
BEGIN
  PERFORM set_config('lock_timeout', '1500ms', true);

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ID_REQUIRED';
  END IF;
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'ACTOR_USER_ID_REQUIRED';
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EXPECTED_TIMESHEET_ID_REQUIRED',
      DETAIL = jsonb_build_object(
        'requested_timesheet_id', p_timesheet_id,
        'message', 'The current Timesheet identity is required. Refresh before continuing.'
      )::text;
  END IF;
  IF NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EXPECTED_ROW_SIGNATURE_REQUIRED',
      DETAIL = jsonb_build_object(
        'requested_timesheet_id', p_timesheet_id,
        'expected_timesheet_id', p_expected_timesheet_id,
        'message', 'The current Timesheet lifecycle signature is required. Refresh before continuing.'
      )::text;
  END IF;
  IF v_action IS NULL OR v_action NOT IN ('ARCHIVE', 'UNARCHIVE') THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_ARCHIVE_ACTION';
  END IF;
  IF v_kind IS NULL OR v_kind NOT IN (
    'STANDARD_DELETE',
    'WEEKLY_CHAIN_DELETE_PARENT',
    'WEEKLY_MANUAL_ADJUSTMENT_DELETE'
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_REMOVAL_KIND';
  END IF;

  SELECT
    COALESCE(
      NULLIF(BTRIM(actor.display_name), ''),
      NULLIF(BTRIM(actor.email), ''),
      'a CloudTMS administrator'
    ),
    actor.role
  INTO v_actor_display, v_actor_role
  FROM public.tms_users AS actor
  WHERE actor.id = p_actor_user_id
    AND actor.is_active = true;

  IF NOT FOUND THEN
    RAISE EXCEPTION USING MESSAGE = 'ACTOR_NOT_FOUND_OR_INACTIVE';
  END IF;

  IF v_kind = 'STANDARD_DELETE' THEN
    v_preview := public.timesheet_standard_delete_preview_v1(
      p_timesheet_id,
      p_actor_user_id,
      p_expected_timesheet_id,
      p_expected_row_signature
    );

    -- A caller may enter through the ordinary Delete/Unarchive route without
    -- knowing that the installed target resolver owns a weekly unit.  Follow
    -- the authoritative preview redirect rather than archiving only one row.
    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_preview -> 'blockers', '[]'::jsonb)) AS blocker(value)
      WHERE blocker.value ->> 'code' = 'WEEKLY_CHAIN_PREVIEW_REQUIRED'
    ) THEN
      v_kind := 'WEEKLY_CHAIN_DELETE_PARENT';
      v_preview := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);
    ELSIF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_preview -> 'blockers', '[]'::jsonb)) AS blocker(value)
      WHERE blocker.value ->> 'code' = 'WEEKLY_MANUAL_ADJUSTMENT_PREVIEW_REQUIRED'
    ) THEN
      v_kind := 'WEEKLY_MANUAL_ADJUSTMENT_DELETE';
      v_preview := public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id, p_actor_user_id);
    END IF;
  ELSIF v_kind = 'WEEKLY_CHAIN_DELETE_PARENT' THEN
    v_preview := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);
  ELSE
    v_preview := public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id, p_actor_user_id);
  END IF;

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_timesheet_ids
  FROM (
    SELECT value::uuid AS id
    FROM jsonb_array_elements_text(COALESCE(v_preview -> 'timesheet_ids', '[]'::jsonb)) AS values(value)
  ) AS parsed;

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_contract_week_ids
  FROM (
    SELECT value::uuid AS id
    FROM jsonb_array_elements_text(COALESCE(v_preview -> 'contract_week_ids', '[]'::jsonb)) AS values(value)
  ) AS parsed;

  IF COALESCE(array_length(v_timesheet_ids, 1), 0) = 0 THEN
    RETURN jsonb_build_object(
      'ok', false,
      'action', v_action,
      'decision', 'BLOCKED',
      'error_code', 'REMOVAL_UNIT_EMPTY',
      'preview', v_preview
    );
  END IF;

  IF array_length(v_timesheet_ids, 1) > 32 THEN
    RAISE EXCEPTION USING MESSAGE = 'REMOVAL_UNIT_TOO_LARGE';
  END IF;

  -- Lock the bounded unit in a deterministic order.  FK-backed writers that
  -- create new financial history cannot acquire a conflicting key-share lock
  -- until this transition commits or rolls back.
  PERFORM 1
  FROM public.timesheets AS t
  WHERE t.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY t.timesheet_id
  FOR UPDATE;

  SELECT COUNT(*)
    INTO v_target_count
  FROM public.timesheets AS t
  WHERE t.timesheet_id = ANY(v_timesheet_ids);

  IF v_target_count <> array_length(v_timesheet_ids, 1) THEN
    RAISE EXCEPTION USING MESSAGE = 'REMOVAL_UNIT_CHANGED';
  END IF;

  PERFORM 1
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = ANY(v_timesheet_ids)
    AND tf.is_current = true
  ORDER BY tf.timesheet_id, tf.id
  FOR UPDATE;

  PERFORM 1
  FROM public.contract_weeks AS cw
  WHERE cw.id = ANY(v_contract_week_ids)
  ORDER BY cw.id
  FOR UPDATE;



  IF v_kind = 'STANDARD_DELETE' THEN
    v_recheck := public.timesheet_standard_delete_preview_v1(
      p_timesheet_id,
      p_actor_user_id,
      p_expected_timesheet_id,
      p_expected_row_signature
    );
  ELSIF v_kind = 'WEEKLY_CHAIN_DELETE_PARENT' THEN
    v_recheck := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);
  ELSE
    v_recheck := public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id, p_actor_user_id);
  END IF;

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_recheck_timesheet_ids
  FROM (
    SELECT value::uuid AS id
    FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'timesheet_ids', '[]'::jsonb)) AS values(value)
  ) AS parsed;

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_recheck_contract_week_ids
  FROM (
    SELECT value::uuid AS id
    FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'contract_week_ids', '[]'::jsonb)) AS values(value)
  ) AS parsed;

  IF v_recheck_timesheet_ids IS DISTINCT FROM v_timesheet_ids
     OR v_recheck_contract_week_ids IS DISTINCT FROM v_contract_week_ids THEN
    RAISE EXCEPTION USING MESSAGE = 'REMOVAL_UNIT_CHANGED';
  END IF;

  v_current_timesheet_id := NULLIF(
    BTRIM(COALESCE(v_recheck ->> 'current_timesheet_id', '')),
    ''
  )::uuid;

  IF v_current_timesheet_id IS NULL
     OR NOT (v_current_timesheet_id = ANY(v_timesheet_ids)) THEN
    RETURN jsonb_build_object(
      'ok', false,
      'action', v_action,
      'decision', 'BLOCKED',
      'error_code', 'CURRENT_TIMESHEET_NOT_FOUND',
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids)
    );
  END IF;

  SELECT
    COUNT(*) FILTER (WHERE t.archived_at_utc IS NOT NULL),
    COALESCE(jsonb_object_agg(
      t.timesheet_id::text,
      jsonb_build_object(
        'archived_at_utc', t.archived_at_utc,
        'archived_by_user_id', t.archived_by_user_id,
        'archived_reason_code', t.archived_reason_code
      )
    ), '{}'::jsonb)
  INTO v_already_archived_count, v_archive_before
  FROM public.timesheets AS t
  WHERE t.timesheet_id = ANY(v_timesheet_ids);

  IF v_action = 'ARCHIVE'
     AND v_already_archived_count = array_length(v_timesheet_ids, 1) THEN
    RETURN jsonb_build_object(
      'ok', true,
      'action', 'ARCHIVE',
      'already_archived', true,
      'decision', 'ARCHIVED',
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids),
      'archive_state', public.timesheet_archive_state_v1(p_timesheet_id)
    );
  END IF;

  IF v_action = 'UNARCHIVE' AND v_already_archived_count = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'action', 'UNARCHIVE',
      'already_unarchived', true,
      'decision', 'ACTIVE',
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids),
      'advance_restored', false,
      'archive_state', public.timesheet_archive_state_v1(p_timesheet_id)
    );
  END IF;

  IF v_already_archived_count > 0
     AND v_already_archived_count <> array_length(v_timesheet_ids, 1) THEN
    RAISE EXCEPTION USING MESSAGE = CASE
      WHEN COALESCE((v_recheck->>'correction_pair')::boolean,false)
        THEN 'CORRECTION_PAIR_PARTIAL_ARCHIVE'
      ELSE 'ARCHIVE_UNIT_PARTIAL_STATE' END;
  END IF;

  IF v_action='UNARCHIVE' AND EXISTS (
    SELECT 1 FROM jsonb_array_elements(COALESCE(v_recheck->'blockers','[]'::jsonb)) blocker(value)
    WHERE blocker.value->>'code'='CORRECTION_PAIR_HAS_LATER_GENERATION'
  ) THEN
    RETURN jsonb_build_object(
      'ok',false,'action','UNARCHIVE','decision','BLOCKED',
      'error_code','CORRECTION_PAIR_UNARCHIVE_SUPERSEDED',
      'timesheet_ids',to_jsonb(v_timesheet_ids),
      'contract_week_ids',to_jsonb(v_contract_week_ids));
  END IF;

  -- Idempotent already-completed outcomes above deliberately precede the
  -- stale guards so a retry after an unknown response can reconcile safely
  -- without replaying any Archive, Unarchive, Advance or audit mutation.
  IF p_expected_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', false,
      'action', v_action,
      'decision', 'BLOCKED',
      'error_code', 'EXPECTED_TIMESHEET_MISMATCH',
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current_timesheet_id,
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids)
    );
  END IF;

  SELECT cw.id INTO v_primary_contract_week_id
  FROM public.contract_weeks cw
  WHERE cw.timesheet_id=v_current_timesheet_id
  ORDER BY cw.id LIMIT 1;
  v_signature_payload := public.timesheet_lifecycle_guard_signature_v1(
    v_current_timesheet_id,
    v_primary_contract_week_id,
    false
  );
  v_current_row_signature := NULLIF(BTRIM(COALESCE(
    v_signature_payload ->> 'backend_row_signature',
    v_signature_payload ->> 'row_signature',
    v_signature_payload ->> 'signature',
    ''
  )), '');

  IF v_current_row_signature IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'action', v_action,
      'decision', 'BLOCKED',
      'error_code', 'ROW_SIGNATURE_UNAVAILABLE',
      'current_timesheet_id', v_current_timesheet_id,
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids)
    );
  END IF;

  IF v_current_row_signature IS DISTINCT FROM BTRIM(p_expected_row_signature) THEN
    RETURN jsonb_build_object(
      'ok', false,
      'action', v_action,
      'decision', 'BLOCKED',
      'error_code', 'ROW_SIGNATURE_MISMATCH',
      'expected_row_signature', BTRIM(p_expected_row_signature),
      'current_row_signature', v_current_row_signature,
      'current_timesheet_id', v_current_timesheet_id,
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids)
    );
  END IF;

  IF v_action = 'ARCHIVE' THEN
    v_decision := COALESCE(v_recheck ->> 'decision', 'BLOCKED');

    IF v_decision = 'PERMANENT_DELETE' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'action', 'ARCHIVE',
        'decision', 'PERMANENT_DELETE',
        'error_code', 'ARCHIVE_NOT_REQUIRED',
        'timesheet_ids', to_jsonb(v_timesheet_ids),
        'contract_week_ids', to_jsonb(v_contract_week_ids)
      );
    END IF;

    IF v_decision <> 'ARCHIVE_REQUIRED' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'action', 'ARCHIVE',
        'decision', 'BLOCKED',
        'error_code', 'ARCHIVE_BLOCKED',
        'blockers', COALESCE(v_recheck -> 'blockers', v_recheck -> 'blocked_reasons', '[]'::jsonb),
        'timesheet_ids', to_jsonb(v_timesheet_ids),
        'contract_week_ids', to_jsonb(v_contract_week_ids)
      );
    END IF;

    v_advance := COALESCE(v_recheck -> 'advance', '{}'::jsonb);

    -- Archive is metadata-only. Active, clearable, consumed and historical
    -- Advance rows remain unchanged and continue to be reported by the preview.
    v_archive_capability_token := gen_random_uuid();

    INSERT INTO public.timesheet_archive_transition_capability (
      capability_token,
      backend_pid,
      transaction_id,
      timesheet_id,
      action
    )
    SELECT
      v_archive_capability_token,
      pg_backend_pid(),
      txid_current(),
      target_id,
      'ARCHIVE'
    FROM unnest(v_timesheet_ids) AS targets(target_id);

    PERFORM set_config(
      'cloudtms.archive_transition_capability',
      v_archive_capability_token::text,
      true
    );

    UPDATE public.timesheets AS t
       SET archived_at_utc = v_now,
           archived_by_user_id = p_actor_user_id,
           archived_reason_code = 'FINANCIAL_HISTORY_PREVENTED_DELETE',
           updated_at = v_now
     WHERE t.timesheet_id = ANY(v_timesheet_ids)
       AND t.archived_at_utc IS NULL;
    GET DIAGNOSTICS v_archived_count = ROW_COUNT;

    PERFORM set_config('cloudtms.archive_transition_capability', '', true);

    SELECT COUNT(*)
      INTO v_remaining_capability_count
    FROM public.timesheet_archive_transition_capability AS capability
    WHERE capability.capability_token = v_archive_capability_token
      AND capability.backend_pid = pg_backend_pid()
      AND capability.transaction_id = txid_current();

    IF v_remaining_capability_count <> 0 THEN
      RAISE EXCEPTION USING
        MESSAGE = 'ARCHIVE_CAPABILITY_NOT_CONSUMED',
        DETAIL = jsonb_build_object(
          'remaining_capabilities', v_remaining_capability_count,
          'target_count', array_length(v_timesheet_ids, 1)
        )::text;
    END IF;

    IF v_archived_count <> array_length(v_timesheet_ids, 1) THEN
      RAISE EXCEPTION USING MESSAGE = 'ARCHIVE_COUNT_MISMATCH';
    END IF;

    INSERT INTO public.audit_events (
      ts_utc,
      actor_user_id,
      actor_display,
      actor_role_at_time,
      object_type,
      object_id_text,
      action,
      before_json,
      after_json,
      reason
    )
    SELECT
      v_now,
      p_actor_user_id,
      v_actor_display,
      v_actor_role,
      'timesheets',
      target_id::text,
      'TIMESHEET_ARCHIVED',
      COALESCE(v_archive_before -> target_id::text, jsonb_build_object('archived_at_utc', NULL)),
      jsonb_build_object(
        'archived_at_utc', v_now,
        'archived_by_user_id', p_actor_user_id,
        'archived_reason_code', 'FINANCIAL_HISTORY_PREVENTED_DELETE',
        'target_unit_ids', to_jsonb(v_timesheet_ids),
        'active_advance_detected', COALESCE((v_advance ->> 'active')::boolean, false),
        'advance_unchanged', true,
        'related_pay_batch_id', v_advance -> 'related_pay_batch_id',
        'related_pay_batch_status', v_advance -> 'related_pay_batch_status'
      ),
      'FINANCIAL_HISTORY_PREVENTED_DELETE'
    FROM unnest(v_timesheet_ids) AS targets(target_id);

    RETURN jsonb_build_object(
      'ok', true,
      'action', 'ARCHIVE',
      'decision', 'ARCHIVED',
      'already_archived', false,
      'archived_count', v_archived_count,
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids),
      'advance', v_advance || jsonb_build_object(
        'unchanged', true
      ),
      'archive_state', public.timesheet_archive_state_v1(p_timesheet_id)
    );
  END IF;

  -- UNARCHIVE is metadata-only. It does not change Advance, authorisation,
  -- processing, TSFIN, pay-state, batch, invoice or contract data.
  v_archive_capability_token := gen_random_uuid();

  INSERT INTO public.timesheet_archive_transition_capability (
    capability_token,
    backend_pid,
    transaction_id,
    timesheet_id,
    action
  )
  SELECT
    v_archive_capability_token,
    pg_backend_pid(),
    txid_current(),
    target_id,
    'UNARCHIVE'
  FROM unnest(v_timesheet_ids) AS targets(target_id);

  PERFORM set_config(
    'cloudtms.archive_transition_capability',
    v_archive_capability_token::text,
    true
  );

  UPDATE public.timesheets AS t
     SET archived_at_utc = NULL,
         archived_by_user_id = NULL,
         archived_reason_code = NULL,
         updated_at = v_now
   WHERE t.timesheet_id = ANY(v_timesheet_ids)
     AND t.archived_at_utc IS NOT NULL;
  GET DIAGNOSTICS v_unarchived_count = ROW_COUNT;

  PERFORM set_config('cloudtms.archive_transition_capability', '', true);

  SELECT COUNT(*)
    INTO v_remaining_capability_count
  FROM public.timesheet_archive_transition_capability AS capability
  WHERE capability.capability_token = v_archive_capability_token
    AND capability.backend_pid = pg_backend_pid()
    AND capability.transaction_id = txid_current();

  IF v_remaining_capability_count <> 0 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ARCHIVE_CAPABILITY_NOT_CONSUMED',
      DETAIL = jsonb_build_object(
        'remaining_capabilities', v_remaining_capability_count,
        'target_count', array_length(v_timesheet_ids, 1)
      )::text;
  END IF;

  IF v_unarchived_count <> array_length(v_timesheet_ids, 1) THEN
    RAISE EXCEPTION USING MESSAGE = 'UNARCHIVE_COUNT_MISMATCH';
  END IF;

  INSERT INTO public.audit_events (
    ts_utc,
    actor_user_id,
    actor_display,
    actor_role_at_time,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  )
  SELECT
    v_now,
    p_actor_user_id,
    v_actor_display,
    v_actor_role,
    'timesheets',
    target_id::text,
    'TIMESHEET_UNARCHIVED',
    COALESCE(v_archive_before -> target_id::text, jsonb_build_object('archived_at_utc', true)),
    jsonb_build_object(
      'archived_at_utc', NULL,
      'archived_by_user_id', NULL,
      'archived_reason_code', NULL,
      'target_unit_ids', to_jsonb(v_timesheet_ids),
      'advance_restored', false,
      'advance_unchanged', true
    ),
    'USER_CONFIRMED_UNARCHIVE'
  FROM unnest(v_timesheet_ids) AS targets(target_id);

  RETURN jsonb_build_object(
    'ok', true,
    'action', 'UNARCHIVE',
    'decision', 'ACTIVE',
    'already_unarchived', false,
    'unarchived_count', v_unarchived_count,
    'timesheet_ids', to_jsonb(v_timesheet_ids),
    'contract_week_ids', to_jsonb(v_contract_week_ids),
    'advance_restored', false,
    'advance_unchanged', true,
    'archive_state', public.timesheet_archive_state_v1(p_timesheet_id)
  );
EXCEPTION
  WHEN lock_not_available OR deadlock_detected THEN
    RETURN jsonb_build_object(
      'ok', false,
      'action', v_action,
      'decision', 'BLOCKED',
      'error_code', 'LOCK_TIMEOUT',
      'message', 'The timesheet removal unit is currently being changed. Refresh and try again.'
    );
END;
$function$;

-- timesheet_archived_evidence_guard_v1()
CREATE OR REPLACE FUNCTION public.timesheet_archived_evidence_guard_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_old_timesheet_id uuid := CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN OLD.timesheet_id ELSE NULL END;
  v_new_timesheet_id uuid := CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN NEW.timesheet_id ELSE NULL END;
  v_timesheet_ids uuid[] := array_remove(ARRAY[v_old_timesheet_id, v_new_timesheet_id]::uuid[], NULL);
  v_archived_timesheet_id uuid := NULL;
BEGIN
  PERFORM 1
  FROM public.timesheets AS t
  WHERE t.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY t.timesheet_id
  FOR KEY SHARE OF t;

  SELECT t.timesheet_id
    INTO v_archived_timesheet_id
  FROM public.timesheets AS t
  WHERE t.archived_at_utc IS NOT NULL
    AND t.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY t.timesheet_id
  LIMIT 1;

  IF v_archived_timesheet_id IS NOT NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_ARCHIVED',
      DETAIL = jsonb_build_object(
        'timesheet_id', v_archived_timesheet_id,
        'reason', 'archived_timesheet_evidence_is_read_only'
      )::text;
  END IF;

  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$function$;

-- timesheet_audit_feed(uuid)
CREATE OR REPLACE FUNCTION public.timesheet_audit_feed(p_timesheet_id uuid)
 RETURNS TABLE(id uuid, ts_utc timestamp with time zone, actor_user_id uuid, actor_display text, actor_role_at_time text, object_type text, object_id_text text, action text, before_json jsonb, after_json jsonb, reason text, ip text, user_agent text, correlation_id text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  with base as (
    select t.booking_id
    from public.timesheets t
    where t.timesheet_id = p_timesheet_id
    limit 1
  ),
  cur as (
    select t.timesheet_id as current_timesheet_id
    from public.timesheets t
    join base b
      on b.booking_id = t.booking_id
    where t.is_current = true
    limit 1
  ),
  ts_ids as (
    select t.timesheet_id::text as ts_id_text
    from public.timesheets t
    join base b
      on b.booking_id = t.booking_id

    union all

    select p_timesheet_id::text
    where not exists (select 1 from base)
  ),
  cw as (
    select cw1.id::text as cw_id_text
    from public.contract_weeks cw1
    where cw1.timesheet_id = (select c.current_timesheet_id from cur c)
    order by cw1.updated_at desc nulls last, cw1.week_ending_date desc
    limit 1
  ),
  ae_scope as (
    select
      ae.id,
      ae.ts_utc,
      ae.actor_user_id,
      ae.actor_display,
      ae.actor_role_at_time,
      ae.object_type,
      ae.object_id_text,
      ae.action,
      ae.before_json,
      ae.after_json,
      ae.reason,
      ae.ip,
      ae.user_agent,
      ae.correlation_id,
      coalesce(ae.after_json, ae.before_json, '{}'::jsonb) as payload_json
    from public.audit_events ae
  )
  select
    aes.id,
    aes.ts_utc,
    aes.actor_user_id,
    coalesce(aes.actor_display, tu.display_name, tu.email, 'CloudTMS server') as actor_display,
    coalesce(aes.actor_role_at_time, tu.role, 'system') as actor_role_at_time,
    aes.object_type,
    aes.object_id_text,
    aes.action,
    aes.before_json,
    aes.after_json,
    aes.reason,
    aes.ip,
    aes.user_agent,
    aes.correlation_id
  from ae_scope aes
  left join public.tms_users tu
    on tu.id = aes.actor_user_id
  where
    (
      aes.object_type in ('timesheet', 'timesheets')
      and aes.object_id_text in (
        select ti.ts_id_text
        from ts_ids ti
      )
    )
    or
    (
      exists (
        select 1
        from cw
      )
      and aes.object_type in ('contract_week', 'contract_weeks')
      and aes.object_id_text = (
        select c2.cw_id_text
        from cw c2
      )
    )
    or
    (
      aes.object_type in ('manual_timesheet_queue', 'manual_timesheet_queues')
      and aes.action in (
        'MANUAL_TIMESHEET_QUEUE_ATTACHED',
        'MANUAL_TIMESHEET_QUEUE_STAGED',
        'MANUAL_TIMESHEET_QUEUE_STAGED_KIND_UPDATED',
        'MANUAL_TIMESHEET_QUEUE_STAGED_RETURNED_TO_QUEUE',
        'MANUAL_TIMESHEET_QUEUE_STAGED_DELETED'
      )
      and (
        trim(coalesce(aes.payload_json ->> 'current_timesheet_id', '')) in (
          select ti.ts_id_text
          from ts_ids ti
        )
        or trim(coalesce(aes.payload_json ->> 'requested_timesheet_id', '')) in (
          select ti.ts_id_text
          from ts_ids ti
        )
        or trim(coalesce(aes.payload_json ->> 'timesheet_id', '')) in (
          select ti.ts_id_text
          from ts_ids ti
        )
        or trim(coalesce(aes.payload_json ->> 'materialised_to_timesheet_id', '')) in (
          select ti.ts_id_text
          from ts_ids ti
        )
        or trim(coalesce(aes.payload_json ->> 'dematerialised_from_timesheet_id', '')) in (
          select ti.ts_id_text
          from ts_ids ti
        )
        or (
          exists (
            select 1
            from base b2
          )
          and trim(coalesce(aes.payload_json ->> 'dematerialised_from_booking_id', '')) = (
            select b3.booking_id
            from base b3
          )
        )
        or (
          exists (
            select 1
            from cw
          )
          and trim(coalesce(aes.payload_json ->> 'contract_week_id', '')) = (
            select c3.cw_id_text
            from cw c3
          )
        )
      )
    )
  order by aes.ts_utc desc, aes.id desc
  limit 500;
$function$;

-- timesheet_authorise_bulk_atomic(jsonb,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.timesheet_authorise_bulk_atomic(p_items jsonb DEFAULT '[]'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_items_array jsonb := '[]'::jsonb;
  v_requested_count integer := 0;
  v_success_count integer := 0;
  v_failure_count integer := 0;
  v_uuid_re text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
  v_out jsonb := '{}'::jsonb;
  v_error_state text := NULL;
  v_capability_items jsonb := NULL;
BEGIN
  PERFORM set_config('lock_timeout', '300ms', true);

  IF p_actor_user_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;
  IF p_items IS NOT NULL AND jsonb_typeof(p_items) NOT IN ('array', 'object') THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'ITEMS_JSON_MUST_BE_ARRAY_OR_OBJECT', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;

  v_items_array := CASE
    WHEN p_items IS NULL THEN '[]'::jsonb
    WHEN jsonb_typeof(p_items) = 'array' THEN p_items
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'items') = 'array' THEN p_items -> 'items'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'rows') = 'array' THEN p_items -> 'rows'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selected') = 'array' THEN p_items -> 'selected'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selections') = 'array' THEN p_items -> 'selections'
    WHEN jsonb_typeof(p_items) = 'object' THEN jsonb_build_array(p_items)
    ELSE '[]'::jsonb
  END;
  IF to_regclass('pg_temp.import_review_lifecycle_capability_v1') IS NOT NULL
     AND nullif(current_setting('cloudtms.import_reconciliation_capability_token',true),'') IS NOT NULL THEN
    IF coalesce(current_setting('request.jwt.claim.role',true),
         nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','') <> 'service_role' THEN
      RAISE EXCEPTION 'IMPORT_REVIEW_LIFECYCLE_CAPABILITY_INVALID' USING ERRCODE='42501';
    END IF;
    SELECT coalesce(jsonb_agg(jsonb_build_object(
      'timesheet_id',c.timesheet_id,'expected_timesheet_id',c.expected_timesheet_id,
      'expected_row_signature',c.expected_row_signature) ORDER BY c.timesheet_id),'[]'::jsonb)
    INTO v_capability_items
    FROM pg_temp.import_review_lifecycle_capability_v1 c
    WHERE c.capability_token=current_setting('cloudtms.import_reconciliation_capability_token',true)
      AND c.txid=txid_current() AND c.actor_user_id=p_actor_user_id AND c.action='AUTHORISE';
    IF jsonb_array_length(v_capability_items)=0 OR
       (SELECT array_agg(distinct nullif(x->>'timesheet_id','')::uuid ORDER BY nullif(x->>'timesheet_id','')::uuid)
          FROM jsonb_array_elements(v_items_array) x)
       IS DISTINCT FROM
       (SELECT array_agg(distinct nullif(x->>'timesheet_id','')::uuid ORDER BY nullif(x->>'timesheet_id','')::uuid)
          FROM jsonb_array_elements(v_capability_items) x) THEN
      RAISE EXCEPTION 'IMPORT_REVIEW_LIFECYCLE_CAPABILITY_ITEM_SET_MISMATCH' USING ERRCODE='22023';
    END IF;
    v_items_array:=v_capability_items;
  ELSE
    v_items_array := public._ctms_expand_lifecycle_items_v1(v_items_array, 'AUTHORISE', p_actor_user_id, 100);
  END IF;
  v_requested_count := jsonb_array_length(v_items_array);
  IF v_requested_count > 100 THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'TOO_MANY_ITEMS', 'requested_count', v_requested_count, 'success_count', 0, 'failure_count', v_requested_count, 'results', '[]'::jsonb);
  END IF;

  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_items;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_state;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_work;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_ts;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_tf;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_cw;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_results;

  CREATE TEMP TABLE timesheet_authorise_bulk_items ON COMMIT DROP AS
  SELECT
    input_values.ordinality::integer AS ordinal,
    CASE WHEN jsonb_typeof(input_values.item_json) = 'object' THEN input_values.item_json ELSE jsonb_build_object('value', input_values.item_json) END AS item_json,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'row_key', input_values.item_json ->> 'rowKey', '')), '') AS row_key,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'timesheet_id', input_values.item_json ->> 'timesheetId', input_values.item_json ->> 'current_timesheet_id', input_values.item_json ->> 'currentTimesheetId', input_values.item_json ->> 'requested_timesheet_id', input_values.item_json ->> 'requestedTimesheetId', '')), '') AS timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'expected_timesheet_id', input_values.item_json ->> 'expectedTimesheetId', input_values.item_json ->> 'expected_current_timesheet_id', input_values.item_json ->> 'expectedCurrentTimesheetId', '')), '') AS expected_timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'backend_row_signature', input_values.item_json ->> 'row_signature', input_values.item_json ->> 'rowSignature', input_values.item_json ->> 'expected_row_signature', input_values.item_json ->> 'expectedRowSignature', '')), '') AS expected_row_signature
  FROM jsonb_array_elements(v_items_array) WITH ORDINALITY AS input_values(item_json, ordinality);

  CREATE TEMP TABLE timesheet_authorise_bulk_state ON COMMIT DROP AS
  SELECT
    item_rows.ordinal,
    item_rows.item_json,
    item_rows.row_key,
    CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END AS requested_timesheet_id,
    CASE WHEN item_rows.expected_timesheet_id_text ~* v_uuid_re THEN item_rows.expected_timesheet_id_text::uuid ELSE NULL::uuid END AS expected_timesheet_id,
    item_rows.expected_row_signature,
    req_ts.timesheet_id AS db_requested_timesheet_id,
    req_ts.booking_id AS requested_booking_id,
    cur_ts.timesheet_id AS current_timesheet_id,
    cur_ts.archived_at_utc AS current_archived_at_utc,
    cur_ts.booking_id AS current_booking_id,
    cur_ts.version AS current_version,
    cur_ts.is_current AS current_is_current,
    cur_ts.authorised_at_server AS current_authorised_at_server,
    cur_ts.qr_status AS current_qr_status,
    cur_ts.qr_token AS current_qr_token,
    cur_ts.qr_generated_at AS current_qr_generated_at,
    cur_ts.qr_scanned_at AS current_qr_scanned_at,
    cur_ts.sheet_scope AS current_sheet_scope,
    cur_ts.contract_id AS current_contract_id,
    tf.id AS tsfin_id,
    tf.processing_status AS tsfin_processing_status,
    tf.basis AS tsfin_basis,
    tf.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
    tf.paid_at_utc AS tsfin_paid_at_utc,
    tf.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
    tf.authorised_at_utc AS tsfin_authorised_at_utc,
    COALESCE(summary_row.client_requires_hr, contract_row.requires_hr, false) AS client_requires_hr,
    COALESCE(summary_row.hr_validation_required_for_invoice, contract_row.requires_hr, false) AS hr_validation_required_for_invoice,
    COALESCE(summary_row.validation_status_text, NULL::text) AS summary_validation_status,
    cw.id AS contract_week_id,
    cw.status AS contract_week_status,
    sig.signature_json AS signature_json,
    sig.signature_text AS current_row_signature,
    COALESCE(segment_state.has_segment_invoice_lock, false) AS has_segment_invoice_lock,
    COALESCE(validation_state.validation_ok, false) AS validation_ok
  FROM pg_temp.timesheet_authorise_bulk_items AS item_rows
  LEFT JOIN LATERAL (
    SELECT ts_req.*
    FROM public.timesheets AS ts_req
    WHERE ts_req.timesheet_id = CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END
    LIMIT 1
    FOR UPDATE
  ) AS req_ts ON true
  LEFT JOIN LATERAL (
    SELECT ts_cur.*
    FROM public.timesheets AS ts_cur
    WHERE req_ts.booking_id IS NOT NULL
      AND ts_cur.booking_id = req_ts.booking_id
    ORDER BY CASE WHEN ts_cur.is_current THEN 0 ELSE 1 END, ts_cur.version DESC NULLS LAST, ts_cur.updated_at DESC NULLS LAST, ts_cur.timesheet_id DESC
    LIMIT 1
    FOR UPDATE
  ) AS cur_ts ON true
  LEFT JOIN LATERAL (
    SELECT tf_sel.*
    FROM public.timesheets_financials AS tf_sel
    WHERE tf_sel.timesheet_id = cur_ts.timesheet_id
      AND tf_sel.is_current = true
    ORDER BY tf_sel.computed_at_utc DESC NULLS LAST, tf_sel.updated_at DESC NULLS LAST, tf_sel.created_at DESC NULLS LAST, tf_sel.id DESC
    LIMIT 1
    FOR UPDATE
  ) AS tf ON true
  LEFT JOIN LATERAL (
    SELECT c.requires_hr
    FROM public.contracts AS c
    WHERE c.id = cur_ts.contract_id
    LIMIT 1
  ) AS contract_row ON true
  LEFT JOIN LATERAL (
    SELECT
      COALESCE(vts.client_requires_hr, false) AS client_requires_hr,
      COALESCE(vts.hr_validation_required_for_invoice, false) AS hr_validation_required_for_invoice,
      CASE
        WHEN vts.validation_status IS NULL THEN NULL::text
        ELSE UPPER(vts.validation_status::text)
      END AS validation_status_text
    FROM public.v_timesheets_summary_base AS vts
    WHERE vts.timesheet_id = cur_ts.timesheet_id
    LIMIT 1
  ) AS summary_row ON true
  LEFT JOIN LATERAL (
    SELECT cw_sel.*
    FROM public.contract_weeks AS cw_sel
    WHERE cw_sel.timesheet_id = cur_ts.timesheet_id
       OR EXISTS (
         SELECT 1
         FROM public.timesheets AS cw_ts
         WHERE cw_ts.timesheet_id = cw_sel.timesheet_id
           AND cw_ts.booking_id = cur_ts.booking_id
       )
    ORDER BY CASE WHEN cw_sel.timesheet_id = cur_ts.timesheet_id THEN 0 ELSE 1 END,
             cw_sel.updated_at DESC NULLS LAST,
             cw_sel.id DESC
    LIMIT 1
    FOR UPDATE OF cw_sel
  ) AS cw ON true
  LEFT JOIN LATERAL (
    SELECT public.timesheet_lifecycle_signature_v1(cur_ts.timesheet_id, cw.id, false) AS signature_json
  ) AS sig_raw ON true
  LEFT JOIN LATERAL (
    SELECT sig_raw.signature_json AS signature_json,
           NULLIF(BTRIM(COALESCE(sig_raw.signature_json ->> 'backend_row_signature', sig_raw.signature_json ->> 'row_signature', sig_raw.signature_json ->> 'signature', '')), '') AS signature_text
  ) AS sig ON true
  LEFT JOIN LATERAL (
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'array' THEN tf.invoice_breakdown_json
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'object' AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments') = 'array' THEN tf.invoice_breakdown_json -> 'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
    ) AS has_segment_invoice_lock
  ) AS segment_state ON true
  LEFT JOIN LATERAL (
    SELECT COALESCE(UPPER(COALESCE(summary_row.validation_status_text, tv.status::text)) IN ('VALIDATION_OK', 'OVERRIDDEN'), false) AS validation_ok
    FROM public.timesheet_validations AS tv
    WHERE tv.timesheet_id = cur_ts.timesheet_id
    ORDER BY tv.updated_at DESC NULLS LAST, tv.created_at DESC NULLS LAST, tv.id DESC
    LIMIT 1
  ) AS validation_state ON true;

  CREATE TEMP TABLE timesheet_authorise_bulk_work ON COMMIT DROP AS
  SELECT
    state_rows.*,
    CASE
      WHEN state_rows.requested_timesheet_id IS NULL THEN 'TIMESHEET_ID_REQUIRED'
      WHEN state_rows.expected_timesheet_id IS NULL THEN 'EXPECTED_TIMESHEET_ID_REQUIRED'
      WHEN state_rows.db_requested_timesheet_id IS NULL THEN 'TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_timesheet_id IS NULL THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_is_current IS DISTINCT FROM true THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.expected_timesheet_id IS DISTINCT FROM state_rows.current_timesheet_id THEN 'TIMESHEET_MOVED'
      WHEN state_rows.tsfin_id IS NULL THEN 'NO_TSFIN'
      WHEN state_rows.expected_row_signature IS NOT NULL AND COALESCE(state_rows.current_row_signature, '') IS DISTINCT FROM state_rows.expected_row_signature THEN 'ROW_SIGNATURE_MISMATCH'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_id IS NULL THEN 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'INVOICED'::public.contract_week_status_enum THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'CANCELLED'::public.contract_week_status_enum THEN 'CONTRACT_WEEK_NOT_AUTHORISABLE'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'AUTHORISED'::public.contract_week_status_enum THEN 'ALREADY_AUTHORISED'
      WHEN state_rows.current_archived_at_utc IS NOT NULL THEN 'TIMESHEET_ARCHIVED'
      WHEN state_rows.tsfin_locked_by_invoice_id IS NOT NULL OR state_rows.has_segment_invoice_lock THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_authorised_at_server IS NOT NULL OR state_rows.tsfin_authorised_at_utc IS NOT NULL THEN 'ALREADY_AUTHORISED'
      WHEN state_rows.tsfin_processing_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN 'AUTHORISE_NOT_ALLOWED'
      ELSE NULL::text
    END AS failure_code,
    CASE
      WHEN state_rows.tsfin_basis IN ('NHSP'::public.timesheet_fin_basis_enum, 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum) THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.hr_validation_required_for_invoice, false) AND NOT COALESCE(state_rows.validation_ok, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.validation_ok, false) THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.client_requires_hr, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
      ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    END AS new_processing_status
  FROM pg_temp.timesheet_authorise_bulk_state AS state_rows;

  -- A linked correction pair is one lifecycle unit.  An already-authorised
  -- sibling in a repairable legacy mixed state is idempotent, while any real
  -- blocker is propagated to both members before either member is mutated.
  UPDATE pg_temp.timesheet_authorise_bulk_work work_rows
     SET failure_code=NULL
   WHERE NULLIF(work_rows.item_json->>'lifecycle_group_id','') IS NOT NULL
     AND work_rows.failure_code='ALREADY_AUTHORISED';

  UPDATE pg_temp.timesheet_authorise_bulk_work work_rows
     SET failure_code='CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED'
   WHERE NULLIF(work_rows.item_json->>'lifecycle_group_id','') IS NOT NULL
     AND EXISTS (
       SELECT 1 FROM pg_temp.timesheet_authorise_bulk_work blocked
       WHERE blocked.item_json->>'lifecycle_group_id'=work_rows.item_json->>'lifecycle_group_id'
         AND blocked.failure_code IS NOT NULL
     );

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_ts ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets AS ts_upd
       SET authorised_at_server = v_now,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
     WHERE work_rows.failure_code IS NULL
       AND ts_upd.timesheet_id = work_rows.current_timesheet_id
       AND ts_upd.is_current = true
     RETURNING ts_upd.timesheet_id, ts_upd.version, ts_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_tf ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets_financials AS tf_upd
       SET processing_status = work_rows.new_processing_status,
           authorised_by_user_id = p_actor_user_id,
           authorised_at_utc = v_now,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_authorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND tf_upd.id = work_rows.tsfin_id
       AND tf_upd.is_current = true
     RETURNING tf_upd.timesheet_id, tf_upd.processing_status, tf_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_cw ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.contract_weeks AS cw_upd
       SET status = 'AUTHORISED'::public.contract_week_status_enum,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND cw_upd.id = work_rows.contract_week_id
     RETURNING cw_upd.id, cw_upd.timesheet_id, cw_upd.status, cw_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  IF EXISTS (
    SELECT 1
    FROM pg_temp.timesheet_authorise_bulk_work work_rows
    JOIN public.timesheets current_pair
      ON current_pair.timesheet_id=work_rows.current_timesheet_id
    JOIN public.timesheets_financials current_tf
      ON current_tf.timesheet_id=current_pair.timesheet_id AND current_tf.is_current=true
    WHERE NULLIF(work_rows.item_json->>'lifecycle_group_id','') IS NOT NULL
      AND work_rows.failure_code IS NULL
    GROUP BY work_rows.item_json->>'lifecycle_group_id',
             (work_rows.item_json->>'lifecycle_group_size')::integer
    HAVING count(*)<>(work_rows.item_json->>'lifecycle_group_size')::integer
       OR count(*) FILTER (WHERE current_pair.authorised_at_server IS NOT NULL
                            AND current_tf.authorised_at_utc IS NOT NULL)<>count(*)
  ) THEN
    RAISE EXCEPTION 'CORRECTION_PAIR_LIFECYCLE_POSTCONDITION_FAILED' USING ERRCODE='P0001';
  END IF;

  PERFORM public._audit_insert(
    'timesheet_batch',
    'bulk_authorise:' || v_now::text,
    'TIMESHEET_BULK_AUTHORISED',
    jsonb_build_object('requested_count', v_requested_count, 'actor_user_id', p_actor_user_id),
    jsonb_build_object(
      'succeeded_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
      'failed_items', COALESCE((SELECT jsonb_agg(jsonb_build_object('item_index', work_rows.ordinal, 'timesheet_id', work_rows.requested_timesheet_id, 'error_code', work_rows.failure_code) ORDER BY work_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_work AS work_rows WHERE work_rows.failure_code IS NOT NULL), '[]'::jsonb)
    ),
    'BULK_AUTHORISE',
    p_actor_user_id
  );

  CREATE TEMP TABLE timesheet_authorise_bulk_results ON COMMIT DROP AS
  SELECT
    work_rows.ordinal,
    (work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL) AS success,
    jsonb_build_object(
      'item_index', work_rows.ordinal,
      'success', work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL,
      'action', 'AUTHORISE',
      'error_code', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN NULL ELSE COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') END,
      'requested_timesheet_id', work_rows.requested_timesheet_id,
      'expected_timesheet_id', work_rows.expected_timesheet_id,
      'expected_row_signature', work_rows.expected_row_signature,
      'current_row_signature', work_rows.current_row_signature,
      'current_timesheet_id', work_rows.current_timesheet_id,
      'current_version', COALESCE(updated_ts.version, work_rows.current_version),
      'processing_status_before', work_rows.tsfin_processing_status::text,
      'processing_status_after', CASE WHEN updated_tf.processing_status IS NULL THEN NULL ELSE updated_tf.processing_status::text END,
      'contract_week_id', work_rows.contract_week_id,
      'lifecycle_group_id', NULLIF(work_rows.item_json ->> 'lifecycle_group_id', ''),
      'pair_fingerprint', NULLIF(work_rows.item_json ->> 'pair_fingerprint', ''),
      'affected_rows', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN jsonb_build_array(jsonb_build_object('timesheet_id', work_rows.current_timesheet_id, 'contract_week_id', work_rows.contract_week_id, 'booking_id', work_rows.current_booking_id, 'row_key', 'timesheet:' || work_rows.current_timesheet_id::text)) ELSE '[]'::jsonb END
    ) AS result_json
  FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
  LEFT JOIN pg_temp.timesheet_authorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id;

  SELECT COUNT(*) FILTER (WHERE result_rows.success)::integer,
         COUNT(*) FILTER (WHERE NOT result_rows.success)::integer
    INTO v_success_count, v_failure_count
  FROM pg_temp.timesheet_authorise_bulk_results AS result_rows;

  SELECT jsonb_build_object(
    'ok', true,
    'batch_completed', true,
    'all_success', v_failure_count = 0,
    'action', 'AUTHORISE',
    'requested_count', v_requested_count,
    'success_count', v_success_count,
    'failure_count', v_failure_count,
    'has_failures', v_failure_count > 0,
    'results', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows), '[]'::jsonb),
    'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
    'failed_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows WHERE result_rows.success = false), '[]'::jsonb),
    'stale_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows WHERE result_rows.result_json ->> 'error_code' = 'ROW_SIGNATURE_MISMATCH'), '[]'::jsonb),
    'count_deltas', jsonb_build_object('processed_eligible', -v_success_count, 'authorised_eligible', v_success_count, 'total', 0),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks'), 'datasets', jsonb_build_array('bulk_authorise'), 'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb))
  ) INTO v_out;

  RETURN v_out;
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'LOCK_TIMEOUT', 'requested_count', COALESCE(v_requested_count, 0), 'success_count', 0, 'failure_count', COALESCE(v_requested_count, 0), 'results', '[]'::jsonb);
  END IF;
  RAISE;
END;
$function$;

-- timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamp with time zone,text)
CREATE OR REPLACE FUNCTION public.timesheet_authorise_generic_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_contract_week public.contract_weeks%ROWTYPE;
  v_prev_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := NULL;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_has_segment_invoice_lock boolean := false;
  v_validation_status text := NULL;
  v_validation_pre_validated boolean := false;
  v_validation_ok boolean := false;
  v_contract_requires_hr boolean := false;
  v_client_requires_hr boolean := false;
  v_hr_validation_required_for_invoice boolean := false;
  v_must_hold_for_hr_validation boolean := false;
  v_prevalidated_fast_track boolean := false;
  v_force_ready_for_invoice boolean := false;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_diag_started_at timestamptz := clock_timestamp();
  v_temp_log_enabled boolean := false;
  v_advance_state_refresh_json jsonb := '{}'::jsonb;
  v_has_uncleared_advance_override boolean := false;
BEGIN

  if coalesce((public._ctms_import_correction_classify_v1(p_timesheet_id)
       ->> 'is_import_authoritative_correction')::boolean, false) then
    return jsonb_build_object(
      'ok', false,
      'error_code', 'IMPORT_CORRECTION_UNIT_REQUIRES_BULK_LIFECYCLE',
      'timesheet_id', p_timesheet_id,
      'required_rpc', case when 'AUTHORISE' = 'AUTHORISE'
        then 'timesheet_authorise_bulk_atomic' else 'timesheet_unauthorise_bulk_atomic' end
    );
  end if;
  PERFORM set_config('lock_timeout', '300ms', true);

  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_temp_log_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      v_temp_log_enabled := false;
    WHEN OTHERS THEN
      v_temp_log_enabled := false;
  END;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'entry',
      'timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'actor_user_id_present', p_actor_user_id IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_id')::text;
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
  END IF;
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id')::text;
  END IF;

  SELECT ts.*
    INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF v_requested_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_requested_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'requested_timesheet_resolved',
      'timesheet_id', v_requested_ts.timesheet_id,
      'is_current', COALESCE(v_requested_ts.is_current, false),
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF COALESCE(v_requested_ts.is_current, false) THEN
    v_current_ts := v_requested_ts;
  ELSE
    SELECT ts.*
      INTO v_current_ts
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_requested_ts.booking_id
      AND ts.is_current = true
    ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;
    IF v_current_ts.timesheet_id IS NULL THEN
      v_current_ts := v_requested_ts;
    END IF;
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RAISE EXCEPTION USING MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH', DETAIL = jsonb_build_object('expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'current_timesheet_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'NO_TSFIN')::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'tsfin_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_current_tsfin.processing_status::text,
      'old_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_current_ts.contract_id IS NOT NULL THEN
    SELECT COALESCE(c.requires_hr, false)
      INTO v_contract_requires_hr
    FROM public.contracts AS c
    WHERE c.id = v_current_ts.contract_id
    LIMIT 1;
  END IF;

  SELECT
    COALESCE(vts.client_requires_hr, COALESCE(v_contract_requires_hr, false)),
    COALESCE(vts.hr_validation_required_for_invoice, COALESCE(v_contract_requires_hr, false)),
    CASE
      WHEN vts.validation_status IS NULL THEN NULL::text
      ELSE UPPER(vts.validation_status::text)
    END
    INTO v_client_requires_hr,
         v_hr_validation_required_for_invoice,
         v_validation_status
  FROM public.v_timesheets_summary_base AS vts
  WHERE vts.timesheet_id = v_current_ts.timesheet_id
  LIMIT 1;

  v_client_requires_hr := COALESCE(v_client_requires_hr, v_contract_requires_hr, false);
  v_hr_validation_required_for_invoice := COALESCE(v_hr_validation_required_for_invoice, v_contract_requires_hr, false);

  SELECT cw.*
    INTO v_contract_week
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current_ts.timesheet_id
     OR EXISTS (
       SELECT 1
       FROM public.timesheets AS cw_ts
       WHERE cw_ts.timesheet_id = cw.timesheet_id
         AND cw_ts.booking_id = v_current_ts.booking_id
     )
  ORDER BY CASE WHEN cw.timesheet_id = v_current_ts.timesheet_id THEN 0 ELSE 1 END,
           cw.updated_at DESC NULLS LAST,
           cw.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND v_contract_week.id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TARGET_NOT_FOUND',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET')::text;
  END IF;

  IF v_contract_week.id IS NOT NULL AND v_contract_week.status = 'INVOICED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_contract_week.id, 'contract_week_status', v_contract_week.status::text, 'lock_scope', 'contract_week_status')::text;
  END IF;

  IF v_contract_week.id IS NOT NULL AND v_contract_week.status = 'CANCELLED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING
      MESSAGE = 'CONTRACT_WEEK_NOT_AUTHORISABLE',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_contract_week.id, 'contract_week_status', v_contract_week.status::text)::text;
  END IF;

  v_prev_status := v_current_tsfin.processing_status;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json -> 'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_current_ts.archived_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL OR COALESCE(v_has_segment_invoice_lock, false) THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'invoice_id', v_current_tsfin.locked_by_invoice_id)::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL OR v_current_tsfin.authorised_at_utc IS NOT NULL OR v_contract_week.status = 'AUTHORISED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_AUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');
  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    IF COALESCE(v_temp_log_enabled, false) THEN
      PERFORM public._temp_diag_log(
        'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
        'TEMP_TIMESHEET_LIFECYCLE',
        v_current_ts.timesheet_id::text,
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
          'function_name', 'timesheet_authorise_generic_atomic',
          'stage', 'row_signature_mismatch_before_authorise',
          'action', 'authorise',
          'route_family', 'timesheet_lifecycle',
          'timesheet_id', p_timesheet_id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
          'expected_row_signature', v_expected_row_signature,
          'current_row_signature', v_current_row_signature,
          'current_signature_payload', v_before_signature_json,
          'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
        ))
      );
    END IF;
    RAISE EXCEPTION USING MESSAGE = 'ROW_SIGNATURE_MISMATCH', DETAIL = jsonb_build_object('expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature, 'current_timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  SELECT COALESCE(tv.pre_validated, false)
    INTO v_validation_pre_validated
  FROM public.timesheet_validations AS tv
  WHERE tv.timesheet_id = v_current_ts.timesheet_id
  ORDER BY tv.updated_at DESC NULLS LAST, tv.created_at DESC NULLS LAST, tv.id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    v_validation_pre_validated := false;
  END IF;

  v_validation_ok := COALESCE(v_validation_status, '') IN ('VALIDATION_OK', 'OVERRIDDEN');
  v_force_ready_for_invoice := v_current_tsfin.basis IN ('NHSP'::public.timesheet_fin_basis_enum, 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum);
  v_must_hold_for_hr_validation := COALESCE(v_hr_validation_required_for_invoice, false) AND NOT v_validation_ok;
  v_prevalidated_fast_track := COALESCE(v_validation_pre_validated, false) AND v_validation_ok;

  IF v_prev_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN
    RAISE EXCEPTION USING MESSAGE = 'AUTHORISE_NOT_ALLOWED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'processing_status', v_prev_status::text)::text;
  END IF;

  v_new_status := CASE
    WHEN v_force_ready_for_invoice THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    WHEN v_must_hold_for_hr_validation THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
    WHEN v_prevalidated_fast_track THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    WHEN COALESCE(v_client_requires_hr, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
    ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
  END;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'before_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'tsfin_id', v_current_tsfin.id,
      'current_row_signature_present', v_current_row_signature IS NOT NULL,
      'expected_row_signature_present', v_expected_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM set_config('cloudtms.lifecycle_mutation_context', 'timesheet_authorise', true);
  PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', v_current_ts.timesheet_id::text, true);
  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'on', true);

  UPDATE public.timesheets AS ts
     SET authorised_at_server = v_now,
         revoked_at = NULL,
         revoked_reason = NULL,
         revoked_by = NULL,
         updated_at = v_now
   WHERE ts.timesheet_id = v_current_ts.timesheet_id
     AND ts.is_current = true
   RETURNING * INTO v_current_ts;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'timesheets_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'new_authorised_present', v_current_ts.authorised_at_server IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  UPDATE public.timesheets_financials AS tf
     SET processing_status = v_new_status,
         authorised_by_user_id = p_actor_user_id,
         authorised_at_utc = v_now,
         updated_at = v_now
   WHERE tf.id = v_current_tsfin.id
     AND tf.is_current = true
   RETURNING * INTO v_current_tsfin;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'tsfin_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_prev_status::text,
      'new_processing_status', v_current_tsfin.processing_status::text,
      'new_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_contract_week.id IS NOT NULL THEN
    UPDATE public.contract_weeks AS cw
       SET status = 'AUTHORISED'::public.contract_week_status_enum,
           updated_at = v_now
     WHERE cw.id = v_contract_week.id
     RETURNING * INTO v_contract_week;
  END IF;

  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'off', true);

  SELECT EXISTS (
    SELECT 1
    FROM public._pay_timesheet_rotation_scope(ARRAY[v_current_ts.timesheet_id]) AS rs
    JOIN public.timesheet_payment_overrides AS payment_override
      ON payment_override.timesheet_id = rs.family_timesheet_id
    WHERE payment_override.cleared_at_utc IS NULL
      AND UPPER(BTRIM(COALESCE(payment_override.override_type, ''))) = 'ADVANCE_THIS_PAYMENT'
  ) INTO v_has_uncleared_advance_override;

  IF COALESCE(v_has_uncleared_advance_override, false) THEN
    v_advance_state_refresh_json := public.pay_timesheet_summary_advance_state_refresh(
      p_timesheet_id => v_current_ts.timesheet_id,
      p_actor_user_id => p_actor_user_id
    );

    PERFORM public._temp_diag_log(
      'TEMP_AUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_authorise_generic_atomic',
        'stage', 'advance_state_refresh_done',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'refresh_result', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  ELSE
    PERFORM public._temp_diag_log(
      'TEMP_AUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_authorise_generic_atomic',
        'stage', 'advance_state_refresh_skipped',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'reason', 'NO_ACTIVE_ADVANCE_THIS_PAYMENT_OVERRIDE',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'after_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'row_signature_present', v_after_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._audit_insert(
    'timesheet',
    v_current_ts.timesheet_id::text,
    'TIMESHEET_AUTHORISED',
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'previous_processing_status', v_prev_status::text, 'previous_row_signature', v_current_row_signature),
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'new_processing_status', v_new_status::text, 'authorised_at_utc', v_now, 'authorised_by_user_id', p_actor_user_id, 'new_row_signature', v_after_row_signature),
    'TIMESHEET_AUTHORISE',
    p_actor_user_id
  );

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'audit_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'return',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'new_processing_status', v_new_status::text,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', 'timesheet_authorise',
    'timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'requested_timesheet_id', v_requested_ts.timesheet_id,
    'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
    'booking_id', v_current_ts.booking_id,
    'candidate_id', v_current_tsfin.candidate_id,
    'authorised_at_server', v_current_ts.authorised_at_server,
    'revoked_at', v_current_ts.revoked_at,
    'authorised_at_utc', v_current_tsfin.authorised_at_utc,
    'advance_state_refresh', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
    'current_version', v_current_ts.version,
    'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id,
    'previous_processing_status', v_prev_status::text,
    'processing_status', v_new_status::text,
    'new_processing_status', v_new_status::text,
    'validation_status', v_validation_status,
    'validation_pre_validated', COALESCE(v_validation_pre_validated, false),
    'hr_validation_required_for_invoice', COALESCE(v_hr_validation_required_for_invoice, false),
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'affected_rows', jsonb_build_array(jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'booking_id', v_current_ts.booking_id, 'row_key', 'timesheet:' || v_current_ts.timesheet_id::text)),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', CASE WHEN COALESCE(v_has_uncleared_advance_override, false) THEN jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks', 'timesheet_summary_pay_state_cache') ELSE jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks') END, 'timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'exception',
      'timesheet_id', p_timesheet_id,
      'sqlstate', v_error_state,
      'error_message', v_error_message,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );
  IF v_error_state = '55P03' THEN
    RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;
  RAISE;
END;
$function$;

-- timesheet_correction_chain_scope_v1(uuid,boolean,integer,integer)
CREATE OR REPLACE FUNCTION public.timesheet_correction_chain_scope_v1(p_timesheet_id uuid, p_lock_rows boolean DEFAULT false, p_max_depth integer DEFAULT 32, p_max_members integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
 SET "plpgsql_check.profiler" TO 'off'
 SET "plpgsql_check.tracer" TO 'off'
 SET "plpgsql_check.constants_tracing" TO 'off'
 SET "plpgsql_check.cursors_leaks" TO 'off'
 SET "plpgsql_check.strict_cursors_leaks" TO 'off'
 SET "plpgsql_check.fatal_errors" TO 'off'
AS $function$
declare
  v_root_id uuid;
  v_member_ids uuid[] := array[]::uuid[];
  v_member_count integer := 0;
  v_cycle boolean := false;
  v_truncated boolean := false;
  v_units jsonb := '[]'::jsonb;
  v_members jsonb := '[]'::jsonb;
  v_errors jsonb := '[]'::jsonb;
  v_latest_positive uuid;
  v_fingerprint_payload jsonb;
  v_chain_fingerprint text;
  v_requested_unit jsonb;
  v_requested_operation_id text;
begin
  if p_timesheet_id is null then
    raise exception 'CORRECTION_CHAIN_TIMESHEET_ID_REQUIRED' using errcode='22023';
  end if;
  if p_max_depth < 1 or p_max_depth > 32 then
    raise exception 'CORRECTION_CHAIN_MAX_DEPTH_OUT_OF_RANGE' using errcode='22023';
  end if;
  if p_max_members < 1 or p_max_members > 100 then
    raise exception 'CORRECTION_CHAIN_MAX_MEMBERS_OUT_OF_RANGE' using errcode='22023';
  end if;
  if not exists (select 1 from public.timesheets where timesheet_id=p_timesheet_id) then
    raise exception 'CORRECTION_CHAIN_TIMESHEET_NOT_FOUND' using errcode='P0002';
  end if;

  with recursive ancestors as (
    select ts.timesheet_id, ts.parent_timesheet_id, 0 depth,
           array[ts.timesheet_id]::uuid[] path, false cycle
    from public.timesheets ts where ts.timesheet_id=p_timesheet_id
    union all
    select p.timesheet_id, p.parent_timesheet_id, a.depth+1,
           a.path||p.timesheet_id, p.timesheet_id=any(a.path)
    from ancestors a
    join public.timesheets p on p.timesheet_id=a.parent_timesheet_id
    where a.parent_timesheet_id is not null and not a.cycle and a.depth<p_max_depth
  )
  select a.timesheet_id,
         coalesce(bool_or(a.cycle) over (),false)
  into v_root_id, v_cycle
  from ancestors a
  order by a.depth desc
  limit 1;

  with recursive descendants as (
    select ts.timesheet_id, ts.parent_timesheet_id, 0 depth,
           array[ts.timesheet_id]::uuid[] path, false cycle
    from public.timesheets ts where ts.timesheet_id=v_root_id
    union all
    select c.timesheet_id, c.parent_timesheet_id, d.depth+1,
           d.path||c.timesheet_id, c.timesheet_id=any(d.path)
    from descendants d
    join public.timesheets c on c.parent_timesheet_id=d.timesheet_id
    where not d.cycle and d.depth<p_max_depth
  ), picked as (
    select distinct d.timesheet_id from descendants d where not d.cycle
  )
  select coalesce(array_agg(p.timesheet_id order by p.timesheet_id),array[]::uuid[]),
         count(*)::integer,
         exists(select 1 from descendants where cycle),
         exists(
           select 1 from public.timesheets child
           join descendants edge on edge.timesheet_id=child.parent_timesheet_id
           where edge.depth=p_max_depth
         )
  into v_member_ids,v_member_count,v_cycle,v_truncated
  from picked p;

  if v_member_count>p_max_members then
    raise exception 'CORRECTION_CHAIN_MEMBER_LIMIT_EXCEEDED'
      using errcode='22023', detail=jsonb_build_object('count',v_member_count,'max',p_max_members)::text;
  end if;
  if p_lock_rows then
    if not pg_try_advisory_xact_lock(hashtextextended('CORRECTION_CHAIN|'||v_root_id::text,21072026)) then
      raise exception 'CORRECTION_CHAIN_LOCK_BUSY' using errcode='55P03';
    end if;
    perform 1 from public.timesheets ts
    where ts.timesheet_id=any(v_member_ids) order by ts.timesheet_id for update;
  end if;

  with member_rows as (
    select ts.*,
      public._ctms_import_correction_classify_v1(ts.timesheet_id) as class_json,
      case when upper(btrim(coalesce(ts.adjustment_origin,''))) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
        then public._ctms_correction_policy_envelope_read_v1(ts.timesheet_id) else null end as envelope
    from public.timesheets ts where ts.timesheet_id=any(v_member_ids)
  ), unit_members as (
    select mr.*,
      mr.envelope#>>'{operation,operation_id}' operation_id,
      mr.envelope->>'correction_chain_id' correction_chain_id,
      case
        when upper(btrim(coalesce(mr.correction_kind,''))) in
          ('CHANGED_HOURS_REVERSAL','CANCELLATION_REVERSAL') then 'REVERSAL'
        when upper(btrim(coalesce(mr.correction_kind,''))) in
          ('CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT') then 'REPLACEMENT'
        else 'INVALID'
      end member_role
    from member_rows mr
    where coalesce((mr.class_json->>'is_import_authoritative_correction')::boolean,false)=true
  ), unit_rows as (
    select operation_id,correction_chain_id,
      min(correction_id) correction_id,
      count(distinct correction_id)::integer correction_id_count,
      count(*) filter (where member_role='REVERSAL')::integer reversal_count,
      count(*) filter (where member_role='REPLACEMENT')::integer replacement_count,
      count(*)::integer actual_member_count,
      jsonb_agg(member_role order by case member_role when 'REVERSAL' then 1 when 'REPLACEMENT' then 2 else 3 end) actual_member_roles,
      array_agg(timesheet_id order by timesheet_id) member_ids,
      count(distinct envelope->>'envelope_fingerprint')::integer envelope_count,
      count(distinct envelope->>'root_timesheet_id')::integer envelope_root_count,
      min(envelope->>'envelope_fingerprint') envelope_fingerprint,
      min(envelope::text)::jsonb envelope
    from unit_members
    where nullif(btrim(coalesce(operation_id,'')),'') is not null
      and nullif(btrim(coalesce(correction_chain_id,'')),'') is not null
    group by operation_id,correction_chain_id
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'correction_id',u.correction_id,
    'correction_operation_id',u.operation_id,
    'correction_chain_id',u.correction_chain_id,
    'correction_shape',u.envelope->>'correction_shape',
    'expected_member_count',(u.envelope->>'expected_member_count')::integer,
    'expected_member_roles',u.envelope->'expected_member_roles',
    'actual_member_count',u.actual_member_count,
    'actual_member_roles',u.actual_member_roles,
    'member_ids',to_jsonb(u.member_ids),
    'reversal_count',u.reversal_count,
    'replacement_count',u.replacement_count,
    'envelope_fingerprint',u.envelope_fingerprint,
    'policy_envelope',u.envelope,
    'valid',u.operation_id is not null
      and u.correction_chain_id is not null
      and u.correction_id_count=1
      and u.envelope_count=1
      and u.envelope_root_count=1
      and u.envelope->>'root_timesheet_id' is not distinct from v_root_id::text
      and u.envelope->>'correction_shape' in ('REVERSAL_ONLY','REVERSAL_REPLACEMENT')
      and u.actual_member_count=(u.envelope->>'expected_member_count')::integer
      and u.actual_member_roles=u.envelope->'expected_member_roles'
      and u.reversal_count=1
      and u.replacement_count=case when u.envelope->>'correction_shape'='REVERSAL_ONLY' then 0 else 1 end
  ) order by u.operation_id),'[]'::jsonb)
  into v_units
  from unit_rows u;

  select coalesce(jsonb_agg(jsonb_build_object(
    'timesheet_id',ts.timesheet_id,
    'parent_timesheet_id',ts.parent_timesheet_id,
    'booking_id',ts.booking_id,
    'is_current',ts.is_current,
    'status',ts.status,
    'correction_id',ts.correction_id,
    'correction_kind',ts.correction_kind,
    'adjustment_origin',ts.adjustment_origin,
    'policy_envelope_fingerprint',coalesce(
      ts.candidate_hint_text#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
      tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'
    ),
    'current_tsfin_id',tf.id,
    'processing_status',tf.processing_status,
    'paid_at_utc',tf.paid_at_utc,
    'locked_by_invoice_id',tf.locked_by_invoice_id
  ) order by ts.created_at,ts.timesheet_id),'[]'::jsonb)
  into v_members
  from public.timesheets ts
  left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
  where ts.timesheet_id=any(v_member_ids);

  if v_cycle then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','CORRECTION_CHAIN_CYCLE')); end if;
  if v_truncated then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','CORRECTION_CHAIN_DEPTH_EXCEEDED')); end if;
  if exists(select 1 from jsonb_array_elements(v_units) u where coalesce((u->>'valid')::boolean,false)=false) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','CORRECTION_UNIT_INVALID'));
  end if;
  if exists (
    select 1
    from public.timesheets ts
    where ts.timesheet_id=any(v_member_ids)
      and upper(btrim(coalesce(ts.adjustment_origin,''))) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
      and not coalesce((public._ctms_import_correction_classify_v1(ts.timesheet_id)->>'is_import_authoritative_correction')::boolean,false)
  ) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','CORRECTION_MEMBER_CONTRACT_INVALID'));
  end if;

  select ts.timesheet_id into v_latest_positive
  from public.timesheets ts
  where ts.timesheet_id=any(v_member_ids)
    and (
      ts.timesheet_id=v_root_id
      or upper(btrim(coalesce(ts.correction_kind,''))) in ('CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT')
    )
  order by ts.created_at desc,ts.timesheet_id desc limit 1;

  if coalesce((public._ctms_import_correction_classify_v1(p_timesheet_id)->>'is_import_authoritative_correction')::boolean,false) then
    v_requested_operation_id:=public._ctms_correction_policy_envelope_read_v1(p_timesheet_id)#>>'{operation,operation_id}';
    select u into v_requested_unit
    from jsonb_array_elements(v_units) u
    where u->>'correction_operation_id'=v_requested_operation_id
    limit 1;
  end if;

  v_fingerprint_payload:=jsonb_build_object(
    'root_timesheet_id',v_root_id,'member_ids',to_jsonb(v_member_ids),
    'correction_units',v_units,'latest_positive_timesheet_id',v_latest_positive
  );
  v_chain_fingerprint:=encode(extensions.digest(convert_to(v_fingerprint_payload::text,'UTF8'),'sha256'::text),'hex');

  return jsonb_build_object(
    'ok',jsonb_array_length(v_errors)=0,
    'valid',jsonb_array_length(v_errors)=0,
    'requested_timesheet_id',p_timesheet_id,
    'root_timesheet_id',v_root_id,
    'latest_positive_timesheet_id',v_latest_positive,
    'member_count',v_member_count,'member_ids',to_jsonb(v_member_ids),
    'member_timesheet_ids',to_jsonb(v_member_ids),'members',v_members,
    'correction_units',v_units,'pairs',v_units,'requested_correction_unit',v_requested_unit,
    'correction_shape',v_requested_unit->>'correction_shape',
    'correction_financials_policy_required',v_requested_unit is not null,
    'correction_financials_policy_envelope_required',v_requested_unit is not null,
    'correction_financials_policy_envelope',v_requested_unit->'policy_envelope',
    'correction_financials_policy_envelope_fingerprint',v_requested_unit->>'envelope_fingerprint',
    'chain_fingerprint',v_chain_fingerprint,'errors',v_errors
  );
end;
$function$;

-- timesheet_correction_pair_lifecycle_preview_v1(jsonb,text,uuid,integer)
CREATE OR REPLACE FUNCTION public.timesheet_correction_pair_lifecycle_preview_v1(p_items jsonb, p_action text, p_actor_user_id uuid, p_max_members integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_item jsonb;
  v_id_text text;
  v_id uuid;
  v_class jsonb;
  v_transition jsonb;
  v_group jsonb;
  v_groups jsonb:='[]'::jsonb;
  v_seen text[]:=array[]::text[];
  v_selected_count integer:=0;
  v_affected_count integer:=0;
  v_ordinary_count integer:=0;
  v_valid boolean:=true;
  v_repairable_mixed boolean:=false;
  v_already_target_state boolean:=false;
  v_group_ready boolean:=false;
  v_group_fingerprint text;
  v_members jsonb;
  v_errors jsonb;
begin
  if v_action not in ('AUTHORISE','UNAUTHORISE') then
    raise exception 'CORRECTION_LIFECYCLE_ACTION_INVALID' using errcode='22023';
  end if;
  if p_actor_user_id is null then
    raise exception 'CORRECTION_LIFECYCLE_ACTOR_REQUIRED' using errcode='22023';
  end if;
  if p_max_members<1 or p_max_members>100
     or jsonb_typeof(coalesce(p_items,'[]'::jsonb))<>'array'
     or jsonb_array_length(coalesce(p_items,'[]'::jsonb))>p_max_members then
    raise exception 'CORRECTION_LIFECYCLE_ITEMS_INVALID' using errcode='22023';
  end if;

  v_selected_count:=jsonb_array_length(coalesce(p_items,'[]'::jsonb));
  for v_item in select value from jsonb_array_elements(coalesce(p_items,'[]'::jsonb))
  loop
    v_id_text:=nullif(btrim(coalesce(
      v_item->>'timesheet_id',v_item->>'timesheetId',
      v_item->>'current_timesheet_id',v_item->>'currentTimesheetId',
      v_item->>'requested_timesheet_id',v_item->>'requestedTimesheetId',
      case when coalesce(v_item->>'row_key','') like 'timesheet:%'
        then substring(v_item->>'row_key' from 11) end,'')), '');
    if v_id_text is null or v_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      v_valid:=false;
      v_groups:=v_groups||jsonb_build_array(jsonb_build_object(
        'valid',false,'action_ready',false,'errors',jsonb_build_array(
          jsonb_build_object('code','TIMESHEET_ID_REQUIRED'))));
      continue;
    end if;

    v_id:=v_id_text::uuid;
    v_class:=public._ctms_import_correction_classify_v1(v_id);
    if coalesce((v_class->>'is_import_authoritative_correction')::boolean,false) is not true then
      v_ordinary_count:=v_ordinary_count+1;
      continue;
    end if;
    if nullif(v_class->>'correction_id','')=any(v_seen) then continue; end if;
    v_seen:=array_append(v_seen,v_class->>'correction_id');

    v_transition:=public.timesheet_correction_pair_transition_v1(
      v_id,v_action,p_actor_user_id,null::uuid,null::text,false,p_max_members);
    v_errors:=coalesce(v_transition->'errors','[]'::jsonb);
    v_repairable_mixed:=
      coalesce((v_transition->>'expected_member_count')::integer,0)=2
      and coalesce((v_transition->>'authorised_count')::integer,0)>0
      and coalesce((v_transition->>'unauthorised_count')::integer,0)>0
      and coalesce((v_transition->>'authorised_count')::integer,0)
          +coalesce((v_transition->>'unauthorised_count')::integer,0)=2
      and coalesce((v_transition->>'paid_count')::integer,0)=0
      and coalesce((v_transition->>'invoice_lined_count')::integer,0)=0
      and (select count(*)=1 and bool_and(e->>'code'='CORRECTION_UNIT_ACTION_STATE_INVALID')
           from jsonb_array_elements(v_errors) e)
      and (v_action='UNAUTHORISE'
        or coalesce((v_transition->>'ready_count')::integer,0)=2);
    v_already_target_state:=coalesce((v_transition->>'expected_member_count')::integer,0)=2
      and coalesce((v_transition->>'paid_count')::integer,0)=0
      and coalesce((v_transition->>'invoice_lined_count')::integer,0)=0
      and ((v_action='AUTHORISE' and coalesce((v_transition->>'authorised_count')::integer,0)=2)
        or (v_action='UNAUTHORISE' and coalesce((v_transition->>'unauthorised_count')::integer,0)=2));
    v_group_ready:=coalesce((v_transition->>'action_ready')::boolean,false)
      or v_repairable_mixed or v_already_target_state;

    select coalesce(jsonb_agg(
      row_json||jsonb_build_object(
        'signed_hours',jsonb_build_object(
          'day',coalesce(tf.hours_day,0),'night',coalesce(tf.hours_night,0),
          'sat',coalesce(tf.hours_sat,0),'sun',coalesce(tf.hours_sun,0),
          'bh',coalesce(tf.hours_bh,0),
          'total',coalesce(tf.hours_day,0)+coalesce(tf.hours_night,0)+coalesce(tf.hours_sat,0)+coalesce(tf.hours_sun,0)+coalesce(tf.hours_bh,0)),
        'current_state',case when coalesce((row_json->>'timesheet_authorised')::boolean,false)
          and coalesce((row_json->>'tsfin_authorised')::boolean,false) then 'AUTHORISED' else 'UNAUTHORISED' end,
        'resulting_state',case when v_action='AUTHORISE' then 'AUTHORISED' else 'UNAUTHORISED' end)
      order by case row_json->>'correction_kind' when 'CHANGED_HOURS_REVERSAL' then 1 else 2 end),
      '[]'::jsonb)
    into v_members
    from jsonb_array_elements(coalesce(v_transition->'pair_rows','[]'::jsonb)) row_json
    left join public.timesheets_financials tf
      on tf.timesheet_id=nullif(row_json->>'timesheet_id','')::uuid and tf.is_current=true;

    v_group_fingerprint:=public._import_review_hash_v1(concat_ws('|',
      'correction-lifecycle-preview-v1',v_action,v_transition->>'chain_fingerprint',
      v_transition->>'correction_id',v_members::text,v_group_ready));
    v_group:=v_transition||jsonb_build_object(
      'valid',v_group_ready,'action_ready',v_group_ready,
      'repairing_legacy_mixed_state',v_repairable_mixed,
      'already_in_target_state',v_already_target_state,
      'source_system',case
        when exists(select 1 from jsonb_array_elements(v_members) m
          join public.timesheets t on t.timesheet_id=(m->>'timesheet_id')::uuid
          left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
          where upper(coalesce(
            t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',
            tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,classification,source_system}',
            tf.rate_source_refs_json#>>'{correction_financials_policy_envelope,classification,source_system}',
            case when tf.basis in ('NHSP','NHSP_ADJUSTMENT') then 'NHSP' end,''))='NHSP')
        then 'NHSP' else 'HealthRoster' end,
      'members',v_members,'pair_fingerprint',v_group_fingerprint,
      'errors',case when v_repairable_mixed or v_already_target_state then '[]'::jsonb else v_errors end);
    v_groups:=v_groups||jsonb_build_array(v_group);
    v_affected_count:=v_affected_count+coalesce((v_transition->>'expected_member_count')::integer,0);
    if not v_group_ready then v_valid:=false; end if;
  end loop;

  return jsonb_build_object(
    'ok',true,'valid',v_valid,'action',v_action,
    'selected_count',v_selected_count,
    'ordinary_timesheet_count',v_ordinary_count,
    'correction_pair_count',jsonb_array_length(v_groups),
    'affected_timesheet_count',v_ordinary_count+v_affected_count,
    'groups',v_groups,
    'preview_fingerprint',public._import_review_hash_v1(concat_ws('|',
      'correction-lifecycle-batch-preview-v1',v_action,v_selected_count,v_groups::text)));
end;
$function$;

-- timesheet_correction_pair_transition_v1(uuid,text,uuid,uuid,text,boolean,integer)
CREATE OR REPLACE FUNCTION public.timesheet_correction_pair_transition_v1(p_timesheet_id uuid, p_action text, p_actor_user_id uuid, p_operation_id uuid DEFAULT NULL::uuid, p_expected_chain_fingerprint text DEFAULT NULL::text, p_lock_rows boolean DEFAULT true, p_max_members integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_chain jsonb;
  v_unit jsonb;
  v_envelope jsonb;
  v_ids uuid[]:=array[]::uuid[];
  v_expected_count integer;
  v_rows jsonb:='[]'::jsonb;
  v_items jsonb:='[]'::jsonb;
  v_errors jsonb:='[]'::jsonb;
  v_authorised integer:=0;
  v_unauthorised integer:=0;
  v_ready integer:=0;
  v_processed integer:=0;
  v_unprocessed integer:=0;
  v_paid integer:=0;
  v_invoiced integer:=0;
  v_action_ready boolean:=false;
  v_idempotent boolean:=false;
  v_required_rpc text;
  r record;
  v_leg jsonb;
  v_actual_envelope_fingerprint text;
  v_actual_leg_fingerprint text;
  v_values_match boolean;
begin
  if p_timesheet_id is null or p_actor_user_id is null then
    raise exception 'CORRECTION_TRANSITION_REQUIRED_ARGUMENT_MISSING' using errcode='22023';
  end if;
  if v_action not in ('AUTHORISE','UNAUTHORISE','PROCESS','UNPROCESS') then
    raise exception 'CORRECTION_TRANSITION_ACTION_INVALID' using errcode='22023';
  end if;
  if p_max_members<1 or p_max_members>100 then
    raise exception 'CORRECTION_TRANSITION_MEMBER_LIMIT_INVALID' using errcode='22023';
  end if;
  perform 1 from public.tms_users u where u.id=p_actor_user_id and coalesce(u.is_active,false);
  if not found then raise exception 'CORRECTION_TRANSITION_ACTOR_INVALID' using errcode='42501'; end if;

  if p_operation_id is not null then
    perform 1 from public.import_apply_operations o
    where o.id=p_operation_id and o.actor_user_id=p_actor_user_id
      and o.state in ('PREPARED','SOURCE_COMMITTED_TSFIN_PENDING','FINANCIALISED_PENDING_FINALISATION')
    for update;
    if not found then raise exception 'CORRECTION_TRANSITION_OPERATION_INVALID' using errcode='P0001'; end if;
  end if;

  v_chain:=public.timesheet_correction_chain_scope_v1(p_timesheet_id,p_lock_rows,32,p_max_members);
  if coalesce((v_chain->>'valid')::boolean,false) is not true then
    raise exception 'CORRECTION_TRANSITION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
  end if;
  if nullif(btrim(coalesce(p_expected_chain_fingerprint,'')),'') is not null
     and p_expected_chain_fingerprint is distinct from v_chain->>'chain_fingerprint' then
    raise exception 'CORRECTION_TRANSITION_CHAIN_STALE' using errcode='40001';
  end if;
  v_unit:=v_chain->'requested_correction_unit';
  if jsonb_typeof(v_unit)<>'object' then
    raise exception 'CORRECTION_TRANSITION_UNIT_NOT_FOUND' using errcode='22023';
  end if;
  if coalesce((v_unit->>'valid')::boolean,false) is not true then
    raise exception 'CORRECTION_TRANSITION_UNIT_INVALID' using errcode='P0001',detail=v_unit::text;
  end if;
  v_envelope:=v_unit->'policy_envelope';
  v_expected_count:=(v_unit->>'expected_member_count')::integer;
  select coalesce(array_agg(value::uuid order by value::text),array[]::uuid[])
  into v_ids from jsonb_array_elements_text(v_unit->'member_ids');
  if cardinality(v_ids)<>v_expected_count then
    raise exception 'CORRECTION_TRANSITION_MEMBER_COUNT_MISMATCH' using errcode='P0001';
  end if;

  if p_lock_rows then
    perform 1 from public.timesheets ts where ts.timesheet_id=any(v_ids)
    order by ts.timesheet_id for update;
    perform 1 from public.timesheets_financials tf
    where tf.timesheet_id=any(v_ids) and tf.is_current=true
    order by tf.timesheet_id,tf.id for update;
  end if;

  for r in
    select ts.*,tf.id tsfin_id,tf.processing_status,tf.processed_at_utc,
      tf.authorised_at_utc,tf.is_stale,tf.paid_at_utc,tf.locked_by_invoice_id,
      tf.candidate_id,tf.client_id,tf.pay_method,tf.has_rate_issue,tf.has_pay_channel_issue,
      tf.policy_snapshot_json,tf.rate_source_refs_json,tf.pay_vat_rate_pct_snapshot,
      exists(select 1 from public.invoice_lines il where il.timesheet_id=ts.timesheet_id) invoice_lined
    from public.timesheets ts
    left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    where ts.timesheet_id=any(v_ids)
    order by ts.timesheet_id
  loop
    v_leg:=public._ctms_correction_policy_leg_read_v1(r.timesheet_id);
    v_actual_envelope_fingerprint:=coalesce(
      r.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
      r.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
      r.rate_source_refs_json->>'correction_financials_policy_envelope_fingerprint'
    );
    v_actual_leg_fingerprint:=coalesce(
      r.policy_snapshot_json->>'correction_leg_fingerprint',
      r.rate_source_refs_json->>'correction_leg_fingerprint'
    );
    v_values_match:=
      v_actual_envelope_fingerprint is not distinct from v_envelope->>'envelope_fingerprint'
      and v_actual_leg_fingerprint is not distinct from v_leg->>'leg_fingerprint'
      and case when coalesce(r.policy_snapshot_json->>'erni_pct','')~'^-?[0-9]+([.][0-9]+)?$'
        then (r.policy_snapshot_json->>'erni_pct')::numeric is not distinct from (v_leg#>>'{tsfin_policy,erni_pct}')::numeric else false end
      and upper(btrim(coalesce(r.policy_snapshot_json->>'apply_erni_to','')))=upper(btrim(coalesce(v_leg#>>'{tsfin_policy,apply_erni_to}','')))
      and coalesce(r.pay_vat_rate_pct_snapshot,
        case when coalesce(r.policy_snapshot_json->>'pay_vat_rate_pct','')~'^-?[0-9]+([.][0-9]+)?$'
          then (r.policy_snapshot_json->>'pay_vat_rate_pct')::numeric else null end)
        is not distinct from (v_leg#>>'{tsfin_policy,applied_pay_vat_rate_pct}')::numeric
      and coalesce(r.policy_snapshot_json->>'correction_tsfin_policy_fingerprint','')
        is not distinct from v_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}'
      and coalesce(r.policy_snapshot_json->>'correction_invoice_policy_fingerprint','')
        is not distinct from v_leg#>>'{invoice_policy,invoice_policy_fingerprint}';

    if r.authorised_at_server is not null and r.authorised_at_utc is not null then v_authorised:=v_authorised+1; end if;
    if r.authorised_at_server is null and r.authorised_at_utc is null then v_unauthorised:=v_unauthorised+1; end if;
    if r.tsfin_id is not null and not coalesce(r.is_stale,false)
       and r.candidate_id is not null and r.client_id is not null
       and nullif(btrim(coalesce(r.pay_method,'')),'') is not null
       and not coalesce(r.has_rate_issue,false) and not coalesce(r.has_pay_channel_issue,false)
       and v_values_match then v_ready:=v_ready+1; end if;
    if r.processed_at_utc is not null or r.processing_status in (
      'READY_FOR_HR'::public.ts_fin_processing_status_enum,
      'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
      'PENDING_AUTH'::public.ts_fin_processing_status_enum) then v_processed:=v_processed+1; end if;
    if r.processed_at_utc is null and r.processing_status='UNPROCESSED'::public.ts_fin_processing_status_enum then v_unprocessed:=v_unprocessed+1; end if;
    if r.paid_at_utc is not null then v_paid:=v_paid+1; end if;
    if r.invoice_lined or r.locked_by_invoice_id is not null then v_invoiced:=v_invoiced+1; end if;
    if not v_values_match then
      v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','CORRECTION_LEG_POLICY_NOT_FROZEN','timesheet_id',r.timesheet_id));
    end if;
    v_rows:=v_rows||jsonb_build_array(jsonb_build_object(
      'timesheet_id',r.timesheet_id,'correction_kind',r.correction_kind,
      'timesheet_authorised',r.authorised_at_server is not null,
      'tsfin_authorised',r.authorised_at_utc is not null,'tsfin_id',r.tsfin_id,
      'processing_status',r.processing_status,'policy_ready',v_values_match,
      'leg_fingerprint',v_leg->>'leg_fingerprint'));
    v_items:=v_items||jsonb_build_array(jsonb_build_object(
      'timesheet_id',r.timesheet_id,'expected_timesheet_id',r.timesheet_id,
      'expected_version',r.version,
      'expected_policy_envelope_fingerprint',v_envelope->>'envelope_fingerprint',
      'expected_leg_fingerprint',v_leg->>'leg_fingerprint'));
  end loop;

  case v_action
    when 'AUTHORISE' then
      v_idempotent:=v_authorised=v_expected_count;
      v_action_ready:=v_idempotent or (v_unauthorised=v_expected_count and v_ready=v_expected_count);
      v_required_rpc:=case when v_idempotent then null else 'timesheet_authorise_bulk_atomic' end;
    when 'UNAUTHORISE' then
      v_idempotent:=v_unauthorised=v_expected_count;
      v_action_ready:=v_idempotent or v_authorised=v_expected_count;
      v_required_rpc:=case when v_idempotent then null else 'timesheet_unauthorise_bulk_atomic' end;
    when 'PROCESS' then
      v_idempotent:=v_processed=v_expected_count;
      v_action_ready:=v_idempotent or v_unprocessed=v_expected_count;
      v_required_rpc:=case when v_idempotent then null else 'contract_week_manual_upsert_bulk_process_atomic' end;
    when 'UNPROCESS' then
      v_idempotent:=v_unprocessed=v_expected_count;
      v_action_ready:=v_idempotent or v_processed=v_expected_count;
      v_required_rpc:=case when v_idempotent then null else 'contract_week_manual_unprocess_atomic' end;
  end case;
  if not v_action_ready then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','CORRECTION_UNIT_ACTION_STATE_INVALID')); end if;

  return jsonb_build_object(
    'ok',true,'valid',jsonb_array_length(v_errors)=0,'action',v_action,
    'action_ready',v_action_ready and jsonb_array_length(v_errors)=0,'idempotent_state',v_idempotent,
    'operation_id',p_operation_id,'root_timesheet_id',v_chain->>'root_timesheet_id',
    'correction_id',v_unit->>'correction_id','correction_shape',v_unit->>'correction_shape',
    'expected_member_count',v_expected_count,'pair_timesheet_ids',to_jsonb(v_ids),
    'correction_financials_policy_envelope',v_envelope,
    'correction_financials_policy_envelope_fingerprint',v_envelope->>'envelope_fingerprint',
    'chain_fingerprint',v_chain->>'chain_fingerprint','pair_rows',v_rows,
    'transition_items',v_items,'required_backend_rpc',v_required_rpc,
    'authorised_count',v_authorised,'unauthorised_count',v_unauthorised,
    'ready_count',v_ready,'processed_count',v_processed,'unprocessed_count',v_unprocessed,
    'paid_count',v_paid,'invoice_lined_count',v_invoiced,'errors',v_errors);
end;
$function$;

-- timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamp with time zone,text)
CREATE OR REPLACE FUNCTION public.timesheet_daily_manual_process_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_patch_json jsonb DEFAULT '{}'::jsonb, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_previous_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := 'PENDING_AUTH'::public.ts_fin_processing_status_enum;
  v_has_segment_invoice_lock boolean := false;
  v_timesheet_patch jsonb := COALESCE(p_timesheet_patch_json, '{}'::jsonb);
  v_tsfin_patch jsonb := COALESCE(p_tsfin_patch_json, '{}'::jsonb);
  v_forbidden_tsfin_patch_keys text[] := ARRAY[
    'candidate_id','client_id','pay_method','candidate_assignment','basis','policy_snapshot_json','rate_source_refs_json',
    'normal_hours','unsocial_hours','saturday_hours','sunday_hours','bank_holiday_hours','sleep_in_units','on_call_units','mileage_units','expenses_units',
    'total_hours','total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat','net_delta_ex_vat','normal_pay_rate','unsocial_pay_rate','saturday_pay_rate','sunday_pay_rate','bank_holiday_pay_rate',
    'normal_charge_rate','unsocial_charge_rate','saturday_charge_rate','sunday_charge_rate','bank_holiday_charge_rate','mileage_pay_rate','mileage_charge_rate','expenses_pay','expenses_charge',
    'has_rate_issue','has_pay_channel_issue','hours_day','hours_night','hours_sat','hours_sun','hours_bh','pay_day','pay_night','pay_sat','pay_sun','pay_bh','charge_day','charge_night','charge_sat','charge_sun','charge_bh',
    'expenses_pay_ex_vat','expenses_charge_ex_vat','mileage_pay_ex_vat','mileage_charge_ex_vat','travel_pay_ex_vat','travel_charge_ex_vat','accommodation_pay_ex_vat','accommodation_charge_ex_vat','other_pay_ex_vat','other_charge_ex_vat','additional_pay_ex_vat','additional_charge_ex_vat','additional_margin_ex_vat'
  ];
  v_financial_affecting_timesheet_patch_keys text[] := ARRAY[
    'worked_start_iso','worked_end_iso','break_start_iso','break_end_iso','break_minutes','worked_minutes','actual_schedule_json','additional_units_week','additional_units_per_day','scheduled_start_iso','scheduled_end_iso','week_ending_date','worked_date','work_date'
  ];
  v_patch_key text := NULL;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_candidate_electronic_context boolean :=
    COALESCE(current_setting('cloudtms.candidate_electronic_finalise', true), '') <> ''
    AND private._candidate_feature_enabled_current_v1('candidate_daily_finalisation');
BEGIN
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_ID_REQUIRED', 'message', 'p_timesheet_id is required.');
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'EXPECTED_TIMESHEET_ID_REQUIRED', 'message', 'p_expected_timesheet_id is required.');
  END IF;
  IF p_actor_user_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'message', 'p_actor_user_id is required.');
  END IF;
  IF jsonb_typeof(v_timesheet_patch) <> 'object' THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_PATCH_MUST_BE_OBJECT', 'message', 'p_timesheet_patch_json must be a JSON object.');
  END IF;
  IF jsonb_typeof(v_tsfin_patch) <> 'object' THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TSFIN_PATCH_MUST_BE_OBJECT', 'message', 'p_tsfin_patch_json must be a JSON object.');
  END IF;

  IF NOT v_candidate_electronic_context
     AND v_timesheet_patch ?| ARRAY[
       'submission_mode','auth_name','auth_job_title','r2_nurse_key','r2_auth_key',
       'img_sha256_nurse','img_sha256_auth','candidate_workflow_id',
       'candidate_workflow_generation','candidate_manager_approved_at_utc'
     ] THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process',
      'error_code', 'CANDIDATE_ELECTRONIC_FIELDS_FORBIDDEN',
      'message', 'Candidate electronic document fields require the bounded Candidate App finalisation authority.');
  END IF;

  FOREACH v_patch_key IN ARRAY v_forbidden_tsfin_patch_keys LOOP
    IF v_tsfin_patch ? v_patch_key THEN
      RETURN jsonb_build_object('ok', false, 'success', false, 'operation', 'daily_manual_process', 'error_code', 'TSFIN_PATCH_FORBIDDEN_FIELD', 'message', 'Cannot process: TSFIN patch contains an authoritative or financial field.', 'field', v_patch_key, 'timesheet_id', p_timesheet_id);
    END IF;
  END LOOP;

  FOREACH v_patch_key IN ARRAY v_financial_affecting_timesheet_patch_keys LOOP
    IF v_timesheet_patch ? v_patch_key THEN
      RETURN jsonb_build_object('ok', false, 'success', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_PATCH_REQUIRES_RECALCULATION', 'message', 'Cannot process while changing worked time, schedule, break, work date, or additional units. Save and recalculate the row before processing.', 'field', v_patch_key, 'timesheet_id', p_timesheet_id);
    END IF;
  END LOOP;

  SELECT ts.*
    INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1;

  IF v_requested_ts.timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_NOT_FOUND', 'message', 'Timesheet was not found.', 'requested_timesheet_id', p_timesheet_id);
  END IF;

  SELECT ts.*
    INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.booking_id = v_requested_ts.booking_id
    AND ts.is_current = true
  ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_ts.timesheet_id IS NULL THEN
    v_current_ts := v_requested_ts;
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_MOVED', 'message', 'Timesheet has moved to a newer current row.', 'requested_timesheet_id', p_timesheet_id, 'expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id, 'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id);
  END IF;

  IF v_current_ts.sheet_scope <> 'DAILY'::public.timesheet_scope_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'NOT_DAILY', 'message', 'Timesheet is not DAILY; daily manual process only applies to DAILY sheets.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_current_ts.submission_mode <> 'MANUAL'::public.submission_mode_enum
     AND NOT (v_candidate_electronic_context
       AND v_current_ts.submission_mode = 'ELECTRONIC'::public.submission_mode_enum) THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'NOT_MANUAL', 'message', 'Timesheet must be MANUAL before processing.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_current_ts.archived_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_ALREADY_AUTHORISED', 'message', 'This timesheet is already authorised.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'NO_TSFIN', 'message', 'No current financial snapshot exists for this timesheet.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  v_previous_status := v_current_tsfin.processing_status;
  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json -> 'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL OR COALESCE(v_has_segment_invoice_lock, false) THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_LOCKED_BY_INVOICE', 'message', 'Timesheet is invoice-locked; cannot process.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_previous_status <> 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'NOT_UNPROCESSED', 'message', 'Timesheet is not in UNPROCESSED state.', 'current_timesheet_id', v_current_ts.timesheet_id, 'previous_status', v_previous_status::text);
  END IF;
  IF v_current_tsfin.candidate_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'CANDIDATE_MISSING', 'message', 'Cannot process: candidate is missing from TSFIN context.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_current_tsfin.client_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'CLIENT_MISSING', 'message', 'Cannot process: client is missing from TSFIN context.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF COALESCE(v_current_tsfin.has_rate_issue, false) THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'RATE_ISSUE', 'message', 'Cannot process: TSFIN has a rate issue.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF COALESCE(v_current_tsfin.has_pay_channel_issue, false) THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'PAY_CHANNEL_ISSUE', 'message', 'Cannot process: TSFIN has a pay channel issue.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_current_tsfin.pay_method, '')), '') IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'PAY_METHOD_MISSING', 'message', 'Cannot process: pay method is missing from TSFIN context.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_current_tsfin.candidate_assignment = 'UNASSIGNED'::public.candidate_assignment_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'CANDIDATE_ASSIGNMENT_UNRESOLVED', 'message', 'Cannot process: candidate assignment is unresolved in TSFIN context.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_signature_v1(v_current_ts.timesheet_id, NULL::uuid, false);
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, v_timesheet_patch ->> 'backend_row_signature', v_timesheet_patch ->> 'row_signature', v_tsfin_patch ->> 'backend_row_signature', v_tsfin_patch ->> 'row_signature', '')), '');

  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'ROW_SIGNATURE_MISMATCH', 'message', 'Timesheet changed after it was loaded. Refresh the row and try again.', 'current_timesheet_id', v_current_ts.timesheet_id, 'expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature);
  END IF;

  UPDATE public.timesheets AS ts
     SET reference_number = CASE WHEN v_timesheet_patch ? 'reference_number' THEN NULLIF(BTRIM(v_timesheet_patch ->> 'reference_number'), '') ELSE ts.reference_number END,
         reference_set_at = CASE
           WHEN v_timesheet_patch ? 'reference_number' THEN
             CASE WHEN NULLIF(BTRIM(v_timesheet_patch ->> 'reference_number'), '') IS NULL THEN NULL::timestamp with time zone
                  WHEN NULLIF(BTRIM(v_timesheet_patch ->> 'reference_number'), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(ts.reference_number, '')), '') THEN v_now
                  ELSE COALESCE(ts.reference_set_at, v_now)
             END
           ELSE ts.reference_set_at
         END,
         submission_mode = CASE WHEN v_candidate_electronic_context
           THEN 'ELECTRONIC'::public.submission_mode_enum ELSE ts.submission_mode END,
         auth_name = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'auth_name'),'') ELSE ts.auth_name END,
         auth_job_title = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'auth_job_title'),'') ELSE ts.auth_job_title END,
         r2_nurse_key = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'r2_nurse_key'),'') ELSE ts.r2_nurse_key END,
         r2_auth_key = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'r2_auth_key'),'') ELSE ts.r2_auth_key END,
         img_sha256_nurse = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'img_sha256_nurse'),'') ELSE ts.img_sha256_nurse END,
         img_sha256_auth = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'img_sha256_auth'),'') ELSE ts.img_sha256_auth END,
         candidate_workflow_id = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(v_timesheet_patch->>'candidate_workflow_id','')::uuid ELSE ts.candidate_workflow_id END,
         candidate_workflow_generation = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(v_timesheet_patch->>'candidate_workflow_generation','')::integer ELSE ts.candidate_workflow_generation END,
         candidate_manager_approved_at_utc = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(v_timesheet_patch->>'candidate_manager_approved_at_utc','')::timestamptz
           ELSE ts.candidate_manager_approved_at_utc END,
         updated_at = v_now
   WHERE ts.timesheet_id = v_current_ts.timesheet_id
     AND ts.is_current = true
   RETURNING * INTO v_current_ts;

  UPDATE public.timesheets_financials AS tf
     SET processing_status = v_new_status,
         processed_by_user_id = p_actor_user_id,
         processed_at_utc = v_now,
         authorised_by_user_id = NULL,
         authorised_at_utc = NULL,
         updated_at = v_now
   WHERE tf.id = v_current_tsfin.id
     AND tf.is_current = true
   RETURNING * INTO v_current_tsfin;

  v_after_signature_json := public.timesheet_lifecycle_signature_v1(v_current_ts.timesheet_id, NULL::uuid, false);
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  PERFORM public._audit_insert(
    'timesheet',
    v_current_ts.timesheet_id::text,
    'TIMESHEET_DAILY_MANUAL_PROCESSED',
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'previous_processing_status', v_previous_status::text, 'previous_row_signature', v_current_row_signature),
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'new_processing_status', v_new_status::text, 'processed_at_utc', v_now, 'processed_by_user_id', p_actor_user_id, 'new_row_signature', v_after_row_signature),
    'DAILY_MANUAL_PROCESS',
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', 'daily_manual_process',
    'processed', true,
    'requested_timesheet_id', p_timesheet_id,
    'expected_timesheet_id', p_expected_timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'timesheet_id', v_current_ts.timesheet_id,
    'timesheet_financials_id', v_current_tsfin.id,
    'current_version', v_current_ts.version,
    'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id,
    'previous_status', v_previous_status::text,
    'processing_status', v_new_status::text,
    'new_processing_status', v_new_status::text,
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'status_transition', jsonb_build_object('from', v_previous_status::text, 'to', v_new_status::text, 'processed_at_utc', v_now, 'processed_by_user_id', p_actor_user_id),
    'affected_rows', jsonb_build_array(jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'row_key', 'timesheet:' || v_current_ts.timesheet_id::text)),
    'count_deltas', jsonb_build_object('unprocessed', -1, 'processed', 1),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials'), 'timesheet_id', v_current_ts.timesheet_id)
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'LOCK_TIMEOUT', 'message', 'The timesheet is currently locked by another operation.', 'timesheet_id', p_timesheet_id);
  END IF;
  RAISE;
END;
$function$;

-- timesheet_daily_manual_unprocess_atomic(uuid,uuid,uuid,timestamp with time zone,text)
CREATE OR REPLACE FUNCTION public.timesheet_daily_manual_unprocess_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_previous_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := 'UNPROCESSED'::public.ts_fin_processing_status_enum;
  v_has_segment_invoice_lock boolean := false;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_history jsonb := '{}'::jsonb;
BEGIN
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'TIMESHEET_ID_REQUIRED', 'message', 'p_timesheet_id is required.');
  END IF;

  IF p_expected_timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'EXPECTED_TIMESHEET_ID_REQUIRED', 'message', 'p_expected_timesheet_id is required.');
  END IF;

  IF p_actor_user_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'message', 'p_actor_user_id is required.');
  END IF;

  IF NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'operation', 'daily_manual_unprocess',
      'error_code', 'EXPECTED_ROW_SIGNATURE_REQUIRED',
      'message', 'The current lifecycle signature is required. Refresh the timesheet and try again.',
      'expected_timesheet_id', p_expected_timesheet_id
    );
  END IF;

  SELECT ts.*
    INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1;

  IF v_requested_ts.timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'TIMESHEET_NOT_FOUND', 'message', 'Timesheet was not found.', 'requested_timesheet_id', p_timesheet_id);
  END IF;

  SELECT ts.*
    INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.booking_id = v_requested_ts.booking_id
    AND ts.is_current = true
  ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_ts.timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'CURRENT_TIMESHEET_NOT_FOUND', 'message', 'Current timesheet was not found.', 'requested_timesheet_id', p_timesheet_id);
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', false,
      'operation', 'daily_manual_unprocess',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'Timesheet has moved to a newer current row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current_ts.timesheet_id,
      'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id
    );
  END IF;

  IF v_current_ts.sheet_scope IS DISTINCT FROM 'DAILY'::public.timesheet_scope_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'NOT_DAILY', 'message', 'Timesheet is not DAILY; daily manual unprocess only applies to DAILY sheets.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  IF v_current_ts.submission_mode IS DISTINCT FROM 'MANUAL'::public.submission_mode_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'NOT_MANUAL', 'message', 'Timesheet must be MANUAL before unprocessing.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  IF v_current_ts.archived_at_utc IS NOT NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'TIMESHEET_ARCHIVED', 'message', 'Archived timesheets must be Unarchived before lifecycle actions.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'TIMESHEET_AUTHORISED_EDIT_BLOCKED', 'message', 'This timesheet is authorised. Unauthorise it before unprocessing.', 'current_timesheet_id', v_current_ts.timesheet_id);
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
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'NO_TSFIN', 'message', 'No current financial snapshot exists for this timesheet.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_signature_v1(v_current_ts.timesheet_id, NULL::uuid, false);
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');

  IF COALESCE((v_before_signature_json ->> 'ok')::boolean, false) IS DISTINCT FROM true OR v_current_row_signature IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'operation', 'daily_manual_unprocess',
      'error_code', 'ROW_SIGNATURE_UNAVAILABLE',
      'message', 'Unable to compute the current lifecycle signature for this timesheet.',
      'current_timesheet_id', v_current_ts.timesheet_id
    );
  END IF;

  IF COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    RETURN jsonb_build_object(
      'ok', false,
      'operation', 'daily_manual_unprocess',
      'error_code', 'ROW_SIGNATURE_MISMATCH',
      'message', 'Timesheet changed after it was loaded. Refresh the row and try again.',
      'current_timesheet_id', v_current_ts.timesheet_id,
      'expected_row_signature', v_expected_row_signature,
      'current_row_signature', v_current_row_signature
    );
  END IF;

  v_previous_status := v_current_tsfin.processing_status;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json -> 'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL OR COALESCE(v_has_segment_invoice_lock, false) THEN
    RETURN jsonb_build_object(
      'ok', false,
      'operation', 'daily_manual_unprocess',
      'error_code', 'TIMESHEET_LOCKED_BY_INVOICE',
      'specific_error_code', 'TIMESHEET_LOCKED_BY_INVOICE',
      'message', 'Cannot unprocess: timesheet is locked by an invoice.',
      'current_timesheet_id', v_current_ts.timesheet_id,
      'current_row_signature', v_current_row_signature
    );
  END IF;

  IF v_previous_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'ALREADY_UNPROCESSED', 'message', 'Timesheet is already UNPROCESSED.', 'current_timesheet_id', v_current_ts.timesheet_id, 'previous_status', v_previous_status::text, 'current_row_signature', v_current_row_signature);
  END IF;

  IF v_previous_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN
    RETURN jsonb_build_object(
      'ok', false,
      'operation', 'daily_manual_unprocess',
      'error_code', 'PROCESSING_STATUS_NOT_UNPROCESSABLE',
      'message', 'Timesheet is not in a processing state that can be moved back to UNPROCESSED.',
      'current_timesheet_id', v_current_ts.timesheet_id,
      'previous_status', v_previous_status::text,
      'current_row_signature', v_current_row_signature
    );
  END IF;

  v_history := public.timesheet_removal_financial_history_v1(
    ARRAY[v_current_ts.timesheet_id]::uuid[],
    ARRAY[v_current_ts.booking_id]::text[],
    ARRAY[]::uuid[]
  );

  IF COALESCE((v_history ->> 'archive_required')::boolean, false) THEN
    RETURN jsonb_build_object(
      'ok', false,
      'success', false,
      'operation', 'daily_manual_unprocess',
      'error_code', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
      'specific_error_code', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
      'message', 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current_ts.timesheet_id,
      'timesheet_id', v_current_ts.timesheet_id,
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
    );
  END IF;

  UPDATE public.timesheets_financials AS tf
     SET processing_status = v_new_status,
         processed_by_user_id = NULL,
         processed_at_utc = NULL,
         authorised_by_user_id = NULL,
         authorised_at_utc = NULL,
         updated_at = v_now
   WHERE tf.id = v_current_tsfin.id
     AND tf.is_current = true
   RETURNING * INTO v_current_tsfin;

  IF v_current_tsfin.id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'TSFIN_UPDATE_FAILED', 'message', 'Failed to move daily timesheet back to UNPROCESSED.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_signature_v1(v_current_ts.timesheet_id, NULL::uuid, false);
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  PERFORM public._audit_insert(
    'timesheet',
    v_current_ts.timesheet_id::text,
    'TIMESHEET_DAILY_MANUAL_UNPROCESSED',
    jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'timesheet_financials_id', v_current_tsfin.id,
      'previous_processing_status', v_previous_status::text,
      'previous_row_signature', v_current_row_signature
    ),
    jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'timesheet_financials_id', v_current_tsfin.id,
      'new_processing_status', v_new_status::text,
      'processed_at_utc', NULL::text,
      'processed_by_user_id', NULL::text,
      'authorised_at_utc', NULL::text,
      'authorised_by_user_id', NULL::text,
      'new_row_signature', v_after_row_signature
    ),
    'DAILY_MANUAL_UNPROCESS',
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', 'daily_manual_unprocess',
    'unprocessed', true,
    'requested_timesheet_id', p_timesheet_id,
    'expected_timesheet_id', p_expected_timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'timesheet_id', v_current_ts.timesheet_id,
    'timesheet_financials_id', v_current_tsfin.id,
    'current_version', v_current_ts.version,
    'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id,
    'previous_status', v_previous_status::text,
    'previous_processing_status', v_previous_status::text,
    'processing_status', v_new_status::text,
    'new_processing_status', v_new_status::text,
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'status_transition', jsonb_build_object('from', v_previous_status::text, 'to', v_new_status::text, 'processed_at_utc', NULL::text, 'processed_by_user_id', NULL::text),
    'affected_rows', jsonb_build_array(jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'row_key', 'timesheet:' || v_current_ts.timesheet_id::text)),
    'count_deltas', jsonb_build_object('unprocessed', 1, 'processed', -1),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials'), 'timesheet_id', v_current_ts.timesheet_id)
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'LOCK_TIMEOUT', 'message', 'The timesheet is currently locked by another operation.', 'timesheet_id', p_timesheet_id);
  END IF;
  RAISE;
END;
$function$;

-- timesheet_daily_manual_unprocess_atomic(uuid,uuid,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.timesheet_daily_manual_unprocess_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN public.timesheet_daily_manual_unprocess_atomic(
    p_timesheet_id => p_timesheet_id,
    p_expected_timesheet_id => p_expected_timesheet_id,
    p_actor_user_id => p_actor_user_id,
    p_now_utc => p_now_utc,
    p_expected_row_signature => NULL::text
  );
END;
$function$;

-- timesheet_doc_flags_batch(uuid[])
CREATE OR REPLACE FUNCTION public.timesheet_doc_flags_batch(p_timesheet_ids uuid[])
 RETURNS TABLE(timesheet_id uuid, candidate_id uuid, candidate_name text, client_id uuid, client_name text, sheet_scope text, week_ending_date date, qr_status text, qr_token text, qr_generated_at timestamp with time zone, qr_scanned_at timestamp with time zone, qr_signed_hash text, current_refs_sig text, qr_sent_refs_sig text, generated_pdf_refs_sig text, qr_refs_changed boolean, electronic_refs_changed boolean)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
with wanted as (
  select distinct unnest(p_timesheet_ids) as timesheet_id
  where p_timesheet_ids is not null
),
t as (
  select
    ts.timesheet_id,
    ts.sheet_scope,
    ts.submission_mode,
    ts.week_ending_date,
    ts.contract_id,

    ts.qr_status,
    ts.qr_token,
    ts.qr_generated_at,
    ts.qr_scanned_at,
    ts.qr_signed_hash,

    ts.qr_sent_refs_sig,
    ts.generated_pdf_at_utc,
    ts.generated_pdf_refs_sig,

    ts.occupant_key_norm,
    ts.hospital_norm
  from wanted w
  join public.timesheets ts
    on ts.timesheet_id = w.timesheet_id
   and ts.is_current = true
),
tf as (
  select
    tf0.timesheet_id,
    tf0.candidate_id,
    tf0.client_id
  from public.timesheets_financials tf0
  join wanted w
    on w.timesheet_id = tf0.timesheet_id
  where tf0.is_current = true
),
ct as (
  select
    t0.timesheet_id,
    ct0.candidate_id as contract_candidate_id,
    ct0.client_id as contract_client_id
  from t t0
  left join public.contracts ct0
    on ct0.id = t0.contract_id
),
vs as (
  select
    v.timesheet_id,
    v.candidate_id as vs_candidate_id,
    v.client_id as vs_client_id,
    v.candidate_name as vs_candidate_name,
    v.client_name as vs_client_name
  from t
  left join public.v_timesheets_summary v
    on v.timesheet_id = t.timesheet_id
),
ids as (
  select
    t1.timesheet_id,
    coalesce(vs1.vs_candidate_id, tf1.candidate_id, ct1.contract_candidate_id) as eff_candidate_id,
    coalesce(vs1.vs_client_id,    tf1.client_id,    ct1.contract_client_id)    as eff_client_id,
    vs1.vs_candidate_name,
    vs1.vs_client_name
  from t t1
  left join vs vs1
    on vs1.timesheet_id = t1.timesheet_id
  left join tf tf1
    on tf1.timesheet_id = t1.timesheet_id
  left join ct ct1
    on ct1.timesheet_id = t1.timesheet_id
),
cand as (
  select c0.id, c0.display_name
  from public.candidates c0
),
cli as (
  select cl0.id, cl0.name
  from public.clients cl0
),
sig as (
  select
    t2.timesheet_id,
    public.timesheet_pdf_reference_sig(t2.timesheet_id) as current_refs_sig
  from t t2
)
select
  t3.timesheet_id,

  ids2.eff_candidate_id as candidate_id,
  coalesce(ids2.vs_candidate_name, c2.display_name, t3.occupant_key_norm) as candidate_name,

  ids2.eff_client_id as client_id,
  coalesce(ids2.vs_client_name, cl2.name, t3.hospital_norm) as client_name,

  t3.sheet_scope::text as sheet_scope,
  t3.week_ending_date as week_ending_date,

  t3.qr_status::text as qr_status,
  nullif(btrim(coalesce(t3.qr_token, '')), '') as qr_token,
  t3.qr_generated_at,
  t3.qr_scanned_at,
  nullif(btrim(coalesce(t3.qr_signed_hash, '')), '') as qr_signed_hash,

  s3.current_refs_sig as current_refs_sig,
  nullif(btrim(coalesce(t3.qr_sent_refs_sig, '')), '') as qr_sent_refs_sig,
  nullif(btrim(coalesce(t3.generated_pdf_refs_sig, '')), '') as generated_pdf_refs_sig,

  (
    upper(coalesce(t3.qr_status::text, '')) = 'PENDING'
    and nullif(btrim(coalesce(t3.qr_token, '')), '') is not null
    and t3.qr_generated_at is not null
    and t3.qr_scanned_at is null
    and nullif(btrim(coalesce(t3.qr_signed_hash, '')), '') is null
    and (
      nullif(btrim(coalesce(t3.qr_sent_refs_sig, '')), '') is null
      or nullif(btrim(coalesce(t3.qr_sent_refs_sig, '')), '') is distinct from s3.current_refs_sig
    )
  ) as qr_refs_changed,

  (
    upper(coalesce(t3.submission_mode::text, '')) = 'ELECTRONIC'
    and t3.generated_pdf_at_utc is not null
    and (
      nullif(btrim(coalesce(t3.generated_pdf_refs_sig, '')), '') is null
      or nullif(btrim(coalesce(t3.generated_pdf_refs_sig, '')), '') is distinct from s3.current_refs_sig
    )
  ) as electronic_refs_changed
from t t3
left join ids ids2
  on ids2.timesheet_id = t3.timesheet_id
left join cand c2
  on c2.id = ids2.eff_candidate_id
left join cli cl2
  on cl2.id = ids2.eff_client_id
join sig s3
  on s3.timesheet_id = t3.timesheet_id
order by t3.timesheet_id;
$function$;

-- timesheet_expense_apply_atomic_v1(uuid,text,uuid,uuid,integer,text,jsonb,uuid[],text,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.timesheet_expense_apply_atomic_v1(p_candidate_id uuid, p_environment text, p_target_timesheet_id uuid, p_workflow_id uuid, p_expected_workflow_generation integer, p_expected_row_signature text, p_claim_json jsonb, p_evidence_component_ids uuid[], p_idempotency_key text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_environment text;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_anchor_timesheet public.timesheets%rowtype;
  v_target_timesheet_id uuid:=p_target_timesheet_id;
  v_claim jsonb:=coalesce(p_claim_json,'{}'::jsonb);
  v_snapshot jsonb;
  v_result jsonb;
  v_capabilities jsonb;
  v_response jsonb;
  v_component public.candidate_submission_components%rowtype;
  v_candidate_signature public.candidate_submission_components%rowtype;
  v_manager_signature public.candidate_submission_components%rowtype;
  v_electronic_patch jsonb:='{}'::jsonb;
  v_is_separate_carrier boolean:=false;
  v_is_paper boolean:=false;
  v_component_count integer:=0;
  v_required_categories text[]:='{}'::text[];
  v_category text;
  v_kind text;
  v_document_role text;
  v_paper_page jsonb;
  v_materialised_storage_key text;
  v_system_actor uuid;
  v_constraint_name text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_expense_atomic_placement');
  if p_candidate_id is null or p_workflow_id is null or p_expected_workflow_generation is null
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null or jsonb_typeof(v_claim)<>'object' then
    raise exception 'CANDIDATE_EXPENSE_APPLY_PAYLOAD_INVALID' using errcode='22023';
  end if;
  select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id for update;
  if not found or v_workflow.environment<>v_environment or v_workflow.candidate_id<>p_candidate_id then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.last_mutation_idempotency_key=p_idempotency_key and v_workflow.last_mutation_response_json is not null then
    return v_workflow.last_mutation_response_json||jsonb_build_object('idempotent_replay',true);
  end if;
  if v_workflow.generation<>p_expected_workflow_generation then
    raise exception 'WORKFLOW_VERSION_MISMATCH'
      using errcode='40001',detail=jsonb_build_object('code','WORKFLOW_VERSION_MISMATCH','current_generation',v_workflow.generation)::text;
  end if;
  v_is_paper:=v_workflow.route='PAPER';
  if ((not v_is_paper and (
          v_workflow.state<>'READY_TO_FINALISE'
          or v_workflow.manager_approved_at_utc is null
        ))
      or (v_is_paper and v_workflow.state<>'RECEIVED')
      or current_setting('cloudtms.candidate_finalize_workflow',true)
        is distinct from v_workflow.id::text||':'||v_workflow.generation::text) then
    raise exception 'CANDIDATE_EXPENSE_APPLY_FINALISE_ONLY' using errcode='55000';
  end if;
  if v_workflow.workflow_kind not in ('CONTRACT_EXPENSE','CONTRACT_COMBINED') then
    raise exception 'CANDIDATE_EXPENSE_WORKFLOW_REQUIRED' using errcode='22023';
  end if;
  v_is_separate_carrier:=v_workflow.workflow_kind='CONTRACT_EXPENSE'
    or v_workflow.target_timesheet_id is distinct from v_workflow.anchor_timesheet_id;
  if v_workflow.workflow_kind='CONTRACT_COMBINED' and not v_is_separate_carrier and not v_is_paper then
    select * into v_candidate_signature from public.candidate_submission_components
    where id=v_workflow.candidate_signature_component_id and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation and state='IMMUTABLE';
    select * into v_manager_signature from public.candidate_submission_components
    where id=v_workflow.manager_signature_component_id and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation and state='IMMUTABLE';
    if v_candidate_signature.id is null or v_manager_signature.id is null
       or v_candidate_signature.source_content_sha256 is distinct from v_workflow.candidate_signature_sha256
       or v_manager_signature.source_content_sha256 is distinct from v_workflow.manager_signature_sha256 then
      raise exception 'ELECTRONIC_SIGNATURE_PAIR_INCOMPLETE' using errcode='55000';
    end if;
    v_electronic_patch:=jsonb_build_object(
      'submission_mode','ELECTRONIC','auth_name',v_workflow.manager_name,
      'auth_job_title',v_workflow.manager_position,'r2_nurse_key',v_candidate_signature.storage_key,
      'r2_auth_key',v_manager_signature.storage_key,
      'img_sha256_nurse',encode(v_candidate_signature.source_content_sha256,'hex'),
      'img_sha256_auth',encode(v_manager_signature.source_content_sha256,'hex'),
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',v_workflow.manager_approved_at_utc);
  elsif v_is_paper then
    v_electronic_patch:=jsonb_build_object(
      'submission_mode','MANUAL',
      'r2_nurse_key',null,
      'r2_auth_key',null,
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',null
    );
  end if;
  if v_workflow.immutable_submission_json is null
     or v_workflow.immutable_submission_sha256 is null
     or (
       (v_workflow.workflow_kind='CONTRACT_EXPENSE'
         and (private._candidate_sha256_jsonb_v1(v_claim) is distinct from v_workflow.immutable_submission_sha256
           or v_claim is distinct from v_workflow.immutable_submission_json))
       or (v_workflow.workflow_kind='CONTRACT_COMBINED'
         and v_claim is distinct from v_workflow.immutable_submission_json
         and v_claim is distinct from v_workflow.immutable_submission_json->'expense_submission')
     ) then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_MISMATCH' using errcode='40001';
  end if;
  select * into v_week from public.contract_weeks
  where id=v_workflow.contract_week_id for update;
  if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id and candidate_id=p_candidate_id for update;
  if not found then raise exception 'CANDIDATE_WORKFLOW_OWNERSHIP_MISMATCH' using errcode='28000'; end if;
  if v_workflow.anchor_timesheet_id is not null then
    select * into v_anchor_timesheet from public.timesheets
    where timesheet_id=v_workflow.anchor_timesheet_id and is_current=true;
  end if;
  select candidate_app_system_actor_user_id into v_system_actor from public.settings_defaults where id=1;
  if v_system_actor is null then
    raise exception 'CANDIDATE_SYSTEM_ACTOR_NOT_CONFIGURED' using errcode='55000';
  end if;
  if v_target_timesheet_id is not null and v_week.timesheet_id is distinct from v_target_timesheet_id then
    raise exception 'TIMESHEET_MOVED' using errcode='40001';
  end if;

  v_snapshot:=v_claim->'canonical_tsfin_snapshot';
  if v_snapshot is null or jsonb_typeof(v_snapshot)<>'object' then
    raise exception 'CANDIDATE_CANONICAL_TSFIN_SNAPSHOT_REQUIRED' using errcode='22023';
  end if;
  if nullif(v_snapshot->>'candidate_id','')::uuid is distinct from p_candidate_id
     or nullif(v_snapshot->>'client_id','')::uuid is distinct from v_contract.client_id then
    raise exception 'TSFIN_SNAPSHOT_MISMATCH' using errcode='22023';
  end if;
  if coalesce(nullif(v_snapshot->>'expenses_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'expenses_charge_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'mileage_units','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'mileage_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'mileage_charge_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'travel_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'travel_charge_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'accommodation_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'accommodation_charge_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'other_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'other_charge_ex_vat','')::numeric,0)<0 then
    raise exception 'CANDIDATE_EXPENSE_VALUE_INVALID' using errcode='22023';
  end if;

  if coalesce(nullif(v_snapshot->>'travel_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'travel_charge_ex_vat','')::numeric,0)>0 then
    v_required_categories:=array_append(v_required_categories,'TRAVEL');
  end if;
  if coalesce(nullif(v_snapshot->>'accommodation_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'accommodation_charge_ex_vat','')::numeric,0)>0 then
    v_required_categories:=array_append(v_required_categories,'ACCOMMODATION');
  end if;
  if coalesce(nullif(v_snapshot->>'other_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'other_charge_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'expenses_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'expenses_charge_ex_vat','')::numeric,0)>0 then
    v_required_categories:=array_append(v_required_categories,'OTHER');
  end if;
  if coalesce(nullif(v_snapshot->>'mileage_units','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'mileage_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'mileage_charge_ex_vat','')::numeric,0)>0 then
    v_required_categories:=array_append(v_required_categories,'MILEAGE');
  end if;

  select count(*)::integer into v_component_count
  from public.candidate_submission_components component
  where component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[]))
    and component.workflow_id=v_workflow.id
    and component.workflow_generation=v_workflow.generation
    and component.state='IMMUTABLE'
    and component.immutable_at_utc is not null
    and (
      (not v_is_paper
        and component.required=true
        and component.component_kind<>'HOURS_TIMESHEET'
        and component.review_render_state='READY'
        and component.final_signed_render_state='READY'
        and component.final_signed_content_sha256 is not null)
      or (v_is_paper
        and component.component_kind='SIGNED_RETURN'
        and component.source_content_sha256 is not null
        and component.paper_return_page_key<>'HOURS_TIMESHEET')
    );
  if v_component_count<>coalesce(cardinality(p_evidence_component_ids),0) then
    raise exception 'CANDIDATE_EVIDENCE_COMPONENT_INVALID' using errcode='22023';
  end if;
  if not v_is_paper and exists(
    select 1 from public.candidate_submission_components component
    where component.workflow_id=v_workflow.id
      and component.workflow_generation=v_workflow.generation
      and component.required=true and component.state<>'SUPERSEDED'
      and component.component_kind<>'HOURS_TIMESHEET'
      and not (component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[])))
  ) then
    raise exception 'CANDIDATE_EVIDENCE_COMPONENT_SET_INCOMPLETE' using errcode='22023';
  end if;
  if v_is_paper and exists(
    select 1
    from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
    where expected_page->>'page_key'<>'HOURS_TIMESHEET'
      and not exists(
        select 1
        from public.candidate_submission_components component
        where component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[]))
          and component.workflow_id=v_workflow.id
          and component.workflow_generation=v_workflow.generation
          and component.component_kind='SIGNED_RETURN'
          and component.paper_return_page_key=expected_page->>'page_key'
          and component.state='IMMUTABLE'
      )
  ) then
    raise exception 'CANDIDATE_EVIDENCE_COMPONENT_SET_INCOMPLETE' using errcode='22023';
  end if;
  foreach v_category in array v_required_categories loop
    if not exists(
      select 1
      from public.candidate_submission_components component
      where component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[]))
        and component.workflow_id=v_workflow.id and component.workflow_generation=v_workflow.generation
        and component.state='IMMUTABLE'
        and (
          (not v_is_paper
            and component.expense_category=v_category
            and component.final_signed_render_state='READY'
            and component.document_role in ('SOURCE_EVIDENCE','MILEAGE_CLAIM_FORM'))
          or (v_is_paper and exists(
            select 1
            from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
            where expected_page->>'page_key'=component.paper_return_page_key
              and expected_page->>'expense_category'=v_category
          ))
        )
    ) then
      raise exception 'EXPENSE_EVIDENCE_REQUIRED'
        using errcode='22023',detail=jsonb_build_object('code','EXPENSE_EVIDENCE_REQUIRED','category',v_category)::text;
    end if;
  end loop;
  if 'MILEAGE'=any(v_required_categories) and not exists(
    select 1 from public.candidate_submission_components component
    where component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[]))
      and component.workflow_id=v_workflow.id
      and component.workflow_generation=v_workflow.generation
      and component.state='IMMUTABLE'
      and (
        (not v_is_paper
          and component.component_kind='MILEAGE_FORM'
          and component.expense_category='MILEAGE'
          and component.final_signed_render_state='READY')
        or (v_is_paper and exists(
          select 1
          from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
          where expected_page->>'page_key'=component.paper_return_page_key
            and expected_page->>'component_kind'='MILEAGE_FORM'
            and expected_page->>'expense_category'='MILEAGE'
        ))
      )
  ) then
    raise exception 'EXPENSE_EVIDENCE_REQUIRED'
      using errcode='22023',detail=jsonb_build_object(
        'code','EXPENSE_EVIDENCE_REQUIRED','category','MILEAGE','document_role','MILEAGE_CLAIM_FORM')::text;
  end if;

  v_result:=public.contract_week_manual_upsert_atomic(
    p_week_id=>v_week.id,
    p_expected_timesheet_id=>v_target_timesheet_id,
    p_timesheet_create_json=>case when v_target_timesheet_id is null then
      coalesce(v_claim->'timesheet_create_json','{}'::jsonb)
        ||case when v_is_separate_carrier then jsonb_build_object(
          'booking_id','CANDIDATE-EXPENSE-'||replace(v_week.id::text,'-',''),
          'contract_id',v_week.contract_id,
          'week_ending_date',v_week.week_ending_date,
          'status','SUBMITTED',
          'submission_mode','MANUAL',
          'sheet_scope','WEEKLY',
          'line_type','EXPENSES',
          'is_adjustment',true,
          'occupant_key_norm',v_anchor_timesheet.occupant_key_norm,
          'hospital_norm',v_anchor_timesheet.hospital_norm,
          'ward_norm',v_anchor_timesheet.ward_norm,
          'job_title_norm',v_anchor_timesheet.job_title_norm,
          'shift_label_norm','weekly-expenses',
          'band',v_anchor_timesheet.band,
          'candidate_hint_text',coalesce(v_anchor_timesheet.candidate_hint_text,'{}'::jsonb),
          'manual_pdf_r2_key',null
        ) else v_electronic_patch end
      else null end,
    p_timesheet_patch_json=>coalesce(v_claim->'timesheet_patch_json','{}'::jsonb)
      ||case when v_is_separate_carrier then '{}'::jsonb else v_electronic_patch end,
    p_contract_week_patch_json=>coalesce(v_claim->'contract_week_patch_json','{}'::jsonb),
    p_tsfin_snapshot_json=>v_snapshot,
    p_rotation_json=>null,
    p_actor_user_id=>v_system_actor,
    p_materialise_staged_evidence=>false,
    p_now_utc=>p_now_utc,
    p_expected_row_signature=>p_expected_row_signature,
    p_queue_timesheet_materialisation_json=>jsonb_build_object('suppress_timesheet_evidence_materialisation',true)
  );
  v_target_timesheet_id:=coalesce(nullif(v_result->>'timesheet_id','')::uuid,nullif(v_result#>>'{timesheet,timesheet_id}','')::uuid);
  if v_target_timesheet_id is null then raise exception 'CANDIDATE_EXPENSE_TARGET_NOT_CREATED' using errcode='55000'; end if;

  for v_component in
    select * from public.candidate_submission_components
    where id=any(coalesce(p_evidence_component_ids,'{}'::uuid[])) order by component_no,id for update
  loop
    update public.candidate_submission_components set timesheet_id=v_target_timesheet_id
    where id=v_component.id;
    if v_is_paper then
      select expected_page into v_paper_page
      from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
      where expected_page->>'page_key'=v_component.paper_return_page_key;
      v_category:=nullif(v_paper_page->>'expense_category','');
      v_document_role:=case v_paper_page->>'component_kind'
        when 'EXPENSE_SUMMARY' then 'EXPENSE_MILEAGE_APPROVAL_SUMMARY'
        when 'MILEAGE_FORM' then 'MILEAGE_CLAIM_FORM'
        else 'SOURCE_EVIDENCE' end;
      v_materialised_storage_key:=v_component.storage_key;
    else
      v_category:=v_component.expense_category;
      v_document_role:=v_component.document_role;
      v_materialised_storage_key:=v_component.final_signed_storage_key;
    end if;
    v_kind:=case
      when v_document_role='EXPENSE_MILEAGE_APPROVAL_SUMMARY' then 'OTHER'
      when v_document_role='MILEAGE_CLAIM_FORM' then 'MILEAGE'
      when v_document_role='SIGNED_TIMESHEET' then 'TIMESHEET'
      when v_category='MILEAGE' then 'MILEAGE'
      else v_category end;
    insert into public.timesheet_evidence(
      timesheet_id,kind,display_name,storage_key,created_at,document_role,candidate_component_id,processing_state
    ) values (
      v_target_timesheet_id,v_kind,coalesce(nullif(v_claim->>'evidence_display_name',''),'Candidate submission evidence'),
       v_materialised_storage_key,p_now_utc,v_document_role,v_component.id,'READY'
    ) on conflict (candidate_component_id) where candidate_component_id is not null do nothing;
  end loop;

  v_capabilities:=private._candidate_record_capabilities_v1(v_target_timesheet_id,v_week.id,'{}'::jsonb);
  if v_capabilities->>'record_role'='CONFLICT' then
    raise exception 'HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS'
      using errcode='55000',detail=v_capabilities::text;
  end if;
  v_response:=jsonb_build_object(
    'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
    'target_timesheet_id',v_target_timesheet_id,'target_contract_week_id',v_week.id,
    'canonical_result',v_result,'capabilities',v_capabilities,'idempotent_replay',false
  );
  update public.candidate_submission_workflows set
    target_timesheet_id=v_target_timesheet_id,contract_week_id=v_week.id,updated_at_utc=p_now_utc
  where id=v_workflow.id;
  perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,'CANDIDATE_EXPENSE_APPLIED',null,
    jsonb_build_object('timesheet_id',v_target_timesheet_id,'contract_week_id',v_week.id,'component_count',v_component_count),
    null,v_system_actor,p_idempotency_key,p_now_utc);
  return v_response;
exception when unique_violation then
  get stacked diagnostics v_constraint_name=constraint_name;
  if v_constraint_name='candidate_submission_components_source_sha256_uq' then
    raise exception 'CANDIDATE_EVIDENCE_BYTES_ALREADY_USED' using errcode='23505';
  elsif v_constraint_name='timesheet_evidence_one_active_timesheet_uq' then
    raise exception 'TIMESHEET_EVIDENCE_ALREADY_ATTACHED' using errcode='23505';
  end if;
  raise;
end;
$function$;

-- timesheet_financial_retention_assert_complete_v1()
CREATE OR REPLACE FUNCTION public.timesheet_financial_retention_assert_complete_v1()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_result jsonb := public.timesheet_financial_retention_completeness_v1(100);
BEGIN
  IF COALESCE((v_result ->> 'complete')::boolean, false) IS DISTINCT FROM true THEN
    RAISE EXCEPTION USING
      MESSAGE = 'RETENTION_MARKER_BACKFILL_INCOMPLETE',
      DETAIL = v_result::text;
  END IF;

  RETURN v_result;
END;
$function$;

-- timesheet_financial_retention_backfill_v1(uuid,integer)
CREATE OR REPLACE FUNCTION public.timesheet_financial_retention_backfill_v1(p_after_timesheet_id uuid DEFAULT NULL::uuid, p_limit integer DEFAULT 500)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 500), 1), 1000);
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_mark_result jsonb := '{}'::jsonb;
  v_next_after_timesheet_id uuid := NULL;
BEGIN
  WITH retained_candidates AS MATERIALIZED (
    SELECT tf.timesheet_id
    FROM public.timesheets_financials AS tf
    WHERE tf.timesheet_id IS NOT NULL
      AND (
        tf.paid_at_utc IS NOT NULL
        OR tf.paid_by_user_id IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(tf.payment_reference, '')), '') IS NOT NULL
        OR tf.remittance_last_sent_at_utc IS NOT NULL
        OR COALESCE(tf.remittance_send_count, 0) > 0
      )
    UNION
    SELECT pbi.timesheet_id
    FROM public.pay_batch_items AS pbi
    WHERE pbi.timesheet_id IS NOT NULL
    UNION
    SELECT forced.timesheet_id
    FROM public.pay_batches AS pb
    CROSS JOIN LATERAL unnest(COALESCE(pb.force_include_timesheet_ids, ARRAY[]::uuid[])) AS forced(timesheet_id)
    WHERE forced.timesheet_id IS NOT NULL
    UNION
    SELECT snapshot.timesheet_id
    FROM public.pay_batch_timesheet_snapshots AS snapshot
    WHERE snapshot.timesheet_id IS NOT NULL
    UNION
    SELECT state.timesheet_id
    FROM public.timesheet_pay_state AS state
    WHERE state.timesheet_id IS NOT NULL
      AND (
        state.last_settled_pay_batch_id IS NOT NULL
        OR state.last_settled_at_utc IS NOT NULL
        OR state.last_settled_signature IS NOT NULL
        OR state.last_settled_snapshot_json IS NOT NULL
        OR state.summary_pay_paid_at_utc IS NOT NULL
        OR UPPER(COALESCE(state.summary_pay_status_code, 'UNPAID')) IN (
          'PAID', 'PARTIALLY_PAID', 'PART_PAID', 'OVERPAID', 'UNDERPAID', 'PROCESSING'
        )
      )
    UNION
    SELECT history.timesheet_id
    FROM public.timesheet_pay_state_history AS history
    WHERE history.timesheet_id IS NOT NULL
    UNION
    SELECT adjustment.timesheet_id
    FROM public.ts_pay_adjustments AS adjustment
    WHERE adjustment.timesheet_id IS NOT NULL
    UNION
    SELECT component.linked_timesheet_id
    FROM public.pay_finance_case_components AS component
    WHERE component.linked_timesheet_id IS NOT NULL
    UNION
    SELECT advance.linked_timesheet_id
    FROM public.pay_advances AS advance
    WHERE advance.linked_timesheet_id IS NOT NULL
    UNION
    SELECT correction.timesheet_id
    FROM public.pay_payment_correction_items AS correction
    WHERE correction.timesheet_id IS NOT NULL
    UNION
    SELECT carry_forward.timesheet_id
    FROM public.pay_manual_adjustment_carry_forwards AS carry_forward
    WHERE carry_forward.timesheet_id IS NOT NULL
    UNION
    SELECT override_row.timesheet_id
    FROM public.timesheet_payment_overrides AS override_row
    WHERE override_row.timesheet_id IS NOT NULL
  ), next_page AS (
    SELECT retained.timesheet_id
    FROM retained_candidates AS retained
    WHERE (p_after_timesheet_id IS NULL OR retained.timesheet_id > p_after_timesheet_id)
      AND EXISTS (
        SELECT 1
        FROM public.timesheets AS existing_timesheet
        WHERE existing_timesheet.timesheet_id = retained.timesheet_id
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.timesheet_financial_retention AS marker
        WHERE marker.timesheet_id = retained.timesheet_id
      )
    ORDER BY retained.timesheet_id
    LIMIT v_limit
  )
  SELECT COALESCE(array_agg(next_page.timesheet_id ORDER BY next_page.timesheet_id), ARRAY[]::uuid[])
    INTO v_timesheet_ids
  FROM next_page;

  IF COALESCE(array_length(v_timesheet_ids, 1), 0) = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'done', true,
      'page_count', 0,
      'next_after_timesheet_id', p_after_timesheet_id,
      'mark_result', jsonb_build_object(
        'requested_count', 0,
        'inserted_count', 0,
        'already_marked_count', 0
      )
    );
  END IF;

  v_mark_result := public.timesheet_financial_retention_mark_v1(v_timesheet_ids);
  v_next_after_timesheet_id := v_timesheet_ids[array_length(v_timesheet_ids, 1)];

  RETURN jsonb_build_object(
    'ok', true,
    'done', COALESCE(array_length(v_timesheet_ids, 1), 0) < v_limit,
    'page_count', COALESCE(array_length(v_timesheet_ids, 1), 0),
    'next_after_timesheet_id', v_next_after_timesheet_id,
    'mark_result', v_mark_result
  );
END;
$function$;

-- timesheet_financial_retention_capture_trigger_v1()
CREATE OR REPLACE FUNCTION public.timesheet_financial_retention_capture_trigger_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $function$
DECLARE
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
BEGIN
  IF TG_TABLE_SCHEMA IS DISTINCT FROM 'public' THEN
    RAISE EXCEPTION USING MESSAGE = 'RETENTION_CAPTURE_UNEXPECTED_SCHEMA';
  END IF;

  IF TG_TABLE_NAME = 'timesheets_financials' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(array_agg(DISTINCT n.timesheet_id ORDER BY n.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids
      FROM new_rows AS n
      WHERE n.timesheet_id IS NOT NULL
        AND (
          n.paid_at_utc IS NOT NULL
          OR n.paid_by_user_id IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(n.payment_reference, '')), '') IS NOT NULL
          OR n.remittance_last_sent_at_utc IS NOT NULL
          OR COALESCE(n.remittance_send_count, 0) > 0
        );
    ELSE
      WITH changed AS (
        SELECT o.timesheet_id
        FROM old_rows AS o
        JOIN new_rows AS n ON n.id = o.id
        WHERE o.timesheet_id IS NOT NULL
          AND (
            o.paid_at_utc IS NOT NULL
            OR o.paid_by_user_id IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(o.payment_reference, '')), '') IS NOT NULL
            OR o.remittance_last_sent_at_utc IS NOT NULL
            OR COALESCE(o.remittance_send_count, 0) > 0
          )
          AND (
            o.timesheet_id IS DISTINCT FROM n.timesheet_id
            OR NOT (
              n.paid_at_utc IS NOT NULL
              OR n.paid_by_user_id IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(n.payment_reference, '')), '') IS NOT NULL
              OR n.remittance_last_sent_at_utc IS NOT NULL
              OR COALESCE(n.remittance_send_count, 0) > 0
            )
          )
        UNION
        SELECT n.timesheet_id
        FROM new_rows AS n
        JOIN old_rows AS o ON o.id = n.id
        WHERE n.timesheet_id IS NOT NULL
          AND (
            n.paid_at_utc IS NOT NULL
            OR n.paid_by_user_id IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(n.payment_reference, '')), '') IS NOT NULL
            OR n.remittance_last_sent_at_utc IS NOT NULL
            OR COALESCE(n.remittance_send_count, 0) > 0
          )
          AND (
            o.timesheet_id IS DISTINCT FROM n.timesheet_id
            OR NOT (
              o.paid_at_utc IS NOT NULL
              OR o.paid_by_user_id IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(o.payment_reference, '')), '') IS NOT NULL
              OR o.remittance_last_sent_at_utc IS NOT NULL
              OR COALESCE(o.remittance_send_count, 0) > 0
            )
          )
      )
      SELECT COALESCE(array_agg(changed.timesheet_id ORDER BY changed.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids
      FROM changed;
    END IF;

  ELSIF TG_TABLE_NAME = 'pay_batch_items' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(array_agg(DISTINCT n.timesheet_id ORDER BY n.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids
      FROM new_rows AS n
      WHERE n.timesheet_id IS NOT NULL;
    ELSE
      WITH changed AS (
        SELECT o.timesheet_id
        FROM old_rows AS o JOIN new_rows AS n ON n.id = o.id
        WHERE o.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
        UNION
        SELECT n.timesheet_id
        FROM new_rows AS n JOIN old_rows AS o ON o.id = n.id
        WHERE n.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
      )
      SELECT COALESCE(array_agg(changed.timesheet_id ORDER BY changed.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM changed;
    END IF;

  ELSIF TG_TABLE_NAME = 'pay_batches' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(array_agg(DISTINCT ids.timesheet_id ORDER BY ids.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids
      FROM new_rows AS n
      CROSS JOIN LATERAL unnest(COALESCE(n.force_include_timesheet_ids, ARRAY[]::uuid[])) AS ids(timesheet_id)
      WHERE ids.timesheet_id IS NOT NULL;
    ELSE
      WITH changed AS (
        SELECT old_ids.timesheet_id
        FROM old_rows AS o
        JOIN new_rows AS n ON n.id = o.id
        CROSS JOIN LATERAL unnest(COALESCE(o.force_include_timesheet_ids, ARRAY[]::uuid[])) AS old_ids(timesheet_id)
        WHERE old_ids.timesheet_id IS NOT NULL
          AND NOT (old_ids.timesheet_id = ANY(COALESCE(n.force_include_timesheet_ids, ARRAY[]::uuid[])))
        UNION
        SELECT new_ids.timesheet_id
        FROM new_rows AS n
        JOIN old_rows AS o ON o.id = n.id
        CROSS JOIN LATERAL unnest(COALESCE(n.force_include_timesheet_ids, ARRAY[]::uuid[])) AS new_ids(timesheet_id)
        WHERE new_ids.timesheet_id IS NOT NULL
          AND NOT (new_ids.timesheet_id = ANY(COALESCE(o.force_include_timesheet_ids, ARRAY[]::uuid[])))
      )
      SELECT COALESCE(array_agg(changed.timesheet_id ORDER BY changed.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM changed;
    END IF;

  ELSIF TG_TABLE_NAME = 'pay_batch_timesheet_snapshots' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(array_agg(DISTINCT n.timesheet_id ORDER BY n.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM new_rows AS n WHERE n.timesheet_id IS NOT NULL;
    ELSE
      WITH changed AS (
        SELECT o.timesheet_id FROM old_rows AS o JOIN new_rows AS n ON n.id = o.id
        WHERE o.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
        UNION
        SELECT n.timesheet_id FROM new_rows AS n JOIN old_rows AS o ON o.id = n.id
        WHERE n.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
      )
      SELECT COALESCE(array_agg(changed.timesheet_id ORDER BY changed.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM changed;
    END IF;

  ELSIF TG_TABLE_NAME = 'timesheet_pay_state' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(array_agg(DISTINCT n.timesheet_id ORDER BY n.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids
      FROM new_rows AS n
      WHERE n.timesheet_id IS NOT NULL
        AND (
          n.last_settled_pay_batch_id IS NOT NULL
          OR n.last_settled_at_utc IS NOT NULL
          OR n.last_settled_signature IS NOT NULL
          OR n.last_settled_snapshot_json IS NOT NULL
          OR n.summary_pay_paid_at_utc IS NOT NULL
          OR UPPER(COALESCE(n.summary_pay_status_code, 'UNPAID')) IN (
            'PAID', 'PARTIALLY_PAID', 'PART_PAID', 'OVERPAID', 'UNDERPAID', 'PROCESSING'
          )
        );
    ELSE
      WITH old_retained AS (
        SELECT o.timesheet_id
        FROM old_rows AS o
        WHERE o.timesheet_id IS NOT NULL
          AND (
            o.last_settled_pay_batch_id IS NOT NULL
            OR o.last_settled_at_utc IS NOT NULL
            OR o.last_settled_signature IS NOT NULL
            OR o.last_settled_snapshot_json IS NOT NULL
            OR o.summary_pay_paid_at_utc IS NOT NULL
            OR UPPER(COALESCE(o.summary_pay_status_code, 'UNPAID')) IN (
              'PAID', 'PARTIALLY_PAID', 'PART_PAID', 'OVERPAID', 'UNDERPAID', 'PROCESSING'
            )
          )
      ), new_retained AS (
        SELECT n.timesheet_id
        FROM new_rows AS n
        WHERE n.timesheet_id IS NOT NULL
          AND (
            n.last_settled_pay_batch_id IS NOT NULL
            OR n.last_settled_at_utc IS NOT NULL
            OR n.last_settled_signature IS NOT NULL
            OR n.last_settled_snapshot_json IS NOT NULL
            OR n.summary_pay_paid_at_utc IS NOT NULL
            OR UPPER(COALESCE(n.summary_pay_status_code, 'UNPAID')) IN (
              'PAID', 'PARTIALLY_PAID', 'PART_PAID', 'OVERPAID', 'UNDERPAID', 'PROCESSING'
            )
          )
      ), changed AS (
        SELECT old_retained.timesheet_id
        FROM old_retained
        WHERE NOT EXISTS (
          SELECT 1 FROM new_retained WHERE new_retained.timesheet_id = old_retained.timesheet_id
        )
        UNION
        SELECT new_retained.timesheet_id
        FROM new_retained
        WHERE NOT EXISTS (
          SELECT 1 FROM old_retained WHERE old_retained.timesheet_id = new_retained.timesheet_id
        )
      )
      SELECT COALESCE(array_agg(changed.timesheet_id ORDER BY changed.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM changed;
    END IF;

  ELSIF TG_TABLE_NAME = 'timesheet_pay_state_history' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(array_agg(DISTINCT n.timesheet_id ORDER BY n.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM new_rows AS n WHERE n.timesheet_id IS NOT NULL;
    ELSE
      WITH changed AS (
        SELECT o.timesheet_id FROM old_rows AS o JOIN new_rows AS n ON n.id = o.id
        WHERE o.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
        UNION
        SELECT n.timesheet_id FROM new_rows AS n JOIN old_rows AS o ON o.id = n.id
        WHERE n.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
      )
      SELECT COALESCE(array_agg(changed.timesheet_id ORDER BY changed.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM changed;
    END IF;

  ELSIF TG_TABLE_NAME = 'ts_pay_adjustments' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(array_agg(DISTINCT n.timesheet_id ORDER BY n.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM new_rows AS n WHERE n.timesheet_id IS NOT NULL;
    ELSE
      WITH changed AS (
        SELECT o.timesheet_id FROM old_rows AS o JOIN new_rows AS n ON n.id = o.id
        WHERE o.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
        UNION
        SELECT n.timesheet_id FROM new_rows AS n JOIN old_rows AS o ON o.id = n.id
        WHERE n.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
      )
      SELECT COALESCE(array_agg(changed.timesheet_id ORDER BY changed.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM changed;
    END IF;

  ELSIF TG_TABLE_NAME = 'pay_finance_case_components' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(array_agg(DISTINCT n.linked_timesheet_id ORDER BY n.linked_timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM new_rows AS n WHERE n.linked_timesheet_id IS NOT NULL;
    ELSE
      WITH changed AS (
        SELECT o.linked_timesheet_id AS timesheet_id
        FROM old_rows AS o JOIN new_rows AS n ON n.id = o.id
        WHERE o.linked_timesheet_id IS NOT NULL
          AND o.linked_timesheet_id IS DISTINCT FROM n.linked_timesheet_id
        UNION
        SELECT n.linked_timesheet_id
        FROM new_rows AS n JOIN old_rows AS o ON o.id = n.id
        WHERE n.linked_timesheet_id IS NOT NULL
          AND o.linked_timesheet_id IS DISTINCT FROM n.linked_timesheet_id
      )
      SELECT COALESCE(array_agg(changed.timesheet_id ORDER BY changed.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM changed;
    END IF;

  ELSIF TG_TABLE_NAME = 'pay_advances' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(array_agg(DISTINCT n.linked_timesheet_id ORDER BY n.linked_timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM new_rows AS n WHERE n.linked_timesheet_id IS NOT NULL;
    ELSE
      WITH changed AS (
        SELECT o.linked_timesheet_id AS timesheet_id
        FROM old_rows AS o JOIN new_rows AS n ON n.id = o.id
        WHERE o.linked_timesheet_id IS NOT NULL
          AND o.linked_timesheet_id IS DISTINCT FROM n.linked_timesheet_id
        UNION
        SELECT n.linked_timesheet_id
        FROM new_rows AS n JOIN old_rows AS o ON o.id = n.id
        WHERE n.linked_timesheet_id IS NOT NULL
          AND o.linked_timesheet_id IS DISTINCT FROM n.linked_timesheet_id
      )
      SELECT COALESCE(array_agg(changed.timesheet_id ORDER BY changed.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM changed;
    END IF;

  ELSIF TG_TABLE_NAME = 'pay_payment_correction_items' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(array_agg(DISTINCT n.timesheet_id ORDER BY n.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM new_rows AS n WHERE n.timesheet_id IS NOT NULL;
    ELSE
      WITH changed AS (
        SELECT o.timesheet_id FROM old_rows AS o JOIN new_rows AS n ON n.id = o.id
        WHERE o.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
        UNION
        SELECT n.timesheet_id FROM new_rows AS n JOIN old_rows AS o ON o.id = n.id
        WHERE n.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
      )
      SELECT COALESCE(array_agg(changed.timesheet_id ORDER BY changed.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM changed;
    END IF;

  ELSIF TG_TABLE_NAME = 'pay_manual_adjustment_carry_forwards' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(array_agg(DISTINCT n.timesheet_id ORDER BY n.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM new_rows AS n WHERE n.timesheet_id IS NOT NULL;
    ELSE
      WITH changed AS (
        SELECT o.timesheet_id FROM old_rows AS o JOIN new_rows AS n ON n.id = o.id
        WHERE o.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
        UNION
        SELECT n.timesheet_id FROM new_rows AS n JOIN old_rows AS o ON o.id = n.id
        WHERE n.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
      )
      SELECT COALESCE(array_agg(changed.timesheet_id ORDER BY changed.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM changed;
    END IF;

  ELSIF TG_TABLE_NAME = 'timesheet_payment_overrides' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(array_agg(DISTINCT n.timesheet_id ORDER BY n.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM new_rows AS n WHERE n.timesheet_id IS NOT NULL;
    ELSE
      WITH changed AS (
        SELECT o.timesheet_id FROM old_rows AS o JOIN new_rows AS n ON n.id = o.id
        WHERE o.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
        UNION
        SELECT n.timesheet_id FROM new_rows AS n JOIN old_rows AS o ON o.id = n.id
        WHERE n.timesheet_id IS NOT NULL AND o.timesheet_id IS DISTINCT FROM n.timesheet_id
      )
      SELECT COALESCE(array_agg(changed.timesheet_id ORDER BY changed.timesheet_id), ARRAY[]::uuid[])
        INTO v_timesheet_ids FROM changed;
    END IF;

  ELSE
    RAISE EXCEPTION USING
      MESSAGE = 'RETENTION_CAPTURE_UNEXPECTED_TABLE',
      DETAIL = jsonb_build_object(
        'table_schema', TG_TABLE_SCHEMA,
        'table_name', TG_TABLE_NAME,
        'operation', TG_OP
      )::text;
  END IF;

  IF COALESCE(array_length(v_timesheet_ids, 1), 0) > 0 THEN
    PERFORM public.timesheet_financial_retention_mark_v1(v_timesheet_ids);
  END IF;

  RETURN NULL;
END;
$function$;

-- timesheet_financial_retention_completeness_v1(integer)
CREATE OR REPLACE FUNCTION public.timesheet_financial_retention_completeness_v1(p_sample_limit integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
WITH retained_sources AS MATERIALIZED (
  SELECT tf.timesheet_id, 'TIMESHEETS_FINANCIALS_PAYMENT_OR_REMITTANCE'::text AS source_code
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id IS NOT NULL
    AND (
      tf.paid_at_utc IS NOT NULL
      OR tf.paid_by_user_id IS NOT NULL
      OR NULLIF(BTRIM(COALESCE(tf.payment_reference, '')), '') IS NOT NULL
      OR tf.remittance_last_sent_at_utc IS NOT NULL
      OR COALESCE(tf.remittance_send_count, 0) > 0
    )
  UNION ALL
  SELECT pbi.timesheet_id, 'PAY_BATCH_ITEM' FROM public.pay_batch_items AS pbi WHERE pbi.timesheet_id IS NOT NULL
  UNION ALL
  SELECT forced.timesheet_id, 'PAY_BATCH_FORCE_INCLUDE'
  FROM public.pay_batches AS pb
  CROSS JOIN LATERAL unnest(COALESCE(pb.force_include_timesheet_ids, ARRAY[]::uuid[])) AS forced(timesheet_id)
  WHERE forced.timesheet_id IS NOT NULL
  UNION ALL
  SELECT snapshot.timesheet_id, 'PAY_BATCH_TIMESHEET_SNAPSHOT' FROM public.pay_batch_timesheet_snapshots AS snapshot WHERE snapshot.timesheet_id IS NOT NULL
  UNION ALL
  SELECT state.timesheet_id, 'TIMESHEET_PAY_STATE'
  FROM public.timesheet_pay_state AS state
  WHERE state.timesheet_id IS NOT NULL
    AND (
      state.last_settled_pay_batch_id IS NOT NULL
      OR state.last_settled_at_utc IS NOT NULL
      OR state.last_settled_signature IS NOT NULL
      OR state.last_settled_snapshot_json IS NOT NULL
      OR state.summary_pay_paid_at_utc IS NOT NULL
      OR UPPER(COALESCE(state.summary_pay_status_code, 'UNPAID')) IN (
        'PAID', 'PARTIALLY_PAID', 'PART_PAID', 'OVERPAID', 'UNDERPAID', 'PROCESSING'
      )
    )
  UNION ALL
  SELECT history.timesheet_id, 'TIMESHEET_PAY_STATE_HISTORY' FROM public.timesheet_pay_state_history AS history WHERE history.timesheet_id IS NOT NULL
  UNION ALL
  SELECT adjustment.timesheet_id, 'TS_PAY_ADJUSTMENT' FROM public.ts_pay_adjustments AS adjustment WHERE adjustment.timesheet_id IS NOT NULL
  UNION ALL
  SELECT component.linked_timesheet_id, 'PAY_FINANCE_CASE_COMPONENT' FROM public.pay_finance_case_components AS component WHERE component.linked_timesheet_id IS NOT NULL
  UNION ALL
  SELECT advance.linked_timesheet_id, 'PAY_ADVANCE' FROM public.pay_advances AS advance WHERE advance.linked_timesheet_id IS NOT NULL
  UNION ALL
  SELECT correction.timesheet_id, 'PAY_PAYMENT_CORRECTION_ITEM' FROM public.pay_payment_correction_items AS correction WHERE correction.timesheet_id IS NOT NULL
  UNION ALL
  SELECT carry_forward.timesheet_id, 'PAY_MANUAL_ADJUSTMENT_CARRY_FORWARD' FROM public.pay_manual_adjustment_carry_forwards AS carry_forward WHERE carry_forward.timesheet_id IS NOT NULL
  UNION ALL
  SELECT override_row.timesheet_id, 'TIMESHEET_PAYMENT_OVERRIDE' FROM public.timesheet_payment_overrides AS override_row WHERE override_row.timesheet_id IS NOT NULL
), retained_candidates AS MATERIALIZED (
  SELECT DISTINCT retained_sources.timesheet_id
  FROM retained_sources
), missing AS MATERIALIZED (
  SELECT candidate.timesheet_id
  FROM retained_candidates AS candidate
  LEFT JOIN public.timesheet_financial_retention AS marker
    ON marker.timesheet_id = candidate.timesheet_id
  WHERE marker.timesheet_id IS NULL
), orphaned AS MATERIALIZED (
  SELECT candidate.timesheet_id
  FROM retained_candidates AS candidate
  LEFT JOIN public.timesheets AS t ON t.timesheet_id = candidate.timesheet_id
  WHERE t.timesheet_id IS NULL
), source_counts AS (
  SELECT retained_sources.source_code, COUNT(DISTINCT retained_sources.timesheet_id)::integer AS timesheet_count
  FROM retained_sources
  GROUP BY retained_sources.source_code
)
SELECT jsonb_build_object(
  'ok', true,
  'complete', (SELECT COUNT(*) FROM missing) = 0 AND (SELECT COUNT(*) FROM orphaned) = 0,
  'retained_identity_count', (SELECT COUNT(*) FROM retained_candidates),
  'marker_count', (SELECT COUNT(*) FROM public.timesheet_financial_retention),
  'missing_marker_count', (SELECT COUNT(*) FROM missing),
  'orphaned_retained_identity_count', (SELECT COUNT(*) FROM orphaned),
  'missing_marker_sample', COALESCE((
    SELECT jsonb_agg(sample.timesheet_id ORDER BY sample.timesheet_id)
    FROM (
      SELECT missing.timesheet_id
      FROM missing
      ORDER BY missing.timesheet_id
      LIMIT LEAST(GREATEST(COALESCE(p_sample_limit, 100), 1), 1000)
    ) AS sample
  ), '[]'::jsonb),
  'orphaned_identity_sample', COALESCE((
    SELECT jsonb_agg(sample.timesheet_id ORDER BY sample.timesheet_id)
    FROM (
      SELECT orphaned.timesheet_id
      FROM orphaned
      ORDER BY orphaned.timesheet_id
      LIMIT LEAST(GREATEST(COALESCE(p_sample_limit, 100), 1), 1000)
    ) AS sample
  ), '[]'::jsonb),
  'source_counts', COALESCE((
    SELECT jsonb_object_agg(source_counts.source_code, source_counts.timesheet_count ORDER BY source_counts.source_code)
    FROM source_counts
  ), '{}'::jsonb)
);
$function$;

-- timesheet_financial_retention_mark_v1(uuid[])
CREATE OR REPLACE FUNCTION public.timesheet_financial_retention_mark_v1(p_timesheet_ids uuid[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_requested_count integer := 0;
  v_locked_count integer := 0;
  v_inserted_count integer := 0;
BEGIN
  SELECT COALESCE(array_agg(DISTINCT requested.timesheet_id ORDER BY requested.timesheet_id), ARRAY[]::uuid[])
    INTO v_timesheet_ids
  FROM unnest(COALESCE(p_timesheet_ids, ARRAY[]::uuid[])) AS requested(timesheet_id)
  WHERE requested.timesheet_id IS NOT NULL;

  v_requested_count := COALESCE(array_length(v_timesheet_ids, 1), 0);

  IF v_requested_count = 0 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_IDS_REQUIRED',
      DETAIL = jsonb_build_object('function', 'timesheet_financial_retention_mark_v1')::text;
  END IF;

  IF v_requested_count > 1000 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'RETENTION_MARK_SCOPE_TOO_LARGE',
      DETAIL = jsonb_build_object('maximum', 1000, 'actual', v_requested_count)::text;
  END IF;

  SELECT COUNT(*)::integer
    INTO v_locked_count
  FROM (
    SELECT t.timesheet_id
    FROM public.timesheets AS t
    JOIN unnest(v_timesheet_ids) AS requested(timesheet_id)
      ON requested.timesheet_id = t.timesheet_id
    ORDER BY t.timesheet_id
    FOR KEY SHARE OF t
  ) AS locked_timesheets;

  IF v_locked_count IS DISTINCT FROM v_requested_count THEN
    RAISE EXCEPTION USING
      MESSAGE = 'RETENTION_MARK_TIMESHEET_NOT_FOUND',
      DETAIL = jsonb_build_object(
        'requested_count', v_requested_count,
        'locked_count', v_locked_count,
        'timesheet_ids', to_jsonb(v_timesheet_ids)
      )::text;
  END IF;

  INSERT INTO public.timesheet_financial_retention(timesheet_id)
  SELECT requested.timesheet_id
  FROM unnest(v_timesheet_ids) AS requested(timesheet_id)
  ORDER BY requested.timesheet_id
  ON CONFLICT (timesheet_id) DO NOTHING;

  GET DIAGNOSTICS v_inserted_count = ROW_COUNT;

  RETURN jsonb_build_object(
    'ok', true,
    'requested_count', v_requested_count,
    'locked_count', v_locked_count,
    'inserted_count', v_inserted_count,
    'already_marked_count', v_requested_count - v_inserted_count
  );
END;
$function$;

-- timesheet_import_rows_for_timesheet_current(uuid,boolean,uuid,uuid)
CREATE OR REPLACE FUNCTION public.timesheet_import_rows_for_timesheet_current(p_timesheet_id uuid, p_include_excluded boolean DEFAULT true, p_import_id uuid DEFAULT NULL::uuid, p_shift_id uuid DEFAULT NULL::uuid)
 RETURNS TABLE(requested_timesheet_id uuid, current_timesheet_id uuid, source_system text, import_id uuid, filename text, uploaded_at_utc timestamp with time zone, file_r2_key text, header_rows jsonb, header_columns jsonb, rows jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  with req as (
    select
      t.timesheet_id as requested_timesheet_id,
      t.booking_id
    from public.timesheets t
    where t.timesheet_id = p_timesheet_id
    limit 1
  ),
  cur as (
    select
      t.timesheet_id as current_timesheet_id
    from public.timesheets t
    join req
      on req.booking_id = t.booking_id
    where t.is_current = true
    order by t.version desc
    limit 1
  ),
  use_id as (
    select
      (select r.requested_timesheet_id from req r) as requested_timesheet_id,
      (select c.current_timesheet_id  from cur c) as current_timesheet_id
  ),

  -- Existing evidence source: attached shifts (unchanged)
  sh as (
    select
      ns.source_system    as source_system,
      ns.latest_import_id as import_id,
      ns.external_row_key as external_row_key,
      ns.invoice_status   as invoice_status
    from public.nhsp_shifts ns
    where ns.timesheet_id = (select u.current_timesheet_id from use_id u)
      and ns.latest_import_id is not null
      and ns.external_row_key is not null
      and (p_shift_id is null or ns.id = p_shift_id)
      and (p_import_id is null or ns.latest_import_id = p_import_id)
      and (
        p_include_excluded is true
        or coalesce(ns.invoice_status,'') <> 'DEFERRED'
      )
  ),

  -- Daily evidence is server-owned once an import review exists.  Historical
  -- pre-review imports retain the legacy payload link as a compatibility read.
  daily as (
    select
      hr.import_id         as import_id,
      hr.external_row_key  as external_row_key,
      'HEALTHROSTER_DAILY'::text as source_system_hint
    from public.hr_rows hr
    join public.hr_imports hi
      on hi.id = hr.import_id
    left join public.import_review_daily_timesheet_resolutions rr
      on rr.import_id=hr.import_id and rr.hr_row_id=hr.id and rr.status='APPLIED'
    where hi.source_system = 'HEALTHROSTER_DAILY'::public.hr_source_enum
      and hr.import_id is not null
      and hr.external_row_key is not null
      and (
        rr.resolved_timesheet_id=(select u.current_timesheet_id from use_id u)
        or (rr.id is null and not exists(select 1 from public.import_review_states s where s.import_id=hr.import_id)
          and (hr.payload_json->>'resolved_timesheet_id')=(select u.current_timesheet_id::text from use_id u))
      )
      and (p_import_id is null or hr.import_id = p_import_id)
      and (p_shift_id is null)  -- shift_id filter only applies to shift-based evidence sources
  ),

  -- Existing evidence source: schedule entries (correction timesheets)
  -- Extract (import_id, external_row_key) pairs from timesheets.actual_schedule_json
  sched_raw as (
    select
      s.elem as seg
    from public.timesheets t
    join use_id u
      on u.current_timesheet_id = t.timesheet_id
    cross join lateral jsonb_array_elements(
      case
        when jsonb_typeof(t.actual_schedule_json) = 'array' then t.actual_schedule_json
        else '[]'::jsonb
      end
    ) as s(elem)
  ),
  sched_keys as (
    select
      case
        when (sr.seg ? 'import_id')
         and (sr.seg->>'import_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then (sr.seg->>'import_id')::uuid
        else null
      end as import_id,
      nullif(btrim(coalesce(sr.seg->>'external_row_key','')), '') as external_row_key,
      case
        when (sr.seg ? 'shift_id')
         and (sr.seg->>'shift_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then (sr.seg->>'shift_id')::uuid
        else null
      end as shift_id
    from sched_raw sr
  ),
  sched_filtered as (
    select
      sk.import_id,
      sk.external_row_key
    from sched_keys sk
    where sk.import_id is not null
      and sk.external_row_key is not null
      and (p_import_id is null or sk.import_id = p_import_id)
      and (p_shift_id is null or sk.shift_id = p_shift_id)
  ),

  -- Combine evidence keys from all sources and de-dupe by (import_id, external_row_key)
  keys_union as (
    select
      sh0.import_id as import_id,
      sh0.external_row_key as external_row_key,
      sh0.source_system::text as source_system_hint
    from sh sh0

    union all

    select
      d0.import_id as import_id,
      d0.external_row_key as external_row_key,
      d0.source_system_hint as source_system_hint
    from daily d0

    union all

    select
      sf.import_id as import_id,
      sf.external_row_key as external_row_key,
      null::text as source_system_hint
    from sched_filtered sf
  ),
  keys as (
    select distinct on (ku.import_id, ku.external_row_key)
      ku.import_id,
      ku.external_row_key,
      ku.source_system_hint
    from keys_union ku
    where ku.import_id is not null
      and ku.external_row_key is not null
    order by ku.import_id, ku.external_row_key, (ku.source_system_hint is null) asc
  ),

  -- Import header per import_id (source_system is taken from hr_imports where possible)
  imp as (
    select
      coalesce(hi.source_system::text, k.any_source_system) as source_system,
      k.import_id as import_id,
      hi.filename as filename,
      hi.uploaded_at_utc as uploaded_at_utc,
      hi.file_r2_key as file_r2_key,

      case
        when jsonb_typeof(hi.parse_summary_json->'header_rows') = 'array'
          then (hi.parse_summary_json->'header_rows')
        when jsonb_typeof(hi.parse_summary_json->'header_columns') = 'array'
          and jsonb_array_length(hi.parse_summary_json->'header_columns') > 0
          then jsonb_build_array(hi.parse_summary_json->'header_columns')
        else '[]'::jsonb
      end as header_rows,

      case
        when jsonb_typeof(hi.parse_summary_json->'header_columns') = 'array'
          then (hi.parse_summary_json->'header_columns')
        else '[]'::jsonb
      end as header_columns
    from (
      select
        k0.import_id,
        min(k0.source_system_hint) as any_source_system
      from keys k0
      group by k0.import_id
    ) as k
    left join public.hr_imports hi
      on hi.id = k.import_id
  ),

  -- Rows per import_id, joining hr_rows by (import_id, external_row_key)
  r as (
    select
      k.import_id as import_id,
      jsonb_agg(
        jsonb_build_object(
          'raw_columns', hr.payload_json->'raw_columns',
          'payload',     hr.payload_json
        )
        order by hr.id
      ) as rows
    from keys k
    join public.hr_rows hr
      on hr.import_id = k.import_id
     and hr.external_row_key = k.external_row_key
    group by k.import_id
  )

  select
    (select u.requested_timesheet_id from use_id u) as requested_timesheet_id,
    (select u.current_timesheet_id from use_id u)   as current_timesheet_id,
    i.source_system,
    i.import_id,
    i.filename,
    i.uploaded_at_utc,
    i.file_r2_key,
    i.header_rows,
    i.header_columns,
    coalesce(r.rows, '[]'::jsonb) as rows
  from imp i
  left join r
    on r.import_id = i.import_id
  order by i.source_system, i.uploaded_at_utc nulls last, i.import_id;
$function$;

-- timesheet_lifecycle_affected_rows_v1(jsonb,text,uuid)
CREATE OR REPLACE FUNCTION public.timesheet_lifecycle_affected_rows_v1(p_items jsonb DEFAULT '[]'::jsonb, p_context text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_items jsonb := '[]'::jsonb;
  v_item jsonb;
  v_rows jsonb := '[]'::jsonb;
  v_missing jsonb := '[]'::jsonb;
  v_removed jsonb := '[]'::jsonb;
  v_seen_keys text[] := ARRAY[]::text[];
  v_uuid_re text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
  v_item_count integer := 0;

  v_item_context text := NULL;
  v_item_key text := NULL;
  v_timesheet_id uuid := NULL;
  v_contract_week_id uuid := NULL;
  v_previous_timesheet_id uuid := NULL;
  v_booking_id text := NULL;
  v_previous_row_key text := NULL;
  v_previous_bucket text := NULL;

  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_week public.contract_weeks%ROWTYPE;
  v_contract public.contracts%ROWTYPE;
  v_tsfin public.timesheets_financials%ROWTYPE;
  v_candidate public.candidates%ROWTYPE;
  v_client public.clients%ROWTYPE;

  v_signature_json jsonb := '{}'::jsonb;
  v_evidence_summary jsonb := '{}'::jsonb;
  v_staged_summary jsonb := '{}'::jsonb;
  v_invoice_segments_locked integer := 0;
  v_is_paid boolean := false;
  v_is_archived boolean := false;
  v_is_invoice_locked boolean := false;
  v_is_locked boolean := false;
  v_is_authorised boolean := false;
  v_is_unprocessed boolean := false;
  v_has_retained_financial_history boolean := false;
  v_can_process boolean := false;
  v_can_unprocess boolean := false;
  v_unprocess_action_visible boolean := false;
  v_unprocess_block_reason text := NULL;
  v_can_authorise boolean := false;
  v_can_unauthorise boolean := false;
  v_qr_unsigned_blocked boolean := false;
  v_disabled_reasons text[] := ARRAY[]::text[];
  v_bucket text := NULL;
  v_row_key text := NULL;
  v_removed_reason text := NULL;
  v_temp_log_enabled boolean := false;
BEGIN
  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_temp_log_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      v_temp_log_enabled := false;
    WHEN OTHERS THEN
      v_temp_log_enabled := false;
  END;

  IF p_items IS NULL THEN
    v_items := '[]'::jsonb;
  ELSIF jsonb_typeof(p_items) = 'array' THEN
    v_items := p_items;
  ELSIF jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'items') = 'array' THEN
    v_items := p_items -> 'items';
  ELSE
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'INVALID_PAYLOAD',
      'message', 'p_items must be a JSON array or an object containing an items array',
      'rows', '[]'::jsonb,
      'missing', '[]'::jsonb,
      'removed', '[]'::jsonb,
      'count_deltas', '{}'::jsonb,
      'cache_invalidation_hints', '{}'::jsonb
    );
  END IF;

  v_item_count := jsonb_array_length(v_items);
  IF v_item_count > 100 THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'TOO_MANY_ITEMS',
      'message', 'timesheet_lifecycle_affected_rows_v1 accepts at most 100 affected identities per call',
      'requested_count', v_item_count,
      'rows', '[]'::jsonb,
      'missing', '[]'::jsonb,
      'removed', '[]'::jsonb,
      'count_deltas', '{}'::jsonb,
      'cache_invalidation_hints', jsonb_build_object(
        'changed_domains', jsonb_build_array('timesheet_lifecycle'),
        'context', NULLIF(BTRIM(COALESCE(p_context, '')), '')
      )
    );
  END IF;

  FOR v_item IN
    SELECT item_value.value
    FROM jsonb_array_elements(v_items) AS item_value(value)
  LOOP
    v_item_context := LOWER(NULLIF(BTRIM(COALESCE(v_item ->> 'context', p_context, '')), ''));
    v_timesheet_id := NULL;
    v_contract_week_id := NULL;
    v_previous_timesheet_id := NULL;
    v_booking_id := NULLIF(BTRIM(COALESCE(v_item ->> 'booking_id', v_item ->> 'bookingId', '')), '');
    v_previous_row_key := NULLIF(BTRIM(COALESCE(v_item ->> 'previous_row_key', v_item ->> 'previousRowKey', '')), '');
    v_previous_bucket := NULLIF(BTRIM(COALESCE(v_item ->> 'previous_bucket', v_item ->> 'previousBucket', '')), '');

    IF NULLIF(BTRIM(COALESCE(v_item ->> 'timesheet_id', v_item ->> 'current_timesheet_id', '')), '') ~* v_uuid_re THEN
      v_timesheet_id := NULLIF(BTRIM(COALESCE(v_item ->> 'timesheet_id', v_item ->> 'current_timesheet_id', '')), '')::uuid;
    END IF;

    IF NULLIF(BTRIM(COALESCE(v_item ->> 'contract_week_id', v_item ->> 'week_id', '')), '') ~* v_uuid_re THEN
      v_contract_week_id := NULLIF(BTRIM(COALESCE(v_item ->> 'contract_week_id', v_item ->> 'week_id', '')), '')::uuid;
    END IF;

    IF NULLIF(BTRIM(COALESCE(v_item ->> 'previous_timesheet_id', v_item ->> 'previousTimesheetId', '')), '') ~* v_uuid_re THEN
      v_previous_timesheet_id := NULLIF(BTRIM(COALESCE(v_item ->> 'previous_timesheet_id', v_item ->> 'previousTimesheetId', '')), '')::uuid;
    END IF;

    v_item_key := COALESCE(v_timesheet_id::text, '') || '|' || COALESCE(v_contract_week_id::text, '') || '|' || COALESCE(v_booking_id, '') || '|' || COALESCE(v_previous_timesheet_id::text, '');
    IF v_item_key = '|||' OR v_item_key = '' THEN
      v_missing := v_missing || jsonb_build_array(jsonb_build_object('input', v_item, 'error_code', 'INVALID_IDENTITY'));
      CONTINUE;
    END IF;
    IF v_item_key = ANY(v_seen_keys) THEN
      CONTINUE;
    END IF;
    v_seen_keys := array_append(v_seen_keys, v_item_key);

    v_requested_ts := NULL;
    v_current_ts := NULL;
    v_week := NULL;
    v_contract := NULL;
    v_tsfin := NULL;
    v_candidate := NULL;
    v_client := NULL;
    v_signature_json := '{}'::jsonb;
    v_evidence_summary := '{}'::jsonb;
    v_staged_summary := '{}'::jsonb;
    v_invoice_segments_locked := 0;
    v_is_paid := false;
    v_is_archived := false;
    v_is_invoice_locked := false;
    v_is_locked := false;
    v_is_authorised := false;
    v_is_unprocessed := false;
    v_has_retained_financial_history := false;
    v_can_process := false;
    v_can_unprocess := false;
    v_unprocess_action_visible := false;
    v_unprocess_block_reason := NULL;
    v_can_authorise := false;
    v_can_unauthorise := false;
    v_qr_unsigned_blocked := false;
    v_disabled_reasons := ARRAY[]::text[];
    v_bucket := NULL;
    v_row_key := NULL;
    v_removed_reason := NULL;

    IF v_contract_week_id IS NOT NULL THEN
      SELECT cw.*
        INTO v_week
      FROM public.contract_weeks AS cw
      WHERE cw.id = v_contract_week_id
      LIMIT 1;
    END IF;

    IF v_timesheet_id IS NOT NULL THEN
      SELECT ts.*
        INTO v_requested_ts
      FROM public.timesheets AS ts
      WHERE ts.timesheet_id = v_timesheet_id
      LIMIT 1;

      IF v_requested_ts.timesheet_id IS NOT NULL THEN
        v_booking_id := COALESCE(v_booking_id, v_requested_ts.booking_id);
      END IF;
    END IF;

    IF v_booking_id IS NOT NULL THEN
      SELECT ts.*
        INTO v_current_ts
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_booking_id
        AND ts.is_current = true
      ORDER BY ts.version DESC, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
      LIMIT 1;
    END IF;

    IF v_current_ts.timesheet_id IS NULL AND v_week.timesheet_id IS NOT NULL THEN
      SELECT ts.*
        INTO v_requested_ts
      FROM public.timesheets AS ts
      WHERE ts.timesheet_id = v_week.timesheet_id
      LIMIT 1;

      IF v_requested_ts.timesheet_id IS NOT NULL THEN
        v_booking_id := COALESCE(v_booking_id, v_requested_ts.booking_id);
        SELECT ts.*
          INTO v_current_ts
        FROM public.timesheets AS ts
        WHERE ts.booking_id = v_requested_ts.booking_id
          AND ts.is_current = true
        ORDER BY ts.version DESC, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
        LIMIT 1;
      END IF;
    END IF;

    IF v_current_ts.timesheet_id IS NULL AND v_requested_ts.timesheet_id IS NOT NULL THEN
      v_current_ts := v_requested_ts;
    END IF;

    IF v_week.id IS NULL AND v_current_ts.timesheet_id IS NOT NULL THEN
      SELECT cw.*
        INTO v_week
      FROM public.contract_weeks AS cw
      WHERE cw.timesheet_id = v_current_ts.timesheet_id
      ORDER BY cw.updated_at DESC NULLS LAST, cw.created_at DESC NULLS LAST, cw.id DESC
      LIMIT 1;
    END IF;

    IF v_current_ts.timesheet_id IS NOT NULL THEN
      SELECT tf.*
        INTO v_tsfin
      FROM public.timesheets_financials AS tf
      WHERE tf.timesheet_id = v_current_ts.timesheet_id
        AND tf.is_current = true
      ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
      LIMIT 1;
    END IF;

    IF v_week.id IS NOT NULL THEN
      SELECT c.*
        INTO v_contract
      FROM public.contracts AS c
      WHERE c.id = v_week.contract_id
      LIMIT 1;
    ELSIF v_current_ts.contract_id IS NOT NULL THEN
      SELECT c.*
        INTO v_contract
      FROM public.contracts AS c
      WHERE c.id = v_current_ts.contract_id
      LIMIT 1;
    END IF;

    IF COALESCE(v_tsfin.candidate_id, v_contract.candidate_id) IS NOT NULL THEN
      SELECT cand.*
        INTO v_candidate
      FROM public.candidates AS cand
      WHERE cand.id = COALESCE(v_tsfin.candidate_id, v_contract.candidate_id)
      LIMIT 1;
    END IF;

    IF COALESCE(v_tsfin.client_id, v_contract.client_id) IS NOT NULL THEN
      SELECT cli.*
        INTO v_client
      FROM public.clients AS cli
      WHERE cli.id = COALESCE(v_tsfin.client_id, v_contract.client_id)
      LIMIT 1;
    END IF;

    IF v_current_ts.timesheet_id IS NULL AND v_week.id IS NULL THEN
      v_missing := v_missing || jsonb_build_array(
        jsonb_build_object(
          'input', v_item,
          'timesheet_id', v_timesheet_id,
          'contract_week_id', v_contract_week_id,
          'booking_id', v_booking_id,
          'error_code', 'TARGET_NOT_FOUND'
        )
      );
      CONTINUE;
    END IF;

    IF v_current_ts.timesheet_id IS NULL AND v_week.id IS NOT NULL AND v_week.timesheet_id IS NULL THEN
      v_removed_reason := 'CONTRACT_WEEK_REOPENED';
    END IF;

    IF v_tsfin.id IS NOT NULL THEN
      SELECT COUNT(*)::integer
        INTO v_invoice_segments_locked
      FROM jsonb_array_elements(
        CASE
          WHEN v_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(v_tsfin.invoice_breakdown_json) = 'array' THEN v_tsfin.invoice_breakdown_json
          WHEN jsonb_typeof(v_tsfin.invoice_breakdown_json) = 'object'
           AND jsonb_typeof(v_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_tsfin.invoice_breakdown_json -> 'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL;
    END IF;

    IF v_current_ts.timesheet_id IS NOT NULL THEN
      SELECT jsonb_strip_nulls(
        jsonb_build_object(
          'count', COUNT(*)::integer,
          'has_timesheet', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(ev.kind), ''), 'OTHER')) = 'TIMESHEET') > 0,
          'has_mileage', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(ev.kind), ''), 'OTHER')) = 'MILEAGE') > 0,
          'has_travel', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(ev.kind), ''), 'OTHER')) = 'TRAVEL') > 0,
          'has_accommodation', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(ev.kind), ''), 'OTHER')) = 'ACCOMMODATION') > 0,
          'has_other', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(ev.kind), ''), 'OTHER')) NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION')) > 0,
          'updated_at', MAX(ev.created_at),
          'primary_storage_key', MIN(ev.storage_key),
          'manual_pdf_r2_key', NULLIF(v_current_ts.manual_pdf_r2_key, ''),
          'manual_pdf_rotation_degrees', v_current_ts.manual_pdf_rotation_degrees
        )
      )
        INTO v_evidence_summary
      FROM public.timesheet_evidence AS ev
      WHERE ev.timesheet_id = v_current_ts.timesheet_id;
    ELSE
      v_evidence_summary := jsonb_build_object(
        'count', 0,
        'has_timesheet', false,
        'has_mileage', false,
        'has_travel', false,
        'has_accommodation', false,
        'has_other', false
      );
    END IF;

    IF v_week.id IS NOT NULL THEN
      WITH staged_rows AS (
        SELECT
          mq.id,
          mq.uploaded_at_utc,
          mq.last_rotation_deg,
          UPPER(COALESCE(
            NULLIF(BTRIM(mq.meta_json ->> 'staged_kind'), ''),
            NULLIF(BTRIM(mq.meta_json ->> 'kind'), ''),
            NULLIF(BTRIM(mq.meta_json ->> 'attached_kind'), ''),
            'TIMESHEET'
          )) AS staged_kind,
          NULLIF(regexp_replace(COALESCE(
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
      )
      SELECT jsonb_strip_nulls(
        jsonb_build_object(
          'staged_count', COUNT(*)::integer,
          'has_staged_timesheet', COUNT(*) FILTER (WHERE sr.staged_kind = 'TIMESHEET') > 0,
          'active_staged_timesheet_count', COUNT(*) FILTER (WHERE sr.staged_kind = 'TIMESHEET')::integer,
          'primary_staged_timesheet_storage_key', MIN(sr.storage_key) FILTER (WHERE sr.staged_kind = 'TIMESHEET'),
          'primary_staged_timesheet_rotation_degrees', MIN(sr.last_rotation_deg) FILTER (WHERE sr.staged_kind = 'TIMESHEET'),
          'updated_at', MAX(sr.uploaded_at_utc)
        )
      )
        INTO v_staged_summary
      FROM staged_rows AS sr;
    ELSE
      v_staged_summary := jsonb_build_object('staged_count', 0, 'has_staged_timesheet', false, 'active_staged_timesheet_count', 0);
    END IF;

    v_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, v_week.id, COALESCE(v_temp_log_enabled, false));

    v_is_paid := COALESCE(v_tsfin.paid_at_utc IS NOT NULL, false);
    v_is_archived := COALESCE(v_current_ts.archived_at_utc IS NOT NULL, false);
    v_is_invoice_locked := COALESCE(v_tsfin.locked_by_invoice_id IS NOT NULL, false) OR COALESCE(v_invoice_segments_locked, 0) > 0;
    v_is_locked := COALESCE(v_is_invoice_locked, false);
    v_is_authorised := COALESCE(v_current_ts.authorised_at_server IS NOT NULL, false)
      OR COALESCE(v_tsfin.authorised_at_utc IS NOT NULL, false)
      OR COALESCE(v_week.status = 'AUTHORISED'::public.contract_week_status_enum, false);
    v_is_unprocessed := COALESCE(
      v_current_ts.timesheet_id IS NULL
      OR v_tsfin.id IS NULL
      OR v_tsfin.processing_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum
      OR v_week.status = 'OPEN'::public.contract_week_status_enum,
      false
    );

    IF v_current_ts.timesheet_id IS NOT NULL THEN
      IF v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum THEN
        SELECT EXISTS (
          SELECT 1
          FROM public.timesheets AS unit_timesheet
          JOIN public.timesheet_financial_retention AS retention
            ON retention.timesheet_id = unit_timesheet.timesheet_id
          WHERE unit_timesheet.booking_id = v_current_ts.booking_id
        )
          INTO v_has_retained_financial_history;
      ELSE
        SELECT EXISTS (
          SELECT 1
          FROM public.timesheet_financial_retention AS retention
          WHERE retention.timesheet_id = v_current_ts.timesheet_id
        )
          INTO v_has_retained_financial_history;
      END IF;
    ELSE
      v_has_retained_financial_history := false;
    END IF;

    v_qr_unsigned_blocked := COALESCE(
      v_current_ts.timesheet_id IS NOT NULL
      AND (
        COALESCE(v_tsfin.processing_status = 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum, false)
        OR (
          UPPER(COALESCE(v_current_ts.qr_status::text, '')) = 'PENDING'
          AND NULLIF(BTRIM(COALESCE(v_current_ts.qr_token, '')), '') IS NOT NULL
          AND v_current_ts.qr_generated_at IS NOT NULL
          AND v_current_ts.qr_scanned_at IS NULL
        )
      ),
      false
    );

    IF v_is_archived THEN
      v_disabled_reasons := array_append(v_disabled_reasons, 'TIMESHEET_ARCHIVED');
    END IF;
    IF v_is_invoice_locked THEN
      v_disabled_reasons := array_append(v_disabled_reasons, 'TIMESHEET_LOCKED_BY_INVOICE');
    END IF;
    IF v_is_authorised THEN
      v_disabled_reasons := array_append(v_disabled_reasons, 'ALREADY_AUTHORISED');
    END IF;
    IF v_qr_unsigned_blocked THEN
      v_disabled_reasons := array_append(v_disabled_reasons, 'AWAITING_SIGNED_QR');
    END IF;

    v_can_process := COALESCE((NOT v_is_archived) AND (NOT v_is_locked) AND (NOT v_is_authorised) AND v_is_unprocessed, false);
    v_unprocess_action_visible := COALESCE(
      (NOT v_is_archived)
      AND (NOT v_is_locked)
      AND (NOT v_is_authorised)
      AND (NOT v_is_unprocessed)
      AND v_current_ts.timesheet_id IS NOT NULL,
      false
    );
    v_can_unprocess := COALESCE(v_unprocess_action_visible AND (NOT v_has_retained_financial_history), false);
    v_unprocess_block_reason := CASE
      WHEN v_unprocess_action_visible AND v_has_retained_financial_history
      THEN 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS'
      ELSE NULL
    END;
    IF v_unprocess_block_reason IS NOT NULL THEN
      v_disabled_reasons := array_append(v_disabled_reasons, v_unprocess_block_reason);
    END IF;
    v_can_authorise := COALESCE(
      (NOT v_is_archived)
      AND (NOT v_is_locked)
      AND (NOT v_is_authorised)
      AND (NOT v_qr_unsigned_blocked)
      AND v_current_ts.timesheet_id IS NOT NULL
      AND v_tsfin.processing_status IN (
        'PENDING_AUTH'::public.ts_fin_processing_status_enum,
        'READY_FOR_HR'::public.ts_fin_processing_status_enum
      ),
      false
    );
    v_can_unauthorise := COALESCE((NOT v_is_archived) AND (NOT v_is_locked) AND v_is_authorised AND v_current_ts.timesheet_id IS NOT NULL, false);

    v_bucket := CASE
      WHEN v_is_archived THEN 'ARCHIVED'
      WHEN v_is_authorised THEN 'AUTHORISED'
      WHEN v_current_ts.timesheet_id IS NULL THEN 'UNPROCESSED'
      WHEN v_is_unprocessed THEN 'UNPROCESSED'
      ELSE 'PROCESSED'
    END;

    v_row_key := CASE
      WHEN v_current_ts.timesheet_id IS NOT NULL THEN 'timesheet:' || v_current_ts.timesheet_id::text
      WHEN v_week.id IS NOT NULL THEN 'contract_week:' || v_week.id::text
      ELSE NULL
    END;

    IF v_removed_reason IS NOT NULL THEN
      v_removed := v_removed || jsonb_build_array(
        jsonb_strip_nulls(jsonb_build_object(
          'reason', v_removed_reason,
          'previous_timesheet_id', COALESCE(v_previous_timesheet_id, v_timesheet_id),
          'contract_week_id', v_week.id,
          'previous_row_key', v_previous_row_key,
          'current_row_key', v_row_key
        ))
      );
    END IF;

    v_rows := v_rows || jsonb_build_array(
      jsonb_strip_nulls(
        jsonb_build_object(
          'identity', jsonb_strip_nulls(jsonb_build_object(
            'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
            'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
            'previous_timesheet_id', COALESCE(v_previous_timesheet_id, v_timesheet_id),
            'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
            'booking_id', COALESCE(v_current_ts.booking_id, v_booking_id),
            'row_key', v_row_key,
            'previous_row_key', v_previous_row_key
          )),
          'status', jsonb_strip_nulls(jsonb_build_object(
            'bucket', v_bucket,
            'previous_bucket', v_previous_bucket,
            'timesheet_status', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.status::text END,
            'processing_status', CASE WHEN v_tsfin.id IS NULL THEN NULL ELSE v_tsfin.processing_status::text END,
            'contract_week_status', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.status::text END,
            'is_paid', v_is_paid,
            'is_archived', v_is_archived,
            'archived_at_utc', CASE WHEN v_is_archived THEN v_current_ts.archived_at_utc ELSE NULL END,
            'archived_by_user_id', CASE WHEN v_is_archived THEN v_current_ts.archived_by_user_id ELSE NULL END,
            'archived_reason_code', CASE WHEN v_is_archived THEN v_current_ts.archived_reason_code ELSE NULL END,
            'is_invoice_locked', v_is_invoice_locked,
            'is_locked', v_is_locked,
            'is_authorised', v_is_authorised,
            'is_unprocessed', v_is_unprocessed,
            'has_retained_financial_history', v_has_retained_financial_history,
            'can_unprocess', v_can_unprocess,
            'unprocess_block_reason', v_unprocess_block_reason,
            'unprocess_action_visible', v_unprocess_action_visible,
            'qr_unsigned_blocked', v_qr_unsigned_blocked
          )),
          'actions', jsonb_build_object(
            'can_process', v_can_process,
            'can_unprocess', v_can_unprocess,
            'has_retained_financial_history', v_has_retained_financial_history,
            'unprocess_block_reason', v_unprocess_block_reason,
            'unprocess_action_visible', v_unprocess_action_visible,
            'can_authorise', v_can_authorise,
            'can_unauthorise', v_can_unauthorise,
            'disabled_reasons', to_jsonb(v_disabled_reasons)
          ),
          'action_flags', jsonb_build_object(
            'can_process', v_can_process,
            'can_unprocess', v_can_unprocess,
            'has_retained_financial_history', v_has_retained_financial_history,
            'unprocess_block_reason', v_unprocess_block_reason,
            'unprocess_action_visible', v_unprocess_action_visible,
            'can_authorise', v_can_authorise,
            'can_unauthorise', v_can_unauthorise
          ),
          'row_patch', jsonb_strip_nulls(jsonb_build_object(
            'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
            'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
            'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
            'row_key', v_row_key,
            'backend_row_signature', v_signature_json ->> 'backend_row_signature',
            'row_signature', v_signature_json ->> 'row_signature',
            'has_retained_financial_history', v_has_retained_financial_history,
            'can_unprocess', v_can_unprocess,
            'unprocess_block_reason', v_unprocess_block_reason,
            'unprocess_action_visible', v_unprocess_action_visible
          ))
        )
        || jsonb_build_object(
          'display', jsonb_strip_nulls(jsonb_build_object(
            'context', v_item_context,
            'week_ending_date', COALESCE(v_current_ts.week_ending_date, v_week.week_ending_date),
            'candidate_id', COALESCE(v_tsfin.candidate_id, v_contract.candidate_id),
            'candidate_display', NULLIF(COALESCE(v_candidate.display_name, BTRIM(CONCAT_WS(' ', v_candidate.first_name, v_candidate.last_name))), ''),
            'client_id', COALESCE(v_tsfin.client_id, v_contract.client_id),
            'client_name', NULLIF(v_client.name, ''),
            'role', COALESCE(NULLIF(v_tsfin.role, ''), NULLIF(v_contract.role, ''), NULLIF(v_current_ts.job_title_norm, '')),
            'band', COALESCE(NULLIF(v_tsfin.band, ''), NULLIF(v_contract.band, ''), NULLIF(v_current_ts.band, '')),
            'total_hours', v_tsfin.total_hours,
            'total_pay_ex_vat', v_tsfin.total_pay_ex_vat,
            'total_charge_ex_vat', v_tsfin.total_charge_ex_vat,
            'margin_ex_vat', v_tsfin.margin_ex_vat
          )),
          'evidence_summary', COALESCE(v_evidence_summary, '{}'::jsonb),
          'staged_evidence_summary', COALESCE(v_staged_summary, '{}'::jsonb),
          'backend_row_signature', v_signature_json ->> 'backend_row_signature',
          'row_signature', v_signature_json ->> 'row_signature',
          'render_signature', v_signature_json ->> 'signature',
          'cache_invalidation_hints', jsonb_build_object(
            'changed_domains', jsonb_build_array('timesheet_lifecycle'),
            'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
            'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END
          )
        )
      )
    );
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'rows', v_rows,
    'missing', v_missing,
    'removed', v_removed,
    'count_deltas', '{}'::jsonb,
    'cache_invalidation_hints', jsonb_build_object(
      'changed_domains', jsonb_build_array('timesheet_lifecycle'),
      'context', NULLIF(BTRIM(COALESCE(p_context, '')), '')
    )
  );
END;
$function$;

-- timesheet_lifecycle_guard_signature_v1(uuid,uuid,boolean)
CREATE OR REPLACE FUNCTION public.timesheet_lifecycle_guard_signature_v1(p_timesheet_id uuid DEFAULT NULL::uuid, p_contract_week_id uuid DEFAULT NULL::uuid, p_include_payload boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_week public.contract_weeks%ROWTYPE;
  v_tsfin public.timesheets_financials%ROWTYPE;

  v_resolved_booking_id text := NULL;
  v_signature_payload jsonb := '{}'::jsonb;
  v_component_hashes jsonb := '{}'::jsonb;
  v_component_values jsonb := '{}'::jsonb;
  v_diagnostic_payload jsonb := '{}'::jsonb;
  v_candidate_component jsonb := '{}'::jsonb;
  v_candidate_component_enabled boolean := private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities');
  v_temp_log_enabled boolean := false;
  v_signature text := NULL;
BEGIN
  IF p_timesheet_id IS NULL AND p_contract_week_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'INVALID_PAYLOAD',
      'message', 'timesheet_id or contract_week_id is required',
      'signature', NULL,
      'backend_row_signature', NULL,
      'row_signature', NULL
    );
  END IF;

  IF p_contract_week_id IS NOT NULL THEN
    SELECT cw.*
      INTO v_week
    FROM public.contract_weeks AS cw
    WHERE cw.id = p_contract_week_id
    LIMIT 1;
  END IF;

  IF p_timesheet_id IS NOT NULL THEN
    SELECT ts.*
      INTO v_requested_ts
    FROM public.timesheets AS ts
    WHERE ts.timesheet_id = p_timesheet_id
    LIMIT 1;

    IF v_requested_ts.timesheet_id IS NOT NULL THEN
      v_resolved_booking_id := v_requested_ts.booking_id;
    END IF;
  END IF;

  IF v_resolved_booking_id IS NULL AND v_week.timesheet_id IS NOT NULL THEN
    SELECT ts.*
      INTO v_requested_ts
    FROM public.timesheets AS ts
    WHERE ts.timesheet_id = v_week.timesheet_id
    LIMIT 1;

    IF v_requested_ts.timesheet_id IS NOT NULL THEN
      v_resolved_booking_id := v_requested_ts.booking_id;
    END IF;
  END IF;

  IF v_resolved_booking_id IS NOT NULL THEN
    SELECT ts.*
      INTO v_current_ts
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_resolved_booking_id
      AND ts.is_current = true
    ORDER BY ts.version DESC, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
    LIMIT 1;
  END IF;

  IF v_current_ts.timesheet_id IS NULL AND v_requested_ts.timesheet_id IS NOT NULL THEN
    v_current_ts := v_requested_ts;
  END IF;

  IF v_week.id IS NULL AND v_current_ts.timesheet_id IS NOT NULL THEN
    SELECT cw.*
      INTO v_week
    FROM public.contract_weeks AS cw
    WHERE cw.timesheet_id = v_current_ts.timesheet_id
       OR EXISTS (
         SELECT 1
         FROM public.timesheets AS cw_ts
         WHERE cw_ts.timesheet_id = cw.timesheet_id
           AND cw_ts.booking_id = v_current_ts.booking_id
       )
    ORDER BY
      CASE WHEN cw.timesheet_id = v_current_ts.timesheet_id THEN 0 ELSE 1 END,
      cw.updated_at DESC NULLS LAST,
      cw.created_at DESC NULLS LAST,
      cw.id DESC
    LIMIT 1;
  END IF;

  IF v_current_ts.timesheet_id IS NULL AND v_week.id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'TARGET_NOT_FOUND',
      'message', 'No current timesheet or contract week was found for the supplied identity',
      'timesheet_id', p_timesheet_id,
      'contract_week_id', p_contract_week_id,
      'signature', NULL,
      'backend_row_signature', NULL,
      'row_signature', NULL
    );
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL THEN
    SELECT tf.*
      INTO v_tsfin
    FROM public.timesheets_financials AS tf
    WHERE tf.timesheet_id = v_current_ts.timesheet_id
      AND tf.is_current = true
    ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
    LIMIT 1;
  END IF;

  IF v_candidate_component_enabled THEN
    v_candidate_component := private._candidate_signature_component_v1(
      CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END
    );
  END IF;

  v_signature_payload := jsonb_strip_nulls(jsonb_build_object(
    'signature_version', 'timesheet_lifecycle_guard_signature_v1',
    'identity', jsonb_build_object(
      'requested_timesheet_id', p_timesheet_id,
      'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
      'booking_id', COALESCE(v_current_ts.booking_id, v_requested_ts.booking_id, v_resolved_booking_id)
    ),
    'timesheet', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'booking_id', v_current_ts.booking_id,
      'version', v_current_ts.version,
      'is_current', v_current_ts.is_current,
    'archived_at_utc', v_current_ts.archived_at_utc,
    'archived_by_user_id', v_current_ts.archived_by_user_id,
    'archived_reason_code', v_current_ts.archived_reason_code,
      'status', v_current_ts.status::text,
      'candidate_submission_route_intent', v_current_ts.candidate_submission_route_intent,
      'authorised_at_server', v_current_ts.authorised_at_server,
      'revoked_at', v_current_ts.revoked_at,
      'updated_at', v_current_ts.updated_at
    ) END,
    'timesheets_financials', CASE WHEN v_tsfin.id IS NULL THEN NULL ELSE jsonb_build_object(
      'id', v_tsfin.id,
      'timesheet_id', v_tsfin.timesheet_id,
      'is_current', v_tsfin.is_current,
      'processing_status', v_tsfin.processing_status::text,
      'authorised_at_utc', v_tsfin.authorised_at_utc,
      'locked_by_invoice_id', v_tsfin.locked_by_invoice_id,
      'locked_at_utc', v_tsfin.locked_at_utc,
      'paid_at_utc', v_tsfin.paid_at_utc,
      'is_stale', v_tsfin.is_stale,
      'computed_at_utc', v_tsfin.computed_at_utc,
      'updated_at', v_tsfin.updated_at
    ) END,
    'contract_week', CASE WHEN v_week.id IS NULL THEN NULL ELSE jsonb_build_object(
      'id', v_week.id,
      'timesheet_id', v_week.timesheet_id,
      'status', v_week.status::text,
      'updated_at', v_week.updated_at
    ) END
  ));

  IF v_candidate_component_enabled THEN
    v_signature_payload := v_signature_payload || jsonb_build_object('candidate_app', v_candidate_component);
  END IF;

  v_signature := MD5(v_signature_payload::text);

  IF COALESCE(p_include_payload, false) THEN
    BEGIN
      SELECT COALESCE(sd.temp_log, false)
        INTO v_temp_log_enabled
      FROM public.settings_defaults AS sd
      ORDER BY sd.id
      LIMIT 1;
    EXCEPTION
      WHEN undefined_table OR undefined_column THEN
        v_temp_log_enabled := false;
      WHEN OTHERS THEN
        v_temp_log_enabled := false;
    END;

    IF COALESCE(v_temp_log_enabled, false) THEN
      v_component_values := jsonb_strip_nulls(jsonb_build_object(
        'identity', v_signature_payload -> 'identity',
        'timesheet', v_signature_payload -> 'timesheet',
        'timesheets_financials', v_signature_payload -> 'timesheets_financials',
        'contract_week', v_signature_payload -> 'contract_week'
      ));
      v_component_hashes := jsonb_strip_nulls(jsonb_build_object(
        'identity', md5(COALESCE((v_signature_payload -> 'identity')::text, 'null')),
        'timesheet', md5(COALESCE((v_signature_payload -> 'timesheet')::text, 'null')),
        'timesheets_financials', md5(COALESCE((v_signature_payload -> 'timesheets_financials')::text, 'null')),
        'contract_week', md5(COALESCE((v_signature_payload -> 'contract_week')::text, 'null')),
        'full_payload', v_signature
      ));
      IF v_candidate_component_enabled THEN
        v_component_values := v_component_values || jsonb_build_object('candidate_app', v_signature_payload -> 'candidate_app');
        v_component_hashes := v_component_hashes || jsonb_build_object(
          'candidate_app', md5(COALESCE((v_signature_payload -> 'candidate_app')::text, 'null'))
        );
      END IF;
      v_diagnostic_payload := jsonb_strip_nulls(jsonb_build_object(
        'tag', 'TIMESHEET_LIFECYCLE_SIGNATURE_PAYLOAD',
        'function_name', 'timesheet_lifecycle_guard_signature_v1',
        'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
        'requested_timesheet_id', p_timesheet_id,
        'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
        'booking_id', COALESCE(v_current_ts.booking_id, v_requested_ts.booking_id, v_resolved_booking_id),
        'signature', v_signature,
        'backend_row_signature', v_signature,
        'component_hashes', v_component_hashes,
        'component_values', v_component_values
      ));
      PERFORM public._temp_diag_log(
        'TIMESHEET_LIFECYCLE_SIGNATURE_PAYLOAD',
        'TEMP_TIMESHEET_LIFECYCLE',
        COALESCE(
          CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END,
          p_timesheet_id::text,
          p_contract_week_id::text
        ),
        v_diagnostic_payload
      );
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'signature_version', 'timesheet_lifecycle_guard_signature_v1',
    'signature', v_signature,
    'backend_row_signature', v_signature,
    'row_signature', v_signature,
    'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
    'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
    'requested_timesheet_id', p_timesheet_id,
    'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
    'booking_id', COALESCE(v_current_ts.booking_id, v_requested_ts.booking_id, v_resolved_booking_id),
    'current_version', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.version END,
    'payload', CASE WHEN COALESCE(p_include_payload, false) THEN v_signature_payload ELSE NULL END
  ) || CASE
    WHEN COALESCE(p_include_payload, false) AND COALESCE(v_temp_log_enabled, false) THEN
      jsonb_build_object(
        'component_hashes', v_component_hashes,
        'component_values', v_component_values,
        'diagnostic_payload', v_diagnostic_payload
      )
    ELSE '{}'::jsonb
  END;
END;
$function$;

-- timesheet_lifecycle_signature_v1(uuid,uuid,boolean)
CREATE OR REPLACE FUNCTION public.timesheet_lifecycle_signature_v1(p_timesheet_id uuid DEFAULT NULL::uuid, p_contract_week_id uuid DEFAULT NULL::uuid, p_include_payload boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_week public.contract_weeks%ROWTYPE;
  v_tsfin public.timesheets_financials%ROWTYPE;
  v_validation public.timesheet_validations%ROWTYPE;

  v_resolved_booking_id text := NULL;
  v_target_exists boolean := FALSE;
  v_invoice_segments_locked integer := 0;

  v_evidence_payload jsonb := '{}'::jsonb;
  v_staged_payload jsonb := '{}'::jsonb;
  v_validation_payload jsonb := '{}'::jsonb;
  v_timesheet_payload jsonb := '{}'::jsonb;
  v_contract_week_payload jsonb := '{}'::jsonb;
  v_tsfin_payload jsonb := '{}'::jsonb;
  v_identity_payload jsonb := '{}'::jsonb;
  v_signature_payload jsonb := '{}'::jsonb;
  v_signature text := NULL;
BEGIN
  IF COALESCE(p_include_payload, false) IS NOT TRUE THEN
    RETURN public.timesheet_lifecycle_guard_signature_v1(p_timesheet_id, p_contract_week_id, false);
  END IF;
  IF p_timesheet_id IS NULL AND p_contract_week_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'INVALID_PAYLOAD',
      'message', 'timesheet_id or contract_week_id is required',
      'signature', NULL,
      'backend_row_signature', NULL,
      'row_signature', NULL
    );
  END IF;

  IF p_contract_week_id IS NOT NULL THEN
    SELECT cw.*
      INTO v_week
    FROM public.contract_weeks AS cw
    WHERE cw.id = p_contract_week_id
    LIMIT 1;
  END IF;

  IF p_timesheet_id IS NOT NULL THEN
    SELECT ts.*
      INTO v_requested_ts
    FROM public.timesheets AS ts
    WHERE ts.timesheet_id = p_timesheet_id
    LIMIT 1;

    IF v_requested_ts.timesheet_id IS NOT NULL THEN
      v_resolved_booking_id := v_requested_ts.booking_id;
    END IF;
  END IF;

  IF v_resolved_booking_id IS NULL AND v_week.timesheet_id IS NOT NULL THEN
    SELECT ts.*
      INTO v_requested_ts
    FROM public.timesheets AS ts
    WHERE ts.timesheet_id = v_week.timesheet_id
    LIMIT 1;

    IF v_requested_ts.timesheet_id IS NOT NULL THEN
      v_resolved_booking_id := v_requested_ts.booking_id;
    END IF;
  END IF;

  IF v_resolved_booking_id IS NOT NULL THEN
    SELECT ts.*
      INTO v_current_ts
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_resolved_booking_id
      AND ts.is_current = true
    ORDER BY ts.version DESC, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
    LIMIT 1;
  END IF;

  IF v_current_ts.timesheet_id IS NULL AND v_requested_ts.timesheet_id IS NOT NULL THEN
    v_current_ts := v_requested_ts;
  END IF;

  IF v_week.id IS NULL AND v_current_ts.timesheet_id IS NOT NULL THEN
    SELECT cw.*
      INTO v_week
    FROM public.contract_weeks AS cw
    WHERE cw.timesheet_id = v_current_ts.timesheet_id
    ORDER BY cw.updated_at DESC NULLS LAST, cw.created_at DESC NULLS LAST, cw.id DESC
    LIMIT 1;
  END IF;

  IF v_week.id IS NOT NULL AND v_resolved_booking_id IS NULL AND v_current_ts.booking_id IS NOT NULL THEN
    v_resolved_booking_id := v_current_ts.booking_id;
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL THEN
    SELECT tf.*
      INTO v_tsfin
    FROM public.timesheets_financials AS tf
    WHERE tf.timesheet_id = v_current_ts.timesheet_id
      AND tf.is_current = true
    ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
    LIMIT 1;
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL OR v_resolved_booking_id IS NOT NULL THEN
    SELECT tv.*
      INTO v_validation
    FROM public.timesheet_validations AS tv
    WHERE (
        v_current_ts.timesheet_id IS NOT NULL
        AND tv.timesheet_id = v_current_ts.timesheet_id
      )
       OR (
        v_resolved_booking_id IS NOT NULL
        AND tv.booking_id = v_resolved_booking_id
      )
    ORDER BY tv.updated_at DESC NULLS LAST, tv.created_at DESC NULLS LAST, tv.id DESC
    LIMIT 1;
  END IF;

  IF v_tsfin.id IS NOT NULL THEN
    SELECT COUNT(*)::integer
      INTO v_invoice_segments_locked
    FROM jsonb_array_elements(
      CASE
        WHEN v_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_tsfin.invoice_breakdown_json) = 'array' THEN v_tsfin.invoice_breakdown_json
        WHEN jsonb_typeof(v_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_tsfin.invoice_breakdown_json -> 'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL;
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL THEN
    WITH evidence_rows AS (
      SELECT
        ev.id,
        ev.kind,
        ev.storage_key,
        ev.created_at,
        ev.display_name
      FROM public.timesheet_evidence AS ev
      WHERE ev.timesheet_id = v_current_ts.timesheet_id
    ), evidence_ordered AS (
      SELECT
        er.id,
        er.kind,
        er.storage_key,
        er.created_at,
        er.display_name,
        ROW_NUMBER() OVER (
          PARTITION BY UPPER(COALESCE(NULLIF(BTRIM(er.kind), ''), 'OTHER'))
          ORDER BY er.created_at ASC NULLS LAST, er.id ASC
        ) AS kind_rank
      FROM evidence_rows AS er
    )
    SELECT jsonb_strip_nulls(
      jsonb_build_object(
        'count', COUNT(*)::integer,
        'updated_at', MAX(eo.created_at),
        'timesheet_count', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(eo.kind), ''), 'OTHER')) = 'TIMESHEET')::integer,
        'mileage_count', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(eo.kind), ''), 'OTHER')) = 'MILEAGE')::integer,
        'travel_count', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(eo.kind), ''), 'OTHER')) = 'TRAVEL')::integer,
        'accommodation_count', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(eo.kind), ''), 'OTHER')) = 'ACCOMMODATION')::integer,
        'other_count', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(eo.kind), ''), 'OTHER')) NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION'))::integer,
        'primary_timesheet_storage_key', MIN(eo.storage_key) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(eo.kind), ''), 'OTHER')) = 'TIMESHEET' AND eo.kind_rank = 1),
        'primary_storage_key', MIN(eo.storage_key) FILTER (WHERE eo.kind_rank = 1)
      )
    )
      INTO v_evidence_payload
    FROM evidence_ordered AS eo;
  ELSE
    v_evidence_payload := jsonb_build_object(
      'count', 0,
      'timesheet_count', 0,
      'mileage_count', 0,
      'travel_count', 0,
      'accommodation_count', 0,
      'other_count', 0
    );
  END IF;

  IF v_week.id IS NOT NULL THEN
    WITH staged_rows AS (
      SELECT
        mq.id,
        mq.timesheet_id,
        mq.r2_key,
        mq.status,
        mq.uploaded_at_utc,
        mq.last_rotation_deg,
        mq.meta_json,
        UPPER(COALESCE(
          NULLIF(BTRIM(mq.meta_json ->> 'staged_kind'), ''),
          NULLIF(BTRIM(mq.meta_json ->> 'kind'), ''),
          NULLIF(BTRIM(mq.meta_json ->> 'attached_kind'), ''),
          'TIMESHEET'
        )) AS staged_kind,
        NULLIF(regexp_replace(COALESCE(
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
    ), staged_key_rows AS (
      SELECT DISTINCT sr.storage_key
      FROM staged_rows AS sr
      WHERE sr.staged_kind = 'TIMESHEET'
        AND sr.storage_key IS NOT NULL
      ORDER BY sr.storage_key
    )
    SELECT jsonb_strip_nulls(
      jsonb_build_object(
        'contract_week_id', v_week.id,
        'staged_count', COUNT(*)::integer,
        'staged_updated_at', MAX(sr.uploaded_at_utc),
        'active_staged_timesheet_count', COUNT(*) FILTER (WHERE sr.staged_kind = 'TIMESHEET')::integer,
        'active_staged_timesheet_storage_key_count', (SELECT COUNT(*)::integer FROM staged_key_rows),
        'active_staged_timesheet_storage_keys', COALESCE((SELECT jsonb_agg(skr.storage_key ORDER BY skr.storage_key) FROM staged_key_rows AS skr), '[]'::jsonb),
        'has_staged_timesheet', COUNT(*) FILTER (WHERE sr.staged_kind = 'TIMESHEET') > 0,
        'has_staged_mileage', COUNT(*) FILTER (WHERE sr.staged_kind = 'MILEAGE') > 0,
        'has_staged_travel', COUNT(*) FILTER (WHERE sr.staged_kind = 'TRAVEL') > 0,
        'has_staged_accommodation', COUNT(*) FILTER (WHERE sr.staged_kind = 'ACCOMMODATION') > 0,
        'has_staged_other', COUNT(*) FILTER (WHERE sr.staged_kind NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION')) > 0,
        'primary_staged_timesheet_storage_key', MIN(sr.storage_key) FILTER (WHERE sr.staged_kind = 'TIMESHEET'),
        'primary_staged_timesheet_rotation_degrees', MIN(sr.last_rotation_deg) FILTER (WHERE sr.staged_kind = 'TIMESHEET' AND sr.storage_key = (SELECT MIN(skr.storage_key) FROM staged_key_rows AS skr))
      )
    )
      INTO v_staged_payload
    FROM staged_rows AS sr;
  ELSE
    v_staged_payload := jsonb_build_object(
      'staged_count', 0,
      'active_staged_timesheet_count', 0,
      'active_staged_timesheet_storage_key_count', 0,
      'active_staged_timesheet_storage_keys', '[]'::jsonb,
      'has_staged_timesheet', false,
      'has_staged_mileage', false,
      'has_staged_travel', false,
      'has_staged_accommodation', false,
      'has_staged_other', false
    );
  END IF;

  v_identity_payload := jsonb_strip_nulls(
    jsonb_build_object(
      'requested_timesheet_id', p_timesheet_id,
      'requested_contract_week_id', p_contract_week_id,
      'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
      'booking_id', COALESCE(v_current_ts.booking_id, v_resolved_booking_id),
      'target_exists', (v_current_ts.timesheet_id IS NOT NULL OR v_week.id IS NOT NULL)
    )
  );

  v_timesheet_payload := jsonb_strip_nulls(
    jsonb_build_object(
      'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      'booking_id', NULLIF(v_current_ts.booking_id, ''),
      'version', v_current_ts.version,
      'is_current', v_current_ts.is_current,
    'archived_at_utc', v_current_ts.archived_at_utc,
    'archived_by_user_id', v_current_ts.archived_by_user_id,
    'archived_reason_code', v_current_ts.archived_reason_code,
      'status', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.status::text END,
      'sheet_scope', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.sheet_scope::text END,
      'submission_mode', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.submission_mode::text END,
      'line_type', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.line_type::text END,
      'updated_at', v_current_ts.updated_at,
      'authorised_at_server', v_current_ts.authorised_at_server,
      'manual_pdf_r2_key', NULLIF(v_current_ts.manual_pdf_r2_key, ''),
      'manual_pdf_rotation_degrees', v_current_ts.manual_pdf_rotation_degrees,
      'qr_status', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.qr_status::text END,
      'qr_generated_at', v_current_ts.qr_generated_at,
      'qr_scanned_at', v_current_ts.qr_scanned_at,
      'qr_signed_at_utc', v_current_ts.qr_signed_at_utc,
      'generated_pdf_at_utc', v_current_ts.generated_pdf_at_utc,
      'reference_number', NULLIF(v_current_ts.reference_number, ''),
      'is_adjustment', v_current_ts.is_adjustment,
      'parent_timesheet_id', v_current_ts.parent_timesheet_id
    )
  );

  v_contract_week_payload := jsonb_strip_nulls(
    jsonb_build_object(
      'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
      'contract_id', v_week.contract_id,
      'week_ending_date', v_week.week_ending_date,
      'additional_seq', v_week.additional_seq,
      'status', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.status::text END,
      'submission_mode_snapshot', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.submission_mode_snapshot::text END,
      'timesheet_id', v_week.timesheet_id,
      'uploaded_pdf_r2_key', NULLIF(v_week.uploaded_pdf_r2_key, ''),
      'updated_at', v_week.updated_at,
      'is_adjustment', v_week.is_adjustment,
      'enforce_day_partition', v_week.enforce_day_partition,
      'allowed_days_mask', NULLIF(v_week.allowed_days_mask, ''),
      'split_boundary_date', v_week.split_boundary_date,
      'split_group_key', NULLIF(v_week.split_group_key, '')
    )
  );

  v_tsfin_payload := jsonb_strip_nulls(
    jsonb_build_object(
      'timesheet_financials_id', CASE WHEN v_tsfin.id IS NULL THEN NULL ELSE v_tsfin.id END,
      'timesheet_version', v_tsfin.timesheet_version,
      'is_current', v_tsfin.is_current,
      'is_stale', v_tsfin.is_stale,
      'processing_status', CASE WHEN v_tsfin.id IS NULL THEN NULL ELSE v_tsfin.processing_status::text END,
      'candidate_assignment', CASE WHEN v_tsfin.id IS NULL THEN NULL ELSE v_tsfin.candidate_assignment::text END,
      'basis', CASE WHEN v_tsfin.id IS NULL THEN NULL ELSE v_tsfin.basis::text END,
      'updated_at', v_tsfin.updated_at,
      'computed_at_utc', v_tsfin.computed_at_utc,
      'locked_by_invoice_id', v_tsfin.locked_by_invoice_id,
      'locked_at_utc', v_tsfin.locked_at_utc,
      'invoice_segments_locked', v_invoice_segments_locked,
      'paid_at_utc', v_tsfin.paid_at_utc,
      'pay_on_hold', v_tsfin.pay_on_hold,
      'candidate_id', v_tsfin.candidate_id,
      'client_id', v_tsfin.client_id,
      'pay_method', NULLIF(v_tsfin.pay_method, ''),
      'has_rate_issue', v_tsfin.has_rate_issue,
      'has_pay_channel_issue', v_tsfin.has_pay_channel_issue,
      'hr_crosscheck_status', NULLIF(v_tsfin.hr_crosscheck_status, ''),
      'total_hours', v_tsfin.total_hours,
      'total_pay_ex_vat', v_tsfin.total_pay_ex_vat,
      'total_charge_ex_vat', v_tsfin.total_charge_ex_vat,
      'margin_ex_vat', v_tsfin.margin_ex_vat,
      'expenses_pay_ex_vat', v_tsfin.expenses_pay_ex_vat,
      'expenses_charge_ex_vat', v_tsfin.expenses_charge_ex_vat,
      'mileage_units', v_tsfin.mileage_units,
      'mileage_pay_ex_vat', v_tsfin.mileage_pay_ex_vat,
      'mileage_charge_ex_vat', v_tsfin.mileage_charge_ex_vat
    )
  );

  v_validation_payload := jsonb_strip_nulls(
    jsonb_build_object(
      'validation_id', CASE WHEN v_validation.id IS NULL THEN NULL ELSE v_validation.id END,
      'status', CASE WHEN v_validation.id IS NULL THEN NULL ELSE v_validation.status::text END,
      'reason_code', NULLIF(v_validation.reason_code, ''),
      'pre_validated', v_validation.pre_validated,
      'validated_at_utc', v_validation.validated_at_utc,
      'override_confirmed_at_utc', v_validation.override_confirmed_at_utc,
      'updated_at', v_validation.updated_at,
      'hr_request_source', CASE WHEN v_validation.id IS NULL THEN NULL ELSE v_validation.hr_request_source::text END
    )
  );

  v_signature_payload :=
    jsonb_build_object(
      'identity', v_identity_payload,
      'timesheet', v_timesheet_payload,
      'contract_week', v_contract_week_payload
    )
    || jsonb_build_object(
      'tsfin', v_tsfin_payload,
      'validation', v_validation_payload,
      'evidence', COALESCE(v_evidence_payload, '{}'::jsonb),
      'staged_evidence', COALESCE(v_staged_payload, '{}'::jsonb)
    );

  v_signature := md5(v_signature_payload::text);
  v_target_exists := (v_current_ts.timesheet_id IS NOT NULL OR v_week.id IS NOT NULL);

  RETURN jsonb_strip_nulls(
    jsonb_build_object(
      'ok', v_target_exists,
      'signature', v_signature,
      'backend_row_signature', v_signature,
      'row_signature', v_signature,
      'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
      'booking_id', COALESCE(v_current_ts.booking_id, v_resolved_booking_id),
      'target_exists', v_target_exists
    )
    || CASE
      WHEN COALESCE(p_include_payload, false) THEN jsonb_build_object('signature_payload', v_signature_payload)
      ELSE '{}'::jsonb
    END
  );
END;
$function$;

-- timesheet_list_ids(jsonb)
CREATE OR REPLACE FUNCTION public.timesheet_list_ids(p_filters jsonb)
 RETURNS TABLE(id uuid)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_source_filters jsonb := '{}'::jsonb;

  v_client_id uuid := NULL;
  v_candidate_id uuid := NULL;
  v_summary_stage text := NULL;
  v_tools_stage text := NULL;
  v_route_type text := NULL;
  v_sheet_scope text := NULL;
  v_qr_status text := NULL;
  v_we_from date := NULL;
  v_we_to date := NULL;
  v_is_adjusted text := NULL;
  v_is_qr text := NULL;
  v_needs_attention text := NULL;
  v_candidate_paid text := NULL;
  v_client_invoiced text := NULL;
  v_hr_issue text := NULL;
  v_proc_status_raw text := NULL;
  v_proc_list text[] := NULL;
  v_status_code text := NULL;
  v_issues_filter text := NULL;
  v_q text := NULL;
  v_q_like text := NULL;
  v_ids text[] := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_has_client_filter boolean := FALSE;
  v_has_candidate_filter boolean := FALSE;
BEGIN
  v_source_filters :=
    v_filters
    - 'q'
    - 'query'
    - 'name'
    - 'client_id'
    - 'clientId'
    - 'candidate_id'
    - 'candidateId'
    - 'week_ending_from'
    - 'weekEndingFrom'
    - 'week_ending_to'
    - 'weekEndingTo'
    - 'route_type'
    - 'routeType'
    - 'status_code'
    - 'statusCode'
    - 'summary_stage'
    - 'summaryStage'
    - 'client_invoiced'
    - 'clientInvoiced'
    - 'needs_attention'
    - 'needsAttention'
    || jsonb_build_object('disable_paging', TRUE, 'purpose', 'membership');

  v_has_client_filter := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '') IS NOT NULL;
  IF v_has_client_filter THEN
    IF COALESCE(v_filters->>'client_id', v_filters->>'clientId') ~* v_uuid_re THEN
      v_client_id := COALESCE(v_filters->>'client_id', v_filters->>'clientId')::uuid;
    ELSE
      RETURN;
    END IF;
  END IF;

  v_has_candidate_filter := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '') IS NOT NULL;
  IF v_has_candidate_filter THEN
    IF COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId') ~* v_uuid_re THEN
      v_candidate_id := COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId')::uuid;
    ELSE
      RETURN;
    END IF;
  END IF;

  v_q := NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'query', v_filters->>'name', '')), '');
  IF v_q IS NOT NULL THEN
    v_q_like := '%' || REPLACE(REPLACE(REPLACE(v_q, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%';
  END IF;

  BEGIN
    IF v_filters ? 'ids' THEN
      IF jsonb_typeof(v_filters->'ids') = 'array' THEN
        SELECT ARRAY_AGG(id_values.id_value)
        INTO v_ids
        FROM (
          SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS id_value
          FROM jsonb_array_elements_text(v_filters->'ids') AS input_values(value)
        ) AS id_values
        WHERE id_values.id_value IS NOT NULL;
      ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL THEN
        SELECT ARRAY_AGG(id_values.id_value)
        INTO v_ids
        FROM (
          SELECT DISTINCT NULLIF(BTRIM(split_values.value), '') AS id_value
          FROM unnest(regexp_split_to_array(v_filters->>'ids', '\s*,\s*')) AS split_values(value)
        ) AS id_values
        WHERE id_values.id_value IS NOT NULL;
      END IF;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_ids := NULL;
  END;

  v_summary_stage := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'summary_stage', v_filters->>'summaryStage', '')), ''));
  IF v_summary_stage = 'ALL' THEN v_summary_stage := NULL; END IF;

  v_tools_stage := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'tools_stage', v_filters->>'toolsStage', '')), ''));
  IF v_tools_stage = 'ALL' THEN v_tools_stage := NULL; END IF;

  v_issues_filter := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'issues_filter', v_filters->>'issuesFilter', '')), ''));
  IF v_issues_filter = 'ALL' THEN v_issues_filter := NULL; END IF;
  -- The row source is the single issue-classification authority. It receives
  -- the original issues filter above, so membership must not reclassify it.
  v_issues_filter := NULL;

  v_route_type := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'route_type', v_filters->>'routeType', '')), ''));
  IF v_route_type = 'ALL' THEN v_route_type := NULL; END IF;

  v_sheet_scope := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  IF v_sheet_scope = 'ALL' THEN v_sheet_scope := NULL; END IF;

  v_qr_status := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'qr_status', v_filters->>'qrStatus', '')), ''));
  IF v_qr_status = 'ALL' THEN v_qr_status := NULL; END IF;

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '') IS NOT NULL THEN
      v_we_from := COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom')::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_we_from := NULL;
  END;

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '') IS NOT NULL THEN
      v_we_to := COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo')::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_we_to := NULL;
  END;

  v_is_adjusted := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_adjusted', v_filters->>'isAdjusted', '')), ''));
  v_is_qr := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_qr', v_filters->>'isQr', '')), ''));
  v_needs_attention := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'needs_attention', v_filters->>'needsAttention', '')), ''));
  v_candidate_paid := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'candidate_paid', v_filters->>'candidatePaid', '')), ''));
  v_client_invoiced := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'client_invoiced', v_filters->>'clientInvoiced', '')), ''));

  v_hr_issue := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')), ''));
  IF v_hr_issue = 'ALL' THEN v_hr_issue := NULL; END IF;

  v_proc_status_raw := NULLIF(BTRIM(COALESCE(v_filters->>'processing_status', v_filters->>'processingStatus', '')), '');
  IF v_proc_status_raw IS NOT NULL AND UPPER(v_proc_status_raw) <> 'ALL' THEN
    SELECT ARRAY_AGG(status_values.status_value)
    INTO v_proc_list
    FROM (
      SELECT NULLIF(BTRIM(UPPER(split_values.value)), '') AS status_value
      FROM unnest(regexp_split_to_array(v_proc_status_raw, '\s*,\s*')) AS split_values(value)
    ) AS status_values
    WHERE status_values.status_value IS NOT NULL;
  END IF;

  v_status_code := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'status_code', v_filters->>'statusCode', '')), ''));
  IF v_status_code = 'ALL' THEN v_status_code := NULL; END IF;

  RETURN QUERY
  WITH filtered_rows AS (
    SELECT
      COALESCE(summary_row.timesheet_id, summary_row.contract_week_id) AS row_id
    FROM public.timesheet_summary_lightweight_rows_v1(v_source_filters) AS summary_row
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = summary_row.timesheet_id
     AND timesheet_row.is_current = TRUE
    WHERE

      (
        v_ids IS NULL
        OR summary_row.timesheet_id::text = ANY(v_ids)
        OR summary_row.contract_week_id::text = ANY(v_ids)
      )
      AND (v_client_id IS NULL OR summary_row.client_id = v_client_id)
      AND (v_candidate_id IS NULL OR summary_row.candidate_id = v_candidate_id)
      AND (
        v_q IS NULL
        OR COALESCE(summary_row.candidate_name, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.client_name, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.booking_id, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.occupant_key_norm, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.hospital_norm, '') ILIKE v_q_like ESCAPE '\'
      )
      AND (v_summary_stage IS NULL OR UPPER(COALESCE(summary_row.summary_stage, '')) = v_summary_stage)
      AND (v_tools_stage IS NULL OR UPPER(COALESCE(summary_row.tools_stage, '')) = v_tools_stage)
      AND (v_we_from IS NULL OR summary_row.week_ending_date >= v_we_from)
      AND (v_we_to IS NULL OR summary_row.week_ending_date <= v_we_to)
      AND (
        v_route_type IS NULL
        OR (v_route_type = 'ELECTRONIC' AND UPPER(COALESCE(summary_row.route_type, '')) IN ('DAILY_ELECTRONIC', 'WEEKLY_ELECTRONIC'))
        OR (v_route_type = 'MANUAL' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('DAILY_MANUAL', 'WEEKLY_MANUAL') OR UPPER(COALESCE(summary_row.route_family, '')) = 'MANUAL'))
        OR (v_route_type = 'NHSP' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP') OR UPPER(COALESCE(summary_row.route_family, '')) = 'NHSP'))
        OR (v_route_type = 'HEALTHROSTER' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') OR UPPER(COALESCE(summary_row.route_family, '')) = 'HEALTHROSTER'))
        OR (v_route_type = 'QR' AND COALESCE(summary_row.is_qr, FALSE) = TRUE)
        OR (v_route_type IN ('NO_TIMESHEET_REQUIRED', 'NO-TIMESHEET-REQUIRED') AND (UPPER(COALESCE(summary_row.route_type, '')) = 'NO_TIMESHEET_REQUIRED' OR UPPER(COALESCE(summary_row.route_family, '')) = 'NO_TIMESHEET_REQUIRED' OR COALESCE(summary_row.client_no_timesheet_required, FALSE) = TRUE))
        OR UPPER(COALESCE(summary_row.route_type, '')) = v_route_type
        OR UPPER(COALESCE(summary_row.route_family, '')) = v_route_type
      )
      AND (v_sheet_scope IS NULL OR UPPER(COALESCE(summary_row.sheet_scope, '')) = v_sheet_scope)
      AND (v_qr_status IS NULL OR UPPER(COALESCE(summary_row.qr_status, '')) = v_qr_status)
      AND (
        v_is_adjusted IS NULL
        OR (v_is_adjusted = 'true' AND COALESCE(summary_row.is_adjusted, FALSE) = TRUE)
        OR (v_is_adjusted = 'false' AND COALESCE(summary_row.is_adjusted, FALSE) = FALSE)
      )
      AND (
        v_is_qr IS NULL
        OR (v_is_qr = 'true' AND COALESCE(summary_row.is_qr, FALSE) = TRUE)
        OR (v_is_qr = 'false' AND COALESCE(summary_row.is_qr, FALSE) = FALSE)
      )
      AND (
        v_needs_attention IS NULL
        OR (v_needs_attention = 'true' AND COALESCE(summary_row.needs_attention, FALSE) = TRUE)
        OR (v_needs_attention = 'false' AND COALESCE(summary_row.needs_attention, FALSE) = FALSE)
      )
      AND (
        v_candidate_paid IS NULL
        OR (v_candidate_paid = 'true' AND summary_row.pay_paid_at_utc IS NOT NULL)
        OR (v_candidate_paid = 'false' AND summary_row.pay_paid_at_utc IS NULL)
      )
      AND (
        v_client_invoiced IS NULL
        OR (v_client_invoiced = 'true' AND COALESCE(summary_row.invoice_segments_locked, 0) > 0)
        OR (v_client_invoiced = 'false' AND COALESCE(summary_row.invoice_segments_locked, 0) = 0)
      )
      AND (
        v_hr_issue IS NULL
        OR EXISTS (
          SELECT 1
          FROM unnest(COALESCE(summary_row.hr_crosscheck_issues, ARRAY[]::text[])) AS hr_issue_value(issue_code)
          WHERE UPPER(COALESCE(hr_issue_value.issue_code, '')) = v_hr_issue
        )
      )
      AND (
        v_proc_list IS NULL
        OR UPPER(COALESCE(summary_row.processing_status, '')) = ANY(v_proc_list)
      )
      AND (
        v_issues_filter IS NULL
        OR (
          v_issues_filter = 'NO_MATCH_ID'
          AND (summary_row.candidate_id IS NULL OR summary_row.client_id IS NULL)
        )
        OR (
          v_issues_filter = 'RATE_MISSING'
          AND (
            COALESCE(summary_row.has_rate_issue, FALSE) = TRUE
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('RATE', 'RATE MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('PAY_CHAN_MISS', 'PAY_CHANNEL_MISSING')
          AND (
            COALESCE(summary_row.has_pay_channel_issue, FALSE) = TRUE
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('PAY CHANNEL', 'PAY CHANNEL MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('AWAITING_HR_VALIDATION', 'AWAITING_HR_VALIDATION_REQUIRED')
          AND (
            UPPER(COALESCE(summary_row.tools_stage, '')) = 'AWAITING_HR_VALIDATION'
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HR VALIDATION', 'AWAITING HR VALIDATION')
            )
          )
        )
        OR (
          v_issues_filter IN ('HR_HOURS_MISMATCH', 'HOURS_MISMATCH_HR')
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HOURS MISMATCH HR', 'HOURS MISMATCH (HEALTHROSTER)')
          )
        )
        OR (
          v_issues_filter = 'HR_HOURS_MISSING'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'HR HOURS MISSING'
          )
        )
        OR (
          v_issues_filter = 'DUPLICATE_CONTRACTS'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'DUPLICATE CONTRACTS'
          )
        )
        OR (
          v_issues_filter = 'REFERENCE_MISSING'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('REFERENCE', 'REFERENCE MISSING')
          )
        )
        OR (
          v_issues_filter = 'VALIDATION'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'VALIDATION'
          )
        )
        OR (
          v_issues_filter = 'AUTHORISATION'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('AUTHORISATION', 'AWAITING AUTHORISATION')
          )
        )
        OR (
          v_issues_filter = 'ON_HOLD'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'ON HOLD'
          )
        )
        OR (
          v_issues_filter = 'REFS_PDF_INVALID'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'REFS - TIMESHEET PDF INVALID'
          )
        )
        OR (
          v_issues_filter = 'QR_NOT_ISSUED'
          AND summary_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(summary_row.qr_status, '')) = 'PENDING'
          AND COALESCE(timesheet_row.qr_token, '') = ''
          AND timesheet_row.qr_generated_at IS NULL
        )
        OR (
          v_issues_filter IN ('QR_AWAITING_SIGNATURE', 'QR_ISSUED_AWAITING_SIGNATURE')
          AND summary_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(summary_row.qr_status, '')) = 'PENDING'
          AND COALESCE(timesheet_row.qr_token, '') <> ''
          AND timesheet_row.qr_generated_at IS NOT NULL
          AND timesheet_row.qr_scanned_at IS NULL
        )
      )
      AND (
        v_status_code IS NULL
        OR (
          v_status_code = 'NO_MATCH_ID'
          AND (summary_row.candidate_id IS NULL OR summary_row.client_id IS NULL)
        )
        OR (v_status_code = 'RATE_MISSING' AND COALESCE(summary_row.has_rate_issue, FALSE) = TRUE)
        OR (v_status_code = 'PAY_CHAN_MISS' AND COALESCE(summary_row.has_pay_channel_issue, FALSE) = TRUE)
        OR (v_status_code = 'READY_FOR_HR' AND UPPER(COALESCE(summary_row.processing_status, '')) = 'READY_FOR_HR')
        OR (v_status_code = 'READY_FOR_INV' AND UPPER(COALESCE(summary_row.processing_status, '')) = 'READY_FOR_INVOICE')
        OR UPPER(COALESCE(summary_row.processing_status, '')) = v_status_code
        OR UPPER(COALESCE(summary_row.summary_stage, '')) = v_status_code
        OR UPPER(COALESCE(summary_row.tools_stage, '')) = v_status_code
      )

  )
  SELECT DISTINCT
    filtered_rows.row_id AS id
  FROM filtered_rows
  WHERE filtered_rows.row_id IS NOT NULL
  ORDER BY filtered_rows.row_id;
END;
$function$;

-- timesheet_list_totals(jsonb)
CREATE OR REPLACE FUNCTION public.timesheet_list_totals(p_filters jsonb)
 RETURNS TABLE(count_all bigint, total_pay_ex_vat_sum numeric, margin_ex_vat_sum numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_source_filters jsonb := '{}'::jsonb;

  v_client_id uuid := NULL;
  v_candidate_id uuid := NULL;
  v_summary_stage text := NULL;
  v_tools_stage text := NULL;
  v_route_type text := NULL;
  v_sheet_scope text := NULL;
  v_qr_status text := NULL;
  v_we_from date := NULL;
  v_we_to date := NULL;
  v_is_adjusted text := NULL;
  v_is_qr text := NULL;
  v_needs_attention text := NULL;
  v_candidate_paid text := NULL;
  v_client_invoiced text := NULL;
  v_hr_issue text := NULL;
  v_proc_status_raw text := NULL;
  v_proc_list text[] := NULL;
  v_status_code text := NULL;
  v_issues_filter text := NULL;
  v_q text := NULL;
  v_q_like text := NULL;
  v_ids text[] := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_has_client_filter boolean := FALSE;
  v_has_candidate_filter boolean := FALSE;
BEGIN
  v_source_filters :=
    v_filters
    - 'q'
    - 'query'
    - 'name'
    - 'client_id'
    - 'clientId'
    - 'candidate_id'
    - 'candidateId'
    - 'week_ending_from'
    - 'weekEndingFrom'
    - 'week_ending_to'
    - 'weekEndingTo'
    - 'route_type'
    - 'routeType'
    - 'status_code'
    - 'statusCode'
    - 'summary_stage'
    - 'summaryStage'
    - 'client_invoiced'
    - 'clientInvoiced'
    - 'needs_attention'
    - 'needsAttention'
    || jsonb_build_object('disable_paging', TRUE, 'purpose', 'totals');

  v_has_client_filter := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '') IS NOT NULL;
  IF v_has_client_filter THEN
    IF COALESCE(v_filters->>'client_id', v_filters->>'clientId') ~* v_uuid_re THEN
      v_client_id := COALESCE(v_filters->>'client_id', v_filters->>'clientId')::uuid;
    ELSE
      RETURN;
    END IF;
  END IF;

  v_has_candidate_filter := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '') IS NOT NULL;
  IF v_has_candidate_filter THEN
    IF COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId') ~* v_uuid_re THEN
      v_candidate_id := COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId')::uuid;
    ELSE
      RETURN;
    END IF;
  END IF;

  v_q := NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'query', v_filters->>'name', '')), '');
  IF v_q IS NOT NULL THEN
    v_q_like := '%' || REPLACE(REPLACE(REPLACE(v_q, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%';
  END IF;

  BEGIN
    IF v_filters ? 'ids' THEN
      IF jsonb_typeof(v_filters->'ids') = 'array' THEN
        SELECT ARRAY_AGG(id_values.id_value)
        INTO v_ids
        FROM (
          SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS id_value
          FROM jsonb_array_elements_text(v_filters->'ids') AS input_values(value)
        ) AS id_values
        WHERE id_values.id_value IS NOT NULL;
      ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL THEN
        SELECT ARRAY_AGG(id_values.id_value)
        INTO v_ids
        FROM (
          SELECT DISTINCT NULLIF(BTRIM(split_values.value), '') AS id_value
          FROM unnest(regexp_split_to_array(v_filters->>'ids', '\s*,\s*')) AS split_values(value)
        ) AS id_values
        WHERE id_values.id_value IS NOT NULL;
      END IF;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_ids := NULL;
  END;

  v_summary_stage := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'summary_stage', v_filters->>'summaryStage', '')), ''));
  IF v_summary_stage = 'ALL' THEN v_summary_stage := NULL; END IF;

  v_tools_stage := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'tools_stage', v_filters->>'toolsStage', '')), ''));
  IF v_tools_stage = 'ALL' THEN v_tools_stage := NULL; END IF;

  v_issues_filter := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'issues_filter', v_filters->>'issuesFilter', '')), ''));
  IF v_issues_filter = 'ALL' THEN v_issues_filter := NULL; END IF;
  -- The row source is the single issue-classification authority. It receives
  -- the original issues filter above, so totals must not reclassify it.
  v_issues_filter := NULL;

  v_route_type := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'route_type', v_filters->>'routeType', '')), ''));
  IF v_route_type = 'ALL' THEN v_route_type := NULL; END IF;

  v_sheet_scope := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  IF v_sheet_scope = 'ALL' THEN v_sheet_scope := NULL; END IF;

  v_qr_status := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'qr_status', v_filters->>'qrStatus', '')), ''));
  IF v_qr_status = 'ALL' THEN v_qr_status := NULL; END IF;

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '') IS NOT NULL THEN
      v_we_from := COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom')::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_we_from := NULL;
  END;

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '') IS NOT NULL THEN
      v_we_to := COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo')::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_we_to := NULL;
  END;

  v_is_adjusted := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_adjusted', v_filters->>'isAdjusted', '')), ''));
  v_is_qr := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_qr', v_filters->>'isQr', '')), ''));
  v_needs_attention := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'needs_attention', v_filters->>'needsAttention', '')), ''));
  v_candidate_paid := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'candidate_paid', v_filters->>'candidatePaid', '')), ''));
  v_client_invoiced := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'client_invoiced', v_filters->>'clientInvoiced', '')), ''));

  v_hr_issue := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')), ''));
  IF v_hr_issue = 'ALL' THEN v_hr_issue := NULL; END IF;

  v_proc_status_raw := NULLIF(BTRIM(COALESCE(v_filters->>'processing_status', v_filters->>'processingStatus', '')), '');
  IF v_proc_status_raw IS NOT NULL AND UPPER(v_proc_status_raw) <> 'ALL' THEN
    SELECT ARRAY_AGG(status_values.status_value)
    INTO v_proc_list
    FROM (
      SELECT NULLIF(BTRIM(UPPER(split_values.value)), '') AS status_value
      FROM unnest(regexp_split_to_array(v_proc_status_raw, '\s*,\s*')) AS split_values(value)
    ) AS status_values
    WHERE status_values.status_value IS NOT NULL;
  END IF;

  v_status_code := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'status_code', v_filters->>'statusCode', '')), ''));
  IF v_status_code = 'ALL' THEN v_status_code := NULL; END IF;

  RETURN QUERY
  WITH filtered_rows AS (
    SELECT
      summary_row.timesheet_id,
      summary_row.contract_week_id,
      COALESCE(summary_row.total_pay_ex_vat, 0::numeric) AS total_pay_ex_vat,
      COALESCE(summary_row.margin_ex_vat, 0::numeric) AS margin_ex_vat
    FROM public.timesheet_summary_lightweight_rows_v1(v_source_filters) AS summary_row
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = summary_row.timesheet_id
     AND timesheet_row.is_current = TRUE
    WHERE

      (
        v_ids IS NULL
        OR summary_row.timesheet_id::text = ANY(v_ids)
        OR summary_row.contract_week_id::text = ANY(v_ids)
      )
      AND (v_client_id IS NULL OR summary_row.client_id = v_client_id)
      AND (v_candidate_id IS NULL OR summary_row.candidate_id = v_candidate_id)
      AND (
        v_q IS NULL
        OR COALESCE(summary_row.candidate_name, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.client_name, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.booking_id, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.occupant_key_norm, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.hospital_norm, '') ILIKE v_q_like ESCAPE '\'
      )
      AND (v_summary_stage IS NULL OR UPPER(COALESCE(summary_row.summary_stage, '')) = v_summary_stage)
      AND (v_tools_stage IS NULL OR UPPER(COALESCE(summary_row.tools_stage, '')) = v_tools_stage)
      AND (v_we_from IS NULL OR summary_row.week_ending_date >= v_we_from)
      AND (v_we_to IS NULL OR summary_row.week_ending_date <= v_we_to)
      AND (
        v_route_type IS NULL
        OR (v_route_type = 'ELECTRONIC' AND UPPER(COALESCE(summary_row.route_type, '')) IN ('DAILY_ELECTRONIC', 'WEEKLY_ELECTRONIC'))
        OR (v_route_type = 'MANUAL' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('DAILY_MANUAL', 'WEEKLY_MANUAL') OR UPPER(COALESCE(summary_row.route_family, '')) = 'MANUAL'))
        OR (v_route_type = 'NHSP' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP') OR UPPER(COALESCE(summary_row.route_family, '')) = 'NHSP'))
        OR (v_route_type = 'HEALTHROSTER' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') OR UPPER(COALESCE(summary_row.route_family, '')) = 'HEALTHROSTER'))
        OR (v_route_type = 'QR' AND COALESCE(summary_row.is_qr, FALSE) = TRUE)
        OR (v_route_type IN ('NO_TIMESHEET_REQUIRED', 'NO-TIMESHEET-REQUIRED') AND (UPPER(COALESCE(summary_row.route_type, '')) = 'NO_TIMESHEET_REQUIRED' OR UPPER(COALESCE(summary_row.route_family, '')) = 'NO_TIMESHEET_REQUIRED' OR COALESCE(summary_row.client_no_timesheet_required, FALSE) = TRUE))
        OR UPPER(COALESCE(summary_row.route_type, '')) = v_route_type
        OR UPPER(COALESCE(summary_row.route_family, '')) = v_route_type
      )
      AND (v_sheet_scope IS NULL OR UPPER(COALESCE(summary_row.sheet_scope, '')) = v_sheet_scope)
      AND (v_qr_status IS NULL OR UPPER(COALESCE(summary_row.qr_status, '')) = v_qr_status)
      AND (
        v_is_adjusted IS NULL
        OR (v_is_adjusted = 'true' AND COALESCE(summary_row.is_adjusted, FALSE) = TRUE)
        OR (v_is_adjusted = 'false' AND COALESCE(summary_row.is_adjusted, FALSE) = FALSE)
      )
      AND (
        v_is_qr IS NULL
        OR (v_is_qr = 'true' AND COALESCE(summary_row.is_qr, FALSE) = TRUE)
        OR (v_is_qr = 'false' AND COALESCE(summary_row.is_qr, FALSE) = FALSE)
      )
      AND (
        v_needs_attention IS NULL
        OR (v_needs_attention = 'true' AND COALESCE(summary_row.needs_attention, FALSE) = TRUE)
        OR (v_needs_attention = 'false' AND COALESCE(summary_row.needs_attention, FALSE) = FALSE)
      )
      AND (
        v_candidate_paid IS NULL
        OR (v_candidate_paid = 'true' AND summary_row.pay_paid_at_utc IS NOT NULL)
        OR (v_candidate_paid = 'false' AND summary_row.pay_paid_at_utc IS NULL)
      )
      AND (
        v_client_invoiced IS NULL
        OR (v_client_invoiced = 'true' AND COALESCE(summary_row.invoice_segments_locked, 0) > 0)
        OR (v_client_invoiced = 'false' AND COALESCE(summary_row.invoice_segments_locked, 0) = 0)
      )
      AND (
        v_hr_issue IS NULL
        OR EXISTS (
          SELECT 1
          FROM unnest(COALESCE(summary_row.hr_crosscheck_issues, ARRAY[]::text[])) AS hr_issue_value(issue_code)
          WHERE UPPER(COALESCE(hr_issue_value.issue_code, '')) = v_hr_issue
        )
      )
      AND (
        v_proc_list IS NULL
        OR UPPER(COALESCE(summary_row.processing_status, '')) = ANY(v_proc_list)
      )
      AND (
        v_issues_filter IS NULL
        OR (
          v_issues_filter = 'NO_MATCH_ID'
          AND (summary_row.candidate_id IS NULL OR summary_row.client_id IS NULL)
        )
        OR (
          v_issues_filter = 'RATE_MISSING'
          AND (
            COALESCE(summary_row.has_rate_issue, FALSE) = TRUE
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('RATE', 'RATE MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('PAY_CHAN_MISS', 'PAY_CHANNEL_MISSING')
          AND (
            COALESCE(summary_row.has_pay_channel_issue, FALSE) = TRUE
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('PAY CHANNEL', 'PAY CHANNEL MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('AWAITING_HR_VALIDATION', 'AWAITING_HR_VALIDATION_REQUIRED')
          AND (
            UPPER(COALESCE(summary_row.tools_stage, '')) = 'AWAITING_HR_VALIDATION'
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HR VALIDATION', 'AWAITING HR VALIDATION')
            )
          )
        )
        OR (
          v_issues_filter IN ('HR_HOURS_MISMATCH', 'HOURS_MISMATCH_HR')
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HOURS MISMATCH HR', 'HOURS MISMATCH (HEALTHROSTER)')
          )
        )
        OR (
          v_issues_filter = 'HR_HOURS_MISSING'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'HR HOURS MISSING'
          )
        )
        OR (
          v_issues_filter = 'DUPLICATE_CONTRACTS'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'DUPLICATE CONTRACTS'
          )
        )
        OR (
          v_issues_filter = 'REFERENCE_MISSING'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('REFERENCE', 'REFERENCE MISSING')
          )
        )
        OR (
          v_issues_filter = 'VALIDATION'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'VALIDATION'
          )
        )
        OR (
          v_issues_filter = 'AUTHORISATION'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('AUTHORISATION', 'AWAITING AUTHORISATION')
          )
        )
        OR (
          v_issues_filter = 'ON_HOLD'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'ON HOLD'
          )
        )
        OR (
          v_issues_filter = 'REFS_PDF_INVALID'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'REFS - TIMESHEET PDF INVALID'
          )
        )
        OR (
          v_issues_filter = 'QR_NOT_ISSUED'
          AND summary_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(summary_row.qr_status, '')) = 'PENDING'
          AND COALESCE(timesheet_row.qr_token, '') = ''
          AND timesheet_row.qr_generated_at IS NULL
        )
        OR (
          v_issues_filter IN ('QR_AWAITING_SIGNATURE', 'QR_ISSUED_AWAITING_SIGNATURE')
          AND summary_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(summary_row.qr_status, '')) = 'PENDING'
          AND COALESCE(timesheet_row.qr_token, '') <> ''
          AND timesheet_row.qr_generated_at IS NOT NULL
          AND timesheet_row.qr_scanned_at IS NULL
        )
      )
      AND (
        v_status_code IS NULL
        OR (
          v_status_code = 'NO_MATCH_ID'
          AND (summary_row.candidate_id IS NULL OR summary_row.client_id IS NULL)
        )
        OR (v_status_code = 'RATE_MISSING' AND COALESCE(summary_row.has_rate_issue, FALSE) = TRUE)
        OR (v_status_code = 'PAY_CHAN_MISS' AND COALESCE(summary_row.has_pay_channel_issue, FALSE) = TRUE)
        OR (v_status_code = 'READY_FOR_HR' AND UPPER(COALESCE(summary_row.processing_status, '')) = 'READY_FOR_HR')
        OR (v_status_code = 'READY_FOR_INV' AND UPPER(COALESCE(summary_row.processing_status, '')) = 'READY_FOR_INVOICE')
        OR UPPER(COALESCE(summary_row.processing_status, '')) = v_status_code
        OR UPPER(COALESCE(summary_row.summary_stage, '')) = v_status_code
        OR UPPER(COALESCE(summary_row.tools_stage, '')) = v_status_code
      )

  )
  SELECT
    COUNT(*)::bigint AS count_all,
    COALESCE(SUM(filtered_rows.total_pay_ex_vat), 0::numeric) AS total_pay_ex_vat_sum,
    COALESCE(SUM(filtered_rows.margin_ex_vat), 0::numeric) AS margin_ex_vat_sum
  FROM filtered_rows;
END;
$function$;

-- timesheet_paid_uninvoiced_rollover_v1(uuid,uuid,uuid,uuid,text,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.timesheet_paid_uninvoiced_rollover_v1(p_timesheet_id uuid, p_actor_user_id uuid, p_operation_id uuid, p_expected_current_tsfin_id uuid, p_expected_preflight_fingerprint text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_expected_preflight_fingerprint text :=
    NULLIF(BTRIM(COALESCE(p_expected_preflight_fingerprint, '')), '');

  v_operation public.import_apply_operations%ROWTYPE;
  v_timesheet public.timesheets%ROWTYPE;
  v_old_tsfin public.timesheets_financials%ROWTYPE;
  v_existing_new_tsfin public.timesheets_financials%ROWTYPE;
  v_new_tsfin public.timesheets_financials%ROWTYPE;

  v_chain jsonb;
  v_preflight jsonb;
  v_root_timesheet_id uuid;
  v_correction_financials_policy_envelope jsonb;
  v_correction_financials_policy_envelope_fingerprint text;
  v_actual_policy_envelope_fingerprint text;
  v_replacement_policy jsonb;
  v_operation_unit_count integer := 0;
  v_operation_contract jsonb;
  v_operation_contract_fingerprint text;
  v_operation_unit jsonb;
  v_request_unit jsonb;
  v_request_unit_count integer := 0;
  v_route text;
  v_is_ordinary_source boolean := false;
  v_replay boolean := false;
  v_old_paid_digest text;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_TIMESHEET_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_ACTOR_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_OPERATION_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_expected_current_tsfin_id IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_EXPECTED_TSFIN_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF v_expected_preflight_fingerprint IS NULL
     OR char_length(v_expected_preflight_fingerprint) > 256 THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_PREFLIGHT_FINGERPRINT_INVALID'
      USING ERRCODE = '22023';
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.import_apply_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0002',
            DETAIL = jsonb_build_object(
              'operation_id', p_operation_id::text
            )::text;
  END IF;

  IF v_operation.actor_user_id IS DISTINCT FROM p_actor_user_id THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_ACTOR_MISMATCH'
      USING ERRCODE = '42501',
            DETAIL = jsonb_build_object(
              'operation_id', p_operation_id::text,
              'expected_actor_user_id', v_operation.actor_user_id::text,
              'supplied_actor_user_id', p_actor_user_id::text
            )::text;
  END IF;

  IF v_operation.state <> 'PREPARED' THEN
    IF v_operation.state IN (
      'SOURCE_COMMITTED_TSFIN_PENDING',
      'FINANCIALISED_PENDING_FINALISATION',
      'COMPLETE'
    ) THEN
      SELECT current_financial.*
      INTO v_existing_new_tsfin
      FROM public.timesheets_financials AS current_financial
      WHERE current_financial.timesheet_id = p_timesheet_id
        AND current_financial.is_current = true
        AND current_financial.id <> p_expected_current_tsfin_id
      ORDER BY current_financial.computed_at_utc DESC, current_financial.id DESC
      LIMIT 1;

      IF FOUND THEN
        RETURN jsonb_build_object(
          'ok', true,
          'replay', true,
          'operation_id', p_operation_id::text,
          'timesheet_id', p_timesheet_id::text,
          'historical_paid_tsfin_id', p_expected_current_tsfin_id::text,
          'new_current_tsfin_id', v_existing_new_tsfin.id::text,
          'new_current_processing_status',
            v_existing_new_tsfin.processing_status::text,
          'operation_state', v_operation.state
        );
      END IF;
    END IF;

    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_OPERATION_STATE_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'operation_id', p_operation_id::text,
              'state', v_operation.state,
              'required_state', 'PREPARED'
            )::text;
  END IF;

  select count(*)::integer,min(request_unit::text)::jsonb
  into v_request_unit_count,v_request_unit
  from jsonb_array_elements(coalesce(
    v_operation.response_json#>'{request_envelope,reconciliation_units}','[]'::jsonb
  )) request_unit
  where request_unit->>'source_timesheet_id'=p_timesheet_id::text
     or coalesce(request_unit->'M_active_member_ids','[]'::jsonb) @> jsonb_build_array(p_timesheet_id);
  if v_request_unit_count<>1
     or v_request_unit->>'source_system' not in ('NHSP','HEALTHROSTER')
     or nullif(v_request_unit->>'action_id','') is null
     or nullif(v_request_unit->>'source_identity','') is null then
    raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_OPERATION_UNIT_INVALID'
      using errcode='P0001',detail=jsonb_build_object(
        'operation_id',p_operation_id,'timesheet_id',p_timesheet_id,
        'matching_unit_count',v_request_unit_count
      )::text;
  end if;
  v_route:=v_request_unit->>'route';
  v_is_ordinary_source:=v_route='AMEND_PAID_UNINVOICED_SOURCE';

  if v_is_ordinary_source then
    if jsonb_array_length(coalesce(v_request_unit->'B_effective_invoice_ids','[]'::jsonb))<>0
       or jsonb_array_length(coalesce(v_request_unit->'B_effective_invoice_line_ids','[]'::jsonb))<>0
       or coalesce((v_request_unit#>>'{B_hours,total_hours}')::numeric,0)<>0 then
      raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_OPERATION_UNIT_INVALID'
        using errcode='P0001';
    end if;
    v_root_timesheet_id:=p_timesheet_id;
    v_chain:=jsonb_build_object('valid',true,'root_timesheet_id',p_timesheet_id,
      'ordinary_source',true);
  else
    v_chain := public.timesheet_correction_chain_scope_v1(
      p_timesheet_id,
      true,
      32,
      100
    );

    IF COALESCE((v_chain ->> 'valid')::boolean, false) IS NOT TRUE THEN
      RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_CHAIN_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'chain', v_chain
              )::text;
    END IF;
    v_root_timesheet_id :=NULLIF(v_chain ->> 'root_timesheet_id', '')::uuid;
  end if;
  v_operation_contract:=v_operation.response_json#>'{correction_operation_contract}';
  if jsonb_typeof(v_operation_contract)<>'object'
     or v_operation_contract->>'schema_version'<>'IMPORT_CORRECTION_OPERATION_V2'
     or v_operation_contract->>'operation_id' is distinct from p_operation_id::text then
    raise exception 'PAID_TSFIN_ROLLOVER_OPERATION_CONTRACT_INVALID' using errcode='P0001';
  end if;
  v_operation_contract_fingerprint:=encode(extensions.digest(
    convert_to((v_operation_contract-'operation_contract_fingerprint')::text,'UTF8'),
    'sha256'::text
  ),'hex');
  if v_operation_contract->>'operation_contract_fingerprint'
     is distinct from v_operation_contract_fingerprint then
    raise exception 'PAID_TSFIN_ROLLOVER_OPERATION_CONTRACT_FINGERPRINT_INVALID'
      using errcode='P0001';
  end if;
  -- The paid source row predates the correction member, so read the one exact
  -- frozen unit from the durable operation contract rather than trusting a
  -- caller-supplied or obsolete top-level response field.
  select count(*)::integer,min(unit::text)::jsonb
  into v_operation_unit_count,v_operation_unit
  from jsonb_array_elements(
    case when jsonb_typeof(v_operation_contract->'correction_units')='array'
      then v_operation_contract->'correction_units'
      else '[]'::jsonb end
  ) unit
  where unit->>'action_id'=v_request_unit->>'action_id'
    and unit->>'root_timesheet_id'=v_root_timesheet_id::text;
  if v_operation_unit_count<>1 then
    raise exception 'PAID_TSFIN_ROLLOVER_OPERATION_UNIT_NOT_UNIQUE'
      using errcode='P0001',detail=jsonb_build_object(
        'operation_id',p_operation_id,'root_timesheet_id',v_root_timesheet_id,
        'matching_unit_count',v_operation_unit_count
      )::text;
  end if;
  if v_operation_unit->>'source_row_key' is distinct from v_request_unit->>'source_identity'
     or nullif(v_operation_unit->>'source_shift_id','') is distinct from nullif(v_request_unit->>'source_shift_id','')
     or v_operation_contract->>'source_system' is distinct from v_request_unit->>'source_system'
     or v_operation_unit#>>'{policy_envelope,classification,source_system}' is distinct from v_request_unit->>'source_system' then
    raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_POLICY_INVALID' using errcode='P0001';
  end if;
  v_correction_financials_policy_envelope:=v_operation_unit->'policy_envelope';
  v_correction_financials_policy_envelope_fingerprint := NULLIF(
    v_correction_financials_policy_envelope ->> 'envelope_fingerprint', ''
  );
  v_replacement_policy := case
    when v_correction_financials_policy_envelope->>'correction_shape'='REVERSAL_ONLY'
      then v_correction_financials_policy_envelope->'reversal'
    else v_correction_financials_policy_envelope->'replacement' end;

  IF jsonb_typeof(v_correction_financials_policy_envelope) <> 'object'
     OR v_correction_financials_policy_envelope_fingerprint IS NULL
     OR v_correction_financials_policy_envelope#>>'{operation,operation_id}'
        IS DISTINCT FROM p_operation_id::text
     OR v_operation_unit->>'policy_envelope_fingerprint'
        IS DISTINCT FROM v_correction_financials_policy_envelope_fingerprint
     OR coalesce((v_replacement_policy->>'applicable')::boolean,false) IS NOT TRUE THEN
    RAISE EXCEPTION '%',case when v_is_ordinary_source
        then 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_POLICY_INVALID'
        else 'PAID_TSFIN_ROLLOVER_POLICY_ENVELOPE_REQUIRED' end
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'root_timesheet_id', v_root_timesheet_id::text,
              'chain_errors', COALESCE(v_chain -> 'errors', '[]'::jsonb)
            )::text;
  END IF;

  v_actual_policy_envelope_fingerprint := encode(
    extensions.digest(
      convert_to(
        (v_correction_financials_policy_envelope - 'envelope_fingerprint')::text,
        'UTF8'
      ),
      'sha256'::text
    ),
    'hex'
  );

  IF v_actual_policy_envelope_fingerprint
       IS DISTINCT FROM v_correction_financials_policy_envelope_fingerprint THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_POLICY_ENVELOPE_FINGERPRINT_INVALID'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'stored_fingerprint',
                v_correction_financials_policy_envelope_fingerprint,
              'actual_fingerprint', v_actual_policy_envelope_fingerprint
            )::text;
  END IF;

  SELECT timesheet_row.*
  INTO v_timesheet
  FROM public.timesheets AS timesheet_row
  WHERE timesheet_row.timesheet_id = p_timesheet_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_TIMESHEET_NOT_FOUND'
      USING ERRCODE = 'P0002';
  END IF;

  IF COALESCE(v_timesheet.is_current, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_TIMESHEET_NOT_CURRENT'
      USING ERRCODE = 'P0001';
  END IF;

  IF v_timesheet.authorised_at_server IS NOT NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_REQUIRES_UNAUTHORISED_TIMESHEET'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'timesheet_id', p_timesheet_id::text,
              'authorised_at_server', v_timesheet.authorised_at_server
            )::text;
  END IF;

  SELECT financial_row.*
  INTO v_old_tsfin
  FROM public.timesheets_financials AS financial_row
  WHERE financial_row.id = p_expected_current_tsfin_id
    AND financial_row.timesheet_id = p_timesheet_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_EXPECTED_TSFIN_NOT_FOUND'
      USING ERRCODE = 'P0002';
  END IF;

  IF COALESCE(v_old_tsfin.is_current, false) IS NOT TRUE THEN
    SELECT current_financial.*
    INTO v_existing_new_tsfin
    FROM public.timesheets_financials AS current_financial
    WHERE current_financial.timesheet_id = p_timesheet_id
      AND current_financial.is_current = true
      AND current_financial.id <> v_old_tsfin.id
    ORDER BY current_financial.computed_at_utc DESC, current_financial.id DESC
    LIMIT 1;

    IF FOUND THEN
      RETURN jsonb_build_object(
        'ok', true,
        'replay', true,
        'operation_id', p_operation_id::text,
        'timesheet_id', p_timesheet_id::text,
        'historical_paid_tsfin_id', v_old_tsfin.id::text,
        'new_current_tsfin_id', v_existing_new_tsfin.id::text,
        'new_current_processing_status',
          v_existing_new_tsfin.processing_status::text,
        'operation_state', v_operation.state
      );
    END IF;

    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_EXPECTED_TSFIN_NOT_CURRENT'
      USING ERRCODE = 'P0001';
  END IF;

  IF v_old_tsfin.authorised_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_REQUIRES_UNAUTHORISED_TSFIN'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'timesheet_id', p_timesheet_id::text,
              'tsfin_id', v_old_tsfin.id::text,
              'authorised_at_utc', v_old_tsfin.authorised_at_utc
            )::text;
  END IF;

  IF v_old_tsfin.paid_at_utc IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_PAID_EVIDENCE_REQUIRED'
      USING ERRCODE = 'P0001';
  END IF;

  IF v_old_tsfin.locked_by_invoice_id IS NOT NULL
     OR EXISTS (
       SELECT 1
       FROM public.invoice_lines AS invoice_line
       WHERE invoice_line.timesheet_id = p_timesheet_id
     ) THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_INVOICE_EVIDENCE_BLOCKS'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'timesheet_id', p_timesheet_id::text,
              'locked_by_invoice_id', CASE
                WHEN v_old_tsfin.locked_by_invoice_id IS NULL THEN NULL
                ELSE v_old_tsfin.locked_by_invoice_id::text
              END
            )::text;
  END IF;

  v_preflight := public.import_timesheet_financial_preflight_v1(
    ARRAY[p_timesheet_id]::uuid[],
    'PAID_UNINVOICED_ROLLOVER',
    p_actor_user_id,
    case when v_is_ordinary_source then '{}'::jsonb else jsonb_build_object(
      'chain_fingerprints', jsonb_build_object(
        v_root_timesheet_id::text,
        v_chain ->> 'chain_fingerprint'
      ),
      'correction_financials_policy_envelope_fingerprints', jsonb_build_object(
        v_root_timesheet_id::text,
        v_correction_financials_policy_envelope_fingerprint
      )
    ) end,
    false,
    100
  );

  IF COALESCE((v_preflight ->> 'allowed')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION '%',case when v_is_ordinary_source
        then 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_PREFLIGHT_INVALID'
        else 'PAID_TSFIN_ROLLOVER_PREFLIGHT_BLOCKED' end
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'preflight', v_preflight
            )::text;
  END IF;

  IF v_preflight ->> 'preflight_fingerprint'
       IS DISTINCT FROM v_expected_preflight_fingerprint THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_PREFLIGHT_STALE'
      USING ERRCODE = '40001',
            DETAIL = jsonb_build_object(
              'expected_preflight_fingerprint',
                v_expected_preflight_fingerprint,
              'actual_preflight_fingerprint',
                v_preflight ->> 'preflight_fingerprint'
            )::text;
  END IF;

  if v_is_ordinary_source and (
       v_preflight->>'required_path' is distinct from 'PAID_UNINVOICED_ROLLOVER'
       or coalesce((v_preflight->>'input_count')::integer,0)<>1
       or coalesce((v_preflight->>'member_count')::integer,0)<>1
       or coalesce((v_preflight->>'paid_count')::integer,0)<>1
       or coalesce((v_preflight->>'invoice_lined_count')::integer,0)<>0
       or coalesce((v_preflight->>'blocking_batch_count')::integer,0)<>0
       or coalesce((v_preflight->>'stale_tsfin_count')::integer,0)<>0
       or jsonb_array_length(coalesce(v_preflight->'errors','[]'::jsonb))<>0
       or (select count(*) from jsonb_array_elements(coalesce(v_preflight->'members','[]'::jsonb)) member
           where member->>'timesheet_id'=p_timesheet_id::text
             and member->>'current_tsfin_id'=p_expected_current_tsfin_id::text
             and coalesce((member->>'paid')::boolean,false)
             and not coalesce((member->>'invoice_lined')::boolean,false))<>1
     ) then
    raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_PREFLIGHT_INVALID' using errcode='P0001';
  end if;

  v_old_paid_digest := encode(
    extensions.digest(
      convert_to(
        jsonb_build_object(
          'id', v_old_tsfin.id::text,
          'timesheet_id', v_old_tsfin.timesheet_id::text,
          'timesheet_version', v_old_tsfin.timesheet_version,
          'paid_at_utc', v_old_tsfin.paid_at_utc,
          'paid_by_user_id', CASE
            WHEN v_old_tsfin.paid_by_user_id IS NULL THEN NULL
            ELSE v_old_tsfin.paid_by_user_id::text
          END,
          'payment_reference', v_old_tsfin.payment_reference,
          'total_hours', v_old_tsfin.total_hours,
          'total_pay_ex_vat', v_old_tsfin.total_pay_ex_vat,
          'total_charge_ex_vat', v_old_tsfin.total_charge_ex_vat,
          'pay_vat_rate_pct_snapshot',
            v_old_tsfin.pay_vat_rate_pct_snapshot,
          'pay_vat_amount_snapshot',
            v_old_tsfin.pay_vat_amount_snapshot,
          'pay_total_inc_vat_snapshot',
            v_old_tsfin.pay_total_inc_vat_snapshot,
          'policy_snapshot_json', v_old_tsfin.policy_snapshot_json,
          'rate_source_refs_json', v_old_tsfin.rate_source_refs_json,
          'actual_schedule_json', v_old_tsfin.actual_schedule_json
        )::text,
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );

  UPDATE public.timesheets_financials AS historical_financial
  SET is_current = false
  WHERE historical_financial.id = v_old_tsfin.id
    AND historical_financial.is_current = true;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_CONCURRENT_CURRENT_CHANGE'
      USING ERRCODE = '40001';
  END IF;

  INSERT INTO public.timesheets_financials (
    timesheet_id,
    timesheet_version,
    basis,
    is_current,
    is_stale,
    stale_reason,
    candidate_id,
    client_id,
    role,
    band,
    pay_method,
    policy_snapshot_json,
    rate_source_refs_json,
    computed_at_utc,
    created_at,
    updated_at,
    occupant_key_norm,
    candidate_assignment,
    processing_status,
    processed_by_user_id,
    processed_at_utc,
    po_number,
    pay_on_hold,
    pay_on_hold_reason,
    pay_on_hold_since_utc,
    expenses_pay_ex_vat,
    expenses_charge_ex_vat,
    expenses_description,
    expenses_evidence_r2_key,
    expenses_evidence_manifest,
    mileage_units,
    mileage_pay_ex_vat,
    mileage_charge_ex_vat,
    mileage_pay_rate,
    mileage_charge_rate,
    mileage_evidence_r2_key,
    mileage_evidence_manifest,
    travel_pay_ex_vat,
    travel_charge_ex_vat,
    accommodation_pay_ex_vat,
    accommodation_charge_ex_vat,
    other_pay_ex_vat,
    other_charge_ex_vat,
    hr_crosscheck_status,
    hr_crosscheck_issues,
    external_source_rows_json,
    actual_schedule_json,
    additional_units_json,
    invoice_breakdown_json,
    nhsp_import_id,
    has_rate_issue,
    has_pay_channel_issue
  )
  VALUES (
    p_timesheet_id,
    v_timesheet.version,
    v_old_tsfin.basis,
    true,
    true,
    'IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION',
    v_old_tsfin.candidate_id,
    v_old_tsfin.client_id,
    v_old_tsfin.role,
    v_old_tsfin.band,
    v_old_tsfin.pay_method,
    jsonb_build_object(
      'import_apply_operation_id', p_operation_id::text,
      'import_authoritative_route',v_route,
      'import_authoritative_source_system',v_request_unit->>'source_system',
      'import_authoritative_source_identity',v_request_unit->>'source_identity',
      'import_authoritative_unit_fingerprint',v_request_unit->>'unit_fingerprint',
      'rollover_source_tsfin_id', v_old_tsfin.id::text,
      'rollover_source_paid_digest', v_old_paid_digest,
      'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
      'correction_financials_policy_envelope_fingerprint',
        v_correction_financials_policy_envelope_fingerprint,
      'requires_frozen_correction_policy', true,
      'correction_finance_override_fields', jsonb_build_array(
        'erni_pct',
        'apply_erni_to',
        'vat_rate_pct'
      ),
      'erni_pct', v_replacement_policy #> '{tsfin_policy,erni_pct}',
      'apply_erni_to',
        v_replacement_policy #>> '{tsfin_policy,apply_erni_to}',
      'vat_rate_pct',
        v_replacement_policy #> '{tsfin_policy,applied_pay_vat_rate_pct}',
      'pay_vat_rate_pct',
        v_replacement_policy #> '{tsfin_policy,applied_pay_vat_rate_pct}',
      'correction_leg_fingerprint',
        v_replacement_policy ->> 'leg_fingerprint',
      'correction_tsfin_policy',
        v_replacement_policy -> 'tsfin_policy',
      'correction_tsfin_policy_fingerprint',
        v_replacement_policy #>> '{tsfin_policy,tsfin_policy_fingerprint}',
      'correction_invoice_policy',
        v_replacement_policy -> 'invoice_policy',
      'correction_invoice_policy_fingerprint',
        v_replacement_policy #>> '{invoice_policy,invoice_policy_fingerprint}',
      'correction_invoice_stream',
        v_replacement_policy #>> '{invoice_policy,invoice_stream}'
    ),
    COALESCE(v_old_tsfin.rate_source_refs_json, '{}'::jsonb)
      || jsonb_build_object(
        'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
        'correction_financials_policy_envelope_fingerprint',
          v_correction_financials_policy_envelope_fingerprint,
        'rollover_source_tsfin_id', v_old_tsfin.id::text
      ),
    v_now,
    v_now,
    v_now,
    v_old_tsfin.occupant_key_norm,
    v_old_tsfin.candidate_assignment,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum,
    v_old_tsfin.processed_by_user_id,
    v_old_tsfin.processed_at_utc,
    v_old_tsfin.po_number,
    v_old_tsfin.pay_on_hold,
    v_old_tsfin.pay_on_hold_reason,
    v_old_tsfin.pay_on_hold_since_utc,
    v_old_tsfin.expenses_pay_ex_vat,
    v_old_tsfin.expenses_charge_ex_vat,
    v_old_tsfin.expenses_description,
    v_old_tsfin.expenses_evidence_r2_key,
    v_old_tsfin.expenses_evidence_manifest,
    v_old_tsfin.mileage_units,
    v_old_tsfin.mileage_pay_ex_vat,
    v_old_tsfin.mileage_charge_ex_vat,
    v_old_tsfin.mileage_pay_rate,
    v_old_tsfin.mileage_charge_rate,
    v_old_tsfin.mileage_evidence_r2_key,
    v_old_tsfin.mileage_evidence_manifest,
    v_old_tsfin.travel_pay_ex_vat,
    v_old_tsfin.travel_charge_ex_vat,
    v_old_tsfin.accommodation_pay_ex_vat,
    v_old_tsfin.accommodation_charge_ex_vat,
    v_old_tsfin.other_pay_ex_vat,
    v_old_tsfin.other_charge_ex_vat,
    v_old_tsfin.hr_crosscheck_status,
    v_old_tsfin.hr_crosscheck_issues,
    v_old_tsfin.external_source_rows_json,
    COALESCE(v_timesheet.actual_schedule_json, '[]'::jsonb),
    COALESCE(
      jsonb_build_object(
        'week', COALESCE(v_timesheet.additional_units_week, '{}'::jsonb),
        'per_day', COALESCE(
          v_timesheet.additional_units_per_day,
          '{}'::jsonb
        )
      ),
      '{}'::jsonb
    ),
    '{}'::jsonb,
    v_old_tsfin.nhsp_import_id,
    false,
    false
  )
  RETURNING *
  INTO v_new_tsfin;

  PERFORM public._inv_write_audit(
    p_actor_user_id,
    'IMPORT_PAID_TSFIN_ROLLED',
    jsonb_build_object(
      'operation_id', p_operation_id::text,
      'timesheet_id', p_timesheet_id::text,
      'historical_paid_tsfin_id', v_old_tsfin.id::text,
      'historical_paid_digest', v_old_paid_digest,
      'new_current_tsfin_id', v_new_tsfin.id::text,
      'new_current_processing_status',
        v_new_tsfin.processing_status::text,
      'correction_financials_policy_envelope_fingerprint',
        v_correction_financials_policy_envelope_fingerprint,
      'import_authoritative_route',v_route
    ),
    'timesheet_financials',
    v_new_tsfin.id::text,
    jsonb_build_object(
      'source_tsfin_id', v_old_tsfin.id::text,
      'source_is_current', true,
      'source_paid_at_utc', v_old_tsfin.paid_at_utc
    ),
    'Paid but uninvoiced TSFIN rollover before import amendment',
    NULL::text,
    NULL::text,
    'import-operation:' || p_operation_id::text
  );

  RETURN jsonb_build_object(
    'ok', true,
    'replay', v_replay,
    'operation_id', p_operation_id::text,
    'timesheet_id', p_timesheet_id::text,
    'historical_paid_tsfin_id', v_old_tsfin.id::text,
    'historical_paid_digest', v_old_paid_digest,
    'new_current_tsfin_id', v_new_tsfin.id::text,
    'new_current_processing_status',
      v_new_tsfin.processing_status::text,
    'new_current_is_stale', v_new_tsfin.is_stale,
    'new_current_stale_reason', v_new_tsfin.stale_reason,
    'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
    'correction_financials_policy_envelope_fingerprint',
      v_correction_financials_policy_envelope_fingerprint,
    'requires_frozen_correction_policy', true,
    'requires_calculation', true,
    'requires_reauthorisation', true
    ,'import_authoritative_route',v_route
    ,'ordinary_source',v_is_ordinary_source
  );
END;
$function$;

-- timesheet_pay_state(uuid,uuid)
CREATE OR REPLACE FUNCTION public.timesheet_pay_state(p_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_out jsonb;
  v_has_tsfin boolean := false;
  v_today_uk date := (now() AT TIME ZONE 'Europe/London')::date;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'timesheet_pay_state: timesheet_id is required';
  END IF;

  WITH
  tf AS (
    SELECT
      tf.timesheet_id,
      tf.candidate_id,
      ts.booking_id,
      tf.pay_on_hold,
      tf.invoice_breakdown_json,
      ts.sheet_scope,
      ts.reference_number,
      ts.worked_start_iso AS ts_worked_start_iso,
      ts.worked_end_iso AS ts_worked_end_iso,
      ts.break_start_iso AS ts_break_start_iso,
      ts.break_end_iso AS ts_break_end_iso,
      ts.break_minutes AS ts_break_minutes,
      ts.actual_schedule_json AS ts_actual_schedule_json,
      tf.worked_start_iso AS tf_worked_start_iso,
      tf.worked_end_iso AS tf_worked_end_iso,
      tf.break_start_iso AS tf_break_start_iso,
      tf.break_end_iso AS tf_break_end_iso,
      tf.break_minutes AS tf_break_minutes,
      tf.actual_schedule_json AS tf_actual_schedule_json
    FROM public.timesheets_financials tf
    LEFT JOIN public.timesheets ts
      ON ts.timesheet_id = tf.timesheet_id
     AND ts.is_current = true
    WHERE tf.timesheet_id = p_timesheet_id
      AND tf.is_current = true
    LIMIT 1
  ),
  tf_norm AS (
    SELECT
      t.timesheet_id,
      t.candidate_id,
      t.booking_id,
      COALESCE(t.pay_on_hold, false) AS pay_on_hold,
      CASE
        WHEN t.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(t.invoice_breakdown_json) = 'object'
          THEN t.invoice_breakdown_json
        ELSE NULL
      END AS invoice_breakdown_json,
      upper(coalesce(t.sheet_scope::text, '')) AS sheet_scope,
      nullif(btrim(coalesce(t.reference_number, '')), '') AS reference_number,
      coalesce(t.tf_worked_start_iso, t.ts_worked_start_iso) AS effective_worked_start_iso,
      coalesce(t.tf_worked_end_iso, t.ts_worked_end_iso) AS effective_worked_end_iso,
      coalesce(t.tf_break_start_iso, t.ts_break_start_iso) AS effective_break_start_iso,
      coalesce(t.tf_break_end_iso, t.ts_break_end_iso) AS effective_break_end_iso,
      coalesce(t.tf_break_minutes, t.ts_break_minutes) AS effective_break_minutes,
      CASE
        WHEN jsonb_typeof(t.tf_actual_schedule_json) = 'object' THEN t.tf_actual_schedule_json
        WHEN jsonb_typeof(t.tf_actual_schedule_json) = 'array' THEN (
          SELECT tf_sched_item.value
          FROM jsonb_array_elements(t.tf_actual_schedule_json) AS tf_sched_item(value)
          WHERE tf_sched_item.value IS NOT NULL
            AND jsonb_typeof(tf_sched_item.value) = 'object'
          LIMIT 1
        )
        WHEN jsonb_typeof(t.ts_actual_schedule_json) = 'object' THEN t.ts_actual_schedule_json
        WHEN jsonb_typeof(t.ts_actual_schedule_json) = 'array' THEN (
          SELECT ts_sched_item.value
          FROM jsonb_array_elements(t.ts_actual_schedule_json) AS ts_sched_item(value)
          WHERE ts_sched_item.value IS NOT NULL
            AND jsonb_typeof(ts_sched_item.value) = 'object'
          LIMIT 1
        )
        ELSE NULL::jsonb
      END AS effective_daily_schedule_json
    FROM tf t
  ),
  mode_flags AS (
    SELECT
      tn.timesheet_id,
      tn.candidate_id,
      tn.booking_id,
      tn.pay_on_hold,
      tn.invoice_breakdown_json,
      tn.sheet_scope,
      tn.reference_number,
      tn.effective_worked_start_iso,
      tn.effective_worked_end_iso,
      tn.effective_break_start_iso,
      tn.effective_break_end_iso,
      tn.effective_break_minutes,
      tn.effective_daily_schedule_json,
      CASE
        WHEN tn.invoice_breakdown_json IS NOT NULL
         AND upper(coalesce(tn.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
         AND jsonb_typeof(tn.invoice_breakdown_json->'segments') = 'array'
          THEN true
        ELSE false
      END AS actual_segments_mode,
      CASE
        WHEN tn.sheet_scope = 'DAILY'
         AND coalesce(
           CASE
             WHEN tn.effective_worked_start_iso IS NOT NULL THEN ((tn.effective_worked_start_iso AT TIME ZONE 'Europe/London')::date)::text
             ELSE coalesce(
               nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'date','')), ''),
               nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'work_date','')), ''),
               nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'ymd','')), ''),
               nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'date_ymd','')), '')
             )
           END,
           ''
         ) <> ''
          THEN true
        ELSE false
      END AS synthetic_daily_mode,
      CASE
        WHEN tn.effective_worked_start_iso IS NOT NULL THEN ((tn.effective_worked_start_iso AT TIME ZONE 'Europe/London')::date)::text
        ELSE coalesce(
          nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'date','')), ''),
          nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'work_date','')), ''),
          nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'ymd','')), ''),
          nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'date_ymd','')), '')
        )
      END AS synthetic_component_date,
      jsonb_strip_nulls(
        case
          when jsonb_typeof(tn.effective_daily_schedule_json->'breaks') = 'array' then tn.effective_daily_schedule_json->'breaks'
          when tn.effective_break_start_iso is not null and tn.effective_break_end_iso is not null then
            jsonb_build_array(
              jsonb_build_object(
                'start', to_char((tn.effective_break_start_iso AT TIME ZONE 'Europe/London'), 'HH24:MI'),
                'end', to_char((tn.effective_break_end_iso AT TIME ZONE 'Europe/London'), 'HH24:MI'),
                'break_mins', coalesce(
                  tn.effective_break_minutes::numeric,
                  case
                    when tn.effective_break_start_iso is not null and tn.effective_break_end_iso is not null
                      then greatest(
                        0::numeric,
                        round((extract(epoch from (tn.effective_break_end_iso - tn.effective_break_start_iso)) / 60.0)::numeric, 0)
                      )
                    else null::numeric
                  end
                )
              )
            )
          when nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'break_start','')), '') is not null
           and nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'break_end','')), '') is not null then
            jsonb_build_array(
              jsonb_build_object(
                'start', nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'break_start','')), ''),
                'end', nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'break_end','')), ''),
                'break_mins', coalesce(
                  tn.effective_break_minutes::numeric,
                  nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'break_minutes','')), '')::numeric,
                  nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'break_mins','')), '')::numeric,
                  nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'breakMinutes','')), '')::numeric,
                  nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'breakMin','')), '')::numeric
                )
              )
            )
          else '[]'::jsonb
        end
      ) AS synthetic_component_breaks
    FROM tf_norm tn
  ),
  segment_components AS (
    SELECT
      nullif(btrim(COALESCE(seg.value->>'segment_id','')), '') AS component_id,
      nullif(btrim(COALESCE(seg.value->>'date','')), '') AS component_date,
      coalesce(
        nullif(btrim(COALESCE(seg.value->>'segment_stable_key','')), ''),
        nullif(btrim(COALESCE(seg.value->>'segment_id','')), ''),
        nullif(btrim(COALESCE(seg.value->>'segment_key','')), ''),
        nullif(btrim(COALESCE(seg.value->>'ref_num','')), ''),
        nullif(btrim(COALESCE(seg.value->>'date','')), '')
      ) AS component_stable_key,
      nullif(btrim(COALESCE(seg.value->>'ref_num','')), '') AS component_ref_num,
      COALESCE(NULLIF(seg.value->>'exclude_from_pay','')::boolean, false) AS is_on_hold,
      nullif(btrim(COALESCE(seg.value->>'start_utc','')), '') AS component_start_utc,
      nullif(btrim(COALESCE(seg.value->>'end_utc','')), '') AS component_end_utc,
      case
        when nullif(btrim(COALESCE(seg.value->>'start','')), '') is not null then nullif(btrim(COALESCE(seg.value->>'start','')), '')
        when nullif(btrim(COALESCE(seg.value->>'start_utc','')), '') is not null then to_char(((seg.value->>'start_utc')::timestamptz AT TIME ZONE 'Europe/London'), 'HH24:MI')
        else null
      end AS component_start,
      case
        when nullif(btrim(COALESCE(seg.value->>'end','')), '') is not null then nullif(btrim(COALESCE(seg.value->>'end','')), '')
        when nullif(btrim(COALESCE(seg.value->>'end_utc','')), '') is not null then to_char(((seg.value->>'end_utc')::timestamptz AT TIME ZONE 'Europe/London'), 'HH24:MI')
        else null
      end AS component_end,
      nullif(btrim(COALESCE(seg.value->>'break_start','')), '') AS component_break_start,
      nullif(btrim(COALESCE(seg.value->>'break_end','')), '') AS component_break_end,
      coalesce(
        nullif(seg.value->>'break_mins','')::numeric,
        nullif(seg.value->>'break_minutes','')::numeric
      ) AS component_break_mins,
      case
        when jsonb_typeof(seg.value->'breaks') = 'array' then seg.value->'breaks'
        else '[]'::jsonb
      end AS component_breaks
    FROM mode_flags mf
    JOIN LATERAL jsonb_array_elements(COALESCE(mf.invoice_breakdown_json->'segments','[]'::jsonb)) AS seg(value)
      ON true
    WHERE mf.actual_segments_mode = true
      AND seg.value IS NOT NULL
      AND jsonb_typeof(seg.value) = 'object'
      AND nullif(btrim(COALESCE(seg.value->>'segment_id','')), '') IS NOT NULL

    UNION ALL

    SELECT
      ('ts:' || mf.timesheet_id::text) AS component_id,
      mf.synthetic_component_date AS component_date,
      ('timesheet:' || COALESCE(mf.booking_id, mf.timesheet_id::text)) AS component_stable_key,
      coalesce(
        mf.reference_number,
        nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'ref_num','')), '')
      ) AS component_ref_num,
      mf.pay_on_hold AS is_on_hold,
      CASE
        WHEN mf.effective_worked_start_iso IS NULL THEN NULL
        ELSE mf.effective_worked_start_iso::text
      END AS component_start_utc,
      CASE
        WHEN mf.effective_worked_end_iso IS NULL THEN NULL
        ELSE mf.effective_worked_end_iso::text
      END AS component_end_utc,
      case
        when mf.effective_worked_start_iso is not null then to_char((mf.effective_worked_start_iso AT TIME ZONE 'Europe/London'), 'HH24:MI')
        else coalesce(
          nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'start','')), ''),
          nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'worked_start','')), '')
        )
      end AS component_start,
      case
        when mf.effective_worked_end_iso is not null then to_char((mf.effective_worked_end_iso AT TIME ZONE 'Europe/London'), 'HH24:MI')
        else coalesce(
          nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'end','')), ''),
          nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'worked_end','')), '')
        )
      end AS component_end,
      case
        when mf.effective_break_start_iso is not null then to_char((mf.effective_break_start_iso AT TIME ZONE 'Europe/London'), 'HH24:MI')
        else nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'break_start','')), '')
      end AS component_break_start,
      case
        when mf.effective_break_end_iso is not null then to_char((mf.effective_break_end_iso AT TIME ZONE 'Europe/London'), 'HH24:MI')
        else nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'break_end','')), '')
      end AS component_break_end,
      coalesce(
        mf.effective_break_minutes::numeric,
        nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'break_minutes','')), '')::numeric,
        nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'break_mins','')), '')::numeric,
        nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'breakMinutes','')), '')::numeric,
        nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'breakMin','')), '')::numeric,
        case
          when mf.effective_break_start_iso is not null and mf.effective_break_end_iso is not null
            then greatest(
              0::numeric,
              round((extract(epoch from (mf.effective_break_end_iso - mf.effective_break_start_iso)) / 60.0)::numeric, 0)
            )
          else null::numeric
        end
      ) AS component_break_mins,
      coalesce(mf.synthetic_component_breaks, '[]'::jsonb) AS component_breaks
    FROM mode_flags mf
    WHERE mf.actual_segments_mode = false
      AND mf.synthetic_daily_mode = true
  ),
  components AS (
    SELECT
      sc.component_id,
      sc.component_date,
      sc.component_stable_key,
      sc.component_ref_num,
      sc.is_on_hold,
      sc.component_start_utc,
      sc.component_end_utc,
      sc.component_start,
      sc.component_end,
      sc.component_break_start,
      sc.component_break_end,
      sc.component_break_mins,
      sc.component_breaks
    FROM segment_components sc

    UNION ALL

    SELECT
      'TOTAL'::text AS component_id,
      NULL::text AS component_date,
      ('timesheet:' || COALESCE(mf.booking_id, mf.timesheet_id::text))::text AS component_stable_key,
      NULL::text AS component_ref_num,
      mf.pay_on_hold AS is_on_hold,
      NULL::text AS component_start_utc,
      NULL::text AS component_end_utc,
      NULL::text AS component_start,
      NULL::text AS component_end,
      NULL::text AS component_break_start,
      NULL::text AS component_break_end,
      NULL::numeric AS component_break_mins,
      '[]'::jsonb AS component_breaks
    FROM mode_flags mf
    WHERE mf.actual_segments_mode = false
      AND mf.synthetic_daily_mode = false
  ),
  active_override AS (
    SELECT
      tpo.id AS override_id,
      tpo.timesheet_id,
      tpo.reason,
      tpo.created_at_utc,
      tpo.consumed_at_utc,
      CASE
        WHEN pb.id IS NOT NULL AND upper(COALESCE(pb.status::text,'')) <> 'CANCELLED'
          THEN pb.id
        ELSE NULL
      END AS consumed_by_batch_id,
      CASE
        WHEN pb.id IS NOT NULL AND upper(COALESCE(pb.status::text,'')) <> 'CANCELLED'
          THEN upper(COALESCE(pb.status::text,''))
        ELSE NULL
      END AS consumed_batch_status,
      CASE
        WHEN pb.id IS NOT NULL AND upper(COALESCE(pb.status::text,'')) <> 'CANCELLED'
          THEN false
        ELSE true
      END AS can_unadvance,
      CASE
        WHEN tpo.consumed_by_pay_batch_id IS NULL THEN 'ADVANCED_PENDING_BATCH'
        WHEN pb.id IS NOT NULL AND upper(COALESCE(pb.status::text,'')) = 'CANCELLED' THEN 'ADVANCED_BATCH_CANCELLED'
        WHEN pb.id IS NOT NULL THEN 'ADVANCED_IN_BATCH'
        ELSE 'ADVANCED_PENDING_BATCH'
      END AS advance_status
    FROM public.timesheet_payment_overrides tpo
    LEFT JOIN public.pay_batches pb
      ON pb.id = tpo.consumed_by_pay_batch_id
    WHERE tpo.timesheet_id = p_timesheet_id
      AND tpo.cleared_at_utc IS NULL
      AND upper(coalesce(tpo.override_type, '')) = 'ADVANCE_THIS_PAYMENT'
    ORDER BY tpo.created_at_utc DESC, tpo.id DESC
    LIMIT 1
  ),
  active_payment_snooze AS (
    SELECT
      s.id AS snooze_id,
      s.snooze_until_date,
      s.note
    FROM tf_norm tn
    JOIN public.pay_item_snoozes s
      ON s.candidate_id = tn.candidate_id
     AND s.source_ref IS NULL
     AND s.segment_id IS NULL
     AND s.segment_stable_key IS NULL
     AND s.cleared_at_utc IS NULL
     AND upper(COALESCE(s.snooze_kind,'')) = 'TIMESHEET_PAYMENT'
     AND (s.snooze_until_date IS NULL OR s.snooze_until_date >= v_today_uk)
     AND (
       (tn.booking_id IS NOT NULL AND s.booking_id IS NOT DISTINCT FROM tn.booking_id)
       OR
       (s.booking_id IS NULL AND s.timesheet_id = p_timesheet_id)
     )
    ORDER BY s.updated_at_utc DESC NULLS LAST, s.created_at_utc DESC, s.id DESC
    LIMIT 1
  ),
  active_segment_snoozes_source AS (
    SELECT
      s.id AS snooze_id,
      s.candidate_id,
      s.timesheet_id,
      s.booking_id,
      s.segment_id,
      coalesce(
        nullif(btrim(COALESCE(s.segment_stable_key,'')), ''),
        nullif(btrim(COALESCE(s.segment_id,'')), '')
      ) AS segment_stable_key,
      upper(COALESCE(s.snooze_kind,'')) AS snooze_kind,
      s.snooze_until_date,
      s.note,
      s.created_at_utc,
      s.updated_at_utc
    FROM tf_norm tn
    JOIN public.pay_item_snoozes s
      ON s.candidate_id = tn.candidate_id
     AND s.source_ref IS NULL
     AND s.cleared_at_utc IS NULL
     AND upper(COALESCE(s.snooze_kind,'')) IN ('DO_NOT_PAY', 'BLOCKED_TIMESHEET')
     AND (s.snooze_until_date IS NULL OR s.snooze_until_date >= v_today_uk)
     AND coalesce(
           nullif(btrim(COALESCE(s.segment_stable_key,'')), ''),
           nullif(btrim(COALESCE(s.segment_id,'')), '')
         ) IS NOT NULL
     AND (
       (tn.booking_id IS NOT NULL AND s.booking_id IS NOT DISTINCT FROM tn.booking_id)
       OR
       (s.booking_id IS NULL AND s.timesheet_id = p_timesheet_id)
     )
  ),
  active_segment_snooze_matches AS (
    SELECT
      sc.component_id,
      sc.component_date,
      sc.component_stable_key,
      sc.component_ref_num,
      sc.component_start_utc,
      sc.component_end_utc,
      sc.component_start,
      sc.component_end,
      sc.component_break_start,
      sc.component_break_end,
      sc.component_break_mins,
      sc.component_breaks,
      ass.snooze_id,
      ass.candidate_id,
      ass.timesheet_id,
      ass.booking_id,
      ass.segment_id,
      ass.segment_stable_key,
      ass.snooze_kind,
      ass.snooze_until_date,
      ass.note,
      ass.created_at_utc,
      ass.updated_at_utc,
      row_number() OVER (
        PARTITION BY sc.component_id
        ORDER BY ass.updated_at_utc DESC NULLS LAST, ass.created_at_utc DESC, ass.snooze_id DESC
      ) AS rn
    FROM segment_components sc
    JOIN active_segment_snoozes_source ass
      ON (
        (
          ass.booking_id IS NOT NULL
          AND (
            ass.segment_stable_key IS NOT DISTINCT FROM sc.component_stable_key
            OR ass.segment_id IS NOT DISTINCT FROM sc.component_id
          )
        )
        OR
        (
          ass.booking_id IS NULL
          AND ass.timesheet_id IS NOT DISTINCT FROM p_timesheet_id
          AND (
            ass.segment_id IS NOT DISTINCT FROM sc.component_id
            OR ass.segment_stable_key IS NOT DISTINCT FROM sc.component_stable_key
          )
        )
      )
  ),
  active_segment_snoozes AS (
    SELECT
      asm.component_id,
      asm.component_date,
      asm.component_stable_key,
      asm.component_ref_num,
      asm.component_start_utc,
      asm.component_end_utc,
      asm.component_start,
      asm.component_end,
      asm.component_break_start,
      asm.component_break_end,
      asm.component_break_mins,
      asm.component_breaks,
      asm.snooze_id,
      asm.candidate_id,
      asm.timesheet_id,
      asm.booking_id,
      asm.segment_id,
      asm.segment_stable_key,
      asm.snooze_kind,
      asm.snooze_until_date,
      asm.note,
      asm.created_at_utc,
      asm.updated_at_utc
    FROM active_segment_snooze_matches asm
    WHERE asm.rn = 1
  ),
  pay_items AS (
    SELECT
      CASE
        WHEN nullif(btrim(COALESCE(pbi.segment_key,'')), '') IS NOT NULL
          THEN nullif(btrim(COALESCE(pbi.segment_key,'')), '')
        WHEN pbi.source_ref IS NOT NULL AND btrim(COALESCE(pbi.source_ref,'')) LIKE 'seg:%'
          THEN nullif(btrim(substring(btrim(COALESCE(pbi.source_ref,'')) from 5)), '')
        ELSE 'TOTAL'
      END AS component_id,
      pb.status AS batch_status,
      pb.completed_at_utc AS completed_at_utc,
      pbt.status AS transfer_status,
      pbt.rail_state AS transfer_rail_state,
      COALESCE(pbt_classifier.is_final_money_moved, false) AS transfer_final_money_moved,
      COALESCE(pbt_classifier.is_pending_non_final, false) AS transfer_pending_non_final,
      COALESCE(pbt_classifier.is_terminal_no_money, false) AS transfer_terminal_no_money
    FROM public.pay_batch_items pbi
    JOIN public.pay_batch_candidates pbc
      ON pbc.id = pbi.pay_batch_candidate_id
    JOIN public.pay_batches pb
      ON pb.id = pbc.pay_batch_id
    LEFT JOIN public.pay_bank_transfers pbt
      ON pbt.id = pbi.pay_bank_transfer_id
     AND pbt.pay_batch_id = pb.id
    LEFT JOIN LATERAL public._pay_rail_state_money_movement_classify(
      pbt.status,
      pbt.rail_state,
      COALESCE(pbt.rail_meta_json, '{}'::jsonb),
      COALESCE(pbt.rail_meta_json, '{}'::jsonb)
    ) AS pbt_classifier
      ON pbt.id IS NOT NULL
    WHERE pbi.timesheet_id = p_timesheet_id
      AND pbi.is_voided = false
      AND pbi.item_type IN ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
      AND pb.cancelled_at_utc IS NULL
  ),
  agg AS (
    SELECT
      pi.component_id,
      max(CASE WHEN upper(COALESCE(pi.batch_status::text,'')) = 'SETTLED' THEN 1 ELSE 0 END)::int AS has_settled_norm,
      max(CASE WHEN upper(COALESCE(pi.batch_status::text,'')) = 'SETTLED' THEN pi.completed_at_utc ELSE NULL END) AS settled_at_utc,
      max(CASE
            WHEN COALESCE(pi.transfer_final_money_moved, false) = false
             AND COALESCE(pi.transfer_terminal_no_money, false) = false
             AND (
               COALESCE(pi.transfer_pending_non_final, false) = true
               OR upper(COALESCE(pi.batch_status::text,'')) IN ('EXECUTING','WAITING_BANK_CONFIRM')
               OR upper(COALESCE(pi.transfer_status::text,'')) IN ('PENDING','PROCESSING','UNKNOWN')
             )
              THEN 1
            ELSE 0
          END)::int AS has_proc_any
    FROM pay_items pi
    WHERE pi.component_id IS NOT NULL
    GROUP BY pi.component_id
  ),
  comp_state AS (
    SELECT
      c.component_id,
      c.component_date,
      c.component_stable_key,
      c.component_ref_num,
      c.is_on_hold,
      c.component_start_utc,
      c.component_end_utc,
      c.component_start,
      c.component_end,
      c.component_break_start,
      c.component_break_end,
      c.component_break_mins,
      c.component_breaks,
      CASE
        WHEN c.is_on_hold = true THEN 'ON_HOLD'
        WHEN COALESCE(a.has_settled_norm,0) = 1 THEN 'PAID'
        WHEN ao.override_id IS NOT NULL AND COALESCE(a.has_proc_any,0) = 1 THEN 'ADVANCE_PROCESSING'
        WHEN COALESCE(a.has_proc_any,0) = 1 THEN 'PAY_PROCESSING'
        WHEN ao.override_id IS NOT NULL THEN 'ADVANCED'
        ELSE 'UNPAID'
      END AS stage,
      a.settled_at_utc,
      CASE
        WHEN c.component_id = 'TOTAL' THEN 'NONE'
        WHEN ass.snooze_id IS NULL THEN 'NONE'
        WHEN ass.snooze_until_date IS NULL THEN 'INDEFINITE_SNOOZED'
        ELSE 'DATED_SNOOZED'
      END AS snooze_state,
      ass.snooze_id,
      ass.snooze_until_date,
      ass.note AS snooze_note,
      ass.segment_id AS snooze_segment_id,
      ass.segment_stable_key AS snooze_segment_stable_key
    FROM components c
    LEFT JOIN agg a
      ON a.component_id = c.component_id
    LEFT JOIN active_override ao
      ON true
    LEFT JOIN active_segment_snoozes ass
      ON ass.component_id = c.component_id
  ),
  counts AS (
    SELECT
      count(*)::int AS total_components,
      count(*) FILTER (WHERE cs.is_on_hold = true)::int AS on_hold_components,
      count(*) FILTER (WHERE cs.is_on_hold = false)::int AS payable_components,
      count(*) FILTER (WHERE cs.is_on_hold = false AND cs.stage = 'PAID')::int AS paid_components,
      count(*) FILTER (WHERE cs.is_on_hold = false AND cs.stage = 'ADVANCED')::int AS advanced_components,
      count(*) FILTER (WHERE cs.is_on_hold = false AND cs.stage IN ('PAY_PROCESSING','ADVANCE_PROCESSING'))::int AS processing_components,
      count(*) FILTER (WHERE cs.is_on_hold = false AND cs.stage = 'UNPAID')::int AS unpaid_components,
      max(CASE WHEN cs.is_on_hold = false AND cs.stage IN ('PAY_PROCESSING','ADVANCE_PROCESSING') THEN 1 ELSE 0 END)::int AS any_processing,
      max(CASE WHEN cs.is_on_hold = false AND cs.stage = 'ADVANCED' THEN 1 ELSE 0 END)::int AS any_advanced,
      max(CASE WHEN cs.is_on_hold = false AND cs.stage = 'PAID' THEN cs.settled_at_utc ELSE NULL END) AS paid_at_utc
    FROM comp_state cs
  ),
  pay_cache AS (
    SELECT
      cache_row.timesheet_id,
      CASE
        WHEN upper(coalesce(cache_row.summary_pay_status_code, '')) IN ('PAID','PARTIALLY_PAID','PROCESSING','ADVANCED')
          THEN upper(coalesce(cache_row.summary_pay_status_code, ''))
        WHEN cache_row.last_settled_at_utc IS NOT NULL
          THEN 'PAID'
        WHEN upper(coalesce(cache_row.summary_pay_status_code, '')) = 'UNPAID'
          THEN 'UNPAID'
        ELSE NULL::text
      END AS cached_pay_status_code,
      coalesce(cache_row.summary_pay_paid_at_utc, cache_row.last_settled_at_utc) AS cached_paid_at_utc,
      cache_row.summary_pay_icon_code,
      cache_row.summary_net_delta_ex_vat,
      cache_row.last_settled_pay_batch_id,
      cache_row.last_settled_at_utc
    FROM (SELECT 1) seed(seed_id)
    LEFT JOIN LATERAL (
      SELECT
        tps.timesheet_id,
        tps.summary_pay_status_code,
        tps.summary_pay_icon_code,
        tps.summary_pay_paid_at_utc,
        tps.summary_net_delta_ex_vat,
        tps.last_settled_pay_batch_id,
        tps.last_settled_at_utc
      FROM public.timesheet_pay_state tps
      WHERE tps.timesheet_id = p_timesheet_id
      LIMIT 1
    ) cache_row
      ON true
  ),
  unpaid_sample AS (
    SELECT
      coalesce(
        jsonb_agg(cs.component_date ORDER BY cs.component_date) FILTER (WHERE cs.component_date IS NOT NULL),
        '[]'::jsonb
      ) AS dates
    FROM (
      SELECT cs.component_date
      FROM comp_state cs
      WHERE cs.is_on_hold = false
        AND cs.stage = 'UNPAID'
        AND cs.component_date IS NOT NULL
      ORDER BY cs.component_date
      LIMIT 3
    ) cs
  ),
  delta AS (
    SELECT
      round(coalesce(sum(coalesce(oc.truth_ex_vat,0) - coalesce(oc.baseline_ex_vat,0)),0),2)::numeric AS net_delta_ex_vat,
      round(coalesce(sum(coalesce(oc.outstanding_ex_vat,0)),0),2)::numeric AS outstanding_ex_vat,
      round(coalesce(sum(coalesce(oc.reserved_ex_vat,0)),0),2)::numeric AS reserved_ex_vat
    FROM public._pay_outstanding_components(ARRAY[p_timesheet_id]) oc
  ),
  paid_totals AS (
    SELECT
      round(coalesce(sum(coalesce(paid_item.amount_ex_vat, 0)), 0), 2)::numeric AS paid_to_date_ex_vat,
      count(paid_item.id)::int AS paid_item_count,
      max(coalesce(paid_candidate.settled_at_utc, paid_transfer.completed_at_utc, paid_batch.completed_at_utc)) AS last_paid_at_utc
    FROM public.pay_batch_items AS paid_item
    JOIN public.pay_batch_candidates AS paid_candidate
      ON paid_candidate.id = paid_item.pay_batch_candidate_id
    JOIN public.pay_batches AS paid_batch
      ON paid_batch.id = paid_candidate.pay_batch_id
    LEFT JOIN public.pay_bank_transfers AS paid_transfer
      ON paid_transfer.id = paid_item.pay_bank_transfer_id
     AND paid_transfer.pay_batch_id = paid_batch.id
    LEFT JOIN LATERAL public._pay_rail_state_money_movement_classify(
      paid_transfer.status,
      paid_transfer.rail_state,
      coalesce(paid_transfer.rail_meta_json, '{}'::jsonb),
      coalesce(paid_transfer.rail_meta_json, '{}'::jsonb)
    ) AS paid_transfer_classifier
      ON paid_transfer.id IS NOT NULL
    WHERE paid_item.timesheet_id = p_timesheet_id
      AND coalesce(paid_item.is_voided, false) = false
      AND paid_item.item_type IN ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
      AND paid_batch.cancelled_at_utc IS NULL
      AND upper(btrim(coalesce(paid_batch.status, ''))) <> 'CANCELLED'
      AND (
        upper(btrim(coalesce(paid_candidate.settlement_status, ''))) IN ('SETTLED','PAID','CONFIRMED')
        OR upper(btrim(coalesce(paid_batch.status, ''))) = 'SETTLED'
        OR coalesce(paid_transfer_classifier.is_final_money_moved, false) = true
        OR upper(btrim(coalesce(paid_transfer.status, ''))) IN ('COMPLETED','COMPLETE','SETTLED','PAID','EXECUTED','COMMITTED','SUCCESS','SUCCESSFUL','SUCCEEDED')
        OR upper(btrim(coalesce(paid_transfer.rail_state, ''))) IN ('COMPLETED','COMPLETE','SETTLED','PAID','EXECUTED','COMMITTED','SUCCESS','SUCCESSFUL','SUCCEEDED')
        OR paid_transfer.completed_at_utc IS NOT NULL
        OR EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_settlement_scope AS paid_settlement_scope
          WHERE paid_settlement_scope.pay_batch_id = paid_batch.id
            AND paid_settlement_scope.pay_batch_candidate_id = paid_candidate.id
            AND paid_settlement_scope.status = 'SETTLED'
            AND (
              (
                paid_item.pay_bank_transfer_id IS NOT NULL
                AND paid_settlement_scope.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}' = paid_item.pay_bank_transfer_id::text
              )
              OR coalesce(paid_settlement_scope.payload_json->'pay_batch_item_ids', '[]'::jsonb) ? paid_item.id::text
            )
        )
      )
  ),
  mode AS (
    SELECT
      COALESCE((SELECT (mf.actual_segments_mode OR mf.synthetic_daily_mode) FROM mode_flags mf LIMIT 1), false) AS is_segments_mode
  ),
  comp_json AS (
    SELECT
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'component_id', cs.component_id,
            'date', cs.component_date,
            'segment_stable_key', cs.component_stable_key,
            'ref_num', cs.component_ref_num,
            'is_on_hold', cs.is_on_hold,
            'stage', cs.stage,
            'paid_at_utc', cs.settled_at_utc,
            'paid_at_label_uk',
              CASE
                WHEN cs.settled_at_utc IS NULL THEN NULL
                ELSE to_char(cs.settled_at_utc AT TIME ZONE 'Europe/London', 'Dy DD/MM/YYYY HH24:MI') || 'hrs'
              END,
            'snooze_state', cs.snooze_state,
            'snooze_id', CASE WHEN cs.snooze_id IS NULL THEN NULL ELSE cs.snooze_id::text END,
            'snooze_until_date', CASE WHEN cs.snooze_until_date IS NULL THEN NULL ELSE cs.snooze_until_date::text END,
            'snooze_note', cs.snooze_note,
            'snooze_segment_id', cs.snooze_segment_id,
            'snooze_segment_stable_key', cs.snooze_segment_stable_key,
            'start_utc', cs.component_start_utc,
            'end_utc', cs.component_end_utc,
            'start', cs.component_start,
            'end', cs.component_end,
            'break_start', cs.component_break_start,
            'break_end', cs.component_break_end,
            'break_mins', cs.component_break_mins,
            'breaks', coalesce(cs.component_breaks, '[]'::jsonb)
          )
          ORDER BY
            CASE WHEN cs.component_id = 'TOTAL' THEN 0 ELSE 1 END,
            cs.component_date NULLS LAST,
            cs.component_id
        ),
        '[]'::jsonb
      ) AS components
    FROM comp_state cs
  ),
  segment_snoozes_json AS (
    SELECT
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'snooze_id', ass.snooze_id::text,
            'candidate_id', ass.candidate_id::text,
            'timesheet_id', CASE WHEN ass.timesheet_id IS NULL THEN NULL ELSE ass.timesheet_id::text END,
            'booking_id', ass.booking_id,
            'segment_id', ass.segment_id,
            'segment_stable_key', ass.segment_stable_key,
            'snooze_kind', ass.snooze_kind,
            'snooze_until_date', CASE WHEN ass.snooze_until_date IS NULL THEN NULL ELSE ass.snooze_until_date::text END,
            'note', ass.note,
            'snooze_note', ass.note,
            'date', ass.component_date,
            'work_date', ass.component_date,
            'ref_num', ass.component_ref_num,
            'start_utc', ass.component_start_utc,
            'end_utc', ass.component_end_utc,
            'start', ass.component_start,
            'end', ass.component_end,
            'break_start', ass.component_break_start,
            'break_end', ass.component_break_end,
            'break_mins', ass.component_break_mins,
            'breaks', coalesce(ass.component_breaks, '[]'::jsonb),
            'snooze_state',
              CASE
                WHEN ass.snooze_until_date IS NULL THEN 'INDEFINITE_SNOOZED'
                ELSE 'DATED_SNOOZED'
              END
          )
          ORDER BY ass.component_date NULLS LAST, ass.segment_stable_key NULLS LAST, ass.segment_id NULLS LAST
        ),
        '[]'::jsonb
      ) AS segment_snoozes
    FROM active_segment_snoozes ass
  ),
  paid_status AS (
    SELECT
      resolved.paid_status_code,
      resolved.paid_at_utc,
      CASE
        WHEN resolved.paid_at_utc IS NULL THEN NULL
        ELSE to_char(resolved.paid_at_utc AT TIME ZONE 'Europe/London', 'Dy DD/MM/YYYY HH24:MI') || 'hrs'
      END AS paid_at_label_uk
    FROM (
      SELECT
        CASE
          WHEN c.processing_components > 0 OR pc.cached_pay_status_code = 'PROCESSING' THEN 'PROCESSING'
          WHEN coalesce(pt.paid_to_date_ex_vat,0) > 0
               AND greatest(coalesce(d.reserved_ex_vat,0),0) <= 0
               AND greatest(coalesce(d.outstanding_ex_vat,0),0) <= 0
               AND coalesce(d.net_delta_ex_vat,0) <= 0 THEN 'PAID'
          WHEN coalesce(pt.paid_to_date_ex_vat,0) > 0 THEN 'PARTIALLY_PAID'
          ELSE 'UNPAID'
        END AS paid_status_code,
        coalesce(pt.last_paid_at_utc, pc.cached_paid_at_utc, c.paid_at_utc) AS paid_at_utc
      FROM counts c
      CROSS JOIN pay_cache pc
      CROSS JOIN paid_totals pt
      CROSS JOIN delta d
    ) resolved
  ),
  adjusted AS (
    SELECT
      coalesce(d.net_delta_ex_vat,0) AS net_delta_ex_vat,
      greatest(coalesce(d.outstanding_ex_vat,0),0) AS outstanding_ex_vat,
      greatest(coalesce(d.reserved_ex_vat,0),0) AS reserved_ex_vat,
      CASE
        WHEN greatest(coalesce(d.outstanding_ex_vat,0),0) > 0 THEN 'PAY_OUTSTANDING'
        WHEN coalesce(d.net_delta_ex_vat,0) < 0 THEN 'OVERPAID'
        ELSE 'NONE'
      END AS adjusted_pill,
      CASE
        WHEN greatest(coalesce(d.outstanding_ex_vat,0),0) > 0 THEN
          'Timesheet adjusted after payment. Additional pay outstanding: £' || to_char(greatest(coalesce(d.outstanding_ex_vat,0),0), 'FM999999990D00')
        WHEN coalesce(d.net_delta_ex_vat,0) > 0
             AND greatest(coalesce(d.reserved_ex_vat,0),0) >= coalesce(d.net_delta_ex_vat,0)
             AND greatest(coalesce(d.outstanding_ex_vat,0),0) <= 0 THEN
          'Timesheet adjusted after payment. Additional pay is reserved. No unreserved amount is currently owed.'
        WHEN coalesce(d.net_delta_ex_vat,0) < 0 THEN
          'Timesheet adjusted after payment. Current overpaid position: £' || to_char(abs(coalesce(d.net_delta_ex_vat,0)), 'FM999999990D00')
        ELSE NULL
      END AS adjusted_message
    FROM delta d
  ),
  hover AS (
    SELECT
      jsonb_build_object(
        'paid', c.paid_components,
        'processing', c.processing_components,
        'unpaid', c.unpaid_components,
        'on_hold', c.on_hold_components,
        'total', c.total_components,
        'payable', c.payable_components,
        'last_payment_utc', ps.paid_at_utc,
        'last_payment_label_uk', ps.paid_at_label_uk,
        'unpaid_sample_dates', us.dates,
        'net_delta_ex_vat', d.net_delta_ex_vat,
        'outstanding_ex_vat', d.outstanding_ex_vat,
        'reserved_ex_vat', d.reserved_ex_vat
      ) AS hover_summary
    FROM counts c
    CROSS JOIN unpaid_sample us
    CROSS JOIN delta d
    CROSS JOIN paid_status ps
  ),
  override_json AS (
    SELECT
      COALESCE((SELECT ao.override_id IS NOT NULL FROM active_override ao LIMIT 1), false) AS is_advanced,
      COALESCE((SELECT ao.can_unadvance FROM active_override ao LIMIT 1), false) AS can_unadvance,
      (SELECT ao.consumed_by_batch_id FROM active_override ao LIMIT 1) AS advanced_consumed_by_batch_id,
      (SELECT ao.consumed_at_utc FROM active_override ao LIMIT 1) AS advanced_consumed_at_utc,
      (SELECT ao.consumed_batch_status FROM active_override ao LIMIT 1) AS advanced_batch_status,
      COALESCE((SELECT ao.advance_status FROM active_override ao LIMIT 1), 'NOT_ADVANCED') AS advance_status
  ),
  snooze_json AS (
    SELECT
      COALESCE((SELECT aps.snooze_id IS NOT NULL FROM active_payment_snooze aps LIMIT 1), false) AS is_snoozed,
      (SELECT aps.snooze_until_date FROM active_payment_snooze aps LIMIT 1) AS snooze_until_date,
      COALESCE((SELECT aps.snooze_until_date IS NULL FROM active_payment_snooze aps LIMIT 1), false) AS snooze_is_indefinite,
      (SELECT aps.note FROM active_payment_snooze aps LIMIT 1) AS snooze_note
  )
  SELECT
    jsonb_build_object(
      'ok', true,
      'timesheet_id', p_timesheet_id::text,
      'is_segments_mode', m.is_segments_mode,
      'paid_status', ps.paid_status_code,
      'paid_at_utc', ps.paid_at_utc,
      'paid_at_label_uk', ps.paid_at_label_uk,
      'advanced_any', ((c.any_advanced = 1) OR ps.paid_status_code = 'ADVANCED'),
      'processing_any', ((c.any_processing = 1) OR ps.paid_status_code = 'PROCESSING'),
      'is_advanced', oj.is_advanced,
      'can_unadvance', oj.can_unadvance,
      'advanced_consumed_by_batch_id', CASE WHEN oj.advanced_consumed_by_batch_id IS NULL THEN NULL ELSE oj.advanced_consumed_by_batch_id::text END,
      'advanced_consumed_at_utc', oj.advanced_consumed_at_utc,
      'advanced_batch_status', oj.advanced_batch_status,
      'advance_status', oj.advance_status,
      'is_snoozed', sj.is_snoozed,
      'snooze_until_date', CASE WHEN sj.snooze_until_date IS NULL THEN NULL ELSE sj.snooze_until_date::text END,
      'snooze_is_indefinite', sj.snooze_is_indefinite,
      'snooze_note', sj.snooze_note,
      'counts', jsonb_build_object(
        'total_components', c.total_components,
        'payable_components', c.payable_components,
        'on_hold_components', c.on_hold_components,
        'paid_components', c.paid_components,
        'advanced_components', c.advanced_components,
        'processing_components', c.processing_components,
        'unpaid_components', c.unpaid_components
      ),
      'adjusted', jsonb_build_object(
        'net_delta_ex_vat', a.net_delta_ex_vat,
        'pill', a.adjusted_pill,
        'message', a.adjusted_message,
        'outstanding_ex_vat', a.outstanding_ex_vat,
        'reserved_ex_vat', a.reserved_ex_vat
      ),
      'paid_totals', jsonb_build_object(
        'paid_to_date_ex_vat', coalesce(pt.paid_to_date_ex_vat, 0),
        'paid_item_count', coalesce(pt.paid_item_count, 0),
        'last_paid_at_utc', pt.last_paid_at_utc
      ),
      'hover', h.hover_summary,
      'components', cj.components,
      'segment_snoozes', ssj.segment_snoozes
    )
  INTO v_out
  FROM counts c
  CROSS JOIN mode m
  CROSS JOIN paid_status ps
  CROSS JOIN adjusted a
  CROSS JOIN paid_totals pt
  CROSS JOIN hover h
  CROSS JOIN comp_json cj
  CROSS JOIN segment_snoozes_json ssj
  CROSS JOIN override_json oj
  CROSS JOIN snooze_json sj;

  v_has_tsfin := EXISTS (
    SELECT 1
    FROM public.timesheets_financials tf
    WHERE tf.timesheet_id = p_timesheet_id
      AND tf.is_current = true
  );

  IF v_out IS NULL THEN
    v_out := jsonb_build_object(
      'ok', false,
      'timesheet_id', p_timesheet_id::text,
      'error', 'NO_DATA'
    );
  END IF;

  BEGIN
    IF p_actor_user_id IS NOT NULL THEN
      PERFORM public._imp_debug_audit(
        p_actor_user_id,
        'TIMESHEET_PAY_STATE',
        jsonb_build_object(
          'timesheet_id', p_timesheet_id::text,
          'has_tsfin', v_has_tsfin,
          'result', v_out
        ),
        'timesheets',
        p_timesheet_id::text
      );
    END IF;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

  RETURN v_out;
END;
$function$;

-- timesheet_payment_override_clear(uuid,uuid,text)
CREATE OR REPLACE FUNCTION public.timesheet_payment_override_clear(p_timesheet_id uuid, p_actor_user_id uuid, p_clear_reason text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_timesheet_id uuid := p_timesheet_id;
  v_actor_user_id uuid := p_actor_user_id;
  v_clear_reason text := nullif(btrim(coalesce(p_clear_reason, '')), '');
  v_now timestamptz := now();
  v_existing record;
  v_batch_status text := null;
  v_pay_state jsonb := '{}'::jsonb;
  v_candidate_id uuid := null;
begin
  if v_timesheet_id is null then
    raise exception 'timesheet_payment_override_clear: timesheet_id is required';
  end if;

  if v_actor_user_id is null then
    raise exception 'timesheet_payment_override_clear: actor_user_id is required';
  end if;

  select tf.candidate_id
  into v_candidate_id
  from public.timesheets_financials tf
  where tf.timesheet_id = v_timesheet_id
    and tf.is_current = true
  limit 1;

  select
    tpo.id,
    tpo.timesheet_id,
    tpo.candidate_id,
    tpo.override_type,
    tpo.reason,
    tpo.created_at_utc,
    tpo.created_by_user_id,
    tpo.consumed_by_pay_batch_id,
    tpo.consumed_at_utc,
    tpo.cleared_at_utc,
    tpo.cleared_by_user_id,
    tpo.clear_reason
  into v_existing
  from public.timesheet_payment_overrides tpo
  where tpo.timesheet_id = v_timesheet_id
    and upper(coalesce(tpo.override_type,'')) = 'ADVANCE_THIS_PAYMENT'
    and tpo.cleared_at_utc is null
  order by tpo.created_at_utc desc, tpo.id desc
  limit 1;

  if v_existing.id is null then
    v_pay_state := public.timesheet_pay_state(
      p_timesheet_id => v_timesheet_id,
      p_actor_user_id => v_actor_user_id
    );

    return jsonb_build_object(
      'ok', true,
      'timesheet_id', v_timesheet_id::text,
      'candidate_id', case when v_candidate_id is null then null else v_candidate_id::text end,
      'already_cleared', true,
      'pay_state', v_pay_state
    );
  end if;

  if v_existing.consumed_by_pay_batch_id is not null then
    select upper(coalesce(pb.status, ''))
    into v_batch_status
    from public.pay_batches pb
    where pb.id = v_existing.consumed_by_pay_batch_id
    limit 1;

    if coalesce(v_batch_status, '') <> 'CANCELLED' then
      raise exception '%', jsonb_build_object(
        'code', 'TIMESHEET_ADVANCE_ALREADY_BATCHED',
        'message', 'This timesheet advance is already in a non-cancelled pay batch.'
      )::text;
    end if;
  end if;

  update public.timesheet_payment_overrides tpo
  set
    cleared_at_utc = v_now,
    cleared_by_user_id = v_actor_user_id,
    clear_reason = coalesce(v_clear_reason, 'CLEARED')
  where tpo.id = v_existing.id;

  v_pay_state := public.timesheet_pay_state(
    p_timesheet_id => v_timesheet_id,
    p_actor_user_id => v_actor_user_id
  );

  perform public._audit_insert(
    'timesheets',
    v_timesheet_id::text,
    'TIMESHEET_PAYMENT_OVERRIDE_CLEARED',
    jsonb_build_object(
      'override_id', v_existing.id::text,
      'timesheet_id', v_timesheet_id::text,
      'candidate_id', case when coalesce(v_existing.candidate_id, v_candidate_id) is null then null else coalesce(v_existing.candidate_id, v_candidate_id)::text end,
      'override_type', v_existing.override_type,
      'reason', v_existing.reason,
      'consumed_by_pay_batch_id', case when v_existing.consumed_by_pay_batch_id is null then null else v_existing.consumed_by_pay_batch_id::text end,
      'consumed_at_utc', v_existing.consumed_at_utc
    ),
    jsonb_build_object(
      'override_id', v_existing.id::text,
      'timesheet_id', v_timesheet_id::text,
      'clear_reason', coalesce(v_clear_reason, 'CLEARED')
    ),
    coalesce(v_clear_reason, 'CLEARED'),
    v_actor_user_id
  );

  return jsonb_build_object(
    'ok', true,
    'timesheet_id', v_timesheet_id::text,
    'candidate_id', case when coalesce(v_existing.candidate_id, v_candidate_id) is null then null else coalesce(v_existing.candidate_id, v_candidate_id)::text end,
    'already_cleared', false,
    'cleared_override_id', v_existing.id::text,
    'pay_state', v_pay_state
  );
end;
$function$;

-- timesheet_payment_override_set(uuid,uuid,text)
CREATE OR REPLACE FUNCTION public.timesheet_payment_override_set(p_timesheet_id uuid, p_actor_user_id uuid, p_reason text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_timesheet_id uuid := p_timesheet_id;
  v_actor_user_id uuid := p_actor_user_id;
  v_reason text := nullif(btrim(coalesce(p_reason, '')), '');
  v_now timestamptz := now();
  v_candidate_id uuid := null;
  v_existing record;
  v_batch_status text := null;
  v_outstanding_ex_vat numeric := 0;
  v_reserved_ex_vat numeric := 0;
  v_baseline_ex_vat numeric := 0;
  v_truth_ex_vat numeric := 0;
  v_inserted record;
  v_pay_state jsonb := '{}'::jsonb;
begin
  if v_timesheet_id is null then
    raise exception 'timesheet_payment_override_set: timesheet_id is required';
  end if;

  if v_actor_user_id is null then
    raise exception 'timesheet_payment_override_set: actor_user_id is required';
  end if;

  if v_reason is null then
    raise exception 'timesheet_payment_override_set: reason is required';
  end if;

  perform 1
  from public.timesheets ts
  where ts.timesheet_id = v_timesheet_id
    and ts.is_current = true;

  if not found then
    raise exception 'timesheet_payment_override_set: current timesheet % not found.', v_timesheet_id;
  end if;

  select tf.candidate_id
  into v_candidate_id
  from public.timesheets_financials tf
  where tf.timesheet_id = v_timesheet_id
    and tf.is_current = true
  limit 1;

  if v_candidate_id is null then
    raise exception 'timesheet_payment_override_set: candidate_id not found for timesheet %.', v_timesheet_id;
  end if;

  select
    tpo.id,
    tpo.timesheet_id,
    tpo.candidate_id,
    tpo.override_type,
    tpo.reason,
    tpo.created_at_utc,
    tpo.created_by_user_id,
    tpo.consumed_by_pay_batch_id,
    tpo.consumed_at_utc,
    tpo.cleared_at_utc,
    tpo.cleared_by_user_id,
    tpo.clear_reason
  into v_existing
  from public.timesheet_payment_overrides tpo
  where tpo.timesheet_id = v_timesheet_id
    and upper(coalesce(tpo.override_type,'')) = 'ADVANCE_THIS_PAYMENT'
    and tpo.cleared_at_utc is null
  order by tpo.created_at_utc desc, tpo.id desc
  limit 1;

  if v_existing.id is not null then
    if v_existing.consumed_by_pay_batch_id is not null then
      select upper(coalesce(pb.status, ''))
      into v_batch_status
      from public.pay_batches pb
      where pb.id = v_existing.consumed_by_pay_batch_id
      limit 1;

      if coalesce(v_batch_status, '') <> 'CANCELLED' then
        raise exception '%', jsonb_build_object(
          'code', 'TIMESHEET_ADVANCE_ALREADY_BATCHED',
          'message', 'This timesheet advance is already in a non-cancelled pay batch.'
        )::text;
      end if;

      update public.timesheet_payment_overrides tpo_old
      set
        cleared_at_utc = v_now,
        cleared_by_user_id = v_actor_user_id,
        clear_reason = coalesce(tpo_old.clear_reason, 'REPLACED_AFTER_CANCELLED_BATCH')
      where tpo_old.id = v_existing.id;
    else
      v_pay_state := public.timesheet_pay_state(
        p_timesheet_id => v_timesheet_id,
        p_actor_user_id => v_actor_user_id
      );

      return jsonb_build_object(
        'ok', true,
        'timesheet_id', v_timesheet_id::text,
        'candidate_id', v_candidate_id::text,
        'already_exists', true,
        'override', jsonb_build_object(
          'id', v_existing.id::text,
          'override_type', v_existing.override_type,
          'reason', v_existing.reason,
          'created_at_utc', v_existing.created_at_utc,
          'created_by_user_id', case when v_existing.created_by_user_id is null then null else v_existing.created_by_user_id::text end,
          'consumed_by_pay_batch_id', case when v_existing.consumed_by_pay_batch_id is null then null else v_existing.consumed_by_pay_batch_id::text end,
          'consumed_at_utc', v_existing.consumed_at_utc
        ),
        'pay_state', v_pay_state
      );
    end if;
  end if;

  select
    round(coalesce(sum(coalesce(oc.outstanding_ex_vat, 0)), 0), 2),
    round(coalesce(sum(coalesce(oc.reserved_ex_vat, 0)), 0), 2),
    round(coalesce(sum(coalesce(oc.baseline_ex_vat, 0)), 0), 2),
    round(coalesce(sum(coalesce(oc.truth_ex_vat, 0)), 0), 2)
  into
    v_outstanding_ex_vat,
    v_reserved_ex_vat,
    v_baseline_ex_vat,
    v_truth_ex_vat
  from public._pay_outstanding_components(array[v_timesheet_id]) oc;

  if coalesce(v_reserved_ex_vat, 0) > 0 then
    raise exception '%', jsonb_build_object(
      'code', 'TIMESHEET_ADVANCE_PAYMENT_PROCESSING',
      'message', 'This timesheet is already reserved in an active pay batch.'
    )::text;
  end if;

  if coalesce(v_outstanding_ex_vat, 0) <= 0 then
    if coalesce(v_baseline_ex_vat, 0) > 0 and coalesce(v_truth_ex_vat, 0) > 0 and coalesce(v_baseline_ex_vat, 0) >= coalesce(v_truth_ex_vat, 0) then
      raise exception '%', jsonb_build_object(
        'code', 'TIMESHEET_ADVANCE_ALREADY_PAID',
        'message', 'This timesheet has already been paid and cannot be advanced.'
      )::text;
    elsif coalesce(v_baseline_ex_vat, 0) > 0 then
      raise exception '%', jsonb_build_object(
        'code', 'TIMESHEET_ADVANCE_PART_PAID_NO_OUTSTANDING',
        'message', 'This timesheet has already been partially paid and has no outstanding payable amount to advance.'
      )::text;
    else
      raise exception '%', jsonb_build_object(
        'code', 'TIMESHEET_ADVANCE_NO_OUTSTANDING_PAY',
        'message', 'This timesheet has no outstanding payable amount to advance.'
      )::text;
    end if;
  end if;

  insert into public.timesheet_payment_overrides(
    timesheet_id,
    candidate_id,
    override_type,
    reason,
    created_at_utc,
    created_by_user_id
  )
  values (
    v_timesheet_id,
    v_candidate_id,
    'ADVANCE_THIS_PAYMENT',
    v_reason,
    v_now,
    v_actor_user_id
  )
  returning * into v_inserted;

  v_pay_state := public.timesheet_pay_state(
    p_timesheet_id => v_timesheet_id,
    p_actor_user_id => v_actor_user_id
  );

  perform public._audit_insert(
    'timesheets',
    v_timesheet_id::text,
    'TIMESHEET_PAYMENT_OVERRIDE_SET',
    null,
    jsonb_build_object(
      'override_id', v_inserted.id::text,
      'timesheet_id', v_timesheet_id::text,
      'candidate_id', v_candidate_id::text,
      'override_type', 'ADVANCE_THIS_PAYMENT',
      'reason', v_reason
    ),
    'ADVANCE_THIS_PAYMENT',
    v_actor_user_id
  );

  return jsonb_build_object(
    'ok', true,
    'timesheet_id', v_timesheet_id::text,
    'candidate_id', v_candidate_id::text,
    'already_exists', false,
    'override', jsonb_build_object(
      'id', v_inserted.id::text,
      'override_type', v_inserted.override_type,
      'reason', v_inserted.reason,
      'created_at_utc', v_inserted.created_at_utc,
      'created_by_user_id', case when v_inserted.created_by_user_id is null then null else v_inserted.created_by_user_id::text end,
      'consumed_by_pay_batch_id', case when v_inserted.consumed_by_pay_batch_id is null then null else v_inserted.consumed_by_pay_batch_id::text end,
      'consumed_at_utc', v_inserted.consumed_at_utc
    ),
    'pay_state', v_pay_state
  );
end;
$function$;

-- timesheet_pdf_load_context_batch(uuid[])
CREATE OR REPLACE FUNCTION public.timesheet_pdf_load_context_batch(p_timesheet_ids uuid[])
 RETURNS TABLE(timesheet_id uuid, out_ts jsonb, out_summary jsonb, out_contract jsonb, out_client jsonb, out_candidate jsonb, out_fin jsonb, out_def jsonb)
 LANGUAGE sql
 STABLE
AS $function$
with wanted as (
  select distinct unnest(p_timesheet_ids) as timesheet_id
  where p_timesheet_ids is not null
),
t as (
  select ts.*
  from wanted w
  join public.timesheets ts
    on ts.timesheet_id = w.timesheet_id
   and ts.is_current = true
),
s as (
  -- ONLY fields that actually exist on v_timesheets_summary and are needed for identity resolution
  select
    t.timesheet_id,
    vs.candidate_id,
    vs.client_id,
    vs.candidate_name,
    vs.client_name,
    vs.contract_id
  from t
  left join public.v_timesheets_summary vs
    on vs.timesheet_id = t.timesheet_id
),
c as (
  select t.timesheet_id, ct.*
  from t
  left join s on s.timesheet_id = t.timesheet_id
  left join public.contracts ct
    on ct.id = coalesce(t.contract_id, s.contract_id)
),
ids as (
  select
    t.timesheet_id,
    coalesce(c.candidate_id, s.candidate_id) as eff_candidate_id,
    coalesce(c.client_id,    s.client_id)    as eff_client_id
  from t
  left join s on s.timesheet_id = t.timesheet_id
  left join c on c.timesheet_id = t.timesheet_id
)
select
  t.timesheet_id,
  (to_jsonb(t) || jsonb_build_object(
    'generated_pdf_refs_sig', t.generated_pdf_refs_sig,
    'generated_pdf_refs_snapshot_json', t.generated_pdf_refs_snapshot_json,
    'generated_pdf_refs_captured_at_utc', t.generated_pdf_refs_captured_at_utc
  )) as out_ts,
  to_jsonb(s) as out_summary,
  to_jsonb(c) as out_contract,
  to_jsonb(cl) as out_client,
  to_jsonb(ca) as out_candidate,
  to_jsonb(tf) as out_fin,
  jsonb_build_object(
    'agency_name', sd.agency_name,
    'agency_logo', sd.agency_logo,
    'timesheet_header_json', sd.timesheet_header_json,
    'timesheet_footer_json', sd.timesheet_footer_json,

    -- TEXT declaration columns do NOT exist in settings_defaults (keep keys for renderer compatibility):
    'temporary_worker_declaration', null::text,
    'client_declaration', null::text,

    -- JSON declarations DO exist:
    'temporary_worker_declaration_json', sd.temporary_worker_declaration_json,
    'client_declaration_json', sd.client_declaration_json
  ) as out_def
from t
left join s on s.timesheet_id = t.timesheet_id
left join c on c.timesheet_id = t.timesheet_id
left join ids on ids.timesheet_id = t.timesheet_id
left join public.clients    cl on cl.id = ids.eff_client_id
left join public.candidates ca on ca.id = ids.eff_candidate_id
left join public.timesheets_financials tf
  on tf.timesheet_id = t.timesheet_id
 and tf.is_current = true
left join public.settings_defaults sd on sd.id = 1;
$function$;

-- timesheet_pdf_reference_rows(uuid)
CREATE OR REPLACE FUNCTION public.timesheet_pdf_reference_rows(p_timesheet_id uuid)
 RETURNS TABLE(timesheet_id uuid, sheet_scope text, submission_mode text, ref_target text, segment_id text, day_ymd text, start_utc text, end_utc text, current_reference text, row_key text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  r_ts record;
  r_seg record;

  v_tf_mode text;
  v_segments_json jsonb;
  v_sched_json jsonb;

  v_idx int;
  v_start_local text;
  v_end_local text;

  v_seg_id_local text;
  v_emitted int := 0;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_id is required';
  end if;

  -- Load the timesheet + current TSFIN snapshot (for SEGMENTS-mode refs when applicable)
  select
    ts.timesheet_id as ts_id,
    ts.sheet_scope as ts_sheet_scope,
    ts.submission_mode as ts_submission_mode,
    ts.reference_number as ts_reference_number,
    ts.week_ending_date as ts_week_ending_date,
    ts.worked_start_iso as ts_worked_start_iso,
    ts.worked_end_iso as ts_worked_end_iso,
    ts.scheduled_start_iso as ts_scheduled_start_iso,
    ts.scheduled_end_iso as ts_scheduled_end_iso,
    ts.actual_schedule_json as ts_actual_schedule_json,
    tf.invoice_breakdown_json as tf_invoice_breakdown_json
  into r_ts
  from public.timesheets ts
  left join public.timesheets_financials tf
    on tf.timesheet_id = ts.timesheet_id
   and tf.is_current = true
  where ts.timesheet_id = p_timesheet_id
  limit 1;

  if not found then
    return;
  end if;

  v_tf_mode := upper(coalesce(r_ts.tf_invoice_breakdown_json->>'mode',''));
  v_segments_json := r_ts.tf_invoice_breakdown_json->'segments';
  v_sched_json := r_ts.ts_actual_schedule_json;

  -- ------------------------------------------------------------
  -- A) SEGMENTS mode (NHSP/HR/etc): derive SEGMENT rows from TSFIN segments
  -- ------------------------------------------------------------
  if v_tf_mode = 'SEGMENTS'
     and jsonb_typeof(v_segments_json) = 'array'
  then
    for r_seg in
      select value as seg
      from jsonb_array_elements(v_segments_json) value
    loop
      if r_seg.seg is null or jsonb_typeof(r_seg.seg) <> 'object' then
        continue;
      end if;

      timesheet_id := r_ts.ts_id;
      sheet_scope := r_ts.ts_sheet_scope::text;
      submission_mode := r_ts.ts_submission_mode::text;
      ref_target := 'SEGMENT';

      day_ymd := nullif(btrim(coalesce(r_seg.seg->>'date','')), '');
      start_utc := nullif(btrim(coalesce(r_seg.seg->>'start_utc','')), '');
      end_utc := nullif(btrim(coalesce(r_seg.seg->>'end_utc','')), '');

      if day_ymd is null and start_utc is not null then
        begin
          day_ymd := (((start_utc::timestamptz) at time zone 'Europe/London')::date)::text;
        exception when others then
          day_ymd := null;
        end;
      end if;

      v_seg_id_local := nullif(btrim(coalesce(r_seg.seg->>'segment_id','')), '');
      segment_id := v_seg_id_local;

      if segment_id is null and start_utc is not null and end_utc is not null then
        segment_id := 'SE:' || start_utc || '|' || end_utc;
      end if;

      current_reference := nullif(btrim(coalesce(r_seg.seg->>'ref_num','')), '');

      row_key := r_ts.ts_id::text
        || '|' || coalesce(ref_target,'')
        || '|' || coalesce(segment_id,'')
        || '|' || coalesce(day_ymd,'')
        || '|' || coalesce(start_utc,'')
        || '|' || coalesce(end_utc,'');

      v_emitted := v_emitted + 1;
      return next;
    end loop;

    if v_emitted > 0 then
      return;
    end if;
  end if;

  -- ------------------------------------------------------------
  -- B) WEEKLY schedule-based rows from timesheets.actual_schedule_json
  -- ------------------------------------------------------------
  if r_ts.ts_sheet_scope::text = 'WEEKLY'
     and jsonb_typeof(v_sched_json) = 'array'
  then
    for r_seg in
      select value as seg, ordinality as idx
      from jsonb_array_elements(v_sched_json) with ordinality
    loop
      if r_seg.seg is null or jsonb_typeof(r_seg.seg) <> 'object' then
        continue;
      end if;

      v_start_local := nullif(btrim(coalesce(r_seg.seg->>'start','')), '');
      v_end_local   := nullif(btrim(coalesce(r_seg.seg->>'end','')), '');

      start_utc := nullif(btrim(coalesce(r_seg.seg->>'start_utc','')), '');
      end_utc   := nullif(btrim(coalesce(r_seg.seg->>'end_utc','')), '');

      if (v_start_local is null or v_end_local is null)
         and (start_utc is null or end_utc is null)
      then
        continue;
      end if;

      v_idx := (r_seg.idx - 1);

      timesheet_id := r_ts.ts_id;
      sheet_scope := r_ts.ts_sheet_scope::text;
      submission_mode := r_ts.ts_submission_mode::text;
      ref_target := 'SEGMENT';

      segment_id := nullif(btrim(coalesce(r_seg.seg->>'segment_id','')), '');
      if segment_id is null then
        segment_id := ('ts:' || r_ts.ts_id::text || ':' || v_idx::text);
      end if;

      day_ymd := nullif(btrim(coalesce(r_seg.seg->>'date','')), '');

      if day_ymd is null and start_utc is not null then
        begin
          day_ymd := (((start_utc::timestamptz) at time zone 'Europe/London')::date)::text;
        exception when others then
          day_ymd := null;
        end;
      end if;

      current_reference :=
        nullif(btrim(coalesce(r_seg.seg->>'ref_num','')), '');
      if current_reference is null then
        current_reference := nullif(btrim(coalesce(r_seg.seg->>'booking_ref','')), '');
      end if;

      row_key := r_ts.ts_id::text
        || '|' || coalesce(ref_target,'')
        || '|' || coalesce(segment_id,'')
        || '|' || coalesce(day_ymd,'')
        || '|' || coalesce(start_utc,'')
        || '|' || coalesce(end_utc,'');

      v_emitted := v_emitted + 1;
      return next;
    end loop;

    if v_emitted > 0 then
      return;
    end if;
  end if;

  -- ------------------------------------------------------------
  -- C) DAILY (or any fallback): single timesheet-level reference row
  -- ------------------------------------------------------------
  timesheet_id := r_ts.ts_id;
  sheet_scope := r_ts.ts_sheet_scope::text;
  submission_mode := r_ts.ts_submission_mode::text;
  ref_target := 'TIMESHEET';
  segment_id := null;
  day_ymd := null;

  if r_ts.ts_worked_start_iso is not null then
    day_ymd := ((r_ts.ts_worked_start_iso at time zone 'Europe/London')::date)::text;
  elsif r_ts.ts_scheduled_start_iso is not null then
    day_ymd := ((r_ts.ts_scheduled_start_iso at time zone 'Europe/London')::date)::text;
  elsif r_ts.ts_week_ending_date is not null then
    day_ymd := r_ts.ts_week_ending_date::text;
  end if;

  start_utc := coalesce(
    (to_jsonb(r_ts.ts_worked_start_iso)#>>'{}'),
    (to_jsonb(r_ts.ts_scheduled_start_iso)#>>'{}')
  );
  end_utc := coalesce(
    (to_jsonb(r_ts.ts_worked_end_iso)#>>'{}'),
    (to_jsonb(r_ts.ts_scheduled_end_iso)#>>'{}')
  );
  current_reference := nullif(btrim(coalesce(r_ts.ts_reference_number,'')), '');

  row_key := r_ts.ts_id::text
    || '|' || coalesce(ref_target,'')
    || '|' || coalesce(segment_id,'')
    || '|' || coalesce(day_ymd,'')
    || '|' || coalesce(start_utc,'')
    || '|' || coalesce(end_utc,'');

  return next;
end;
$function$;

-- timesheet_pdf_reference_sig(uuid)
CREATE OR REPLACE FUNCTION public.timesheet_pdf_reference_sig(p_timesheet_id uuid)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_raw_sig text;
  v_sig text;
begin
  if p_timesheet_id is null then
    return null;
  end if;

  select
    coalesce(
      string_agg(
        (r.row_key || '=' || coalesce(r.current_reference, '')),
        '||'
        order by r.row_key
      ),
      ''
    )
  into v_raw_sig
  from public.timesheet_pdf_reference_rows(p_timesheet_id) r;

  select
    encode(
      extensions.digest(coalesce(v_raw_sig, ''), 'sha256'),
      'hex'
    )
  into v_sig;

  return v_sig;
end;
$function$;

-- timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)
CREATE OR REPLACE FUNCTION public.timesheet_qr_refuse_and_reset(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_reason text, p_actor_user_id uuid)
 RETURNS TABLE(timesheet_id uuid, old_version integer, new_version integer, sheet_scope text, submission_mode text, qr_status text, qr_token text, processing_status text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_any public.timesheets%rowtype;
  v_current public.timesheets%rowtype;

  v_fin public.timesheets_financials%rowtype;

  v_booking_id text;          -- timesheets.booking_id is TEXT
  v_new_version int;
  v_new_timesheet_id uuid;
  v_candidate_rejection_enabled boolean := private._candidate_feature_enabled_current_v1('candidate_paper_qr');
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_id is required';
  end if;

  if p_expected_timesheet_id is null then
    raise exception '%',
      jsonb_build_object('error','EXPECTED_TIMESHEET_ID_REQUIRED')::text;
  end if;

  -- 0) Resolve booking_id from ANY row (stale or current)
  select t.*
  into v_any
  from public.timesheets t
  where t.timesheet_id = p_timesheet_id
  limit 1;

  if not found then
    raise exception 'Timesheet not found';
  end if;

  v_booking_id := v_any.booking_id;
  if v_booking_id is null or btrim(v_booking_id) = '' then
    raise exception 'Timesheet booking_id is missing; cannot rotate versions';
  end if;

  -- 1) Lock CURRENT timesheet row for this booking_id
  select t.*
  into v_current
  from public.timesheets t
  where t.booking_id = v_booking_id
    and t.is_current = true
  for update;

  if not found then
    raise exception 'Current timesheet not found for booking_id';
  end if;

  -- ✅ ATOMIC expected-guard (no side effects beyond row lock)
  if p_expected_timesheet_id <> v_current.timesheet_id then
    raise exception '%',
      jsonb_build_object(
        'error','TIMESHEET_MOVED',
        'current_timesheet_id', v_current.timesheet_id
      )::text;
  end if;

  -- 2) Lock CURRENT TSFIN row (keyed to CURRENT timesheet_id) and block if paid/invoiced
  select f.*
  into v_fin
  from public.timesheets_financials f
  where f.timesheet_id = v_current.timesheet_id
    and f.is_current = true
  for update;

  if found then
    if v_fin.locked_by_invoice_id is not null or v_fin.paid_at_utc is not null then
      raise exception 'Cannot refuse hours: timesheet is invoiced/locked or paid';
    end if;
  end if;

  v_new_version := coalesce(v_current.version, 1) + 1;

  -- 3) Rotate CURRENT version -> history (invalidate QR on historical)
  update public.timesheets as t
  set
    is_current      = false,
    status          = 'REVOKED'::public.timesheet_status_enum,
    revoked_reason  = nullif(btrim(p_reason), ''),
    revoked_by      = case when p_actor_user_id is null then null else p_actor_user_id::text end,
    qr_status       = 'CANCELLED'::public.timesheet_qr_status_enum,
    updated_at      = v_now
  where t.timesheet_id = v_current.timesheet_id
    and t.is_current = true;

  -- 4) Insert NEW current version = Scenario 1
  insert into public.timesheets as nt (
    booking_id,
    version,
    is_current,
    status,
    revoked_reason,
    revoked_by,
    contract_id,
    submission_mode,
    line_type,
    sheet_scope,

    occupant_key_norm,
    hospital_norm,
    ward_norm,
    job_title_norm,
    shift_label_norm,
    week_ending_date,

    worked_start_iso,
    worked_end_iso,
    break_start_iso,
    break_end_iso,
    break_minutes,

    actual_schedule_json,
    additional_units_week,
    additional_units_per_day,

    manual_pdf_r2_key,
    authorised_at_server,
    reference_number,
    day_references_json,

    qr_token,
    qr_status,
    qr_payload_json,
    qr_generated_at,
    qr_scanned_at,
    qr_scan_info_json,
    qr_r2_key,

    created_at,
    updated_at
  )
  values (
    v_current.booking_id,
    v_new_version,
    true,
    'RECEIVED'::public.timesheet_status_enum,
    null,
    null,
    v_current.contract_id,
    v_current.submission_mode,
    v_current.line_type,
    v_current.sheet_scope,

    v_current.occupant_key_norm,
    v_current.hospital_norm,
    v_current.ward_norm,
    v_current.job_title_norm,
    v_current.shift_label_norm,
    v_current.week_ending_date,

    null::timestamptz,
    null::timestamptz,
    null::timestamptz,
    null::timestamptz,
    null::int,

    null::jsonb,
    CASE WHEN v_candidate_rejection_enabled THEN '{}'::jsonb ELSE COALESCE(v_current.additional_units_week, '{}'::jsonb) END,
    CASE WHEN v_candidate_rejection_enabled THEN '{}'::jsonb ELSE COALESCE(v_current.additional_units_per_day, '{}'::jsonb) END,

    null::text,
    null::timestamptz,
    null::text,
    null::jsonb,

    null::text,
    'PENDING'::public.timesheet_qr_status_enum,
    '{}'::jsonb,
    null::timestamptz,
    null::timestamptz,
    null::jsonb,
    null::text,

    v_now,
    v_now
  )
  returning nt.timesheet_id into v_new_timesheet_id;

  if v_new_timesheet_id is null then
    raise exception 'Insert succeeded but no new timesheet_id returned';
  end if;

  -- 5) Move contract_week pointer (if any) from OLD current id -> NEW current id
  update public.contract_weeks as cw
  set timesheet_id = v_new_timesheet_id,
      status       = CASE WHEN v_candidate_rejection_enabled THEN 'OPEN'::public.contract_week_status_enum ELSE cw.status END,
      day_entries_json = CASE WHEN v_candidate_rejection_enabled THEN '[]'::jsonb ELSE cw.day_entries_json END,
      totals_json  = CASE WHEN v_candidate_rejection_enabled THEN '{}'::jsonb ELSE cw.totals_json END,
      updated_at   = v_now
  where cw.timesheet_id = v_current.timesheet_id;

  -- 6) Reset TSFIN in-place and MOVE it to new timesheet_id
  if v_fin.id is not null then
    update public.timesheets_financials as f
    set
      timesheet_id             = v_new_timesheet_id,
      timesheet_version        = v_new_version,
      processing_status        = CASE
        WHEN v_candidate_rejection_enabled THEN 'UNPROCESSED'::public.ts_fin_processing_status_enum
        ELSE 'UNASSIGNED'::public.ts_fin_processing_status_enum
      END,

      worked_start_iso         = CASE WHEN v_candidate_rejection_enabled THEN null ELSE f.worked_start_iso END,
      worked_end_iso           = CASE WHEN v_candidate_rejection_enabled THEN null ELSE f.worked_end_iso END,
      break_start_iso          = CASE WHEN v_candidate_rejection_enabled THEN null ELSE f.break_start_iso END,
      break_end_iso            = CASE WHEN v_candidate_rejection_enabled THEN null ELSE f.break_end_iso END,
      break_minutes            = CASE WHEN v_candidate_rejection_enabled THEN null ELSE f.break_minutes END,
      actual_schedule_json     = CASE WHEN v_candidate_rejection_enabled THEN null ELSE f.actual_schedule_json END,
      actual_minutes_by_day_json = CASE WHEN v_candidate_rejection_enabled THEN null ELSE f.actual_minutes_by_day_json END,

      hours_day                = 0,
      hours_night              = 0,
      hours_sat                = 0,
      hours_sun                = 0,
      hours_bh                 = 0,
      total_hours              = 0,

      total_pay_ex_vat         = 0,
      total_charge_ex_vat      = 0,
      margin_ex_vat            = 0,
      additional_pay_ex_vat    = 0,
      additional_charge_ex_vat = 0,
      additional_margin_ex_vat = 0,
      additional_units_json    = CASE WHEN v_candidate_rejection_enabled THEN '{}'::jsonb ELSE f.additional_units_json END,

      expenses_pay_ex_vat      = CASE WHEN v_candidate_rejection_enabled THEN 0 ELSE f.expenses_pay_ex_vat END,
      expenses_charge_ex_vat   = CASE WHEN v_candidate_rejection_enabled THEN 0 ELSE f.expenses_charge_ex_vat END,
      expenses_description     = CASE WHEN v_candidate_rejection_enabled THEN null ELSE f.expenses_description END,
      expenses_evidence_r2_key = CASE WHEN v_candidate_rejection_enabled THEN null ELSE f.expenses_evidence_r2_key END,
      expenses_evidence_manifest = CASE WHEN v_candidate_rejection_enabled THEN null ELSE f.expenses_evidence_manifest END,
      mileage_units            = CASE WHEN v_candidate_rejection_enabled THEN 0 ELSE f.mileage_units END,
      mileage_pay_ex_vat       = CASE WHEN v_candidate_rejection_enabled THEN 0 ELSE f.mileage_pay_ex_vat END,
      mileage_charge_ex_vat    = CASE WHEN v_candidate_rejection_enabled THEN 0 ELSE f.mileage_charge_ex_vat END,
      mileage_evidence_r2_key  = CASE WHEN v_candidate_rejection_enabled THEN null ELSE f.mileage_evidence_r2_key END,
      mileage_evidence_manifest = CASE WHEN v_candidate_rejection_enabled THEN null ELSE f.mileage_evidence_manifest END,
      travel_pay_ex_vat        = CASE WHEN v_candidate_rejection_enabled THEN 0 ELSE f.travel_pay_ex_vat END,
      travel_charge_ex_vat     = CASE WHEN v_candidate_rejection_enabled THEN 0 ELSE f.travel_charge_ex_vat END,
      accommodation_pay_ex_vat = CASE WHEN v_candidate_rejection_enabled THEN 0 ELSE f.accommodation_pay_ex_vat END,
      accommodation_charge_ex_vat = CASE WHEN v_candidate_rejection_enabled THEN 0 ELSE f.accommodation_charge_ex_vat END,
      other_pay_ex_vat         = CASE WHEN v_candidate_rejection_enabled THEN 0 ELSE f.other_pay_ex_vat END,
      other_charge_ex_vat      = CASE WHEN v_candidate_rejection_enabled THEN 0 ELSE f.other_charge_ex_vat END,

      authorised_at_utc        = null,
      authorised_by_user_id    = null,
      locked_by_invoice_id     = null,
      locked_at_utc            = null,
      paid_at_utc              = null,
      paid_by_user_id          = null,
      payment_reference        = null,

      updated_at               = v_now
    where f.id = v_fin.id;

    -- 7) Enqueue TSFIN recompute (optional)
    -- ✅ FIX: use constraint name to avoid ambiguous (timesheet_id, reason)
    insert into public.ts_financials_outbox(timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
    values (v_new_timesheet_id, 'REVOKED'::public.ts_fin_reason_enum, 0, v_now, null, v_now)
    on conflict on constraint uq_tsfin_outbox do nothing;
  end if;

  IF v_candidate_rejection_enabled THEN
    update public.timesheet_evidence as evidence_row
    set processing_state='SUPERSEDED'
    where evidence_row.timesheet_id=v_current.timesheet_id
      and evidence_row.processing_state<>'SUPERSEDED';
  END IF;

  -- 8) Audit event
  insert into public.audit_events(
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason,
    actor_user_id,
    ts_utc
  )
  values(
    'timesheet',
    v_new_timesheet_id::text,
    'QR_HOURS_REFUSED',
    jsonb_build_object(
      'booking_id', v_booking_id,
      'old_timesheet_id', v_current.timesheet_id::text,
      'old_version', v_current.version
    ),
    jsonb_build_object(
      'new_timesheet_id', v_new_timesheet_id::text,
      'new_version', v_new_version
    ),
    nullif(btrim(p_reason), ''),
    p_actor_user_id,
    v_now
  );

  -- Return
  timesheet_id := v_new_timesheet_id;
  old_version := v_current.version;
  new_version := v_new_version;
  sheet_scope := v_current.sheet_scope::text;
  submission_mode := v_current.submission_mode::text;
  qr_status := 'PENDING';
  qr_token := null;
  processing_status := CASE WHEN v_candidate_rejection_enabled THEN 'UNPROCESSED' ELSE 'UNASSIGNED' END;
  return next;
end;
$function$;

-- timesheet_qr_restore_version(uuid,uuid,text,uuid)
CREATE OR REPLACE FUNCTION public.timesheet_qr_restore_version(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_restore_kind text, p_actor_user_id uuid)
 RETURNS TABLE(timesheet_id uuid, restored_version integer, sheet_scope text, submission_mode text, qr_status text, qr_token text, restored_has_signed_pdf boolean)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  if private._candidate_feature_enabled_current_v1('candidate_route_confirmation') then
    raise exception 'QR_RESTORE_RETIRED_USE_FRESH_GENERATION'
      using errcode='55000',detail=jsonb_build_object(
        'code','QR_RESTORE_RETIRED_USE_FRESH_GENERATION',
        'allowed_actions',jsonb_build_array('ALLOW_QR_AGAIN','REISSUE_QR')
      )::text;
  end if;
  return query select * from private._timesheet_qr_restore_legacy_v1(
    p_timesheet_id,p_expected_timesheet_id,p_restore_kind,p_actor_user_id
  );
end;
$function$;

-- timesheet_qr_send_enqueue_v1(uuid,uuid,uuid,text,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.timesheet_qr_send_enqueue_v1(p_timesheet_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_idempotency_key text DEFAULT NULL::text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_timesheet_id uuid := NULL;
  v_requested_booking_id text := NULL;
  v_current_timesheet_id uuid := NULL;
  v_current_version integer := NULL;
  v_sheet_scope text := NULL;
  v_submission_mode text := NULL;
  v_contract_id uuid := NULL;
  v_week_ending_date date := NULL;
  v_worked_start_iso timestamp with time zone := NULL;
  v_worked_end_iso timestamp with time zone := NULL;
  v_actual_schedule_json jsonb := NULL;
  v_qr_status text := NULL;
  v_qr_token text := NULL;
  v_effective_qr_token text := NULL;
  v_qr_payload_json jsonb := '{}'::jsonb;
  v_payload_qr_token text := NULL;
  v_qr_generated_at timestamp with time zone := NULL;
  v_qr_scanned_at timestamp with time zone := NULL;
  v_qr_signed_hash text := NULL;
  v_qr_signed_at_utc timestamp with time zone := NULL;
  v_document_revision bigint := NULL;
  v_document_state text := NULL;
  v_updated_at timestamp with time zone := NULL;
  v_tsfin_id uuid := NULL;
  v_tsfin_processing_status text := NULL;
  v_locked_by_invoice_id uuid := NULL;
  v_paid_at_utc timestamp with time zone := NULL;
  v_invoice_breakdown_json jsonb := NULL;
  v_has_segment_invoice_lock boolean := FALSE;
  v_has_hours_for_send boolean := FALSE;
  v_candidate_id uuid := NULL;
  v_client_id uuid := NULL;
  v_candidate_email text := NULL;
  v_candidate_name text := NULL;
  v_candidate_opt_in_email boolean := TRUE;
  v_recipient_available boolean := FALSE;
  v_contract_candidate_id uuid := NULL;
  v_contract_client_id uuid := NULL;
  v_idempotency_key text := NULL;
  v_client_idempotency_key text := NULL;
  v_recipient_namespace text := NULL;
  v_mail_held_until_pdf_rendered boolean := TRUE;
  v_mail_hold_reason text := NULL;
  v_mail_scope_json jsonb := '{}'::jsonb;
  v_existing_mail_id uuid := NULL;
  v_existing_mail_status text := NULL;
  v_existing_mail_scope_json jsonb := '{}'::jsonb;
  v_mail_job_id uuid := NULL;
  v_pdf_job_id uuid := NULL;
  v_existing_pdf_job_id uuid := NULL;
  v_job_id uuid := NULL;
  v_send_state text := NULL;
  v_pdf_key text := NULL;
  v_document_idempotency text := NULL;
  v_document_operation_id uuid := NULL;
  v_document_version_id uuid := NULL;
  v_document_version_status text := NULL;
  v_snapshot_json jsonb := '{}'::jsonb;
  v_snapshot_model jsonb := '{}'::jsonb;
  v_snapshot_hash text := NULL;
  v_snapshot_valid boolean := FALSE;
  v_snapshot_error_code text := NULL;
  v_week_period_hash text := NULL;
  v_schedule_hash text := NULL;
  v_reference_signature text := NULL;
  v_additional_units_hash text := NULL;
  v_presentation_settings_hash text := NULL;
  v_qr_payload_hash text := NULL;
  v_complete_printable_content_hash text := NULL;
  v_issue_type text := NULL;
  v_rotate_token boolean := FALSE;
  v_mail_subject text := NULL;
  v_mail_body_text text := NULL;
  v_mail_body_html text := NULL;
  v_mail_reference text := NULL;
  v_mail_scheduled_for_utc timestamp with time zone := NULL;
  v_created_by_user_id uuid := NULL;
  v_post_row jsonb := NULL;
  v_row_key text := NULL;
  v_storage_key text := NULL;
  v_row_signature text := NULL;
  v_candidate_paper_workflow_count integer := 0;
  v_candidate_paper_workflow_id uuid := NULL;
  v_candidate_paper_workflow_generation integer := NULL;
  v_candidate_paper_manifest_sha256 text := NULL;
  v_candidate_paper_binding_json jsonb := '{}'::jsonb;
  v_candidate_paper_exact_mail_exists boolean := FALSE;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_ID_REQUIRED',
      'message', 'p_timesheet_id is required.',
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT requested_ts.timesheet_id,
         requested_ts.booking_id
    INTO v_requested_timesheet_id,
         v_requested_booking_id
  FROM public.timesheets AS requested_ts
  WHERE requested_ts.timesheet_id = p_timesheet_id
  LIMIT 1;

  IF v_requested_timesheet_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_NOT_FOUND',
      'message', 'Timesheet was not found.',
      'requested_timesheet_id', p_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT current_ts.timesheet_id,
         current_ts.version
    INTO v_current_timesheet_id,
         v_current_version
  FROM public.timesheets AS current_ts
  WHERE current_ts.booking_id = v_requested_booking_id
    AND current_ts.is_current = TRUE
  ORDER BY current_ts.version DESC NULLS LAST,
           current_ts.updated_at DESC NULLS LAST,
           current_ts.created_at DESC NULLS LAST,
           current_ts.timesheet_id DESC
  LIMIT 1;

  IF v_current_timesheet_id IS NULL THEN
    v_current_timesheet_id := v_requested_timesheet_id;
  END IF;

  IF p_expected_timesheet_id IS NOT NULL AND p_expected_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'Timesheet has moved to a newer current row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_requested_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'timesheet_qr_send_enqueue_v1 requires the current timesheet row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', COALESCE(p_expected_timesheet_id, p_timesheet_id),
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT ts_current.sheet_scope::text,
         ts_current.submission_mode::text,
         ts_current.contract_id,
         ts_current.week_ending_date,
         ts_current.worked_start_iso,
         ts_current.worked_end_iso,
         ts_current.actual_schedule_json,
         ts_current.qr_status::text,
         ts_current.qr_token,
         ts_current.qr_payload_json,
         ts_current.qr_generated_at,
         ts_current.qr_scanned_at,
         ts_current.qr_signed_hash,
         ts_current.qr_signed_at_utc,
         ts_current.document_revision,
         ts_current.document_state::text,
         ts_current.version,
         ts_current.updated_at
    INTO v_sheet_scope,
         v_submission_mode,
         v_contract_id,
         v_week_ending_date,
         v_worked_start_iso,
         v_worked_end_iso,
         v_actual_schedule_json,
         v_qr_status,
         v_qr_token,
         v_qr_payload_json,
         v_qr_generated_at,
         v_qr_scanned_at,
         v_qr_signed_hash,
         v_qr_signed_at_utc,
         v_document_revision,
         v_document_state,
         v_current_version,
         v_updated_at
  FROM public.timesheets AS ts_current
  WHERE ts_current.timesheet_id = v_current_timesheet_id
    AND ts_current.is_current = TRUE
  LIMIT 1
  FOR UPDATE;

  IF v_sheet_scope IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'CURRENT_TIMESHEET_NOT_FOUND',
      'message', 'Current timesheet was not found.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_sheet_scope, '')) NOT IN ('DAILY', 'WEEKLY') THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'UNSUPPORTED_SHEET_SCOPE',
      'message', 'QR send is only supported for DAILY or WEEKLY timesheets.',
      'current_timesheet_id', v_current_timesheet_id,
      'sheet_scope', v_sheet_scope,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_submission_mode, '')) <> 'MANUAL' THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NOT_MANUAL_QR_ROUTE',
      'message', 'QR send requires a MANUAL submission-mode timesheet with QR enabled.',
      'current_timesheet_id', v_current_timesheet_id,
      'submission_mode', v_submission_mode,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_qr_status, '')) <> 'PENDING' THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'QR_NOT_PENDING',
      'message', 'QR send requires qr_status=PENDING.',
      'current_timesheet_id', v_current_timesheet_id,
      'qr_status', v_qr_status,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_qr_scanned_at IS NOT NULL OR NULLIF(BTRIM(COALESCE(v_qr_signed_hash, '')), '') IS NOT NULL OR v_qr_signed_at_utc IS NOT NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'QR_ALREADY_SIGNED',
      'message', 'Cannot queue QR send: timesheet already has signed QR markers.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  WITH locked_candidate_paper AS MATERIALIZED (
    SELECT workflow.id,
           workflow.generation,
           workflow.paper_return_manifest_sha256
    FROM public.candidate_submission_workflows AS workflow
    WHERE workflow.route = 'PAPER'
      AND workflow.state = 'AWAITING_PAPER_RETURN'
      AND (
        workflow.target_timesheet_id = v_current_timesheet_id
        OR workflow.anchor_timesheet_id = v_current_timesheet_id
      )
    ORDER BY workflow.id
    FOR UPDATE
  )
  SELECT count(*)::integer,
         (array_agg(locked_candidate_paper.id ORDER BY locked_candidate_paper.id))[1],
         (array_agg(locked_candidate_paper.generation ORDER BY locked_candidate_paper.id))[1],
         (array_agg(encode(locked_candidate_paper.paper_return_manifest_sha256, 'hex')
                    ORDER BY locked_candidate_paper.id))[1]
    INTO v_candidate_paper_workflow_count,
         v_candidate_paper_workflow_id,
         v_candidate_paper_workflow_generation,
         v_candidate_paper_manifest_sha256
  FROM locked_candidate_paper;

  IF v_candidate_paper_workflow_count > 1 THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'CANDIDATE_PAPER_WORKFLOW_CONFLICT',
      'message', 'More than one active Candidate PAPER workflow targets this timesheet.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_candidate_paper_workflow_count = 1 THEN
    IF v_candidate_paper_workflow_generation IS NULL
       OR v_candidate_paper_workflow_generation < 1
       OR COALESCE(v_candidate_paper_manifest_sha256, '') !~ '^[0-9a-f]{64}$' THEN
      RETURN jsonb_build_object(
        'ok', FALSE,
        'queued', FALSE,
        'operation', 'timesheet_qr_send_enqueue',
        'error_code', 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE',
        'message', 'The Candidate PAPER workflow does not have a valid frozen return manifest.',
        'current_timesheet_id', v_current_timesheet_id,
        'recipient_available', FALSE,
        'send_state', 'REJECTED'
      );
    END IF;
    v_candidate_paper_binding_json := jsonb_build_object(
      'candidate_mail_authority', 'CANDIDATE_PAPER_V1',
      'candidate_workflow_id', v_candidate_paper_workflow_id,
      'candidate_workflow_generation', v_candidate_paper_workflow_generation,
      'paper_return_manifest_sha256', v_candidate_paper_manifest_sha256,
      'candidate_paper_pack_ready', FALSE,
      'candidate_paper_pack_retryable', FALSE,
      'candidate_paper_pack_failure_class', NULL,
      'candidate_paper_pack_failure_code', NULL,
      'candidate_paper_pack_failure_contract_version', NULL,
      'candidate_paper_pack_failed_at_utc', NULL,
      'candidate_paper_pack_preparation_started_at_utc', p_now_utc,
      'candidate_paper_pack_preparation_deadline_at_utc', p_now_utc + interval '15 minutes',
      'candidate_paper_pack_attempt_count', 0,
      'candidate_paper_pack_attempt_token', NULL,
      'candidate_paper_pack_attempt_expires_at_utc', NULL,
      'candidate_paper_pack_next_retry_at_utc', NULL
    );
  END IF;

  SELECT tf_current.id,
         tf_current.processing_status::text,
         tf_current.locked_by_invoice_id,
         tf_current.paid_at_utc,
         tf_current.invoice_breakdown_json,
         tf_current.candidate_id,
         tf_current.client_id
    INTO v_tsfin_id,
         v_tsfin_processing_status,
         v_locked_by_invoice_id,
         v_paid_at_utc,
         v_invoice_breakdown_json,
         v_candidate_id,
         v_client_id
  FROM public.timesheets_financials AS tf_current
  WHERE tf_current.timesheet_id = v_current_timesheet_id
    AND tf_current.is_current = TRUE
  ORDER BY tf_current.computed_at_utc DESC NULLS LAST,
           tf_current.created_at DESC NULLS LAST,
           tf_current.updated_at DESC NULLS LAST,
           tf_current.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_tsfin_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NO_TSFIN',
      'message', 'No current financial snapshot exists for this timesheet.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'array' THEN v_invoice_breakdown_json
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_invoice_breakdown_json->'segments') = 'array' THEN v_invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_locked_by_invoice_id IS NOT NULL OR v_paid_at_utc IS NOT NULL OR v_has_segment_invoice_lock = TRUE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_LOCKED_OR_PAID',
      'message', 'Cannot queue QR send: timesheet is locked, invoice-locked, or paid.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(COALESCE(v_actual_schedule_json, '[]'::jsonb)) = 'array' THEN COALESCE(v_actual_schedule_json, '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) AS schedule_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(schedule_segment.segment_json->>'start', schedule_segment.segment_json->>'start_utc', '')), '') IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(schedule_segment.segment_json->>'end', schedule_segment.segment_json->>'end_utc', '')), '') IS NOT NULL
    ) OR EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN v_invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(v_invoice_breakdown_json) = 'array' THEN v_invoice_breakdown_json
          WHEN jsonb_typeof(v_invoice_breakdown_json) = 'object'
           AND jsonb_typeof(v_invoice_breakdown_json->'segments') = 'array' THEN v_invoice_breakdown_json->'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
    ) INTO v_has_hours_for_send;
  ELSE
    v_has_hours_for_send := v_worked_start_iso IS NOT NULL AND v_worked_end_iso IS NOT NULL;
  END IF;

  IF COALESCE(v_has_hours_for_send, FALSE) = FALSE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NO_HOURS_RECORDED',
      'message', 'Cannot queue QR send: no hours are recorded yet.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_contract_id IS NOT NULL THEN
    SELECT contract_row.candidate_id,
           contract_row.client_id
      INTO v_contract_candidate_id,
           v_contract_client_id
    FROM public.contracts AS contract_row
    WHERE contract_row.id = v_contract_id
    LIMIT 1;
  END IF;

  v_candidate_id := COALESCE(v_contract_candidate_id, v_candidate_id);
  v_client_id := COALESCE(v_contract_client_id, v_client_id);

  IF v_candidate_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'CANDIDATE_NOT_FOUND',
      'message', 'Cannot queue QR send: candidate could not be resolved.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT NULLIF(BTRIM(candidate_row.email), ''),
         COALESCE(NULLIF(BTRIM(candidate_row.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(candidate_row.first_name), ''), NULLIF(BTRIM(candidate_row.last_name), ''))), '')),
         COALESCE(candidate_row.opt_in_email, TRUE)
    INTO v_candidate_email,
         v_candidate_name,
         v_candidate_opt_in_email
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = v_candidate_id
  LIMIT 1;

  v_recipient_available := NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NOT NULL AND COALESCE(v_candidate_opt_in_email, TRUE) = TRUE;

  IF v_recipient_available = FALSE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', CASE WHEN NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NULL THEN 'CANDIDATE_EMAIL_MISSING' ELSE 'CANDIDATE_EMAIL_OPTED_OUT' END,
      'message', CASE WHEN NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NULL THEN 'Cannot queue QR send: candidate email is missing.' ELSE 'Cannot queue QR send: candidate has opted out of email.' END,
      'current_timesheet_id', v_current_timesheet_id,
      'candidate_id', v_candidate_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  v_payload_qr_token := CASE
    WHEN jsonb_typeof(v_qr_payload_json) = 'object' THEN NULLIF(BTRIM(COALESCE(v_qr_payload_json->>'tok', '')), '')
    ELSE NULL
  END;
  IF v_candidate_paper_workflow_count = 1 THEN
    SELECT EXISTS(
      SELECT 1
      FROM public.mail_outbox candidate_mail
      WHERE candidate_mail.type='TIMESHEET_QR'
        AND candidate_mail.context_kind='timesheets'
        AND candidate_mail.context_id=v_current_timesheet_id
        AND candidate_mail.payment_scope_json->>'candidate_workflow_id'=v_candidate_paper_workflow_id::text
        AND candidate_mail.payment_scope_json->>'candidate_workflow_generation'=v_candidate_paper_workflow_generation::text
        AND lower(coalesce(candidate_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
              =v_candidate_paper_manifest_sha256
        AND lower(coalesce(candidate_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
              IN ('false','f','0','no')
    ) INTO v_candidate_paper_exact_mail_exists;
  END IF;
  v_rotate_token :=
    NULLIF(BTRIM(COALESCE(v_qr_token, '')), '') IS NULL
    OR UPPER(COALESCE(v_document_state, 'STALE')) IN ('STALE', 'FAILED')
    OR (v_candidate_paper_workflow_count = 1 AND NOT v_candidate_paper_exact_mail_exists);
  v_effective_qr_token := CASE
    WHEN v_rotate_token THEN gen_random_uuid()::text
    ELSE COALESCE(NULLIF(BTRIM(COALESCE(v_qr_token, '')), ''),
      v_payload_qr_token, gen_random_uuid()::text)
  END;
  v_qr_payload_json := jsonb_build_object('v', 1, 'tok', v_effective_qr_token);
  v_issue_type := CASE
    WHEN NULLIF(BTRIM(COALESCE(v_qr_token, '')), '') IS NULL THEN 'NEW_ISSUE'
    WHEN v_rotate_token THEN 'REISSUED_CHANGED'
    ELSE 'RESENT_UNCHANGED'
  END;

  UPDATE public.timesheets AS ts_qr_update
     SET qr_token = v_effective_qr_token,
         qr_payload_json = v_qr_payload_json,
         qr_generated_at = CASE
           WHEN v_rotate_token THEN v_now
           ELSE COALESCE(ts_qr_update.qr_generated_at, v_now) END,
         qr_r2_key = NULL,
         qr_scanned_at = NULL,
         qr_scan_info_json = NULL,
         updated_at = v_now
   WHERE ts_qr_update.timesheet_id = v_current_timesheet_id
     AND ts_qr_update.is_current = TRUE
  RETURNING ts_qr_update.qr_generated_at,
            ts_qr_update.updated_at,
            ts_qr_update.document_revision
       INTO v_qr_generated_at,
            v_updated_at,
            v_document_revision;

  SELECT t.document_revision
    INTO v_document_revision
  FROM public.timesheets t
  WHERE t.timesheet_id=v_current_timesheet_id AND t.is_current
  LIMIT 1;

  IF UPPER(COALESCE(v_tsfin_processing_status, '')) <> 'AWAITING_MANUAL_SIGNATURE' THEN
    UPDATE public.timesheets_financials AS tf_update
       SET processing_status = 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum,
           updated_at = v_now
     WHERE tf_update.id = v_tsfin_id
       AND tf_update.is_current = TRUE;
  END IF;

  SELECT tms_user_check.id
    INTO v_created_by_user_id
  FROM public.tms_users AS tms_user_check
  WHERE tms_user_check.id = p_actor_user_id
  LIMIT 1;

  v_client_idempotency_key := NULLIF(
    REGEXP_REPLACE(
      LOWER(BTRIM(COALESCE(p_idempotency_key, ''))),
      '[^a-z0-9._:-]+',
      '-',
      'g'
    ),
    ''
  );

  IF v_client_idempotency_key IS NULL THEN
    v_client_idempotency_key := 'auto:' || md5(CONCAT_WS('|',
      v_current_timesheet_id::text,
      COALESCE(v_current_version::text, ''),
      LOWER(COALESCE(v_candidate_email, ''))
    ));
  END IF;

  v_recipient_namespace := md5(LOWER(COALESCE(v_candidate_email, '')));

  v_idempotency_key := CASE
    WHEN v_candidate_paper_workflow_count = 1 THEN
      'candidate_paper_send:'||v_candidate_paper_workflow_id::text
      ||':g'||v_candidate_paper_workflow_generation::text
      ||':manifest:'||v_candidate_paper_manifest_sha256
      ||':timesheet:'||v_current_timesheet_id::text
      ||':recipient:'||v_recipient_namespace
    ELSE
      'timesheet_qr_send:'||v_current_timesheet_id::text
      ||':v'||COALESCE(v_current_version::text,'0')
      ||':recipient:'||v_recipient_namespace
      ||':key:'||md5(v_client_idempotency_key)
    END;

  PERFORM pg_advisory_xact_lock(hashtext('timesheet_qr_send:' || v_current_timesheet_id::text));
  PERFORM pg_advisory_xact_lock(hashtext(v_idempotency_key));

  v_pdf_key := 'docs-pdf/timesheets/ts_' || v_current_timesheet_id::text || '.pdf';
  v_mail_reference := v_idempotency_key;
  v_mail_scheduled_for_utc := TIMESTAMPTZ '9999-12-31 00:00:00+00';
  v_mail_subject := CASE
    WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Weekly QR timesheet – week ending ' || COALESCE(v_week_ending_date::text, '(unknown)')
    ELSE 'Daily QR timesheet – ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)')
  END;
  v_mail_body_text := CONCAT_WS(E'\n',
    'Please print the attached timesheet, ask the ward manager to sign it,',
    'and then upload the signed copy via the app.',
    CASE
      WHEN private._candidate_feature_enabled_current_v1('candidate_paper_qr')
       AND COALESCE(v_current_version,1)>1
      THEN 'Please remember to sign the replacement timesheet before returning it.'
      ELSE NULL
    END,
    '',
    CASE WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Week ending: ' || COALESCE(v_week_ending_date::text, '(unknown)') ELSE 'Date: ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)') END,
    'Timesheet ID: ' || v_current_timesheet_id::text
  );
  v_mail_body_html := '<p>Please print the attached timesheet, ask the ward manager to sign it, and then upload the signed copy via the app.<br/>'
    || CASE
      WHEN private._candidate_feature_enabled_current_v1('candidate_paper_qr')
       AND COALESCE(v_current_version,1)>1
      THEN 'Please remember to sign the replacement timesheet before returning it.<br/>'
      ELSE ''
    END
    || '<br/>'
    || CASE WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Week ending: ' || COALESCE(v_week_ending_date::text, '(unknown)') ELSE 'Date: ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)') END
    || '<br/>Timesheet ID: ' || v_current_timesheet_id::text || '</p>';

  SELECT p.presentation_model,p.snapshot_json,p.snapshot_hash,p.valid,p.error_code
    INTO v_snapshot_model,v_snapshot_json,v_snapshot_hash,
      v_snapshot_valid,v_snapshot_error_code
  FROM private._invoice_presentation_snapshot_batch(
    jsonb_build_array(jsonb_build_object(
      'request_key','timesheet-qr:'||v_current_timesheet_id::text,
      'entity_type','TIMESHEET',
      'entity_id',v_current_timesheet_id,
      'purpose','TIMESHEET',
      'template_version','timesheet-professional-v2')),
    v_now
  ) p
  LIMIT 1;

  IF NOT COALESCE(v_snapshot_valid,FALSE)
     OR v_snapshot_model->>'schema_version'<>'TIMESHEET_RENDER_MODEL_V2'
     OR v_snapshot_model->>'form_variant'<>'QR_UNSIGNED' THEN
    RETURN jsonb_build_object(
      'ok',FALSE,'queued',FALSE,
      'operation','timesheet_qr_send_enqueue',
      'error_code',coalesce(v_snapshot_error_code,
        'TIMESHEET_PRESENTATION_SNAPSHOT_INVALID'),
      'message','The official QR timesheet could not be frozen safely.',
      'current_timesheet_id',v_current_timesheet_id,
      'recipient_available',TRUE,'send_state','REJECTED');
  END IF;

  v_week_period_hash := coalesce(
    nullif(v_snapshot_model->>'week_period_hash',''),
    encode(extensions.digest(convert_to(coalesce(
      v_snapshot_model->'week_period','{}'::jsonb)::text,'UTF8'),'sha256'),'hex'));
  v_schedule_hash := encode(extensions.digest(convert_to(
    coalesce(v_snapshot_model#>'{week_period,days}','[]'::jsonb)::text,
    'UTF8'),'sha256'),'hex');
  v_reference_signature := coalesce(
    nullif(v_snapshot_model->>'reference_signature',''),
    encode(extensions.digest(convert_to(coalesce(v_snapshot_model#>'{week_period,days}',
      '[]'::jsonb)::text,'UTF8'),'sha256'),'hex'));
  v_additional_units_hash := coalesce(
    nullif(v_snapshot_model->>'additional_units_hash',''),
    encode(extensions.digest(convert_to(coalesce(v_snapshot_model#>'{additional_units_section,rows}',
      '[]'::jsonb)::text,'UTF8'),'sha256'),'hex'));
  v_presentation_settings_hash := coalesce(
    nullif(v_snapshot_model->>'presentation_settings_hash',''),
    encode(extensions.digest(convert_to(jsonb_build_object(
      'branding',v_snapshot_model->'branding',
      'wording',v_snapshot_model->'wording')::text,'UTF8'),'sha256'),'hex'));
  v_qr_payload_hash := encode(extensions.digest(
    convert_to(v_qr_payload_json::text,'UTF8'),'sha256'),'hex');
  v_complete_printable_content_hash := encode(extensions.digest(convert_to(
    concat_ws('|',v_snapshot_hash,v_week_period_hash,v_schedule_hash,
      v_reference_signature,v_additional_units_hash,
      v_presentation_settings_hash,v_qr_payload_hash,
      'timesheet-professional-v2'),'UTF8'),'sha256'),'hex');
  v_document_idempotency := encode(extensions.digest(convert_to(concat_ws('|',
    'BUILD_DOCUMENT','TIMESHEET',v_current_timesheet_id::text,'TIMESHEET',
    v_document_revision::text,'timesheet-professional-v2',
    v_qr_payload_hash,v_complete_printable_content_hash),'UTF8'),'sha256'),'hex');

  SELECT v.id,v.operation_id,v.status::text,v.r2_key
    INTO v_document_version_id,v_document_operation_id,
      v_document_version_status,v_pdf_key
  FROM public.invoice_document_versions v
  WHERE v.entity_type='TIMESHEET' AND v.entity_id=v_current_timesheet_id
    AND v.purpose='TIMESHEET'
    AND v.source_revision=v_document_revision::text
    AND v.template_version='timesheet-professional-v2'
    AND v.status IN(
      'PLANNING','WAITING_FOR_INPUTS','RENDERING',
      'ASSEMBLING','VERIFYING','READY')
  ORDER BY (v.status='READY') DESC,v.created_at_utc DESC,v.id DESC
  LIMIT 1;

  IF v_document_version_id IS NULL THEN
    INSERT INTO public.invoice_operations(
      operation_type,entity_type,entity_id,actor_user_id,idempotency_key,
      status,phase,priority,source_revision,template_version,input_json,
      config_json,progress_json,total_units,chunk_count,control_version,
      change_seq,created_at_utc,updated_at_utc
    ) VALUES(
      'BUILD_DOCUMENT','TIMESHEET',v_current_timesheet_id,p_actor_user_id,
      v_document_idempotency,'QUEUED','BUILD_MANIFEST',550,
      v_document_revision::text,'timesheet-professional-v2',
      jsonb_build_object(
        'entity_type','TIMESHEET','entity_id',v_current_timesheet_id,
        'purpose','TIMESHEET','source_revision',v_document_revision,
        'template_version','timesheet-professional-v2',
        'qr_payload_hash',v_qr_payload_hash,
        'printable_content_hash',v_complete_printable_content_hash),
      jsonb_build_object('processor_policy',private._invoice_processor_limits()),
      jsonb_build_object('status_message','Official QR timesheet queued'),
      1,1,1,nextval('public.invoice_operation_change_seq'),v_now,v_now
    )
    ON CONFLICT DO NOTHING
    RETURNING id INTO v_document_operation_id;

    IF v_document_operation_id IS NULL THEN
      SELECT o.id INTO v_document_operation_id
      FROM public.invoice_operations o
      WHERE o.idempotency_key=v_document_idempotency
        AND o.status IN('QUEUED','RUNNING','WAITING','RETRY_WAIT')
      ORDER BY o.created_at_utc DESC,o.id DESC
      LIMIT 1;
    END IF;

    INSERT INTO public.invoice_document_versions(
      entity_type,entity_id,purpose,operation_id,source_revision,
      template_version,status,snapshot_json,snapshot_hash,
      manifest_json,manifest_hash,created_at_utc
    ) VALUES(
      'TIMESHEET',v_current_timesheet_id,'TIMESHEET',
      v_document_operation_id,v_document_revision::text,
      'timesheet-professional-v2','PLANNING',
      v_snapshot_json,v_snapshot_hash,'[]'::jsonb,
      encode(extensions.digest(convert_to('[]','UTF8'),'sha256'),'hex'),v_now
    )
    ON CONFLICT(entity_type,entity_id,purpose,source_revision,template_version)
      WHERE purpose IN('DRAFT_PREVIEW','TIMESHEET')
        AND status IN(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING',
          'ASSEMBLING','VERIFYING','READY')
    DO NOTHING
    RETURNING id,status::text,r2_key
      INTO v_document_version_id,v_document_version_status,v_pdf_key;

    IF v_document_version_id IS NULL THEN
      SELECT v.id,v.operation_id,v.status::text,v.r2_key
        INTO v_document_version_id,v_document_operation_id,
          v_document_version_status,v_pdf_key
      FROM public.invoice_document_versions v
      WHERE v.entity_type='TIMESHEET' AND v.entity_id=v_current_timesheet_id
        AND v.purpose='TIMESHEET'
        AND v.source_revision=v_document_revision::text
        AND v.template_version='timesheet-professional-v2'
        AND v.status IN(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING',
          'ASSEMBLING','VERIFYING','READY')
      ORDER BY (v.status='READY') DESC,v.created_at_utc DESC,v.id DESC
      LIMIT 1;
    END IF;

    INSERT INTO public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,
      operation_control_version,created_at_utc,updated_at_utc
    )
    SELECT v_document_operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
      encode(extensions.digest(convert_to(concat_ws('|','DOCUMENT_PLAN',
        v_document_version_id::text,v_document_revision::text,
        'timesheet-professional-v2','1'),'UTF8'),'sha256'),'hex'),
      0,'TIMESHEET',v_current_timesheet_id,v_document_version_id,
      'QUEUED',550,v_now,
      jsonb_build_object(
        'purpose','TIMESHEET','source_revision',v_document_revision,
        'template_version','timesheet-professional-v2',
        'qr_payload_hash',v_qr_payload_hash,
        'printable_content_hash',v_complete_printable_content_hash),
      o.control_version,v_now,v_now
    FROM public.invoice_operations o
    WHERE o.id=v_document_operation_id
    ON CONFLICT(operation_id,chunk_type,level_no,sequence_no,work_key)
      DO NOTHING;
  END IF;

  UPDATE public.timesheets t
  SET current_document_version_id=v_document_version_id,
      active_document_operation_id=case
        when v_document_version_status='READY' then null
        else v_document_operation_id end,
      document_state=case
        when v_document_version_status='READY' then 'READY'
        else 'QUEUED' end,
      last_document_error_json=null,updated_at=v_now
  WHERE t.timesheet_id=v_current_timesheet_id AND t.is_current
    AND t.document_revision=v_document_revision;

  v_pdf_job_id := v_document_operation_id;
  v_mail_held_until_pdf_rendered :=
    v_candidate_paper_workflow_count = 1
    OR COALESCE(v_document_version_status,'')<>'READY';
  v_mail_hold_reason := CASE
    WHEN v_candidate_paper_workflow_count = 1 THEN 'CANDIDATE_PAPER_PACK_PENDING'
    WHEN v_mail_held_until_pdf_rendered THEN 'PDF_RENDER_PENDING'
    ELSE NULL
  END;
  v_mail_scheduled_for_utc := CASE
    WHEN v_mail_held_until_pdf_rendered THEN
      TIMESTAMPTZ '9999-12-31 00:00:00+00'
    ELSE v_now
  END;

  v_mail_scope_json := jsonb_build_object(
    'job_kind', 'TIMESHEET_QR_SEND',
    'document_operation_id',v_document_operation_id,
    'document_version_id',v_document_version_id,
    'document_revision',v_document_revision,
    'template_version','timesheet-professional-v2',
    'idempotency_key', v_idempotency_key,
    'client_idempotency_key', v_client_idempotency_key,
    'requires_pdf_render', TRUE,
    'release_mail_after_pdf_render', TRUE,
    'mail_delayed_for_pdf_render',v_mail_held_until_pdf_rendered,
    'mail_held_until_pdf_rendered',v_mail_held_until_pdf_rendered,
    'mail_hold_reason',v_mail_hold_reason,
    'pdf_storage_key', v_pdf_key,
    'current_timesheet_id', v_current_timesheet_id::text,
    'current_version', v_current_version,
    'qr_token_hash',encode(extensions.digest(
      convert_to(v_effective_qr_token,'UTF8'),'sha256'),'hex'),
    'qr_payload_hash',v_qr_payload_hash,
    'week_period_hash',v_week_period_hash,
    'schedule_hash',v_schedule_hash,
    'reference_signature',v_reference_signature,
    'additional_units_hash',v_additional_units_hash,
    'presentation_settings_hash',v_presentation_settings_hash,
    'printable_content_hash',v_complete_printable_content_hash,
    'recipient_email', v_candidate_email
  ) || v_candidate_paper_binding_json;

  SELECT mail_existing.id,
         mail_existing.status::text,
         COALESCE(mail_existing.payment_scope_json, '{}'::jsonb)
    INTO v_existing_mail_id,
         v_existing_mail_status,
         v_existing_mail_scope_json
  FROM public.mail_outbox AS mail_existing
  WHERE mail_existing.type = 'TIMESHEET_QR'
    AND mail_existing.reference = v_mail_reference
    AND mail_existing.context_kind = 'timesheets'
    AND mail_existing.context_id = v_current_timesheet_id
    AND mail_existing."to" = v_candidate_email
  ORDER BY mail_existing.created_at_utc DESC,
           mail_existing.id DESC
  LIMIT 1;

  IF v_existing_mail_id IS NOT NULL THEN
    v_mail_job_id := v_existing_mail_id;

    IF v_candidate_paper_workflow_count = 1
       AND NULLIF(BTRIM(COALESCE(v_existing_mail_scope_json->>'candidate_workflow_id', '')), '') IS NOT NULL
       AND (
         v_existing_mail_scope_json->>'candidate_workflow_id' IS DISTINCT FROM v_candidate_paper_workflow_id::text
         OR COALESCE(v_existing_mail_scope_json->>'candidate_workflow_generation', '') IS DISTINCT FROM v_candidate_paper_workflow_generation::text
         OR LOWER(COALESCE(v_existing_mail_scope_json->>'paper_return_manifest_sha256', ''))
              IS DISTINCT FROM v_candidate_paper_manifest_sha256
       ) THEN
      RETURN jsonb_build_object(
        'ok', FALSE,
        'queued', FALSE,
        'operation', 'timesheet_qr_send_enqueue',
        'error_code', 'CANDIDATE_PAPER_OUTBOX_IDENTITY_CONFLICT',
        'message', 'The existing QR email is bound to a different Candidate PAPER workflow.',
        'current_timesheet_id', v_current_timesheet_id,
        'mail_outbox_id', v_existing_mail_id,
        'recipient_available', FALSE,
        'send_state', 'REJECTED'
      );
    ELSIF UPPER(COALESCE(v_existing_mail_status, '')) = 'SENT' THEN
      v_send_state := 'ALREADY_SENT';
    ELSIF v_candidate_paper_workflow_count = 1
          AND UPPER(COALESCE(v_existing_mail_status, '')) = 'FAILED' THEN
      v_send_state := 'CANDIDATE_PAPER_MAIL_FAILED';
    ELSIF NULLIF(BTRIM(COALESCE(v_existing_mail_scope_json->>'candidate_workflow_id', '')), '') IS NOT NULL
          AND (
            LOWER(COALESCE(v_existing_mail_scope_json->>'candidate_paper_pack_ready', 'false'))
              IN ('true','t','1','yes')
            OR v_candidate_paper_workflow_count = 0
          ) THEN
      -- A Candidate complete pack (or a historical Candidate binding) must never
      -- be replaced by the ordinary one-page QR attachment on enqueue replay.
      v_send_state := 'CANDIDATE_PAPER_OUTBOX_PRESERVED';
      v_mail_held_until_pdf_rendered := LOWER(COALESCE(
        v_existing_mail_scope_json->>'mail_held_until_pdf_rendered', 'false'))
        IN ('true','t','1','yes');
      v_mail_hold_reason := NULLIF(BTRIM(COALESCE(
        v_existing_mail_scope_json->>'mail_hold_reason', '')), '');
    ELSE
      UPDATE public.mail_outbox AS mail_update
         SET status = 'QUEUED'::public.mail_status_enum,
             subject = v_mail_subject,
             body_html = v_mail_body_html,
             body_text = v_mail_body_text,
             attachments = CASE
               WHEN v_mail_held_until_pdf_rendered THEN '[]'::jsonb
               ELSE jsonb_build_array(jsonb_build_object(
                 'r2_key',v_pdf_key,
                 'filename','Timesheet_'||COALESCE(
                   v_week_ending_date::text,v_current_timesheet_id::text)||'.pdf'))
             END,
             last_error = NULL,
             failed_at = NULL,
             scheduled_for_utc = v_mail_scheduled_for_utc,
             next_attempt_at_utc = v_mail_scheduled_for_utc,
             provider_status = NULL,
             provider_message_id = NULL,
             attempt_lease_token = NULL,
             attempt_leased_at_utc = NULL,
             attempt_lease_expires_at_utc = NULL,
             payment_scope_json = v_mail_scope_json
       WHERE mail_update.id = v_existing_mail_id;

      v_send_state := CASE
        WHEN v_candidate_paper_workflow_count = 1 THEN 'CANDIDATE_PAPER_PACK_MAIL_HELD'
        WHEN NOT v_mail_held_until_pdf_rendered THEN 'DOCUMENT_READY_MAIL_QUEUED'
        WHEN UPPER(COALESCE(v_existing_mail_status,''))='FAILED'
          THEN 'DOCUMENT_REQUEUED_MAIL_HELD'
        ELSE 'ALREADY_QUEUED' END;
    END IF;
  ELSE
    INSERT INTO public.mail_outbox AS mail_insert (
      type,
      "to",
      cc,
      bcc,
      reply_to,
      importance,
      email_type,
      subject,
      body_html,
      body_text,
      attachments,
      status,
      last_error,
      created_at_utc,
      sent_at,
      created_by,
      reference,
      recipient_kind,
      recipient_id,
      context_kind,
      context_id,
      mailshot_run_id,
      document_template_id,
      provider_status,
      delivered_at,
      read_at,
      scheduled_for_utc,
      next_attempt_at_utc,
      payment_scope_json
    ) VALUES (
      'TIMESHEET_QR',
      v_candidate_email,
      NULL,
      NULL,
      NULL,
      'Normal',
      'html',
      v_mail_subject,
      v_mail_body_html,
      v_mail_body_text,
      CASE
        WHEN v_mail_held_until_pdf_rendered THEN '[]'::jsonb
        ELSE jsonb_build_array(jsonb_build_object(
          'r2_key',v_pdf_key,
          'filename','Timesheet_'||COALESCE(
            v_week_ending_date::text,v_current_timesheet_id::text)||'.pdf'))
      END,
      'QUEUED'::public.mail_status_enum,
      NULL,
      v_now,
      NULL,
      v_created_by_user_id,
      v_mail_reference,
      'candidate',
      v_candidate_id,
      'timesheets',
      v_current_timesheet_id,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      v_mail_scheduled_for_utc,
      v_mail_scheduled_for_utc,
      v_mail_scope_json
    )
    RETURNING id INTO v_mail_job_id;

    v_send_state := CASE
      WHEN v_candidate_paper_workflow_count = 1 THEN 'CANDIDATE_PAPER_PACK_MAIL_HELD'
      WHEN v_mail_held_until_pdf_rendered THEN 'DOCUMENT_QUEUED_MAIL_HELD'
      ELSE 'DOCUMENT_READY_MAIL_QUEUED' END;
  END IF;

  v_job_id := v_mail_job_id;

  SELECT decision_result.row_json
    INTO v_post_row
  FROM public.bulk_timesheet_row_decision_v1(jsonb_build_object(
    'dataset_mode', 'process',
    'timesheet_id', v_current_timesheet_id::text
  )) AS decision_result(row_json)
  LIMIT 1;

  v_row_key := NULLIF(BTRIM(COALESCE(v_post_row->>'row_key', '')), '');
  v_storage_key := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_storage_key', '')), '');
  v_row_signature := NULLIF(BTRIM(COALESCE(v_post_row->>'row_signature', '')), '');

  INSERT INTO public.audit_events AS audit_insert (
    ts_utc,
    actor_user_id,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  ) VALUES (
    v_now,
    p_actor_user_id,
    'timesheets',
    v_current_timesheet_id::text,
    'TIMESHEET_QR_SEND_QUEUED',
    NULL,
    jsonb_build_object(
      'timesheet_id', v_current_timesheet_id,
      'job_id', v_job_id,
      'mail_outbox_id', v_mail_job_id,
      'pdf_job_id', v_pdf_job_id,
      'idempotency_key', v_idempotency_key,
      'send_state', v_send_state,
      'recipient_email', v_candidate_email,
      'scheduled_for_utc', v_mail_scheduled_for_utc,
      'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
      'mail_delayed_for_pdf_render',v_mail_held_until_pdf_rendered,
      'mail_hold_reason',v_mail_hold_reason,
      'pdf_storage_key', v_pdf_key,
      'document_operation_id',v_document_operation_id,
      'document_version_id',v_document_version_id,
      'issue_type',v_issue_type,
      'client_idempotency_key', v_client_idempotency_key,
      'recipient_namespace', v_recipient_namespace
    ),
    'Bulk QR send enqueue'
  );

  RETURN jsonb_build_object(
    'ok', TRUE,
    'queued', TRUE,
    'operation', 'timesheet_qr_send_enqueue',
    'job_id', v_job_id,
    'mail_outbox_id', v_mail_job_id,
    'pdf_job_id', v_pdf_job_id,
    'idempotency_key', v_idempotency_key,
    'current_timesheet_id', v_current_timesheet_id,
    'timesheet_id', v_current_timesheet_id,
    'expected_timesheet_id', v_current_timesheet_id,
    'current_version', v_current_version,
    'recipient_available', TRUE,
    'recipient_email', v_candidate_email,
    'recipient_name', v_candidate_name,
    'send_state', v_send_state,
    'scheduled_for_utc', v_mail_scheduled_for_utc,
    'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
    'mail_delayed_for_pdf_render',v_mail_held_until_pdf_rendered,
    'mail_hold_reason',v_mail_hold_reason,
    'candidate_paper_pack_ready',CASE
      WHEN NULLIF(BTRIM(COALESCE(v_existing_mail_scope_json->>'candidate_workflow_id', '')), '') IS NOT NULL
        THEN LOWER(COALESCE(v_existing_mail_scope_json->>'candidate_paper_pack_ready', 'false'))
          IN ('true','t','1','yes')
      WHEN v_candidate_paper_workflow_count = 1 THEN FALSE
      ELSE NULL
    END,
    'pdf_storage_key', v_pdf_key,
    'document_operation_id',v_document_operation_id,
    'document_version_id',v_document_version_id,
    'document_revision',v_document_revision,
    'template_version','timesheet-professional-v2',
    'issue_type',v_issue_type,
    'client_idempotency_key', v_client_idempotency_key,
    'recipient_namespace', v_recipient_namespace,
    'row_patch', COALESCE(v_post_row->'row_patch', jsonb_build_object()),
    'data_row', COALESCE(v_post_row, jsonb_build_object()),
    'row', COALESCE(v_post_row, jsonb_build_object()),
    'cache_invalidation_hints', jsonb_build_object(
      'row_keys', jsonb_build_array(COALESCE(v_row_key, 'timesheet:' || v_current_timesheet_id::text)),
      'timesheet_ids', jsonb_build_array(v_current_timesheet_id),
      'storage_keys', jsonb_build_array(v_storage_key, v_pdf_key),
      'datasets', jsonb_build_array('bulk_process', 'bulk_authorise'),
      'row_signature', v_row_signature,
      'invalidate_context', TRUE,
      'invalidate_preview', TRUE
    ),
    'cache_invalidation', jsonb_build_object(
      'rows', jsonb_build_array(jsonb_build_object(
        'row_key', COALESCE(v_row_key, 'timesheet:' || v_current_timesheet_id::text),
        'timesheet_id', v_current_timesheet_id,
        'new_row_signature', v_row_signature
      )),
      'artifacts', jsonb_build_array(jsonb_build_object(
        'timesheet_id', v_current_timesheet_id,
        'storage_key', COALESCE(v_storage_key, v_pdf_key),
        'pdf_storage_key', v_pdf_key,
        'changed', TRUE
      )),
      'datasets', jsonb_build_array('bulk_process', 'bulk_authorise')
    )
  );
END;
$function$;

-- timesheet_query_email_delivery_mark_v1(uuid,text,text,timestamp with time zone,uuid)
CREATE OR REPLACE FUNCTION public.timesheet_query_email_delivery_mark_v1(p_mail_outbox_id uuid, p_provider_message_id text DEFAULT NULL::text, p_provider_status text DEFAULT NULL::text, p_accepted_at_utc timestamp with time zone DEFAULT NULL::timestamp with time zone, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
begin perform public._import_review_assert_actor_v1(p_actor_user_id);
  return public._timesheet_query_email_delivery_mark_core_v1(p_mail_outbox_id,p_provider_message_id,p_provider_status,p_accepted_at_utc,p_actor_user_id); end $function$;

-- timesheet_query_email_delivery_reconcile_v1(uuid,integer,uuid)
CREATE OR REPLACE FUNCTION public.timesheet_query_email_delivery_reconcile_v1(p_after_delivery_id uuid DEFAULT NULL::uuid, p_limit integer DEFAULT 50, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare v_limit integer:=least(greatest(coalesce(p_limit,50),1),100); r record; v_processed integer:=0; v_repaired integer:=0; v_skipped integer:=0; v_last uuid;
begin perform public._import_review_assert_actor_v1(p_actor_user_id);
  for r in select d.id,d.mail_outbox_id,o.provider_message_id,o.provider_status,o.sent_at
    from public.hr_issue_email_deliveries d join public.mail_outbox o on o.id=d.mail_outbox_id
    where d.status<>'SENT' and (o.status::text='SENT' or o.sent_at is not null) and (p_after_delivery_id is null or d.id>p_after_delivery_id)
    order by d.id limit v_limit
  loop v_processed:=v_processed+1;v_last:=r.id;
    begin perform public._timesheet_query_email_delivery_mark_core_v1(r.mail_outbox_id,r.provider_message_id,r.provider_status,r.sent_at,p_actor_user_id);v_repaired:=v_repaired+1;
    exception when others then v_skipped:=v_skipped+1; end;
  end loop;
  return jsonb_build_object('ok',true,'processed',v_processed,'repaired',v_repaired,'skipped',v_skipped,'next_cursor',case when v_processed=v_limit then v_last end,
    'has_more',exists(select 1 from public.hr_issue_email_deliveries d join public.mail_outbox o on o.id=d.mail_outbox_id
      where d.status<>'SENT' and (o.status::text='SENT' or o.sent_at is not null)
        and (v_last is null or d.id>v_last) limit 1));
end $function$;

-- timesheet_query_email_enqueue_v1(uuid,uuid,jsonb,uuid,integer,integer)
CREATE OR REPLACE FUNCTION public.timesheet_query_email_enqueue_v1(p_import_id uuid, p_operation_id uuid, p_selected_action_ids jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_max_actions integer DEFAULT 5000, p_max_groups integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_state public.import_review_states%rowtype; v_operation public.import_apply_operations%rowtype;
  v_ids text[]; v_db_ids text[]; v_group record; v_route jsonb;
  v_issue_ids uuid[]; v_issue_set_hash text; v_outbox_key text; v_delivery_id uuid; v_outbox_id uuid;
  v_html text; v_text text; v_subject text; v_results jsonb:='[]'; v_group_count integer:=0; v_reminder integer;
  v_new_delivery_count integer:=0; v_group_replay boolean; v_recipient_group_key text;
  v_attachments jsonb:='[]'::jsonb; v_greeting text; v_agency_name text;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or p_operation_id is null or jsonb_typeof(coalesce(p_selected_action_ids,'[]'))<>'array'
    or p_max_actions<1 or p_max_actions>5000 or p_max_groups<1 or p_max_groups>100
    or jsonb_array_length(coalesce(p_selected_action_ids,'[]'))>p_max_actions then
    raise exception 'TIMESHEET_QUERY_ENQUEUE_INPUT_INVALID' using errcode='22023'; end if;
  if exists(select 1 from jsonb_array_elements(coalesce(p_selected_action_ids,'[]'))x where jsonb_typeof(x)<>'string' or trim(both '"' from x::text)!~'^[0-9a-f]{64}$') then
    raise exception 'TIMESHEET_QUERY_ACTION_ID_INVALID' using errcode='22023'; end if;
  select coalesce(array_agg(distinct value order by value),array[]::text[]) into v_ids from jsonb_array_elements_text(coalesce(p_selected_action_ids,'[]'))value;
  if cardinality(v_ids)<>jsonb_array_length(coalesce(p_selected_action_ids,'[]')) then raise exception 'TIMESHEET_QUERY_ACTION_ID_DUPLICATE' using errcode='22023'; end if;
  select * into v_state from public.import_review_states where import_id=p_import_id for update;
  select * into v_operation from public.import_apply_operations
  where id=p_operation_id and import_id=p_import_id for update;
  if v_state.import_id is null
    or v_state.status not in ('IN_REVIEW','BLOCKED','READY','APPLIED')
    or v_state.last_operation_id is distinct from p_operation_id
    or v_operation.id is null
    or v_operation.committed_at_utc is null
    or v_operation.state not in ('SOURCE_COMMITTED_TSFIN_PENDING','COMPLETE') then
    raise exception 'TIMESHEET_QUERY_REVIEW_NOT_APPLIED' using errcode='55000'; end if;
  select coalesce(array_agg(value order by value),array[]::text[]) into v_db_ids
  from jsonb_array_elements_text(coalesce(v_operation.response_json->'post_commit_email_action_ids','[]'::jsonb)) value;
  if v_ids is distinct from v_db_ids then raise exception 'TIMESHEET_QUERY_SELECTED_ACTION_SET_MISMATCH' using errcode='40001'; end if;
  if exists(
    select 1
    from unnest(v_ids) x
    left join public.import_review_decisions d
      on d.action_id=x and d.import_id=p_import_id
    left join public.import_review_action_outcomes o
      on o.action_id=x and o.import_id=p_import_id and o.operation_id=p_operation_id
    where d.action_id is null or not d.selected
      or d.action_kind not in ('EMAIL_ISSUE','EMAIL_REMINDER')
      or o.action_id is null
  ) then
    raise exception 'TIMESHEET_QUERY_ACTION_STALE' using errcode='40001'; end if;

  create temporary table pg_temp.query_email_issues on commit drop as
  select d.*,coalesce(nullif(d.summary_json->>'issue_fingerprint',''),d.source_identity) issue_fingerprint,
    public._timesheet_query_recipient_resolve_core_v1(d.client_id,d.contract_id) route,
    evidence.evidence_json attachment_evidence
  from public.import_review_decisions d
  cross join lateral (select public._import_review_query_evidence_core_v1(d.timesheet_id) evidence_json) evidence
  where d.import_id=p_import_id and d.action_id=any(v_ids) order by d.action_id;
  if exists(select 1 from pg_temp.query_email_issues where route->>'recipient_scope_key' is distinct from summary_json->>'recipient_scope_key'
    or route->>'route_fingerprint' is distinct from summary_json->>'recipient_route_fingerprint') then
    raise exception 'TIMESHEET_QUERY_RECIPIENT_ROUTE_CHANGED' using errcode='40001'; end if;
  if exists(select 1 from pg_temp.query_email_issues
    where not coalesce((attachment_evidence->>'document_ready')::boolean,false)
      or attachment_evidence->>'evidence_fingerprint' is distinct from summary_json->>'attachment_fingerprint') then
    raise exception 'TIMESHEET_QUERY_ATTACHMENT_EVIDENCE_STALE' using errcode='40001'; end if;

  select coalesce(nullif(btrim(agency_name),''),'CloudTMS') into v_agency_name
  from public.settings_defaults order by agency_name nulls last limit 1;
  v_agency_name:=coalesce(v_agency_name,'CloudTMS');
  v_greeting:=case when (statement_timestamp() at time zone 'Europe/London')::time<time '12:00'
    then 'Good morning' else 'Good afternoon' end;

  -- Create/reuse issue identities without changing successful-send history.
  insert into public.hr_issue_emails(source_system,import_id,client_id,contract_id,timesheet_id,hr_row_id,staff_norm,work_date,
    reason_code,issue_fingerprint,last_sent_at,recipient_scope,recipient_scope_key,delivery_history_status)
  select hi.source_system::text,p_import_id,q.client_id,q.contract_id,q.timesheet_id,q.hr_row_id,q.summary_json->>'candidate_name',
    nullif(q.summary_json->>'work_date','')::date,coalesce(q.summary_json->>'reason_code','VALIDATION_MISMATCH'),q.issue_fingerprint,
    null,q.route->>'recipient_scope',q.route->>'recipient_scope_key','PENDING'
  from pg_temp.query_email_issues q join public.hr_imports hi on hi.id=p_import_id
  on conflict(issue_fingerprint) do update set contract_id=excluded.contract_id,recipient_scope=excluded.recipient_scope,
    recipient_scope_key=excluded.recipient_scope_key,updated_at=now();
  update pg_temp.query_email_issues q set issue_id=e.id from public.hr_issue_emails e where e.issue_fingerprint=q.issue_fingerprint;

  for v_group in
    select lower(route->>'recipient_email') recipient_email,
      case when count(distinct route->>'recipient_scope_key')=1 then min(route->>'recipient_scope') else 'CONTRACT_OVERRIDE' end recipient_scope,
      public._import_review_hash_v1(string_agg(distinct route->>'route_fingerprint','|' order by route->>'route_fingerprint')) route_fingerprint,
      count(distinct route->>'recipient_scope_key') business_route_count,
      count(*) issue_count,max(coalesce(e.sent_count,0))+case when bool_or(q.action_kind='EMAIL_REMINDER') then 1 else 0 end reminder_sequence
    from pg_temp.query_email_issues q join public.hr_issue_emails e on e.id=q.issue_id
    group by lower(route->>'recipient_email')
    order by lower(route->>'recipient_email')
  loop
    v_group_count:=v_group_count+1; if v_group_count>p_max_groups then raise exception 'TIMESHEET_QUERY_GROUP_LIMIT_EXCEEDED' using errcode='54000'; end if;
    v_recipient_group_key:='RECIPIENT_EMAIL:'||public._import_review_hash_v1(v_group.recipient_email);
    select array_agg(q.issue_id order by q.issue_id),
      public._import_review_hash_v1(string_agg(q.issue_fingerprint||':'||(q.attachment_evidence->>'evidence_fingerprint'),'|' order by q.issue_fingerprint)),
      coalesce((select jsonb_agg(jsonb_build_object(
        'r2_key',attachment_row.attachment_evidence->>'attachment_r2_key',
        'filename',attachment_row.attachment_evidence->>'attachment_filename'
      ) order by attachment_row.timesheet_id)
      from (
        select distinct on (q2.timesheet_id) q2.timesheet_id,q2.attachment_evidence
        from pg_temp.query_email_issues q2
        where lower(q2.route->>'recipient_email')=v_group.recipient_email
        order by q2.timesheet_id,q2.action_id
      ) attachment_row),'[]'::jsonb)
      into v_issue_ids,v_issue_set_hash,v_attachments
    from pg_temp.query_email_issues q
    where lower(q.route->>'recipient_email')=v_group.recipient_email;
    v_reminder:=v_group.reminder_sequence;
    v_outbox_key:='TIMESHEET_QUERY:'||public._import_review_hash_v1(concat_ws('|',p_operation_id,v_recipient_group_key,v_issue_set_hash,v_reminder));
    v_delivery_id:=null;v_outbox_id:=null;v_group_replay:=false;
    select d.id,d.mail_outbox_id into v_delivery_id,v_outbox_id from public.hr_issue_email_deliveries d where d.deterministic_outbox_key=v_outbox_key for update;
    if not found then
      v_new_delivery_count:=v_new_delivery_count+1;
      v_delivery_id:=gen_random_uuid();
      v_subject:=case when v_reminder>0 then 'Reminder: timesheet corrections required' else 'Timesheet corrections required' end;
      with lines as (
        select q.action_id||':'||row_number() over(partition by q.action_id order by cx.value->>'comparison_key') sort_key,
          coalesce(nullif(cl.name,''),nullif(q.summary_json->>'client_name',''),'Client') client_name,
          coalesce(q.contract_id::text,'client-default') contract_sort,
          case when q.contract_id is null then 'Client default'
            else coalesce(nullif(concat_ws(' · ',nullif(ct.display_site,''),nullif(ct.role,''),nullif(ct.band,'')),''),'Contract')
              ||case when ct.start_date is not null then ' ('||to_char(ct.start_date,'FMDD Mon YYYY')||'–'||to_char(ct.end_date,'FMDD Mon YYYY')||')' else '' end
          end contract_label,
          q.summary_json->>'candidate_name' candidate_name,
          coalesce(nullif(cx.value->>'work_date',''),nullif(q.summary_json->>'work_date','')) work_date,
          coalesce(nullif(cx.value->>'match_status',''),nullif(q.summary_json->>'reason_code','')) issue_code,
          concat_ws(' ',nullif(cx.value->>'timesheet_start','')||case when nullif(cx.value->>'timesheet_end','') is null then '' else '–'||(cx.value->>'timesheet_end') end,
            case when nullif(cx.value->>'timesheet_break_mins','') is not null then 'break '||(cx.value->>'timesheet_break_mins')||' min' end,
            case when nullif(cx.value->>'timesheet_start','') is null then concat_ws('–',q.summary_json->>'start_time',q.summary_json->>'end_time') end,
            case when nullif(q.summary_json->>'hours_worked','') is not null then (q.summary_json->>'hours_worked')||' hours' end) timesheet_detail,
          concat_ws(' ',nullif(cx.value->>'healthroster_start','')||case when nullif(cx.value->>'healthroster_end','') is null then '' else '–'||(cx.value->>'healthroster_end') end,
            case when nullif(cx.value->>'healthroster_break_mins','') is not null then 'break '||(cx.value->>'healthroster_break_mins')||' min' end) import_detail,
          concat_ws(' · ',nullif(cx.value->>'healthroster_hospital',''),nullif(cx.value->>'healthroster_unit','')) source_location,
          nullif(cx.value->>'healthroster_request_grade','') request_grade,
          case
            when nullif(cx.value->>'ref_before','') is not null
              and cx.value->>'ref_before'=cx.value->>'ref_after' then cx.value->>'ref_before'
            when nullif(cx.value->>'ref_before','') is not null and nullif(cx.value->>'ref_after','') is not null
              then (cx.value->>'ref_before')||' → '||(cx.value->>'ref_after')
            else coalesce(nullif(cx.value->>'ref_after',''),nullif(cx.value->>'ref_before',''))
          end reference_detail
        from pg_temp.query_email_issues q
        left join public.clients cl on cl.id=q.client_id
        left join public.contracts ct on ct.id=q.contract_id
        cross join lateral (
          select c.value from jsonb_array_elements(coalesce(q.summary_json->'comparisons','[]'::jsonb)) c(value)
          where coalesce(c.value->>'match_status','')<>'MATCH'
             or coalesce(c.value->>'ref_before','')<>coalesce(c.value->>'ref_after','')
          union all
          select '{}'::jsonb where not exists(
            select 1 from jsonb_array_elements(coalesce(q.summary_json->'comparisons','[]'::jsonb)) c2(value)
            where coalesce(c2.value->>'match_status','')<>'MATCH'
               or coalesce(c2.value->>'ref_before','')<>coalesce(c2.value->>'ref_after',''))
        ) cx
        where lower(q.route->>'recipient_email')=v_group.recipient_email
      ), rendered_rows as (
        select l.*,'<tr><td style="padding:8px;border:1px solid #dbe2ea">'||public._import_review_html_escape_v1(l.candidate_name)||'</td><td style="padding:8px;border:1px solid #dbe2ea;white-space:nowrap">'||
          public._import_review_html_escape_v1(case when l.work_date~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then to_char(l.work_date::date,'FMDD Mon YYYY') else l.work_date end)||'</td><td style="padding:8px;border:1px solid #dbe2ea">'||
          public._import_review_html_escape_v1(replace(initcap(lower(l.issue_code)),'_',' '))||'</td><td style="padding:8px;border:1px solid #dbe2ea">'||
          public._import_review_html_escape_v1(l.timesheet_detail)||'</td><td style="padding:8px;border:1px solid #dbe2ea">'||public._import_review_html_escape_v1(l.import_detail)||
          '</td><td style="padding:8px;border:1px solid #dbe2ea">'||public._import_review_html_escape_v1(l.source_location)||
          '</td><td style="padding:8px;border:1px solid #dbe2ea">'||public._import_review_html_escape_v1(l.request_grade)||
          '</td><td style="padding:8px;border:1px solid #dbe2ea">'||
          public._import_review_html_escape_v1(l.reference_detail)||'</td></tr>' row_html
        from lines l
      ), contract_tables as (
        select client_name,contract_sort,contract_label,
          '<h4 style="margin:18px 0 8px;color:#334155;font-size:14px">'||public._import_review_html_escape_v1(contract_label)||'</h4>'||
          '<table role="table" cellspacing="0" cellpadding="0" style="width:100%;border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px"><thead><tr style="background:#eef2f7;color:#1e293b"><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">Worker</th><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">Date</th><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">Issue</th><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">Timesheet</th><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">HealthRoster</th><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">Unit / ward</th><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">Request grade</th><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">Reference</th></tr></thead><tbody>'||
          string_agg(row_html,'' order by sort_key)||'</tbody></table>' contract_html
        from rendered_rows group by client_name,contract_sort,contract_label
      ), client_sections as (
        select client_name,'<section style="margin:24px 0"><h3 style="margin:0 0 10px;color:#0f172a;font-size:17px">'||
          public._import_review_html_escape_v1(client_name)||'</h3>'||string_agg(contract_html,'' order by contract_label,contract_sort)||'</section>' client_html
        from contract_tables group by client_name
      )
      select '<div style="font-family:Arial,sans-serif;color:#0f172a;line-height:1.45"><p>'||
        public._import_review_html_escape_v1(v_greeting)||',</p><p>Please can you kindly make amendments on HealthRoster for the below shifts. The relevant timesheets have been attached to this email.</p>'||
        string_agg(client_html,'' order by client_name)||'<p>Many thanks,<br>'||
        public._import_review_html_escape_v1(v_agency_name)||'</p></div>' into v_html from client_sections;
      with lines as (
        select q.action_id||':'||row_number() over(partition by q.action_id order by cx.value->>'comparison_key') sort_key,
          coalesce(nullif(cl.name,''),nullif(q.summary_json->>'client_name',''),'Client') client_name,
          case when q.contract_id is null then 'Client default'
            else coalesce(nullif(concat_ws(' · ',nullif(ct.display_site,''),nullif(ct.role,''),nullif(ct.band,'')),''),'Contract') end contract_label,
          q.summary_json->>'candidate_name' candidate_name,
          coalesce(nullif(cx.value->>'work_date',''),nullif(q.summary_json->>'work_date','')) work_date,
          coalesce(nullif(cx.value->>'match_status',''),nullif(q.summary_json->>'reason_code','')) issue_code,
          concat_ws(' ',cx.value->>'timesheet_start',cx.value->>'timesheet_end',cx.value->>'healthroster_start',cx.value->>'healthroster_end',
            q.summary_json->>'start_time',q.summary_json->>'end_time',q.summary_json->>'hours_worked',
            case when nullif(cx.value->>'healthroster_hospital','') is not null or nullif(cx.value->>'healthroster_unit','') is not null
              then 'Unit / ward: '||concat_ws(' · ',nullif(cx.value->>'healthroster_hospital',''),nullif(cx.value->>'healthroster_unit','')) end,
            case when nullif(cx.value->>'healthroster_request_grade','') is not null
              then 'Request grade: '||(cx.value->>'healthroster_request_grade') end) detail
        from pg_temp.query_email_issues q
        left join public.clients cl on cl.id=q.client_id
        left join public.contracts ct on ct.id=q.contract_id
        cross join lateral (
          select c.value from jsonb_array_elements(coalesce(q.summary_json->'comparisons','[]'::jsonb)) c(value)
          where coalesce(c.value->>'match_status','')<>'MATCH'
             or coalesce(c.value->>'ref_before','')<>coalesce(c.value->>'ref_after','')
          union all
          select '{}'::jsonb where not exists(
            select 1 from jsonb_array_elements(coalesce(q.summary_json->'comparisons','[]'::jsonb)) c2(value)
            where coalesce(c2.value->>'match_status','')<>'MATCH'
               or coalesce(c2.value->>'ref_before','')<>coalesce(c2.value->>'ref_after',''))
        ) cx
        where lower(q.route->>'recipient_email')=v_group.recipient_email
      )
      select v_greeting||E',\n\nPlease can you kindly make amendments on HealthRoster for the below shifts. The relevant timesheets have been attached to this email.\n\n'||
        string_agg(concat_ws(' | ',l.client_name,l.contract_label,l.candidate_name,l.work_date,l.issue_code,l.detail),'\n' order by l.client_name,l.contract_label,l.sort_key)
        ||E'\n\nMany thanks,\n'||v_agency_name into v_text from lines l;
      if length(v_html)>262144 or length(v_text)>131072 then raise exception 'TIMESHEET_QUERY_BODY_LIMIT_EXCEEDED' using errcode='54000'; end if;
      insert into public.mail_outbox(type,"to",subject,body_html,body_text,attachments,status,created_by,reference,recipient_kind,recipient_id,
        context_kind,context_id,email_type,deterministic_outbox_key,payment_scope_json)
      values('TIMESHEET_QUERY',v_group.recipient_email,v_subject,v_html,v_text,v_attachments,'QUEUED'::public.mail_status_enum,p_actor_user_id,
        v_outbox_key,'TIMESHEET_QUERY_EMAIL',null,
        'TIMESHEET_QUERY_DELIVERY',v_delivery_id,'TIMESHEET_QUERY',v_outbox_key,'{}'::jsonb)
      on conflict do nothing;
      select id into v_outbox_id from public.mail_outbox where deterministic_outbox_key=v_outbox_key;
      if v_outbox_id is null then raise exception 'TIMESHEET_QUERY_OUTBOX_CLAIM_FAILED' using errcode='23505'; end if;
      insert into public.hr_issue_email_deliveries(id,import_id,operation_id,recipient_scope,recipient_scope_key,recipient_route_fingerprint,
        recipient_email,reminder_sequence,issue_set_fingerprint,deterministic_outbox_key,mail_outbox_id,status,created_by_user_id)
      values(v_delivery_id,p_import_id,p_operation_id,v_group.recipient_scope,v_recipient_group_key,v_group.route_fingerprint,
        v_group.recipient_email,v_reminder,v_issue_set_hash,v_outbox_key,v_outbox_id,'QUEUED',p_actor_user_id);
      insert into public.hr_issue_email_delivery_items(delivery_id,issue_id,action_id,issue_fingerprint)
      select v_delivery_id,q.issue_id,q.action_id,q.issue_fingerprint from pg_temp.query_email_issues q
      where lower(q.route->>'recipient_email')=v_group.recipient_email order by q.action_id;
    else
      v_group_replay:=true;
    end if;
    v_results:=v_results||jsonb_build_array(jsonb_build_object('delivery_id',v_delivery_id,'mail_outbox_id',v_outbox_id,
      'recipient_scope',v_group.recipient_scope,'recipient_scope_key',v_recipient_group_key,
      'business_route_count',v_group.business_route_count,'issue_count',v_group.issue_count,
      'reminder_sequence',v_reminder,'replay',v_group_replay));
  end loop;
  if v_new_delivery_count>0 then
    update public.import_review_states set follow_up_status='PENDING',state_version=state_version+1,
      updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=p_import_id returning * into v_state;
    insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
    values(p_import_id,v_state.state_version,p_operation_id,'QUERY_EMAILS_ENQUEUED',p_actor_user_id,
      jsonb_build_object('group_count',v_group_count,'new_delivery_count',v_new_delivery_count,'action_count',cardinality(v_ids)));
  else select * into v_state from public.import_review_states where import_id=p_import_id; end if;
  return jsonb_build_object('ok',true,'import_id',p_import_id,'operation_id',p_operation_id,'group_count',v_group_count,'groups',v_results,
    'new_delivery_count',v_new_delivery_count,'replay',v_group_count>0 and v_new_delivery_count=0,
    'follow_up_status',v_state.follow_up_status,'state_version',v_state.state_version);
end $function$;

-- timesheet_query_recipient_resolve_v1(uuid,uuid)
CREATE OR REPLACE FUNCTION public.timesheet_query_recipient_resolve_v1(p_client_id uuid, p_contract_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
begin return public._timesheet_query_recipient_resolve_core_v1(p_client_id,p_contract_id); end $function$;

-- timesheet_r2_cleanup_claim_v1(integer,integer)
CREATE OR REPLACE FUNCTION public.timesheet_r2_cleanup_claim_v1(p_limit integer DEFAULT 50, p_lease_seconds integer DEFAULT 300)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $function$
DECLARE
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 50), 1), 100);
  v_lease_seconds integer := LEAST(GREATEST(COALESCE(p_lease_seconds, 300), 30), 3600);
  v_claim_token uuid := gen_random_uuid();
  v_now timestamptz := clock_timestamp();
  v_rows jsonb := '[]'::jsonb;
  v_more_due boolean := false;
BEGIN
  WITH claimable AS (
    SELECT q.delete_operation_id, q.r2_key
    FROM public.timesheet_r2_cleanup_queue AS q
    WHERE (
      q.status = 'PENDING'
      AND q.next_attempt_at_utc <= v_now
    ) OR (
      q.status = 'IN_PROGRESS'
      AND q.claimed_at_utc < v_now - make_interval(secs => v_lease_seconds)
    )
    ORDER BY
      CASE WHEN q.status = 'IN_PROGRESS' THEN 0 ELSE 1 END,
      q.next_attempt_at_utc,
      q.last_attempt_at_utc,
      q.delete_operation_id,
      q.r2_key
    FOR UPDATE SKIP LOCKED
    LIMIT v_limit
  ), claimed AS (
    UPDATE public.timesheet_r2_cleanup_queue AS q
       SET status = 'IN_PROGRESS',
           claim_token = v_claim_token,
           claimed_at_utc = v_now,
           last_attempt_at_utc = v_now,
           attempt_count = q.attempt_count + 1
      FROM claimable AS c
     WHERE q.delete_operation_id = c.delete_operation_id
       AND q.r2_key = c.r2_key
    RETURNING q.delete_operation_id, q.r2_key, q.requested_timesheet_id,
              q.deleted_timesheet_ids, q.attempt_count, q.claim_token
  )
  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'delete_operation_id', c.delete_operation_id,
        'r2_key', c.r2_key,
        'requested_timesheet_id', c.requested_timesheet_id,
        'deleted_timesheet_ids', to_jsonb(c.deleted_timesheet_ids),
        'attempt_count', c.attempt_count,
        'claim_token', c.claim_token
      )
      ORDER BY c.delete_operation_id, c.r2_key
    ),
    '[]'::jsonb
  )
  INTO v_rows
  FROM claimed AS c;

  SELECT EXISTS (
    SELECT 1
    FROM public.timesheet_r2_cleanup_queue AS q
    WHERE (q.status = 'PENDING' AND q.next_attempt_at_utc <= v_now)
       OR (
         q.status = 'IN_PROGRESS'
         AND q.claimed_at_utc < v_now - make_interval(secs => v_lease_seconds)
       )
  )
  INTO v_more_due;

  RETURN jsonb_build_object(
    'ok', true,
    'claim_token', v_claim_token,
    'count', jsonb_array_length(v_rows),
    'more_due', v_more_due,
    'items', v_rows
  );
END;
$function$;

-- timesheet_r2_cleanup_complete_v1(text,uuid,text[])
CREATE OR REPLACE FUNCTION public.timesheet_r2_cleanup_complete_v1(p_delete_operation_id text, p_claim_token uuid, p_r2_keys text[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $function$
DECLARE
  v_operation_id text := NULLIF(BTRIM(COALESCE(p_delete_operation_id, '')), '');
  v_keys text[] := ARRAY[]::text[];
  v_count integer := 0;
  v_now timestamptz := clock_timestamp();
BEGIN
  IF v_operation_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'DELETE_OPERATION_ID_REQUIRED';
  END IF;
  IF p_claim_token IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'R2_CLEANUP_CLAIM_TOKEN_REQUIRED';
  END IF;

  SELECT COALESCE(
           array_agg(exact_key.r2_key ORDER BY exact_key.r2_key COLLATE "C"),
           ARRAY[]::text[]
         )
    INTO v_keys
  FROM (
    SELECT DISTINCT key_value.value COLLATE "C" AS r2_key
    FROM unnest(COALESCE(p_r2_keys, ARRAY[]::text[])) AS key_value(value)
    WHERE key_value.value IS NOT NULL
      AND NULLIF(BTRIM(key_value.value), '') IS NOT NULL
  ) AS exact_key;

  IF COALESCE(array_length(v_keys, 1), 0) > 256 THEN
    RAISE EXCEPTION USING MESSAGE = 'R2_CLEANUP_KEY_LIMIT_EXCEEDED';
  END IF;

  UPDATE public.timesheet_r2_cleanup_queue AS q
     SET status = 'COMPLETE',
         last_error = NULL,
         last_attempt_at_utc = v_now,
         next_attempt_at_utc = v_now,
         completed_at_utc = v_now,
         claim_token = NULL,
         claimed_at_utc = NULL
   WHERE q.delete_operation_id = v_operation_id
     AND q.r2_key = ANY(v_keys)
     AND q.status = 'IN_PROGRESS'
     AND q.claim_token = p_claim_token;

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN jsonb_build_object(
    'ok', true,
    'delete_operation_id', v_operation_id,
    'claim_token', p_claim_token,
    'requested', COALESCE(array_length(v_keys, 1), 0),
    'completed', v_count,
    'stale_or_missing_ignored', GREATEST(COALESCE(array_length(v_keys, 1), 0) - v_count, 0)
  );
END;
$function$;

-- timesheet_r2_cleanup_record_v1(text,uuid,uuid[],jsonb,uuid)
CREATE OR REPLACE FUNCTION public.timesheet_r2_cleanup_record_v1(p_delete_operation_id text, p_requested_timesheet_id uuid, p_deleted_timesheet_ids uuid[], p_failures jsonb, p_claim_token uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $function$
DECLARE
  v_operation_id text := NULLIF(BTRIM(COALESCE(p_delete_operation_id, '')), '');
  v_failures jsonb := COALESCE(p_failures, '[]'::jsonb);
  v_input_count integer := 0;
  v_valid_count integer := 0;
  v_recorded_count integer := 0;
  v_now timestamptz := clock_timestamp();
BEGIN
  IF v_operation_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'DELETE_OPERATION_ID_REQUIRED';
  END IF;
  IF jsonb_typeof(v_failures) <> 'array' THEN
    RAISE EXCEPTION USING MESSAGE = 'FAILURES_MUST_BE_JSON_ARRAY';
  END IF;

  v_input_count := jsonb_array_length(v_failures);
  IF v_input_count > 256 THEN
    RAISE EXCEPTION USING MESSAGE = 'R2_CLEANUP_FAILURE_LIMIT_EXCEEDED';
  END IF;

  IF p_claim_token IS NULL THEN
    WITH parsed AS (
      SELECT
        CASE
          WHEN jsonb_typeof(item.value -> 'r2_key') = 'string'
           AND NULLIF(BTRIM(item.value ->> 'r2_key'), '') IS NOT NULL
            THEN item.value ->> 'r2_key'
          ELSE NULL
        END AS r2_key,
        GREATEST(COALESCE(NULLIF(item.value ->> 'attempt_count', '')::integer, 1), 1) AS attempt_count,
        LEFT(
          COALESCE(NULLIF(BTRIM(item.value ->> 'error'), ''), 'R2_DELETE_FAILED'),
          2000
        ) AS error_text,
        item.ordinality
      FROM jsonb_array_elements(v_failures) WITH ORDINALITY AS item(value, ordinality)
    ), deduplicated AS (
      SELECT DISTINCT ON (parsed.r2_key COLLATE "C")
        parsed.r2_key,
        parsed.attempt_count,
        parsed.error_text
      FROM parsed
      WHERE parsed.r2_key IS NOT NULL
      ORDER BY parsed.r2_key COLLATE "C", parsed.ordinality DESC
    ), inserted AS (
      INSERT INTO public.timesheet_r2_cleanup_queue AS q (
        delete_operation_id,
        r2_key,
        requested_timesheet_id,
        deleted_timesheet_ids,
        status,
        attempt_count,
        last_error,
        claim_token,
        claimed_at_utc,
        next_attempt_at_utc,
        first_failed_at_utc,
        last_attempt_at_utc,
        completed_at_utc
      )
      SELECT
        v_operation_id,
        d.r2_key,
        p_requested_timesheet_id,
        COALESCE(p_deleted_timesheet_ids, ARRAY[]::uuid[]),
        'PENDING',
        d.attempt_count,
        d.error_text,
        NULL,
        NULL,
        v_now + make_interval(
          secs => LEAST(
            3600,
            15 * power(2::numeric, LEAST(GREATEST(d.attempt_count - 1, 0), 8))::integer
          )
        ),
        v_now,
        v_now,
        NULL
      FROM deduplicated AS d
      ON CONFLICT (delete_operation_id, r2_key) DO UPDATE
      SET requested_timesheet_id = COALESCE(EXCLUDED.requested_timesheet_id, q.requested_timesheet_id),
          deleted_timesheet_ids = CASE
            WHEN COALESCE(array_length(EXCLUDED.deleted_timesheet_ids, 1), 0) > 0
              THEN EXCLUDED.deleted_timesheet_ids
            ELSE q.deleted_timesheet_ids
          END,
          status = 'PENDING',
          attempt_count = GREATEST(q.attempt_count, EXCLUDED.attempt_count),
          last_error = EXCLUDED.last_error,
          claim_token = NULL,
          claimed_at_utc = NULL,
          next_attempt_at_utc = EXCLUDED.next_attempt_at_utc,
          last_attempt_at_utc = v_now,
          completed_at_utc = NULL
      WHERE q.status <> 'COMPLETE'
      RETURNING 1
    )
    SELECT
      (SELECT COUNT(*) FROM deduplicated),
      (SELECT COUNT(*) FROM inserted)
    INTO v_valid_count, v_recorded_count;
  ELSE
    WITH parsed AS (
      SELECT
        CASE
          WHEN jsonb_typeof(item.value -> 'r2_key') = 'string'
           AND NULLIF(BTRIM(item.value ->> 'r2_key'), '') IS NOT NULL
            THEN item.value ->> 'r2_key'
          ELSE NULL
        END AS r2_key,
        LEFT(
          COALESCE(NULLIF(BTRIM(item.value ->> 'error'), ''), 'R2_DELETE_FAILED'),
          2000
        ) AS error_text,
        item.ordinality
      FROM jsonb_array_elements(v_failures) WITH ORDINALITY AS item(value, ordinality)
    ), deduplicated AS (
      SELECT DISTINCT ON (parsed.r2_key COLLATE "C")
        parsed.r2_key,
        parsed.error_text
      FROM parsed
      WHERE parsed.r2_key IS NOT NULL
      ORDER BY parsed.r2_key COLLATE "C", parsed.ordinality DESC
    ), released AS (
      UPDATE public.timesheet_r2_cleanup_queue AS q
         SET status = 'PENDING',
             last_error = d.error_text,
             claim_token = NULL,
             claimed_at_utc = NULL,
             next_attempt_at_utc = v_now + make_interval(
               secs => LEAST(
                 3600,
                 15 * power(2::numeric, LEAST(GREATEST(q.attempt_count - 1, 0), 8))::integer
               )
             ),
             last_attempt_at_utc = v_now,
             completed_at_utc = NULL
        FROM deduplicated AS d
       WHERE q.delete_operation_id = v_operation_id
         AND q.r2_key = d.r2_key
         AND q.status = 'IN_PROGRESS'
         AND q.claim_token = p_claim_token
      RETURNING 1
    )
    SELECT
      (SELECT COUNT(*) FROM deduplicated),
      (SELECT COUNT(*) FROM released)
    INTO v_valid_count, v_recorded_count;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'delete_operation_id', v_operation_id,
    'claim_token', p_claim_token,
    'input_count', v_input_count,
    'valid_distinct_keys', v_valid_count,
    'recorded', v_recorded_count,
    'stale_or_complete_ignored', GREATEST(v_valid_count - v_recorded_count, 0)
  );
END;
$function$;

-- timesheet_removal_financial_history_v1(uuid[],text[],uuid[])
CREATE OR REPLACE FUNCTION public.timesheet_removal_financial_history_v1(p_timesheet_ids uuid[], p_booking_ids text[] DEFAULT NULL::text[], p_contract_week_ids uuid[] DEFAULT NULL::uuid[])
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_booking_ids text[] := ARRAY[]::text[];
  v_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_blockers jsonb := '[]'::jsonb;
  v_retention_reasons jsonb := '[]'::jsonb;
  v_invoice_info jsonb := '[]'::jsonb;
  v_missing_count integer := 0;
  v_active_advance boolean := false;
  v_clearable_advance boolean := false;
  v_consumed_advance boolean := false;
  v_historical_advance boolean := false;
  v_related_pay_batch_id uuid := NULL;
  v_related_pay_batch_status text := NULL;
BEGIN
  SELECT COALESCE(array_agg(DISTINCT requested.id ORDER BY requested.id), ARRAY[]::uuid[])
    INTO v_timesheet_ids
  FROM unnest(COALESCE(p_timesheet_ids, ARRAY[]::uuid[])) AS requested(id)
  WHERE requested.id IS NOT NULL;

  IF COALESCE(array_length(v_timesheet_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_IDS_REQUIRED';
  END IF;

  IF array_length(v_timesheet_ids, 1) > 32 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'REMOVAL_UNIT_TOO_LARGE',
      DETAIL = jsonb_build_object('maximum', 32, 'actual', array_length(v_timesheet_ids, 1))::text;
  END IF;

  SELECT COUNT(*)
    INTO v_missing_count
  FROM unnest(v_timesheet_ids) AS requested(id)
  LEFT JOIN public.timesheets AS t ON t.timesheet_id = requested.id
  WHERE t.timesheet_id IS NULL;

  IF v_missing_count > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TARGET_NOT_FOUND',
      'message', 'One or more timesheets in the resolved removal unit no longer exist.',
      'missing_count', v_missing_count
    ));
  END IF;

  SELECT COALESCE(array_agg(DISTINCT booking_value ORDER BY booking_value), ARRAY[]::text[])
    INTO v_booking_ids
  FROM (
    SELECT NULLIF(BTRIM(supplied.value), '') AS booking_value
    FROM unnest(COALESCE(p_booking_ids, ARRAY[]::text[])) AS supplied(value)
    UNION
    SELECT NULLIF(BTRIM(t.booking_id), '')
    FROM public.timesheets AS t
    WHERE t.timesheet_id = ANY(v_timesheet_ids)
  ) AS booking_values
  WHERE booking_value IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT contract_week_id ORDER BY contract_week_id), ARRAY[]::uuid[])
    INTO v_contract_week_ids
  FROM (
    SELECT supplied.value AS contract_week_id
    FROM unnest(COALESCE(p_contract_week_ids, ARRAY[]::uuid[])) AS supplied(value)
    WHERE supplied.value IS NOT NULL
    UNION
    SELECT cw.id
    FROM public.contract_weeks AS cw
    WHERE cw.timesheet_id = ANY(v_timesheet_ids)
  ) AS contract_week_values;

  IF EXISTS (
    SELECT 1
    FROM public.timesheets AS t
    WHERE t.timesheet_id = ANY(v_timesheet_ids)
      AND t.archived_at_utc IS NOT NULL
  ) THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TIMESHEET_ARCHIVED',
      'message', 'The resolved removal unit already contains an Archived timesheet.'
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.timesheets AS t
    WHERE t.timesheet_id = ANY(v_timesheet_ids)
      AND t.authorised_at_server IS NOT NULL
  ) OR EXISTS (
    SELECT 1
    FROM public.timesheets_financials AS tf
    WHERE tf.timesheet_id = ANY(v_timesheet_ids)
      AND tf.is_current = true
      AND tf.authorised_at_utc IS NOT NULL
  ) OR EXISTS (
    SELECT 1
    FROM public.contract_weeks AS cw
    WHERE cw.id = ANY(v_contract_week_ids)
      AND cw.status = 'AUTHORISED'::public.contract_week_status_enum
  ) THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TIMESHEET_STILL_AUTHORISED',
      'message', 'Every timesheet must be unauthorised before Delete or Archive.'
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.timesheets_financials AS tf
    WHERE tf.timesheet_id = ANY(v_timesheet_ids)
      AND tf.is_current = true
      AND (
        tf.locked_by_invoice_id IS NOT NULL
        OR (tf.locked_at_utc IS NOT NULL AND tf.unlocked_by_credit_note_id IS NULL)
        OR EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'array' THEN tf.invoice_breakdown_json
              WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'object'
               AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments') = 'array'
                THEN tf.invoice_breakdown_json -> 'segments'
              ELSE '[]'::jsonb
            END
          ) AS segment(segment_json)
          WHERE NULLIF(BTRIM(COALESCE(segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
             OR NULLIF(BTRIM(COALESCE(segment.segment_json ->> 'locked_by_invoice_id', '')), '') IS NOT NULL
        )
      )
  ) OR EXISTS (
    SELECT 1
    FROM public.invoice_lines AS il
    WHERE (il.timesheet_id IS NOT NULL AND il.timesheet_id = ANY(v_timesheet_ids))
       OR (
         il.timesheet_id IS NULL
         AND il.booking_id IS NOT NULL
         AND il.booking_id = ANY(v_booking_ids)
       )
  ) OR EXISTS (
    SELECT 1
    FROM public.contract_weeks AS cw
    WHERE cw.id = ANY(v_contract_week_ids)
      AND cw.status = 'INVOICED'::public.contract_week_status_enum
  ) THEN
    SELECT COALESCE(
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', invoice_rows.invoice_id,
          'invoice_status', invoice_rows.invoice_status
        )
        ORDER BY invoice_rows.invoice_id
      ),
      '[]'::jsonb
    )
      INTO v_invoice_info
    FROM (
      SELECT DISTINCT
        inv.id AS invoice_id,
        inv.status AS invoice_status
      FROM public.invoice_lines AS il
      JOIN public.invoices AS inv ON inv.id = il.invoice_id
      WHERE (il.timesheet_id IS NOT NULL AND il.timesheet_id = ANY(v_timesheet_ids))
         OR (
           il.timesheet_id IS NULL
           AND il.booking_id IS NOT NULL
           AND il.booking_id = ANY(v_booking_ids)
         )
    ) AS invoice_rows;

    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TIMESHEET_INVOICE_LOCKED',
      'message', 'One or more timesheets in the resolved unit are invoiced or invoice-locked.',
      'invoices', v_invoice_info
    ));
  END IF;

  -- Marker-only hot path.  Durable writers and the capture/backfill sequence
  -- establish this sticky exact-identity authority before activation.
  IF EXISTS (
    SELECT 1
    FROM public.timesheet_financial_retention AS retention
    WHERE retention.timesheet_id = ANY(v_timesheet_ids)
  ) THEN
    v_retention_reasons := jsonb_build_array(jsonb_build_object(
      'code', 'FINANCIAL_HISTORY',
      'message', 'Retained financial history exists for one or more timesheets in the resolved removal unit.'
    ));
  END IF;

  SELECT
    COALESCE(bool_or(override_row.cleared_at_utc IS NULL), false),
    COALESCE(bool_or(
      override_row.cleared_at_utc IS NULL
      AND (
        override_row.consumed_by_pay_batch_id IS NULL
        OR UPPER(COALESCE(batch.status, '')) = 'CANCELLED'
      )
    ), false),
    COALESCE(bool_or(
      override_row.consumed_by_pay_batch_id IS NOT NULL
      AND UPPER(COALESCE(batch.status, '')) <> 'CANCELLED'
    ), false),
    COUNT(*) > 0
  INTO
    v_active_advance,
    v_clearable_advance,
    v_consumed_advance,
    v_historical_advance
  FROM public.timesheet_payment_overrides AS override_row
  LEFT JOIN public.pay_batches AS batch ON batch.id = override_row.consumed_by_pay_batch_id
  WHERE override_row.timesheet_id = ANY(v_timesheet_ids)
    AND UPPER(COALESCE(override_row.override_type, '')) = 'ADVANCE_THIS_PAYMENT';

  SELECT override_row.consumed_by_pay_batch_id, batch.status
    INTO v_related_pay_batch_id, v_related_pay_batch_status
  FROM public.timesheet_payment_overrides AS override_row
  LEFT JOIN public.pay_batches AS batch ON batch.id = override_row.consumed_by_pay_batch_id
  WHERE override_row.timesheet_id = ANY(v_timesheet_ids)
    AND override_row.consumed_by_pay_batch_id IS NOT NULL
  ORDER BY override_row.consumed_at_utc DESC NULLS LAST,
           override_row.created_at_utc DESC,
           override_row.id DESC
  LIMIT 1;

  RETURN jsonb_build_object(
    'ok', true,
    'timesheet_ids', to_jsonb(v_timesheet_ids),
    'booking_ids', to_jsonb(v_booking_ids),
    'contract_week_ids', to_jsonb(v_contract_week_ids),
    'blocked', jsonb_array_length(v_blockers) > 0,
    'blockers', v_blockers,
    'archive_required', jsonb_array_length(v_retention_reasons) > 0,
    'retention_reasons', v_retention_reasons,
    'advance', jsonb_build_object(
      'active', v_active_advance,
      'clearable', v_clearable_advance,
      'consumed', v_consumed_advance,
      'historical', v_historical_advance,
      'related_pay_batch_id', v_related_pay_batch_id,
      'related_pay_batch_status', v_related_pay_batch_status
    )
  );
END;
$function$;

-- timesheet_route_version_confirmed_v1(uuid,uuid,text,text,text,uuid,text,text,text,boolean,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.timesheet_route_version_confirmed_v1(p_current_timesheet_id uuid, p_expected_timesheet_id uuid, p_expected_row_signature text, p_expected_context_sha256 text, p_target_action text, p_actor_user_id uuid, p_reason_code text DEFAULT NULL::text, p_reason_note text DEFAULT NULL::text, p_idempotency_key text DEFAULT NULL::text, p_allow_manual_only boolean DEFAULT false, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_action text:=upper(btrim(coalesce(p_target_action,'')));
  v_reason_code text:=upper(btrim(coalesce(p_reason_code,'')));
  v_reason_note text:=nullif(btrim(coalesce(p_reason_note,'')),'');
  v_requested public.timesheets%rowtype;
  v_current public.timesheets%rowtype;
  v_environment text;
  v_route_family_key text;
  v_context jsonb;
  v_result jsonb;
  v_workflow_result jsonb:='{}'::jsonb;
  v_primary_workflow_result jsonb:='{}'::jsonb;
  v_incomplete_workflow_result jsonb:='{}'::jsonb;
  v_primary_workflow_id uuid;
  v_incomplete_expense_workflow_id uuid;
  v_notification_result jsonb:=jsonb_build_object(
    'notification_required',false,'notification_created',false,
    'notification_recipient_unavailable',false,'notification_dedupe_keys','[]'::jsonb
  );
  v_current_signature text;
  v_new_timesheet_id uuid;
  v_new_version integer;
  v_reason_required boolean;
  v_retire_workflow boolean:=false;
  v_retirement_reason text;
  v_intervention_reasons constant text[]:=array[
    'CANDIDATE_SUPPLIED_MANUAL_TIMESHEET',
    'CANDIDATE_REPORTED_HOURS_INCORRECT',
    'HIRING_MANAGER_REPORTED_HOURS_INCORRECT',
    'ELECTRONIC_SUBMISSION_TECHNICAL_FAILURE',
    'OTHER_EXCEPTIONAL_OFFICE_INTERVENTION'
  ];
begin
  if p_actor_user_id is null or p_expected_timesheet_id is null
     or nullif(btrim(coalesce(p_expected_row_signature,'')),'') is null
     or coalesce(p_expected_context_sha256,'') !~ '^[0-9a-fA-F]{64}$' then
    raise exception 'ROUTE_CHANGE_CONFIRMATION_INPUT_INVALID' using errcode='22023';
  end if;
  select * into v_requested from public.timesheets
  where timesheet_id=p_current_timesheet_id;
  if not found then raise exception 'TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
  select candidate_app_environment into v_environment
  from public.settings_defaults where id=1;
  v_environment:=private._candidate_assert_environment(v_environment);
  if not private._candidate_feature_enabled_current_v1('candidate_route_confirmation')
     and not private._candidate_office_service_context_valid_v1(
       v_environment,p_actor_user_id,'ROUTE_CONFIRM'
     ) then
    raise exception 'CANDIDATE_ROUTE_CONFIRMATION_DISABLED' using errcode='42501';
  end if;
  -- The same stable Candidate contract/week family lock is taken by office
  -- rejection, workflow cancellation/supersession and route intervention.
  -- It must precede target, booking, workflow and QR-source row locks.
  v_route_family_key:='CANDIDATE_PAPER_FAMILY:'||v_environment||':'
    ||coalesce(v_requested.contract_id::text,'-')||':'
    ||coalesce(v_requested.week_ending_date::text,'-');
  perform pg_advisory_xact_lock(hashtextextended(v_route_family_key,0));
  perform pg_advisory_xact_lock(hashtext(btrim(v_requested.booking_id)));
  perform 1 from public.timesheets
  where booking_id=v_requested.booking_id for update;
  select * into v_current from public.timesheets
  where booking_id=v_requested.booking_id and is_current=true
  order by version desc,updated_at desc,created_at desc limit 1 for update;
  if v_current.timesheet_id is distinct from p_expected_timesheet_id then
    raise exception 'TIMESHEET_MOVED' using errcode='40001',detail=jsonb_build_object(
      'current_timesheet_id',v_current.timesheet_id)::text;
  end if;
  perform 1 from public.contract_weeks
  where timesheet_id=v_current.timesheet_id for update;
  perform 1 from public.timesheets_financials
  where timesheet_id=v_current.timesheet_id and is_current=true for update;
  -- Lock the complete booking/version-family workflow catalogue, including a
  -- draft or submitted PAPER claim that has no mail receipt yet.  The family
  -- advisory lock above keeps this order consistent with rejection and claim
  -- retirement; UUID ordering keeps row acquisition deterministic.
  perform 1 from public.candidate_submission_workflows workflow
  where workflow.id=v_current.candidate_workflow_id
     or exists(
       select 1
       from public.timesheets binding_source
       where binding_source.timesheet_id in (
           workflow.target_timesheet_id,workflow.anchor_timesheet_id
         )
         and binding_source.booking_id=v_current.booking_id
     )
  order by workflow.id for update;
  perform 1 from public.candidate_approval_requests request_row
  where request_row.workflow_id in (
    select workflow.id
    from public.candidate_submission_workflows workflow
    where workflow.id=v_current.candidate_workflow_id
       or exists(
         select 1
         from public.timesheets binding_source
         where binding_source.timesheet_id in (
             workflow.target_timesheet_id,workflow.anchor_timesheet_id
           )
           and binding_source.booking_id=v_current.booking_id
       )
  ) order by request_row.id for update;

  v_context:=private._timesheet_route_change_context_v1(v_current.timesheet_id,v_action);
  v_current_signature:=nullif(btrim(coalesce(v_context->>'current_row_signature','')),'');
  if v_current_signature is distinct from btrim(p_expected_row_signature) then
    raise exception 'ROW_SIGNATURE_MISMATCH' using errcode='40001',detail=jsonb_build_object(
      'expected_row_signature',p_expected_row_signature,
      'current_row_signature',v_current_signature)::text;
  end if;
  if lower(v_context->>'context_sha256') is distinct from lower(p_expected_context_sha256) then
    raise exception 'ROUTE_CHANGE_CONTEXT_CHANGED' using errcode='40001',detail=jsonb_build_object(
      'warning_code',v_context->>'warning_code',
      'current_context_sha256',v_context->>'context_sha256')::text;
  end if;
  if not coalesce((v_context->>'permitted_action')::boolean,false) then
    raise exception 'ROUTE_CHANGE_NOT_PERMITTED' using errcode='55000',detail=v_context::text;
  end if;
  v_reason_required:=coalesce((v_context->>'reason_required')::boolean,false);
  if v_reason_required and not (v_reason_code=any(v_intervention_reasons)) then
    raise exception 'ROUTE_INTERVENTION_REASON_REQUIRED' using errcode='22023';
  end if;
  if v_reason_code='OTHER_EXCEPTIONAL_OFFICE_INTERVENTION' and v_reason_note is null then
    raise exception 'ROUTE_INTERVENTION_REASON_NOTE_REQUIRED' using errcode='22023';
  end if;

  v_retire_workflow:=v_action in (
    'CONVERT_QR_TO_MANUAL','DISABLE_QR','INVALIDATE_QR','REISSUE_QR',
    'SWITCH_TO_MANUAL','SWITCH_DAILY_TO_MANUAL'
  );
  v_retirement_reason:=case when v_action in ('INVALIDATE_QR','REISSUE_QR')
    then 'QR_REPLACED_BY_OFFICE' else v_reason_code end;
  if v_retire_workflow then
    v_primary_workflow_id:=nullif(case
        when v_action in ('CONVERT_QR_TO_MANUAL','DISABLE_QR','INVALIDATE_QR','REISSUE_QR')
          then coalesce(v_context->>'paper_workflow_id',v_context->>'linked_workflow_id')
        else v_context->>'linked_workflow_id'
      end,'')::uuid;
    v_primary_workflow_result:=private._timesheet_route_supersede_candidate_v1(
      v_primary_workflow_id,v_action,
      v_retirement_reason,v_reason_note,p_actor_user_id,p_now_utc
    );
    v_workflow_result:=v_primary_workflow_result;
  end if;
  if coalesce(
      (v_context->>'incomplete_expense_claim_removal_required')::boolean,false
    ) then
    v_incomplete_expense_workflow_id:=nullif(
      v_context->>'incomplete_expense_workflow_id',''
    )::uuid;
    if v_incomplete_expense_workflow_id is null then
      raise exception 'CANDIDATE_INCOMPLETE_EXPENSE_WORKFLOW_CONTEXT_CHANGED'
        using errcode='40001';
    end if;
    if v_incomplete_expense_workflow_id is not distinct from v_primary_workflow_id then
      v_incomplete_workflow_result:=v_primary_workflow_result;
    else
      v_incomplete_workflow_result:=private._timesheet_route_supersede_candidate_v1(
        v_incomplete_expense_workflow_id,v_action,
        v_retirement_reason,v_reason_note,p_actor_user_id,p_now_utc
      );
    end if;
    if not coalesce(
      (v_incomplete_workflow_result->>'workflow_changed')::boolean,false
    ) then
      raise exception 'CANDIDATE_INCOMPLETE_EXPENSE_CLAIM_REMOVAL_NOT_PROVEN'
        using errcode='40001',detail=v_incomplete_workflow_result::text;
    end if;
    v_workflow_result:=v_primary_workflow_result||jsonb_build_object(
      'workflow_changed',coalesce(
          (v_primary_workflow_result->>'workflow_changed')::boolean,false
        ) or coalesce(
          (v_incomplete_workflow_result->>'workflow_changed')::boolean,false
        ),
      'manager_request_cancelled',coalesce(
          (v_primary_workflow_result->>'manager_request_cancelled')::boolean,false
        ) or coalesce(
          (v_incomplete_workflow_result->>'manager_request_cancelled')::boolean,false
        ),
      'manager_cancellation_email_queued',coalesce(
          (v_primary_workflow_result->>'manager_cancellation_email_queued')::boolean,false
        ) or coalesce(
          (v_incomplete_workflow_result->>'manager_cancellation_email_queued')::boolean,false
        ),
      'manager_cancellation_mail_ids',
        coalesce(v_primary_workflow_result->'manager_cancellation_mail_ids','[]'::jsonb)
        ||case
          when v_incomplete_expense_workflow_id is distinct from v_primary_workflow_id
            then coalesce(
              v_incomplete_workflow_result->'manager_cancellation_mail_ids','[]'::jsonb
            )
          else '[]'::jsonb
        end,
      'incomplete_expense_claim_removed',true,
      'incomplete_expense_workflow_id',v_incomplete_expense_workflow_id,
      'primary_workflow_retirement',v_primary_workflow_result,
      'incomplete_expense_workflow_retirement',v_incomplete_workflow_result
    );
  end if;
  -- Last transactional invariant before any route/version rotation: no live
  -- PAPER workflow may remain tied exclusively to the worked-row source that
  -- is about to become historical.  This is intentionally mail-independent;
  -- WORKER_DRAFT and approval-stage workflows have no receipt to discover.
  if v_action in (
      'CONVERT_QR_TO_MANUAL','DISABLE_QR','INVALIDATE_QR','REISSUE_QR'
    ) and exists(
      select 1
      from public.candidate_submission_workflows workflow
      where workflow.route='PAPER'
        and workflow.state not in (
          'FINALISED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'
        )
        and workflow.contract_id is not distinct from v_current.contract_id
        and workflow.week_ending_date is not distinct from v_current.week_ending_date
        and exists(
          select 1
          from public.timesheets binding_source
          where binding_source.timesheet_id in (
              workflow.target_timesheet_id,workflow.anchor_timesheet_id
            )
            and binding_source.booking_id=v_current.booking_id
        )
    ) then
    raise exception 'CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT',
        'current_timesheet_id',v_current.timesheet_id,
        'target_action',v_action
      )::text;
  end if;
  -- Equivalent mail-independent invariant for ELECTRONIC source rotation.
  -- The selected mutable workflow is superseded above.  Historical FINALISED
  -- truth may remain, but no active or amendable Candidate workflow may still
  -- resolve only through the booking/version family about to become historic.
  if v_action in ('SWITCH_TO_MANUAL','SWITCH_DAILY_TO_MANUAL') and exists(
      select 1
      from public.candidate_submission_workflows workflow
      where workflow.state not in (
          'FINALISED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'
        )
        and workflow.contract_id is not distinct from v_current.contract_id
        and exists(
          select 1
          from public.timesheets binding_source
          where binding_source.timesheet_id in (
              workflow.target_timesheet_id,workflow.anchor_timesheet_id
            )
            and binding_source.booking_id=v_current.booking_id
        )
    ) then
    raise exception 'CANDIDATE_ROUTE_ACTIVE_WORKFLOW_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_ROUTE_ACTIVE_WORKFLOW_CONFLICT',
        'current_timesheet_id',v_current.timesheet_id,
        'target_action',v_action
      )::text;
  end if;
  perform set_config('cloudtms.route_transition_confirmed','on',true);
  v_result:=public.timesheet_route_version_rotate(
    v_current.timesheet_id,p_expected_timesheet_id,v_action,p_actor_user_id,
    case when v_action='REVERT_TO_ELECTRONIC' then true else p_allow_manual_only end
  );
  v_new_timesheet_id:=nullif(v_result->>'new_timesheet_id','')::uuid;
  v_new_version:=nullif(v_result->>'new_version','')::integer;

  if v_reason_required then
    update public.timesheets set
      revoked_reason=left(coalesce(revoked_reason,v_action)||':'||v_reason_code,500),
      updated_at=p_now_utc
    where timesheet_id=v_current.timesheet_id and is_current=false;
  end if;
  perform private._candidate_audit_v1(
    'timesheet_route',v_current.timesheet_id::text,'ROUTE_VERSION_CONFIRMED',
    v_context-'context_sha256',
    jsonb_build_object('result',v_result,'reason_code',nullif(v_reason_code,''),
      'reason_note',v_reason_note,'idempotency_key',nullif(btrim(coalesce(p_idempotency_key,'')),'')),
    nullif(v_reason_code||case when v_reason_note is not null then ': '||v_reason_note else '' end,''),
    p_actor_user_id,nullif(btrim(coalesce(p_idempotency_key,'')),''),p_now_utc
  );

  if v_action='ALLOW_ELECTRONIC_AGAIN'
     and v_new_timesheet_id is not null then
    v_notification_result:=private._timesheet_route_resubmission_notifications_v1(
      v_current.timesheet_id,v_new_timesheet_id,v_new_version,v_action,p_now_utc
    );
  end if;
  return v_result||jsonb_build_object(
    'warning_code',v_context->>'warning_code',
    'confirmed_context_sha256',v_context->>'context_sha256',
    'intervention_reason_code',nullif(v_reason_code,''),
    'intervention_reason_note',v_reason_note,
    'workflow_retirement',v_workflow_result,
    'fresh_submission_required',v_action in ('ALLOW_ELECTRONIC_AGAIN','ALLOW_QR_AGAIN','INVALIDATE_QR','REISSUE_QR'),
    'notification_deferred_until_pack_ready',v_action in ('ALLOW_QR_AGAIN','INVALIDATE_QR','REISSUE_QR'),
    'retain_historical_evidence',true
  )||v_notification_result;
end;
$function$;

-- timesheet_route_version_preview_v1(uuid,text)
CREATE OR REPLACE FUNCTION public.timesheet_route_version_preview_v1(p_current_timesheet_id uuid, p_target_action text)
 RETURNS jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'extensions', 'pg_temp'
AS $function$
  select private._timesheet_route_change_context_v1(p_current_timesheet_id,p_target_action);
$function$;

-- timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean)
CREATE OR REPLACE FUNCTION public.timesheet_route_version_rotate(p_current_timesheet_id uuid, p_expected_timesheet_id uuid, p_target_action text, p_actor_user_id uuid, p_allow_manual_only boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_environment text;
  v_office_service boolean:=false;
begin
  select candidate_app_environment into v_environment
  from public.settings_defaults where id=1;
  v_environment:=private._candidate_assert_environment(v_environment);
  v_office_service:=private._candidate_office_service_context_valid_v1(
    v_environment,p_actor_user_id,'ROUTE_CONFIRM'
  );
  if not private._candidate_feature_enabled_current_v1('candidate_route_confirmation')
     and not v_office_service then
    return private._timesheet_route_version_legacy_v1(
      p_current_timesheet_id,p_expected_timesheet_id,p_target_action,
      p_actor_user_id,p_allow_manual_only
    );
  end if;
  if coalesce(current_setting('cloudtms.route_transition_confirmed',true),'') <> 'on' then
    raise exception 'ROUTE_CHANGE_CONFIRMATION_REQUIRED'
      using errcode='55000',detail=jsonb_build_object(
        'code','ROUTE_CHANGE_CONFIRMATION_REQUIRED',
        'preview_rpc','timesheet_route_version_preview_v1',
        'confirm_rpc','timesheet_route_version_confirmed_v1'
      )::text;
  end if;
  return private._timesheet_route_version_core_v1(
    p_current_timesheet_id,p_expected_timesheet_id,p_target_action,
    p_actor_user_id,p_allow_manual_only
  );
end;
$function$;

-- timesheet_standard_delete_apply_v1(uuid,uuid,uuid,text)
CREATE OR REPLACE FUNCTION public.timesheet_standard_delete_apply_v1(p_timesheet_id uuid, p_actor_user_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_preview jsonb;
  v_recheck jsonb;
  v_decision text := 'BLOCKED';
  v_current_timesheet_id uuid := NULL;
  v_recheck_current_timesheet_id uuid := NULL;
  v_initial_row_signature text := NULL;
  v_recheck_row_signature text := NULL;
  v_booking_id text := NULL;
  v_recheck_booking_id text := NULL;

  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_nhsp_shift_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_all_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_all_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_contract_ids uuid[] := ARRAY[]::uuid[];

  v_recheck_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_nhsp_shift_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_preserved_source_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_preserved_source_contract_week_ids uuid[] := ARRAY[]::uuid[];

  v_r2_keys text[] := ARRAY[]::text[];
  v_locked_contracts integer := 0;
  v_locked_timesheets integer := 0;
  v_locked_contract_weeks integer := 0;
  v_locked_nhsp_shifts integer := 0;
  v_detached_contract_weeks integer := 0;
  v_detached_nhsp_shifts integer := 0;
  v_deleted_count integer := 0;
BEGIN
  PERFORM set_config('lock_timeout', '750ms', true);

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ID_REQUIRED';
  END IF;
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'ACTOR_USER_ID_REQUIRED';
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'EXPECTED_TIMESHEET_ID_REQUIRED';
  END IF;
  IF NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'EXPECTED_ROW_SIGNATURE_REQUIRED';
  END IF;

  v_preview := public.timesheet_standard_delete_preview_v1(
    p_timesheet_id,
    p_actor_user_id,
    p_expected_timesheet_id,
    p_expected_row_signature
  );
  v_decision := COALESCE(v_preview ->> 'decision', 'BLOCKED');

  IF v_decision <> 'PERMANENT_DELETE' THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', CASE
        WHEN v_decision = 'ARCHIVE_REQUIRED' THEN 'ARCHIVE_REQUIRED'
        ELSE 'DELETE_BLOCKED'
      END
    );
  END IF;

  v_current_timesheet_id := NULLIF(v_preview ->> 'current_timesheet_id', '')::uuid;
  v_initial_row_signature := NULLIF(BTRIM(COALESCE(v_preview ->> 'current_row_signature', '')), '');
  v_booking_id := NULLIF(v_preview ->> 'booking_id', '');

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'timesheet_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'contract_week_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_nhsp_shift_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'nhsp_shift_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_preserved_source_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'preserved_source_timesheet_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_preserved_source_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'preserved_source_contract_week_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(DISTINCT all_timesheet_ids.id ORDER BY all_timesheet_ids.id), ARRAY[]::uuid[])
    INTO v_all_timesheet_ids
  FROM unnest(v_timesheet_ids || v_preserved_source_timesheet_ids) AS all_timesheet_ids(id)
  WHERE all_timesheet_ids.id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT all_contract_week_ids.id ORDER BY all_contract_week_ids.id), ARRAY[]::uuid[])
    INTO v_all_contract_week_ids
  FROM unnest(v_contract_week_ids || v_preserved_source_contract_week_ids) AS all_contract_week_ids(id)
  WHERE all_contract_week_ids.id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT represented_contract.contract_id ORDER BY represented_contract.contract_id), ARRAY[]::uuid[])
    INTO v_contract_ids
  FROM (
    SELECT represented_contract_week.contract_id
    FROM public.contract_weeks AS represented_contract_week
    WHERE represented_contract_week.id = ANY(v_all_contract_week_ids)
      AND represented_contract_week.contract_id IS NOT NULL
  ) AS represented_contract;

  IF v_current_timesheet_id IS NULL
     OR NULLIF(BTRIM(COALESCE(v_booking_id, '')), '') IS NULL
     OR v_current_timesheet_id IS DISTINCT FROM p_expected_timesheet_id
     OR v_initial_row_signature IS DISTINCT FROM BTRIM(p_expected_row_signature) THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'DELETE_PREVIEW_STALE'
    );
  END IF;

  IF COALESCE(array_length(v_timesheet_ids, 1), 0) = 0 THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'REMOVAL_UNIT_EMPTY'
    );
  END IF;

  IF COALESCE(array_length(v_all_timesheet_ids, 1), 0) > 64
     OR COALESCE(array_length(v_all_contract_week_ids, 1), 0) > 64
     OR COALESCE(array_length(v_nhsp_shift_ids, 1), 0) > 512 THEN
    RAISE EXCEPTION USING MESSAGE = 'REMOVAL_UNIT_TOO_LARGE';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM unnest(v_timesheet_ids) AS target_timesheet(id)
    WHERE target_timesheet.id = ANY(v_preserved_source_timesheet_ids)
  ) OR EXISTS (
    SELECT 1
    FROM unnest(v_contract_week_ids) AS target_contract_week(id)
    WHERE target_contract_week.id = ANY(v_preserved_source_contract_week_ids)
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_OVERLAPS_DELETE_TARGET';
  END IF;

  -- Use the same booking-series advisory authority as Timesheet rotation.
  -- A concurrent rotation must finish before this Delete can confirm its removal unit.
  IF NOT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtext(v_booking_id)::bigint) THEN
    RAISE EXCEPTION USING
      ERRCODE = '55P03',
      MESSAGE = 'BOOKING_LOCK_NOT_AVAILABLE';
  END IF;

  -- Lock every represented Contract root before its child rows. A Contract Week
  -- insert or contract reassignment must take a foreign-key key-share lock on the
  -- destination Contract, so it cannot create a preserved-source phantom after
  -- these roots have been locked.
  SELECT COUNT(*)::integer
    INTO v_locked_contracts
  FROM (
    SELECT contract_root.id
    FROM public.contracts AS contract_root
    WHERE contract_root.id = ANY(v_contract_ids)
    ORDER BY contract_root.id
    FOR UPDATE NOWAIT
  ) AS locked_contracts;

  IF v_locked_contracts <> COALESCE(array_length(v_contract_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  -- Freeze all existing Contract Weeks below the represented Contract roots.
  -- This prevents an existing sibling week from changing date/sequence/type and
  -- entering or leaving the canonical preserved-source predicate after recheck.
  PERFORM 1
  FROM public.contract_weeks AS contract_week_predicate_row
  WHERE contract_week_predicate_row.contract_id = ANY(v_contract_ids)
  ORDER BY contract_week_predicate_row.contract_id,
           contract_week_predicate_row.week_ending_date,
           contract_week_predicate_row.additional_seq,
           contract_week_predicate_row.id
  FOR UPDATE NOWAIT;

  -- Lock the complete booking series together with every target or preserved
  -- Timesheet in deterministic UUID order. The advisory lock serialises standard
  -- rotation authorities; the row locks freeze exact identity and parent links.
  SELECT COUNT(*) FILTER (
           WHERE locked_timesheet.timesheet_id = ANY(v_all_timesheet_ids)
         )::integer
    INTO v_locked_timesheets
  FROM (
    SELECT target_timesheet.timesheet_id
    FROM public.timesheets AS target_timesheet
    WHERE target_timesheet.booking_id = v_booking_id
       OR target_timesheet.timesheet_id = ANY(v_all_timesheet_ids)
    ORDER BY target_timesheet.timesheet_id
    FOR UPDATE NOWAIT
  ) AS locked_timesheet;

  IF v_locked_timesheets <> COALESCE(array_length(v_all_timesheet_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  PERFORM 1
  FROM public.timesheets_financials AS target_financial
  WHERE target_financial.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY target_financial.timesheet_id, target_financial.id
  FOR UPDATE;

  SELECT COUNT(*)::integer
    INTO v_locked_contract_weeks
  FROM (
    SELECT target_contract_week.id
    FROM public.contract_weeks AS target_contract_week
    WHERE target_contract_week.id = ANY(v_all_contract_week_ids)
    ORDER BY target_contract_week.id
    FOR UPDATE NOWAIT
  ) AS locked_contract_weeks;

  IF v_locked_contract_weeks <> COALESCE(array_length(v_all_contract_week_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.contract_weeks AS represented_contract_week
    WHERE represented_contract_week.id = ANY(v_all_contract_week_ids)
      AND NOT (represented_contract_week.contract_id = ANY(v_contract_ids))
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  SELECT COUNT(*)::integer
    INTO v_locked_nhsp_shifts
  FROM (
    SELECT target_shift.id
    FROM public.nhsp_shifts AS target_shift
    WHERE target_shift.id = ANY(v_nhsp_shift_ids)
    ORDER BY target_shift.id
    FOR UPDATE
  ) AS locked_nhsp_shifts;

  IF v_locked_nhsp_shifts <> COALESCE(array_length(v_nhsp_shift_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'NHSP_SHIFT_TARGET_SET_CHANGED';
  END IF;

  PERFORM 1
  FROM public.manual_timesheet_queue AS queue_row
  WHERE queue_row.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY queue_row.id
  FOR UPDATE;

  PERFORM 1
  FROM public.timesheet_evidence AS evidence_row
  WHERE evidence_row.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY evidence_row.id
  FOR UPDATE;

  IF EXISTS (
    SELECT 1
    FROM public.contract_weeks AS target_contract_week
    WHERE target_contract_week.id = ANY(v_contract_week_ids)
      AND (
        target_contract_week.timesheet_id IS NULL
        OR target_contract_week.timesheet_id <> ALL(v_timesheet_ids)
      )
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'CONTRACT_WEEK_TARGET_SET_CHANGED';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.nhsp_shifts AS target_shift
    WHERE target_shift.id = ANY(v_nhsp_shift_ids)
      AND (
        target_shift.timesheet_id IS NULL
        OR target_shift.timesheet_id <> ALL(v_timesheet_ids)
      )
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'NHSP_SHIFT_TARGET_SET_CHANGED';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.contract_weeks AS source_contract_week
    WHERE source_contract_week.id = ANY(v_preserved_source_contract_week_ids)
      AND (
        COALESCE(source_contract_week.is_adjustment, false)
        OR COALESCE(source_contract_week.additional_seq, 0) <> 0
        OR NOT EXISTS (
          SELECT 1
          FROM public.contract_weeks AS target_contract_week
          WHERE target_contract_week.id = ANY(v_contract_week_ids)
            AND target_contract_week.contract_id = source_contract_week.contract_id
            AND target_contract_week.week_ending_date = source_contract_week.week_ending_date
        )
        OR (
          source_contract_week.timesheet_id IS NOT NULL
          AND source_contract_week.timesheet_id <> ALL(v_preserved_source_timesheet_ids)
        )
      )
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.timesheets AS source_timesheet
    WHERE source_timesheet.timesheet_id = ANY(v_preserved_source_timesheet_ids)
      AND NOT EXISTS (
        SELECT 1
        FROM public.timesheets AS target_timesheet
        WHERE target_timesheet.timesheet_id = ANY(v_timesheet_ids)
          AND target_timesheet.parent_timesheet_id = source_timesheet.timesheet_id
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.contract_weeks AS source_contract_week
        WHERE source_contract_week.id = ANY(v_preserved_source_contract_week_ids)
          AND source_contract_week.timesheet_id = source_timesheet.timesheet_id
      )
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  v_recheck := public.timesheet_standard_delete_preview_v1(
    p_timesheet_id,
    p_actor_user_id,
    p_expected_timesheet_id,
    p_expected_row_signature
  );
  v_decision := COALESCE(v_recheck ->> 'decision', 'BLOCKED');
  v_recheck_current_timesheet_id := NULLIF(v_recheck ->> 'current_timesheet_id', '')::uuid;
  v_recheck_row_signature := NULLIF(BTRIM(COALESCE(v_recheck ->> 'current_row_signature', '')), '');
  v_recheck_booking_id := NULLIF(v_recheck ->> 'booking_id', '');

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'timesheet_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'contract_week_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_nhsp_shift_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'nhsp_shift_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_preserved_source_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'preserved_source_timesheet_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_preserved_source_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'preserved_source_contract_week_ids', '[]'::jsonb)) AS ids(value);

  IF v_decision <> 'PERMANENT_DELETE'
     OR v_recheck_current_timesheet_id IS DISTINCT FROM v_current_timesheet_id
     OR v_recheck_booking_id IS DISTINCT FROM v_booking_id
     OR v_recheck_row_signature IS DISTINCT FROM v_initial_row_signature
     OR v_recheck_row_signature IS DISTINCT FROM BTRIM(p_expected_row_signature)
     OR v_recheck_timesheet_ids IS DISTINCT FROM v_timesheet_ids
     OR v_recheck_contract_week_ids IS DISTINCT FROM v_contract_week_ids
     OR v_recheck_nhsp_shift_ids IS DISTINCT FROM v_nhsp_shift_ids
     OR v_recheck_preserved_source_timesheet_ids IS DISTINCT FROM v_preserved_source_timesheet_ids
     OR v_recheck_preserved_source_contract_week_ids IS DISTINCT FROM v_preserved_source_contract_week_ids THEN
    RETURN v_recheck || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'REMOVAL_UNIT_CHANGED',
      'locked_target_set', jsonb_build_object(
        'timesheet_ids', to_jsonb(v_timesheet_ids),
        'contract_week_ids', to_jsonb(v_contract_week_ids),
        'nhsp_shift_ids', to_jsonb(v_nhsp_shift_ids),
        'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
        'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids)
      )
    );
  END IF;

  SELECT COALESCE(
           array_agg(exact_key.queue_key ORDER BY convert_to(exact_key.queue_key, 'UTF8')),
           ARRAY[]::text[]
         )
    INTO v_r2_keys
  FROM (
    SELECT DISTINCT ON (convert_to(raw_key.queue_key, 'UTF8')) raw_key.queue_key
    FROM (
      SELECT direct_key.key_value AS queue_key
      FROM public.timesheets AS target_timesheet
      CROSS JOIN LATERAL unnest(ARRAY[
        target_timesheet.manual_pdf_r2_key,
        target_timesheet.r2_nurse_key,
        target_timesheet.r2_auth_key,
        target_timesheet.qr_r2_key
      ]) AS direct_key(key_value)
      WHERE target_timesheet.timesheet_id = ANY(v_timesheet_ids)

      UNION ALL

      SELECT evidence_row.storage_key AS queue_key
      FROM public.timesheet_evidence AS evidence_row
      WHERE evidence_row.timesheet_id = ANY(v_timesheet_ids)

      UNION ALL

      SELECT queue_row.r2_key AS queue_key
      FROM public.manual_timesheet_queue AS queue_row
      WHERE queue_row.timesheet_id = ANY(v_timesheet_ids)

      UNION ALL

      SELECT financial_key.key_value AS queue_key
      FROM public.timesheets_financials AS target_financial
      CROSS JOIN LATERAL unnest(ARRAY[
        target_financial.expenses_evidence_r2_key,
        target_financial.mileage_evidence_r2_key
      ]) AS financial_key(key_value)
      WHERE target_financial.timesheet_id = ANY(v_timesheet_ids)

      UNION ALL

      SELECT manifest_key.key_value AS queue_key
      FROM public.timesheets_financials AS target_financial
      CROSS JOIN LATERAL (
        WITH RECURSIVE manifest_roots(root_value) AS (
          SELECT COALESCE(target_financial.expenses_evidence_manifest, 'null'::jsonb)
          UNION ALL
          SELECT COALESCE(target_financial.mileage_evidence_manifest, 'null'::jsonb)
        ), manifest_walk(value, edge_key, depth) AS (
          SELECT manifest_roots.root_value, NULL::text, 0
          FROM manifest_roots

          UNION ALL

          SELECT manifest_child.value, manifest_child.edge_key, manifest_walk.depth + 1
          FROM manifest_walk
          CROSS JOIN LATERAL (
            SELECT object_entry.key AS edge_key, object_entry.value
            FROM jsonb_each(
              CASE
                WHEN jsonb_typeof(manifest_walk.value) = 'object' THEN manifest_walk.value
                ELSE '{}'::jsonb
              END
            ) AS object_entry(key, value)

            UNION ALL

            SELECT NULL::text AS edge_key, array_entry.value
            FROM jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(manifest_walk.value) = 'array' THEN manifest_walk.value
                ELSE '[]'::jsonb
              END
            ) AS array_entry(value)
          ) AS manifest_child
          WHERE manifest_walk.depth < 8
        )
        SELECT manifest_walk.value #>> '{}' AS key_value
        FROM manifest_walk
        WHERE LOWER(COALESCE(manifest_walk.edge_key, '')) IN (
          'r2_key',
          'storage_key',
          'file_key',
          'canonical_key',
          'object_key'
        )
          AND jsonb_typeof(manifest_walk.value) = 'string'
      ) AS manifest_key
      WHERE target_financial.timesheet_id = ANY(v_timesheet_ids)
    ) AS raw_key
    WHERE raw_key.queue_key IS NOT NULL
      AND BTRIM(raw_key.queue_key) <> ''
    ORDER BY convert_to(raw_key.queue_key, 'UTF8')
  ) AS exact_key;

  INSERT INTO public.audit_events (
    actor_user_id,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  ) VALUES (
    p_actor_user_id,
    'timesheets',
    v_current_timesheet_id::text,
    'TIMESHEET_PERMANENT_DELETE_APPLIED',
    jsonb_build_object(
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids),
      'nhsp_shift_ids', to_jsonb(v_nhsp_shift_ids),
      'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
      'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids),
      'decision', 'PERMANENT_DELETE'
    ),
    jsonb_build_object('deleted', true),
    'FINANCIALLY_CLEAN_TIMESHEET'
  );

  UPDATE public.contract_weeks AS target_contract_week
  SET timesheet_id = NULL,
      status = 'OPEN'::public.contract_week_status_enum,
      updated_at = now()
  WHERE target_contract_week.id = ANY(v_contract_week_ids)
    AND target_contract_week.timesheet_id = ANY(v_timesheet_ids);
  GET DIAGNOSTICS v_detached_contract_weeks = ROW_COUNT;

  IF v_detached_contract_weeks <> COALESCE(array_length(v_contract_week_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'CONTRACT_WEEK_TARGET_SET_CHANGED';
  END IF;

  UPDATE public.nhsp_shifts AS target_shift
  SET timesheet_id = NULL,
      updated_at = now()
  WHERE target_shift.id = ANY(v_nhsp_shift_ids)
    AND target_shift.timesheet_id = ANY(v_timesheet_ids);
  GET DIAGNOSTICS v_detached_nhsp_shifts = ROW_COUNT;

  IF v_detached_nhsp_shifts <> COALESCE(array_length(v_nhsp_shift_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'NHSP_SHIFT_TARGET_SET_CHANGED';
  END IF;

  DELETE FROM public.manual_timesheet_queue AS queue_row
  WHERE queue_row.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.timesheet_evidence AS evidence_row
  WHERE evidence_row.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.ts_pdfs_outbox AS pdf_outbox
  WHERE pdf_outbox.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.ts_financials_outbox AS financial_outbox
  WHERE financial_outbox.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.timesheet_validations AS validation_row
  WHERE validation_row.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.hr_results AS result_row
  WHERE result_row.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.hr_issue_emails AS issue_email
  WHERE issue_email.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.pay_item_snoozes AS snooze_row
  WHERE snooze_row.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.timesheet_summary_pay_state_cache AS pay_state_cache
  WHERE pay_state_cache.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.timesheets_financials AS target_financial
  WHERE target_financial.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.timesheets AS target_timesheet
  WHERE target_timesheet.timesheet_id = ANY(v_timesheet_ids);
  GET DIAGNOSTICS v_deleted_count = ROW_COUNT;

  IF v_deleted_count <> COALESCE(array_length(v_timesheet_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'DELETE_COUNT_MISMATCH';
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'kind', 'STANDARD_DELETE',
    'decision', 'PERMANENT_DELETE',
    'apply_performed', true,
    'deleted', true,
    'committed', true,
    'database_commit_confirmed', true,
    'current_timesheet_id', v_current_timesheet_id,
    'current_row_signature', v_initial_row_signature,
    'timesheet_ids', to_jsonb(v_timesheet_ids),
    'contract_week_ids', to_jsonb(v_contract_week_ids),
    'nhsp_shift_ids', to_jsonb(v_nhsp_shift_ids),
    'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
    'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids),
    'deleted_timesheet_ids', to_jsonb(v_timesheet_ids),
    'deleted_contract_week_ids', '[]'::jsonb,
    'detached_contract_week_ids', to_jsonb(v_contract_week_ids),
    'detached_nhsp_shift_ids', to_jsonb(v_nhsp_shift_ids),
    'detached_contract_weeks', v_detached_contract_weeks,
    'detached_nhsp_shifts', v_detached_nhsp_shifts,
    'deleted_timesheets', v_deleted_count,
    'r2_cleanup_keys', to_jsonb(v_r2_keys),
    'r2_cleanup_required', COALESCE(array_length(v_r2_keys, 1), 0) > 0
  );
EXCEPTION
  WHEN lock_not_available OR deadlock_detected THEN
    RETURN jsonb_build_object(
      'ok', false,
      'kind', 'STANDARD_DELETE',
      'decision', 'BLOCKED',
      'apply_performed', false,
      'error_code', 'LOCK_TIMEOUT',
      'message', 'The Timesheet is currently being changed. Refresh and try again.'
    );
END;
$function$;

-- timesheet_standard_delete_preview_v1(uuid,uuid,uuid,text)
CREATE OR REPLACE FUNCTION public.timesheet_standard_delete_preview_v1(p_timesheet_id uuid, p_actor_user_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_requested public.timesheets%ROWTYPE;
  v_current public.timesheets%ROWTYPE;
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_nhsp_shift_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_primary_contract_week_id uuid := NULL;
  v_history jsonb := '{}'::jsonb;
  v_blockers jsonb := '[]'::jsonb;
  v_decision text := 'BLOCKED';
  v_signature_payload jsonb := '{}'::jsonb;
  v_current_signature text := NULL;
  v_class jsonb := '{}'::jsonb;
  v_chain jsonb := '{}'::jsonb;
  v_unit jsonb := '{}'::jsonb;
  v_is_correction_pair boolean := false;
  v_pair_fingerprint text := NULL;
  v_pair_members jsonb := '[]'::jsonb;
  v_pair_net_hours numeric := 0;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ID_REQUIRED';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'ACTOR_USER_ID_REQUIRED';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM public.tms_users AS actor
    WHERE actor.id = p_actor_user_id
      AND actor.is_active = true
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'ACTOR_NOT_FOUND_OR_INACTIVE';
  END IF;

  SELECT t.*
    INTO v_requested
  FROM public.timesheets AS t
  WHERE t.timesheet_id = p_timesheet_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', true,
      'kind', 'STANDARD_DELETE',
      'decision', 'BLOCKED',
      'eligible', false,
      'blockers', jsonb_build_array(jsonb_build_object('code', 'TARGET_NOT_FOUND')),
      'blocked_reasons', jsonb_build_array(jsonb_build_object('code', 'TARGET_NOT_FOUND')),
      'timesheet_ids', '[]'::jsonb,
      'contract_week_ids', '[]'::jsonb,
      'nhsp_shift_ids', '[]'::jsonb,
      'preserved_source_timesheet_ids', '[]'::jsonb,
      'preserved_source_contract_week_ids', '[]'::jsonb,
      'retention_reasons', '[]'::jsonb,
      'advance', jsonb_build_object(
        'active', false,
        'clearable', false,
        'consumed', false,
        'historical', false
      )
    );
  END IF;

  SELECT t.*
    INTO v_current
  FROM public.timesheets AS t
  WHERE t.booking_id = v_requested.booking_id
    AND t.is_current = true
  ORDER BY t.version DESC NULLS LAST,
           t.updated_at DESC NULLS LAST,
           t.created_at DESC NULLS LAST,
           t.timesheet_id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', true,
      'kind', 'STANDARD_DELETE',
      'decision', 'BLOCKED',
      'eligible', false,
      'requested_timesheet_id', p_timesheet_id,
      'blockers', jsonb_build_array(jsonb_build_object('code', 'CURRENT_TIMESHEET_NOT_FOUND')),
      'blocked_reasons', jsonb_build_array(jsonb_build_object('code', 'CURRENT_TIMESHEET_NOT_FOUND')),
      'timesheet_ids', '[]'::jsonb,
      'contract_week_ids', '[]'::jsonb,
      'nhsp_shift_ids', '[]'::jsonb,
      'preserved_source_timesheet_ids', '[]'::jsonb,
      'preserved_source_contract_week_ids', '[]'::jsonb,
      'retention_reasons', '[]'::jsonb,
      'advance', jsonb_build_object(
        'active', false,
        'clearable', false,
        'consumed', false,
        'historical', false
      )
    );
  END IF;

  -- Import-authoritative changed-hours members are one exact two-row removal
  -- unit.  Resolve the requested correction generation, never the wider Weekly
  -- parent chain or a neighbouring generation.
  v_class:=public._ctms_import_correction_classify_v1(v_current.timesheet_id);
  IF COALESCE((v_class->>'is_import_authoritative_correction')::boolean,false) THEN
    v_chain:=public.timesheet_correction_chain_scope_v1(v_current.timesheet_id,false,32,100);
    v_unit:=v_chain->'requested_correction_unit';
    IF COALESCE((v_chain->>'valid')::boolean,false) IS TRUE
       AND COALESCE((v_unit->>'valid')::boolean,false) IS TRUE
       AND v_unit->>'correction_shape'='REVERSAL_REPLACEMENT'
       AND COALESCE((v_unit->>'expected_member_count')::integer,0)=2 THEN
      SELECT COALESCE(array_agg(value::uuid ORDER BY value::uuid),ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM jsonb_array_elements_text(v_unit->'member_ids') member(value);
      v_is_correction_pair:=cardinality(v_timesheet_ids)=2;
      v_pair_fingerprint:=public._import_review_hash_v1(concat_ws('|',
        'correction-pair-removal-v1',v_chain->>'chain_fingerprint',v_unit->>'correction_id',
        array_to_string(v_timesheet_ids,',')));
    ELSE
      -- Archived pairs are intentionally excluded from some active-chain
      -- projections.  Their exact durable correction identity remains enough
      -- to support an atomic Unarchive preview, but never a guessed role.
      SELECT COALESCE(array_agg(t.timesheet_id ORDER BY t.timesheet_id),ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM public.timesheets t
      WHERE t.correction_id=v_current.correction_id AND t.is_current
        AND t.correction_kind IN ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT');
      IF cardinality(v_timesheet_ids)=2
         AND (SELECT count(*) FROM public.timesheets t WHERE t.timesheet_id=ANY(v_timesheet_ids)
                AND t.correction_kind='CHANGED_HOURS_REVERSAL')=1
         AND (SELECT count(*) FROM public.timesheets t WHERE t.timesheet_id=ANY(v_timesheet_ids)
                AND t.correction_kind='CHANGED_HOURS_REPLACEMENT')=1 THEN
        v_is_correction_pair:=true;
        v_unit:=jsonb_build_object('valid',true,'correction_id',v_current.correction_id,
          'correction_shape','REVERSAL_REPLACEMENT','expected_member_count',2,
          'member_ids',to_jsonb(v_timesheet_ids));
        v_pair_fingerprint:=public._import_review_hash_v1(concat_ws('|',
          'correction-pair-removal-v1',v_current.correction_id,array_to_string(v_timesheet_ids,',')));
      ELSE
        v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object(
          'code','CORRECTION_PAIR_MALFORMED','detail',v_chain));
      END IF;
    END IF;
  END IF;
  IF NOT v_is_correction_pair THEN
    -- Ordinary and non-authoritative paths keep the installed single-row unit.
    v_timesheet_ids:=ARRAY[v_current.timesheet_id]::uuid[];
  END IF;

  SELECT COALESCE(array_agg(DISTINCT cw.id ORDER BY cw.id), ARRAY[]::uuid[])
    INTO v_contract_week_ids
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = ANY(v_timesheet_ids);

  SELECT cw.id INTO v_primary_contract_week_id
  FROM public.contract_weeks cw
  WHERE cw.timesheet_id=v_current.timesheet_id
  ORDER BY cw.id LIMIT 1;

  SELECT COALESCE(array_agg(DISTINCT nhsp_shift.id ORDER BY nhsp_shift.id), ARRAY[]::uuid[])
    INTO v_nhsp_shift_ids
  FROM public.nhsp_shifts AS nhsp_shift
  WHERE nhsp_shift.timesheet_id = ANY(v_timesheet_ids);

  SELECT COALESCE(array_agg(DISTINCT source_contract_week.id ORDER BY source_contract_week.id), ARRAY[]::uuid[])
    INTO v_preserved_source_contract_week_ids
  FROM public.contract_weeks AS target_contract_week
  JOIN public.contract_weeks AS source_contract_week
    ON source_contract_week.contract_id = target_contract_week.contract_id
   AND source_contract_week.week_ending_date = target_contract_week.week_ending_date
   AND COALESCE(source_contract_week.additional_seq, 0) = 0
   AND COALESCE(source_contract_week.is_adjustment, false) = false
  WHERE target_contract_week.id = ANY(v_contract_week_ids)
    AND source_contract_week.id <> ALL(v_contract_week_ids);

  SELECT COALESCE(array_agg(DISTINCT preserved_row.preserved_timesheet_id ORDER BY preserved_row.preserved_timesheet_id), ARRAY[]::uuid[])
    INTO v_preserved_source_timesheet_ids
  FROM (
    SELECT source_contract_week.timesheet_id AS preserved_timesheet_id
    FROM public.contract_weeks AS source_contract_week
    WHERE source_contract_week.id = ANY(v_preserved_source_contract_week_ids)
      AND source_contract_week.timesheet_id IS NOT NULL

    UNION

    SELECT target_timesheet.parent_timesheet_id AS preserved_timesheet_id
    FROM public.timesheets AS target_timesheet
    WHERE target_timesheet.timesheet_id = ANY(v_timesheet_ids)
      AND target_timesheet.parent_timesheet_id IS NOT NULL
  ) AS preserved_row
  WHERE preserved_row.preserved_timesheet_id IS NOT NULL
    AND preserved_row.preserved_timesheet_id <> ALL(v_timesheet_ids);

  IF p_expected_timesheet_id IS NULL THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'EXPECTED_TIMESHEET_ID_REQUIRED',
      'current_timesheet_id', v_current.timesheet_id,
      'message', 'The current Timesheet identity is required. Refresh before continuing.'
    ));
  ELSIF p_expected_timesheet_id IS DISTINCT FROM v_current.timesheet_id THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'EXPECTED_TIMESHEET_MISMATCH',
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current.timesheet_id
    ));
  END IF;

  v_signature_payload := public.timesheet_lifecycle_guard_signature_v1(
    v_current.timesheet_id,
    v_primary_contract_week_id,
    false
  );
  v_current_signature := NULLIF(BTRIM(COALESCE(
    v_signature_payload ->> 'backend_row_signature',
    v_signature_payload ->> 'row_signature',
    v_signature_payload ->> 'signature',
    ''
  )), '');

  IF NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'EXPECTED_ROW_SIGNATURE_REQUIRED',
      'current_row_signature', v_current_signature,
      'message', 'The current Timesheet lifecycle signature is required. Refresh before continuing.'
    ));
  ELSIF v_current_signature IS DISTINCT FROM BTRIM(p_expected_row_signature) THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'ROW_SIGNATURE_MISMATCH',
      'expected_row_signature', BTRIM(p_expected_row_signature),
      'current_row_signature', v_current_signature
    ));
  END IF;

  IF NOT v_is_correction_pair AND v_current.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum THEN
    IF COALESCE(v_current.is_adjustment, false)
       OR EXISTS (
         SELECT 1
         FROM public.contract_weeks AS cw
         WHERE cw.timesheet_id = v_current.timesheet_id
           AND (COALESCE(cw.is_adjustment, false) OR COALESCE(cw.additional_seq, 0) > 0)
       ) THEN
      v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
        'code', 'WEEKLY_MANUAL_ADJUSTMENT_PREVIEW_REQUIRED'
      ));
    ELSE
      v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
        'code', 'WEEKLY_CHAIN_PREVIEW_REQUIRED'
      ));
    END IF;
  END IF;

  IF NOT v_is_correction_pair AND (UPPER(COALESCE(v_current.adjustment_origin, '')) IN ('IMPORT_CORRECTION', 'IMPORT_CANCELLATION')
     OR NULLIF(BTRIM(COALESCE(v_current.correction_kind, '')), '') IS NOT NULL
     OR v_current.correction_id IS NOT NULL) THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'IMPORT_DERIVED_CHILD',
      'message', 'Import-derived children must be handled through the parent-chain removal path.'
    ));
  END IF;

  IF v_is_correction_pair THEN
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'timesheet_id',t.timesheet_id,'correction_kind',t.correction_kind,
      'signed_hours',COALESCE(tf.hours_day,0)+COALESCE(tf.hours_night,0)+COALESCE(tf.hours_sat,0)+COALESCE(tf.hours_sun,0)+COALESCE(tf.hours_bh,0),
      'authorised',t.authorised_at_server IS NOT NULL OR tf.authorised_at_utc IS NOT NULL,
      'invoice_linked',EXISTS(SELECT 1 FROM public.invoice_lines il WHERE il.timesheet_id=t.timesheet_id),
      'paid',tf.paid_at_utc IS NOT NULL,'archived',t.archived_at_utc IS NOT NULL)
      ORDER BY CASE t.correction_kind WHEN 'CHANGED_HOURS_REVERSAL' THEN 1 ELSE 2 END),'[]'::jsonb),
      COALESCE(sum(COALESCE(tf.hours_day,0)+COALESCE(tf.hours_night,0)+COALESCE(tf.hours_sat,0)+COALESCE(tf.hours_sun,0)+COALESCE(tf.hours_bh,0)),0)
    INTO v_pair_members,v_pair_net_hours
    FROM public.timesheets t
    LEFT JOIN public.timesheets_financials tf ON tf.timesheet_id=t.timesheet_id AND tf.is_current=true
    WHERE t.timesheet_id=ANY(v_timesheet_ids);

    IF (SELECT count(*) FROM public.timesheets t WHERE t.timesheet_id=ANY(v_timesheet_ids)
          AND t.archived_at_utc IS NOT NULL)=1 THEN
      v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','CORRECTION_PAIR_PARTIAL_ARCHIVE'));
    END IF;
    IF EXISTS(SELECT 1 FROM public.timesheets t LEFT JOIN public.timesheets_financials tf
        ON tf.timesheet_id=t.timesheet_id AND tf.is_current=true
      WHERE t.timesheet_id=ANY(v_timesheet_ids)
        AND (t.authorised_at_server IS NOT NULL OR tf.authorised_at_utc IS NOT NULL)) THEN
      v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','CORRECTION_PAIR_MUST_BE_UNAUTHORISED'));
    END IF;
    IF EXISTS(
      SELECT 1 FROM public.timesheets later
      WHERE later.is_current AND later.archived_at_utc IS NULL
        AND later.adjustment_origin='IMPORT_CORRECTION'
        AND later.parent_timesheet_id=(
          SELECT replacement.timesheet_id FROM public.timesheets replacement
          WHERE replacement.timesheet_id=ANY(v_timesheet_ids)
            AND replacement.correction_kind='CHANGED_HOURS_REPLACEMENT' LIMIT 1)
        AND later.timesheet_id<>ALL(v_timesheet_ids)
    ) THEN
      v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','CORRECTION_PAIR_HAS_LATER_GENERATION'));
    END IF;
  END IF;

  v_history := public.timesheet_removal_financial_history_v1(
    v_timesheet_ids,
    ARRAY[v_current.booking_id]::text[],
    v_contract_week_ids
  );

  v_blockers := v_blockers || COALESCE(v_history -> 'blockers', '[]'::jsonb);

  IF jsonb_array_length(v_blockers) > 0 THEN
    v_decision := 'BLOCKED';
  ELSIF COALESCE((v_history ->> 'archive_required')::boolean, false) THEN
    v_decision := 'ARCHIVE_REQUIRED';
  ELSE
    v_decision := 'PERMANENT_DELETE';
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'kind', 'STANDARD_DELETE',
    'decision', v_decision,
    'eligible', v_decision = 'PERMANENT_DELETE',
    'requested_timesheet_id', p_timesheet_id,
    'current_timesheet_id', v_current.timesheet_id,
    'was_stale', v_current.timesheet_id IS DISTINCT FROM p_timesheet_id,
    'booking_id', v_current.booking_id,
    'timesheet_ids', to_jsonb(v_timesheet_ids),
    'contract_week_ids', to_jsonb(v_contract_week_ids),
    'nhsp_shift_ids', to_jsonb(v_nhsp_shift_ids),
    'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
    'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids),
    'blockers', v_blockers,
    'blocked_reasons', v_blockers,
    'retention_reasons', COALESCE(v_history -> 'retention_reasons', '[]'::jsonb),
    'advance', COALESCE(v_history -> 'advance', '{}'::jsonb),
    'current_row_signature', v_current_signature,
    'correction_pair',v_is_correction_pair,
    'source_system',CASE WHEN v_is_correction_pair THEN CASE
      WHEN upper(coalesce(
        v_current.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',
        v_current.adjustment_origin,'')) like 'NHSP%' THEN 'NHSP'
      ELSE 'HEALTHROSTER' END END,
    'correction_id',CASE WHEN v_is_correction_pair THEN v_unit->>'correction_id' END,
    'pair_timesheet_ids',CASE WHEN v_is_correction_pair THEN to_jsonb(v_timesheet_ids) ELSE '[]'::jsonb END,
    'pair_fingerprint',v_pair_fingerprint,
    'members',CASE WHEN v_is_correction_pair THEN v_pair_members ELSE '[]'::jsonb END,
    'pair_net_position',CASE WHEN v_is_correction_pair THEN jsonb_build_object('hours',v_pair_net_hours) ELSE NULL END,
    'resulting_financial_position',CASE WHEN v_is_correction_pair THEN jsonb_build_object('pair_removed_hours',v_pair_net_hours) ELSE NULL END
  );
END;
$function$;

-- timesheet_summary_lightweight_rows_v1(jsonb)
CREATE OR REPLACE FUNCTION public.timesheet_summary_lightweight_rows_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(timesheet_id uuid, contract_week_id uuid, contract_id uuid, candidate_id uuid, candidate_name text, candidate_display_name text, client_id uuid, client_name text, booking_id text, occupant_key_norm text, hospital_norm text, candidate_hint_text jsonb, week_ending_date date, work_date date, sheet_scope text, submission_mode text, submission_mode_snapshot text, basis text, route_type text, route_display text, route_family text, route_subfamily text, underlying_channel_family text, summary_stage text, tools_stage text, processing_status text, processing_status_display text, authorised_at_utc timestamp with time zone, authorised_at_server timestamp with time zone, processed_at_utc timestamp with time zone, is_authorised boolean, total_hours numeric, total_pay_ex_vat numeric, total_charge_ex_vat numeric, margin_ex_vat numeric, net_delta_ex_vat numeric, paid_at_utc timestamp with time zone, pay_icon_code text, pay_status_code text, pay_paid_at_utc timestamp with time zone, invoice_is_paid boolean, invoice_issue_stage text, invoice_segment_stage text, invoice_segments_total integer, invoice_segments_locked integer, invoice_segments_unlocked integer, issue_codes text[], validation_status text, validation_summary text, hr_crosscheck_status text, hr_crosscheck_issues text[], qr_status text, is_qr boolean, is_adjusted boolean, needs_attention boolean, has_rate_issue boolean, has_pay_channel_issue boolean, client_no_timesheet_required boolean, client_autoprocess_hr boolean, client_is_nhsp boolean, has_any_evidence boolean, attached_evidence_count integer, primary_artifact_storage_key text, primary_artifact_display_name text, primary_artifact_preview_mode text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_source_filters jsonb := COALESCE(p_filters, '{}'::jsonb);

  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

  v_id_text text := NULL;
  v_lookup_ids uuid[] := NULL;
  v_has_lookup_filter boolean := FALSE;

  v_timesheet_ids_filter uuid[] := NULL;
  v_contract_week_ids_filter uuid[] := NULL;
  v_has_timesheet_filter boolean := FALSE;
  v_has_contract_week_filter boolean := FALSE;

  v_candidate_id_text text := NULL;
  v_client_id_text text := NULL;
  v_candidate_id_filter uuid := NULL;
  v_client_id_filter uuid := NULL;
  v_has_candidate_filter boolean := FALSE;
  v_has_client_filter boolean := FALSE;

  v_q text := NULL;
  v_tools_stage text := NULL;
  v_route_type text := NULL;
  v_sheet_scope text := NULL;
  v_qr_status text := NULL;
  v_status_code text := NULL;
  v_issues_filter text := NULL;

  v_candidate_paid boolean := NULL;
  v_is_adjusted boolean := NULL;
  v_is_qr boolean := NULL;
  v_hr_issue boolean := NULL;
  v_hr_issue_token text := NULL;

  v_week_ending_from_text text := NULL;
  v_week_ending_to_text text := NULL;
  v_week_ending_from date := NULL;
  v_week_ending_to date := NULL;

  v_order_by text := 'candidate_name';
  v_order_dir text := 'asc';
  v_limit integer := 100;
  v_offset integer := 0;
  v_disable_paging boolean := FALSE;
BEGIN
  v_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '');
  v_has_lookup_filter := (v_id_text IS NOT NULL) OR (v_filters ? 'ids');

  IF v_id_text IS NOT NULL AND v_id_text ~* v_uuid_re THEN
    v_lookup_ids := ARRAY[v_id_text::uuid];
  ELSIF v_filters ? 'ids' AND jsonb_typeof(v_filters->'ids') = 'array' THEN
    SELECT ARRAY_AGG(id_values.id_value)
      INTO v_lookup_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS id_value
      FROM jsonb_array_elements_text(v_filters->'ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS id_values;
  ELSIF v_filters ? 'ids'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(id_values.id_value)
      INTO v_lookup_ids
    FROM (
      SELECT DISTINCT split_values.value::uuid AS id_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'ids', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS id_values;
  END IF;

  IF v_has_lookup_filter AND COALESCE(ARRAY_LENGTH(v_lookup_ids, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_lookup_ids, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object(
           'timesheet_ids', to_jsonb(v_lookup_ids),
           'contract_week_ids', to_jsonb(v_lookup_ids)
         );
  END IF;

  v_has_timesheet_filter :=
    (v_filters ? 'timesheet_id')
    OR (v_filters ? 'timesheetId')
    OR (v_filters ? 'timesheet_ids')
    OR (v_filters ? 'timesheetIds');

  IF v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids_filter
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheet_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheet_ids'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_ids', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids_filter
    FROM (
      SELECT DISTINCT split_values.value::uuid AS uuid_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'timesheet_ids', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids_filter
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheetIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheetIds'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheetIds', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids_filter
    FROM (
      SELECT DISTINCT split_values.value::uuid AS uuid_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'timesheetIds', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheet_id'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', '')), '') IS NOT NULL
        AND (v_filters->>'timesheet_id') ~* v_uuid_re THEN
    v_timesheet_ids_filter := ARRAY[(v_filters->>'timesheet_id')::uuid];
  ELSIF v_filters ? 'timesheetId'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheetId', '')), '') IS NOT NULL
        AND (v_filters->>'timesheetId') ~* v_uuid_re THEN
    v_timesheet_ids_filter := ARRAY[(v_filters->>'timesheetId')::uuid];
  END IF;

  IF v_has_timesheet_filter AND COALESCE(ARRAY_LENGTH(v_timesheet_ids_filter, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_timesheet_ids_filter, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('timesheet_ids', to_jsonb(v_timesheet_ids_filter));
  END IF;

  v_has_contract_week_filter :=
    (v_filters ? 'contract_week_id')
    OR (v_filters ? 'contractWeekId')
    OR (v_filters ? 'contract_week_ids')
    OR (v_filters ? 'contractWeekIds');

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids_filter
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contract_week_ids'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_ids', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids_filter
    FROM (
      SELECT DISTINCT split_values.value::uuid AS uuid_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'contract_week_ids', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids_filter
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contractWeekIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contractWeekIds'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'contractWeekIds', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids_filter
    FROM (
      SELECT DISTINCT split_values.value::uuid AS uuid_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'contractWeekIds', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contract_week_id'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', '')), '') IS NOT NULL
        AND (v_filters->>'contract_week_id') ~* v_uuid_re THEN
    v_contract_week_ids_filter := ARRAY[(v_filters->>'contract_week_id')::uuid];
  ELSIF v_filters ? 'contractWeekId'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'contractWeekId', '')), '') IS NOT NULL
        AND (v_filters->>'contractWeekId') ~* v_uuid_re THEN
    v_contract_week_ids_filter := ARRAY[(v_filters->>'contractWeekId')::uuid];
  END IF;

  IF v_has_contract_week_filter AND COALESCE(ARRAY_LENGTH(v_contract_week_ids_filter, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_contract_week_ids_filter, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('contract_week_ids', to_jsonb(v_contract_week_ids_filter));
  END IF;

  v_candidate_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '');
  v_has_candidate_filter := v_candidate_id_text IS NOT NULL;
  BEGIN
    IF v_candidate_id_text IS NOT NULL AND v_candidate_id_text ~* v_uuid_re THEN
      v_candidate_id_filter := v_candidate_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_candidate_id_filter := NULL;
  END;

  IF v_has_candidate_filter AND v_candidate_id_filter IS NULL THEN
    RETURN;
  END IF;

  v_client_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '');
  v_has_client_filter := v_client_id_text IS NOT NULL;
  BEGIN
    IF v_client_id_text IS NOT NULL AND v_client_id_text ~* v_uuid_re THEN
      v_client_id_filter := v_client_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_client_id_filter := NULL;
  END;

  IF v_has_client_filter AND v_client_id_filter IS NULL THEN
    RETURN;
  END IF;

  v_q := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'query', v_filters->>'name', '')), ''));
  v_tools_stage := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'tools_stage', v_filters->>'toolsStage', '')), ''));
  IF v_tools_stage = 'all' THEN v_tools_stage := NULL; END IF;
  v_route_type := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'route_type', v_filters->>'routeType', '')), ''));
  v_sheet_scope := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  v_qr_status := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'qr_status', v_filters->>'qrStatus', '')), ''));
  v_status_code := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'status_code', v_filters->>'statusCode', '')), ''));
  v_issues_filter := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'issues_filter', v_filters->>'issuesFilter', '')), ''));
  IF v_issues_filter = 'all' THEN v_issues_filter := NULL; END IF;

  IF LOWER(COALESCE(v_filters->>'candidate_paid', v_filters->>'candidatePaid', '')) IN ('true','t','yes','y','1') THEN
    v_candidate_paid := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'candidate_paid', v_filters->>'candidatePaid', '')) IN ('false','f','no','n','0') THEN
    v_candidate_paid := FALSE;
  END IF;

  IF LOWER(COALESCE(v_filters->>'is_adjusted', v_filters->>'isAdjusted', '')) IN ('true','t','yes','y','1') THEN
    v_is_adjusted := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'is_adjusted', v_filters->>'isAdjusted', '')) IN ('false','f','no','n','0') THEN
    v_is_adjusted := FALSE;
  END IF;

  IF LOWER(COALESCE(v_filters->>'is_qr', v_filters->>'isQr', '')) IN ('true','t','yes','y','1') THEN
    v_is_qr := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'is_qr', v_filters->>'isQr', '')) IN ('false','f','no','n','0') THEN
    v_is_qr := FALSE;
  END IF;

  IF LOWER(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')) IN ('true','t','yes','y','1') THEN
    v_hr_issue := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')) IN ('false','f','no','n','0') THEN
    v_hr_issue := FALSE;
  ELSE
    v_hr_issue_token := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')), ''));
    IF v_hr_issue_token = 'ALL' THEN
      v_hr_issue_token := NULL;
    END IF;
  END IF;

  v_week_ending_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '');
  v_week_ending_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '');

  BEGIN
    IF v_week_ending_from_text IS NOT NULL THEN
      v_week_ending_from := v_week_ending_from_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_from := NULL;
  END;

  BEGIN
    IF v_week_ending_to_text IS NOT NULL THEN
      v_week_ending_to := v_week_ending_to_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_to := NULL;
  END;

  v_order_by := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'order_by', v_filters->>'orderBy', 'candidate_name')), ''));
  v_order_dir := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'order_dir', v_filters->>'orderDir', 'asc')), ''));

  IF v_order_dir NOT IN ('asc', 'desc') THEN
    v_order_dir := 'asc';
  END IF;

  v_disable_paging :=
    LOWER(COALESCE(v_filters->>'disable_paging', v_filters->>'disablePaging', v_filters->>'no_paging', v_filters->>'noPaging', '')) IN ('true','t','yes','y','1')
    OR LOWER(COALESCE(v_filters->>'apply_paging', v_filters->>'applyPaging', '')) IN ('false','f','no','n','0')
    OR LOWER(COALESCE(v_filters->>'purpose', '')) IN ('membership','memberships','ids','totals','total','count','counts');

  IF v_disable_paging THEN
    v_limit := NULL;
    v_offset := 0;
  ELSE
    IF COALESCE(v_filters->>'limit', '') ~ '^[0-9]+$' THEN
      v_limit := LEAST(GREATEST((v_filters->>'limit')::integer, 1), 5000);
    ELSIF COALESCE(v_filters->>'page_size', v_filters->>'pageSize', '') ~ '^[0-9]+$' THEN
      v_limit := LEAST(GREATEST(COALESCE(v_filters->>'page_size', v_filters->>'pageSize')::integer, 1), 5000);
    END IF;

    IF COALESCE(v_filters->>'offset', '') ~ '^[0-9]+$' THEN
      v_offset := GREATEST((v_filters->>'offset')::integer, 0);
    ELSIF COALESCE(v_filters->>'page', '') ~ '^[0-9]+$'
          AND COALESCE(v_filters->>'page_size', v_filters->>'pageSize', '') ~ '^[0-9]+$' THEN
      v_offset :=
        GREATEST(((v_filters->>'page')::integer - 1), 0)
        * LEAST(GREATEST(COALESCE(v_filters->>'page_size', v_filters->>'pageSize')::integer, 1), 5000);
    END IF;
  END IF;

  RETURN QUERY
  WITH source_rows AS MATERIALIZED (
    SELECT
      source_row.*
    FROM public.bulk_timesheet_workbench_row_source_v1(v_source_filters) AS source_row
  ),
  source_correction_ids AS MATERIALIZED (
    SELECT DISTINCT timesheet_row.correction_id
    FROM source_rows
    JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = source_rows.timesheet_id
     AND timesheet_row.is_current = TRUE
     AND timesheet_row.archived_at_utc IS NULL
     AND UPPER(COALESCE(timesheet_row.adjustment_origin, '')) = 'IMPORT_CORRECTION'
     AND timesheet_row.correction_id IS NOT NULL
  ),
  correction_pair_members AS MATERIALIZED (
    SELECT
      timesheet_row.timesheet_id,
      timesheet_row.correction_id,
      UPPER(COALESCE(timesheet_row.correction_kind, '')) AS correction_kind
    FROM public.timesheets AS timesheet_row
    JOIN source_correction_ids
      ON source_correction_ids.correction_id = timesheet_row.correction_id
    WHERE timesheet_row.is_current = TRUE
      AND timesheet_row.archived_at_utc IS NULL
      AND UPPER(COALESCE(timesheet_row.adjustment_origin, '')) = 'IMPORT_CORRECTION'
      AND UPPER(COALESCE(timesheet_row.correction_kind, '')) IN (
        'CHANGED_HOURS_REVERSAL',
        'CHANGED_HOURS_REPLACEMENT'
      )
  ),
  correction_pair_placed_members AS MATERIALIZED (
    SELECT DISTINCT invoice_line.timesheet_id
    FROM public.invoice_lines AS invoice_line
    JOIN public.invoices AS invoice_row
      ON invoice_row.id = invoice_line.invoice_id
     AND UPPER(COALESCE(invoice_row.type::text, '')) <> 'CREDIT_NOTE'
    WHERE EXISTS (
      SELECT 1
      FROM correction_pair_members AS pair_member
      WHERE pair_member.timesheet_id = invoice_line.timesheet_id
    )
  ),
  correction_pair_shapes AS MATERIALIZED (
    SELECT
      pair_member.correction_id,
      COUNT(*)::integer AS member_count,
      COUNT(*) FILTER (
        WHERE pair_member.correction_kind = 'CHANGED_HOURS_REVERSAL'
      )::integer AS reversal_count,
      COUNT(*) FILTER (
        WHERE pair_member.correction_kind = 'CHANGED_HOURS_REPLACEMENT'
      )::integer AS replacement_count,
      COUNT(*) FILTER (
        WHERE placed_member.timesheet_id IS NOT NULL
      )::integer AS placed_member_count
    FROM correction_pair_members AS pair_member
    LEFT JOIN correction_pair_placed_members AS placed_member
      ON placed_member.timesheet_id = pair_member.timesheet_id
    GROUP BY pair_member.correction_id
  ),
  correction_pair_issue_timesheets AS MATERIALIZED (
    SELECT pair_member.timesheet_id
    FROM correction_pair_members AS pair_member
    JOIN correction_pair_shapes AS pair_shape
      ON pair_shape.correction_id = pair_member.correction_id
     AND pair_shape.member_count = 2
     AND pair_shape.reversal_count = 1
     AND pair_shape.replacement_count = 1
     AND pair_shape.placed_member_count = 1
    LEFT JOIN correction_pair_placed_members AS placed_member
      ON placed_member.timesheet_id = pair_member.timesheet_id
    WHERE placed_member.timesheet_id IS NULL
  ),
  client_reference_settings AS MATERIALIZED (
    SELECT
      client_setting.client_id,
      COALESCE(BOOL_OR(client_setting.reference_number_required_to_issue_invoice), FALSE)
        AS issue_reference_required
    FROM public.client_settings AS client_setting
    WHERE EXISTS (
      SELECT 1
      FROM source_rows
      WHERE source_rows.client_id = client_setting.client_id
    )
    GROUP BY client_setting.client_id
  ),
  enriched_base AS MATERIALIZED (
    SELECT
      source_rows.timesheet_id,
      source_rows.contract_week_id,
      COALESCE(timesheet_row.contract_id, contract_week_row.contract_id) AS contract_id,

      source_rows.candidate_id,
      source_rows.candidate_name,
      source_rows.candidate_name AS candidate_display_name,
      source_rows.client_id,
      source_rows.client_name,

      source_rows.booking_id,
      source_rows.occupant_key_norm,
      source_rows.hospital_norm,
      source_rows.candidate_hint_text,

      COALESCE(source_rows.contract_week_ending_date, source_rows.week_ending_date) AS week_ending_date,

      CASE
        WHEN source_rows.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN
          COALESCE(timesheet_row.worked_start_iso::date, timesheet_row.scheduled_start_iso::date, source_rows.week_ending_date)
        ELSE NULL::date
      END AS work_date,

      source_rows.sheet_scope::text AS sheet_scope,
      source_rows.submission_mode::text AS submission_mode,
      COALESCE(contract_week_row.submission_mode_snapshot::text, source_rows.submission_mode::text) AS submission_mode_snapshot,
      source_rows.basis::text AS basis,

      source_rows.route_type,
      CASE
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        )
         AND COALESCE(source_rows.client_is_nhsp, FALSE) = TRUE THEN 'NHSP Manual Adjustment'
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        )
         AND COALESCE(source_rows.client_autoprocess_hr, FALSE) = TRUE THEN 'HealthRoster Manual Adjustment'
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        ) THEN 'Manual Adjustment'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'NHSP' THEN 'NHSP'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'HEALTHROSTER' THEN 'HealthRoster'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'HEALTHROSTER_DAILY' THEN 'HealthRoster Daily'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'QR' THEN 'QR'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'NO_TIMESHEET_REQUIRED' THEN 'No timesheet required'
        WHEN COALESCE(source_rows.route_type, '') <> '' THEN INITCAP(REPLACE(source_rows.route_type, '_', ' '))
        ELSE 'Manual'
      END AS route_display,

      CASE
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        ) THEN 'MANUAL'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE '%NHSP%' THEN 'NHSP'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE '%HEALTHROSTER%' THEN 'HEALTHROSTER'
        WHEN COALESCE(source_rows.is_qr, FALSE) THEN 'QR'
        WHEN COALESCE(source_rows.client_no_timesheet_required, FALSE) THEN 'NO_TIMESHEET_REQUIRED'
        ELSE 'MANUAL'
      END AS route_family,

      CASE
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        ) THEN 'MANUAL_ADJUSTMENT'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'HEALTHROSTER_DAILY' THEN 'DAILY'
        WHEN source_rows.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS route_subfamily,

      CASE
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        ) THEN 'MANUAL'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE '%NHSP%' THEN 'IMPORT'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE '%HEALTHROSTER%' THEN 'IMPORT'
        WHEN COALESCE(source_rows.is_qr, FALSE) THEN 'QR'
        ELSE 'MANUAL'
      END AS underlying_channel_family,

      source_rows.summary_stage,
      source_rows.tools_stage,
      source_rows.processing_status::text AS processing_status,
      source_rows.processing_status_display,

      COALESCE(financial_row.authorised_at_utc, source_rows.authorised_at_server) AS authorised_at_utc,
      source_rows.authorised_at_server,
      financial_row.processed_at_utc,

      (COALESCE(financial_row.authorised_at_utc, source_rows.authorised_at_server) IS NOT NULL) AS is_authorised,

      COALESCE(source_rows.total_hours, 0::numeric) AS total_hours,
      COALESCE(source_rows.total_pay_ex_vat, 0::numeric) AS total_pay_ex_vat,
      COALESCE(source_rows.total_charge_ex_vat, 0::numeric) AS total_charge_ex_vat,
      COALESCE(source_rows.margin_ex_vat, 0::numeric) AS margin_ex_vat,
      COALESCE(source_rows.net_delta_ex_vat, COALESCE(source_rows.total_charge_ex_vat, 0::numeric) - COALESCE(source_rows.total_pay_ex_vat, 0::numeric)) AS net_delta_ex_vat,

      source_rows.paid_at_utc,
      source_rows.pay_icon_code,
      source_rows.pay_status_code,
      source_rows.pay_paid_at_utc,

      COALESCE(source_rows.invoice_is_paid, FALSE) AS invoice_is_paid,

      CASE
        WHEN COALESCE(source_rows.invoice_is_paid, FALSE) THEN 'PAID'
        WHEN COALESCE(source_rows.invoice_segments_locked, 0) > 0 THEN 'LOCKED'
        WHEN COALESCE(source_rows.invoice_segments_total, 0) > 0 THEN 'DRAFT'
        ELSE NULL::text
      END AS invoice_issue_stage,

      source_rows.invoice_segment_stage,
      COALESCE(source_rows.invoice_segments_total, 0)::integer AS invoice_segments_total,
      COALESCE(source_rows.invoice_segments_locked, 0)::integer AS invoice_segments_locked,
      COALESCE(source_rows.invoice_segments_unlocked, 0)::integer AS invoice_segments_unlocked,

      COALESCE(
        ARRAY(
          SELECT issue_values.issue_code
          FROM UNNEST(COALESCE(source_rows.issue_codes, ARRAY[]::text[]))
            WITH ORDINALITY AS issue_values(issue_code, issue_ordinality)
          WHERE issue_values.issue_code NOT IN (
            '__PAY_BADGE_ADV__',
            '__PAY_BADGE_OVERPAID__',
            '__PAY_BADGE_PROCESSING__',
            'Authorisation'
          )
          ORDER BY issue_values.issue_ordinality
        ),
        ARRAY[]::text[]
      ) AS base_business_issue_codes,
      COALESCE(
        ARRAY(
          SELECT payment_badges.badge_code
          FROM UNNEST(COALESCE(summary_pay_cache.summary_badge_codes, ARRAY[]::text[]))
            WITH ORDINALITY AS payment_badges(badge_code, badge_ordinality)
          WHERE payment_badges.badge_code IN (
            '__PAY_BADGE_ADV__',
            '__PAY_BADGE_PROCESSING__'
          )
          ORDER BY payment_badges.badge_ordinality
        ),
        ARRAY[]::text[]
      ) AS base_payment_badge_codes,
      (
        COALESCE(summary_pay_cache.paid_to_date_ex_vat, 0::numeric) > 0.01
        AND (
          COALESCE(summary_pay_cache.paid_to_date_ex_vat, 0::numeric)
          + COALESCE(summary_pay_cache.net_delta_ex_vat, 0::numeric)
        ) > 0.01
        AND COALESCE(summary_pay_cache.net_delta_ex_vat, 0::numeric) < -0.01
      ) AS genuine_overpaid,
      (correction_pair_issue_timesheets.timesheet_id IS NOT NULL) AS correction_pair_placement_incomplete,
      CASE
        WHEN source_rows.timesheet_id IS NULL
          OR COALESCE(source_rows.total_hours, 0::numeric) <= 0::numeric
          OR NOT (
            COALESCE(source_rows.require_reference_to_pay, FALSE)
            OR COALESCE(source_rows.require_reference_to_invoice, FALSE)
            OR COALESCE(source_rows.client_ts_reference_required, FALSE)
            OR COALESCE(source_rows.client_pay_reference_required, FALSE)
            OR COALESCE(source_rows.client_invoice_reference_required, FALSE)
            OR COALESCE(client_reference_settings.issue_reference_required, FALSE)
          ) THEN FALSE
        WHEN source_rows.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN
          NULLIF(BTRIM(COALESCE(timesheet_row.reference_number, '')), '') IS NULL
        WHEN financial_row.invoice_breakdown_json IS NOT NULL
          AND jsonb_typeof(financial_row.invoice_breakdown_json) = 'object'
          AND UPPER(COALESCE(financial_row.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
          AND jsonb_typeof(financial_row.invoice_breakdown_json->'segments') = 'array' THEN
          EXISTS (
            SELECT 1
            FROM jsonb_array_elements(financial_row.invoice_breakdown_json->'segments') AS segment_rows(segment_json)
            WHERE NULLIF(BTRIM(COALESCE(segment_rows.segment_json->>'invoice_locked_invoice_id', '')), '') IS NULL
              AND (
                COALESCE(NULLIF(segment_rows.segment_json->>'hours_day', '')::numeric, 0::numeric)
                + COALESCE(NULLIF(segment_rows.segment_json->>'hours_night', '')::numeric, 0::numeric)
                + COALESCE(NULLIF(segment_rows.segment_json->>'hours_sat', '')::numeric, 0::numeric)
                + COALESCE(NULLIF(segment_rows.segment_json->>'hours_sun', '')::numeric, 0::numeric)
                + COALESCE(NULLIF(segment_rows.segment_json->>'hours_bh', '')::numeric, 0::numeric)
              ) > 0::numeric
              AND NULLIF(BTRIM(COALESCE(segment_rows.segment_json->>'ref_num', '')), '') IS NULL
          )
        WHEN source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum THEN
          timesheet_row.actual_schedule_json IS NULL
          OR jsonb_typeof(timesheet_row.actual_schedule_json) <> 'array'
          OR jsonb_array_length(timesheet_row.actual_schedule_json) = 0
          OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements(timesheet_row.actual_schedule_json) AS schedule_rows(schedule_json)
            WHERE NULLIF(BTRIM(COALESCE(schedule_rows.schedule_json->>'start', '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(schedule_rows.schedule_json->>'end', '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(schedule_rows.schedule_json->>'ref_num', '')), '') IS NULL
          )
        ELSE NOT (
          NULLIF(BTRIM(COALESCE(timesheet_row.reference_number, '')), '') IS NOT NULL
          OR EXISTS (
            SELECT 1
            FROM jsonb_each_text(
              CASE
                WHEN jsonb_typeof(timesheet_row.day_references_json) = 'object'
                  THEN timesheet_row.day_references_json
                ELSE '{}'::jsonb
              END
            ) AS day_reference(reference_key, reference_value)
            WHERE LEFT(COALESCE(day_reference.reference_key, ''), 2) <> '__'
              AND NULLIF(BTRIM(COALESCE(day_reference.reference_value, '')), '') IS NOT NULL
          )
          OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(timesheet_row.day_references_json) = 'array'
                  THEN timesheet_row.day_references_json
                WHEN jsonb_typeof(timesheet_row.day_references_json) = 'object'
                  AND jsonb_typeof(timesheet_row.day_references_json->'__freeform_refs') = 'array'
                  THEN timesheet_row.day_references_json->'__freeform_refs'
                WHEN jsonb_typeof(timesheet_row.day_references_json) = 'object'
                  AND jsonb_typeof(timesheet_row.day_references_json->'__freeform') = 'array'
                  THEN timesheet_row.day_references_json->'__freeform'
                WHEN jsonb_typeof(timesheet_row.day_references_json) = 'object'
                  AND jsonb_typeof(timesheet_row.day_references_json->'__freeform_lines') = 'array'
                  THEN timesheet_row.day_references_json->'__freeform_lines'
                ELSE '[]'::jsonb
              END
            ) AS freeform_reference(reference_json)
            WHERE NULLIF(BTRIM(COALESCE(
              CASE
                WHEN jsonb_typeof(freeform_reference.reference_json) = 'string'
                  THEN freeform_reference.reference_json #>> '{}'
                WHEN jsonb_typeof(freeform_reference.reference_json) = 'object'
                  THEN COALESCE(
                    freeform_reference.reference_json->>'reference',
                    freeform_reference.reference_json->>'ref_num',
                    freeform_reference.reference_json->>'value'
                  )
                ELSE NULL::text
              END,
              ''
            )), '') IS NOT NULL
          )
        )
      END AS reference_missing,
      source_rows.validation_status::text AS validation_status,

      CASE
        WHEN source_rows.validation_status IS NULL THEN NULL::text
        ELSE source_rows.validation_status::text
      END AS validation_summary,

      source_rows.hr_crosscheck_status,
      COALESCE(source_rows.hr_crosscheck_issues, ARRAY[]::text[]) AS hr_crosscheck_issues,

      source_rows.qr_status::text AS qr_status,
      timesheet_row.qr_token AS qr_token,
      timesheet_row.qr_generated_at AS qr_generated_at,
      timesheet_row.qr_scanned_at AS qr_scanned_at,
      COALESCE(source_rows.is_qr, FALSE) AS is_qr,
      COALESCE(source_rows.is_adjusted, FALSE) AS is_adjusted,
      COALESCE(source_rows.needs_attention, FALSE) AS needs_attention,
      COALESCE(source_rows.has_rate_issue, FALSE) AS has_rate_issue,
      COALESCE(source_rows.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,

      COALESCE(source_rows.client_no_timesheet_required, FALSE) AS client_no_timesheet_required,
      COALESCE(source_rows.client_autoprocess_hr, FALSE) AS client_autoprocess_hr,
      COALESCE(source_rows.client_is_nhsp, FALSE) AS client_is_nhsp,

      (
        COALESCE(evidence_summary.attached_evidence_count, 0) > 0
        OR NULLIF(timesheet_row.manual_pdf_r2_key, '') IS NOT NULL
        OR NULLIF(timesheet_row.qr_r2_key, '') IS NOT NULL
        OR (source_rows.timesheet_id IS NULL AND NULLIF(contract_week_row.uploaded_pdf_r2_key, '') IS NOT NULL)
      ) AS has_any_evidence,

      COALESCE(evidence_summary.attached_evidence_count, 0)::integer AS attached_evidence_count,

      COALESCE(
        evidence_summary.primary_storage_key,
        NULLIF(timesheet_row.manual_pdf_r2_key, ''),
        NULLIF(timesheet_row.qr_r2_key, ''),
        CASE WHEN source_rows.timesheet_id IS NULL THEN NULLIF(contract_week_row.uploaded_pdf_r2_key, '') END
      ) AS primary_artifact_storage_key,

      COALESCE(
        evidence_summary.primary_display_name,
        CASE WHEN NULLIF(timesheet_row.manual_pdf_r2_key, '') IS NOT NULL THEN 'Manual timesheet PDF' END,
        CASE WHEN NULLIF(timesheet_row.qr_r2_key, '') IS NOT NULL THEN 'QR timesheet' END,
        CASE WHEN source_rows.timesheet_id IS NULL AND NULLIF(contract_week_row.uploaded_pdf_r2_key, '') IS NOT NULL THEN 'Uploaded weekly PDF' END
      ) AS primary_artifact_display_name,

      CASE
        WHEN COALESCE(evidence_summary.primary_storage_key, NULLIF(timesheet_row.manual_pdf_r2_key, ''), NULLIF(timesheet_row.qr_r2_key, ''), CASE WHEN source_rows.timesheet_id IS NULL THEN NULLIF(contract_week_row.uploaded_pdf_r2_key, '') END) IS NOT NULL THEN 'document'
        ELSE NULL::text
      END AS primary_artifact_preview_mode

    FROM source_rows
    LEFT JOIN public.timesheet_summary_pay_state_cache AS summary_pay_cache
      ON summary_pay_cache.timesheet_id = source_rows.timesheet_id
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = source_rows.timesheet_id
     AND timesheet_row.is_current = TRUE
    LEFT JOIN public.contract_weeks AS contract_week_row
      ON contract_week_row.id = source_rows.contract_week_id
    LEFT JOIN public.timesheets_financials AS financial_row
      ON financial_row.timesheet_id = source_rows.timesheet_id
     AND financial_row.is_current = TRUE
    LEFT JOIN correction_pair_issue_timesheets
      ON correction_pair_issue_timesheets.timesheet_id = source_rows.timesheet_id
    LEFT JOIN client_reference_settings
      ON client_reference_settings.client_id = source_rows.client_id
    LEFT JOIN LATERAL (
      SELECT
        COUNT(timesheet_evidence_row.id) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(timesheet_evidence_row.storage_key, '')), '') IS NOT NULL
        )::integer AS attached_evidence_count,
        (ARRAY_AGG(
          timesheet_evidence_row.storage_key
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(timesheet_evidence_row.storage_key, '')), '') IS NOT NULL
        ))[1] AS primary_storage_key,
        (ARRAY_AGG(
          COALESCE(NULLIF(timesheet_evidence_row.display_name, ''), timesheet_evidence_row.kind, 'Evidence')
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(timesheet_evidence_row.storage_key, '')), '') IS NOT NULL
        ))[1] AS primary_display_name
      FROM public.timesheet_evidence AS timesheet_evidence_row
      WHERE timesheet_evidence_row.timesheet_id = source_rows.timesheet_id
    ) AS evidence_summary ON TRUE
  ),
  enriched AS MATERIALIZED (
    SELECT
      enriched_base.*,
      (
        enriched_base.base_business_issue_codes
        || CASE
             WHEN enriched_base.reference_missing THEN ARRAY['Refs missing'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN enriched_base.correction_pair_placement_incomplete THEN ARRAY['Paired needs invoicing'::text]
             ELSE ARRAY[]::text[]
           END
      ) AS business_issue_codes,
      (
        enriched_base.base_business_issue_codes
        || CASE
             WHEN enriched_base.reference_missing THEN ARRAY['Refs missing'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN enriched_base.correction_pair_placement_incomplete THEN ARRAY['Paired needs invoicing'::text]
             ELSE ARRAY[]::text[]
           END
        || enriched_base.base_payment_badge_codes
        || CASE
             WHEN enriched_base.genuine_overpaid THEN ARRAY['__PAY_BADGE_OVERPAID__'::text]
             ELSE ARRAY[]::text[]
           END
      ) AS issue_codes
    FROM enriched_base
  ),
  filtered AS MATERIALIZED (
    SELECT enriched_row.*
    FROM enriched AS enriched_row
    WHERE
      (
        NOT v_has_lookup_filter
        OR (
          v_lookup_ids IS NOT NULL
          AND (
            enriched_row.timesheet_id = ANY(v_lookup_ids)
            OR enriched_row.contract_week_id = ANY(v_lookup_ids)
          )
        )
      )
      AND (
        NOT v_has_timesheet_filter
        OR (
          v_timesheet_ids_filter IS NOT NULL
          AND enriched_row.timesheet_id = ANY(v_timesheet_ids_filter)
        )
      )
      AND (
        NOT v_has_contract_week_filter
        OR (
          v_contract_week_ids_filter IS NOT NULL
          AND enriched_row.contract_week_id = ANY(v_contract_week_ids_filter)
        )
      )
      AND (
        NOT v_has_candidate_filter
        OR enriched_row.candidate_id = v_candidate_id_filter
      )
      AND (
        NOT v_has_client_filter
        OR enriched_row.client_id = v_client_id_filter
      )
      AND (
        v_week_ending_from IS NULL
        OR enriched_row.week_ending_date >= v_week_ending_from
      )
      AND (
        v_week_ending_to IS NULL
        OR enriched_row.week_ending_date <= v_week_ending_to
      )
      AND (
        v_q IS NULL
        OR LOWER(COALESCE(enriched_row.candidate_name, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.candidate_display_name, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.client_name, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.booking_id, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.occupant_key_norm, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.hospital_norm, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.route_display, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.processing_status_display, '')) LIKE '%' || v_q || '%'
      )
      AND (
        (v_tools_stage IS NULL AND LOWER(COALESCE(enriched_row.tools_stage, '')) <> 'archived')
        OR (v_tools_stage = 'archived' AND LOWER(COALESCE(enriched_row.tools_stage, '')) = 'archived')
        OR (
          v_tools_stage IS NOT NULL
          AND v_tools_stage <> 'archived'
          AND LOWER(COALESCE(enriched_row.tools_stage, '')) = v_tools_stage
          AND LOWER(COALESCE(enriched_row.tools_stage, '')) <> 'archived'
        )
      )
      AND (
        v_route_type IS NULL
        OR (
          v_route_type = 'electronic'
          AND UPPER(COALESCE(enriched_row.route_type, '')) IN ('DAILY_ELECTRONIC', 'WEEKLY_ELECTRONIC')
        )
        OR (
          v_route_type = 'manual'
          AND (
            UPPER(COALESCE(enriched_row.route_type, '')) IN ('DAILY_MANUAL', 'WEEKLY_MANUAL')
            OR UPPER(COALESCE(enriched_row.route_family, '')) = 'MANUAL'
          )
        )
        OR (
          v_route_type = 'nhsp'
          AND (
            UPPER(COALESCE(enriched_row.route_type, '')) IN ('WEEKLY_NHSP', 'NHSP')
            OR UPPER(COALESCE(enriched_row.route_family, '')) = 'NHSP'
          )
        )
        OR (
          v_route_type = 'healthroster'
          AND (
            UPPER(COALESCE(enriched_row.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
            OR UPPER(COALESCE(enriched_row.route_family, '')) = 'HEALTHROSTER'
          )
        )
        OR (
          v_route_type = 'qr'
          AND COALESCE(enriched_row.is_qr, FALSE) = TRUE
        )
        OR (
          v_route_type IN ('no_timesheet_required', 'no-timesheet-required')
          AND (
            UPPER(COALESCE(enriched_row.route_type, '')) = 'NO_TIMESHEET_REQUIRED'
            OR UPPER(COALESCE(enriched_row.route_family, '')) = 'NO_TIMESHEET_REQUIRED'
            OR COALESCE(enriched_row.client_no_timesheet_required, FALSE) = TRUE
          )
        )
        OR LOWER(COALESCE(enriched_row.route_type, '')) = v_route_type
        OR LOWER(COALESCE(enriched_row.route_family, '')) = v_route_type
      )
      AND (
        v_sheet_scope IS NULL
        OR LOWER(COALESCE(enriched_row.sheet_scope, '')) = v_sheet_scope
      )
      AND (
        v_qr_status IS NULL
        OR LOWER(COALESCE(enriched_row.qr_status, '')) = v_qr_status
      )
      AND (
        v_status_code IS NULL
        OR (
          v_status_code = 'no_match_id'
          AND (enriched_row.candidate_id IS NULL OR enriched_row.client_id IS NULL)
        )
        OR (
          v_status_code = 'rate_missing'
          AND enriched_row.has_rate_issue
        )
        OR (
          v_status_code IN ('pay_chan_miss', 'pay_channel_missing')
          AND enriched_row.has_pay_channel_issue
        )
        OR (
          v_status_code = 'ready_for_hr'
          AND UPPER(COALESCE(enriched_row.processing_status, '')) = 'READY_FOR_HR'
        )
        OR (
          v_status_code = 'ready_for_inv'
          AND UPPER(COALESCE(enriched_row.processing_status, '')) = 'READY_FOR_INVOICE'
        )
        OR LOWER(COALESCE(enriched_row.processing_status, '')) = v_status_code
        OR LOWER(COALESCE(enriched_row.summary_stage, '')) = v_status_code
        OR LOWER(COALESCE(enriched_row.tools_stage, '')) = v_status_code
      )
      AND (
        v_candidate_paid IS NULL
        OR (
          (
            UPPER(COALESCE(enriched_row.pay_status_code, '')) IN ('PAID','PARTIALLY_PAID','OVERPAID')
            OR enriched_row.pay_paid_at_utc IS NOT NULL
            OR enriched_row.paid_at_utc IS NOT NULL
          ) = v_candidate_paid
        )
      )
      AND (
        v_is_adjusted IS NULL
        OR enriched_row.is_adjusted = v_is_adjusted
      )
      AND (
        v_is_qr IS NULL
        OR enriched_row.is_qr = v_is_qr
      )
      AND (
        v_hr_issue IS NULL
        OR (
          (
            COALESCE(ARRAY_LENGTH(enriched_row.hr_crosscheck_issues, 1), 0) > 0
            OR (
              enriched_row.hr_crosscheck_status IS NOT NULL
              AND UPPER(enriched_row.hr_crosscheck_status) NOT IN ('OK', 'MATCHED', 'MATCH', 'VALID', 'PASSED', 'CLEAR')
            )
          ) = v_hr_issue
        )
      )
      AND (
        v_hr_issue_token IS NULL
        OR EXISTS (
          SELECT 1
          FROM UNNEST(COALESCE(enriched_row.hr_crosscheck_issues, ARRAY[]::text[])) AS hr_issue_value(issue_code)
          WHERE UPPER(COALESCE(hr_issue_value.issue_code, '')) = v_hr_issue_token
        )
      )
      AND (
        v_issues_filter IS NULL
        OR (
          v_issues_filter = 'any'
          AND (
            COALESCE(ARRAY_LENGTH(enriched_row.business_issue_codes, 1), 0) > 0
            OR enriched_row.genuine_overpaid
          )
        )
        OR (
          v_issues_filter IN ('none', 'clear')
          AND COALESCE(ARRAY_LENGTH(enriched_row.business_issue_codes, 1), 0) = 0
          AND NOT enriched_row.genuine_overpaid
        )
        OR (
          v_issues_filter IN ('no_match_id', 'identity_missing')
          AND (enriched_row.candidate_id IS NULL OR enriched_row.client_id IS NULL)
        )
        OR (
          v_issues_filter IN ('rate', 'rates', 'rate_missing')
          AND 'Rate' = ANY(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[]))
        )
        OR (
          v_issues_filter IN ('pay', 'pay_channel', 'pay-channel', 'pay_chan_miss', 'pay_channel_missing')
          AND 'Pay channel' = ANY(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[]))
        )
        OR (
          v_issues_filter = 'on_hold'
          AND 'On hold' = ANY(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[]))
        )
        OR (
          v_issues_filter IN ('hr_hours_mismatch', 'hours_mismatch_hr')
          AND 'Hours mismatch HR' = ANY(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[]))
        )
        OR (
          v_issues_filter = 'hr_hours_missing'
          AND 'HR hours missing' = ANY(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[]))
        )
        OR (
          v_issues_filter = 'duplicate_contracts'
          AND 'Duplicate contracts' = ANY(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[]))
        )
        OR (
          v_issues_filter = 'expenses_evidence'
          AND 'Expenses evidence' = ANY(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[]))
        )
        OR (
          v_issues_filter = 'mileage_evidence'
          AND 'Mileage evidence' = ANY(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[]))
        )
        OR (
          v_issues_filter IN ('refs_missing', 'reference_missing')
          AND 'Refs missing' = ANY(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[]))
        )
        OR (
          v_issues_filter IN ('awaiting_validation', 'awaiting_hr_validation')
          AND 'Awaiting validation' = ANY(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[]))
        )
        OR (
          v_issues_filter IN ('validation_failed', 'validation')
          AND 'Validation failed' = ANY(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[]))
        )
        OR (
          v_issues_filter IN ('qr_awaiting_signature', 'qr_issued_awaiting_signature')
          AND 'Awaiting signed QR timesheet' = ANY(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[]))
        )
        OR (
          v_issues_filter = 'paired_needs_invoicing'
          AND enriched_row.correction_pair_placement_incomplete
        )
        OR (
          v_issues_filter = 'overpaid'
          AND enriched_row.genuine_overpaid
        )
      )
  )
  SELECT
    filtered_row.timesheet_id,
    filtered_row.contract_week_id,
    filtered_row.contract_id,
    filtered_row.candidate_id,
    filtered_row.candidate_name,
    filtered_row.candidate_display_name,
    filtered_row.client_id,
    filtered_row.client_name,
    filtered_row.booking_id,
    filtered_row.occupant_key_norm,
    filtered_row.hospital_norm,
    filtered_row.candidate_hint_text,
    filtered_row.week_ending_date,
    filtered_row.work_date,
    filtered_row.sheet_scope,
    filtered_row.submission_mode,
    filtered_row.submission_mode_snapshot,
    filtered_row.basis,
    filtered_row.route_type,
    filtered_row.route_display,
    filtered_row.route_family,
    filtered_row.route_subfamily,
    filtered_row.underlying_channel_family,
    filtered_row.summary_stage,
    filtered_row.tools_stage,
    filtered_row.processing_status,
    filtered_row.processing_status_display,
    filtered_row.authorised_at_utc,
    filtered_row.authorised_at_server,
    filtered_row.processed_at_utc,
    filtered_row.is_authorised,
    filtered_row.total_hours,
    filtered_row.total_pay_ex_vat,
    filtered_row.total_charge_ex_vat,
    filtered_row.margin_ex_vat,
    filtered_row.net_delta_ex_vat,
    filtered_row.paid_at_utc,
    filtered_row.pay_icon_code,
    filtered_row.pay_status_code,
    filtered_row.pay_paid_at_utc,
    filtered_row.invoice_is_paid,
    filtered_row.invoice_issue_stage,
    filtered_row.invoice_segment_stage,
    filtered_row.invoice_segments_total,
    filtered_row.invoice_segments_locked,
    filtered_row.invoice_segments_unlocked,
    filtered_row.issue_codes,
    filtered_row.validation_status,
    filtered_row.validation_summary,
    filtered_row.hr_crosscheck_status,
    filtered_row.hr_crosscheck_issues,
    filtered_row.qr_status,
    filtered_row.is_qr,
    filtered_row.is_adjusted,
    filtered_row.needs_attention,
    filtered_row.has_rate_issue,
    filtered_row.has_pay_channel_issue,
    filtered_row.client_no_timesheet_required,
    filtered_row.client_autoprocess_hr,
    filtered_row.client_is_nhsp,
    filtered_row.has_any_evidence,
    filtered_row.attached_evidence_count,
    filtered_row.primary_artifact_storage_key,
    filtered_row.primary_artifact_display_name,
    filtered_row.primary_artifact_preview_mode
  FROM filtered AS filtered_row
  ORDER BY
    CASE WHEN v_order_by IN ('candidate_name', 'candidate') AND v_order_dir = 'asc' THEN filtered_row.candidate_name END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('candidate_name', 'candidate') AND v_order_dir = 'desc' THEN filtered_row.candidate_name END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('client_name', 'client') AND v_order_dir = 'asc' THEN filtered_row.client_name END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('client_name', 'client') AND v_order_dir = 'desc' THEN filtered_row.client_name END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('week_ending_date', 'week_ending', 'date') AND v_order_dir = 'asc' THEN filtered_row.week_ending_date END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('week_ending_date', 'week_ending', 'date') AND v_order_dir = 'desc' THEN filtered_row.week_ending_date END DESC NULLS LAST,

    CASE WHEN v_order_by = 'work_date' AND v_order_dir = 'asc' THEN filtered_row.work_date END ASC NULLS LAST,
    CASE WHEN v_order_by = 'work_date' AND v_order_dir = 'desc' THEN filtered_row.work_date END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('processing_status', 'status') AND v_order_dir = 'asc' THEN filtered_row.processing_status END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('processing_status', 'status') AND v_order_dir = 'desc' THEN filtered_row.processing_status END DESC NULLS LAST,

    CASE WHEN v_order_by = 'tools_stage' AND v_order_dir = 'asc' THEN filtered_row.tools_stage END ASC NULLS LAST,
    CASE WHEN v_order_by = 'tools_stage' AND v_order_dir = 'desc' THEN filtered_row.tools_stage END DESC NULLS LAST,

    CASE WHEN v_order_by = 'route_type' AND v_order_dir = 'asc' THEN filtered_row.route_type END ASC NULLS LAST,
    CASE WHEN v_order_by = 'route_type' AND v_order_dir = 'desc' THEN filtered_row.route_type END DESC NULLS LAST,

    CASE WHEN v_order_by = 'sheet_scope' AND v_order_dir = 'asc' THEN filtered_row.sheet_scope END ASC NULLS LAST,
    CASE WHEN v_order_by = 'sheet_scope' AND v_order_dir = 'desc' THEN filtered_row.sheet_scope END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('total_pay_ex_vat', 'pay') AND v_order_dir = 'asc' THEN filtered_row.total_pay_ex_vat END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('total_pay_ex_vat', 'pay') AND v_order_dir = 'desc' THEN filtered_row.total_pay_ex_vat END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('total_charge_ex_vat', 'charge') AND v_order_dir = 'asc' THEN filtered_row.total_charge_ex_vat END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('total_charge_ex_vat', 'charge') AND v_order_dir = 'desc' THEN filtered_row.total_charge_ex_vat END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('margin_ex_vat', 'margin') AND v_order_dir = 'asc' THEN filtered_row.margin_ex_vat END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('margin_ex_vat', 'margin') AND v_order_dir = 'desc' THEN filtered_row.margin_ex_vat END DESC NULLS LAST,

    filtered_row.candidate_name ASC NULLS LAST,
    filtered_row.week_ending_date DESC NULLS LAST,
    filtered_row.work_date DESC NULLS LAST,
    filtered_row.timesheet_id NULLS LAST,
    filtered_row.contract_week_id NULLS LAST
  LIMIT v_limit
  OFFSET v_offset;
END;
$function$;

-- timesheet_unauthorise_atomic(uuid,uuid,uuid,timestamp with time zone,text)
CREATE OR REPLACE FUNCTION public.timesheet_unauthorise_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_contract_week public.contract_weeks%ROWTYPE;
  v_prev_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := 'PENDING_AUTH'::public.ts_fin_processing_status_enum;
  v_has_segment_invoice_lock boolean := false;
  v_has_invoice_membership boolean := false;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_diag_started_at timestamptz := clock_timestamp();
  v_temp_log_enabled boolean := false;
  v_advance_state_refresh_json jsonb := '{}'::jsonb;
  v_has_uncleared_advance_override boolean := false;
BEGIN

  if coalesce((public._ctms_import_correction_classify_v1(p_timesheet_id)
       ->> 'is_import_authoritative_correction')::boolean, false) then
    return jsonb_build_object(
      'ok', false,
      'error_code', 'IMPORT_CORRECTION_UNIT_REQUIRES_BULK_LIFECYCLE',
      'timesheet_id', p_timesheet_id,
      'required_rpc', case when 'UNAUTHORISE' = 'AUTHORISE'
        then 'timesheet_authorise_bulk_atomic' else 'timesheet_unauthorise_bulk_atomic' end
    );
  end if;
  PERFORM set_config('lock_timeout', '300ms', true);

  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_temp_log_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      v_temp_log_enabled := false;
    WHEN OTHERS THEN
      v_temp_log_enabled := false;
  END;

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'entry',
      'timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'actor_user_id_present', p_actor_user_id IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_id')::text;
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
  END IF;

  SELECT ts.*
    INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF v_requested_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_requested_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'requested_timesheet_resolved',
      'timesheet_id', v_requested_ts.timesheet_id,
      'is_current', COALESCE(v_requested_ts.is_current, false),
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF COALESCE(v_requested_ts.is_current, false) THEN
    v_current_ts := v_requested_ts;
  ELSE
    SELECT ts.*
      INTO v_current_ts
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_requested_ts.booking_id
      AND ts.is_current = true
    ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;
    IF v_current_ts.timesheet_id IS NULL THEN
      v_current_ts := v_requested_ts;
    END IF;
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RAISE EXCEPTION USING MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH', DETAIL = jsonb_build_object('expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_ts.correction_id IS NOT NULL
     OR v_current_ts.correction_kind IN ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT') THEN
    RAISE EXCEPTION USING
      MESSAGE = 'CORRECTION_PAIR_BULK_REQUIRED',
      ERRCODE = 'P0001',
      DETAIL = jsonb_build_object(
        'code','CORRECTION_PAIR_BULK_REQUIRED',
        'timesheet_id',v_current_ts.timesheet_id,
        'correction_id',v_current_ts.correction_id,
        'correction_kind',v_current_ts.correction_kind,
        'required_function','timesheet_unauthorise_bulk_atomic',
        'pair_timesheet_ids',coalesce((
          select jsonb_agg(tpair.timesheet_id order by tpair.correction_kind,tpair.timesheet_id)
          from public.timesheets tpair
          where tpair.correction_id=v_current_ts.correction_id
            and tpair.is_current=true
            and tpair.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
        ),'[]'::jsonb)
      )::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'current_timesheet_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'NO_TSFIN')::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'tsfin_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_current_tsfin.processing_status::text,
      'old_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  SELECT cw.*
    INTO v_contract_week
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current_ts.timesheet_id
  ORDER BY cw.updated_at DESC NULLS LAST, cw.id DESC
  LIMIT 1
  FOR UPDATE;

  v_prev_status := v_current_tsfin.processing_status;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json -> 'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  SELECT EXISTS (
    SELECT 1
    FROM public.invoice_lines AS invoice_line
    WHERE invoice_line.timesheet_id = v_current_ts.timesheet_id
  ) INTO v_has_invoice_membership;

  IF v_current_ts.archived_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL
     OR COALESCE(v_has_segment_invoice_lock, false)
     OR COALESCE(v_has_invoice_membership, false) THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NULL AND v_current_tsfin.authorised_at_utc IS NULL AND COALESCE(v_contract_week.status = 'AUTHORISED'::public.contract_week_status_enum, false) = false THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_UNAUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');
  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    IF COALESCE(v_temp_log_enabled, false) THEN
      PERFORM public._temp_diag_log(
        'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
        'TEMP_TIMESHEET_LIFECYCLE',
        v_current_ts.timesheet_id::text,
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
          'function_name', 'timesheet_unauthorise_atomic',
          'stage', 'row_signature_mismatch_before_unauthorise',
          'action', 'unauthorise',
          'route_family', 'timesheet_lifecycle',
          'timesheet_id', p_timesheet_id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
          'expected_row_signature', v_expected_row_signature,
          'current_row_signature', v_current_row_signature,
          'current_signature_payload', v_before_signature_json,
          'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
        ))
      );
    END IF;
    RAISE EXCEPTION USING MESSAGE = 'ROW_SIGNATURE_MISMATCH', DETAIL = jsonb_build_object('expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature, 'current_timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'before_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'tsfin_id', v_current_tsfin.id,
      'current_row_signature_present', v_current_row_signature IS NOT NULL,
      'expected_row_signature_present', v_expected_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM set_config('cloudtms.lifecycle_mutation_context', 'timesheet_unauthorise', true);
  PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', v_current_ts.timesheet_id::text, true);
  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'on', true);

  UPDATE public.timesheets AS ts
     SET authorised_at_server = NULL,
         revoked_at = v_now,
         revoked_reason = 'TIMESHEET_UNAUTHORISE',
         revoked_by = p_actor_user_id::text,
         updated_at = v_now
   WHERE ts.timesheet_id = v_current_ts.timesheet_id
     AND ts.is_current = true
   RETURNING * INTO v_current_ts;

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'timesheets_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'new_authorised_present', v_current_ts.authorised_at_server IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  UPDATE public.timesheets_financials AS tf
     SET processing_status = v_new_status,
         authorised_by_user_id = NULL,
         authorised_at_utc = NULL,
         updated_at = v_now
   WHERE tf.id = v_current_tsfin.id
     AND tf.is_current = true
   RETURNING * INTO v_current_tsfin;

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'tsfin_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_prev_status::text,
      'new_processing_status', v_current_tsfin.processing_status::text,
      'new_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum THEN
    IF v_contract_week.id IS NULL THEN
      SELECT cw.*
        INTO v_contract_week
      FROM public.contract_weeks AS cw
      JOIN public.timesheets AS tw ON tw.timesheet_id = cw.timesheet_id
      WHERE tw.booking_id = v_current_ts.booking_id
      ORDER BY cw.updated_at DESC NULLS LAST, cw.id DESC
      LIMIT 1
      FOR UPDATE OF cw;
    END IF;
    IF v_contract_week.id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET')::text;
    END IF;
    UPDATE public.contract_weeks AS cw
       SET timesheet_id = v_current_ts.timesheet_id,
           status = 'SUBMITTED'::public.contract_week_status_enum,
           updated_at = v_now
     WHERE cw.id = v_contract_week.id
     RETURNING * INTO v_contract_week;
  END IF;

  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'off', true);

  SELECT EXISTS (
    SELECT 1
    FROM public._pay_timesheet_rotation_scope(ARRAY[v_current_ts.timesheet_id]) AS rs
    JOIN public.timesheet_payment_overrides AS payment_override
      ON payment_override.timesheet_id = rs.family_timesheet_id
    WHERE payment_override.cleared_at_utc IS NULL
      AND UPPER(BTRIM(COALESCE(payment_override.override_type, ''))) = 'ADVANCE_THIS_PAYMENT'
  ) INTO v_has_uncleared_advance_override;

  IF COALESCE(v_has_uncleared_advance_override, false) THEN
    v_advance_state_refresh_json := public.pay_timesheet_summary_advance_state_refresh(
      p_timesheet_id => v_current_ts.timesheet_id,
      p_actor_user_id => p_actor_user_id
    );

    PERFORM public._temp_diag_log(
      'TEMP_UNAUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_unauthorise_atomic',
        'stage', 'advance_state_refresh_done',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'refresh_result', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  ELSE
    PERFORM public._temp_diag_log(
      'TEMP_UNAUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_unauthorise_atomic',
        'stage', 'advance_state_refresh_skipped',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'reason', 'NO_ACTIVE_ADVANCE_THIS_PAYMENT_OVERRIDE',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'after_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'row_signature_present', v_after_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._audit_insert(
    'timesheet',
    v_current_ts.timesheet_id::text,
    'TIMESHEET_UNAUTHORISED',
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'previous_processing_status', v_prev_status::text, 'previous_row_signature', v_current_row_signature),
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'new_processing_status', v_new_status::text, 'new_row_signature', v_after_row_signature),
    'TIMESHEET_UNAUTHORISE',
    p_actor_user_id
  );

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'audit_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'return',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'new_processing_status', v_new_status::text,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', 'timesheet_unauthorise',
    'timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'requested_timesheet_id', v_requested_ts.timesheet_id,
    'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
    'booking_id', v_current_ts.booking_id,
    'candidate_id', v_current_tsfin.candidate_id,
    'authorised_at_server', v_current_ts.authorised_at_server,
    'revoked_at', v_current_ts.revoked_at,
    'authorised_at_utc', v_current_tsfin.authorised_at_utc,
    'advance_state_refresh', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
    'current_version', v_current_ts.version,
    'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id,
    'previous_processing_status', v_prev_status::text,
    'processing_status', v_new_status::text,
    'new_processing_status', v_new_status::text,
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'affected_rows', jsonb_build_array(jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'booking_id', v_current_ts.booking_id, 'row_key', 'timesheet:' || v_current_ts.timesheet_id::text)),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', CASE WHEN COALESCE(v_has_uncleared_advance_override, false) THEN jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks', 'timesheet_summary_pay_state_cache') ELSE jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks') END, 'timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'exception',
      'timesheet_id', p_timesheet_id,
      'sqlstate', v_error_state,
      'error_message', v_error_message,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );
  IF v_error_state = '55P03' THEN
    RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;
  RAISE;
END;
$function$;

