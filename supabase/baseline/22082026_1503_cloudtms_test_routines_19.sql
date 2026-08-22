-- Immutable CloudTMS TEST function snapshot, page 19.
-- Generated from pg_get_functiondef; definitions only, with function body checks deferred for forward references.
-- Do not edit an applied baseline page. Add or replace routine authority in supabase/repeatable.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- timesheet_unauthorise_bulk_atomic(jsonb,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.timesheet_unauthorise_bulk_atomic(p_items jsonb DEFAULT '[]'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
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
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;
  IF p_items IS NOT NULL AND jsonb_typeof(p_items) NOT IN ('array', 'object') THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'ITEMS_JSON_MUST_BE_ARRAY_OR_OBJECT', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
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
      AND c.txid=txid_current() AND c.actor_user_id=p_actor_user_id AND c.action='UNAUTHORISE';
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
    v_items_array := public._ctms_expand_lifecycle_items_v1(v_items_array, 'UNAUTHORISE', p_actor_user_id, 100);
  END IF;
  v_requested_count := jsonb_array_length(v_items_array);
  IF v_requested_count > 100 THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'TOO_MANY_ITEMS', 'requested_count', v_requested_count, 'success_count', 0, 'failure_count', v_requested_count, 'results', '[]'::jsonb);
  END IF;

  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_items;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_state;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_work;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_ts;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_tf;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_cw;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_results;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_items ON COMMIT DROP AS
  SELECT
    input_values.ordinality::integer AS ordinal,
    CASE WHEN jsonb_typeof(input_values.item_json) = 'object' THEN input_values.item_json ELSE jsonb_build_object('value', input_values.item_json) END AS item_json,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'row_key', input_values.item_json ->> 'rowKey', '')), '') AS row_key,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'timesheet_id', input_values.item_json ->> 'timesheetId', input_values.item_json ->> 'current_timesheet_id', input_values.item_json ->> 'currentTimesheetId', input_values.item_json ->> 'requested_timesheet_id', input_values.item_json ->> 'requestedTimesheetId', '')), '') AS timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'expected_timesheet_id', input_values.item_json ->> 'expectedTimesheetId', input_values.item_json ->> 'expected_current_timesheet_id', input_values.item_json ->> 'expectedCurrentTimesheetId', '')), '') AS expected_timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'backend_row_signature', input_values.item_json ->> 'row_signature', input_values.item_json ->> 'rowSignature', input_values.item_json ->> 'expected_row_signature', input_values.item_json ->> 'expectedRowSignature', '')), '') AS expected_row_signature
  FROM jsonb_array_elements(v_items_array) WITH ORDINALITY AS input_values(item_json, ordinality);

  CREATE TEMP TABLE timesheet_unauthorise_bulk_state ON COMMIT DROP AS
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
    cur_ts.sheet_scope AS current_sheet_scope,
    tf.id AS tsfin_id,
    tf.processing_status AS tsfin_processing_status,
    tf.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
    tf.paid_at_utc AS tsfin_paid_at_utc,
    tf.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
    tf.authorised_at_utc AS tsfin_authorised_at_utc,
    cw.id AS contract_week_id,
    sig.signature_json AS signature_json,
    sig.signature_text AS current_row_signature,
    COALESCE(segment_state.has_segment_invoice_lock, false) AS has_segment_invoice_lock
  FROM pg_temp.timesheet_unauthorise_bulk_items AS item_rows
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
    SELECT cw_sel.*
    FROM public.contract_weeks AS cw_sel
    WHERE cw_sel.timesheet_id = cur_ts.timesheet_id
       OR EXISTS (SELECT 1 FROM public.timesheets AS cw_ts WHERE cw_ts.timesheet_id = cw_sel.timesheet_id AND cw_ts.booking_id = cur_ts.booking_id)
    ORDER BY CASE WHEN cw_sel.timesheet_id = cur_ts.timesheet_id THEN 0 ELSE 1 END, cw_sel.updated_at DESC NULLS LAST, cw_sel.id DESC
    LIMIT 1
    FOR UPDATE OF cw_sel
  ) AS cw ON cur_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
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
  ) AS segment_state ON true;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_work ON COMMIT DROP AS
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
      WHEN state_rows.current_archived_at_utc IS NOT NULL THEN 'TIMESHEET_ARCHIVED'
      WHEN state_rows.tsfin_locked_by_invoice_id IS NOT NULL OR state_rows.has_segment_invoice_lock THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_authorised_at_server IS NULL AND state_rows.tsfin_authorised_at_utc IS NULL THEN 'ALREADY_UNAUTHORISED'
      ELSE NULL::text
    END AS failure_code,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum AS new_processing_status
  FROM pg_temp.timesheet_unauthorise_bulk_state AS state_rows;

  -- Correction pairs are one lifecycle unit.  Treat an already-unauthorised
  -- sibling as idempotent only inside a proven pair group and propagate any
  -- real blocker to both members before mutation.
  UPDATE pg_temp.timesheet_unauthorise_bulk_work work_rows
     SET failure_code=NULL
   WHERE NULLIF(work_rows.item_json->>'lifecycle_group_id','') IS NOT NULL
     AND work_rows.failure_code='ALREADY_UNAUTHORISED';

  UPDATE pg_temp.timesheet_unauthorise_bulk_work work_rows
     SET failure_code='CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED'
   WHERE NULLIF(work_rows.item_json->>'lifecycle_group_id','') IS NOT NULL
     AND EXISTS (
       SELECT 1 FROM pg_temp.timesheet_unauthorise_bulk_work blocked
       WHERE blocked.item_json->>'lifecycle_group_id'=work_rows.item_json->>'lifecycle_group_id'
         AND blocked.failure_code IS NOT NULL
     );

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_ts ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets AS ts_upd
       SET authorised_at_server = NULL,
           updated_at = v_now
      FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
     WHERE work_rows.failure_code IS NULL
       AND ts_upd.timesheet_id = work_rows.current_timesheet_id
       AND ts_upd.is_current = true
     RETURNING ts_upd.timesheet_id, ts_upd.version, ts_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_tf ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets_financials AS tf_upd
       SET processing_status = work_rows.new_processing_status,
           authorised_by_user_id = NULL,
           authorised_at_utc = NULL,
           updated_at = v_now
      FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_unauthorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND tf_upd.id = work_rows.tsfin_id
       AND tf_upd.is_current = true
     RETURNING tf_upd.timesheet_id, tf_upd.processing_status, tf_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_cw ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.contract_weeks AS cw_upd
       SET timesheet_id = work_rows.current_timesheet_id,
           status = 'SUBMITTED'::public.contract_week_status_enum,
           updated_at = v_now
      FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND cw_upd.id = work_rows.contract_week_id
     RETURNING cw_upd.id, cw_upd.timesheet_id, cw_upd.status, cw_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  IF EXISTS (
    SELECT 1
    FROM pg_temp.timesheet_unauthorise_bulk_work work_rows
    JOIN public.timesheets current_pair
      ON current_pair.timesheet_id=work_rows.current_timesheet_id
    JOIN public.timesheets_financials current_tf
      ON current_tf.timesheet_id=current_pair.timesheet_id AND current_tf.is_current=true
    WHERE NULLIF(work_rows.item_json->>'lifecycle_group_id','') IS NOT NULL
      AND work_rows.failure_code IS NULL
    GROUP BY work_rows.item_json->>'lifecycle_group_id',
             (work_rows.item_json->>'lifecycle_group_size')::integer
    HAVING count(*)<>(work_rows.item_json->>'lifecycle_group_size')::integer
       OR count(*) FILTER (WHERE current_pair.authorised_at_server IS NULL
                            AND current_tf.authorised_at_utc IS NULL)<>count(*)
  ) THEN
    RAISE EXCEPTION 'CORRECTION_PAIR_LIFECYCLE_POSTCONDITION_FAILED' USING ERRCODE='P0001';
  END IF;

  PERFORM public._audit_insert(
    'timesheet_batch',
    'bulk_unauthorise:' || v_now::text,
    'TIMESHEET_BULK_UNAUTHORISED',
    jsonb_build_object('requested_count', v_requested_count, 'actor_user_id', p_actor_user_id),
    jsonb_build_object(
      'succeeded_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
      'failed_items', COALESCE((SELECT jsonb_agg(jsonb_build_object('item_index', work_rows.ordinal, 'timesheet_id', work_rows.requested_timesheet_id, 'error_code', work_rows.failure_code) ORDER BY work_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows WHERE work_rows.failure_code IS NOT NULL), '[]'::jsonb)
    ),
    'BULK_UNAUTHORISE',
    p_actor_user_id
  );

  CREATE TEMP TABLE timesheet_unauthorise_bulk_results ON COMMIT DROP AS
  SELECT
    work_rows.ordinal,
    (work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL) AS success,
    jsonb_build_object(
      'item_index', work_rows.ordinal,
      'success', work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL,
      'action', 'UNAUTHORISE',
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
  FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
  LEFT JOIN pg_temp.timesheet_unauthorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id;

  SELECT COUNT(*) FILTER (WHERE result_rows.success)::integer,
         COUNT(*) FILTER (WHERE NOT result_rows.success)::integer
    INTO v_success_count, v_failure_count
  FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows;

  SELECT jsonb_build_object(
    'ok', true,
    'batch_completed', true,
    'all_success', v_failure_count = 0,
    'action', 'UNAUTHORISE',
    'requested_count', v_requested_count,
    'success_count', v_success_count,
    'failure_count', v_failure_count,
    'has_failures', v_failure_count > 0,
    'results', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows), '[]'::jsonb),
    'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
    'failed_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows WHERE result_rows.success = false), '[]'::jsonb),
    'stale_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows WHERE result_rows.result_json ->> 'error_code' = 'ROW_SIGNATURE_MISMATCH'), '[]'::jsonb),
    'count_deltas', jsonb_build_object('processed_eligible', v_success_count, 'authorised_eligible', -v_success_count, 'total', 0),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks'), 'datasets', jsonb_build_array('bulk_authorise'), 'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf), '[]'::jsonb))
  ) INTO v_out;

  RETURN v_out;
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'LOCK_TIMEOUT', 'requested_count', COALESCE(v_requested_count, 0), 'success_count', 0, 'failure_count', COALESCE(v_requested_count, 0), 'results', '[]'::jsonb);
  END IF;
  RAISE;
END;
$function$;

-- timesheet_weekly_chain_delete_apply(uuid,uuid,uuid[],uuid[],uuid[],text)
CREATE OR REPLACE FUNCTION public.timesheet_weekly_chain_delete_apply(p_timesheet_id uuid, p_actor_user_id uuid, p_expected_timesheet_ids uuid[] DEFAULT NULL::uuid[], p_expected_contract_week_ids uuid[] DEFAULT NULL::uuid[], p_expected_nhsp_shift_ids uuid[] DEFAULT NULL::uuid[], p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_preview jsonb;
  v_recheck jsonb;
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_nhsp_shift_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_nhsp_shift_ids uuid[] := ARRAY[]::uuid[];
  v_expected_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_expected_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_expected_nhsp_shift_ids uuid[] := ARRAY[]::uuid[];
  v_signature_payload jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_primary_contract_week_id uuid := NULL;
  v_r2_keys text[] := ARRAY[]::text[];
  v_current_timesheet_id uuid;
  v_contract_id uuid;
  v_week_ending_date date;
  v_deleted_timesheets integer := 0;
  v_deleted_contract_weeks integer := 0;
  v_deleted_shifts integer := 0;
  v_locked_timesheets integer := 0;
  v_locked_contract_weeks integer := 0;
  v_locked_nhsp_shifts integer := 0;
BEGIN
  PERFORM set_config('lock_timeout', '750ms', true);
  IF p_timesheet_id IS NULL THEN RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ID_REQUIRED'; END IF;
  IF p_actor_user_id IS NULL THEN RAISE EXCEPTION USING MESSAGE = 'ACTOR_USER_ID_REQUIRED'; END IF;

  IF p_expected_timesheet_ids IS NULL
     OR p_expected_contract_week_ids IS NULL
     OR p_expected_nhsp_shift_ids IS NULL
     OR NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'kind', 'WEEKLY_CHAIN_DELETE_PARENT',
      'decision', 'BLOCKED',
      'apply_performed', false,
      'error_code', 'EXPECTED_DELETE_PREVIEW_REQUIRED'
    );
  END IF;

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_timesheet_ids
  FROM unnest(p_expected_timesheet_ids) AS expected(id)
  WHERE id IS NOT NULL;
  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_contract_week_ids
  FROM unnest(p_expected_contract_week_ids) AS expected(id)
  WHERE id IS NOT NULL;
  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_nhsp_shift_ids
  FROM unnest(p_expected_nhsp_shift_ids) AS expected(id)
  WHERE id IS NOT NULL;

  v_preview := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);
  IF COALESCE(v_preview ->> 'decision', 'BLOCKED') <> 'PERMANENT_DELETE' THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', CASE WHEN v_preview ->> 'decision' = 'ARCHIVE_REQUIRED' THEN 'ARCHIVE_REQUIRED' ELSE 'DELETE_BLOCKED' END
    );
  END IF;

  v_current_timesheet_id := NULLIF(v_preview ->> 'current_timesheet_id', '')::uuid;
  v_contract_id := NULLIF(v_preview ->> 'contract_id', '')::uuid;
  v_week_ending_date := NULLIF(v_preview ->> 'week_ending_date', '')::date;
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'timesheet_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'contract_week_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_nhsp_shift_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'nhsp_shift_ids', '[]'::jsonb)) AS ids(value);

  IF v_timesheet_ids IS DISTINCT FROM v_expected_timesheet_ids
     OR v_contract_week_ids IS DISTINCT FROM v_expected_contract_week_ids
     OR v_nhsp_shift_ids IS DISTINCT FROM v_expected_nhsp_shift_ids THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'DELETE_PREVIEW_STALE',
      'expected_target_set', jsonb_build_object(
        'timesheet_ids', to_jsonb(v_expected_timesheet_ids),
        'contract_week_ids', to_jsonb(v_expected_contract_week_ids),
        'nhsp_shift_ids', to_jsonb(v_expected_nhsp_shift_ids)
      )
    );
  END IF;

  IF COALESCE(array_length(v_timesheet_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION USING MESSAGE = 'EMPTY_REMOVAL_UNIT';
  END IF;
  IF COALESCE(array_length(v_timesheet_ids, 1), 0) > 32
     OR COALESCE(array_length(v_contract_week_ids, 1), 0) > 32
     OR COALESCE(array_length(v_nhsp_shift_ids, 1), 0) > 512 THEN
    RAISE EXCEPTION USING MESSAGE = 'REMOVAL_UNIT_TOO_LARGE';
  END IF;
  IF v_contract_id IS NULL OR v_week_ending_date IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'WEEKLY_TARGET_CONTEXT_MISSING';
  END IF;

  -- Lock the relational root first.  Child target identities are then locked in
  -- deterministic order and counted, so a missing or substituted target cannot
  -- be accepted merely because the preview JSON still looks plausible.
  PERFORM 1
  FROM public.contracts AS c
  WHERE c.id = v_contract_id
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION USING MESSAGE = 'CONTRACT_TARGET_CHANGED';
  END IF;

  SELECT COUNT(*)::integer
    INTO v_locked_timesheets
  FROM (
    SELECT t.timesheet_id
    FROM public.timesheets AS t
    WHERE t.timesheet_id = ANY(v_timesheet_ids)
    ORDER BY t.timesheet_id
    FOR UPDATE
  ) AS locked_timesheets;
  IF v_locked_timesheets <> COALESCE(array_length(v_timesheet_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_TARGET_SET_CHANGED';
  END IF;

  PERFORM 1
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY tf.timesheet_id, tf.id
  FOR UPDATE;

  SELECT COUNT(*)::integer
    INTO v_locked_contract_weeks
  FROM (
    SELECT cw.id
    FROM public.contract_weeks AS cw
    WHERE cw.id = ANY(v_contract_week_ids)
    ORDER BY cw.id
    FOR UPDATE
  ) AS locked_contract_weeks;
  IF v_locked_contract_weeks <> COALESCE(array_length(v_contract_week_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'CONTRACT_WEEK_TARGET_SET_CHANGED';
  END IF;

  SELECT COUNT(*)::integer
    INTO v_locked_nhsp_shifts
  FROM (
    SELECT ns.id
    FROM public.nhsp_shifts AS ns
    WHERE ns.id = ANY(v_nhsp_shift_ids)
    ORDER BY ns.id
    FOR UPDATE
  ) AS locked_nhsp_shifts;
  IF v_locked_nhsp_shifts <> COALESCE(array_length(v_nhsp_shift_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'NHSP_SHIFT_TARGET_SET_CHANGED';
  END IF;

  v_recheck := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'timesheet_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'contract_week_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_nhsp_shift_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'nhsp_shift_ids', '[]'::jsonb)) AS ids(value);

  IF COALESCE(v_recheck ->> 'decision', 'BLOCKED') <> 'PERMANENT_DELETE'
     OR NULLIF(v_recheck ->> 'current_timesheet_id', '')::uuid IS DISTINCT FROM v_current_timesheet_id
     OR NULLIF(v_recheck ->> 'contract_id', '')::uuid IS DISTINCT FROM v_contract_id
     OR NULLIF(v_recheck ->> 'week_ending_date', '')::date IS DISTINCT FROM v_week_ending_date
     OR v_recheck_timesheet_ids IS DISTINCT FROM v_timesheet_ids
     OR v_recheck_contract_week_ids IS DISTINCT FROM v_contract_week_ids
     OR v_recheck_nhsp_shift_ids IS DISTINCT FROM v_nhsp_shift_ids THEN
    RETURN v_recheck || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'DELETE_RECLASSIFIED',
      'locked_target_set', jsonb_build_object(
        'timesheet_ids', to_jsonb(v_timesheet_ids),
        'contract_week_ids', to_jsonb(v_contract_week_ids),
        'nhsp_shift_ids', to_jsonb(v_nhsp_shift_ids)
      )
    );
  END IF;

  SELECT cw.id
    INTO v_primary_contract_week_id
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current_timesheet_id
    AND cw.id = ANY(v_contract_week_ids)
  ORDER BY cw.id
  LIMIT 1;

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

  IF v_current_row_signature IS DISTINCT FROM BTRIM(p_expected_row_signature) THEN
    RETURN v_recheck || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'ROW_SIGNATURE_MISMATCH',
      'expected_row_signature', BTRIM(p_expected_row_signature),
      'current_row_signature', v_current_row_signature
    );
  END IF;

  -- Freeze existing mutable manual-queue R2 keys before key collection.
  -- The exact queue rows are locked in deterministic order so an r2_key cannot
  -- change between server-side key collection and the relational delete.
  PERFORM 1
  FROM public.manual_timesheet_queue AS queue_row
  WHERE queue_row.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY queue_row.id
  FOR UPDATE;

  SELECT COALESCE(array_agg(DISTINCT storage_key ORDER BY storage_key), ARRAY[]::text[])
    INTO v_r2_keys
  FROM (
    SELECT NULLIF(BTRIM(key_value), '') AS storage_key
    FROM public.timesheets AS t
    CROSS JOIN LATERAL unnest(ARRAY[t.manual_pdf_r2_key, t.r2_nurse_key, t.r2_auth_key, t.qr_r2_key]) AS keys(key_value)
    WHERE t.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT NULLIF(BTRIM(e.storage_key), '') FROM public.timesheet_evidence e WHERE e.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT NULLIF(BTRIM(q.r2_key), '') FROM public.manual_timesheet_queue q WHERE q.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT NULLIF(BTRIM(key_value), '')
      FROM public.timesheets_financials tf
      CROSS JOIN LATERAL unnest(ARRAY[tf.expenses_evidence_r2_key, tf.mileage_evidence_r2_key]) AS keys(key_value)
      WHERE tf.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT manifest_key
      FROM public.timesheets_financials tf
      CROSS JOIN LATERAL unnest(
        public.cloudtms_jsonb_storage_keys_v1(tf.expenses_evidence_manifest, 8)
        || public.cloudtms_jsonb_storage_keys_v1(tf.mileage_evidence_manifest, 8)
      ) AS manifest(manifest_key)
      WHERE tf.timesheet_id = ANY(v_timesheet_ids)
  ) AS keys
  WHERE storage_key IS NOT NULL;

  INSERT INTO public.audit_events(actor_user_id, object_type, object_id_text, action, before_json, after_json, reason)
  VALUES (
    p_actor_user_id,
    'timesheets',
    v_current_timesheet_id::text,
    'WEEKLY_CHAIN_DELETE_APPLIED',
    jsonb_build_object(
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids),
      'contract_id', v_contract_id,
      'week_ending_date', v_week_ending_date
    ),
    jsonb_build_object('deleted', true),
    'FINANCIALLY_CLEAN_WEEKLY_CHAIN'
  );

  IF COALESCE(array_length(v_nhsp_shift_ids, 1), 0) > 0 THEN
    DELETE FROM public.nhsp_shifts AS ns WHERE ns.id = ANY(v_nhsp_shift_ids);
    GET DIAGNOSTICS v_deleted_shifts = ROW_COUNT;
    IF v_deleted_shifts <> COALESCE(array_length(v_nhsp_shift_ids, 1), 0) THEN
      RAISE EXCEPTION USING MESSAGE = 'NHSP_SHIFT_DELETE_COUNT_MISMATCH';
    END IF;
  ELSE
    UPDATE public.nhsp_shifts AS ns SET timesheet_id = NULL, updated_at = now()
    WHERE ns.timesheet_id = ANY(v_timesheet_ids);
  END IF;

  DELETE FROM public.pay_item_snoozes AS s WHERE s.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheet_validations AS v WHERE v.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.hr_results AS h WHERE h.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.hr_issue_emails AS h WHERE h.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheet_evidence AS e WHERE e.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.manual_timesheet_queue AS q WHERE q.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.ts_pdfs_outbox AS o WHERE o.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.ts_financials_outbox AS o WHERE o.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheet_summary_pay_state_cache AS c WHERE c.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheets_financials AS tf WHERE tf.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.contract_weeks AS cw WHERE cw.id = ANY(v_contract_week_ids);
  GET DIAGNOSTICS v_deleted_contract_weeks = ROW_COUNT;
  DELETE FROM public.timesheets AS t WHERE t.timesheet_id = ANY(v_timesheet_ids);
  GET DIAGNOSTICS v_deleted_timesheets = ROW_COUNT;

  IF v_deleted_timesheets <> array_length(v_timesheet_ids, 1)
     OR v_deleted_contract_weeks <> COALESCE(array_length(v_contract_week_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'DELETE_COUNT_MISMATCH';
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'kind', 'WEEKLY_CHAIN_DELETE_PARENT',
    'decision', 'PERMANENT_DELETE',
    'apply_performed', true,
    'deleted_timesheets', v_deleted_timesheets,
    'deleted_contract_weeks', v_deleted_contract_weeks,
    'deleted_nhsp_shifts', v_deleted_shifts,
    'deleted_timesheet_ids', to_jsonb(v_timesheet_ids),
    'deleted_contract_week_ids', to_jsonb(v_contract_week_ids),
    'r2_cleanup_keys', to_jsonb(v_r2_keys)
  );
EXCEPTION
  WHEN lock_not_available OR deadlock_detected THEN
    RETURN jsonb_build_object(
      'ok', false,
      'kind', 'WEEKLY_CHAIN_DELETE_PARENT',
      'decision', 'BLOCKED',
      'apply_performed', false,
      'error_code', 'LOCK_TIMEOUT',
      'message', 'The weekly removal unit is currently being changed. Refresh and try again.'
    );
END;
$function$;

-- timesheet_weekly_chain_delete_preview(uuid,uuid)
CREATE OR REPLACE FUNCTION public.timesheet_weekly_chain_delete_preview(p_timesheet_id uuid, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_in_ts public.timesheets%rowtype;
  v_current_ts public.timesheets%rowtype;

  v_contract_id uuid := null;
  v_week_ending_date date := null;

  v_base_booking_id text := null;

  v_booking_ids text[] := array[]::text[];
  v_timesheet_ids uuid[] := array[]::uuid[];
  v_contract_week_ids uuid[] := array[]::uuid[];
  v_current_contract_week_ids uuid[] := array[]::uuid[];
  v_nhsp_shift_ids uuid[] := array[]::uuid[];

  v_blocked jsonb := '[]'::jsonb;

  v_history jsonb := '{}'::jsonb;
  v_decision text := 'BLOCKED';
  v_current_effective_adjustment boolean := false;
  v_manual_contract_week_targeted boolean := false;

  v_invoice_info jsonb := '[]'::jsonb;

  v_delete_items jsonb := '[]'::jsonb;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_weekly_chain_delete_preview: timesheet_id is required';
  end if;

  if p_actor_user_id is null then
    raise exception using message = 'ACTOR_USER_ID_REQUIRED';
  end if;

  if not exists (
    select 1
    from public.tms_users as actor
    where actor.id = p_actor_user_id
      and actor.is_active = true
  ) then
    raise exception using message = 'ACTOR_NOT_FOUND_OR_INACTIVE';
  end if;

  select t.*
  into v_in_ts
  from public.timesheets as t
  where t.timesheet_id = p_timesheet_id;

  if not found then
    raise exception 'timesheet_weekly_chain_delete_preview: timesheet % not found', p_timesheet_id;
  end if;

  v_base_booking_id := v_in_ts.booking_id;

  -- Resolve "current" by booking_id (robust even if is_current flags drift)
  select tcur.*
  into v_current_ts
  from public.timesheets as tcur
  where tcur.booking_id = v_base_booking_id
  order by tcur.is_current desc,
           tcur.version desc nulls last,
           tcur.updated_at desc nulls last,
           tcur.created_at desc nulls last,
           tcur.timesheet_id desc
  limit 1;

  if not found then
    raise exception 'timesheet_weekly_chain_delete_preview: booking_id % not found', v_base_booking_id;
  end if;

  select
    coalesce(array_agg(distinct cw_current.id order by cw_current.id), array[]::uuid[]),
    coalesce(bool_or(coalesce(cw_current.is_adjustment, false) = true OR coalesce(cw_current.additional_seq, 0) > 0), false)
  into v_current_contract_week_ids,
       v_current_effective_adjustment
  from public.contract_weeks as cw_current
  where cw_current.timesheet_id = v_current_ts.timesheet_id;

  -- Must be WEEKLY parent (not adjustment and not linked to an additional/manual adjustment contract_week)
  if v_current_ts.sheet_scope <> 'WEEKLY'::public.timesheet_scope_enum then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','NOT_WEEKLY',
      'message','Parent-chain delete applies only to WEEKLY parent timesheets.',
      'timesheet_id', v_current_ts.timesheet_id::text
    ));
  end if;

  if v_current_ts.is_adjustment is true OR coalesce(v_current_effective_adjustment, false) = true then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','NOT_PARENT',
      'message','Timesheet is an adjustment/additional manual row; parent-chain delete can only be requested for a non-adjustment WEEKLY parent timesheet.',
      'timesheet_id', v_current_ts.timesheet_id::text,
      'contract_week_ids', to_jsonb(coalesce(v_current_contract_week_ids, array[]::uuid[]))
    ));
  end if;

  if v_current_ts.contract_id is null or v_current_ts.week_ending_date is null then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','MISSING_CONTEXT',
      'message','Timesheet is missing contract_id and/or week_ending_date; cannot resolve chain safely.',
      'timesheet_id', v_current_ts.timesheet_id::text
    ));
  end if;

  v_contract_id := v_current_ts.contract_id;
  v_week_ending_date := v_current_ts.week_ending_date;

  -- Booking IDs in scope:
  -- - the parent booking_id
  -- - import-derived weekly adjustments in the same contract/week (NHSP/HR corrections/cancellations)
  if v_contract_id is not null and v_week_ending_date is not null then
    select coalesce(array_agg(distinct scoped_booking.booking_id order by scoped_booking.booking_id), array[]::text[])
    into v_booking_ids
    from (
      select v_current_ts.booking_id as booking_id
      union all
      select import_adjustment_timesheet.booking_id
      from public.timesheets as import_adjustment_timesheet
      where import_adjustment_timesheet.contract_id = v_contract_id
        and import_adjustment_timesheet.week_ending_date = v_week_ending_date
        and import_adjustment_timesheet.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
        and import_adjustment_timesheet.is_adjustment is true
        and (
          upper(coalesce(import_adjustment_timesheet.adjustment_origin,'')) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
          or nullif(btrim(coalesce(import_adjustment_timesheet.correction_kind,'')),'') is not null
          or import_adjustment_timesheet.correction_id is not null
        )
    ) as scoped_booking;
  else
    v_booking_ids := array[v_current_ts.booking_id];
  end if;

  -- All timesheet IDs (all versions) for those booking IDs
  select coalesce(array_agg(all_versions.timesheet_id order by all_versions.timesheet_id), array[]::uuid[])
  into v_timesheet_ids
  from public.timesheets as all_versions
  where all_versions.booking_id = any(v_booking_ids);

  -- Contract-week IDs to delete for a true parent chain only:
  -- (a) base week row additional_seq=0 / non-adjustment
  -- (b) contract_week rows linked to timesheets in scope, but only when they are base rows or import-derived children
  -- Manual additional contract_weeks are deliberately excluded and guarded against.
  if v_contract_id is not null and v_week_ending_date is not null then
    select coalesce(array_agg(distinct scoped_contract_week.cw_id order by scoped_contract_week.cw_id), array[]::uuid[])
    into v_contract_week_ids
    from (
      select base_contract_week.id as cw_id
      from public.contract_weeks as base_contract_week
      where base_contract_week.contract_id = v_contract_id
        and base_contract_week.week_ending_date = v_week_ending_date
        and coalesce(base_contract_week.additional_seq, 0) = 0
        and coalesce(base_contract_week.is_adjustment, false) = false
      union all
      select linked_contract_week.id as cw_id
      from public.contract_weeks as linked_contract_week
      join public.timesheets as linked_timesheet
        on linked_timesheet.timesheet_id = linked_contract_week.timesheet_id
      where linked_contract_week.timesheet_id = any(v_timesheet_ids)
        and (
          (
            coalesce(linked_contract_week.is_adjustment, false) = false
            and coalesce(linked_contract_week.additional_seq, 0) = 0
          )
          or (
            (coalesce(linked_contract_week.is_adjustment, false) = true or coalesce(linked_contract_week.additional_seq, 0) > 0)
            and (
              upper(coalesce(linked_timesheet.adjustment_origin,'')) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
              or nullif(btrim(coalesce(linked_timesheet.correction_kind,'')),'') is not null
              or linked_timesheet.correction_id is not null
            )
          )
        )
    ) as scoped_contract_week;
  else
    v_contract_week_ids := array[]::uuid[];
  end if;

  select exists(
    select 1
    from public.contract_weeks as guarded_contract_week
    left join public.timesheets as guarded_timesheet
      on guarded_timesheet.timesheet_id = guarded_contract_week.timesheet_id
    where guarded_contract_week.id = any(v_contract_week_ids)
      and (coalesce(guarded_contract_week.is_adjustment, false) = true or coalesce(guarded_contract_week.additional_seq, 0) > 0)
      and not (
        upper(coalesce(guarded_timesheet.adjustment_origin,'')) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
        or nullif(btrim(coalesce(guarded_timesheet.correction_kind,'')),'') is not null
        or guarded_timesheet.correction_id is not null
      )
  )
  into v_manual_contract_week_targeted;

  if coalesce(v_manual_contract_week_targeted, false) = true then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','MANUAL_ADJUSTMENT_CONTRACT_WEEK_IN_CHAIN',
      'message','Parent-chain delete would target a manual additional contract_week. Use additional-only delete for manual adjustment rows.',
      'contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[]))
    ));
  end if;

  -- NHSP/HR truth rows in this contract/week (for informational preview)
  if v_contract_id is not null and v_week_ending_date is not null then
    select coalesce(array_agg(nhsp_shift.id order by nhsp_shift.id), array[]::uuid[])
    into v_nhsp_shift_ids
    from public.nhsp_shifts as nhsp_shift
    where nhsp_shift.contract_id = v_contract_id
      and nhsp_shift.week_ending_date = v_week_ending_date
      and nhsp_shift.source_system in ('NHSP'::public.hr_source_enum, 'HEALTHROSTER'::public.hr_source_enum);
  else
    v_nhsp_shift_ids := array[]::uuid[];
  end if;

  v_history := public.timesheet_removal_financial_history_v1(
    v_timesheet_ids,
    v_booking_ids,
    v_contract_week_ids
  );

  v_blocked := v_blocked || COALESCE(v_history -> 'blockers', '[]'::jsonb);

  IF jsonb_array_length(v_blocked) > 0 THEN
    v_decision := 'BLOCKED';
  ELSIF COALESCE((v_history ->> 'archive_required')::boolean, false) THEN
    v_decision := 'ARCHIVE_REQUIRED';
  ELSE
    v_decision := 'PERMANENT_DELETE';
  END IF;

  -- delete_items[] for warning modal (one display row per booking_id)
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'timesheet_id', selected_timesheet.timesheet_id::text,
        'booking_id', selected_timesheet.booking_id,
        'week_ending_date', selected_timesheet.week_ending_date::text,
        'status', selected_timesheet.status::text,
        'is_adjustment', selected_timesheet.is_adjustment,
        'adjustment_origin', selected_timesheet.adjustment_origin,
        'correction_id', selected_timesheet.correction_id,
        'correction_kind', selected_timesheet.correction_kind,
        'display_role',
          case
            when selected_timesheet.booking_id = v_current_ts.booking_id then 'PARENT'
            else 'ADJUSTMENT'
          end,
        'total_hours', coalesce(selected_tsfin.total_hours, 0),
        'total_pay_ex_vat', coalesce(selected_tsfin.total_pay_ex_vat, 0),
        'total_charge_ex_vat', coalesce(selected_tsfin.total_charge_ex_vat, 0)
      )
      ORDER BY
        (case when selected_timesheet.booking_id = v_current_ts.booking_id then 0 else 1 end),
        selected_timesheet.booking_id,
        selected_timesheet.timesheet_id,
        selected_tsfin.id
    ),
    '[]'::jsonb
  )
  into v_delete_items
  from (
    select distinct on (timesheet_pick.booking_id)
      timesheet_pick.booking_id,
      timesheet_pick.timesheet_id,
      timesheet_pick.week_ending_date,
      timesheet_pick.status,
      timesheet_pick.is_adjustment,
      timesheet_pick.adjustment_origin,
      timesheet_pick.correction_id,
      timesheet_pick.correction_kind
    from public.timesheets as timesheet_pick
    where timesheet_pick.booking_id = any(v_booking_ids)
    order by timesheet_pick.booking_id,
             timesheet_pick.is_current desc,
             timesheet_pick.version desc nulls last,
             timesheet_pick.updated_at desc nulls last,
             timesheet_pick.created_at desc nulls last,
             timesheet_pick.timesheet_id desc
  ) as selected_timesheet
  left join public.timesheets_financials as selected_tsfin
    on selected_tsfin.timesheet_id = selected_timesheet.timesheet_id
   and selected_tsfin.is_current is true;

  return jsonb_build_object(
    'kind', 'WEEKLY_CHAIN_DELETE_PARENT',
    'requested_timesheet_id', p_timesheet_id::text,
    'current_timesheet_id', v_current_ts.timesheet_id::text,
    'contract_id', case when v_contract_id is null then null else v_contract_id::text end,
    'week_ending_date', case when v_week_ending_date is null then null else v_week_ending_date::text end,
    'booking_ids', to_jsonb(coalesce(v_booking_ids, array[]::text[])),
    'timesheet_ids', to_jsonb(coalesce(v_timesheet_ids, array[]::uuid[])),
    'contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[])),
    'nhsp_shift_ids', to_jsonb(coalesce(v_nhsp_shift_ids, array[]::uuid[])),
    'delete_items', v_delete_items,
    'decision', v_decision,
    'eligible', (v_decision = 'PERMANENT_DELETE'),
    'blocked_reasons', v_blocked,
    'blockers', v_blocked,
    'retention_reasons', COALESCE(v_history -> 'retention_reasons', '[]'::jsonb),
    'advance', COALESCE(v_history -> 'advance', '{}'::jsonb)
  );
end;
$function$;

-- timesheet_weekly_manual_adjustment_delete_apply(uuid,uuid,uuid[],uuid[],uuid[],uuid[],text)
CREATE OR REPLACE FUNCTION public.timesheet_weekly_manual_adjustment_delete_apply(p_timesheet_id uuid, p_actor_user_id uuid, p_expected_timesheet_ids uuid[] DEFAULT NULL::uuid[], p_expected_contract_week_ids uuid[] DEFAULT NULL::uuid[], p_expected_preserved_source_timesheet_ids uuid[] DEFAULT NULL::uuid[], p_expected_preserved_source_contract_week_ids uuid[] DEFAULT NULL::uuid[], p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_preview jsonb;
  v_recheck jsonb;
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_all_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_all_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_preserved_source_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_preserved_source_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_expected_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_expected_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_expected_preserved_source_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_expected_preserved_source_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_signature_payload jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_primary_contract_week_id uuid := NULL;
  v_r2_keys text[] := ARRAY[]::text[];
  v_current_timesheet_id uuid;
  v_deleted_timesheets integer := 0;
  v_deleted_contract_weeks integer := 0;
  v_locked_timesheets integer := 0;
  v_locked_contract_weeks integer := 0;
BEGIN
  PERFORM set_config('lock_timeout', '750ms', true);
  IF p_timesheet_id IS NULL THEN RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ID_REQUIRED'; END IF;
  IF p_actor_user_id IS NULL THEN RAISE EXCEPTION USING MESSAGE = 'ACTOR_USER_ID_REQUIRED'; END IF;

  IF p_expected_timesheet_ids IS NULL
     OR p_expected_contract_week_ids IS NULL
     OR p_expected_preserved_source_timesheet_ids IS NULL
     OR p_expected_preserved_source_contract_week_ids IS NULL
     OR NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'kind', 'WEEKLY_MANUAL_ADJUSTMENT_DELETE',
      'decision', 'BLOCKED',
      'apply_performed', false,
      'error_code', 'EXPECTED_DELETE_PREVIEW_REQUIRED'
    );
  END IF;

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_timesheet_ids
  FROM unnest(p_expected_timesheet_ids) AS expected(id)
  WHERE id IS NOT NULL;
  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_contract_week_ids
  FROM unnest(p_expected_contract_week_ids) AS expected(id)
  WHERE id IS NOT NULL;
  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_preserved_source_timesheet_ids
  FROM unnest(p_expected_preserved_source_timesheet_ids) AS expected(id)
  WHERE id IS NOT NULL;
  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_preserved_source_contract_week_ids
  FROM unnest(p_expected_preserved_source_contract_week_ids) AS expected(id)
  WHERE id IS NOT NULL;

  v_preview := public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id, p_actor_user_id);
  IF COALESCE(v_preview ->> 'decision', 'BLOCKED') <> 'PERMANENT_DELETE' THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', CASE WHEN v_preview ->> 'decision' = 'ARCHIVE_REQUIRED' THEN 'ARCHIVE_REQUIRED' ELSE 'DELETE_BLOCKED' END
    );
  END IF;

  v_current_timesheet_id := NULLIF(v_preview ->> 'current_timesheet_id', '')::uuid;
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[]) INTO v_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'timesheet_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[]) INTO v_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'contract_week_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[]) INTO v_preserved_source_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'preserved_source_timesheet_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[]) INTO v_preserved_source_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'preserved_source_contract_week_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_all_timesheet_ids
  FROM unnest(v_timesheet_ids || v_preserved_source_timesheet_ids) AS all_ids(id);
  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_all_contract_week_ids
  FROM unnest(v_contract_week_ids || v_preserved_source_contract_week_ids) AS all_ids(id);

  IF v_timesheet_ids IS DISTINCT FROM v_expected_timesheet_ids
     OR v_contract_week_ids IS DISTINCT FROM v_expected_contract_week_ids
     OR v_preserved_source_timesheet_ids IS DISTINCT FROM v_expected_preserved_source_timesheet_ids
     OR v_preserved_source_contract_week_ids IS DISTINCT FROM v_expected_preserved_source_contract_week_ids THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'DELETE_PREVIEW_STALE',
      'expected_target_set', jsonb_build_object(
        'timesheet_ids', to_jsonb(v_expected_timesheet_ids),
        'contract_week_ids', to_jsonb(v_expected_contract_week_ids),
        'preserved_source_timesheet_ids', to_jsonb(v_expected_preserved_source_timesheet_ids),
        'preserved_source_contract_week_ids', to_jsonb(v_expected_preserved_source_contract_week_ids)
      )
    );
  END IF;

  IF COALESCE(array_length(v_timesheet_ids, 1), 0) = 0
     OR COALESCE(array_length(v_contract_week_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION USING MESSAGE = 'EMPTY_REMOVAL_UNIT';
  END IF;
  IF COALESCE(array_length(v_all_timesheet_ids, 1), 0) > 64
     OR COALESCE(array_length(v_all_contract_week_ids, 1), 0) > 64 THEN
    RAISE EXCEPTION USING MESSAGE = 'REMOVAL_UNIT_TOO_LARGE';
  END IF;
  IF EXISTS (
    SELECT 1 FROM unnest(v_timesheet_ids) AS target(id)
    WHERE target.id = ANY(v_preserved_source_timesheet_ids)
  ) OR EXISTS (
    SELECT 1 FROM unnest(v_contract_week_ids) AS target(id)
    WHERE target.id = ANY(v_preserved_source_contract_week_ids)
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_OVERLAPS_DELETE_TARGET';
  END IF;

  -- Lock all relational roots before child rows.  Queue metadata is never used
  -- as deletion authority; only exact foreign-key identities are authoritative.
  PERFORM 1
  FROM public.contracts AS c
  WHERE c.id IN (
    SELECT DISTINCT cw.contract_id
    FROM public.contract_weeks AS cw
    WHERE cw.id = ANY(v_all_contract_week_ids)
      AND cw.contract_id IS NOT NULL
  )
  ORDER BY c.id
  FOR UPDATE;

  SELECT COUNT(*)::integer
    INTO v_locked_timesheets
  FROM (
    SELECT t.timesheet_id
    FROM public.timesheets AS t
    WHERE t.timesheet_id = ANY(v_all_timesheet_ids)
    ORDER BY t.timesheet_id
    FOR UPDATE
  ) AS locked_timesheets;
  IF v_locked_timesheets <> COALESCE(array_length(v_all_timesheet_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  PERFORM 1
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY tf.timesheet_id, tf.id
  FOR UPDATE;

  SELECT COUNT(*)::integer
    INTO v_locked_contract_weeks
  FROM (
    SELECT cw.id
    FROM public.contract_weeks AS cw
    WHERE cw.id = ANY(v_all_contract_week_ids)
    ORDER BY cw.id
    FOR UPDATE
  ) AS locked_contract_weeks;
  IF v_locked_contract_weeks <> COALESCE(array_length(v_all_contract_week_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  -- Validate both sides of the relationship while the exact rows are locked.
  IF EXISTS (
    SELECT 1
    FROM public.contract_weeks AS target_cw
    WHERE target_cw.id = ANY(v_contract_week_ids)
      AND (
        COALESCE(target_cw.is_adjustment, false) IS NOT TRUE
        OR COALESCE(target_cw.additional_seq, 0) <= 0
        OR target_cw.timesheet_id IS NULL
        OR target_cw.timesheet_id <> ALL(v_timesheet_ids)
      )
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'MANUAL_ADJUSTMENT_TARGET_CHANGED';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.contract_weeks AS source_cw
    WHERE source_cw.id = ANY(v_preserved_source_contract_week_ids)
      AND (
        COALESCE(source_cw.is_adjustment, false)
        OR COALESCE(source_cw.additional_seq, 0) <> 0
        OR (source_cw.timesheet_id IS NOT NULL AND source_cw.timesheet_id <> ALL(v_preserved_source_timesheet_ids))
      )
  ) OR EXISTS (
    SELECT 1
    FROM public.contract_weeks AS target_cw
    WHERE target_cw.id = ANY(v_contract_week_ids)
      AND NOT EXISTS (
        SELECT 1
        FROM public.contract_weeks AS source_cw
        WHERE source_cw.id = ANY(v_preserved_source_contract_week_ids)
          AND source_cw.contract_id = target_cw.contract_id
          AND source_cw.week_ending_date = target_cw.week_ending_date
          AND COALESCE(source_cw.is_adjustment, false) = false
          AND COALESCE(source_cw.additional_seq, 0) = 0
      )
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  v_recheck := public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id, p_actor_user_id);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'timesheet_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'contract_week_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_preserved_source_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'preserved_source_timesheet_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_preserved_source_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'preserved_source_contract_week_ids', '[]'::jsonb)) AS ids(value);

  IF COALESCE(v_recheck ->> 'decision', 'BLOCKED') <> 'PERMANENT_DELETE'
     OR NULLIF(v_recheck ->> 'current_timesheet_id', '')::uuid IS DISTINCT FROM v_current_timesheet_id
     OR v_recheck_timesheet_ids IS DISTINCT FROM v_timesheet_ids
     OR v_recheck_contract_week_ids IS DISTINCT FROM v_contract_week_ids
     OR v_recheck_preserved_source_timesheet_ids IS DISTINCT FROM v_preserved_source_timesheet_ids
     OR v_recheck_preserved_source_contract_week_ids IS DISTINCT FROM v_preserved_source_contract_week_ids THEN
    RETURN v_recheck || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'DELETE_RECLASSIFIED',
      'locked_target_set', jsonb_build_object(
        'timesheet_ids', to_jsonb(v_timesheet_ids),
        'contract_week_ids', to_jsonb(v_contract_week_ids),
        'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
        'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids)
      )
    );
  END IF;

  SELECT cw.id
    INTO v_primary_contract_week_id
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current_timesheet_id
    AND cw.id = ANY(v_contract_week_ids)
  ORDER BY cw.id
  LIMIT 1;

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

  IF v_current_row_signature IS DISTINCT FROM BTRIM(p_expected_row_signature) THEN
    RETURN v_recheck || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'ROW_SIGNATURE_MISMATCH',
      'expected_row_signature', BTRIM(p_expected_row_signature),
      'current_row_signature', v_current_row_signature
    );
  END IF;

  -- Freeze existing mutable manual-queue R2 keys before key collection.
  -- The exact queue rows are locked in deterministic order so an r2_key cannot
  -- change between server-side key collection and the relational delete.
  PERFORM 1
  FROM public.manual_timesheet_queue AS queue_row
  WHERE queue_row.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY queue_row.id
  FOR UPDATE;

  SELECT COALESCE(array_agg(DISTINCT storage_key ORDER BY storage_key), ARRAY[]::text[])
    INTO v_r2_keys
  FROM (
    SELECT NULLIF(BTRIM(key_value), '') AS storage_key
    FROM public.timesheets AS t
    CROSS JOIN LATERAL unnest(ARRAY[t.manual_pdf_r2_key, t.r2_nurse_key, t.r2_auth_key, t.qr_r2_key]) AS keys(key_value)
    WHERE t.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT NULLIF(BTRIM(e.storage_key), '') FROM public.timesheet_evidence e WHERE e.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT NULLIF(BTRIM(q.r2_key), '') FROM public.manual_timesheet_queue q WHERE q.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT NULLIF(BTRIM(key_value), '')
      FROM public.timesheets_financials tf
      CROSS JOIN LATERAL unnest(ARRAY[tf.expenses_evidence_r2_key, tf.mileage_evidence_r2_key]) AS keys(key_value)
      WHERE tf.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT manifest_key
      FROM public.timesheets_financials tf
      CROSS JOIN LATERAL unnest(
        public.cloudtms_jsonb_storage_keys_v1(tf.expenses_evidence_manifest, 8)
        || public.cloudtms_jsonb_storage_keys_v1(tf.mileage_evidence_manifest, 8)
      ) AS manifest(manifest_key)
      WHERE tf.timesheet_id = ANY(v_timesheet_ids)
  ) AS keys WHERE storage_key IS NOT NULL;

  INSERT INTO public.audit_events(actor_user_id, object_type, object_id_text, action, before_json, after_json, reason)
  VALUES (
    p_actor_user_id,
    'timesheets',
    v_current_timesheet_id::text,
    'WEEKLY_MANUAL_ADJUSTMENT_DELETE_APPLIED',
    jsonb_build_object(
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids),
      'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
      'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids)
    ),
    jsonb_build_object('deleted', true),
    'FINANCIALLY_CLEAN_MANUAL_ADJUSTMENT'
  );

  UPDATE public.nhsp_shifts AS ns SET timesheet_id = NULL, updated_at = now()
  WHERE ns.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.pay_item_snoozes AS s WHERE s.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheet_validations AS v WHERE v.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.hr_results AS h WHERE h.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.hr_issue_emails AS h WHERE h.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheet_evidence AS e WHERE e.timesheet_id = ANY(v_timesheet_ids);
  -- Relational-only queue deletion.  Caller-controlled/cached meta_json is not
  -- an ownership relationship and therefore cannot authorise deletion.
  DELETE FROM public.manual_timesheet_queue AS q
  WHERE q.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.ts_pdfs_outbox AS o WHERE o.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.ts_financials_outbox AS o WHERE o.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheet_summary_pay_state_cache AS c WHERE c.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheets_financials AS tf WHERE tf.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.contract_weeks AS cw
  WHERE cw.id = ANY(v_contract_week_ids)
    AND COALESCE(cw.is_adjustment, false)
    AND COALESCE(cw.additional_seq, 0) > 0;
  GET DIAGNOSTICS v_deleted_contract_weeks = ROW_COUNT;
  DELETE FROM public.timesheets AS t WHERE t.timesheet_id = ANY(v_timesheet_ids);
  GET DIAGNOSTICS v_deleted_timesheets = ROW_COUNT;

  IF v_deleted_timesheets <> array_length(v_timesheet_ids, 1)
     OR v_deleted_contract_weeks <> array_length(v_contract_week_ids, 1) THEN
    RAISE EXCEPTION USING MESSAGE = 'DELETE_COUNT_MISMATCH';
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'kind', 'WEEKLY_MANUAL_ADJUSTMENT_DELETE',
    'decision', 'PERMANENT_DELETE',
    'apply_performed', true,
    'deleted_timesheets', v_deleted_timesheets,
    'deleted_contract_weeks', v_deleted_contract_weeks,
    'deleted_timesheet_ids', to_jsonb(v_timesheet_ids),
    'deleted_contract_week_ids', to_jsonb(v_contract_week_ids),
    'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
    'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids),
    'r2_cleanup_keys', to_jsonb(v_r2_keys)
  );
EXCEPTION
  WHEN lock_not_available OR deadlock_detected THEN
    RETURN jsonb_build_object(
      'ok', false,
      'kind', 'WEEKLY_MANUAL_ADJUSTMENT_DELETE',
      'decision', 'BLOCKED',
      'apply_performed', false,
      'error_code', 'LOCK_TIMEOUT',
      'message', 'The manual-adjustment removal unit is currently being changed. Refresh and try again.'
    );
END;
$function$;

-- timesheet_weekly_manual_adjustment_delete_preview(uuid,uuid)
CREATE OR REPLACE FUNCTION public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id uuid, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_in_ts public.timesheets%rowtype;
  v_current_ts public.timesheets%rowtype;

  v_booking_id text := null;

  v_timesheet_ids uuid[] := array[]::uuid[];
  v_contract_week_ids uuid[] := array[]::uuid[];
  v_contract_week_id uuid := null;
  v_preserved_source_contract_week_ids uuid[] := array[]::uuid[];
  v_preserved_source_timesheet_ids uuid[] := array[]::uuid[];
  v_effective_manual_adjustment boolean := false;
  v_linked_adjustment_contract_week_exists boolean := false;
  v_has_base_contract_week_target boolean := false;

  v_blocked jsonb := '[]'::jsonb;

  v_history jsonb := '{}'::jsonb;
  v_decision text := 'BLOCKED';

  v_delete_items jsonb := '[]'::jsonb;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_weekly_manual_adjustment_delete_preview: timesheet_id is required';
  end if;

  if p_actor_user_id is null then
    raise exception using message = 'ACTOR_USER_ID_REQUIRED';
  end if;

  if not exists (
    select 1
    from public.tms_users as actor
    where actor.id = p_actor_user_id
      and actor.is_active = true
  ) then
    raise exception using message = 'ACTOR_NOT_FOUND_OR_INACTIVE';
  end if;

  select input_timesheet.*
  into v_in_ts
  from public.timesheets as input_timesheet
  where input_timesheet.timesheet_id = p_timesheet_id;

  if not found then
    raise exception 'timesheet_weekly_manual_adjustment_delete_preview: timesheet % not found', p_timesheet_id;
  end if;

  v_booking_id := v_in_ts.booking_id;

  select current_timesheet.*
  into v_current_ts
  from public.timesheets as current_timesheet
  where current_timesheet.booking_id = v_booking_id
  order by current_timesheet.is_current desc, current_timesheet.version desc, current_timesheet.updated_at desc nulls last, current_timesheet.created_at desc nulls last, current_timesheet.timesheet_id desc
  limit 1;

  if not found then
    raise exception 'timesheet_weekly_manual_adjustment_delete_preview: booking_id % not found', v_booking_id;
  end if;

  select coalesce(array_agg(all_versions.timesheet_id order by all_versions.version asc nulls last, all_versions.created_at asc nulls last, all_versions.timesheet_id asc), array[]::uuid[])
  into v_timesheet_ids
  from public.timesheets as all_versions
  where all_versions.booking_id = v_current_ts.booking_id;

  select
    coalesce(bool_or(coalesce(linked_contract_week.is_adjustment, false) = true or coalesce(linked_contract_week.additional_seq, 0) > 0), false),
    coalesce(bool_or(coalesce(linked_contract_week.is_adjustment, false) = false or coalesce(linked_contract_week.additional_seq, 0) <= 0), false)
  into v_linked_adjustment_contract_week_exists,
       v_has_base_contract_week_target
  from public.contract_weeks as linked_contract_week
  where linked_contract_week.timesheet_id = any(v_timesheet_ids);

  select coalesce(array_agg(distinct adjustment_contract_week.id order by adjustment_contract_week.id), array[]::uuid[])
  into v_contract_week_ids
  from public.contract_weeks as adjustment_contract_week
  where adjustment_contract_week.timesheet_id = any(v_timesheet_ids)
    and coalesce(adjustment_contract_week.is_adjustment, false) = true
    and coalesce(adjustment_contract_week.additional_seq, 0) > 0;

  v_effective_manual_adjustment :=
    coalesce(v_current_ts.is_adjustment, false) = true
    or coalesce(v_linked_adjustment_contract_week_exists, false) = true
    or v_current_ts.parent_timesheet_id is not null
    or v_current_ts.correction_id is not null
    or nullif(btrim(coalesce(v_current_ts.correction_kind, '')), '') is not null
    or upper(coalesce(v_current_ts.adjustment_origin, '')) in ('MANUAL_ADJUSTMENT', 'IMPORT_CORRECTION', 'IMPORT_CANCELLATION');

  if coalesce(array_length(v_contract_week_ids, 1), 0) > 0 then
    v_contract_week_id := v_contract_week_ids[1];
  end if;

  select coalesce(array_agg(distinct source_contract_week.id order by source_contract_week.id), array[]::uuid[])
  into v_preserved_source_contract_week_ids
  from public.contract_weeks as adjustment_contract_week_for_source
  join public.contract_weeks as source_contract_week
    on source_contract_week.contract_id = adjustment_contract_week_for_source.contract_id
   and source_contract_week.week_ending_date = adjustment_contract_week_for_source.week_ending_date
   and coalesce(source_contract_week.additional_seq, 0) = 0
   and coalesce(source_contract_week.is_adjustment, false) = false
  where adjustment_contract_week_for_source.id = any(v_contract_week_ids);

  select coalesce(array_agg(distinct preserved_timesheet_id order by preserved_timesheet_id), array[]::uuid[])
  into v_preserved_source_timesheet_ids
  from (
    select source_contract_week_for_ts.timesheet_id as preserved_timesheet_id
    from public.contract_weeks as source_contract_week_for_ts
    where source_contract_week_for_ts.id = any(v_preserved_source_contract_week_ids)
      and source_contract_week_for_ts.timesheet_id is not null
    union
    select v_current_ts.parent_timesheet_id as preserved_timesheet_id
    where v_current_ts.parent_timesheet_id is not null
  ) as preserved_source_timesheet_ids
  where preserved_source_timesheet_ids.preserved_timesheet_id is not null;

  if v_current_ts.sheet_scope <> 'WEEKLY'::public.timesheet_scope_enum then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','NOT_WEEKLY',
      'message','Manual adjustment delete applies only to WEEKLY adjustment timesheets.',
      'timesheet_id', v_current_ts.timesheet_id::text
    ));
  end if;

  if coalesce(v_effective_manual_adjustment, false) is not true then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','NOT_ADJUSTMENT',
      'message','Timesheet is not an adjustment/additional manual row; this preview is only for WEEKLY manual adjustment deletes.',
      'timesheet_id', v_current_ts.timesheet_id::text,
      'contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[]))
    ));
  end if;

  if (
    upper(coalesce(v_current_ts.adjustment_origin,'')) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
    or nullif(btrim(coalesce(v_current_ts.correction_kind,'')),'') is not null
    or v_current_ts.correction_id is not null
  ) then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','IMPORT_DERIVED_CHILD',
      'message','Import-derived adjustments cannot be deleted directly; they must be deleted via the WEEKLY parent-chain delete.',
      'timesheet_id', v_current_ts.timesheet_id::text,
      'adjustment_origin', v_current_ts.adjustment_origin,
      'correction_id', v_current_ts.correction_id,
      'correction_kind', v_current_ts.correction_kind
    ));
  end if;

  if coalesce(array_length(v_contract_week_ids,1),0) = 0 then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','MISSING_CONTRACT_WEEK',
      'message','No linked additional/manual adjustment contract_week found for this weekly manual adjustment; refusing delete to avoid orphan/corruption.',
      'timesheet_id', v_current_ts.timesheet_id::text
    ));
  end if;

  if coalesce(v_has_base_contract_week_target, false) = true then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','BASE_CONTRACT_WEEK_TARGETED',
      'message','Delete preview resolved a base/source contract_week for a manual adjustment delete; refusing delete to protect the parent/source row.',
      'contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[])),
      'preserved_source_contract_week_ids', to_jsonb(coalesce(v_preserved_source_contract_week_ids, array[]::uuid[]))
    ));
  end if;

  v_history := public.timesheet_removal_financial_history_v1(
    v_timesheet_ids,
    ARRAY[v_current_ts.booking_id],
    v_contract_week_ids
  );

  v_blocked := v_blocked || COALESCE(v_history -> 'blockers', '[]'::jsonb);

  IF jsonb_array_length(v_blocked) > 0 THEN
    v_decision := 'BLOCKED';
  ELSIF COALESCE((v_history ->> 'archive_required')::boolean, false) THEN
    v_decision := 'ARCHIVE_REQUIRED';
  ELSE
    v_decision := 'PERMANENT_DELETE';
  END IF;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'timesheet_id', v_current_ts.timesheet_id::text,
        'booking_id', v_current_ts.booking_id,
        'week_ending_date', v_current_ts.week_ending_date::text,
        'status', v_current_ts.status::text,
        'is_adjustment', coalesce(v_effective_manual_adjustment, false),
        'adjustment_origin', v_current_ts.adjustment_origin,
        'correction_id', v_current_ts.correction_id,
        'correction_kind', v_current_ts.correction_kind,
        'display_role', 'MANUAL_ADJUSTMENT',
        'total_hours', coalesce(selected_tsfin.total_hours, 0),
        'total_pay_ex_vat', coalesce(selected_tsfin.total_pay_ex_vat, 0),
        'total_charge_ex_vat', coalesce(selected_tsfin.total_charge_ex_vat, 0)
      )
      order by selected_tsfin.id
    ),
    '[]'::jsonb
  )
  into v_delete_items
  from public.timesheets_financials as selected_tsfin
  where selected_tsfin.timesheet_id = v_current_ts.timesheet_id
    and selected_tsfin.is_current is true;

  if jsonb_array_length(v_delete_items) = 0 then
    v_delete_items := jsonb_build_array(jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id::text,
      'booking_id', v_current_ts.booking_id,
      'week_ending_date', v_current_ts.week_ending_date::text,
      'status', v_current_ts.status::text,
      'is_adjustment', coalesce(v_effective_manual_adjustment, false),
      'adjustment_origin', v_current_ts.adjustment_origin,
      'correction_id', v_current_ts.correction_id,
      'correction_kind', v_current_ts.correction_kind,
      'display_role', 'MANUAL_ADJUSTMENT',
      'total_hours', 0,
      'total_pay_ex_vat', 0,
      'total_charge_ex_vat', 0
    ));
  end if;

  return jsonb_build_object(
    'kind', 'WEEKLY_MANUAL_ADJUSTMENT_DELETE',
    'requested_timesheet_id', p_timesheet_id::text,
    'current_timesheet_id', v_current_ts.timesheet_id::text,
    'booking_id', v_current_ts.booking_id,
    'contract_week_id', case when v_contract_week_id is null then null else v_contract_week_id::text end,
    'contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[])),
    'timesheet_ids', to_jsonb(coalesce(v_timesheet_ids, array[]::uuid[])),
    'preserved_source_timesheet_ids', to_jsonb(coalesce(v_preserved_source_timesheet_ids, array[]::uuid[])),
    'preserved_source_contract_week_ids', to_jsonb(coalesce(v_preserved_source_contract_week_ids, array[]::uuid[])),
    'delete_items', v_delete_items,
    'decision', v_decision,
    'eligible', (v_decision = 'PERMANENT_DELETE'),
    'blocked_reasons', v_blocked,
    'blockers', v_blocked,
    'retention_reasons', COALESCE(v_history -> 'retention_reasons', '[]'::jsonb),
    'advance', COALESCE(v_history -> 'advance', '{}'::jsonb)
  );
end;
$function$;

-- timesheets_invalidate_prevalidation_on_change()
CREATE OR REPLACE FUNCTION public.timesheets_invalidate_prevalidation_on_change()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_tv_status public.validation_status_enum;
  v_tv_pre_validated boolean;
  v_tv_hr_request_id text;
  v_tv_hr_request_source public.reference_source_enum;

  v_changed boolean := false;

  -- DAILY change flags
  v_daily_changed_worked boolean := false;
  v_daily_changed_break boolean := false;
  v_daily_changed_refnum boolean := false;
  v_daily_changed_dayrefs boolean := false;

  -- WEEKLY change flags
  v_weekly_changed_sched boolean := false;
  v_weekly_changed_refnum boolean := false;
  v_weekly_changed_dayrefs boolean := false;
  v_weekly_changed_units_week boolean := false;
  v_weekly_changed_units_per_day boolean := false;

  v_ignore_daily_ref_autoset boolean := false;
begin
  if tg_op <> 'UPDATE' then
    return new;
  end if;

  -- Only apply to current row updates
  if coalesce(new.is_current, false) is not true then
    return new;
  end if;

  -- If the old row was not current, do not invalidate (avoid version-rotation noise)
  if coalesce(old.is_current, false) is not true then
    return new;
  end if;

  -- Load current validation row (if any)
  select
    tv.status,
    coalesce(tv.pre_validated, false),
    tv.hr_request_id,
    tv.hr_request_source
  into
    v_tv_status,
    v_tv_pre_validated,
    v_tv_hr_request_id,
    v_tv_hr_request_source
  from public.timesheet_validations tv
  where tv.timesheet_id = new.timesheet_id
  limit 1;

  if not found then
    return new;
  end if;

  -- Only invalidate when validation was previously "good" OR pre_validated was set
  if not (
    v_tv_pre_validated is true
    or v_tv_status = 'VALIDATION_OK'::public.validation_status_enum
    or v_tv_status = 'OVERRIDDEN'::public.validation_status_enum
  ) then
    return new;
  end if;

  -- ─────────────────────────────────────────────
  -- Determine whether validation-relevant truth changed
  -- ─────────────────────────────────────────────
  if new.sheet_scope = 'DAILY'::public.timesheet_scope_enum then
    v_daily_changed_worked :=
      (new.worked_start_iso is distinct from old.worked_start_iso)
      or (new.worked_end_iso is distinct from old.worked_end_iso);

    v_daily_changed_break :=
      (new.break_start_iso is distinct from old.break_start_iso)
      or (new.break_end_iso is distinct from old.break_end_iso)
      or (new.break_minutes is distinct from old.break_minutes);

    v_daily_changed_refnum :=
      (new.reference_number is distinct from old.reference_number);

    v_daily_changed_dayrefs :=
      (new.day_references_json is distinct from old.day_references_json);

    -- Special-case: DAILY reference_number auto-set to imported HR request id
    -- Do NOT invalidate if the ONLY relevant change is reference_number and it matches the imported hr_request_id.
    if v_daily_changed_refnum is true
       and v_daily_changed_worked is false
       and v_daily_changed_break is false
       and v_daily_changed_dayrefs is false
       and v_tv_hr_request_source = 'IMPORTED'::public.reference_source_enum
       and v_tv_hr_request_id is not null
       and new.reference_number is not distinct from v_tv_hr_request_id
    then
      v_ignore_daily_ref_autoset := true;
    end if;

    v_changed :=
      (v_daily_changed_worked or v_daily_changed_break or v_daily_changed_dayrefs or v_daily_changed_refnum)
      and v_ignore_daily_ref_autoset is false;

  elsif new.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum then
    v_weekly_changed_sched :=
      (new.actual_schedule_json is distinct from old.actual_schedule_json);

    v_weekly_changed_refnum :=
      (new.reference_number is distinct from old.reference_number);

    v_weekly_changed_dayrefs :=
      (new.day_references_json is distinct from old.day_references_json);

    v_weekly_changed_units_week :=
      (new.additional_units_week is distinct from old.additional_units_week);

    v_weekly_changed_units_per_day :=
      (new.additional_units_per_day is distinct from old.additional_units_per_day);

    v_changed :=
      v_weekly_changed_sched
      or v_weekly_changed_refnum
      or v_weekly_changed_dayrefs
      or v_weekly_changed_units_week
      or v_weekly_changed_units_per_day;
  else
    return new;
  end if;

  if v_changed is not true then
    return new;
  end if;

  -- ─────────────────────────────────────────────
  -- Invalidate pre-validation + force re-validation
  -- ─────────────────────────────────────────────
  update public.timesheet_validations tvu
     set status = 'PENDING'::public.validation_status_enum,
         pre_validated = false,
         validated_at_utc = null,
         reason_code = 'TIMESHEET_CHANGED',
         updated_at = now()
   where tvu.timesheet_id = new.timesheet_id;

  return new;
end;
$function$;

-- tms_touch_updated_at()
CREATE OR REPLACE FUNCTION public.tms_touch_updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
begin
  new.updated_at := now();
  return new;
end $function$;

-- trg_banking_pay_preview_selection_carry_apply()
CREATE OR REPLACE FUNCTION public.trg_banking_pay_preview_selection_carry_apply()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_stable_selection_key text;
  v_registration record;
  v_selected_ids jsonb;
BEGIN
  IF pg_trigger_depth() > 1
     OR NEW.status <> 'READY'
     OR NEW.candidate_id IS NULL THEN
    RETURN NEW;
  END IF;

  v_stable_selection_key := public._pay_workbench_preview_selection_key_v1(
    NEW.candidate_id,
    NEW.section,
    NEW.timesheet_id,
    NEW.key_type,
    NEW.key_value,
    NEW.row_key,
    NEW.row_json
  );

  IF v_stable_selection_key IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT registration_row.*
  INTO v_registration
  FROM public.banking_pay_workbench_selection_carry_registrations registration_row
  WHERE registration_row.target_session_id = NEW.session_id
    AND registration_row.candidate_id = NEW.candidate_id
    AND registration_row.stable_selection_key = v_stable_selection_key
    AND registration_row.status = 'PENDING'
  ORDER BY
    registration_row.source_priority,
    registration_row.created_at_utc,
    registration_row.id
  LIMIT 1
  FOR UPDATE;

  IF v_registration.id IS NULL THEN
    RETURN NEW;
  END IF;

  UPDATE public.banking_pay_workbench_preview_rows target_preview
  SET selected = v_registration.selected,
      selection_state = v_registration.selection_state,
      row_json = jsonb_strip_nulls(
        COALESCE(target_preview.row_json, '{}'::jsonb)
        || jsonb_build_object(
          'selected', v_registration.selected,
          'selection_state', v_registration.selection_state,
          'selection_user_override', CASE
            WHEN upper(btrim(COALESCE(v_registration.source_row_snapshot_json->>'selection_user_override', '')))
              IN ('SELECTED', 'UNSELECTED')
              THEN upper(btrim(v_registration.source_row_snapshot_json->>'selection_user_override'))
            ELSE NULL::text
          END,
          'selection_origin', 'SESSION_REPLACEMENT_CARRY',
          'selection_carry_registration_id', v_registration.id,
          'selection_carried_at_utc', clock_timestamp(),
          'policy_x_selection_authority_scope', 'PRE_DRAFT_SELECTION_INTENT_ONLY'
        )
      ),
      updated_at_utc = clock_timestamp()
  WHERE target_preview.id = NEW.id;

  UPDATE public.banking_pay_workbench_selection_carry_registrations applied
  SET status = 'APPLIED',
      target_preview_row_id = NEW.id,
      state_reason_code = 'MATCHED_STABLE_SELECTION_KEY',
      updated_at_utc = clock_timestamp(),
      completed_at_utc = clock_timestamp()
  WHERE applied.id = v_registration.id;

  UPDATE public.banking_pay_workbench_selection_carry_registrations superseded
  SET status = 'SUPERSEDED',
      state_reason_code = 'HIGHER_PRIORITY_SELECTION_CARRIED',
      updated_at_utc = clock_timestamp(),
      completed_at_utc = clock_timestamp()
  WHERE superseded.target_session_id = NEW.session_id
    AND superseded.candidate_id = NEW.candidate_id
    AND superseded.stable_selection_key = v_stable_selection_key
    AND superseded.status = 'PENDING'
    AND superseded.id <> v_registration.id;

  SELECT COALESCE(
    jsonb_agg(selected_preview.id::text ORDER BY selected_preview.row_ordinal, selected_preview.id),
    '[]'::jsonb
  )
  INTO v_selected_ids
  FROM public.banking_pay_workbench_preview_rows selected_preview
  WHERE selected_preview.session_id = NEW.session_id
    AND selected_preview.status = 'READY'
    AND selected_preview.selected IS TRUE
    AND selected_preview.selection_state = 'SELECTED';

  UPDATE public.banking_pay_workbench_sessions target_session
  SET server_selected_preview_row_ids = v_selected_ids,
      server_selected_preview_row_ids_provided = true,
      selected_row_count = jsonb_array_length(v_selected_ids),
      progress_json = COALESCE(target_session.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'selection_intent_v1',
          COALESCE(target_session.progress_json->'selection_intent_v1', '{}'::jsonb)
          || jsonb_build_object(
            'canonical_preview_lines',
            jsonb_build_object(
              'mode', 'EXPLICIT_INCLUDE',
              'server_selected_preview_row_ids_provided', true,
              'selected_preview_row_ids', v_selected_ids,
              'updated_at_utc', clock_timestamp(),
              'source', 'SESSION_REPLACEMENT_CARRY'
            )
          )
        ),
      updated_at_utc = clock_timestamp()
  WHERE target_session.id = NEW.session_id
    AND target_session.status = 'OPEN'
    AND target_session.discarded_at_utc IS NULL;

  RETURN NEW;
END;
$function$;

-- trg_candidates_tombstone()
CREATE OR REPLACE FUNCTION public.trg_candidates_tombstone()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  INSERT INTO public.candidates_tombstones (id, deleted_rev, deleted_at)
  VALUES (OLD.id, nextval('public.global_rev_seq'), now())
  ON CONFLICT (id) DO UPDATE
  SET deleted_rev = EXCLUDED.deleted_rev,
      deleted_at  = EXCLUDED.deleted_at;
  RETURN OLD;
END;
$function$;

-- trg_clients_tombstone()
CREATE OR REPLACE FUNCTION public.trg_clients_tombstone()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  INSERT INTO public.clients_tombstones (id, deleted_rev, deleted_at)
  VALUES (OLD.id, nextval('public.global_rev_seq'), now())
  ON CONFLICT (id) DO UPDATE
  SET deleted_rev = EXCLUDED.deleted_rev,
      deleted_at  = EXCLUDED.deleted_at;
  RETURN OLD;
END;
$function$;

-- trg_id_invoice_lines_ad_stmt()
CREATE OR REPLACE FUNCTION public.trg_id_invoice_lines_ad_stmt()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_invoice_id uuid;
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return null;
  end if;

  for v_invoice_id in
    select distinct orw.invoice_id
    from old_rows orw
    where orw.invoice_id is not null
  loop
    perform public.id_ledger_recompute_and_sync_invoice(v_invoice_id);
  end loop;

  return null;
end;
$function$;

-- trg_id_invoice_lines_ai_stmt()
CREATE OR REPLACE FUNCTION public.trg_id_invoice_lines_ai_stmt()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_invoice_id uuid;
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return null;
  end if;

  for v_invoice_id in
    select distinct nr.invoice_id
    from new_rows nr
    where nr.invoice_id is not null
  loop
    perform public.id_ledger_recompute_and_sync_invoice(v_invoice_id);
  end loop;

  return null;
end;
$function$;

-- trg_id_invoice_lines_au_stmt()
CREATE OR REPLACE FUNCTION public.trg_id_invoice_lines_au_stmt()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_invoice_id uuid;
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return null;
  end if;

  for v_invoice_id in
    with ids as (
      select invoice_id from new_rows where invoice_id is not null
      union
      select invoice_id from old_rows where invoice_id is not null
    )
    select distinct invoice_id from ids
  loop
    perform public.id_ledger_recompute_and_sync_invoice(v_invoice_id);
  end loop;

  return null;
end;
$function$;

-- trg_id_invoices_after_delete()
CREATE OR REPLACE FUNCTION public.trg_id_invoices_after_delete()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return old;
  end if;

  -- Force ledger current_* to zero, keep snapshots from OLD row.
  perform public.id_ledger_upsert_from_invoice_row(
    old.id,
    true,
    old.invoice_no,
    old.status::text,
    old.type::text
  );

  return old;
end;
$function$;

-- trg_id_invoices_meta_aiu()
CREATE OR REPLACE FUNCTION public.trg_id_invoices_meta_aiu()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return new;
  end if;

  perform public.id_ledger_sync_invoice_metadata(new.id);
  return new;
end;
$function$;

-- trg_invoice_document_immutability_guard()
CREATE OR REPLACE FUNCTION public.trg_invoice_document_immutability_guard()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
begin
  if tg_table_name='invoice_document_versions' then
    if tg_op='DELETE' then
      if old.status='READY' then
        raise exception using errcode='55000',
          message='IMMUTABLE_READY_DOCUMENT';
      end if;
      return old;
    end if;

    if old.status='READY'
       and(
         new.operation_id is distinct from old.operation_id
         or new.entity_type is distinct from old.entity_type
         or new.entity_id is distinct from old.entity_id
         or new.purpose is distinct from old.purpose
         or new.source_revision is distinct from old.source_revision
         or new.template_version is distinct from old.template_version
         or new.snapshot_json is distinct from old.snapshot_json
         or new.snapshot_hash is distinct from old.snapshot_hash
         or new.manifest_json is distinct from old.manifest_json
         or new.manifest_hash is distinct from old.manifest_hash
         or new.r2_key is distinct from old.r2_key
         or new.sha256 is distinct from old.sha256
         or new.size_bytes is distinct from old.size_bytes
         or new.expected_page_count is distinct from old.expected_page_count
         or new.page_count is distinct from old.page_count
         or new.core_page_count is distinct from old.core_page_count
         or new.supporting_page_count is distinct from old.supporting_page_count
         or new.ready_at_utc is distinct from old.ready_at_utc
         or new.verified_at_utc is distinct from old.verified_at_utc
      ) then
      raise exception using errcode='55000',
        message='IMMUTABLE_READY_DOCUMENT';
    end if;
    if old.status='READY' and old.purpose='FINAL_ISSUE'
       and new.status is distinct from old.status then
      raise exception using errcode='55000',
        message='IMMUTABLE_READY_FINAL_ISSUE_STATUS';
    end if;
    if old.status='READY' and old.purpose<>'FINAL_ISSUE'
       and new.status not in('READY','SUPERSEDED') then
      raise exception using errcode='55000',
        message='READY_DOCUMENT_MAY_ONLY_BE_SUPERSEDED';
    end if;
    return new;
  end if;

  if tg_table_name='invoice_document_assets' then
    if tg_op='DELETE' then
      if old.status='READY' then
        raise exception using errcode='55000',
          message='IMMUTABLE_READY_DOCUMENT_ASSET';
      end if;
      return old;
    end if;
    if new.source_kind is distinct from old.source_kind
       or new.source_id is distinct from old.source_id
       or new.source_revision is distinct from old.source_revision
       or new.original_r2_key is distinct from old.original_r2_key
       or(old.original_sha256 is not null
          and new.original_sha256 is distinct from old.original_sha256) then
      raise exception using errcode='55000',
        message='IMMUTABLE_DOCUMENT_ASSET_SOURCE_IDENTITY';
    end if;
    if old.status='READY' and(
       new.status is distinct from old.status
       or new.normalised_manifest_json is distinct from old.normalised_manifest_json
       or new.normalised_r2_key is distinct from old.normalised_r2_key
       or new.normalised_sha256 is distinct from old.normalised_sha256
       or new.normalised_manifest_hash is distinct from old.normalised_manifest_hash
       or new.normalised_size_bytes is distinct from old.normalised_size_bytes
       or new.normalised_page_count is distinct from old.normalised_page_count
       or new.ready_at_utc is distinct from old.ready_at_utc) then
      raise exception using errcode='55000',
        message='IMMUTABLE_READY_DOCUMENT_ASSET_OUTPUT';
    end if;
    return new;
  end if;

  raise exception using errcode='55000',
    message='IMMUTABILITY_GUARD_ATTACHED_TO_UNSUPPORTED_TABLE';
end;
$function$;

-- trg_invoice_document_invalidate()
CREATE OR REPLACE FUNCTION public.trg_invoice_document_invalidate()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_invoice_ids uuid[]:=array[]::uuid[];
  v_version_ids uuid[]:=array[]::uuid[];
  v_document_operation_ids uuid[]:=array[]::uuid[];
  v_issue_operation_ids uuid[]:=array[]::uuid[];
  v_delivery_operation_ids uuid[]:=array[]::uuid[];
  v_operation_ids uuid[]:=array[]::uuid[];
begin
  if tg_table_name='invoices' and tg_op='UPDATE' then
    select coalesce(array_agg(distinct n.id),array[]::uuid[]) into v_invoice_ids
    from new_rows n join old_rows o on o.id=n.id
    where n.status in('DRAFT','ON_HOLD')
      and o.status in('DRAFT','ON_HOLD')
      and n.issued_at_utc is null and o.issued_at_utc is null
      and n.paid_at_utc is null and o.paid_at_utc is null
      and(
       n.client_id is distinct from o.client_id
       or n.invoice_no is distinct from o.invoice_no
       or n.type is distinct from o.type
       or n.original_invoice_id is distinct from o.original_invoice_id
       or n.subtotal_ex_vat is distinct from o.subtotal_ex_vat
       or n.vat_amount is distinct from o.vat_amount
       or n.total_inc_vat is distinct from o.total_inc_vat
       or n.due_at_utc is distinct from o.due_at_utc
       or n.notes is distinct from o.notes
       or n.do_not_send is distinct from o.do_not_send
       or n.header_snapshot_json is distinct from o.header_snapshot_json);
  elsif tg_table_name='invoice_lines' and tg_op='INSERT' then
    select coalesce(array_agg(distinct invoice_id),array[]::uuid[]) into v_invoice_ids from new_rows;
  elsif tg_table_name='invoice_lines' and tg_op='DELETE' then
    select coalesce(array_agg(distinct invoice_id),array[]::uuid[]) into v_invoice_ids from old_rows;
  elsif tg_table_name='invoice_lines' and tg_op='UPDATE' then
    select coalesce(array_agg(distinct invoice_id),array[]::uuid[]) into v_invoice_ids
    from (
      select n.invoice_id from new_rows n join old_rows o on o.id=n.id
      where n.invoice_id is distinct from o.invoice_id
         or n.timesheet_id is distinct from o.timesheet_id
         or n.booking_id is distinct from o.booking_id
         or n.source_key is distinct from o.source_key
         or n.description is distinct from o.description
         or n.hours_day is distinct from o.hours_day
         or n.hours_night is distinct from o.hours_night
         or n.hours_sat is distinct from o.hours_sat
         or n.hours_sun is distinct from o.hours_sun
         or n.hours_bh is distinct from o.hours_bh
         or n.pay_day is distinct from o.pay_day
         or n.pay_night is distinct from o.pay_night
         or n.pay_sat is distinct from o.pay_sat
         or n.pay_sun is distinct from o.pay_sun
         or n.pay_bh is distinct from o.pay_bh
         or n.charge_day is distinct from o.charge_day
         or n.charge_night is distinct from o.charge_night
         or n.charge_sat is distinct from o.charge_sat
         or n.charge_sun is distinct from o.charge_sun
         or n.charge_bh is distinct from o.charge_bh
         or n.total_pay_ex_vat is distinct from o.total_pay_ex_vat
         or n.total_charge_ex_vat is distinct from o.total_charge_ex_vat
         or n.vat_rate_pct is distinct from o.vat_rate_pct
         or n.vat_amount is distinct from o.vat_amount
         or n.total_inc_vat is distinct from o.total_inc_vat
         or n.margin_ex_vat is distinct from o.margin_ex_vat
         or (
           n.meta_json is distinct from o.meta_json
           and not exists(
             select 1
             from public.timesheets ts
             cross join lateral (
               select coalesce(jsonb_agg(to_jsonb(ref_value) order by ref_value),'[]'::jsonb) refs
               from (
                 select nullif(btrim(coalesce(ts.reference_number,'')),'') ref_value
                 union
                  select nullif(btrim(value),'')
                  from jsonb_each_text(coalesce(ts.day_references_json,'{}'::jsonb))
                  union
                  select nullif(btrim(coalesce(seg->>'ref_num','')),'')
                  from jsonb_array_elements(
                    coalesce(ts.actual_schedule_json,'[]'::jsonb)) seg
                  where not exists(
                    select 1
                    from public.timesheets_financials mode_tf
                    where mode_tf.timesheet_id=ts.timesheet_id
                      and mode_tf.is_current
                      and upper(coalesce(
                        mode_tf.invoice_breakdown_json->>'mode',''))='SEGMENTS'
                  )
                  union
                  select nullif(btrim(coalesce(seg.value->>'ref_num','')),'')
                  from public.timesheets_financials tf
                  cross join lateral jsonb_array_elements(
                    coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg(value)
                  where tf.timesheet_id=ts.timesheet_id
                    and tf.is_current
                    and upper(coalesce(tf.invoice_breakdown_json->>'mode',''))='SEGMENTS'
                    and nullif(btrim(coalesce(
                      seg.value->>'invoice_locked_invoice_id','')),'')=n.invoice_id::text
               ) canonical_refs
               where ref_value is not null
             ) calculated
             where ts.is_current
               and ts.timesheet_id=coalesce(
                 n.timesheet_id,
                 case when coalesce(n.meta_json->>'timesheet_id','') ~
                   '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                   then (n.meta_json->>'timesheet_id')::uuid end)
               and (
                 n.timesheet_id is null
                 or not (coalesce(n.meta_json->>'timesheet_id','') ~
                   '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
                 or (n.meta_json->>'timesheet_id')::uuid=n.timesheet_id)
               and (coalesce(n.meta_json,'{}'::jsonb)
                     -'ts_reference_number'-'schedule_ref_nums'-'schedule_ref_count')
                   is not distinct from
                   (coalesce(o.meta_json,'{}'::jsonb)
                     -'ts_reference_number'-'schedule_ref_nums'-'schedule_ref_count')
               and n.meta_json->'ts_reference_number' is not distinct from
                 coalesce(to_jsonb(nullif(btrim(coalesce(ts.reference_number,'')),'')),'null'::jsonb)
               and n.meta_json->'schedule_ref_nums' is not distinct from calculated.refs
               and n.meta_json->'schedule_ref_count' is not distinct from
                 to_jsonb(jsonb_array_length(calculated.refs))
           )
         )
      union
      select o.invoice_id from old_rows o join new_rows n on n.id=o.id
      where n.invoice_id is distinct from o.invoice_id
         or n.timesheet_id is distinct from o.timesheet_id
         or n.booking_id is distinct from o.booking_id
         or n.source_key is distinct from o.source_key
         or n.description is distinct from o.description
         or n.hours_day is distinct from o.hours_day
         or n.hours_night is distinct from o.hours_night
         or n.hours_sat is distinct from o.hours_sat
         or n.hours_sun is distinct from o.hours_sun
         or n.hours_bh is distinct from o.hours_bh
         or n.pay_day is distinct from o.pay_day
         or n.pay_night is distinct from o.pay_night
         or n.pay_sat is distinct from o.pay_sat
         or n.pay_sun is distinct from o.pay_sun
         or n.pay_bh is distinct from o.pay_bh
         or n.charge_day is distinct from o.charge_day
         or n.charge_night is distinct from o.charge_night
         or n.charge_sat is distinct from o.charge_sat
         or n.charge_sun is distinct from o.charge_sun
         or n.charge_bh is distinct from o.charge_bh
         or n.total_pay_ex_vat is distinct from o.total_pay_ex_vat
         or n.total_charge_ex_vat is distinct from o.total_charge_ex_vat
         or n.vat_rate_pct is distinct from o.vat_rate_pct
         or n.vat_amount is distinct from o.vat_amount
         or n.total_inc_vat is distinct from o.total_inc_vat
         or n.margin_ex_vat is distinct from o.margin_ex_vat
         or (
           n.meta_json is distinct from o.meta_json
           and not exists(
             select 1
             from public.timesheets ts
             cross join lateral (
               select coalesce(jsonb_agg(to_jsonb(ref_value) order by ref_value),'[]'::jsonb) refs
               from (
                 select nullif(btrim(coalesce(ts.reference_number,'')),'') ref_value
                 union
                  select nullif(btrim(value),'')
                  from jsonb_each_text(coalesce(ts.day_references_json,'{}'::jsonb))
                  union
                  select nullif(btrim(coalesce(seg->>'ref_num','')),'')
                  from jsonb_array_elements(
                    coalesce(ts.actual_schedule_json,'[]'::jsonb)) seg
                  where not exists(
                    select 1
                    from public.timesheets_financials mode_tf
                    where mode_tf.timesheet_id=ts.timesheet_id
                      and mode_tf.is_current
                      and upper(coalesce(
                        mode_tf.invoice_breakdown_json->>'mode',''))='SEGMENTS'
                  )
                  union
                  select nullif(btrim(coalesce(seg.value->>'ref_num','')),'')
                  from public.timesheets_financials tf
                  cross join lateral jsonb_array_elements(
                    coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg(value)
                  where tf.timesheet_id=ts.timesheet_id
                    and tf.is_current
                    and upper(coalesce(tf.invoice_breakdown_json->>'mode',''))='SEGMENTS'
                    and nullif(btrim(coalesce(
                      seg.value->>'invoice_locked_invoice_id','')),'')=o.invoice_id::text
               ) canonical_refs
               where ref_value is not null
             ) calculated
             where ts.is_current
               and ts.timesheet_id=coalesce(
                 n.timesheet_id,
                 case when coalesce(n.meta_json->>'timesheet_id','') ~
                   '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                   then (n.meta_json->>'timesheet_id')::uuid end)
               and (
                 n.timesheet_id is null
                 or not (coalesce(n.meta_json->>'timesheet_id','') ~
                   '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
                 or (n.meta_json->>'timesheet_id')::uuid=n.timesheet_id)
               and (coalesce(n.meta_json,'{}'::jsonb)
                     -'ts_reference_number'-'schedule_ref_nums'-'schedule_ref_count')
                   is not distinct from
                   (coalesce(o.meta_json,'{}'::jsonb)
                     -'ts_reference_number'-'schedule_ref_nums'-'schedule_ref_count')
               and n.meta_json->'ts_reference_number' is not distinct from
                 coalesce(to_jsonb(nullif(btrim(coalesce(ts.reference_number,'')),'')),'null'::jsonb)
               and n.meta_json->'schedule_ref_nums' is not distinct from calculated.refs
               and n.meta_json->'schedule_ref_count' is not distinct from
                 to_jsonb(jsonb_array_length(calculated.refs))
           )
         )
    ) s;
  elsif tg_table_name='invoice_hr_source_rows' and tg_op='INSERT' then
    select coalesce(array_agg(distinct invoice_id),array[]::uuid[]) into v_invoice_ids from new_rows;
  elsif tg_table_name='invoice_hr_source_rows' and tg_op='DELETE' then
    select coalesce(array_agg(distinct invoice_id),array[]::uuid[]) into v_invoice_ids from old_rows;
  elsif tg_table_name='invoice_hr_source_rows' and tg_op='UPDATE' then
    select coalesce(array_agg(distinct coalesce(n.invoice_id,o.invoice_id)),array[]::uuid[])
      into v_invoice_ids
    from new_rows n full join old_rows o
      on n.invoice_id=o.invoice_id
     and n.source_system=o.source_system
     and n.import_id is not distinct from o.import_id
    where n.invoice_id is null or o.invoice_id is null
       or n.header_rows is distinct from o.header_rows
       or n.header_columns is distinct from o.header_columns
       or n.rows_json is distinct from o.rows_json;
  end if;

  if cardinality(v_invoice_ids)=0 then return null; end if;

  select coalesce(array_agg(i.id),array[]::uuid[]) into v_invoice_ids
  from public.invoices i
  where i.id=any(v_invoice_ids)
    and i.status in('DRAFT','ON_HOLD')
    and i.issued_at_utc is null
    and i.paid_at_utc is null;
  if cardinality(v_invoice_ids)=0 then return null; end if;

  update public.invoices i set document_revision=i.document_revision+1,
    document_state='STALE',preview_document_version_id=null,active_document_operation_id=null,
    invoice_pdf_r2_key=null,invoice_pdf_generated_at_utc=null,
    issue_state=case when i.issue_state in ('VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE')
      then 'SUPERSEDED' else i.issue_state end,last_document_error_json=null
  where i.id=any(v_invoice_ids)
    and i.status in('DRAFT','ON_HOLD')
    and i.issued_at_utc is null
    and i.paid_at_utc is null;

  with changed as materialized (
    update public.invoice_document_versions v
    set status='SUPERSEDED',superseded_at_utc=now(),
      error_json=jsonb_build_object(
        'code','INVOICE_SOURCE_CHANGED','invoice_id',v.entity_id)
    where v.entity_type='INVOICE' and v.entity_id=any(v_invoice_ids)
      and v.purpose in('DRAFT_PREVIEW','FINAL_ISSUE')
      and v.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING')
    returning v.id,v.operation_id
  )
  select coalesce(array_agg(id),array[]::uuid[]),
    coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_version_ids,v_document_operation_ids from changed;

  with changed as materialized (
    update public.invoice_operation_chunks c
    set status='SUPERSEDED',phase='SUPERSEDED',
      replacement_required=false,replaced_by_chunk_id=null,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      completed_at_utc=now(),updated_at_utc=now(),
      error_json=jsonb_build_object(
        'code','INVOICE_SOURCE_CHANGED','invoice_id',c.entity_id)
    where c.chunk_type='ISSUE_INVOICE' and c.entity_type='INVOICE'
      and c.entity_id=any(v_invoice_ids)
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning c.operation_id
  )
  select coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_issue_operation_ids from changed;

  with changed as materialized (
    update public.invoice_operation_chunks c
    set status='SUPERSEDED',phase='SUPERSEDED',
      replacement_required=false,replaced_by_chunk_id=null,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      completed_at_utc=now(),updated_at_utc=now(),
      error_json=jsonb_build_object(
        'code','INVOICE_SOURCE_CHANGED','invoice_id',c.entity_id)
    where c.chunk_type='DELIVERY_PREPARE' and c.entity_type='INVOICE'
      and c.entity_id=any(v_invoice_ids)
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning c.operation_id
  )
  select coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_delivery_operation_ids from changed;

  update public.invoice_operation_chunks c
  set status='SUPERSEDED',phase='SUPERSEDED',
    replacement_required=false,replaced_by_chunk_id=null,
    lease_owner=null,lease_token=null,lease_expires_at_utc=null,
    completed_at_utc=now(),updated_at_utc=now(),
    error_json=jsonb_build_object(
      'code','INVOICE_SOURCE_CHANGED','document_version_id',c.document_version_id)
  where c.document_version_id=any(v_version_ids)
    and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  update public.invoice_operations o
  set control_version=o.control_version+1,updated_at_utc=now(),
    change_seq=nextval('public.invoice_operation_change_seq')
  where o.id=any(v_document_operation_ids)
    and o.operation_type='BUILD_DOCUMENT'
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  v_operation_ids:=array(
    select distinct x
    from unnest(coalesce(v_document_operation_ids,array[]::uuid[])
      ||coalesce(v_issue_operation_ids,array[]::uuid[])
      ||coalesce(v_delivery_operation_ids,array[]::uuid[])) x
    where x is not null);

  update public.invoices i
  set active_issue_operation_id=null
  where i.id=any(v_invoice_ids)
    and i.active_issue_operation_id=any(v_operation_ids);

  -- A statement trigger must remain bounded to the directly affected rows.  It
  -- marks their roots dirty; the normal worker/reconciliation path performs
  -- descendant aggregation outside the write-trigger call stack.
  update public.invoice_operations o
  set progress_json=jsonb_set(coalesce(o.progress_json,'{}'::jsonb),
        '{rollup_required}','true'::jsonb,true),
    updated_at_utc=now(),
    change_seq=nextval('public.invoice_operation_change_seq')
  where o.id=any(v_operation_ids)
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');
  return null;
end;
$function$;

-- trg_invoices_set_invoice_no()
CREATE OR REPLACE FUNCTION public.trg_invoices_set_invoice_no()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin
  if new.invoice_no is null or btrim(new.invoice_no) = '' then
    new.invoice_no := public.invoice_no_next();
  end if;
  return new;
end;
$function$;

-- trg_pay_advances_set_next_due()
CREATE OR REPLACE FUNCTION public.trg_pay_advances_set_next_due()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_next date;
BEGIN
  -- If schedule_json is null or not an array, just clear next_due_week_start
  IF NEW.schedule_json IS NULL
     OR jsonb_typeof(NEW.schedule_json) <> 'array' THEN
    NEW.next_due_week_start := NULL;
    RETURN NEW;
  END IF;

  /*
    schedule_json is expected to look like:
      [
        {"week_start": "2025-09-08", "amount": -250},
        {"week_start": "2025-09-15", "amount": -250}
      ]

    We take the MIN(week_start) where amount < 0.
  */
  SELECT MIN(week_start::date)
  INTO v_next
  FROM jsonb_to_recordset(NEW.schedule_json)
       AS s(week_start text, amount numeric)
  WHERE amount IS NOT NULL
    AND amount < 0
    AND week_start IS NOT NULL
    AND week_start ~ '^\d{4}-\d{2}-\d{2}$';

  NEW.next_due_week_start := v_next;
  RETURN NEW;
END;
$function$;

-- trg_set_cli_ref()
CREATE OR REPLACE FUNCTION public.trg_set_cli_ref()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  IF TG_OP = 'INSERT' THEN
    -- Always mint a fresh CLI code on create
    NEW.cli_ref := 'CLI-' || to_char(nextval('public.client_cliref_seq'), 'FM00000');
  ELSIF TG_OP = 'UPDATE' THEN
    -- Immutable after creation
    IF NEW.cli_ref IS DISTINCT FROM OLD.cli_ref THEN
      RAISE EXCEPTION 'cli_ref is immutable once assigned';
    END IF;
  END IF;
  RETURN NEW;
END;
$function$;

-- trg_set_rev()
CREATE OR REPLACE FUNCTION public.trg_set_rev()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  NEW.rev := nextval('public.global_rev_seq');
  RETURN NEW;
END;
$function$;

-- trg_set_tms_ref()
CREATE OR REPLACE FUNCTION public.trg_set_tms_ref()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  IF TG_OP = 'INSERT' THEN
    -- Always mint a fresh CCR code on create
    NEW.tms_ref := 'CCR-' || to_char(nextval('public.candidate_tmsref_seq'), 'FM00000');
  ELSIF TG_OP = 'UPDATE' THEN
    -- Immutable after creation
    IF NEW.tms_ref IS DISTINCT FROM OLD.tms_ref THEN
      RAISE EXCEPTION 'tms_ref is immutable once assigned';
    END IF;
  END IF;
  RETURN NEW;
END;
$function$;

-- trg_set_updated_at_settings_finance_windows()
CREATE OR REPLACE FUNCTION public.trg_set_updated_at_settings_finance_windows()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
begin
  new.updated_at := now();
  return new;
end;
$function$;

-- trg_timesheet_document_invalidate()
CREATE OR REPLACE FUNCTION public.trg_timesheet_document_invalidate()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_timesheet_ids uuid[]:=array[]::uuid[];
  v_timesheet_version_ids uuid[]:=array[]::uuid[];
  v_invoice_ids uuid[]:=array[]::uuid[];
  v_invoice_version_ids uuid[]:=array[]::uuid[];
  v_timesheet_document_operation_ids uuid[]:=array[]::uuid[];
  v_invoice_document_operation_ids uuid[]:=array[]::uuid[];
  v_issue_operation_ids uuid[]:=array[]::uuid[];
  v_delivery_operation_ids uuid[]:=array[]::uuid[];
  v_operation_ids uuid[]:=array[]::uuid[];
  v_source_marker jsonb:='{}'::jsonb;
begin
  begin
    v_source_marker:=coalesce(
      nullif(current_setting('cloudtms.invoice_source_edit_marker',true),'')::jsonb,
      '{}'::jsonb);
  exception when others then
    v_source_marker:='{}'::jsonb;
  end;
  if tg_table_name='timesheets' and tg_op='UPDATE' then
    select coalesce(array_agg(distinct n.timesheet_id),array[]::uuid[])
    into v_timesheet_ids
    from new_rows n join old_rows o on o.timesheet_id=n.timesheet_id
    where(n.is_current or o.is_current) and(
      n.booking_id is distinct from o.booking_id
      or n.occupant_key_norm is distinct from o.occupant_key_norm
      or n.hospital_norm is distinct from o.hospital_norm
      or n.ward_norm is distinct from o.ward_norm
      or n.job_title_norm is distinct from o.job_title_norm
      or n.shift_label_norm is distinct from o.shift_label_norm
      or n.scheduled_start_iso is distinct from o.scheduled_start_iso
      or n.scheduled_end_iso is distinct from o.scheduled_end_iso
      or n.worked_start_iso is distinct from o.worked_start_iso
      or n.worked_end_iso is distinct from o.worked_end_iso
      or n.break_start_iso is distinct from o.break_start_iso
      or n.break_end_iso is distinct from o.break_end_iso
      or n.break_minutes is distinct from o.break_minutes
      or n.worked_minutes is distinct from o.worked_minutes
      or n.week_ending_date is distinct from o.week_ending_date
      or n.auth_name is distinct from o.auth_name
      or n.auth_job_title is distinct from o.auth_job_title
      or n.authorised_at_server is distinct from o.authorised_at_server
      or n.r2_nurse_key is distinct from o.r2_nurse_key
      or n.r2_auth_key is distinct from o.r2_auth_key
      or n.img_sha256_nurse is distinct from o.img_sha256_nurse
      or n.img_sha256_auth is distinct from o.img_sha256_auth
      or n.reference_number is distinct from o.reference_number
      or n.status is distinct from o.status
      or n.version is distinct from o.version
      or n.is_current is distinct from o.is_current
      or n.revoked_at is distinct from o.revoked_at
      or n.contract_id is distinct from o.contract_id
      or n.submission_mode is distinct from o.submission_mode
      or n.line_type is distinct from o.line_type
      or n.sheet_scope is distinct from o.sheet_scope
      or n.actual_schedule_json is distinct from o.actual_schedule_json
      or n.additional_units_week is distinct from o.additional_units_week
      or n.additional_units_per_day is distinct from o.additional_units_per_day
      or n.day_references_json is distinct from o.day_references_json
      or n.qr_token is distinct from o.qr_token
      or n.qr_payload_json is distinct from o.qr_payload_json
      or n.qr_signed_hash is distinct from o.qr_signed_hash
      or n.qr_signed_at_utc is distinct from o.qr_signed_at_utc
      or n.qr_status is distinct from o.qr_status
      or n.qr_r2_key is distinct from o.qr_r2_key
      or n.candidate_hint_text is distinct from o.candidate_hint_text
      or n.band is distinct from o.band
      or n.is_adjustment is distinct from o.is_adjustment
      or n.parent_timesheet_id is distinct from o.parent_timesheet_id
      or n.correction_id is distinct from o.correction_id
      or n.correction_kind is distinct from o.correction_kind
      or n.adjustment_origin is distinct from o.adjustment_origin);
  elsif tg_table_name='timesheets_financials' and tg_op='INSERT' then
    select coalesce(array_agg(distinct timesheet_id),array[]::uuid[])
    into v_timesheet_ids from new_rows where is_current;
  elsif tg_table_name='timesheets_financials' and tg_op='DELETE' then
    select coalesce(array_agg(distinct timesheet_id),array[]::uuid[])
    into v_timesheet_ids from old_rows where is_current;
  elsif tg_table_name='timesheets_financials' and tg_op='UPDATE' then
    select coalesce(array_agg(distinct timesheet_id),array[]::uuid[])
    into v_timesheet_ids
    from (
      select n.timesheet_id
      from new_rows n join old_rows o on o.id=n.id
      where(n.is_current or o.is_current) and(
        n.timesheet_id is distinct from o.timesheet_id
        or n.timesheet_version is distinct from o.timesheet_version
        or n.basis is distinct from o.basis
        or n.is_current is distinct from o.is_current
        or n.is_stale is distinct from o.is_stale
        or n.worked_start_iso is distinct from o.worked_start_iso
        or n.worked_end_iso is distinct from o.worked_end_iso
        or n.break_start_iso is distinct from o.break_start_iso
        or n.break_end_iso is distinct from o.break_end_iso
        or n.break_minutes is distinct from o.break_minutes
        or n.candidate_id is distinct from o.candidate_id
        or n.client_id is distinct from o.client_id
        or n.role is distinct from o.role
        or n.band is distinct from o.band
        or n.policy_snapshot_json is distinct from o.policy_snapshot_json
        or n.rate_source_refs_json is distinct from o.rate_source_refs_json
        or n.hours_day is distinct from o.hours_day
        or n.hours_night is distinct from o.hours_night
        or n.hours_sat is distinct from o.hours_sat
        or n.hours_sun is distinct from o.hours_sun
        or n.hours_bh is distinct from o.hours_bh
        or n.pay_day is distinct from o.pay_day
        or n.pay_night is distinct from o.pay_night
        or n.pay_sat is distinct from o.pay_sat
        or n.pay_sun is distinct from o.pay_sun
        or n.pay_bh is distinct from o.pay_bh
        or n.charge_day is distinct from o.charge_day
        or n.charge_night is distinct from o.charge_night
        or n.charge_sat is distinct from o.charge_sat
        or n.charge_sun is distinct from o.charge_sun
        or n.charge_bh is distinct from o.charge_bh
        or n.total_hours is distinct from o.total_hours
        or n.total_pay_ex_vat is distinct from o.total_pay_ex_vat
        or n.total_charge_ex_vat is distinct from o.total_charge_ex_vat
        or n.margin_ex_vat is distinct from o.margin_ex_vat
        or n.processing_status is distinct from o.processing_status
        or n.expenses_pay_ex_vat is distinct from o.expenses_pay_ex_vat
        or n.expenses_charge_ex_vat is distinct from o.expenses_charge_ex_vat
        or n.expenses_description is distinct from o.expenses_description
        or n.expenses_evidence_manifest is distinct from o.expenses_evidence_manifest
        or n.mileage_pay_ex_vat is distinct from o.mileage_pay_ex_vat
        or n.mileage_charge_ex_vat is distinct from o.mileage_charge_ex_vat
        or n.mileage_pay_rate is distinct from o.mileage_pay_rate
        or n.mileage_charge_rate is distinct from o.mileage_charge_rate
        or n.mileage_evidence_manifest is distinct from o.mileage_evidence_manifest
        or n.actual_schedule_json is distinct from o.actual_schedule_json
        or n.actual_minutes_by_day_json is distinct from o.actual_minutes_by_day_json
        or n.additional_units_json is distinct from o.additional_units_json
        or n.additional_pay_ex_vat is distinct from o.additional_pay_ex_vat
        or n.additional_charge_ex_vat is distinct from o.additional_charge_ex_vat
        or n.additional_margin_ex_vat is distinct from o.additional_margin_ex_vat
        or n.invoice_breakdown_json is distinct from o.invoice_breakdown_json
        or n.nhsp_import_id is distinct from o.nhsp_import_id
        or n.has_rate_issue is distinct from o.has_rate_issue
        or n.hr_crosscheck_status is distinct from o.hr_crosscheck_status
        or n.hr_crosscheck_issues is distinct from o.hr_crosscheck_issues
        or n.external_source_rows_json is distinct from o.external_source_rows_json
        or n.mileage_units is distinct from o.mileage_units
        or n.travel_pay_ex_vat is distinct from o.travel_pay_ex_vat
        or n.travel_charge_ex_vat is distinct from o.travel_charge_ex_vat
        or n.accommodation_pay_ex_vat is distinct from o.accommodation_pay_ex_vat
        or n.accommodation_charge_ex_vat is distinct from o.accommodation_charge_ex_vat
        or n.other_pay_ex_vat is distinct from o.other_pay_ex_vat
        or n.other_charge_ex_vat is distinct from o.other_charge_ex_vat)
      union
      select o.timesheet_id
      from old_rows o join new_rows n on n.id=o.id
      where n.timesheet_id is distinct from o.timesheet_id
    ) changed
    where timesheet_id is not null;

    -- A source-edit transaction may synchronise only SEGMENTS.ref_num into the
    -- current financial projection. Suppress that second invalidation solely
    -- when the row shape is ref-only and the transaction-local marker proves
    -- this exact timesheet revision already advanced. Any other TSFIN change
    -- keeps the ordinary invalidation path.
    v_timesheet_ids:=array(
      select changed_id.candidate_id
      from unnest(v_timesheet_ids) changed_id(candidate_id)
      where exists(
        select 1
        from new_rows n
        join old_rows o on o.id=n.id
        where (n.timesheet_id=changed_id.candidate_id
          or o.timesheet_id=changed_id.candidate_id)
          and not (
            (to_jsonb(n)-'invoice_breakdown_json'-'updated_at')
              is not distinct from
            (to_jsonb(o)-'invoice_breakdown_json'-'updated_at')
            and n.id=o.id
            and n.timesheet_id=o.timesheet_id
            and n.is_current and o.is_current
            and upper(coalesce(n.invoice_breakdown_json->>'mode',''))='SEGMENTS'
            and upper(coalesce(o.invoice_breakdown_json->>'mode',''))='SEGMENTS'
            and jsonb_typeof(n.invoice_breakdown_json->'segments')='array'
            and jsonb_typeof(o.invoice_breakdown_json->'segments')='array'
            and jsonb_array_length(n.invoice_breakdown_json->'segments')
                =jsonb_array_length(o.invoice_breakdown_json->'segments')
            and v_source_marker->>'txid'=txid_current()::text
            and jsonb_typeof(v_source_marker->'rows')='object'
            and (v_source_marker->'rows') ? changed_id.candidate_id::text
            and coalesce(v_source_marker#>>
                  array['rows',changed_id.candidate_id::text,'expected_revision'],'')
                  ~'^[1-9][0-9]*$'
            and exists(
              select 1
              from public.timesheets marker_ts
              where marker_ts.timesheet_id=changed_id.candidate_id
                and marker_ts.is_current
                and marker_ts.document_revision=(
                  v_source_marker#>>
                    array['rows',changed_id.candidate_id::text,'expected_revision'])::bigint
            )
            and (
              select coalesce(jsonb_agg(seg.value-'ref_num' order by seg.ord),'[]'::jsonb)
              from jsonb_array_elements(n.invoice_breakdown_json->'segments')
                with ordinality seg(value,ord)
            ) is not distinct from (
              select coalesce(jsonb_agg(seg.value-'ref_num' order by seg.ord),'[]'::jsonb)
              from jsonb_array_elements(o.invoice_breakdown_json->'segments')
                with ordinality seg(value,ord)
            )
            and not exists(
              select 1
              from jsonb_array_elements(n.invoice_breakdown_json->'segments')
                with ordinality ns(value,ord)
              join jsonb_array_elements(o.invoice_breakdown_json->'segments')
                with ordinality os(value,ord) using(ord)
              where nullif(btrim(coalesce(ns.value->>'ref_num','')),'')
                      is distinct from
                    nullif(btrim(coalesce(os.value->>'ref_num','')),'')
                and not exists(
                  select 1
                  from public.timesheets ts
                  where ts.timesheet_id=n.timesheet_id
                    and ts.is_current
                    and (
                      (
                        jsonb_typeof(ts.actual_schedule_json)='array'
                        and (
                      (
                        nullif(btrim(coalesce(ns.value->>'segment_id','')),'') is not null
                        and (
                          select count(*)
                          from jsonb_array_elements(ts.actual_schedule_json) sch(value)
                          where nullif(btrim(coalesce(sch.value->>'segment_id','')),'')
                            =nullif(btrim(coalesce(ns.value->>'segment_id','')),'')
                        )=1
                        and (
                          select nullif(btrim(coalesce(sch.value->>'ref_num','')),'')
                          from jsonb_array_elements(ts.actual_schedule_json) sch(value)
                          where nullif(btrim(coalesce(sch.value->>'segment_id','')),'')
                            =nullif(btrim(coalesce(ns.value->>'segment_id','')),'')
                          limit 1
                        ) is not distinct from
                          nullif(btrim(coalesce(ns.value->>'ref_num','')),'')
                      )
                      or (
                        (
                          nullif(btrim(coalesce(ns.value->>'segment_id','')),'') is null
                          or (
                            select count(*)
                            from jsonb_array_elements(ts.actual_schedule_json) sch(value)
                            where nullif(btrim(coalesce(sch.value->>'segment_id','')),'')
                              =nullif(btrim(coalesce(ns.value->>'segment_id','')),'')
                          )=0
                        )
                        and nullif(btrim(coalesce(
                          ns.value->>'start_utc',ns.value->>'start','')),'') is not null
                        and nullif(btrim(coalesce(
                          ns.value->>'end_utc',ns.value->>'end','')),'') is not null
                        and (
                          select count(*)
                          from jsonb_array_elements(ts.actual_schedule_json) sch(value)
                          where nullif(btrim(coalesce(
                                  sch.value->>'start_utc',sch.value->>'start','')),'')
                                =nullif(btrim(coalesce(
                                  ns.value->>'start_utc',ns.value->>'start','')),'')
                            and nullif(btrim(coalesce(
                                  sch.value->>'end_utc',sch.value->>'end','')),'')
                                =nullif(btrim(coalesce(
                                  ns.value->>'end_utc',ns.value->>'end','')),'')
                        )=1
                        and (
                          select nullif(btrim(coalesce(sch.value->>'ref_num','')),'')
                          from jsonb_array_elements(ts.actual_schedule_json) sch(value)
                          where nullif(btrim(coalesce(
                                  sch.value->>'start_utc',sch.value->>'start','')),'')
                                =nullif(btrim(coalesce(
                                  ns.value->>'start_utc',ns.value->>'start','')),'')
                            and nullif(btrim(coalesce(
                                  sch.value->>'end_utc',sch.value->>'end','')),'')
                                =nullif(btrim(coalesce(
                                  ns.value->>'end_utc',ns.value->>'end','')),'')
                          limit 1
                        ) is not distinct from
                          nullif(btrim(coalesce(ns.value->>'ref_num','')),'')
                      )
                        )
                      )
                      or (
                        v_source_marker->>'txid'=txid_current()::text
                        and (v_source_marker->'rows') ? changed_id.candidate_id::text
                      )
                    )
                )
            )
          )
      )
    );
  elsif tg_table_name='timesheet_evidence' and tg_op='INSERT' then
    select coalesce(array_agg(distinct timesheet_id),array[]::uuid[])
    into v_timesheet_ids from new_rows;
  elsif tg_table_name='timesheet_evidence' and tg_op='DELETE' then
    select coalesce(array_agg(distinct timesheet_id),array[]::uuid[])
    into v_timesheet_ids from old_rows;
  elsif tg_table_name='timesheet_evidence' and tg_op='UPDATE' then
    select coalesce(array_agg(distinct timesheet_id),array[]::uuid[])
    into v_timesheet_ids
    from (
      select n.timesheet_id
      from new_rows n join old_rows o on o.id=n.id
      where n.timesheet_id is distinct from o.timesheet_id
        or n.kind is distinct from o.kind
        or n.storage_key is distinct from o.storage_key
        or n.source_revision is distinct from o.source_revision
        or n.display_name is distinct from o.display_name
      union
      select o.timesheet_id
      from old_rows o join new_rows n on n.id=o.id
      where n.timesheet_id is distinct from o.timesheet_id
        or n.kind is distinct from o.kind
        or n.storage_key is distinct from o.storage_key
        or n.source_revision is distinct from o.source_revision
        or n.display_name is distinct from o.display_name
    ) changed
    where timesheet_id is not null;
  end if;

  if cardinality(v_timesheet_ids)=0 then return null; end if;

  update public.timesheets t
  set document_revision=t.document_revision+1,document_state='STALE',
    current_document_version_id=null,active_document_operation_id=null,
    last_document_error_json=null
  where t.timesheet_id=any(v_timesheet_ids) and t.is_current;

  with changed as materialized (
    update public.invoice_document_versions v
    set status='SUPERSEDED',superseded_at_utc=now(),
      error_json=jsonb_build_object(
        'code','TIMESHEET_SOURCE_CHANGED','timesheet_id',v.entity_id)
    where v.entity_type='TIMESHEET' and v.entity_id=any(v_timesheet_ids)
      and v.purpose='TIMESHEET'
      and v.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING')
    returning v.id,v.operation_id
  )
  select coalesce(array_agg(id),array[]::uuid[]),
    coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_timesheet_version_ids,v_timesheet_document_operation_ids
  from changed;

  update public.invoice_operation_chunks c
  set status='SUPERSEDED',phase='SUPERSEDED',
    replacement_required=false,replaced_by_chunk_id=null,
    lease_owner=null,lease_token=null,lease_expires_at_utc=null,
    completed_at_utc=now(),updated_at_utc=now(),
    error_json=jsonb_build_object(
      'code','TIMESHEET_SOURCE_CHANGED','timesheet_id',c.entity_id)
  where c.document_version_id=any(v_timesheet_version_ids)
    and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  update public.invoice_operations o
  set control_version=o.control_version+1,status='SUPERSEDED',phase='SUPERSEDED',
    completed_at_utc=now(),updated_at_utc=now(),
    error_json=jsonb_build_object('code','TIMESHEET_SOURCE_CHANGED'),
    change_seq=nextval('public.invoice_operation_change_seq')
  where o.id=any(v_timesheet_document_operation_ids)
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  select coalesce(array_agg(distinct l.invoice_id),array[]::uuid[])
  into v_invoice_ids
  from public.invoice_lines l
  join public.invoices i on i.id=l.invoice_id
  where (
      l.timesheet_id=any(v_timesheet_ids)
      or (
        l.timesheet_id is null
        and coalesce(l.meta_json->>'timesheet_id','') ~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        and (l.meta_json->>'timesheet_id')::uuid=any(v_timesheet_ids)
      )
    )
    and i.status in('DRAFT','ON_HOLD')
    and i.issued_at_utc is null
    and i.paid_at_utc is null;

  if cardinality(v_invoice_ids)>0 then
    update public.invoices i
    set document_revision=i.document_revision+1,document_state='STALE',
      preview_document_version_id=null,active_document_operation_id=null,
      issue_state=case
        when i.issue_state in('VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE')
          then 'SUPERSEDED' else i.issue_state end,
      active_issue_operation_id=null,last_document_error_json=null
    where i.id=any(v_invoice_ids);

    with changed as materialized (
      update public.invoice_document_versions v
      set status='SUPERSEDED',superseded_at_utc=now(),
        error_json=jsonb_build_object(
          'code','TIMESHEET_SOURCE_CHANGED','invoice_id',v.entity_id)
      where v.entity_type='INVOICE' and v.entity_id=any(v_invoice_ids)
        and v.purpose in('DRAFT_PREVIEW','FINAL_ISSUE')
        and v.status in(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING')
      returning v.id,v.operation_id
    )
    select coalesce(array_agg(id),array[]::uuid[]),
      coalesce(array_agg(distinct operation_id),array[]::uuid[])
    into v_invoice_version_ids,v_invoice_document_operation_ids
    from changed;

    update public.invoice_operation_chunks c
    set status='SUPERSEDED',phase='SUPERSEDED',
      replacement_required=false,replaced_by_chunk_id=null,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      completed_at_utc=now(),updated_at_utc=now(),
      error_json=jsonb_build_object(
        'code','TIMESHEET_SOURCE_CHANGED','document_version_id',c.document_version_id)
    where c.document_version_id=any(v_invoice_version_ids)
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

    update public.invoice_operations o
    set control_version=o.control_version+1,updated_at_utc=now(),
      change_seq=nextval('public.invoice_operation_change_seq')
    where o.id=any(v_invoice_document_operation_ids)
      and o.operation_type='BUILD_DOCUMENT'
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

    with changed as materialized (
      update public.invoice_operation_chunks c
      set status='SUPERSEDED',phase='SUPERSEDED',
        replacement_required=false,replaced_by_chunk_id=null,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        completed_at_utc=now(),updated_at_utc=now(),
        error_json=jsonb_build_object(
          'code','TIMESHEET_SOURCE_CHANGED','invoice_id',c.entity_id)
      where c.chunk_type='ISSUE_INVOICE' and c.entity_type='INVOICE'
        and c.entity_id=any(v_invoice_ids)
        and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      returning c.operation_id
    )
    select coalesce(array_agg(distinct operation_id),array[]::uuid[])
    into v_issue_operation_ids from changed;

    with changed as materialized (
      update public.invoice_operation_chunks c
      set status='SUPERSEDED',phase='SUPERSEDED',
        replacement_required=false,replaced_by_chunk_id=null,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        completed_at_utc=now(),updated_at_utc=now(),
        error_json=jsonb_build_object(
          'code','TIMESHEET_SOURCE_CHANGED','invoice_id',c.entity_id)
      where c.chunk_type='DELIVERY_PREPARE' and c.entity_type='INVOICE'
        and c.entity_id=any(v_invoice_ids)
        and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      returning c.operation_id
    )
    select coalesce(array_agg(distinct operation_id),array[]::uuid[])
    into v_delivery_operation_ids from changed;
  end if;

  v_operation_ids:=array(
    select distinct x
    from unnest(coalesce(v_invoice_document_operation_ids,array[]::uuid[])
      ||coalesce(v_issue_operation_ids,array[]::uuid[])
      ||coalesce(v_delivery_operation_ids,array[]::uuid[])) x
    where x is not null);

  -- Keep the statement trigger bounded to direct invalidation.  Descendant
  -- aggregation is performed later by the worker/reconciliation path.
  update public.invoice_operations o
  set progress_json=jsonb_set(coalesce(o.progress_json,'{}'::jsonb),
        '{rollup_required}','true'::jsonb,true),
    updated_at_utc=now(),
    change_seq=nextval('public.invoice_operation_change_seq')
  where o.id=any(v_operation_ids)
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  return null;
end;
$function$;

-- trg_timesheets_enqueue_pdf_regen_on_refs_change()
CREATE OR REPLACE FUNCTION public.trg_timesheets_enqueue_pdf_regen_on_refs_change()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_contract_id uuid := null;
  v_client_id uuid := null;

  v_overrideclientsettings boolean := false;
  v_no_timesheet_required boolean := false;
  v_is_nhsp boolean := false;

  v_as_of_date date := null;
begin
  -- CURRENT only
  if coalesce(new.is_current, false) is not true then
    return new;
  end if;

  -- Ignore revoked
  if new.revoked_at is not null then
    return new;
  end if;

  -- ✅ Signed QR evidence is immutable: never enqueue regen for signed markers
  if new.qr_scanned_at is not null
     or new.qr_signed_hash is not null
     or new.qr_signed_at_utc is not null
  then
    return new;
  end if;

  -- ELECTRONIC only (generated PDF path)
  if upper(coalesce(new.submission_mode::text, '')) <> 'ELECTRONIC' then
    return new;
  end if;

  -- Manual PDF overrides mean we don't own the canonical generated PDF
  if new.manual_pdf_r2_key is not null then
    return new;
  end if;

  -- If no generated PDF baseline exists, do nothing (per spec)
  if new.generated_pdf_at_utc is null then
    return new;
  end if;

  -- Determine an "as-of" date for client_settings effective_from resolution
  v_as_of_date := coalesce(
    new.week_ending_date,
    ((new.worked_start_iso at time zone 'Europe/London')::date),
    ((new.scheduled_start_iso at time zone 'Europe/London')::date),
    (now() at time zone 'Europe/London')::date
  );

  -- Resolve contract_id (timesheets.contract_id or from contract_weeks)
  v_contract_id := new.contract_id;

  if v_contract_id is null then
    select cw.contract_id
      into v_contract_id
    from public.contract_weeks cw
    where cw.timesheet_id = new.timesheet_id
    limit 1;
  end if;

  -- Contract override path (only when overrideclientsettings=true)
  if v_contract_id is not null then
    select
      coalesce(ct.overrideclientsettings, false),
      coalesce(ct.no_timesheet_required, false),
      coalesce(ct.is_nhsp, false),
      ct.client_id
    into
      v_overrideclientsettings,
      v_no_timesheet_required,
      v_is_nhsp,
      v_client_id
    from public.contracts ct
    where ct.id = v_contract_id
    limit 1;

    if coalesce(v_overrideclientsettings, false) = true then
      if coalesce(v_no_timesheet_required, false) = true
         or coalesce(v_is_nhsp, false) = true
      then
        return new;
      end if;
    end if;
  end if;

  -- If overrideclientsettings is FALSE, use the effective client_settings row (NOT bool_or across history)
  if coalesce(v_overrideclientsettings, false) = false then
    -- If client_id still unknown, fall back to current TSFIN client_id
    if v_client_id is null then
      select tf.client_id
        into v_client_id
      from public.timesheets_financials tf
      where tf.timesheet_id = new.timesheet_id
        and tf.is_current = true
      order by tf.created_at desc
      limit 1;
    end if;

    if v_client_id is not null then
      select
        coalesce(csx.no_timesheet_required, false),
        coalesce(csx.is_nhsp, false)
      into
        v_no_timesheet_required,
        v_is_nhsp
      from public.client_settings csx
      where csx.client_id = v_client_id
        and (csx.effective_from is null or csx.effective_from <= v_as_of_date)
      order by csx.effective_from desc nulls last,
               csx.updated_at desc nulls last,
               csx.created_at desc nulls last
      limit 1;

      if coalesce(v_no_timesheet_required, false) = true
         or coalesce(v_is_nhsp, false) = true
      then
        return new;
      end if;
    end if;
  end if;

  -- ✅ Enqueue-only (regen-check): no signature computation or dirty comparison in-trigger
  perform public.tspdf_enqueue_one(
    p_timesheet_id := new.timesheet_id,
    p_force_regen := false,
    p_prefer_generated := true,
    p_reason := 'READY_FOR_INVOICE'::public.ts_pdf_reason_enum
  );

  return new;
end;
$function$;

-- trg_tsfin_after_insert()
CREATE OR REPLACE FUNCTION public.trg_tsfin_after_insert()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  IF NEW.is_current = true AND NEW.authorised_at_server IS NOT NULL THEN
    PERFORM public.enqueue_ts_financials(NEW.timesheet_id, 'NEW_AUTHORISED');
  END IF;
  RETURN NEW;
END;
$function$;

-- trg_tsfin_after_update()
CREATE OR REPLACE FUNCTION public.trg_tsfin_after_update()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  -- Became authorised while current
  IF NEW.is_current = true
     AND (OLD.authorised_at_server IS NULL AND NEW.authorised_at_server IS NOT NULL)
  THEN
    PERFORM public.enqueue_ts_financials(NEW.timesheet_id, 'NEW_AUTHORISED');
  END IF;

  -- New authorised version became current
  IF (COALESCE(OLD.is_current,false) <> COALESCE(NEW.is_current,false))
     AND NEW.is_current = true
     AND NEW.authorised_at_server IS NOT NULL
  THEN
    PERFORM public.enqueue_ts_financials(NEW.timesheet_id, 'VERSION_ROTATED');
  END IF;

  -- Current version lost currency (revoked or replaced)
  IF OLD.is_current = true AND NEW.is_current = false THEN
    PERFORM public.enqueue_ts_financials(NEW.timesheet_id, 'REVOKED');
  END IF;

  RETURN NEW;
END;
$function$;

-- trg_tsfin_candidates_wakeup()
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

-- trg_tsfin_client_hospitals_wakeup()
CREATE OR REPLACE FUNCTION public.trg_tsfin_client_hospitals_wakeup()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin
  /*
    SAFETY: Do NOT enqueue timesheets whose current TSFIN snapshot is locked by invoice
    or already paid.

    UPDATED LOGIC:
    - We now enqueue BOTH authorised and unauthorised current timesheets (no authorised_at_server filter),
      so changes to client_hospitals aliases requeue TSFIN even before authorisation.
    - We still exclude revoked timesheets.
    - We still exclude locked/paid current TSFIN snapshots.

    Trigger is wired:
      trg_tsfin_client_hospitals_wakeup_aiu
        AFTER INSERT OR UPDATE OF hospital_name_norm, client_id
        ON public.client_hospitals
        EXECUTE FUNCTION public.trg_tsfin_client_hospitals_wakeup()
  */

  with alias_norms as (
    select distinct lower(btrim(x)) as norm
    from jsonb_array_elements_text(
      case
        when tg_op = 'INSERT' then coalesce(new.hospital_name_norm, '[]'::jsonb)
        else coalesce(new.hospital_name_norm, '[]'::jsonb) || coalesce(old.hospital_name_norm, '[]'::jsonb)
      end
    ) as t(x)
    where x is not null and btrim(x) <> ''
  )
  insert into public.ts_financials_outbox (timesheet_id, reason)
  select distinct ts.timesheet_id, 'CONTEXT_CHANGED'::ts_fin_reason_enum
  from public.timesheets ts
  join alias_norms a
    on ts.hospital_norm = a.norm
  left join public.timesheets_financials tf
    on tf.timesheet_id = ts.timesheet_id
   and tf.is_current = true
  where ts.is_current = true
    and ts.revoked_at is null
    and (
      tf.timesheet_id is null
      or (tf.locked_by_invoice_id is null and tf.paid_at_utc is null)
    )
  on conflict (timesheet_id, reason)
  do update set next_attempt_at = null;

  return new;
end;
$function$;

-- trg_tsfin_enqueue_tspdf_on_refs_change()
CREATE OR REPLACE FUNCTION public.trg_tsfin_enqueue_tspdf_on_refs_change()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_timesheet_id uuid := null;

  v_ts record;

  v_contract_id uuid := null;
  v_client_id uuid := null;

  v_overrideclientsettings boolean := false;
  v_no_timesheet_required boolean := false;
  v_is_nhsp boolean := false;

  v_as_of_date date := null;
begin
  -- Only for current TSFIN rows
  if coalesce(new.is_current, false) is not true then
    return new;
  end if;

  -- Avoid work if UPDATE didn't actually change the JSON (defence-in-depth)
  if tg_op = 'UPDATE' then
    if new.invoice_breakdown_json is not distinct from old.invoice_breakdown_json then
      return new;
    end if;
  end if;

  v_timesheet_id := new.timesheet_id;
  if v_timesheet_id is null then
    return new;
  end if;

  -- Load current timesheet row (needed for submission_mode + baseline + gating)
  select
    ts.timesheet_id,
    ts.is_current,
    ts.revoked_at,
    ts.submission_mode,
    ts.manual_pdf_r2_key,
    ts.generated_pdf_at_utc,
    ts.contract_id,
    ts.week_ending_date,
    ts.worked_start_iso,
    ts.scheduled_start_iso,
    ts.qr_scanned_at,
    ts.qr_signed_hash,
    ts.qr_signed_at_utc
  into v_ts
  from public.timesheets ts
  where ts.timesheet_id = v_timesheet_id
    and ts.is_current = true
  limit 1;

  if not found then
    return new;
  end if;

  -- Ignore revoked
  if v_ts.revoked_at is not null then
    return new;
  end if;

  -- ✅ Signed QR evidence is immutable: never enqueue regen for signed markers
  if v_ts.qr_scanned_at is not null
     or v_ts.qr_signed_hash is not null
     or v_ts.qr_signed_at_utc is not null
  then
    return new;
  end if;

  -- ELECTRONIC only
  if upper(coalesce(v_ts.submission_mode::text, '')) <> 'ELECTRONIC' then
    return new;
  end if;

  -- Manual override means we don't own the generated PDF
  if v_ts.manual_pdf_r2_key is not null then
    return new;
  end if;

  -- If no baseline exists, do nothing (match spec)
  if v_ts.generated_pdf_at_utc is null then
    return new;
  end if;

  -- Determine as-of date for effective client_settings lookup
  v_as_of_date := coalesce(
    v_ts.week_ending_date,
    ((v_ts.worked_start_iso at time zone 'Europe/London')::date),
    ((v_ts.scheduled_start_iso at time zone 'Europe/London')::date),
    (now() at time zone 'Europe/London')::date
  );

  -- ------------------------------------------------------------
  -- Exclusion: do not enqueue when timesheet PDF is not required
  -- (NHSP / no_timesheet_required), respecting contract overrides.
  -- Mirrors the timesheets trigger precedence.
  -- ------------------------------------------------------------
  v_contract_id := v_ts.contract_id;

  if v_contract_id is null then
    select cw.contract_id
      into v_contract_id
    from public.contract_weeks cw
    where cw.timesheet_id = v_timesheet_id
    limit 1;
  end if;

  if v_contract_id is not null then
    select
      coalesce(ct.overrideclientsettings, false),
      coalesce(ct.no_timesheet_required, false),
      coalesce(ct.is_nhsp, false),
      ct.client_id
    into
      v_overrideclientsettings,
      v_no_timesheet_required,
      v_is_nhsp,
      v_client_id
    from public.contracts ct
    where ct.id = v_contract_id
    limit 1;

    if coalesce(v_overrideclientsettings, false) = true then
      if coalesce(v_no_timesheet_required, false) = true
         or coalesce(v_is_nhsp, false) = true
      then
        return new;
      end if;
    end if;
  end if;

  -- If overrideclientsettings is FALSE, use effective client_settings row
  if coalesce(v_overrideclientsettings, false) = false then
    -- Prefer contract client_id, else TSFIN client_id
    if v_client_id is null then
      v_client_id := new.client_id;
    end if;

    if v_client_id is not null then
      select
        coalesce(csx.no_timesheet_required, false),
        coalesce(csx.is_nhsp, false)
      into
        v_no_timesheet_required,
        v_is_nhsp
      from public.client_settings csx
      where csx.client_id = v_client_id
        and (csx.effective_from is null or csx.effective_from <= v_as_of_date)
      order by csx.effective_from desc nulls last,
               csx.updated_at desc nulls last,
               csx.created_at desc nulls last
      limit 1;

      if coalesce(v_no_timesheet_required, false) = true
         or coalesce(v_is_nhsp, false) = true
      then
        return new;
      end if;
    end if;
  end if;

  -- ✅ Enqueue-only (regen-check): no signature computation or dirty comparison in-trigger
  perform public.tspdf_enqueue_one(
    p_timesheet_id := v_timesheet_id,
    p_force_regen := false,
    p_prefer_generated := true,
    p_reason := 'READY_FOR_INVOICE'::public.ts_pdf_reason_enum
  );

  return new;
end;
$function$;

-- trg_tsfin_finance_windows_erni_wakeup_all()
CREATE OR REPLACE FUNCTION public.trg_tsfin_finance_windows_erni_wakeup_all()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare
  v_now timestamptz := now();
begin
  -- Only run if relevant fields actually changed (for UPDATE)
  if tg_op = 'UPDATE' then
    if new.erni_pct is not distinct from old.erni_pct
       and new.apply_erni_to is not distinct from old.apply_erni_to
       and new.mileage_pay_defaults is not distinct from old.mileage_pay_defaults
       and new.mileage_charge_defaults is not distinct from old.mileage_charge_defaults
       and new.date_from = old.date_from
       and new.date_to is not distinct from old.date_to
    then
      return new;
    end if;
  end if;

  with picked as (
    select distinct ts.timesheet_id
    from public.timesheets ts
    left join public.timesheets_financials tf
      on tf.timesheet_id = ts.timesheet_id
     and tf.is_current = true
    where ts.is_current = true
      and ts.revoked_at is null

      -- authorised OR processed
      and (ts.authorised_at_server is not null or tf.timesheet_id is not null)

      -- safety: skip locked/paid when a current TSFIN exists
      and (
        tf.timesheet_id is null
        or (tf.locked_by_invoice_id is null and tf.paid_at_utc is null)
      )
  )
  insert into public.ts_financials_outbox
    (timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
  select
    p.timesheet_id,
    'CONTEXT_CHANGED'::public.ts_fin_reason_enum,
    0,
    null,
    null,
    v_now
  from picked p
  on conflict (timesheet_id, reason)
  do update set
    attempt_count   = 0,
    next_attempt_at = null,
    last_error      = null,
    created_at      = excluded.created_at;

  return new;
end;
$function$;

-- trg_tsfin_finance_windows_erni_wakeup()
CREATE OR REPLACE FUNCTION public.trg_tsfin_finance_windows_erni_wakeup()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare
  v_from date;
  v_to   date;
  v_end_of_year date;
begin
  -- Only care if relevant values/range actually changed on UPDATE
  if tg_op = 'UPDATE' then
    if new.erni_pct      is not distinct from old.erni_pct
       and new.apply_erni_to is not distinct from old.apply_erni_to
       and new.mileage_pay_defaults is not distinct from old.mileage_pay_defaults
       and new.mileage_charge_defaults is not distinct from old.mileage_charge_defaults
       and new.date_from = old.date_from
       and new.date_to   is not distinct from old.date_to
    then
      return new;
    end if;
  end if;

  -- End of the current year in Europe/London
  v_end_of_year :=
    (
      date_trunc('year', (now() at time zone 'Europe/London'))::date
      + interval '1 year'
      - interval '1 day'
    )::date;

  -- Affected range start: window start
  v_from := new.date_from;

  -- Affected range end: window end clamped to end-of-year
  v_to := least(coalesce(new.date_to, v_end_of_year), v_end_of_year);

  -- If window starts after end-of-year, nothing to do
  if v_from is null or v_from > v_end_of_year then
    return new;
  end if;

  -- Enqueue PAYE authorised processed unlocked timesheets in range
  perform public.enqueue_tsfin_for_authorised_range(
    p_from   => v_from,
    p_to     => v_to,
    p_reason => 'CONTEXT_CHANGED'::public.ts_fin_reason_enum,
    p_limit  => 20000
  );

  return new;
end;
$function$;

-- trg_tsfin_timesheet_validations_wakeup()
CREATE OR REPLACE FUNCTION public.trg_tsfin_timesheet_validations_wakeup()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_ts_id uuid;
  v_skip boolean := false;
begin
  v_ts_id := coalesce(new.timesheet_id, old.timesheet_id);

  if v_ts_id is null then
    return coalesce(new, old);
  end if;

  -- ------------------------------------------------------------
  -- Skip if timesheet is invoice-locked (issued/paid etc), or revoked, or missing
  -- We gate on timesheets_financials.is_current + locked_by_invoice_id
  -- and also ensure the timesheet exists and is current.
  -- ------------------------------------------------------------
  select
    (
      tf.timesheet_id is null
      or tf.is_current is not true
      or tf.locked_by_invoice_id is not null
      or ts.timesheet_id is null
      or ts.is_current is not true
      or ts.revoked_at is not null
    ) as should_skip
  into v_skip
  from public.timesheets ts
  left join public.timesheets_financials tf
    on tf.timesheet_id = ts.timesheet_id
   and tf.is_current = true
  where ts.timesheet_id = v_ts_id
  limit 1;

  if coalesce(v_skip, true) then
    return coalesce(new, old);
  end if;

  -- Priority TSFIN enqueue (idempotent on (timesheet_id, reason))
  perform public.enqueue_ts_financials_priority(
    array[v_ts_id],
    'CONTEXT_CHANGED'::public.ts_fin_reason_enum
  );

  return coalesce(new, old);
end;
$function$;

-- tsfin_dequeue_batch_ids(integer)
CREATE OR REPLACE FUNCTION public.tsfin_dequeue_batch_ids(p_limit integer DEFAULT 50)
 RETURNS TABLE(outbox_id uuid, timesheet_id uuid, reason ts_fin_reason_enum, attempt_count integer, next_attempt_at timestamp with time zone, created_at timestamp with time zone)
 LANGUAGE plpgsql
AS $function$
declare
  v_now timestamptz := now();
begin
  return query
  with picked as (
    select o.id
    from public.ts_financials_outbox o
    where o.next_attempt_at is null or o.next_attempt_at <= v_now
    order by o.next_attempt_at nulls first, o.created_at
    limit p_limit
    for update skip locked
  ),
  leased as (
    update public.ts_financials_outbox o
    set attempt_count   = o.attempt_count + 1,
        next_attempt_at = v_now + interval '5 minutes'
    where o.id in (select id from picked)
    returning o.*
  )
  select
    l.id           as outbox_id,
    l.timesheet_id as timesheet_id,
    l.reason       as reason,
    l.attempt_count,
    l.next_attempt_at,
    l.created_at
  from leased l;
end;
$function$;

-- tsfin_dequeue_batch(integer)
CREATE OR REPLACE FUNCTION public.tsfin_dequeue_batch(p_limit integer DEFAULT 50)
 RETURNS SETOF ts_financials_outbox
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
BEGIN
  /*
    Pick up to p_limit outbox rows that are due to run (or never scheduled),
    mark them as "leased" by bumping attempt_count and next_attempt_at,
    and return the updated rows to the worker.
  */

  RETURN QUERY
  WITH picked AS (
    SELECT o.id
    FROM public.ts_financials_outbox AS o
    WHERE
      o.next_attempt_at IS NULL
      OR o.next_attempt_at <= v_now
    ORDER BY o.next_attempt_at NULLS FIRST, o.created_at
    LIMIT p_limit
    FOR UPDATE SKIP LOCKED
  )
  UPDATE public.ts_financials_outbox AS o
  SET attempt_count  = o.attempt_count + 1,
      next_attempt_at = v_now + interval '5 minutes'
  WHERE o.id IN (SELECT p.id FROM picked AS p)
  RETURNING o.*;

END;
$function$;

-- tsfin_dequeue_specific(uuid[],integer)
CREATE OR REPLACE FUNCTION public.tsfin_dequeue_specific(p_timesheet_ids uuid[], p_limit integer DEFAULT NULL::integer)
 RETURNS SETOF ts_financials_outbox
 LANGUAGE plpgsql
AS $function$
declare
  v_now timestamptz := now();
  v_lim integer;
begin
  if p_timesheet_ids is null or array_length(p_timesheet_ids, 1) is null then
    return;
  end if;

  if cardinality(p_timesheet_ids) > 5000 then
    raise exception 'TSFIN_SPECIFIC_DEQUEUE_TARGET_LIMIT_EXCEEDED'
      using errcode = '22023';
  end if;

  v_lim := least(50, coalesce(p_limit, array_length(p_timesheet_ids, 1)));

  return query
  with requested as (
    select distinct input.timesheet_id
    from unnest(p_timesheet_ids) input(timesheet_id)
    where input.timesheet_id is not null
    order by input.timesheet_id
    limit v_lim
  ), expanded as (
    select r.timesheet_id from requested r
    union
    select partner.timesheet_id
    from requested r
    join public.timesheets seed
      on seed.timesheet_id=r.timesheet_id
     and seed.is_current=true
     and seed.correction_id is not null
     and upper(btrim(coalesce(seed.adjustment_origin,''))) in (
       'IMPORT_CORRECTION','IMPORT_CANCELLATION',
       'HEALTHROSTER_CHANGED_HOURS','NHSP_CHANGED_HOURS',
       'HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
     )
    join public.timesheets partner
      on partner.correction_id=seed.correction_id
     and partner.is_current=true
     and upper(btrim(coalesce(partner.adjustment_origin,''))) in (
       'IMPORT_CORRECTION','IMPORT_CANCELLATION',
       'HEALTHROSTER_CHANGED_HOURS','NHSP_CHANGED_HOURS',
       'HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
     )
  ), wanted as (
    select distinct timesheet_id from expanded
  ), picked as (
    select o.id
    from public.ts_financials_outbox o
    join wanted w on w.timesheet_id=o.timesheet_id
    where o.next_attempt_at is null or o.next_attempt_at<=v_now
    order by o.next_attempt_at nulls first,o.created_at,o.id
    limit 100
    for update skip locked
  )

  update public.ts_financials_outbox o
  set attempt_count   = o.attempt_count + 1,
      next_attempt_at = v_now + interval '5 minutes'
  where o.id in (select id from picked)
  returning o.*;
end;
$function$;

-- tsfin_follow_up_target_summary_v1(uuid[],timestamp with time zone)
CREATE OR REPLACE FUNCTION public.tsfin_follow_up_target_summary_v1(p_timesheet_ids uuid[], p_not_before_utc timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_target_count integer := 0;
  v_current_target_count integer := 0;
  v_fresh_target_count integer := 0;
  v_pending_total integer := 0;
  v_pending_ready integer := 0;
  v_latest_computed_at timestamptz := null;
  v_next_attempt_at_min timestamptz := null;
  v_now timestamptz := now();
begin
  if p_not_before_utc is null then
    raise exception 'TSFIN_FOLLOW_UP_COMMIT_FENCE_REQUIRED' using errcode = '22023';
  end if;

  if cardinality(coalesce(p_timesheet_ids, array[]::uuid[])) > 5000 then
    raise exception 'TSFIN_FOLLOW_UP_TARGET_LIMIT_EXCEEDED' using errcode = '22023';
  end if;

  select count(distinct input_id)::integer
  into v_target_count
  from unnest(coalesce(p_timesheet_ids, array[]::uuid[])) as requested(input_id)
  where input_id is not null;

  if v_target_count > 5000 then
    raise exception 'TSFIN_FOLLOW_UP_TARGET_LIMIT_EXCEEDED' using errcode = '22023';
  end if;

  if v_target_count = 0 then
    return jsonb_build_object(
      'ok', true,
      'target_count', 0,
      'current_target_count', 0,
      'fresh_target_count', 0,
      'stale_or_missing_count', 0,
      'pending_total', 0,
      'pending_ready', 0,
      'next_attempt_at_min', null,
      'latest_computed_at', null,
      'not_before_utc', p_not_before_utc,
      'all_targets_fresh', true,
      'all_targets_settled', true
    );
  end if;

  with wanted as (
    select distinct input_id as timesheet_id
    from unnest(p_timesheet_ids) as requested(input_id)
    where input_id is not null
  ),
  target_state as (
    select
      w.timesheet_id,
      (
        ts.timesheet_id is not null
        and ts.is_current is true
        and ts.revoked_at is null
      ) as is_current_target,
      (
        ts.timesheet_id is not null
        and ts.is_current is true
        and ts.revoked_at is null
        and tf.timesheet_id is not null
        and tf.is_current is true
        and coalesce(tf.is_stale, false) is false
        and tf.timesheet_version is not distinct from ts.version
        and (
          coalesce((public._ctms_import_correction_classify_v1(ts.timesheet_id)
            ->>'is_import_authoritative_correction')::boolean,false) is false
          or (
            tf.policy_snapshot_json->'correction_financials_policy_envelope'
              is not distinct from ts.candidate_hint_text->'correction_financials_policy_envelope'
          )
        )
        and coalesce(tf.computed_at_utc, tf.updated_at, tf.created_at) >= p_not_before_utc
      ) as is_fresh_target,
      coalesce(tf.computed_at_utc, tf.updated_at, tf.created_at) as computed_at_utc
    from wanted w
    left join public.timesheets ts
      on ts.timesheet_id = w.timesheet_id
    left join lateral (
      select tf0.*
      from public.timesheets_financials tf0
      where tf0.timesheet_id = w.timesheet_id
        and tf0.is_current is true
      order by
        tf0.computed_at_utc desc nulls last,
        tf0.updated_at desc nulls last,
        tf0.created_at desc nulls last,
        tf0.id desc
      limit 1
    ) tf on true
  ),
  pending_state as (
    select o.*
    from public.ts_financials_outbox o
    join wanted w on w.timesheet_id = o.timesheet_id
  )
  select
    count(*) filter (where target_state.is_current_target)::integer,
    count(*) filter (where target_state.is_fresh_target)::integer,
    max(target_state.computed_at_utc),
    (select count(*)::integer from pending_state),
    (select count(*)::integer from pending_state where next_attempt_at is null or next_attempt_at <= v_now),
    (select min(next_attempt_at) from pending_state where next_attempt_at is not null and next_attempt_at > v_now)
  into
    v_current_target_count,
    v_fresh_target_count,
    v_latest_computed_at,
    v_pending_total,
    v_pending_ready,
    v_next_attempt_at_min
  from target_state;

  return jsonb_build_object(
    'ok', true,
    'target_count', v_target_count,
    'current_target_count', coalesce(v_current_target_count, 0),
    'fresh_target_count', coalesce(v_fresh_target_count, 0),
    'stale_or_missing_count', greatest(v_target_count - coalesce(v_fresh_target_count, 0), 0),
    'pending_total', coalesce(v_pending_total, 0),
    'pending_ready', coalesce(v_pending_ready, 0),
    'next_attempt_at_min', v_next_attempt_at_min,
    'latest_computed_at', v_latest_computed_at,
    'not_before_utc', p_not_before_utc,
    'all_targets_fresh', coalesce(v_fresh_target_count, 0) = v_target_count,
    'all_targets_settled',
      coalesce(v_fresh_target_count, 0) = v_target_count
      and coalesce(v_pending_total, 0) = 0
  );
end;
$function$;

-- tsfin_load_context_batch(uuid[])
CREATE OR REPLACE FUNCTION public.tsfin_load_context_batch(p_timesheet_ids uuid[])
 RETURNS TABLE(effective_timesheet_id uuid, out_timesheet jsonb, out_cur_fin jsonb, out_candidate jsonb, out_umbrella jsonb, out_client_id uuid, out_effective_flags jsonb, out_policy jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
with input_ids as (
  select distinct unnest(p_timesheet_ids) as input_timesheet_id
  where p_timesheet_ids is not null
),
t_in as (
  select
    i.input_timesheet_id,
    t0.*
  from input_ids i
  join public.timesheets t0
    on t0.timesheet_id = i.input_timesheet_id
),
t_eff_id as (
  select
    t_in.input_timesheet_id,
    coalesce(tc.timesheet_id, t_in.timesheet_id) as effective_timesheet_id
  from t_in
  left join public.timesheets tc
    on tc.booking_id = t_in.booking_id
   and tc.is_current = true
),
t_eff as (
  select
    e.input_timesheet_id,
    e.effective_timesheet_id,
    te.*
  from t_eff_id e
  join public.timesheets te
    on te.timesheet_id = e.effective_timesheet_id
),
-- settings_defaults is NON-FINANCE ONLY now (explicit select list; no select *)
def as (
  select
    timezone_id,
    day_start, day_end,
    night_start, night_end,
    sat_start, sat_end,
    sun_start, sun_end,
    bh_start, bh_end,
    bh_list,
    hr_attach_to_invoice,
    ts_attach_to_invoice
  from public.settings_defaults
  where id = 1
  limit 1
),
base as (
  select
    te.effective_timesheet_id,
    correction_policy.envelope_json as correction_policy_envelope,
    correction_policy.leg_json as correction_policy_leg,

    -- TIME anchor date for client_settings.effective_from selection:
    -- DAILY: worked_start_iso local date
    -- WEEKLY: week_ending_date (fallback)
    coalesce(
      case
        when te.worked_start_iso is not null
          then (te.worked_start_iso at time zone 'Europe/London')::date
        else null
      end,
      te.week_ending_date::date
    ) as time_anchor_date,

    -- FINANCE anchor date for finance windows:
    -- authorised_at_server local date if present, else "today" local date
    coalesce(
      case
        when nullif(correction_policy.leg_json #>> '{tsfin_policy,pay_policy_date}', '') is not null
          then (correction_policy.leg_json #>> '{tsfin_policy,pay_policy_date}')::date
        else null
      end,
      case
        when te.authorised_at_server is not null
          then (te.authorised_at_server at time zone 'Europe/London')::date
        else null
      end,
      (now() at time zone 'Europe/London')::date
    ) as finance_anchor_date,

    -- ✅ Ensure new correction fields are always present in out_timesheet (even if null)
    (to_jsonb(te)
      || jsonb_build_object(
        'correction_id', te.correction_id,
        'correction_kind', te.correction_kind,
        'adjustment_origin', te.adjustment_origin
      )
    ) as out_timesheet,

    -- ✅ This will now include travel_/accommodation_/other_ columns automatically
    to_jsonb(tf) as out_cur_fin,

    to_jsonb(c)  as out_candidate,
    to_jsonb(u)  as out_umbrella,

    cid.client_id as out_client_id,

    -- ✅ Expand “effective flags” for weekly consumers (still source-of-truth = v_timesheets_summary)
    jsonb_build_object(
      'route_type',                    v.route_type,

      'contract_id',                   v.contract_id,
      'contract_week_id',              v.contract_week_id,
      'contract_week_ending_date',     v.contract_week_ending_date,
      'basis',                         v.basis,

      'client_requires_hr',            v.client_requires_hr,
      'client_autoprocess_hr',         v.client_autoprocess_hr,
      'client_no_timesheet_required',  v.client_no_timesheet_required,
      'client_is_nhsp',                v.client_is_nhsp,

      'require_reference_to_pay',      v.require_reference_to_pay,
      'require_reference_to_invoice',  v.require_reference_to_invoice,

      'client_hr_validation_required', v.client_hr_validation_required,
      'client_ts_reference_required',  v.client_ts_reference_required,
      'client_pay_reference_required', v.client_pay_reference_required,
      'client_invoice_reference_required', v.client_invoice_reference_required,

      'pay_method',                    v.pay_method,
      'processing_status',             v.processing_status,
      'authorised_at_server',          v.authorised_at_server,

      -- ✅ NEW: computed here (because v_timesheets_summary does not expose hr_validation_required_for_invoice)
      'hr_validation_required_for_invoice',
        (
          v.timesheet_id is not null
          and coalesce(v.client_hr_validation_required, false) = true
          and coalesce(v.client_no_timesheet_required, false) = false
          and coalesce(v.total_hours, tf.total_hours, 0::numeric) > 0::numeric
        ),

      -- ✅ NEW: pass through validation status for TSFIN recompute gating
      'validation_status', v.validation_status
    ) as out_effective_flags,

    cs as cs_row,
    def as def_row,
    ct as ct_row
  from t_eff te
  cross join def

  left join lateral (
    select
      public._ctms_correction_policy_envelope_read_v1(te.effective_timesheet_id) as envelope_json,
      public._ctms_correction_policy_leg_read_v1(te.effective_timesheet_id) as leg_json
  ) correction_policy on true

  left join public.timesheets_financials tf
    on tf.timesheet_id = te.effective_timesheet_id
   and tf.is_current = true

  left join lateral (
    with candidate_resolution as (
      select
        case
          when upper(coalesce(te.sheet_scope::text, '')) = 'DAILY'
           and upper(coalesce(te.submission_mode::text, '')) = 'MANUAL'
           and coalesce(te.candidate_hint_text->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            then (te.candidate_hint_text->>'candidate_id')::uuid
          else null::uuid
        end as manual_candidate_hint_id,
        case
          when upper(coalesce(te.sheet_scope::text, '')) = 'DAILY'
           and upper(coalesce(te.submission_mode::text, '')) = 'MANUAL'
            then tf.candidate_id
          else null::uuid
        end as manual_current_candidate_id
    ),
    candidate_matches as (
      select
        c1.id as candidate_id,
        0 as match_priority,
        c1.updated_at as match_updated_at,
        c1.created_at as match_created_at
      from public.candidates c1
      cross join candidate_resolution cr
      where cr.manual_candidate_hint_id is not null
        and c1.id = cr.manual_candidate_hint_id

      union all

      select
        c1.id as candidate_id,
        1 as match_priority,
        c1.updated_at as match_updated_at,
        c1.created_at as match_created_at
      from public.candidates c1
      cross join candidate_resolution cr
      where cr.manual_current_candidate_id is not null
        and c1.id = cr.manual_current_candidate_id
        and (
          cr.manual_candidate_hint_id is null
          or c1.id <> cr.manual_candidate_hint_id
        )

      union all

      select
        c1.id as candidate_id,
        case
          when c1.key_norm = te.occupant_key_norm then 2
          else 3
        end as match_priority,
        c1.updated_at as match_updated_at,
        c1.created_at as match_created_at
      from public.candidates c1
      where
        c1.key_norm = te.occupant_key_norm
        or (
          te.occupant_key_norm is not null
          and c1.nhsp_hr_name_aliases @> to_jsonb(array[te.occupant_key_norm]::text[])
        )
    )
    select candidate_matches.candidate_id
    from candidate_matches
    order by
      candidate_matches.match_priority,
      candidate_matches.match_updated_at desc nulls last,
      candidate_matches.match_created_at desc nulls last
    limit 1
  ) candidate_pick on true

  left join public.candidates c
    on c.id = candidate_pick.candidate_id

  left join public.umbrellas u
    on (c.umbrella_id is not null and u.id = c.umbrella_id)

  left join public.v_timesheets_summary v
    on v.timesheet_id = te.effective_timesheet_id

  left join public.contracts ct
    on ct.id = te.contract_id

  left join lateral (
    select ch.client_id
    from public.client_hospitals ch
    where te.hospital_norm is not null
      and ch.hospital_name_norm @> jsonb_build_array(te.hospital_norm)
    limit 1
  ) ch on true

  -- ✅ Resolve a single client_id for this timesheet (works for WEEKLY and DAILY)
  left join lateral (
    select coalesce(v.client_id, tf.client_id, ch.client_id) as client_id
  ) cid on true

  -- client_settings chosen by TIME anchor (work date / week ending)
  left join lateral (
    select cs1.*
    from public.client_settings cs1
    where cid.client_id is not null
      and cs1.client_id = cid.client_id
    order by
      case
        when (coalesce(
          case when te.worked_start_iso is not null then (te.worked_start_iso at time zone 'Europe/London')::date end,
          te.week_ending_date::date
        )) is null then 0

        when cs1.effective_from is not null
         and cs1.effective_from <= coalesce(
           case when te.worked_start_iso is not null then (te.worked_start_iso at time zone 'Europe/London')::date end,
           te.week_ending_date::date
         ) then 0

        when cs1.effective_from is null then 1

        else 2
      end,
      cs1.effective_from desc nulls last,
      cs1.created_at desc
    limit 1
  ) cs on true
),
ctx as (
  select
    b.effective_timesheet_id,

    b.out_timesheet,
    b.out_cur_fin,
    b.out_candidate,
    b.out_umbrella,
    b.out_client_id,
    b.out_effective_flags,

    -- Finance window row in-scope for FINANCE anchor date (authorised date else today)
    jsonb_build_object(
      'timezone_id', coalesce((b.cs_row).timezone_id, (b.def_row).timezone_id, 'Europe/London'),

      'day_start',   coalesce(to_char((b.cs_row).day_start,   'HH24:MI:SS'), to_char((b.def_row).day_start,   'HH24:MI:SS'), '06:00:00'),
      'day_end',     coalesce(to_char((b.cs_row).day_end,     'HH24:MI:SS'), to_char((b.def_row).day_end,     'HH24:MI:SS'), '20:00:00'),
      'night_start', coalesce(to_char((b.cs_row).night_start, 'HH24:MI:SS'), to_char((b.def_row).night_start, 'HH24:MI:SS'), '20:00:00'),
      'night_end',   coalesce(to_char((b.cs_row).night_end,   'HH24:MI:SS'), to_char((b.def_row).night_end,   'HH24:MI:SS'), '06:00:00'),

      'sat_start',   coalesce(to_char((b.cs_row).sat_start, 'HH24:MI:SS'), to_char((b.def_row).sat_start, 'HH24:MI:SS'), '00:00:00'),
      'sat_end',     coalesce(to_char((b.cs_row).sat_end,   'HH24:MI:SS'), to_char((b.def_row).sat_end,   'HH24:MI:SS'), '00:00:00'),
      'sun_start',   coalesce(to_char((b.cs_row).sun_start, 'HH24:MI:SS'), to_char((b.def_row).sun_start, 'HH24:MI:SS'), '00:00:00'),
      'sun_end',     coalesce(to_char((b.cs_row).sun_end,   'HH24:MI:SS'), to_char((b.def_row).sun_end,   'HH24:MI:SS'), '00:00:00'),

      'bh_start',    coalesce(to_char((b.cs_row).bh_start, 'HH24:MI:SS'), to_char((b.def_row).bh_start, 'HH24:MI:SS'), '00:00:00'),
      'bh_end',      coalesce(to_char((b.cs_row).bh_end,   'HH24:MI:SS'), to_char((b.def_row).bh_end,   'HH24:MI:SS'), '00:00:00'),

      'vat_rate_pct',
      coalesce(nullif(b.correction_policy_leg #>> '{tsfin_policy,applied_pay_vat_rate_pct}', '')::numeric, (b.cs_row).vat_rate_pct, fin.vat_rate_pct, 20::numeric),

      'pay_vat_rate_pct',
      coalesce(nullif(b.correction_policy_leg #>> '{tsfin_policy,applied_pay_vat_rate_pct}', '')::numeric, (b.cs_row).vat_rate_pct, fin.vat_rate_pct, 20::numeric),

      'correction_financials_policy_envelope', b.correction_policy_envelope,
      'correction_financials_policy_envelope_fingerprint', b.correction_policy_leg ->> 'envelope_fingerprint',
      'correction_leg_fingerprint', b.correction_policy_leg ->> 'leg_fingerprint',
      'correction_tsfin_policy', b.correction_policy_leg -> 'tsfin_policy',
      'correction_tsfin_policy_fingerprint', b.correction_policy_leg #>> '{tsfin_policy,tsfin_policy_fingerprint}',
      'correction_invoice_policy', b.correction_policy_leg -> 'invoice_policy',
      'correction_invoice_policy_fingerprint', b.correction_policy_leg #>> '{invoice_policy,invoice_policy_fingerprint}',
      'correction_invoice_stream', b.correction_policy_leg #>> '{invoice_policy,invoice_stream}',
      'correction_pay_policy_date', b.correction_policy_leg #>> '{tsfin_policy,pay_policy_date}',
      'correction_invoice_policy_date', b.correction_policy_leg #>> '{invoice_policy,invoice_policy_date}',

      'holiday_pay_pct',
      coalesce((b.cs_row).holiday_pay_pct, fin.holiday_pay_pct, 12.07::numeric),

      'erni_pct',
      coalesce(nullif(b.correction_policy_leg #>> '{tsfin_policy,erni_pct}', '')::numeric, fin.erni_pct, 13.8::numeric),

      'mileage_pay_defaults',
      fin.mileage_pay_defaults,

      'mileage_charge_defaults',
      fin.mileage_charge_defaults,

      'apply_holiday_to',
      coalesce((b.cs_row).apply_holiday_to, fin.apply_holiday_to, 'PAYE_ONLY'),

      'apply_erni_to',
      coalesce(nullif(b.correction_policy_leg #>> '{tsfin_policy,apply_erni_to}', ''), (b.cs_row).apply_erni_to, fin.apply_erni_to, 'PAYE_ONLY'),

      'margin_includes',
      jsonb_build_object(
        'expenses',
        coalesce(
          nullif((
            case
              when (b.cs_row).margin_includes is null then null
              when jsonb_typeof((b.cs_row).margin_includes) = 'object' then (b.cs_row).margin_includes
              when jsonb_typeof((b.cs_row).margin_includes) = 'string'
                   and ((b.cs_row).margin_includes #>> '{}') ~ '^\s*\{'
                then ((b.cs_row).margin_includes #>> '{}')::jsonb
              else null
            end
          ) ->> 'expenses', '')::boolean,

          nullif((
            case
              when fin.margin_includes is null then null
              when jsonb_typeof(fin.margin_includes) = 'object' then fin.margin_includes
              when jsonb_typeof(fin.margin_includes) = 'string'
                   and (fin.margin_includes #>> '{}') ~ '^\s*\{'
                then (fin.margin_includes #>> '{}')::jsonb
              else null
            end
          ) ->> 'expenses', '')::boolean,

          false
        ),

        'mileage',
        coalesce(
          nullif((
            case
              when (b.cs_row).margin_includes is null then null
              when jsonb_typeof((b.cs_row).margin_includes) = 'object' then (b.cs_row).margin_includes
              when jsonb_typeof((b.cs_row).margin_includes) = 'string'
                   and ((b.cs_row).margin_includes #>> '{}') ~ '^\s*\{'
                then ((b.cs_row).margin_includes #>> '{}')::jsonb
              else null
            end
          ) ->> 'mileage', '')::boolean,

          nullif((
            case
              when fin.margin_includes is null then null
              when jsonb_typeof(fin.margin_includes) = 'object' then fin.margin_includes
              when jsonb_typeof(fin.margin_includes) = 'string'
                   and (fin.margin_includes #>> '{}') ~ '^\s*\{'
                then (fin.margin_includes #>> '{}')::jsonb
              else null
            end
          ) ->> 'mileage', '')::boolean,

          false
        )
      ),

      'bh_list',
      case
        when (b.def_row).bh_list is null then '[]'::jsonb
        when jsonb_typeof((b.def_row).bh_list) = 'array' then (b.def_row).bh_list
        when jsonb_typeof((b.def_row).bh_list) = 'string'
             and ((b.def_row).bh_list #>> '{}') ~ '^\s*\['
          then ((b.def_row).bh_list #>> '{}')::jsonb
        else '[]'::jsonb
      end,

      'hr_attach_to_invoice', coalesce((b.ct_row).hr_attach_to_invoice, (b.cs_row).hr_attach_to_invoice, (b.def_row).hr_attach_to_invoice, true),
      'ts_attach_to_invoice', coalesce((b.ct_row).ts_attach_to_invoice, (b.cs_row).ts_attach_to_invoice, (b.def_row).ts_attach_to_invoice, true),

      'week_ending_weekday',     coalesce((b.cs_row).week_ending_weekday, 0),
      'default_submission_mode', coalesce((b.cs_row).default_submission_mode, 'ELECTRONIC')
    ) as out_policy
  from base b
  left join lateral public.settings_finance_pick(p_date => b.finance_anchor_date) fin on true
)
select
  effective_timesheet_id,
  out_timesheet,
  out_cur_fin,
  out_candidate,
  out_umbrella,
  out_client_id,
  out_effective_flags,
  out_policy
from ctx;
$function$;

-- tsfin_load_nhsp_shifts_batch(uuid[])
CREATE OR REPLACE FUNCTION public.tsfin_load_nhsp_shifts_batch(p_timesheet_ids uuid[])
 RETURNS TABLE(timesheet_id uuid, shifts jsonb)
 LANGUAGE sql
 STABLE
AS $function$
  with input_ids as (
    select distinct unnest(p_timesheet_ids) as timesheet_id
    where p_timesheet_ids is not null
  )
  select
    i.timesheet_id,
    coalesce(
      jsonb_agg(to_jsonb(s) order by s.work_date asc, s.start_utc asc, s.id asc)
        filter (where s.id is not null),
      '[]'::jsonb
    ) as shifts
  from input_ids i
  left join public.nhsp_shifts s
    on s.timesheet_id = i.timesheet_id
   and s.cancelled_at_utc is null
  group by i.timesheet_id
  order by i.timesheet_id;
$function$;

-- tsfin_load_weekly_context_batch(uuid[])
CREATE OR REPLACE FUNCTION public.tsfin_load_weekly_context_batch(p_timesheet_ids uuid[])
 RETURNS TABLE(timesheet_id uuid, out_cw jsonb, out_contract jsonb)
 LANGUAGE sql
 STABLE
AS $function$
with input_ids as (
  select distinct unnest(p_timesheet_ids) as input_timesheet_id
  where p_timesheet_ids is not null
),

t_in as (
  select i.input_timesheet_id, t0.*
  from input_ids i
  join public.timesheets t0
    on t0.timesheet_id = i.input_timesheet_id
),

t_eff_id as (
  select
    t_in.input_timesheet_id,

    -- Effective/current mapping:
    -- 1) Prefer explicit current row for booking_id
    -- 2) If none exists, fall back to highest version for that booking_id
    -- 3) Else fall back to the input id itself
    coalesce(tc.timesheet_id, tmax.timesheet_id, t_in.timesheet_id) as effective_timesheet_id

  from t_in

  left join public.timesheets tc
    on tc.booking_id = t_in.booking_id
   and tc.is_current = true

  left join lateral (
    select t1.timesheet_id
    from public.timesheets t1
    where t_in.booking_id is not null
      and t1.booking_id = t_in.booking_id
    order by
      t1.version desc nulls last,
      t1.updated_at desc nulls last,
      t1.created_at desc nulls last
    limit 1
  ) tmax on true
),

-- one row per effective/current timesheet id
eff_ids as (
  select distinct effective_timesheet_id
  from t_eff_id
  where effective_timesheet_id is not null
),

t_eff as (
  select te.*
  from eff_ids e
  join public.timesheets te
    on te.timesheet_id = e.effective_timesheet_id
),

v as (
  select
    te.timesheet_id,
    te.booking_id,
    te.contract_id as ts_contract_id,
    vts.contract_week_id,
    vts.contract_id
  from t_eff te
  left join public.v_timesheets_summary vts
    on vts.timesheet_id = te.timesheet_id
),

cw_pick as (
  select
    v.timesheet_id,
    cw.id as cw_id,
    cw.contract_id as cw_contract_id,
    to_jsonb(cw) as cw_json
  from v
  left join lateral (
    select cw1.*
    from public.contract_weeks cw1
    where
      -- Preferred: use the view’s resolved contract_week_id
      (v.contract_week_id is not null and cw1.id = v.contract_week_id)

      or
      (
        v.contract_week_id is null
        and
        (
          -- Hardened fallback: cw may point at an older timesheet version.
          -- Match cw.timesheet_id to ANY timesheet_id in this booking_id series.
          (v.booking_id is not null and exists (
            select 1
            from public.timesheets tx
            where tx.booking_id = v.booking_id
              and tx.timesheet_id = cw1.timesheet_id
          ))

          -- If booking_id is absent, last-resort: exact timesheet id
          or (v.booking_id is null and cw1.timesheet_id = v.timesheet_id)
        )
      )
    order by
      case when v.contract_week_id is not null and cw1.id = v.contract_week_id then 0 else 1 end,
      cw1.updated_at desc nulls last,
      cw1.created_at desc nulls last
    limit 1
  ) cw on true
),

contract_pick as (
  select
    cwp.timesheet_id,
    c.id as contract_id,
    to_jsonb(c) as contract_json
  from cw_pick cwp
  join v on v.timesheet_id = cwp.timesheet_id
  left join lateral (
    select c1.*
    from public.contracts c1
    where c1.id = coalesce(v.contract_id, cwp.cw_contract_id, v.ts_contract_id)
    limit 1
  ) c on true
)

select
  v.timesheet_id,
  case when cwp.cw_id is null then null else cwp.cw_json end as out_cw,
  case when cp.contract_id is null then null else cp.contract_json end as out_contract
from v
left join cw_pick cwp on cwp.timesheet_id = v.timesheet_id
left join contract_pick cp on cp.timesheet_id = v.timesheet_id;
$function$;

-- tsfin_mark_revoked(uuid)
CREATE OR REPLACE FUNCTION public.tsfin_mark_revoked(p_timesheet_id uuid)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
  -- Guard: do not revoke the current snapshot if it is invoice-locked.
  -- (Same reasoning as tsfin_prepare_write: SEGMENTS can be partially/fully invoiced while summary lock is NULL.)
  IF EXISTS (
    SELECT 1
    FROM public.timesheets_financials tf
    WHERE tf.timesheet_id = p_timesheet_id
      AND tf.is_current = true
      AND (
        tf.locked_by_invoice_id IS NOT NULL
        OR (
          upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
          AND EXISTS (
            SELECT 1
            FROM jsonb_array_elements(
              CASE
                WHEN tf.invoice_breakdown_json IS NOT NULL
                  AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'
                  AND jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
                THEN tf.invoice_breakdown_json->'segments'
                ELSE '[]'::jsonb
              END
            ) AS seg(value)
            WHERE nullif(btrim(coalesce(seg.value->>'invoice_locked_invoice_id','')), '') IS NOT NULL
          )
        )
      )
  ) THEN
    RAISE EXCEPTION 'TSFIN_LOCKED';
  END IF;

  UPDATE public.timesheets_financials tfu
  SET is_current = false
  WHERE tfu.timesheet_id = p_timesheet_id
    AND tfu.is_current = true;
END;
$function$;

-- tsfin_outbox_pending_summary(uuid[])
CREATE OR REPLACE FUNCTION public.tsfin_outbox_pending_summary(p_timesheet_ids uuid[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_total int := 0;
  v_ready int := 0;
  v_next_attempt_at_min timestamptz := null;
  v_latest_created_at timestamptz := null;
begin
  if p_timesheet_ids is null or array_length(p_timesheet_ids, 1) is null then
    return jsonb_build_object(
      'total', 0,
      'ready', 0,
      'next_attempt_at_min', null,
      'latest_created_at', null,
      'now', v_now
    );
  end if;

  with wanted as (
    select distinct unnest(p_timesheet_ids) as timesheet_id
  ),
  o as (
    select o.*
    from public.ts_financials_outbox o
    join wanted w on w.timesheet_id = o.timesheet_id
  )
  select
    count(*)::int as total,
    coalesce(sum(case when (o.next_attempt_at is null or o.next_attempt_at <= v_now) then 1 else 0 end), 0)::int as ready,
    min(o.next_attempt_at) filter (where o.next_attempt_at is not null and o.next_attempt_at > v_now) as next_attempt_at_min,
    max(o.created_at) as latest_created_at
  into
    v_total,
    v_ready,
    v_next_attempt_at_min,
    v_latest_created_at
  from o;

  return jsonb_build_object(
    'total', coalesce(v_total, 0),
    'ready', coalesce(v_ready, 0),
    'next_attempt_at_min', v_next_attempt_at_min,
    'latest_created_at', v_latest_created_at,
    'now', v_now
  );
end;
$function$;

-- tsfin_prepare_write(uuid)
CREATE OR REPLACE FUNCTION public.tsfin_prepare_write(p_timesheet_id uuid)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
  -- Guard: refuse writes if the current snapshot is paid OR whole-timesheet locked by an invoice.
  -- IMPORTANT: Do NOT block just because some SEGMENTS are invoice-locked; partial recompute is allowed.
  IF EXISTS (
    SELECT 1
    FROM public.timesheets_financials tf
    WHERE tf.timesheet_id = p_timesheet_id
      AND tf.is_current = true
      AND (
        tf.paid_at_utc IS NOT NULL
        OR tf.locked_by_invoice_id IS NOT NULL
      )
  ) THEN
    RAISE EXCEPTION 'TSFIN_LOCKED';
  END IF;

  -- Make any current snapshots non-current (we keep history)
  UPDATE public.timesheets_financials tfu
  SET is_current = false
  WHERE tfu.timesheet_id = p_timesheet_id
    AND tfu.is_current = true;
END;
$function$;

-- tsfin_repair_merge_segments_locked(uuid,jsonb,uuid,text)
CREATE OR REPLACE FUNCTION public.tsfin_repair_merge_segments_locked(p_timesheet_id uuid, p_new_segments jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_correlation_id text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_tsfin_id uuid;
  v_ib jsonb;
  v_new_ib jsonb;

  v_old_invalid int := 0;
  v_new_invalid int := 0;

  -- Locked maps (preserve these objects exactly)
  v_locked_nhsp_map jsonb := '{}'::jsonb;  -- nhsp_shift_id -> locked seg json
  v_locked_ext_map  jsonb := '{}'::jsonb;  -- external_row_key -> locked seg json
  v_locked_sig_map  jsonb := '{}'::jsonb;  -- sig_key -> locked seg json

  -- Locked key -> locked segment_id (for error reporting)
  v_locked_nhsp_idmap jsonb := '{}'::jsonb;
  v_locked_ext_idmap  jsonb := '{}'::jsonb;
  v_locked_sig_idmap  jsonb := '{}'::jsonb;

  -- Fresh maps (built from p_new_segments)
  v_fresh_nhsp_map jsonb := '{}'::jsonb;
  v_fresh_ext_map  jsonb := '{}'::jsonb;
  v_fresh_sig_map  jsonb := '{}'::jsonb;

  -- NEW: preserve delay overrides from the current TSFIN JSON (even for unlocked segments)
  v_delay_by_segment_id jsonb := '{}'::jsonb; -- segment_id -> invoice_target_week_start (jsonb string)
  v_delay_text text := null;
  v_delays_reapplied int := 0;

  v_need_sig boolean := false;

  v_missing_locked_ids text[] := array[]::text[];

  v_preserved_locked_count int := 0;
  v_merged_len int := 0;

  e jsonb;

  sid text;
  lock_invoice_id text;

  v_date text;
  v_ref  text;
  v_start_ts timestamptz;
  v_start_norm text;
  v_sig_key text;

  v_nhsp_id text;
  v_ext_key text;

  chosen jsonb;

  v_merged_segments jsonb := '[]'::jsonb;

  v_rows_updated int := 0;

  -- scratch
  k text;
  v_locked_count int := 0;

  -- uuid regex test
  is_uuid boolean;

  -- =====================================================
  -- DEBUG (invoice_debug): single audit row per RPC call
  -- =====================================================
  v_invoice_debug boolean := false;
  v_dbg_started_at timestamptz := now();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_sqlstate text := null;
  v_dbg_error text := null;
  v_dbg_stats jsonb := '{}'::jsonb;

  -- corruption / diagnostics
  v_old_segments_len int := 0;
  v_old_non_object_count int := 0;
  v_old_missing_segment_id_count int := 0;
  v_old_locked_count int := 0;
  v_old_unlocked_count int := 0;
  v_old_unlocked_delayed_count int := 0;

  v_new_segments_len int := 0;

  v_old_invalid_samples jsonb := '[]'::jsonb;
  v_old_invalid_samples_cap int := 10;

  r record;

  -- helper payload for early returns
  v_ret jsonb;

  -- ✅ preserve additional/expenses and keep totals consistent
  v_add_pay numeric := 0;
  v_add_charge numeric := 0;

  -- ✅ fallback if v_ib.additional is missing/invalid (derive non-seg totals from stored totals - original segment sums)
  v_tf_total_pay numeric := 0;
  v_tf_total_charge numeric := 0;
  v_old_seg_pay_sum numeric := 0;
  v_old_seg_charge_sum numeric := 0;
  v_add_ok boolean := false;
  v_add_pay_text text := null;
  v_add_charge_text text := null;

  v_seg_pay_sum numeric := 0;
  v_seg_charge_sum numeric := 0;
  v_total_pay numeric := 0;
  v_total_charge numeric := 0;

  -- ✅ ERNI-aware margin (PAYE only; wage pay only; never expenses/mileage)
  v_policy jsonb := '{}'::jsonb;
  v_pay_method_text text := '';
  v_additional_pay_ex_vat numeric := 0;
  v_expenses_pay_ex_vat numeric := 0;
  v_mileage_pay_ex_vat numeric := 0;

  v_apply_to text := 'PAYE_ONLY';
  v_erni_pct_raw numeric := 0;
  v_erni_mult numeric := 1;
  v_pay_method_u text := '';
  v_erni_applies boolean := false;

  v_wage_pay numeric := 0;
  v_reimb_pay numeric := 0;
  v_wage_pay_cost numeric := 0;
  v_pay_cost numeric := 0;

  v_nonseg_wage_pay numeric := 0;
  v_nonseg_reimb_pay numeric := 0;
  v_nonseg_wage_pay_cost numeric := 0;
  v_nonseg_pay_cost numeric := 0;
  v_nonseg_margin numeric := 0;

  v_margin numeric := 0;
  v_exclude boolean := false;
begin
  -- Load invoice_debug flag (safe even if column not yet present)
  begin
    select coalesce(sd.invoice_debug, false)
      into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','start',
        'at_utc', public._inv_iso_utc(v_dbg_started_at),
        'timesheet_id', coalesce(p_timesheet_id::text,''),
        'has_new_segments', (p_new_segments is not null),
        'correlation_id', p_correlation_id
      )
    );
  end if;

  if p_timesheet_id is null then
    v_ret := jsonb_build_object('ok', false, 'error', 'TIMESHEET_ID_REQUIRED');

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','TIMESHEET_ID_REQUIRED')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('timesheet:' || coalesce(p_timesheet_id::text,'')),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  if p_new_segments is null or jsonb_typeof(p_new_segments) <> 'array' then
    v_ret := jsonb_build_object('ok', false, 'error', 'NEW_SEGMENTS_MUST_BE_ARRAY');

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','NEW_SEGMENTS_MUST_BE_ARRAY')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('timesheet:' || p_timesheet_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- Lock the current TSFIN row for this timesheet (also load stored totals + policy/pay inputs for ERNI-aware margin)
  select
      tf.id,
      tf.invoice_breakdown_json,
      coalesce(tf.total_pay_ex_vat, 0)::numeric as total_pay_ex_vat,
      coalesce(tf.total_charge_ex_vat, 0)::numeric as total_charge_ex_vat,
      coalesce(tf.policy_snapshot_json, '{}'::jsonb) as policy_snapshot_json,
      coalesce(tf.pay_method::text, '') as pay_method_text,
      coalesce(tf.additional_pay_ex_vat, 0)::numeric as additional_pay_ex_vat,
      coalesce(tf.expenses_pay_ex_vat, 0)::numeric as expenses_pay_ex_vat,
      coalesce(tf.mileage_pay_ex_vat, 0)::numeric as mileage_pay_ex_vat
    into
      v_tsfin_id,
      v_ib,
      v_tf_total_pay,
      v_tf_total_charge,
      v_policy,
      v_pay_method_text,
      v_additional_pay_ex_vat,
      v_expenses_pay_ex_vat,
      v_mileage_pay_ex_vat
  from public.timesheets_financials tf
  where tf.timesheet_id = p_timesheet_id
    and tf.is_current = true
  for update;

  if not found or v_tsfin_id is null then
    v_ret := jsonb_build_object('ok', false, 'error', 'CURRENT_TSFIN_NOT_FOUND');

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','CURRENT_TSFIN_NOT_FOUND')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('timesheet:' || p_timesheet_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object('step','tsfin_locked','at_utc', public._inv_iso_utc(now()), 'tsfin_id', v_tsfin_id::text)
    );
  end if;

  if v_ib is null or jsonb_typeof(v_ib) <> 'object' then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'CURRENT_INVOICE_BREAKDOWN_INVALID',
      'tsfin_id', v_tsfin_id::text
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','CURRENT_INVOICE_BREAKDOWN_INVALID')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  if upper(coalesce(v_ib->>'mode','')) <> 'SEGMENTS' then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'CURRENT_NOT_SEGMENTS_MODE',
      'tsfin_id', v_tsfin_id::text,
      'mode', upper(coalesce(v_ib->>'mode',''))
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','CURRENT_NOT_SEGMENTS_MODE')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  if jsonb_typeof(v_ib->'segments') <> 'array' then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'CURRENT_SEGMENTS_NOT_ARRAY',
      'tsfin_id', v_tsfin_id::text
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','CURRENT_SEGMENTS_NOT_ARRAY')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- Record old invalid count (for reporting)
  v_old_invalid := public._tsfin_invalid_segment_count(v_ib);

  -- Diagnostics: count what is "corrupted" in current segments (non-object/null/missing ids etc.)
  begin
    v_old_segments_len := jsonb_array_length(v_ib->'segments');
  exception when others then
    v_old_segments_len := 0;
  end;

  for r in
    select value as seg, ordinality as ord
    from jsonb_array_elements(v_ib->'segments') with ordinality
  loop
    if r.seg is null or jsonb_typeof(r.seg) <> 'object' then
      v_old_non_object_count := v_old_non_object_count + 1;

      if v_invoice_debug then
        begin
          if jsonb_array_length(v_old_invalid_samples) < v_old_invalid_samples_cap then
            v_old_invalid_samples := v_old_invalid_samples || jsonb_build_array(
              jsonb_build_object(
                'ord', r.ord,
                'kind', 'NON_OBJECT',
                'type', coalesce(jsonb_typeof(r.seg), 'null')
              )
            );
          end if;
        exception when others then
          null;
        end;
      end if;

      continue;
    end if;

    sid := nullif(btrim(coalesce(r.seg->>'segment_id','')), '');
    if sid is null then
      v_old_missing_segment_id_count := v_old_missing_segment_id_count + 1;

      if v_invoice_debug then
        begin
          if jsonb_array_length(v_old_invalid_samples) < v_old_invalid_samples_cap then
            v_old_invalid_samples := v_old_invalid_samples || jsonb_build_array(
              jsonb_build_object(
                'ord', r.ord,
                'kind', 'MISSING_SEGMENT_ID'
              )
            );
          end if;
        exception when others then
          null;
        end;
      end if;
    end if;

    lock_invoice_id := nullif(btrim(coalesce(r.seg->>'invoice_locked_invoice_id','')), '');
    if lock_invoice_id is not null then
      v_old_locked_count := v_old_locked_count + 1;
    else
      v_old_unlocked_count := v_old_unlocked_count + 1;

      if nullif(btrim(coalesce(r.seg->>'invoice_target_week_start','')), '') is not null then
        v_old_unlocked_delayed_count := v_old_unlocked_delayed_count + 1;
      end if;
    end if;
  end loop;

  begin
    v_new_segments_len := jsonb_array_length(p_new_segments);
  exception when others then
    v_new_segments_len := 0;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','loaded_current',
        'at_utc', public._inv_iso_utc(now()),
        'tsfin_id', v_tsfin_id::text,
        'old_invalid_count', v_old_invalid,
        'old_segments_len', v_old_segments_len,
        'old_non_object_count', v_old_non_object_count,
        'old_missing_segment_id_count', v_old_missing_segment_id_count,
        'old_locked_count', v_old_locked_count,
        'old_unlocked_count', v_old_unlocked_count,
        'old_unlocked_delayed_count', v_old_unlocked_delayed_count,
        'new_segments_len', v_new_segments_len
      )
    );
  end if;

  -- ============================================================
  -- Build delay map from current segments (preserve overrides)
  -- We preserve only non-blank invoice_target_week_start, keyed by segment_id.
  -- ============================================================
  for e in
    select value from jsonb_array_elements(v_ib->'segments') value
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      continue;
    end if;

    sid := nullif(btrim(coalesce(e->>'segment_id','')), '');
    if sid is null then
      continue;
    end if;

    v_delay_text := nullif(btrim(coalesce(e->>'invoice_target_week_start','')), '');
    if v_delay_text is not null then
      v_delay_by_segment_id := jsonb_set(v_delay_by_segment_id, array[sid], to_jsonb(v_delay_text), true);
    end if;
  end loop;

  -- ------------------------------------------------------------
  -- Build locked maps from current segments (preserve these exactly)
  -- Key precedence for locked segments:
  --   1) nhsp_shift_id (if valid uuid)
  --   2) external_row_key (if present)
  --   3) signature (date + start_utc UTC + ref_num)
  -- Fail closed if chosen key is missing or duplicated among locked segments.
  -- ------------------------------------------------------------
  for e in
    select value from jsonb_array_elements(v_ib->'segments') value
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      continue;
    end if;

    lock_invoice_id := nullif(btrim(coalesce(e->>'invoice_locked_invoice_id','')), '');
    if lock_invoice_id is null then
      continue;
    end if;

    v_locked_count := v_locked_count + 1;

    sid := nullif(btrim(coalesce(e->>'segment_id','')), '');
    if sid is null then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'LOCKED_SEGMENT_MISSING_SEGMENT_ID',
        'tsfin_id', v_tsfin_id::text
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','LOCKED_SEGMENT_MISSING_SEGMENT_ID')
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'stats', jsonb_build_object('locked_count', v_locked_count), 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    -- optional identities
    v_nhsp_id := nullif(btrim(coalesce(e->>'nhsp_shift_id','')), '');
    v_ext_key := nullif(btrim(coalesce(e->>'external_row_key','')), '');

    -- validate nhsp_shift_id looks like uuid
    is_uuid := false;
    if v_nhsp_id is not null then
      if v_nhsp_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
        is_uuid := true;
      else
        v_nhsp_id := null;
      end if;
    end if;

    if v_nhsp_id is not null and is_uuid then
      if v_locked_nhsp_map ? v_nhsp_id then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_LOCKED_NHSP_SHIFT_ID',
          'tsfin_id', v_tsfin_id::text,
          'nhsp_shift_id', v_nhsp_id,
          'segment_id', sid
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_LOCKED_NHSP_SHIFT_ID', 'nhsp_shift_id', v_nhsp_id, 'segment_id', sid)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;

      v_locked_nhsp_map := jsonb_set(v_locked_nhsp_map, array[v_nhsp_id], e, true);
      v_locked_nhsp_idmap := jsonb_set(v_locked_nhsp_idmap, array[v_nhsp_id], to_jsonb(sid), true);
      continue;
    end if;

    if v_ext_key is not null then
      if v_locked_ext_map ? v_ext_key then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_LOCKED_EXTERNAL_ROW_KEY',
          'tsfin_id', v_tsfin_id::text,
          'external_row_key', v_ext_key,
          'segment_id', sid
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_LOCKED_EXTERNAL_ROW_KEY', 'external_row_key', v_ext_key, 'segment_id', sid)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;

      v_locked_ext_map := jsonb_set(v_locked_ext_map, array[v_ext_key], e, true);
      v_locked_ext_idmap := jsonb_set(v_locked_ext_idmap, array[v_ext_key], to_jsonb(sid), true);
      continue;
    end if;

    -- fallback signature key
    v_need_sig := true;

    v_date := nullif(btrim(coalesce(e->>'date', e->>'work_date', '')), '');
    v_ref  := nullif(btrim(coalesce(e->>'ref_num', e->>'ref', e->>'reference', '')), '');

    begin
      v_start_ts := nullif(btrim(coalesce(e->>'start_utc','')), '')::timestamptz;
    exception when others then
      v_start_ts := null;
    end;

    if v_date is null or v_start_ts is null then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'LOCKED_SEGMENT_KEY_MISSING',
        'tsfin_id', v_tsfin_id::text,
        'segment_id', sid
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','LOCKED_SEGMENT_KEY_MISSING', 'segment_id', sid)
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    v_start_norm := to_char(v_start_ts at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
    v_sig_key := v_date || '|' || v_start_norm || '|' || coalesce(v_ref, '');

    if v_locked_sig_map ? v_sig_key then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'DUPLICATE_LOCKED_SEGMENT_KEY',
        'tsfin_id', v_tsfin_id::text,
        'sig_key', v_sig_key,
        'segment_id', sid
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_LOCKED_SEGMENT_KEY', 'sig_key', v_sig_key, 'segment_id', sid)
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    v_locked_sig_map := jsonb_set(v_locked_sig_map, array[v_sig_key], e, true);
    v_locked_sig_idmap := jsonb_set(v_locked_sig_idmap, array[v_sig_key], to_jsonb(sid), true);
  end loop;

  v_preserved_locked_count := v_locked_count;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','locked_maps_built',
        'at_utc', public._inv_iso_utc(now()),
        'locked_count', v_locked_count,
        'need_sig', v_need_sig
      )
    );
  end if;

  -- ------------------------------------------------------------
  -- Build fresh maps from p_new_segments.
  -- ------------------------------------------------------------
  for e in
    select value from jsonb_array_elements(p_new_segments) value
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'NEW_SEGMENTS_CONTAINS_NON_OBJECT',
        'tsfin_id', v_tsfin_id::text
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','NEW_SEGMENTS_CONTAINS_NON_OBJECT')
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    sid := nullif(btrim(coalesce(e->>'segment_id','')), '');
    if sid is null then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'NEW_SEGMENTS_MISSING_SEGMENT_ID',
        'tsfin_id', v_tsfin_id::text
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','NEW_SEGMENTS_MISSING_SEGMENT_ID')
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    v_nhsp_id := nullif(btrim(coalesce(e->>'nhsp_shift_id','')), '');
    v_ext_key := nullif(btrim(coalesce(e->>'external_row_key','')), '');

    -- nhsp_shift_id map
    if v_nhsp_id is not null and v_nhsp_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
      if v_fresh_nhsp_map ? v_nhsp_id then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_NEW_NHSP_SHIFT_ID',
          'tsfin_id', v_tsfin_id::text,
          'nhsp_shift_id', v_nhsp_id
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_NEW_NHSP_SHIFT_ID', 'nhsp_shift_id', v_nhsp_id)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;
      v_fresh_nhsp_map := jsonb_set(v_fresh_nhsp_map, array[v_nhsp_id], e, true);
    end if;

    -- external_row_key map
    if v_ext_key is not null then
      if v_fresh_ext_map ? v_ext_key then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_NEW_EXTERNAL_ROW_KEY',
          'tsfin_id', v_tsfin_id::text,
          'external_row_key', v_ext_key
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_NEW_EXTERNAL_ROW_KEY', 'external_row_key', v_ext_key)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;
      v_fresh_ext_map := jsonb_set(v_fresh_ext_map, array[v_ext_key], e, true);
    end if;

    -- signature map (only if required)
    if v_need_sig then
      v_date := nullif(btrim(coalesce(e->>'date', e->>'work_date', '')), '');
      v_ref  := nullif(btrim(coalesce(e->>'ref_num', e->>'ref', e->>'reference', '')), '');

      begin
        v_start_ts := nullif(btrim(coalesce(e->>'start_utc','')), '')::timestamptz;
      exception when others then
        v_start_ts := null;
      end;

      if v_date is null or v_start_ts is null then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'NEW_SEGMENT_KEY_MISSING',
          'tsfin_id', v_tsfin_id::text,
          'segment_id', sid
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','NEW_SEGMENT_KEY_MISSING', 'segment_id', sid)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;

      v_start_norm := to_char(v_start_ts at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
      v_sig_key := v_date || '|' || v_start_norm || '|' || coalesce(v_ref, '');

      if v_fresh_sig_map ? v_sig_key then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_NEW_SEGMENT_KEY',
          'tsfin_id', v_tsfin_id::text,
          'sig_key', v_sig_key
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_NEW_SEGMENT_KEY', 'sig_key', v_sig_key)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;

      v_fresh_sig_map := jsonb_set(v_fresh_sig_map, array[v_sig_key], e, true);
    end if;
  end loop;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object('step','fresh_maps_built','at_utc', public._inv_iso_utc(now()))
    );
  end if;

  -- ------------------------------------------------------------
  -- Safety check: every locked key must exist in new segments set.
  -- ------------------------------------------------------------

  -- locked by nhsp_shift_id
  if jsonb_typeof(v_locked_nhsp_map) = 'object' then
    for k in select jsonb_object_keys(v_locked_nhsp_map)
    loop
      if not (v_fresh_nhsp_map ? k) then
        v_missing_locked_ids := array_append(
          v_missing_locked_ids,
          coalesce(nullif(btrim(coalesce(v_locked_nhsp_idmap->>k,'')), ''), k)
        );
      end if;
    end loop;
  end if;

  -- locked by external_row_key
  if jsonb_typeof(v_locked_ext_map) = 'object' then
    for k in select jsonb_object_keys(v_locked_ext_map)
    loop
      if not (v_fresh_ext_map ? k) then
        v_missing_locked_ids := array_append(
          v_missing_locked_ids,
          coalesce(nullif(btrim(coalesce(v_locked_ext_idmap->>k,'')), ''), k)
        );
      end if;
    end loop;
  end if;

  -- locked by signature
  if v_need_sig and jsonb_typeof(v_locked_sig_map) = 'object' then
    for k in select jsonb_object_keys(v_locked_sig_map)
    loop
      if not (v_fresh_sig_map ? k) then
        v_missing_locked_ids := array_append(
          v_missing_locked_ids,
          coalesce(nullif(btrim(coalesce(v_locked_sig_idmap->>k,'')), ''), k)
        );
      end if;
    end loop;
  end if;

  if v_missing_locked_ids is not null and array_length(v_missing_locked_ids, 1) is not null then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'MISSING_LOCKED_SEGMENTS_IN_NEW',
      'tsfin_id', v_tsfin_id::text,
      'missing_locked_segment_ids', to_jsonb(v_missing_locked_ids),
      'preserved_locked_count', v_preserved_locked_count,
      'old_invalid_count', v_old_invalid
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','MISSING_LOCKED_SEGMENTS_IN_NEW')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object(
            'result', v_ret,
            'corruption', jsonb_build_object(
              'old_invalid_count', v_old_invalid,
              'old_segments_len', v_old_segments_len,
              'old_non_object_count', v_old_non_object_count,
              'old_missing_segment_id_count', v_old_missing_segment_id_count,
              'old_invalid_samples', v_old_invalid_samples
            ),
            'steps', v_dbg_steps
          ),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- ------------------------------------------------------------
  -- Build merged segments in the same order as p_new_segments:
  -- Prefer matching by nhsp_shift_id, then external_row_key, then signature (if needed).
  -- If a match exists -> use locked JSON exactly, else use new JSON.
  -- Re-apply invoice_target_week_start from current TSFIN (by segment_id).
  -- ------------------------------------------------------------
  v_merged_segments := '[]'::jsonb;

  for e in
    select value from jsonb_array_elements(p_new_segments) value
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      continue;
    end if;

    chosen := null;

    v_nhsp_id := nullif(btrim(coalesce(e->>'nhsp_shift_id','')), '');
    if v_nhsp_id is not null and v_nhsp_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
      if v_locked_nhsp_map ? v_nhsp_id then
        chosen := v_locked_nhsp_map->v_nhsp_id;
      end if;
    end if;

    if chosen is null then
      v_ext_key := nullif(btrim(coalesce(e->>'external_row_key','')), '');
      if v_ext_key is not null then
        if v_locked_ext_map ? v_ext_key then
          chosen := v_locked_ext_map->v_ext_key;
        end if;
      end if;
    end if;

    if chosen is null and v_need_sig then
      v_date := nullif(btrim(coalesce(e->>'date', e->>'work_date', '')), '');
      v_ref  := nullif(btrim(coalesce(e->>'ref_num', e->>'ref', e->>'reference', '')), '');

      begin
        v_start_ts := nullif(btrim(coalesce(e->>'start_utc','')), '')::timestamptz;
      exception when others then
        v_start_ts := null;
      end;

      if v_date is not null and v_start_ts is not null then
        v_start_norm := to_char(v_start_ts at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
        v_sig_key := v_date || '|' || v_start_norm || '|' || coalesce(v_ref, '');
        if v_locked_sig_map ? v_sig_key then
          chosen := v_locked_sig_map->v_sig_key;
        end if;
      end if;
    end if;

    if chosen is null then
      chosen := e;
    end if;

    -- preserve delay override from current TSFIN JSON (segment_id match)
    if chosen is not null and jsonb_typeof(chosen) = 'object' then
      sid := nullif(btrim(coalesce(chosen->>'segment_id','')), '');
      if sid is not null and (v_delay_by_segment_id ? sid) then
        chosen := jsonb_set(chosen, '{invoice_target_week_start}', v_delay_by_segment_id->sid, true);
        v_delays_reapplied := v_delays_reapplied + 1;
      end if;
    end if;

    v_merged_segments := v_merged_segments || jsonb_build_array(chosen);
  end loop;

  -- Apply merged segments to the existing invoice_breakdown_json
  v_new_ib := jsonb_set(v_ib, '{segments}', v_merged_segments, true);

  v_new_invalid := public._tsfin_invalid_segment_count(v_new_ib);
  if v_new_invalid > 0 then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'MERGE_RESULT_STILL_INVALID',
      'tsfin_id', v_tsfin_id::text,
      'old_invalid_count', v_old_invalid,
      'new_invalid_count', v_new_invalid,
      'preserved_locked_count', v_preserved_locked_count
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','MERGE_RESULT_STILL_INVALID')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object(
            'result', v_ret,
            'corruption', jsonb_build_object(
              'old_invalid_count', v_old_invalid,
              'new_invalid_count', v_new_invalid,
              'old_segments_len', v_old_segments_len,
              'old_non_object_count', v_old_non_object_count,
              'old_missing_segment_id_count', v_old_missing_segment_id_count,
              'old_invalid_samples', v_old_invalid_samples
            ),
            'steps', v_dbg_steps
          ),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- preserve additional/expenses and keep totals consistent with merged segments
  v_add_ok := false;
  v_add_pay := 0;
  v_add_charge := 0;

  if (v_ib ? 'additional') and jsonb_typeof(v_ib->'additional') = 'object' then
    v_add_pay_text := nullif(btrim(coalesce(v_ib->'additional'->>'pay_ex_vat','')), '');
    v_add_charge_text := nullif(btrim(coalesce(v_ib->'additional'->>'charge_ex_vat','')), '');

    if v_add_pay_text is not null and v_add_charge_text is not null then
      begin
        v_add_pay := v_add_pay_text::numeric;
        v_add_charge := v_add_charge_text::numeric;
        v_add_ok := true;
      exception when others then
        v_add_ok := false;
        v_add_pay := 0;
        v_add_charge := 0;
      end;
    end if;
  end if;

  -- Fallback: derive non-segment totals from stored totals minus ORIGINAL segment sums (pre-merge)
  if not v_add_ok then
    v_old_seg_pay_sum := 0;
    v_old_seg_charge_sum := 0;

    for e in
      select value from jsonb_array_elements(v_ib->'segments') as t(value)
    loop
      if e is null or jsonb_typeof(e) <> 'object' then
        continue;
      end if;

      begin
        v_old_seg_charge_sum := v_old_seg_charge_sum + coalesce(nullif(btrim(coalesce(e->>'charge_amount','')), '')::numeric, 0);
      exception when others then
        null;
      end;

      v_exclude := false;
      begin
        v_exclude := coalesce(nullif(btrim(coalesce(e->>'exclude_from_pay','')), '')::boolean, false);
      exception when others then
        v_exclude := false;
      end;

      begin
        if not v_exclude then
          v_old_seg_pay_sum := v_old_seg_pay_sum + coalesce(nullif(btrim(coalesce(e->>'pay_amount','')), '')::numeric, 0);
        end if;
      exception when others then
        null;
      end;
    end loop;

    v_old_seg_pay_sum := round(coalesce(v_old_seg_pay_sum, 0), 2);
    v_old_seg_charge_sum := round(coalesce(v_old_seg_charge_sum, 0), 2);

    v_add_pay := round(coalesce(v_tf_total_pay, 0) - coalesce(v_old_seg_pay_sum, 0), 2);
    v_add_charge := round(coalesce(v_tf_total_charge, 0) - coalesce(v_old_seg_charge_sum, 0), 2);
  else
    v_add_pay := round(coalesce(v_add_pay, 0), 2);
    v_add_charge := round(coalesce(v_add_charge, 0), 2);
  end if;

  v_seg_pay_sum := 0;
  v_seg_charge_sum := 0;

  for e in
    select value from jsonb_array_elements(v_merged_segments) as t(value)
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      continue;
    end if;

    -- segment charge sum (include all segments; negative allowed)
    begin
      v_seg_charge_sum := v_seg_charge_sum + coalesce(nullif(btrim(coalesce(e->>'charge_amount','')), '')::numeric, 0);
    exception when others then
      null;
    end;

    -- pay sum respects exclude_from_pay when present/true
    v_exclude := false;
    begin
      v_exclude := coalesce(nullif(btrim(coalesce(e->>'exclude_from_pay','')), '')::boolean, false);
    exception when others then
      v_exclude := false;
    end;

    begin
      if not v_exclude then
        v_seg_pay_sum := v_seg_pay_sum + coalesce(nullif(btrim(coalesce(e->>'pay_amount','')), '')::numeric, 0);
      end if;
    exception when others then
      null;
    end;
  end loop;

  v_seg_pay_sum := round(coalesce(v_seg_pay_sum, 0), 2);
  v_seg_charge_sum := round(coalesce(v_seg_charge_sum, 0), 2);

  -- Totals are pure ex-VAT totals (NO ERNI in totals)
  v_total_pay := round(v_seg_pay_sum + coalesce(v_add_pay, 0), 2);
  v_total_charge := round(v_seg_charge_sum + coalesce(v_add_charge, 0), 2);

  -- ERNI-aware margin:
  -- - PAYE only
  -- - wage pay only = merged segment pay + additional_pay_ex_vat
  -- - never apply ERNI to expenses or mileage
  if v_policy is null or jsonb_typeof(v_policy) <> 'object' then
    v_policy := '{}'::jsonb;
  end if;

  v_apply_to := upper(coalesce(nullif(btrim(coalesce(v_policy->>'apply_erni_to','')), ''), 'PAYE_ONLY'));

  v_erni_pct_raw := 0;
  begin
    v_erni_pct_raw := coalesce(nullif(btrim(coalesce(v_policy->>'erni_pct','')), '')::numeric, 0);
  exception when others then
    v_erni_pct_raw := 0;
  end;

  v_erni_mult := 1;
  if coalesce(v_erni_pct_raw,0) > 0 then
    if v_erni_pct_raw > 1 then
      v_erni_mult := 1 + (v_erni_pct_raw / 100);
    else
      v_erni_mult := 1 + v_erni_pct_raw;
    end if;
  end if;

  v_pay_method_u := upper(coalesce(nullif(btrim(coalesce(v_pay_method_text,'')), ''), ''));

  v_erni_applies :=
    (v_pay_method_u = 'PAYE')
    and (v_apply_to = 'ALL' or v_apply_to = 'PAYE_ONLY');

  v_wage_pay := round(coalesce(v_seg_pay_sum, 0) + coalesce(v_additional_pay_ex_vat, 0), 2);
  v_reimb_pay := round(coalesce(v_expenses_pay_ex_vat, 0) + coalesce(v_mileage_pay_ex_vat, 0), 2);

  v_wage_pay_cost := v_wage_pay;
  if v_erni_applies then
    v_wage_pay_cost := round(v_wage_pay * v_erni_mult, 2);
  end if;

  v_pay_cost := round(v_wage_pay_cost + v_reimb_pay, 2);
  v_margin := round(v_total_charge - v_pay_cost, 2);

  -- Keep invoice_breakdown_json.totals in sync (do not touch other keys)
  v_new_ib := jsonb_set(v_new_ib, '{totals,total_pay_ex_vat}', to_jsonb(v_total_pay), true);
  v_new_ib := jsonb_set(v_new_ib, '{totals,total_charge_ex_vat}', to_jsonb(v_total_charge), true);
  v_new_ib := jsonb_set(v_new_ib, '{totals,margin_ex_vat}', to_jsonb(v_margin), true);

  -- Keep additional.margin_ex_vat ERNI-accurate if additional object exists
  if (v_new_ib ? 'additional') and jsonb_typeof(v_new_ib->'additional') = 'object' then
    v_nonseg_wage_pay := round(coalesce(v_additional_pay_ex_vat, 0), 2);
    v_nonseg_reimb_pay := v_reimb_pay;

    v_nonseg_wage_pay_cost := v_nonseg_wage_pay;
    if v_erni_applies then
      v_nonseg_wage_pay_cost := round(v_nonseg_wage_pay * v_erni_mult, 2);
    end if;

    v_nonseg_pay_cost := round(v_nonseg_wage_pay_cost + v_nonseg_reimb_pay, 2);
    v_nonseg_margin := round(coalesce(v_add_charge, 0) - v_nonseg_pay_cost, 2);

    v_new_ib := jsonb_set(v_new_ib, '{additional,margin_ex_vat}', to_jsonb(v_nonseg_margin), true);
  end if;

  -- Update segments + totals + columns (do NOT change lock summary fields)
  update public.timesheets_financials tfu
  set
    invoice_breakdown_json = v_new_ib,
    total_pay_ex_vat = v_total_pay,
    total_charge_ex_vat = v_total_charge,
    margin_ex_vat = v_margin,
    updated_at = now()
  where tfu.id = v_tsfin_id
    and tfu.is_current = true;

  get diagnostics v_rows_updated = row_count;

  if v_rows_updated <> 1 then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'UPDATE_DID_NOT_APPLY',
      'tsfin_id', v_tsfin_id::text,
      'rows_updated', v_rows_updated
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','UPDATE_DID_NOT_APPLY')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- merged length (for reporting)
  begin
    v_merged_len := jsonb_array_length(v_merged_segments);
  exception when others then
    v_merged_len := null;
  end;

  v_ret := jsonb_build_object(
    'ok', true,
    'tsfin_id', v_tsfin_id::text,
    'timesheet_id', p_timesheet_id::text,
    'merged_len', v_merged_len,
    'preserved_locked_count', v_preserved_locked_count,
    'old_invalid_count', v_old_invalid,
    'new_invalid_count', v_new_invalid,
    'delays_reapplied', v_delays_reapplied,
    'actor_user_id', case when p_actor_user_id is null then null else p_actor_user_id::text end,
    'correlation_id', p_correlation_id
  );

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfin_id', v_tsfin_id::text,
        'old_invalid_count', v_old_invalid,
        'new_invalid_count', v_new_invalid,
        'old_segments_len', v_old_segments_len,
        'old_non_object_count', v_old_non_object_count,
        'old_missing_segment_id_count', v_old_missing_segment_id_count,
        'old_locked_count', v_old_locked_count,
        'old_unlocked_count', v_old_unlocked_count,
        'old_unlocked_delayed_count', v_old_unlocked_delayed_count,
        'new_segments_len', v_new_segments_len,
        'locked_preserved', v_preserved_locked_count,
        'merged_len', v_merged_len,
        'rows_updated', v_rows_updated,
        'delays_reapplied', v_delays_reapplied
      );

      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','finish',
          'at_utc', public._inv_iso_utc(now()),
          'stats', v_dbg_stats
        )
      );

      perform public._inv_write_audit(
        p_actor_user_id,
        'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
        jsonb_build_object(
          'result', v_ret,
          'corruption', jsonb_build_object(
            'old_invalid_count', v_old_invalid,
            'old_segments_len', v_old_segments_len,
            'old_non_object_count', v_old_non_object_count,
            'old_missing_segment_id_count', v_old_missing_segment_id_count,
            'old_invalid_samples', v_old_invalid_samples
          ),
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'timesheets_financials',
        ('tsfin:' || v_tsfin_id::text),
        null,
        'INVOICE_DEBUG',
        null, null, p_correlation_id
      );
    exception when others then
      null;
    end;
  end if;

  return v_ret;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfin_id', coalesce(v_tsfin_id::text,''),
        'old_invalid_count', v_old_invalid,
        'new_invalid_count', v_new_invalid,
        'old_segments_len', v_old_segments_len,
        'old_non_object_count', v_old_non_object_count,
        'old_missing_segment_id_count', v_old_missing_segment_id_count,
        'old_invalid_samples', v_old_invalid_samples,
        'locked_count', v_locked_count,
        'preserved_locked_count', v_preserved_locked_count,
        'delays_reapplied', v_delays_reapplied
      );

      perform public._inv_write_audit(
        p_actor_user_id,
        'TSFIN_REPAIR_MERGE_SEGMENTS_ERROR',
        jsonb_build_object(
          'timesheet_id', coalesce(p_timesheet_id::text,''),
          'tsfin_id', coalesce(v_tsfin_id::text,''),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps,
          'correlation_id', p_correlation_id
        ),
        'timesheets_financials',
        case
          when v_tsfin_id is null then ('timesheet:' || coalesce(p_timesheet_id::text,''))
          else ('tsfin:' || v_tsfin_id::text)
        end,
        null,
        'INVOICE_DEBUG',
        null, null, p_correlation_id
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$function$;

-- tsfin_report_timesheets_v2(date,date,text,uuid[],uuid[],boolean,boolean,boolean)
CREATE OR REPLACE FUNCTION public.tsfin_report_timesheets_v2(p_week_ending_from date DEFAULT NULL::date, p_week_ending_to date DEFAULT NULL::date, p_pay_method text DEFAULT NULL::text, p_client_ids uuid[] DEFAULT NULL::uuid[], p_candidate_ids uuid[] DEFAULT NULL::uuid[], p_include_on_hold boolean DEFAULT false, p_paid boolean DEFAULT NULL::boolean, p_invoiced boolean DEFAULT NULL::boolean)
 RETURNS SETOF jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
with base as (
  select
    tf.timesheet_id,
    tf.candidate_id,
    tf.client_id,
    tf.pay_method,
    tf.total_pay_ex_vat,
    tf.total_charge_ex_vat,
    tf.margin_ex_vat,
    tf.expenses_charge_ex_vat,
    tf.mileage_charge_ex_vat,
    tf.paid_at_utc,
    tf.pay_on_hold,
    tf.locked_by_invoice_id,
    ts.week_ending_date,
    c.name as client_name,
    (
      tf.locked_by_invoice_id is not null
      or (
        upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
        and exists (
          select 1
          from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg
          where nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is not null
        )
      )
    ) as invoiced_any
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id = tf.timesheet_id
   and ts.is_current = true
  left join public.clients c
    on c.id = tf.client_id
  where tf.is_current = true
    and (p_week_ending_from is null or ts.week_ending_date::date >= p_week_ending_from)
    and (p_week_ending_to is null or ts.week_ending_date::date <= p_week_ending_to)
    and (p_pay_method is null or upper(coalesce(tf.pay_method,'')) = upper(p_pay_method))
    and (p_include_on_hold or coalesce(tf.pay_on_hold,false) = false)
    and (
      p_client_ids is null
      or array_length(p_client_ids,1) is null
      or tf.client_id = any(p_client_ids)
    )
    and (
      p_candidate_ids is null
      or array_length(p_candidate_ids,1) is null
      or tf.candidate_id = any(p_candidate_ids)
    )
    and (
      p_paid is null
      or (p_paid = true and tf.paid_at_utc is not null)
      or (p_paid = false and tf.paid_at_utc is null)
    )
)
select jsonb_build_object(
  'timesheet_id', base.timesheet_id,
  'candidate_id', base.candidate_id,
  'client_id', base.client_id,
  'pay_method', base.pay_method,
  'locked_by_invoice_id', base.locked_by_invoice_id,
  'total_pay_ex_vat', base.total_pay_ex_vat,
  'total_charge_ex_vat', base.total_charge_ex_vat,
  'margin_ex_vat', base.margin_ex_vat,
  'expenses_charge_ex_vat', base.expenses_charge_ex_vat,
  'mileage_charge_ex_vat', base.mileage_charge_ex_vat,
  'paid_at_utc', base.paid_at_utc,
  'pay_on_hold', base.pay_on_hold,
  'invoiced_any', base.invoiced_any,
  'timesheet', jsonb_build_object(
    'week_ending_date', base.week_ending_date
  ),
  'client', jsonb_build_object(
    'name', base.client_name
  )
)
from base
where (p_invoiced is null or base.invoiced_any = p_invoiced)
order by
  base.week_ending_date desc nulls last,
  base.client_name asc nulls last,
  base.timesheet_id::text asc;
$function$;

-- tsfin_resolve_rates_batch(jsonb)
CREATE OR REPLACE FUNCTION public.tsfin_resolve_rates_batch(p_items jsonb)
 RETURNS TABLE(k text, candidate_id uuid, client_id uuid, role text, band text, date_ymd date, rate_type text, source_kind text, override_id uuid, default_id uuid, pay_day numeric, pay_night numeric, pay_sat numeric, pay_sun numeric, pay_bh numeric, charge_day numeric, charge_night numeric, charge_sat numeric, charge_sun numeric, charge_bh numeric)
 LANGUAGE sql
 STABLE
AS $function$
with items as (
  select
    coalesce(nullif(elem->>'k',''), nullif(elem->>'timesheet_id','')) as k,

    nullif(elem->>'candidate_id','')::uuid as candidate_id,
    nullif(elem->>'client_id','')::uuid as client_id,

    nullif(elem->>'role','') as role,
    nullif(elem->>'band','') as band,

    nullif(elem->>'date','')::date as date_ymd,

    upper(coalesce(nullif(elem->>'rate_type',''), 'UMBRELLA')) as rate_type_raw
  from jsonb_array_elements(coalesce(p_items, '[]'::jsonb)) as elem
),
norm as (
  select
    i.*,
    case
      when i.rate_type_raw in ('PAYE','UMBRELLA') then i.rate_type_raw
      else 'UMBRELLA'
    end as rate_type
  from items i
),
resolved as (
  select
    n.k,
    n.candidate_id,
    n.client_id,
    n.role,
    n.band,
    n.date_ymd,
    n.rate_type,

    ov.id as override_id,

    df.id as default_id,

    -- pay buckets: override first, else client defaults by rate_type
    case
      when ov.id is not null then ov.pay_day
      when df.id is not null and n.rate_type = 'PAYE' then df.paye_day
      when df.id is not null and n.rate_type = 'UMBRELLA' then df.umb_day
      else null
    end as pay_day,

    case
      when ov.id is not null then ov.pay_night
      when df.id is not null and n.rate_type = 'PAYE' then df.paye_night
      when df.id is not null and n.rate_type = 'UMBRELLA' then df.umb_night
      else null
    end as pay_night,

    case
      when ov.id is not null then ov.pay_sat
      when df.id is not null and n.rate_type = 'PAYE' then df.paye_sat
      when df.id is not null and n.rate_type = 'UMBRELLA' then df.umb_sat
      else null
    end as pay_sat,

    case
      when ov.id is not null then ov.pay_sun
      when df.id is not null and n.rate_type = 'PAYE' then df.paye_sun
      when df.id is not null and n.rate_type = 'UMBRELLA' then df.umb_sun
      else null
    end as pay_sun,

    case
      when ov.id is not null then ov.pay_bh
      when df.id is not null and n.rate_type = 'PAYE' then df.paye_bh
      when df.id is not null and n.rate_type = 'UMBRELLA' then df.umb_bh
      else null
    end as pay_bh,

    -- charge buckets: always from client defaults (may be null if no default)
    case when df.id is not null then df.charge_day else null end as charge_day,
    case when df.id is not null then df.charge_night else null end as charge_night,
    case when df.id is not null then df.charge_sat else null end as charge_sat,
    case when df.id is not null then df.charge_sun else null end as charge_sun,
    case when df.id is not null then df.charge_bh else null end as charge_bh

  from norm n

  -- Candidate override (pay-only): exact client + exact role + exact rate_type.
  -- Band rule (STRICT):
  --  - if band provided: ONLY exact band
  --  - if band null: ONLY band is null (no guessing)
  left join lateral (
    select o.*
    from public.rates_candidate_overrides o
    where n.candidate_id is not null
      and n.client_id is not null
      and n.role is not null
      and n.date_ymd is not null
      and o.candidate_id = n.candidate_id
      and o.client_id = n.client_id
      and o.role = n.role
      and o.rate_type = n.rate_type
      and o.date_from <= n.date_ymd
      and (o.date_to is null or o.date_to >= n.date_ymd)
      and (
        (n.band is null and o.band is null)
        or
        (n.band is not null and o.band = n.band)
      )
    order by
      case
        when n.band is not null and o.band = n.band then 0
        when o.band is null then 1
        else 9
      end,
      o.date_from desc,
      o.updated_at desc
    limit 1
  ) ov on true

  -- Client defaults (charge always, pay fallback if no override).
  -- Must be enabled: disabled_at_utc is null.
  -- Band rule (STRICT):
  --  - if band provided: ONLY exact band
  --  - if band null: ONLY band null
  left join lateral (
    select d.*
    from public.rates_client_defaults d
    where n.client_id is not null
      and n.role is not null
      and n.date_ymd is not null
      and d.client_id = n.client_id
      and d.role = n.role
      and d.disabled_at_utc is null
      and d.date_from <= n.date_ymd
      and (d.date_to is null or d.date_to >= n.date_ymd)
      and (
        (n.band is null and d.band is null)
        or
        (n.band is not null and d.band = n.band)
      )
    order by
      case
        when n.band is not null and d.band = n.band then 0
        when d.band is null then 1
        else 9
      end,
      d.date_from desc,
      d.updated_at desc
    limit 1
  ) df on true
)
select
  r.k,
  r.candidate_id,
  r.client_id,
  r.role,
  r.band,
  r.date_ymd,
  r.rate_type,

  case
    when r.override_id is not null then 'CANDIDATE_OVERRIDE'
    when r.default_id is not null then 'CLIENT_DEFAULT'
    else 'NONE'
  end as source_kind,

  r.override_id,
  r.default_id,

  r.pay_day, r.pay_night, r.pay_sat, r.pay_sun, r.pay_bh,
  r.charge_day, r.charge_night, r.charge_sat, r.charge_sun, r.charge_bh
from resolved r;
$function$;

-- tsfin_search_timesheets_v2(date,date,text,uuid,uuid,boolean,boolean,boolean,text,text,text,text,text,timestamp with time zone,timestamp with time zone,timestamp with time zone,timestamp with time zone,text[],uuid[],text,text,integer,integer)
CREATE OR REPLACE FUNCTION public.tsfin_search_timesheets_v2(p_week_ending_from date DEFAULT NULL::date, p_week_ending_to date DEFAULT NULL::date, p_pay_method text DEFAULT NULL::text, p_client_id uuid DEFAULT NULL::uuid, p_candidate_id uuid DEFAULT NULL::uuid, p_include_on_hold boolean DEFAULT false, p_paid boolean DEFAULT NULL::boolean, p_invoiced boolean DEFAULT NULL::boolean, p_sheet_scope text DEFAULT NULL::text, p_qr_status text DEFAULT NULL::text, p_booking_id text DEFAULT NULL::text, p_occupant_key_norm text DEFAULT NULL::text, p_hospital_norm text DEFAULT NULL::text, p_worked_from timestamp with time zone DEFAULT NULL::timestamp with time zone, p_worked_to timestamp with time zone DEFAULT NULL::timestamp with time zone, p_created_from timestamp with time zone DEFAULT NULL::timestamp with time zone, p_created_to timestamp with time zone DEFAULT NULL::timestamp with time zone, p_statuses text[] DEFAULT NULL::text[], p_timesheet_ids uuid[] DEFAULT NULL::uuid[], p_order_by text DEFAULT 'week_ending_date'::text, p_order_dir text DEFAULT 'desc'::text, p_limit integer DEFAULT 50, p_offset integer DEFAULT 0)
 RETURNS SETOF jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
with base as (
  select
    tf.timesheet_id,
    tf.candidate_id,
    tf.client_id,
    tf.pay_method,
    tf.processing_status,
    tf.basis,
    tf.total_charge_ex_vat,
    tf.total_pay_ex_vat,
    tf.margin_ex_vat,
    tf.paid_at_utc,
    tf.locked_by_invoice_id,
    tf.pay_on_hold,
    tf.created_at,
    tf.worked_start_iso,
    tf.worked_end_iso,

    ts.week_ending_date,
    ts.status as ts_status,
    ts.booking_id,
    ts.occupant_key_norm,
    ts.hospital_norm,
    ts.sheet_scope,
    ts.submission_mode,
    ts.qr_status,

    c.name as client_name,

    (
      tf.locked_by_invoice_id is not null
      or (
        upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
        and exists (
          select 1
          from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg
          where nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is not null
        )
      )
    ) as invoiced_any

  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id = tf.timesheet_id
   and ts.is_current = true
  left join public.clients c
    on c.id = tf.client_id
  where tf.is_current = true

    and (p_week_ending_from is null or ts.week_ending_date::date >= p_week_ending_from)
    and (p_week_ending_to is null or ts.week_ending_date::date <= p_week_ending_to)

    and (p_pay_method is null or upper(coalesce(tf.pay_method,'')) = upper(p_pay_method))

    and (p_client_id is null or tf.client_id = p_client_id)
    and (p_candidate_id is null or tf.candidate_id = p_candidate_id)

    and (p_include_on_hold or coalesce(tf.pay_on_hold,false) = false)

    and (
      p_paid is null
      or (p_paid = true and tf.paid_at_utc is not null)
      or (p_paid = false and tf.paid_at_utc is null)
    )

    and (p_sheet_scope is null or upper(coalesce(ts.sheet_scope::text,'')) = upper(p_sheet_scope))
    and (p_qr_status is null or upper(coalesce(ts.qr_status::text,'')) = upper(p_qr_status))

    and (p_booking_id is null or coalesce(ts.booking_id::text,'') = p_booking_id)
    and (p_occupant_key_norm is null or coalesce(ts.occupant_key_norm,'') = p_occupant_key_norm)
    and (p_hospital_norm is null or coalesce(ts.hospital_norm,'') = p_hospital_norm)

    and (p_worked_from is null or tf.worked_start_iso >= p_worked_from)
    and (p_worked_to is null or tf.worked_end_iso <= p_worked_to)

    and (p_created_from is null or tf.created_at >= p_created_from)
    and (p_created_to is null or tf.created_at <= p_created_to)

    and (
      p_statuses is null
      or array_length(p_statuses,1) is null
      or upper(coalesce(ts.status::text,'')) = any (array(select upper(x) from unnest(p_statuses) x))
    )

    and (
      p_timesheet_ids is null
      or array_length(p_timesheet_ids,1) is null
      or tf.timesheet_id = any(p_timesheet_ids)
    )
)
select jsonb_build_object(
  'timesheet_id', base.timesheet_id,
  'candidate_id', base.candidate_id,
  'client_id', base.client_id,
  'pay_method', base.pay_method,
  'processing_status', base.processing_status,
  'basis', base.basis,
  'total_charge_ex_vat', base.total_charge_ex_vat,
  'total_pay_ex_vat', base.total_pay_ex_vat,
  'margin_ex_vat', base.margin_ex_vat,
  'paid_at_utc', base.paid_at_utc,
  'locked_by_invoice_id', base.locked_by_invoice_id,
  'pay_on_hold', base.pay_on_hold,
  'created_at', base.created_at,
  'invoiced_any', base.invoiced_any,
  'timesheet', jsonb_build_object(
    'week_ending_date', base.week_ending_date,
    'status', base.ts_status,
    'booking_id', base.booking_id,
    'occupant_key_norm', base.occupant_key_norm,
    'hospital_norm', base.hospital_norm,
    'sheet_scope', base.sheet_scope,
    'submission_mode', base.submission_mode,
    'qr_status', base.qr_status
  ),
  'client', jsonb_build_object(
    'name', base.client_name
  )
)
from base
where (p_invoiced is null or base.invoiced_any = p_invoiced)
order by
  -- week_ending_date
  case when lower(coalesce(p_order_by,'')) in ('week_ending_date') and lower(coalesce(p_order_dir,'')) = 'asc'
    then base.week_ending_date end asc nulls last,
  case when lower(coalesce(p_order_by,'')) in ('week_ending_date') and lower(coalesce(p_order_dir,'')) <> 'asc'
    then base.week_ending_date end desc nulls last,

  -- margin
  case when lower(coalesce(p_order_by,'')) in ('margin','margin_ex_vat') and lower(coalesce(p_order_dir,'')) = 'asc'
    then base.margin_ex_vat end asc nulls last,
  case when lower(coalesce(p_order_by,'')) in ('margin','margin_ex_vat') and lower(coalesce(p_order_dir,'')) <> 'asc'
    then base.margin_ex_vat end desc nulls last,

  -- charge
  case when lower(coalesce(p_order_by,'')) in ('charge','total_charge_ex_vat') and lower(coalesce(p_order_dir,'')) = 'asc'
    then base.total_charge_ex_vat end asc nulls last,
  case when lower(coalesce(p_order_by,'')) in ('charge','total_charge_ex_vat') and lower(coalesce(p_order_dir,'')) <> 'asc'
    then base.total_charge_ex_vat end desc nulls last,

  -- pay
  case when lower(coalesce(p_order_by,'')) in ('pay','total_pay_ex_vat') and lower(coalesce(p_order_dir,'')) = 'asc'
    then base.total_pay_ex_vat end asc nulls last,
  case when lower(coalesce(p_order_by,'')) in ('pay','total_pay_ex_vat') and lower(coalesce(p_order_dir,'')) <> 'asc'
    then base.total_pay_ex_vat end desc nulls last,

  -- stable fallback
  base.week_ending_date desc nulls last,
  base.timesheet_id::text asc
limit greatest(1, least(coalesce(p_limit, 50), 200))
offset greatest(coalesce(p_offset, 0), 0);
$function$;

-- tsfin_update_segments_locked(uuid,jsonb,uuid)
CREATE OR REPLACE FUNCTION public.tsfin_update_segments_locked(p_timesheet_id uuid, p_segment_updates jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  -- locked row
  v_tf_id uuid;
  v_basis text;
  v_breakdown jsonb;

  -- stored totals (used to preserve non-segment totals if breakdown.additional is missing/incomplete)
  v_total_pay_ex_vat numeric;
  v_total_charge_ex_vat numeric;

  -- ERNI / policy inputs (for margin accuracy)
  v_policy jsonb;
  v_pay_method_text text;
  v_additional_pay_ex_vat numeric;
  v_expenses_pay_ex_vat numeric;
  v_mileage_pay_ex_vat numeric;

  v_week_ending date;
  v_natural_week_start date;

  v_allow_invoice_target_change boolean := false;

  -- updates map: { "<segment_id>": {exclude_from_pay:bool?, invoice_target_week_start:text?}, ... }
  v_updates jsonb := '{}'::jsonb;
  v_elem jsonb;
  v_sid text;
  v_entry jsonb;
  v_req_target text;

  -- apply loop
  v_segments jsonb;
  v_seg jsonb;
  v_out_segments jsonb := '[]'::jsonb;

  v_exclude boolean;
  v_new_total_pay_segments numeric := 0;
  v_new_total_charge_segments numeric := 0;

  -- ✅ preserve "additional/expenses" totals from breakdown.additional (now includes expenses+mileage in TSFIN invariant)
  v_add_pay numeric := 0;
  v_add_charge numeric := 0;

  -- ✅ if breakdown.additional is missing/incomplete, derive non-segment totals from stored totals minus ORIGINAL segment totals
  v_old_total_pay_segments numeric := 0;
  v_old_total_charge_segments numeric := 0;
  v_use_breakdown_additional boolean := false;
  v_add_text_pay text;
  v_add_text_charge text;
  v_add_units jsonb;

  v_new_total_pay numeric := 0;
  v_new_total_charge numeric := 0;
  v_new_margin numeric := 0;

  -- ✅ ERNI-aware margin (PAYE only; applies to wage pay only: segments pay + additional_pay_ex_vat; never to expenses/mileage)
  v_apply_to text;
  v_erni_pct_raw numeric := 0;
  v_erni_mult numeric := 1;
  v_pay_method_u text;
  v_erni_applies boolean := false;

  v_wage_pay numeric := 0;
  v_reimb_pay numeric := 0;
  v_wage_pay_cost numeric := 0;
  v_pay_cost numeric := 0;

  v_nonseg_wage_pay numeric := 0;
  v_nonseg_reimb_pay numeric := 0;
  v_nonseg_wage_pay_cost numeric := 0;
  v_nonseg_pay_cost numeric := 0;
  v_nonseg_margin numeric := 0;

  -- validation helpers
  v_locked_invoice_id_text text;
  v_cur_target text;
  v_cur_target_has boolean;
  v_clear_requested boolean;

  v_updated_row jsonb;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_id required';
  end if;

  if p_segment_updates is null or jsonb_typeof(p_segment_updates) <> 'array' then
    raise exception 'segment_updates must be a json array';
  end if;

  -- 1) Lock the current TSFIN row (critical: prevents lost-update corruption)
  select
    tf.id,
    upper(coalesce(tf.basis::text, '')) as basis,
    tf.invoice_breakdown_json,
    coalesce(tf.total_pay_ex_vat, 0)::numeric as total_pay_ex_vat,
    coalesce(tf.total_charge_ex_vat, 0)::numeric as total_charge_ex_vat,
    ts.week_ending_date::date as week_ending_date,

    -- ERNI/policy inputs for accurate margin recompute
    coalesce(tf.policy_snapshot_json, '{}'::jsonb) as policy_snapshot_json,
    coalesce(tf.pay_method::text, '') as pay_method_text,
    coalesce(tf.additional_pay_ex_vat, 0)::numeric as additional_pay_ex_vat,
    coalesce(tf.expenses_pay_ex_vat, 0)::numeric as expenses_pay_ex_vat,
    coalesce(tf.mileage_pay_ex_vat, 0)::numeric as mileage_pay_ex_vat
  into
    v_tf_id,
    v_basis,
    v_breakdown,
    v_total_pay_ex_vat,
    v_total_charge_ex_vat,
    v_week_ending,

    v_policy,
    v_pay_method_text,
    v_additional_pay_ex_vat,
    v_expenses_pay_ex_vat,
    v_mileage_pay_ex_vat
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id = tf.timesheet_id
  where tf.timesheet_id = p_timesheet_id
    and tf.is_current = true
  order by tf.created_at desc
  limit 1
  for update;

  if not found then
    raise exception 'Current financial snapshot not found for timesheet_id %', p_timesheet_id;
  end if;

  if v_breakdown is null or jsonb_typeof(v_breakdown) <> 'object' then
    raise exception 'No invoice_breakdown_json present on snapshot';
  end if;

  if upper(coalesce(v_breakdown->>'mode','')) <> 'SEGMENTS'
     or jsonb_typeof(v_breakdown->'segments') <> 'array' then
    raise exception 'This snapshot is not SEGMENTS-based; cannot update per-line settings';
  end if;

  v_segments := v_breakdown->'segments';

  -- Allowed bases (match Worker)
  v_allow_invoice_target_change :=
    v_basis in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT');

  if v_week_ending is not null then
    v_natural_week_start := (v_week_ending - 6);
  else
    v_natural_week_start := null;
  end if;

  -- 2) Build update map (last write wins)
  for v_elem in
    select value
    from jsonb_array_elements(p_segment_updates) as t(value)
  loop
    if v_elem is null or jsonb_typeof(v_elem) <> 'object' then
      continue;
    end if;

    v_sid := nullif(btrim(coalesce(v_elem->>'segment_id','')), '');
    if v_sid is null then
      continue;
    end if;

    v_entry := '{}'::jsonb;

    if v_elem ? 'exclude_from_pay' then
      v_entry := v_entry || jsonb_build_object('exclude_from_pay', v_elem->'exclude_from_pay');
    end if;

    -- ✅ FIX: support explicit clear (key present with null/blank) by carrying a json null in the update map
    if (v_elem ? 'invoice_target_week_start') then
      if nullif(btrim(coalesce(v_elem->>'invoice_target_week_start','')), '') is not null then
        v_entry := v_entry || jsonb_build_object(
          'invoice_target_week_start',
          nullif(btrim(coalesce(v_elem->>'invoice_target_week_start','')), '')
        );
      else
        v_entry := v_entry || jsonb_build_object('invoice_target_week_start', null);
      end if;
    end if;

    -- merge into map
    v_updates := v_updates || jsonb_build_object(v_sid, v_entry);
  end loop;

  if jsonb_typeof(v_updates) <> 'object' or v_updates = '{}'::jsonb then
    raise exception 'No valid segment_id entries to update';
  end if;

  -- 3) Pre-validate invoice_target_week_start changes (match Worker semantics)
  if not v_allow_invoice_target_change then
    if exists (
      select 1
      from jsonb_each(v_updates) as e(key, value)
      where (e.value ? 'invoice_target_week_start')
    ) then
      raise exception 'invoice_target_week_start cannot be changed for this snapshot basis';
    end if;
  else
    for v_sid, v_entry in
      select key, value
      from jsonb_each(v_updates)
    loop
      if v_entry is null or jsonb_typeof(v_entry) <> 'object' then
        continue;
      end if;

      if not (v_entry ? 'invoice_target_week_start') then
        continue;
      end if;

      v_req_target := nullif(btrim(coalesce(v_entry->>'invoice_target_week_start','')), '');
      v_clear_requested := (v_req_target is null);

      -- Find the current segment (objects only; unknown ids are ignored like Worker)
      select
        nullif(btrim(coalesce(seg.value->>'invoice_locked_invoice_id','')), '') as locked_inv,
        nullif(btrim(coalesce(seg.value->>'invoice_target_week_start','')), '') as cur_target,
        (seg.value ? 'invoice_target_week_start') as has_target
      into
        v_locked_invoice_id_text,
        v_cur_target,
        v_cur_target_has
      from jsonb_array_elements(v_segments) as seg(value)
      where jsonb_typeof(seg.value) = 'object'
        and nullif(btrim(coalesce(seg.value->>'segment_id','')), '') = v_sid
      limit 1;

      if v_locked_invoice_id_text is not null then
        -- ✅ FIX: explicit clear must be treated as a change unless it is a no-op
        if v_clear_requested then
          if not (
            (v_cur_target_has is not true) or
            (v_cur_target is null) or
            (v_natural_week_start is not null and v_cur_target = v_natural_week_start::text)
          ) then
            raise exception
              'Segment % is attached to an invoice and cannot have invoice delay changed. Remove from invoice first.',
              v_sid;
          end if;
          continue;
        end if;

        -- No-op tolerant rule (match Worker)
        if not (
          v_req_target = coalesce(v_cur_target, '')
          or (v_cur_target is null and v_natural_week_start is not null and v_req_target = v_natural_week_start::text)
        ) then
          raise exception
            'Segment % is attached to an invoice and cannot have invoice delay changed. Remove from invoice first.',
            v_sid;
        end if;
        continue;
      end if;

      -- For unlocked segments: only validate when setting an actual date (clears skip validation)
      if v_clear_requested then
        continue;
      end if;

      if v_natural_week_start is not null then
        begin
          if (v_req_target::date < v_natural_week_start) then
            raise exception
              'invoice_target_week_start for segment % cannot be earlier than natural week start %',
              v_sid, v_natural_week_start::text;
          end if;
        exception when others then
          raise exception 'invoice_target_week_start for segment % is invalid: %', v_sid, v_req_target;
        end;
      end if;
    end loop;
  end if;

  -- ✅ Preserve "additional/expenses" totals.
  -- Prefer breakdown.additional if present & numeric; otherwise derive from stored totals minus ORIGINAL segment totals.
  v_use_breakdown_additional := false;
  v_add_pay := 0;
  v_add_charge := 0;

  if (v_breakdown ? 'additional') and jsonb_typeof(v_breakdown->'additional') = 'object' then
    v_add_text_pay := nullif(btrim(coalesce(v_breakdown->'additional'->>'pay_ex_vat','')), '');
    v_add_text_charge := nullif(btrim(coalesce(v_breakdown->'additional'->>'charge_ex_vat','')), '');

    if v_add_text_pay is not null and v_add_text_charge is not null then
      begin
        v_add_pay := v_add_text_pay::numeric;
        v_add_charge := v_add_text_charge::numeric;
        v_use_breakdown_additional := true;
      exception when others then
        v_use_breakdown_additional := false;
        v_add_pay := 0;
        v_add_charge := 0;
      end;
    end if;
  end if;

  -- 4) Apply updates + recompute totals (segments pay) WITHOUT losing additional/expenses
  v_old_total_pay_segments := 0;
  v_old_total_charge_segments := 0;

  v_new_total_pay_segments := 0;
  v_new_total_charge_segments := 0;
  v_out_segments := '[]'::jsonb;

  for v_seg in
    select value
    from jsonb_array_elements(v_segments) as t(value)
  loop
    if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
      v_out_segments := v_out_segments || jsonb_build_array(v_seg);
      continue;
    end if;

    v_sid := nullif(btrim(coalesce(v_seg->>'segment_id','')), '');
    if v_sid is null then
      v_out_segments := v_out_segments || jsonb_build_array(v_seg);
      continue;
    end if;

    v_entry := v_updates->v_sid;

    -- ORIGINAL exclude_from_pay (for old totals)
    begin
      v_exclude := coalesce(nullif(btrim(coalesce(v_seg->>'exclude_from_pay','')), '')::boolean, false);
    exception when others then
      v_exclude := false;
    end;

    -- ORIGINAL totals
    begin
      v_old_total_charge_segments :=
        v_old_total_charge_segments
        + coalesce(nullif(btrim(coalesce(v_seg->>'charge_amount','')), '')::numeric, 0);
    exception when others then
      null;
    end;

    begin
      if not v_exclude then
        v_old_total_pay_segments :=
          v_old_total_pay_segments
          + coalesce(nullif(btrim(coalesce(v_seg->>'pay_amount','')), '')::numeric, 0);
      end if;
    exception when others then
      null;
    end;

    -- UPDATED exclude_from_pay
    if v_entry is not null and jsonb_typeof(v_entry) = 'object' and (v_entry ? 'exclude_from_pay') then
      begin
        v_exclude := coalesce(nullif(btrim(coalesce(v_entry->>'exclude_from_pay','')), '')::boolean, false);
      exception when others then
        v_exclude := false;
      end;
      v_seg := jsonb_set(v_seg, '{exclude_from_pay}', to_jsonb(v_exclude), true);
    end if;

    -- invoice_target_week_start (allowed bases only; unlocked segments only)
    if v_allow_invoice_target_change
       and v_entry is not null
       and jsonb_typeof(v_entry) = 'object'
       and (v_entry ? 'invoice_target_week_start') then
      v_locked_invoice_id_text := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
      if v_locked_invoice_id_text is null then
        v_req_target := nullif(btrim(coalesce(v_entry->>'invoice_target_week_start','')), '');
        if v_req_target is not null then
          v_seg := jsonb_set(v_seg, '{invoice_target_week_start}', to_jsonb(v_req_target), true);
        else
          -- ✅ FIX: explicit clear removes the key entirely
          v_seg := v_seg - 'invoice_target_week_start';
        end if;
      end if;
    end if;

    -- NEW totals
    begin
      v_new_total_charge_segments :=
        v_new_total_charge_segments
        + coalesce(nullif(btrim(coalesce(v_seg->>'charge_amount','')), '')::numeric, 0);
    exception when others then
      null;
    end;

    begin
      if not v_exclude then
        v_new_total_pay_segments :=
          v_new_total_pay_segments
          + coalesce(nullif(btrim(coalesce(v_seg->>'pay_amount','')), '')::numeric, 0);
      end if;
    exception when others then
      null;
    end;

    v_out_segments := v_out_segments || jsonb_build_array(v_seg);
  end loop;

  v_old_total_pay_segments := round(coalesce(v_old_total_pay_segments, 0), 2);
  v_old_total_charge_segments := round(coalesce(v_old_total_charge_segments, 0), 2);

  -- If breakdown.additional missing/incomplete, derive non-segment totals from stored totals minus ORIGINAL segment totals
  if v_use_breakdown_additional is not true then
    v_add_pay := round(coalesce(v_total_pay_ex_vat, 0) - coalesce(v_old_total_pay_segments, 0), 2);
    v_add_charge := round(coalesce(v_total_charge_ex_vat, 0) - coalesce(v_old_total_charge_segments, 0), 2);

    -- Ensure breakdown.additional exists so future edits don't lose non-segment totals
    if (v_breakdown ? 'additional') and jsonb_typeof(v_breakdown->'additional') = 'object' then
      v_add_units := case
        when (v_breakdown->'additional' ? 'units') and jsonb_typeof(v_breakdown->'additional'->'units') = 'object'
          then v_breakdown->'additional'->'units'
        else '{}'::jsonb
      end;
    else
      v_add_units := '{}'::jsonb;
    end if;

    v_breakdown := jsonb_set(
      v_breakdown,
      '{additional}',
      jsonb_build_object(
        'units', v_add_units,
        'pay_ex_vat', v_add_pay,
        'charge_ex_vat', v_add_charge,
        'margin_ex_vat', round(v_add_charge - v_add_pay, 2)
      ),
      true
    );
  else
    v_add_pay := round(coalesce(v_add_pay, 0), 2);
    v_add_charge := round(coalesce(v_add_charge, 0), 2);
  end if;

  -- ✅ totals (pure ex-VAT totals; NO ERNI in totals)
  v_new_total_pay_segments := round(coalesce(v_new_total_pay_segments, 0), 2);
  v_new_total_charge_segments := round(coalesce(v_new_total_charge_segments, 0), 2);

  v_new_total_pay := round(v_new_total_pay_segments + coalesce(v_add_pay, 0), 2);
  v_new_total_charge := round(v_new_total_charge_segments + coalesce(v_add_charge, 0), 2);

  -- ✅ ERNI-aware margin recompute (PAYE only; wage pay only)
  if v_policy is null or jsonb_typeof(v_policy) <> 'object' then
    v_policy := '{}'::jsonb;
  end if;

  v_apply_to := upper(coalesce(nullif(btrim(coalesce(v_policy->>'apply_erni_to','')), ''), 'PAYE_ONLY'));

  v_erni_pct_raw := 0;
  begin
    v_erni_pct_raw := coalesce(nullif(btrim(coalesce(v_policy->>'erni_pct','')), '')::numeric, 0);
  exception when others then
    v_erni_pct_raw := 0;
  end;

  v_erni_mult := 1;
  if coalesce(v_erni_pct_raw,0) > 0 then
    if v_erni_pct_raw > 1 then
      v_erni_mult := 1 + (v_erni_pct_raw / 100);
    else
      v_erni_mult := 1 + v_erni_pct_raw;
    end if;
  end if;

  v_pay_method_u := upper(coalesce(nullif(btrim(coalesce(v_pay_method_text,'')), ''), ''));

  -- PAYE only (apply_erni_to never makes it apply to non-PAYE)
  v_erni_applies :=
    (v_pay_method_u = 'PAYE')
    and (v_apply_to = 'ALL' or v_apply_to = 'PAYE_ONLY');

  -- Wage-like pay: segments pay + additional pay (NOT expenses/mileage)
  v_wage_pay := round(coalesce(v_new_total_pay_segments, 0) + coalesce(v_additional_pay_ex_vat, 0), 2);

  -- Reimbursements: expenses + mileage (NEVER ERNI)
  v_reimb_pay := round(coalesce(v_expenses_pay_ex_vat, 0) + coalesce(v_mileage_pay_ex_vat, 0), 2);

  v_wage_pay_cost := v_wage_pay;
  if v_erni_applies then
    v_wage_pay_cost := round(v_wage_pay * v_erni_mult, 2);
  end if;

  v_pay_cost := round(v_wage_pay_cost + v_reimb_pay, 2);

  v_new_margin := round(v_new_total_charge - v_pay_cost, 2);

  -- Update breakdown segments
  v_breakdown := jsonb_set(v_breakdown, '{segments}', v_out_segments, true);

  -- ✅ Update breakdown.additional.margin_ex_vat to be ERNI-accurate:
  -- Non-segment wage pay is additional_pay_ex_vat; reimbursements are expenses+mileage; ERNI applies only to PAYE wage pay.
  v_nonseg_wage_pay := round(coalesce(v_additional_pay_ex_vat, 0), 2);
  v_nonseg_reimb_pay := v_reimb_pay;

  v_nonseg_wage_pay_cost := v_nonseg_wage_pay;
  if v_erni_applies then
    v_nonseg_wage_pay_cost := round(v_nonseg_wage_pay * v_erni_mult, 2);
  end if;

  v_nonseg_pay_cost := round(v_nonseg_wage_pay_cost + v_nonseg_reimb_pay, 2);
  v_nonseg_margin := round(coalesce(v_add_charge, 0) - v_nonseg_pay_cost, 2);

  if (v_breakdown ? 'additional') and jsonb_typeof(v_breakdown->'additional') = 'object' then
    v_breakdown := jsonb_set(v_breakdown, '{additional,margin_ex_vat}', to_jsonb(v_nonseg_margin), true);
  else
    v_breakdown := jsonb_set(
      v_breakdown,
      '{additional}',
      jsonb_build_object(
        'units', '{}'::jsonb,
        'pay_ex_vat', coalesce(v_add_pay, 0),
        'charge_ex_vat', coalesce(v_add_charge, 0),
        'margin_ex_vat', v_nonseg_margin
      ),
      true
    );
  end if;

  -- ✅ keep breakdown.totals in sync
  v_breakdown := jsonb_set(v_breakdown, '{totals,total_pay_ex_vat}', to_jsonb(v_new_total_pay), true);
  v_breakdown := jsonb_set(v_breakdown, '{totals,total_charge_ex_vat}', to_jsonb(v_new_total_charge), true);
  v_breakdown := jsonb_set(v_breakdown, '{totals,margin_ex_vat}', to_jsonb(v_new_margin), true);

  update public.timesheets_financials tfu
  set
    invoice_breakdown_json = v_breakdown,
    total_pay_ex_vat       = v_new_total_pay,
    total_charge_ex_vat    = v_new_total_charge,
    margin_ex_vat          = v_new_margin,
    updated_at             = v_now
  where tfu.id = v_tf_id
  returning to_jsonb(tfu) into v_updated_row;

  return jsonb_build_object('updated', v_updated_row);

end;
$function$;

-- tsfin_work_fail(uuid,text)
CREATE OR REPLACE FUNCTION public.tsfin_work_fail(p_id uuid, p_error text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
  v_attempt int;
  v_delay_minutes int;
BEGIN
  UPDATE public.ts_financials_outbox t
  SET attempt_count = t.attempt_count + 1,
      last_error    = p_error
  WHERE t.id = p_id
  RETURNING t.attempt_count INTO v_attempt;

  v_delay_minutes := LEAST(60, GREATEST(1, (2 ^ GREATEST(0, v_attempt - 1))));
  UPDATE public.ts_financials_outbox t2
  SET next_attempt_at = now() + (v_delay_minutes || ' minutes')::interval
  WHERE t2.id = p_id;
END;
$function$;

-- tsfin_work_success_bulk(uuid[])
CREATE OR REPLACE FUNCTION public.tsfin_work_success_bulk(p_ids uuid[])
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
  v_count int := 0;
  v_id uuid;
begin
  if p_ids is null then
    return 0;
  end if;

  foreach v_id in array p_ids loop
    begin
      perform public.tsfin_work_success(v_id);
      v_count := v_count + 1;
    exception when others then
      -- swallow (caller already recorded primary success/fail; extras are best-effort)
      null;
    end;
  end loop;

  return v_count;
end;
$function$;

