-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 18bc103be6f4.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
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
-- CloudTMS deployment metadata: deterministic owner and API-role ACLs.
ALTER FUNCTION public.timesheet_unauthorise_bulk_atomic(jsonb, uuid, timestamp with time zone) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.timesheet_unauthorise_bulk_atomic(jsonb, uuid, timestamp with time zone) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.timesheet_unauthorise_bulk_atomic(jsonb, uuid, timestamp with time zone) TO authenticated, service_role;
