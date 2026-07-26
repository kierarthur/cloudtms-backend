-- CloudTMS invoice batch private worker replacement
-- Pulled from live TEST DB signature and wrapped for the locked Batch Issue selection-expansion plan.
-- Install after CloudTMS_invoice_batch_private_helpers_v3_20260726 and 26072026_invoice_batch_modal_query_support are installed.
-- This file preserves the current live implementation as private._invoice_issue_advance_batch_legacy_20260726
-- and replaces private._invoice_issue_advance_batch with an EXPAND_SELECTION-aware entry point.

DO $preserve_legacy$
DECLARE
  v_def text;
BEGIN
  IF to_regprocedure('private._invoice_issue_advance_batch_legacy_20260726(jsonb, timestamp with time zone)') IS NULL THEN
    SELECT pg_get_functiondef(p.oid)
      INTO v_def
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'private'
      AND p.proname = '_invoice_issue_advance_batch'
      AND pg_get_function_identity_arguments(p.oid) = 'p_claims jsonb, p_now_utc timestamp with time zone';

    IF v_def IS NULL THEN
      RAISE EXCEPTION 'Cannot preserve legacy private._invoice_issue_advance_batch(jsonb, timestamptz): source function not found and legacy copy missing';
    END IF;

    IF position('_invoice_issue_advance_batch_legacy_20260726' IN v_def) > 0 THEN
      RAISE EXCEPTION 'Refusing to clone an already-wrapped private._invoice_issue_advance_batch; legacy copy is missing';
    END IF;

    v_def := regexp_replace(
      v_def,
      '^CREATE OR REPLACE FUNCTION private\._invoice_issue_advance_batch',
      'CREATE OR REPLACE FUNCTION private._invoice_issue_advance_batch_legacy_20260726'
    );

    EXECUTE v_def;
    EXECUTE 'REVOKE ALL ON FUNCTION private._invoice_issue_advance_batch_legacy_20260726(jsonb, timestamp with time zone) FROM PUBLIC, anon, authenticated';
    EXECUTE 'GRANT EXECUTE ON FUNCTION private._invoice_issue_advance_batch_legacy_20260726(jsonb, timestamp with time zone) TO service_role';
  END IF;
END
$preserve_legacy$;

CREATE OR REPLACE FUNCTION private._invoice_issue_advance_batch(
  p_claims jsonb,
  p_now_utc timestamp with time zone
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := coalesce(p_now_utc, now());
  v_result jsonb := '[]'::jsonb;
  v_part jsonb := '[]'::jsonb;
  v_passthrough jsonb := '[]'::jsonb;
BEGIN
  IF p_claims IS NULL OR jsonb_typeof(p_claims) IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION USING errcode = '22023',
      message = 'p_claims must be a JSON array containing 1..100 claims';
  END IF;

  IF jsonb_array_length(p_claims) < 1 OR jsonb_array_length(p_claims) > 100 THEN
    RAISE EXCEPTION USING errcode = '22023',
      message = 'p_claims must be a JSON array containing 1..100 claims';
  END IF;

  SELECT coalesce(jsonb_agg(claim_row.claim_json ORDER BY claim_row.claim_no), '[]'::jsonb)
    INTO v_passthrough
  FROM (
    SELECT e.value claim_json,
           e.ordinality::integer claim_no,
           CASE WHEN coalesce(e.value->>'chunk_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             THEN (e.value->>'chunk_id')::uuid END chunk_id
    FROM jsonb_array_elements(p_claims) WITH ORDINALITY e(value, ordinality)
  ) claim_row
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.invoice_operation_chunks c
    WHERE c.id = claim_row.chunk_id
      AND c.chunk_type = 'ISSUE_INVOICE'
      AND c.phase = 'EXPAND_SELECTION'
      AND coalesce(c.payload_json->>'is_selection_expander', 'false') IN ('true','t','1','yes','on')
  );

  WITH
  claim_rows AS MATERIALIZED (
    SELECT e.ordinality::integer claim_no,
           e.value claim_json,
           CASE WHEN coalesce(e.value->>'chunk_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             THEN (e.value->>'chunk_id')::uuid END chunk_id
    FROM jsonb_array_elements(p_claims) WITH ORDINALITY e(value, ordinality)
  ),
  expanders AS MATERIALIZED (
    SELECT c.id chunk_id,
           c.operation_id,
           c.priority,
           c.payload_json,
           coalesce(c.payload_json->>'command_token', o.input_json->>'command_token') command_token,
           o.actor_user_id,
           o.control_version,
           CASE WHEN coalesce(c.payload_json->>'scanned','') ~ '^[0-9]{1,9}$'
             THEN (c.payload_json->>'scanned')::integer ELSE 0 END scanned_before,
           CASE WHEN coalesce(c.payload_json->>'selected','') ~ '^[0-9]{1,9}$'
             THEN (c.payload_json->>'selected')::integer ELSE 0 END selected_before,
           CASE WHEN coalesce(c.payload_json->>'queued','') ~ '^[0-9]{1,9}$'
             THEN (c.payload_json->>'queued')::integer ELSE 0 END queued_before,
           CASE WHEN coalesce(c.payload_json->>'blocked','') ~ '^[0-9]{1,9}$'
             THEN (c.payload_json->>'blocked')::integer ELSE 0 END blocked_before,
           CASE WHEN coalesce(c.payload_json->>'changed','') ~ '^[0-9]{1,9}$'
             THEN (c.payload_json->>'changed')::integer ELSE 0 END changed_before,
           CASE WHEN coalesce(c.payload_json->>'already_active','') ~ '^[0-9]{1,9}$'
             THEN (c.payload_json->>'already_active')::integer ELSE 0 END already_active_before,
           CASE WHEN coalesce(c.payload_json->>'send_blocked','') ~ '^[0-9]{1,9}$'
             THEN (c.payload_json->>'send_blocked')::integer ELSE 0 END send_blocked_before
    FROM claim_rows claim
    JOIN public.invoice_operation_chunks c ON c.id = claim.chunk_id
    JOIN public.invoice_operations o ON o.id = c.operation_id
    WHERE c.chunk_type = 'ISSUE_INVOICE'
      AND c.phase = 'EXPAND_SELECTION'
      AND coalesce(c.payload_json->>'is_selection_expander', 'false') IN ('true','t','1','yes','on')
  ),
  helper_pages AS MATERIALIZED (
    SELECT e.*,
           private._invoice_batch_issue_candidate_rows_v1(
             (CASE WHEN jsonb_typeof(e.payload_json->'query') = 'object'
               THEN e.payload_json->'query' ELSE '{}'::jsonb END)
             || jsonb_build_object(
               'contract_version', 'INVOICE_BATCH_QUERY_V1',
               'action', 'ISSUE',
               'mode', 'EXPAND_SELECTION',
               'page_size', 100,
               'cursor', CASE WHEN jsonb_typeof(e.payload_json->'cursor') = 'object'
                 THEN e.payload_json->'cursor' ELSE '{}'::jsonb END,
               'selection', coalesce(
                 CASE WHEN jsonb_typeof(e.payload_json#>'{selection_contract,selection}') = 'object'
                   THEN e.payload_json#>'{selection_contract,selection}' END,
                 CASE WHEN jsonb_typeof(e.payload_json#>'{query,selection}') = 'object'
                   THEN e.payload_json#>'{query,selection}' END,
                 jsonb_build_object('contract_version','INVOICE_BATCH_SELECTION_V1','mode','IMPLICIT_ALL','default_selected',true,'rules','[]'::jsonb)
               )
             ),
             v_now
           ) page_json
    FROM expanders e
  ),
  page_rows AS MATERIALIZED (
    SELECT hp.chunk_id,
           hp.operation_id,
           hp.priority,
           hp.actor_user_id,
           hp.control_version,
           hp.payload_json expander_payload,
           hp.command_token,
           hp.scanned_before,
           row_item.value row_json,
           row_item.ordinality::integer row_no
    FROM helper_pages hp
    CROSS JOIN LATERAL jsonb_array_elements(
      CASE WHEN jsonb_typeof(hp.page_json->'rows') = 'array'
        THEN hp.page_json->'rows' ELSE '[]'::jsonb END
    ) WITH ORDINALITY row_item(value, ordinality)
    WHERE lower(coalesce(row_item.value->>'selectable','false')) IN ('true','t','1','yes','on')
      AND coalesce(row_item.value->>'invoice_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),
  inserted_issue_chunks AS MATERIALIZED (
    INSERT INTO public.invoice_operation_chunks(
      operation_id,
      chunk_type,
      phase,
      work_key,
      sequence_no,
      level_no,
      entity_type,
      entity_id,
      status,
      priority,
      run_after_utc,
      payload_json,
      progress_json,
      operation_control_version,
      created_at_utc,
      updated_at_utc
    )
    SELECT r.operation_id,
           'ISSUE_INVOICE',
           'VALIDATE',
           encode(digest(concat_ws('|',
             'ISSUE_INVOICE',
             r.row_json->>'invoice_id',
             coalesce(r.row_json->>'document_revision',''),
             coalesce(r.command_token,''),
             'FILTERED_SELECTION_V1'
           ), 'sha256'), 'hex'),
           r.scanned_before + r.row_no,
           0,
           'INVOICE',
           (r.row_json->>'invoice_id')::uuid,
           'QUEUED',
           greatest(coalesce(r.priority, 850), 850),
           v_now,
           jsonb_build_object(
             'request_key', r.row_json->>'selection_key',
             'selection_key', r.row_json->>'selection_key',
             'invoice_id', r.row_json->>'invoice_id',
             'invoice_number', r.row_json->>'invoice_number',
             'source_revision', coalesce(r.row_json->>'document_revision',''),
             'expected_revision', coalesce(r.row_json->>'document_revision',''),
             'evaluation_date', (v_now at time zone 'Europe/London')::date,
             'frozen_issue_at_utc', v_now,
             'allow_early', coalesce(r.expander_payload#>'{query,allow_early}', r.expander_payload->'allow_early', 'false'::jsonb),
             'deliver', coalesce(r.expander_payload->'deliver','false'::jsonb),
             'command_token', r.command_token,
             'delivery_request_token', r.expander_payload->>'delivery_request_token',
             'delivery_intent', coalesce(r.expander_payload->'delivery_intent','{}'::jsonb),
             'blocked_for_sending', coalesce(r.row_json->'blocked_for_sending','false'::jsonb),
             'client_id', r.row_json->>'client_id',
             'client_name', r.row_json->>'client_name',
             'candidate_display', r.row_json->>'candidate_display',
             'week_ending_display', r.row_json->>'week_ending_display',
             'currency', coalesce(r.row_json->>'currency','GBP'),
             'total_ex_vat', r.row_json->'total_ex_vat',
             'total_inc_vat', r.row_json->'total_inc_vat',
             'parent_expander_chunk_id', r.chunk_id
           ),
           jsonb_build_object(
             'status_message', 'Queued from filtered issue selection',
             'selection_key', r.row_json->>'selection_key',
             'invoice_number', r.row_json->>'invoice_number'
           ),
           r.control_version,
           v_now,
           v_now
    FROM page_rows r
    ON CONFLICT DO NOTHING
    RETURNING (payload_json->>'parent_expander_chunk_id')::uuid parent_expander_chunk_id,
              id chunk_id,
              entity_id invoice_id,
              status
  ),
  per_expander AS MATERIALIZED (
    SELECT hp.chunk_id,
           hp.operation_id,
           lower(coalesce(hp.page_json#>>'{page,has_more}','false')) IN ('true','t','1','yes','on') has_more,
           CASE WHEN jsonb_typeof(hp.page_json#>'{page,next_cursor_values}') = 'object'
             THEN hp.page_json#>'{page,next_cursor_values}' ELSE '{}'::jsonb END next_cursor,
           coalesce((SELECT count(*)::integer FROM page_rows r WHERE r.chunk_id = hp.chunk_id), 0) selected_count,
           coalesce((SELECT count(*)::integer FROM inserted_issue_chunks ic WHERE ic.parent_expander_chunk_id = hp.chunk_id), 0) queued_count,
           coalesce((SELECT count(*)::integer FROM page_rows r WHERE r.chunk_id = hp.chunk_id AND lower(coalesce(r.row_json->>'blocked_for_sending','false')) IN ('true','t','1','yes','on')), 0) send_blocked_count,
           hp.scanned_before,
           hp.selected_before,
           hp.queued_before,
           hp.blocked_before,
           hp.changed_before,
           hp.already_active_before,
           hp.send_blocked_before,
           hp.page_json
    FROM helper_pages hp
  ),
  updated_expanders AS MATERIALIZED (
    UPDATE public.invoice_operation_chunks c
    SET status = CASE WHEN p.has_more THEN 'QUEUED' ELSE 'COMPLETE' END,
        phase = CASE WHEN p.has_more THEN 'EXPAND_SELECTION' ELSE 'COMPLETE' END,
        run_after_utc = CASE WHEN p.has_more THEN v_now ELSE c.run_after_utc END,
        completed_at_utc = CASE WHEN p.has_more THEN NULL ELSE v_now END,
        payload_json = c.payload_json || jsonb_build_object(
          'cursor', CASE WHEN p.has_more THEN p.next_cursor ELSE '{}'::jsonb END,
          'scanned', p.scanned_before + p.selected_count,
          'selected', p.selected_before + p.selected_count,
          'queued', p.queued_before + p.queued_count,
          'blocked', p.blocked_before,
          'changed', p.changed_before,
          'already_active', p.already_active_before,
          'send_blocked', p.send_blocked_before + p.send_blocked_count,
          'completed', NOT p.has_more,
          'last_expanded_at_utc', v_now,
          'last_page', jsonb_build_object(
            'selected', p.selected_count,
            'queued_issue', p.queued_count,
            'send_blocked', p.send_blocked_count,
            'has_more', p.has_more
          )
        ),
        progress_json = jsonb_build_object(
          'status_message', CASE WHEN p.has_more THEN 'Expanding issue selection' ELSE 'Issue selection expansion complete' END,
          'selected_total', p.selected_before + p.selected_count,
          'queued_total', p.queued_before + p.queued_count,
          'issued_send_blocked_total', p.send_blocked_before + p.send_blocked_count,
          'selection_expansion_pending', p.has_more
        ),
        result_json = coalesce(c.result_json,'{}'::jsonb) || jsonb_build_object(
          'selection_expansion_pending', p.has_more,
          'selected_total', p.selected_before + p.selected_count,
          'queued_total', p.queued_before + p.queued_count,
          'issued_send_blocked_total', p.send_blocked_before + p.send_blocked_count,
          'last_candidate_page', p.page_json->'page'
        ),
        lease_owner = NULL,
        lease_token = NULL,
        lease_expires_at_utc = NULL,
        updated_at_utc = v_now
    FROM per_expander p
    WHERE c.id = p.chunk_id
    RETURNING c.id, c.operation_id, c.status, c.phase, c.result_json, c.error_json
  ),
  operation_progress AS MATERIALIZED (
    SELECT p.operation_id,
           sum(p.selected_count)::integer selected_count,
           sum(p.queued_count)::integer queued_count,
           sum(p.send_blocked_count)::integer send_blocked_count,
           bool_or(p.has_more) has_more
    FROM per_expander p
    GROUP BY p.operation_id
  ),
  updated_operations AS MATERIALIZED (
    UPDATE public.invoice_operations o
    SET progress_json = coalesce(o.progress_json,'{}'::jsonb) || jsonb_build_object(
          'status_message', CASE WHEN op.has_more THEN 'Expanding issue selection' ELSE 'Issue selection expanded' END,
          'selection_expansion_pending', op.has_more,
          'selected_total', (CASE WHEN coalesce(o.progress_json->>'selected_total','') ~ '^[0-9]{1,9}$' THEN (o.progress_json->>'selected_total')::integer ELSE 0 END) + op.selected_count,
          'queued_total', (CASE WHEN coalesce(o.progress_json->>'queued_total','') ~ '^[0-9]{1,9}$' THEN (o.progress_json->>'queued_total')::integer ELSE 0 END) + op.queued_count,
          'issued_send_blocked_total', (CASE WHEN coalesce(o.progress_json->>'issued_send_blocked_total','') ~ '^[0-9]{1,9}$' THEN (o.progress_json->>'issued_send_blocked_total')::integer ELSE 0 END) + op.send_blocked_count
        ),
        total_units = greatest(o.total_units, o.chunk_count + op.queued_count),
        chunk_count = greatest(o.chunk_count, o.chunk_count + op.queued_count),
        updated_at_utc = v_now,
        change_seq = nextval('public.invoice_operation_change_seq')
    FROM operation_progress op
    WHERE o.id = op.operation_id
    RETURNING o.id
  )
  SELECT coalesce(jsonb_agg(jsonb_build_object(
           'chunk_id', u.id,
           'status', u.status,
           'phase', u.phase,
           'result', u.result_json,
           'error', u.error_json
         ) ORDER BY u.id), '[]'::jsonb)
    INTO v_part
  FROM updated_expanders u;

  v_result := v_result || coalesce(v_part, '[]'::jsonb);

  IF jsonb_array_length(v_passthrough) > 0 THEN
    v_result := v_result || private._invoice_issue_advance_batch_legacy_20260726(v_passthrough, v_now);
  END IF;

  RETURN coalesce(v_result, '[]'::jsonb);
END;
$function$;

REVOKE ALL ON FUNCTION private._invoice_issue_advance_batch(jsonb, timestamp with time zone) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION private._invoice_issue_advance_batch(jsonb, timestamp with time zone) TO service_role;
