-- CloudTMS invoice batch private worker replacement
-- Pulled from live TEST DB signature and wrapped for the locked Batch Generate selection-expansion plan.
-- Install after CloudTMS_invoice_batch_private_helpers_v3_20260726 and 26072026_invoice_batch_modal_query_support are installed.
-- This file preserves the current live implementation as private._invoice_generation_advance_batch_legacy_20260726
-- and replaces private._invoice_generation_advance_batch with an EXPAND_SELECTION-aware entry point.

DO $preserve_legacy$
DECLARE
  v_def text;
BEGIN
  IF to_regprocedure('private._invoice_generation_advance_batch_legacy_20260726(jsonb, timestamp with time zone)') IS NULL THEN
    SELECT pg_get_functiondef(p.oid)
      INTO v_def
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'private'
      AND p.proname = '_invoice_generation_advance_batch'
      AND pg_get_function_identity_arguments(p.oid) = 'p_claims jsonb, p_now_utc timestamp with time zone';

    IF v_def IS NULL THEN
      RAISE EXCEPTION 'Cannot preserve legacy private._invoice_generation_advance_batch(jsonb, timestamptz): source function not found and legacy copy missing';
    END IF;

    IF position('_invoice_generation_advance_batch_legacy_20260726' IN v_def) > 0 THEN
      RAISE EXCEPTION 'Refusing to clone an already-wrapped private._invoice_generation_advance_batch; legacy copy is missing';
    END IF;

    v_def := regexp_replace(
      v_def,
      '^CREATE OR REPLACE FUNCTION private\._invoice_generation_advance_batch',
      'CREATE OR REPLACE FUNCTION private._invoice_generation_advance_batch_legacy_20260726'
    );

    EXECUTE v_def;
    EXECUTE 'REVOKE ALL ON FUNCTION private._invoice_generation_advance_batch_legacy_20260726(jsonb, timestamp with time zone) FROM PUBLIC, anon, authenticated';
    EXECUTE 'GRANT EXECUTE ON FUNCTION private._invoice_generation_advance_batch_legacy_20260726(jsonb, timestamp with time zone) TO service_role';
  END IF;
END
$preserve_legacy$;

CREATE OR REPLACE FUNCTION private._invoice_generation_advance_batch(
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
      AND c.chunk_type = 'GENERATION_GROUP'
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
             THEN (c.payload_json->>'already_active')::integer ELSE 0 END already_active_before
    FROM claim_rows claim
    JOIN public.invoice_operation_chunks c ON c.id = claim.chunk_id
    JOIN public.invoice_operations o ON o.id = c.operation_id
    WHERE c.chunk_type = 'GENERATION_GROUP'
      AND c.phase = 'EXPAND_SELECTION'
      AND coalesce(c.payload_json->>'is_selection_expander', 'false') IN ('true','t','1','yes','on')
  ),
  helper_pages AS MATERIALIZED (
    SELECT e.*,
           private._invoice_batch_generate_candidate_rows_v1(
             (CASE WHEN jsonb_typeof(e.payload_json->'query') = 'object'
               THEN e.payload_json->'query' ELSE '{}'::jsonb END)
             || jsonb_build_object(
               'contract_version', 'INVOICE_BATCH_QUERY_V1',
               'action', 'GENERATE',
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
           hp.scanned_before,
           row_item.value row_json,
           row_item.ordinality::integer row_no
    FROM helper_pages hp
    CROSS JOIN LATERAL jsonb_array_elements(
      CASE WHEN jsonb_typeof(hp.page_json->'rows') = 'array'
        THEN hp.page_json->'rows' ELSE '[]'::jsonb END
    ) WITH ORDINALITY row_item(value, ordinality)
  ),
  create_rows AS MATERIALIZED (
    SELECT *
    FROM page_rows r
    WHERE upper(coalesce(r.row_json->>'row_kind','')) = 'CREATE_INVOICE'
      AND lower(coalesce(r.row_json->>'selectable','false')) IN ('true','t','1','yes','on')
  ),
  document_rows AS MATERIALIZED (
    SELECT *
    FROM page_rows r
    WHERE upper(coalesce(r.row_json->>'row_kind','')) IN ('REGENERATE_DRAFT','RETRY_GENERATION')
      AND lower(coalesce(r.row_json->>'selectable','false')) IN ('true','t','1','yes','on')
      AND coalesce(r.row_json->>'invoice_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),
  inserted_generation_chunks AS MATERIALIZED (
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
           'GENERATION_GROUP',
           'VALIDATE_SOURCES',
           encode(digest(concat_ws('|',
             'GENERATION_GROUP',
             coalesce(r.row_json->>'selection_key',''),
             coalesce(r.row_json->>'source_revision',''),
             'FILTERED_SELECTION_V1'
           ), 'sha256'), 'hex'),
           r.scanned_before + r.row_no,
           0,
           'CLIENT',
           CASE WHEN pg_input_is_valid(coalesce(r.row_json->>'client_id',''), 'uuid')
             THEN (r.row_json->>'client_id')::uuid END,
           'QUEUED',
           greatest(coalesce(r.priority, 600), 600),
           v_now,
           (CASE WHEN jsonb_typeof(r.row_json->'command_payload') = 'object'
             THEN r.row_json->'command_payload' ELSE '{}'::jsonb END)
           || jsonb_build_object(
             'command_type', 'GENERATE_SELECTED',
             'selection_key', r.row_json->>'selection_key',
             'parent_expander_chunk_id', r.chunk_id,
             'row_kind', coalesce(r.row_json->>'row_kind','CREATE_INVOICE'),
             'client_id', r.row_json->>'client_id',
             'client_name', r.row_json->>'client_name',
             'candidate_display', r.row_json->>'candidate_display',
             'week_ending_display', r.row_json->>'week_ending_display',
             'currency', coalesce(r.row_json->>'currency','GBP'),
             'total_ex_vat', r.row_json->'total_ex_vat',
             'total_inc_vat', r.row_json->'total_inc_vat',
             'is_early', coalesce(r.row_json->'is_early','false'::jsonb),
             'source_revision', coalesce(r.row_json->>'source_revision', r.row_json#>>'{command_payload,source_revision}')
           ),
           jsonb_build_object(
             'status_message', 'Queued from filtered selection',
             'selection_key', r.row_json->>'selection_key'
           ),
           r.control_version,
           v_now,
           v_now
    FROM create_rows r
    ON CONFLICT DO NOTHING
    RETURNING (payload_json->>'parent_expander_chunk_id')::uuid parent_expander_chunk_id,
              id chunk_id,
              status
  ),
  document_request_groups AS MATERIALIZED (
    SELECT r.chunk_id parent_expander_chunk_id,
           r.operation_id,
           coalesce(jsonb_agg(jsonb_build_object(
             'request_key', r.row_json->>'selection_key',
             'invoice_id', r.row_json->>'invoice_id',
             'document_revision', coalesce(r.row_json->>'document_revision', r.row_json->>'source_revision'),
             'purpose', 'DRAFT_PREVIEW',
             'priority', greatest(coalesce(r.priority, 550), 550),
             'parent_operation_id', r.operation_id,
             'actor_user_id', r.actor_user_id,
             'template_version', 'invoice-professional-v1',
             'selection_key', r.row_json->>'selection_key',
             'row_kind', r.row_json->>'row_kind',
             'client_name', r.row_json->>'client_name',
             'candidate_display', r.row_json->>'candidate_display',
             'week_ending_display', r.row_json->>'week_ending_display',
             'currency', coalesce(r.row_json->>'currency','GBP'),
             'total_ex_vat', r.row_json->'total_ex_vat',
             'total_inc_vat', r.row_json->'total_inc_vat'
           ) ORDER BY r.row_no), '[]'::jsonb) requests
    FROM document_rows r
    GROUP BY r.chunk_id, r.operation_id
    HAVING count(*) > 0
  ),
  ensured_documents AS MATERIALIZED (
    SELECT g.parent_expander_chunk_id,
           g.operation_id,
           private._invoice_document_operation_ensure_batch(g.requests, v_now) ensure_json
    FROM document_request_groups g
    WHERE jsonb_array_length(g.requests) > 0
  ),
  ensure_result_rows AS MATERIALIZED (
    SELECT e.parent_expander_chunk_id,
           e.operation_id,
           item.value result_json
    FROM ensured_documents e
    CROSS JOIN LATERAL jsonb_array_elements(
      CASE WHEN jsonb_typeof(e.ensure_json->'results') = 'array'
        THEN e.ensure_json->'results' ELSE '[]'::jsonb END
    ) item(value)
  ),
  per_expander AS MATERIALIZED (
    SELECT hp.chunk_id,
           hp.operation_id,
           lower(coalesce(hp.page_json#>>'{page,has_more}','false')) IN ('true','t','1','yes','on') has_more,
           CASE WHEN jsonb_typeof(hp.page_json#>'{page,next_cursor_values}') = 'object'
             THEN hp.page_json#>'{page,next_cursor_values}' ELSE '{}'::jsonb END next_cursor,
           coalesce((SELECT count(*)::integer FROM page_rows r WHERE r.chunk_id = hp.chunk_id), 0) selected_count,
           coalesce((SELECT count(*)::integer FROM inserted_generation_chunks ig WHERE ig.parent_expander_chunk_id = hp.chunk_id), 0) generated_queue_count,
           coalesce((SELECT sum(CASE WHEN coalesce(ed.ensure_json->>'created_count','') ~ '^[0-9]{1,9}$' THEN (ed.ensure_json->>'created_count')::integer ELSE 0 END)::integer FROM ensured_documents ed WHERE ed.parent_expander_chunk_id = hp.chunk_id), 0) document_created_count,
           coalesce((SELECT sum(CASE WHEN coalesce(ed.ensure_json->>'ready_count','') ~ '^[0-9]{1,9}$' THEN (ed.ensure_json->>'ready_count')::integer ELSE 0 END)::integer FROM ensured_documents ed WHERE ed.parent_expander_chunk_id = hp.chunk_id), 0) document_ready_count,
           coalesce((SELECT sum(CASE WHEN coalesce(ed.ensure_json->>'active_count','') ~ '^[0-9]{1,9}$' THEN (ed.ensure_json->>'active_count')::integer ELSE 0 END)::integer FROM ensured_documents ed WHERE ed.parent_expander_chunk_id = hp.chunk_id), 0) document_active_count,
           coalesce((SELECT sum(CASE WHEN coalesce(ed.ensure_json->>'blocked_count','') ~ '^[0-9]{1,9}$' THEN (ed.ensure_json->>'blocked_count')::integer ELSE 0 END)::integer FROM ensured_documents ed WHERE ed.parent_expander_chunk_id = hp.chunk_id), 0) document_blocked_count,
           hp.scanned_before,
           hp.selected_before,
           hp.queued_before,
           hp.blocked_before,
           hp.changed_before,
           hp.already_active_before,
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
          'queued', p.queued_before + p.generated_queue_count + p.document_created_count,
          'blocked', p.blocked_before + p.document_blocked_count,
          'changed', p.changed_before,
          'already_active', p.already_active_before + p.document_active_count + p.document_ready_count,
          'completed', NOT p.has_more,
          'last_expanded_at_utc', v_now,
          'last_page', jsonb_build_object(
            'selected', p.selected_count,
            'queued_generation', p.generated_queue_count,
            'document_created', p.document_created_count,
            'document_ready', p.document_ready_count,
            'document_active', p.document_active_count,
            'document_blocked', p.document_blocked_count,
            'has_more', p.has_more
          )
        ),
        progress_json = jsonb_build_object(
          'status_message', CASE WHEN p.has_more THEN 'Expanding selection' ELSE 'Selection expansion complete' END,
          'selected_total', p.selected_before + p.selected_count,
          'queued_total', p.queued_before + p.generated_queue_count + p.document_created_count,
          'blocked_total', p.blocked_before + p.document_blocked_count,
          'already_active_total', p.already_active_before + p.document_active_count + p.document_ready_count,
          'selection_expansion_pending', p.has_more
        ),
        result_json = coalesce(c.result_json,'{}'::jsonb) || jsonb_build_object(
          'selection_expansion_pending', p.has_more,
          'selected_total', p.selected_before + p.selected_count,
          'queued_total', p.queued_before + p.generated_queue_count + p.document_created_count,
          'blocked_total', p.blocked_before + p.document_blocked_count,
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
           sum(p.generated_queue_count + p.document_created_count)::integer queued_count,
           sum(p.document_blocked_count)::integer blocked_count,
           bool_or(p.has_more) has_more
    FROM per_expander p
    GROUP BY p.operation_id
  ),
  updated_operations AS MATERIALIZED (
    UPDATE public.invoice_operations o
    SET progress_json = coalesce(o.progress_json,'{}'::jsonb) || jsonb_build_object(
          'status_message', CASE WHEN op.has_more THEN 'Expanding selection' ELSE 'Selection expanded' END,
          'selection_expansion_pending', op.has_more,
          'selected_total', (CASE WHEN coalesce(o.progress_json->>'selected_total','') ~ '^[0-9]{1,9}$' THEN (o.progress_json->>'selected_total')::integer ELSE 0 END) + op.selected_count,
          'queued_total', (CASE WHEN coalesce(o.progress_json->>'queued_total','') ~ '^[0-9]{1,9}$' THEN (o.progress_json->>'queued_total')::integer ELSE 0 END) + op.queued_count,
          'blocked_total', (CASE WHEN coalesce(o.progress_json->>'blocked_total','') ~ '^[0-9]{1,9}$' THEN (o.progress_json->>'blocked_total')::integer ELSE 0 END) + op.blocked_count
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
    v_result := v_result || private._invoice_generation_advance_batch_legacy_20260726(v_passthrough, v_now);
  END IF;

  RETURN coalesce(v_result, '[]'::jsonb);
END;
$function$;

REVOKE ALL ON FUNCTION private._invoice_generation_advance_batch(jsonb, timestamp with time zone) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION private._invoice_generation_advance_batch(jsonb, timestamp with time zone) TO service_role;
