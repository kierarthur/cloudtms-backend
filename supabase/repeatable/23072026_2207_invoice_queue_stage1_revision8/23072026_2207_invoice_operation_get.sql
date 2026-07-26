-- CloudTMS invoice batch public RPC replacement
-- Generated 2026-07-26 from live TEST DB signatures plus locked invoice batch implementation plan.
-- Install after the v3 private helper package and modal-query-support migration are installed.
-- The preservation block keeps a private copy of the live legacy implementation so existing callers remain compatible.

DO $preserve_legacy$
DECLARE
  v_def text;
BEGIN
  IF to_regprocedure('private._invoice_operation_get_legacy_20260726(uuid[], uuid, text)') IS NULL THEN
    SELECT pg_get_functiondef(p.oid)
      INTO v_def
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public'
      AND p.proname = 'invoice_operation_get'
      AND pg_get_function_identity_arguments(p.oid) = 'p_operation_ids uuid[], p_actor_user_id uuid, p_mode text';

    IF v_def IS NULL THEN
      RAISE EXCEPTION 'Cannot preserve legacy public.invoice_operation_get(uuid[], uuid, text): source function not found and private legacy copy missing';
    END IF;

    IF position('_invoice_operation_get_legacy_20260726' IN v_def) > 0 THEN
      RAISE EXCEPTION 'Refusing to clone an already-wrapped public.invoice_operation_get; private legacy copy is missing';
    END IF;

    v_def := regexp_replace(
      v_def,
      '^CREATE OR REPLACE FUNCTION public\.invoice_operation_get',
      'CREATE OR REPLACE FUNCTION private._invoice_operation_get_legacy_20260726'
    );

    EXECUTE v_def;
    EXECUTE 'REVOKE ALL ON FUNCTION private._invoice_operation_get_legacy_20260726(uuid[], uuid, text) FROM PUBLIC, anon, authenticated';
    EXECUTE 'GRANT EXECUTE ON FUNCTION private._invoice_operation_get_legacy_20260726(uuid[], uuid, text) TO service_role';
  END IF;
END
$preserve_legacy$;

DROP FUNCTION IF EXISTS public.invoice_operation_get(uuid[], uuid, text);

CREATE OR REPLACE FUNCTION public.invoice_operation_get(
  p_operation_ids uuid[],
  p_actor_user_id uuid,
  p_mode text DEFAULT 'PROGRESS'::text,
  p_page_request jsonb DEFAULT NULL::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_mode text := upper(btrim(coalesce(p_mode, 'PROGRESS')));
  v_service boolean := coalesce(auth.role(), '') = 'service_role';
  v_role text;
  v_operations jsonb;
  v_category text;
  v_after_selection_key text;
  v_after_chunk_id uuid;
  v_limit integer;
  v_result_page jsonb;
BEGIN
  IF p_page_request IS NULL THEN
    RETURN private._invoice_operation_get_legacy_20260726(p_operation_ids, p_actor_user_id, p_mode);
  END IF;

  IF cardinality(coalesce(p_operation_ids, ARRAY[]::uuid[])) < 1
     OR cardinality(p_operation_ids) > 100 THEN
    RAISE EXCEPTION USING errcode = '22023', message = 'p_operation_ids must contain 1..100 IDs';
  END IF;

  IF NOT v_service
     AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_actor_user_id) THEN
    RAISE EXCEPTION USING errcode = '42501', message = 'Authenticated actor mismatch';
  END IF;

  SELECT lower(btrim(coalesce(u.role, '')))
    INTO v_role
  FROM public.tms_users u
  WHERE u.id = p_actor_user_id
    AND u.is_active;

  IF NOT FOUND AND NOT v_service THEN
    RAISE EXCEPTION USING errcode = '42501', message = 'Active actor required';
  END IF;

  IF jsonb_typeof(p_page_request) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION USING errcode = '22023', message = 'OPERATION_RESULT_PAGE_REQUEST_INVALID';
  END IF;

  v_category := upper(coalesce(nullif(p_page_request->>'category', ''), 'ALL'));
  IF v_category NOT IN (
    'ALL','READY','IN_PROGRESS','COMPLETED','BLOCKED','FAILED','CHANGED','ISSUED','ISSUED_SEND_BLOCKED'
  ) THEN
    RAISE EXCEPTION USING errcode = '22023', message = 'OPERATION_RESULT_CATEGORY_INVALID';
  END IF;

  v_after_selection_key := nullif(btrim(coalesce(
    p_page_request->>'after_selection_key',
    p_page_request#>>'{cursor,after_selection_key}',
    ''
  )), '');

  IF coalesce(p_page_request->>'after_chunk_id', p_page_request#>>'{cursor,after_chunk_id}', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_after_chunk_id := coalesce(p_page_request->>'after_chunk_id', p_page_request#>>'{cursor,after_chunk_id}')::uuid;
  ELSE
    v_after_chunk_id := NULL;
  END IF;

  v_limit := CASE
    WHEN coalesce(p_page_request->>'limit', '') ~ '^[1-9][0-9]{0,8}$'
      THEN greatest(1, least((p_page_request->>'limit')::integer, 100))
    ELSE 100
  END;

  v_operations := private._invoice_operation_get_legacy_20260726(p_operation_ids, p_actor_user_id, p_mode);

  WITH RECURSIVE requested AS MATERIALIZED (
    SELECT id, min(ordinality) ordinality
    FROM unnest(p_operation_ids) WITH ORDINALITY x(id, ordinality)
    GROUP BY id
  ),
  authorised AS MATERIALIZED (
    SELECT o.*, r.ordinality
    FROM requested r
    JOIN public.invoice_operations o ON o.id = r.id
    WHERE v_service OR v_role = 'admin' OR o.actor_user_id = p_actor_user_id
  ),
  descendants(root_id, parent_id, id, depth, path) AS MATERIALIZED (
    SELECT a.id, ch.parent_operation_id, ch.id, 1, ARRAY[a.id, ch.id]::uuid[]
    FROM authorised a
    JOIN public.invoice_operations ch ON ch.parent_operation_id = a.id
    UNION ALL
    SELECT d.root_id, ch.parent_operation_id, ch.id, d.depth + 1, d.path || ch.id
    FROM descendants d
    JOIN public.invoice_operations ch ON ch.parent_operation_id = d.id
    WHERE NOT ch.id = ANY(d.path)
  ),
  scope_operation_ids AS MATERIALIZED (
    SELECT id operation_id FROM authorised
    UNION
    SELECT id operation_id FROM descendants
  ),
  scope_array AS MATERIALIZED (
    SELECT coalesce(array_agg(operation_id ORDER BY operation_id), ARRAY[]::uuid[]) operation_ids
    FROM scope_operation_ids
  ),
  current_slots AS MATERIALIZED (
    SELECT slot.*
    FROM scope_array s
    CROSS JOIN LATERAL private._invoice_current_chunks_batch(s.operation_ids, NULL, NULL, 10000) slot
  ),
  current_chunks AS MATERIALIZED (
    SELECT
      slot.logical_slot_key,
      coalesce(c.id, slot.current_chunk_id) chunk_id,
      slot.operation_id,
      slot.chunk_type,
      slot.level_no,
      slot.sequence_no,
      slot.work_key,
      slot.plan_generation,
      slot.entity_type,
      slot.entity_id,
      slot.document_version_id,
      slot.document_asset_id,
      slot.input_document_version_id,
      CASE WHEN slot.replacement_chain_status = 'INVALID' THEN 'BLOCKED' ELSE c.status END status,
      CASE WHEN slot.replacement_chain_status = 'INVALID' THEN 'REPLACEMENT_VALIDATION' ELSE c.phase END phase,
      c.payload_json,
      c.progress_json,
      c.result_json,
      CASE WHEN slot.replacement_chain_status = 'INVALID' THEN slot.replacement_chain_error ELSE c.error_json END error_json,
      c.created_at_utc,
      c.updated_at_utc
    FROM current_slots slot
    LEFT JOIN public.invoice_operation_chunks c ON c.id = slot.current_chunk_id
  ),
  row_base AS MATERIALIZED (
    SELECT
      coalesce(
        nullif(c.payload_json->>'selection_key', ''),
        CASE WHEN c.entity_id IS NOT NULL THEN lower(c.chunk_type) || ':' || c.entity_id::text END,
        c.chunk_type || ':' || c.chunk_id::text
      ) selection_key,
      c.chunk_id,
      c.operation_id,
      c.chunk_type,
      c.entity_type,
      c.entity_id,
      CASE
        WHEN c.entity_type = 'INVOICE' THEN c.entity_id
        WHEN coalesce(c.payload_json->>'invoice_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (c.payload_json->>'invoice_id')::uuid
        ELSE NULL::uuid
      END invoice_id,
      c.document_version_id,
      c.payload_json,
      c.progress_json,
      c.result_json,
      c.error_json,
      c.status,
      c.phase,
      lower(coalesce(c.payload_json->>'is_selection_expander','false')) IN ('true','t','1','yes','on') is_selection_expander,
      lower(coalesce(c.payload_json->>'blocked_for_sending', c.result_json->>'blocked_for_sending', 'false')) IN ('true','t','1','yes','on') blocked_for_sending,
      c.created_at_utc,
      c.updated_at_utc
    FROM current_chunks c
  ),
  enriched AS MATERIALIZED (
    SELECT
      r.selection_key,
      r.chunk_id,
      r.operation_id,
      r.chunk_type,
      r.entity_type,
      r.entity_id,
      r.invoice_id,
      coalesce(r.payload_json->>'invoice_number', i.invoice_no) invoice_number,
      coalesce(r.payload_json->>'client_name', cl.name) client_name,
      coalesce(r.payload_json->>'candidate_display', r.payload_json->>'candidate_name') candidate_display,
      coalesce(r.payload_json->>'week_ending_display', r.payload_json->>'week_ending_date') week_ending_display,
      coalesce(nullif(r.payload_json->>'currency',''), 'GBP') currency,
      CASE WHEN coalesce(r.payload_json->>'total_ex_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        THEN (r.payload_json->>'total_ex_vat')::numeric
        ELSE i.subtotal_ex_vat END total_ex_vat,
      CASE WHEN coalesce(r.payload_json->>'total_inc_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        THEN (r.payload_json->>'total_inc_vat')::numeric
        ELSE i.total_inc_vat END total_inc_vat,
      r.payload_json->>'row_kind' row_kind,
      r.status,
      r.phase,
      coalesce(
        CASE WHEN jsonb_typeof(r.payload_json->'badge_codes')='array' THEN r.payload_json->'badge_codes' END,
        CASE WHEN jsonb_typeof(r.payload_json->'action_blocker_codes')='array' THEN r.payload_json->'action_blocker_codes' END,
        CASE WHEN r.error_json ? 'code' THEN jsonb_build_array(r.error_json->>'code') END,
        '[]'::jsonb
      ) badge_codes,
      r.error_json->>'code' error_code,
      CASE
        WHEN coalesce(r.result_json->>'issued_document_version_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (r.result_json->>'issued_document_version_id')::uuid
        WHEN coalesce(r.result_json->>'document_version_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (r.result_json->>'document_version_id')::uuid
        ELSE r.document_version_id
      END resolved_document_version_id,
      r.blocked_for_sending,
      r.is_selection_expander,
      r.created_at_utc,
      r.updated_at_utc
    FROM row_base r
    LEFT JOIN public.invoices i ON i.id = r.invoice_id
    LEFT JOIN public.clients cl ON cl.id = i.client_id
    WHERE NOT r.is_selection_expander
  ),
  categorised AS MATERIALIZED (
    SELECT e.*,
      CASE
        WHEN e.status IN ('QUEUED','RUNNING','WAITING','RETRY_WAIT') THEN 'IN_PROGRESS'
        WHEN e.status = 'COMPLETE' THEN 'COMPLETED'
        WHEN e.status = 'BLOCKED' THEN 'BLOCKED'
        WHEN e.status IN ('FAILED','DEAD_LETTER') THEN 'FAILED'
        ELSE e.status
      END commercial_category,
      (
        e.error_code IN ('SOURCE_CHANGED','CURRENT_STATE_CHANGED','INVOICE_CHANGED','DOCUMENT_REVISION_CHANGED')
        OR e.status = 'SUPERSEDED'
        OR coalesce(e.payload_json->>'outcome', e.result_json->>'outcome', '') IN ('SOURCE_CHANGED','CHANGED','SKIPPED_CHANGED')
      ) is_changed
    FROM enriched e
  ),
  matching AS MATERIALIZED (
    SELECT c.*
    FROM categorised c
    WHERE
      v_category = 'ALL'
      OR (v_category = 'READY' AND c.status = 'COMPLETE')
      OR (v_category = 'COMPLETED' AND c.status = 'COMPLETE')
      OR (v_category = 'IN_PROGRESS' AND c.status IN ('QUEUED','RUNNING','WAITING','RETRY_WAIT'))
      OR (v_category = 'BLOCKED' AND c.status = 'BLOCKED')
      OR (v_category = 'FAILED' AND c.status IN ('FAILED','DEAD_LETTER'))
      OR (v_category = 'CHANGED' AND c.is_changed)
      OR (v_category = 'ISSUED' AND c.chunk_type='ISSUE_INVOICE' AND c.status='COMPLETE' AND NOT c.blocked_for_sending)
      OR (v_category = 'ISSUED_SEND_BLOCKED' AND c.chunk_type='ISSUE_INVOICE' AND c.status='COMPLETE' AND c.blocked_for_sending)
  ),
  cursor_filtered AS MATERIALIZED (
    SELECT m.*
    FROM matching m
    WHERE v_after_selection_key IS NULL
       OR (m.selection_key, m.chunk_id) > (v_after_selection_key, coalesce(v_after_chunk_id, '00000000-0000-0000-0000-000000000000'::uuid))
  ),
  ordered AS MATERIALIZED (
    SELECT cf.*, row_number() OVER (ORDER BY cf.selection_key, cf.chunk_id) rn
    FROM cursor_filtered cf
  ),
  page_rows AS MATERIALIZED (
    SELECT * FROM ordered WHERE rn <= v_limit + 1
  ),
  visible_rows AS MATERIALIZED (
    SELECT * FROM page_rows WHERE rn <= v_limit
  )
  SELECT jsonb_build_object(
    'category', v_category,
    'rows', coalesce((
      SELECT jsonb_agg(jsonb_build_object(
        'selection_key', selection_key,
        'chunk_id', chunk_id,
        'entity_type', entity_type,
        'entity_id', entity_id,
        'invoice_id', invoice_id,
        'invoice_number', invoice_number,
        'client_name', client_name,
        'candidate_display', candidate_display,
        'week_ending_display', week_ending_display,
        'currency', currency,
        'total_ex_vat', total_ex_vat,
        'total_inc_vat', total_inc_vat,
        'row_kind', row_kind,
        'status', status,
        'phase', phase,
        'badge_codes', badge_codes,
        'error_code', error_code,
        'document_version_id', resolved_document_version_id,
        'can_view', resolved_document_version_id IS NOT NULL AND status = 'COMPLETE',
        'blocked_for_sending', blocked_for_sending
      ) ORDER BY selection_key, chunk_id)
      FROM visible_rows
    ), '[]'::jsonb),
    'has_more', (SELECT count(*) FROM page_rows) > v_limit,
    'next_cursor_values', CASE WHEN (SELECT count(*) FROM page_rows) > v_limit THEN (
      SELECT jsonb_build_object(
        'after_selection_key', selection_key,
        'after_chunk_id', chunk_id
      )
      FROM visible_rows
      ORDER BY rn DESC
      LIMIT 1
    ) ELSE NULL END,
    'total_count', (SELECT count(*) FROM matching),
    'limit', v_limit
  ) INTO v_result_page;

  RETURN jsonb_build_object(
    'operations', coalesce(v_operations, '[]'::jsonb),
    'result_page', v_result_page
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.invoice_operation_get(uuid[], uuid, text, jsonb) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.invoice_operation_get(uuid[], uuid, text, jsonb) TO authenticated, service_role;
