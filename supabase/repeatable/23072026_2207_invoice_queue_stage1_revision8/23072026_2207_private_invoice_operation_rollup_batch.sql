-- CloudTMS invoice batch private rollup replacement
-- Pulled from live TEST DB signature and wrapped for the locked Batch Generate / Batch Issue progress contract.
-- Install after the selection-expansion worker replacements.
-- This file preserves the current live implementation as private._invoice_operation_rollup_batch_legacy_20260726
-- and then adds compact commercial batch progress/result counters while leaving all existing rollup behaviour intact.

DO $preserve_legacy$
DECLARE
  v_def text;
BEGIN
  IF to_regprocedure('private._invoice_operation_rollup_batch_legacy_20260726(uuid[], timestamp with time zone, boolean)') IS NULL THEN
    SELECT pg_get_functiondef(p.oid)
      INTO v_def
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'private'
      AND p.proname = '_invoice_operation_rollup_batch'
      AND pg_get_function_identity_arguments(p.oid) = 'p_operation_ids uuid[], p_now_utc timestamp with time zone, p_propagate_ancestors boolean';

    IF v_def IS NULL THEN
      RAISE EXCEPTION 'Cannot preserve legacy private._invoice_operation_rollup_batch(uuid[], timestamptz, boolean): source function not found and legacy copy missing';
    END IF;

    IF position('_invoice_operation_rollup_batch_legacy_20260726' IN v_def) > 0 THEN
      RAISE EXCEPTION 'Refusing to clone an already-wrapped private._invoice_operation_rollup_batch; legacy copy is missing';
    END IF;

    v_def := regexp_replace(
      v_def,
      '^CREATE OR REPLACE FUNCTION private\._invoice_operation_rollup_batch',
      'CREATE OR REPLACE FUNCTION private._invoice_operation_rollup_batch_legacy_20260726'
    );

    EXECUTE v_def;
    EXECUTE 'REVOKE ALL ON FUNCTION private._invoice_operation_rollup_batch_legacy_20260726(uuid[], timestamp with time zone, boolean) FROM PUBLIC, anon, authenticated';
    EXECUTE 'GRANT EXECUTE ON FUNCTION private._invoice_operation_rollup_batch_legacy_20260726(uuid[], timestamp with time zone, boolean) TO service_role';
  END IF;
END
$preserve_legacy$;

CREATE OR REPLACE FUNCTION private._invoice_operation_rollup_batch(
  p_operation_ids uuid[],
  p_now_utc timestamp with time zone DEFAULT now(),
  p_propagate_ancestors boolean DEFAULT true
)
RETURNS TABLE(
  operation_id uuid,
  status text,
  phase text,
  total_units integer,
  completed_units integer,
  failed_units integer,
  blocked_required_count integer,
  requires_user_action boolean,
  change_seq bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := coalesce(p_now_utc, now());
BEGIN
  RETURN QUERY
  WITH RECURSIVE
  legacy_rows AS MATERIALIZED (
    SELECT *
    FROM private._invoice_operation_rollup_batch_legacy_20260726(
      p_operation_ids,
      v_now,
      p_propagate_ancestors
    ) legacy_result
  ),
  roots AS MATERIALIZED (
    SELECT DISTINCT lr.operation_id
    FROM legacy_rows lr
    JOIN public.invoice_operations o ON o.id = lr.operation_id
    WHERE o.operation_type IN ('GENERATE_INVOICES','ISSUE_INVOICES')
      AND o.entity_type = 'INVOICE_BATCH'
      AND coalesce(o.input_json->>'contract_version','') = 'INVOICE_BATCH_SELECTION_ROOT_V1'
  ),
  descendants(root_operation_id, operation_id, depth, path) AS (
    SELECT r.operation_id, r.operation_id, 0, array[r.operation_id]::uuid[]
    FROM roots r
    UNION ALL
    SELECT d.root_operation_id, child.id, d.depth + 1, d.path || child.id
    FROM descendants d
    JOIN public.invoice_operations child ON child.parent_operation_id = d.operation_id
    WHERE d.depth < 16
      AND NOT child.id = ANY(d.path)
  ),
  chunks AS MATERIALIZED (
    SELECT d.root_operation_id,
           d.operation_id,
           op.operation_type descendant_operation_type,
           op.input_json descendant_input_json,
           c.id chunk_id,
           c.chunk_type,
           c.phase,
           c.status,
           c.entity_type,
           c.entity_id,
           c.payload_json,
           c.result_json,
           c.error_json,
           c.document_version_id,
           c.updated_at_utc,
           coalesce(c.payload_json->>'is_selection_expander','false') IN ('true','t','1','yes','on') is_selection_expander
    FROM descendants d
    JOIN public.invoice_operation_chunks c ON c.operation_id = d.operation_id
    JOIN public.invoice_operations op ON op.id = d.operation_id
    WHERE c.replaced_by_chunk_id IS NULL
  ),
  expander_payloads AS MATERIALIZED (
    SELECT c.root_operation_id,
           sum(CASE WHEN coalesce(c.payload_json->>'selected','') ~ '^[0-9]{1,9}$' THEN (c.payload_json->>'selected')::integer ELSE 0 END)::integer selected_total,
           sum(CASE WHEN coalesce(c.payload_json->>'queued','') ~ '^[0-9]{1,9}$' THEN (c.payload_json->>'queued')::integer ELSE 0 END)::integer queued_total,
           sum(CASE WHEN coalesce(c.payload_json->>'blocked','') ~ '^[0-9]{1,9}$' THEN (c.payload_json->>'blocked')::integer ELSE 0 END)::integer expander_blocked_total,
           sum(CASE WHEN coalesce(c.payload_json->>'changed','') ~ '^[0-9]{1,9}$' THEN (c.payload_json->>'changed')::integer ELSE 0 END)::integer expander_changed_total,
           sum(CASE WHEN coalesce(c.payload_json->>'already_active','') ~ '^[0-9]{1,9}$' THEN (c.payload_json->>'already_active')::integer ELSE 0 END)::integer already_active_total,
           bool_or(c.status IN ('QUEUED','RUNNING','WAITING','RETRY_WAIT')) selection_expansion_pending
    FROM chunks c
    WHERE c.is_selection_expander
    GROUP BY c.root_operation_id
  ),
  generation_counts AS MATERIALIZED (
    SELECT r.operation_id root_operation_id,
           count(c.chunk_id) FILTER (
             WHERE c.chunk_type = 'GENERATION_GROUP'
               AND NOT c.is_selection_expander
           )::integer
           + count(DISTINCT c.operation_id) FILTER (
             WHERE c.descendant_operation_type = 'BUILD_DOCUMENT'
               AND c.descendant_input_json->>'created_by' = 'private._invoice_document_operation_ensure_batch'
           )::integer expanded_total,
           count(c.chunk_id) FILTER (
             WHERE NOT c.is_selection_expander
               AND (
                 c.chunk_type = 'GENERATION_GROUP'
                 OR (c.descendant_operation_type = 'BUILD_DOCUMENT'
                   AND c.descendant_input_json->>'created_by' = 'private._invoice_document_operation_ensure_batch')
               )
               AND c.status IN ('QUEUED','RUNNING','WAITING','RETRY_WAIT')
           )::integer generation_in_progress_total,
           count(c.chunk_id) FILTER (
             WHERE c.chunk_type = 'GENERATION_GROUP'
               AND NOT c.is_selection_expander
               AND c.status = 'COMPLETE'
               AND coalesce(c.payload_json->>'row_kind','CREATE_INVOICE') = 'CREATE_INVOICE'
           )::integer generated_total,
           count(c.chunk_id) FILTER (
             WHERE c.chunk_type = 'DOCUMENT_PLAN'
               AND c.descendant_operation_type = 'BUILD_DOCUMENT'
               AND c.descendant_input_json->>'created_by' = 'private._invoice_document_operation_ensure_batch'
               AND c.status = 'COMPLETE'
           )::integer regenerated_total,
           count(c.chunk_id) FILTER (WHERE NOT c.is_selection_expander AND c.status = 'SUPERSEDED')::integer changed_total,
           count(c.chunk_id) FILTER (WHERE NOT c.is_selection_expander AND c.status = 'BLOCKED')::integer blocked_total,
           count(c.chunk_id) FILTER (WHERE NOT c.is_selection_expander AND c.status IN ('FAILED','DEAD_LETTER'))::integer failed_total
    FROM roots r
    LEFT JOIN chunks c ON c.root_operation_id = r.operation_id
    JOIN public.invoice_operations o ON o.id = r.operation_id
    WHERE o.operation_type = 'GENERATE_INVOICES'
    GROUP BY r.operation_id
  ),
  issue_counts AS MATERIALIZED (
    SELECT r.operation_id root_operation_id,
           count(c.chunk_id) FILTER (WHERE c.chunk_type = 'ISSUE_INVOICE' AND NOT c.is_selection_expander)::integer expanded_total,
           count(c.chunk_id) FILTER (WHERE c.chunk_type = 'ISSUE_INVOICE' AND NOT c.is_selection_expander AND c.status IN ('QUEUED','RUNNING','WAITING','RETRY_WAIT'))::integer issue_in_progress_total,
           count(c.chunk_id) FILTER (WHERE c.chunk_type = 'ISSUE_INVOICE' AND NOT c.is_selection_expander AND c.status = 'COMPLETE')::integer issued_total,
           count(c.chunk_id) FILTER (WHERE c.chunk_type = 'ISSUE_INVOICE' AND NOT c.is_selection_expander AND c.status = 'COMPLETE' AND lower(coalesce(c.payload_json->>'blocked_for_sending','false')) IN ('true','t','1','yes','on'))::integer issued_send_blocked_total,
           count(c.chunk_id) FILTER (WHERE c.chunk_type = 'ISSUE_INVOICE' AND NOT c.is_selection_expander AND c.status = 'SUPERSEDED')::integer changed_total,
           count(c.chunk_id) FILTER (WHERE c.chunk_type = 'ISSUE_INVOICE' AND NOT c.is_selection_expander AND c.status = 'BLOCKED')::integer blocked_total,
           count(c.chunk_id) FILTER (WHERE c.chunk_type = 'ISSUE_INVOICE' AND NOT c.is_selection_expander AND c.status IN ('FAILED','DEAD_LETTER'))::integer failed_total,
           count(c.chunk_id) FILTER (WHERE c.chunk_type = 'DELIVERY_PREPARE' AND c.status IN ('QUEUED','RUNNING','WAITING','RETRY_WAIT'))::integer delivery_pending_total,
           count(c.chunk_id) FILTER (WHERE c.chunk_type = 'DELIVERY_PREPARE' AND c.status = 'COMPLETE')::integer delivery_complete_total,
           count(c.chunk_id) FILTER (WHERE c.chunk_type = 'DELIVERY_PREPARE' AND c.status IN ('BLOCKED','FAILED','DEAD_LETTER'))::integer delivery_blocked_total
    FROM roots r
    LEFT JOIN chunks c ON c.root_operation_id = r.operation_id
    JOIN public.invoice_operations o ON o.id = r.operation_id
    WHERE o.operation_type = 'ISSUE_INVOICES'
    GROUP BY r.operation_id
  ),
  batch_progress AS MATERIALIZED (
    SELECT o.id operation_id,
           CASE WHEN o.operation_type = 'GENERATE_INVOICES' THEN jsonb_build_object(
             'candidate_total', coalesce(ep.selected_total, 0) + coalesce(ep.expander_blocked_total, 0) + coalesce(ep.expander_changed_total, 0),
             'selected_total', coalesce(ep.selected_total, 0),
             'expanded_total', coalesce(gc.expanded_total, 0),
             'queued_total', coalesce(ep.queued_total, 0),
             'generated_total', coalesce(gc.generated_total, 0),
             'regenerated_total', coalesce(gc.regenerated_total, 0),
             'already_active_total', coalesce(ep.already_active_total, 0),
             'blocked_total', coalesce(ep.expander_blocked_total, 0) + coalesce(gc.blocked_total, 0),
             'changed_total', coalesce(ep.expander_changed_total, 0) + coalesce(gc.changed_total, 0),
             'failed_total', coalesce(gc.failed_total, 0),
             'in_progress_total', coalesce(gc.generation_in_progress_total, 0),
             'selection_expansion_pending', coalesce(ep.selection_expansion_pending, false)
           ) ELSE jsonb_build_object(
             'invoice_total', coalesce(ep.selected_total, 0) + coalesce(ep.expander_blocked_total, 0) + coalesce(ep.expander_changed_total, 0),
             'selected_total', coalesce(ep.selected_total, 0),
             'expanded_total', coalesce(ic.expanded_total, 0),
             'issued_total', coalesce(ic.issued_total, 0),
             'issued_send_blocked_total', coalesce(ic.issued_send_blocked_total, 0),
             'already_active_total', coalesce(ep.already_active_total, 0),
             'blocked_total', coalesce(ep.expander_blocked_total, 0) + coalesce(ic.blocked_total, 0),
             'changed_total', coalesce(ep.expander_changed_total, 0) + coalesce(ic.changed_total, 0),
             'failed_total', coalesce(ic.failed_total, 0),
             'delivery_pending_total', coalesce(ic.delivery_pending_total, 0),
             'delivery_complete_total', coalesce(ic.delivery_complete_total, 0),
             'delivery_blocked_total', coalesce(ic.delivery_blocked_total, 0),
             'selection_expansion_pending', coalesce(ep.selection_expansion_pending, false)
           ) END progress_patch
    FROM public.invoice_operations o
    JOIN roots r ON r.operation_id = o.id
    LEFT JOIN expander_payloads ep ON ep.root_operation_id = o.id
    LEFT JOIN generation_counts gc ON gc.root_operation_id = o.id
    LEFT JOIN issue_counts ic ON ic.root_operation_id = o.id
  ),
  updated_batch_roots AS MATERIALIZED (
    UPDATE public.invoice_operations o
    SET progress_json = coalesce(o.progress_json,'{}'::jsonb) || bp.progress_patch,
        result_json = coalesce(o.result_json,'{}'::jsonb) || jsonb_build_object('batch_progress', bp.progress_patch),
        updated_at_utc = CASE
          WHEN coalesce(o.progress_json,'{}'::jsonb) IS DISTINCT FROM (coalesce(o.progress_json,'{}'::jsonb) || bp.progress_patch)
          THEN v_now ELSE o.updated_at_utc END,
        change_seq = CASE
          WHEN coalesce(o.progress_json,'{}'::jsonb) IS DISTINCT FROM (coalesce(o.progress_json,'{}'::jsonb) || bp.progress_patch)
          THEN nextval('public.invoice_operation_change_seq') ELSE o.change_seq END
    FROM batch_progress bp
    WHERE o.id = bp.operation_id
    RETURNING o.id, o.change_seq
  )
  SELECT coalesce(ub.id, o.id) AS id,
         o.status,
         o.phase,
         o.total_units,
         o.completed_units,
         o.failed_units,
         CASE WHEN coalesce(o.progress_json->>'blocked_required_count','') ~ '^[0-9]{1,9}$'
           THEN (o.progress_json->>'blocked_required_count')::integer ELSE 0 END blocked_required_count,
         o.requires_user_action,
         coalesce(ub.change_seq, o.change_seq) AS change_seq
  FROM legacy_rows lr
  JOIN public.invoice_operations o ON o.id = lr.operation_id
  LEFT JOIN updated_batch_roots ub ON ub.id = o.id
  ORDER BY coalesce(ub.id, o.id);
END;
$function$;

REVOKE ALL ON FUNCTION private._invoice_operation_rollup_batch(uuid[], timestamp with time zone, boolean) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION private._invoice_operation_rollup_batch(uuid[], timestamp with time zone, boolean) TO service_role;
