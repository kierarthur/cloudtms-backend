-- Bounded snapshot pagination repair for the existing Create Draft owner.
-- Runtime authority is Miget TEST. The `supabase` directory name is historical only.
-- This replacement changes page orchestration only: existing snapshots are excluded
-- before LIMIT so each retry advances to the next untouched Timesheets.
CREATE OR REPLACE FUNCTION public.pay_batch_create_timesheet_snapshots(p_pay_batch_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_operation_id uuid DEFAULT NULL::uuid, p_candidate_scope_ids jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_scope_ids jsonb := COALESCE(p_candidate_scope_ids, '[]'::jsonb);
  v_scope_id_count integer := 0;
  v_inserted_count integer := 0;
  v_updated_count integer := 0;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_id is required';
  END IF;

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'p_operation_id is required for row-backed timesheet snapshot creation';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'p_actor_user_id is required for row-backed timesheet snapshot creation';
  END IF;

  IF p_candidate_scope_ids IS NULL OR jsonb_typeof(v_scope_ids) <> 'array' OR jsonb_array_length(v_scope_ids) = 0 THEN
    RAISE EXCEPTION 'p_candidate_scope_ids must be a non-empty JSON array';
  END IF;

  v_scope_id_count := jsonb_array_length(v_scope_ids);

  IF v_scope_id_count > 100 THEN
    RAISE EXCEPTION 'p_candidate_scope_ids exceeds the 100 row cap: %', v_scope_id_count;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
    WHERE NOT ((supplied_scope.scope_value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
  ) THEN
    RAISE EXCEPTION 'p_candidate_scope_ids must contain UUID strings only';
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_snapshot_page;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_batch_snapshot_page ON COMMIT DROP AS
  WITH scoped_items AS (
    SELECT
      pay_batch_item.id AS pay_batch_item_id,
      pay_batch_item.timesheet_id,
      pay_batch_item.pay_channel,
      pay_batch_item.frozen_component_snapshot_json,
      pay_batch_item.frozen_source_basis_json,
      pay_batch_item.frozen_component_key_type,
      pay_batch_item.frozen_component_key_value,
      pay_batch_item.frozen_target_amount_ex_vat,
      pay_batch_item.frozen_target_amount_vat,
      pay_batch_item.frozen_target_amount_inc_vat,
      pay_batch_candidate.candidate_id,
      allocation_row.allocation_basis_json
    FROM public.pay_batch_items AS pay_batch_item
    JOIN public.pay_batch_candidates AS pay_batch_candidate
      ON pay_batch_candidate.id = pay_batch_item.pay_batch_candidate_id
    JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      ON allocation_row.pay_batch_item_id = pay_batch_item.id
     AND allocation_row.operation_id = p_operation_id
    WHERE pay_batch_candidate.pay_batch_id = p_pay_batch_id
      AND allocation_row.candidate_scope_id IN (
        SELECT (supplied_scope.scope_value #>> '{}')::uuid
        FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
      )
      AND pay_batch_item.timesheet_id IS NOT NULL
      AND COALESCE(pay_batch_item.is_voided, false) = false
  ), touched_timesheets AS (
    SELECT DISTINCT scoped_items.timesheet_id, scoped_items.candidate_id, scoped_items.pay_channel
    FROM scoped_items
    WHERE NOT EXISTS (
      SELECT 1
      FROM public.pay_batch_timesheet_snapshots AS existing_snapshot
      WHERE existing_snapshot.pay_batch_id = p_pay_batch_id
        AND existing_snapshot.timesheet_id = scoped_items.timesheet_id
        AND existing_snapshot.pay_channel = scoped_items.pay_channel
    )
    ORDER BY scoped_items.timesheet_id, scoped_items.pay_channel
    LIMIT 100
  )
  SELECT touched_timesheets.*
  FROM touched_timesheets;

  PERFORM private.pay_workbench_draft_expected_effects_v1(
    p_operation_id,'CREATE_TIMESHEET_SNAPSHOTS','REGISTER',
    jsonb_build_array(
      jsonb_build_object('relation_name','pay_batch_timesheet_snapshots','operation','INSERT'),
      jsonb_build_object('relation_name','pay_batch_timesheet_snapshots','operation','UPDATE')
    ),jsonb_build_object('pay_batch_id',p_pay_batch_id)
  );

  WITH snapshot_rows AS (
    SELECT
      snapshot_page.timesheet_id,
      snapshot_page.candidate_id,
      snapshot_page.pay_channel,
      COALESCE(display_source.base_snapshot_json, '{}'::jsonb) AS base_snapshot_json,
      (
        jsonb_strip_nulls(
          jsonb_build_object(
            'source', 'pay_batch_items_frozen_artifacts',
            'timesheet_id', snapshot_page.timesheet_id::text,
            'candidate_id', snapshot_page.candidate_id::text,
            'pay_channel', snapshot_page.pay_channel,
            'resolved_components', COALESCE((
              SELECT jsonb_agg(
                jsonb_strip_nulls(
                  jsonb_build_object(
                    'pay_batch_item_id', component_item.id::text,
                    'component_key_type', component_item.frozen_component_key_type,
                    'component_key_value', component_item.frozen_component_key_value,
                    'frozen_component_snapshot_json', component_item.frozen_component_snapshot_json,
                    'frozen_source_basis_json', component_item.frozen_source_basis_json,
                    'frozen_target_amount_ex_vat', component_item.frozen_target_amount_ex_vat,
                    'frozen_target_amount_vat', component_item.frozen_target_amount_vat,
                    'frozen_target_amount_inc_vat', component_item.frozen_target_amount_inc_vat
                  )
                )
                ORDER BY component_item.id
              )
              FROM public.pay_batch_items AS component_item
              JOIN public.pay_batch_candidates AS component_candidate
                ON component_candidate.id = component_item.pay_batch_candidate_id
              WHERE component_candidate.pay_batch_id = p_pay_batch_id
                AND component_item.timesheet_id = snapshot_page.timesheet_id
                AND component_item.pay_channel = snapshot_page.pay_channel
                AND COALESCE(component_item.is_voided, false) = false
            ), '[]'::jsonb)
          )
        )
        || COALESCE(display_source.display_metadata_json, '{}'::jsonb)
      ) AS target_snapshot_json
    FROM pg_temp.tmp_pay_batch_snapshot_page AS snapshot_page
    LEFT JOIN LATERAL (
      WITH source_material AS (
        SELECT
          source_allocation.allocation_basis_json -> 'line' AS allocation_line_json,
          source_allocation.allocation_basis_json #> '{line,target_snapshot_json}' AS allocation_target_json,
          source_allocation.allocation_basis_json #> '{line,base_snapshot_json}' AS allocation_base_json,
          source_batch_item.frozen_source_basis_json AS frozen_source_basis_json,
          source_batch_item.frozen_component_snapshot_json AS frozen_component_snapshot_json
        FROM public.banking_pay_operation_candidate_allocation_rows AS source_allocation
        JOIN public.pay_batch_items AS source_batch_item
          ON source_batch_item.id = source_allocation.pay_batch_item_id
        WHERE source_allocation.operation_id = p_operation_id
          AND source_batch_item.timesheet_id = snapshot_page.timesheet_id
          AND source_batch_item.pay_channel = snapshot_page.pay_channel
          AND COALESCE(source_batch_item.is_voided, false) = false
        ORDER BY source_allocation.sort_order, source_allocation.id
        LIMIT 1
      )
      SELECT
        COALESCE(
          CASE WHEN jsonb_typeof(source_material.allocation_base_json) = 'object' THEN source_material.allocation_base_json ELSE NULL::jsonb END,
          '{}'::jsonb
        ) AS base_snapshot_json,
        jsonb_strip_nulls(
          jsonb_build_object(
            'client_name', NULLIF(BTRIM(COALESCE(
              source_material.allocation_target_json ->> 'client_name',
              source_material.allocation_target_json #>> '{client,name}',
              source_material.allocation_line_json ->> 'client_name',
              source_material.allocation_line_json #>> '{client,name}',
              source_material.allocation_base_json ->> 'client_name',
              source_material.allocation_base_json #>> '{client,name}',
              source_material.frozen_source_basis_json ->> 'client_name',
              source_material.frozen_source_basis_json #>> '{client,name}',
              source_material.frozen_source_basis_json #>> '{target_snapshot_json,client_name}',
              source_material.frozen_component_snapshot_json ->> 'client_name',
              ''
            )), ''),
            'client_id', NULLIF(BTRIM(COALESCE(
              source_material.allocation_target_json ->> 'client_id',
              source_material.allocation_target_json #>> '{client,id}',
              source_material.allocation_line_json ->> 'client_id',
              source_material.allocation_line_json #>> '{client,id}',
              source_material.allocation_base_json ->> 'client_id',
              source_material.allocation_base_json #>> '{client,id}',
              source_material.frozen_source_basis_json ->> 'client_id',
              source_material.frozen_source_basis_json #>> '{client,id}',
              source_material.frozen_source_basis_json #>> '{target_snapshot_json,client_id}',
              source_material.frozen_component_snapshot_json ->> 'client_id',
              ''
            )), ''),
            'week_ending_date', NULLIF(BTRIM(COALESCE(
              source_material.allocation_target_json ->> 'week_ending_date',
              source_material.allocation_target_json ->> 'week_ending',
              source_material.allocation_line_json ->> 'week_ending_date',
              source_material.allocation_line_json ->> 'week_ending',
              source_material.allocation_base_json ->> 'week_ending_date',
              source_material.allocation_base_json ->> 'week_ending',
              source_material.frozen_source_basis_json ->> 'week_ending_date',
              source_material.frozen_source_basis_json ->> 'week_ending',
              source_material.frozen_source_basis_json #>> '{target_snapshot_json,week_ending_date}',
              source_material.frozen_component_snapshot_json ->> 'week_ending_date',
              ''
            )), ''),
            'job_title', NULLIF(BTRIM(COALESCE(
              source_material.allocation_target_json ->> 'job_title',
              source_material.allocation_target_json ->> 'jobTitle',
              source_material.allocation_line_json ->> 'job_title',
              source_material.allocation_line_json ->> 'jobTitle',
              source_material.allocation_base_json ->> 'job_title',
              source_material.frozen_source_basis_json ->> 'job_title',
              source_material.frozen_source_basis_json #>> '{target_snapshot_json,job_title}',
              source_material.frozen_component_snapshot_json ->> 'job_title',
              ''
            )), ''),
            'band', NULLIF(BTRIM(COALESCE(
              source_material.allocation_target_json ->> 'band',
              source_material.allocation_line_json ->> 'band',
              source_material.allocation_base_json ->> 'band',
              source_material.frozen_source_basis_json ->> 'band',
              source_material.frozen_source_basis_json #>> '{target_snapshot_json,band}',
              source_material.frozen_component_snapshot_json ->> 'band',
              ''
            )), ''),
            'grade', NULLIF(BTRIM(COALESCE(
              source_material.allocation_target_json ->> 'grade',
              source_material.allocation_line_json ->> 'grade',
              source_material.allocation_base_json ->> 'grade',
              source_material.frozen_source_basis_json ->> 'grade',
              source_material.frozen_source_basis_json #>> '{target_snapshot_json,grade}',
              source_material.frozen_component_snapshot_json ->> 'grade',
              ''
            )), ''),
            'reference_number', NULLIF(BTRIM(COALESCE(
              source_material.allocation_target_json ->> 'reference_number',
              source_material.allocation_target_json ->> 'timesheet_reference',
              source_material.allocation_line_json ->> 'reference_number',
              source_material.allocation_line_json ->> 'timesheet_reference',
              source_material.allocation_base_json ->> 'reference_number',
              source_material.allocation_base_json ->> 'timesheet_reference',
              source_material.frozen_source_basis_json ->> 'reference_number',
              source_material.frozen_source_basis_json ->> 'timesheet_reference',
              source_material.frozen_source_basis_json #>> '{target_snapshot_json,reference_number}',
              source_material.frozen_component_snapshot_json ->> 'reference_number',
              ''
            )), ''),
            'timesheet_type', NULLIF(BTRIM(COALESCE(
              source_material.allocation_target_json ->> 'timesheet_type',
              source_material.allocation_line_json ->> 'timesheet_type',
              source_material.allocation_base_json ->> 'timesheet_type',
              source_material.frozen_source_basis_json ->> 'timesheet_type',
              source_material.frozen_source_basis_json #>> '{target_snapshot_json,timesheet_type}',
              source_material.frozen_component_snapshot_json ->> 'timesheet_type',
              ''
            )), ''),
            'schedule_rows', COALESCE(
              CASE WHEN jsonb_typeof(source_material.allocation_target_json -> 'schedule_rows') = 'array' THEN source_material.allocation_target_json -> 'schedule_rows' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.allocation_line_json -> 'schedule_rows') = 'array' THEN source_material.allocation_line_json -> 'schedule_rows' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.allocation_base_json -> 'schedule_rows') = 'array' THEN source_material.allocation_base_json -> 'schedule_rows' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_source_basis_json -> 'schedule_rows') = 'array' THEN source_material.frozen_source_basis_json -> 'schedule_rows' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_source_basis_json #> '{target_snapshot_json,schedule_rows}') = 'array' THEN source_material.frozen_source_basis_json #> '{target_snapshot_json,schedule_rows}' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_component_snapshot_json -> 'schedule_rows') = 'array' THEN source_material.frozen_component_snapshot_json -> 'schedule_rows' ELSE NULL::jsonb END
            ),
            'segments', COALESCE(
              CASE WHEN jsonb_typeof(source_material.allocation_target_json -> 'segments') = 'array' THEN source_material.allocation_target_json -> 'segments' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.allocation_line_json -> 'segments') = 'array' THEN source_material.allocation_line_json -> 'segments' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.allocation_base_json -> 'segments') = 'array' THEN source_material.allocation_base_json -> 'segments' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_source_basis_json -> 'segments') = 'array' THEN source_material.frozen_source_basis_json -> 'segments' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_source_basis_json #> '{target_snapshot_json,segments}') = 'array' THEN source_material.frozen_source_basis_json #> '{target_snapshot_json,segments}' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_component_snapshot_json -> 'segments') = 'array' THEN source_material.frozen_component_snapshot_json -> 'segments' ELSE NULL::jsonb END
            ),
            'segment_rows', COALESCE(
              CASE WHEN jsonb_typeof(source_material.allocation_target_json -> 'segment_rows') = 'array' THEN source_material.allocation_target_json -> 'segment_rows' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.allocation_line_json -> 'segment_rows') = 'array' THEN source_material.allocation_line_json -> 'segment_rows' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.allocation_base_json -> 'segment_rows') = 'array' THEN source_material.allocation_base_json -> 'segment_rows' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_source_basis_json -> 'segment_rows') = 'array' THEN source_material.frozen_source_basis_json -> 'segment_rows' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_source_basis_json #> '{target_snapshot_json,segment_rows}') = 'array' THEN source_material.frozen_source_basis_json #> '{target_snapshot_json,segment_rows}' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_component_snapshot_json -> 'segment_rows') = 'array' THEN source_material.frozen_component_snapshot_json -> 'segment_rows' ELSE NULL::jsonb END
            ),
            'shift_rows', COALESCE(
              CASE WHEN jsonb_typeof(source_material.allocation_target_json -> 'shift_rows') = 'array' THEN source_material.allocation_target_json -> 'shift_rows' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.allocation_line_json -> 'shift_rows') = 'array' THEN source_material.allocation_line_json -> 'shift_rows' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.allocation_base_json -> 'shift_rows') = 'array' THEN source_material.allocation_base_json -> 'shift_rows' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_source_basis_json -> 'shift_rows') = 'array' THEN source_material.frozen_source_basis_json -> 'shift_rows' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_source_basis_json #> '{target_snapshot_json,shift_rows}') = 'array' THEN source_material.frozen_source_basis_json #> '{target_snapshot_json,shift_rows}' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_component_snapshot_json -> 'shift_rows') = 'array' THEN source_material.frozen_component_snapshot_json -> 'shift_rows' ELSE NULL::jsonb END
            ),
            'schedule_changes', COALESCE(
              CASE WHEN jsonb_typeof(source_material.allocation_target_json -> 'schedule_changes') = 'array' THEN source_material.allocation_target_json -> 'schedule_changes' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.allocation_line_json -> 'schedule_changes') = 'array' THEN source_material.allocation_line_json -> 'schedule_changes' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.allocation_base_json -> 'schedule_changes') = 'array' THEN source_material.allocation_base_json -> 'schedule_changes' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_source_basis_json -> 'schedule_changes') = 'array' THEN source_material.frozen_source_basis_json -> 'schedule_changes' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_source_basis_json #> '{target_snapshot_json,schedule_changes}') = 'array' THEN source_material.frozen_source_basis_json #> '{target_snapshot_json,schedule_changes}' ELSE NULL::jsonb END,
              CASE WHEN jsonb_typeof(source_material.frozen_component_snapshot_json -> 'schedule_changes') = 'array' THEN source_material.frozen_component_snapshot_json -> 'schedule_changes' ELSE NULL::jsonb END
            ),
            'amendment_note', NULLIF(BTRIM(COALESCE(
              source_material.allocation_target_json ->> 'amendment_note',
              source_material.allocation_target_json ->> 'schedule_note',
              source_material.allocation_line_json ->> 'amendment_note',
              source_material.allocation_line_json ->> 'schedule_note',
              source_material.allocation_base_json ->> 'amendment_note',
              source_material.frozen_source_basis_json ->> 'amendment_note',
              source_material.frozen_source_basis_json #>> '{target_snapshot_json,amendment_note}',
              source_material.frozen_component_snapshot_json ->> 'amendment_note',
              ''
            )), '')
          )
        ) AS display_metadata_json
      FROM source_material
    ) AS display_source ON true
  ), inserted_snapshots AS (
    INSERT INTO public.pay_batch_timesheet_snapshots(
      pay_batch_id,
      timesheet_id,
      candidate_id,
      pay_channel,
      base_snapshot_json,
      target_snapshot_json,
      signature,
      created_at_utc
    )
    SELECT
      p_pay_batch_id,
      snapshot_rows.timesheet_id,
      snapshot_rows.candidate_id,
      snapshot_rows.pay_channel,
      snapshot_rows.base_snapshot_json,
      snapshot_rows.target_snapshot_json,
      md5(snapshot_rows.target_snapshot_json::text),
      v_now
    FROM snapshot_rows
    ON CONFLICT (pay_batch_id, timesheet_id, pay_channel)
    DO UPDATE
    SET candidate_id = EXCLUDED.candidate_id,
        base_snapshot_json = EXCLUDED.base_snapshot_json,
        target_snapshot_json = EXCLUDED.target_snapshot_json,
        signature = EXCLUDED.signature
    RETURNING public.pay_batch_timesheet_snapshots.id
  )
  SELECT COUNT(*)::integer
  INTO v_inserted_count
  FROM inserted_snapshots;

  v_updated_count := 0;

  PERFORM private.pay_workbench_draft_expected_effects_v1(
    p_operation_id,'CREATE_TIMESHEET_SNAPSHOTS','ASSERT_COMPLETE','[]'::jsonb,
    jsonb_build_object('pay_batch_id',p_pay_batch_id)
  );

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'operation_id', p_operation_id::text,
    'candidate_scope_count', v_scope_id_count,
    'inserted_count', COALESCE(v_inserted_count, 0),
    'updated_count', COALESCE(v_updated_count, 0),
    'reused_count', 0,
    'failed_count', 0,
    'inserted_snapshot_rows', COALESCE(v_inserted_count, 0),
    'missing_snapshot_count', 0,
    'has_more', EXISTS (
      SELECT 1
      FROM public.pay_batch_items AS remaining_item
      JOIN public.pay_batch_candidates AS remaining_candidate
        ON remaining_candidate.id = remaining_item.pay_batch_candidate_id
      JOIN public.banking_pay_operation_candidate_allocation_rows AS remaining_allocation
        ON remaining_allocation.pay_batch_item_id = remaining_item.id
       AND remaining_allocation.operation_id = p_operation_id
      WHERE remaining_candidate.pay_batch_id = p_pay_batch_id
        AND remaining_allocation.candidate_scope_id IN (
          SELECT (supplied_scope.scope_value #>> '{}')::uuid
          FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
        )
        AND remaining_item.timesheet_id IS NOT NULL
        AND COALESCE(remaining_item.is_voided, false) = false
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_batch_timesheet_snapshots AS remaining_snapshot
          WHERE remaining_snapshot.pay_batch_id = p_pay_batch_id
            AND remaining_snapshot.timesheet_id = remaining_item.timesheet_id
            AND remaining_snapshot.pay_channel = remaining_item.pay_channel
        )
    )
  );
END;
$function$
;

ALTER FUNCTION public.pay_batch_create_timesheet_snapshots(uuid, uuid, uuid, jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_batch_create_timesheet_snapshots(uuid, uuid, uuid, jsonb) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_batch_create_timesheet_snapshots(uuid, uuid, uuid, jsonb) TO service_role;
