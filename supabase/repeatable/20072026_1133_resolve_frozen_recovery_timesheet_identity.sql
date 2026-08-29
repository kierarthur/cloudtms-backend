-- Restore deterministic post-draft timesheet identity for frozen overpayment recovery items.
-- Policy X: this function reads only the frozen batch artefact after draft creation;
-- it does not consult live timesheet, TSFIN, finance-component, or workbench truth.

CREATE OR REPLACE FUNCTION public._pay_policy_x_resolve_post_draft_economic_key(
  p_pay_batch_item_id uuid DEFAULT NULL::uuid,
  p_pay_batch_id uuid DEFAULT NULL::uuid,
  p_timesheet_id uuid DEFAULT NULL::uuid,
  p_item_type text DEFAULT NULL::text,
  p_frozen_key_type text DEFAULT NULL::text,
  p_frozen_key_value text DEFAULT NULL::text,
  p_frozen_component_snapshot_json jsonb DEFAULT '{}'::jsonb,
  p_frozen_source_basis_json jsonb DEFAULT '{}'::jsonb,
  p_breakdown_meta_json jsonb DEFAULT '{}'::jsonb,
  p_target_snapshot_json jsonb DEFAULT NULL::jsonb
)
RETURNS TABLE(
  timesheet_id uuid,
  key_type text,
  key_value text,
  key_resolution_source text,
  key_resolution_failure_reason text
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
SET plpgsql_check.mode TO 'disabled'
AS $function$
DECLARE
  v_pay_batch_id uuid := p_pay_batch_id;
  v_pay_batch_item_id uuid := p_pay_batch_item_id;
  v_timesheet_id uuid := p_timesheet_id;
  v_item_type text := UPPER(NULLIF(BTRIM(COALESCE(p_item_type, '')), ''));
  v_segment_key text := NULL::text;
  v_source_ref text := NULL::text;
  v_frozen_key_type text := UPPER(NULLIF(BTRIM(COALESCE(p_frozen_key_type, '')), ''));
  v_frozen_key_value text := NULLIF(BTRIM(COALESCE(p_frozen_key_value, '')), '');
  v_frozen_component_snapshot_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_frozen_component_snapshot_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_frozen_component_snapshot_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_frozen_source_basis_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_frozen_source_basis_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_frozen_source_basis_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_breakdown_meta_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_breakdown_meta_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_breakdown_meta_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_target_snapshot_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_target_snapshot_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_target_snapshot_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_breakdown_count integer := 0;
  v_snapshot_key_type text := NULL::text;
  v_snapshot_key_value text := NULL::text;
  v_work_date_text text := NULL::text;
  v_segment_id_text text := NULL::text;
  v_segment_key_text text := NULL::text;
  v_segment_stable_key_text text := NULL::text;
  v_ref_num_text text := NULL::text;
  v_adjustment_id_text text := NULL::text;
  v_additional_code_text text := NULL::text;
  v_expense_code_text text := NULL::text;
  v_segments_json jsonb := '[]'::jsonb;
  v_segment_match_found boolean := false;
  v_segment_match_date_text text := NULL::text;
  v_resolved_key_type text := NULL::text;
  v_resolved_key_value text := NULL::text;
  v_resolved_source text := NULL::text;
  v_assert_ok boolean := false;
  v_assert_key_type text := NULL::text;
  v_assert_key_value text := NULL::text;
  v_assert_failure_reason text := NULL::text;
  v_failure_reason text := NULL::text;
  v_source_json jsonb := '{}'::jsonb;
  v_frozen_timesheet_id_text text := NULL::text;
  v_frozen_timesheet_id_count integer := 0;
  v_frozen_linked_timesheet_id_text text := NULL::text;
  v_frozen_linked_timesheet_id_count integer := 0;
  v_frozen_direct_timesheet_id_text text := NULL::text;
  v_frozen_direct_timesheet_id_count integer := 0;
  v_frozen_carrier_timesheet_id_text text := NULL::text;
  v_frozen_carrier_timesheet_id_count integer := 0;
BEGIN
  IF v_pay_batch_item_id IS NOT NULL THEN
    SELECT
      batch_candidate_row.pay_batch_id,
      batch_item_row.timesheet_id,
      UPPER(NULLIF(BTRIM(COALESCE(batch_item_row.item_type, '')), '')),
      batch_item_row.segment_key,
      batch_item_row.source_ref,
      UPPER(NULLIF(BTRIM(COALESCE(batch_item_row.frozen_component_key_type, '')), '')),
      NULLIF(BTRIM(COALESCE(batch_item_row.frozen_component_key_value, '')), ''),
      CASE
        WHEN jsonb_typeof(COALESCE(batch_item_row.frozen_component_snapshot_json, '{}'::jsonb)) = 'object' THEN COALESCE(batch_item_row.frozen_component_snapshot_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END,
      CASE
        WHEN jsonb_typeof(COALESCE(batch_item_row.frozen_source_basis_json, '{}'::jsonb)) = 'object' THEN COALESCE(batch_item_row.frozen_source_basis_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END
    INTO
      v_pay_batch_id,
      v_timesheet_id,
      v_item_type,
      v_segment_key,
      v_source_ref,
      v_frozen_key_type,
      v_frozen_key_value,
      v_frozen_component_snapshot_json,
      v_frozen_source_basis_json
    FROM public.pay_batch_items AS batch_item_row
    JOIN public.pay_batch_candidates AS batch_candidate_row
      ON batch_candidate_row.id = batch_item_row.pay_batch_candidate_id
    WHERE batch_item_row.id = v_pay_batch_item_id
      AND (p_pay_batch_id IS NULL OR batch_candidate_row.pay_batch_id = p_pay_batch_id)
    LIMIT 1;

    IF v_pay_batch_id IS NOT NULL THEN
      SELECT
        COUNT(*)::integer,
        CASE
          WHEN COUNT(*) = 1 THEN (ARRAY_AGG(breakdown_row.meta_json ORDER BY breakdown_row.id))[1]
          ELSE '{}'::jsonb
        END
      INTO
        v_breakdown_count,
        v_breakdown_meta_json
      FROM public.pay_batch_item_breakdowns AS breakdown_row
      WHERE breakdown_row.pay_batch_item_id = v_pay_batch_item_id;

      IF jsonb_typeof(COALESCE(v_breakdown_meta_json, '{}'::jsonb)) <> 'object' THEN
        v_breakdown_meta_json := '{}'::jsonb;
      END IF;

      IF v_timesheet_id IS NOT NULL
         AND (p_target_snapshot_json IS NULL OR jsonb_typeof(COALESCE(p_target_snapshot_json, '{}'::jsonb)) <> 'object' OR p_target_snapshot_json = '{}'::jsonb) THEN
        SELECT snapshot_row.target_snapshot_json
        INTO v_target_snapshot_json
        FROM public.pay_batch_timesheet_snapshots AS snapshot_row
        WHERE snapshot_row.pay_batch_id = v_pay_batch_id
          AND snapshot_row.timesheet_id = v_timesheet_id
        ORDER BY snapshot_row.created_at_utc DESC, snapshot_row.id DESC
        LIMIT 1;

        IF jsonb_typeof(COALESCE(v_target_snapshot_json, '{}'::jsonb)) <> 'object' THEN
          v_target_snapshot_json := '{}'::jsonb;
        END IF;
      END IF;
    END IF;
  END IF;

  -- Recovery rows freeze both the stable correction-root identity and the
  -- current carrier member inside the batch artefact. Those UUIDs may
  -- intentionally differ: linked_timesheet_id is the stable correction-root
  -- namespace, while carrier_timesheet_id identifies the member which carried
  -- the component when the draft was frozen. Do not treat that valid
  -- root/carrier pair as contradictory evidence.
  --
  -- Resolve each semantic role independently and fail closed only when frozen
  -- artefacts disagree within the same role. Prefer the stable linked/root
  -- identity, then a direct frozen timesheet identity, then the carrier.
  -- Policy X is preserved because every candidate below comes exclusively from
  -- the frozen batch artefact.
  IF v_timesheet_id IS NULL
     AND v_item_type = 'OVERPAYMENT_RECOVERY' THEN
    SELECT
      COUNT(DISTINCT frozen_timesheet_candidate.candidate_value)::integer,
      MIN(frozen_timesheet_candidate.candidate_value)
    INTO
      v_frozen_linked_timesheet_id_count,
      v_frozen_linked_timesheet_id_text
    FROM (
      VALUES
        (NULLIF(BTRIM(COALESCE(v_frozen_source_basis_json->>'linked_timesheet_id', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_frozen_component_snapshot_json->>'linked_timesheet_id', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_breakdown_meta_json->>'linked_timesheet_id', '')), ''))
    ) AS frozen_timesheet_candidate(candidate_value)
    WHERE frozen_timesheet_candidate.candidate_value IS NOT NULL;

    SELECT
      COUNT(DISTINCT frozen_timesheet_candidate.candidate_value)::integer,
      MIN(frozen_timesheet_candidate.candidate_value)
    INTO
      v_frozen_direct_timesheet_id_count,
      v_frozen_direct_timesheet_id_text
    FROM (
      VALUES
        (NULLIF(BTRIM(COALESCE(v_frozen_source_basis_json->>'timesheet_id', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_frozen_component_snapshot_json#>>'{source_basis_json,timesheet_id}', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_frozen_component_snapshot_json->>'timesheet_id', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_breakdown_meta_json->>'timesheet_id', '')), ''))
    ) AS frozen_timesheet_candidate(candidate_value)
    WHERE frozen_timesheet_candidate.candidate_value IS NOT NULL;

    SELECT
      COUNT(DISTINCT frozen_timesheet_candidate.candidate_value)::integer,
      MIN(frozen_timesheet_candidate.candidate_value)
    INTO
      v_frozen_carrier_timesheet_id_count,
      v_frozen_carrier_timesheet_id_text
    FROM (
      VALUES
        (NULLIF(BTRIM(COALESCE(v_frozen_source_basis_json->>'carrier_timesheet_id', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_frozen_component_snapshot_json#>>'{source_basis_json,carrier_timesheet_id}', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_frozen_component_snapshot_json->>'carrier_timesheet_id', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_breakdown_meta_json->>'carrier_timesheet_id', '')), ''))
    ) AS frozen_timesheet_candidate(candidate_value)
    WHERE frozen_timesheet_candidate.candidate_value IS NOT NULL;

    IF COALESCE(v_frozen_linked_timesheet_id_count, 0) > 1
       OR COALESCE(v_frozen_direct_timesheet_id_count, 0) > 1
       OR COALESCE(v_frozen_carrier_timesheet_id_count, 0) > 1 THEN
      v_failure_reason := 'CONFLICTING_FROZEN_TIMESHEET_ID';
    ELSE
      v_frozen_timesheet_id_count :=
        COALESCE(v_frozen_linked_timesheet_id_count, 0)
        + COALESCE(v_frozen_direct_timesheet_id_count, 0)
        + COALESCE(v_frozen_carrier_timesheet_id_count, 0);
      v_frozen_timesheet_id_text := COALESCE(
        v_frozen_linked_timesheet_id_text,
        v_frozen_direct_timesheet_id_text,
        v_frozen_carrier_timesheet_id_text
      );

      IF v_frozen_timesheet_id_text IS NOT NULL
         AND pg_input_is_valid(v_frozen_timesheet_id_text, 'uuid') THEN
        v_timesheet_id := v_frozen_timesheet_id_text::uuid;
      ELSIF v_frozen_timesheet_id_text IS NOT NULL THEN
        v_failure_reason := 'INVALID_FROZEN_TIMESHEET_ID';
      END IF;
    END IF;

    IF v_failure_reason IS NULL
       AND COALESCE(v_frozen_timesheet_id_count, 0) > 0
       AND v_timesheet_id IS NULL THEN
      v_failure_reason := 'INVALID_FROZEN_TIMESHEET_ID';
    END IF;
  END IF;

  v_source_json := jsonb_build_object(
    'pay_batch_item_id', CASE WHEN v_pay_batch_item_id IS NULL THEN NULL ELSE v_pay_batch_item_id::text END,
    'pay_batch_id', CASE WHEN v_pay_batch_id IS NULL THEN NULL ELSE v_pay_batch_id::text END,
    'authority_scope', 'POST_DRAFT'
  );

  IF v_frozen_key_type IS NOT NULL OR v_frozen_key_value IS NOT NULL THEN
    SELECT
      key_check.ok,
      key_check.key_type,
      key_check.key_value,
      key_check.failure_reason
    INTO
      v_assert_ok,
      v_assert_key_type,
      v_assert_key_value,
      v_assert_failure_reason
    FROM public._pay_policy_x_assert_economic_key(
      p_timesheet_id => v_timesheet_id,
      p_key_type => v_frozen_key_type,
      p_key_value => v_frozen_key_value,
      p_context => 'POST_DRAFT_RESOLVE_FROZEN_ITEM_KEY',
      p_authority_scope => 'POST_DRAFT',
      p_resolution_source => 'FROZEN_ITEM_KEY',
      p_required => true,
      p_source_json => v_source_json
    ) AS key_check;

    IF COALESCE(v_assert_ok, false) THEN
      timesheet_id := v_timesheet_id;
      key_type := v_assert_key_type;
      key_value := v_assert_key_value;
      key_resolution_source := 'FROZEN_ITEM_KEY';
      key_resolution_failure_reason := NULL::text;
      RETURN NEXT;
      RETURN;
    ELSE
      v_failure_reason := COALESCE(v_failure_reason, v_assert_failure_reason);
    END IF;
  END IF;

  v_snapshot_key_type := UPPER(NULLIF(BTRIM(COALESCE(
    v_frozen_component_snapshot_json->>'component_key_type',
    v_frozen_component_snapshot_json->>'key_type',
    v_frozen_component_snapshot_json#>>'{source_basis_json,component_key_type}',
    v_frozen_component_snapshot_json#>>'{source_basis_json,key_type}',
    v_frozen_source_basis_json->>'component_key_type',
    v_frozen_source_basis_json->>'key_type',
    v_breakdown_meta_json->>'component_key_type',
    v_breakdown_meta_json->>'key_type',
    ''
  )), ''));

  v_snapshot_key_value := NULLIF(BTRIM(COALESCE(
    v_frozen_component_snapshot_json->>'component_key_value',
    v_frozen_component_snapshot_json->>'key_value',
    v_frozen_component_snapshot_json#>>'{source_basis_json,component_key_value}',
    v_frozen_component_snapshot_json#>>'{source_basis_json,key_value}',
    v_frozen_source_basis_json->>'component_key_value',
    v_frozen_source_basis_json->>'key_value',
    v_breakdown_meta_json->>'component_key_value',
    v_breakdown_meta_json->>'key_value',
    ''
  )), '');

  IF v_snapshot_key_type IS NOT NULL OR v_snapshot_key_value IS NOT NULL THEN
    SELECT
      key_check.ok,
      key_check.key_type,
      key_check.key_value,
      key_check.failure_reason
    INTO
      v_assert_ok,
      v_assert_key_type,
      v_assert_key_value,
      v_assert_failure_reason
    FROM public._pay_policy_x_assert_economic_key(
      p_timesheet_id => v_timesheet_id,
      p_key_type => v_snapshot_key_type,
      p_key_value => v_snapshot_key_value,
      p_context => 'POST_DRAFT_RESOLVE_FROZEN_COMPONENT_OR_BASIS_KEY',
      p_authority_scope => 'POST_DRAFT',
      p_resolution_source => 'FROZEN_COMPONENT_OR_BASIS_KEY',
      p_required => true,
      p_source_json => v_source_json
    ) AS key_check;

    IF COALESCE(v_assert_ok, false) THEN
      timesheet_id := v_timesheet_id;
      key_type := v_assert_key_type;
      key_value := v_assert_key_value;
      key_resolution_source := 'FROZEN_COMPONENT_OR_BASIS_KEY';
      key_resolution_failure_reason := NULL::text;
      RETURN NEXT;
      RETURN;
    ELSE
      v_failure_reason := COALESCE(v_failure_reason, v_assert_failure_reason);
    END IF;
  END IF;

  v_work_date_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'work_date',
    v_frozen_source_basis_json->>'date',
    v_frozen_component_snapshot_json#>>'{source_basis_json,work_date}',
    v_frozen_component_snapshot_json#>>'{source_basis_json,date}',
    v_frozen_component_snapshot_json->>'work_date',
    v_frozen_component_snapshot_json->>'date',
    v_breakdown_meta_json->>'work_date',
    v_breakdown_meta_json->>'date',
    ''
  )), '');

  v_segment_id_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'segment_id',
    v_frozen_component_snapshot_json#>>'{source_basis_json,segment_id}',
    v_frozen_component_snapshot_json->>'segment_id',
    v_breakdown_meta_json->>'segment_id',
    CASE
      WHEN v_source_ref IS NOT NULL AND BTRIM(v_source_ref) LIKE 'seg:%' THEN split_part(v_source_ref, ':', 2)
      ELSE NULL::text
    END,
    ''
  )), '');

  v_segment_key_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'segment_key',
    v_frozen_source_basis_json->>'segment_stable_key',
    v_frozen_component_snapshot_json#>>'{source_basis_json,segment_key}',
    v_frozen_component_snapshot_json#>>'{source_basis_json,segment_stable_key}',
    v_frozen_component_snapshot_json->>'segment_key',
    v_frozen_component_snapshot_json->>'segment_stable_key',
    v_breakdown_meta_json->>'segment_key',
    v_breakdown_meta_json->>'segment_stable_key',
    v_segment_key,
    ''
  )), '');

  v_segment_stable_key_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'segment_stable_key',
    v_frozen_component_snapshot_json#>>'{source_basis_json,segment_stable_key}',
    v_frozen_component_snapshot_json->>'segment_stable_key',
    v_breakdown_meta_json->>'segment_stable_key',
    ''
  )), '');

  v_ref_num_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'ref_num',
    v_frozen_component_snapshot_json#>>'{source_basis_json,ref_num}',
    v_frozen_component_snapshot_json->>'ref_num',
    v_breakdown_meta_json->>'ref_num',
    ''
  )), '');

  v_adjustment_id_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'adjustment_id',
    v_frozen_source_basis_json->>'adjustment_code',
    v_frozen_component_snapshot_json#>>'{source_basis_json,adjustment_id}',
    v_frozen_component_snapshot_json#>>'{source_basis_json,adjustment_code}',
    v_frozen_component_snapshot_json->>'adjustment_id',
    v_frozen_component_snapshot_json->>'adjustment_code',
    v_breakdown_meta_json->>'adjustment_id',
    v_breakdown_meta_json->>'adjustment_code',
    ''
  )), '');

  v_additional_code_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'additional_code',
    v_frozen_component_snapshot_json#>>'{source_basis_json,additional_code}',
    v_frozen_component_snapshot_json->>'additional_code',
    v_breakdown_meta_json->>'additional_code',
    ''
  )), '');

  v_expense_code_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'expense_code',
    v_frozen_source_basis_json->>'mileage_code',
    v_frozen_component_snapshot_json#>>'{source_basis_json,expense_code}',
    v_frozen_component_snapshot_json#>>'{source_basis_json,mileage_code}',
    v_frozen_component_snapshot_json->>'expense_code',
    v_frozen_component_snapshot_json->>'mileage_code',
    v_breakdown_meta_json->>'expense_code',
    v_breakdown_meta_json->>'mileage_code',
    ''
  )), '');

  IF v_item_type = 'SEGMENT_DELTA' AND v_work_date_text IS NOT NULL AND v_work_date_text ~ '^\d{4}-\d{2}-\d{2}$' THEN
    v_resolved_key_type := 'TS_DAY';
    v_resolved_key_value := v_work_date_text;
    v_resolved_source := 'FROZEN_SOURCE_BASIS_DATE';

  ELSIF v_item_type = 'SEGMENT_DELTA' THEN
    IF jsonb_typeof(v_target_snapshot_json->'segments') = 'array' THEN
      v_segments_json := v_target_snapshot_json->'segments';
    ELSIF jsonb_typeof(v_target_snapshot_json#>'{actual_schedule_json,segments}') = 'array' THEN
      v_segments_json := v_target_snapshot_json#>'{actual_schedule_json,segments}';
    ELSIF jsonb_typeof(v_target_snapshot_json#>'{schedule_json,segments}') = 'array' THEN
      v_segments_json := v_target_snapshot_json#>'{schedule_json,segments}';
    ELSE
      v_segments_json := '[]'::jsonb;
    END IF;

    IF jsonb_array_length(v_segments_json) > 0 THEN
      SELECT
        NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'date', segment_choice.segment_json->>'work_date', '')), ''),
        true
      INTO
        v_segment_match_date_text,
        v_segment_match_found
      FROM jsonb_array_elements(v_segments_json) AS segment_choice(segment_json)
      WHERE segment_choice.segment_json IS NOT NULL
        AND jsonb_typeof(segment_choice.segment_json) = 'object'
        AND (
          (v_work_date_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'date', segment_choice.segment_json->>'work_date', '')), '') = v_work_date_text)
          OR (v_ref_num_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'ref_num', '')), '') = v_ref_num_text)
          OR (v_segment_id_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_id', '')), '') = v_segment_id_text)
          OR (v_segment_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_key', segment_choice.segment_json->>'segment_id', '')), '') = v_segment_key_text)
          OR (v_segment_stable_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_stable_key', segment_choice.segment_json->>'segment_id', segment_choice.segment_json->>'segment_key', '')), '') = v_segment_stable_key_text)
        )
      ORDER BY
        CASE
          WHEN v_work_date_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'date', segment_choice.segment_json->>'work_date', '')), '') = v_work_date_text THEN 0
          WHEN v_ref_num_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'ref_num', '')), '') = v_ref_num_text THEN 1
          WHEN v_segment_id_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_id', '')), '') = v_segment_id_text THEN 2
          WHEN v_segment_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_key', segment_choice.segment_json->>'segment_id', '')), '') = v_segment_key_text THEN 3
          WHEN v_segment_stable_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_stable_key', segment_choice.segment_json->>'segment_id', segment_choice.segment_json->>'segment_key', '')), '') = v_segment_stable_key_text THEN 4
          ELSE 9
        END
      LIMIT 1;

      IF COALESCE(v_segment_match_found, false)
         AND v_segment_match_date_text IS NOT NULL
         AND v_segment_match_date_text ~ '^\d{4}-\d{2}-\d{2}$' THEN
        v_resolved_key_type := 'TS_DAY';
        v_resolved_key_value := v_segment_match_date_text;
        v_resolved_source := 'FROZEN_TIMESHEET_SNAPSHOT';
      ELSIF COALESCE(v_segment_match_found, false) THEN
        v_resolved_key_type := 'TS_TOTAL';
        v_resolved_key_value := 'TOTAL';
        v_resolved_source := 'FROZEN_TIMESHEET_SNAPSHOT_TOTAL_FALLBACK';
      END IF;
    END IF;

  ELSIF v_item_type = 'ADJUSTMENT_DELTA' AND v_adjustment_id_text IS NOT NULL THEN
    v_resolved_key_type := 'ADJUSTMENT_CODE';
    v_resolved_key_value := v_adjustment_id_text;
    v_resolved_source := 'FROZEN_SOURCE_BASIS_KEY';

  ELSIF v_item_type = 'EXPENSE_DELTA' AND v_additional_code_text IS NOT NULL THEN
    v_resolved_key_type := 'ADDITIONAL_CODE';
    v_resolved_key_value := UPPER(v_additional_code_text);
    v_resolved_source := 'FROZEN_SOURCE_BASIS_KEY';

  ELSIF v_item_type IN ('EXPENSE_DELTA', 'MILEAGE_DELTA') AND v_expense_code_text IS NOT NULL THEN
    v_resolved_key_type := 'EXPENSE_CODE';
    v_resolved_key_value := UPPER(v_expense_code_text);
    v_resolved_source := 'FROZEN_SOURCE_BASIS_KEY';
  END IF;

  IF v_resolved_key_type IS NULL AND v_item_type = 'SEGMENT_DELTA' THEN
    v_failure_reason := COALESCE(v_failure_reason, 'MISSING_DETERMINISTIC_POST_DRAFT_SNAPSHOT_EVIDENCE');
    v_source_json := v_source_json || jsonb_build_object('missing_deterministic_post_draft_snapshot', true);
  END IF;

  SELECT
    key_check.ok,
    key_check.key_type,
    key_check.key_value,
    key_check.failure_reason
  INTO
    v_assert_ok,
    v_assert_key_type,
    v_assert_key_value,
    v_assert_failure_reason
  FROM public._pay_policy_x_assert_economic_key(
    p_timesheet_id => v_timesheet_id,
    p_key_type => v_resolved_key_type,
    p_key_value => v_resolved_key_value,
    p_context => 'POST_DRAFT_RESOLVE_FINAL',
    p_authority_scope => 'POST_DRAFT',
    p_resolution_source => COALESCE(v_resolved_source, 'POST_DRAFT_KEY_RESOLUTION_FAILED'),
    p_required => true,
    p_source_json => v_source_json
  ) AS key_check;

  IF COALESCE(v_assert_ok, false) THEN
    timesheet_id := v_timesheet_id;
    key_type := v_assert_key_type;
    key_value := v_assert_key_value;
    key_resolution_source := v_resolved_source;
    key_resolution_failure_reason := NULL::text;
  ELSE
    timesheet_id := v_timesheet_id;
    key_type := NULL::text;
    key_value := NULL::text;
    key_resolution_source := 'KEY_RESOLUTION_FAILED';
    key_resolution_failure_reason := COALESCE(v_failure_reason, v_assert_failure_reason, 'POST_DRAFT_KEY_RESOLUTION_FAILED');
  END IF;

  RETURN NEXT;
END;
$function$;
