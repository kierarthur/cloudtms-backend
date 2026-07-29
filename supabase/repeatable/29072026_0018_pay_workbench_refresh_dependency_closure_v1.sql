-- Banking Pay pre-draft targeted refresh dependency closure.
-- This helper performs bounded indexed identity reads only. It does not
-- calculate economics, mutate workbench state, enqueue work, or alter
-- post-draft frozen authority.

CREATE OR REPLACE FUNCTION public._pay_workbench_refresh_dependency_closure_v1(
  p_candidate_id uuid,
  p_targeted_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_linked_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_finance_case_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_max_timesheets integer DEFAULT 250,
  p_max_finance_cases integer DEFAULT 100
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY INVOKER
STABLE
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_seed_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_effective_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_effective_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_invalid_seed_count integer := 0;
  v_invalid_finance_case_count integer := 0;
  v_iteration integer := 0;
  v_timesheet_count integer := 0;
  v_finance_case_count integer := 0;
  v_max_timesheets integer := LEAST(GREATEST(COALESCE(p_max_timesheets, 250), 1), 1000);
  v_max_finance_cases integer := LEAST(GREATEST(COALESCE(p_max_finance_cases, 100), 1), 500);
  v_requires_full_candidate boolean := false;
  v_fallback_reason text := NULL::text;
BEGIN
  IF p_candidate_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'coverage_complete', false,
      'requires_full_candidate', true,
      'fallback_reason', 'DEPENDENCY_CLOSURE_CANDIDATE_REQUIRED',
      'effective_targeted_timesheet_ids', '[]'::jsonb,
      'effective_linked_timesheet_ids', '[]'::jsonb,
      'effective_finance_case_ids', '[]'::jsonb
    );
  END IF;

  SELECT COALESCE(array_agg(DISTINCT seed_id ORDER BY seed_id), ARRAY[]::uuid[])
  INTO v_seed_timesheet_ids
  FROM unnest(
    COALESCE(p_targeted_timesheet_ids, ARRAY[]::uuid[])
    || COALESCE(p_linked_timesheet_ids, ARRAY[]::uuid[])
  ) AS seed(seed_id)
  WHERE seed_id IS NOT NULL;

  SELECT COUNT(*)::integer
  INTO v_invalid_seed_count
  FROM unnest(v_seed_timesheet_ids) AS seed(seed_id)
  WHERE NOT EXISTS (
      SELECT 1
      FROM public.timesheets_financials AS tsfin
      WHERE tsfin.timesheet_id = seed.seed_id
        AND tsfin.candidate_id = p_candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_advances AS finance_case
      WHERE finance_case.linked_timesheet_id = seed.seed_id
        AND finance_case.candidate_id = p_candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_finance_case_components AS component
      WHERE component.linked_timesheet_id = seed.seed_id
        AND component.candidate_id = p_candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.timesheet_payment_overrides AS payment_override
      WHERE payment_override.timesheet_id = seed.seed_id
        AND payment_override.candidate_id = p_candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.ts_pay_adjustments AS pay_adjustment
      WHERE pay_adjustment.timesheet_id = seed.seed_id
        AND pay_adjustment.candidate_id = p_candidate_id
    );

  SELECT COUNT(*)::integer
  INTO v_invalid_finance_case_count
  FROM unnest(COALESCE(p_finance_case_ids, ARRAY[]::uuid[])) AS requested_case(finance_case_id)
  WHERE requested_case.finance_case_id IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_advances AS finance_case
      WHERE finance_case.id = requested_case.finance_case_id
        AND finance_case.candidate_id = p_candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_finance_case_components AS component
      WHERE component.finance_case_id = requested_case.finance_case_id
        AND component.candidate_id = p_candidate_id
    );

  IF COALESCE(v_invalid_seed_count, 0) > 0
     OR COALESCE(v_invalid_finance_case_count, 0) > 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'coverage_complete', false,
      'requires_full_candidate', true,
      'fallback_reason', CASE
        WHEN COALESCE(v_invalid_seed_count, 0) > 0
          THEN 'DEPENDENCY_CLOSURE_TIMESHEET_OWNERSHIP_UNPROVEN'
        ELSE 'DEPENDENCY_CLOSURE_FINANCE_CASE_OWNERSHIP_UNPROVEN'
      END,
      'invalid_seed_count', COALESCE(v_invalid_seed_count, 0),
      'invalid_finance_case_count', COALESCE(v_invalid_finance_case_count, 0),
      'effective_targeted_timesheet_ids', '[]'::jsonb,
      'effective_linked_timesheet_ids', '[]'::jsonb,
      'effective_finance_case_ids', '[]'::jsonb
    );
  END IF;

  /*
   * Close the direct graph in four bounded set-based passes. The relationship
   * types are deliberately constrained to timesheet rotation families and
   * finance-case/component links; no candidate-wide financial calculation
   * occurs.
   */
  v_effective_timesheet_ids := v_seed_timesheet_ids;
  SELECT COALESCE(array_agg(DISTINCT finance_case_id ORDER BY finance_case_id), ARRAY[]::uuid[])
  INTO v_effective_finance_case_ids
  FROM unnest(COALESCE(p_finance_case_ids, ARRAY[]::uuid[]))
    AS requested_case(finance_case_id)
  WHERE finance_case_id IS NOT NULL;

  FOR v_iteration IN 1..4 LOOP
    SELECT COALESCE(array_agg(DISTINCT timesheet_id ORDER BY timesheet_id), ARRAY[]::uuid[])
    INTO v_effective_timesheet_ids
    FROM (
      SELECT existing_id AS timesheet_id
      FROM unnest(v_effective_timesheet_ids) AS existing_timesheet(existing_id)
      UNION
      SELECT rotation_scope.family_timesheet_id
      FROM public._pay_timesheet_rotation_scope(v_effective_timesheet_ids)
        AS rotation_scope
      WHERE rotation_scope.family_timesheet_id IS NOT NULL
      UNION
      SELECT component.linked_timesheet_id
      FROM public.pay_finance_case_components AS component
      WHERE component.candidate_id = p_candidate_id
        AND component.finance_case_id = ANY(v_effective_finance_case_ids)
        AND component.linked_timesheet_id IS NOT NULL
      UNION
      SELECT finance_case.linked_timesheet_id
      FROM public.pay_advances AS finance_case
      WHERE finance_case.candidate_id = p_candidate_id
        AND finance_case.id = ANY(v_effective_finance_case_ids)
        AND finance_case.linked_timesheet_id IS NOT NULL
    ) AS expanded_timesheets
    WHERE timesheet_id IS NOT NULL;

    SELECT COALESCE(array_agg(DISTINCT finance_case_id ORDER BY finance_case_id), ARRAY[]::uuid[])
    INTO v_effective_finance_case_ids
    FROM (
      SELECT existing_id AS finance_case_id
      FROM unnest(v_effective_finance_case_ids) AS existing_case(existing_id)
      UNION
      SELECT component.finance_case_id
      FROM public.pay_finance_case_components AS component
      WHERE component.candidate_id = p_candidate_id
        AND component.linked_timesheet_id = ANY(v_effective_timesheet_ids)
      UNION
      SELECT finance_case.id
      FROM public.pay_advances AS finance_case
      WHERE finance_case.candidate_id = p_candidate_id
        AND finance_case.linked_timesheet_id = ANY(v_effective_timesheet_ids)
    ) AS expanded_cases
    WHERE finance_case_id IS NOT NULL;

    EXIT WHEN COALESCE(array_length(v_effective_timesheet_ids, 1), 0) > v_max_timesheets
           OR COALESCE(array_length(v_effective_finance_case_ids, 1), 0) > v_max_finance_cases;
  END LOOP;

  v_timesheet_count := COALESCE(array_length(v_effective_timesheet_ids, 1), 0);
  v_finance_case_count := COALESCE(array_length(v_effective_finance_case_ids, 1), 0);

  IF v_timesheet_count > v_max_timesheets THEN
    v_requires_full_candidate := true;
    v_fallback_reason := 'DEPENDENCY_CLOSURE_TIMESHEET_CAP_EXCEEDED';
  ELSIF v_finance_case_count > v_max_finance_cases THEN
    v_requires_full_candidate := true;
    v_fallback_reason := 'DEPENDENCY_CLOSURE_FINANCE_CASE_CAP_EXCEEDED';
  ELSIF COALESCE(array_length(v_seed_timesheet_ids, 1), 0) = 0
        AND v_finance_case_count = 0 THEN
    v_requires_full_candidate := true;
    v_fallback_reason := 'DEPENDENCY_CLOSURE_EMPTY_ROOT';
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'coverage_complete', NOT v_requires_full_candidate,
    'requires_full_candidate', v_requires_full_candidate,
    'fallback_reason', v_fallback_reason,
    'candidate_id', p_candidate_id::text,
    'requested_timesheet_count', COALESCE(array_length(v_seed_timesheet_ids, 1), 0),
    'effective_timesheet_count', v_timesheet_count,
    'effective_finance_case_count', v_finance_case_count,
    'effective_targeted_timesheet_ids', CASE
      WHEN v_requires_full_candidate THEN '[]'::jsonb
      ELSE to_jsonb(v_effective_timesheet_ids)
    END,
    'effective_linked_timesheet_ids', '[]'::jsonb,
    'effective_finance_case_ids', CASE
      WHEN v_requires_full_candidate THEN '[]'::jsonb
      ELSE to_jsonb(v_effective_finance_case_ids)
    END,
    'closure_depth_cap', 4,
    'max_timesheets', v_max_timesheets,
    'max_finance_cases', v_max_finance_cases,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
    'economic_calculation_performed', false,
    'queue_mutation_performed', false
  );
END;
$function$;

ALTER FUNCTION public._pay_workbench_refresh_dependency_closure_v1(
  uuid, uuid[], uuid[], uuid[], integer, integer
) OWNER TO postgres;

REVOKE ALL ON FUNCTION public._pay_workbench_refresh_dependency_closure_v1(
  uuid, uuid[], uuid[], uuid[], integer, integer
) FROM PUBLIC;
REVOKE ALL ON FUNCTION public._pay_workbench_refresh_dependency_closure_v1(
  uuid, uuid[], uuid[], uuid[], integer, integer
) FROM anon;
REVOKE ALL ON FUNCTION public._pay_workbench_refresh_dependency_closure_v1(
  uuid, uuid[], uuid[], uuid[], integer, integer
) FROM authenticated;
REVOKE ALL ON FUNCTION public._pay_workbench_refresh_dependency_closure_v1(
  uuid, uuid[], uuid[], uuid[], integer, integer
) FROM service_role;
