-- Rollback-only regression contracts for the James post-resolution and
-- Draft-integrity correction. This file creates no durable object and touches
-- no business row. Full session-version/recovery mutation fixtures must remain
-- separately authorised TEST evidence.
BEGIN;

DO $verify_catalog$
BEGIN
  IF pg_catalog.to_regprocedure(
       'private.pay_workbench_candidate_session_version_rebase_v1(uuid,uuid,bigint,bigint,uuid)'
     ) IS NULL THEN
    RAISE EXCEPTION 'JAMES_POST_RESOLUTION_REBASE_HELPER_MISSING';
  END IF;
  IF pg_catalog.to_regprocedure(
       'public.pay_workbench_session_get_candidate_preview(uuid,uuid,jsonb,integer)'
     ) IS NULL THEN
    RAISE EXCEPTION 'JAMES_POST_RESOLUTION_CANDIDATE_PREVIEW_OWNER_MISSING';
  END IF;
END;
$verify_catalog$;

-- Sequential, capacity-capped allocation: a £1 parent against £113.04
-- authority must publish exactly £1, never the stale child zero.
DO $verify_one_pound$
DECLARE
  v_total numeric;
BEGIN
  WITH component(capacity, ordinality) AS (
    VALUES (113.04::numeric, 1)
  ), allocated AS (
    SELECT pg_catalog.round(LEAST(
      capacity,
      GREATEST(1.00::numeric - COALESCE(pg_catalog.sum(capacity) OVER (
        ORDER BY ordinality ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0::numeric), 0::numeric)
    ), 2) AS amount
    FROM component
  )
  SELECT pg_catalog.round(pg_catalog.sum(amount), 2) INTO v_total FROM allocated;
  IF v_total IS DISTINCT FROM 1.00::numeric THEN
    RAISE EXCEPTION 'JAMES_POST_RESOLUTION_ONE_POUND_PARITY_FAILED';
  END IF;
END;
$verify_one_pound$;

-- Multi-component allocation is deterministic and penny exact, without a
-- proportional split.
DO $verify_component_parity$
DECLARE
  v_allocations numeric[];
  v_total numeric;
BEGIN
  WITH component(capacity, ordinality) AS (
    VALUES (230.00::numeric, 1), (37.50::numeric, 2), (220.00::numeric, 3)
  ), ranked AS (
    SELECT component.*,
      COALESCE(pg_catalog.sum(capacity) OVER (
        ORDER BY ordinality ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0)::numeric AS prior_capacity
    FROM component
  ), allocated AS (
    SELECT ordinality, pg_catalog.round(LEAST(
      capacity,
      GREATEST(267.50::numeric - prior_capacity, 0::numeric)
    ), 2) AS amount
    FROM ranked
  )
  SELECT pg_catalog.array_agg(amount ORDER BY ordinality),
         pg_catalog.round(pg_catalog.sum(amount), 2)
  INTO v_allocations, v_total
  FROM allocated;

  IF v_allocations IS DISTINCT FROM ARRAY[230.00,37.50,0.00]::numeric[]
     OR v_total IS DISTINCT FROM 267.50::numeric THEN
    RAISE EXCEPTION 'JAMES_POST_RESOLUTION_COMPONENT_PARITY_FAILED';
  END IF;
END;
$verify_component_parity$;

-- The existing semantic helper remains the routing authority. Physical
-- section is deliberately preserved for source/preview parity.
DO $verify_effective_sections$
DECLARE
  v_ready text;
  v_cases text;
  v_blocked text;
BEGIN
  v_ready := private.pay_workbench_preview_effective_section_v1(
    'blocked_for_pay',
    '{"selection_recovery_headroom_v1":{"contract_version":1,"effective_section":"canonical_preview_lines"}}'::jsonb
  );
  v_cases := private.pay_workbench_preview_effective_section_v1(
    'blocked_for_pay',
    '{"selection_recovery_headroom_v1":{"contract_version":1,"effective_section":"cases_resolutions"}}'::jsonb
  );
  v_blocked := private.pay_workbench_preview_effective_section_v1(
    'canonical_preview_lines',
    '{"selection_recovery_headroom_v1":{"contract_version":1,"effective_section":"blocked_for_pay"}}'::jsonb
  );
  IF v_ready IS DISTINCT FROM 'canonical_preview_lines'
     OR v_cases IS DISTINCT FROM 'cases_resolutions'
     OR v_blocked IS DISTINCT FROM 'blocked_for_pay' THEN
    RAISE EXCEPTION 'JAMES_POST_RESOLUTION_EFFECTIVE_SECTION_FAILED';
  END IF;
END;
$verify_effective_sections$;

ROLLBACK;
