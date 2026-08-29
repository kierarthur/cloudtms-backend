-- Exact presentation guards from the current renderComponentRows declaration.
-- Component identity, basis, amount and action execution remain with existing
-- authorities. This helper never decides that an optional Clear is mandatory.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_component_actions_v2(p_component jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  WITH flags AS (
    SELECT
      COALESCE(p_component->'needs_action'='true'::jsonb,false) AS needs_action,
      COALESCE(p_component->'show_suggested_rate'='true'::jsonb,false)
        AND COALESCE(p_component->'suggested_available'='true'::jsonb,false) AS suggested,
      COALESCE(p_component->'show_manual_rate_control'='true'::jsonb,false) AS manual_rate,
      COALESCE(p_component->'show_manual_amount_control'='true'::jsonb,false) AS manual_amount,
      -- These three legacy flags use JavaScript truthiness, unlike the strict
      -- booleans above. Empty arrays/objects and the string "false" are truthy.
      COALESCE(p_component->'has_operator_choice' NOT IN ('null'::jsonb,'false'::jsonb,'0'::jsonb,'""'::jsonb),false)
        OR UPPER(BTRIM(COALESCE(p_component->>'resolution_state',''))) IN ('STALE','RESOLVED') AS clear,
      COALESCE(p_component->'is_fixed_reimbursement' NOT IN ('null'::jsonb,'false'::jsonb,'0'::jsonb,'""'::jsonb),false)
        OR COALESCE(p_component->'is_fixed_no_action_taxable_row' NOT IN ('null'::jsonb,'false'::jsonb,'0'::jsonb,'""'::jsonb),false)
        OR UPPER(BTRIM(COALESCE(p_component->>'resolution_state',''))) IN ('FIXED','NOT_REQUIRED') AS fixed_no_action
    WHERE jsonb_typeof(p_component)='object'
  )
  SELECT jsonb_build_object('needs_action',f.needs_action,'fixed_no_action',f.fixed_no_action,
    'actions',COALESCE((SELECT jsonb_agg(action ORDER BY ordinal)
      FROM (VALUES
        (1,'banking:pay:componentUseSuggested',f.suggested),
        (2,'banking:pay:componentManualRate',f.manual_rate),
        (3,'banking:pay:componentManualAmount',f.manual_amount),
        (4,'banking:pay:componentClearResolution',f.clear)
      ) a(ordinal,action,visible) WHERE visible),'[]'::jsonb))
  FROM flags f;
$function$;
ALTER FUNCTION private.pay_workbench_modal_component_actions_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_component_actions_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;

commit;
