-- One read-only visibility fence for all v2 lists, totals and task counts.
-- Hidden/indefinite payment rows stay exclusively in the existing Snoozes tab.
-- Only the row and its legacy payload aliases are inspected: a hidden child
-- component must never hide an otherwise-visible multi-component parent.
\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_hidden_v2(p_row jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  WITH payloads AS (
    SELECT value AS r FROM unnest(ARRAY[p_row,p_row->'row_json',p_row->'rowJson']) AS item(value)
    WHERE jsonb_typeof(value)='object'
  ), facts AS (
    SELECT r,
      UPPER(BTRIM(COALESCE(NULLIF(r->>'presentation_role',''),r->>'presentationRole',''))) AS role,
      UPPER(BTRIM(COALESCE(NULLIF(r->>'presentation_section',''),r->>'presentationSection',''))) AS section,
      UPPER(BTRIM(COALESCE(NULLIF(r->>'readiness_state',''),r->>'readinessState',''))) AS readiness,
      UPPER(BTRIM(COALESCE(NULLIF(r#>>'{snooze_state,state}',''),
        CASE WHEN jsonb_typeof(r->'snooze_state')='string' THEN NULLIF(r->>'snooze_state','') END,
        r->>'blocked_snooze_state',''))) AS snooze_state,
      BTRIM(COALESCE(NULLIF(r#>>'{snooze_state,snooze_until_date}',''),r->>'snooze_until_date','')) AS until_date
    FROM payloads
  )
  SELECT COALESCE(bool_or(
    role IN ('HIDDEN','HIDDEN_INDEFINITE_SNOOZE')
    OR section IN ('HIDDEN','HIDDEN_INDEFINITE_SNOOZE')
    OR readiness IN ('HIDDEN','HIDDEN_INDEFINITE_SNOOZE')
    OR LOWER(BTRIM(COALESCE(r->>'is_hidden',''))) IN ('true','1','yes','y','on')
    OR LOWER(BTRIM(COALESCE(r->>'hidden',''))) IN ('true','1','yes','y','on')
    OR LOWER(BTRIM(COALESCE(r->>'hidden_indefinite_snooze',''))) IN ('true','1','yes','y','on')
    OR LOWER(BTRIM(COALESCE(r->>'is_indefinitely_snoozed',''))) IN ('true','1','yes','y','on')
    OR (snooze_state NOT IN ('','NONE','NOT_SNOOZED','CLEARED') AND until_date='')
  ),false) FROM facts;
$function$;
ALTER FUNCTION private.pay_workbench_modal_hidden_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_hidden_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;
