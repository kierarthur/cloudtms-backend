-- Banking Pay bounded-scope Version 1.2.9.
-- Materialise one bounded physical candidate finance-item page exactly once.
-- Raw count/amount/key/digest evidence is computed before the dispatcher turns
-- any successfully resolved row into a durable economic fact.

CREATE OR REPLACE FUNCTION private.pay_workbench_finance_item_authority_page_bundle_v1(
  p_build_id uuid,
  p_last_batch_candidate_id uuid DEFAULT NULL::uuid,
  p_last_item_id uuid DEFAULT NULL::uuid,
  p_limit integer DEFAULT 25
)
RETURNS jsonb
LANGUAGE sql
STABLE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
  WITH bounded AS MATERIALIZED (
    SELECT page.*
    FROM private.pay_workbench_finance_item_authority_page_v1(
      p_build_id,p_last_batch_candidate_id,p_last_item_id,
      LEAST(GREATEST(COALESCE(p_limit,25),1),25)
    ) page
  ), accepted AS MATERIALIZED (
    SELECT bounded.*
    FROM bounded
    ORDER BY bounded.source_key
    LIMIT LEAST(GREATEST(COALESCE(p_limit,25),1),25)
  ), evidence AS (
    SELECT COUNT(*)::bigint AS raw_page_count,
      COUNT(*) FILTER(WHERE NULLIF(BTRIM(accepted.resolution_failure),'') IS NULL)::bigint
        AS resolved_page_count,
      COUNT(*) FILTER(WHERE NULLIF(BTRIM(accepted.resolution_failure),'') IS NOT NULL)::bigint
        AS failed_page_count,
      ROUND(COALESCE(SUM(COALESCE(
        (accepted.source_payload_json->>'raw_physical_amount_ex_vat')::numeric,0)),0),2)
        AS raw_amount_ex_vat,
      ROUND(COALESCE(SUM(COALESCE(accepted.source_amount_ex_vat,0)) FILTER(
        WHERE NULLIF(BTRIM(accepted.resolution_failure),'') IS NULL),0),2)
        AS resolved_amount_ex_vat,
      MAX(accepted.source_key) AS last_raw_source_key,
      md5(COALESCE(string_agg(accepted.source_key||':'||COALESCE(
        NULLIF(accepted.source_payload_json->>'raw_physical_digest',''),
        accepted.financial_digest),'' ORDER BY accepted.source_key),'')) AS evidence_digest,
      COALESCE(jsonb_agg(to_jsonb(accepted) ORDER BY accepted.source_key),'[]'::jsonb) AS rows_json
    FROM accepted
  )
  SELECT jsonb_build_object(
    'raw_page_count',evidence.raw_page_count,
    'resolved_page_count',evidence.resolved_page_count,
    'failed_page_count',evidence.failed_page_count,
    'raw_amount_ex_vat',evidence.raw_amount_ex_vat,
    'resolved_amount_ex_vat',evidence.resolved_amount_ex_vat,
    'raw_source_has_more',(SELECT COUNT(*)>LEAST(GREATEST(COALESCE(p_limit,25),1),25)
      FROM bounded),
    'raw_source_exhausted',NOT (SELECT COUNT(*)>LEAST(GREATEST(COALESCE(p_limit,25),1),25)
      FROM bounded),
    'last_raw_source_key',evidence.last_raw_source_key,
    'raw_terminal_source_key',CASE
      WHEN NOT (SELECT COUNT(*)>LEAST(GREATEST(COALESCE(p_limit,25),1),25) FROM bounded)
      THEN evidence.last_raw_source_key END,
    'evidence_digest',evidence.evidence_digest,
    'rows',evidence.rows_json
  )
  FROM evidence;
$function$;

ALTER FUNCTION private.pay_workbench_finance_item_authority_page_bundle_v1(
  uuid,uuid,uuid,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_finance_item_authority_page_bundle_v1(
  uuid,uuid,uuid,integer) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_finance_item_authority_page_bundle_v1(
  uuid,uuid,uuid,integer) TO postgres;
