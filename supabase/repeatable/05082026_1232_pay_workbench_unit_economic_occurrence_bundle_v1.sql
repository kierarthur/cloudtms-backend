-- Banking Pay bounded-scope Version 1.2.8.
-- Return one bounded physical occurrence page together with raw-source
-- evidence calculated before the dispatcher creates durable facts.

CREATE OR REPLACE FUNCTION private.pay_workbench_unit_economic_occurrence_bundle_v1(
  p_build_id uuid,
  p_dependency_unit_key text,
  p_fact_family text,
  p_projected_timesheet_id uuid,
  p_last_source_key text DEFAULT NULL::text,
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
    SELECT occurrence.*,
      row_number() OVER(ORDER BY occurrence.source_key) AS page_ordinal
    FROM private.pay_workbench_unit_economic_occurrence_page_v1(
      p_build_id,p_dependency_unit_key,p_fact_family,p_projected_timesheet_id,
      p_last_source_key,LEAST(GREATEST(COALESCE(p_limit,25),1),25)
    ) occurrence
    ORDER BY occurrence.source_key
  ), accepted AS MATERIALIZED (
    SELECT * FROM bounded
    WHERE page_ordinal<=LEAST(GREATEST(COALESCE(p_limit,25),1),25)
    ORDER BY source_key
  ), evidence AS (
    SELECT
      count(*) FILTER(WHERE COALESCE(source_payload_json->>'evidence_only','false')<>'true')::integer
        AS raw_page_count,
      count(*) FILTER(WHERE resolution_failure IS NULL
        AND COALESCE(source_payload_json->>'evidence_only','false')<>'true')::integer
        AS resolved_page_count,
      count(*) FILTER(WHERE resolution_failure IS NOT NULL
        AND COALESCE(source_payload_json->>'evidence_only','false')<>'true')::integer
        AS failed_page_count,
      round(COALESCE(sum(COALESCE(truth_ex_vat,amount_ex_vat,0)) FILTER(
        WHERE COALESCE(source_payload_json->>'evidence_only','false')<>'true'),0),2)
        AS raw_amount_ex_vat,
      round(COALESCE(sum(COALESCE(truth_ex_vat,amount_ex_vat,0))
        FILTER(WHERE resolution_failure IS NULL
          AND COALESCE(source_payload_json->>'evidence_only','false')<>'true'),0),2)
        AS resolved_amount_ex_vat,
      min(source_key) FILTER(WHERE COALESCE(source_payload_json->>'evidence_only','false')<>'true')
        AS first_raw_source_key,
      max(source_key) FILTER(WHERE COALESCE(source_payload_json->>'evidence_only','false')<>'true')
        AS last_raw_source_key
    FROM accepted
  )
  SELECT jsonb_build_object(
    'bundle_version',1,
    'build_id',p_build_id,
    'dependency_unit_key',p_dependency_unit_key,
    'fact_family',UPPER(BTRIM(p_fact_family)),
    'projected_timesheet_id',p_projected_timesheet_id,
    'cursor_start_source_key',p_last_source_key,
    'rows',COALESCE((SELECT jsonb_agg(to_jsonb(accepted)-'page_ordinal'
      ORDER BY accepted.source_key) FROM accepted),'[]'::jsonb),
    'raw_page_count',evidence.raw_page_count,
    'resolved_page_count',evidence.resolved_page_count,
    'failed_page_count',evidence.failed_page_count,
    'raw_amount_ex_vat',evidence.raw_amount_ex_vat,
    'resolved_amount_ex_vat',evidence.resolved_amount_ex_vat,
    'first_raw_source_key',evidence.first_raw_source_key,
    'last_raw_source_key',evidence.last_raw_source_key,
    'raw_source_has_more',(SELECT count(*)>
      LEAST(GREATEST(COALESCE(p_limit,25),1),25) FROM bounded),
    'raw_source_exhausted',(SELECT count(*)<=
      LEAST(GREATEST(COALESCE(p_limit,25),1),25) FROM bounded),
    'evidence_digest',md5(jsonb_build_object(
      'raw_page_count',evidence.raw_page_count,
      'resolved_page_count',evidence.resolved_page_count,
      'failed_page_count',evidence.failed_page_count,
      'raw_amount_ex_vat',evidence.raw_amount_ex_vat,
      'resolved_amount_ex_vat',evidence.resolved_amount_ex_vat,
      'first_raw_source_key',evidence.first_raw_source_key,
      'last_raw_source_key',evidence.last_raw_source_key,
      'raw_source_has_more',(SELECT count(*)>
        LEAST(GREATEST(COALESCE(p_limit,25),1),25) FROM bounded)
    )::text)
  )
  FROM evidence;
$function$;

ALTER FUNCTION private.pay_workbench_unit_economic_occurrence_bundle_v1(
  uuid,text,text,uuid,text,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_unit_economic_occurrence_bundle_v1(
  uuid,text,text,uuid,text,integer) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_unit_economic_occurrence_bundle_v1(
  uuid,text,text,uuid,text,integer) TO postgres;
