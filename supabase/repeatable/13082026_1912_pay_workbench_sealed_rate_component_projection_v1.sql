-- Banking Pay James Cases/Resolutions: project immutable physical rate
-- evidence from the sealed pre-Draft economic build.  This function never
-- consults live timesheet, candidate, Umbrella or finance-window authority.

CREATE OR REPLACE FUNCTION private.pay_workbench_sealed_rate_component_projection_v1(
  p_build_id uuid,
  p_candidate_id uuid,
  p_timesheet_ids uuid[] DEFAULT NULL::uuid[]
)
RETURNS TABLE(
  build_id uuid,
  candidate_id uuid,
  timesheet_id uuid,
  financial_row_id uuid,
  source_family_key text,
  economic_key_type text,
  economic_key_value text,
  component_kind text,
  component_member_identity text,
  segment_id text,
  segment_key text,
  segment_stable_key text,
  bucket_code text,
  bucket_sort_ordinal integer,
  physical_bucket_key text,
  physical_bucket_digest text,
  source_units numeric,
  source_rate numeric,
  source_charge_rate numeric,
  truth_ex_vat numeric,
  baseline_ex_vat numeric,
  reserved_ex_vat numeric,
  outstanding_ex_vat numeric,
  source_charge_ex_vat numeric,
  source_pay_method text,
  target_pay_method text,
  umbrella_id uuid,
  umbrella_enabled boolean,
  umbrella_vat_chargeable boolean,
  erni_pct numeric,
  vat_rate_pct numeric,
  financial_revision_digest text,
  target_authority_digest text,
  conversion_context_digest text,
  sealed_evidence_digest text,
  projection_status text,
  failure_code text,
  evidence_json jsonb
)
LANGUAGE sql
STABLE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
  WITH build_authority AS MATERIALIZED (
    SELECT build.id,build.candidate_id
    FROM private.banking_pay_workbench_economic_builds build
    WHERE build.id=p_build_id
  ), scoped_facts AS MATERIALIZED (
    SELECT fact.*,build.candidate_id AS build_candidate_id,
      fact.source_payload_json->'rate_authority' AS rate_authority
    FROM build_authority build
    JOIN private.banking_pay_workbench_economic_build_scope scope_row
      ON scope_row.build_id=build.id AND scope_row.candidate_id=build.candidate_id
    JOIN private.banking_pay_workbench_economic_build_facts fact
      ON fact.build_id=scope_row.build_id AND fact.timesheet_id=scope_row.timesheet_id
    WHERE build.candidate_id=p_candidate_id
      AND fact.fact_family='LIVE_ENTITLEMENT_INPUT'
      AND fact.source_relation<>'UNIT_PROJECTION'
      AND fact.timesheet_id IS NOT NULL
      AND (p_timesheet_ids IS NULL OR fact.timesheet_id=ANY(p_timesheet_ids))
  ), occurrence_base AS MATERIALIZED (
    SELECT fact.*,
      CASE
        WHEN jsonb_typeof(fact.rate_authority)<>'object'
          THEN 'RATE_AUTHORITY_PAYLOAD_VERSION_UNSUPPORTED'
        WHEN jsonb_typeof(fact.rate_authority->'rate_authority_version')<>'number'
          THEN 'RATE_AUTHORITY_PAYLOAD_VERSION_UNSUPPORTED'
        WHEN COALESCE(fact.rate_authority->>'rate_authority_version','') !~ '^\d+$'
          THEN 'RATE_AUTHORITY_PAYLOAD_VERSION_UNSUPPORTED'
        WHEN (fact.rate_authority->>'rate_authority_version')::integer<>1
          THEN 'RATE_AUTHORITY_PAYLOAD_VERSION_UNSUPPORTED'
        WHEN fact.build_candidate_id IS DISTINCT FROM p_candidate_id
          OR COALESCE(fact.rate_authority#>>'{build,candidate_id}','')
            IS DISTINCT FROM p_candidate_id::text
          THEN 'RATE_AUTHORITY_CANDIDATE_MISMATCH'
        WHEN NULLIF(BTRIM(COALESCE(fact.source_payload_json->>'resolution_failure','')),'')
          IS NOT NULL
          THEN COALESCE(NULLIF(BTRIM(fact.rate_authority->>'failure_code'),''),
            NULLIF(BTRIM(fact.source_payload_json->>'resolution_failure'),''))
        WHEN jsonb_typeof(fact.rate_authority->'physical_buckets')<>'array'
          THEN 'RATE_AUTHORITY_PHYSICAL_IDENTITY_INCOMPLETE'
        WHEN jsonb_array_length(fact.rate_authority->'physical_buckets')=0
          THEN 'RATE_AUTHORITY_PHYSICAL_IDENTITY_INCOMPLETE'
      END AS occurrence_failure
    FROM scoped_facts fact
  ), bucket_base AS MATERIALIZED (
    SELECT occurrence.*,
      bucket.value AS bucket_json,bucket.ordinality::integer AS bucket_ordinality,
      NULLIF(BTRIM(bucket.value->>'physical_bucket_key'),'') AS parsed_physical_bucket_key,
      NULLIF(BTRIM(bucket.value->>'component_kind'),'') AS parsed_component_kind,
      NULLIF(BTRIM(bucket.value->>'component_member_identity'),'')
        AS parsed_component_member_identity,
      NULLIF(BTRIM(bucket.value->>'segment_id'),'') AS parsed_segment_id,
      NULLIF(BTRIM(bucket.value->>'segment_key'),'') AS parsed_segment_key,
      NULLIF(BTRIM(bucket.value->>'segment_stable_key'),'') AS parsed_segment_stable_key,
      UPPER(NULLIF(BTRIM(bucket.value->>'bucket_code'),'')) AS parsed_bucket_code,
      CASE WHEN COALESCE(bucket.value->>'bucket_sort_ordinal','') ~ '^\d+$'
        THEN (bucket.value->>'bucket_sort_ordinal')::integer END AS parsed_bucket_sort_ordinal,
      CASE WHEN COALESCE(bucket.value->>'source_units','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((bucket.value->>'source_units')::numeric,6) END AS parsed_source_units,
      CASE WHEN COALESCE(bucket.value->>'source_rate','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((bucket.value->>'source_rate')::numeric,6) END AS parsed_source_rate,
      CASE WHEN COALESCE(bucket.value->>'source_charge_rate','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((bucket.value->>'source_charge_rate')::numeric,6) END
        AS parsed_source_charge_rate,
      CASE WHEN COALESCE(bucket.value->>'source_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((bucket.value->>'source_pay_ex_vat')::numeric,2) END
        AS parsed_source_pay_ex_vat,
      CASE WHEN COALESCE(bucket.value->>'source_charge_ex_vat','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((bucket.value->>'source_charge_ex_vat')::numeric,2) END
        AS parsed_source_charge_ex_vat,
      CASE WHEN COALESCE(bucket.value->>'baseline_source_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((bucket.value->>'baseline_source_pay_ex_vat')::numeric,2) END
        AS parsed_baseline_ex_vat,
      CASE WHEN COALESCE(bucket.value->>'reserved_source_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((bucket.value->>'reserved_source_pay_ex_vat')::numeric,2) END
        AS parsed_reserved_ex_vat,
      CASE WHEN COALESCE(bucket.value->>'outstanding_source_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((bucket.value->>'outstanding_source_pay_ex_vat')::numeric,2) END
        AS parsed_outstanding_ex_vat,
      UPPER(NULLIF(BTRIM(occurrence.rate_authority#>>'{source,source_pay_method}'),''))
        AS parsed_source_pay_method,
      UPPER(NULLIF(BTRIM(occurrence.rate_authority#>>'{target,target_pay_method}'),''))
        AS parsed_target_pay_method
    FROM occurrence_base occurrence
    CROSS JOIN LATERAL jsonb_array_elements(
      CASE WHEN jsonb_typeof(occurrence.rate_authority->'physical_buckets')='array'
        THEN occurrence.rate_authority->'physical_buckets' ELSE '[]'::jsonb END
    ) WITH ORDINALITY bucket(value,ordinality)
  ), bucket_canonical AS MATERIALIZED (
    SELECT bucket.*,
      jsonb_build_object(
        'physical_bucket_version',1,
        'physical_bucket_key',bucket.parsed_physical_bucket_key,
        'component_kind',bucket.parsed_component_kind,
        'component_member_identity',bucket.parsed_component_member_identity,
        'bucket_code',bucket.parsed_bucket_code,
        'source_units',bucket.parsed_source_units,
        'source_rate',bucket.parsed_source_rate,
        'source_charge_rate',bucket.parsed_source_charge_rate,
        'source_pay_ex_vat',bucket.parsed_source_pay_ex_vat,
        'source_charge_ex_vat',bucket.parsed_source_charge_ex_vat,
        'baseline_source_pay_ex_vat',bucket.parsed_baseline_ex_vat,
        'reserved_source_pay_ex_vat',bucket.parsed_reserved_ex_vat,
        'outstanding_source_pay_ex_vat',bucket.parsed_outstanding_ex_vat,
        'source_pay_method',bucket.parsed_source_pay_method,
        'target_pay_method',bucket.parsed_target_pay_method) AS physical_canonical_json
    FROM bucket_base bucket
  ), occurrence_physical AS MATERIALIZED (
    SELECT canonical.natural_key,
      md5(COALESCE(jsonb_agg(canonical.physical_canonical_json ORDER BY
        canonical.parsed_bucket_sort_ordinal,canonical.parsed_physical_bucket_key)::text,'[]'))
        AS recomputed_physical_bucket_digest,
      ROUND(COALESCE(SUM(canonical.parsed_source_pay_ex_vat),0),2) AS physical_truth_ex_vat,
      ROUND(COALESCE(SUM(canonical.parsed_baseline_ex_vat),0),2) AS physical_baseline_ex_vat,
      ROUND(COALESCE(SUM(canonical.parsed_reserved_ex_vat),0),2) AS physical_reserved_ex_vat
    FROM bucket_canonical canonical
    GROUP BY canonical.natural_key
  ), baseline_totals AS MATERIALIZED (
    SELECT baseline.timesheet_id,UPPER(BTRIM(baseline.economic_key_type)) AS economic_key_type,
      BTRIM(baseline.economic_key_value) AS economic_key_value,
      ROUND(COALESCE(SUM(baseline.amount_ex_vat),0),2) AS baseline_ex_vat
    FROM private.banking_pay_workbench_economic_build_facts baseline
    JOIN build_authority build ON build.id=baseline.build_id
    WHERE baseline.fact_family IN ('FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK')
      AND baseline.candidate_id=p_candidate_id
      AND (p_timesheet_ids IS NULL OR baseline.timesheet_id=ANY(p_timesheet_ids))
    GROUP BY baseline.timesheet_id,UPPER(BTRIM(baseline.economic_key_type)),
      BTRIM(baseline.economic_key_value)
  ), reservation_totals AS MATERIALIZED (
    SELECT reservation.timesheet_id,
      UPPER(BTRIM(reservation.economic_key_type)) AS economic_key_type,
      BTRIM(reservation.economic_key_value) AS economic_key_value,
      ROUND(COALESCE(SUM(reservation.reserved_source_amount),0),2) AS reserved_ex_vat
    FROM private.banking_pay_workbench_economic_build_facts reservation
    JOIN build_authority build ON build.id=reservation.build_id
    WHERE reservation.fact_family='RESERVATION_COMPONENT'
      AND reservation.candidate_id=p_candidate_id
      AND (p_timesheet_ids IS NULL OR reservation.timesheet_id=ANY(p_timesheet_ids))
    GROUP BY reservation.timesheet_id,UPPER(BTRIM(reservation.economic_key_type)),
      BTRIM(reservation.economic_key_value)
  ), economic_totals AS MATERIALIZED (
    SELECT live.timesheet_id,UPPER(BTRIM(live.economic_key_type)) AS economic_key_type,
      BTRIM(live.economic_key_value) AS economic_key_value,
      ROUND(COALESCE(SUM(live.truth_ex_vat),0),2) AS truth_ex_vat,
      ROUND(COALESCE(MIN(baseline.baseline_ex_vat),0),2) AS baseline_ex_vat,
      ROUND(COALESCE(MIN(reservation.reserved_ex_vat),0),2) AS reserved_ex_vat
    FROM scoped_facts live
    LEFT JOIN baseline_totals baseline
      ON baseline.timesheet_id=live.timesheet_id
     AND baseline.economic_key_type=UPPER(BTRIM(live.economic_key_type))
     AND baseline.economic_key_value=BTRIM(live.economic_key_value)
    LEFT JOIN reservation_totals reservation
      ON reservation.timesheet_id=live.timesheet_id
     AND reservation.economic_key_type=UPPER(BTRIM(live.economic_key_type))
     AND reservation.economic_key_value=BTRIM(live.economic_key_value)
    GROUP BY live.timesheet_id,UPPER(BTRIM(live.economic_key_type)),
      BTRIM(live.economic_key_value)
  ), occurrence_validated AS MATERIALIZED (
    SELECT occurrence.*,
      COALESCE(occurrence.occurrence_failure,
        CASE
          WHEN physical.recomputed_physical_bucket_digest IS DISTINCT FROM
            occurrence.rate_authority#>>'{economic,physical_bucket_digest}'
            THEN 'RATE_AUTHORITY_SEALED_DIGEST_MISMATCH'
          WHEN occurrence.rate_authority->>'sealed_evidence_digest' IS DISTINCT FROM
            md5(jsonb_build_object(
              'sealed_evidence_version',1,
              'financial_revision_digest',occurrence.rate_authority#>>'{source,financial_revision_digest}',
              'target_authority_digest',occurrence.rate_authority#>>'{target,target_authority_digest}',
              'conversion_context_digest',occurrence.rate_authority#>>'{conversion,conversion_context_digest}',
              'physical_bucket_digest',occurrence.rate_authority#>>'{economic,physical_bucket_digest}',
              'economic_key_type',occurrence.economic_key_type,
              'economic_key_value',occurrence.economic_key_value,
              'parent_source_pay_ex_vat',ROUND(occurrence.truth_ex_vat,2),
              'parent_source_charge_ex_vat',CASE
                WHEN COALESCE(occurrence.rate_authority#>>'{economic,parent_source_charge_ex_vat}','')
                  ~ '^-?\d+(\.\d+)?$'
                  THEN ROUND((occurrence.rate_authority#>>'{economic,parent_source_charge_ex_vat}')::numeric,2)
                ELSE NULL::numeric END,
              'source_pay_method',occurrence.rate_authority#>>'{source,source_pay_method}',
              'target_pay_method',occurrence.rate_authority#>>'{target,target_pay_method}')::text)
            THEN 'RATE_AUTHORITY_SEALED_DIGEST_MISMATCH'
        END) AS validated_failure
    FROM occurrence_base occurrence
    LEFT JOIN occurrence_physical physical USING(natural_key)
  ), bucket_validated AS MATERIALIZED (
    SELECT bucket.*,
      COALESCE(occurrence.validated_failure,
        CASE
          WHEN bucket.parsed_physical_bucket_key IS NULL
            OR bucket.parsed_component_kind IS NULL
            OR bucket.parsed_component_member_identity IS NULL
            OR bucket.parsed_bucket_code IS NULL
            OR bucket.parsed_bucket_sort_ordinal IS NULL
            THEN 'RATE_AUTHORITY_PHYSICAL_IDENTITY_INCOMPLETE'
          WHEN bucket.bucket_json->>'physical_bucket_digest'
            IS DISTINCT FROM md5(bucket.physical_canonical_json::text)
            THEN 'RATE_AUTHORITY_SEALED_DIGEST_MISMATCH'
        END) AS validated_failure
    FROM bucket_canonical bucket
    JOIN occurrence_validated occurrence USING(natural_key)
  ), economic_physical_totals AS MATERIALIZED (
    SELECT bucket.timesheet_id,UPPER(BTRIM(bucket.economic_key_type)) AS economic_key_type,
      BTRIM(bucket.economic_key_value) AS economic_key_value,
      ROUND(COALESCE(SUM(bucket.parsed_source_pay_ex_vat),0),2) AS truth_ex_vat,
      ROUND(COALESCE(SUM(bucket.parsed_baseline_ex_vat),0),2) AS baseline_ex_vat,
      ROUND(COALESCE(SUM(bucket.parsed_reserved_ex_vat),0),2) AS reserved_ex_vat
    FROM bucket_validated bucket
    WHERE bucket.validated_failure IS NULL
    GROUP BY bucket.timesheet_id,UPPER(BTRIM(bucket.economic_key_type)),
      BTRIM(bucket.economic_key_value)
  ), economic_failures AS MATERIALIZED (
    SELECT expected.timesheet_id,expected.economic_key_type,expected.economic_key_value,
      CASE
        WHEN actual.truth_ex_vat IS NULL OR actual.truth_ex_vat IS DISTINCT FROM expected.truth_ex_vat
          THEN 'RATE_AUTHORITY_PARENT_PAY_MISMATCH'
        WHEN actual.baseline_ex_vat IS DISTINCT FROM expected.baseline_ex_vat
          THEN 'RATE_AUTHORITY_PHYSICAL_BASELINE_REQUIRED'
        WHEN actual.reserved_ex_vat IS DISTINCT FROM expected.reserved_ex_vat
          THEN 'RATE_AUTHORITY_PHYSICAL_RESERVATION_REQUIRED'
      END AS failure_code
    FROM economic_totals expected
    LEFT JOIN economic_physical_totals actual
      USING(timesheet_id,economic_key_type,economic_key_value)
    WHERE actual.truth_ex_vat IS DISTINCT FROM expected.truth_ex_vat
       OR actual.baseline_ex_vat IS DISTINCT FROM expected.baseline_ex_vat
       OR actual.reserved_ex_vat IS DISTINCT FROM expected.reserved_ex_vat
  ), failed_occurrences AS (
    SELECT occurrence.build_id,occurrence.build_candidate_id AS candidate_id,
      occurrence.timesheet_id,
      CASE WHEN COALESCE(occurrence.rate_authority#>>'{source,financial_row_id}','')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (occurrence.rate_authority#>>'{source,financial_row_id}')::uuid END AS financial_row_id,
      occurrence.rate_authority#>>'{economic,source_family_key}' AS source_family_key,
      occurrence.economic_key_type,occurrence.economic_key_value,
      occurrence.rate_authority#>>'{economic,component_kind}' AS component_kind,
      NULL::text AS component_member_identity,NULL::text AS segment_id,
      NULL::text AS segment_key,NULL::text AS segment_stable_key,NULL::text AS bucket_code,
      NULL::integer AS bucket_sort_ordinal,NULL::text AS physical_bucket_key,
      NULL::text AS physical_bucket_digest,NULL::numeric AS source_units,
      NULL::numeric AS source_rate,NULL::numeric AS source_charge_rate,
      ROUND(occurrence.truth_ex_vat,2) AS truth_ex_vat,
      0::numeric AS baseline_ex_vat,0::numeric AS reserved_ex_vat,
      ROUND(occurrence.truth_ex_vat,2) AS outstanding_ex_vat,
      NULL::numeric AS source_charge_ex_vat,
      UPPER(NULLIF(BTRIM(occurrence.rate_authority#>>'{source,source_pay_method}'),''))
        AS source_pay_method,
      UPPER(NULLIF(BTRIM(occurrence.rate_authority#>>'{target,target_pay_method}'),''))
        AS target_pay_method,
      CASE WHEN COALESCE(occurrence.rate_authority#>>'{target,umbrella_id}','')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (occurrence.rate_authority#>>'{target,umbrella_id}')::uuid END AS umbrella_id,
      CASE WHEN jsonb_typeof(occurrence.rate_authority#>'{target,umbrella_enabled}')='boolean'
        THEN (occurrence.rate_authority#>>'{target,umbrella_enabled}')::boolean END
        AS umbrella_enabled,
      CASE WHEN jsonb_typeof(occurrence.rate_authority#>'{target,umbrella_vat_chargeable}')='boolean'
        THEN (occurrence.rate_authority#>>'{target,umbrella_vat_chargeable}')::boolean END
        AS umbrella_vat_chargeable,
      CASE WHEN COALESCE(occurrence.rate_authority#>>'{conversion,erni_pct}','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((occurrence.rate_authority#>>'{conversion,erni_pct}')::numeric,6) END AS erni_pct,
      CASE WHEN COALESCE(occurrence.rate_authority#>>'{conversion,vat_rate_pct}','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((occurrence.rate_authority#>>'{conversion,vat_rate_pct}')::numeric,6) END AS vat_rate_pct,
      occurrence.rate_authority#>>'{source,financial_revision_digest}' AS financial_revision_digest,
      occurrence.rate_authority#>>'{target,target_authority_digest}' AS target_authority_digest,
      occurrence.rate_authority#>>'{conversion,conversion_context_digest}'
        AS conversion_context_digest,
      occurrence.rate_authority->>'sealed_evidence_digest' AS sealed_evidence_digest,
      'FAILED'::text AS projection_status,
      COALESCE(occurrence.validated_failure,economic.failure_code) AS failure_code,
      jsonb_build_object('source_key',occurrence.source_payload_json->>'source_key',
        'source_ordinal',occurrence.source_payload_json#>'{rate_authority,lineage,source_ordinal}',
        'source_payload',occurrence.source_payload_json,'physical_bucket',NULL) AS evidence_json
    FROM occurrence_validated occurrence
    LEFT JOIN economic_failures economic
      ON economic.timesheet_id=occurrence.timesheet_id
     AND economic.economic_key_type=UPPER(BTRIM(occurrence.economic_key_type))
     AND economic.economic_key_value=BTRIM(occurrence.economic_key_value)
    WHERE COALESCE(occurrence.validated_failure,economic.failure_code) IS NOT NULL
  ), bucket_outputs AS (
    SELECT bucket.build_id,bucket.build_candidate_id AS candidate_id,bucket.timesheet_id,
      CASE WHEN COALESCE(bucket.rate_authority#>>'{source,financial_row_id}','')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (bucket.rate_authority#>>'{source,financial_row_id}')::uuid END AS financial_row_id,
      bucket.rate_authority#>>'{economic,source_family_key}' AS source_family_key,
      UPPER(BTRIM(bucket.economic_key_type)) AS economic_key_type,
      BTRIM(bucket.economic_key_value) AS economic_key_value,
      bucket.parsed_component_kind,bucket.parsed_component_member_identity,
      bucket.parsed_segment_id,bucket.parsed_segment_key,bucket.parsed_segment_stable_key,
      bucket.parsed_bucket_code,bucket.parsed_bucket_sort_ordinal,
      bucket.parsed_physical_bucket_key,bucket.bucket_json->>'physical_bucket_digest',
      bucket.parsed_source_units,bucket.parsed_source_rate,bucket.parsed_source_charge_rate,
      bucket.parsed_source_pay_ex_vat,bucket.parsed_baseline_ex_vat,
      bucket.parsed_reserved_ex_vat,bucket.parsed_outstanding_ex_vat,
      bucket.parsed_source_charge_ex_vat,bucket.parsed_source_pay_method,
      bucket.parsed_target_pay_method,
      CASE WHEN COALESCE(bucket.rate_authority#>>'{target,umbrella_id}','')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (bucket.rate_authority#>>'{target,umbrella_id}')::uuid END AS umbrella_id,
      CASE WHEN jsonb_typeof(bucket.rate_authority#>'{target,umbrella_enabled}')='boolean'
        THEN (bucket.rate_authority#>>'{target,umbrella_enabled}')::boolean END,
      CASE WHEN jsonb_typeof(bucket.rate_authority#>'{target,umbrella_vat_chargeable}')='boolean'
        THEN (bucket.rate_authority#>>'{target,umbrella_vat_chargeable}')::boolean END,
      CASE WHEN COALESCE(bucket.rate_authority#>>'{conversion,erni_pct}','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((bucket.rate_authority#>>'{conversion,erni_pct}')::numeric,6) END,
      CASE WHEN COALESCE(bucket.rate_authority#>>'{conversion,vat_rate_pct}','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((bucket.rate_authority#>>'{conversion,vat_rate_pct}')::numeric,6) END,
      bucket.rate_authority#>>'{source,financial_revision_digest}',
      bucket.rate_authority#>>'{target,target_authority_digest}',
      bucket.rate_authority#>>'{conversion,conversion_context_digest}',
      bucket.rate_authority->>'sealed_evidence_digest',
      CASE WHEN jsonb_typeof(bucket.bucket_json->'is_actionable_candidate')='boolean'
        AND (bucket.bucket_json->>'is_actionable_candidate')::boolean
        THEN 'READY' ELSE 'FIXED' END AS projection_status,
      COALESCE(bucket.validated_failure,economic.failure_code) AS failure_code,
      jsonb_build_object('source_key',bucket.source_payload_json->>'source_key',
        'source_ordinal',bucket.source_payload_json#>'{rate_authority,lineage,source_ordinal}',
        'source_payload',bucket.source_payload_json,
        'physical_bucket',bucket.bucket_json) AS evidence_json
    FROM bucket_validated bucket
    LEFT JOIN economic_failures economic
      ON economic.timesheet_id=bucket.timesheet_id
     AND economic.economic_key_type=UPPER(BTRIM(bucket.economic_key_type))
     AND economic.economic_key_value=BTRIM(bucket.economic_key_value)
    WHERE NOT EXISTS(SELECT 1 FROM failed_occurrences failed
      WHERE failed.build_id=bucket.build_id AND failed.timesheet_id=bucket.timesheet_id
        AND failed.economic_key_type=bucket.economic_key_type
        AND failed.economic_key_value=bucket.economic_key_value)
  ), missing_authority AS (
    SELECT p_build_id,p_candidate_id,NULL::uuid,NULL::uuid,NULL::text,NULL::text,NULL::text,
      NULL::text,NULL::text,NULL::text,NULL::text,NULL::text,NULL::text,NULL::integer,
      NULL::text,NULL::text,NULL::numeric,NULL::numeric,NULL::numeric,NULL::numeric,
      NULL::numeric,NULL::numeric,NULL::numeric,NULL::numeric,NULL::text,NULL::text,
      NULL::uuid,NULL::boolean,NULL::boolean,NULL::numeric,NULL::numeric,NULL::text,NULL::text,
      NULL::text,NULL::text,'FAILED'::text,
      CASE
        WHEN NOT EXISTS(SELECT 1 FROM build_authority)
          THEN 'RATE_AUTHORITY_BUILD_NOT_FOUND'::text
        WHEN EXISTS(SELECT 1 FROM build_authority build
          WHERE build.candidate_id IS DISTINCT FROM p_candidate_id)
          THEN 'RATE_AUTHORITY_CANDIDATE_MISMATCH'::text
        ELSE 'RATE_AUTHORITY_SCOPE_NOT_FOUND'::text
      END,
      jsonb_build_object('build_id',p_build_id::text,'candidate_id',p_candidate_id::text)
    WHERE NOT EXISTS(SELECT 1 FROM scoped_facts)
  )
  SELECT * FROM failed_occurrences
  UNION ALL SELECT * FROM bucket_outputs
  UNION ALL SELECT * FROM missing_authority
  ORDER BY timesheet_id NULLS FIRST,economic_key_type NULLS FIRST,
    economic_key_value NULLS FIRST,bucket_sort_ordinal NULLS FIRST,
    physical_bucket_key NULLS FIRST;
$function$;

ALTER FUNCTION private.pay_workbench_sealed_rate_component_projection_v1(uuid,uuid,uuid[])
  OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_sealed_rate_component_projection_v1(uuid,uuid,uuid[])
  FROM PUBLIC;
REVOKE ALL ON FUNCTION private.pay_workbench_sealed_rate_component_projection_v1(uuid,uuid,uuid[])
  FROM anon;
REVOKE ALL ON FUNCTION private.pay_workbench_sealed_rate_component_projection_v1(uuid,uuid,uuid[])
  FROM authenticated;
REVOKE ALL ON FUNCTION private.pay_workbench_sealed_rate_component_projection_v1(uuid,uuid,uuid[])
  FROM service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_sealed_rate_component_projection_v1(uuid,uuid,uuid[])
  TO postgres;
