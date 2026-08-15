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
  ), candidate_context_values AS MATERIALIZED (
    SELECT DISTINCT
      UPPER(NULLIF(BTRIM(fact.rate_authority#>>'{target,target_pay_method}'),''))
        AS target_pay_method,
      NULLIF(BTRIM(fact.rate_authority#>>'{target,target_authority_digest}'),'')
        AS target_authority_digest,
      NULLIF(BTRIM(fact.rate_authority#>>'{conversion,conversion_context_digest}'),'')
        AS conversion_context_digest,
      CASE WHEN COALESCE(fact.rate_authority#>>'{target,umbrella_id}','')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (fact.rate_authority#>>'{target,umbrella_id}')::uuid END AS umbrella_id,
      CASE WHEN jsonb_typeof(fact.rate_authority#>'{target,umbrella_enabled}')='boolean'
        THEN (fact.rate_authority#>>'{target,umbrella_enabled}')::boolean END
        AS umbrella_enabled,
      CASE WHEN jsonb_typeof(fact.rate_authority#>'{target,umbrella_vat_chargeable}')='boolean'
        THEN (fact.rate_authority#>>'{target,umbrella_vat_chargeable}')::boolean END
        AS umbrella_vat_chargeable,
      CASE WHEN COALESCE(fact.rate_authority#>>'{conversion,erni_pct}','')
          ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((fact.rate_authority#>>'{conversion,erni_pct}')::numeric,6) END AS erni_pct,
      CASE WHEN COALESCE(fact.rate_authority#>>'{conversion,vat_rate_pct}','')
          ~ '^-?\d+(\.\d+)?$'
        THEN ROUND((fact.rate_authority#>>'{conversion,vat_rate_pct}')::numeric,6) END
        AS vat_rate_pct
    FROM scoped_facts fact
    WHERE jsonb_typeof(fact.rate_authority)='object'
      AND COALESCE(fact.rate_authority->>'rate_authority_version','')='1'
  ), candidate_sealed_context AS MATERIALIZED (
    SELECT MIN(context.target_pay_method) AS target_pay_method,
      MIN(context.target_authority_digest) AS target_authority_digest,
      MIN(context.conversion_context_digest) AS conversion_context_digest,
      MIN(context.umbrella_id::text)::uuid AS umbrella_id,
      BOOL_AND(context.umbrella_enabled) AS umbrella_enabled,
      BOOL_AND(context.umbrella_vat_chargeable) AS umbrella_vat_chargeable,
      MIN(context.erni_pct) AS erni_pct,MIN(context.vat_rate_pct) AS vat_rate_pct,
      CASE
        WHEN COUNT(*)=0 OR COUNT(context.target_authority_digest)=0
          OR COUNT(context.conversion_context_digest)=0
          THEN 'RATE_AUTHORITY_TARGET_AUTHORITY_NOT_SEALED'
        WHEN COUNT(DISTINCT context.target_authority_digest)<>1
          OR COUNT(DISTINCT context.conversion_context_digest)<>1
          OR COUNT(DISTINCT context.target_pay_method)<>1
          THEN 'RATE_AUTHORITY_TARGET_AUTHORITY_CONFLICT'
      END AS context_failure
    FROM candidate_context_values context
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
        -- Validate the serializer-owned sealed payload against its frozen V1
        -- canonical document.  The helper publishes a separate attributed V2
        -- document after baseline/reservation ownership has been resolved.
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
  ), entitlement_economic_domain AS MATERIALIZED (
    SELECT entitlement.timesheet_id,UPPER(BTRIM(entitlement.key_type)) AS economic_key_type,
      BTRIM(entitlement.key_value) AS economic_key_value,
      ROUND(COALESCE(SUM(entitlement.truth_ex_vat),0),2) AS truth_ex_vat,
      ROUND(COALESCE(SUM(entitlement.baseline_ex_vat),0),2) AS baseline_ex_vat,
      ROUND(COALESCE(SUM(entitlement.truth_inc_vat),SUM(entitlement.truth_ex_vat),0),2)
        AS truth_inc_vat,
      ROUND(COALESCE(SUM(entitlement.baseline_inc_vat),SUM(entitlement.baseline_ex_vat),0),2)
        AS baseline_inc_vat
    FROM private.pay_current_timesheet_entitlement_components_from_build_v1(
      p_build_id,NULL::text) entitlement
    WHERE entitlement.timesheet_id IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(entitlement.key_type,'')),'') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(entitlement.key_value,'')),'') IS NOT NULL
      AND (p_timesheet_ids IS NULL OR entitlement.timesheet_id=ANY(p_timesheet_ids))
    GROUP BY entitlement.timesheet_id,UPPER(BTRIM(entitlement.key_type)),
      BTRIM(entitlement.key_value)
  ), economic_totals AS MATERIALIZED (
    SELECT domain.timesheet_id,domain.economic_key_type,domain.economic_key_value,
      domain.truth_ex_vat,domain.baseline_ex_vat,
      ROUND(COALESCE(reservation.reserved_ex_vat,0),2) AS reserved_ex_vat,
      domain.truth_inc_vat,domain.baseline_inc_vat
    FROM entitlement_economic_domain domain
    LEFT JOIN reservation_totals reservation
      USING(timesheet_id,economic_key_type,economic_key_value)
    UNION ALL
    SELECT reservation.timesheet_id,reservation.economic_key_type,
      reservation.economic_key_value,0::numeric AS truth_ex_vat,
      0::numeric AS baseline_ex_vat,reservation.reserved_ex_vat,
      0::numeric AS truth_inc_vat,0::numeric AS baseline_inc_vat
    FROM reservation_totals reservation
    WHERE NOT EXISTS(
      SELECT 1 FROM entitlement_economic_domain domain
      WHERE domain.timesheet_id=reservation.timesheet_id
        AND domain.economic_key_type=reservation.economic_key_type
        AND domain.economic_key_value=reservation.economic_key_value)
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
  ), physical_bucket_cardinality AS MATERIALIZED (
    SELECT bucket.timesheet_id,UPPER(BTRIM(bucket.economic_key_type)) AS economic_key_type,
      BTRIM(bucket.economic_key_value) AS economic_key_value,
      COUNT(DISTINCT bucket.parsed_physical_bucket_key)::integer AS physical_bucket_count
    FROM bucket_validated bucket
    WHERE bucket.validated_failure IS NULL
    GROUP BY bucket.timesheet_id,UPPER(BTRIM(bucket.economic_key_type)),
      BTRIM(bucket.economic_key_value)
  ), sealed_parent_facts AS MATERIALIZED (
    SELECT fact.fact_family||':'||fact.natural_key AS fact_identity,
      CASE WHEN fact.fact_family='RESERVATION_COMPONENT' THEN 'RESERVATION'
        ELSE 'BASELINE' END AS authority_kind,
      fact.timesheet_id,UPPER(BTRIM(fact.economic_key_type)) AS economic_key_type,
      BTRIM(fact.economic_key_value) AS economic_key_value,
      ROUND(CASE WHEN fact.fact_family='RESERVATION_COMPONENT'
        THEN COALESCE(fact.reserved_source_amount,0)
        ELSE COALESCE(fact.amount_ex_vat,0) END,2) AS parent_amount_ex_vat,
      CASE WHEN fact.fact_family='RESERVATION_COMPONENT' THEN NULL::numeric
        WHEN COALESCE(
          fact.source_payload_json#>>'{pay_batch_item,frozen_source_basis_json,source_charge_ex_vat}',
          fact.source_payload_json#>>'{pay_batch_item,frozen_component_snapshot_json,source_basis_json,source_charge_ex_vat}',
          fact.source_payload_json#>>'{pay_batch_item,frozen_resolution_result_json,source_charge_ex_vat}',
          fact.source_payload_json#>>'{frozen_source_basis_json,source_charge_ex_vat}',
          fact.source_payload_json#>>'{frozen_component_snapshot_json,source_basis_json,source_charge_ex_vat}',
          fact.source_payload_json#>>'{frozen_resolution_result_json,source_charge_ex_vat}','')
            ~ '^-?\d+(\.\d+)?$'
          THEN ROUND(COALESCE(
            fact.source_payload_json#>>'{pay_batch_item,frozen_source_basis_json,source_charge_ex_vat}',
            fact.source_payload_json#>>'{pay_batch_item,frozen_component_snapshot_json,source_basis_json,source_charge_ex_vat}',
            fact.source_payload_json#>>'{pay_batch_item,frozen_resolution_result_json,source_charge_ex_vat}',
            fact.source_payload_json#>>'{frozen_source_basis_json,source_charge_ex_vat}',
            fact.source_payload_json#>>'{frozen_component_snapshot_json,source_basis_json,source_charge_ex_vat}',
            fact.source_payload_json#>>'{frozen_resolution_result_json,source_charge_ex_vat}')::numeric,2)
        END AS parent_source_charge_ex_vat,
      fact.source_payload_json,fact.financial_digest,
      UPPER(NULLIF(BTRIM(COALESCE(
        fact.source_payload_json#>>'{pay_batch_item,frozen_source_pay_method}',
        fact.source_payload_json->>'frozen_source_pay_method')),'')) AS sealed_source_pay_method,
      UPPER(NULLIF(BTRIM(fact.source_payload_json->>'item_type'),'')) AS parent_item_type,
      UPPER(NULLIF(BTRIM(fact.source_payload_json->>'key_resolution_source'),''))
        AS key_resolution_source,
      UPPER(NULLIF(BTRIM(fact.source_payload_json->>'item_type'),''))='OVERPAYMENT_RECOVERY'
        AND UPPER(NULLIF(BTRIM(fact.source_payload_json->>'key_resolution_source'),''))=
          'FINANCE_ITEM_AUTHORITY' AS is_signed_non_charge_recovery
    FROM private.banking_pay_workbench_economic_build_facts fact
    JOIN build_authority build ON build.id=fact.build_id
    WHERE fact.fact_family IN ('FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK',
        'RESERVATION_COMPONENT')
      AND fact.candidate_id=p_candidate_id AND fact.timesheet_id IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(fact.economic_key_type,'')),'') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(fact.economic_key_value,'')),'') IS NOT NULL
      AND (p_timesheet_ids IS NULL OR fact.timesheet_id=ANY(p_timesheet_ids))
  ), allocative_parent_facts AS MATERIALIZED (
    SELECT parent.*
    FROM sealed_parent_facts parent
    WHERE parent.is_signed_non_charge_recovery IS NOT TRUE
  ), nested_evidence_shape_failures AS MATERIALIZED (
    SELECT parent.fact_identity,
      'RATE_AUTHORITY_NESTED_EVIDENCE_INVALID'::text AS failure_code
    FROM allocative_parent_facts parent
    WHERE (parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,case_components}' IS NOT NULL
        AND jsonb_typeof(parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,case_components}')<>'array')
       OR (parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,correction_chain_component,resolution_rows}' IS NOT NULL
        AND jsonb_typeof(parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,correction_chain_component,resolution_rows}')<>'array')
       OR (parent.source_payload_json#>'{pay_batch_item,frozen_source_basis_json,correction_chain_residual,components}' IS NOT NULL
        AND jsonb_typeof(parent.source_payload_json#>'{pay_batch_item,frozen_source_basis_json,correction_chain_residual,components}')<>'array')
       OR (parent.source_payload_json#>'{pay_batch_item,frozen_component_snapshot_json,correction_chain_residual,components}' IS NOT NULL
        AND jsonb_typeof(parent.source_payload_json#>'{pay_batch_item,frozen_component_snapshot_json,correction_chain_residual,components}')<>'array')
       OR (parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,correction_chain_residual,components}' IS NOT NULL
        AND jsonb_typeof(parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,correction_chain_residual,components}')<>'array')
    UNION
    SELECT parent.fact_identity,'RATE_AUTHORITY_NESTED_EVIDENCE_INVALID'::text
    FROM allocative_parent_facts parent
    CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(
      parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,case_components}')='array'
      THEN parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,case_components}'
      ELSE '[]'::jsonb END) component(value)
    WHERE component.value#>'{saved_resolution_payload_json,bucket_resolutions}' IS NOT NULL
      AND jsonb_typeof(component.value#>'{saved_resolution_payload_json,bucket_resolutions}')<>'array'
    UNION
    SELECT parent.fact_identity,'RATE_AUTHORITY_NESTED_EVIDENCE_INVALID'::text
    FROM allocative_parent_facts parent
    CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(
      parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,correction_chain_component,resolution_rows}')='array'
      THEN parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,correction_chain_component,resolution_rows}'
      ELSE '[]'::jsonb END) resolution(value)
    WHERE resolution.value#>'{payload_json,bucket_resolutions}' IS NOT NULL
      AND jsonb_typeof(resolution.value#>'{payload_json,bucket_resolutions}')<>'array'
  ), nested_evidence_raw AS MATERIALIZED (
    SELECT parent.*, 'TOP_LEVEL_SOURCE_BASIS'::text AS evidence_origin,
      NULL::text AS evidence_container_identity,basis.source_basis_json AS evidence_json
    FROM allocative_parent_facts parent
    CROSS JOIN LATERAL (
      SELECT source_basis_json
      FROM (VALUES
        (CASE WHEN jsonb_typeof(parent.source_payload_json#>'{pay_batch_item,frozen_source_basis_json}')='object'
          THEN parent.source_payload_json#>'{pay_batch_item,frozen_source_basis_json}' END),
        (CASE WHEN jsonb_typeof(parent.source_payload_json#>'{pay_batch_item,frozen_component_snapshot_json,source_basis_json}')='object'
          THEN parent.source_payload_json#>'{pay_batch_item,frozen_component_snapshot_json,source_basis_json}' END),
        (CASE WHEN jsonb_typeof(parent.source_payload_json->'frozen_source_basis_json')='object'
          THEN parent.source_payload_json->'frozen_source_basis_json' END),
        (CASE WHEN jsonb_typeof(parent.source_payload_json#>'{frozen_component_snapshot_json,source_basis_json}')='object'
          THEN parent.source_payload_json#>'{frozen_component_snapshot_json,source_basis_json}' END)
      ) basis_values(source_basis_json)
    ) basis
    WHERE basis.source_basis_json IS NOT NULL AND basis.source_basis_json<>'{}'::jsonb
    UNION ALL
    SELECT parent.*,'CASE_COMPONENT',COALESCE(NULLIF(component.value->>'source_basis_fingerprint',''),
      md5(component.value::text)),component.value
    FROM allocative_parent_facts parent
    CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(
      parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,case_components}')='array'
      THEN parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,case_components}'
      ELSE '[]'::jsonb END) component(value)
    UNION ALL
    SELECT parent.*,'CASE_BUCKET_RESOLUTION',COALESCE(
      NULLIF(component.value->>'source_basis_fingerprint',''),md5(component.value::text)),bucket.value
    FROM allocative_parent_facts parent
    CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(
      parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,case_components}')='array'
      THEN parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,case_components}'
      ELSE '[]'::jsonb END) component(value)
    CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(
      component.value#>'{saved_resolution_payload_json,bucket_resolutions}')='array'
      THEN component.value#>'{saved_resolution_payload_json,bucket_resolutions}'
      ELSE '[]'::jsonb END) bucket(value)
    UNION ALL
    SELECT parent.*,'CORRECTION_BUCKET_RESOLUTION',COALESCE(
      NULLIF(resolution.value->>'source_basis_fingerprint',''),md5(resolution.value::text)),bucket.value
    FROM allocative_parent_facts parent
    CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(
      parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,correction_chain_component,resolution_rows}')='array'
      THEN parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,correction_chain_component,resolution_rows}'
      ELSE '[]'::jsonb END) resolution(value)
    CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(
      resolution.value#>'{payload_json,bucket_resolutions}')='array'
      THEN resolution.value#>'{payload_json,bucket_resolutions}' ELSE '[]'::jsonb END) bucket(value)
    UNION ALL
    SELECT parent.*,'SOURCE_BASIS_CORRECTION_RESIDUAL',NULL::text,residual.value
    FROM allocative_parent_facts parent
    CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(
      parent.source_payload_json#>'{pay_batch_item,frozen_source_basis_json,correction_chain_residual,components}')='array'
      THEN parent.source_payload_json#>'{pay_batch_item,frozen_source_basis_json,correction_chain_residual,components}'
      ELSE '[]'::jsonb END) residual(value)
    UNION ALL
    SELECT parent.*,'SNAPSHOT_CORRECTION_RESIDUAL',NULL::text,residual.value
    FROM allocative_parent_facts parent
    CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(
      parent.source_payload_json#>'{pay_batch_item,frozen_component_snapshot_json,correction_chain_residual,components}')='array'
      THEN parent.source_payload_json#>'{pay_batch_item,frozen_component_snapshot_json,correction_chain_residual,components}'
      ELSE '[]'::jsonb END) residual(value)
    UNION ALL
    SELECT parent.*,'RESOLUTION_CORRECTION_RESIDUAL',NULL::text,residual.value
    FROM allocative_parent_facts parent
    CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(
      parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,correction_chain_residual,components}')='array'
      THEN parent.source_payload_json#>'{pay_batch_item,frozen_resolution_payload_json,correction_chain_residual,components}'
      ELSE '[]'::jsonb END) residual(value)
  ), nested_evidence_parsed AS MATERIALIZED (
    SELECT raw.*,
      CASE WHEN jsonb_typeof(raw.evidence_json->'source_basis_json')='object'
        THEN raw.evidence_json->'source_basis_json' ELSE raw.evidence_json END AS source_basis_json,
      NULLIF(BTRIM(COALESCE(raw.evidence_json->>'physical_bucket_key',
        raw.evidence_json#>>'{source_basis_json,physical_bucket_key}')),'') AS direct_physical_bucket_key,
      NULLIF(BTRIM(COALESCE(raw.evidence_json->>'component_member_identity',
        raw.evidence_json#>>'{source_basis_json,component_member_identity}')),'')
        AS direct_component_member_identity,
      NULLIF(BTRIM(COALESCE(raw.evidence_json->>'source_family_key',
        raw.evidence_json#>>'{source_basis_json,source_family_key}',
        raw.evidence_json#>>'{source_basis_json,represented_source_family_key}')),'')
        AS direct_source_family_key,
      NULLIF(BTRIM(COALESCE(raw.evidence_json->>'source_basis_fingerprint',
        raw.evidence_json#>>'{source_basis_json,source_basis_fingerprint}')),'')
        AS source_basis_fingerprint,
      NULLIF(BTRIM(COALESCE(raw.evidence_json->>'component_fingerprint',
        raw.evidence_json#>>'{source_basis_json,component_fingerprint}')),'')
        AS component_fingerprint,
      UPPER(NULLIF(BTRIM(COALESCE(raw.evidence_json->>'source_pay_method',
        raw.evidence_json#>>'{source_basis_json,source_pay_method}')),''))
        AS evidence_source_pay_method,
      UPPER(NULLIF(BTRIM(COALESCE(raw.evidence_json->>'component_fallback',
        raw.evidence_json#>>'{source_basis_json,component_fallback}')),''))
        AS component_fallback
    FROM nested_evidence_raw raw
  ), source_method_evidence AS MATERIALIZED (
    SELECT occurrence.timesheet_id,
      UPPER(BTRIM(occurrence.economic_key_type)) AS economic_key_type,
      BTRIM(occurrence.economic_key_value) AS economic_key_value,
      NULLIF(BTRIM(occurrence.rate_authority#>>'{source,source_pay_method}'),'')
        AS raw_source_pay_method,
      UPPER(NULLIF(BTRIM(occurrence.rate_authority#>>'{source,source_pay_method}'),''))
        AS normalized_source_pay_method,
      10::integer AS authority_priority,
      'LIVE_OCCURRENCE'::text AS evidence_kind,
      occurrence.natural_key AS evidence_identity
    FROM occurrence_validated occurrence
    WHERE occurrence.validated_failure IS NULL
      AND NULLIF(BTRIM(COALESCE(occurrence.economic_key_type,'')),'') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(occurrence.economic_key_value,'')),'') IS NOT NULL
    UNION ALL
    SELECT parent.timesheet_id,parent.economic_key_type,parent.economic_key_value,
      NULLIF(BTRIM(parent.sealed_source_pay_method),''),
      UPPER(NULLIF(BTRIM(parent.sealed_source_pay_method),'')),
      20::integer,
      'SEALED_PARENT',parent.fact_identity
    FROM sealed_parent_facts parent
    UNION ALL
    SELECT nested.timesheet_id,nested.economic_key_type,nested.economic_key_value,
      NULLIF(BTRIM(nested.evidence_source_pay_method),''),
      UPPER(NULLIF(BTRIM(nested.evidence_source_pay_method),'')),
      20::integer,
      'NESTED_'||nested.evidence_origin,
      nested.fact_identity||':'||COALESCE(nested.evidence_container_identity,
        md5(nested.evidence_json::text))
    FROM nested_evidence_parsed nested
  ), source_method_evidence_documents AS MATERIALIZED (
    SELECT evidence.*,
      jsonb_build_object(
        'authority_priority',evidence.authority_priority,
        'evidence_kind',evidence.evidence_kind,
        'evidence_identity',evidence.evidence_identity,
        'source_pay_method',evidence.normalized_source_pay_method) AS evidence_document
    FROM source_method_evidence evidence
  ), source_method_authority_tier AS MATERIALIZED (
    SELECT evidence.timesheet_id,evidence.economic_key_type,evidence.economic_key_value,
      MIN(evidence.authority_priority)::integer AS selected_authority_priority
    FROM source_method_evidence_documents evidence
    GROUP BY evidence.timesheet_id,evidence.economic_key_type,evidence.economic_key_value
  ), source_method_authority_summary AS MATERIALIZED (
    SELECT evidence.timesheet_id,evidence.economic_key_type,evidence.economic_key_value,
      COUNT(*)::integer AS total_evidence_count,
      COUNT(*) FILTER(WHERE evidence.authority_priority=tier.selected_authority_priority)::integer
        AS selected_evidence_count,
      tier.selected_authority_priority,
      COUNT(*) FILTER(WHERE evidence.authority_priority=tier.selected_authority_priority
        AND evidence.normalized_source_pay_method IN ('PAYE','UMBRELLA'))::integer
        AS supported_method_evidence_count,
      COUNT(*) FILTER(WHERE evidence.authority_priority=tier.selected_authority_priority
        AND evidence.raw_source_pay_method IS NOT NULL
        AND evidence.normalized_source_pay_method NOT IN ('PAYE','UMBRELLA'))::integer
        AS invalid_method_count,
      COUNT(DISTINCT evidence.normalized_source_pay_method) FILTER(
        WHERE evidence.authority_priority=tier.selected_authority_priority
          AND evidence.normalized_source_pay_method IN ('PAYE','UMBRELLA'))::integer
        AS distinct_supported_source_method_count,
      CASE WHEN COUNT(*) FILTER(WHERE evidence.authority_priority=tier.selected_authority_priority
          AND evidence.raw_source_pay_method IS NOT NULL
          AND evidence.normalized_source_pay_method NOT IN ('PAYE','UMBRELLA'))=0
          AND COUNT(DISTINCT evidence.normalized_source_pay_method) FILTER(
            WHERE evidence.authority_priority=tier.selected_authority_priority
              AND evidence.normalized_source_pay_method IN ('PAYE','UMBRELLA'))=1
        THEN (jsonb_agg(DISTINCT evidence.normalized_source_pay_method
          ORDER BY evidence.normalized_source_pay_method) FILTER(
            WHERE evidence.authority_priority=tier.selected_authority_priority
              AND evidence.normalized_source_pay_method IN ('PAYE','UMBRELLA'))->>0)
      END AS authoritative_source_pay_method,
      pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
        COALESCE(jsonb_agg(evidence.evidence_document ORDER BY evidence.evidence_kind,
          evidence.evidence_identity),'[]'::jsonb)::text,'UTF8'),'sha256'),'hex')
        AS complete_evidence_digest
    FROM source_method_evidence_documents evidence
    JOIN source_method_authority_tier tier
      USING(timesheet_id,economic_key_type,economic_key_value)
    GROUP BY evidence.timesheet_id,evidence.economic_key_type,evidence.economic_key_value,
      tier.selected_authority_priority
  ), source_method_authority AS MATERIALIZED (
    SELECT summary.*,
      CASE
        WHEN summary.invalid_method_count>0
          THEN 'RATE_AUTHORITY_SOURCE_PAY_METHOD_MISSING'
        WHEN summary.distinct_supported_source_method_count=0
          THEN 'RATE_AUTHORITY_SOURCE_PAY_METHOD_MISSING'
        WHEN summary.distinct_supported_source_method_count>1
          THEN 'RATE_AUTHORITY_SOURCE_PAY_METHOD_CONFLICT'
      END AS failure_code,
      jsonb_build_object(
        'authority_contract_version',1,
        'total_evidence_count',summary.total_evidence_count,
        'selected_evidence_count',summary.selected_evidence_count,
        'selected_authority_priority',summary.selected_authority_priority,
        'supported_method_evidence_count',summary.supported_method_evidence_count,
        'invalid_method_count',summary.invalid_method_count,
        'distinct_supported_source_method_count',
          summary.distinct_supported_source_method_count,
        'authoritative_source_pay_method',CASE
          WHEN summary.invalid_method_count=0
            AND summary.distinct_supported_source_method_count=1
            THEN summary.authoritative_source_pay_method END,
        'complete_evidence_digest',summary.complete_evidence_digest,
        'sample_truncated',summary.total_evidence_count>25,
        'evidence_sample',COALESCE((
          SELECT jsonb_agg(sample.evidence_document ORDER BY sample.authority_priority,
            sample.evidence_kind,sample.evidence_identity)
          FROM (
            SELECT evidence.evidence_document,evidence.authority_priority,evidence.evidence_kind,
              evidence.evidence_identity
            FROM source_method_evidence_documents evidence
            WHERE evidence.timesheet_id=summary.timesheet_id
              AND evidence.economic_key_type=summary.economic_key_type
              AND evidence.economic_key_value=summary.economic_key_value
            ORDER BY evidence.authority_priority,evidence.evidence_kind,evidence.evidence_identity
            LIMIT 25
          ) sample
        ),'[]'::jsonb)) AS evidence_json
    FROM source_method_authority_summary summary
  ), nested_evidence_normalized AS MATERIALIZED (
    SELECT parsed.*,
      CASE
        WHEN parsed.direct_physical_bucket_key IS NOT NULL THEN 10
        WHEN parsed.evidence_origin='CASE_COMPONENT' THEN 20
        WHEN parsed.evidence_origin IN ('CASE_BUCKET_RESOLUTION','CORRECTION_BUCKET_RESOLUTION')
          THEN 30
        ELSE 40 END AS evidence_priority,
      COALESCE(parsed.direct_source_family_key,'timesheet:'||parsed.timesheet_id::text)
        AS source_family_key,
      COALESCE(parsed.direct_component_member_identity,
        CASE
          WHEN parsed.component_fallback='WORKED_TIME_AMOUNT'
            THEN 'worked-time-residual:'||parsed.economic_key_type||':'||parsed.economic_key_value
          WHEN parsed.economic_key_type IN ('TS_DAY','TS_TOTAL') THEN COALESCE(
            NULLIF(BTRIM(parsed.source_basis_json->>'segment_stable_key'),''),
            NULLIF(BTRIM(parsed.source_basis_json->>'segment_id'),''),
            NULLIF(BTRIM(parsed.source_basis_json->>'segment_key'),''))
          WHEN parsed.economic_key_type='ADDITIONAL_CODE'
            THEN 'additional:'||UPPER(parsed.economic_key_value)
          WHEN parsed.economic_key_type='EXPENSE_CODE'
            THEN 'expense:'||UPPER(parsed.economic_key_value)
          WHEN parsed.economic_key_type='ADJUSTMENT_CODE'
            THEN 'adjustment:'||parsed.economic_key_value
        END) AS component_member_identity,
      UPPER(COALESCE(CASE WHEN parsed.component_fallback='WORKED_TIME_AMOUNT'
          THEN 'FIXED' END,
        NULLIF(BTRIM(parsed.evidence_json->>'bucket_code'),''),
        NULLIF(BTRIM(parsed.source_basis_json->>'bucket_code'),''),
        CASE WHEN UPPER(BTRIM(COALESCE(parsed.source_basis_json->>'band','')))
          IN ('DAY','NIGHT','SAT','SUN','BH')
          THEN UPPER(BTRIM(parsed.source_basis_json->>'band')) END,
        CASE parsed.economic_key_type WHEN 'ADDITIONAL_CODE' THEN 'ADDITIONAL'
          WHEN 'EXPENSE_CODE' THEN 'FIXED' WHEN 'ADJUSTMENT_CODE' THEN 'FIXED' END))
        AS bucket_code,
      NULLIF(BTRIM(COALESCE(parsed.evidence_json->>'segment_id',
        parsed.source_basis_json->>'segment_id')),'') AS segment_id,
      COALESCE(NULLIF(BTRIM(COALESCE(parsed.evidence_json->>'segment_key',
        parsed.source_basis_json->>'segment_key')),''),
        NULLIF(BTRIM(COALESCE(parsed.evidence_json->>'segment_id',
          parsed.source_basis_json->>'segment_id')),'')) AS segment_key,
      NULLIF(BTRIM(COALESCE(parsed.evidence_json->>'segment_stable_key',
        parsed.source_basis_json->>'segment_stable_key')),'') AS segment_stable_key,
      CASE WHEN COALESCE(parsed.evidence_json->>'source_units',
          parsed.source_basis_json->>'source_units','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND(COALESCE(parsed.evidence_json->>'source_units',
          parsed.source_basis_json->>'source_units')::numeric,6) END AS source_units,
      CASE WHEN COALESCE(parsed.evidence_json->>'source_rate',
          parsed.source_basis_json->>'source_rate','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND(COALESCE(parsed.evidence_json->>'source_rate',
          parsed.source_basis_json->>'source_rate')::numeric,6) END AS source_rate,
      CASE WHEN COALESCE(parsed.evidence_json->>'source_charge_rate',
          parsed.source_basis_json->>'source_charge_rate','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND(COALESCE(parsed.evidence_json->>'source_charge_rate',
          parsed.source_basis_json->>'source_charge_rate')::numeric,6) END
        AS source_charge_rate,
      CASE WHEN COALESCE(parsed.evidence_json->>'source_charge_ex_vat',
          parsed.source_basis_json->>'source_charge_ex_vat','') ~ '^-?\d+(\.\d+)?$'
        THEN ROUND(COALESCE(parsed.evidence_json->>'source_charge_ex_vat',
          parsed.source_basis_json->>'source_charge_ex_vat')::numeric,2) END
        AS source_charge_ex_vat,
      CASE
        WHEN COALESCE(parsed.evidence_json->>'component_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$'
          THEN ROUND((parsed.evidence_json->>'component_amount_ex_vat')::numeric,2)
        WHEN COALESCE(parsed.evidence_json->>'source_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$'
          THEN ROUND((parsed.evidence_json->>'source_amount_ex_vat')::numeric,2)
        WHEN COALESCE(parsed.evidence_json->>'source_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
          THEN ROUND((parsed.evidence_json->>'source_pay_ex_vat')::numeric,2)
        WHEN COALESCE(parsed.source_basis_json->>'source_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$'
          THEN ROUND((parsed.source_basis_json->>'source_amount_ex_vat')::numeric,2)
        WHEN COALESCE(parsed.source_basis_json->>'source_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
          THEN ROUND((parsed.source_basis_json->>'source_pay_ex_vat')::numeric,2)
        WHEN COALESCE(parsed.evidence_json->>'source_units',parsed.source_basis_json->>'source_units','')
            ~ '^-?\d+(\.\d+)?$'
          AND COALESCE(parsed.evidence_json->>'source_rate',parsed.source_basis_json->>'source_rate','')
            ~ '^-?\d+(\.\d+)?$'
          THEN ROUND(COALESCE(parsed.evidence_json->>'source_units',
            parsed.source_basis_json->>'source_units')::numeric
            *COALESCE(parsed.evidence_json->>'source_rate',
              parsed.source_basis_json->>'source_rate')::numeric,2)
      END AS allocated_amount_ex_vat,
      CASE
        WHEN parsed.evidence_origin IN ('CASE_COMPONENT','CASE_BUCKET_RESOLUTION',
            'CORRECTION_BUCKET_RESOLUTION','SOURCE_BASIS_CORRECTION_RESIDUAL',
            'SNAPSHOT_CORRECTION_RESIDUAL','RESOLUTION_CORRECTION_RESIDUAL')
          THEN COALESCE(parsed.evidence_json->>'component_amount_ex_vat',
            parsed.evidence_json->>'source_amount_ex_vat',parsed.evidence_json->>'source_pay_ex_vat',
            CASE WHEN COALESCE(parsed.evidence_json->>'source_units','') ~ '^-?\d+(\.\d+)?$'
              AND COALESCE(parsed.evidence_json->>'source_rate','') ~ '^-?\d+(\.\d+)?$'
              THEN 'DERIVED_UNITS_RATE' END) IS NOT NULL
        WHEN parsed.evidence_origin='TOP_LEVEL_SOURCE_BASIS'
          THEN COALESCE(parsed.source_basis_json->>'source_amount_ex_vat',
            parsed.source_basis_json->>'source_pay_ex_vat') IS NOT NULL
        ELSE false END AS allocation_claimed
    FROM nested_evidence_parsed parsed
  ), nested_evidence_digest AS MATERIALIZED (
    SELECT normalized.*,md5(jsonb_build_object(
      'evidence_contract_version',1,
      'economic_key_type',normalized.economic_key_type,
      'economic_key_value',normalized.economic_key_value,
      'source_family_key',normalized.source_family_key,
      'physical_bucket_key',normalized.direct_physical_bucket_key,
      'component_member_identity',normalized.component_member_identity,
      'bucket_code',normalized.bucket_code,
      'source_basis_fingerprint',normalized.source_basis_fingerprint,
      'component_fingerprint',normalized.component_fingerprint,
      'source_units',normalized.source_units,
      'source_rate',normalized.source_rate,
      'source_charge_rate',normalized.source_charge_rate,
      'source_amount_ex_vat',normalized.allocated_amount_ex_vat)::text)
        AS logical_evidence_digest
    FROM nested_evidence_normalized normalized
  ), nested_evidence_preferred AS MATERIALIZED (
    SELECT normalized.*
    FROM nested_evidence_digest normalized
    WHERE NOT (normalized.evidence_origin='CASE_COMPONENT'
      AND EXISTS(
        SELECT 1 FROM nested_evidence_digest specific
        WHERE specific.fact_identity=normalized.fact_identity
          AND specific.evidence_container_identity IS NOT DISTINCT FROM
            normalized.evidence_container_identity
          AND specific.evidence_origin='CASE_BUCKET_RESOLUTION'
          AND specific.bucket_code IS NOT NULL
          AND specific.allocation_claimed
          AND specific.allocated_amount_ex_vat IS NOT NULL))
  ), nested_evidence_deduped AS MATERIALIZED (
    SELECT ranked.*
    FROM (
      SELECT preferred.*,ROW_NUMBER() OVER(PARTITION BY preferred.fact_identity,
        preferred.logical_evidence_digest
        ORDER BY preferred.evidence_priority,preferred.evidence_origin,
          preferred.direct_physical_bucket_key NULLS LAST,
          preferred.source_basis_fingerprint NULLS LAST,
          preferred.component_fingerprint NULLS LAST)::integer AS evidence_rank
      FROM nested_evidence_preferred preferred
    ) ranked
    WHERE ranked.evidence_rank=1
  ), exact_allocation_candidates AS MATERIALIZED (
    SELECT evidence.*
    FROM nested_evidence_deduped evidence
    WHERE evidence.allocation_claimed AND evidence.allocated_amount_ex_vat IS NOT NULL
      AND ABS(evidence.allocated_amount_ex_vat)>0.005
      AND (evidence.direct_physical_bucket_key IS NOT NULL
        OR (evidence.component_member_identity IS NOT NULL AND evidence.bucket_code IS NOT NULL))
      AND (evidence.parent_amount_ex_vat=0
        OR SIGN(evidence.allocated_amount_ex_vat)=SIGN(evidence.parent_amount_ex_vat)
        OR evidence.component_fallback='WORKED_TIME_AMOUNT')
  ), nested_allocation_failures AS MATERIALIZED (
    SELECT parent.timesheet_id,parent.economic_key_type,parent.economic_key_value,
      MIN(failure.failure_rank) AS failure_rank,
      (array_agg(failure.failure_code ORDER BY failure.failure_rank,failure.failure_code))[1]
        AS failure_code
    FROM sealed_parent_facts parent
    JOIN (
      SELECT shape.fact_identity,10 AS failure_rank,shape.failure_code
      FROM nested_evidence_shape_failures shape
      UNION ALL
      SELECT evidence.fact_identity,20,'RATE_AUTHORITY_NESTED_AMOUNT_SIGN_MISMATCH'::text
      FROM nested_evidence_deduped evidence
      WHERE evidence.allocation_claimed AND evidence.allocated_amount_ex_vat IS NOT NULL
        AND ABS(evidence.allocated_amount_ex_vat)>0.005
        AND evidence.parent_amount_ex_vat<>0
        AND SIGN(evidence.allocated_amount_ex_vat)<>SIGN(evidence.parent_amount_ex_vat)
        AND evidence.component_fallback IS DISTINCT FROM 'WORKED_TIME_AMOUNT'
    ) failure USING(fact_identity)
    GROUP BY parent.timesheet_id,parent.economic_key_type,parent.economic_key_value
  ), exact_allocation_matched AS MATERIALIZED (
    SELECT allocation.*,
      COALESCE(match.matched_physical_bucket_key,
        CASE WHEN allocation.direct_physical_bucket_key IS NOT NULL
          THEN allocation.direct_physical_bucket_key
          WHEN allocation.component_member_identity IS NOT NULL AND allocation.bucket_code IS NOT NULL
            THEN concat_ws('|','RATE_BUCKET_V1',allocation.timesheet_id::text,
              allocation.source_family_key,allocation.economic_key_type,
              allocation.economic_key_value,allocation.component_member_identity,
              allocation.bucket_code) END) AS physical_bucket_key,
      COALESCE(match.match_authority,
        CASE WHEN allocation.direct_physical_bucket_key IS NOT NULL THEN 'PHYSICAL_BUCKET_KEY'
          ELSE 'SEALED_NESTED_IDENTITY' END) AS match_authority,
      COALESCE(match.matched_count,0) AS matched_count
    FROM exact_allocation_candidates allocation
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::integer AS matched_count,MIN(candidate.physical_bucket_key)
          AS matched_physical_bucket_key,MIN(candidate.match_authority) AS match_authority
      FROM (
        SELECT bucket.parsed_physical_bucket_key AS physical_bucket_key,
          CASE
            WHEN allocation.direct_physical_bucket_key IS NOT NULL
              AND bucket.parsed_physical_bucket_key=allocation.direct_physical_bucket_key
              THEN 'PHYSICAL_BUCKET_KEY'
            WHEN bucket.parsed_component_member_identity=allocation.component_member_identity
              AND bucket.parsed_bucket_code=allocation.bucket_code THEN 'STRUCTURAL_IDENTITY'
            WHEN bucket.parsed_bucket_code=allocation.bucket_code THEN 'BUCKET_RESOLUTION'
            ELSE 'SOLE_BUCKET' END AS match_authority,
          CASE
            WHEN allocation.direct_physical_bucket_key IS NOT NULL
              AND bucket.parsed_physical_bucket_key=allocation.direct_physical_bucket_key THEN 10
            WHEN bucket.parsed_component_member_identity=allocation.component_member_identity
              AND bucket.parsed_bucket_code=allocation.bucket_code THEN 20
            WHEN bucket.parsed_bucket_code=allocation.bucket_code THEN 30 ELSE 40 END AS match_rank
        FROM bucket_validated bucket
        WHERE bucket.validated_failure IS NULL
          AND bucket.timesheet_id=allocation.timesheet_id
          AND UPPER(BTRIM(bucket.economic_key_type))=allocation.economic_key_type
          AND BTRIM(bucket.economic_key_value)=allocation.economic_key_value
          AND (bucket.parsed_physical_bucket_key=allocation.direct_physical_bucket_key
            OR (bucket.parsed_component_member_identity=allocation.component_member_identity
              AND bucket.parsed_bucket_code=allocation.bucket_code)
            OR bucket.parsed_bucket_code=allocation.bucket_code
            OR (SELECT COUNT(*) FROM bucket_validated sole
              WHERE sole.validated_failure IS NULL
                AND sole.timesheet_id=allocation.timesheet_id
                AND UPPER(BTRIM(sole.economic_key_type))=allocation.economic_key_type
                AND BTRIM(sole.economic_key_value)=allocation.economic_key_value)=1)
      ) candidate
      WHERE candidate.match_rank=(SELECT MIN(ranked.match_rank) FROM (
        SELECT CASE
          WHEN allocation.direct_physical_bucket_key IS NOT NULL
            AND bucket_rank.parsed_physical_bucket_key=allocation.direct_physical_bucket_key THEN 10
          WHEN bucket_rank.parsed_component_member_identity=allocation.component_member_identity
            AND bucket_rank.parsed_bucket_code=allocation.bucket_code THEN 20
          WHEN bucket_rank.parsed_bucket_code=allocation.bucket_code THEN 30 ELSE 40 END AS match_rank
        FROM bucket_validated bucket_rank
        WHERE bucket_rank.validated_failure IS NULL
          AND bucket_rank.timesheet_id=allocation.timesheet_id
          AND UPPER(BTRIM(bucket_rank.economic_key_type))=allocation.economic_key_type
          AND BTRIM(bucket_rank.economic_key_value)=allocation.economic_key_value
          AND (bucket_rank.parsed_physical_bucket_key=allocation.direct_physical_bucket_key
            OR (bucket_rank.parsed_component_member_identity=allocation.component_member_identity
              AND bucket_rank.parsed_bucket_code=allocation.bucket_code)
            OR bucket_rank.parsed_bucket_code=allocation.bucket_code
            OR (SELECT COUNT(*) FROM bucket_validated sole_rank
              WHERE sole_rank.validated_failure IS NULL
                AND sole_rank.timesheet_id=allocation.timesheet_id
                AND UPPER(BTRIM(sole_rank.economic_key_type))=allocation.economic_key_type
                AND BTRIM(sole_rank.economic_key_value)=allocation.economic_key_value)=1)
      ) ranked)
    ) match ON true
  ), parent_allocation_totals AS MATERIALIZED (
    SELECT parent.fact_identity,ROUND(COALESCE(SUM(
        allocation.allocated_amount_ex_vat),0),2)
        AS exact_allocated_ex_vat,
      ROUND(SUM(allocation.source_charge_ex_vat),2)
        AS exact_allocated_charge_ex_vat,
      BOOL_OR(allocation.component_fallback='WORKED_TIME_AMOUNT') AS has_explicit_residual
    FROM sealed_parent_facts parent
    LEFT JOIN exact_allocation_matched allocation USING(fact_identity)
    GROUP BY parent.fact_identity
  ), parent_residual_bucket_authority AS MATERIALIZED (
    SELECT parent.fact_identity,bucket.parsed_physical_bucket_key,
      bucket.parsed_component_member_identity,bucket.parsed_bucket_code,
      bucket.parsed_segment_id,bucket.parsed_segment_key,
      bucket.parsed_segment_stable_key,bucket.parsed_source_rate,
      bucket.parsed_source_charge_rate,
      CASE WHEN parent.authority_kind='RESERVATION' THEN NULL::numeric
        WHEN bucket.parsed_source_units IS NOT NULL
          AND ROUND(parent.parent_amount_ex_vat-total.exact_allocated_ex_vat,2)=
            ROUND(bucket.parsed_source_pay_ex_vat,2)
        THEN bucket.parsed_source_units
        ELSE ROUND((parent.parent_amount_ex_vat-total.exact_allocated_ex_vat)
          /bucket.parsed_source_rate,6) END AS residual_source_units,
      CASE
        WHEN parent.authority_kind='RESERVATION' THEN NULL::numeric
        WHEN parent.parent_source_charge_ex_vat IS NOT NULL
          THEN ROUND(parent.parent_source_charge_ex_vat
            - COALESCE(total.exact_allocated_charge_ex_vat,0),2)
        WHEN bucket.parsed_source_charge_ex_vat IS NOT NULL
          AND ROUND(parent.parent_amount_ex_vat-total.exact_allocated_ex_vat,2)=
            ROUND(bucket.parsed_source_pay_ex_vat,2)
          THEN bucket.parsed_source_charge_ex_vat
        ELSE ROUND(ROUND((parent.parent_amount_ex_vat-total.exact_allocated_ex_vat)
            /bucket.parsed_source_rate,6)*bucket.parsed_source_charge_rate,2)
      END AS residual_source_charge_ex_vat,
      CASE WHEN parent.authority_kind='RESERVATION'
        THEN 'SEALED_SOLE_BUCKET_RESERVATION_ATTRIBUTION_V1'
        ELSE 'SEALED_SOLE_BUCKET_RATE_DERIVATION_V1' END::text AS match_authority
    FROM sealed_parent_facts parent
    JOIN parent_allocation_totals total USING(fact_identity)
    JOIN physical_bucket_cardinality cardinality
      ON cardinality.timesheet_id=parent.timesheet_id
     AND cardinality.economic_key_type=parent.economic_key_type
     AND cardinality.economic_key_value=parent.economic_key_value
     AND cardinality.physical_bucket_count=1
    JOIN bucket_validated bucket
      ON bucket.validated_failure IS NULL
     AND bucket.timesheet_id=parent.timesheet_id
     AND UPPER(BTRIM(bucket.economic_key_type))=parent.economic_key_type
     AND BTRIM(bucket.economic_key_value)=parent.economic_key_value
    WHERE parent.authority_kind IN ('BASELINE','RESERVATION')
      AND parent.economic_key_type IN ('TS_DAY','TS_TOTAL')
      AND parent.is_signed_non_charge_recovery IS NOT TRUE
      AND ABS(ROUND(parent.parent_amount_ex_vat-total.exact_allocated_ex_vat,2))>0.005
      AND (parent.authority_kind='RESERVATION' OR (
        bucket.parsed_source_rate IS NOT NULL
        AND bucket.parsed_source_rate<>0
        AND (parent.parent_source_charge_ex_vat IS NOT NULL
          OR bucket.parsed_source_charge_ex_vat IS NOT NULL
          OR bucket.parsed_source_charge_rate IS NOT NULL)
        AND ROUND(ROUND((parent.parent_amount_ex_vat-total.exact_allocated_ex_vat)
            /bucket.parsed_source_rate,6)*bucket.parsed_source_rate,2)=
          ROUND(parent.parent_amount_ex_vat-total.exact_allocated_ex_vat,2)))
  ), parent_reconciliation_failures AS MATERIALIZED (
    SELECT parent.timesheet_id,parent.economic_key_type,parent.economic_key_value,
      MIN(failure.failure_rank) AS failure_rank,
      (array_agg(failure.failure_code ORDER BY failure.failure_rank,failure.failure_code))[1]
        AS failure_code
    FROM sealed_parent_facts parent
    JOIN parent_allocation_totals total USING(fact_identity)
    CROSS JOIN LATERAL (VALUES
      (CASE WHEN NOT COALESCE(total.has_explicit_residual,false)
          AND parent.parent_amount_ex_vat<>0 AND total.exact_allocated_ex_vat<>0
          AND SIGN(total.exact_allocated_ex_vat)=SIGN(parent.parent_amount_ex_vat)
          AND ABS(total.exact_allocated_ex_vat)>ABS(parent.parent_amount_ex_vat)+0.005
        THEN 30 END,
       CASE WHEN NOT COALESCE(total.has_explicit_residual,false)
          AND parent.parent_amount_ex_vat<>0 AND total.exact_allocated_ex_vat<>0
          AND SIGN(total.exact_allocated_ex_vat)=SIGN(parent.parent_amount_ex_vat)
          AND ABS(total.exact_allocated_ex_vat)>ABS(parent.parent_amount_ex_vat)+0.005
        THEN 'RATE_AUTHORITY_NESTED_AMOUNT_OVERCONSUMED' END),
      (CASE WHEN parent.authority_kind='BASELINE'
          AND parent.economic_key_type IN ('TS_DAY','TS_TOTAL')
          AND parent.is_signed_non_charge_recovery IS NOT TRUE
          AND ABS(ROUND(parent.parent_amount_ex_vat-total.exact_allocated_ex_vat,2))>0.005
          AND parent.parent_source_charge_ex_vat IS NULL
          AND NOT EXISTS(SELECT 1 FROM parent_residual_bucket_authority authority
            WHERE authority.fact_identity=parent.fact_identity) THEN 40 END,
       CASE WHEN parent.authority_kind='BASELINE'
          AND parent.economic_key_type IN ('TS_DAY','TS_TOTAL')
          AND parent.is_signed_non_charge_recovery IS NOT TRUE
          AND ABS(ROUND(parent.parent_amount_ex_vat-total.exact_allocated_ex_vat,2))>0.005
          AND parent.parent_source_charge_ex_vat IS NULL
          AND NOT EXISTS(SELECT 1 FROM parent_residual_bucket_authority authority
            WHERE authority.fact_identity=parent.fact_identity)
        THEN 'RATE_AUTHORITY_PARENT_SOURCE_CHARGE_MISSING' END)
      ,(CASE WHEN ROUND(parent.parent_amount_ex_vat-total.exact_allocated_ex_vat
            - ROUND(parent.parent_amount_ex_vat-total.exact_allocated_ex_vat,2),2)<>0
          THEN 50 END,
        CASE WHEN ROUND(parent.parent_amount_ex_vat-total.exact_allocated_ex_vat
            - ROUND(parent.parent_amount_ex_vat-total.exact_allocated_ex_vat,2),2)<>0
          THEN 'RATE_AUTHORITY_PARENT_COMPONENT_RECONCILIATION_MISMATCH' END)
    ) failure(failure_rank,failure_code)
    WHERE failure.failure_code IS NOT NULL
    GROUP BY parent.timesheet_id,parent.economic_key_type,parent.economic_key_value
  ), sealed_physical_amount_facts AS MATERIALIZED (
    SELECT allocation.fact_identity,allocation.authority_kind,allocation.timesheet_id,
      allocation.economic_key_type,allocation.economic_key_value,
      allocation.allocated_amount_ex_vat AS authority_amount_ex_vat,
      allocation.source_basis_json,allocation.physical_bucket_key AS direct_physical_bucket_key,
      allocation.component_member_identity AS direct_component_member_identity,
      allocation.source_family_key AS direct_source_family_key,
      allocation.bucket_code,allocation.segment_id,allocation.segment_key,
      allocation.segment_stable_key,allocation.source_units,allocation.source_rate,
      allocation.source_charge_rate,allocation.source_charge_ex_vat,
      COALESCE(allocation.evidence_source_pay_method,allocation.sealed_source_pay_method)
        AS sealed_source_pay_method,
      allocation.match_authority,(allocation.component_fallback='WORKED_TIME_AMOUNT') AS is_residual
    FROM exact_allocation_matched allocation
    WHERE allocation.physical_bucket_key IS NOT NULL AND allocation.matched_count<=1
    UNION ALL
    SELECT parent.fact_identity||':RESIDUAL',parent.authority_kind,parent.timesheet_id,
      parent.economic_key_type,parent.economic_key_value,
      ROUND(parent.parent_amount_ex_vat-total.exact_allocated_ex_vat,2),
      jsonb_build_object('timesheet_id',parent.timesheet_id::text,
        'work_date',CASE WHEN parent.economic_key_type='TS_DAY'
          THEN parent.economic_key_value END,'component_fallback','WORKED_TIME_AMOUNT',
        'residual_contract_version',1,
        'sole_bucket_rate_derivation_contract',residual_bucket.match_authority,
        'signed_non_charge_recovery_contract',CASE
          WHEN parent.is_signed_non_charge_recovery
            THEN 'SIGNED_NON_CHARGE_RECOVERY_V1' END),
      COALESCE(residual_bucket.parsed_physical_bucket_key,concat_ws('|','RATE_BUCKET_V1',parent.timesheet_id::text,
        'timesheet:'||parent.timesheet_id::text,parent.economic_key_type,
        parent.economic_key_value,
        CASE WHEN parent.economic_key_type='EXPENSE_CODE'
          THEN 'expense:'||UPPER(parent.economic_key_value)
          WHEN parent.economic_key_type='ADJUSTMENT_CODE'
          THEN 'adjustment:'||parent.economic_key_value
          ELSE 'worked-time-residual:'||parent.economic_key_type||':'||parent.economic_key_value END,
        'FIXED')),
      COALESCE(residual_bucket.parsed_component_member_identity,CASE WHEN parent.economic_key_type='EXPENSE_CODE'
          THEN 'expense:'||UPPER(parent.economic_key_value)
        WHEN parent.economic_key_type='ADJUSTMENT_CODE'
          THEN 'adjustment:'||parent.economic_key_value
        ELSE 'worked-time-residual:'||parent.economic_key_type||':'||parent.economic_key_value END),
      'timesheet:'||parent.timesheet_id::text,
      COALESCE(residual_bucket.parsed_bucket_code,'FIXED'),
      residual_bucket.parsed_segment_id,residual_bucket.parsed_segment_key,
      residual_bucket.parsed_segment_stable_key,residual_bucket.residual_source_units,
      residual_bucket.parsed_source_rate,residual_bucket.parsed_source_charge_rate,
      CASE WHEN parent.is_signed_non_charge_recovery THEN 0::numeric
        WHEN parent.authority_kind='BASELINE'
        THEN COALESCE(residual_bucket.residual_source_charge_ex_vat,
          ROUND(parent.parent_source_charge_ex_vat
            - COALESCE(total.exact_allocated_charge_ex_vat,0),2)) END,
      parent.sealed_source_pay_method,
      CASE WHEN parent.is_signed_non_charge_recovery
        THEN 'SIGNED_NON_CHARGE_RECOVERY_V1'
        ELSE COALESCE(residual_bucket.match_authority,'ECONOMIC_RESIDUAL') END,true
    FROM sealed_parent_facts parent
    JOIN parent_allocation_totals total USING(fact_identity)
    LEFT JOIN parent_residual_bucket_authority residual_bucket USING(fact_identity)
    WHERE ABS(ROUND(parent.parent_amount_ex_vat-total.exact_allocated_ex_vat,2))>0.005
      AND parent.economic_key_type<>'ADDITIONAL_CODE'
      AND NOT EXISTS(SELECT 1 FROM nested_allocation_failures failure
        WHERE failure.timesheet_id=parent.timesheet_id
          AND failure.economic_key_type=parent.economic_key_type
          AND failure.economic_key_value=parent.economic_key_value)
      AND NOT EXISTS(SELECT 1 FROM parent_reconciliation_failures failure
        WHERE failure.timesheet_id=parent.timesheet_id
          AND failure.economic_key_type=parent.economic_key_type
          AND failure.economic_key_value=parent.economic_key_value)
  ), sealed_physical_amount_attribution AS MATERIALIZED (
    SELECT fact.timesheet_id,fact.economic_key_type,fact.economic_key_value,
      fact.direct_physical_bucket_key AS matched_physical_bucket_key,
      ROUND(COALESCE(SUM(fact.authority_amount_ex_vat) FILTER(
        WHERE fact.authority_kind='BASELINE'),0),2) AS baseline_ex_vat,
      ROUND(COALESCE(SUM(fact.authority_amount_ex_vat) FILTER(
        WHERE fact.authority_kind='RESERVATION'),0),2) AS reserved_ex_vat,
      ROUND(COALESCE(SUM(fact.source_units) FILTER(
        WHERE fact.authority_kind='BASELINE'),0),6) AS baseline_source_units,
      ROUND(SUM(fact.source_charge_ex_vat) FILTER(
        WHERE fact.authority_kind='BASELINE'),2) AS baseline_source_charge_ex_vat,
      COALESCE(jsonb_agg(DISTINCT fact.match_authority ORDER BY fact.match_authority),
        '[]'::jsonb) AS match_authorities
    FROM sealed_physical_amount_facts fact
    GROUP BY fact.timesheet_id,fact.economic_key_type,fact.economic_key_value,
      fact.direct_physical_bucket_key
  ), sealed_physical_amount_ambiguity AS MATERIALIZED (
    SELECT parent.timesheet_id,parent.economic_key_type,parent.economic_key_value,
      BOOL_OR(parent.authority_kind='BASELINE' AND (
        EXISTS(SELECT 1 FROM exact_allocation_matched allocation
          WHERE allocation.fact_identity=parent.fact_identity AND allocation.matched_count>1)
        OR ABS(total.exact_allocated_ex_vat)>ABS(parent.parent_amount_ex_vat)+0.005))
        AS baseline_ambiguous,
      BOOL_OR(parent.authority_kind='RESERVATION' AND (
        EXISTS(SELECT 1 FROM exact_allocation_matched allocation
          WHERE allocation.fact_identity=parent.fact_identity AND allocation.matched_count>1)
        OR ABS(total.exact_allocated_ex_vat)>ABS(parent.parent_amount_ex_vat)+0.005))
        AS reservation_ambiguous
    FROM sealed_parent_facts parent
    JOIN parent_allocation_totals total USING(fact_identity)
    GROUP BY parent.timesheet_id,parent.economic_key_type,parent.economic_key_value
  ), bucket_attributed AS MATERIALIZED (
    SELECT bucket.*,
      ROUND(COALESCE(attribution.baseline_ex_vat,0),2) AS attributed_baseline_ex_vat,
      ROUND(COALESCE(attribution.reserved_ex_vat,0),2) AS attributed_reserved_ex_vat,
      ROUND(COALESCE(attribution.baseline_source_units,0),6)
        AS attributed_baseline_source_units,
      ROUND(COALESCE(attribution.baseline_source_charge_ex_vat,0),2)
        AS attributed_baseline_source_charge_ex_vat,
      ROUND(bucket.parsed_source_pay_ex_vat
        - COALESCE(attribution.baseline_ex_vat,0)
        - COALESCE(attribution.reserved_ex_vat,0),2) AS attributed_outstanding_ex_vat,
      (bucket.parsed_component_kind<>'WORKED_TIME'
        OR (COALESCE(bucket.parsed_source_units,0)>0
          AND bucket.parsed_source_rate IS NOT NULL
          AND bucket.parsed_source_pay_ex_vat>0)) AS builder_bucket_eligible,
      COALESCE(attribution.match_authorities,'[]'::jsonb) AS attribution_authorities
    FROM bucket_validated bucket
    LEFT JOIN sealed_physical_amount_attribution attribution
      ON attribution.timesheet_id=bucket.timesheet_id
     AND attribution.economic_key_type=UPPER(BTRIM(bucket.economic_key_type))
     AND attribution.economic_key_value=BTRIM(bucket.economic_key_value)
     AND attribution.matched_physical_bucket_key=bucket.parsed_physical_bucket_key
  ), bucket_builder_delta AS MATERIALIZED (
    SELECT bucket.*,
      ROUND(COALESCE(bucket.parsed_source_units,0)
        - bucket.attributed_baseline_source_units,6) AS raw_delta_source_units,
      ROUND(bucket.parsed_source_pay_ex_vat-bucket.attributed_baseline_ex_vat,2)
        AS raw_delta_before_reservation_ex,
      ROUND(COALESCE(bucket.parsed_source_charge_ex_vat,0)
        - bucket.attributed_baseline_source_charge_ex_vat,2) AS raw_delta_charge_ex_vat
    FROM bucket_attributed bucket
  ), bucket_builder_expected AS MATERIALIZED (
    SELECT delta.*,
      ROUND(delta.raw_delta_before_reservation_ex-delta.attributed_reserved_ex_vat,2)
        AS builder_component_amount_ex_vat,
      CASE
        WHEN delta.parsed_source_charge_rate IS NOT NULL
          AND delta.parsed_source_rate IS NOT NULL
          AND delta.parsed_source_rate<>0
          AND ROUND(GREATEST(delta.raw_delta_source_units,0),6)>0
          AND ROUND(GREATEST(delta.raw_delta_source_units,0)*delta.parsed_source_rate,2)=
            ROUND(delta.raw_delta_before_reservation_ex-delta.attributed_reserved_ex_vat,2)
          THEN ROUND(GREATEST(delta.raw_delta_source_units,0)
            *delta.parsed_source_charge_rate,2)
        WHEN ROUND(delta.raw_delta_before_reservation_ex,2)=0
          THEN ROUND(delta.raw_delta_charge_ex_vat,2)
        ELSE ROUND(delta.raw_delta_charge_ex_vat
          *((delta.raw_delta_before_reservation_ex-delta.attributed_reserved_ex_vat)
            /NULLIF(delta.raw_delta_before_reservation_ex,0)),2)
      END AS builder_source_charge_ex_vat,
      CASE WHEN delta.parsed_source_rate IS NOT NULL AND delta.parsed_source_rate<>0
          AND ROUND(GREATEST(delta.raw_delta_source_units,0),6)>0
          AND ROUND(GREATEST(delta.raw_delta_source_units,0)*delta.parsed_source_rate,2)=
            ROUND(delta.raw_delta_before_reservation_ex-delta.attributed_reserved_ex_vat,2)
        THEN ROUND(GREATEST(delta.raw_delta_source_units,0),6) END AS builder_source_units,
      CASE WHEN delta.parsed_source_rate IS NOT NULL AND delta.parsed_source_rate<>0
          AND ROUND(GREATEST(delta.raw_delta_source_units,0),6)>0
          AND ROUND(GREATEST(delta.raw_delta_source_units,0)*delta.parsed_source_rate,2)=
            ROUND(delta.raw_delta_before_reservation_ex-delta.attributed_reserved_ex_vat,2)
        THEN delta.parsed_source_rate END AS builder_source_rate,
      CASE WHEN delta.parsed_source_rate IS NOT NULL AND delta.parsed_source_rate<>0
          AND ROUND(GREATEST(delta.raw_delta_source_units,0),6)>0
          AND ROUND(GREATEST(delta.raw_delta_source_units,0)*delta.parsed_source_rate,2)=
            ROUND(delta.raw_delta_before_reservation_ex-delta.attributed_reserved_ex_vat,2)
        THEN CASE
          WHEN delta.parsed_source_charge_rate IS NOT NULL
            THEN delta.parsed_source_charge_rate
          WHEN ROUND(delta.raw_delta_charge_ex_vat,2)=0 THEN NULL::numeric
          ELSE ROUND(delta.raw_delta_charge_ex_vat
            /NULLIF(ROUND(GREATEST(delta.raw_delta_source_units,0),6),0),6)
        END
      END AS builder_source_charge_rate,
      ABS(ROUND(delta.raw_delta_before_reservation_ex
        - delta.attributed_reserved_ex_vat,2))>0.005 AS builder_component_expected
    FROM bucket_builder_delta delta
  ), synthetic_baseline_sources AS MATERIALIZED (
    SELECT attribution.timesheet_id,attribution.economic_key_type,attribution.economic_key_value,
      attribution.matched_physical_bucket_key AS physical_bucket_key,
      CASE WHEN BOOL_OR(fact.is_residual) THEN 'WORKED_TIME_RESIDUAL'
        WHEN attribution.economic_key_type IN ('TS_DAY','TS_TOTAL') THEN 'WORKED_TIME'
        WHEN attribution.economic_key_type='ADDITIONAL_CODE' THEN 'ADDITIONAL_UNIT'
        WHEN attribution.economic_key_type='EXPENSE_CODE' THEN 'EXPENSE'
        ELSE 'ADJUSTMENT' END AS component_kind,
      (array_agg(fact.direct_component_member_identity ORDER BY fact.is_residual DESC,
        fact.fact_identity))[1] AS component_member_identity,
      (array_agg(fact.segment_id ORDER BY fact.is_residual DESC,fact.fact_identity))[1]
        AS segment_id,
      (array_agg(fact.segment_key ORDER BY fact.is_residual DESC,fact.fact_identity))[1]
        AS segment_key,
      (array_agg(fact.segment_stable_key ORDER BY fact.is_residual DESC,fact.fact_identity))[1]
        AS segment_stable_key,
      (array_agg(fact.bucket_code ORDER BY fact.is_residual DESC,fact.fact_identity))[1]
        AS bucket_code,
      NULL::numeric AS source_units,NULL::numeric AS source_rate,
      NULL::numeric AS source_charge_rate,0::numeric AS truth_ex_vat,
      attribution.baseline_ex_vat,attribution.reserved_ex_vat,
      -COALESCE(attribution.baseline_source_charge_ex_vat,0) AS source_charge_ex_vat,
      method.authoritative_source_pay_method AS source_pay_method,
      context.target_pay_method,context.umbrella_id,context.umbrella_enabled,
      context.umbrella_vat_chargeable,context.erni_pct,context.vat_rate_pct,
      string_agg(fact.fact_identity||':'||fact.authority_amount_ex_vat::text,''
        ORDER BY fact.fact_identity) AS revision_identity,
      context.target_authority_digest,context.conversion_context_digest,
      (array_agg(fact.source_basis_json ORDER BY fact.is_residual DESC,fact.fact_identity))[1]
        AS source_basis_json,
      attribution.match_authorities,BOOL_OR(fact.is_residual) AS is_residual
    FROM sealed_physical_amount_attribution attribution
    JOIN sealed_physical_amount_facts fact
      ON fact.timesheet_id=attribution.timesheet_id
     AND fact.economic_key_type=attribution.economic_key_type
     AND fact.economic_key_value=attribution.economic_key_value
     AND fact.direct_physical_bucket_key=attribution.matched_physical_bucket_key
    JOIN source_method_authority method
      ON method.timesheet_id=attribution.timesheet_id
     AND method.economic_key_type=attribution.economic_key_type
     AND method.economic_key_value=attribution.economic_key_value
    CROSS JOIN candidate_sealed_context context
    WHERE NOT EXISTS(
      SELECT 1 FROM bucket_validated live_bucket
      WHERE live_bucket.validated_failure IS NULL
        AND live_bucket.timesheet_id=attribution.timesheet_id
        AND live_bucket.parsed_physical_bucket_key=attribution.matched_physical_bucket_key)
    GROUP BY attribution.timesheet_id,attribution.economic_key_type,
      attribution.economic_key_value,attribution.matched_physical_bucket_key,
      attribution.baseline_ex_vat,attribution.reserved_ex_vat,
      attribution.baseline_source_charge_ex_vat,attribution.match_authorities,
      context.target_pay_method,context.umbrella_id,context.umbrella_enabled,
      context.umbrella_vat_chargeable,context.erni_pct,context.vat_rate_pct,
      context.target_authority_digest,context.conversion_context_digest,
      method.authoritative_source_pay_method
  ), truth_residual_sources AS MATERIALIZED (
    SELECT expected.timesheet_id,expected.economic_key_type,expected.economic_key_value,
      concat_ws('|','RATE_BUCKET_V1',expected.timesheet_id::text,
        'timesheet:'||expected.timesheet_id::text,expected.economic_key_type,
        expected.economic_key_value,
        'worked-time-residual:'||expected.economic_key_type||':'||expected.economic_key_value,
        'FIXED') AS physical_bucket_key,
      'WORKED_TIME_RESIDUAL'::text AS component_kind,
      'worked-time-residual:'||expected.economic_key_type||':'||expected.economic_key_value
        AS component_member_identity,
      NULL::text AS segment_id,NULL::text AS segment_key,NULL::text AS segment_stable_key,
      'FIXED'::text AS bucket_code,NULL::numeric AS source_units,NULL::numeric AS source_rate,
      NULL::numeric AS source_charge_rate,
      ROUND(expected.truth_ex_vat-COALESCE(represented.truth_ex_vat,0),2) AS truth_ex_vat,
      0::numeric AS baseline_ex_vat,0::numeric AS reserved_ex_vat,
      represented.residual_source_charge_ex_vat AS source_charge_ex_vat,
      method.authoritative_source_pay_method,context.target_pay_method,context.umbrella_id,
      context.umbrella_enabled,context.umbrella_vat_chargeable,context.erni_pct,
      context.vat_rate_pct,
      'TRUTH_RESIDUAL:'||expected.timesheet_id::text||':'||expected.economic_key_type||':'
        ||expected.economic_key_value AS revision_identity,
      context.target_authority_digest,context.conversion_context_digest,
      jsonb_build_object('timesheet_id',expected.timesheet_id::text,
        'work_date',CASE WHEN expected.economic_key_type='TS_DAY'
          THEN expected.economic_key_value END,
        'component_fallback','WORKED_TIME_AMOUNT','residual_contract_version',1)
        AS source_basis_json,
      jsonb_build_array('ECONOMIC_TRUTH_RESIDUAL') AS match_authorities,true AS is_residual
    FROM economic_totals expected
    JOIN source_method_authority method
      USING(timesheet_id,economic_key_type,economic_key_value)
    CROSS JOIN candidate_sealed_context context
    LEFT JOIN LATERAL (
      SELECT ROUND(COALESCE(SUM(bucket.parsed_source_pay_ex_vat)
          FILTER(WHERE bucket.builder_bucket_eligible),0),2) AS truth_ex_vat,
        ROUND(COALESCE(SUM(bucket.parsed_source_charge_ex_vat)
          FILTER(WHERE NOT bucket.builder_bucket_eligible),0),2)
          AS residual_source_charge_ex_vat
      FROM bucket_attributed bucket
      WHERE bucket.validated_failure IS NULL
        AND bucket.timesheet_id=expected.timesheet_id
        AND UPPER(BTRIM(bucket.economic_key_type))=expected.economic_key_type
        AND BTRIM(bucket.economic_key_value)=expected.economic_key_value
    ) represented ON true
    WHERE expected.economic_key_type IN ('TS_DAY','TS_TOTAL')
      AND ABS(ROUND(expected.truth_ex_vat-COALESCE(represented.truth_ex_vat,0),2))>0.005
  ), synthetic_component_sources AS MATERIALIZED (
    SELECT * FROM synthetic_baseline_sources
    UNION ALL SELECT * FROM truth_residual_sources
  ), synthetic_bucket_attributed AS MATERIALIZED (
    SELECT source.timesheet_id,source.economic_key_type,source.economic_key_value,
      source.physical_bucket_key,
      CASE WHEN BOOL_OR(source.is_residual) THEN 'WORKED_TIME_RESIDUAL'
        ELSE MIN(source.component_kind) END AS component_kind,
      (array_agg(source.component_member_identity ORDER BY source.is_residual DESC,
        source.revision_identity))[1] AS component_member_identity,
      (array_agg(source.segment_id ORDER BY source.is_residual DESC,source.revision_identity))[1]
        AS segment_id,
      (array_agg(source.segment_key ORDER BY source.is_residual DESC,source.revision_identity))[1]
        AS segment_key,
      (array_agg(source.segment_stable_key ORDER BY source.is_residual DESC,
        source.revision_identity))[1] AS segment_stable_key,
      (array_agg(source.bucket_code ORDER BY source.is_residual DESC,source.revision_identity))[1]
        AS bucket_code,
      CASE WHEN BOOL_OR(source.is_residual) THEN 90 ELSE 7 END AS bucket_sort_ordinal,
      NULL::numeric AS source_units,NULL::numeric AS source_rate,
      NULL::numeric AS source_charge_rate,
      ROUND(SUM(source.truth_ex_vat),2) AS truth_ex_vat,
      ROUND(SUM(source.baseline_ex_vat),2) AS baseline_ex_vat,
      ROUND(SUM(source.reserved_ex_vat),2) AS reserved_ex_vat,
      ROUND(SUM(source.truth_ex_vat)-SUM(source.baseline_ex_vat)-SUM(source.reserved_ex_vat),2)
        AS outstanding_ex_vat,
      ROUND(SUM(source.source_charge_ex_vat),2) AS source_charge_ex_vat,
      (jsonb_agg(DISTINCT source.source_pay_method ORDER BY source.source_pay_method)
        FILTER(WHERE source.source_pay_method IS NOT NULL)->>0) AS source_pay_method,
      MIN(source.target_pay_method)
        AS target_pay_method,MIN(source.umbrella_id::text)::uuid AS umbrella_id,
      BOOL_AND(source.umbrella_enabled) AS umbrella_enabled,
      BOOL_AND(source.umbrella_vat_chargeable) AS umbrella_vat_chargeable,
      MIN(source.erni_pct) AS erni_pct,MIN(source.vat_rate_pct) AS vat_rate_pct,
      md5(string_agg(source.revision_identity,'' ORDER BY source.revision_identity))
        AS financial_revision_digest,
      MIN(source.target_authority_digest) AS target_authority_digest,
      MIN(source.conversion_context_digest) AS conversion_context_digest,
      (array_agg(source.source_basis_json ORDER BY source.is_residual DESC,
        source.revision_identity))[1] AS source_basis_json,
      COALESCE(jsonb_agg(DISTINCT authority.value ORDER BY authority.value)
        FILTER(WHERE authority.value IS NOT NULL), '[]'::jsonb)
        AS match_authorities,BOOL_OR(source.is_residual) AS is_residual
    FROM synthetic_component_sources source
    LEFT JOIN LATERAL jsonb_array_elements_text(COALESCE(source.match_authorities,'[]'::jsonb))
      authority(value) ON true
    GROUP BY source.timesheet_id,source.economic_key_type,source.economic_key_value,
      source.physical_bucket_key
  ), physical_amount_rows AS MATERIALIZED (
    SELECT bucket.timesheet_id,UPPER(BTRIM(bucket.economic_key_type)) AS economic_key_type,
      BTRIM(bucket.economic_key_value) AS economic_key_value,
      bucket.parsed_source_pay_ex_vat AS truth_ex_vat,
      bucket.attributed_baseline_ex_vat AS baseline_ex_vat,
      bucket.attributed_reserved_ex_vat AS reserved_ex_vat
    FROM bucket_builder_expected bucket
    WHERE bucket.validated_failure IS NULL AND bucket.builder_bucket_eligible
    UNION ALL
    SELECT synthetic.timesheet_id,synthetic.economic_key_type,synthetic.economic_key_value,
      synthetic.truth_ex_vat,synthetic.baseline_ex_vat,synthetic.reserved_ex_vat
    FROM synthetic_bucket_attributed synthetic
  ), economic_physical_totals AS MATERIALIZED (
    SELECT bucket.timesheet_id,bucket.economic_key_type,bucket.economic_key_value,
      ROUND(COALESCE(SUM(bucket.truth_ex_vat),0),2) AS truth_ex_vat,
      ROUND(COALESCE(SUM(bucket.baseline_ex_vat),0),2) AS baseline_ex_vat,
      ROUND(COALESCE(SUM(bucket.reserved_ex_vat),0),2) AS reserved_ex_vat
    FROM physical_amount_rows bucket
    GROUP BY bucket.timesheet_id,bucket.economic_key_type,bucket.economic_key_value
  ), economic_failures AS MATERIALIZED (
    SELECT expected.timesheet_id,expected.economic_key_type,expected.economic_key_value,
      CASE
        WHEN context.context_failure IS NOT NULL THEN context.context_failure
        WHEN method.failure_code IS NOT NULL THEN method.failure_code
        WHEN method.timesheet_id IS NULL THEN 'RATE_AUTHORITY_SOURCE_PAY_METHOD_MISSING'
        WHEN nested.failure_code IS NOT NULL THEN nested.failure_code
        WHEN reconciliation.failure_code IS NOT NULL THEN reconciliation.failure_code
        WHEN actual.truth_ex_vat IS NULL OR actual.truth_ex_vat IS DISTINCT FROM expected.truth_ex_vat
          THEN 'RATE_AUTHORITY_PARENT_PAY_MISMATCH'
        WHEN COALESCE(ambiguity.baseline_ambiguous,false)
          OR actual.baseline_ex_vat IS DISTINCT FROM expected.baseline_ex_vat
          THEN 'RATE_AUTHORITY_PHYSICAL_BASELINE_REQUIRED'
        WHEN COALESCE(ambiguity.reservation_ambiguous,false)
          OR actual.reserved_ex_vat IS DISTINCT FROM expected.reserved_ex_vat
          THEN 'RATE_AUTHORITY_PHYSICAL_RESERVATION_REQUIRED'
      END AS failure_code,
      COALESCE(method.evidence_json,jsonb_build_object(
        'authority_contract_version',1,
        'total_evidence_count',0,
        'supported_method_evidence_count',0,
        'invalid_method_count',0,
        'distinct_supported_source_method_count',0,
        'authoritative_source_pay_method',NULL,
        'complete_evidence_digest',pg_catalog.encode(extensions.digest(
          pg_catalog.convert_to('[]','UTF8'),'sha256'),'hex'),
        'sample_truncated',false,
        'evidence_sample','[]'::jsonb)) AS source_method_evidence_json
    FROM economic_totals expected
    CROSS JOIN candidate_sealed_context context
    LEFT JOIN source_method_authority method
      USING(timesheet_id,economic_key_type,economic_key_value)
    LEFT JOIN nested_allocation_failures nested
      USING(timesheet_id,economic_key_type,economic_key_value)
    LEFT JOIN parent_reconciliation_failures reconciliation
      USING(timesheet_id,economic_key_type,economic_key_value)
    LEFT JOIN economic_physical_totals actual
      USING(timesheet_id,economic_key_type,economic_key_value)
    LEFT JOIN sealed_physical_amount_ambiguity ambiguity
      USING(timesheet_id,economic_key_type,economic_key_value)
    WHERE context.context_failure IS NOT NULL
       OR method.failure_code IS NOT NULL
       OR method.timesheet_id IS NULL
       OR nested.failure_code IS NOT NULL
       OR reconciliation.failure_code IS NOT NULL
       OR actual.truth_ex_vat IS DISTINCT FROM expected.truth_ex_vat
       OR COALESCE(ambiguity.baseline_ambiguous,false)
       OR actual.baseline_ex_vat IS DISTINCT FROM expected.baseline_ex_vat
       OR COALESCE(ambiguity.reservation_ambiguous,false)
       OR actual.reserved_ex_vat IS DISTINCT FROM expected.reserved_ex_vat
  ), bucket_documents AS MATERIALIZED (
    SELECT bucket.*,
      jsonb_build_object(
        'physical_bucket_version',2,
        'physical_bucket_key',bucket.parsed_physical_bucket_key,
        'component_kind',bucket.parsed_component_kind,
        'component_member_identity',bucket.parsed_component_member_identity,
        'bucket_code',bucket.parsed_bucket_code,
        'source_units',bucket.builder_source_units,
        'source_rate',bucket.builder_source_rate,
        'source_charge_rate',bucket.builder_source_charge_rate,
        'source_pay_ex_vat',bucket.parsed_source_pay_ex_vat,
        'source_charge_ex_vat',bucket.builder_source_charge_ex_vat,
        'baseline_source_pay_ex_vat',bucket.attributed_baseline_ex_vat,
        'reserved_source_pay_ex_vat',bucket.attributed_reserved_ex_vat,
        'outstanding_source_pay_ex_vat',bucket.attributed_outstanding_ex_vat,
        'source_pay_method',bucket.parsed_source_pay_method,
        'target_pay_method',bucket.parsed_target_pay_method) AS attributed_physical_canonical_json,
      md5(jsonb_build_object(
        'builder_comparison_version',1,'timesheet_id',bucket.timesheet_id::text,
        'source_family_key',bucket.rate_authority#>>'{economic,source_family_key}',
        'component_key_type',UPPER(BTRIM(bucket.economic_key_type)),
        'component_key_value',BTRIM(bucket.economic_key_value),
        'segment_id',bucket.parsed_segment_id,
        'segment_key',COALESCE(bucket.parsed_segment_key,bucket.parsed_segment_id),
        'segment_stable_key',bucket.parsed_segment_stable_key,
        'work_date',NULLIF(BTRIM(COALESCE(bucket.bucket_json->>'work_date',
          bucket.source_payload_json#>>'{segment,work_date}',
          bucket.source_payload_json#>>'{segment,date}')),''),
        'ref_num',NULLIF(BTRIM(COALESCE(bucket.bucket_json->>'ref_num',
          bucket.source_payload_json#>>'{segment,ref_num}')),''),
        'additional_code',NULLIF(UPPER(BTRIM(bucket.bucket_json->>'additional_code')),''),
        'expense_code',NULLIF(UPPER(BTRIM(bucket.bucket_json->>'expense_code')),''),
        'adjustment_id',NULLIF(BTRIM(bucket.bucket_json->>'adjustment_id'),''),
        'bucket_code',bucket.parsed_bucket_code,
        'source_units',bucket.builder_source_units,
        'source_rate',bucket.builder_source_rate,
        'source_charge_rate',bucket.builder_source_charge_rate,
        'source_pay_ex_vat',bucket.builder_component_amount_ex_vat,
        'source_charge_ex_vat',bucket.builder_source_charge_ex_vat,
        'source_pay_method',bucket.parsed_source_pay_method,
        'target_pay_method',bucket.parsed_target_pay_method)::text)
        AS attributed_builder_comparison_digest
    FROM bucket_builder_expected bucket
    WHERE bucket.builder_bucket_eligible
  ), synthetic_bucket_documents AS MATERIALIZED (
    SELECT synthetic.*,
      jsonb_build_object(
        'physical_bucket_version',2,
        'physical_bucket_key',synthetic.physical_bucket_key,
        'component_kind',synthetic.component_kind,
        'component_member_identity',synthetic.component_member_identity,
        'bucket_code',synthetic.bucket_code,
        'source_units',synthetic.source_units,
        'source_rate',synthetic.source_rate,
        'source_charge_rate',synthetic.source_charge_rate,
        'source_pay_ex_vat',synthetic.truth_ex_vat,
        'source_charge_ex_vat',synthetic.source_charge_ex_vat,
        'baseline_source_pay_ex_vat',synthetic.baseline_ex_vat,
        'reserved_source_pay_ex_vat',synthetic.reserved_ex_vat,
        'outstanding_source_pay_ex_vat',synthetic.outstanding_ex_vat,
        'source_pay_method',synthetic.source_pay_method,
        'target_pay_method',synthetic.target_pay_method) AS physical_canonical_json,
      md5(jsonb_build_object(
        'builder_comparison_version',1,
        'timesheet_id',synthetic.timesheet_id::text,
        'source_family_key','timesheet:'||synthetic.timesheet_id::text,
        'component_key_type',synthetic.economic_key_type,
        'component_key_value',synthetic.economic_key_value,
        'segment_id',synthetic.segment_id,
        'segment_key',COALESCE(synthetic.segment_key,synthetic.segment_id),
        'segment_stable_key',synthetic.segment_stable_key,
        'work_date',NULLIF(BTRIM(COALESCE(synthetic.source_basis_json->>'work_date',
          synthetic.source_basis_json->>'date')),''),
        'ref_num',NULLIF(BTRIM(synthetic.source_basis_json->>'ref_num'),''),
        'additional_code',CASE WHEN synthetic.economic_key_type='ADDITIONAL_CODE'
          THEN UPPER(synthetic.economic_key_value) END,
        'expense_code',CASE WHEN synthetic.economic_key_type='EXPENSE_CODE'
          THEN UPPER(synthetic.economic_key_value) END,
        'adjustment_id',CASE WHEN synthetic.economic_key_type='ADJUSTMENT_CODE'
          THEN synthetic.economic_key_value END,
        'bucket_code',synthetic.bucket_code,
        'source_units',synthetic.source_units,
        'source_rate',synthetic.source_rate,
        'source_charge_rate',synthetic.source_charge_rate,
        'source_pay_ex_vat',synthetic.outstanding_ex_vat,
        'source_charge_ex_vat',synthetic.source_charge_ex_vat,
        'source_pay_method',synthetic.source_pay_method,
        'target_pay_method',synthetic.target_pay_method)::text)
        AS builder_comparison_digest
    FROM synthetic_bucket_attributed synthetic
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
      NULL::text AS source_pay_method,
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
        'source_payload',occurrence.source_payload_json,'physical_bucket',NULL,
        'source_method_authority',economic.source_method_evidence_json) AS evidence_json
    FROM occurrence_validated occurrence
    LEFT JOIN economic_failures economic
      ON economic.timesheet_id=occurrence.timesheet_id
     AND economic.economic_key_type=UPPER(BTRIM(occurrence.economic_key_type))
     AND economic.economic_key_value=BTRIM(occurrence.economic_key_value)
    WHERE COALESCE(occurrence.validated_failure,economic.failure_code) IS NOT NULL
  ), failed_economic_domain AS (
    SELECT p_build_id,p_candidate_id,economic.timesheet_id,NULL::uuid AS financial_row_id,
      'timesheet:'||economic.timesheet_id::text AS source_family_key,
      economic.economic_key_type,economic.economic_key_value,NULL::text AS component_kind,
      NULL::text AS component_member_identity,NULL::text AS segment_id,
      NULL::text AS segment_key,NULL::text AS segment_stable_key,NULL::text AS bucket_code,
      NULL::integer AS bucket_sort_ordinal,NULL::text AS physical_bucket_key,
      NULL::text AS physical_bucket_digest,NULL::numeric AS source_units,
      NULL::numeric AS source_rate,NULL::numeric AS source_charge_rate,
      expected.truth_ex_vat,expected.baseline_ex_vat,expected.reserved_ex_vat,
      ROUND(expected.truth_ex_vat-expected.baseline_ex_vat-expected.reserved_ex_vat,2)
        AS outstanding_ex_vat,
      NULL::numeric AS source_charge_ex_vat,NULL::text AS source_pay_method,
      context.target_pay_method,context.umbrella_id,context.umbrella_enabled,
      context.umbrella_vat_chargeable,context.erni_pct,context.vat_rate_pct,
      NULL::text AS financial_revision_digest,context.target_authority_digest,
      context.conversion_context_digest,NULL::text AS sealed_evidence_digest,
      'FAILED'::text AS projection_status,economic.failure_code,
      jsonb_build_object('build_id',p_build_id::text,'candidate_id',p_candidate_id::text,
        'timesheet_id',economic.timesheet_id::text,
        'economic_key_type',economic.economic_key_type,
        'economic_key_value',economic.economic_key_value,
        'source_method_authority',economic.source_method_evidence_json) AS evidence_json
    FROM economic_failures economic
    JOIN economic_totals expected USING(timesheet_id,economic_key_type,economic_key_value)
    CROSS JOIN candidate_sealed_context context
    WHERE NOT EXISTS(
      SELECT 1 FROM occurrence_validated occurrence
      WHERE occurrence.timesheet_id=economic.timesheet_id
        AND UPPER(BTRIM(occurrence.economic_key_type))=economic.economic_key_type
        AND BTRIM(occurrence.economic_key_value)=economic.economic_key_value)
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
      bucket.parsed_physical_bucket_key,md5(bucket.attributed_physical_canonical_json::text),
      bucket.builder_source_units,bucket.builder_source_rate,bucket.builder_source_charge_rate,
      bucket.parsed_source_pay_ex_vat,bucket.attributed_baseline_ex_vat,
      bucket.attributed_reserved_ex_vat,bucket.attributed_outstanding_ex_vat,
      bucket.builder_source_charge_ex_vat,CASE
        WHEN COALESCE(bucket.validated_failure,economic.failure_code) IS NULL
          THEN method.authoritative_source_pay_method END,
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
      md5(jsonb_build_object(
        'sealed_evidence_version',2,
        'financial_revision_digest',bucket.rate_authority#>>'{source,financial_revision_digest}',
        'target_authority_digest',bucket.rate_authority#>>'{target,target_authority_digest}',
        'conversion_context_digest',bucket.rate_authority#>>'{conversion,conversion_context_digest}',
        'physical_bucket_digest',md5(bucket.attributed_physical_canonical_json::text),
        'economic_key_type',UPPER(BTRIM(bucket.economic_key_type)),
        'economic_key_value',BTRIM(bucket.economic_key_value),
        'truth_ex_vat',bucket.parsed_source_pay_ex_vat,
        'baseline_ex_vat',bucket.attributed_baseline_ex_vat,
        'reserved_ex_vat',bucket.attributed_reserved_ex_vat)::text),
      CASE WHEN COALESCE(bucket.validated_failure,economic.failure_code) IS NOT NULL
        THEN 'FAILED'
        WHEN bucket.builder_component_expected
        AND bucket.builder_source_units IS NOT NULL
        AND bucket.builder_source_rate IS NOT NULL
        AND method.authoritative_source_pay_method IS DISTINCT FROM bucket.parsed_target_pay_method
        THEN 'READY' ELSE 'FIXED' END AS projection_status,
      COALESCE(bucket.validated_failure,economic.failure_code) AS failure_code,
      jsonb_build_object('source_key',bucket.source_payload_json->>'source_key',
        'source_ordinal',bucket.source_payload_json#>'{rate_authority,lineage,source_ordinal}',
        'source_payload',bucket.source_payload_json,
        'physical_bucket',bucket.attributed_physical_canonical_json||jsonb_build_object(
          'physical_bucket_digest',md5(bucket.attributed_physical_canonical_json::text),
          'builder_comparison_digest',bucket.attributed_builder_comparison_digest,
          'builder_component_amount_ex_vat',bucket.builder_component_amount_ex_vat,
          'builder_component_expected',bucket.builder_component_expected,
          'is_rate_bearing',bucket.builder_source_units IS NOT NULL
            AND bucket.builder_source_rate IS NOT NULL,
          'is_actionable_candidate',bucket.builder_component_expected
            AND bucket.builder_source_units IS NOT NULL
            AND bucket.builder_source_rate IS NOT NULL
            AND COALESCE(bucket.validated_failure,economic.failure_code) IS NULL
            AND method.authoritative_source_pay_method IS DISTINCT FROM bucket.parsed_target_pay_method),
        'sealed_physical_attribution',jsonb_build_object(
          'baseline_ex_vat',bucket.attributed_baseline_ex_vat,
          'reserved_ex_vat',bucket.attributed_reserved_ex_vat,
          'outstanding_ex_vat',bucket.attributed_outstanding_ex_vat,
          'match_authorities',bucket.attribution_authorities),
        'economic_authority',(SELECT jsonb_build_object(
            'truth_inc_vat',economic_total.truth_inc_vat,
            'baseline_inc_vat',economic_total.baseline_inc_vat)
          FROM economic_totals economic_total
          WHERE economic_total.timesheet_id=bucket.timesheet_id
            AND economic_total.economic_key_type=UPPER(BTRIM(bucket.economic_key_type))
            AND economic_total.economic_key_value=BTRIM(bucket.economic_key_value)),
        'source_method_authority',method.evidence_json) AS evidence_json
    FROM bucket_documents bucket
    LEFT JOIN source_method_authority method
      ON method.timesheet_id=bucket.timesheet_id
     AND method.economic_key_type=UPPER(BTRIM(bucket.economic_key_type))
     AND method.economic_key_value=BTRIM(bucket.economic_key_value)
    LEFT JOIN economic_failures economic
      ON economic.timesheet_id=bucket.timesheet_id
     AND economic.economic_key_type=UPPER(BTRIM(bucket.economic_key_type))
     AND economic.economic_key_value=BTRIM(bucket.economic_key_value)
    WHERE NOT EXISTS(SELECT 1 FROM failed_occurrences failed
      WHERE failed.build_id=bucket.build_id AND failed.timesheet_id=bucket.timesheet_id
        AND failed.economic_key_type=bucket.economic_key_type
        AND failed.economic_key_value=bucket.economic_key_value)
  ), synthetic_bucket_outputs AS (
    SELECT p_build_id,p_candidate_id,synthetic.timesheet_id,NULL::uuid AS financial_row_id,
      'timesheet:'||synthetic.timesheet_id::text AS source_family_key,
      synthetic.economic_key_type,synthetic.economic_key_value,synthetic.component_kind,
      synthetic.component_member_identity,synthetic.segment_id,synthetic.segment_key,
      synthetic.segment_stable_key,synthetic.bucket_code,synthetic.bucket_sort_ordinal,
      synthetic.physical_bucket_key,md5(synthetic.physical_canonical_json::text)
        AS physical_bucket_digest,
      synthetic.source_units,synthetic.source_rate,synthetic.source_charge_rate,
      synthetic.truth_ex_vat,synthetic.baseline_ex_vat,synthetic.reserved_ex_vat,
      synthetic.outstanding_ex_vat,synthetic.source_charge_ex_vat,
      synthetic.source_pay_method,synthetic.target_pay_method,synthetic.umbrella_id,
      synthetic.umbrella_enabled,synthetic.umbrella_vat_chargeable,synthetic.erni_pct,
      synthetic.vat_rate_pct,synthetic.financial_revision_digest,
      synthetic.target_authority_digest,synthetic.conversion_context_digest,
      md5(jsonb_build_object(
        'sealed_evidence_version',2,
        'financial_revision_digest',synthetic.financial_revision_digest,
        'target_authority_digest',synthetic.target_authority_digest,
        'conversion_context_digest',synthetic.conversion_context_digest,
        'physical_bucket_digest',md5(synthetic.physical_canonical_json::text),
        'economic_key_type',synthetic.economic_key_type,
        'economic_key_value',synthetic.economic_key_value,
        'truth_ex_vat',synthetic.truth_ex_vat,
        'baseline_ex_vat',synthetic.baseline_ex_vat,
        'reserved_ex_vat',synthetic.reserved_ex_vat)::text) AS sealed_evidence_digest,
      CASE WHEN ABS(ROUND(synthetic.outstanding_ex_vat,2))>0.005
        AND NOT synthetic.is_residual AND synthetic.source_units IS NOT NULL
        AND synthetic.source_rate IS NOT NULL
        AND synthetic.source_pay_method IS DISTINCT FROM synthetic.target_pay_method
        THEN 'READY' ELSE 'FIXED' END AS projection_status,
      economic.failure_code,
      jsonb_build_object(
        'source_key','SEALED_SYNTHETIC:'||synthetic.physical_bucket_key,
        'source_payload',jsonb_build_object(
          'source_kind',CASE WHEN synthetic.is_residual THEN 'WORKED_TIME_RESIDUAL'
            ELSE 'SEALED_BASELINE_COMPONENT' END,
          'source_value',synthetic.source_basis_json,
          'segment',synthetic.source_basis_json,
          'expense_code',CASE WHEN synthetic.economic_key_type='EXPENSE_CODE'
            THEN UPPER(synthetic.economic_key_value) END,
          'adjustment_id',CASE WHEN synthetic.economic_key_type='ADJUSTMENT_CODE'
            THEN synthetic.economic_key_value END),
        'physical_bucket',synthetic.physical_canonical_json||jsonb_build_object(
          'physical_bucket_digest',md5(synthetic.physical_canonical_json::text),
          'builder_comparison_digest',synthetic.builder_comparison_digest,
          'builder_component_amount_ex_vat',synthetic.outstanding_ex_vat,
          'builder_component_expected',ABS(ROUND(synthetic.outstanding_ex_vat,2))>0.005,
          'is_rate_bearing',NOT synthetic.is_residual
            AND synthetic.source_units IS NOT NULL AND synthetic.source_rate IS NOT NULL,
          'is_actionable_candidate',ABS(ROUND(synthetic.outstanding_ex_vat,2))>0.005
            AND NOT synthetic.is_residual
            AND synthetic.source_units IS NOT NULL AND synthetic.source_rate IS NOT NULL
            AND synthetic.source_pay_method IS DISTINCT FROM synthetic.target_pay_method),
        'sealed_physical_attribution',jsonb_build_object(
          'baseline_ex_vat',synthetic.baseline_ex_vat,
          'reserved_ex_vat',synthetic.reserved_ex_vat,
          'outstanding_ex_vat',synthetic.outstanding_ex_vat,
          'match_authorities',synthetic.match_authorities,
          'residual_contract_version',CASE WHEN synthetic.is_residual THEN 1 END),
        'economic_authority',(SELECT jsonb_build_object(
            'truth_inc_vat',economic_total.truth_inc_vat,
            'baseline_inc_vat',economic_total.baseline_inc_vat)
          FROM economic_totals economic_total
          WHERE economic_total.timesheet_id=synthetic.timesheet_id
            AND economic_total.economic_key_type=synthetic.economic_key_type
            AND economic_total.economic_key_value=synthetic.economic_key_value),
        'source_method_authority',method.evidence_json)
        AS evidence_json
    FROM synthetic_bucket_documents synthetic
    JOIN source_method_authority method
      USING(timesheet_id,economic_key_type,economic_key_value)
    LEFT JOIN economic_failures economic
      USING(timesheet_id,economic_key_type,economic_key_value)
    WHERE economic.failure_code IS NULL
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
  UNION ALL SELECT * FROM failed_economic_domain
  UNION ALL SELECT * FROM bucket_outputs
  UNION ALL SELECT * FROM synthetic_bucket_outputs
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
