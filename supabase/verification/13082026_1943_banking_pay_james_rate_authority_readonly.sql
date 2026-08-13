-- James Cases/Resolutions rate-authority verification.
-- TEST-only, SELECT/DO-only: no application DML and no durable object creation.

DO $verification$
DECLARE
  v_build_id uuid;
  v_economic_rows integer;
  v_physical_rows integer;
  v_physical_keys integer;
  v_day_rows integer;
  v_night_rows integer;
  v_failure_count integer;
  v_value_mismatch_count integer;
  v_helper record;
BEGIN
  SELECT build.id
  INTO STRICT v_build_id
  FROM private.banking_pay_workbench_economic_builds build
  JOIN private.banking_pay_workbench_economic_build_scope scope_row
    ON scope_row.build_id=build.id
  WHERE build.candidate_id='6e8493ae-c207-497e-8d83-0b518753f590'::uuid
    AND scope_row.timesheet_id=ANY(ARRAY[
      '0ed36e08-3073-4dbc-a90b-f247dc3e62e4'::uuid,
      '60548d68-50fd-4951-99ff-7fe17d778930'::uuid,
      '690194e5-2cd7-4681-b25c-df09a231cedf'::uuid])
  GROUP BY build.id,build.created_at_utc
  HAVING count(DISTINCT scope_row.timesheet_id)=3
  ORDER BY build.created_at_utc DESC
  LIMIT 1;

  WITH target(timesheet_id) AS (VALUES
    ('0ed36e08-3073-4dbc-a90b-f247dc3e62e4'::uuid),
    ('60548d68-50fd-4951-99ff-7fe17d778930'::uuid),
    ('690194e5-2cd7-4681-b25c-df09a231cedf'::uuid)
  ), occurrence AS MATERIALIZED (
    SELECT target.timesheet_id AS target_timesheet_id,page_row.*
    FROM target
    CROSS JOIN LATERAL private.pay_workbench_unit_economic_occurrence_page_v1(
      v_build_id,
      'UNIT:0ed36e08-3073-4dbc-a90b-f247dc3e62e4',
      'LIVE_ENTITLEMENT_INPUT',target.timesheet_id,NULL::text,25) page_row
  ), bucket AS MATERIALIZED (
    SELECT occurrence.timesheet_id,occurrence.economic_key_type,
      occurrence.economic_key_value,occurrence.resolution_failure,
      occurrence.source_payload_json#>>'{rate_authority,source,source_pay_method}' AS source_method,
      occurrence.source_payload_json#>>'{rate_authority,target,target_pay_method}' AS target_method,
      physical.value->>'bucket_code' AS bucket_code,
      (physical.value->>'source_units')::numeric AS source_units,
      (physical.value->>'source_rate')::numeric AS source_rate,
      (physical.value->>'source_charge_rate')::numeric AS source_charge_rate,
      (physical.value->>'source_pay_ex_vat')::numeric AS source_pay_ex_vat,
      (physical.value->>'source_charge_ex_vat')::numeric AS source_charge_ex_vat,
      physical.value->>'physical_bucket_key' AS physical_bucket_key
    FROM occurrence
    CROSS JOIN LATERAL jsonb_array_elements(
      occurrence.source_payload_json#>'{rate_authority,physical_buckets}') physical(value)
  )
  SELECT (SELECT count(*) FROM occurrence)::integer,count(*)::integer,
    count(DISTINCT physical_bucket_key)::integer,
    count(*) FILTER(WHERE bucket_code='DAY')::integer,
    count(*) FILTER(WHERE bucket_code='NIGHT')::integer,
    count(*) FILTER(WHERE resolution_failure IS NOT NULL)::integer,
    count(*) FILTER(WHERE economic_key_type<>'TS_DAY'
      OR economic_key_value<>'2026-06-08' OR source_method<>'PAYE'
      OR target_method<>'UMBRELLA'
      OR NOT ((timesheet_id='0ed36e08-3073-4dbc-a90b-f247dc3e62e4'::uuid
          AND bucket_code='DAY' AND source_units=11.5 AND source_rate=20
          AND source_charge_rate=40 AND source_pay_ex_vat=230 AND source_charge_ex_vat=460)
        OR (timesheet_id='0ed36e08-3073-4dbc-a90b-f247dc3e62e4'::uuid
          AND bucket_code='NIGHT' AND source_units=1.5 AND source_rate=25
          AND source_charge_rate=45 AND source_pay_ex_vat=37.5 AND source_charge_ex_vat=67.5)
        OR (timesheet_id='60548d68-50fd-4951-99ff-7fe17d778930'::uuid
          AND bucket_code='DAY' AND source_units=11 AND source_rate=20
          AND source_charge_rate=40 AND source_pay_ex_vat=220 AND source_charge_ex_vat=440)
        OR (timesheet_id='690194e5-2cd7-4681-b25c-df09a231cedf'::uuid
          AND bucket_code='DAY' AND source_units=10 AND source_rate=20
          AND source_charge_rate=40 AND source_pay_ex_vat=200 AND source_charge_ex_vat=400)))::integer
  INTO v_economic_rows,v_physical_rows,v_physical_keys,v_day_rows,v_night_rows,
    v_failure_count,v_value_mismatch_count
  FROM bucket;

  IF v_economic_rows<>3 OR v_physical_rows<>4 OR v_physical_keys<>4
     OR v_day_rows<>3 OR v_night_rows<>1 OR v_failure_count<>0
     OR v_value_mismatch_count<>0 THEN
    RAISE EXCEPTION 'JAMES_RATE_AUTHORITY_READONLY_PARITY_FAILED';
  END IF;

  SELECT p.prosecdef,p.provolatile,p.proparallel,
    coalesce(to_jsonb(p.proconfig),'[]'::jsonb) AS proconfig,
    pg_catalog.pg_get_userbyid(p.proowner) AS owner_name
  INTO STRICT v_helper
  FROM pg_catalog.pg_proc p
  WHERE p.oid='private.pay_workbench_sealed_rate_component_projection_v1(uuid,uuid,uuid[])'::regprocedure;

  IF v_helper.prosecdef IS DISTINCT FROM true OR v_helper.provolatile<>'s'
     OR v_helper.proparallel<>'u' OR v_helper.owner_name<>'postgres'
     OR v_helper.proconfig<>jsonb_build_array('search_path=""') THEN
    RAISE EXCEPTION 'JAMES_RATE_HELPER_METADATA_FAILED';
  END IF;
END;
$verification$;
