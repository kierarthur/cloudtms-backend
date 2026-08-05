-- Banking Pay bounded-scope Version 1.2.9.
-- Extend the immutable owner/candidate/case authority fence with complete
-- finance-component and economic-key agreement.  Frozen post-draft evidence is
-- compared with mutable identity; it is never silently replaced by it.

CREATE OR REPLACE FUNCTION private.pay_workbench_financial_source_authority_v2(
  p_expected_candidate_id uuid,
  p_owner_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_candidate_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_finance_case_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_finance_component_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_component_key_assertions jsonb DEFAULT '[]'::jsonb,
  p_frozen_source_basis_json jsonb DEFAULT '{}'::jsonb,
  p_frozen_component_snapshot_json jsonb DEFAULT '{}'::jsonb,
  p_context text DEFAULT 'FINANCE'::text
)
RETURNS TABLE(
  owner_ids uuid[],
  candidate_ids uuid[],
  finance_case_ids uuid[],
  finance_component_ids uuid[],
  component_key_pairs text[],
  resolved_timesheet_id uuid,
  resolved_candidate_id uuid,
  resolved_finance_case_id uuid,
  resolved_finance_component_id uuid,
  resolved_component_key_type text,
  resolved_component_key_value text,
  resolution_failure text,
  evidence_json jsonb
)
LANGUAGE plpgsql
STABLE
PARALLEL SAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_context text:=UPPER(NULLIF(BTRIM(COALESCE(p_context,'')),''));
  v_basis jsonb:=COALESCE(p_frozen_source_basis_json,'{}'::jsonb);
  v_snapshot jsonb:=COALESCE(p_frozen_component_snapshot_json,'{}'::jsonb);
  v_key_assertions jsonb:=COALESCE(p_component_key_assertions,'[]'::jsonb);
  v_base record;
  v_invalid_component_uuid_count integer:=0;
  v_incomplete_key_count integer:=0;
BEGIN
  IF p_expected_candidate_id IS NULL OR v_context NOT IN ('FINANCE','RESERVATION')
     OR jsonb_typeof(v_basis)<>'object' OR jsonb_typeof(v_snapshot)<>'object'
     OR jsonb_typeof(v_key_assertions)<>'array' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_FINANCIAL_SOURCE_AUTHORITY_INVALID'
      USING ERRCODE='22023';
  END IF;

  SELECT * INTO STRICT v_base
  FROM private.pay_workbench_financial_source_authority_v1(
    p_expected_candidate_id,p_owner_ids,p_candidate_ids,p_finance_case_ids,
    v_basis,v_snapshot,v_context
  );

  owner_ids:=COALESCE(v_base.owner_ids,ARRAY[]::uuid[]);
  candidate_ids:=COALESCE(v_base.candidate_ids,ARRAY[]::uuid[]);
  finance_case_ids:=COALESCE(v_base.finance_case_ids,ARRAY[]::uuid[]);
  resolved_timesheet_id:=v_base.resolved_timesheet_id;
  resolved_candidate_id:=v_base.resolved_candidate_id;
  resolved_finance_case_id:=v_base.resolved_finance_case_id;
  resolution_failure:=v_base.resolution_failure;

  WITH component_value(value_text) AS (
    SELECT value::text FROM unnest(COALESCE(p_finance_component_ids,ARRAY[]::uuid[])) value
    WHERE value IS NOT NULL
    UNION ALL VALUES
      (NULLIF(BTRIM(v_basis->>'finance_component_id'),'')),
      (NULLIF(BTRIM(v_basis#>>'{component,finance_component_id}'),'')),
      (NULLIF(BTRIM(v_snapshot->>'finance_component_id'),'')),
      (NULLIF(BTRIM(v_snapshot#>>'{source_basis_json,finance_component_id}'),'')),
      (NULLIF(BTRIM(v_snapshot#>>'{component,finance_component_id}'),''))
  ), classified AS (
    SELECT value_text,pg_input_is_valid(value_text,'uuid') AS is_valid
    FROM component_value WHERE value_text IS NOT NULL
  )
  SELECT COUNT(*) FILTER(WHERE NOT is_valid)::integer,
    COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid)
      FILTER(WHERE is_valid),ARRAY[]::uuid[])
  INTO v_invalid_component_uuid_count,finance_component_ids
  FROM classified;

  WITH supplied_assertion AS (
    SELECT NULLIF(UPPER(BTRIM(COALESCE(assertion.value->>'key_type',
        assertion.value->>'component_key_type',''))),'') AS key_type,
      NULLIF(BTRIM(COALESCE(assertion.value->>'key_value',
        assertion.value->>'component_key_value','')),'') AS key_value
    FROM jsonb_array_elements(v_key_assertions) assertion(value)
    WHERE jsonb_typeof(assertion.value)='object'
  ), frozen_assertion(key_type,key_value) AS (
    VALUES
      (NULLIF(UPPER(BTRIM(COALESCE(v_basis->>'component_key_type',v_basis->>'key_type',''))),''),
       NULLIF(BTRIM(COALESCE(v_basis->>'component_key_value',v_basis->>'key_value','')),'')),
      (NULLIF(UPPER(BTRIM(COALESCE(v_snapshot->>'component_key_type',v_snapshot->>'key_type',''))),''),
       NULLIF(BTRIM(COALESCE(v_snapshot->>'component_key_value',v_snapshot->>'key_value','')),'')),
      (NULLIF(UPPER(BTRIM(COALESCE(v_snapshot#>>'{source_basis_json,component_key_type}',
          v_snapshot#>>'{source_basis_json,key_type}',''))),''),
       NULLIF(BTRIM(COALESCE(v_snapshot#>>'{source_basis_json,component_key_value}',
          v_snapshot#>>'{source_basis_json,key_value}','')),''))
  ), all_assertion AS (
    SELECT * FROM supplied_assertion
    UNION ALL SELECT * FROM frozen_assertion
  )
  SELECT COUNT(*) FILTER(WHERE (key_type IS NULL)<>(key_value IS NULL))::integer,
    COALESCE(array_agg(DISTINCT key_type||E'\x1f'||key_value
      ORDER BY key_type||E'\x1f'||key_value)
      FILTER(WHERE key_type IS NOT NULL AND key_value IS NOT NULL),ARRAY[]::text[])
  INTO v_incomplete_key_count,component_key_pairs
  FROM all_assertion
  WHERE key_type IS NOT NULL OR key_value IS NOT NULL;

  IF resolution_failure IS NULL THEN
    IF v_invalid_component_uuid_count>0 THEN
      resolution_failure:=CASE WHEN v_context='FINANCE' THEN 'FINANCE_COMPONENT_UUID_INVALID'
        ELSE 'RESERVATION_COMPONENT_UUID_INVALID' END;
    ELSIF cardinality(finance_component_ids)>1 THEN
      resolution_failure:=CASE WHEN v_context='FINANCE' THEN 'FINANCE_COMPONENT_CONFLICT'
        ELSE 'RESERVATION_COMPONENT_CONFLICT' END;
    ELSIF v_incomplete_key_count>0 THEN
      resolution_failure:=CASE WHEN v_context='FINANCE' THEN 'FINANCE_COMPONENT_KEY_INCOMPLETE'
        ELSE 'RESERVATION_COMPONENT_KEY_INCOMPLETE' END;
    ELSIF cardinality(component_key_pairs)>1 THEN
      resolution_failure:=CASE WHEN v_context='FINANCE' THEN 'FINANCE_COMPONENT_KEY_CONFLICT'
        ELSE 'RESERVATION_COMPONENT_KEY_CONFLICT' END;
    END IF;
  END IF;

  resolved_finance_component_id:=CASE WHEN cardinality(finance_component_ids)=1
    THEN finance_component_ids[1] END;
  resolved_component_key_type:=CASE WHEN cardinality(component_key_pairs)=1
    THEN split_part(component_key_pairs[1],E'\x1f',1) END;
  resolved_component_key_value:=CASE WHEN cardinality(component_key_pairs)=1
    THEN split_part(component_key_pairs[1],E'\x1f',2) END;

  evidence_json:=COALESCE(v_base.evidence_json,'{}'::jsonb)||jsonb_build_object(
    'authority_version',2,
    'finance_component_ids',to_jsonb(finance_component_ids),
    'component_key_pairs',to_jsonb(component_key_pairs),
    'invalid_finance_component_uuid_count',v_invalid_component_uuid_count,
    'incomplete_component_key_count',v_incomplete_key_count,
    'resolved_finance_component_id',resolved_finance_component_id,
    'resolved_component_key_type',resolved_component_key_type,
    'resolved_component_key_value',resolved_component_key_value,
    'resolution_failure',resolution_failure
  );
  RETURN NEXT;
END;
$function$;

ALTER FUNCTION private.pay_workbench_financial_source_authority_v2(
  uuid,uuid[],uuid[],uuid[],uuid[],jsonb,jsonb,jsonb,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_financial_source_authority_v2(
  uuid,uuid[],uuid[],uuid[],uuid[],jsonb,jsonb,jsonb,text)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_financial_source_authority_v2(
  uuid,uuid[],uuid[],uuid[],uuid[],jsonb,jsonb,jsonb,text) TO postgres;
