-- Banking Pay bounded-scope Version 1.2.16.
-- Resolve one immutable finance/reservation owner from every applicable mutable
-- and frozen assertion. Policy X is preserved: frozen post-draft evidence is
-- compared, never replaced with a live fallback.

CREATE OR REPLACE FUNCTION private.pay_workbench_financial_source_authority_v1(
  p_expected_candidate_id uuid,
  p_owner_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_candidate_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_finance_case_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_frozen_source_basis_json jsonb DEFAULT '{}'::jsonb,
  p_frozen_component_snapshot_json jsonb DEFAULT '{}'::jsonb,
  p_context text DEFAULT 'FINANCE'::text
)
RETURNS TABLE(
  owner_ids uuid[],
  candidate_ids uuid[],
  finance_case_ids uuid[],
  resolved_timesheet_id uuid,
  resolved_candidate_id uuid,
  resolved_finance_case_id uuid,
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
  v_invalid_uuid_count integer:=0;
  v_linked_ids uuid[]:=ARRAY[]::uuid[];
  v_direct_ids uuid[]:=ARRAY[]::uuid[];
  v_carrier_ids uuid[]:=ARRAY[]::uuid[];
  v_frozen_candidate_ids uuid[]:=ARRAY[]::uuid[];
  v_frozen_case_ids uuid[]:=ARRAY[]::uuid[];
  v_frozen_owner uuid;
BEGIN
  IF p_expected_candidate_id IS NULL OR v_context NOT IN ('FINANCE','RESERVATION') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_FINANCIAL_SOURCE_AUTHORITY_INVALID'
      USING ERRCODE='22023';
  END IF;

  IF jsonb_typeof(v_basis)<>'object' OR jsonb_typeof(v_snapshot)<>'object' THEN
    resolution_failure:=v_context||'_FROZEN_SOURCE_BASIS_INVALID';
    owner_ids:=ARRAY[]::uuid[];
    candidate_ids:=ARRAY[]::uuid[];
    finance_case_ids:=ARRAY[]::uuid[];
    evidence_json:=jsonb_build_object('context',v_context,'frozen_json_valid',false);
    RETURN NEXT;
    RETURN;
  END IF;

  WITH frozen_value(role_name,value_text) AS (
    VALUES
      ('LINKED_OWNER',NULLIF(BTRIM(v_basis->>'linked_timesheet_id'),'')),
      ('LINKED_OWNER',NULLIF(BTRIM(v_snapshot->>'linked_timesheet_id'),'')),
      ('LINKED_OWNER',NULLIF(BTRIM(v_snapshot#>>'{source_basis_json,linked_timesheet_id}'),'')),
      ('DIRECT_OWNER',NULLIF(BTRIM(v_basis->>'timesheet_id'),'')),
      ('DIRECT_OWNER',NULLIF(BTRIM(v_basis#>>'{economic_key,timesheet_id}'),'')),
      ('DIRECT_OWNER',NULLIF(BTRIM(v_snapshot->>'timesheet_id'),'')),
      ('DIRECT_OWNER',NULLIF(BTRIM(v_snapshot#>>'{source_basis_json,timesheet_id}'),'')),
      ('CARRIER_OWNER',NULLIF(BTRIM(v_basis->>'carrier_timesheet_id'),'')),
      ('CARRIER_OWNER',NULLIF(BTRIM(v_snapshot->>'carrier_timesheet_id'),'')),
      ('CARRIER_OWNER',NULLIF(BTRIM(v_snapshot#>>'{source_basis_json,carrier_timesheet_id}'),'')),
      ('CANDIDATE',NULLIF(BTRIM(v_basis->>'candidate_id'),'')),
      ('CANDIDATE',NULLIF(BTRIM(v_snapshot->>'candidate_id'),'')),
      ('CANDIDATE',NULLIF(BTRIM(v_snapshot#>>'{source_basis_json,candidate_id}'),'')),
      ('FINANCE_CASE',NULLIF(BTRIM(v_basis->>'finance_case_id'),'')),
      ('FINANCE_CASE',NULLIF(BTRIM(v_snapshot->>'finance_case_id'),'')),
      ('FINANCE_CASE',NULLIF(BTRIM(v_snapshot#>>'{source_basis_json,finance_case_id}'),''))
  ), classified AS (
    SELECT role_name,value_text,
      pg_input_is_valid(value_text,'uuid') AS is_valid
    FROM frozen_value
    WHERE value_text IS NOT NULL
  )
  SELECT
    COUNT(*) FILTER(WHERE NOT is_valid)::integer,
    COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid)
      FILTER(WHERE role_name='LINKED_OWNER' AND is_valid),ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid)
      FILTER(WHERE role_name='DIRECT_OWNER' AND is_valid),ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid)
      FILTER(WHERE role_name='CARRIER_OWNER' AND is_valid),ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid)
      FILTER(WHERE role_name='CANDIDATE' AND is_valid),ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid)
      FILTER(WHERE role_name='FINANCE_CASE' AND is_valid),ARRAY[]::uuid[])
  INTO v_invalid_uuid_count,v_linked_ids,v_direct_ids,v_carrier_ids,
    v_frozen_candidate_ids,v_frozen_case_ids
  FROM classified;

  -- Recovery artefacts may intentionally contain a stable linked/root member
  -- and a different carrier member. Resolve the frozen role with the existing
  -- Policy X preference instead of treating that valid pair as a conflict.
  IF cardinality(v_linked_ids)>1 OR cardinality(v_direct_ids)>1
     OR cardinality(v_carrier_ids)>1 THEN
    resolution_failure:=v_context||'_FROZEN_OWNER_CONFLICT';
  ELSIF v_invalid_uuid_count>0 THEN
    resolution_failure:=v_context||'_FROZEN_UUID_INVALID';
  ELSE
    v_frozen_owner:=COALESCE(v_linked_ids[1],v_direct_ids[1],v_carrier_ids[1]);
  END IF;

  SELECT COALESCE(array_agg(DISTINCT value ORDER BY value),ARRAY[]::uuid[])
  INTO owner_ids
  FROM unnest(COALESCE(p_owner_ids,ARRAY[]::uuid[])||
    CASE WHEN v_frozen_owner IS NULL THEN ARRAY[]::uuid[] ELSE ARRAY[v_frozen_owner] END) value
  WHERE value IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT value ORDER BY value),ARRAY[]::uuid[])
  INTO candidate_ids
  FROM unnest(COALESCE(p_candidate_ids,ARRAY[]::uuid[])||v_frozen_candidate_ids) value
  WHERE value IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT value ORDER BY value),ARRAY[]::uuid[])
  INTO finance_case_ids
  FROM unnest(COALESCE(p_finance_case_ids,ARRAY[]::uuid[])||v_frozen_case_ids) value
  WHERE value IS NOT NULL;

  IF resolution_failure IS NULL THEN
    IF cardinality(owner_ids)=0 THEN
      resolution_failure:=v_context||'_OWNER_UNRESOLVED';
    ELSIF cardinality(owner_ids)>1 THEN
      resolution_failure:=v_context||'_OWNER_CONFLICT';
    ELSIF cardinality(candidate_ids)<>1
       OR candidate_ids[1] IS DISTINCT FROM p_expected_candidate_id THEN
      resolution_failure:=v_context||'_CANDIDATE_CONFLICT';
    ELSIF cardinality(finance_case_ids)>1 THEN
      resolution_failure:=v_context||'_FINANCE_CASE_CONFLICT';
    END IF;
  END IF;

  resolved_timesheet_id:=CASE WHEN cardinality(owner_ids)=1 THEN owner_ids[1] END;
  resolved_candidate_id:=CASE WHEN cardinality(candidate_ids)=1 THEN candidate_ids[1] END;
  resolved_finance_case_id:=CASE WHEN cardinality(finance_case_ids)=1 THEN finance_case_ids[1] END;
  evidence_json:=jsonb_build_object(
    'context',v_context,
    'owner_ids',to_jsonb(owner_ids),
    'candidate_ids',to_jsonb(candidate_ids),
    'finance_case_ids',to_jsonb(finance_case_ids),
    'frozen_owner_role',CASE WHEN cardinality(v_linked_ids)=1 THEN 'LINKED'
      WHEN cardinality(v_direct_ids)=1 THEN 'DIRECT'
      WHEN cardinality(v_carrier_ids)=1 THEN 'CARRIER' END,
    'frozen_owner_id',v_frozen_owner,
    'resolution_failure',resolution_failure
  );
  RETURN NEXT;
END;
$function$;

ALTER FUNCTION private.pay_workbench_financial_source_authority_v1(
  uuid,uuid[],uuid[],uuid[],jsonb,jsonb,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_financial_source_authority_v1(
  uuid,uuid[],uuid[],uuid[],jsonb,jsonb,text) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_financial_source_authority_v1(
  uuid,uuid[],uuid[],uuid[],jsonb,jsonb,text) TO postgres;
