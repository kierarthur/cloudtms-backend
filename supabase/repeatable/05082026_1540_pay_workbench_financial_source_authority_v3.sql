-- Banking Pay bounded-scope Version 1.2.11.
-- Resolve mutable and every independently supplied frozen assertion without
-- COALESCE precedence, including aliases inside the same document.  Empty or
-- partial reservation documents cannot mask linked batch-item authority.
-- Policy X remains frozen post-draft authority.

CREATE OR REPLACE FUNCTION private.pay_workbench_financial_source_authority_v3(
  p_expected_candidate_id uuid,
  p_owner_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_candidate_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_finance_case_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_finance_component_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_component_key_assertions jsonb DEFAULT '[]'::jsonb,
  p_frozen_source_basis_documents jsonb DEFAULT '[]'::jsonb,
  p_frozen_component_snapshot_documents jsonb DEFAULT '[]'::jsonb,
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
  v_basis_documents jsonb:=COALESCE(p_frozen_source_basis_documents,'[]'::jsonb);
  v_snapshot_documents jsonb:=COALESCE(p_frozen_component_snapshot_documents,'[]'::jsonb);
  v_key_assertions jsonb:=COALESCE(p_component_key_assertions,'[]'::jsonb);
  v_invalid_uuid_count integer:=0;
  v_invalid_component_uuid_count integer:=0;
  v_incomplete_key_count integer:=0;
  v_linked_ids uuid[]:=ARRAY[]::uuid[];
  v_direct_ids uuid[]:=ARRAY[]::uuid[];
  v_carrier_ids uuid[]:=ARRAY[]::uuid[];
  v_frozen_candidate_ids uuid[]:=ARRAY[]::uuid[];
  v_frozen_case_ids uuid[]:=ARRAY[]::uuid[];
  v_frozen_owner uuid;
  v_document_evidence jsonb:='[]'::jsonb;
BEGIN
  IF p_expected_candidate_id IS NULL
     OR v_context NOT IN ('FINANCE','RESERVATION')
     OR jsonb_typeof(v_basis_documents)<>'array'
     OR jsonb_typeof(v_snapshot_documents)<>'array'
     OR jsonb_typeof(v_key_assertions)<>'array'
     OR EXISTS(
       SELECT 1
       FROM jsonb_array_elements(v_basis_documents||v_snapshot_documents) entry(value)
       WHERE jsonb_typeof(entry.value)<>'object'
          OR NULLIF(BTRIM(COALESCE(entry.value->>'source','')),'') IS NULL
          OR jsonb_typeof(entry.value->'document')<>'object'
     ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_FINANCIAL_SOURCE_AUTHORITY_INVALID'
      USING ERRCODE='22023';
  END IF;

  WITH document AS (
    SELECT 'SOURCE_BASIS'::text AS document_kind,
      UPPER(BTRIM(entry.value->>'source')) AS source_name,
      entry.value->'document' AS body
    FROM jsonb_array_elements(v_basis_documents) entry(value)
    UNION ALL
    SELECT 'COMPONENT_SNAPSHOT',UPPER(BTRIM(entry.value->>'source')),
      entry.value->'document'
    FROM jsonb_array_elements(v_snapshot_documents) entry(value)
  ), frozen_value(role_name,value_text) AS (
    SELECT value.role_name,NULLIF(BTRIM(value.value_text),'')
    FROM document
    CROSS JOIN LATERAL (VALUES
      ('LINKED_OWNER',document.body->>'linked_timesheet_id'),
      ('LINKED_OWNER',document.body#>>'{source_basis_json,linked_timesheet_id}'),
      ('DIRECT_OWNER',document.body->>'timesheet_id'),
      ('DIRECT_OWNER',document.body#>>'{economic_key,timesheet_id}'),
      ('DIRECT_OWNER',document.body#>>'{source_basis_json,timesheet_id}'),
      ('DIRECT_OWNER',document.body#>>'{source_basis_json,economic_key,timesheet_id}'),
      ('CARRIER_OWNER',document.body->>'carrier_timesheet_id'),
      ('CARRIER_OWNER',document.body#>>'{source_basis_json,carrier_timesheet_id}'),
      ('CANDIDATE',document.body->>'candidate_id'),
      ('CANDIDATE',document.body#>>'{source_basis_json,candidate_id}'),
      ('FINANCE_CASE',document.body->>'finance_case_id'),
      ('FINANCE_CASE',document.body#>>'{source_basis_json,finance_case_id}')
    ) value(role_name,value_text)
  ), classified AS (
    SELECT role_name,value_text,pg_input_is_valid(value_text,'uuid') AS is_valid
    FROM frozen_value WHERE value_text IS NOT NULL
  )
  SELECT COUNT(*) FILTER(WHERE NOT is_valid)::integer,
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

  IF cardinality(v_linked_ids)>1 OR cardinality(v_direct_ids)>1
     OR cardinality(v_carrier_ids)>1 THEN
    resolution_failure:=v_context||'_FROZEN_OWNER_CONFLICT';
  ELSIF v_invalid_uuid_count>0 THEN
    resolution_failure:=v_context||'_FROZEN_UUID_INVALID';
  ELSE
    -- Preserve the existing Policy X role preference.  A stable linked/root
    -- owner may legitimately differ from a carrier member, but two assertions
    -- within the same role can never be accepted.
    v_frozen_owner:=COALESCE(v_linked_ids[1],v_direct_ids[1],v_carrier_ids[1]);
  END IF;

  SELECT COALESCE(array_agg(DISTINCT value ORDER BY value),ARRAY[]::uuid[])
  INTO owner_ids
  FROM unnest(COALESCE(p_owner_ids,ARRAY[]::uuid[])||CASE
    WHEN v_frozen_owner IS NULL THEN ARRAY[]::uuid[] ELSE ARRAY[v_frozen_owner] END) value
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

  WITH document AS (
    SELECT entry.value->'document' AS body
    FROM jsonb_array_elements(v_basis_documents||v_snapshot_documents) entry(value)
  ), component_value(value_text) AS (
    SELECT value::text
    FROM unnest(COALESCE(p_finance_component_ids,ARRAY[]::uuid[])) value
    WHERE value IS NOT NULL
    UNION ALL
    SELECT NULLIF(BTRIM(value.value_text),'')
    FROM document
    CROSS JOIN LATERAL (VALUES
      (document.body->>'finance_component_id'),
      (document.body#>>'{source_basis_json,finance_component_id}'),
      (document.body#>>'{component,finance_component_id}')
    ) value(value_text)
  ), classified AS (
    SELECT value_text,pg_input_is_valid(value_text,'uuid') AS is_valid
    FROM component_value WHERE value_text IS NOT NULL
  )
  SELECT COUNT(*) FILTER(WHERE NOT is_valid)::integer,
    COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid)
      FILTER(WHERE is_valid),ARRAY[]::uuid[])
  INTO v_invalid_component_uuid_count,finance_component_ids
  FROM classified;

  WITH document AS (
    SELECT entry.value->'document' AS body
    FROM jsonb_array_elements(v_basis_documents||v_snapshot_documents) entry(value)
  ), supplied_assertion AS (
    SELECT NULLIF(UPPER(BTRIM(COALESCE(value.key_type,''))),'') AS key_type,
      NULLIF(BTRIM(COALESCE(value.key_value,'')),'') AS key_value
    FROM jsonb_array_elements(v_key_assertions) assertion(value)
    CROSS JOIN LATERAL (VALUES
      (assertion.value->>'component_key_type',
       assertion.value->>'component_key_value'),
      (assertion.value->>'key_type',assertion.value->>'key_value')
    ) value(key_type,key_value)
    WHERE jsonb_typeof(assertion.value)='object'
  ), frozen_assertion AS (
    SELECT NULLIF(UPPER(BTRIM(COALESCE(value.key_type,''))),'') AS key_type,
      NULLIF(BTRIM(COALESCE(value.key_value,'')),'') AS key_value
    FROM document
    CROSS JOIN LATERAL (VALUES
      (document.body->>'component_key_type',
       document.body->>'component_key_value'),
      (document.body->>'key_type',document.body->>'key_value'),
      (document.body#>>'{economic_key,component_key_type}',
       document.body#>>'{economic_key,component_key_value}'),
      (document.body#>>'{economic_key,key_type}',
       document.body#>>'{economic_key,key_value}'),
      (document.body#>>'{source_basis_json,component_key_type}',
       document.body#>>'{source_basis_json,component_key_value}'),
      (document.body#>>'{source_basis_json,key_type}',
       document.body#>>'{source_basis_json,key_value}'),
      (document.body#>>'{source_basis_json,economic_key,component_key_type}',
       document.body#>>'{source_basis_json,economic_key,component_key_value}'),
      (document.body#>>'{source_basis_json,economic_key,key_type}',
       document.body#>>'{source_basis_json,economic_key,key_value}'),
      (document.body#>>'{component,component_key_type}',
       document.body#>>'{component,component_key_value}'),
      (document.body#>>'{component,key_type}',
       document.body#>>'{component,key_value}')
    ) value(key_type,key_value)
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

  resolved_timesheet_id:=CASE WHEN cardinality(owner_ids)=1 THEN owner_ids[1] END;
  resolved_candidate_id:=CASE WHEN cardinality(candidate_ids)=1 THEN candidate_ids[1] END;
  resolved_finance_case_id:=CASE WHEN cardinality(finance_case_ids)=1
    THEN finance_case_ids[1] END;
  resolved_finance_component_id:=CASE WHEN cardinality(finance_component_ids)=1
    THEN finance_component_ids[1] END;
  resolved_component_key_type:=CASE WHEN cardinality(component_key_pairs)=1
    THEN split_part(component_key_pairs[1],E'\x1f',1) END;
  resolved_component_key_value:=CASE WHEN cardinality(component_key_pairs)=1
    THEN split_part(component_key_pairs[1],E'\x1f',2) END;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'document_kind',document_kind,'source',source_name,
      'document_digest',md5(body::text)) ORDER BY document_kind,source_name,md5(body::text)),
    '[]'::jsonb)
  INTO v_document_evidence
  FROM (
    SELECT 'SOURCE_BASIS'::text document_kind,
      UPPER(BTRIM(entry.value->>'source')) source_name,entry.value->'document' body
    FROM jsonb_array_elements(v_basis_documents) entry(value)
    UNION ALL
    SELECT 'COMPONENT_SNAPSHOT',UPPER(BTRIM(entry.value->>'source')),
      entry.value->'document'
    FROM jsonb_array_elements(v_snapshot_documents) entry(value)
  ) documents;

  evidence_json:=jsonb_build_object(
    'authority_version',3,'context',v_context,
    'documents',v_document_evidence,
    'owner_ids',to_jsonb(owner_ids),'candidate_ids',to_jsonb(candidate_ids),
    'finance_case_ids',to_jsonb(finance_case_ids),
    'finance_component_ids',to_jsonb(finance_component_ids),
    'component_key_pairs',to_jsonb(component_key_pairs),
    'invalid_frozen_uuid_count',v_invalid_uuid_count,
    'invalid_finance_component_uuid_count',v_invalid_component_uuid_count,
    'incomplete_component_key_count',v_incomplete_key_count,
    'resolved_timesheet_id',resolved_timesheet_id,
    'resolved_candidate_id',resolved_candidate_id,
    'resolved_finance_case_id',resolved_finance_case_id,
    'resolved_finance_component_id',resolved_finance_component_id,
    'resolved_component_key_type',resolved_component_key_type,
    'resolved_component_key_value',resolved_component_key_value,
    'resolution_failure',resolution_failure);
  RETURN NEXT;
END;
$function$;

ALTER FUNCTION private.pay_workbench_financial_source_authority_v3(
  uuid,uuid[],uuid[],uuid[],uuid[],jsonb,jsonb,jsonb,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_financial_source_authority_v3(
  uuid,uuid[],uuid[],uuid[],uuid[],jsonb,jsonb,jsonb,text)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_financial_source_authority_v3(
  uuid,uuid[],uuid[],uuid[],uuid[],jsonb,jsonb,jsonb,text) TO postgres;
