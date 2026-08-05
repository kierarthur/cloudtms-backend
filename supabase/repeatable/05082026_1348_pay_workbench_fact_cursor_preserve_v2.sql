-- Banking Pay bounded-scope Version 1.2.9.
-- Validate a Version-2 fact cursor without reconstructing or normalising it.
-- The returned JSON value is exactly the supplied value so a committed page-end
-- cursor is the next page's byte-for-byte canonical JSON authority.

CREATE OR REPLACE FUNCTION private.pay_workbench_fact_cursor_preserve_v2(
  p_cursor_json jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_cursor jsonb:=p_cursor_json;
  v_required_keys text[]:=ARRAY[
    'cursor_kind','cursor_version','build_id','candidate_id',
    'captured_candidate_generation','captured_source_change_seq',
    'dependency_unit_key','fact_family','page_number','last_source_key',
    'previous_page_digest','cumulative_fact_count','cumulative_digest','terminal',
    'raw_physical_source_count','resolved_physical_source_count',
    'failed_physical_source_count','raw_physical_amount_ex_vat',
    'resolved_physical_amount_ex_vat','last_raw_physical_source_key',
    'source_exhausted','raw_terminal_source_key','raw_page_evidence_digest',
    'input_phase','input_projection_id'
  ];
  v_key text;
BEGIN
  IF jsonb_typeof(v_cursor)<>'object'
     OR v_cursor->>'cursor_kind'<>'WORKSPACE_FACT'
     OR COALESCE((v_cursor->>'cursor_version')::integer,0)<>2 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_VERSION_OBSOLETE'
      USING ERRCODE='40001';
  END IF;

  FOREACH v_key IN ARRAY v_required_keys LOOP
    IF NOT (v_cursor ? v_key) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_V2_FIELD_MISSING'
        USING ERRCODE='22023',DETAIL=v_key;
    END IF;
  END LOOP;

  IF EXISTS(
    SELECT 1
    FROM jsonb_object_keys(v_cursor) supplied(key_name)
    WHERE NOT supplied.key_name=ANY(v_required_keys)
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_V2_FIELD_UNKNOWN'
      USING ERRCODE='22023';
  END IF;

  IF COALESCE(v_cursor->>'input_phase','') NOT IN ('PHYSICAL_SOURCE','PROJECTION','COMPONENTS')
     OR jsonb_typeof(v_cursor->'terminal')<>'boolean'
     OR jsonb_typeof(v_cursor->'source_exhausted')<>'boolean'
     OR COALESCE((v_cursor->>'page_number')::integer,0)<1
     OR COALESCE((v_cursor->>'cumulative_fact_count')::bigint,-1)<0
     OR COALESCE((v_cursor->>'raw_physical_source_count')::bigint,-1)<0
     OR COALESCE((v_cursor->>'resolved_physical_source_count')::bigint,-1)<0
     OR COALESCE((v_cursor->>'failed_physical_source_count')::bigint,-1)<0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID'
      USING ERRCODE='22023';
  END IF;

  RETURN v_cursor;
END;
$function$;

ALTER FUNCTION private.pay_workbench_fact_cursor_preserve_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_fact_cursor_preserve_v2(jsonb)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_fact_cursor_preserve_v2(jsonb)
  TO postgres;
