-- Read-only projection of the unchanged Banking Pay bank-action guards.
-- This does not perform a name check, accept details, create a payee or change
-- any financial/readiness authority. Existing mutation owners still revalidate.
\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_bank_blockers_v2(p_row jsonb)
RETURNS text[] LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE
  v_context jsonb := CASE WHEN jsonb_typeof(p_row->'payee_context')='object' THEN p_row->'payee_context' ELSE '{}'::jsonb END;
  v_raw jsonb; v_array jsonb; v_item jsonb; v_text text; v_code text;
  v_result text[] := ARRAY[]::text[];
BEGIN
  FOREACH v_raw IN ARRAY ARRAY[p_row->'blockers',p_row->'blocked_reason_codes',p_row->'blockedReasonCodes',
    v_context->'blockers',v_context->'blocked_reason_codes',v_context->'blockedReasonCodes'] LOOP
    v_array := '[]'::jsonb;
    IF jsonb_typeof(v_raw)='array' THEN v_array := v_raw;
    ELSIF jsonb_typeof(v_raw)='string' THEN
      v_text := BTRIM(v_raw #>> '{}');
      BEGIN v_array := v_text::jsonb;
      EXCEPTION WHEN invalid_text_representation THEN v_array := NULL; END;
      IF jsonb_typeof(v_array) IS DISTINCT FROM 'array' THEN
        v_array := to_jsonb(string_to_array(v_text,','));
      END IF;
    END IF;
    FOR v_item IN SELECT value FROM jsonb_array_elements(COALESCE(v_array,'[]'::jsonb)) LOOP
      v_code := UPPER(BTRIM(COALESCE(v_item #>> '{}','')));
      IF v_code<>'' AND NOT v_code=ANY(v_result) THEN v_result:=array_append(v_result,v_code); END IF;
    END LOOP;
  END LOOP;
  RETURN v_result;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_bank_blockers_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_bank_blockers_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_bank_meta_v2(p_row jsonb, p_candidate_meta jsonb DEFAULT NULL)
RETURNS jsonb LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  WITH source AS (
    SELECT p_row AS r, COALESCE(p_candidate_meta,'{}'::jsonb) AS m,
      CASE WHEN jsonb_typeof(p_row->'payee_context')='object' THEN p_row->'payee_context' ELSE '{}'::jsonb END AS c,
      CASE WHEN jsonb_typeof(p_candidate_meta->'payee_context')='object' THEN p_candidate_meta->'payee_context' ELSE '{}'::jsonb END AS mc
  )
  SELECT CASE WHEN jsonb_typeof(r) IS DISTINCT FROM 'object' THEN NULL ELSE jsonb_build_object(
    'candidate_id', BTRIM(COALESCE(NULLIF(r->>'candidate_id',''),NULLIF(m->>'candidate_id',''),'')),
    'preview_row_id', BTRIM(COALESCE(NULLIF(r->>'preview_row_id',''),NULLIF(r->>'previewRowId',''),NULLIF(r->>'row_id',''),
      NULLIF(r->>'rowId',''),NULLIF(r->>'line_id',''),NULLIF(r->>'lineId',''),r->>'id','')),
    'display_name', BTRIM(COALESCE(NULLIF(r->>'display_name',''),m->>'display_name','')),
    'tms_ref', BTRIM(COALESCE(NULLIF(r->>'tms_ref',''),m->>'tms_ref','')),
    'current_pay_method', UPPER(BTRIM(COALESCE(NULLIF(r->>'pay_channel',''),NULLIF(r->>'current_pay_method',''),m->>'current_pay_method',''))),
    'is_ready_for_draft', LOWER(BTRIM(COALESCE(CASE WHEN r ? 'is_ready_for_draft' THEN r->>'is_ready_for_draft' ELSE m->>'is_ready_for_draft' END,''))) IN ('true','1','yes','y','on'),
    'blockers', to_jsonb(private.pay_workbench_modal_bank_blockers_v2(CASE
      WHEN r ?| ARRAY['blockers','blocked_reason_codes','blockedReasonCodes'] OR c ?| ARRAY['blockers','blocked_reason_codes','blockedReasonCodes'] THEN r ELSE m END)),
    'payee_entity_kind', UPPER(BTRIM(COALESCE(NULLIF(r->>'payee_entity_kind',''),NULLIF(c->>'payee_entity_kind',''),m->>'payee_entity_kind',''))),
    'payee_entity_id', BTRIM(COALESCE(NULLIF(r->>'payee_entity_id',''),NULLIF(c->>'payee_entity_id',''),m->>'payee_entity_id','')),
    'bank_details_hash', BTRIM(COALESCE(NULLIF(r->>'bank_details_hash',''),NULLIF(r->>'payee_bank_hash',''),
      NULLIF(r->>'bank_details_hash_snapshot',''),NULLIF(r->>'snapshot_bank_details_hash',''),
      NULLIF(c->>'bank_details_hash',''),NULLIF(c->>'payee_bank_hash',''),NULLIF(c->>'bank_details_hash_snapshot',''),NULLIF(c->>'snapshot_bank_details_hash',''),
      NULLIF(m->>'bank_details_hash',''),NULLIF(m->>'payee_bank_hash',''),NULLIF(m->>'bank_details_hash_snapshot',''),NULLIF(m->>'snapshot_bank_details_hash',''),
      NULLIF(mc->>'bank_details_hash',''),NULLIF(mc->>'payee_bank_hash',''),NULLIF(mc->>'bank_details_hash_snapshot',''),mc->>'snapshot_bank_details_hash','')),
    'payee_bank_hash', BTRIM(COALESCE(NULLIF(r->>'payee_bank_hash',''),NULLIF(c->>'payee_bank_hash',''),m->>'payee_bank_hash','')),
    'bank_details_hash_snapshot', BTRIM(COALESCE(NULLIF(r->>'bank_details_hash_snapshot',''),NULLIF(c->>'bank_details_hash_snapshot',''),m->>'bank_details_hash_snapshot','')),
    'snapshot_bank_details_hash', BTRIM(COALESCE(NULLIF(r->>'snapshot_bank_details_hash',''),NULLIF(c->>'snapshot_bank_details_hash',''),m->>'snapshot_bank_details_hash','')),
    'name_check_status', UPPER(BTRIM(COALESCE(NULLIF(r->>'name_check_status',''),NULLIF(c->>'name_check_status',''),m->>'name_check_status',''))),
    'name_check_has_override', LOWER(BTRIM(COALESCE(CASE WHEN r ? 'name_check_has_override' THEN r->>'name_check_has_override'
      WHEN c ? 'name_check_has_override' THEN c->>'name_check_has_override' ELSE m->>'name_check_has_override' END,''))) IN ('true','1','yes','y','on'),
    'payee_map_present', LOWER(BTRIM(COALESCE(CASE WHEN r ? 'payee_map_present' THEN r->>'payee_map_present'
      WHEN c ? 'payee_map_present' THEN c->>'payee_map_present' ELSE m->>'payee_map_present' END,''))) IN ('true','1','yes','y','on')
  ) END FROM source;
$function$;
ALTER FUNCTION private.pay_workbench_modal_bank_meta_v2(jsonb,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_bank_meta_v2(jsonb,jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_bank_action_v2(p_meta jsonb)
RETURNS text LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  WITH flags AS (
    SELECT UPPER(BTRIM(COALESCE(p_meta->>'payee_entity_kind',''))) IN ('CANDIDATE','UMBRELLA')
        AND BTRIM(COALESCE(p_meta->>'payee_entity_id',''))<>'' AS valid_entity,
      BTRIM(COALESCE(NULLIF(p_meta->>'bank_details_hash',''),NULLIF(p_meta->>'payee_bank_hash',''),
        NULLIF(p_meta->>'bank_details_hash_snapshot',''),p_meta->>'snapshot_bank_details_hash',''))<>'' AS exact_hash,
      CASE WHEN jsonb_typeof(p_meta->'blockers')='array' THEN p_meta->'blockers' ELSE '[]'::jsonb END AS blockers,
      LOWER(BTRIM(COALESCE(p_meta->>'name_check_has_override',''))) IN ('true','1','yes','y','on') AS has_override,
      UPPER(BTRIM(COALESCE(p_meta->>'name_check_status',''))) AS name_status
  )
  SELECT CASE
    WHEN NOT valid_entity OR NOT exact_hash OR blockers ? 'BLOCKED_BANK_DETAILS' OR blockers ? 'BLOCKED_UMBRELLA_INACTIVE' THEN NULL
    WHEN blockers ? 'BLOCKED_NAME_CHECK' AND NOT has_override AND name_status IN ('FAIL','NEAR_MATCH','UNAVAILABLE')
      THEN 'banking:pay:acceptBankDetails'
    WHEN blockers ? 'BLOCKED_NAME_CHECK' AND NOT has_override AND name_status NOT IN ('PASS','FAIL','NEAR_MATCH','UNAVAILABLE')
      THEN 'banking:pay:runBankNameCheck'
    WHEN blockers ? 'BLOCKED_NO_PAYEE_MAP' AND (has_override OR name_status='PASS')
      THEN 'banking:pay:ensurePayeeMap'
    ELSE NULL END FROM flags;
$function$;
ALTER FUNCTION private.pay_workbench_modal_bank_action_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_bank_action_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;
