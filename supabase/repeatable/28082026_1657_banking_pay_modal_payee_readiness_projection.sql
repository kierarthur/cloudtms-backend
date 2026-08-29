-- Exact synthetic bank-readiness presentation from the current Banking renderer.
-- A bank problem does not require a physical blocked preview row to remain
-- visible. This private projection preserves metadata only: no provider call,
-- bank acceptance, selection change or financial calculation.
\set ON_ERROR_STOP on
\ir 28082026_1333_banking_pay_modal_bank_action_facts.sql
begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_route_text_v2(p_values jsonb[])
RETURNS text LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  -- These legacy aliases use || before trim: explicit whitespace is truthy
  -- and must not silently fall through to a different bank-account identity.
  SELECT COALESCE((SELECT BTRIM(v.value #>> '{}') FROM unnest(p_values) WITH ORDINALITY v(value,ord)
    WHERE v.value IS NOT NULL AND v.value NOT IN ('null'::jsonb,'false'::jsonb,'0'::jsonb,'""'::jsonb)
    ORDER BY v.ord LIMIT 1),'');
$function$;
ALTER FUNCTION private.pay_workbench_modal_route_text_v2(jsonb[]) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_route_text_v2(jsonb[]) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_candidate_bank_meta_v2(p_row jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  -- Exact current candidateMetaById value, including context-to-snake aliases.
  -- Passing a raw stored fragment as this fallback would lose nested owner data.
  WITH source AS (SELECT p_row AS r,CASE WHEN jsonb_typeof(p_row->'payee_context')='object' THEN p_row->'payee_context' ELSE '{}'::jsonb END AS c)
  SELECT CASE WHEN BTRIM(COALESCE(r->>'candidate_id',''))='' THEN NULL ELSE jsonb_build_object(
    'candidate_id',BTRIM(r->>'candidate_id'),
    'display_name',private.pay_workbench_modal_route_text_v2(ARRAY[r->'display_name',r->'candidate_name']),
    'tms_ref',private.pay_workbench_modal_route_text_v2(ARRAY[r->'tms_ref']),
    'current_pay_method',UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[r->'current_pay_method'])),
    'is_ready_for_draft',LOWER(BTRIM(COALESCE(r->>'is_ready_for_draft',''))) IN ('true','1','yes','y','on'),
    'blockers',to_jsonb(private.pay_workbench_modal_bank_blockers_v2(r)),
    'payee_entity_kind',UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[r->'payee_entity_kind',c->'payee_entity_kind'])),
    'payee_entity_id',private.pay_workbench_modal_route_text_v2(ARRAY[r->'payee_entity_id',c->'payee_entity_id']),
    'bank_details_hash',private.pay_workbench_modal_route_text_v2(ARRAY[r->'bank_details_hash',r->'payee_bank_hash',r->'bank_details_hash_snapshot',r->'snapshot_bank_details_hash',c->'bank_details_hash',c->'payee_bank_hash']),
    'payee_bank_hash',private.pay_workbench_modal_route_text_v2(ARRAY[r->'payee_bank_hash',r->'bank_details_hash',c->'payee_bank_hash',c->'bank_details_hash']),
    'bank_details_hash_snapshot',private.pay_workbench_modal_route_text_v2(ARRAY[r->'bank_details_hash_snapshot',r->'snapshot_bank_details_hash',c->'bank_details_hash_snapshot',c->'snapshot_bank_details_hash']),
    'snapshot_bank_details_hash',private.pay_workbench_modal_route_text_v2(ARRAY[r->'snapshot_bank_details_hash',r->'bank_details_hash_snapshot',c->'snapshot_bank_details_hash',c->'bank_details_hash_snapshot']),
    'name_check_status',UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[r->'name_check_status',c->'name_check_status'])),
    'name_check_has_override',LOWER(BTRIM(COALESCE(CASE WHEN r ? 'name_check_has_override' THEN r->>'name_check_has_override' ELSE c->>'name_check_has_override' END,''))) IN ('true','1','yes','y','on'),
    'payee_map_present',LOWER(BTRIM(COALESCE(CASE WHEN r ? 'payee_map_present' THEN r->>'payee_map_present' ELSE c->>'payee_map_present' END,''))) IN ('true','1','yes','y','on'),
    'payee_readiness_status',UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[r->'payee_readiness_status',r->'readiness_status',r->'payee_setup_status'])),
    'payee_readiness_job_status',UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[r->'payee_readiness_job_status',r->'readiness_job_status',r->'latest_payee_readiness_job_status'])),
    'latest_job_type',UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[r->'latest_job_type'])),
    'latest_job_status',UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[r->'latest_job_status']))
  ) END FROM source;
$function$;
ALTER FUNCTION private.pay_workbench_modal_candidate_bank_meta_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_candidate_bank_meta_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_payee_route_v2(p_row jsonb,p_fallback jsonb DEFAULT NULL)
RETURNS jsonb LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  WITH f AS (
    SELECT UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[
      p_row->'payee_entity_kind',p_row->'payeeEntityKind',p_row->'entity_kind',p_row->'entityKind',
      p_fallback->'payee_entity_kind',p_fallback->'payeeEntityKind',p_fallback->'entity_kind',p_fallback->'entityKind'])) AS kind,
      private.pay_workbench_modal_route_text_v2(ARRAY[
      p_row->'payee_entity_id',p_row->'payeeEntityId',p_row->'entity_id',p_row->'entityId',
      p_fallback->'payee_entity_id',p_fallback->'payeeEntityId',p_fallback->'entity_id',p_fallback->'entityId']) AS id,
      private.pay_workbench_modal_route_text_v2(ARRAY[
      p_row->'bank_details_hash',p_row->'bankDetailsHash',p_row->'payee_bank_hash',p_row->'payeeBankHash',
      p_row->'bank_details_hash_snapshot',p_row->'bankDetailsHashSnapshot',p_row->'snapshot_bank_details_hash',p_row->'snapshotBankDetailsHash',
      p_fallback->'bank_details_hash',p_fallback->'bankDetailsHash',p_fallback->'payee_bank_hash',p_fallback->'payeeBankHash',
      p_fallback->'bank_details_hash_snapshot',p_fallback->'bankDetailsHashSnapshot',p_fallback->'snapshot_bank_details_hash',p_fallback->'snapshotBankDetailsHash']) AS hash,
      private.pay_workbench_modal_route_text_v2(ARRAY[p_row->'candidate_id',p_row->'candidateId',p_fallback->'candidate_id',p_fallback->'candidateId']) AS candidate
  )
  SELECT jsonb_build_object('entity_kind',f.kind,'entity_id',f.id,'route_bank_hash',f.hash,'candidate_id',f.candidate,
    'legacy_display_route',CASE WHEN f.kind<>'' AND f.id<>'' THEN f.kind || '|' || f.id || '|' || COALESCE(NULLIF(f.hash,''),'NO_BANK_HASH')
      WHEN f.candidate<>'' THEN 'CANDIDATE_FALLBACK|' || f.candidate ELSE '' END)
  FROM f;
$function$;
ALTER FUNCTION private.pay_workbench_modal_payee_route_v2(jsonb,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_payee_route_v2(jsonb,jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_payee_readiness_row_v2(
  p_payee jsonb,p_candidate_meta jsonb DEFAULT NULL,p_candidate_section_amount numeric DEFAULT NULL
) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_candidate text; v_route jsonb; v_bank jsonb; v_hash text; v_kind text; v_id text;
  v_blockers text[]; v_status text; v_job_type text; v_amount jsonb; v_value jsonb;
  v_raw_blockers jsonb; v_blocker_text text;
BEGIN
  IF jsonb_typeof(p_payee) IS DISTINCT FROM 'object' THEN RETURN NULL; END IF;
  -- This synthetic renderer uses parseBlockerCodes, not the line-meta helper's
  -- union/deduplication: preserve the complete legacy array and its order.
  v_raw_blockers:=p_payee->'blockers';
  IF jsonb_typeof(v_raw_blockers)='string' THEN
    v_blocker_text:=BTRIM(v_raw_blockers #>> '{}');
    BEGIN v_raw_blockers:=v_blocker_text::jsonb;
    EXCEPTION WHEN invalid_text_representation THEN v_raw_blockers:=NULL; END;
    IF jsonb_typeof(v_raw_blockers) IS DISTINCT FROM 'array' THEN
      v_raw_blockers:=to_jsonb(string_to_array(v_blocker_text,','));
    END IF;
  END IF;
  SELECT COALESCE(array_agg(code ORDER BY ord),ARRAY[]::text[]) INTO v_blockers
  FROM (SELECT UPPER(BTRIM(value #>> '{}')) AS code,ord
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(v_raw_blockers)='array' THEN v_raw_blockers ELSE '[]'::jsonb END) WITH ORDINALITY b(value,ord)) codes
  WHERE code IN ('BLOCKED_BANK_DETAILS','BLOCKED_NAME_CHECK','BLOCKED_NO_PAYEE_MAP','BLOCKED_UMBRELLA_INACTIVE');
  v_status:=UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'payee_readiness_status',p_payee->'readiness_status',
    p_payee->'payee_setup_status',p_payee->'payee_readiness_job_status',p_payee->'readiness_job_status',
    p_payee->'latest_payee_readiness_job_status',p_payee->'latest_job_status']));
  v_job_type:=UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'latest_job_type',p_payee->'job_type']));
  IF cardinality(v_blockers)=0 AND NOT (v_job_type='PAYEE_READINESS_ENSURE'
    AND v_status IN ('QUEUED','RUNNING','PENDING','IN_PROGRESS','FAILED','ERROR')) THEN RETURN NULL; END IF;
  -- Most payment rows have no bank issue. Do not format their unused bank
  -- identity/presentation before the identical eligibility test has passed.
  v_route:=private.pay_workbench_modal_payee_route_v2(p_payee,p_candidate_meta);
  v_candidate:=private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'candidate_id',p_payee->'candidateId']);
  v_kind:=UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'payee_entity_kind',p_payee->'payeeEntityKind',
    p_payee->'entity_kind',p_payee->'entityKind',p_candidate_meta->'payee_entity_kind']));
  v_id:=private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'payee_entity_id',p_payee->'payeeEntityId',
    p_payee->'entity_id',p_payee->'entityId',p_candidate_meta->'payee_entity_id']);
  IF v_route->>'legacy_display_route'='' THEN RETURN NULL; END IF;
  -- The action hash uses the existing exact-bank-target helper's snake aliases
  -- and payee_context. The display route's camel aliases do NOT widen it.
  v_bank:=private.pay_workbench_modal_bank_meta_v2(p_payee,p_candidate_meta);
  v_hash:=v_bank->>'bank_details_hash';
  FOREACH v_value IN ARRAY ARRAY[p_payee->'section_amount_display',p_payee->'section_amount_ex_vat',
    p_payee->'amount_display',p_payee->'amount_ex_vat',p_payee->'safe_amount_ex',p_payee->'safe_amount_inc_vat',
    CASE WHEN v_candidate<>'' THEN to_jsonb(p_candidate_section_amount) END] LOOP
    IF v_value IS NULL OR v_value='null'::jsonb OR (jsonb_typeof(v_value)='string' AND BTRIM(v_value #>> '{}')='') THEN CONTINUE; END IF;
    v_amount:=v_value;EXIT;
  END LOOP;
  RETURN jsonb_strip_nulls(jsonb_build_object(
    '__payee_readiness_blocker',true,'__payee_route_key',v_route->>'legacy_display_route',
    'line_type','PAYEE_READINESS','presentation_section','BLOCKED_FOR_PAY','readiness_state','BLOCKED_FOR_PAY',
    'candidate_id',v_candidate,
    'display_name',private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'display_name',p_payee->'candidate_name',p_payee->'payee_name',p_candidate_meta->'display_name']),
    'tms_ref',private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'tms_ref',p_candidate_meta->'tms_ref']),
    'client_id',private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'client_id',p_payee->'clientId']),
    'client_name',private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'client_name']),
    'pay_channel',UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'pay_channel',p_payee->'current_pay_method',
      p_candidate_meta->'current_pay_method',CASE WHEN v_kind='UMBRELLA' THEN '"UMBRELLA"'::jsonb END])),
    'current_pay_method',UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'current_pay_method',p_payee->'pay_channel',
      p_candidate_meta->'current_pay_method',CASE WHEN v_kind='UMBRELLA' THEN '"UMBRELLA"'::jsonb END])),
    'payee_entity_kind',v_kind,'payee_entity_id',v_id,'bank_details_hash',v_hash,
    'payee_bank_hash',private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'payee_bank_hash',p_payee->'payeeBankHash',to_jsonb(v_hash)]),
    'bank_details_hash_snapshot',private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'bank_details_hash_snapshot',p_payee->'bankDetailsHashSnapshot',to_jsonb(v_hash)]),
    'snapshot_bank_details_hash',private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'snapshot_bank_details_hash',p_payee->'snapshotBankDetailsHash',to_jsonb(v_hash)]),
    'blockers',to_jsonb(v_blockers),
    'name_check_status',UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'name_check_status',p_payee#>'{name_check,status}'])),
    'name_check_has_override',LOWER(BTRIM(COALESCE(CASE WHEN p_payee ? 'name_check_has_override' THEN p_payee->>'name_check_has_override' ELSE p_payee#>>'{name_check,has_override}' END,''))) IN ('true','1','yes','y','on'),
    'payee_map_present',LOWER(BTRIM(COALESCE(CASE WHEN p_payee ? 'payee_map_present' THEN p_payee->>'payee_map_present' ELSE p_payee#>>'{payee_map,present}' END,''))) IN ('true','1','yes','y','on'),
    'payee_readiness_status',v_status,
    'payee_readiness_job_status',UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'payee_readiness_job_status',p_payee->'readiness_job_status'])),
    'latest_job_type',v_job_type,'latest_job_status',UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[p_payee->'latest_job_status'])),
    'is_ready_for_draft',false,'section_amount_display',v_amount,'section_amount_ex_vat',v_amount,
    'amount_display',v_amount,'amount_ex_vat',v_amount));
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_payee_readiness_row_v2(jsonb,jsonb,numeric) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_payee_readiness_row_v2(jsonb,jsonb,numeric) FROM PUBLIC, anon, authenticated, service_role;
commit;
