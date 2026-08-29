-- Repeatable CloudTMS function/view authority: banking_pay_modal_structure_v2
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on
\ir 28082026_1308_banking_pay_modal_ready_members.sql
\ir 28082026_1333_banking_pay_modal_bank_action_facts.sql
\ir 28082026_1354_banking_pay_modal_case_action_facts.sql
\ir 28082026_1448_banking_pay_modal_component_action_facts.sql
\ir 28082026_1451_banking_pay_modal_draft_gate.sql

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_scope_hash_v2(
  p_session public.banking_pay_workbench_sessions, p_channel text
) RETURNS text
LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  SELECT encode(extensions.digest(convert_to(
    'BANKING_PAY_MODAL_STRUCTURE_V2' || E'\n' || p_session.id::text || E'\n'
    || p_session.version::text || E'\n' || p_session.pay_date::text || E'\n'
    || p_session.week_ending_cutoff::text || E'\n' || p_channel || E'\n'
    || p_session.session_signature, 'UTF8'), 'sha256'), 'hex');
$function$;
ALTER FUNCTION private.pay_workbench_modal_scope_hash_v2(public.banking_pay_workbench_sessions, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_scope_hash_v2(public.banking_pay_workbench_sessions, text) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_context_v2(
  p_session_id uuid, p_options_json jsonb, p_actor_user_id uuid
) RETURNS public.banking_pay_workbench_sessions
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_contract jsonb;
BEGIN
  IF jsonb_typeof(p_options_json) IS DISTINCT FROM 'object'
     OR COALESCE(p_options_json->>'expected_session_version', '') !~ '^[0-9]{1,16}$'
     OR COALESCE(p_options_json->>'expected_progress_counter_version', '') !~ '^[0-9]{1,16}$'
     OR COALESCE(p_options_json->>'scope_hash', '') !~ '^[a-f0-9]{64}$'
     OR COALESCE(p_options_json->>'pay_channel_scope', '') NOT IN ('ALL','PAYE','UMBRELLA')
     OR EXISTS (SELECT 1 FROM jsonb_object_keys(p_options_json) AS k(value)
                WHERE k.value NOT IN ('expected_session_version','expected_progress_counter_version','scope_hash','pay_channel_scope')) THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE = '22023';
  END IF;
  -- Banking remains admin-only. Shared sessions are not restricted to their
  -- creator: another currently authorised admin retains existing access.
  IF NOT EXISTS (SELECT 1 FROM public.tms_users AS actor
                 WHERE actor.id = p_actor_user_id AND actor.is_active IS TRUE AND lower(actor.role) = 'admin') THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_UNAUTHORISED' USING ERRCODE = '42501';
  END IF;
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id = p_session_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'WORKBENCH_SESSION_NOT_FOUND' USING ERRCODE = 'P0001'; END IF;
  IF v_session.status <> 'OPEN' OR v_session.discarded_at_utc IS NOT NULL OR v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'OBSOLETE_SESSION' USING ERRCODE = 'P0001';
  END IF;
  IF v_session.version <> (p_options_json->>'expected_session_version')::bigint
     OR v_session.progress_counter_version <> (p_options_json->>'expected_progress_counter_version')::bigint THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_STALE_REVISION' USING ERRCODE = 'P0001';
  END IF;
  IF private.pay_workbench_modal_scope_hash_v2(v_session, p_options_json->>'pay_channel_scope')
     IS DISTINCT FROM p_options_json->>'scope_hash' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_SCOPE_MISMATCH' USING ERRCODE = 'P0001';
  END IF;
  v_contract := public.pay_workbench_contract_version_get_v1();
  IF v_contract->>'contract_version' IS DISTINCT FROM 'BANKING_PAY_WORKBENCH_DB_V1'
     OR v_contract->>'canonical_correction_carrier_version' IS DISTINCT FROM 'BANKING_PAY_CANONICAL_CORRECTION_CARRIER_V1'
     OR v_contract->>'targeted_family_materialisation_version' IS DISTINCT FROM 'BANKING_PAY_TARGETED_FAMILY_MATERIALISATION_V1'
     OR v_contract#>>'{candidate_projection_contract,canonical_correction_carrier_version}' IS DISTINCT FROM 'BANKING_PAY_CANONICAL_CORRECTION_CARRIER_V1'
     OR v_contract#>>'{candidate_projection_contract,targeted_family_materialisation_version}' IS DISTINCT FROM 'BANKING_PAY_TARGETED_FAMILY_MATERIALISATION_V1' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE = 'P0001';
  END IF;
  RETURN v_session;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_context_v2(uuid, jsonb, uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_context_v2(uuid, jsonb, uuid) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_summary_context_v2(
  p_session_id uuid, p_options_json jsonb, p_actor_user_id uuid, p_cursor text
) RETURNS public.banking_pay_workbench_sessions
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_session public.banking_pay_workbench_sessions%ROWTYPE; v_options jsonb:=p_options_json;
BEGIN
  IF jsonb_typeof(p_options_json) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  IF p_options_json->>'scope_hash' IS NULL THEN
    -- Only the first page may discover the server-generated scope. Opening the
    -- modal must not need a second round trip or a client copy of hash rules.
    -- Current revisions, channel, session filters and actor are still proved by
    -- the unchanged strict context owner below. No mutation uses this helper.
    IF p_cursor IS NOT NULL THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_SCOPE_MISMATCH' USING ERRCODE='P0001';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.tms_users AS actor
      WHERE actor.id=p_actor_user_id AND actor.is_active IS TRUE AND lower(actor.role)='admin') THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_UNAUTHORISED' USING ERRCODE='42501';
    END IF;
    SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id=p_session_id;
    IF NOT FOUND THEN RAISE EXCEPTION 'WORKBENCH_SESSION_NOT_FOUND' USING ERRCODE='P0001'; END IF;
    v_options:=p_options_json || jsonb_build_object('scope_hash',
      private.pay_workbench_modal_scope_hash_v2(v_session,p_options_json->>'pay_channel_scope'));
  END IF;
  RETURN private.pay_workbench_modal_context_v2(p_session_id,v_options,p_actor_user_id);
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_summary_context_v2(uuid,jsonb,uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_summary_context_v2(uuid,jsonb,uuid,text) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_cursor_encode_v2(p_value jsonb)
RETURNS text LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  SELECT translate(replace(encode(convert_to(p_value::text, 'UTF8'), 'base64'), E'\n', ''), '+/=', '-_');
$function$;
ALTER FUNCTION private.pay_workbench_modal_cursor_encode_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_cursor_encode_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_cursor_decode_v2(p_cursor text, p_binding jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_value jsonb; v_base64 text;
BEGIN
  IF p_cursor IS NULL THEN RETURN NULL; END IF;
  IF length(p_cursor) NOT BETWEEN 1 AND 4096 OR p_cursor !~ '^[A-Za-z0-9_-]+$' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_CURSOR' USING ERRCODE = '22023';
  END IF;
  BEGIN
    v_base64 := translate(p_cursor, '-_', '+/');
    v_value := convert_from(decode(rpad(v_base64, ((length(v_base64)+3)/4)*4, '='), 'base64'), 'UTF8')::jsonb;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_CURSOR' USING ERRCODE = '22023';
  END;
  IF jsonb_typeof(v_value) IS DISTINCT FROM 'object' OR NOT (v_value @> p_binding) THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE = 'P0001';
  END IF;
  RETURN v_value;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_cursor_decode_v2(text, jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_cursor_decode_v2(text, jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_candidate_row_v2(p_facts jsonb, p_binding jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  WITH exact_ids AS (
    SELECT COALESCE(p_facts->'selected_timesheet_ids','[]'::jsonb) AS ids
  ), scope AS (
    SELECT ids, jsonb_array_length(ids) AS count,
      encode(extensions.digest(convert_to(ids::text,'UTF8'),'sha256'),'hex') AS timesheet_hash
    FROM exact_ids
  )
  SELECT p_facts || jsonb_build_object(
    -- Complete canonical content, including all selected Timesheet IDs before
    -- compaction. Navigation metadata is deliberately outside this digest.
    'facts_digest',encode(extensions.digest(convert_to(p_facts::text,'UTF8'),'sha256'),'hex'),
    'selected_display_amount',to_char((p_facts->>'selected_display_amount')::numeric,'FM999999999999999990.00'),
    'selected_timesheet_count',scope.count,
    'selected_timesheet_ids',CASE WHEN scope.count<=25 THEN scope.ids ELSE '[]'::jsonb END,
    'selected_timesheet_scope_token',CASE WHEN scope.count>25 THEN
      private.pay_workbench_modal_cursor_encode_v2(p_binding || jsonb_build_object(
        'kind','SELECTED_READY_TIMESHEETS','candidate_id',p_facts->>'candidate_id','timesheet_hash',scope.timesheet_hash)) ELSE NULL END,
    'child_revision',(p_binding->>'session_version') || ':' || (p_binding->>'progress_counter_version') || ':' || (p_binding->>'scope_hash')
  ) FROM scope;
$function$;
ALTER FUNCTION private.pay_workbench_modal_candidate_row_v2(jsonb, jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_candidate_row_v2(jsonb, jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_candidate_page_v2(
  p_session public.banking_pay_workbench_sessions, p_options_json jsonb,
  p_sort_key text, p_sort_direction text, p_cursor text, p_limit integer
) RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE
  v_binding jsonb; v_cursor jsonb; v_page jsonb;
  v_id uuid; v_name text; v_reference text; v_amount numeric; v_deduction boolean;
  v_anchor boolean:=false; v_navigation text:='STAY';
BEGIN
  IF p_sort_key IS NULL OR p_sort_key NOT IN ('CANDIDATE','DEDUCTIONS','READY_TO_PAY')
     OR p_sort_direction IS NULL OR p_sort_direction NOT IN ('ASC','DESC')
     OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  v_binding := jsonb_build_object('contract','BANKING_PAY_MODAL_STRUCTURE_V2','kind','CANDIDATES',
    'session_id',p_session.id,'session_version',p_session.version,'progress_counter_version',p_session.progress_counter_version,
    'scope_hash',p_options_json->>'scope_hash','sort_key',p_sort_key,'sort_direction',p_sort_direction,'page_limit',p_limit);
  v_cursor := private.pay_workbench_modal_cursor_decode_v2(p_cursor,'{}'::jsonb);
  IF v_cursor->>'kind'='CANDIDATE_PAGE_ANCHOR' THEN
    -- A page anchor is read-position metadata, not a financial cursor or a
    -- selection authority. It can renew only within this same session/version,
    -- scope and sort, at a confirmed current revision. Normal page cursors
    -- below still require an exact progress revision.
    v_anchor:=true;
    v_cursor:=private.pay_workbench_modal_cursor_decode_v2(p_cursor,
      (v_binding-'progress_counter_version') || jsonb_build_object('kind','CANDIDATE_PAGE_ANCHOR'));
    IF COALESCE(v_cursor->>'progress_counter_version','') !~ '^[0-9]{1,16}$'
       OR (v_cursor->>'progress_counter_version')::bigint>p_session.progress_counter_version THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE='P0001';
    END IF;
    IF v_cursor ? 'navigation' THEN
      IF jsonb_typeof(v_cursor->'navigation') IS DISTINCT FROM 'string'
         OR v_cursor->>'navigation' NOT IN ('STAY','NEXT','PREVIOUS') THEN
        RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_CURSOR' USING ERRCODE='22023';
      END IF;
      v_navigation:=v_cursor->>'navigation';
    END IF;
  ELSE
    v_cursor:=private.pay_workbench_modal_cursor_decode_v2(p_cursor,v_binding);
    IF v_cursor ? 'navigation' THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_CURSOR' USING ERRCODE='22023';
    END IF;
  END IF;
  IF v_cursor IS NOT NULL THEN
    IF COALESCE(v_cursor->>'last_id','') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR jsonb_typeof(v_cursor->'last_name') IS DISTINCT FROM 'string'
       OR jsonb_typeof(v_cursor->'last_reference') IS DISTINCT FROM 'string'
       OR jsonb_typeof(v_cursor->'last_deduction') IS DISTINCT FROM 'boolean'
       OR COALESCE(v_cursor->>'last_amount','') !~ '^-?[0-9]{1,16}[.][0-9]{2}$' THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_CURSOR' USING ERRCODE='22023';
    END IF;
    v_id:=(v_cursor->>'last_id')::uuid; v_name:=v_cursor->>'last_name'; v_reference:=v_cursor->>'last_reference';
    v_deduction:=(v_cursor->>'last_deduction')::boolean; v_amount:=(v_cursor->>'last_amount')::numeric;
  END IF;
  WITH facts AS MATERIALIZED (
    SELECT * FROM private.pay_workbench_modal_candidate_facts_v2(p_session,p_options_json->>'pay_channel_scope')
  ), anchor_values AS (
    -- Use this candidate's CURRENT sort values when it survives. If it leaves
    -- Ready, continue from its former boundary and backfill with current rows.
    SELECT COALESCE(f.candidate_sort_name,v_name) AS name,
      COALESCE(f.candidate_sort_reference,v_reference) AS reference,
      COALESCE(f.selected_display_amount,v_amount) AS amount,
      COALESCE(f.selected_deduction_exists,v_deduction) AS deduction
    FROM (SELECT 1) seed LEFT JOIN facts f ON v_anchor AND f.candidate_id=v_id
  ), ranked AS MATERIALIZED (
    SELECT f.*,row_number() OVER (ORDER BY
      CASE WHEN p_sort_key='DEDUCTIONS' AND p_sort_direction='ASC' THEN f.selected_deduction_exists END ASC,
      CASE WHEN p_sort_key='DEDUCTIONS' AND p_sort_direction='DESC' THEN f.selected_deduction_exists END DESC,
      CASE WHEN p_sort_key='READY_TO_PAY' AND p_sort_direction='ASC' THEN f.selected_display_amount END ASC,
      CASE WHEN p_sort_key='READY_TO_PAY' AND p_sort_direction='DESC' THEN f.selected_display_amount END DESC,
      CASE WHEN p_sort_key='CANDIDATE' AND p_sort_direction='DESC' THEN f.candidate_sort_name END COLLATE "C" DESC,
      CASE WHEN NOT(p_sort_key='CANDIDATE' AND p_sort_direction='DESC') THEN f.candidate_sort_name END COLLATE "C" ASC,
      f.candidate_sort_reference COLLATE "C",f.candidate_id) AS page_order
    FROM facts AS f
  ), matching AS MATERIALIZED (
    SELECT f.* FROM ranked AS f CROSS JOIN anchor_values a
    WHERE v_id IS NULL OR (v_anchor AND f.candidate_id=v_id) OR CASE p_sort_key
      WHEN 'CANDIDATE' THEN
        (CASE WHEN p_sort_direction='ASC' THEN f.candidate_sort_name COLLATE "C">a.name COLLATE "C"
              ELSE f.candidate_sort_name COLLATE "C"<a.name COLLATE "C" END)
        OR (f.candidate_sort_name=a.name AND (f.candidate_sort_reference COLLATE "C",f.candidate_id)>(a.reference COLLATE "C",v_id))
      WHEN 'DEDUCTIONS' THEN
        (CASE WHEN p_sort_direction='ASC' THEN f.selected_deduction_exists>a.deduction ELSE f.selected_deduction_exists<a.deduction END)
        OR (f.selected_deduction_exists=a.deduction AND
          (f.candidate_sort_name COLLATE "C",f.candidate_sort_reference COLLATE "C",f.candidate_id)>(a.name COLLATE "C",a.reference COLLATE "C",v_id))
      WHEN 'READY_TO_PAY' THEN
        (CASE WHEN p_sort_direction='ASC' THEN f.selected_display_amount>a.amount ELSE f.selected_display_amount<a.amount END)
        OR (f.selected_display_amount=a.amount AND
          (f.candidate_sort_name COLLATE "C",f.candidate_sort_reference COLLATE "C",f.candidate_id)>(a.name COLLATE "C",a.reference COLLATE "C",v_id))
    END
  ), anchor_origin AS (
    -- Keep the anchor in its current, correctly aligned page. Alignment makes
    -- Previous/Next round trips deterministic even after rows move. A departed
    -- final page falls back to the last current page, never a false empty list.
    SELECT ((COALESCE((SELECT min(page_order) FROM matching),
      (SELECT max(page_order) FROM ranked),1)-1)/p_limit)*p_limit+1 AS first_order
  ), anchor_page AS (
    -- Direction is server-issued position metadata. Resolve the current page
    -- first; never revive an expired ordinary cursor or accept a client position.
    SELECT first_order+CASE v_navigation WHEN 'NEXT' THEN p_limit
      WHEN 'PREVIOUS' THEN -p_limit ELSE 0 END AS first_order FROM anchor_origin
  ), following AS (
    SELECT * FROM matching WHERE NOT v_anchor
    UNION ALL
    SELECT r.* FROM ranked r CROSS JOIN anchor_page a
      WHERE v_anchor AND a.first_order>=1 AND r.page_order>=a.first_order
  ), ordered AS MATERIALIZED (
    SELECT * FROM following ORDER BY page_order LIMIT (p_limit+1)
  ), limited AS MATERIALIZED (
    SELECT * FROM ordered ORDER BY page_order LIMIT p_limit
  ), totals AS (
    SELECT count(*) AS candidate_count,count(*) FILTER(WHERE selected_ready_count>0) AS selected_candidate_count,
      COALESCE(sum(selectable_ready_count),0)::bigint AS selectable_count,
      COALESCE(sum(selected_ready_count),0)::bigint AS selected_count,
      COALESCE(sum(selected_display_amount),0) AS amount
    FROM facts
  )
  SELECT jsonb_build_object(
    'view_digest',(SELECT encode(extensions.digest(convert_to(jsonb_build_object(
      'scope',p_options_json->>'scope_hash','facts',
      COALESCE(jsonb_agg(jsonb_build_array(f.candidate_id,
        encode(extensions.digest(convert_to(to_jsonb(f)::text,'UTF8'),'sha256'),'hex'))
        ORDER BY f.candidate_id),'[]'::jsonb))::text,'UTF8'),'sha256'),'hex') FROM facts f),
    'rows',COALESCE((SELECT jsonb_agg(private.pay_workbench_modal_candidate_row_v2(to_jsonb(f)-'page_order',
      v_binding-'kind'-'sort_key'-'sort_direction'-'page_limit') ORDER BY f.page_order) FROM limited AS f),'[]'::jsonb),
    'total_count',totals.candidate_count,'has_more',(SELECT count(*)>p_limit FROM ordered),
    'next_cursor',CASE WHEN (SELECT count(*)>p_limit FROM ordered) THEN (
      SELECT private.pay_workbench_modal_cursor_encode_v2(v_binding || jsonb_build_object(
        'last_id',f.candidate_id,'last_name',f.candidate_sort_name,'last_reference',f.candidate_sort_reference,
        'last_deduction',f.selected_deduction_exists,'last_amount',to_char(f.selected_display_amount,'FM999999999999999990.00')))
      FROM limited AS f ORDER BY page_order DESC LIMIT 1
    ) ELSE NULL END,
    'has_previous',COALESCE((SELECT min(page_order)>1 FROM limited),false),
    'page_number',COALESCE((SELECT ((min(page_order)-1)/p_limit)+1 FROM limited),0),
    'previous_cursor',(
      SELECT private.pay_workbench_modal_cursor_encode_v2(v_binding || jsonb_build_object(
        'last_id',f.candidate_id,'last_name',f.candidate_sort_name,'last_reference',f.candidate_sort_reference,
        'last_deduction',f.selected_deduction_exists,'last_amount',to_char(f.selected_display_amount,'FM999999999999999990.00')))
      FROM ranked AS f WHERE f.page_order=(SELECT min(page_order) FROM limited)-p_limit-1
    ),
    'page_anchor',(
      SELECT private.pay_workbench_modal_cursor_encode_v2(v_binding || jsonb_build_object('kind','CANDIDATE_PAGE_ANCHOR',
        'last_id',f.candidate_id,'last_name',f.candidate_sort_name,'last_reference',f.candidate_sort_reference,
        'last_deduction',f.selected_deduction_exists,'last_amount',to_char(f.selected_display_amount,'FM999999999999999990.00')))
      FROM limited AS f ORDER BY f.page_order LIMIT 1
    ),
    'next_page_anchor',CASE WHEN (SELECT count(*)>p_limit FROM ordered) THEN (
      SELECT private.pay_workbench_modal_cursor_encode_v2(v_binding || jsonb_build_object(
        'kind','CANDIDATE_PAGE_ANCHOR','navigation','NEXT',
        'last_id',f.candidate_id,'last_name',f.candidate_sort_name,'last_reference',f.candidate_sort_reference,
        'last_deduction',f.selected_deduction_exists,'last_amount',to_char(f.selected_display_amount,'FM999999999999999990.00')))
      FROM limited AS f ORDER BY f.page_order LIMIT 1
    ) ELSE NULL END,
    'previous_page_anchor',CASE WHEN COALESCE((SELECT min(page_order)>1 FROM limited),false) THEN (
      SELECT private.pay_workbench_modal_cursor_encode_v2(v_binding || jsonb_build_object(
        'kind','CANDIDATE_PAGE_ANCHOR','navigation','PREVIOUS',
        'last_id',f.candidate_id,'last_name',f.candidate_sort_name,'last_reference',f.candidate_sort_reference,
        'last_deduction',f.selected_deduction_exists,'last_amount',to_char(f.selected_display_amount,'FM999999999999999990.00')))
      FROM limited AS f ORDER BY f.page_order LIMIT 1
    ) ELSE NULL END,
    'ready_global',jsonb_build_object('candidate_count',totals.candidate_count,'selected_candidate_count',totals.selected_candidate_count,
      'selectable_ready_count',totals.selectable_count,'selected_ready_count',totals.selected_count,
      'selection_state',CASE WHEN totals.selected_count=0 THEN 'NONE' WHEN totals.selected_count=totals.selectable_count THEN 'ALL' ELSE 'SOME' END,
      'selected_ready_display_amount',to_char(totals.amount,'FM999999999999999990.00')),
    'sort_key',p_sort_key,'sort_direction',p_sort_direction,
    'cursor_identity_current',v_id IS NULL OR v_anchor OR EXISTS(SELECT 1 FROM facts AS f WHERE f.candidate_id=v_id
      AND f.candidate_sort_name=v_name AND f.candidate_sort_reference=v_reference
      AND f.selected_display_amount=v_amount AND f.selected_deduction_exists=v_deduction),
    'cursor_has_page',v_id IS NULL OR (v_anchor AND v_navigation='STAY') OR EXISTS(SELECT 1 FROM limited)
  ) INTO v_page FROM totals;
  IF v_page->>'cursor_identity_current'='false' OR v_page->>'cursor_has_page'='false' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE='P0001';
  END IF;
  RETURN v_page-'cursor_identity_current'-'cursor_has_page';
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_candidate_page_v2(public.banking_pay_workbench_sessions, jsonb, text, text, text, integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_candidate_page_v2(public.banking_pay_workbench_sessions, jsonb, text, text, text, integer) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_selected_ready_timesheets_v1(
  p_session_id uuid, p_candidate_id uuid, p_options_json jsonb, p_actor_user_id uuid, p_scope_token text
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO '' SET statement_timeout TO '3s' SET lock_timeout TO '1s'
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_binding jsonb; v_token jsonb; v_ids jsonb; v_hash text;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('PREVIEW_PROGRESS');
  v_session := private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
  IF p_candidate_id IS NULL OR p_scope_token IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  v_binding := jsonb_build_object('contract','BANKING_PAY_MODAL_STRUCTURE_V2','kind','SELECTED_READY_TIMESHEETS',
    'session_id',p_session_id,'candidate_id',p_candidate_id,'session_version',v_session.version,
    'progress_counter_version',v_session.progress_counter_version,'scope_hash',p_options_json->>'scope_hash');
  -- A different candidate's selection can advance progress without changing
  -- this shortcut's selected IDs. The request above must still be current.
  -- Only this read token can retain an older revision, and only after exact
  -- current membership equality below. Ordinary paging stays revision-strict.
  v_token := private.pay_workbench_modal_cursor_decode_v2(p_scope_token,v_binding-'progress_counter_version');
  IF jsonb_typeof(v_token->'progress_counter_version') IS DISTINCT FROM 'number'
     OR COALESCE(v_token->>'progress_counter_version','') !~ '^[0-9]{1,16}$' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE='P0001';
  END IF;
  IF (v_token->>'progress_counter_version')::bigint>v_session.progress_counter_version THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE='P0001';
  END IF;
  -- Exact current candidate + selected Ready membership. Never call a broad
  -- Timesheets/candidate search and never return a first-25 partial result.
  SELECT to_jsonb(facts.selected_timesheet_ids) INTO v_ids
  FROM private.pay_workbench_modal_candidate_facts_v2(v_session,p_options_json->>'pay_channel_scope') AS facts
  WHERE facts.candidate_id=p_candidate_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'BANKING_PAY_V2_CANDIDATE_NOT_CURRENT' USING ERRCODE='P0001'; END IF;
  v_hash := encode(extensions.digest(convert_to(v_ids::text,'UTF8'),'sha256'),'hex');
  IF v_token->>'timesheet_hash' IS DISTINCT FROM v_hash OR jsonb_array_length(v_ids)<=25 THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE='P0001';
  END IF;
  v_session := private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
  RETURN (v_binding - 'kind') || jsonb_build_object('ok',true,'contract_version',1,
    'timesheet_ids',v_ids,'timesheet_count',jsonb_array_length(v_ids));
END;
$function$;
ALTER FUNCTION public.pay_workbench_session_get_selected_ready_timesheets_v1(uuid, uuid, jsonb, uuid, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_get_selected_ready_timesheets_v1(uuid, uuid, jsonb, uuid, text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_get_selected_ready_timesheets_v1(uuid, uuid, jsonb, uuid, text) TO service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_candidate_ready_page_v1(
  p_session_id uuid, p_candidate_id uuid, p_options_json jsonb, p_actor_user_id uuid,
  p_cursor text DEFAULT NULL, p_limit integer DEFAULT 100
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO '' SET statement_timeout TO '3s' SET lock_timeout TO '1s'
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100);
  v_binding jsonb;
  v_cursor jsonb;
  v_last_id uuid;
  v_last_ordinal bigint;
  v_page jsonb;
  v_candidate jsonb;
  v_after public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_anchor boolean := false;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('PREVIEW_PROGRESS');
  v_session := private.pay_workbench_modal_context_v2(p_session_id, p_options_json, p_actor_user_id);
  IF p_candidate_id IS NULL OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO v_scope FROM public.banking_pay_workbench_session_scope
  WHERE session_id = p_session_id AND candidate_id = p_candidate_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_CANDIDATE_NOT_CURRENT' USING ERRCODE = 'P0001';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM private.pay_workbench_modal_source_progress_facts_v2(p_session_id,v_session.version) f
    WHERE f.candidate_id=p_candidate_id AND f.source_state='CURRENT') THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_NOT_READY' USING ERRCODE = 'P0001';
  END IF;
  v_binding := jsonb_build_object('contract', 'BANKING_PAY_MODAL_STRUCTURE_V2', 'kind', 'READY',
    'session_id', p_session_id, 'candidate_id', p_candidate_id, 'session_version', v_session.version,
    'progress_counter_version', v_session.progress_counter_version, 'scope_hash', p_options_json->>'scope_hash');
  -- The open child must retain complete, current candidate authority even when
  -- an amount/deduction sort moves that candidate off the visible main page.
  -- Reuse the exact summary projection; never sum this child page in a client.
  SELECT private.pay_workbench_modal_candidate_row_v2(to_jsonb(f),v_binding-'kind'-'candidate_id')
    INTO v_candidate
    FROM private.pay_workbench_modal_candidate_facts_v2(v_session,p_options_json->>'pay_channel_scope') f
    WHERE f.candidate_id=p_candidate_id;
  v_cursor := private.pay_workbench_modal_cursor_decode_v2(p_cursor, '{}'::jsonb);
  IF v_cursor->>'kind'='READY_PAGE_ANCHOR' THEN
    -- Read position only, under independently proved CURRENT request authority.
    -- Normal Ready cursors below remain bound to their exact progress version.
    v_anchor:=true;
    v_cursor:=private.pay_workbench_modal_cursor_decode_v2(p_cursor,
      (v_binding-'progress_counter_version') || jsonb_build_object('kind','READY_PAGE_ANCHOR','page_limit',v_limit));
    IF COALESCE(v_cursor->>'progress_counter_version','') !~ '^[0-9]{1,16}$'
      OR (v_cursor->>'progress_counter_version')::bigint>v_session.progress_counter_version THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE='P0001';
    END IF;
  ELSE
    v_cursor:=private.pay_workbench_modal_cursor_decode_v2(p_cursor,v_binding);
  END IF;
  IF v_cursor IS NOT NULL THEN
    IF COALESCE(v_cursor->>'last_id', '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR COALESCE(v_cursor->>'last_ordinal', '') !~ '^[0-9]{1,18}$' THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_CURSOR' USING ERRCODE = '22023';
    END IF;
    v_last_id := (v_cursor->>'last_id')::uuid;
    v_last_ordinal := (v_cursor->>'last_ordinal')::bigint;
  END IF;
  WITH scoped_rows AS MATERIALIZED (
    SELECT r.* FROM private.pay_workbench_modal_eligible_rows_v2(p_session_id, v_session.version, 'canonical_preview_lines') AS r
    WHERE r.candidate_id = p_candidate_id
      -- Ready context-only parents cannot keep a departed candidate in a child
      -- view. An unchecked candidate with eligible payments still has a fact.
      AND v_candidate IS NOT NULL
      -- The certified reader overlays the physical candidate identity. Apply
      -- that same identity before filtering, even when raw JSON omits it.
      AND private.pay_workbench_modal_row_matches_scope_v2(r.row_json || jsonb_build_object('candidate_id',r.candidate_id), v_session.filters_json,
        p_options_json->>'pay_channel_scope', 'canonical_preview_lines')
      AND NOT private.pay_workbench_modal_hidden_v2(r.row_json)
  ), ranked AS MATERIALIZED (
    SELECT r.id,r.row_ordinal,r::public.banking_pay_workbench_preview_rows AS source_row,
      row_number() OVER (ORDER BY r.row_ordinal,r.id) AS page_order FROM scoped_rows r
  ), anchor_page AS (
    -- Keep a surviving first row on its current aligned page. If it has moved
    -- out of Ready, backfill from the former key; never show a false empty last
    -- page while current Ready rows remain. No old amount/selection is reused.
    SELECT ((COALESCE((SELECT page_order FROM ranked WHERE id=v_last_id),
      (SELECT min(page_order) FROM ranked WHERE (row_ordinal,id)>(v_last_ordinal,v_last_id)),
      (SELECT max(page_order) FROM ranked),1)-1)/v_limit)*v_limit+1 AS first_order
  ), page_rows AS MATERIALIZED (
    SELECT r.* FROM ranked r CROSS JOIN anchor_page a
    WHERE (v_anchor AND r.page_order>=a.first_order)
      OR (NOT v_anchor AND (v_last_id IS NULL OR (r.row_ordinal,r.id)>(v_last_ordinal,v_last_id)))
    ORDER BY r.page_order LIMIT (v_limit + 1)
  ), limited_rows AS MATERIALIZED (
    SELECT r.* FROM page_rows AS r ORDER BY r.row_ordinal, r.id LIMIT v_limit
  )
  SELECT jsonb_build_object(
    'rows', COALESCE((SELECT jsonb_agg(private.pay_workbench_modal_row_payload_v2(r.source_row)
      || jsonb_build_object('identity', r.id::text) ORDER BY r.row_ordinal, r.id) FROM limited_rows AS r), '[]'::jsonb),
    'total_count', (SELECT count(*) FROM scoped_rows),
    'has_more', (SELECT count(*) > v_limit FROM page_rows),
    'next_cursor', CASE WHEN (SELECT count(*) > v_limit FROM page_rows) THEN (
      SELECT private.pay_workbench_modal_cursor_encode_v2(v_binding || jsonb_build_object('last_id', r.id, 'last_ordinal', r.row_ordinal))
      FROM limited_rows AS r ORDER BY r.row_ordinal DESC, r.id DESC LIMIT 1
    ) ELSE NULL END,
    'page_number',COALESCE((SELECT ((min(page_order)-1)/v_limit)+1 FROM limited_rows),0),
    'has_previous',COALESCE((SELECT min(page_order)>1 FROM limited_rows),false),
    'previous_cursor',(
      SELECT private.pay_workbench_modal_cursor_encode_v2(v_binding || jsonb_build_object('last_id',r.id,'last_ordinal',r.row_ordinal))
      FROM ranked r WHERE r.page_order=(SELECT min(page_order) FROM limited_rows)-v_limit-1
    ),
    'page_anchor',(
      SELECT private.pay_workbench_modal_cursor_encode_v2(v_binding || jsonb_build_object('kind','READY_PAGE_ANCHOR',
        'page_limit',v_limit,'last_id',r.id,'last_ordinal',r.row_ordinal))
      FROM limited_rows r ORDER BY r.page_order LIMIT 1
    ),
    'cursor_identity_current', v_last_id IS NULL OR v_anchor OR EXISTS (
      SELECT 1 FROM scoped_rows AS r WHERE r.id = v_last_id AND r.row_ordinal = v_last_ordinal
    )
  ) INTO v_page;
  IF v_page->>'cursor_identity_current' = 'false'
     OR ((v_page->>'total_count')::bigint>0 AND jsonb_array_length(v_page->'rows')=0) THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE = 'P0001';
  END IF;
  -- A read spanning a committed selection must not publish a mixed revision.
  v_after := private.pay_workbench_modal_context_v2(p_session_id, p_options_json, p_actor_user_id);
  v_page := (v_page - 'cursor_identity_current') || jsonb_build_object(
    'ok', true, 'contract', 'BANKING_PAY_MODAL_STRUCTURE_V2', 'contract_version', 1,
    'session_id', p_session_id, 'candidate_id', p_candidate_id,
    'candidate', v_candidate,
    'session_version', v_after.version, 'progress_counter_version', v_after.progress_counter_version,
    'scope_hash', p_options_json->>'scope_hash'
  );
  -- Direct opening and an already-open selection response share the same
  -- complete child allowance. Never trim financial/action detail to fit.
  IF octet_length(v_page::text)>512*1024 THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_READY_TOO_LARGE' USING ERRCODE='P0001';
  END IF;
  RETURN v_page;
END;
$function$;
ALTER FUNCTION public.pay_workbench_session_get_candidate_ready_page_v1(uuid, uuid, jsonb, uuid, text, integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_get_candidate_ready_page_v1(uuid, uuid, jsonb, uuid, text, integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_get_candidate_ready_page_v1(uuid, uuid, jsonb, uuid, text, integer) TO service_role;

NOTIFY pgrst, 'reload schema';

commit;
