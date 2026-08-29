-- Current bank-target/readiness identity facts only. Reuse the existing exact
-- owner/hash and one-off edit gate; do not return bank fields or grant actions.
\set ON_ERROR_STOP on
begin;
CREATE OR REPLACE FUNCTION private.pay_workbench_modal_bank_target_facts_v2(
  p_targets jsonb,p_rail_provider text,p_rail_env text
) RETURNS TABLE(target jsonb,facts jsonb)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
BEGIN
  IF jsonb_typeof(p_targets) IS DISTINCT FROM 'array'
    OR NULLIF(BTRIM(p_rail_provider),'') IS NULL OR NULLIF(BTRIM(p_rail_env),'') IS NULL
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(p_targets) item WHERE jsonb_typeof(item)<>'object') THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(p_targets) item CROSS JOIN LATERAL jsonb_object_keys(item) k
    WHERE k NOT IN ('candidate_id','entity_kind','entity_id','bank_details_hash')) THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  RETURN QUERY
  WITH requested AS MATERIALIZED (
    SELECT DISTINCT item AS target,
      CASE WHEN item->>'candidate_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (item->>'candidate_id')::uuid END AS candidate_id,
      UPPER(BTRIM(COALESCE(item->>'entity_kind',''))) AS kind,
      CASE WHEN item->>'entity_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (item->>'entity_id')::uuid END AS entity_id,
      BTRIM(COALESCE(item->>'bank_details_hash','')) AS bank_hash
    FROM jsonb_array_elements(p_targets) item
  ), oneoff AS MATERIALIZED (
    SELECT DISTINCT o.candidate_id,o.bank_details_hash
    FROM requested t
    JOIN public.pay_finance_case_oneoff_payout_bank_details o
      ON t.kind='CANDIDATE' AND t.candidate_id=t.entity_id
      AND o.candidate_id=t.entity_id AND o.bank_details_hash=t.bank_hash AND t.bank_hash<>''
    JOIN public.v_finance_cases_register r
      ON r.finance_case_id=o.finance_case_id AND r.candidate_id=o.candidate_id
      AND r.routing_kind::text='ONE_OFF_SPECIFIED_BANK_ACCOUNT' AND r.edit_bank_details_allowed IS TRUE
  ), joined AS MATERIALIZED (
    SELECT t.target,t.kind,t.entity_id,t.bank_hash,
      CASE WHEN t.kind='CANDIDATE' THEN c.id IS NOT NULL
        WHEN t.kind='UMBRELLA' THEN u.id IS NOT NULL ELSE false END AS owner_exists,
      COALESCE(CASE WHEN t.kind='CANDIDATE' THEN t.candidate_id=c.id
        WHEN t.kind='UMBRELLA' THEN linked.umbrella_id=u.id ELSE false END,false) AS owner_link_valid,
      CASE WHEN t.kind='CANDIDATE' AND NULLIF(BTRIM(c.bank_details_hash),'')=t.bank_hash THEN 'CANDIDATE_CURRENT'
        WHEN t.kind='CANDIDATE' AND o.candidate_id IS NOT NULL THEN 'CANDIDATE_ONEOFF_PAYOUT'
        WHEN t.kind='UMBRELLA' AND NULLIF(BTRIM(u.bank_details_hash),'')=t.bank_hash THEN 'UMBRELLA_CURRENT' END AS target_source,
      u.enabled AS umbrella_enabled,
      n.id IS NOT NULL AS name_check_exists,n.status AS name_check_status,
      COALESCE(n.override_reason IS NOT NULL AND n.override_hash=t.bank_hash,false) AS override_current,
      CASE WHEN n.id IS NOT NULL THEN encode(extensions.digest(convert_to(jsonb_build_array(n.id,n.updated_at_utc,
        n.checked_at_utc,n.status,n.override_hash,n.override_reason IS NOT NULL)::text,'UTF8'),'sha256'),'hex') END AS name_check_version,
      m.payee_id IS NOT NULL AS mapping_present,
      CASE WHEN m.id IS NOT NULL THEN encode(extensions.digest(convert_to(jsonb_build_array(m.id,m.updated_at_utc)::text,'UTF8'),'sha256'),'hex') END AS mapping_version
    FROM requested t
    LEFT JOIN public.candidates linked ON linked.id=t.candidate_id
    LEFT JOIN public.candidates c ON t.kind='CANDIDATE' AND c.id=t.entity_id
    LEFT JOIN public.umbrellas u ON t.kind='UMBRELLA' AND u.id=t.entity_id
    LEFT JOIN oneoff o ON t.kind='CANDIDATE' AND o.candidate_id=t.entity_id AND o.bank_details_hash=t.bank_hash
    LEFT JOIN public.bank_name_checks n ON n.rail_provider=p_rail_provider AND n.rail_env=p_rail_env
      AND n.entity_kind=t.kind AND n.entity_id=t.entity_id AND n.bank_details_hash=t.bank_hash
    LEFT JOIN public.bank_payee_map m ON m.rail_provider=p_rail_provider AND m.rail_env=p_rail_env
      AND m.entity_kind=t.kind AND m.entity_id=t.entity_id AND m.bank_details_hash=t.bank_hash
  )
  SELECT j.target,jsonb_build_object('rail_provider',p_rail_provider,'rail_env',p_rail_env,
    'owner_exists',j.owner_exists,'owner_link_valid',j.owner_link_valid,
    'target_is_current',j.owner_link_valid AND j.target_source IS NOT NULL,
    'target_source',CASE WHEN j.owner_link_valid THEN j.target_source END,
    'umbrella_enabled',j.umbrella_enabled,'name_check_exists',j.name_check_exists,
    'name_check_status',j.name_check_status,'override_current',j.override_current,
    'name_check_version',j.name_check_version,'mapping_present',j.mapping_present,'mapping_version',j.mapping_version)
  FROM joined j;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_bank_target_facts_v2(jsonb,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_bank_target_facts_v2(jsonb,text,text) FROM PUBLIC, anon, authenticated, service_role;
commit;
