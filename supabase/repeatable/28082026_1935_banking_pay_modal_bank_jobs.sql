-- Read-only current PAYEE_READINESS_ENSURE evidence. Preview status strings are
-- not job ownership. Existing enqueue/fail/executor owners remain unchanged.
\set ON_ERROR_STOP on
\ir 28082026_1657_banking_pay_modal_payee_readiness_projection.sql
begin;
CREATE OR REPLACE FUNCTION private.pay_workbench_modal_bank_job_facts_v2(
 p_session public.banking_pay_workbench_sessions,p_targets jsonb,p_rail_provider text,p_rail_env text
) RETURNS TABLE(target jsonb,job_facts jsonb)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
BEGIN
 IF jsonb_typeof(p_targets) IS DISTINCT FROM 'array'
   OR NULLIF(BTRIM(p_rail_provider),'') IS NULL OR NULLIF(BTRIM(p_rail_env),'') IS NULL
   OR EXISTS(SELECT 1 FROM jsonb_array_elements(p_targets) i WHERE jsonb_typeof(i)<>'object') THEN
   RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
 END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(p_targets) i CROSS JOIN LATERAL jsonb_object_keys(i) k
   WHERE k NOT IN ('candidate_id','entity_kind','entity_id','bank_details_hash')) THEN
   RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
 END IF;
 IF p_session.id IS NULL OR p_session.status<>'OPEN' OR p_session.discarded_at_utc IS NOT NULL
   OR p_session.replacement_session_id IS NOT NULL THEN RAISE EXCEPTION 'OBSOLETE_SESSION' USING ERRCODE='P0001'; END IF;
 RETURN QUERY
 WITH targets AS MATERIALIZED (
   SELECT DISTINCT i AS target,i->>'candidate_id' AS candidate_id,
     UPPER(BTRIM(COALESCE(i->>'entity_kind',''))) AS kind,
     BTRIM(COALESCE(i->>'entity_id','')) AS entity_id,BTRIM(COALESCE(i->>'bank_details_hash','')) AS bank_hash
   FROM jsonb_array_elements(p_targets) i
 ), jobs AS MATERIALIZED (
   SELECT j.*,owner_candidate.umbrella_id AS current_umbrella_id,UPPER(BTRIM(j.status)) AS status_key,
     CASE WHEN COALESCE(j.payload_json->>'source_change_seq','')~'^[0-9]{1,18}$'
       THEN (j.payload_json->>'source_change_seq')::bigint END AS source_seq,
     CASE WHEN j.payload_json ? 'payees_json' THEN
       CASE WHEN jsonb_typeof(j.payload_json->'payees_json')='array' THEN j.payload_json->'payees_json' ELSE '[]'::jsonb END
       ELSE CASE WHEN jsonb_typeof(j.payload_json->'payees')='array' THEN j.payload_json->'payees' ELSE '[]'::jsonb END END AS payees
   FROM public.banking_pay_workbench_jobs j
   LEFT JOIN public.candidates owner_candidate ON owner_candidate.id=j.candidate_id
   WHERE j.session_id=p_session.id AND j.snapshot_run_id IS NOT DISTINCT FROM p_session.source_snapshot_run_id
     AND j.job_type='PAYEE_READINESS_ENSURE' AND UPPER(BTRIM(j.status)) IN ('QUEUED','RUNNING','FAILED','DEAD')
     AND j.completed_at_utc IS NULL
     AND j.payload_json->>'session_id'=p_session.id::text
     AND j.payload_json->>'candidate_id'=j.candidate_id::text
     AND CASE WHEN COALESCE(j.payload_json->>'session_version','')~'^[0-9]{1,18}$'
       THEN (j.payload_json->>'session_version')::bigint END=p_session.version
     -- Exact existing executor contract: REVOLUT and rail_env/railEnv, default
     -- PROD. These are job-routing facts, not permission to call any provider.
     AND p_rail_provider='REVOLUT'
     AND COALESCE(NULLIF(UPPER(private.pay_workbench_modal_route_text_v2(ARRAY[
       j.payload_json->'rail_env',j.payload_json->'railEnv','"PROD"'::jsonb])),''),'PROD')=p_rail_env
     AND EXISTS(SELECT 1 FROM targets t WHERE t.candidate_id=j.candidate_id::text
       OR (t.kind='UMBRELLA' AND t.entity_id=owner_candidate.umbrella_id::text))
 ), current_jobs AS MATERIALIZED (
   SELECT j.*,jsonb_array_length(j.payees) AS payee_count
   FROM jobs j LEFT JOIN public.app_change_counters c ON c.entity_key='pay_candidate:'||j.candidate_id::text
   WHERE j.source_seq>=COALESCE(c.seq,0)
     AND j.payload_json->>'readiness_fingerprint'=md5(j.payees::text)
     AND NOT EXISTS(SELECT 1 FROM public.banking_pay_workbench_jobs completed
       WHERE completed.id<>j.id AND completed.job_type='PAYEE_READINESS_ENSURE'
         AND completed.dedupe_key=j.dedupe_key AND completed.completed_at_utc IS NOT NULL AND completed.failed_at_utc IS NULL)
 ), matched AS MATERIALIZED (
   SELECT DISTINCT t.target,j.id,j.status_key,j.source_seq,j.updated_at_utc,j.created_at_utc,
     j.attempt_count,j.failed_at_utc,j.payee_count
   FROM current_jobs j
   CROSS JOIN LATERAL jsonb_array_elements(j.payees) item
   CROSS JOIN LATERAL (SELECT private.pay_workbench_modal_payee_route_v2(item,NULL) AS route) r
   -- A current operation on a shared umbrella account still applies when the
   -- candidate that started it is outside the display filter. Its original
   -- candidate/source/fingerprint and current umbrella link remain mandatory.
   JOIN targets t ON (t.candidate_id=j.candidate_id::text
       OR (t.kind='UMBRELLA' AND t.entity_id=j.current_umbrella_id::text)) AND t.kind=r.route->>'entity_kind'
     AND t.entity_id=r.route->>'entity_id' AND t.bank_hash<>'' AND t.bank_hash=r.route->>'route_bank_hash'
   WHERE COALESCE(NULLIF(r.route->>'candidate_id',''),j.candidate_id::text)=j.candidate_id::text
 ), effective AS (
   SELECT DISTINCT ON(m.target) m.* FROM matched m
   ORDER BY m.target,
     CASE WHEN m.status_key IN ('QUEUED','RUNNING') AND m.failed_at_utc IS NULL AND m.payee_count BETWEEN 1 AND 25 THEN 0
       WHEN m.status_key IN ('QUEUED','RUNNING') THEN 1 ELSE 2 END,
     m.source_seq DESC,m.updated_at_utc DESC,m.created_at_utc DESC,m.id DESC
 )
 SELECT t.target,CASE WHEN e.id IS NOT NULL THEN jsonb_build_object(
   'job_id',e.id,'job_type','PAYEE_READINESS_ENSURE','status',e.status_key,
   'source_change_seq',e.source_seq,'session_version',p_session.version,
   'is_failed',e.status_key IN ('FAILED','DEAD'),
   'can_progress',e.status_key IN ('QUEUED','RUNNING') AND e.failed_at_utc IS NULL AND e.payee_count BETWEEN 1 AND 25,
   'blocked_code',CASE WHEN e.payee_count>25 THEN 'READINESS_JOB_UNIT_TOO_LARGE' END,
   'job_generation',encode(extensions.digest(convert_to(jsonb_build_array(e.id,e.source_seq,p_session.version,
     e.status_key,e.updated_at_utc,e.failed_at_utc,e.attempt_count)::text,'UTF8'),'sha256'),'hex')) END
 FROM targets t LEFT JOIN effective e ON e.target=t.target;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_bank_job_facts_v2(public.banking_pay_workbench_sessions,jsonb,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_bank_job_facts_v2(public.banking_pay_workbench_sessions,jsonb,text,text) FROM PUBLIC, anon, authenticated, service_role;
commit;
