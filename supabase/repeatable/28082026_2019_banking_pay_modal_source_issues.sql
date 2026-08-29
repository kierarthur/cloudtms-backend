-- Complete source-issue membership, including sources that have not published
-- a payment yet. Existing progress/job owners supply state; no job is executed.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_source_issue_members_v2(
  p_session public.banking_pay_workbench_sessions,p_channel text,p_progress jsonb,p_can_refresh boolean
) RETURNS TABLE(task_key text,candidate_id uuid,source_kind text,preview_row_id uuid,source_payload jsonb,task_json jsonb)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
BEGIN
  IF p_channel IS NULL OR p_channel NOT IN ('ALL','PAYE','UMBRELLA') OR jsonb_typeof(p_progress) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  -- Most open sessions have no stale/pending source. The same current-source
  -- owner proves that this entire result is empty before any three-section
  -- payment/detail expansion. STABLE uses one snapshot; no fact is inferred
  -- from a loaded page or from the browser.
  IF NOT EXISTS (
    SELECT 1 FROM private.pay_workbench_modal_source_progress_facts_v2(p_session.id,p_session.version) f
    WHERE f.source_state<>'CURRENT'
  ) THEN RETURN; END IF;
  RETURN QUERY
  WITH facts AS MATERIALIZED (
    SELECT f.*,COALESCE(cs.effective_candidate_fragment_json,'{}'::jsonb) AS fragment,
      jsonb_build_object('candidate_id',f.candidate_id,'candidate_name',COALESCE(NULLIF(BTRIM(c.display_name),''),c.tms_ref,c.id::text),
        'candidate_reference',COALESCE(c.tms_ref,''),'pay_channel',c.pay_method) AS candidate_meta
    FROM private.pay_workbench_modal_source_progress_facts_v2(p_session.id,p_session.version) f
    JOIN public.candidates c ON c.id=f.candidate_id
    LEFT JOIN public.banking_pay_workbench_session_candidate_state cs
      ON cs.session_id=p_session.id AND cs.candidate_id=f.candidate_id AND cs.session_version=p_session.version
    WHERE f.source_state<>'CURRENT'
      AND NOT private.pay_workbench_modal_hidden_v2(cs.effective_candidate_fragment_json)
  ), rows AS MATERIALIZED (
    SELECT r.candidate_id,r.id,private.pay_workbench_modal_row_payload_v2(r) AS payload
    FROM (VALUES ('canonical_preview_lines'),('cases_resolutions'),('blocked_for_pay')) section(name)
    CROSS JOIN LATERAL private.pay_workbench_modal_eligible_rows_v2(p_session.id,p_session.version,section.name) r
    JOIN facts f ON f.candidate_id=r.candidate_id
    WHERE NOT private.pay_workbench_modal_hidden_v2(r.row_json)
      AND private.pay_workbench_modal_row_matches_scope_v2(r.row_json||jsonb_build_object('candidate_id',r.candidate_id),
        p_session.filters_json,p_channel,section.name)
  ), visible AS MATERIALIZED (
    SELECT f.* FROM facts f WHERE EXISTS(SELECT 1 FROM rows r WHERE r.candidate_id=f.candidate_id)
      OR (NOT EXISTS(SELECT 1 FROM public.banking_pay_workbench_preview_rows r WHERE r.session_id=p_session.id
          AND r.session_version=p_session.version AND r.candidate_id=f.candidate_id)
        AND private.pay_workbench_modal_row_matches_scope_v2(f.candidate_meta||f.fragment||jsonb_build_object('candidate_id',f.candidate_id),
          p_session.filters_json,p_channel,'blocked_for_pay'))
  ), jobs AS MATERIALIZED (
    SELECT j.* FROM private.pay_workbench_modal_stage_job_facts_v2(p_session) j
  ), choices AS MATERIALIZED (
    SELECT DISTINCT ON(f.candidate_id) f.*,j.job_facts
    FROM visible f LEFT JOIN jobs j ON j.candidate_id=f.candidate_id
      OR (j.candidate_id IS NULL AND (
        (j.job_type='WORKBENCH_SESSION_SCOPE_SEED' AND NOT f.seeded)
        OR j.job_type IN ('WORKBENCH_SESSION_CLONE_REBASE','WORKBENCH_CANDIDATE_LINE_WORK_PROCESS','WORKBENCH_PREVIEW_ROWS_MATERIALISE')))
    ORDER BY f.candidate_id,CASE WHEN j.job_facts->'can_progress'='true'::jsonb THEN 0
      WHEN j.job_facts->'is_failed'='true'::jsonb THEN 1 ELSE 2 END,
      CASE WHEN j.candidate_id IS NOT NULL THEN 0 ELSE 1 END,j.job_facts->>'job_generation',j.job_facts->>'job_id'
  ), states AS MATERIALIZED (
    SELECT f.*,
      CASE WHEN NOT f.recovery_required AND f.job_facts->'can_progress'='true'::jsonb
          -- SOURCE_BUILD_PENDING keeps its exact existing owner/successor.
          AND (f.scope_status<>'SOURCE_BUILD_PENDING' OR f.job_facts->>'job_id'=COALESCE(f.successor_job_id,f.pending_job_id)::text)
        THEN 'UPDATING'
        WHEN p_can_refresh IS TRUE AND p_progress->>'next_recommended_action' IN ('REFRESH_OR_RETRY','RETRY_OR_REFRESH','OPEN_NEW_SESSION')
        THEN 'ACTION_REQUIRED' ELSE 'BLOCKED' END AS issue_state
    FROM choices f
  ), tasks AS MATERIALIZED (
    SELECT f.*,
      encode(extensions.digest(convert_to(jsonb_build_array('SOURCE_PROGRESS',p_session.id,p_session.version,
        p_session.progress_counter_version,f.issue_state,
        CASE WHEN f.issue_state='ACTION_REQUIRED' THEN 'REFRESH_WORKBENCH'
          WHEN f.issue_state='UPDATING' THEN f.job_facts->>'job_id' ELSE f.candidate_id::text END)::text,'UTF8'),'sha256'),'hex') AS issue_key,
      jsonb_build_object('task_family','SOURCE_PROGRESS','state',f.issue_state,
        'code',CASE WHEN f.issue_state='UPDATING' THEN 'SOURCE_PROGRESS_PENDING'
          WHEN f.recovery_required THEN COALESCE(f.owner_failure_reason,'SOURCE_REFRESH_REQUIRED')
          WHEN f.source_failed OR f.job_facts->'is_failed'='true'::jsonb THEN 'SOURCE_REFRESH_FAILED'
          ELSE COALESCE(f.job_facts->>'blocked_code','SOURCE_REFRESH_REQUIRED') END,
        'title',CASE WHEN f.issue_state='UPDATING' THEN 'Refreshing…'
          WHEN f.source_failed OR f.job_facts->'is_failed'='true'::jsonb THEN 'Refresh failed' ELSE 'Payment preview needs refreshing.' END,
        'action',CASE WHEN f.issue_state='ACTION_REQUIRED' THEN 'banking:pay:refreshAll' END,
        'job',f.job_facts,'affected_payment_count_complete',false) AS issue
    FROM states f
  )
  SELECT f.issue_key,f.candidate_id,'SOURCE_PROGRESS'::text,NULL::uuid,
    f.candidate_meta||jsonb_build_object('source_progress',jsonb_build_object('scope_id',f.scope_id,'scope_status',f.scope_status,
      'source_state',f.source_state,'publication_current',f.publication_current,'source_failed',f.source_failed,
      'recovery_required',f.recovery_required,'recovery_scheduled',f.recovery_scheduled,'owner_failure_reason',f.owner_failure_reason)),
    f.issue||jsonb_build_object('context_only',false)
  FROM tasks f
  UNION ALL
  SELECT f.issue_key,f.candidate_id,'PREVIEW_ROW'::text,r.id,r.payload,
    f.issue||jsonb_build_object('context_only',true)
  FROM tasks f JOIN rows r ON r.candidate_id=f.candidate_id;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_source_issue_members_v2(public.banking_pay_workbench_sessions,text,jsonb,boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_source_issue_members_v2(public.banking_pay_workbench_sessions,text,jsonb,boolean) FROM PUBLIC, anon, authenticated, service_role;

commit;
