-- Private presentation only. No provider, bank, selection or financial write.
-- Record-editor availability is supplied by the existing authorised UI adapter;
-- this private function is not a new permission or mutation authority.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_bank_issue_v2(
  p_row jsonb,p_facts jsonb,p_job jsonb,p_can_open_owner boolean
) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE
  v_meta jsonb:=private.pay_workbench_modal_bank_meta_v2(p_row,NULL);
  v_state text:='BLOCKED';v_code text:='BANKING_PAY_REQUIRES_REFRESH';v_action text;v_message text;
  v_blockers text[]:=private.pay_workbench_modal_bank_blockers_v2(p_row);
  v_name text:=UPPER(BTRIM(COALESCE(p_facts->>'name_check_status','')));
  v_current boolean:=COALESCE(p_facts->'target_is_current'='true'::jsonb,false);
  v_owner boolean:=COALESCE(p_facts->'owner_exists'='true'::jsonb AND p_facts->'owner_link_valid'='true'::jsonb,false);
  v_same_result boolean;
BEGIN
  IF jsonb_typeof(p_row) IS DISTINCT FROM 'object' OR private.pay_workbench_modal_hidden_v2(p_row) THEN RETURN NULL; END IF;
  v_same_result:=COALESCE(v_meta->>'name_check_status','')=v_name
    AND COALESCE(v_meta->'name_check_has_override','false'::jsonb)=COALESCE(p_facts->'override_current','false'::jsonb)
    AND COALESCE(v_meta->'payee_map_present','false'::jsonb)=COALESCE(p_facts->'mapping_present','false'::jsonb)
    AND (v_name='' OR (p_facts->'name_check_exists'='true'::jsonb AND NULLIF(p_facts->>'name_check_version','') IS NOT NULL));
  IF NOT v_owner THEN v_code:='BANK_TARGET_CHANGED';
  ELSIF 'BLOCKED_UMBRELLA_INACTIVE'=ANY(v_blockers) AND v_meta->>'payee_entity_kind'='UMBRELLA'
    AND p_facts->'umbrella_enabled'='false'::jsonb THEN
    v_code:='BLOCKED_UMBRELLA_INACTIVE';v_message:='MSG-064';
    IF p_can_open_owner IS TRUE THEN v_state:='ACTION_REQUIRED';v_action:='openUmbrella';END IF;
  ELSIF 'BLOCKED_BANK_DETAILS'=ANY(v_blockers) THEN
    v_code:='BLOCKED_BANK_DETAILS';
    v_message:=CASE WHEN v_meta->>'payee_entity_kind'='UMBRELLA' THEN 'MSG-067' ELSE 'MSG-066' END;
    IF p_can_open_owner IS TRUE THEN
      v_state:='ACTION_REQUIRED';v_action:=CASE WHEN v_meta->>'payee_entity_kind'='UMBRELLA' THEN 'openUmbrella' ELSE 'openCandidate' END;
    END IF;
  ELSIF NOT v_current THEN v_code:='BANK_TARGET_CHANGED';
  ELSIF p_job->'can_progress'='true'::jsonb THEN
    v_state:='UPDATING';v_code:='PAYEE_READINESS_PENDING';v_message:='MSG-060';
  ELSIF NULLIF(p_job->>'blocked_code','') IS NOT NULL THEN v_code:=p_job->>'blocked_code';v_message:='MSG-062';
  ELSIF v_same_result IS NOT TRUE THEN v_code:='BANK_RESULT_CHANGED';
  ELSE
    v_action:=private.pay_workbench_modal_bank_action_v2(v_meta);
    IF v_action IS NOT NULL THEN
      v_state:='ACTION_REQUIRED';
      v_code:=CASE v_action WHEN 'banking:pay:acceptBankDetails' THEN 'NAME_CHECK_REVIEW'
        WHEN 'banking:pay:runBankNameCheck' THEN 'NAME_CHECK_REQUIRED' ELSE 'PAYEE_MAP_REQUIRED' END;
      v_message:=CASE v_action WHEN 'banking:pay:acceptBankDetails' THEN
        CASE v_name WHEN 'NEAR_MATCH' THEN 'MSG-071' WHEN 'UNAVAILABLE' THEN 'MSG-072' ELSE 'MSG-073' END
        WHEN 'banking:pay:runBankNameCheck' THEN 'MSG-068' ELSE 'MSG-077' END;
    END IF;
    IF p_job->'is_failed'='true'::jsonb THEN v_code:='PAYEE_READINESS_FAILED';v_message:='MSG-062'; END IF;
  END IF;
  RETURN jsonb_build_object('state',v_state,'code',v_code,'action',v_action,'title_message_id',v_message,
    'bank_meta',v_meta,'name_check_version',p_facts->'name_check_version','mapping_version',p_facts->'mapping_version',
    'job_generation',p_job->'job_generation','job_id',p_job->'job_id');
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_bank_issue_v2(jsonb,jsonb,jsonb,boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_bank_issue_v2(jsonb,jsonb,jsonb,boolean) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_bank_issue_members_v2(
  p_session public.banking_pay_workbench_sessions,p_channel text,p_rail_provider text,p_rail_env text,p_can_open_owner boolean
) RETURNS TABLE(task_key text,candidate_id uuid,source_kind text,preview_row_id uuid,bank_row jsonb,source_payload jsonb,task_json jsonb)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
BEGIN
  RETURN QUERY
  WITH sources AS MATERIALIZED (
    SELECT s.*,jsonb_build_object('candidate_id',s.candidate_id,'entity_kind',s.bank_row->>'payee_entity_kind',
      'entity_id',s.bank_row->>'payee_entity_id','bank_details_hash',s.bank_row->>'bank_details_hash') AS target
    FROM private.pay_workbench_modal_bank_sources_v2(p_session,p_channel) s
  ), targets AS MATERIALIZED (
    SELECT COALESCE(jsonb_agg(DISTINCT s.target),'[]'::jsonb) AS items FROM sources s
  ), facts AS MATERIALIZED (
    SELECT f.* FROM targets t CROSS JOIN LATERAL private.pay_workbench_modal_bank_target_facts_v2(t.items,p_rail_provider,p_rail_env) f
  ), jobs AS MATERIALIZED (
    SELECT j.*,j.target-'candidate_id' AS owner FROM targets t
    CROSS JOIN LATERAL private.pay_workbench_modal_bank_job_facts_v2(p_session,t.items,p_rail_provider,p_rail_env) j
    -- Only a currently linked candidate may contribute a shared-owner job.
    JOIN facts f ON f.target=j.target AND f.facts->'target_is_current'='true'::jsonb
    WHERE j.job_facts IS NOT NULL
  ), shared_jobs AS MATERIALIZED (
    SELECT DISTINCT ON(j.owner) j.owner,j.job_facts FROM jobs j
    ORDER BY j.owner,CASE WHEN j.job_facts->'can_progress'='true'::jsonb THEN 0
      WHEN j.job_facts->'is_failed'='true'::jsonb THEN 2 ELSE 1 END,
      j.job_facts->>'job_generation',j.job_facts->>'job_id'
  ), classified AS MATERIALIZED (
    SELECT s.*,CASE WHEN f.facts->'owner_exists'='true'::jsonb AND f.facts->'owner_link_valid'='true'::jsonb
        THEN s.target-'candidate_id' ELSE s.target END AS owner_key,
      private.pay_workbench_modal_bank_issue_v2(s.bank_row,f.facts,j.job_facts,p_can_open_owner) AS issue
    FROM sources s JOIN facts f ON f.target=s.target
    LEFT JOIN shared_jobs j ON j.owner=s.target-'candidate_id'
  )
  SELECT encode(extensions.digest(convert_to(jsonb_build_array('BANK_ACCOUNT',p_session.id,p_session.version,
      p_rail_provider,p_rail_env,c.owner_key,c.issue->'name_check_version',c.issue->'mapping_version',c.issue->'job_generation')::text,'UTF8'),'sha256'),'hex'),
    c.candidate_id,c.source_kind,c.preview_row_id,c.bank_row,c.source_payload,
    c.issue || jsonb_build_object('task_family','BANK_ACCOUNT','target',c.target,'source_ordinal',c.source_ordinal)
  FROM classified c WHERE c.issue IS NOT NULL;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_bank_issue_members_v2(public.banking_pay_workbench_sessions,text,text,text,boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_bank_issue_members_v2(public.banking_pay_workbench_sessions,text,text,text,boolean) FROM PUBLIC, anon, authenticated, service_role;

-- Every visible current payment for a affected candidate remains reachable in
-- task detail. An exact payee route marks affected payments; other payments are
-- explicitly context, never silently assigned to that bank account or summed.
CREATE OR REPLACE FUNCTION private.pay_workbench_modal_bank_task_members_v2(
  p_session public.banking_pay_workbench_sessions,p_channel text,p_rail_provider text,p_rail_env text,p_can_open_owner boolean
) RETURNS TABLE(task_key text,task_state text,candidate_id uuid,source_kind text,preview_row_id uuid,
  source_payload jsonb,bank_row jsonb,context_only boolean,affected_by_task boolean,task_json jsonb)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
BEGIN
  RETURN QUERY
  WITH issues AS MATERIALIZED (
    SELECT s.* FROM private.pay_workbench_modal_bank_issue_members_v2(p_session,p_channel,p_rail_provider,p_rail_env,p_can_open_owner) s
  ), groups AS MATERIALIZED (
    SELECT i.task_key,CASE WHEN bool_or(i.task_json->>'state'='UPDATING') THEN 'UPDATING'
      WHEN bool_or(i.task_json->>'state'='ACTION_REQUIRED') THEN 'ACTION_REQUIRED' ELSE 'BLOCKED' END AS state
    FROM issues i GROUP BY i.task_key
  ), candidates AS MATERIALIZED (
    SELECT DISTINCT i.task_key,i.candidate_id,i.task_json->'target' AS target FROM issues i
  ), metadata AS MATERIALIZED (
    SELECT c.candidate_id,private.pay_workbench_modal_candidate_bank_meta_v2(
      COALESCE(s.effective_candidate_fragment_json,'{}'::jsonb)||jsonb_build_object('candidate_id',c.candidate_id)) AS meta
    FROM (SELECT DISTINCT i.candidate_id FROM issues i) c
    LEFT JOIN public.banking_pay_workbench_session_candidate_state s
      ON s.session_id=p_session.id AND s.session_version=p_session.version AND s.candidate_id=c.candidate_id
  ), payments AS MATERIALIZED (
    SELECT r.candidate_id,r.id,private.pay_workbench_modal_row_payload_v2(r) AS payload,
      section.name AS section,m.meta
    FROM (VALUES ('canonical_preview_lines'),('cases_resolutions'),('blocked_for_pay')) section(name)
    CROSS JOIN LATERAL private.pay_workbench_modal_eligible_rows_v2(p_session.id,p_session.version,section.name) r
    JOIN metadata m ON m.candidate_id=r.candidate_id
    WHERE NOT private.pay_workbench_modal_hidden_v2(r.row_json)
      AND private.pay_workbench_modal_row_matches_scope_v2(r.row_json||jsonb_build_object('candidate_id',r.candidate_id),
        p_session.filters_json,p_channel,section.name)
  ), routed AS MATERIALIZED (
    SELECT p.*,private.pay_workbench_modal_payee_route_v2(p.payload,
      private.pay_workbench_modal_bank_meta_v2(p.payload,p.meta)) AS route FROM payments p
  ), contexts AS (
    SELECT c.task_key,c.candidate_id,r.id,r.payload,
      (NULLIF(r.route->>'entity_kind','') IS NOT NULL AND NULLIF(r.route->>'entity_id','') IS NOT NULL
        AND NULLIF(r.route->>'route_bank_hash','') IS NOT NULL) AS route_known,
      (NULLIF(c.target->>'entity_kind','') IS NOT NULL AND NULLIF(c.target->>'entity_id','') IS NOT NULL
        AND NULLIF(c.target->>'bank_details_hash','') IS NOT NULL
        AND c.target->>'entity_kind'=r.route->>'entity_kind'
        AND c.target->>'entity_id'=r.route->>'entity_id'
        AND c.target->>'bank_details_hash'=r.route->>'route_bank_hash') AS exact_route
    FROM candidates c JOIN routed r ON r.candidate_id=c.candidate_id
    WHERE NOT EXISTS(SELECT 1 FROM issues i WHERE i.task_key=c.task_key AND i.preview_row_id=r.id)
  )
  SELECT i.task_key,g.state,i.candidate_id,i.source_kind,i.preview_row_id,i.source_payload,i.bank_row,
    (i.task_json->>'state'<>g.state OR i.source_payload->>'effective_section'='canonical_preview_lines') IS TRUE,
    i.preview_row_id IS NOT NULL,i.task_json
  FROM issues i JOIN groups g ON g.task_key=i.task_key
  UNION ALL
  SELECT c.task_key,g.state,c.candidate_id,'PREVIEW_ROW'::text,c.id,c.payload,NULL::jsonb,true,COALESCE(c.exact_route,false),
    jsonb_build_object('task_family','BANK_ACCOUNT','context_only',true,'action',NULL,
      'payment_membership_known',c.route_known)
  FROM contexts c JOIN groups g ON g.task_key=c.task_key;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_bank_task_members_v2(public.banking_pay_workbench_sessions,text,text,text,boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_bank_task_members_v2(public.banking_pay_workbench_sessions,text,text,text,boolean) FROM PUBLIC, anon, authenticated, service_role;

commit;
