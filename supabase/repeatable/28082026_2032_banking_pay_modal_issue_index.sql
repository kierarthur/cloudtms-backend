-- Compact complete issue references for counts and list pagination. Detail
-- expands only the requested key through the same certified member readers.
-- This index contains no payment arithmetic and grants no new action.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_issue_index_v2(
  p_session public.banking_pay_workbench_sessions,p_channel text,p_rail_provider text,p_rail_env text,
  p_progress jsonb,p_can_open_owner boolean,p_can_refresh boolean
) RETURNS TABLE(identity text,issue_state text,task_family text,task_key text,candidate_id uuid,
  source_kind text,preview_row_id uuid,source_ordinal bigint)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
BEGIN
  RETURN QUERY
  WITH source_members AS MATERIALIZED (
    SELECT s.* FROM private.pay_workbench_modal_source_issue_members_v2(p_session,p_channel,p_progress,p_can_refresh) s
    WHERE s.source_kind='SOURCE_PROGRESS'
  ), finance_all AS MATERIALIZED (
    SELECT f.* FROM private.pay_workbench_modal_finance_task_members_v2(p_session,p_channel) f
  ), finance_members AS MATERIALIZED (
    SELECT f.* FROM finance_all f WHERE f.task_json->'context_only' IS DISTINCT FROM 'true'::jsonb
  ), bank_members AS MATERIALIZED (
    SELECT b.* FROM private.pay_workbench_modal_bank_issue_members_v2(p_session,p_channel,p_rail_provider,p_rail_env,p_can_open_owner) b
  ), bank_groups AS MATERIALIZED (
    SELECT b.task_key,CASE WHEN bool_or(b.task_json->>'state'='UPDATING') THEN 'UPDATING'
      WHEN bool_or(b.task_json->>'state'='ACTION_REQUIRED') THEN 'ACTION_REQUIRED' ELSE 'BLOCKED' END AS state
    FROM bank_members b GROUP BY b.task_key
  ), source_current AS MATERIALIZED (
    SELECT s.candidate_id FROM private.pay_workbench_modal_source_progress_facts_v2(p_session.id,p_session.version) s
    WHERE s.source_state='CURRENT'
  ), physical AS MATERIALIZED (
    SELECT r.id,r.candidate_id,r.row_ordinal,section.name AS section
    FROM (VALUES ('cases_resolutions'),('blocked_for_pay')) section(name)
    CROSS JOIN LATERAL private.pay_workbench_modal_eligible_rows_v2(p_session.id,p_session.version,section.name) r
    JOIN source_current s ON s.candidate_id=r.candidate_id
    WHERE NOT private.pay_workbench_modal_hidden_v2(r.row_json)
      AND private.pay_workbench_modal_row_matches_scope_v2(r.row_json||jsonb_build_object('candidate_id',r.candidate_id),
        p_session.filters_json,p_channel,section.name)
  ), entries AS (
    SELECT DISTINCT f.task_key AS key,'ACTION_REQUIRED'::text AS state,f.task_family AS family,f.task_key AS detail_key,
      f.candidate_id,NULL::text AS kind,NULL::uuid AS row_id,NULL::bigint AS ord FROM finance_members f
    UNION ALL
    SELECT g.task_key,g.state,'BANK_ACCOUNT',g.task_key,NULL::uuid,NULL::text,NULL::uuid,NULL::bigint
    FROM bank_groups g WHERE g.state IN ('ACTION_REQUIRED','UPDATING')
    UNION ALL
    SELECT DISTINCT s.task_key,s.task_json->>'state','SOURCE_PROGRESS',s.task_key,
      CASE WHEN s.task_json->>'state'='BLOCKED' THEN s.candidate_id ELSE NULL::uuid END,
      CASE WHEN s.task_json->>'state'='BLOCKED' THEN s.source_kind ELSE NULL::text END,NULL::uuid,NULL::bigint
    FROM source_members s
    UNION ALL
    SELECT encode(extensions.digest(convert_to(jsonb_build_array('BLOCKED_BANK',b.task_key,b.candidate_id,b.source_kind,
        b.preview_row_id,b.task_json->'source_ordinal')::text,'UTF8'),'sha256'),'hex'),
      'BLOCKED','BANK_ACCOUNT',b.task_key,b.candidate_id,b.source_kind,b.preview_row_id,(b.task_json->>'source_ordinal')::bigint
    FROM bank_members b JOIN bank_groups g ON g.task_key=b.task_key AND g.state='BLOCKED'
    WHERE (b.preview_row_id IS NULL OR EXISTS(SELECT 1 FROM physical p WHERE p.id=b.preview_row_id))
      AND NOT EXISTS(SELECT 1 FROM finance_members f WHERE f.preview_row_id=b.preview_row_id)
    UNION ALL
    SELECT encode(extensions.digest(convert_to(jsonb_build_array('PASSIVE',p_session.id,p_session.version,p.id)::text,'UTF8'),'sha256'),'hex'),
      'BLOCKED','PASSIVE_PAYMENT',p.id::text,p.candidate_id,'PREVIEW_ROW',p.id,p.row_ordinal::bigint
    FROM physical p
    WHERE NOT EXISTS(SELECT 1 FROM finance_members f WHERE f.preview_row_id=p.id)
      -- A resolved/component-only CASES presentation remains in its current
      -- primary case's detail. A related passive Blocked row stays Blocked.
      AND NOT EXISTS(SELECT 1 FROM finance_all f WHERE f.preview_row_id=p.id AND p.section='cases_resolutions')
      AND NOT EXISTS(SELECT 1 FROM bank_members b WHERE b.preview_row_id=p.id)
  )
  SELECT e.key,e.state,e.family,e.detail_key,e.candidate_id,e.kind,e.row_id,e.ord FROM entries e;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_issue_index_v2(public.banking_pay_workbench_sessions,text,text,text,jsonb,boolean,boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_issue_index_v2(public.banking_pay_workbench_sessions,text,text,text,jsonb,boolean,boolean) FROM PUBLIC, anon, authenticated, service_role;

commit;
