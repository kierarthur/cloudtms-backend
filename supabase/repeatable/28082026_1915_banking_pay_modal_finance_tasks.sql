-- Private read-only task grouping over the existing case/component button
-- guards. Task keys are presentation identities, never economic/payment keys.
\set ON_ERROR_STOP on
\ir 28082026_1308_banking_pay_modal_ready_members.sql
\ir 28082026_1354_banking_pay_modal_case_action_facts.sql
\ir 28082026_1448_banking_pay_modal_component_action_facts.sql
\ir 28082026_1650_banking_pay_modal_source_progress_facts.sql
begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_finance_tasks_v2(
  p_row jsonb,p_case_identity text,p_lineage text
) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_meta jsonb;v_actions jsonb;v_component jsonb;v_facts jsonb;
  v_tasks jsonb:='[]'::jsonb;v_identity jsonb;v_component_key text;v_title text;
BEGIN
  IF private.pay_workbench_modal_hidden_v2(p_row) THEN RETURN v_tasks; END IF;
  v_meta:=private.pay_workbench_modal_case_meta_v2(p_row);
  IF v_meta IS NULL THEN RETURN v_tasks; END IF;
  IF NULLIF(BTRIM(p_case_identity),'') IS NULL OR NULLIF(BTRIM(p_lineage),'') IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INCOMPLETE_TASK_IDENTITY' USING ERRCODE='P0001';
  END IF;
  v_actions:=private.pay_workbench_modal_case_actions_v2(v_meta);
  IF v_actions ?| ARRAY['banking:pay:openBucketedResolution','banking:pay:openNonBucketResolution',
    'banking:pay:openTaxableFinanceCaseRestructure'] THEN
    v_identity:=jsonb_build_array('FINANCE_CASE',v_meta->>'candidate_id',p_case_identity,p_lineage);
    v_title:=CASE v_meta->>'resolution_family' WHEN 'TAXABLE_CHANNEL_RESTRUCTURE' THEN 'MSG-044'
      WHEN 'NON_BUCKET' THEN 'MSG-046' ELSE 'MSG-045' END;
    RETURN jsonb_build_array(jsonb_build_object(
      'task_key',encode(extensions.digest(convert_to(v_identity::text,'UTF8'),'sha256'),'hex'),
      'family','FINANCE_CASE','title_message_id',v_title,'actions',v_actions,
      'case_key',v_meta->'case_key','finance_case_id',v_meta->'finance_case_id',
      'resolution_family',v_meta->'resolution_family','linked_timesheet_id',v_meta->'linked_timesheet_id',
      'source_lineage',p_lineage));
  END IF;
  FOR v_component IN SELECT value FROM jsonb_array_elements(v_meta->'components') LOOP
    IF private.pay_workbench_modal_hidden_v2(v_component) THEN CONTINUE; END IF;
    v_facts:=private.pay_workbench_modal_component_actions_v2(v_component);
    IF v_facts->'needs_action' IS DISTINCT FROM 'true'::jsonb
      OR v_facts->'fixed_no_action' IS DISTINCT FROM 'false'::jsonb THEN CONTINUE; END IF;
    v_actions:=v_facts->'actions';
    IF NOT(v_actions ?| ARRAY['banking:pay:componentUseSuggested','banking:pay:componentManualRate',
      'banking:pay:componentManualAmount']) THEN CONTINUE; END IF;
    v_component_key:=NULLIF(BTRIM(COALESCE(NULLIF(v_component->>'finance_component_id',''),v_component->>'key')),'');
    IF v_component_key IS NULL THEN
      -- The existing controls need an exact component key. An incomplete task
      -- must be surfaced as a contract failure, never silently dropped/guessed.
      RAISE EXCEPTION 'BANKING_PAY_V2_INCOMPLETE_TASK_IDENTITY' USING ERRCODE='P0001';
    END IF;
    v_identity:=jsonb_build_array('FINANCE_COMPONENT',v_meta->>'candidate_id',p_case_identity,
      v_component_key,v_component->>'source_basis_fingerprint',p_lineage);
    v_title:=CASE WHEN v_actions ? 'banking:pay:componentManualAmount'
      AND NOT(v_actions ?| ARRAY['banking:pay:componentUseSuggested','banking:pay:componentManualRate'])
      THEN 'MSG-046' ELSE 'MSG-045' END;
    v_tasks:=v_tasks || jsonb_build_array(jsonb_build_object(
      'task_key',encode(extensions.digest(convert_to(v_identity::text,'UTF8'),'sha256'),'hex'),
      'family','FINANCE_COMPONENT','title_message_id',v_title,'actions',v_actions,
      'case_key',v_meta->'case_key','finance_case_id',v_meta->'finance_case_id',
      'resolution_family',v_meta->'resolution_family','linked_timesheet_id',v_meta->'linked_timesheet_id',
      'component',v_component,'source_lineage',p_lineage));
  END LOOP;
  RETURN v_tasks;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_finance_tasks_v2(jsonb,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_finance_tasks_v2(jsonb,text,text) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_finance_task_members_v2(
  p_session public.banking_pay_workbench_sessions,p_channel text
) RETURNS TABLE(task_key text,task_family text,title_message_id text,candidate_id uuid,
  preview_row_id uuid,row_payload jsonb,task_json jsonb)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_member record;
BEGIN
  IF p_channel IS NULL OR p_channel NOT IN ('ALL','PAYE','UMBRELLA') THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  -- No per-candidate query, first-page limit, financial sum or case mutation.
  -- Current case-owner aliases are resolved across the entire scoped result.
  FOR v_member IN
    WITH source AS MATERIALIZED (
      SELECT f.candidate_id,cs.source_change_seq
      FROM private.pay_workbench_modal_source_progress_facts_v2(p_session.id,p_session.version) f
      LEFT JOIN public.banking_pay_workbench_session_candidate_state cs
        ON cs.session_id=p_session.id AND cs.candidate_id=f.candidate_id AND cs.session_version=p_session.version
      WHERE f.source_state='CURRENT'
    ), rows AS MATERIALIZED (
      SELECT r.id,r.candidate_id,section.name AS source_section,private.pay_workbench_modal_row_payload_v2(r) AS payload,
        jsonb_build_array(p_session.version,s.source_change_seq)::text AS lineage
      FROM (VALUES ('cases_resolutions'),('blocked_for_pay'),('canonical_preview_lines')) section(name)
      CROSS JOIN LATERAL private.pay_workbench_modal_eligible_rows_v2(p_session.id,p_session.version,section.name) r
      JOIN source s ON s.candidate_id=r.candidate_id
      WHERE NOT private.pay_workbench_modal_hidden_v2(r.row_json)
        AND private.pay_workbench_modal_row_matches_scope_v2(r.row_json || jsonb_build_object('candidate_id',r.candidate_id),
          p_session.filters_json,p_channel,section.name)
    ), facts AS MATERIALIZED (
      SELECT r.*,private.pay_workbench_modal_case_meta_v2(r.payload) AS meta FROM rows r
    ), aliases AS MATERIALIZED (
      SELECT f.candidate_id,f.meta->>'case_key' AS case_key,
        count(DISTINCT NULLIF(f.meta->>'finance_case_id','')) AS owners,
        min(NULLIF(f.meta->>'finance_case_id','')) AS finance_id
      FROM facts f WHERE NULLIF(f.meta->>'case_key','') IS NOT NULL
      GROUP BY f.candidate_id,f.meta->>'case_key'
    ), resolved AS MATERIALIZED (
      SELECT f.*,a.owners,COALESCE(NULLIF(f.meta->>'finance_case_id',''),a.finance_id,
        NULLIF(f.meta->>'case_key',''),NULLIF(f.meta->>'linked_timesheet_id','')) AS case_identity
      FROM facts f LEFT JOIN aliases a ON a.candidate_id=f.candidate_id AND a.case_key=f.meta->>'case_key'
      WHERE f.meta IS NOT NULL
    ), described AS MATERIALIZED (
      SELECT r.*,t.value AS task FROM resolved r
      LEFT JOIN LATERAL jsonb_array_elements(CASE WHEN COALESCE(r.owners,0)<=1 AND r.source_section<>'canonical_preview_lines'
        THEN private.pay_workbench_modal_finance_tasks_v2(r.payload,r.case_identity,r.lineage)
        ELSE '[]'::jsonb END) t ON true
    ), primary_counts AS (
      SELECT d.candidate_id,d.case_identity,d.lineage,count(DISTINCT d.task->>'resolution_family') AS families
      FROM described d WHERE d.task->>'family'='FINANCE_CASE'
      GROUP BY d.candidate_id,d.case_identity,d.lineage
    ), primary_tasks AS (
      SELECT DISTINCT ON(d.candidate_id,d.case_identity,d.lineage)
        d.candidate_id,d.case_identity,d.lineage,d.task
      FROM described d WHERE d.task->>'family'='FINANCE_CASE'
      ORDER BY d.candidate_id,d.case_identity,d.lineage,d.id
    )
    -- A primary case task owns its complete current affected detail, including
    -- component-only and resolved presentation rows. Do not create a second
    -- task for the same case simply because another row exposes fewer buttons.
    SELECT DISTINCT ON(d.candidate_id,d.id,COALESCE(p.task,d.task)->>'task_key')
      d.*,COALESCE(p.task,d.task) AS final_task,pc.families AS primary_family_count
    FROM described d LEFT JOIN primary_tasks p
      ON p.candidate_id=d.candidate_id AND p.case_identity=d.case_identity AND p.lineage=d.lineage
    LEFT JOIN primary_counts pc
      ON pc.candidate_id=d.candidate_id AND pc.case_identity=d.case_identity AND pc.lineage=d.lineage
    ORDER BY d.candidate_id,d.id,COALESCE(p.task,d.task)->>'task_key'
  LOOP
    IF COALESCE(v_member.owners,0)>1 OR COALESCE(v_member.primary_family_count,0)>1 THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_CONFLICTING_TASK_OWNER' USING ERRCODE='P0001';
    END IF;
    IF v_member.final_task IS NULL THEN CONTINUE; END IF;
    task_key:=v_member.final_task->>'task_key';task_family:=v_member.final_task->>'family';
    title_message_id:=v_member.final_task->>'title_message_id';candidate_id:=v_member.candidate_id;
    preview_row_id:=v_member.id;row_payload:=v_member.payload;
    -- Related Ready/resolved/passive rows are evidence, not newly actionable
    -- payments. The section classifier must retain their existing owner.
    task_json:=v_member.final_task || jsonb_build_object('context_only',v_member.task IS NULL);
    RETURN NEXT;
  END LOOP;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_finance_task_members_v2(public.banking_pay_workbench_sessions,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_finance_task_members_v2(public.banking_pay_workbench_sessions,text) FROM PUBLIC, anon, authenticated, service_role;
commit;
