-- Complete private bank-source facts. Public task classification, grouping,
-- permissions and bounded list/detail envelopes remain separate owners.
\set ON_ERROR_STOP on
\ir 28082026_1308_banking_pay_modal_ready_members.sql
\ir 28082026_1650_banking_pay_modal_source_progress_facts.sql
\ir 28082026_1657_banking_pay_modal_payee_readiness_projection.sql
begin;
CREATE OR REPLACE FUNCTION private.pay_workbench_modal_bank_sources_v2(
  p_session public.banking_pay_workbench_sessions,p_channel text
) RETURNS TABLE(candidate_id uuid,source_kind text,source_ordinal bigint,preview_row_id uuid,bank_row jsonb,source_payload jsonb)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_member record;
BEGIN
  IF p_channel IS NULL OR p_channel NOT IN ('ALL','PAYE','UMBRELLA') THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  -- One set-wise query obtains complete current sources. The loop validates
  -- already-read identity only; it performs no per-candidate queries or writes.
  FOR v_member IN
    WITH current_scope AS MATERIALIZED (
      SELECT f.candidate_id FROM private.pay_workbench_modal_source_progress_facts_v2(p_session.id,p_session.version) f
      WHERE f.source_state='CURRENT'
    ), eligible AS MATERIALIZED (
      SELECT r.* FROM (VALUES ('canonical_preview_lines'),('cases_resolutions'),('blocked_for_pay')) section(name)
      CROSS JOIN LATERAL private.pay_workbench_modal_eligible_rows_v2(p_session.id,p_session.version,section.name) r
      JOIN current_scope s ON s.candidate_id=r.candidate_id
      WHERE NOT private.pay_workbench_modal_hidden_v2(r.row_json)
        AND private.pay_workbench_modal_row_matches_scope_v2(r.row_json || jsonb_build_object('candidate_id',r.candidate_id),
          p_session.filters_json,p_channel,section.name)
    ), visible_candidates AS MATERIALIZED (
      SELECT DISTINCT r.candidate_id FROM eligible r
    ), metadata AS MATERIALIZED (
      SELECT s.candidate_id,s.session_version,s.effective_payees_json,s.effective_non_paye_payee_json,s.effective_paye_candidate_json,
        private.pay_workbench_modal_candidate_bank_meta_v2(
          (CASE WHEN jsonb_typeof(s.effective_candidate_fragment_json)='object' THEN s.effective_candidate_fragment_json ELSE '{}'::jsonb END)
          || jsonb_build_object('candidate_id',s.candidate_id)) AS payload
      FROM public.banking_pay_workbench_session_candidate_state s
      JOIN current_scope c ON c.candidate_id=s.candidate_id
      WHERE s.session_id=p_session.id AND s.session_version=p_session.version
        AND NOT private.pay_workbench_modal_hidden_v2(s.effective_candidate_fragment_json)
        AND (EXISTS(SELECT 1 FROM visible_candidates v WHERE v.candidate_id=s.candidate_id)
          -- A current stored bank issue may precede its first payment row, as
          -- in the existing synthetic-bank presentation. This is NOT a route
          -- around hidden, excluded, out-of-filter or ineligible physical rows.
          OR NOT EXISTS(SELECT 1 FROM public.banking_pay_workbench_preview_rows r
            WHERE r.session_id=p_session.id AND r.session_version=p_session.version
              AND r.candidate_id=s.candidate_id))
    ), physical_payloads AS MATERIALIZED (
      SELECT r.id,r.candidate_id,r.row_ordinal,private.pay_workbench_modal_row_payload_v2(r) AS payload,m.payload AS meta
      FROM eligible r LEFT JOIN metadata m ON m.candidate_id=r.candidate_id
    ), physical_sources AS MATERIALIZED (
      SELECT r.id,r.candidate_id,r.row_ordinal,r.payload,
        private.pay_workbench_modal_payee_readiness_row_v2(
          r.payload || private.pay_workbench_modal_bank_meta_v2(r.payload,r.meta),r.meta,NULL) AS bank_row
      FROM physical_payloads r
    ), raw_payees AS MATERIALIZED (
      -- Only byte-equivalent JSON aliases for the SAME candidate are reduced
      -- here. Different payment rows and shared-owner candidates are retained.
      SELECT DISTINCT ON (m.candidate_id,item.value)
        m.candidate_id,m.payload AS meta,origin.priority,item.ordinality::bigint AS ord,item.value AS original,
        item.value || jsonb_build_object('candidate_id',m.candidate_id) AS payload,
        private.pay_workbench_modal_route_text_v2(ARRAY[item.value->'candidate_id',item.value->'candidateId']) AS original_candidate
      FROM metadata m
      CROSS JOIN LATERAL (VALUES (1,m.effective_payees_json),
        (2,jsonb_build_array(m.effective_non_paye_payee_json)),(3,jsonb_build_array(m.effective_paye_candidate_json))) origin(priority,payees)
      CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(origin.payees)='array' THEN origin.payees ELSE '[]'::jsonb END) WITH ORDINALITY item
      WHERE jsonb_typeof(item.value)='object' AND NOT private.pay_workbench_modal_hidden_v2(item.value)
      ORDER BY m.candidate_id,item.value,origin.priority,item.ordinality
    ), routes AS MATERIALIZED (
      SELECT p.*,private.pay_workbench_modal_payee_route_v2(p.payload,p.meta) AS route FROM raw_payees p
    ), non_candidate_routes AS (
      SELECT DISTINCT r.candidate_id FROM routes r
      WHERE r.route->>'entity_kind' NOT IN ('','CANDIDATE') AND r.route->>'entity_id'<>''
    ), projected AS MATERIALIZED (
      SELECT r.*,private.pay_workbench_modal_payee_readiness_row_v2(r.payload,r.meta,NULL) AS bank_row
      FROM routes r
      WHERE NOT (r.route->>'entity_kind'='CANDIDATE' AND EXISTS(SELECT 1 FROM non_candidate_routes n WHERE n.candidate_id=r.candidate_id))
    ), distinct_payees AS (
      SELECT DISTINCT ON (p.candidate_id,p.bank_row->>'__payee_route_key') p.*
      FROM projected p WHERE p.bank_row IS NOT NULL
      ORDER BY p.candidate_id,p.bank_row->>'__payee_route_key',p.priority,p.ord
    )
    SELECT r.candidate_id,'PREVIEW_ROW'::text AS source_kind,r.row_ordinal::bigint AS source_ordinal,
      r.id AS preview_row_id,r.bank_row,r.payload AS source_payload,NULL::text AS original_candidate
    FROM physical_sources r WHERE r.bank_row IS NOT NULL
    UNION ALL
    SELECT p.candidate_id,'STORED_PAYEE'::text,p.ord,NULL::uuid,p.bank_row,p.original,p.original_candidate
    FROM distinct_payees p
    WHERE NOT EXISTS(SELECT 1 FROM physical_sources r WHERE r.bank_row IS NOT NULL
      AND r.candidate_id=p.candidate_id AND r.bank_row->>'__payee_route_key'=p.bank_row->>'__payee_route_key')
      AND private.pay_workbench_modal_row_matches_scope_v2(p.bank_row,p_session.filters_json,p_channel,'blocked_for_pay')
  LOOP
    IF v_member.original_candidate IS NOT NULL AND v_member.original_candidate<>''
      AND v_member.original_candidate IS DISTINCT FROM v_member.candidate_id::text THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_SOURCE_IDENTITY_MISMATCH' USING ERRCODE='P0001';
    END IF;
    candidate_id:=v_member.candidate_id;source_kind:=v_member.source_kind;source_ordinal:=v_member.source_ordinal;
    preview_row_id:=v_member.preview_row_id;bank_row:=v_member.bank_row;source_payload:=v_member.source_payload;
    RETURN NEXT;
  END LOOP;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_bank_sources_v2(public.banking_pay_workbench_sessions,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_bank_sources_v2(public.banking_pay_workbench_sessions,text) FROM PUBLIC, anon, authenticated, service_role;
commit;
