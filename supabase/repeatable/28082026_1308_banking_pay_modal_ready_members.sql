-- Exact read-only selection predicate copied from the existing selection owner.
-- Source parity tests prevent a second, drifting eligibility implementation.
\set ON_ERROR_STOP on
\ir 28082026_1232_banking_pay_modal_certified_projection.sql
\ir 28082026_1303_banking_pay_modal_display_projection.sql
\ir 28082026_1342_banking_pay_modal_visibility.sql
\ir 28082026_1650_banking_pay_modal_source_progress_facts.sql

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_selection_rows_v2(p_session_id uuid, p_session_version bigint)
RETURNS TABLE(id uuid, row_ordinal bigint, row_key text, was_selected boolean, contract_json jsonb, is_synthetic_resolved_total boolean, is_selectable boolean)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
      SELECT preview_row.id,
             preview_row.row_ordinal,
             preview_row.row_key,
             COALESCE(preview_row.selected, false) AS was_selected,
             contract_check.contract_json,
             synthetic_check.is_synthetic_resolved_total,
             (
               LOWER(BTRIM(COALESCE(contract_check.contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND LOWER(BTRIM(COALESCE(contract_check.contract_json->>'materialisable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND LOWER(BTRIM(COALESCE(contract_check.contract_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND COALESCE(contract_check.contract_json->>'target_section', '') = 'canonical_preview_lines'
               AND UPPER(BTRIM(COALESCE(contract_check.contract_json->>'presentation_section', ''))) = 'READY_TO_PAY'
               AND LOWER(BTRIM(COALESCE(contract_check.contract_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND LOWER(BTRIM(COALESCE(contract_check.contract_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND COALESCE(contract_check.contract_json->>'key_type', '') <> ''
               AND COALESCE(contract_check.contract_json->>'key_value', '') <> ''
               AND (
                 UPPER(BTRIM(COALESCE(contract_check.contract_json->>'key_type', ''))) <> 'TS_DAY'
                 OR COALESCE(contract_check.contract_json->>'key_value', '') ~ '^\d{4}-\d{2}-\d{2}$'
               )
               AND UPPER(BTRIM(COALESCE(contract_check.contract_json->>'source_kind', ''))) NOT IN (
                 'TIMESHEET_SNAPSHOT',
                 'TIMESHEET_SNAPSHOT_EVIDENCE',
                 'RAW_TIMESHEET_SNAPSHOT',
                 'INTERNAL_ONLY',
                 'NO_DELTA',
                 'EXCLUDED'
               )
               AND preview_row.row_key NOT LIKE 'timesheet_snapshot:%'
               AND synthetic_check.is_synthetic_resolved_total IS NOT TRUE
             ) AS is_selectable
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      CROSS JOIN LATERAL (
        SELECT public.pay_workbench_preview_line_contract_ok(
          p_line_json => COALESCE(preview_row.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'line_key', preview_row.row_key,
              'row_key', preview_row.row_key,
              'section', private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json),
              'target_section', private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json),
              'key_type', preview_row.key_type,
              'key_value', preview_row.key_value
            ),
          p_economic_key_json => jsonb_build_object(
            'key_type', preview_row.key_type,
            'key_value', preview_row.key_value
          ),
          p_target_section => private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json)
        ) AS contract_json
      ) AS contract_check
      CROSS JOIN LATERAL (
        SELECT (
          preview_row.timesheet_id IS NOT NULL
          AND UPPER(BTRIM(COALESCE(
            preview_row.key_type,
            preview_row.row_json#>>'{economic_key,key_type}',
            preview_row.row_json->>'component_key_type',
            preview_row.row_json->>'key_type',
            ''
          ))) = 'TS_TOTAL'
          AND UPPER(BTRIM(COALESCE(
            preview_row.key_value,
            preview_row.row_json#>>'{economic_key,key_value}',
            preview_row.row_json->>'component_key_value',
            preview_row.row_json->>'key_value',
            ''
          ))) = 'TOTAL'
          AND LOWER(BTRIM(COALESCE(
            preview_row.row_key,
            preview_row.row_json->>'row_key',
            preview_row.row_json->>'line_key',
            preview_row.row_json->>'source_ref',
            preview_row.row_json#>>'{source_basis,row_key}',
            preview_row.row_json#>>'{source_basis,line_key}',
            preview_row.row_json#>>'{source_basis,source_ref}',
            preview_row.row_json#>>'{source_basis_json,row_key}',
            preview_row.row_json#>>'{source_basis_json,line_key}',
            preview_row.row_json#>>'{source_basis_json,source_ref}',
            ''
          ))) LIKE '%:non_segment:total%'
          AND EXISTS (
            SELECT 1
            FROM public.banking_pay_workbench_preview_rows AS sibling_row
            WHERE sibling_row.session_id = preview_row.session_id
              AND sibling_row.session_version = preview_row.session_version
              AND sibling_row.id <> preview_row.id
              AND sibling_row.timesheet_id = preview_row.timesheet_id
              AND lower(private.pay_workbench_preview_effective_section_v1(sibling_row.section, sibling_row.row_json)) = 'canonical_preview_lines'
              AND UPPER(BTRIM(COALESCE(sibling_row.status, ''))) = 'READY'
              AND UPPER(BTRIM(COALESCE(
                sibling_row.key_type,
                sibling_row.row_json#>>'{economic_key,key_type}',
                sibling_row.row_json->>'component_key_type',
                sibling_row.row_json->>'key_type',
                ''
              ))) = 'TS_DAY'
              AND COALESCE(
                sibling_row.key_value,
                sibling_row.row_json#>>'{economic_key,key_value}',
                sibling_row.row_json->>'component_key_value',
                sibling_row.row_json->>'key_value',
                ''
              ) ~ '^\d{4}-\d{2}-\d{2}$'
          )
        ) AS is_synthetic_resolved_total
      ) AS synthetic_check
      WHERE preview_row.session_id = p_session_id
        AND preview_row.session_version = p_session_version
        AND lower(private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json)) = 'canonical_preview_lines'
        AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY';
$function$;
ALTER FUNCTION private.pay_workbench_modal_selection_rows_v2(uuid, bigint) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_selection_rows_v2(uuid, bigint) FROM PUBLIC, anon, authenticated, service_role;

-- Exact existing Timesheet-shortcut traversal, applied ONLY to selected Ready
-- payments by the caller. Do not broaden to a candidate-name/ID search.
CREATE OR REPLACE FUNCTION private.pay_workbench_modal_related_timesheets_v2(p_row jsonb)
RETURNS uuid[] LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  WITH RECURSIVE walk(value, depth) AS (
    SELECT p_row, 0
    UNION ALL
    SELECT child.value, walk.depth + 1 FROM walk
    CROSS JOIN LATERAL (
      SELECT field.value FROM jsonb_each(CASE WHEN jsonb_typeof(walk.value)='object' THEN walk.value ELSE '{}'::jsonb END) AS field
      WHERE field.key IN (
        'economic_key','economicKey','row_json','rowJson','source_basis_json','sourceBasisJson',
        'case_resolution_summary','caseResolutionSummary','__case_entry','raw_case','rawCase','payload_json','payloadJson',
        'frozen_resolution_payload_json','frozenResolutionPayloadJson','components','case_components','caseComponents',
        'resolution_components','resolutionComponents','resolution_rows','resolutionRows','bucket_resolutions','bucketResolutions',
        'overpayment_components','overpaymentComponents','display_lines','displayLines'
      )
      UNION ALL
      SELECT item.value FROM jsonb_array_elements(CASE WHEN jsonb_typeof(walk.value)='array' THEN walk.value ELSE '[]'::jsonb END) AS item
    ) AS child
    WHERE walk.depth < 8 AND jsonb_typeof(child.value) IN ('object','array')
  ), fields AS (
    SELECT field.key, field.value FROM walk
    CROSS JOIN LATERAL jsonb_each(CASE WHEN jsonb_typeof(walk.value)='object' THEN walk.value ELSE '{}'::jsonb END) AS field
  ), ids AS (
    SELECT BTRIM(value #>> '{}') AS id FROM fields
    WHERE key IN ('timesheet_id','timesheetId','linked_timesheet_id','linkedTimesheetId',
      'original_timesheet_id','originalTimesheetId','current_timesheet_id','currentTimesheetId')
    UNION ALL
    SELECT BTRIM(item.value #>> '{}') FROM fields
    CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(fields.value)='array' THEN fields.value ELSE '[]'::jsonb END) AS item
    WHERE fields.key IN ('timesheet_ids','timesheetIds','linked_timesheet_ids','linkedTimesheetIds',
      'selected_timesheet_ids','selectedTimesheetIds','affected_timesheet_ids','affectedTimesheetIds')
  )
  SELECT COALESCE(array_agg(DISTINCT id::uuid ORDER BY id::uuid), ARRAY[]::uuid[]) FROM ids
  WHERE id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
$function$;
ALTER FUNCTION private.pay_workbench_modal_related_timesheets_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_related_timesheets_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_ready_members_v2(
  p_session public.banking_pay_workbench_sessions, p_channel text
) RETURNS TABLE(row_id uuid, candidate_id uuid, selected boolean, display_amount numeric,
  is_deduction boolean, related_timesheet_ids uuid[])
LANGUAGE sql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  WITH eligible AS MATERIALIZED (
    SELECT r.* FROM private.pay_workbench_modal_eligible_rows_v2(p_session.id,p_session.version,'canonical_preview_lines') AS r
    JOIN private.pay_workbench_modal_source_progress_facts_v2(p_session.id,p_session.version) AS scope
      ON scope.candidate_id=r.candidate_id
    WHERE scope.source_state='CURRENT'
      AND NOT private.pay_workbench_modal_hidden_v2(r.row_json)
  ), selection AS MATERIALIZED (
    SELECT * FROM private.pay_workbench_modal_selection_rows_v2(p_session.id,p_session.version)
    WHERE is_selectable IS TRUE
  ), current_rows AS MATERIALIZED (
    SELECT r.id, r.candidate_id, COALESCE(r.selected,false) AS selected,
      private.pay_workbench_modal_row_payload_v2(r) AS payload,
      selection.contract_json->>'is_recognised_finance_deduction'='true' AS is_deduction
    FROM eligible AS r JOIN selection ON selection.id=r.id
  )
  SELECT r.id, r.candidate_id, r.selected,
    CASE WHEN r.selected THEN private.pay_workbench_modal_line_display_amount_v2(r.payload) ELSE 0::numeric END,
    r.selected AND COALESCE(r.is_deduction,false),
    CASE WHEN r.selected THEN private.pay_workbench_modal_related_timesheets_v2(r.payload) ELSE ARRAY[]::uuid[] END
  FROM current_rows AS r
  WHERE private.pay_workbench_modal_row_matches_scope_v2(r.payload,p_session.filters_json,p_channel,'canonical_preview_lines');
$function$;
ALTER FUNCTION private.pay_workbench_modal_ready_members_v2(public.banking_pay_workbench_sessions, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_ready_members_v2(public.banking_pay_workbench_sessions, text) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_candidate_facts_v2(
  p_session public.banking_pay_workbench_sessions, p_channel text
) RETURNS TABLE(candidate_id uuid, candidate_name text, candidate_reference text,
  candidate_sort_name text, candidate_sort_reference text,
  selectable_ready_count bigint, selected_ready_count bigint, selection_state text,
  selected_display_amount numeric, selected_deduction_exists boolean, selected_timesheet_ids uuid[])
LANGUAGE sql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  WITH members AS MATERIALIZED (
    SELECT * FROM private.pay_workbench_modal_ready_members_v2(p_session,p_channel)
  ), amounts AS (
    SELECT m.candidate_id, count(*) AS selectable_count, count(*) FILTER(WHERE selected) AS selected_count,
      COALESCE(sum(m.display_amount) FILTER(WHERE selected),0) AS amount,
      COALESCE(bool_or(m.is_deduction) FILTER(WHERE selected),false) AS deductions
    FROM members AS m GROUP BY m.candidate_id
  ), timesheets AS (
    SELECT m.candidate_id, array_agg(DISTINCT id.value ORDER BY id.value) AS ids
    FROM members AS m CROSS JOIN LATERAL unnest(m.related_timesheet_ids) AS id(value)
    WHERE m.selected GROUP BY m.candidate_id
  )
  SELECT a.candidate_id, COALESCE(NULLIF(BTRIM(c.display_name),''),NULLIF(BTRIM(c.tms_ref),''),c.id::text),
    COALESCE(c.tms_ref,''), lower(COALESCE(NULLIF(BTRIM(c.display_name),''),NULLIF(BTRIM(c.tms_ref),''),c.id::text)),
    lower(COALESCE(c.tms_ref,'')), a.selectable_count,a.selected_count,
    CASE WHEN a.selected_count=0 THEN 'NONE' WHEN a.selected_count=a.selectable_count THEN 'ALL' ELSE 'SOME' END,
    a.amount,a.deductions,COALESCE(t.ids,ARRAY[]::uuid[])
  FROM amounts AS a JOIN public.candidates AS c ON c.id=a.candidate_id LEFT JOIN timesheets AS t ON t.candidate_id=a.candidate_id;
$function$;
ALTER FUNCTION private.pay_workbench_modal_candidate_facts_v2(public.banking_pay_workbench_sessions, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_candidate_facts_v2(public.banking_pay_workbench_sessions, text) FROM PUBLIC, anon, authenticated, service_role;
