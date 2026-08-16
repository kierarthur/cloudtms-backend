-- Installed TEST read-only verification for the James post-resolution and
-- Draft-integrity correction. SELECT/WITH only: no DDL, DML or state-changing
-- RPC is used by this file.

WITH required_functions(schema_name, function_name, identity_arguments) AS (
  VALUES
    ('public', 'pay_preview_candidate_build_canonical_lines', 'jsonb, uuid, jsonb'),
    ('private', 'pay_workbench_recovery_selection_overlay_apply_v1', 'uuid, uuid, jsonb'),
    ('private', 'pay_workbench_candidate_session_version_rebase_v1', 'uuid, uuid, bigint, bigint, uuid'),
    ('public', 'pay_workbench_session_refresh_current_authority_v1', 'uuid, uuid, jsonb, integer'),
    ('public', 'pay_workbench_session_get_candidate_preview', 'uuid, uuid, jsonb, integer')
), installed AS (
  SELECT
    namespace_row.nspname AS schema_name,
    procedure_row.proname AS function_name,
    pg_catalog.pg_get_function_identity_arguments(procedure_row.oid) AS identity_arguments,
    role_row.rolname AS owner,
    procedure_row.prosecdef AS security_definer,
    procedure_row.provolatile,
    procedure_row.proparallel,
    procedure_row.proconfig,
    pg_catalog.encode(
      extensions.digest(
        pg_catalog.convert_to(pg_catalog.pg_get_functiondef(procedure_row.oid), 'UTF8'),
        'sha256'
      ),
      'hex'
    ) AS definition_sha256,
    pg_catalog.pg_get_functiondef(procedure_row.oid) AS definition_sql
  FROM pg_catalog.pg_proc AS procedure_row
  JOIN pg_catalog.pg_namespace AS namespace_row ON namespace_row.oid = procedure_row.pronamespace
  JOIN pg_catalog.pg_roles AS role_row ON role_row.oid = procedure_row.proowner
  JOIN required_functions AS required
    ON required.schema_name = namespace_row.nspname
   AND required.function_name = procedure_row.proname
   AND required.identity_arguments = pg_catalog.pg_get_function_identity_arguments(procedure_row.oid)
)
SELECT
  required.schema_name,
  required.function_name,
  required.identity_arguments,
  installed.owner,
  installed.security_definer,
  installed.provolatile,
  installed.proparallel,
  installed.proconfig,
  installed.definition_sha256,
  CASE
    WHEN installed.function_name = 'pay_preview_candidate_build_canonical_lines'
      THEN installed.definition_sql LIKE '%PAY_WORKBENCH_ALLOCATION_SEGMENT_IDENTITY_MISSING%'
       AND installed.definition_sql LIKE '%PAY_WORKBENCH_ALLOCATION_SEGMENT_IDENTITY_AMBIGUOUS%'
    WHEN installed.function_name = 'pay_workbench_recovery_selection_overlay_apply_v1'
      THEN installed.definition_sql LIKE '%PAY_WORKBENCH_RECOVERY_COMPONENT_CAPACITY_INSUFFICIENT%'
       AND installed.definition_sql LIKE '%penny_residual_ex_vat%'
    WHEN installed.function_name = 'pay_workbench_candidate_session_version_rebase_v1'
      THEN installed.definition_sql LIKE '%SESSION_VERSION_REBASE%'
       AND installed.definition_sql LIKE '%pay_workbench_delta_update_candidate_state_v1%'
    WHEN installed.function_name = 'pay_workbench_session_refresh_current_authority_v1'
      THEN installed.definition_sql LIKE '%SESSION_VERSION_REBASE%'
    WHEN installed.function_name = 'pay_workbench_session_get_candidate_preview'
      THEN installed.definition_sql LIKE '%effective_section_row_ordinal_id%'
       AND installed.definition_sql LIKE '%physical_section%'
    ELSE false
  END AS source_contract_present
FROM required_functions AS required
LEFT JOIN installed
  ON installed.schema_name = required.schema_name
 AND installed.function_name = required.function_name
 AND installed.identity_arguments = required.identity_arguments
ORDER BY required.schema_name, required.function_name;

-- Source/preview physical parity must remain exact. Effective routing is a
-- read/presentation overlay and is intentionally not substituted here.
WITH current_source AS (
  SELECT
    source_row.session_id,
    source_row.candidate_id,
    source_row.session_version,
    public.pay_workbench_preview_section_from_line_json(source_row.source_row_json) AS physical_section,
    source_row.line_key,
    source_row.source_ordinal
  FROM public.banking_pay_workbench_candidate_source_lines AS source_row
  WHERE pg_catalog.upper(pg_catalog.btrim(COALESCE(source_row.status, ''))) = 'CURRENT'
), current_preview AS (
  SELECT
    preview_row.session_id,
    preview_row.candidate_id,
    preview_row.session_version,
    preview_row.section AS physical_section,
    preview_row.row_key,
    (preview_row.row_ordinal - (scope_row.scope_ordinal * 1000000))::bigint AS row_ordinal
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  JOIN public.banking_pay_workbench_session_scope AS scope_row
    ON scope_row.session_id = preview_row.session_id
   AND scope_row.candidate_id = preview_row.candidate_id
  WHERE pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.status, ''))) = 'READY'
)
SELECT
  source_rows.session_id,
  source_rows.candidate_id,
  source_rows.session_version,
  source_rows.source_minus_preview_count,
  preview_rows.preview_minus_source_count
FROM (
  SELECT source.session_id, source.candidate_id, source.session_version,
         pg_catalog.count(*)::integer AS source_minus_preview_count
  FROM (
    SELECT * FROM current_source
    EXCEPT ALL
    SELECT session_id, candidate_id, session_version, physical_section, row_key, row_ordinal
    FROM current_preview
  ) AS source
  GROUP BY source.session_id, source.candidate_id, source.session_version
) AS source_rows
FULL JOIN (
  SELECT preview.session_id, preview.candidate_id, preview.session_version,
         pg_catalog.count(*)::integer AS preview_minus_source_count
  FROM (
    SELECT session_id, candidate_id, session_version, physical_section, row_key, row_ordinal
    FROM current_preview
    EXCEPT ALL
    SELECT * FROM current_source
  ) AS preview
  GROUP BY preview.session_id, preview.candidate_id, preview.session_version
) AS preview_rows
  ON preview_rows.session_id = source_rows.session_id
 AND preview_rows.candidate_id = source_rows.candidate_id
 AND preview_rows.session_version = source_rows.session_version
WHERE COALESCE(source_rows.source_minus_preview_count, 0) <> 0
   OR COALESCE(preview_rows.preview_minus_source_count, 0) <> 0
ORDER BY 1, 2, 3
LIMIT 100;
