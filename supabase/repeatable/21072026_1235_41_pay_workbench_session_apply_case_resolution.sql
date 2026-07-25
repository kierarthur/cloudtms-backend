-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 2e1d3cc6dc58.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.pay_workbench_session_apply_case_resolution(p_session_id uuid, p_actor_user_id uuid, p_resolution_payload_json jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_expected_session_version bigint := NULL::bigint;
  v_expected_progress_counter_version bigint := NULL::bigint;
  v_expected_session_version_text text := '';
  v_expected_progress_counter_version_text text := '';
  v_resolution_payload_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_resolution_payload_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_resolution_payload_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_top_source_basis_json jsonb := '{}'::jsonb;
  v_bucket_resolutions_json jsonb := '[]'::jsonb;
  v_candidate_id uuid := NULL::uuid;
  v_candidate_id_text text := '';
  v_case_key text := '';
  v_resolution_family text := '';
  v_default_resolution_mode text := '';
  v_linked_timesheet_id uuid := NULL::uuid;
  v_linked_timesheet_id_text text := '';
  v_finance_case_id_text text := '';
  v_resolve_all_linked_timesheets boolean := false;
  v_resolved_candidate_id uuid := NULL::uuid;
  v_matching_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_matching_candidate_count integer := 0;
  v_target_match_count integer := 0;
  v_row_backed_case_count integer := 0;
  v_row_backed_component_count integer := 0;
  v_bucket_count integer := 0;
  v_existing_deleted_count integer := 0;
  v_new_session_version bigint := 0;
  v_new_progress_counter_version bigint := 0;
  v_job_json jsonb := '{}'::jsonb;
  v_job_id_text text := NULL::text;
  v_job_id uuid := NULL::uuid;
  v_case_resolution_ids jsonb := '[]'::jsonb;
  v_resolution_identity_keys jsonb := '[]'::jsonb;
  v_case_resolution_id_text text := null;
  v_resolution_origins_json jsonb := '[]'::jsonb;
  v_resolution_origin_count integer := 0;
  v_single_resolution_origin_session_id uuid := NULL::uuid;
  v_single_resolution_origin_pay_date date := NULL::date;
  v_action text := 'SESSION_CASE_RESOLUTION_APPLIED';
  v_case_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_case_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_case_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_target_amount_ex_vat numeric := NULL::numeric;
  v_nonbucket_resolution_mode text := '';
  v_nonbucket_resolution_identity_key text := '';
  v_nonbucket_existing_row public.banking_pay_workbench_session_case_resolutions%ROWTYPE;
  v_nonbucket_upserted_row public.banking_pay_workbench_session_case_resolutions%ROWTYPE;
  v_existing_row public.banking_pay_workbench_session_case_resolutions%ROWTYPE;
  v_upserted_row public.banking_pay_workbench_session_case_resolutions%ROWTYPE;
  v_bucket_row record;
  v_bucket_element record;
  v_bucket_source_basis_json jsonb := '{}'::jsonb;
  v_bucket_source_basis_fingerprint text := '';
  v_bucket_source_family_key text := '';
  v_bucket_bucket_code text := '';
  v_bucket_component_key_type text := '';
  v_bucket_component_key_value text := '';
  v_bucket_resolution_mode text := '';
  v_bucket_timesheet_id uuid := NULL::uuid;
  v_bucket_timesheet_id_text text := '';
  v_bucket_source_units numeric := NULL::numeric;
  v_bucket_source_rate numeric := NULL::numeric;
  v_bucket_source_charge_rate numeric := NULL::numeric;
  v_bucket_target_rate numeric := NULL::numeric;
  v_bucket_resolution_identity_key text := '';
  v_normalized_payload_json jsonb := '{}'::jsonb;
  v_taxable_finance_case_id uuid := NULL::uuid;
  v_taxable_case_candidate_id uuid := NULL::uuid;
  v_taxable_resolution_path text := '';
  v_taxable_schedule_input_mode text := NULL::text;
  v_taxable_weeks_total integer := NULL::integer;
  v_taxable_weekly_due numeric := NULL::numeric;
  v_taxable_manual_total_remaining numeric := NULL::numeric;
  v_taxable_effective_pay_date date := NULL::date;
  v_taxable_note text := NULL::text;
  v_taxable_result_json jsonb := '{}'::jsonb;
  v_audit_before_json jsonb := NULL;
  v_audit_after_json jsonb := '{}'::jsonb;
  v_anchor_evidence_key text := NULL::text;
  v_bucket_matched_candidate_id uuid := NULL::uuid;
  v_matching_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_anchor_timesheet_id uuid := NULL::uuid;
  v_anchor_contract_id uuid := NULL::uuid;
  v_anchor_client_id uuid := NULL::uuid;
  v_anchor_component_count integer := 0;
  v_linked_target_timesheet_count integer := 0;
  v_linked_target_component_count integer := 0;
  v_materialized_target_timesheet_count integer := 0;
  v_skipped_mismatch_count integer := 0;
  v_linked_scope_type text := 'SELF_ONLY';
  v_target_resolution_row record;
  v_target_resolution_result_json jsonb := '{}'::jsonb;
  v_target_bucket_payload_json jsonb := '{}'::jsonb;
  v_linked_scope_json jsonb := '{}'::jsonb;
  v_operation text := 'APPLY';
  v_eligible_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_excluded_linked_timesheets jsonb := '[]'::jsonb;
  v_excluded_linked_timesheet_count integer := 0;
  v_total_affected_timesheet_count integer := 1;
  v_anchor_blocking_batches jsonb := '[]'::jsonb;
BEGIN
  v_resolution_payload_json:=public._ctms_enrich_correction_resolution_payload_v1(
    p_session_id,
    v_resolution_payload_json
  );
  v_operation := UPPER(BTRIM(COALESCE(
    v_resolution_payload_json->>'operation',
    v_resolution_payload_json->>'action',
    'APPLY'
  )));
  IF v_operation IN ('DISCOVER', 'DISCOVER_LINKED_SCOPE', 'PREVIEW_SCOPE', 'LIST_ELIGIBLE_LINKED') THEN
    v_operation := 'DISCOVER';
  ELSIF v_operation IN ('', 'APPLY', 'SAVE', 'APPLY_RESOLUTION') THEN
    v_operation := 'APPLY';
  ELSE
    RAISE EXCEPTION 'WORKBENCH_RESOLUTION_OPERATION_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_RESOLUTION_OPERATION_INVALID',
              'operation', v_operation,
              'message', 'The requested resolved-rate action is not valid.'
            )::text;
  END IF;

  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'session_id is required';
  END IF;

  IF p_actor_user_id IS NOT NULL THEN
    PERFORM 1
    FROM public.tms_users
    WHERE public.tms_users.id = p_actor_user_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'tms_users row % not found', p_actor_user_id;
    END IF;
  END IF;

  IF v_operation = 'DISCOVER' THEN
    SELECT public.banking_pay_workbench_sessions.*
    INTO v_session_row
    FROM public.banking_pay_workbench_sessions
    WHERE public.banking_pay_workbench_sessions.id = p_session_id;
  ELSE
    SELECT public.banking_pay_workbench_sessions.*
    INTO v_session_row
    FROM public.banking_pay_workbench_sessions
    WHERE public.banking_pay_workbench_sessions.id = p_session_id
    FOR UPDATE;
  END IF;

  IF v_session_row.id IS NULL THEN
    RAISE EXCEPTION 'banking_pay_workbench_sessions row % not found', p_session_id;
  END IF;

  IF v_session_row.status <> 'OPEN'
     OR v_session_row.discarded_at_utc IS NOT NULL
     OR v_session_row.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'OBSOLETE_SESSION'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'OBSOLETE_SESSION',
              'session_id', p_session_id::text,
              'status', COALESCE(v_session_row.status, ''),
              'replacement_session_id', CASE WHEN v_session_row.replacement_session_id IS NULL THEN NULL ELSE v_session_row.replacement_session_id::text END,
              'message', 'The Banking Pay workbench session is no longer current. Refresh the preview before saving the case resolution.'
            )::text;
  END IF;

  v_expected_session_version_text := NULLIF(BTRIM(COALESCE(
    v_resolution_payload_json->>'expected_session_version',
    v_resolution_payload_json->>'expectedSessionVersion',
    v_resolution_payload_json->>'session_version',
    v_resolution_payload_json->>'sessionVersion',
    ''
  )), '');

  v_expected_progress_counter_version_text := NULLIF(BTRIM(COALESCE(
    v_resolution_payload_json->>'expected_progress_counter_version',
    v_resolution_payload_json->>'expectedProgressCounterVersion',
    v_resolution_payload_json->>'progress_counter_version',
    v_resolution_payload_json->>'progressCounterVersion',
    ''
  )), '');

  IF v_expected_session_version_text IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_VERSION_CONTEXT_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SESSION_VERSION_CONTEXT_REQUIRED',
              'session_id', p_session_id::text,
              'current_session_version', COALESCE(v_session_row.version, 0),
              'message', 'The Banking Pay case resolution request was missing the current session version. Refresh the preview before saving.'
            )::text;
  END IF;

  IF v_expected_session_version_text !~ '^[0-9]{1,18}$' THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_VERSION_CONTEXT_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SESSION_VERSION_CONTEXT_INVALID',
              'session_id', p_session_id::text,
              'provided_session_version', v_expected_session_version_text
            )::text;
  END IF;

  v_expected_session_version := v_expected_session_version_text::bigint;
  IF v_expected_session_version < 1 THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_VERSION_CONTEXT_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'WORKBENCH_SESSION_VERSION_CONTEXT_INVALID', 'session_id', p_session_id::text)::text;
  END IF;

  IF v_expected_progress_counter_version_text IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_PROGRESS_CONTEXT_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SESSION_PROGRESS_CONTEXT_REQUIRED',
              'session_id', p_session_id::text,
              'current_progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0),
              'message', 'The Banking Pay case resolution request was missing the current progress version. Refresh the preview before saving.'
            )::text;
  END IF;

  IF v_expected_progress_counter_version_text !~ '^[0-9]{1,18}$' THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_PROGRESS_CONTEXT_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SESSION_PROGRESS_CONTEXT_INVALID',
              'session_id', p_session_id::text,
              'provided_progress_counter_version', v_expected_progress_counter_version_text
            )::text;
  END IF;

  v_expected_progress_counter_version := v_expected_progress_counter_version_text::bigint;

  IF COALESCE(v_session_row.version, 0) IS DISTINCT FROM v_expected_session_version THEN
    RAISE EXCEPTION 'STALE_SESSION'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'STALE_SESSION',
              'session_id', p_session_id::text,
              'expected_session_version', v_expected_session_version,
              'current_session_version', COALESCE(v_session_row.version, 0),
              'message', 'The Banking Pay workbench session changed in another window. Refresh the preview before saving the case resolution.'
            )::text;
  END IF;

  IF COALESCE(v_session_row.progress_counter_version, 0) IS DISTINCT FROM v_expected_progress_counter_version THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_PROGRESS_CHANGED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SESSION_PROGRESS_CHANGED',
              'session_id', p_session_id::text,
              'expected_progress_counter_version', v_expected_progress_counter_version,
              'current_progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0),
              'message', 'The Banking Pay workbench changed in another window. Refresh the preview before saving the case resolution.'
            )::text;
  END IF;

  v_candidate_id_text := BTRIM(COALESCE(
    v_resolution_payload_json->>'candidate_id',
    v_resolution_payload_json #>> '{case,candidate_id}',
    ''
  ));
  IF v_candidate_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_candidate_id := v_candidate_id_text::uuid;
  END IF;

  v_case_key := BTRIM(COALESCE(
    v_resolution_payload_json->>'case_key',
    v_resolution_payload_json #>> '{case,case_key}',
    ''
  ));
  IF v_case_key = '' THEN
    RAISE EXCEPTION 'case_key is required';
  END IF;

  v_resolution_family := UPPER(BTRIM(COALESCE(v_resolution_payload_json->>'resolution_family', v_resolution_payload_json->>'resolution_kind', '')));
  v_default_resolution_mode := UPPER(BTRIM(COALESCE(
    v_resolution_payload_json->>'resolution_mode',
    v_resolution_payload_json->>'mode',
    ''
  )));

  v_linked_timesheet_id_text := BTRIM(COALESCE(
    v_resolution_payload_json->>'linked_timesheet_id',
    v_resolution_payload_json->>'timesheet_id',
    ''
  ));
  IF v_linked_timesheet_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_linked_timesheet_id := v_linked_timesheet_id_text::uuid;
  END IF;

  v_finance_case_id_text := BTRIM(COALESCE(
    v_resolution_payload_json->>'finance_case_id',
    v_resolution_payload_json #>> '{case,finance_case_id}',
    ''
  ));

  v_resolve_all_linked_timesheets := CASE
    WHEN LOWER(BTRIM(COALESCE(v_resolution_payload_json->>'resolve_all_linked_timesheets', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN true
    ELSE false
  END;

  v_top_source_basis_json := CASE
    WHEN jsonb_typeof(v_resolution_payload_json->'source_basis_json') = 'object' THEN COALESCE(v_resolution_payload_json->'source_basis_json', '{}'::jsonb)
    ELSE '{}'::jsonb
  END;

  v_bucket_resolutions_json := CASE
    WHEN jsonb_typeof(v_resolution_payload_json->'bucket_resolutions') = 'array' THEN COALESCE(v_resolution_payload_json->'bucket_resolutions', '[]'::jsonb)
    ELSE '[]'::jsonb
  END;

  IF v_resolution_family = '' THEN
    IF jsonb_array_length(v_bucket_resolutions_json) > 0 THEN
      v_resolution_family := 'BUCKETED';
    ELSE
      v_resolution_family := 'NON_BUCKET';
    END IF;
  END IF;

  IF v_resolution_family NOT IN ('BUCKETED', 'NON_BUCKET', 'TAXABLE_CHANNEL_RESTRUCTURE') THEN
    RAISE EXCEPTION 'resolution_family must be BUCKETED, NON_BUCKET or TAXABLE_CHANNEL_RESTRUCTURE';
  END IF;

  CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_session_candidate_scope (
    candidate_id uuid PRIMARY KEY
  ) ON COMMIT DROP;

  TRUNCATE TABLE _tmp_bpay_session_candidate_scope;

  INSERT INTO _tmp_bpay_session_candidate_scope (candidate_id)
  SELECT scoped_candidate.candidate_id
  FROM unnest(COALESCE(v_session_row.scope_candidate_ids, ARRAY[]::uuid[])) AS scoped_candidate(candidate_id)
  WHERE scoped_candidate.candidate_id IS NOT NULL
  ON CONFLICT (candidate_id) DO NOTHING;

  INSERT INTO _tmp_bpay_session_candidate_scope (candidate_id)
  SELECT DISTINCT session_scope.candidate_id
  FROM public.banking_pay_workbench_session_scope AS session_scope
  WHERE session_scope.session_id = p_session_id
    AND session_scope.candidate_id IS NOT NULL
  ON CONFLICT (candidate_id) DO NOTHING;

  INSERT INTO _tmp_bpay_session_candidate_scope (candidate_id)
  SELECT DISTINCT preview_row.candidate_id
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  WHERE preview_row.session_id = p_session_id
    AND preview_row.candidate_id IS NOT NULL
  ON CONFLICT (candidate_id) DO NOTHING;

  INSERT INTO _tmp_bpay_session_candidate_scope (candidate_id)
  SELECT DISTINCT line_work.candidate_id
  FROM public.banking_pay_workbench_candidate_line_work AS line_work
  WHERE line_work.session_id = p_session_id
    AND line_work.candidate_id IS NOT NULL
  ON CONFLICT (candidate_id) DO NOTHING;

  IF v_candidate_id IS NOT NULL
     AND NOT EXISTS (
       SELECT 1
       FROM _tmp_bpay_session_candidate_scope AS candidate_scope
       WHERE candidate_scope.candidate_id = v_candidate_id
     ) THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_CANDIDATE_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SESSION_CANDIDATE_NOT_FOUND',
              'session_id', p_session_id::text,
              'candidate_id', v_candidate_id::text
            )::text;
  END IF;

  CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_session_row_backed_case_baseline (
    preview_row_id uuid PRIMARY KEY,
    candidate_id uuid NOT NULL,
    case_key text NOT NULL,
    timesheet_id uuid NULL,
    finance_case_id uuid NULL,
    resolution_family text NULL,
    row_key text NOT NULL,
    row_json jsonb NOT NULL
  ) ON COMMIT DROP;

  TRUNCATE TABLE _tmp_bpay_session_row_backed_case_baseline;

  INSERT INTO _tmp_bpay_session_row_backed_case_baseline (
    preview_row_id,
    candidate_id,
    case_key,
    timesheet_id,
    finance_case_id,
    resolution_family,
    row_key,
    row_json
  )
  WITH preview_source AS (
    SELECT
      preview_row.id AS preview_row_id,
      preview_row.candidate_id,
      preview_row.row_key,
      COALESCE(preview_row.row_json, '{}'::jsonb) AS row_json,
      BTRIM(COALESCE(
        preview_row.row_json->>'case_key',
        preview_row.row_json #>> '{case_resolution_summary,case_key}',
        preview_row.row_json #>> '{case,case_key}',
        preview_row.row_json #>> '{raw_case,case_key}',
        ''
      )) AS extracted_case_key,
      BTRIM(COALESCE(
        preview_row.timesheet_id::text,
        preview_row.row_json->>'timesheet_id',
        preview_row.row_json->>'real_business_timesheet_id',
        preview_row.row_json->>'linked_timesheet_id',
        preview_row.row_json #>> '{case,timesheet_id}',
        preview_row.row_json #>> '{raw_case,timesheet_id}',
        ''
      )) AS timesheet_id_text,
      BTRIM(COALESCE(
        preview_row.row_json->>'finance_case_id',
        preview_row.row_json #>> '{case,finance_case_id}',
        preview_row.row_json #>> '{raw_case,finance_case_id}',
        ''
      )) AS finance_case_id_text,
      UPPER(BTRIM(COALESCE(
        preview_row.row_json->>'resolution_family',
        preview_row.row_json #>> '{case_resolution_summary,resolution_family}',
        preview_row.row_json #>> '{case,resolution_family}',
        preview_row.row_json #>> '{raw_case,resolution_family}',
        ''
      ))) AS resolution_family
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_session_id
      AND LOWER(BTRIM(COALESCE(preview_row.section, ''))) = 'cases_resolutions'
      AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
  ),
  normalized_preview_source AS (
    SELECT
      preview_source.*,
      CASE
        WHEN preview_source.timesheet_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN preview_source.timesheet_id_text::uuid
        ELSE NULL::uuid
      END AS timesheet_id,
      CASE
        WHEN preview_source.finance_case_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN preview_source.finance_case_id_text::uuid
        ELSE NULL::uuid
      END AS finance_case_id
    FROM preview_source
  )
  SELECT
    normalized_preview_source.preview_row_id,
    normalized_preview_source.candidate_id,
    COALESCE(NULLIF(normalized_preview_source.extracted_case_key, ''), v_case_key),
    normalized_preview_source.timesheet_id,
    normalized_preview_source.finance_case_id,
    NULLIF(normalized_preview_source.resolution_family, ''),
    normalized_preview_source.row_key,
    normalized_preview_source.row_json
  FROM normalized_preview_source
  WHERE normalized_preview_source.candidate_id IS NOT NULL
    AND (
      normalized_preview_source.extracted_case_key = v_case_key
      OR (
        normalized_preview_source.extracted_case_key = ''
        AND v_linked_timesheet_id IS NOT NULL
        AND (
          normalized_preview_source.timesheet_id = v_linked_timesheet_id
          OR normalized_preview_source.row_key = v_linked_timesheet_id::text
          OR normalized_preview_source.row_key LIKE (v_linked_timesheet_id::text || ':%')
        )
      )
      OR (
        normalized_preview_source.extracted_case_key = ''
        AND v_finance_case_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND normalized_preview_source.finance_case_id = v_finance_case_id_text::uuid
      )
    );

  create temp table if not exists _tmp_bpay_session_case_resolution_existing
  as
  select *
  from public.banking_pay_workbench_session_case_resolutions
  with no data;

  truncate table _tmp_bpay_session_case_resolution_existing;

  IF v_resolution_family = 'TAXABLE_CHANNEL_RESTRUCTURE' THEN
    IF v_finance_case_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'finance_case_id is required for TAXABLE_CHANNEL_RESTRUCTURE resolution';
    END IF;

    v_taxable_finance_case_id := v_finance_case_id_text::uuid;

    SELECT pa.candidate_id
    INTO v_taxable_case_candidate_id
    FROM public.pay_advances AS pa
    WHERE pa.id = v_taxable_finance_case_id
      AND pa.case_type IN (
        'OVERPAYMENT'::public.pay_finance_case_type_enum,
        'UNDERPAYMENT'::public.pay_finance_case_type_enum,
        'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum,
        'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
      )
      AND pa.status = 'ACTIVE'::public.pay_advance_status_enum
    LIMIT 1;

    IF v_taxable_case_candidate_id IS NULL THEN
      RAISE EXCEPTION 'No active taxable-channel finance case found for finance_case_id %', v_taxable_finance_case_id;
    END IF;

    IF v_candidate_id IS NOT NULL AND v_candidate_id <> v_taxable_case_candidate_id THEN
      RAISE EXCEPTION 'candidate_id % does not match taxable-channel finance case candidate %', v_candidate_id, v_taxable_case_candidate_id;
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM _tmp_bpay_session_candidate_scope AS candidate_scope
      WHERE candidate_scope.candidate_id = v_taxable_case_candidate_id
    ) THEN
      RAISE EXCEPTION 'WORKBENCH_SESSION_CANDIDATE_NOT_FOUND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_SESSION_CANDIDATE_NOT_FOUND',
                'session_id', p_session_id::text,
                'candidate_id', v_taxable_case_candidate_id::text
              )::text;
    END IF;

    v_resolved_candidate_id := v_taxable_case_candidate_id;

    SELECT COUNT(*)::integer
    INTO v_row_backed_case_count
    FROM _tmp_bpay_session_row_backed_case_baseline AS row_backed_case
    WHERE COALESCE(row_backed_case.resolution_family, '') IN ('', 'TAXABLE_CHANNEL_RESTRUCTURE')
      AND (
        row_backed_case.finance_case_id IS NULL
        OR row_backed_case.finance_case_id = v_taxable_finance_case_id
      );

    IF v_row_backed_case_count > 0 THEN
      SELECT COUNT(*)::integer
      INTO v_target_match_count
      FROM _tmp_bpay_session_row_backed_case_baseline AS row_backed_case
      WHERE row_backed_case.candidate_id = v_resolved_candidate_id
        AND COALESCE(row_backed_case.resolution_family, '') IN ('', 'TAXABLE_CHANNEL_RESTRUCTURE')
        AND (
          row_backed_case.finance_case_id IS NULL
          OR row_backed_case.finance_case_id = v_taxable_finance_case_id
        );

      IF v_target_match_count = 0 THEN
        RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_CASE_BASELINE_NOT_FOUND'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'WORKBENCH_ROW_BACKED_CASE_BASELINE_NOT_FOUND',
                  'session_id', p_session_id::text,
                  'candidate_id', v_resolved_candidate_id::text,
                  'case_key', v_case_key,
                  'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE'
                )::text;
      ELSIF v_target_match_count > 1 THEN
        RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_CASE_BASELINE_AMBIGUOUS'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'WORKBENCH_ROW_BACKED_CASE_BASELINE_AMBIGUOUS',
                  'session_id', p_session_id::text,
                  'candidate_id', v_resolved_candidate_id::text,
                  'case_key', v_case_key,
                  'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE'
                )::text;
      END IF;
    ELSE
      SELECT COUNT(*)::integer
      INTO v_target_match_count
      FROM public.banking_pay_snapshot_case_state AS snapshot_case
      WHERE snapshot_case.snapshot_run_id = v_session_row.source_snapshot_run_id
        AND snapshot_case.candidate_id = v_resolved_candidate_id
        AND snapshot_case.case_key = v_case_key;

      IF v_target_match_count = 0 THEN
        RAISE EXCEPTION 'No matching snapshot baseline case found for TAXABLE_CHANNEL_RESTRUCTURE candidate % in session %', v_resolved_candidate_id, p_session_id;
      ELSIF v_target_match_count > 1 THEN
        RAISE EXCEPTION 'Ambiguous snapshot baseline case found for TAXABLE_CHANNEL_RESTRUCTURE candidate % in session %', v_resolved_candidate_id, p_session_id;
      END IF;
    END IF;

    v_taxable_resolution_path := UPPER(BTRIM(COALESCE(
      v_resolution_payload_json->>'resolution_path',
      v_resolution_payload_json->>'path',
      'SUGGESTED'
    )));
    IF v_taxable_resolution_path = '' THEN
      v_taxable_resolution_path := 'SUGGESTED';
    END IF;

    IF v_taxable_resolution_path NOT IN ('SUGGESTED', 'MANUAL') THEN
      RAISE EXCEPTION 'resolution_path must be SUGGESTED or MANUAL for TAXABLE_CHANNEL_RESTRUCTURE';
    END IF;

    v_taxable_schedule_input_mode := NULLIF(UPPER(BTRIM(COALESCE(
      v_resolution_payload_json->>'schedule_input_mode',
      v_resolution_payload_json->>'schedule_mode',
      ''
    ))), '');
    IF v_taxable_schedule_input_mode IS NOT NULL
       AND v_taxable_schedule_input_mode NOT IN ('BY_WEEKS', 'BY_WEEKLY_DUE') THEN
      RAISE EXCEPTION 'schedule_input_mode must be BY_WEEKS or BY_WEEKLY_DUE for TAXABLE_CHANNEL_RESTRUCTURE';
    END IF;

    IF NULLIF(BTRIM(COALESCE(v_resolution_payload_json->>'weeks_total', '')), '') IS NOT NULL THEN
      IF BTRIM(COALESCE(v_resolution_payload_json->>'weeks_total', '')) !~ '^\d+$' THEN
        RAISE EXCEPTION 'weeks_total must be a positive integer for TAXABLE_CHANNEL_RESTRUCTURE';
      END IF;
      v_taxable_weeks_total := (v_resolution_payload_json->>'weeks_total')::integer;
      IF v_taxable_weeks_total < 1 THEN
        RAISE EXCEPTION 'weeks_total must be >= 1 for TAXABLE_CHANNEL_RESTRUCTURE';
      END IF;
    END IF;

    IF NULLIF(BTRIM(COALESCE(v_resolution_payload_json->>'weekly_due', '')), '') IS NOT NULL THEN
      IF BTRIM(COALESCE(v_resolution_payload_json->>'weekly_due', '')) !~ '^-?[0-9]+(\.[0-9]+)?$' THEN
        RAISE EXCEPTION 'weekly_due must be numeric for TAXABLE_CHANNEL_RESTRUCTURE';
      END IF;
      v_taxable_weekly_due := round((v_resolution_payload_json->>'weekly_due')::numeric, 2);
      IF v_taxable_weekly_due <= 0 THEN
        RAISE EXCEPTION 'weekly_due must be > 0 for TAXABLE_CHANNEL_RESTRUCTURE';
      END IF;
    END IF;

    IF COALESCE(
      NULLIF(BTRIM(COALESCE(v_resolution_payload_json->>'manual_total_remaining', '')), ''),
      NULLIF(BTRIM(COALESCE(v_resolution_payload_json->>'target_remaining_balance_ex_vat', '')), '')
    ) IS NOT NULL THEN
      IF COALESCE(
        NULLIF(BTRIM(COALESCE(v_resolution_payload_json->>'manual_total_remaining', '')), ''),
        NULLIF(BTRIM(COALESCE(v_resolution_payload_json->>'target_remaining_balance_ex_vat', '')), '')
      ) !~ '^-?[0-9]+(\.[0-9]+)?$' THEN
        RAISE EXCEPTION 'manual_total_remaining must be numeric for TAXABLE_CHANNEL_RESTRUCTURE';
      END IF;
      v_taxable_manual_total_remaining := round(COALESCE(
        NULLIF(BTRIM(COALESCE(v_resolution_payload_json->>'manual_total_remaining', '')), '')::numeric,
        NULLIF(BTRIM(COALESCE(v_resolution_payload_json->>'target_remaining_balance_ex_vat', '')), '')::numeric
      ), 2);
      IF v_taxable_manual_total_remaining <= 0 THEN
        RAISE EXCEPTION 'manual_total_remaining must be > 0 for TAXABLE_CHANNEL_RESTRUCTURE';
      END IF;
    END IF;

    IF NULLIF(BTRIM(COALESCE(v_resolution_payload_json->>'effective_pay_date', '')), '') IS NOT NULL THEN
      IF BTRIM(COALESCE(v_resolution_payload_json->>'effective_pay_date', '')) !~ '^\d{4}-\d{2}-\d{2}$' THEN
        RAISE EXCEPTION 'effective_pay_date must be YYYY-MM-DD for TAXABLE_CHANNEL_RESTRUCTURE';
      END IF;
      v_taxable_effective_pay_date := (v_resolution_payload_json->>'effective_pay_date')::date;
    END IF;

    v_taxable_note := NULLIF(BTRIM(COALESCE(
      v_resolution_payload_json->>'note',
      v_resolution_payload_json->>'reason',
      ''
    )), '');

    v_taxable_result_json := public.pay_finance_case_apply_taxable_channel_restructure(
      p_finance_case_id => v_taxable_finance_case_id,
      p_actor_user_id => p_actor_user_id,
      p_resolution_path => v_taxable_resolution_path,
      p_schedule_input_mode => v_taxable_schedule_input_mode,
      p_weeks_total => v_taxable_weeks_total,
      p_weekly_due => v_taxable_weekly_due,
      p_manual_total_remaining => v_taxable_manual_total_remaining,
      p_effective_pay_date => v_taxable_effective_pay_date,
      p_note => v_taxable_note
    );

    v_resolution_identity_keys := jsonb_build_array(
      concat_ws('|', 'TAXABLE_CHANNEL_RESTRUCTURE', v_case_key, v_taxable_finance_case_id::text)
    );
    v_case_resolution_ids := '[]'::jsonb;
    v_case_resolution_id_text := NULL;
    v_action := 'TAXABLE_CHANNEL_RESTRUCTURE_APPLIED';

  ELSIF v_resolution_family = 'BUCKETED' THEN
    IF jsonb_typeof(v_bucket_resolutions_json) <> 'array' OR jsonb_array_length(v_bucket_resolutions_json) = 0 THEN
      RAISE EXCEPTION 'bucket_resolutions must be a non-empty JSON array for BUCKETED resolution';
    END IF;

    CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_session_bucket_resolution (
      bucket_ordinal integer PRIMARY KEY,
      timesheet_id uuid NULL,
      source_basis_json jsonb NOT NULL,
      source_basis_fingerprint text NOT NULL,
      source_family_key text NOT NULL,
      bucket_code text NULL,
      component_key_type text NOT NULL,
      component_key_value text NOT NULL,
      source_units numeric NOT NULL,
      source_rate numeric NOT NULL,
      source_charge_rate numeric NOT NULL,
      resolution_mode text NOT NULL,
      target_rate numeric NOT NULL,
      resolution_identity_key text NOT NULL,
      matched_candidate_id uuid NULL,
      anchor_evidence_key text NULL,
      decision_context_key text NULL,
      category_identity text NULL
    ) ON COMMIT DROP;

    TRUNCATE TABLE _tmp_bpay_session_bucket_resolution;

    FOR v_bucket_element IN
      SELECT bucket_item.ordinality AS bucket_ordinal, bucket_item.value AS bucket_json
      FROM jsonb_array_elements(v_bucket_resolutions_json) WITH ORDINALITY AS bucket_item(value, ordinality)
      WHERE jsonb_typeof(bucket_item.value) = 'object'
      ORDER BY bucket_item.ordinality
    LOOP
      v_bucket_source_basis_json := CASE
        WHEN jsonb_typeof(v_bucket_element.bucket_json->'source_basis_json') = 'object' THEN COALESCE(v_bucket_element.bucket_json->'source_basis_json', '{}'::jsonb)
        WHEN jsonb_typeof(v_resolution_payload_json->'source_basis_json') = 'object' THEN COALESCE(v_resolution_payload_json->'source_basis_json', '{}'::jsonb)
        ELSE '{}'::jsonb
      END;

      v_bucket_source_basis_fingerprint := BTRIM(COALESCE(
        v_bucket_element.bucket_json->>'source_basis_fingerprint',
        v_resolution_payload_json->>'source_basis_fingerprint',
        ''
      ));
      IF v_bucket_source_basis_fingerprint = '' AND v_bucket_source_basis_json <> '{}'::jsonb THEN
        v_bucket_source_basis_fingerprint := md5(v_bucket_source_basis_json::text);
      END IF;

      v_bucket_source_family_key := BTRIM(COALESCE(
        v_bucket_element.bucket_json->>'source_family_key',
        v_resolution_payload_json->>'source_family_key',
        ''
      ));
      v_bucket_bucket_code := UPPER(BTRIM(COALESCE(
        v_bucket_element.bucket_json->>'bucket_code',
        v_resolution_payload_json->>'bucket_code',
        ''
      )));
      v_bucket_component_key_type := UPPER(BTRIM(COALESCE(
        v_bucket_element.bucket_json->>'component_key_type',
        v_resolution_payload_json->>'component_key_type',
        ''
      )));
      v_bucket_component_key_value := BTRIM(COALESCE(
        v_bucket_element.bucket_json->>'component_key_value',
        v_resolution_payload_json->>'component_key_value',
        ''
      ));
      v_bucket_resolution_mode := UPPER(BTRIM(COALESCE(
        v_bucket_element.bucket_json->>'resolution_mode',
        v_default_resolution_mode,
        ''
      )));
      v_bucket_timesheet_id_text := BTRIM(COALESCE(
        v_bucket_element.bucket_json->>'timesheet_id',
        v_resolution_payload_json->>'linked_timesheet_id',
        v_resolution_payload_json->>'timesheet_id',
        ''
      ));
      v_bucket_timesheet_id := NULL::uuid;
      IF v_bucket_timesheet_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        v_bucket_timesheet_id := v_bucket_timesheet_id_text::uuid;
      END IF;

      IF COALESCE(
        v_bucket_element.bucket_json->>'source_units',
        v_resolution_payload_json->>'source_units',
        ''
      ) !~ '^-?[0-9]+(\.[0-9]+)?$' THEN
        RAISE EXCEPTION 'source_units is required for BUCKETED resolution bucket %', v_bucket_element.bucket_ordinal;
      END IF;
      v_bucket_source_units := round(COALESCE(
        NULLIF(v_bucket_element.bucket_json->>'source_units', '')::numeric,
        NULLIF(v_resolution_payload_json->>'source_units', '')::numeric
      ), 6);

      IF COALESCE(
        v_bucket_element.bucket_json->>'source_rate',
        v_resolution_payload_json->>'source_rate',
        ''
      ) !~ '^-?[0-9]+(\.[0-9]+)?$' THEN
        RAISE EXCEPTION 'source_rate is required for BUCKETED resolution bucket %', v_bucket_element.bucket_ordinal;
      END IF;
      v_bucket_source_rate := round(COALESCE(
        NULLIF(v_bucket_element.bucket_json->>'source_rate', '')::numeric,
        NULLIF(v_resolution_payload_json->>'source_rate', '')::numeric
      ), 6);

      IF COALESCE(
        v_bucket_element.bucket_json->>'source_charge_rate',
        v_resolution_payload_json->>'source_charge_rate',
        ''
      ) !~ '^-?[0-9]+(\.[0-9]+)?$' THEN
        RAISE EXCEPTION 'source_charge_rate is required for BUCKETED resolution bucket %', v_bucket_element.bucket_ordinal;
      END IF;
      v_bucket_source_charge_rate := round(COALESCE(
        NULLIF(v_bucket_element.bucket_json->>'source_charge_rate', '')::numeric,
        NULLIF(v_resolution_payload_json->>'source_charge_rate', '')::numeric
      ), 6);

      IF COALESCE(
        v_bucket_element.bucket_json->>'target_rate',
        v_resolution_payload_json->>'target_rate',
        ''
      ) !~ '^-?[0-9]+(\.[0-9]+)?$' THEN
        RAISE EXCEPTION 'target_rate is required for BUCKETED resolution bucket %', v_bucket_element.bucket_ordinal;
      END IF;
      v_bucket_target_rate := round(COALESCE(
        NULLIF(v_bucket_element.bucket_json->>'target_rate', '')::numeric,
        NULLIF(v_resolution_payload_json->>'target_rate', '')::numeric
      ), 2);

      IF v_bucket_target_rate < 0 THEN
        RAISE EXCEPTION 'target_rate must be non-negative for BUCKETED resolution bucket %', v_bucket_element.bucket_ordinal;
      END IF;

      IF v_bucket_resolution_mode NOT IN ('SUGGESTED_EQUIVALENT_BASIS', 'MANUAL_REPLACEMENT_RATE') THEN
        RAISE EXCEPTION 'BUCKETED resolution_mode must be SUGGESTED_EQUIVALENT_BASIS or MANUAL_REPLACEMENT_RATE for bucket %', v_bucket_element.bucket_ordinal;
      END IF;

      IF v_bucket_source_family_key = '' THEN
        RAISE EXCEPTION 'source_family_key is required for BUCKETED resolution bucket %', v_bucket_element.bucket_ordinal;
      END IF;

      IF v_bucket_component_key_type = '' THEN
        RAISE EXCEPTION 'component_key_type is required for BUCKETED resolution bucket %', v_bucket_element.bucket_ordinal;
      END IF;

      IF v_bucket_component_key_value = '' THEN
        RAISE EXCEPTION 'component_key_value is required for BUCKETED resolution bucket %', v_bucket_element.bucket_ordinal;
      END IF;

      IF v_bucket_source_basis_fingerprint = '' THEN
        RAISE EXCEPTION 'source_basis_fingerprint or source_basis_json is required for BUCKETED resolution bucket %', v_bucket_element.bucket_ordinal;
      END IF;

      IF v_bucket_component_key_type IN ('TS_DAY', 'TS_TOTAL') AND v_bucket_bucket_code = '' THEN
        RAISE EXCEPTION 'bucket_code is required for BUCKETED worked-time resolution bucket %', v_bucket_element.bucket_ordinal;
      END IF;

      IF v_bucket_component_key_type = 'TS_DAY'
         AND v_bucket_component_key_value !~ '^\d{4}-\d{2}-\d{2}$' THEN
        RAISE EXCEPTION 'Invalid TS_DAY economic key in BUCKETED resolution bucket %', v_bucket_element.bucket_ordinal;
      END IF;

      IF v_bucket_source_family_key LIKE 'correction-chain:%' THEN
        v_bucket_resolution_identity_key :=
          public._ctms_correction_carrier_identity_v1(
            v_candidate_id,
            NULLIF(
              COALESCE(
                v_bucket_element.bucket_json->>'correction_root_id',
                v_resolution_payload_json->>'correction_root_id'
              ),
              ''
            )::uuid,
            v_bucket_component_key_type,
            v_bucket_component_key_value
          );

        IF NULLIF(
          COALESCE(
            v_bucket_element.bucket_json->>'canonical_correction_key',
            v_resolution_payload_json->>'canonical_correction_key'
          ),
          ''
        ) IS DISTINCT FROM v_bucket_resolution_identity_key THEN
          RAISE EXCEPTION 'CORRECTION_CARRIER_IDENTITY_MISMATCH'
            USING ERRCODE='P0001',
                  DETAIL=jsonb_build_object(
                    'expected_identity',v_bucket_resolution_identity_key,
                    'provided_identity',COALESCE(
                      v_bucket_element.bucket_json->>'canonical_correction_key',
                      v_resolution_payload_json->>'canonical_correction_key'
                    )
                  )::text;
        END IF;
      ELSE
        v_bucket_resolution_identity_key := concat_ws(
          '|',
          'BUCKETED',
          COALESCE(NULLIF(v_case_key, ''), '~'),
          COALESCE(CASE WHEN v_bucket_timesheet_id IS NULL THEN NULL ELSE v_bucket_timesheet_id::text END, '~'),
          COALESCE(NULLIF(v_bucket_source_basis_fingerprint, ''), '~'),
          COALESCE(NULLIF(v_bucket_source_family_key, ''), '~'),
          COALESCE(NULLIF(v_bucket_bucket_code, ''), '~'),
          COALESCE(NULLIF(v_bucket_component_key_type, ''), '~'),
          COALESCE(NULLIF(v_bucket_component_key_value, ''), '~')
        );
      END IF;

      INSERT INTO _tmp_bpay_session_bucket_resolution (
        bucket_ordinal,
        timesheet_id,
        source_basis_json,
        source_basis_fingerprint,
        source_family_key,
        bucket_code,
        component_key_type,
        component_key_value,
        source_units,
        source_rate,
        source_charge_rate,
        resolution_mode,
        target_rate,
        resolution_identity_key,
        matched_candidate_id,
        anchor_evidence_key,
        decision_context_key,
        category_identity
      )
      VALUES (
        v_bucket_element.bucket_ordinal,
        v_bucket_timesheet_id,
        v_bucket_source_basis_json,
        v_bucket_source_basis_fingerprint,
        v_bucket_source_family_key,
        NULLIF(v_bucket_bucket_code, ''),
        v_bucket_component_key_type,
        v_bucket_component_key_value,
        v_bucket_source_units,
        v_bucket_source_rate,
        v_bucket_source_charge_rate,
        v_bucket_resolution_mode,
        v_bucket_target_rate,
        v_bucket_resolution_identity_key,
        NULL::uuid,
        NULL::text,
        NULL::text,
        NULL::text
      );
    END LOOP;

    SELECT COUNT(*)::integer
    INTO v_bucket_count
    FROM _tmp_bpay_session_bucket_resolution AS input_bucket;

    IF v_bucket_count = 0 THEN
      RAISE EXCEPTION 'bucket_resolutions must contain at least one JSON object for BUCKETED resolution';
    END IF;

    IF EXISTS (
      SELECT 1
      FROM _tmp_bpay_session_bucket_resolution AS duplicate_bucket
      GROUP BY duplicate_bucket.resolution_identity_key
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Duplicate bucket resolution identity detected for case_key %', v_case_key;
    END IF;

    CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_session_bucket_component_raw (
      evidence_key text PRIMARY KEY,
      evidence_source text NOT NULL,
      candidate_id uuid NOT NULL,
      case_key text NOT NULL,
      timesheet_id uuid NOT NULL,
      case_client_id uuid NULL,
      source_basis_json jsonb NOT NULL,
      source_basis_fingerprint text NOT NULL,
      source_family_key text NOT NULL,
      bucket_code text NULL,
      component_key_type text NOT NULL,
      component_key_value text NOT NULL,
      component_fingerprint text NULL,
      classification text NOT NULL,
      source_pay_method text NOT NULL,
      target_pay_method text NOT NULL,
      source_units numeric NOT NULL,
      source_rate numeric NOT NULL,
      source_charge_rate numeric NOT NULL,
      source_pay_ex_vat numeric NULL,
      source_charge_ex_vat numeric NULL,
      source_margin_ex_vat numeric NULL,
      suggested_target_rate numeric NULL,
      suggested_target_charge_rate numeric NULL,
      relevant_erni_pct numeric NULL,
      vat_rate_pct numeric NULL,
      umbrella_vat_chargeable boolean NULL,
      charge_basis text NULL,
      taxability_context text NULL,
      gross_net_context text NULL,
      reuse_mode text NULL,
      suggested_resolution_payload_json jsonb NOT NULL,
      suggested_resolution_result_json jsonb NOT NULL,
      requires_resolution boolean NOT NULL,
      is_actionable_resolution_row boolean NOT NULL,
      component_state_json jsonb NOT NULL
    ) ON COMMIT DROP;

    TRUNCATE TABLE _tmp_bpay_session_bucket_component_raw;

    INSERT INTO _tmp_bpay_session_bucket_component_raw (
      evidence_key,
      evidence_source,
      candidate_id,
      case_key,
      timesheet_id,
      case_client_id,
      source_basis_json,
      source_basis_fingerprint,
      source_family_key,
      bucket_code,
      component_key_type,
      component_key_value,
      component_fingerprint,
      classification,
      source_pay_method,
      target_pay_method,
      source_units,
      source_rate,
      source_charge_rate,
      source_pay_ex_vat,
      source_charge_ex_vat,
      source_margin_ex_vat,
      suggested_target_rate,
      suggested_target_charge_rate,
      relevant_erni_pct,
      vat_rate_pct,
      umbrella_vat_chargeable,
      charge_basis,
      taxability_context,
      gross_net_context,
      reuse_mode,
      suggested_resolution_payload_json,
      suggested_resolution_result_json,
      requires_resolution,
      is_actionable_resolution_row,
      component_state_json
    )
    WITH preview_source AS (
      SELECT
        preview_row.id AS preview_row_id,
        preview_row.candidate_id,
        preview_row.row_key,
        preview_row.timesheet_id AS preview_timesheet_id,
        COALESCE(preview_row.row_json, '{}'::jsonb) AS row_json,
        BTRIM(COALESCE(
          preview_row.row_json->>'case_key',
          preview_row.row_json #>> '{case_resolution_summary,case_key}',
          preview_row.row_json #>> '{case,case_key}',
          preview_row.row_json #>> '{raw_case,case_key}',
          ''
        )) AS extracted_case_key,
        BTRIM(COALESCE(
          preview_row.timesheet_id::text,
          preview_row.row_json->>'timesheet_id',
          preview_row.row_json->>'real_business_timesheet_id',
          preview_row.row_json->>'linked_timesheet_id',
          preview_row.row_json #>> '{case,timesheet_id}',
          preview_row.row_json #>> '{raw_case,timesheet_id}',
          ''
        )) AS row_timesheet_id_text,
        BTRIM(COALESCE(
          preview_row.row_json->>'client_id',
          preview_row.row_json #>> '{case,client_id}',
          preview_row.row_json #>> '{raw_case,client_id}',
          ''
        )) AS case_client_id_text,
        UPPER(BTRIM(COALESCE(
          preview_row.row_json->>'resolution_family',
          preview_row.row_json #>> '{case_resolution_summary,resolution_family}',
          preview_row.row_json #>> '{case,resolution_family}',
          preview_row.row_json #>> '{raw_case,resolution_family}',
          ''
        ))) AS resolution_family
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.session_id = p_session_id
        AND preview_row.session_version = COALESCE(v_session_row.version, 1)
        AND (v_candidate_id IS NULL OR preview_row.candidate_id = v_candidate_id)
        AND LOWER(BTRIM(COALESCE(preview_row.section, ''))) = 'cases_resolutions'
        AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
        AND EXISTS (
          SELECT 1
          FROM _tmp_bpay_session_candidate_scope AS candidate_scope
          WHERE candidate_scope.candidate_id = preview_row.candidate_id
        )
    ), normalized_preview AS (
      SELECT
        preview_source.preview_row_id,
        preview_source.candidate_id,
        preview_source.row_key,
        preview_source.row_json,
        CASE
          WHEN preview_source.row_timesheet_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN preview_source.row_timesheet_id_text::uuid
          ELSE NULL::uuid
        END AS row_timesheet_id,
        CASE
          WHEN preview_source.case_client_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN preview_source.case_client_id_text::uuid
          ELSE NULL::uuid
        END AS case_client_id,
        COALESCE(
          NULLIF(preview_source.extracted_case_key, ''),
          CASE
            WHEN preview_source.row_timesheet_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              THEN 'timesheet:' || preview_source.row_timesheet_id_text
            ELSE NULL::text
          END,
          ''
        ) AS case_key,
        preview_source.resolution_family
      FROM preview_source
    ), case_component_arrays AS (
      SELECT
        normalized_preview.preview_row_id,
        normalized_preview.candidate_id,
        normalized_preview.case_key,
        normalized_preview.row_timesheet_id,
        normalized_preview.case_client_id,
        normalized_preview.row_json,
        CASE
          WHEN jsonb_typeof(normalized_preview.row_json->'case_components') = 'array' THEN normalized_preview.row_json->'case_components'
          WHEN jsonb_typeof(normalized_preview.row_json->'components') = 'array' THEN normalized_preview.row_json->'components'
          WHEN jsonb_typeof(normalized_preview.row_json->'component_states') = 'array' THEN normalized_preview.row_json->'component_states'
          WHEN jsonb_typeof(normalized_preview.row_json->'component_resolution_states') = 'array' THEN normalized_preview.row_json->'component_resolution_states'
          WHEN jsonb_typeof(normalized_preview.row_json->'preview_components') = 'array' THEN normalized_preview.row_json->'preview_components'
          WHEN jsonb_typeof(normalized_preview.row_json #> '{case,case_components}') = 'array' THEN normalized_preview.row_json #> '{case,case_components}'
          WHEN jsonb_typeof(normalized_preview.row_json #> '{case,components}') = 'array' THEN normalized_preview.row_json #> '{case,components}'
          WHEN jsonb_typeof(normalized_preview.row_json #> '{raw_case,case_components}') = 'array' THEN normalized_preview.row_json #> '{raw_case,case_components}'
          WHEN jsonb_typeof(normalized_preview.row_json #> '{raw_case,components}') = 'array' THEN normalized_preview.row_json #> '{raw_case,components}'
          ELSE '[]'::jsonb
        END AS components_json
      FROM normalized_preview
      WHERE normalized_preview.case_key <> ''
        AND COALESCE(normalized_preview.resolution_family, '') IN ('', 'BUCKETED')
    ), expanded_components AS (
      SELECT
        case_component_arrays.preview_row_id,
        component_element.ordinality::integer AS component_ordinal,
        case_component_arrays.candidate_id,
        case_component_arrays.case_key,
        case_component_arrays.row_timesheet_id,
        case_component_arrays.case_client_id,
        component_element.value AS component_json
      FROM case_component_arrays
      CROSS JOIN LATERAL jsonb_array_elements(case_component_arrays.components_json) WITH ORDINALITY AS component_element(value, ordinality)
      WHERE jsonb_typeof(component_element.value) = 'object'
    ), components_with_basis AS (
      SELECT
        expanded_components.*,
        CASE
          WHEN jsonb_typeof(expanded_components.component_json->'source_basis_json') = 'object' THEN COALESCE(expanded_components.component_json->'source_basis_json', '{}'::jsonb)
          WHEN jsonb_typeof(expanded_components.component_json->'frozen_source_basis_json') = 'object' THEN COALESCE(expanded_components.component_json->'frozen_source_basis_json', '{}'::jsonb)
          WHEN jsonb_typeof(expanded_components.component_json #> '{raw,source_basis_json}') = 'object' THEN COALESCE(expanded_components.component_json #> '{raw,source_basis_json}', '{}'::jsonb)
          ELSE '{}'::jsonb
        END AS source_basis_json,
        CASE
          WHEN jsonb_typeof(expanded_components.component_json->'suggested_resolution_payload_json') = 'object' THEN COALESCE(expanded_components.component_json->'suggested_resolution_payload_json', '{}'::jsonb)
          WHEN jsonb_typeof(expanded_components.component_json #> '{raw,suggested_resolution_payload_json}') = 'object' THEN COALESCE(expanded_components.component_json #> '{raw,suggested_resolution_payload_json}', '{}'::jsonb)
          ELSE '{}'::jsonb
        END AS suggested_resolution_payload_json,
        CASE
          WHEN jsonb_typeof(expanded_components.component_json->'suggested_resolution_result_json') = 'object' THEN COALESCE(expanded_components.component_json->'suggested_resolution_result_json', '{}'::jsonb)
          WHEN jsonb_typeof(expanded_components.component_json #> '{raw,suggested_resolution_result_json}') = 'object' THEN COALESCE(expanded_components.component_json #> '{raw,suggested_resolution_result_json}', '{}'::jsonb)
          ELSE '{}'::jsonb
        END AS suggested_resolution_result_json
      FROM expanded_components
    ), normalized_components AS (
      SELECT
        components_with_basis.*,
        BTRIM(COALESCE(
          components_with_basis.row_timesheet_id::text,
          components_with_basis.component_json->>'timesheet_id',
          components_with_basis.source_basis_json->>'timesheet_id',
          components_with_basis.component_json #>> '{raw,timesheet_id}',
          components_with_basis.component_json #>> '{raw,source_basis_json,timesheet_id}',
          ''
        )) AS timesheet_id_text,
        BTRIM(COALESCE(
          components_with_basis.component_json->>'source_basis_fingerprint',
          components_with_basis.component_json #>> '{raw,source_basis_fingerprint}',
          CASE WHEN components_with_basis.source_basis_json <> '{}'::jsonb THEN md5(components_with_basis.source_basis_json::text) ELSE NULL::text END,
          ''
        )) AS source_basis_fingerprint,
        BTRIM(COALESCE(
          components_with_basis.component_json->>'source_family_key',
          components_with_basis.component_json #>> '{raw,source_family_key}',
          components_with_basis.component_json->>'frozen_source_family_key',
          ''
        )) AS source_family_key,
        UPPER(BTRIM(COALESCE(
          components_with_basis.component_json->>'bucket_code',
          components_with_basis.source_basis_json->>'bucket_code',
          components_with_basis.component_json #>> '{raw,bucket_code}',
          components_with_basis.component_json #>> '{raw,source_basis_json,bucket_code}',
          ''
        ))) AS bucket_code,
        UPPER(BTRIM(COALESCE(
          components_with_basis.component_json->>'component_key_type',
          components_with_basis.component_json->>'frozen_component_key_type',
          components_with_basis.component_json #>> '{raw,component_key_type}',
          ''
        ))) AS component_key_type,
        BTRIM(COALESCE(
          components_with_basis.component_json->>'component_key_value',
          components_with_basis.component_json->>'frozen_component_key_value',
          components_with_basis.component_json #>> '{raw,component_key_value}',
          ''
        )) AS component_key_value,
        NULLIF(BTRIM(COALESCE(
          components_with_basis.component_json->>'component_fingerprint',
          components_with_basis.component_json #>> '{raw,component_fingerprint}',
          ''
        )), '') AS component_fingerprint,
        UPPER(BTRIM(COALESCE(
          components_with_basis.component_json->>'classification',
          components_with_basis.component_json->>'frozen_component_classification',
          components_with_basis.component_json #>> '{raw,classification}',
          ''
        ))) AS classification,
        UPPER(BTRIM(COALESCE(
          components_with_basis.component_json->>'source_pay_method',
          components_with_basis.source_basis_json->>'source_pay_method',
          components_with_basis.component_json #>> '{raw,source_pay_method}',
          ''
        ))) AS source_pay_method,
        UPPER(BTRIM(COALESCE(
          components_with_basis.component_json->>'current_target_pay_method',
          components_with_basis.component_json->>'target_pay_method',
          components_with_basis.suggested_resolution_payload_json->>'target_pay_method',
          components_with_basis.suggested_resolution_result_json->>'target_pay_method',
          components_with_basis.component_json #>> '{raw,current_target_pay_method}',
          components_with_basis.component_json #>> '{raw,target_pay_method}',
          ''
        ))) AS target_pay_method,
        COALESCE(
          components_with_basis.component_json->>'source_units',
          components_with_basis.source_basis_json->>'source_units',
          components_with_basis.component_json #>> '{raw,source_units}',
          ''
        ) AS source_units_text,
        COALESCE(
          components_with_basis.component_json->>'source_rate',
          components_with_basis.source_basis_json->>'source_rate',
          components_with_basis.component_json #>> '{raw,source_rate}',
          ''
        ) AS source_rate_text,
        COALESCE(
          components_with_basis.component_json->>'source_charge_rate',
          components_with_basis.source_basis_json->>'source_charge_rate',
          components_with_basis.component_json #>> '{raw,source_charge_rate}',
          ''
        ) AS source_charge_rate_text,
        COALESCE(
          components_with_basis.component_json->>'source_pay_ex_vat',
          components_with_basis.component_json->>'component_amount_ex_vat',
          components_with_basis.component_json #>> '{raw,source_pay_ex_vat}',
          ''
        ) AS source_pay_ex_vat_text,
        COALESCE(
          components_with_basis.component_json->>'source_charge_ex_vat',
          components_with_basis.component_json #>> '{raw,source_charge_ex_vat}',
          ''
        ) AS source_charge_ex_vat_text,
        COALESCE(
          components_with_basis.component_json->>'source_margin_ex_vat',
          components_with_basis.component_json #>> '{raw,source_margin_ex_vat}',
          ''
        ) AS source_margin_ex_vat_text,
        COALESCE(
          components_with_basis.suggested_resolution_payload_json->>'suggested_target_rate',
          components_with_basis.suggested_resolution_result_json->>'replacement_rate',
          components_with_basis.component_json->>'target_rate',
          components_with_basis.component_json #>> '{raw,target_rate}',
          ''
        ) AS suggested_target_rate_text,
        COALESCE(
          components_with_basis.suggested_resolution_result_json->>'target_charge_rate',
          components_with_basis.suggested_resolution_payload_json->>'target_charge_rate',
          components_with_basis.component_json->>'target_charge_rate',
          ''
        ) AS suggested_target_charge_rate_text,
        COALESCE(
          components_with_basis.suggested_resolution_payload_json->>'relevant_erni_pct',
          components_with_basis.suggested_resolution_result_json->>'relevant_erni_pct',
          ''
        ) AS relevant_erni_pct_text,
        COALESCE(
          components_with_basis.suggested_resolution_payload_json->>'vat_rate_pct',
          components_with_basis.suggested_resolution_result_json->>'vat_rate_pct',
          ''
        ) AS vat_rate_pct_text,
        COALESCE(
          components_with_basis.suggested_resolution_payload_json->>'umbrella_vat_chargeable',
          components_with_basis.suggested_resolution_result_json->>'umbrella_vat_chargeable',
          components_with_basis.component_json->>'umbrella_vat_chargeable',
          ''
        ) AS umbrella_vat_chargeable_text,
        NULLIF(BTRIM(COALESCE(
          components_with_basis.suggested_resolution_payload_json->>'target_charge_basis',
          components_with_basis.suggested_resolution_result_json->>'target_charge_basis',
          components_with_basis.suggested_resolution_payload_json->>'charge_basis',
          components_with_basis.suggested_resolution_result_json->>'charge_basis',
          ''
        )), '') AS charge_basis,
        NULLIF(UPPER(BTRIM(COALESCE(
          components_with_basis.component_json->>'taxability',
          components_with_basis.source_basis_json->>'taxability',
          components_with_basis.component_json #>> '{raw,taxability}',
          ''
        ))), '') AS taxability_context,
        NULLIF(UPPER(BTRIM(COALESCE(
          components_with_basis.component_json->>'gross_net_handling',
          components_with_basis.source_basis_json->>'gross_net_handling',
          components_with_basis.component_json #>> '{raw,gross_net_handling}',
          ''
        ))), '') AS gross_net_context,
        NULLIF(UPPER(BTRIM(COALESCE(
          components_with_basis.suggested_resolution_payload_json->>'reuse_mode',
          components_with_basis.suggested_resolution_result_json->>'reuse_mode',
          ''
        ))), '') AS reuse_mode,
        LOWER(BTRIM(COALESCE(components_with_basis.component_json->>'requires_resolution', ''))) AS requires_resolution_text,
        LOWER(BTRIM(COALESCE(components_with_basis.component_json->>'is_actionable_resolution_row', ''))) AS is_actionable_resolution_row_text
      FROM components_with_basis
    ), typed_components AS (
      SELECT
        normalized_components.*,
        CASE
          WHEN normalized_components.timesheet_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN normalized_components.timesheet_id_text::uuid
          ELSE NULL::uuid
        END AS timesheet_id,
        CASE WHEN normalized_components.source_units_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_components.source_units_text::numeric, 6) ELSE NULL::numeric END AS source_units,
        CASE WHEN normalized_components.source_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_components.source_rate_text::numeric, 6) ELSE NULL::numeric END AS source_rate,
        CASE WHEN normalized_components.source_charge_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_components.source_charge_rate_text::numeric, 6) ELSE NULL::numeric END AS source_charge_rate,
        CASE WHEN normalized_components.source_pay_ex_vat_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_components.source_pay_ex_vat_text::numeric, 2) ELSE NULL::numeric END AS source_pay_ex_vat,
        CASE WHEN normalized_components.source_charge_ex_vat_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_components.source_charge_ex_vat_text::numeric, 2) ELSE NULL::numeric END AS source_charge_ex_vat,
        CASE WHEN normalized_components.source_margin_ex_vat_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_components.source_margin_ex_vat_text::numeric, 2) ELSE NULL::numeric END AS source_margin_ex_vat,
        CASE WHEN normalized_components.suggested_target_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_components.suggested_target_rate_text::numeric, 2) ELSE NULL::numeric END AS suggested_target_rate,
        CASE WHEN normalized_components.suggested_target_charge_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_components.suggested_target_charge_rate_text::numeric, 6) ELSE NULL::numeric END AS suggested_target_charge_rate,
        CASE WHEN normalized_components.relevant_erni_pct_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_components.relevant_erni_pct_text::numeric, 6) ELSE NULL::numeric END AS relevant_erni_pct,
        CASE WHEN normalized_components.vat_rate_pct_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_components.vat_rate_pct_text::numeric, 6) ELSE NULL::numeric END AS vat_rate_pct,
        CASE
          WHEN normalized_components.umbrella_vat_chargeable_text = '' THEN NULL::boolean
          WHEN LOWER(normalized_components.umbrella_vat_chargeable_text) IN ('true', 't', '1', 'yes', 'y', 'on') THEN true
          ELSE false
        END AS umbrella_vat_chargeable,
        CASE
          WHEN normalized_components.requires_resolution_text = '' THEN (
            normalized_components.classification = 'TAXABLE_CHANNEL_SENSITIVE'
            AND normalized_components.source_pay_method <> ''
            AND normalized_components.target_pay_method <> ''
            AND normalized_components.source_pay_method IS DISTINCT FROM normalized_components.target_pay_method
          )
          ELSE normalized_components.requires_resolution_text IN ('true', 't', '1', 'yes', 'y', 'on')
        END AS requires_resolution,
        CASE
          WHEN normalized_components.is_actionable_resolution_row_text = '' THEN (
            normalized_components.source_units_text ~ '^-?[0-9]+(\.[0-9]+)?$'
            AND normalized_components.source_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$'
            AND normalized_components.source_charge_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$'
            AND normalized_components.component_key_type <> 'ADJUSTMENT_CODE'
          )
          ELSE normalized_components.is_actionable_resolution_row_text IN ('true', 't', '1', 'yes', 'y', 'on')
        END AS is_actionable_resolution_row
      FROM normalized_components
    )
    SELECT
      'ROW|' || typed_components.preview_row_id::text || '|' || typed_components.component_ordinal::text,
      'ROW_BACKED',
      typed_components.candidate_id,
      COALESCE(NULLIF(typed_components.case_key, ''), 'timesheet:' || typed_components.timesheet_id::text),
      typed_components.timesheet_id,
      typed_components.case_client_id,
      typed_components.source_basis_json,
      typed_components.source_basis_fingerprint,
      typed_components.source_family_key,
      NULLIF(typed_components.bucket_code, ''),
      typed_components.component_key_type,
      typed_components.component_key_value,
      typed_components.component_fingerprint,
      typed_components.classification,
      typed_components.source_pay_method,
      typed_components.target_pay_method,
      typed_components.source_units,
      typed_components.source_rate,
      typed_components.source_charge_rate,
      typed_components.source_pay_ex_vat,
      typed_components.source_charge_ex_vat,
      typed_components.source_margin_ex_vat,
      typed_components.suggested_target_rate,
      typed_components.suggested_target_charge_rate,
      typed_components.relevant_erni_pct,
      typed_components.vat_rate_pct,
      typed_components.umbrella_vat_chargeable,
      typed_components.charge_basis,
      typed_components.taxability_context,
      typed_components.gross_net_context,
      typed_components.reuse_mode,
      typed_components.suggested_resolution_payload_json,
      typed_components.suggested_resolution_result_json,
      typed_components.requires_resolution,
      typed_components.is_actionable_resolution_row,
      typed_components.component_json
    FROM typed_components
    WHERE typed_components.timesheet_id IS NOT NULL
      AND typed_components.source_basis_fingerprint <> ''
      AND typed_components.source_family_key <> ''
      AND typed_components.component_key_type <> ''
      AND typed_components.component_key_value <> ''
      AND typed_components.source_units IS NOT NULL
      AND typed_components.source_rate IS NOT NULL
      AND typed_components.source_charge_rate IS NOT NULL
    ON CONFLICT (evidence_key) DO NOTHING;

    SELECT COUNT(*)::integer
    INTO v_row_backed_component_count
    FROM _tmp_bpay_session_bucket_component_raw AS raw_component
    WHERE raw_component.evidence_source = 'ROW_BACKED'
      AND raw_component.case_key = v_case_key
      AND (v_candidate_id IS NULL OR raw_component.candidate_id = v_candidate_id)
      AND (v_linked_timesheet_id IS NULL OR raw_component.timesheet_id = v_linked_timesheet_id);

    IF v_row_backed_component_count = 0 THEN
      TRUNCATE TABLE _tmp_bpay_session_bucket_component_raw;

      INSERT INTO _tmp_bpay_session_bucket_component_raw (
        evidence_key,
        evidence_source,
        candidate_id,
        case_key,
        timesheet_id,
        case_client_id,
        source_basis_json,
        source_basis_fingerprint,
        source_family_key,
        bucket_code,
        component_key_type,
        component_key_value,
        component_fingerprint,
        classification,
        source_pay_method,
        target_pay_method,
        source_units,
        source_rate,
        source_charge_rate,
        source_pay_ex_vat,
        source_charge_ex_vat,
        source_margin_ex_vat,
        suggested_target_rate,
        suggested_target_charge_rate,
        relevant_erni_pct,
        vat_rate_pct,
        umbrella_vat_chargeable,
        charge_basis,
        taxability_context,
        gross_net_context,
        reuse_mode,
        suggested_resolution_payload_json,
        suggested_resolution_result_json,
        requires_resolution,
        is_actionable_resolution_row,
        component_state_json
      )
      WITH snapshot_source AS (
        SELECT
          snapshot_component.id AS snapshot_component_id,
          snapshot_component.candidate_id,
          snapshot_component.case_key,
          snapshot_component.timesheet_id,
          COALESCE(snapshot_component.component_state_json, '{}'::jsonb) AS component_json,
          CASE
            WHEN jsonb_typeof(snapshot_component.component_state_json->'source_basis_json') = 'object' THEN COALESCE(snapshot_component.component_state_json->'source_basis_json', '{}'::jsonb)
            WHEN jsonb_typeof(snapshot_component.component_state_json->'frozen_source_basis_json') = 'object' THEN COALESCE(snapshot_component.component_state_json->'frozen_source_basis_json', '{}'::jsonb)
            WHEN jsonb_typeof(snapshot_component.component_state_json #> '{raw,source_basis_json}') = 'object' THEN COALESCE(snapshot_component.component_state_json #> '{raw,source_basis_json}', '{}'::jsonb)
            ELSE '{}'::jsonb
          END AS source_basis_json,
          CASE
            WHEN jsonb_typeof(snapshot_component.component_state_json->'suggested_resolution_payload_json') = 'object' THEN COALESCE(snapshot_component.component_state_json->'suggested_resolution_payload_json', '{}'::jsonb)
            WHEN jsonb_typeof(snapshot_component.component_state_json #> '{raw,suggested_resolution_payload_json}') = 'object' THEN COALESCE(snapshot_component.component_state_json #> '{raw,suggested_resolution_payload_json}', '{}'::jsonb)
            ELSE '{}'::jsonb
          END AS suggested_resolution_payload_json,
          CASE
            WHEN jsonb_typeof(snapshot_component.component_state_json->'suggested_resolution_result_json') = 'object' THEN COALESCE(snapshot_component.component_state_json->'suggested_resolution_result_json', '{}'::jsonb)
            WHEN jsonb_typeof(snapshot_component.component_state_json #> '{raw,suggested_resolution_result_json}') = 'object' THEN COALESCE(snapshot_component.component_state_json #> '{raw,suggested_resolution_result_json}', '{}'::jsonb)
            ELSE '{}'::jsonb
          END AS suggested_resolution_result_json,
          snapshot_component.source_basis_fingerprint,
          snapshot_component.source_family_key,
          snapshot_component.bucket_code,
          snapshot_component.component_key_type,
          snapshot_component.component_key_value,
          snapshot_component.component_fingerprint
        FROM public.banking_pay_snapshot_case_component_state AS snapshot_component
        WHERE snapshot_component.snapshot_run_id = v_session_row.source_snapshot_run_id
          AND EXISTS (
            SELECT 1
            FROM _tmp_bpay_session_candidate_scope AS candidate_scope
            WHERE candidate_scope.candidate_id = snapshot_component.candidate_id
          )
      ), normalized_snapshot AS (
        SELECT
          snapshot_source.*,
          BTRIM(COALESCE(
            snapshot_source.source_basis_fingerprint,
            snapshot_source.component_json->>'source_basis_fingerprint',
            snapshot_source.component_json #>> '{raw,source_basis_fingerprint}',
            CASE WHEN snapshot_source.source_basis_json <> '{}'::jsonb THEN md5(snapshot_source.source_basis_json::text) ELSE NULL::text END,
            ''
          )) AS normalized_source_basis_fingerprint,
          BTRIM(COALESCE(
            snapshot_source.source_family_key,
            snapshot_source.component_json->>'source_family_key',
            snapshot_source.component_json #>> '{raw,source_family_key}',
            CASE WHEN snapshot_source.timesheet_id IS NULL THEN NULL ELSE 'timesheet:' || snapshot_source.timesheet_id::text END,
            ''
          )) AS normalized_source_family_key,
          UPPER(BTRIM(COALESCE(
            snapshot_source.bucket_code,
            snapshot_source.component_json->>'bucket_code',
            snapshot_source.source_basis_json->>'bucket_code',
            snapshot_source.component_json #>> '{raw,bucket_code}',
            ''
          ))) AS normalized_bucket_code,
          UPPER(BTRIM(COALESCE(
            snapshot_source.component_key_type,
            snapshot_source.component_json->>'component_key_type',
            snapshot_source.component_json->>'frozen_component_key_type',
            snapshot_source.component_json #>> '{raw,component_key_type}',
            ''
          ))) AS normalized_component_key_type,
          BTRIM(COALESCE(
            snapshot_source.component_key_value,
            snapshot_source.component_json->>'component_key_value',
            snapshot_source.component_json->>'frozen_component_key_value',
            snapshot_source.component_json #>> '{raw,component_key_value}',
            ''
          )) AS normalized_component_key_value,
          NULLIF(BTRIM(COALESCE(
            snapshot_source.component_fingerprint,
            snapshot_source.component_json->>'component_fingerprint',
            snapshot_source.component_json #>> '{raw,component_fingerprint}',
            ''
          )), '') AS normalized_component_fingerprint,
          UPPER(BTRIM(COALESCE(
            snapshot_source.component_json->>'classification',
            snapshot_source.component_json->>'frozen_component_classification',
            snapshot_source.component_json #>> '{raw,classification}',
            ''
          ))) AS classification,
          UPPER(BTRIM(COALESCE(
            snapshot_source.component_json->>'source_pay_method',
            snapshot_source.source_basis_json->>'source_pay_method',
            snapshot_source.component_json #>> '{raw,source_pay_method}',
            ''
          ))) AS source_pay_method,
          UPPER(BTRIM(COALESCE(
            snapshot_source.component_json->>'current_target_pay_method',
            snapshot_source.component_json->>'target_pay_method',
            snapshot_source.suggested_resolution_payload_json->>'target_pay_method',
            snapshot_source.suggested_resolution_result_json->>'target_pay_method',
            snapshot_source.component_json #>> '{raw,current_target_pay_method}',
            snapshot_source.component_json #>> '{raw,target_pay_method}',
            ''
          ))) AS target_pay_method,
          COALESCE(snapshot_source.component_json->>'source_units', snapshot_source.source_basis_json->>'source_units', snapshot_source.component_json #>> '{raw,source_units}', '') AS source_units_text,
          COALESCE(snapshot_source.component_json->>'source_rate', snapshot_source.source_basis_json->>'source_rate', snapshot_source.component_json #>> '{raw,source_rate}', '') AS source_rate_text,
          COALESCE(snapshot_source.component_json->>'source_charge_rate', snapshot_source.source_basis_json->>'source_charge_rate', snapshot_source.component_json #>> '{raw,source_charge_rate}', '') AS source_charge_rate_text,
          COALESCE(snapshot_source.component_json->>'source_pay_ex_vat', snapshot_source.component_json->>'component_amount_ex_vat', snapshot_source.component_json #>> '{raw,source_pay_ex_vat}', '') AS source_pay_ex_vat_text,
          COALESCE(snapshot_source.component_json->>'source_charge_ex_vat', snapshot_source.component_json #>> '{raw,source_charge_ex_vat}', '') AS source_charge_ex_vat_text,
          COALESCE(snapshot_source.component_json->>'source_margin_ex_vat', snapshot_source.component_json #>> '{raw,source_margin_ex_vat}', '') AS source_margin_ex_vat_text,
          COALESCE(snapshot_source.suggested_resolution_payload_json->>'suggested_target_rate', snapshot_source.suggested_resolution_result_json->>'replacement_rate', snapshot_source.component_json->>'target_rate', '') AS suggested_target_rate_text,
          COALESCE(snapshot_source.suggested_resolution_result_json->>'target_charge_rate', snapshot_source.suggested_resolution_payload_json->>'target_charge_rate', snapshot_source.component_json->>'target_charge_rate', '') AS suggested_target_charge_rate_text,
          COALESCE(snapshot_source.suggested_resolution_payload_json->>'relevant_erni_pct', snapshot_source.suggested_resolution_result_json->>'relevant_erni_pct', '') AS relevant_erni_pct_text,
          COALESCE(snapshot_source.suggested_resolution_payload_json->>'vat_rate_pct', snapshot_source.suggested_resolution_result_json->>'vat_rate_pct', '') AS vat_rate_pct_text,
          COALESCE(snapshot_source.suggested_resolution_payload_json->>'umbrella_vat_chargeable', snapshot_source.suggested_resolution_result_json->>'umbrella_vat_chargeable', snapshot_source.component_json->>'umbrella_vat_chargeable', '') AS umbrella_vat_chargeable_text,
          NULLIF(BTRIM(COALESCE(snapshot_source.suggested_resolution_payload_json->>'target_charge_basis', snapshot_source.suggested_resolution_result_json->>'target_charge_basis', snapshot_source.suggested_resolution_payload_json->>'charge_basis', snapshot_source.suggested_resolution_result_json->>'charge_basis', '')), '') AS charge_basis,
          NULLIF(UPPER(BTRIM(COALESCE(snapshot_source.component_json->>'taxability', snapshot_source.source_basis_json->>'taxability', snapshot_source.component_json #>> '{raw,taxability}', ''))), '') AS taxability_context,
          NULLIF(UPPER(BTRIM(COALESCE(snapshot_source.component_json->>'gross_net_handling', snapshot_source.source_basis_json->>'gross_net_handling', snapshot_source.component_json #>> '{raw,gross_net_handling}', ''))), '') AS gross_net_context,
          NULLIF(UPPER(BTRIM(COALESCE(snapshot_source.suggested_resolution_payload_json->>'reuse_mode', snapshot_source.suggested_resolution_result_json->>'reuse_mode', ''))), '') AS reuse_mode,
          LOWER(BTRIM(COALESCE(snapshot_source.component_json->>'requires_resolution', ''))) AS requires_resolution_text,
          LOWER(BTRIM(COALESCE(snapshot_source.component_json->>'is_actionable_resolution_row', ''))) AS is_actionable_resolution_row_text,
          BTRIM(COALESCE(snapshot_source.component_json->>'client_id', snapshot_source.source_basis_json->>'client_id', snapshot_source.component_json #>> '{raw,client_id}', '')) AS case_client_id_text
        FROM snapshot_source
      ), typed_snapshot AS (
        SELECT
          normalized_snapshot.*,
          CASE WHEN normalized_snapshot.case_client_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN normalized_snapshot.case_client_id_text::uuid ELSE NULL::uuid END AS case_client_id,
          CASE WHEN normalized_snapshot.source_units_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_snapshot.source_units_text::numeric, 6) ELSE NULL::numeric END AS source_units,
          CASE WHEN normalized_snapshot.source_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_snapshot.source_rate_text::numeric, 6) ELSE NULL::numeric END AS source_rate,
          CASE WHEN normalized_snapshot.source_charge_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_snapshot.source_charge_rate_text::numeric, 6) ELSE NULL::numeric END AS source_charge_rate,
          CASE WHEN normalized_snapshot.source_pay_ex_vat_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_snapshot.source_pay_ex_vat_text::numeric, 2) ELSE NULL::numeric END AS source_pay_ex_vat,
          CASE WHEN normalized_snapshot.source_charge_ex_vat_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_snapshot.source_charge_ex_vat_text::numeric, 2) ELSE NULL::numeric END AS source_charge_ex_vat,
          CASE WHEN normalized_snapshot.source_margin_ex_vat_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_snapshot.source_margin_ex_vat_text::numeric, 2) ELSE NULL::numeric END AS source_margin_ex_vat,
          CASE WHEN normalized_snapshot.suggested_target_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_snapshot.suggested_target_rate_text::numeric, 2) ELSE NULL::numeric END AS suggested_target_rate,
          CASE WHEN normalized_snapshot.suggested_target_charge_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_snapshot.suggested_target_charge_rate_text::numeric, 6) ELSE NULL::numeric END AS suggested_target_charge_rate,
          CASE WHEN normalized_snapshot.relevant_erni_pct_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_snapshot.relevant_erni_pct_text::numeric, 6) ELSE NULL::numeric END AS relevant_erni_pct,
          CASE WHEN normalized_snapshot.vat_rate_pct_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN round(normalized_snapshot.vat_rate_pct_text::numeric, 6) ELSE NULL::numeric END AS vat_rate_pct,
          CASE WHEN normalized_snapshot.umbrella_vat_chargeable_text = '' THEN NULL::boolean WHEN LOWER(normalized_snapshot.umbrella_vat_chargeable_text) IN ('true', 't', '1', 'yes', 'y', 'on') THEN true ELSE false END AS umbrella_vat_chargeable,
          CASE
            WHEN normalized_snapshot.requires_resolution_text = '' THEN (
              normalized_snapshot.classification = 'TAXABLE_CHANNEL_SENSITIVE'
              AND normalized_snapshot.source_pay_method <> ''
              AND normalized_snapshot.target_pay_method <> ''
              AND normalized_snapshot.source_pay_method IS DISTINCT FROM normalized_snapshot.target_pay_method
            )
            ELSE normalized_snapshot.requires_resolution_text IN ('true', 't', '1', 'yes', 'y', 'on')
          END AS requires_resolution,
          CASE
            WHEN normalized_snapshot.is_actionable_resolution_row_text = '' THEN (
              normalized_snapshot.source_units_text ~ '^-?[0-9]+(\.[0-9]+)?$'
              AND normalized_snapshot.source_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$'
              AND normalized_snapshot.source_charge_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$'
              AND normalized_snapshot.normalized_component_key_type <> 'ADJUSTMENT_CODE'
            )
            ELSE normalized_snapshot.is_actionable_resolution_row_text IN ('true', 't', '1', 'yes', 'y', 'on')
          END AS is_actionable_resolution_row
        FROM normalized_snapshot
      )
      SELECT
        'SNAPSHOT|' || typed_snapshot.snapshot_component_id::text,
        'SNAPSHOT',
        typed_snapshot.candidate_id,
        typed_snapshot.case_key,
        typed_snapshot.timesheet_id,
        typed_snapshot.case_client_id,
        typed_snapshot.source_basis_json,
        typed_snapshot.normalized_source_basis_fingerprint,
        typed_snapshot.normalized_source_family_key,
        NULLIF(typed_snapshot.normalized_bucket_code, ''),
        typed_snapshot.normalized_component_key_type,
        typed_snapshot.normalized_component_key_value,
        typed_snapshot.normalized_component_fingerprint,
        typed_snapshot.classification,
        typed_snapshot.source_pay_method,
        typed_snapshot.target_pay_method,
        typed_snapshot.source_units,
        typed_snapshot.source_rate,
        typed_snapshot.source_charge_rate,
        typed_snapshot.source_pay_ex_vat,
        typed_snapshot.source_charge_ex_vat,
        typed_snapshot.source_margin_ex_vat,
        typed_snapshot.suggested_target_rate,
        typed_snapshot.suggested_target_charge_rate,
        typed_snapshot.relevant_erni_pct,
        typed_snapshot.vat_rate_pct,
        typed_snapshot.umbrella_vat_chargeable,
        typed_snapshot.charge_basis,
        typed_snapshot.taxability_context,
        typed_snapshot.gross_net_context,
        typed_snapshot.reuse_mode,
        typed_snapshot.suggested_resolution_payload_json,
        typed_snapshot.suggested_resolution_result_json,
        typed_snapshot.requires_resolution,
        typed_snapshot.is_actionable_resolution_row,
        typed_snapshot.component_json
      FROM typed_snapshot
      WHERE typed_snapshot.timesheet_id IS NOT NULL
        AND typed_snapshot.case_key <> ''
        AND typed_snapshot.normalized_source_basis_fingerprint <> ''
        AND typed_snapshot.normalized_source_family_key <> ''
        AND typed_snapshot.normalized_component_key_type <> ''
        AND typed_snapshot.normalized_component_key_value <> ''
        AND typed_snapshot.source_units IS NOT NULL
        AND typed_snapshot.source_rate IS NOT NULL
        AND typed_snapshot.source_charge_rate IS NOT NULL
      ON CONFLICT (evidence_key) DO NOTHING;
    END IF;

    CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_session_bucket_component_evidence (
      evidence_key text PRIMARY KEY,
      evidence_source text NOT NULL,
      candidate_id uuid NOT NULL,
      case_key text NOT NULL,
      timesheet_id uuid NOT NULL,
      contract_id uuid NULL,
      client_id uuid NULL,
      source_basis_json jsonb NOT NULL,
      source_basis_fingerprint text NOT NULL,
      source_family_key text NOT NULL,
      bucket_code text NULL,
      component_key_type text NOT NULL,
      component_key_value text NOT NULL,
      component_fingerprint text NULL,
      classification text NOT NULL,
      source_pay_method text NOT NULL,
      target_pay_method text NOT NULL,
      source_units numeric NOT NULL,
      source_rate numeric NOT NULL,
      source_charge_rate numeric NOT NULL,
      source_pay_ex_vat numeric NOT NULL,
      source_charge_ex_vat numeric NOT NULL,
      source_margin_ex_vat numeric NOT NULL,
      suggested_target_rate numeric NULL,
      suggested_target_charge_rate numeric NOT NULL,
      category_identity text NOT NULL,
      resolution_context_json jsonb NOT NULL,
      decision_context_key text NOT NULL,
      suggested_resolution_payload_json jsonb NOT NULL,
      suggested_resolution_result_json jsonb NOT NULL,
      requires_resolution boolean NOT NULL,
      is_actionable_resolution_row boolean NOT NULL,
      component_state_json jsonb NOT NULL
    ) ON COMMIT DROP;

    TRUNCATE TABLE _tmp_bpay_session_bucket_component_evidence;

    INSERT INTO _tmp_bpay_session_bucket_component_evidence (
      evidence_key,
      evidence_source,
      candidate_id,
      case_key,
      timesheet_id,
      contract_id,
      client_id,
      source_basis_json,
      source_basis_fingerprint,
      source_family_key,
      bucket_code,
      component_key_type,
      component_key_value,
      component_fingerprint,
      classification,
      source_pay_method,
      target_pay_method,
      source_units,
      source_rate,
      source_charge_rate,
      source_pay_ex_vat,
      source_charge_ex_vat,
      source_margin_ex_vat,
      suggested_target_rate,
      suggested_target_charge_rate,
      category_identity,
      resolution_context_json,
      decision_context_key,
      suggested_resolution_payload_json,
      suggested_resolution_result_json,
      requires_resolution,
      is_actionable_resolution_row,
      component_state_json
    )
    WITH enriched_raw AS (
      SELECT
        raw_component.*,
        timesheet_row.contract_id,
        COALESCE(current_financial.client_id, raw_component.case_client_id) AS trusted_client_id,
        COALESCE(raw_component.source_pay_ex_vat, round(raw_component.source_units * raw_component.source_rate, 2)) AS effective_source_pay_ex_vat,
        COALESCE(raw_component.source_charge_ex_vat, round(raw_component.source_units * raw_component.source_charge_rate, 2)) AS effective_source_charge_ex_vat
      FROM _tmp_bpay_session_bucket_component_raw AS raw_component
      LEFT JOIN public.timesheets AS timesheet_row
        ON timesheet_row.timesheet_id = raw_component.timesheet_id
      LEFT JOIN LATERAL (
        SELECT
          financial_row.candidate_id,
          financial_row.client_id
        FROM public.timesheets_financials AS financial_row
        WHERE financial_row.timesheet_id = raw_component.timesheet_id
          AND financial_row.is_current = true
        ORDER BY financial_row.timesheet_version DESC, financial_row.updated_at DESC, financial_row.id DESC
        LIMIT 1
      ) AS current_financial ON true
      WHERE current_financial.candidate_id IS NULL
         OR current_financial.candidate_id = raw_component.candidate_id
    ), categorized_raw AS (
      SELECT
        enriched_raw.*,
        COALESCE(
          enriched_raw.source_margin_ex_vat,
          round(enriched_raw.effective_source_charge_ex_vat - enriched_raw.effective_source_pay_ex_vat, 2)
        ) AS effective_source_margin_ex_vat,
        COALESCE(enriched_raw.suggested_target_charge_rate, enriched_raw.source_charge_rate) AS effective_suggested_target_charge_rate,
        CASE
          WHEN enriched_raw.component_key_type IN ('TS_DAY', 'TS_TOTAL') THEN concat_ws(
            '|',
            'WORKED_TIME',
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.bucket_code, ''))), ''), '~')
          )
          WHEN enriched_raw.component_key_type IN ('ADDITIONAL_CODE', 'ADDITIONAL_UNIT', 'ADDITIONAL_UNITS', 'PAY_CODE') THEN concat_ws(
            '|',
            enriched_raw.component_key_type,
            COALESCE(NULLIF(UPPER(BTRIM(enriched_raw.component_key_value)), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'additional_code', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'additional_unit_code', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'pay_code', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'additional_unit_name', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'additional_unit_description', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'additional_name', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'additional_description', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'additional_code_name', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'additional_code_description', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'pay_code_name', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'pay_code_description', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'name', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'description', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.component_state_json->>'label', ''))), ''), '~')
          )
          ELSE concat_ws(
            '|',
            COALESCE(NULLIF(enriched_raw.component_key_type, ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(enriched_raw.component_key_value)), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.bucket_code, ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'code', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'name', ''))), ''), '~'),
            COALESCE(NULLIF(UPPER(BTRIM(COALESCE(enriched_raw.source_basis_json->>'description', ''))), ''), '~')
          )
        END AS category_identity
      FROM enriched_raw
    ), contextualized_raw AS (
      SELECT
        categorized_raw.*,
        jsonb_strip_nulls(
          jsonb_build_object(
            'resolution_family', 'BUCKETED',
            'classification', NULLIF(categorized_raw.classification, ''),
            'component_semantics', NULLIF(categorized_raw.component_key_type, ''),
            'category_identity', categorized_raw.category_identity,
            'source_pay_method', NULLIF(categorized_raw.source_pay_method, ''),
            'target_pay_method', NULLIF(categorized_raw.target_pay_method, ''),
            'source_rate', round(categorized_raw.source_rate, 6),
            'source_charge_rate', round(categorized_raw.source_charge_rate, 6)
          )
          || jsonb_build_object(
            'suggested_target_charge_rate', categorized_raw.effective_suggested_target_charge_rate,
            'relevant_erni_pct', categorized_raw.relevant_erni_pct,
            'vat_rate_pct', categorized_raw.vat_rate_pct,
            'umbrella_vat_chargeable', categorized_raw.umbrella_vat_chargeable,
            'charge_basis', COALESCE(NULLIF(UPPER(BTRIM(COALESCE(categorized_raw.charge_basis, ''))), ''), 'FIXED_SOURCE_CHARGE'),
            'taxability_context', categorized_raw.taxability_context,
            'gross_net_context', categorized_raw.gross_net_context,
            'reuse_mode', categorized_raw.reuse_mode
          )
        ) AS resolution_context_json
      FROM categorized_raw
    )
    SELECT
      contextualized_raw.evidence_key,
      contextualized_raw.evidence_source,
      contextualized_raw.candidate_id,
      contextualized_raw.case_key,
      contextualized_raw.timesheet_id,
      contextualized_raw.contract_id,
      contextualized_raw.trusted_client_id,
      contextualized_raw.source_basis_json,
      contextualized_raw.source_basis_fingerprint,
      contextualized_raw.source_family_key,
      contextualized_raw.bucket_code,
      contextualized_raw.component_key_type,
      contextualized_raw.component_key_value,
      contextualized_raw.component_fingerprint,
      contextualized_raw.classification,
      contextualized_raw.source_pay_method,
      contextualized_raw.target_pay_method,
      contextualized_raw.source_units,
      contextualized_raw.source_rate,
      contextualized_raw.source_charge_rate,
      contextualized_raw.effective_source_pay_ex_vat,
      contextualized_raw.effective_source_charge_ex_vat,
      contextualized_raw.effective_source_margin_ex_vat,
      contextualized_raw.suggested_target_rate,
      contextualized_raw.effective_suggested_target_charge_rate,
      contextualized_raw.category_identity,
      contextualized_raw.resolution_context_json,
      md5(contextualized_raw.resolution_context_json::text),
      contextualized_raw.suggested_resolution_payload_json,
      contextualized_raw.suggested_resolution_result_json,
      contextualized_raw.requires_resolution,
      contextualized_raw.is_actionable_resolution_row,
      contextualized_raw.component_state_json
    FROM contextualized_raw;

    FOR v_bucket_row IN
      SELECT input_bucket.*
      FROM _tmp_bpay_session_bucket_resolution AS input_bucket
      ORDER BY input_bucket.bucket_ordinal
    LOOP
      SELECT
        COUNT(*)::integer,
        (array_agg(component_evidence.evidence_key ORDER BY component_evidence.evidence_key))[1],
        (array_agg(component_evidence.candidate_id ORDER BY component_evidence.candidate_id))[1]
      INTO
        v_target_match_count,
        v_anchor_evidence_key,
        v_bucket_matched_candidate_id
      FROM _tmp_bpay_session_bucket_component_evidence AS component_evidence
      WHERE component_evidence.case_key = v_case_key
        AND (v_candidate_id IS NULL OR component_evidence.candidate_id = v_candidate_id)
        AND (
          v_bucket_row.timesheet_id IS NULL
          OR component_evidence.timesheet_id = v_bucket_row.timesheet_id
        )
        AND component_evidence.source_basis_fingerprint = v_bucket_row.source_basis_fingerprint
        AND component_evidence.source_family_key = v_bucket_row.source_family_key
        AND COALESCE(component_evidence.bucket_code, '') = COALESCE(v_bucket_row.bucket_code, '')
        AND component_evidence.component_key_type = v_bucket_row.component_key_type
        AND component_evidence.component_key_value = v_bucket_row.component_key_value
        AND round(component_evidence.source_units, 6) = round(v_bucket_row.source_units, 6)
        AND round(component_evidence.source_rate, 6) = round(v_bucket_row.source_rate, 6)
        AND round(component_evidence.source_charge_rate, 6) = round(v_bucket_row.source_charge_rate, 6)
        AND component_evidence.requires_resolution = true
        AND component_evidence.is_actionable_resolution_row = true;

      IF v_target_match_count = 0 THEN
        RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_NOT_FOUND'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_NOT_FOUND',
                  'session_id', p_session_id::text,
                  'candidate_id', CASE WHEN v_candidate_id IS NULL THEN NULL ELSE v_candidate_id::text END,
                  'case_key', v_case_key,
                  'bucket_ordinal', v_bucket_row.bucket_ordinal
                )::text;
      ELSIF v_target_match_count > 1 THEN
        RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_AMBIGUOUS'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_AMBIGUOUS',
                  'session_id', p_session_id::text,
                  'candidate_id', CASE WHEN v_candidate_id IS NULL THEN NULL ELSE v_candidate_id::text END,
                  'case_key', v_case_key,
                  'bucket_ordinal', v_bucket_row.bucket_ordinal
                )::text;
      END IF;

      UPDATE _tmp_bpay_session_bucket_resolution AS update_bucket
      SET matched_candidate_id = v_bucket_matched_candidate_id,
          anchor_evidence_key = v_anchor_evidence_key,
          decision_context_key = component_evidence.decision_context_key,
          category_identity = component_evidence.category_identity
      FROM _tmp_bpay_session_bucket_component_evidence AS component_evidence
      WHERE update_bucket.bucket_ordinal = v_bucket_row.bucket_ordinal
        AND component_evidence.evidence_key = v_anchor_evidence_key;
    END LOOP;

    SELECT COALESCE(array_agg(DISTINCT input_bucket.matched_candidate_id ORDER BY input_bucket.matched_candidate_id), ARRAY[]::uuid[])
    INTO v_matching_candidate_ids
    FROM _tmp_bpay_session_bucket_resolution AS input_bucket
    WHERE input_bucket.matched_candidate_id IS NOT NULL;

    v_matching_candidate_count := COALESCE(array_length(v_matching_candidate_ids, 1), 0);
    IF v_matching_candidate_count = 0 THEN
      RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_NOT_FOUND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_NOT_FOUND',
                'session_id', p_session_id::text,
                'case_key', v_case_key
              )::text;
    ELSIF v_matching_candidate_count > 1 THEN
      RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_AMBIGUOUS'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_AMBIGUOUS',
                'session_id', p_session_id::text,
                'case_key', v_case_key
              )::text;
    END IF;

    v_resolved_candidate_id := v_matching_candidate_ids[1];

    IF v_candidate_id IS NOT NULL AND v_resolved_candidate_id <> v_candidate_id THEN
      RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_NOT_FOUND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_NOT_FOUND',
                'session_id', p_session_id::text,
                'candidate_id', v_candidate_id::text,
                'case_key', v_case_key
              )::text;
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM _tmp_bpay_session_candidate_scope AS candidate_scope
      WHERE candidate_scope.candidate_id = v_resolved_candidate_id
    ) THEN
      RAISE EXCEPTION 'WORKBENCH_SESSION_CANDIDATE_NOT_FOUND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_SESSION_CANDIDATE_NOT_FOUND',
                'session_id', p_session_id::text,
                'candidate_id', v_resolved_candidate_id::text
              )::text;
    END IF;

    SELECT COALESCE(array_agg(DISTINCT component_evidence.timesheet_id ORDER BY component_evidence.timesheet_id), ARRAY[]::uuid[])
    INTO v_matching_timesheet_ids
    FROM _tmp_bpay_session_bucket_resolution AS input_bucket
    JOIN _tmp_bpay_session_bucket_component_evidence AS component_evidence
      ON component_evidence.evidence_key = input_bucket.anchor_evidence_key;

    IF COALESCE(array_length(v_matching_timesheet_ids, 1), 0) <> 1 THEN
      RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_AMBIGUOUS'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_AMBIGUOUS',
                'session_id', p_session_id::text,
                'candidate_id', v_resolved_candidate_id::text,
                'case_key', v_case_key
              )::text;
    END IF;

    v_anchor_timesheet_id := v_matching_timesheet_ids[1];

    IF v_linked_timesheet_id IS NOT NULL AND v_linked_timesheet_id <> v_anchor_timesheet_id THEN
      RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_NOT_FOUND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_NOT_FOUND',
                'session_id', p_session_id::text,
                'candidate_id', v_resolved_candidate_id::text,
                'case_key', v_case_key,
                'timesheet_id', v_linked_timesheet_id::text
              )::text;
    END IF;

    SELECT
      component_evidence.contract_id,
      component_evidence.client_id
    INTO
      v_anchor_contract_id,
      v_anchor_client_id
    FROM _tmp_bpay_session_bucket_component_evidence AS component_evidence
    WHERE component_evidence.candidate_id = v_resolved_candidate_id
      AND component_evidence.timesheet_id = v_anchor_timesheet_id
      AND component_evidence.case_key = v_case_key
    ORDER BY component_evidence.evidence_key
    LIMIT 1;

    IF v_resolve_all_linked_timesheets
       AND v_anchor_contract_id IS NULL
       AND v_anchor_client_id IS NULL THEN
      RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_CASE_BASELINE_NOT_FOUND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_ROW_BACKED_CASE_BASELINE_NOT_FOUND',
                'session_id', p_session_id::text,
                'candidate_id', v_resolved_candidate_id::text,
                'case_key', v_case_key,
                'reason', 'LINKED_SCOPE_CLIENT_NOT_AVAILABLE'
              )::text;
    END IF;

    IF v_resolve_all_linked_timesheets AND EXISTS (
      SELECT 1
      FROM _tmp_bpay_session_bucket_resolution AS input_bucket
      JOIN _tmp_bpay_session_bucket_component_evidence AS component_evidence
        ON component_evidence.evidence_key = input_bucket.anchor_evidence_key
      WHERE component_evidence.classification = ''
         OR component_evidence.source_pay_method = ''
         OR component_evidence.target_pay_method = ''
         OR component_evidence.category_identity = ''
    ) THEN
      RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_CASE_BASELINE_NOT_FOUND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_ROW_BACKED_CASE_BASELINE_NOT_FOUND',
                'session_id', p_session_id::text,
                'candidate_id', v_resolved_candidate_id::text,
                'case_key', v_case_key,
                'reason', 'LINKED_DECISION_CONTEXT_INCOMPLETE'
              )::text;
    END IF;

    IF EXISTS (
      SELECT 1
      FROM _tmp_bpay_session_bucket_resolution AS input_bucket
      WHERE input_bucket.anchor_evidence_key IS NULL
         OR input_bucket.decision_context_key IS NULL
         OR input_bucket.category_identity IS NULL
    ) THEN
      RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_NOT_FOUND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_NOT_FOUND',
                'session_id', p_session_id::text,
                'candidate_id', v_resolved_candidate_id::text,
                'case_key', v_case_key,
                'reason', 'VALIDATED_COMPONENT_CONTEXT_NOT_ATTACHED'
              )::text;
    END IF;

    IF EXISTS (
      SELECT 1
      FROM _tmp_bpay_session_bucket_resolution AS input_bucket
      GROUP BY input_bucket.decision_context_key
      HAVING COUNT(DISTINCT (input_bucket.resolution_mode || '|' || round(input_bucket.target_rate, 2)::text)) > 1
    ) THEN
      RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_AMBIGUOUS'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_ROW_BACKED_BUCKETED_COMPONENT_AMBIGUOUS',
                'session_id', p_session_id::text,
                'candidate_id', v_resolved_candidate_id::text,
                'case_key', v_case_key,
                'reason', 'CONFLICTING_RATE_DECISIONS'
              )::text;
    END IF;

    CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_session_bucket_decision (
      decision_context_key text PRIMARY KEY,
      resolution_mode text NOT NULL,
      target_rate numeric NOT NULL,
      source_bucket_ordinal integer NOT NULL
    ) ON COMMIT DROP;

    TRUNCATE TABLE _tmp_bpay_session_bucket_decision;

    INSERT INTO _tmp_bpay_session_bucket_decision (
      decision_context_key,
      resolution_mode,
      target_rate,
      source_bucket_ordinal
    )
    SELECT
      input_bucket.decision_context_key,
      MIN(input_bucket.resolution_mode),
      MIN(input_bucket.target_rate),
      MIN(input_bucket.bucket_ordinal)
    FROM _tmp_bpay_session_bucket_resolution AS input_bucket
    GROUP BY input_bucket.decision_context_key;

    CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_session_target_bucket_resolution (
      resolution_identity_key text PRIMARY KEY,
      candidate_id uuid NOT NULL,
      case_key text NOT NULL,
      timesheet_id uuid NOT NULL,
      contract_id uuid NULL,
      client_id uuid NULL,
      source_basis_json jsonb NOT NULL,
      source_basis_fingerprint text NOT NULL,
      source_family_key text NOT NULL,
      bucket_code text NULL,
      component_key_type text NOT NULL,
      component_key_value text NOT NULL,
      component_fingerprint text NULL,
      classification text NOT NULL,
      source_pay_method text NOT NULL,
      target_pay_method text NOT NULL,
      source_units numeric NOT NULL,
      source_rate numeric NOT NULL,
      source_charge_rate numeric NOT NULL,
      source_pay_ex_vat numeric NOT NULL,
      source_charge_ex_vat numeric NOT NULL,
      source_margin_ex_vat numeric NOT NULL,
      resolution_mode text NOT NULL,
      target_rate numeric NOT NULL,
      target_pay_ex_vat numeric NOT NULL,
      target_charge_ex_vat numeric NOT NULL,
      target_margin_ex_vat numeric NOT NULL,
      margin_delta_ex_vat numeric NOT NULL,
      decision_context_key text NOT NULL,
      category_identity text NOT NULL,
      suggested_resolution_payload_json jsonb NOT NULL,
      suggested_resolution_result_json jsonb NOT NULL,
      component_state_json jsonb NOT NULL,
      applied_via_linked_scope boolean NOT NULL,
      source_bucket_ordinal integer NOT NULL
    ) ON COMMIT DROP;

    TRUNCATE TABLE _tmp_bpay_session_target_bucket_resolution;

    INSERT INTO _tmp_bpay_session_target_bucket_resolution (
      resolution_identity_key,
      candidate_id,
      case_key,
      timesheet_id,
      contract_id,
      client_id,
      source_basis_json,
      source_basis_fingerprint,
      source_family_key,
      bucket_code,
      component_key_type,
      component_key_value,
      component_fingerprint,
      classification,
      source_pay_method,
      target_pay_method,
      source_units,
      source_rate,
      source_charge_rate,
      source_pay_ex_vat,
      source_charge_ex_vat,
      source_margin_ex_vat,
      resolution_mode,
      target_rate,
      target_pay_ex_vat,
      target_charge_ex_vat,
      target_margin_ex_vat,
      margin_delta_ex_vat,
      decision_context_key,
      category_identity,
      suggested_resolution_payload_json,
      suggested_resolution_result_json,
      component_state_json,
      applied_via_linked_scope,
      source_bucket_ordinal
    )
    SELECT
      concat_ws(
        '|',
        'BUCKETED',
        COALESCE(NULLIF(component_evidence.case_key, ''), '~'),
        component_evidence.timesheet_id::text,
        COALESCE(NULLIF(component_evidence.source_basis_fingerprint, ''), '~'),
        COALESCE(NULLIF(component_evidence.source_family_key, ''), '~'),
        COALESCE(NULLIF(component_evidence.bucket_code, ''), '~'),
        COALESCE(NULLIF(component_evidence.component_key_type, ''), '~'),
        COALESCE(NULLIF(component_evidence.component_key_value, ''), '~')
      ),
      component_evidence.candidate_id,
      component_evidence.case_key,
      component_evidence.timesheet_id,
      component_evidence.contract_id,
      component_evidence.client_id,
      component_evidence.source_basis_json,
      component_evidence.source_basis_fingerprint,
      component_evidence.source_family_key,
      component_evidence.bucket_code,
      component_evidence.component_key_type,
      component_evidence.component_key_value,
      component_evidence.component_fingerprint,
      component_evidence.classification,
      component_evidence.source_pay_method,
      component_evidence.target_pay_method,
      component_evidence.source_units,
      component_evidence.source_rate,
      component_evidence.source_charge_rate,
      component_evidence.source_pay_ex_vat,
      component_evidence.source_charge_ex_vat,
      component_evidence.source_margin_ex_vat,
      input_bucket.resolution_mode,
      input_bucket.target_rate,
      round(component_evidence.source_units * input_bucket.target_rate, 2),
      component_evidence.source_charge_ex_vat,
      round(component_evidence.source_charge_ex_vat - round(component_evidence.source_units * input_bucket.target_rate, 2), 2),
      round(component_evidence.source_pay_ex_vat - round(component_evidence.source_units * input_bucket.target_rate, 2), 2),
      component_evidence.decision_context_key,
      component_evidence.category_identity,
      component_evidence.suggested_resolution_payload_json,
      component_evidence.suggested_resolution_result_json,
      component_evidence.component_state_json,
      false,
      input_bucket.bucket_ordinal
    FROM _tmp_bpay_session_bucket_resolution AS input_bucket
    JOIN _tmp_bpay_session_bucket_component_evidence AS component_evidence
      ON component_evidence.evidence_key = input_bucket.anchor_evidence_key;

    IF v_resolve_all_linked_timesheets THEN
      INSERT INTO _tmp_bpay_session_target_bucket_resolution (
        resolution_identity_key,
        candidate_id,
        case_key,
        timesheet_id,
        contract_id,
        client_id,
        source_basis_json,
        source_basis_fingerprint,
        source_family_key,
        bucket_code,
        component_key_type,
        component_key_value,
        component_fingerprint,
        classification,
        source_pay_method,
        target_pay_method,
        source_units,
        source_rate,
        source_charge_rate,
        source_pay_ex_vat,
        source_charge_ex_vat,
        source_margin_ex_vat,
        resolution_mode,
        target_rate,
        target_pay_ex_vat,
        target_charge_ex_vat,
        target_margin_ex_vat,
        margin_delta_ex_vat,
        decision_context_key,
        category_identity,
        suggested_resolution_payload_json,
        suggested_resolution_result_json,
        component_state_json,
        applied_via_linked_scope,
        source_bucket_ordinal
      )
      SELECT
        concat_ws(
          '|',
          'BUCKETED',
          COALESCE(NULLIF(component_evidence.case_key, ''), '~'),
          component_evidence.timesheet_id::text,
          COALESCE(NULLIF(component_evidence.source_basis_fingerprint, ''), '~'),
          COALESCE(NULLIF(component_evidence.source_family_key, ''), '~'),
          COALESCE(NULLIF(component_evidence.bucket_code, ''), '~'),
          COALESCE(NULLIF(component_evidence.component_key_type, ''), '~'),
          COALESCE(NULLIF(component_evidence.component_key_value, ''), '~')
        ),
        component_evidence.candidate_id,
        component_evidence.case_key,
        component_evidence.timesheet_id,
        component_evidence.contract_id,
        component_evidence.client_id,
        component_evidence.source_basis_json,
        component_evidence.source_basis_fingerprint,
        component_evidence.source_family_key,
        component_evidence.bucket_code,
        component_evidence.component_key_type,
        component_evidence.component_key_value,
        component_evidence.component_fingerprint,
        component_evidence.classification,
        component_evidence.source_pay_method,
        component_evidence.target_pay_method,
        component_evidence.source_units,
        component_evidence.source_rate,
        component_evidence.source_charge_rate,
        component_evidence.source_pay_ex_vat,
        component_evidence.source_charge_ex_vat,
        component_evidence.source_margin_ex_vat,
        bucket_decision.resolution_mode,
        bucket_decision.target_rate,
        round(component_evidence.source_units * bucket_decision.target_rate, 2),
        component_evidence.source_charge_ex_vat,
        round(component_evidence.source_charge_ex_vat - round(component_evidence.source_units * bucket_decision.target_rate, 2), 2),
        round(component_evidence.source_pay_ex_vat - round(component_evidence.source_units * bucket_decision.target_rate, 2), 2),
        component_evidence.decision_context_key,
        component_evidence.category_identity,
        component_evidence.suggested_resolution_payload_json,
        component_evidence.suggested_resolution_result_json,
        component_evidence.component_state_json,
        true,
        bucket_decision.source_bucket_ordinal
      FROM _tmp_bpay_session_bucket_component_evidence AS component_evidence
      JOIN _tmp_bpay_session_bucket_decision AS bucket_decision
        ON bucket_decision.decision_context_key = component_evidence.decision_context_key
      WHERE component_evidence.evidence_source = 'ROW_BACKED'
        AND component_evidence.candidate_id = v_resolved_candidate_id
        AND component_evidence.timesheet_id <> v_anchor_timesheet_id
        AND component_evidence.requires_resolution = true
        AND component_evidence.is_actionable_resolution_row = true
        AND (
          (
            v_anchor_contract_id IS NOT NULL
            AND component_evidence.contract_id = v_anchor_contract_id
          )
          OR (
            v_anchor_contract_id IS NULL
            AND component_evidence.contract_id IS NULL
            AND component_evidence.client_id = v_anchor_client_id
          )
        )
      ON CONFLICT (resolution_identity_key) DO NOTHING;
    END IF;

    IF v_operation = 'APPLY' THEN
      -- Serialize the financial-boundary revalidation with draft creation.
      PERFORM pg_advisory_xact_lock(94201, 1);
    END IF;

    CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_resolution_batch_boundary (
      timesheet_id uuid NOT NULL,
      pay_batch_id uuid NOT NULL,
      batch_status text NOT NULL,
      PRIMARY KEY (timesheet_id, pay_batch_id)
    ) ON COMMIT DROP;
    TRUNCATE TABLE _tmp_bpay_resolution_batch_boundary;

    INSERT INTO _tmp_bpay_resolution_batch_boundary(timesheet_id, pay_batch_id, batch_status)
    SELECT DISTINCT boundary_rows.timesheet_id, boundary_rows.pay_batch_id, boundary_rows.batch_status
    FROM (
      SELECT
        batch_item.timesheet_id,
        batch_row.id AS pay_batch_id,
        UPPER(BTRIM(COALESCE(batch_row.status, ''))) AS batch_status
      FROM public.pay_batch_items AS batch_item
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id = batch_item.pay_batch_candidate_id
      JOIN public.pay_batches AS batch_row
        ON batch_row.id = batch_candidate.pay_batch_id
      WHERE batch_item.timesheet_id IS NOT NULL
        AND COALESCE(batch_item.is_voided, false) = false
        AND EXISTS (
          SELECT 1
          FROM _tmp_bpay_session_target_bucket_resolution AS target_resolution
          WHERE target_resolution.timesheet_id = batch_item.timesheet_id
        )
        AND UPPER(BTRIM(COALESCE(batch_row.status, ''))) NOT IN ('CANCELLED', 'CANCELED')

      UNION ALL

      SELECT
        batch_snapshot.timesheet_id,
        batch_row.id AS pay_batch_id,
        UPPER(BTRIM(COALESCE(batch_row.status, ''))) AS batch_status
      FROM public.pay_batch_timesheet_snapshots AS batch_snapshot
      JOIN public.pay_batches AS batch_row
        ON batch_row.id = batch_snapshot.pay_batch_id
      WHERE batch_snapshot.timesheet_id IS NOT NULL
        AND EXISTS (
          SELECT 1
          FROM _tmp_bpay_session_target_bucket_resolution AS target_resolution
          WHERE target_resolution.timesheet_id = batch_snapshot.timesheet_id
        )
        AND UPPER(BTRIM(COALESCE(batch_row.status, ''))) NOT IN ('CANCELLED', 'CANCELED')
    ) AS boundary_rows
    ON CONFLICT (timesheet_id, pay_batch_id) DO UPDATE
      SET batch_status = EXCLUDED.batch_status;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
             'pay_batch_id', boundary_row.pay_batch_id::text,
             'batch_status', boundary_row.batch_status
           ) ORDER BY boundary_row.pay_batch_id::text), '[]'::jsonb)
    INTO v_anchor_blocking_batches
    FROM _tmp_bpay_resolution_batch_boundary AS boundary_row
    WHERE boundary_row.timesheet_id = v_anchor_timesheet_id;

    IF jsonb_array_length(COALESCE(v_anchor_blocking_batches, '[]'::jsonb)) > 0 THEN
      RAISE EXCEPTION 'WORKBENCH_RESOLUTION_ANCHOR_FINANCIAL_BOUNDARY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_RESOLUTION_ANCHOR_FINANCIAL_BOUNDARY',
                'session_id', p_session_id::text,
                'candidate_id', v_resolved_candidate_id::text,
                'anchor_timesheet_id', v_anchor_timesheet_id::text,
                'message', 'The payment details changed because this timesheet is now included in a payment batch. Refresh Banking Pay and review the latest details.'
              )::text;
    END IF;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
             'timesheet_id', excluded_scope.timesheet_id::text,
             'reason', 'ALREADY_IN_LIVE_BATCH',
             'batch_status', excluded_scope.batch_status
           ) ORDER BY excluded_scope.timesheet_id::text), '[]'::jsonb)
    INTO v_excluded_linked_timesheets
    FROM (
      SELECT
        boundary_row.timesheet_id,
        MIN(boundary_row.batch_status) AS batch_status
      FROM _tmp_bpay_resolution_batch_boundary AS boundary_row
      WHERE boundary_row.timesheet_id <> v_anchor_timesheet_id
      GROUP BY boundary_row.timesheet_id
    ) AS excluded_scope;

    v_excluded_linked_timesheet_count := jsonb_array_length(COALESCE(v_excluded_linked_timesheets, '[]'::jsonb));

    DELETE FROM _tmp_bpay_session_target_bucket_resolution AS target_resolution
    WHERE target_resolution.applied_via_linked_scope = true
      AND EXISTS (
        SELECT 1
        FROM _tmp_bpay_resolution_batch_boundary AS boundary_row
        WHERE boundary_row.timesheet_id = target_resolution.timesheet_id
      );

    SELECT COUNT(*)::integer
    INTO v_anchor_component_count
    FROM _tmp_bpay_session_target_bucket_resolution AS target_resolution
    WHERE target_resolution.applied_via_linked_scope = false;

    SELECT COUNT(*)::integer
    INTO v_linked_target_component_count
    FROM _tmp_bpay_session_target_bucket_resolution AS target_resolution
    WHERE target_resolution.applied_via_linked_scope = true;

    SELECT COUNT(DISTINCT target_resolution.timesheet_id)::integer
    INTO v_linked_target_timesheet_count
    FROM _tmp_bpay_session_target_bucket_resolution AS target_resolution
    WHERE target_resolution.applied_via_linked_scope = true;

    SELECT COUNT(DISTINCT target_resolution.timesheet_id)::integer
    INTO v_materialized_target_timesheet_count
    FROM _tmp_bpay_session_target_bucket_resolution AS target_resolution;

    IF v_resolve_all_linked_timesheets THEN
      SELECT COUNT(*)::integer
      INTO v_skipped_mismatch_count
      FROM _tmp_bpay_session_bucket_component_evidence AS component_evidence
      WHERE component_evidence.evidence_source = 'ROW_BACKED'
        AND component_evidence.candidate_id = v_resolved_candidate_id
        AND component_evidence.timesheet_id <> v_anchor_timesheet_id
        AND component_evidence.requires_resolution = true
        AND component_evidence.is_actionable_resolution_row = true
        AND (
          (
            v_anchor_contract_id IS NOT NULL
            AND component_evidence.contract_id = v_anchor_contract_id
          )
          OR (
            v_anchor_contract_id IS NULL
            AND component_evidence.contract_id IS NULL
            AND component_evidence.client_id = v_anchor_client_id
          )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM _tmp_bpay_session_bucket_decision AS bucket_decision
          WHERE bucket_decision.decision_context_key = component_evidence.decision_context_key
        );
    ELSE
      v_skipped_mismatch_count := 0;
    END IF;

    SELECT COALESCE(array_agg(DISTINCT target_resolution.timesheet_id ORDER BY target_resolution.timesheet_id), ARRAY[]::uuid[])
    INTO v_eligible_linked_timesheet_ids
    FROM _tmp_bpay_session_target_bucket_resolution AS target_resolution
    WHERE target_resolution.applied_via_linked_scope = true
      AND target_resolution.timesheet_id <> v_anchor_timesheet_id;

    v_linked_target_timesheet_count := COALESCE(array_length(v_eligible_linked_timesheet_ids, 1), 0);
    v_total_affected_timesheet_count := 1 + v_linked_target_timesheet_count;

    SELECT COALESCE(array_agg(DISTINCT target_resolution.timesheet_id ORDER BY target_resolution.timesheet_id), ARRAY[]::uuid[])
    INTO v_case_targeted_timesheet_ids
    FROM _tmp_bpay_session_target_bucket_resolution AS target_resolution;
    v_case_linked_timesheet_ids := v_eligible_linked_timesheet_ids;

    v_linked_scope_type := CASE
      WHEN NOT v_resolve_all_linked_timesheets THEN 'SELF_ONLY'
      WHEN v_anchor_contract_id IS NOT NULL THEN 'CONTRACT'
      ELSE 'DAILY_SAME_BASIS'
    END;

    IF v_operation = 'DISCOVER' THEN
      RETURN jsonb_build_object(
        'ok', true,
        'operation', 'DISCOVER',
        'session_id', p_session_id::text,
        'session_version', v_session_row.version,
        'progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0),
        'candidate_id', v_resolved_candidate_id::text,
        'anchor_timesheet_id', v_anchor_timesheet_id::text,
        'anchor_case_key', v_case_key,
        'eligible_linked_timesheet_ids', COALESCE(to_jsonb(v_eligible_linked_timesheet_ids), '[]'::jsonb),
        'eligible_linked_timesheet_count', v_linked_target_timesheet_count,
        'total_affected_timesheet_count', v_total_affected_timesheet_count,
        'excluded_linked_timesheets', COALESCE(v_excluded_linked_timesheets, '[]'::jsonb),
        'excluded_linked_timesheet_count', v_excluded_linked_timesheet_count,
        'targeted_timesheet_ids', COALESCE(to_jsonb(v_case_targeted_timesheet_ids), '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(to_jsonb(v_case_linked_timesheet_ids), '[]'::jsonb),
        'state_changed', false,
        'job_id', NULL::text
      );
    END IF;

    INSERT INTO _tmp_bpay_session_case_resolution_existing
    SELECT existing_resolution.*
    FROM public.banking_pay_workbench_session_case_resolutions AS existing_resolution
    WHERE existing_resolution.session_id = p_session_id
      AND existing_resolution.candidate_id = v_resolved_candidate_id
      AND existing_resolution.resolution_family = 'BUCKETED'
      AND (
        existing_resolution.timesheet_id = v_anchor_timesheet_id
        OR (
          existing_resolution.timesheet_id IS NOT NULL
          AND existing_resolution.timesheet_id = ANY(COALESCE(v_eligible_linked_timesheet_ids, ARRAY[]::uuid[]))
          AND BTRIM(COALESCE(existing_resolution.payload_json->>'source_anchor_timesheet_id', '')) = v_anchor_timesheet_id::text
          AND BTRIM(COALESCE(existing_resolution.payload_json->>'source_anchor_case_key', '')) = v_case_key
          AND LOWER(BTRIM(COALESCE(existing_resolution.payload_json->>'applied_via_linked_scope', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        )
      );

    SELECT COUNT(*)::integer
    INTO v_existing_deleted_count
    FROM _tmp_bpay_session_case_resolution_existing AS existing_resolution;

    IF v_existing_deleted_count > 0 THEN
      DELETE FROM public.banking_pay_workbench_session_case_resolutions AS delete_resolution
      WHERE delete_resolution.id IN (
        SELECT existing_resolution.id
        FROM _tmp_bpay_session_case_resolution_existing AS existing_resolution
      );
    END IF;

    FOR v_target_resolution_row IN
      SELECT target_resolution.*
      FROM _tmp_bpay_session_target_bucket_resolution AS target_resolution
      ORDER BY
        target_resolution.timesheet_id,
        target_resolution.component_key_type,
        target_resolution.component_key_value,
        COALESCE(target_resolution.bucket_code, ''),
        target_resolution.source_basis_fingerprint
    LOOP
      v_existing_row := NULL;
      SELECT existing_resolution.*
      INTO v_existing_row
      FROM _tmp_bpay_session_case_resolution_existing AS existing_resolution
      WHERE existing_resolution.resolution_identity_key = v_target_resolution_row.resolution_identity_key
      LIMIT 1;

      v_target_resolution_result_json := jsonb_strip_nulls(
        jsonb_build_object(
          'target_units', round(v_target_resolution_row.source_units, 6),
          'target_rate', round(v_target_resolution_row.target_rate, 2),
          'target_amount_ex_vat', round(v_target_resolution_row.target_pay_ex_vat, 2),
          'target_pay_ex_vat', round(v_target_resolution_row.target_pay_ex_vat, 2),
          'target_charge_ex_vat', round(v_target_resolution_row.target_charge_ex_vat, 2),
          'target_margin_ex_vat', round(v_target_resolution_row.target_margin_ex_vat, 2),
          'margin_delta_ex_vat', round(v_target_resolution_row.margin_delta_ex_vat, 2),
          'source_pay_ex_vat', round(v_target_resolution_row.source_pay_ex_vat, 2),
          'source_charge_ex_vat', round(v_target_resolution_row.source_charge_ex_vat, 2),
          'source_margin_ex_vat', round(v_target_resolution_row.source_margin_ex_vat, 2)
        )
      );

      v_target_bucket_payload_json := jsonb_strip_nulls(
        jsonb_build_object(
          'timesheet_id', v_target_resolution_row.timesheet_id::text,
          'finance_component_id', NULLIF(BTRIM(COALESCE(v_target_resolution_row.component_state_json->>'finance_component_id', '')), ''),
          'source_basis_json', CASE WHEN v_target_resolution_row.source_basis_json = '{}'::jsonb THEN NULL ELSE v_target_resolution_row.source_basis_json END,
          'source_basis_fingerprint', NULLIF(v_target_resolution_row.source_basis_fingerprint, ''),
          'source_family_key', NULLIF(v_target_resolution_row.source_family_key, ''),
          'bucket_code', v_target_resolution_row.bucket_code,
          'component_key_type', NULLIF(v_target_resolution_row.component_key_type, ''),
          'component_key_value', NULLIF(v_target_resolution_row.component_key_value, ''),
          'component_fingerprint', v_target_resolution_row.component_fingerprint,
          'classification', NULLIF(v_target_resolution_row.classification, '')
        )
        || jsonb_build_object(
          'source_pay_method', NULLIF(v_target_resolution_row.source_pay_method, ''),
          'target_pay_method', NULLIF(v_target_resolution_row.target_pay_method, ''),
          'source_units', round(v_target_resolution_row.source_units, 6),
          'target_units', round(v_target_resolution_row.source_units, 6),
          'source_rate', round(v_target_resolution_row.source_rate, 6),
          'source_charge_rate', round(v_target_resolution_row.source_charge_rate, 6),
          'source_pay_ex_vat', round(v_target_resolution_row.source_pay_ex_vat, 2),
          'source_charge_ex_vat', round(v_target_resolution_row.source_charge_ex_vat, 2),
          'source_margin_ex_vat', round(v_target_resolution_row.source_margin_ex_vat, 2),
          'resolution_mode', v_target_resolution_row.resolution_mode
        )
        || jsonb_build_object(
          'target_rate', round(v_target_resolution_row.target_rate, 2),
          'target_amount_ex_vat', round(v_target_resolution_row.target_pay_ex_vat, 2),
          'target_pay_ex_vat', round(v_target_resolution_row.target_pay_ex_vat, 2),
          'target_charge_ex_vat', round(v_target_resolution_row.target_charge_ex_vat, 2),
          'target_margin_ex_vat', round(v_target_resolution_row.target_margin_ex_vat, 2),
          'margin_delta_ex_vat', round(v_target_resolution_row.margin_delta_ex_vat, 2),
          'decision_context_key', v_target_resolution_row.decision_context_key,
          'category_identity', v_target_resolution_row.category_identity,
          'suggested_resolution_payload_json', CASE WHEN v_target_resolution_row.suggested_resolution_payload_json = '{}'::jsonb THEN NULL ELSE v_target_resolution_row.suggested_resolution_payload_json END,
          'suggested_resolution_result_json', CASE WHEN v_target_resolution_row.suggested_resolution_result_json = '{}'::jsonb THEN NULL ELSE v_target_resolution_row.suggested_resolution_result_json END
        )
        || jsonb_build_object(
          'saved_resolution_payload_json', jsonb_build_object(
            'resolution_family', 'BUCKETED',
            'resolution_mode', v_target_resolution_row.resolution_mode,
            'target_pay_method', NULLIF(v_target_resolution_row.target_pay_method, ''),
            'target_units', round(v_target_resolution_row.source_units, 6),
            'target_rate', round(v_target_resolution_row.target_rate, 2),
            'decision_context_key', v_target_resolution_row.decision_context_key
          ),
          'saved_resolution_result_json', v_target_resolution_result_json,
          'applied_via_linked_scope', v_target_resolution_row.applied_via_linked_scope,
          'source_anchor_case_key', v_case_key,
          'source_anchor_timesheet_id', v_anchor_timesheet_id::text,
          'resolved_rate_cancel_scope', 'ANCHOR_FAMILY_PRE_DRAFT'
        )
      );

      v_linked_scope_json := jsonb_strip_nulls(
        jsonb_build_object(
          'linked_scope_type', v_linked_scope_type,
          'contract_id', CASE WHEN v_anchor_contract_id IS NULL THEN NULL ELSE v_anchor_contract_id::text END,
          'client_id', CASE WHEN v_anchor_client_id IS NULL THEN NULL ELSE v_anchor_client_id::text END,
          'source_anchor_case_key', v_case_key,
          'source_anchor_timesheet_id', v_anchor_timesheet_id::text,
          'target_case_key', v_target_resolution_row.case_key,
          'target_timesheet_id', v_target_resolution_row.timesheet_id::text,
          'requested_resolve_all_linked_timesheets', v_resolve_all_linked_timesheets,
          'applied_via_linked_scope', v_target_resolution_row.applied_via_linked_scope,
          'resolved_rate_cancel_scope', 'ANCHOR_FAMILY_PRE_DRAFT'
        )
      );

      v_normalized_payload_json := jsonb_strip_nulls(
        (
          COALESCE(v_resolution_payload_json, '{}'::jsonb)
          - 'bucket_resolutions'
          - 'resolution_mode'
          - 'mode'
          - 'candidate_id'
          - 'case_key'
          - 'resolution_family'
          - 'timesheet_id'
          - 'linked_timesheet_id'
          - 'target_rate'
          - 'source_units'
          - 'source_rate'
          - 'source_charge_rate'
          - 'resolve_all_linked_timesheets'
          - 'linked_resolution_scope_json'
        )
        || jsonb_build_object(
          'case_key', v_target_resolution_row.case_key,
          'candidate_id', v_resolved_candidate_id::text,
          'finance_case_id', CASE WHEN v_finance_case_id_text = '' THEN NULL ELSE v_finance_case_id_text END,
          'linked_timesheet_id', v_target_resolution_row.timesheet_id::text,
          'timesheet_id', v_target_resolution_row.timesheet_id::text,
          'resolution_family', 'BUCKETED',
          'resolve_all_linked_timesheets', false,
          'requested_resolve_all_linked_timesheets', v_resolve_all_linked_timesheets,
          'applied_via_linked_scope', v_target_resolution_row.applied_via_linked_scope,
          'source_anchor_case_key', v_case_key
        )
        || jsonb_build_object(
          'source_anchor_timesheet_id', v_anchor_timesheet_id::text,
          'linked_resolution_scope_json', v_linked_scope_json,
          'resolved_rate_cancel_scope', 'ANCHOR_FAMILY_PRE_DRAFT',
          'decision_context_key', v_target_resolution_row.decision_context_key,
          'category_identity', v_target_resolution_row.category_identity,
          'bucket_resolutions', jsonb_build_array(v_target_bucket_payload_json)
        )
      );

      v_audit_before_json := CASE
        WHEN v_existing_row.id IS NULL THEN NULL
        ELSE jsonb_build_object(
          'id', v_existing_row.id::text,
          'session_id', v_existing_row.session_id::text,
          'candidate_id', v_existing_row.candidate_id::text,
          'case_key', v_existing_row.case_key,
          'resolution_family', v_existing_row.resolution_family,
          'resolution_identity_key', v_existing_row.resolution_identity_key,
          'timesheet_id', CASE WHEN v_existing_row.timesheet_id IS NULL THEN NULL ELSE v_existing_row.timesheet_id::text END,
          'source_basis_fingerprint', v_existing_row.source_basis_fingerprint,
          'source_family_key', v_existing_row.source_family_key,
          'bucket_code', v_existing_row.bucket_code,
          'component_key_type', v_existing_row.component_key_type,
          'component_key_value', v_existing_row.component_key_value,
          'payload_json', v_existing_row.payload_json
        )
      END;

      INSERT INTO public.banking_pay_workbench_session_case_resolutions (
        session_id,
        candidate_id,
        case_key,
        resolution_family,
        resolution_identity_key,
        timesheet_id,
        source_basis_fingerprint,
        source_family_key,
        bucket_code,
        component_key_type,
        component_key_value,
        payload_json,
        created_at_utc,
        updated_at_utc
      )
      VALUES (
        p_session_id,
        v_resolved_candidate_id,
        v_target_resolution_row.case_key,
        'BUCKETED',
        v_target_resolution_row.resolution_identity_key,
        v_target_resolution_row.timesheet_id,
        NULLIF(v_target_resolution_row.source_basis_fingerprint, ''),
        NULLIF(v_target_resolution_row.source_family_key, ''),
        v_target_resolution_row.bucket_code,
        NULLIF(v_target_resolution_row.component_key_type, ''),
        NULLIF(v_target_resolution_row.component_key_value, ''),
        v_normalized_payload_json,
        v_now,
        v_now
      )
      ON CONFLICT (session_id, resolution_identity_key)
      DO UPDATE
      SET candidate_id = EXCLUDED.candidate_id,
          case_key = EXCLUDED.case_key,
          resolution_family = EXCLUDED.resolution_family,
          timesheet_id = EXCLUDED.timesheet_id,
          source_basis_fingerprint = EXCLUDED.source_basis_fingerprint,
          source_family_key = EXCLUDED.source_family_key,
          bucket_code = EXCLUDED.bucket_code,
          component_key_type = EXCLUDED.component_key_type,
          component_key_value = EXCLUDED.component_key_value,
          payload_json = EXCLUDED.payload_json,
          updated_at_utc = v_now
      RETURNING public.banking_pay_workbench_session_case_resolutions.*
      INTO v_upserted_row;

      v_case_resolution_ids := v_case_resolution_ids || jsonb_build_array(v_upserted_row.id::text);
      v_resolution_identity_keys := v_resolution_identity_keys || jsonb_build_array(v_upserted_row.resolution_identity_key);
      IF v_case_resolution_id_text IS NULL THEN
        v_case_resolution_id_text := v_upserted_row.id::text;
      END IF;

      v_audit_after_json := jsonb_build_object(
        'id', v_upserted_row.id::text,
        'session_id', v_upserted_row.session_id::text,
        'candidate_id', v_upserted_row.candidate_id::text,
        'case_key', v_upserted_row.case_key,
        'resolution_family', v_upserted_row.resolution_family,
        'resolution_identity_key', v_upserted_row.resolution_identity_key,
        'timesheet_id', CASE WHEN v_upserted_row.timesheet_id IS NULL THEN NULL ELSE v_upserted_row.timesheet_id::text END,
        'source_basis_fingerprint', v_upserted_row.source_basis_fingerprint,
        'source_family_key', v_upserted_row.source_family_key,
        'bucket_code', v_upserted_row.bucket_code,
        'component_key_type', v_upserted_row.component_key_type,
        'component_key_value', v_upserted_row.component_key_value,
        'payload_json', v_upserted_row.payload_json
      );

      PERFORM public._audit_insert(
        'banking_pay_workbench_session_case_resolution',
        v_upserted_row.id::text,
        CASE WHEN v_existing_row.id IS NULL THEN 'SESSION_CASE_RESOLUTION_CREATED' ELSE 'SESSION_CASE_RESOLUTION_UPDATED' END,
        v_audit_before_json,
        v_audit_after_json,
        'SESSION_CASE_RESOLUTION_APPLIED',
        p_actor_user_id
      );
    END LOOP;
  ELSE
    v_nonbucket_resolution_mode := UPPER(BTRIM(COALESCE(
      v_resolution_payload_json->>'resolution_mode',
      v_resolution_payload_json->>'mode',
      ''
    )));

    IF v_nonbucket_resolution_mode NOT IN ('SUGGESTED_EQUIVALENT_BASIS', 'MANUAL_AMOUNT') THEN
      RAISE EXCEPTION 'NON_BUCKET resolution_mode must be SUGGESTED_EQUIVALENT_BASIS or MANUAL_AMOUNT';
    END IF;

    IF COALESCE(
      v_resolution_payload_json->>'target_amount_ex_vat',
      v_resolution_payload_json->>'target_amount',
      v_resolution_payload_json->>'amount_ex_vat',
      v_resolution_payload_json->>'amount',
      ''
    ) !~ '^-?[0-9]+(\.[0-9]+)?$' THEN
      RAISE EXCEPTION 'target_amount_ex_vat is required for NON_BUCKET resolution';
    END IF;

    v_target_amount_ex_vat := round(COALESCE(
      NULLIF(v_resolution_payload_json->>'target_amount_ex_vat', '')::numeric,
      NULLIF(v_resolution_payload_json->>'target_amount', '')::numeric,
      NULLIF(v_resolution_payload_json->>'amount_ex_vat', '')::numeric,
      NULLIF(v_resolution_payload_json->>'amount', '')::numeric
    ), 2);

    IF v_target_amount_ex_vat < 0 THEN
      RAISE EXCEPTION 'target_amount_ex_vat must be non-negative';
    END IF;

    SELECT COUNT(*)::integer
    INTO v_row_backed_case_count
    FROM _tmp_bpay_session_row_backed_case_baseline AS row_backed_case
    WHERE COALESCE(row_backed_case.resolution_family, '') IN ('', 'NON_BUCKET')
      AND (
        v_linked_timesheet_id IS NULL
        OR row_backed_case.timesheet_id IS NULL
        OR row_backed_case.timesheet_id = v_linked_timesheet_id
      )
      AND (
        v_finance_case_id_text = ''
        OR row_backed_case.finance_case_id IS NULL
        OR row_backed_case.finance_case_id::text = v_finance_case_id_text
      );

    IF v_row_backed_case_count > 0 THEN
      IF v_candidate_id IS NULL THEN
        IF v_row_backed_case_count > 1 THEN
          RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_CASE_BASELINE_AMBIGUOUS'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'WORKBENCH_ROW_BACKED_CASE_BASELINE_AMBIGUOUS',
                    'session_id', p_session_id::text,
                    'case_key', v_case_key,
                    'resolution_family', 'NON_BUCKET'
                  )::text;
        END IF;

        SELECT COALESCE(array_agg(DISTINCT row_backed_case.candidate_id ORDER BY row_backed_case.candidate_id), ARRAY[]::uuid[])
        INTO v_matching_candidate_ids
        FROM _tmp_bpay_session_row_backed_case_baseline AS row_backed_case
        WHERE COALESCE(row_backed_case.resolution_family, '') IN ('', 'NON_BUCKET')
          AND (
            v_linked_timesheet_id IS NULL
            OR row_backed_case.timesheet_id IS NULL
            OR row_backed_case.timesheet_id = v_linked_timesheet_id
          )
          AND (
            v_finance_case_id_text = ''
            OR row_backed_case.finance_case_id IS NULL
            OR row_backed_case.finance_case_id::text = v_finance_case_id_text
          );

        v_matching_candidate_count := COALESCE(array_length(v_matching_candidate_ids, 1), 0);
        IF v_matching_candidate_count = 0 THEN
          RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_CASE_BASELINE_NOT_FOUND'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'WORKBENCH_ROW_BACKED_CASE_BASELINE_NOT_FOUND',
                    'session_id', p_session_id::text,
                    'case_key', v_case_key,
                    'resolution_family', 'NON_BUCKET'
                  )::text;
        ELSIF v_matching_candidate_count > 1 THEN
          RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_CASE_BASELINE_AMBIGUOUS'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'WORKBENCH_ROW_BACKED_CASE_BASELINE_AMBIGUOUS',
                    'session_id', p_session_id::text,
                    'case_key', v_case_key,
                    'resolution_family', 'NON_BUCKET'
                  )::text;
        END IF;

        v_resolved_candidate_id := v_matching_candidate_ids[1];
      ELSE
        SELECT COUNT(*)::integer
        INTO v_target_match_count
        FROM _tmp_bpay_session_row_backed_case_baseline AS row_backed_case
        WHERE row_backed_case.candidate_id = v_candidate_id
          AND COALESCE(row_backed_case.resolution_family, '') IN ('', 'NON_BUCKET')
          AND (
            v_linked_timesheet_id IS NULL
            OR row_backed_case.timesheet_id IS NULL
            OR row_backed_case.timesheet_id = v_linked_timesheet_id
          )
          AND (
            v_finance_case_id_text = ''
            OR row_backed_case.finance_case_id IS NULL
            OR row_backed_case.finance_case_id::text = v_finance_case_id_text
          );

        IF v_target_match_count = 0 THEN
          RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_CASE_BASELINE_NOT_FOUND'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'WORKBENCH_ROW_BACKED_CASE_BASELINE_NOT_FOUND',
                    'session_id', p_session_id::text,
                    'candidate_id', v_candidate_id::text,
                    'case_key', v_case_key,
                    'resolution_family', 'NON_BUCKET'
                  )::text;
        ELSIF v_target_match_count > 1 THEN
          RAISE EXCEPTION 'WORKBENCH_ROW_BACKED_CASE_BASELINE_AMBIGUOUS'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'WORKBENCH_ROW_BACKED_CASE_BASELINE_AMBIGUOUS',
                    'session_id', p_session_id::text,
                    'candidate_id', v_candidate_id::text,
                    'case_key', v_case_key,
                    'resolution_family', 'NON_BUCKET'
                  )::text;
        END IF;

        v_resolved_candidate_id := v_candidate_id;
      END IF;
    ELSIF v_candidate_id IS NULL THEN
      SELECT COALESCE(array_agg(DISTINCT snapshot_case.candidate_id ORDER BY snapshot_case.candidate_id), ARRAY[]::uuid[])
      INTO v_matching_candidate_ids
      FROM public.banking_pay_snapshot_case_state AS snapshot_case
      WHERE snapshot_case.snapshot_run_id = v_session_row.source_snapshot_run_id
        AND EXISTS (
          SELECT 1
          FROM _tmp_bpay_session_candidate_scope AS candidate_scope
          WHERE candidate_scope.candidate_id = snapshot_case.candidate_id
        )
        AND snapshot_case.case_key = v_case_key;

      v_matching_candidate_count := COALESCE(array_length(v_matching_candidate_ids, 1), 0);
      IF v_matching_candidate_count = 0 THEN
        RAISE EXCEPTION 'No snapshot baseline case found for NON_BUCKET resolution in session % for case_key %', p_session_id, v_case_key;
      ELSIF v_matching_candidate_count > 1 THEN
        RAISE EXCEPTION 'Ambiguous NON_BUCKET resolution target for case_key % in session %', v_case_key, p_session_id;
      END IF;

      v_resolved_candidate_id := v_matching_candidate_ids[1];
    ELSE
      SELECT COUNT(*)::integer
      INTO v_target_match_count
      FROM public.banking_pay_snapshot_case_state AS snapshot_case
      WHERE snapshot_case.snapshot_run_id = v_session_row.source_snapshot_run_id
        AND snapshot_case.candidate_id = v_candidate_id
        AND snapshot_case.case_key = v_case_key;

      IF v_target_match_count = 0 THEN
        RAISE EXCEPTION 'No matching NON_BUCKET snapshot baseline case found for candidate % in session %', v_candidate_id, p_session_id;
      ELSIF v_target_match_count > 1 THEN
        RAISE EXCEPTION 'Ambiguous NON_BUCKET snapshot baseline case found for candidate % in session %', v_candidate_id, p_session_id;
      END IF;

      v_resolved_candidate_id := v_candidate_id;
    END IF;

    INSERT INTO _tmp_bpay_session_case_resolution_existing
    SELECT existing_resolution.*
    FROM public.banking_pay_workbench_session_case_resolutions AS existing_resolution
    WHERE existing_resolution.session_id = p_session_id
      AND existing_resolution.candidate_id = v_resolved_candidate_id
      AND existing_resolution.case_key = v_case_key
      AND existing_resolution.resolution_family = 'NON_BUCKET'
      AND (
        v_linked_timesheet_id IS NULL
        OR existing_resolution.timesheet_id = v_linked_timesheet_id
        OR NULLIF(BTRIM(COALESCE(existing_resolution.payload_json->>'linked_timesheet_id', '')), '') = v_linked_timesheet_id::text
        OR NULLIF(BTRIM(COALESCE(existing_resolution.payload_json->>'timesheet_id', '')), '') = v_linked_timesheet_id::text
      )
      AND (
        v_finance_case_id_text = ''
        OR NULLIF(BTRIM(COALESCE(existing_resolution.payload_json->>'finance_case_id', '')), '') IS NULL
        OR NULLIF(BTRIM(COALESCE(existing_resolution.payload_json->>'finance_case_id', '')), '') = v_finance_case_id_text
      );

    v_nonbucket_resolution_identity_key := concat_ws(
      '|',
      'NON_BUCKET',
      COALESCE(NULLIF(v_case_key, ''), '~'),
      COALESCE(CASE WHEN v_linked_timesheet_id IS NULL THEN NULL ELSE v_linked_timesheet_id::text END, '~'),
      '~',
      '~',
      '~',
      '~',
      '~'
    );

    v_nonbucket_existing_row := NULL;
    SELECT temp_existing.*
    INTO v_nonbucket_existing_row
    FROM _tmp_bpay_session_case_resolution_existing AS temp_existing
    WHERE temp_existing.resolution_identity_key = v_nonbucket_resolution_identity_key
    LIMIT 1;

    v_normalized_payload_json := jsonb_strip_nulls(
      (COALESCE(v_resolution_payload_json, '{}'::jsonb) - 'bucket_resolutions' - 'candidate_id' - 'case_key' - 'resolution_family' - 'timesheet_id' - 'linked_timesheet_id')
      || jsonb_build_object(
        'case_key', v_case_key,
        'candidate_id', v_resolved_candidate_id::text,
        'finance_case_id', CASE WHEN v_finance_case_id_text = '' THEN NULL ELSE v_finance_case_id_text END,
        'linked_timesheet_id', CASE WHEN v_linked_timesheet_id IS NULL THEN NULL ELSE v_linked_timesheet_id::text END,
        'timesheet_id', CASE WHEN v_linked_timesheet_id IS NULL THEN NULL ELSE v_linked_timesheet_id::text END,
        'resolution_family', 'NON_BUCKET',
        'resolution_mode', v_nonbucket_resolution_mode,
        'target_amount_ex_vat', round(v_target_amount_ex_vat, 2)
      )
    );

    v_audit_before_json := CASE
      WHEN v_nonbucket_existing_row.id IS NULL THEN NULL
      ELSE jsonb_build_object(
        'id', v_nonbucket_existing_row.id::text,
        'session_id', v_nonbucket_existing_row.session_id::text,
        'candidate_id', v_nonbucket_existing_row.candidate_id::text,
        'case_key', v_nonbucket_existing_row.case_key,
        'resolution_family', v_nonbucket_existing_row.resolution_family,
        'resolution_identity_key', v_nonbucket_existing_row.resolution_identity_key,
        'timesheet_id', CASE WHEN v_nonbucket_existing_row.timesheet_id IS NULL THEN NULL ELSE v_nonbucket_existing_row.timesheet_id::text END,
        'source_basis_fingerprint', v_nonbucket_existing_row.source_basis_fingerprint,
        'source_family_key', v_nonbucket_existing_row.source_family_key,
        'bucket_code', v_nonbucket_existing_row.bucket_code,
        'component_key_type', v_nonbucket_existing_row.component_key_type,
        'component_key_value', v_nonbucket_existing_row.component_key_value,
        'payload_json', v_nonbucket_existing_row.payload_json
      )
    END;

    INSERT INTO public.banking_pay_workbench_session_case_resolutions (
      session_id,
      candidate_id,
      case_key,
      resolution_family,
      resolution_identity_key,
      timesheet_id,
      source_basis_fingerprint,
      source_family_key,
      bucket_code,
      component_key_type,
      component_key_value,
      payload_json,
      created_at_utc,
      updated_at_utc
    )
    VALUES (
      p_session_id,
      v_resolved_candidate_id,
      v_case_key,
      'NON_BUCKET',
      v_nonbucket_resolution_identity_key,
      v_linked_timesheet_id,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      v_normalized_payload_json,
      v_now,
      v_now
    )
    ON CONFLICT (session_id, resolution_identity_key)
    DO UPDATE
    SET candidate_id = EXCLUDED.candidate_id,
        case_key = EXCLUDED.case_key,
        resolution_family = EXCLUDED.resolution_family,
        timesheet_id = EXCLUDED.timesheet_id,
        payload_json = EXCLUDED.payload_json,
        updated_at_utc = v_now
    RETURNING public.banking_pay_workbench_session_case_resolutions.*
    INTO v_nonbucket_upserted_row;

    v_case_resolution_ids := jsonb_build_array(v_nonbucket_upserted_row.id::text);
    v_resolution_identity_keys := jsonb_build_array(v_nonbucket_upserted_row.resolution_identity_key);
    v_case_resolution_id_text := v_nonbucket_upserted_row.id::text;

    v_audit_after_json := jsonb_build_object(
      'id', v_nonbucket_upserted_row.id::text,
      'session_id', v_nonbucket_upserted_row.session_id::text,
      'candidate_id', v_nonbucket_upserted_row.candidate_id::text,
      'case_key', v_nonbucket_upserted_row.case_key,
      'resolution_family', v_nonbucket_upserted_row.resolution_family,
      'resolution_identity_key', v_nonbucket_upserted_row.resolution_identity_key,
      'timesheet_id', CASE WHEN v_nonbucket_upserted_row.timesheet_id IS NULL THEN NULL ELSE v_nonbucket_upserted_row.timesheet_id::text END,
      'payload_json', v_nonbucket_upserted_row.payload_json,
      'resolution_origin_session_id', CASE WHEN v_nonbucket_upserted_row.resolution_origin_session_id IS NULL THEN NULL ELSE v_nonbucket_upserted_row.resolution_origin_session_id::text END,
      'resolution_origin_pay_date', CASE WHEN v_nonbucket_upserted_row.resolution_origin_pay_date IS NULL THEN NULL ELSE v_nonbucket_upserted_row.resolution_origin_pay_date::text END,
      'resolution_origin_source_basis_fingerprint', v_nonbucket_upserted_row.resolution_origin_source_basis_fingerprint
    );

    PERFORM public._audit_insert(
      'banking_pay_workbench_session_case_resolution',
      v_nonbucket_upserted_row.id::text,
      CASE WHEN v_nonbucket_existing_row.id IS NULL THEN 'SESSION_CASE_RESOLUTION_CREATED' ELSE 'SESSION_CASE_RESOLUTION_UPDATED' END,
      v_audit_before_json,
      v_audit_after_json,
      'SESSION_CASE_RESOLUTION_APPLIED',
      p_actor_user_id
    );
  END IF;

  IF v_resolution_family='BUCKETED'
     AND v_anchor_timesheet_id IS NOT NULL
     AND COALESCE(v_resolved_candidate_id,v_candidate_id) IS NOT NULL THEN
    PERFORM public._ctms_normalise_correction_case_resolutions_v1(
      p_session_id,
      COALESCE(v_resolved_candidate_id,v_candidate_id),
      v_anchor_timesheet_id
    );
  END IF;

  -- A fresh target-session decision is authoritative for its canonical
  -- component.  Any deferred or stale predecessor carry for that exact key
  -- must stop blocking draftability once this transaction has saved and
  -- normalised the replacement decision.
  WITH superseded_carry AS (
    UPDATE
      public.banking_pay_workbench_case_resolution_carry_registrations
        carry_row
    SET status = 'SUPERSEDED',
        state_reason_code = 'TARGET_AUTHORITATIVE_DECISION_EXISTS',
        target_resolution_id = target_resolution.id,
        target_economic_fingerprint =
          NULLIF(
            target_resolution.payload_json
              ->> 'resolution_economic_fingerprint',
            ''
          ),
        last_error_json = NULL,
        updated_at_utc = v_now,
        completed_at_utc = v_now
    FROM public.banking_pay_workbench_session_case_resolutions
      target_resolution
    WHERE carry_row.target_session_id = p_session_id
      AND carry_row.candidate_id =
        COALESCE(v_resolved_candidate_id, v_candidate_id)
      AND carry_row.status IN ('PENDING', 'STALE')
      AND target_resolution.session_id = p_session_id
      AND target_resolution.candidate_id =
        COALESCE(v_resolved_candidate_id, v_candidate_id)
      AND target_resolution.resolution_identity_key =
        carry_row.canonical_resolution_key
      AND target_resolution.resolution_identity_key IN (
        SELECT resolution_key.value
        FROM jsonb_array_elements_text(
          COALESCE(v_resolution_identity_keys, '[]'::jsonb)
        ) resolution_key(value)
      )
    RETURNING
      carry_row.id,
      carry_row.source_session_id,
      carry_row.source_resolution_id,
      carry_row.canonical_resolution_key,
      target_resolution.id AS target_resolution_id
  )
  INSERT INTO public.audit_events (
    actor_user_id,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  )
  SELECT
    p_actor_user_id,
    'BANKING_PAY_CASE_RESOLUTION_CARRY',
    superseded_carry.id::text,
    'CARRY_SUPERSEDED',
    jsonb_build_object(
      'status',
      'STALE_OR_PENDING'
    ),
    jsonb_build_object(
      'status',
      'SUPERSEDED',
      'source_session_id',
      superseded_carry.source_session_id,
      'source_resolution_id',
      superseded_carry.source_resolution_id,
      'canonical_resolution_key',
      superseded_carry.canonical_resolution_key,
      'target_resolution_id',
      superseded_carry.target_resolution_id,
      'state_reason_code',
      'TARGET_AUTHORITATIVE_DECISION_EXISTS'
    ),
    'TARGET_AUTHORITATIVE_DECISION_EXISTS'
  FROM superseded_carry;

  UPDATE public.banking_pay_workbench_sessions
  SET version = public.banking_pay_workbench_sessions.version + 1,
      progress_counter_version = COALESCE(public.banking_pay_workbench_sessions.progress_counter_version, 0) + 1,
      progress_updated_at_utc = v_now,
      updated_at_utc = v_now
  WHERE public.banking_pay_workbench_sessions.id = p_session_id
  RETURNING public.banking_pay_workbench_sessions.version, public.banking_pay_workbench_sessions.progress_counter_version
  INTO v_new_session_version, v_new_progress_counter_version;

  IF v_resolution_family = 'BUCKETED' THEN
    SELECT COALESCE(array_agg(DISTINCT target_resolution.timesheet_id ORDER BY target_resolution.timesheet_id), ARRAY[]::uuid[])
    INTO v_case_targeted_timesheet_ids
    FROM _tmp_bpay_session_target_bucket_resolution AS target_resolution;

    SELECT COALESCE(array_agg(DISTINCT target_resolution.timesheet_id ORDER BY target_resolution.timesheet_id), ARRAY[]::uuid[])
    INTO v_case_linked_timesheet_ids
    FROM _tmp_bpay_session_target_bucket_resolution AS target_resolution
    WHERE target_resolution.timesheet_id <> v_anchor_timesheet_id;

    IF COALESCE(array_length(v_case_targeted_timesheet_ids, 1), 0) = 0 THEN
      RAISE EXCEPTION 'WORKBENCH_RESOLUTION_REFRESH_SCOPE_EMPTY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_RESOLUTION_REFRESH_SCOPE_EMPTY',
                'session_id', p_session_id::text,
                'candidate_id', COALESCE(v_resolved_candidate_id, v_candidate_id)::text,
                'case_key', v_case_key,
                'resolution_family', v_resolution_family
              )::text;
    END IF;

    v_case_refresh_scope_kind := 'TARGETED_TIMESHEETS';
  ELSE
    SELECT COALESCE(array_agg(DISTINCT case_timesheet_ids.timesheet_id ORDER BY case_timesheet_ids.timesheet_id), ARRAY[]::uuid[])
    INTO v_case_targeted_timesheet_ids
    FROM (
      SELECT v_linked_timesheet_id AS timesheet_id
      UNION ALL
      SELECT v_anchor_timesheet_id AS timesheet_id
      UNION ALL
      SELECT unnest(COALESCE(v_matching_timesheet_ids, ARRAY[]::uuid[])) AS timesheet_id
    ) AS case_timesheet_ids
    WHERE case_timesheet_ids.timesheet_id IS NOT NULL;

    IF COALESCE(array_length(v_case_targeted_timesheet_ids, 1), 0) > 0 THEN
      SELECT COALESCE(array_agg(DISTINCT rotation_scope.family_timesheet_id ORDER BY rotation_scope.family_timesheet_id), ARRAY[]::uuid[])
      INTO v_case_linked_timesheet_ids
      FROM public._pay_timesheet_rotation_scope(v_case_targeted_timesheet_ids) AS rotation_scope
      WHERE rotation_scope.family_timesheet_id IS NOT NULL
        AND NOT (rotation_scope.family_timesheet_id = ANY(v_case_targeted_timesheet_ids));

      v_case_refresh_scope_kind := 'TARGETED_TIMESHEETS';
    ELSE
      v_case_linked_timesheet_ids := ARRAY[]::uuid[];
      v_case_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
    END IF;
  END IF;

  -- Applying any case resolution advances the session version. Rebuild the
  -- complete affected candidate so independent ready lines and the candidate-
  -- wide recovery headroom remain on that same version. A targeted rebuild
  -- would correctly refresh the resolved chain but leave unrelated candidate
  -- lines on the previous version, making them disappear from the preview.
  v_case_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';

  v_job_json := public.pay_workbench_enqueue_session_candidate_refresh(
    p_session_id => p_session_id,
    p_candidate_id => COALESCE(v_resolved_candidate_id, v_candidate_id),
    p_reason => v_action,
    p_actor_user_id => p_actor_user_id,
    p_payload_json => jsonb_build_object(
      'case_key', v_case_key,
      'resolution_family', v_resolution_family,
      'finance_case_id', CASE WHEN v_finance_case_id_text = '' THEN NULL ELSE v_finance_case_id_text END,
      'linked_timesheet_id', CASE WHEN v_linked_timesheet_id IS NULL THEN NULL ELSE v_linked_timesheet_id::text END,
      'resolution_identity_keys', v_resolution_identity_keys,
      'force_legacy', true,
      'projection_class', 'CASE_RESOLUTION',
      'fallback_reason', 'CASE_RESOLUTION_CHANGED',
      'refresh_scope_kind', v_case_refresh_scope_kind,
      -- A case-resolution save advances the whole candidate to a new session
      -- version.  Do not let the affected-row evidence below silently turn
      -- that rebuild back into a targeted refresh: later source pages may
      -- contain other members required to prove a correction-chain residual.
      'targeted_timesheet_ids', CASE
        WHEN v_case_refresh_scope_kind = 'CANDIDATE_FULL_LIVE' THEN '[]'::jsonb
        ELSE COALESCE(to_jsonb(v_case_targeted_timesheet_ids), '[]'::jsonb)
      END,
      'linked_timesheet_ids', CASE
        WHEN v_case_refresh_scope_kind = 'CANDIDATE_FULL_LIVE' THEN '[]'::jsonb
        ELSE COALESCE(to_jsonb(v_case_linked_timesheet_ids), '[]'::jsonb)
      END,
      'source_build_required', true,
      'line_work_required', true,
      'delta_refresh_required', false
    )
  );

  v_job_id_text := COALESCE(
    NULLIF(BTRIM(v_job_json->>'job_id'), ''),
    NULLIF(BTRIM(v_job_json#>>'{enqueue_result,job_id}'), ''),
    NULLIF(BTRIM(v_job_json#>>'{enqueue_result,job_ids,0}'), ''),
    NULLIF(BTRIM(v_job_json#>>'{job_ids,0}'), ''),
    NULLIF(BTRIM(v_job_json#>>'{enqueue_result,session_recompute_job_ids,0}'), ''),
    NULLIF(BTRIM(v_job_json#>>'{session_recompute_job_ids,0}'), '')
  );

  IF v_job_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_job_id := v_job_id_text::uuid;
  END IF;

  IF v_job_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_REFRESH_JOB_NOT_PROVEN'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_REFRESH_JOB_NOT_PROVEN',
              'session_id', p_session_id::text,
              'candidate_id', COALESCE(v_resolved_candidate_id, v_candidate_id)::text,
              'case_key', v_case_key,
              'resolution_family', v_resolution_family,
              'enqueue_ok', COALESCE(v_job_json->>'ok', ''),
              'enqueue_job_type', NULLIF(BTRIM(COALESCE(v_job_json->>'job_type', v_job_json#>>'{enqueue_result,job_type}', '')), ''),
              'enqueue_candidate_count', NULLIF(BTRIM(COALESCE(v_job_json->>'candidate_count', '')), '')
            )::text;
  END IF;


  SELECT COALESCE(
           jsonb_agg(
             jsonb_strip_nulls(
               jsonb_build_object(
                 'resolution_id', origin_row.id::text,
                 'resolution_identity_key', origin_row.resolution_identity_key,
                 'origin_session_id', origin_row.resolution_origin_session_id::text,
                 'origin_pay_date', origin_row.resolution_origin_pay_date::text,
                 'origin_source_basis_fingerprint', origin_row.resolution_origin_source_basis_fingerprint
               )
             )
             ORDER BY origin_row.resolution_identity_key, origin_row.id
           ),
           '[]'::jsonb
         ),
         COUNT(*)::integer,
         CASE WHEN COUNT(*) = 1 THEN (array_agg(origin_row.resolution_origin_session_id ORDER BY origin_row.resolution_origin_session_id))[1] ELSE NULL::uuid END,
         CASE WHEN COUNT(*) = 1 THEN MIN(origin_row.resolution_origin_pay_date) ELSE NULL::date END
  INTO v_resolution_origins_json,
       v_resolution_origin_count,
       v_single_resolution_origin_session_id,
       v_single_resolution_origin_pay_date
  FROM public.banking_pay_workbench_session_case_resolutions AS origin_row
  JOIN (
    SELECT DISTINCT resolution_id_text.value::uuid AS resolution_id
    FROM jsonb_array_elements_text(COALESCE(v_case_resolution_ids, '[]'::jsonb)) AS resolution_id_text(value)
    WHERE resolution_id_text.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) AS selected_resolution
    ON selected_resolution.resolution_id = origin_row.id;

  IF v_resolution_family IN ('BUCKETED', 'NON_BUCKET')
     AND v_resolution_origin_count <> jsonb_array_length(COALESCE(v_case_resolution_ids, '[]'::jsonb)) THEN
    RAISE EXCEPTION 'WORKBENCH_RESOLUTION_ORIGIN_NOT_PROVEN'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_RESOLUTION_ORIGIN_NOT_PROVEN',
              'session_id', p_session_id::text,
              'case_key', v_case_key,
              'resolution_family', v_resolution_family,
              'resolution_count', jsonb_array_length(COALESCE(v_case_resolution_ids, '[]'::jsonb)),
              'origin_count', v_resolution_origin_count,
              'message', 'The immutable origin of one or more Banking Pay resolutions could not be verified.'
            )::text;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'session_id', p_session_id::text,
    'candidate_id', COALESCE(v_resolved_candidate_id, v_candidate_id)::text,
    'session_version', v_new_session_version,
    'progress_counter_version', v_new_progress_counter_version,
    'job_id', CASE WHEN v_job_id IS NULL THEN NULL ELSE v_job_id::text END,
    'case_resolution_id', v_case_resolution_id_text,
    'case_resolution_ids', v_case_resolution_ids,
    'resolution_identity_keys', v_resolution_identity_keys,
    'case_resolution_count', jsonb_array_length(v_case_resolution_ids),
    'resolution_origins', v_resolution_origins_json,
    'resolution_origin_count', v_resolution_origin_count,
    'resolution_origin_session_id', CASE WHEN v_single_resolution_origin_session_id IS NULL THEN NULL ELSE v_single_resolution_origin_session_id::text END,
    'resolution_origin_pay_date', CASE WHEN v_single_resolution_origin_pay_date IS NULL THEN NULL ELSE v_single_resolution_origin_pay_date::text END,
    'anchor_component_count', CASE WHEN v_resolution_family = 'BUCKETED' THEN v_anchor_component_count ELSE NULL END,
    'anchor_timesheet_id', CASE WHEN v_resolution_family = 'BUCKETED' AND v_anchor_timesheet_id IS NOT NULL THEN v_anchor_timesheet_id::text ELSE NULL END,
    'eligible_linked_timesheet_ids', CASE WHEN v_resolution_family = 'BUCKETED' THEN COALESCE(to_jsonb(v_eligible_linked_timesheet_ids), '[]'::jsonb) ELSE NULL END,
    'eligible_linked_timesheet_count', CASE WHEN v_resolution_family = 'BUCKETED' THEN v_linked_target_timesheet_count ELSE NULL END,
    'total_affected_timesheet_count', CASE WHEN v_resolution_family = 'BUCKETED' THEN v_total_affected_timesheet_count ELSE NULL END,
    'excluded_linked_timesheets', CASE WHEN v_resolution_family = 'BUCKETED' THEN COALESCE(v_excluded_linked_timesheets, '[]'::jsonb) ELSE NULL END,
    'excluded_linked_timesheet_count', CASE WHEN v_resolution_family = 'BUCKETED' THEN v_excluded_linked_timesheet_count ELSE NULL END,
    'linked_target_timesheet_count', CASE WHEN v_resolution_family = 'BUCKETED' THEN v_linked_target_timesheet_count ELSE NULL END,
    'linked_target_component_count', CASE WHEN v_resolution_family = 'BUCKETED' THEN v_linked_target_component_count ELSE NULL END,
    'materialized_target_timesheet_count', CASE WHEN v_resolution_family = 'BUCKETED' THEN v_materialized_target_timesheet_count ELSE NULL END,
    'skipped_mismatch_count', CASE WHEN v_resolution_family = 'BUCKETED' THEN v_skipped_mismatch_count ELSE NULL END,
    'state_changed', true,
    'action', v_action,
    'refresh_scope_kind', v_case_refresh_scope_kind,
    'targeted_timesheet_ids', COALESCE(to_jsonb(v_case_targeted_timesheet_ids), '[]'::jsonb),
    'linked_timesheet_ids', COALESCE(to_jsonb(v_case_linked_timesheet_ids), '[]'::jsonb),
    'targeted_refresh_enqueued', (v_job_id IS NOT NULL),
    'legacy_refresh_enqueued', CASE
      WHEN COALESCE(v_job_json->>'legacy_queued_count', '') ~ '^[0-9]+$' THEN (v_job_json->>'legacy_queued_count')::integer > 0
      ELSE upper(btrim(coalesce(v_job_json->>'job_type', v_job_json#>>'{enqueue_result,job_type}', ''))) <> 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
    END,
    'enqueue_result', jsonb_strip_nulls(
      jsonb_build_object(
        'job_id', v_job_id::text,
        'job_type', NULLIF(BTRIM(COALESCE(v_job_json->>'job_type', v_job_json#>>'{enqueue_result,job_type}', '')), ''),
        'delta_queued_count', CASE WHEN COALESCE(v_job_json->>'delta_queued_count', '') ~ '^[0-9]+$' THEN (v_job_json->>'delta_queued_count')::integer ELSE NULL END,
        'legacy_queued_count', CASE WHEN COALESCE(v_job_json->>'legacy_queued_count', '') ~ '^[0-9]+$' THEN (v_job_json->>'legacy_queued_count')::integer ELSE NULL END,
        'candidate_count', CASE WHEN COALESCE(v_job_json->>'candidate_count', '') ~ '^[0-9]+$' THEN (v_job_json->>'candidate_count')::integer ELSE NULL END,
        'paged', CASE WHEN lower(btrim(coalesce(v_job_json->>'paged', ''))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN true WHEN NULLIF(BTRIM(COALESCE(v_job_json->>'paged', '')), '') IS NOT NULL THEN false ELSE NULL END
      )
    ),
    'durable_result', CASE WHEN v_resolution_family = 'TAXABLE_CHANNEL_RESTRUCTURE' THEN v_taxable_result_json ELSE NULL END
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.pay_workbench_session_apply_case_resolution(uuid, uuid, jsonb)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_apply_case_resolution(uuid, uuid, jsonb)
  TO service_role;
