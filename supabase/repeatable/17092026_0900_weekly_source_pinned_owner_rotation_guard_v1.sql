-- Plan 6.2 Gate 6, G6-11: the managed-root rotation guard for the four
-- rotation entry points whose winning definition lives in a hash-pinned
-- historical file.
--
-- supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql (E21, E22 and the base
-- body that 29082026_1914 renames to
-- private.timesheet_weekly_chain_delete_apply_base_v1) and
-- supabase/repeatable/06092026_1636_candidate_advanced_expense_component_policy_v1.sql
-- (E11) are both pinned byte-for-byte by repository tests
-- (tests/banking-pay-legacy-monolith-authority-reassert.test.cjs and
-- tests/09092026_1902_candidate_expense_update_submit_final_authority.test.cjs),
-- so neither may be edited.  This file therefore re-creates exactly those four
-- routines, byte-identical to the pinned source apart from the guard call and
-- its refusal, using the same later-repeatable override the repository already
-- uses for the historical omnibus (29082026_0326, 27082026_2205, 09092026_1548).
--
-- It sorts after every pinned file and after 29082026_1914's rename block, so
-- private.timesheet_weekly_chain_delete_apply_base_v1 already exists when this
-- file replaces its body.
--
-- CREATE OR REPLACE preserves each routine's existing owner and ACL when the
-- signature is unchanged, but that alone is not safe: any signature drift would
-- create a NEW overload carrying the default EXECUTE to PUBLIC.  The closing
-- block of this file therefore restates the measured installed owner, revokes
-- and grants of each of the four routines exactly (see the note above those
-- statements).  It adds no privilege: public, anon and authenticated are
-- revoked everywhere, service_role keeps execute on exactly the three owners
-- that already have it, and private._candidate_expense_payment_edit_shell_v1
-- keeps none, so the installed service-role exposure is unchanged.  Nothing
-- here calls a Banking Pay, Draft, execution, cancellation, settlement,
-- provider, recovery or remittance owner.

\set ON_ERROR_STOP on

begin;

-- E21 public.timesheet_standard_delete_apply_v1
CREATE OR REPLACE FUNCTION public.timesheet_standard_delete_apply_v1(p_timesheet_id uuid, p_actor_user_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_preview jsonb;
  v_recheck jsonb;
  v_decision text := 'BLOCKED';
  v_current_timesheet_id uuid := NULL;
  v_recheck_current_timesheet_id uuid := NULL;
  v_initial_row_signature text := NULL;
  v_recheck_row_signature text := NULL;
  v_booking_id text := NULL;
  v_recheck_booking_id text := NULL;

  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_nhsp_shift_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_all_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_all_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_contract_ids uuid[] := ARRAY[]::uuid[];

  v_recheck_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_nhsp_shift_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_preserved_source_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_preserved_source_contract_week_ids uuid[] := ARRAY[]::uuid[];

  v_r2_keys text[] := ARRAY[]::text[];
  v_locked_contracts integer := 0;
  v_locked_timesheets integer := 0;
  v_locked_contract_weeks integer := 0;
  v_locked_nhsp_shifts integer := 0;
  v_detached_contract_weeks integer := 0;
  v_detached_nhsp_shifts integer := 0;
  v_deleted_count integer := 0;

  -- Plan 6.2 G6-11 (proof/34 section 3, entry point E21).
  v_weekly_source_guard jsonb;
  v_weekly_source_root uuid;
BEGIN
  PERFORM set_config('lock_timeout', '750ms', true);

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ID_REQUIRED';
  END IF;
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'ACTOR_USER_ID_REQUIRED';
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'EXPECTED_TIMESHEET_ID_REQUIRED';
  END IF;
  IF NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'EXPECTED_ROW_SIGNATURE_REQUIRED';
  END IF;

  v_preview := public.timesheet_standard_delete_preview_v1(
    p_timesheet_id,
    p_actor_user_id,
    p_expected_timesheet_id,
    p_expected_row_signature
  );
  v_decision := COALESCE(v_preview ->> 'decision', 'BLOCKED');

  IF v_decision <> 'PERMANENT_DELETE' THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', CASE
        WHEN v_decision = 'ARCHIVE_REQUIRED' THEN 'ARCHIVE_REQUIRED'
        ELSE 'DELETE_BLOCKED'
      END
    );
  END IF;

  v_current_timesheet_id := NULLIF(v_preview ->> 'current_timesheet_id', '')::uuid;
  v_initial_row_signature := NULLIF(BTRIM(COALESCE(v_preview ->> 'current_row_signature', '')), '');
  v_booking_id := NULLIF(v_preview ->> 'booking_id', '');

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'timesheet_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'contract_week_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_nhsp_shift_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'nhsp_shift_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_preserved_source_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'preserved_source_timesheet_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_preserved_source_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'preserved_source_contract_week_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(DISTINCT all_timesheet_ids.id ORDER BY all_timesheet_ids.id), ARRAY[]::uuid[])
    INTO v_all_timesheet_ids
  FROM unnest(v_timesheet_ids || v_preserved_source_timesheet_ids) AS all_timesheet_ids(id)
  WHERE all_timesheet_ids.id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT all_contract_week_ids.id ORDER BY all_contract_week_ids.id), ARRAY[]::uuid[])
    INTO v_all_contract_week_ids
  FROM unnest(v_contract_week_ids || v_preserved_source_contract_week_ids) AS all_contract_week_ids(id)
  WHERE all_contract_week_ids.id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT represented_contract.contract_id ORDER BY represented_contract.contract_id), ARRAY[]::uuid[])
    INTO v_contract_ids
  FROM (
    SELECT represented_contract_week.contract_id
    FROM public.contract_weeks AS represented_contract_week
    WHERE represented_contract_week.id = ANY(v_all_contract_week_ids)
      AND represented_contract_week.contract_id IS NOT NULL
  ) AS represented_contract;

  IF v_current_timesheet_id IS NULL
     OR NULLIF(BTRIM(COALESCE(v_booking_id, '')), '') IS NULL
     OR v_current_timesheet_id IS DISTINCT FROM p_expected_timesheet_id
     OR v_initial_row_signature IS DISTINCT FROM BTRIM(p_expected_row_signature) THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'DELETE_PREVIEW_STALE'
    );
  END IF;

  IF COALESCE(array_length(v_timesheet_ids, 1), 0) = 0 THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'REMOVAL_UNIT_EMPTY'
    );
  END IF;

  IF COALESCE(array_length(v_all_timesheet_ids, 1), 0) > 64
     OR COALESCE(array_length(v_all_contract_week_ids, 1), 0) > 64
     OR COALESCE(array_length(v_nhsp_shift_ids, 1), 0) > 512 THEN
    RAISE EXCEPTION USING MESSAGE = 'REMOVAL_UNIT_TOO_LARGE';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM unnest(v_timesheet_ids) AS target_timesheet(id)
    WHERE target_timesheet.id = ANY(v_preserved_source_timesheet_ids)
  ) OR EXISTS (
    SELECT 1
    FROM unnest(v_contract_week_ids) AS target_contract_week(id)
    WHERE target_contract_week.id = ANY(v_preserved_source_contract_week_ids)
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_OVERLAPS_DELETE_TARGET';
  END IF;

  -- Use the same booking-series advisory authority as Timesheet rotation.
  -- A concurrent rotation must finish before this Delete can confirm its removal unit.
  IF NOT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtext(v_booking_id)::bigint) THEN
    RAISE EXCEPTION USING
      ERRCODE = '55P03',
      MESSAGE = 'BOOKING_LOCK_NOT_AVAILABLE';
  END IF;

  -- Lock every represented Contract root before its child rows. A Contract Week
  -- insert or contract reassignment must take a foreign-key key-share lock on the
  -- destination Contract, so it cannot create a preserved-source phantom after
  -- these roots have been locked.
  SELECT COUNT(*)::integer
    INTO v_locked_contracts
  FROM (
    SELECT contract_root.id
    FROM public.contracts AS contract_root
    WHERE contract_root.id = ANY(v_contract_ids)
    ORDER BY contract_root.id
    FOR UPDATE NOWAIT
  ) AS locked_contracts;

  IF v_locked_contracts <> COALESCE(array_length(v_contract_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  -- Freeze all existing Contract Weeks below the represented Contract roots.
  -- This prevents an existing sibling week from changing date/sequence/type and
  -- entering or leaving the canonical preserved-source predicate after recheck.
  PERFORM 1
  FROM public.contract_weeks AS contract_week_predicate_row
  WHERE contract_week_predicate_row.contract_id = ANY(v_contract_ids)
  ORDER BY contract_week_predicate_row.contract_id,
           contract_week_predicate_row.week_ending_date,
           contract_week_predicate_row.additional_seq,
           contract_week_predicate_row.id
  FOR UPDATE NOWAIT;

  -- Lock the complete booking series together with every target or preserved
  -- Timesheet in deterministic UUID order. The advisory lock serialises standard
  -- rotation authorities; the row locks freeze exact identity and parent links.
  SELECT COUNT(*) FILTER (
           WHERE locked_timesheet.timesheet_id = ANY(v_all_timesheet_ids)
         )::integer
    INTO v_locked_timesheets
  FROM (
    SELECT target_timesheet.timesheet_id
    FROM public.timesheets AS target_timesheet
    WHERE target_timesheet.booking_id = v_booking_id
       OR target_timesheet.timesheet_id = ANY(v_all_timesheet_ids)
    ORDER BY target_timesheet.timesheet_id
    FOR UPDATE NOWAIT
  ) AS locked_timesheet;

  IF v_locked_timesheets <> COALESCE(array_length(v_all_timesheet_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  PERFORM 1
  FROM public.timesheets_financials AS target_financial
  WHERE target_financial.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY target_financial.timesheet_id, target_financial.id
  FOR UPDATE;

  SELECT COUNT(*)::integer
    INTO v_locked_contract_weeks
  FROM (
    SELECT target_contract_week.id
    FROM public.contract_weeks AS target_contract_week
    WHERE target_contract_week.id = ANY(v_all_contract_week_ids)
    ORDER BY target_contract_week.id
    FOR UPDATE NOWAIT
  ) AS locked_contract_weeks;

  IF v_locked_contract_weeks <> COALESCE(array_length(v_all_contract_week_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.contract_weeks AS represented_contract_week
    WHERE represented_contract_week.id = ANY(v_all_contract_week_ids)
      AND NOT (represented_contract_week.contract_id = ANY(v_contract_ids))
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  SELECT COUNT(*)::integer
    INTO v_locked_nhsp_shifts
  FROM (
    SELECT target_shift.id
    FROM public.nhsp_shifts AS target_shift
    WHERE target_shift.id = ANY(v_nhsp_shift_ids)
    ORDER BY target_shift.id
    FOR UPDATE
  ) AS locked_nhsp_shifts;

  IF v_locked_nhsp_shifts <> COALESCE(array_length(v_nhsp_shift_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'NHSP_SHIFT_TARGET_SET_CHANGED';
  END IF;

  PERFORM 1
  FROM public.manual_timesheet_queue AS queue_row
  WHERE queue_row.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY queue_row.id
  FOR UPDATE;

  PERFORM 1
  FROM public.timesheet_evidence AS evidence_row
  WHERE evidence_row.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY evidence_row.id
  FOR UPDATE;

  IF EXISTS (
    SELECT 1
    FROM public.contract_weeks AS target_contract_week
    WHERE target_contract_week.id = ANY(v_contract_week_ids)
      AND (
        target_contract_week.timesheet_id IS NULL
        OR target_contract_week.timesheet_id <> ALL(v_timesheet_ids)
      )
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'CONTRACT_WEEK_TARGET_SET_CHANGED';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.nhsp_shifts AS target_shift
    WHERE target_shift.id = ANY(v_nhsp_shift_ids)
      AND (
        target_shift.timesheet_id IS NULL
        OR target_shift.timesheet_id <> ALL(v_timesheet_ids)
      )
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'NHSP_SHIFT_TARGET_SET_CHANGED';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.contract_weeks AS source_contract_week
    WHERE source_contract_week.id = ANY(v_preserved_source_contract_week_ids)
      AND (
        COALESCE(source_contract_week.is_adjustment, false)
        OR COALESCE(source_contract_week.additional_seq, 0) <> 0
        OR NOT EXISTS (
          SELECT 1
          FROM public.contract_weeks AS target_contract_week
          WHERE target_contract_week.id = ANY(v_contract_week_ids)
            AND target_contract_week.contract_id = source_contract_week.contract_id
            AND target_contract_week.week_ending_date = source_contract_week.week_ending_date
        )
        OR (
          source_contract_week.timesheet_id IS NOT NULL
          AND source_contract_week.timesheet_id <> ALL(v_preserved_source_timesheet_ids)
        )
      )
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.timesheets AS source_timesheet
    WHERE source_timesheet.timesheet_id = ANY(v_preserved_source_timesheet_ids)
      AND NOT EXISTS (
        SELECT 1
        FROM public.timesheets AS target_timesheet
        WHERE target_timesheet.timesheet_id = ANY(v_timesheet_ids)
          AND target_timesheet.parent_timesheet_id = source_timesheet.timesheet_id
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.contract_weeks AS source_contract_week
        WHERE source_contract_week.id = ANY(v_preserved_source_contract_week_ids)
          AND source_contract_week.timesheet_id = source_timesheet.timesheet_id
      )
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  v_recheck := public.timesheet_standard_delete_preview_v1(
    p_timesheet_id,
    p_actor_user_id,
    p_expected_timesheet_id,
    p_expected_row_signature
  );
  v_decision := COALESCE(v_recheck ->> 'decision', 'BLOCKED');
  v_recheck_current_timesheet_id := NULLIF(v_recheck ->> 'current_timesheet_id', '')::uuid;
  v_recheck_row_signature := NULLIF(BTRIM(COALESCE(v_recheck ->> 'current_row_signature', '')), '');
  v_recheck_booking_id := NULLIF(v_recheck ->> 'booking_id', '');

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'timesheet_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'contract_week_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_nhsp_shift_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'nhsp_shift_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_preserved_source_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'preserved_source_timesheet_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(ids.value::uuid ORDER BY ids.value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_preserved_source_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'preserved_source_contract_week_ids', '[]'::jsonb)) AS ids(value);

  IF v_decision <> 'PERMANENT_DELETE'
     OR v_recheck_current_timesheet_id IS DISTINCT FROM v_current_timesheet_id
     OR v_recheck_booking_id IS DISTINCT FROM v_booking_id
     OR v_recheck_row_signature IS DISTINCT FROM v_initial_row_signature
     OR v_recheck_row_signature IS DISTINCT FROM BTRIM(p_expected_row_signature)
     OR v_recheck_timesheet_ids IS DISTINCT FROM v_timesheet_ids
     OR v_recheck_contract_week_ids IS DISTINCT FROM v_contract_week_ids
     OR v_recheck_nhsp_shift_ids IS DISTINCT FROM v_nhsp_shift_ids
     OR v_recheck_preserved_source_timesheet_ids IS DISTINCT FROM v_preserved_source_timesheet_ids
     OR v_recheck_preserved_source_contract_week_ids IS DISTINCT FROM v_preserved_source_contract_week_ids THEN
    RETURN v_recheck || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'REMOVAL_UNIT_CHANGED',
      'locked_target_set', jsonb_build_object(
        'timesheet_ids', to_jsonb(v_timesheet_ids),
        'contract_week_ids', to_jsonb(v_contract_week_ids),
        'nhsp_shift_ids', to_jsonb(v_nhsp_shift_ids),
        'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
        'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids)
      )
    );
  END IF;

  SELECT COALESCE(
           array_agg(exact_key.queue_key ORDER BY convert_to(exact_key.queue_key, 'UTF8')),
           ARRAY[]::text[]
         )
    INTO v_r2_keys
  FROM (
    SELECT DISTINCT ON (convert_to(raw_key.queue_key, 'UTF8')) raw_key.queue_key
    FROM (
      SELECT direct_key.key_value AS queue_key
      FROM public.timesheets AS target_timesheet
      CROSS JOIN LATERAL unnest(ARRAY[
        target_timesheet.manual_pdf_r2_key,
        target_timesheet.r2_nurse_key,
        target_timesheet.r2_auth_key,
        target_timesheet.qr_r2_key
      ]) AS direct_key(key_value)
      WHERE target_timesheet.timesheet_id = ANY(v_timesheet_ids)

      UNION ALL

      SELECT evidence_row.storage_key AS queue_key
      FROM public.timesheet_evidence AS evidence_row
      WHERE evidence_row.timesheet_id = ANY(v_timesheet_ids)

      UNION ALL

      SELECT queue_row.r2_key AS queue_key
      FROM public.manual_timesheet_queue AS queue_row
      WHERE queue_row.timesheet_id = ANY(v_timesheet_ids)

      UNION ALL

      SELECT financial_key.key_value AS queue_key
      FROM public.timesheets_financials AS target_financial
      CROSS JOIN LATERAL unnest(ARRAY[
        target_financial.expenses_evidence_r2_key,
        target_financial.mileage_evidence_r2_key
      ]) AS financial_key(key_value)
      WHERE target_financial.timesheet_id = ANY(v_timesheet_ids)

      UNION ALL

      SELECT manifest_key.key_value AS queue_key
      FROM public.timesheets_financials AS target_financial
      CROSS JOIN LATERAL (
        WITH RECURSIVE manifest_roots(root_value) AS (
          SELECT COALESCE(target_financial.expenses_evidence_manifest, 'null'::jsonb)
          UNION ALL
          SELECT COALESCE(target_financial.mileage_evidence_manifest, 'null'::jsonb)
        ), manifest_walk(value, edge_key, depth) AS (
          SELECT manifest_roots.root_value, NULL::text, 0
          FROM manifest_roots

          UNION ALL

          SELECT manifest_child.value, manifest_child.edge_key, manifest_walk.depth + 1
          FROM manifest_walk
          CROSS JOIN LATERAL (
            SELECT object_entry.key AS edge_key, object_entry.value
            FROM jsonb_each(
              CASE
                WHEN jsonb_typeof(manifest_walk.value) = 'object' THEN manifest_walk.value
                ELSE '{}'::jsonb
              END
            ) AS object_entry(key, value)

            UNION ALL

            SELECT NULL::text AS edge_key, array_entry.value
            FROM jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(manifest_walk.value) = 'array' THEN manifest_walk.value
                ELSE '[]'::jsonb
              END
            ) AS array_entry(value)
          ) AS manifest_child
          WHERE manifest_walk.depth < 8
        )
        SELECT manifest_walk.value #>> '{}' AS key_value
        FROM manifest_walk
        WHERE LOWER(COALESCE(manifest_walk.edge_key, '')) IN (
          'r2_key',
          'storage_key',
          'file_key',
          'canonical_key',
          'object_key'
        )
          AND jsonb_typeof(manifest_walk.value) = 'string'
      ) AS manifest_key
      WHERE target_financial.timesheet_id = ANY(v_timesheet_ids)
    ) AS raw_key
    WHERE raw_key.queue_key IS NOT NULL
      AND BTRIM(raw_key.queue_key) <> ''
    ORDER BY convert_to(raw_key.queue_key, 'UTF8')
  ) AS exact_key;

  -- Plan 6.2 G6-11 (proof/34 section 3, entry point E21): permanent deletion
  -- destroys the physical Timesheet identity a Weekly Source root is bound to,
  -- so an authorised Weekly-Source-managed root refuses here: before this
  -- owner's first write and while it holds the booking-series advisory lock
  -- and every target row FOR UPDATE.  An unmanaged family is unaffected.
  FOREACH v_weekly_source_root IN ARRAY COALESCE(v_timesheet_ids, ARRAY[]::uuid[])
  LOOP
    v_weekly_source_guard := private.weekly_source_managed_root_guard_v1(v_weekly_source_root);
    -- HANDOVER 2 round-5 ruling B3 and A4 (18 September 2026).  The refusal is
    -- NARROWED: it applies to a Weekly-Source managed root, to a bound or
    -- protected family whose identity cannot be resolved, to a family carrying
    -- protected pay evidence but no authorisation row
    -- (PROTECTED_ROOT_AUTHORITY_MISSING), and to the live-record-on-an-
    -- unauthorised-Timesheet contradiction, which must never be allowed to
    -- continue merely because managed is false.  An unrelated, UNBOUND ordinary
    -- family -- including a malformed one -- keeps exactly the behaviour it had
    -- before this feature was installed.  Absent, null and non-boolean take the
    -- unsafe value at every read (Part 1 addendum rule 4).
    IF (COALESCE((v_weekly_source_guard->>'managed')::boolean, true)
         and (COALESCE((v_weekly_source_guard->>'ok')::boolean, true)
              or COALESCE((v_weekly_source_guard->>'weekly_source_bound')::boolean, true)))
       or COALESCE((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
       or (v_weekly_source_guard->>'protected_target_ownership_state') is not null THEN
      RAISE EXCEPTION 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
        USING ERRCODE = '55000',
              DETAIL = jsonb_build_object(
                'code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
                'entry_point','E21:public.timesheet_standard_delete_apply_v1',
                'block_reason','WEEKLY_SOURCE_MANAGED_ROOT',
                'refusal_basis', case
                  -- HANDOVER 2 round-5 Part E: the trim-equivalent split family is a
                  -- canonical booking-reference collision and must be named as one.
                  -- WP-03 handoff N20: accept BOTH the installed token and the ruled name for one
                  -- release, so the order of this edit and WP-03's rename cannot open a gap in
                  -- which Office stops seeing the ruled name.
                  when v_weekly_source_guard->>'reason' in (
                         'FAMILY_SPLIT_BY_WHITESPACE','BOOKING_REFERENCE_CANONICAL_COLLISION')
                    then 'BOOKING_REFERENCE_CANONICAL_COLLISION'
                  when coalesce((v_weekly_source_guard->>'managed')::boolean, true) and coalesce((v_weekly_source_guard->>'ok')::boolean, true)
                    then 'WEEKLY_SOURCE_MANAGED_ROOT'
                  when coalesce((v_weekly_source_guard->>'managed')::boolean, true)
                    then 'WEEKLY_SOURCE_BOUND_OR_PROTECTED_ROOT_UNRESOLVABLE'
                  when coalesce((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
                    then 'AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET'
                  else 'PROTECTED_ROOT_AUTHORITY_MISSING' end,
                'integrity_failure', not coalesce((v_weekly_source_guard->>'ok')::boolean,false),
                'timesheet_id', v_weekly_source_root,
                'reason', v_weekly_source_guard->>'reason'
              )::text;
    END IF;
  END LOOP;

  INSERT INTO public.audit_events (
    actor_user_id,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  ) VALUES (
    p_actor_user_id,
    'timesheets',
    v_current_timesheet_id::text,
    'TIMESHEET_PERMANENT_DELETE_APPLIED',
    jsonb_build_object(
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids),
      'nhsp_shift_ids', to_jsonb(v_nhsp_shift_ids),
      'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
      'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids),
      'decision', 'PERMANENT_DELETE'
    ),
    jsonb_build_object('deleted', true),
    'FINANCIALLY_CLEAN_TIMESHEET'
  );

  UPDATE public.contract_weeks AS target_contract_week
  SET timesheet_id = NULL,
      status = 'OPEN'::public.contract_week_status_enum,
      updated_at = now()
  WHERE target_contract_week.id = ANY(v_contract_week_ids)
    AND target_contract_week.timesheet_id = ANY(v_timesheet_ids);
  GET DIAGNOSTICS v_detached_contract_weeks = ROW_COUNT;

  IF v_detached_contract_weeks <> COALESCE(array_length(v_contract_week_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'CONTRACT_WEEK_TARGET_SET_CHANGED';
  END IF;

  UPDATE public.nhsp_shifts AS target_shift
  SET timesheet_id = NULL,
      updated_at = now()
  WHERE target_shift.id = ANY(v_nhsp_shift_ids)
    AND target_shift.timesheet_id = ANY(v_timesheet_ids);
  GET DIAGNOSTICS v_detached_nhsp_shifts = ROW_COUNT;

  IF v_detached_nhsp_shifts <> COALESCE(array_length(v_nhsp_shift_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'NHSP_SHIFT_TARGET_SET_CHANGED';
  END IF;

  DELETE FROM public.manual_timesheet_queue AS queue_row
  WHERE queue_row.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.timesheet_evidence AS evidence_row
  WHERE evidence_row.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.ts_pdfs_outbox AS pdf_outbox
  WHERE pdf_outbox.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.ts_financials_outbox AS financial_outbox
  WHERE financial_outbox.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.timesheet_validations AS validation_row
  WHERE validation_row.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.hr_results AS result_row
  WHERE result_row.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.hr_issue_emails AS issue_email
  WHERE issue_email.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.pay_item_snoozes AS snooze_row
  WHERE snooze_row.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.timesheet_summary_pay_state_cache AS pay_state_cache
  WHERE pay_state_cache.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.timesheets_financials AS target_financial
  WHERE target_financial.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.timesheets AS target_timesheet
  WHERE target_timesheet.timesheet_id = ANY(v_timesheet_ids);
  GET DIAGNOSTICS v_deleted_count = ROW_COUNT;

  IF v_deleted_count <> COALESCE(array_length(v_timesheet_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'DELETE_COUNT_MISMATCH';
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'kind', 'STANDARD_DELETE',
    'decision', 'PERMANENT_DELETE',
    'apply_performed', true,
    'deleted', true,
    'committed', true,
    'database_commit_confirmed', true,
    'current_timesheet_id', v_current_timesheet_id,
    'current_row_signature', v_initial_row_signature,
    'timesheet_ids', to_jsonb(v_timesheet_ids),
    'contract_week_ids', to_jsonb(v_contract_week_ids),
    'nhsp_shift_ids', to_jsonb(v_nhsp_shift_ids),
    'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
    'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids),
    'deleted_timesheet_ids', to_jsonb(v_timesheet_ids),
    'deleted_contract_week_ids', '[]'::jsonb,
    'detached_contract_week_ids', to_jsonb(v_contract_week_ids),
    'detached_nhsp_shift_ids', to_jsonb(v_nhsp_shift_ids),
    'detached_contract_weeks', v_detached_contract_weeks,
    'detached_nhsp_shifts', v_detached_nhsp_shifts,
    'deleted_timesheets', v_deleted_count,
    'r2_cleanup_keys', to_jsonb(v_r2_keys),
    'r2_cleanup_required', COALESCE(array_length(v_r2_keys, 1), 0) > 0
  );
EXCEPTION
  WHEN lock_not_available OR deadlock_detected THEN
    RETURN jsonb_build_object(
      'ok', false,
      'kind', 'STANDARD_DELETE',
      'decision', 'BLOCKED',
      'apply_performed', false,
      'error_code', 'LOCK_TIMEOUT',
      'message', 'The Timesheet is currently being changed. Refresh and try again.'
    );
END;
$function$;

-- E22 public.timesheet_weekly_manual_adjustment_delete_apply
CREATE OR REPLACE FUNCTION public.timesheet_weekly_manual_adjustment_delete_apply(
  p_timesheet_id uuid,
  p_actor_user_id uuid,
  p_expected_timesheet_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_contract_week_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_preserved_source_timesheet_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_preserved_source_contract_week_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_row_signature text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_preview jsonb;
  v_recheck jsonb;
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_all_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_all_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_preserved_source_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_preserved_source_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_expected_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_expected_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_expected_preserved_source_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_expected_preserved_source_contract_week_ids uuid[] := ARRAY[]::uuid[];
  -- Plan 6.2 G6-11 (proof/34 section 3, entry point E22).
  v_weekly_source_guard jsonb;
  v_weekly_source_root uuid;
  v_signature_payload jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_primary_contract_week_id uuid := NULL;
  v_r2_keys text[] := ARRAY[]::text[];
  v_current_timesheet_id uuid;
  v_deleted_timesheets integer := 0;
  v_deleted_contract_weeks integer := 0;
  v_locked_timesheets integer := 0;
  v_locked_contract_weeks integer := 0;
BEGIN
  PERFORM set_config('lock_timeout', '750ms', true);
  IF p_timesheet_id IS NULL THEN RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ID_REQUIRED'; END IF;
  IF p_actor_user_id IS NULL THEN RAISE EXCEPTION USING MESSAGE = 'ACTOR_USER_ID_REQUIRED'; END IF;

  IF p_expected_timesheet_ids IS NULL
     OR p_expected_contract_week_ids IS NULL
     OR p_expected_preserved_source_timesheet_ids IS NULL
     OR p_expected_preserved_source_contract_week_ids IS NULL
     OR NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'kind', 'WEEKLY_MANUAL_ADJUSTMENT_DELETE',
      'decision', 'BLOCKED',
      'apply_performed', false,
      'error_code', 'EXPECTED_DELETE_PREVIEW_REQUIRED'
    );
  END IF;

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_timesheet_ids
  FROM unnest(p_expected_timesheet_ids) AS expected(id)
  WHERE id IS NOT NULL;
  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_contract_week_ids
  FROM unnest(p_expected_contract_week_ids) AS expected(id)
  WHERE id IS NOT NULL;
  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_preserved_source_timesheet_ids
  FROM unnest(p_expected_preserved_source_timesheet_ids) AS expected(id)
  WHERE id IS NOT NULL;
  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_preserved_source_contract_week_ids
  FROM unnest(p_expected_preserved_source_contract_week_ids) AS expected(id)
  WHERE id IS NOT NULL;

  v_preview := public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id, p_actor_user_id);
  IF COALESCE(v_preview ->> 'decision', 'BLOCKED') <> 'PERMANENT_DELETE' THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', CASE WHEN v_preview ->> 'decision' = 'ARCHIVE_REQUIRED' THEN 'ARCHIVE_REQUIRED' ELSE 'DELETE_BLOCKED' END
    );
  END IF;

  v_current_timesheet_id := NULLIF(v_preview ->> 'current_timesheet_id', '')::uuid;
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[]) INTO v_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'timesheet_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[]) INTO v_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'contract_week_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[]) INTO v_preserved_source_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'preserved_source_timesheet_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[]) INTO v_preserved_source_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'preserved_source_contract_week_ids', '[]'::jsonb)) AS ids(value);

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_all_timesheet_ids
  FROM unnest(v_timesheet_ids || v_preserved_source_timesheet_ids) AS all_ids(id);
  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_all_contract_week_ids
  FROM unnest(v_contract_week_ids || v_preserved_source_contract_week_ids) AS all_ids(id);

  IF v_timesheet_ids IS DISTINCT FROM v_expected_timesheet_ids
     OR v_contract_week_ids IS DISTINCT FROM v_expected_contract_week_ids
     OR v_preserved_source_timesheet_ids IS DISTINCT FROM v_expected_preserved_source_timesheet_ids
     OR v_preserved_source_contract_week_ids IS DISTINCT FROM v_expected_preserved_source_contract_week_ids THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'DELETE_PREVIEW_STALE',
      'expected_target_set', jsonb_build_object(
        'timesheet_ids', to_jsonb(v_expected_timesheet_ids),
        'contract_week_ids', to_jsonb(v_expected_contract_week_ids),
        'preserved_source_timesheet_ids', to_jsonb(v_expected_preserved_source_timesheet_ids),
        'preserved_source_contract_week_ids', to_jsonb(v_expected_preserved_source_contract_week_ids)
      )
    );
  END IF;

  IF COALESCE(array_length(v_timesheet_ids, 1), 0) = 0
     OR COALESCE(array_length(v_contract_week_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION USING MESSAGE = 'EMPTY_REMOVAL_UNIT';
  END IF;
  IF COALESCE(array_length(v_all_timesheet_ids, 1), 0) > 64
     OR COALESCE(array_length(v_all_contract_week_ids, 1), 0) > 64 THEN
    RAISE EXCEPTION USING MESSAGE = 'REMOVAL_UNIT_TOO_LARGE';
  END IF;
  IF EXISTS (
    SELECT 1 FROM unnest(v_timesheet_ids) AS target(id)
    WHERE target.id = ANY(v_preserved_source_timesheet_ids)
  ) OR EXISTS (
    SELECT 1 FROM unnest(v_contract_week_ids) AS target(id)
    WHERE target.id = ANY(v_preserved_source_contract_week_ids)
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_OVERLAPS_DELETE_TARGET';
  END IF;

  -- Lock all relational roots before child rows.  Queue metadata is never used
  -- as deletion authority; only exact foreign-key identities are authoritative.
  PERFORM 1
  FROM public.contracts AS c
  WHERE c.id IN (
    SELECT DISTINCT cw.contract_id
    FROM public.contract_weeks AS cw
    WHERE cw.id = ANY(v_all_contract_week_ids)
      AND cw.contract_id IS NOT NULL
  )
  ORDER BY c.id
  FOR UPDATE;

  SELECT COUNT(*)::integer
    INTO v_locked_timesheets
  FROM (
    SELECT t.timesheet_id
    FROM public.timesheets AS t
    WHERE t.timesheet_id = ANY(v_all_timesheet_ids)
    ORDER BY t.timesheet_id
    FOR UPDATE
  ) AS locked_timesheets;
  IF v_locked_timesheets <> COALESCE(array_length(v_all_timesheet_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  PERFORM 1
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY tf.timesheet_id, tf.id
  FOR UPDATE;

  SELECT COUNT(*)::integer
    INTO v_locked_contract_weeks
  FROM (
    SELECT cw.id
    FROM public.contract_weeks AS cw
    WHERE cw.id = ANY(v_all_contract_week_ids)
    ORDER BY cw.id
    FOR UPDATE
  ) AS locked_contract_weeks;
  IF v_locked_contract_weeks <> COALESCE(array_length(v_all_contract_week_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  -- Validate both sides of the relationship while the exact rows are locked.
  IF EXISTS (
    SELECT 1
    FROM public.contract_weeks AS target_cw
    WHERE target_cw.id = ANY(v_contract_week_ids)
      AND (
        COALESCE(target_cw.is_adjustment, false) IS NOT TRUE
        OR COALESCE(target_cw.additional_seq, 0) <= 0
        OR target_cw.timesheet_id IS NULL
        OR target_cw.timesheet_id <> ALL(v_timesheet_ids)
      )
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'MANUAL_ADJUSTMENT_TARGET_CHANGED';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.contract_weeks AS source_cw
    WHERE source_cw.id = ANY(v_preserved_source_contract_week_ids)
      AND (
        COALESCE(source_cw.is_adjustment, false)
        OR COALESCE(source_cw.additional_seq, 0) <> 0
        OR (source_cw.timesheet_id IS NOT NULL AND source_cw.timesheet_id <> ALL(v_preserved_source_timesheet_ids))
      )
  ) OR EXISTS (
    SELECT 1
    FROM public.contract_weeks AS target_cw
    WHERE target_cw.id = ANY(v_contract_week_ids)
      AND NOT EXISTS (
        SELECT 1
        FROM public.contract_weeks AS source_cw
        WHERE source_cw.id = ANY(v_preserved_source_contract_week_ids)
          AND source_cw.contract_id = target_cw.contract_id
          AND source_cw.week_ending_date = target_cw.week_ending_date
          AND COALESCE(source_cw.is_adjustment, false) = false
          AND COALESCE(source_cw.additional_seq, 0) = 0
      )
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'PRESERVED_SOURCE_CHANGED';
  END IF;

  v_recheck := public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id, p_actor_user_id);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'timesheet_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'contract_week_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_preserved_source_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'preserved_source_timesheet_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_preserved_source_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'preserved_source_contract_week_ids', '[]'::jsonb)) AS ids(value);

  IF COALESCE(v_recheck ->> 'decision', 'BLOCKED') <> 'PERMANENT_DELETE'
     OR NULLIF(v_recheck ->> 'current_timesheet_id', '')::uuid IS DISTINCT FROM v_current_timesheet_id
     OR v_recheck_timesheet_ids IS DISTINCT FROM v_timesheet_ids
     OR v_recheck_contract_week_ids IS DISTINCT FROM v_contract_week_ids
     OR v_recheck_preserved_source_timesheet_ids IS DISTINCT FROM v_preserved_source_timesheet_ids
     OR v_recheck_preserved_source_contract_week_ids IS DISTINCT FROM v_preserved_source_contract_week_ids THEN
    RETURN v_recheck || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'DELETE_RECLASSIFIED',
      'locked_target_set', jsonb_build_object(
        'timesheet_ids', to_jsonb(v_timesheet_ids),
        'contract_week_ids', to_jsonb(v_contract_week_ids),
        'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
        'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids)
      )
    );
  END IF;

  SELECT cw.id
    INTO v_primary_contract_week_id
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current_timesheet_id
    AND cw.id = ANY(v_contract_week_ids)
  ORDER BY cw.id
  LIMIT 1;

  v_signature_payload := public.timesheet_lifecycle_guard_signature_v1(
    v_current_timesheet_id,
    v_primary_contract_week_id,
    false
  );
  v_current_row_signature := NULLIF(BTRIM(COALESCE(
    v_signature_payload ->> 'backend_row_signature',
    v_signature_payload ->> 'row_signature',
    v_signature_payload ->> 'signature',
    ''
  )), '');

  IF v_current_row_signature IS DISTINCT FROM BTRIM(p_expected_row_signature) THEN
    RETURN v_recheck || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'ROW_SIGNATURE_MISMATCH',
      'expected_row_signature', BTRIM(p_expected_row_signature),
      'current_row_signature', v_current_row_signature
    );
  END IF;

  -- Freeze existing mutable manual-queue R2 keys before key collection.
  -- The exact queue rows are locked in deterministic order so an r2_key cannot
  -- change between server-side key collection and the relational delete.
  PERFORM 1
  FROM public.manual_timesheet_queue AS queue_row
  WHERE queue_row.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY queue_row.id
  FOR UPDATE;

  SELECT COALESCE(array_agg(DISTINCT storage_key ORDER BY storage_key), ARRAY[]::text[])
    INTO v_r2_keys
  FROM (
    SELECT NULLIF(BTRIM(key_value), '') AS storage_key
    FROM public.timesheets AS t
    CROSS JOIN LATERAL unnest(ARRAY[t.manual_pdf_r2_key, t.r2_nurse_key, t.r2_auth_key, t.qr_r2_key]) AS keys(key_value)
    WHERE t.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT NULLIF(BTRIM(e.storage_key), '') FROM public.timesheet_evidence e WHERE e.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT NULLIF(BTRIM(q.r2_key), '') FROM public.manual_timesheet_queue q WHERE q.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT NULLIF(BTRIM(key_value), '')
      FROM public.timesheets_financials tf
      CROSS JOIN LATERAL unnest(ARRAY[tf.expenses_evidence_r2_key, tf.mileage_evidence_r2_key]) AS keys(key_value)
      WHERE tf.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT manifest_key
      FROM public.timesheets_financials tf
      CROSS JOIN LATERAL unnest(
        public.cloudtms_jsonb_storage_keys_v1(tf.expenses_evidence_manifest, 8)
        || public.cloudtms_jsonb_storage_keys_v1(tf.mileage_evidence_manifest, 8)
      ) AS manifest(manifest_key)
      WHERE tf.timesheet_id = ANY(v_timesheet_ids)
  ) AS keys WHERE storage_key IS NOT NULL;

  -- Plan 6.2 G6-11 (proof/34 section 3, entry point E22): permanent deletion
  -- destroys the physical Timesheet identity a Weekly Source root is bound to,
  -- so an authorised Weekly-Source-managed root refuses here: before this
  -- owner's first write and while it holds every target row FOR UPDATE.  An
  -- unmanaged family is unaffected.
  FOREACH v_weekly_source_root IN ARRAY COALESCE(v_timesheet_ids, ARRAY[]::uuid[])
  LOOP
    v_weekly_source_guard := private.weekly_source_managed_root_guard_v1(v_weekly_source_root);
    -- HANDOVER 2 round-5 ruling B3 and A4 (18 September 2026).  The refusal is
    -- NARROWED: it applies to a Weekly-Source managed root, to a bound or
    -- protected family whose identity cannot be resolved, to a family carrying
    -- protected pay evidence but no authorisation row
    -- (PROTECTED_ROOT_AUTHORITY_MISSING), and to the live-record-on-an-
    -- unauthorised-Timesheet contradiction, which must never be allowed to
    -- continue merely because managed is false.  An unrelated, UNBOUND ordinary
    -- family -- including a malformed one -- keeps exactly the behaviour it had
    -- before this feature was installed.  Absent, null and non-boolean take the
    -- unsafe value at every read (Part 1 addendum rule 4).
    IF (COALESCE((v_weekly_source_guard->>'managed')::boolean, true)
         and (COALESCE((v_weekly_source_guard->>'ok')::boolean, true)
              or COALESCE((v_weekly_source_guard->>'weekly_source_bound')::boolean, true)))
       or COALESCE((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
       or (v_weekly_source_guard->>'protected_target_ownership_state') is not null THEN
      RAISE EXCEPTION 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
        USING ERRCODE = '55000',
              DETAIL = jsonb_build_object(
                'code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
                'entry_point','E22:public.timesheet_weekly_manual_adjustment_delete_apply',
                'block_reason','WEEKLY_SOURCE_MANAGED_ROOT',
                'refusal_basis', case
                  -- HANDOVER 2 round-5 Part E: the trim-equivalent split family is a
                  -- canonical booking-reference collision and must be named as one.
                  -- WP-03 handoff N20: accept BOTH the installed token and the ruled name for one
                  -- release, so the order of this edit and WP-03's rename cannot open a gap in
                  -- which Office stops seeing the ruled name.
                  when v_weekly_source_guard->>'reason' in (
                         'FAMILY_SPLIT_BY_WHITESPACE','BOOKING_REFERENCE_CANONICAL_COLLISION')
                    then 'BOOKING_REFERENCE_CANONICAL_COLLISION'
                  when coalesce((v_weekly_source_guard->>'managed')::boolean, true) and coalesce((v_weekly_source_guard->>'ok')::boolean, true)
                    then 'WEEKLY_SOURCE_MANAGED_ROOT'
                  when coalesce((v_weekly_source_guard->>'managed')::boolean, true)
                    then 'WEEKLY_SOURCE_BOUND_OR_PROTECTED_ROOT_UNRESOLVABLE'
                  when coalesce((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
                    then 'AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET'
                  else 'PROTECTED_ROOT_AUTHORITY_MISSING' end,
                'integrity_failure', not coalesce((v_weekly_source_guard->>'ok')::boolean,false),
                'timesheet_id', v_weekly_source_root,
                'reason', v_weekly_source_guard->>'reason'
              )::text;
    END IF;
  END LOOP;

  INSERT INTO public.audit_events(actor_user_id, object_type, object_id_text, action, before_json, after_json, reason)
  VALUES (
    p_actor_user_id,
    'timesheets',
    v_current_timesheet_id::text,
    'WEEKLY_MANUAL_ADJUSTMENT_DELETE_APPLIED',
    jsonb_build_object(
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids),
      'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
      'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids)
    ),
    jsonb_build_object('deleted', true),
    'FINANCIALLY_CLEAN_MANUAL_ADJUSTMENT'
  );

  UPDATE public.nhsp_shifts AS ns SET timesheet_id = NULL, updated_at = now()
  WHERE ns.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.pay_item_snoozes AS s WHERE s.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheet_validations AS v WHERE v.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.hr_results AS h WHERE h.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.hr_issue_emails AS h WHERE h.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheet_evidence AS e WHERE e.timesheet_id = ANY(v_timesheet_ids);
  -- Relational-only queue deletion.  Caller-controlled/cached meta_json is not
  -- an ownership relationship and therefore cannot authorise deletion.
  DELETE FROM public.manual_timesheet_queue AS q
  WHERE q.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.ts_pdfs_outbox AS o WHERE o.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.ts_financials_outbox AS o WHERE o.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheet_summary_pay_state_cache AS c WHERE c.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheets_financials AS tf WHERE tf.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.contract_weeks AS cw
  WHERE cw.id = ANY(v_contract_week_ids)
    AND COALESCE(cw.is_adjustment, false)
    AND COALESCE(cw.additional_seq, 0) > 0;
  GET DIAGNOSTICS v_deleted_contract_weeks = ROW_COUNT;
  DELETE FROM public.timesheets AS t WHERE t.timesheet_id = ANY(v_timesheet_ids);
  GET DIAGNOSTICS v_deleted_timesheets = ROW_COUNT;

  IF v_deleted_timesheets <> array_length(v_timesheet_ids, 1)
     OR v_deleted_contract_weeks <> array_length(v_contract_week_ids, 1) THEN
    RAISE EXCEPTION USING MESSAGE = 'DELETE_COUNT_MISMATCH';
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'kind', 'WEEKLY_MANUAL_ADJUSTMENT_DELETE',
    'decision', 'PERMANENT_DELETE',
    'apply_performed', true,
    'deleted_timesheets', v_deleted_timesheets,
    'deleted_contract_weeks', v_deleted_contract_weeks,
    'deleted_timesheet_ids', to_jsonb(v_timesheet_ids),
    'deleted_contract_week_ids', to_jsonb(v_contract_week_ids),
    'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
    'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids),
    'r2_cleanup_keys', to_jsonb(v_r2_keys)
  );
EXCEPTION
  WHEN lock_not_available OR deadlock_detected THEN
    RETURN jsonb_build_object(
      'ok', false,
      'kind', 'WEEKLY_MANUAL_ADJUSTMENT_DELETE',
      'decision', 'BLOCKED',
      'apply_performed', false,
      'error_code', 'LOCK_TIMEOUT',
      'message', 'The manual-adjustment removal unit is currently being changed. Refresh and try again.'
    );
END;
$function$;

-- E23 private.timesheet_weekly_chain_delete_apply_base_v1
CREATE OR REPLACE FUNCTION private.timesheet_weekly_chain_delete_apply_base_v1(
  p_timesheet_id uuid,
  p_actor_user_id uuid,
  p_expected_timesheet_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_contract_week_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_nhsp_shift_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_row_signature text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_preview jsonb;
  v_recheck jsonb;
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_nhsp_shift_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_nhsp_shift_ids uuid[] := ARRAY[]::uuid[];
  v_expected_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_expected_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_expected_nhsp_shift_ids uuid[] := ARRAY[]::uuid[];
  -- Plan 6.2 G6-11 (proof/34 section 3, entry point E23).
  v_weekly_source_guard jsonb;
  v_weekly_source_root uuid;
  v_signature_payload jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_primary_contract_week_id uuid := NULL;
  v_r2_keys text[] := ARRAY[]::text[];
  v_current_timesheet_id uuid;
  v_contract_id uuid;
  v_week_ending_date date;
  v_deleted_timesheets integer := 0;
  v_deleted_contract_weeks integer := 0;
  v_deleted_shifts integer := 0;
  v_locked_timesheets integer := 0;
  v_locked_contract_weeks integer := 0;
  v_locked_nhsp_shifts integer := 0;
BEGIN
  PERFORM set_config('lock_timeout', '750ms', true);
  IF p_timesheet_id IS NULL THEN RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ID_REQUIRED'; END IF;
  IF p_actor_user_id IS NULL THEN RAISE EXCEPTION USING MESSAGE = 'ACTOR_USER_ID_REQUIRED'; END IF;

  IF p_expected_timesheet_ids IS NULL
     OR p_expected_contract_week_ids IS NULL
     OR p_expected_nhsp_shift_ids IS NULL
     OR NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'kind', 'WEEKLY_CHAIN_DELETE_PARENT',
      'decision', 'BLOCKED',
      'apply_performed', false,
      'error_code', 'EXPECTED_DELETE_PREVIEW_REQUIRED'
    );
  END IF;

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_timesheet_ids
  FROM unnest(p_expected_timesheet_ids) AS expected(id)
  WHERE id IS NOT NULL;
  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_contract_week_ids
  FROM unnest(p_expected_contract_week_ids) AS expected(id)
  WHERE id IS NOT NULL;
  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_expected_nhsp_shift_ids
  FROM unnest(p_expected_nhsp_shift_ids) AS expected(id)
  WHERE id IS NOT NULL;

  v_preview := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);
  IF COALESCE(v_preview ->> 'decision', 'BLOCKED') <> 'PERMANENT_DELETE' THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', CASE WHEN v_preview ->> 'decision' = 'ARCHIVE_REQUIRED' THEN 'ARCHIVE_REQUIRED' ELSE 'DELETE_BLOCKED' END
    );
  END IF;

  v_current_timesheet_id := NULLIF(v_preview ->> 'current_timesheet_id', '')::uuid;
  v_contract_id := NULLIF(v_preview ->> 'contract_id', '')::uuid;
  v_week_ending_date := NULLIF(v_preview ->> 'week_ending_date', '')::date;
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'timesheet_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'contract_week_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_nhsp_shift_ids
  FROM jsonb_array_elements_text(COALESCE(v_preview -> 'nhsp_shift_ids', '[]'::jsonb)) AS ids(value);

  IF v_timesheet_ids IS DISTINCT FROM v_expected_timesheet_ids
     OR v_contract_week_ids IS DISTINCT FROM v_expected_contract_week_ids
     OR v_nhsp_shift_ids IS DISTINCT FROM v_expected_nhsp_shift_ids THEN
    RETURN v_preview || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'DELETE_PREVIEW_STALE',
      'expected_target_set', jsonb_build_object(
        'timesheet_ids', to_jsonb(v_expected_timesheet_ids),
        'contract_week_ids', to_jsonb(v_expected_contract_week_ids),
        'nhsp_shift_ids', to_jsonb(v_expected_nhsp_shift_ids)
      )
    );
  END IF;

  IF COALESCE(array_length(v_timesheet_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION USING MESSAGE = 'EMPTY_REMOVAL_UNIT';
  END IF;
  IF COALESCE(array_length(v_timesheet_ids, 1), 0) > 32
     OR COALESCE(array_length(v_contract_week_ids, 1), 0) > 32
     OR COALESCE(array_length(v_nhsp_shift_ids, 1), 0) > 512 THEN
    RAISE EXCEPTION USING MESSAGE = 'REMOVAL_UNIT_TOO_LARGE';
  END IF;
  IF v_contract_id IS NULL OR v_week_ending_date IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'WEEKLY_TARGET_CONTEXT_MISSING';
  END IF;

  -- Lock the relational root first.  Child target identities are then locked in
  -- deterministic order and counted, so a missing or substituted target cannot
  -- be accepted merely because the preview JSON still looks plausible.
  PERFORM 1
  FROM public.contracts AS c
  WHERE c.id = v_contract_id
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION USING MESSAGE = 'CONTRACT_TARGET_CHANGED';
  END IF;

  SELECT COUNT(*)::integer
    INTO v_locked_timesheets
  FROM (
    SELECT t.timesheet_id
    FROM public.timesheets AS t
    WHERE t.timesheet_id = ANY(v_timesheet_ids)
    ORDER BY t.timesheet_id
    FOR UPDATE
  ) AS locked_timesheets;
  IF v_locked_timesheets <> COALESCE(array_length(v_timesheet_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_TARGET_SET_CHANGED';
  END IF;

  PERFORM 1
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY tf.timesheet_id, tf.id
  FOR UPDATE;

  SELECT COUNT(*)::integer
    INTO v_locked_contract_weeks
  FROM (
    SELECT cw.id
    FROM public.contract_weeks AS cw
    WHERE cw.id = ANY(v_contract_week_ids)
    ORDER BY cw.id
    FOR UPDATE
  ) AS locked_contract_weeks;
  IF v_locked_contract_weeks <> COALESCE(array_length(v_contract_week_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'CONTRACT_WEEK_TARGET_SET_CHANGED';
  END IF;

  SELECT COUNT(*)::integer
    INTO v_locked_nhsp_shifts
  FROM (
    SELECT ns.id
    FROM public.nhsp_shifts AS ns
    WHERE ns.id = ANY(v_nhsp_shift_ids)
    ORDER BY ns.id
    FOR UPDATE
  ) AS locked_nhsp_shifts;
  IF v_locked_nhsp_shifts <> COALESCE(array_length(v_nhsp_shift_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'NHSP_SHIFT_TARGET_SET_CHANGED';
  END IF;

  v_recheck := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_timesheet_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'timesheet_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_contract_week_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'contract_week_ids', '[]'::jsonb)) AS ids(value);
  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
    INTO v_recheck_nhsp_shift_ids
  FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'nhsp_shift_ids', '[]'::jsonb)) AS ids(value);

  IF COALESCE(v_recheck ->> 'decision', 'BLOCKED') <> 'PERMANENT_DELETE'
     OR NULLIF(v_recheck ->> 'current_timesheet_id', '')::uuid IS DISTINCT FROM v_current_timesheet_id
     OR NULLIF(v_recheck ->> 'contract_id', '')::uuid IS DISTINCT FROM v_contract_id
     OR NULLIF(v_recheck ->> 'week_ending_date', '')::date IS DISTINCT FROM v_week_ending_date
     OR v_recheck_timesheet_ids IS DISTINCT FROM v_timesheet_ids
     OR v_recheck_contract_week_ids IS DISTINCT FROM v_contract_week_ids
     OR v_recheck_nhsp_shift_ids IS DISTINCT FROM v_nhsp_shift_ids THEN
    RETURN v_recheck || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'DELETE_RECLASSIFIED',
      'locked_target_set', jsonb_build_object(
        'timesheet_ids', to_jsonb(v_timesheet_ids),
        'contract_week_ids', to_jsonb(v_contract_week_ids),
        'nhsp_shift_ids', to_jsonb(v_nhsp_shift_ids)
      )
    );
  END IF;

  SELECT cw.id
    INTO v_primary_contract_week_id
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current_timesheet_id
    AND cw.id = ANY(v_contract_week_ids)
  ORDER BY cw.id
  LIMIT 1;

  v_signature_payload := public.timesheet_lifecycle_guard_signature_v1(
    v_current_timesheet_id,
    v_primary_contract_week_id,
    false
  );
  v_current_row_signature := NULLIF(BTRIM(COALESCE(
    v_signature_payload ->> 'backend_row_signature',
    v_signature_payload ->> 'row_signature',
    v_signature_payload ->> 'signature',
    ''
  )), '');

  IF v_current_row_signature IS DISTINCT FROM BTRIM(p_expected_row_signature) THEN
    RETURN v_recheck || jsonb_build_object(
      'ok', false,
      'apply_performed', false,
      'error_code', 'ROW_SIGNATURE_MISMATCH',
      'expected_row_signature', BTRIM(p_expected_row_signature),
      'current_row_signature', v_current_row_signature
    );
  END IF;

  -- Freeze existing mutable manual-queue R2 keys before key collection.
  -- The exact queue rows are locked in deterministic order so an r2_key cannot
  -- change between server-side key collection and the relational delete.
  PERFORM 1
  FROM public.manual_timesheet_queue AS queue_row
  WHERE queue_row.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY queue_row.id
  FOR UPDATE;

  SELECT COALESCE(array_agg(DISTINCT storage_key ORDER BY storage_key), ARRAY[]::text[])
    INTO v_r2_keys
  FROM (
    SELECT NULLIF(BTRIM(key_value), '') AS storage_key
    FROM public.timesheets AS t
    CROSS JOIN LATERAL unnest(ARRAY[t.manual_pdf_r2_key, t.r2_nurse_key, t.r2_auth_key, t.qr_r2_key]) AS keys(key_value)
    WHERE t.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT NULLIF(BTRIM(e.storage_key), '') FROM public.timesheet_evidence e WHERE e.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT NULLIF(BTRIM(q.r2_key), '') FROM public.manual_timesheet_queue q WHERE q.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT NULLIF(BTRIM(key_value), '')
      FROM public.timesheets_financials tf
      CROSS JOIN LATERAL unnest(ARRAY[tf.expenses_evidence_r2_key, tf.mileage_evidence_r2_key]) AS keys(key_value)
      WHERE tf.timesheet_id = ANY(v_timesheet_ids)
    UNION SELECT manifest_key
      FROM public.timesheets_financials tf
      CROSS JOIN LATERAL unnest(
        public.cloudtms_jsonb_storage_keys_v1(tf.expenses_evidence_manifest, 8)
        || public.cloudtms_jsonb_storage_keys_v1(tf.mileage_evidence_manifest, 8)
      ) AS manifest(manifest_key)
      WHERE tf.timesheet_id = ANY(v_timesheet_ids)
  ) AS keys
  WHERE storage_key IS NOT NULL;

  -- Plan 6.2 G6-11 (proof/34 section 3, entry point E23): permanent deletion
  -- destroys the physical Timesheet identity a Weekly Source root is bound to,
  -- so an authorised Weekly-Source-managed root refuses here: before this
  -- owner's first write and while it holds every target row FOR UPDATE.  An
  -- unmanaged family is unaffected.
  FOREACH v_weekly_source_root IN ARRAY COALESCE(v_timesheet_ids, ARRAY[]::uuid[])
  LOOP
    v_weekly_source_guard := private.weekly_source_managed_root_guard_v1(v_weekly_source_root);
    -- HANDOVER 2 round-5 ruling B3 and A4 (18 September 2026).  The refusal is
    -- NARROWED: it applies to a Weekly-Source managed root, to a bound or
    -- protected family whose identity cannot be resolved, to a family carrying
    -- protected pay evidence but no authorisation row
    -- (PROTECTED_ROOT_AUTHORITY_MISSING), and to the live-record-on-an-
    -- unauthorised-Timesheet contradiction, which must never be allowed to
    -- continue merely because managed is false.  An unrelated, UNBOUND ordinary
    -- family -- including a malformed one -- keeps exactly the behaviour it had
    -- before this feature was installed.  Absent, null and non-boolean take the
    -- unsafe value at every read (Part 1 addendum rule 4).
    IF (COALESCE((v_weekly_source_guard->>'managed')::boolean, true)
         and (COALESCE((v_weekly_source_guard->>'ok')::boolean, true)
              or COALESCE((v_weekly_source_guard->>'weekly_source_bound')::boolean, true)))
       or COALESCE((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
       or (v_weekly_source_guard->>'protected_target_ownership_state') is not null THEN
      RAISE EXCEPTION 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
        USING ERRCODE = '55000',
              DETAIL = jsonb_build_object(
                'code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
                'entry_point','E23:private.timesheet_weekly_chain_delete_apply_base_v1',
                'block_reason','WEEKLY_SOURCE_MANAGED_ROOT',
                'refusal_basis', case
                  -- HANDOVER 2 round-5 Part E: the trim-equivalent split family is a
                  -- canonical booking-reference collision and must be named as one.
                  -- WP-03 handoff N20: accept BOTH the installed token and the ruled name for one
                  -- release, so the order of this edit and WP-03's rename cannot open a gap in
                  -- which Office stops seeing the ruled name.
                  when v_weekly_source_guard->>'reason' in (
                         'FAMILY_SPLIT_BY_WHITESPACE','BOOKING_REFERENCE_CANONICAL_COLLISION')
                    then 'BOOKING_REFERENCE_CANONICAL_COLLISION'
                  when coalesce((v_weekly_source_guard->>'managed')::boolean, true) and coalesce((v_weekly_source_guard->>'ok')::boolean, true)
                    then 'WEEKLY_SOURCE_MANAGED_ROOT'
                  when coalesce((v_weekly_source_guard->>'managed')::boolean, true)
                    then 'WEEKLY_SOURCE_BOUND_OR_PROTECTED_ROOT_UNRESOLVABLE'
                  when coalesce((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
                    then 'AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET'
                  else 'PROTECTED_ROOT_AUTHORITY_MISSING' end,
                'integrity_failure', not coalesce((v_weekly_source_guard->>'ok')::boolean,false),
                'timesheet_id', v_weekly_source_root,
                'reason', v_weekly_source_guard->>'reason'
              )::text;
    END IF;
  END LOOP;

  INSERT INTO public.audit_events(actor_user_id, object_type, object_id_text, action, before_json, after_json, reason)
  VALUES (
    p_actor_user_id,
    'timesheets',
    v_current_timesheet_id::text,
    'WEEKLY_CHAIN_DELETE_APPLIED',
    jsonb_build_object(
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids),
      'contract_id', v_contract_id,
      'week_ending_date', v_week_ending_date
    ),
    jsonb_build_object('deleted', true),
    'FINANCIALLY_CLEAN_WEEKLY_CHAIN'
  );

  IF COALESCE(array_length(v_nhsp_shift_ids, 1), 0) > 0 THEN
    DELETE FROM public.nhsp_shifts AS ns WHERE ns.id = ANY(v_nhsp_shift_ids);
    GET DIAGNOSTICS v_deleted_shifts = ROW_COUNT;
    IF v_deleted_shifts <> COALESCE(array_length(v_nhsp_shift_ids, 1), 0) THEN
      RAISE EXCEPTION USING MESSAGE = 'NHSP_SHIFT_DELETE_COUNT_MISMATCH';
    END IF;
  ELSE
    UPDATE public.nhsp_shifts AS ns SET timesheet_id = NULL, updated_at = now()
    WHERE ns.timesheet_id = ANY(v_timesheet_ids);
  END IF;

  DELETE FROM public.pay_item_snoozes AS s WHERE s.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheet_validations AS v WHERE v.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.hr_results AS h WHERE h.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.hr_issue_emails AS h WHERE h.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheet_evidence AS e WHERE e.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.manual_timesheet_queue AS q WHERE q.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.ts_pdfs_outbox AS o WHERE o.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.ts_financials_outbox AS o WHERE o.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheet_summary_pay_state_cache AS c WHERE c.timesheet_id = ANY(v_timesheet_ids);
  DELETE FROM public.timesheets_financials AS tf WHERE tf.timesheet_id = ANY(v_timesheet_ids);

  DELETE FROM public.contract_weeks AS cw WHERE cw.id = ANY(v_contract_week_ids);
  GET DIAGNOSTICS v_deleted_contract_weeks = ROW_COUNT;
  DELETE FROM public.timesheets AS t WHERE t.timesheet_id = ANY(v_timesheet_ids);
  GET DIAGNOSTICS v_deleted_timesheets = ROW_COUNT;

  IF v_deleted_timesheets <> array_length(v_timesheet_ids, 1)
     OR v_deleted_contract_weeks <> COALESCE(array_length(v_contract_week_ids, 1), 0) THEN
    RAISE EXCEPTION USING MESSAGE = 'DELETE_COUNT_MISMATCH';
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'kind', 'WEEKLY_CHAIN_DELETE_PARENT',
    'decision', 'PERMANENT_DELETE',
    'apply_performed', true,
    'deleted_timesheets', v_deleted_timesheets,
    'deleted_contract_weeks', v_deleted_contract_weeks,
    'deleted_nhsp_shifts', v_deleted_shifts,
    'deleted_timesheet_ids', to_jsonb(v_timesheet_ids),
    'deleted_contract_week_ids', to_jsonb(v_contract_week_ids),
    'r2_cleanup_keys', to_jsonb(v_r2_keys)
  );
EXCEPTION
  WHEN lock_not_available OR deadlock_detected THEN
    RETURN jsonb_build_object(
      'ok', false,
      'kind', 'WEEKLY_CHAIN_DELETE_PARENT',
      'decision', 'BLOCKED',
      'apply_performed', false,
      'error_code', 'LOCK_TIMEOUT',
      'message', 'The weekly removal unit is currently being changed. Refresh and try again.'
    );
END;
$function$;

-- E11 private._candidate_expense_payment_edit_shell_v1
create or replace function private._candidate_expense_payment_edit_shell_v1(
  p_timesheet_id uuid,
  p_now_utc timestamptz default now()
)
returns uuid
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_new public.timesheets_financials%rowtype;
  v_payment jsonb;
  -- Plan 6.2 G6-11 (proof/34 section 3, entry point E11).
  v_weekly_source_guard jsonb;
begin
  select row.* into v_timesheet from public.timesheets row
  where row.timesheet_id=p_timesheet_id and row.is_current
    and row.archived_at_utc is null for update;
  if not found or v_timesheet.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum then
    raise exception 'CANDIDATE_EXPENSE_OWNING_TIMESHEET_CHANGED' using errcode='40001';
  end if;
  select row.* into v_fin from public.timesheets_financials row
  where row.timesheet_id=p_timesheet_id and row.is_current
  order by row.computed_at_utc desc nulls last,row.updated_at desc,row.id desc
  limit 1 for update;
  if not found then raise exception 'CANDIDATE_EXPENSE_FINANCIALS_NOT_FOUND' using errcode='P0002'; end if;
  if v_timesheet.authorised_at_server is not null
     or upper(coalesce(v_timesheet.status::text,'')) in ('AUTHORISED','AUTHORIZED','INVOICED')
     or v_fin.authorised_at_utc is not null or v_fin.locked_by_invoice_id is not null then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_PROTECTED' using errcode='55000';
  end if;
  v_payment:=private._candidate_expense_effective_payment_v1(p_timesheet_id);
  if coalesce((v_payment->>'payment_protected')::boolean,false)
     and not coalesce((v_payment->>'payment_only_eligible')::boolean,false) then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_PROTECTED' using errcode='55000';
  end if;
  if coalesce((v_payment->>'edit_shell')::boolean,false)
     or not coalesce((v_payment->>'payment_protected')::boolean,false) then
    return v_fin.id;
  end if;

  -- Plan 6.2 G6-11 (proof/34 section 3, entry point E11): refuse for the
  -- authorised Weekly-Source-managed work-hours root, before this owner's
  -- first write and while it holds the timesheet and TSFIN row locks taken
  -- above.  A separate expense Timesheet that is not a managed root, and
  -- every unmanaged family, are unaffected.
  v_weekly_source_guard := private.weekly_source_managed_root_guard_v1(v_timesheet.timesheet_id);
  -- HANDOVER 2 round-5 ruling B3 and A4 (18 September 2026).  The refusal is
  -- NARROWED: it applies to a Weekly-Source managed root, to a bound or
  -- protected family whose identity cannot be resolved, to a family carrying
  -- protected pay evidence but no authorisation row
  -- (PROTECTED_ROOT_AUTHORITY_MISSING), and to the live-record-on-an-
  -- unauthorised-Timesheet contradiction, which must never be allowed to
  -- continue merely because managed is false.  An unrelated, UNBOUND ordinary
  -- family -- including a malformed one -- keeps exactly the behaviour it had
  -- before this feature was installed.  Absent, null and non-boolean take the
  -- unsafe value at every read (Part 1 addendum rule 4).
  IF (COALESCE((v_weekly_source_guard->>'managed')::boolean, true)
       and (COALESCE((v_weekly_source_guard->>'ok')::boolean, true)
            or COALESCE((v_weekly_source_guard->>'weekly_source_bound')::boolean, true)))
     or COALESCE((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
     or (v_weekly_source_guard->>'protected_target_ownership_state') is not null THEN
    RAISE EXCEPTION 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
      USING ERRCODE = '55000',
            DETAIL = jsonb_build_object(
              'code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
              'entry_point','E11:private._candidate_expense_payment_edit_shell_v1',
              'block_reason','WEEKLY_SOURCE_MANAGED_ROOT',
              'refusal_basis', case
                -- HANDOVER 2 round-5 Part E: the trim-equivalent split family is a
                -- canonical booking-reference collision and must be named as one.
                -- WP-03 handoff N20: accept BOTH the installed token and the ruled name for one
                -- release, so the order of this edit and WP-03's rename cannot open a gap in
                -- which Office stops seeing the ruled name.
                when v_weekly_source_guard->>'reason' in (
                       'FAMILY_SPLIT_BY_WHITESPACE','BOOKING_REFERENCE_CANONICAL_COLLISION')
                  then 'BOOKING_REFERENCE_CANONICAL_COLLISION'
                when coalesce((v_weekly_source_guard->>'managed')::boolean, true) and coalesce((v_weekly_source_guard->>'ok')::boolean, true)
                  then 'WEEKLY_SOURCE_MANAGED_ROOT'
                when coalesce((v_weekly_source_guard->>'managed')::boolean, true)
                  then 'WEEKLY_SOURCE_BOUND_OR_PROTECTED_ROOT_UNRESOLVABLE'
                when coalesce((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
                  then 'AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET'
                else 'PROTECTED_ROOT_AUTHORITY_MISSING' end,
              'integrity_failure', not coalesce((v_weekly_source_guard->>'ok')::boolean,false),
              'timesheet_id', v_timesheet.timesheet_id,
              'reason', v_weekly_source_guard->>'reason'
            )::text;
  END IF;

  -- Preserve the settled/part-settled row byte-for-byte apart from its current
  -- marker. Candidate Expense corrections occur only on the new unpaid shell;
  -- payment/remittance evidence and paid worked Hours remain historical truth.
  update public.timesheets_financials row set is_current=false
  where row.id=v_fin.id and row.is_current;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_FINANCIALS_CHANGED' using errcode='40001';
  end if;
  v_new:=v_fin;
  v_new.id:=gen_random_uuid();
  v_new.is_current:=true;
  v_new.is_stale:=true;
  v_new.stale_reason:='CANDIDATE_EXPENSE_PAYMENT_EDIT_SHELL';
  v_new.computed_at_utc:=p_now_utc;
  v_new.created_at:=p_now_utc;
  v_new.updated_at:=p_now_utc;
  v_new.processing_status:='PENDING_AUTH'::public.ts_fin_processing_status_enum;
  v_new.processed_by_user_id:=null;
  v_new.processed_at_utc:=null;
  v_new.authorised_at_utc:=null;
  v_new.authorised_by_user_id:=null;
  v_new.locked_by_invoice_id:=null;
  v_new.locked_at_utc:=null;
  v_new.unlocked_by_credit_note_id:=null;
  v_new.invoice_breakdown_json:='{}'::jsonb;
  v_new.paid_at_utc:=null;
  v_new.paid_by_user_id:=null;
  v_new.payment_reference:=null;
  v_new.remittance_last_sent_at_utc:=null;
  v_new.remittance_send_count:=0;
  v_new.pay_vat_amount_snapshot:=0;
  v_new.pay_total_inc_vat_snapshot:=0;
  v_new.policy_snapshot_json:=coalesce(v_fin.policy_snapshot_json,'{}'::jsonb)
    ||jsonb_build_object('candidate_expense_payment_edit_shell_v1',jsonb_build_object(
      'active',true,'source_tsfin_id',v_fin.id,'source_payment_status',v_payment->>'status_code',
      'created_at_utc',p_now_utc
    ));
  insert into public.timesheets_financials select v_new.*;
  return v_new.id;
end;
$function$;

-- Owner and ACL, set explicitly rather than relied upon.
--
-- CREATE OR REPLACE preserves an existing routine's owner and ACL, and every
-- routine above already exists with these exact privileges when this file runs.
-- Relying on that is unsafe: any signature drift at all -- a changed type, an
-- added argument, a changed default -- makes PostgreSQL create a NEW overload
-- that inherits the default EXECUTE to PUBLIC, and nothing here would revoke
-- it.  The statements below therefore restate the measured installed ACL of
-- each owner exactly.  They grant nothing new: public, anon and authenticated
-- are revoked everywhere, service_role keeps execute on exactly the three
-- owners that already have it, and private._candidate_expense_payment_edit_shell_v1
-- keeps none.

alter function public.timesheet_standard_delete_apply_v1(uuid,uuid,uuid,text)
  owner to postgres;
revoke all on function public.timesheet_standard_delete_apply_v1(uuid,uuid,uuid,text)
  from public,anon,authenticated;
grant execute on function public.timesheet_standard_delete_apply_v1(uuid,uuid,uuid,text)
  to service_role;

alter function public.timesheet_weekly_manual_adjustment_delete_apply(uuid,uuid,uuid[],uuid[],uuid[],uuid[],text)
  owner to postgres;
revoke all on function public.timesheet_weekly_manual_adjustment_delete_apply(uuid,uuid,uuid[],uuid[],uuid[],uuid[],text)
  from public,anon,authenticated;
grant execute on function public.timesheet_weekly_manual_adjustment_delete_apply(uuid,uuid,uuid[],uuid[],uuid[],uuid[],text)
  to service_role;

alter function private.timesheet_weekly_chain_delete_apply_base_v1(uuid,uuid,uuid[],uuid[],uuid[],text)
  owner to postgres;
revoke all on function private.timesheet_weekly_chain_delete_apply_base_v1(uuid,uuid,uuid[],uuid[],uuid[],text)
  from public,anon,authenticated;
grant execute on function private.timesheet_weekly_chain_delete_apply_base_v1(uuid,uuid,uuid[],uuid[],uuid[],text)
  to service_role;

-- No service grant: this owner is reached only from inside other definer-rights
-- Candidate expense owners, and has none installed today.
alter function private._candidate_expense_payment_edit_shell_v1(uuid,timestamptz)
  owner to postgres;
revoke all on function private._candidate_expense_payment_edit_shell_v1(uuid,timestamptz)
  from public,anon,authenticated,service_role;


-- E23w public.timesheet_weekly_chain_delete_apply -- the THIN public wrapper.
--
-- WP-09b executed evidence (18 September 2026), answering the installed-writer
-- census finding that a re-run of the historical monolith "cannot disturb it":
-- it does disturb it.  On a clone of a fresh NEW build the installed wrapper was
-- 7,750 characters, delegated to
-- private.timesheet_weekly_chain_delete_apply_base_v1 and carried this guard;
-- after re-applying supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql ALONE
-- it was 14,003 characters, the full-bodied deleter, with no guard and no
-- delegation.  A release is safe because the runner applies
-- 29082026_1914 (-> 20260829) and then this file (-> 20260917) after the
-- monolith (-> 20260526), but an out-of-order re-run of the monolith alone
-- removes the guard at this entry point.
--
-- The supersession is therefore made EXPLICIT here, in the newest file of the
-- release, exactly as it already is for E21, E22, E23b and E11: this body is
-- the byte-identical wrapper of
-- 29082026_1914_contract_week_delete_boundary_reconciliation.sql, so re-applying
-- the final Weekly Source repeatable restores the guarded wrapper.

CREATE OR REPLACE FUNCTION public.timesheet_weekly_chain_delete_apply(
  p_timesheet_id uuid,
  p_actor_user_id uuid,
  p_expected_timesheet_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_contract_week_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_nhsp_shift_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_row_signature text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_preview jsonb;
  v_result jsonb;
  -- Plan 6.2 G6-11 (proof/34 section 3, entry point E23).
  v_weekly_source_guard jsonb;
  v_weekly_source_root uuid;
  v_contract_id uuid;
  v_contract_start date;
  v_contract_end date;
  v_week_ending_weekday integer;
  v_first_remaining_week date;
  v_last_remaining_week date;
  v_start_week date;
  v_end_week date;
  v_first_remaining_planned_date date;
  v_last_remaining_planned_date date;
  v_new_start date;
  v_new_end date;
BEGIN
  v_preview := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);
  v_contract_id := NULLIF(v_preview ->> 'contract_id', '')::uuid;

  -- Plan 6.2 G6-11 (proof/34 section 3, entry point E23).  The delete base
  -- owner carries the same refusal under its own locks; this call covers the
  -- upgrade path, where the base body is not recreated because the rename block
  -- above is already satisfied.  Checked before any write of this chain.  An
  -- unmanaged family is unaffected.
  FOR v_weekly_source_root IN
    SELECT DISTINCT target.timesheet_id
    FROM (
      SELECT p_timesheet_id AS timesheet_id
      UNION
      SELECT unnest(COALESCE(p_expected_timesheet_ids, ARRAY[]::uuid[]))
      UNION
      SELECT NULLIF(element.value, '')::uuid
      FROM jsonb_array_elements_text(
        CASE WHEN jsonb_typeof(v_preview -> 'timesheet_ids') = 'array'
          THEN v_preview -> 'timesheet_ids' ELSE '[]'::jsonb END
      ) AS element(value)
    ) AS target(timesheet_id)
    WHERE target.timesheet_id IS NOT NULL
    ORDER BY 1
  LOOP
    v_weekly_source_guard := private.weekly_source_managed_root_guard_v1(v_weekly_source_root);
    -- HANDOVER 2 round-5 ruling B3 and A4 (18 September 2026).  The refusal is
    -- NARROWED: it applies to a Weekly-Source managed root, to a bound or
    -- protected family whose identity cannot be resolved, to a family carrying
    -- protected pay evidence but no authorisation row
    -- (PROTECTED_ROOT_AUTHORITY_MISSING), and to the live-record-on-an-
    -- unauthorised-Timesheet contradiction, which must never be allowed to
    -- continue merely because managed is false.  An unrelated, UNBOUND ordinary
    -- family -- including a malformed one -- keeps exactly the behaviour it had
    -- before this feature was installed.  Absent, null and non-boolean take the
    -- unsafe value at every read (Part 1 addendum rule 4).
    IF (COALESCE((v_weekly_source_guard->>'managed')::boolean, true)
         and (COALESCE((v_weekly_source_guard->>'ok')::boolean, true)
              or COALESCE((v_weekly_source_guard->>'weekly_source_bound')::boolean, true)))
       or COALESCE((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
       or (v_weekly_source_guard->>'protected_target_ownership_state') is not null THEN
      RAISE EXCEPTION 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
        USING ERRCODE = '55000',
              DETAIL = jsonb_build_object(
                'code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
                'entry_point','E23:public.timesheet_weekly_chain_delete_apply',
                'block_reason','WEEKLY_SOURCE_MANAGED_ROOT',
                'refusal_basis', case
                  -- HANDOVER 2 round-5 Part E: the trim-equivalent split family is a
                  -- canonical booking-reference collision and must be named as one.
                  -- WP-03 handoff N20: accept BOTH the installed token and the ruled name for one
                  -- release, so the order of this edit and WP-03's rename cannot open a gap in
                  -- which Office stops seeing the ruled name.
                  when v_weekly_source_guard->>'reason' in (
                         'FAMILY_SPLIT_BY_WHITESPACE','BOOKING_REFERENCE_CANONICAL_COLLISION')
                    then 'BOOKING_REFERENCE_CANONICAL_COLLISION'
                  when coalesce((v_weekly_source_guard->>'managed')::boolean, true) and coalesce((v_weekly_source_guard->>'ok')::boolean, true)
                    then 'WEEKLY_SOURCE_MANAGED_ROOT'
                  when coalesce((v_weekly_source_guard->>'managed')::boolean, true)
                    then 'WEEKLY_SOURCE_BOUND_OR_PROTECTED_ROOT_UNRESOLVABLE'
                  when coalesce((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
                    then 'AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET'
                  else 'PROTECTED_ROOT_AUTHORITY_MISSING' end,
                'integrity_failure', not coalesce((v_weekly_source_guard->>'ok')::boolean,false),
                'timesheet_id', v_weekly_source_root,
                'reason', v_weekly_source_guard->>'reason'
              )::text;
    END IF;
  END LOOP;

  v_result := private.timesheet_weekly_chain_delete_apply_base_v1(
    p_timesheet_id,
    p_actor_user_id,
    p_expected_timesheet_ids,
    p_expected_contract_week_ids,
    p_expected_nhsp_shift_ids,
    p_expected_row_signature
  );

  IF COALESCE((v_result ->> 'ok')::boolean, false) IS NOT TRUE
     OR COALESCE((v_result ->> 'apply_performed')::boolean, false) IS NOT TRUE THEN
    RETURN v_result;
  END IF;

  IF v_contract_id IS NULL THEN
    RETURN v_result;
  END IF;

  SELECT c.start_date, c.end_date, COALESCE(c.week_ending_weekday_snapshot, 0)
  INTO v_contract_start, v_contract_end, v_week_ending_weekday
  FROM public.contracts c
  WHERE c.id = v_contract_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN v_result;
  END IF;

  SELECT min(cw.week_ending_date), max(cw.week_ending_date)
  INTO v_first_remaining_week, v_last_remaining_week
  FROM public.contract_weeks cw
  WHERE cw.contract_id = v_contract_id;

  v_new_start := v_contract_start;
  v_new_end := v_contract_end;

  IF v_first_remaining_week IS NOT NULL THEN
    v_start_week := v_contract_start
      + mod(v_week_ending_weekday - extract(dow from v_contract_start)::integer + 7, 7);
    v_end_week := v_contract_end
      + mod(v_week_ending_weekday - extract(dow from v_contract_end)::integer + 7, 7);

    IF v_first_remaining_week > v_start_week THEN
      SELECT min((entry.item ->> 'date')::date)
      INTO v_first_remaining_planned_date
      FROM public.contract_weeks remaining
      CROSS JOIN LATERAL jsonb_array_elements(COALESCE(remaining.planned_schedule_json, '[]'::jsonb)) entry(item)
      WHERE remaining.contract_id = v_contract_id
        AND remaining.week_ending_date = v_first_remaining_week
        AND jsonb_typeof(entry.item) = 'object'
        AND COALESCE(entry.item ->> 'date', '') ~ '^\d{4}-\d{2}-\d{2}$';

      v_new_start := COALESCE(v_first_remaining_planned_date, v_first_remaining_week - 6);
    END IF;

    IF v_last_remaining_week < v_end_week THEN
      SELECT max((entry.item ->> 'date')::date)
      INTO v_last_remaining_planned_date
      FROM public.contract_weeks remaining
      CROSS JOIN LATERAL jsonb_array_elements(COALESCE(remaining.planned_schedule_json, '[]'::jsonb)) entry(item)
      WHERE remaining.contract_id = v_contract_id
        AND remaining.week_ending_date = v_last_remaining_week
        AND jsonb_typeof(entry.item) = 'object'
        AND COALESCE(entry.item ->> 'date', '') ~ '^\d{4}-\d{2}-\d{2}$';

      v_new_end := COALESCE(v_last_remaining_planned_date, v_last_remaining_week);
    END IF;

    IF v_new_start IS DISTINCT FROM v_contract_start
       OR v_new_end IS DISTINCT FROM v_contract_end THEN
      UPDATE public.contracts
      SET start_date = v_new_start,
          end_date = v_new_end
      WHERE id = v_contract_id;

      INSERT INTO public.audit_events(
        actor_user_id,
        object_type,
        object_id_text,
        action,
        before_json,
        after_json,
        reason
      )
      VALUES (
        p_actor_user_id,
        'contract',
        v_contract_id::text,
        'CONTRACT_DATES_RECONCILED_AFTER_WEEK_DELETE',
        jsonb_build_object('start_date', v_contract_start, 'end_date', v_contract_end),
        jsonb_build_object('start_date', v_new_start, 'end_date', v_new_end),
        'WEEKLY_TIMESHEET_CHAIN_DELETED'
      );
    END IF;
  END IF;

  RETURN v_result || jsonb_build_object(
    'contract_dates', jsonb_build_object(
      'start_date', v_new_start,
      'end_date', v_new_end,
      'changed', v_new_start IS DISTINCT FROM v_contract_start
        OR v_new_end IS DISTINCT FROM v_contract_end
    )
  );
END;
$function$;

alter function public.timesheet_weekly_chain_delete_apply(uuid,uuid,uuid[],uuid[],uuid[],text)
  owner to postgres;
revoke all on function public.timesheet_weekly_chain_delete_apply(uuid,uuid,uuid[],uuid[],uuid[],text)
  from public,anon,authenticated;
grant execute on function public.timesheet_weekly_chain_delete_apply(uuid,uuid,uuid[],uuid[],uuid[],text)
  to service_role;

commit;
