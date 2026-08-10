-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline, extended only to initialise typed source-build jobs.

CREATE OR REPLACE FUNCTION public.pay_workbench_session_clone_eligible_rows_v1(p_target_session_id uuid, p_source_session_id uuid DEFAULT NULL::uuid, p_limit integer DEFAULT 100, p_cursor_json jsonb DEFAULT '{}'::jsonb, p_options_json jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_cursor_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_cursor_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_cursor_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_options_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_options_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_options_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_target_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_source_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_source_session_id uuid := p_source_session_id;
  v_source_session_id_text text := NULL::text;
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 250);
  v_after_scope_ordinal bigint := NULL::bigint;
  v_after_candidate_id uuid := NULL::uuid;
  v_after_candidate_id_text text := NULL::text;
  v_processed_candidate_count integer := 0;
  v_copied_candidate_count integer := 0;
  v_copied_source_row_count integer := 0;
  v_copied_line_work_count integer := 0;
  v_copied_preview_row_count integer := 0;
  v_legacy_refresh_enqueued_count integer := 0;
  v_ready_empty_candidate_count integer := 0;
  v_last_scope_ordinal bigint := NULL::bigint;
  v_last_candidate_id uuid := NULL::uuid;
  v_more_due boolean := false;
  v_next_cursor_json jsonb := NULL::jsonb;
  v_clone_eligibility jsonb := '{}'::jsonb;
  v_bounded_clone_eligibility jsonb := '{}'::jsonb;
  v_bounded_build_clone boolean := false;
  v_clone_projection_run_id uuid := NULL::uuid;
  v_candidate_state_result jsonb := '{}'::jsonb;
  v_candidate_id uuid := NULL::uuid;
  v_scope_ordinal bigint := NULL::bigint;
  v_job_id uuid := NULL::uuid;
  v_ineligible_source_change_seq bigint := 0;
  v_ineligible_source_build_seed_text text := NULL::text;
  v_ineligible_source_build_hash text := NULL::text;
  v_ineligible_source_build_run_id uuid := NULL::uuid;
  v_ineligible_source_build_base_dedupe_key text := NULL::text;
  v_ineligible_source_build_dedupe_key text := NULL::text;
  v_ineligible_source_build_payload_json jsonb := '{}'::jsonb;
  v_ineligible_source_build_job_status text := NULL::text;
  v_ineligible_source_build_job_payload_json jsonb := '{}'::jsonb;
  v_existing_source_build_job_id uuid := NULL::uuid;
  v_existing_source_build_job_status text := NULL::text;
  v_existing_source_build_job_payload_json jsonb := '{}'::jsonb;
  v_existing_source_build_run_id_text text := NULL::text;
  v_running_fail_close_result jsonb := '{}'::jsonb;
  v_ineligible_source_build_corrected_dedupe_key text := NULL::text;
  v_running_conflict_replacement_enqueued boolean := false;
  v_running_conflict_job_id uuid := NULL::uuid;
  v_running_conflict_status text := NULL::text;
  v_running_conflict_run_id_text text := NULL::text;
  v_active_conflict_replacement_enqueued boolean := false;
  v_active_conflict_job_id uuid := NULL::uuid;
  v_active_conflict_status text := NULL::text;
  v_active_conflict_run_id_text text := NULL::text;
  v_ineligible_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_fallback_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_fallback_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_fallback_targeted_timesheet_count integer := 0;
  v_current_authority_timesheet_ids_json jsonb := '[]'::jsonb;
  v_current_authority_timesheet_count integer := 0;
  v_current_authority_scope_digest text := md5('[]');
  v_ineligible_final_pay_channel_scope text := 'ALL';
  v_existing_source_build_contract_valid boolean := false;
  v_target_invalidated_preview_count integer := 0;
  v_target_invalidated_source_count integer := 0;
  v_lock_candidate_id uuid := NULL::uuid;
  v_direct_candidate_id uuid := NULL::uuid;
  v_direct_candidate_id_text text := NULL::text;
  v_source_selection jsonb := '{}'::jsonb;
  v_clone_owner_job_id uuid := NULL::uuid;
  v_clone_fence jsonb := '{}'::jsonb;
  v_certified_publication jsonb := '{}'::jsonb;
  v_current_source_change_seq bigint := 0;
  v_semantic_publication_v3_enabled boolean := false;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  SELECT COALESCE(settings_row.banking_pay_workbench_semantic_ready_publication_v3_enabled,false)
  INTO v_semantic_publication_v3_enabled
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id=1;

  IF p_target_session_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'clone_rebase_applied', false,
      'fallback_required', true,
      'fallback_reason', 'TARGET_SESSION_ID_REQUIRED',
      'copied_candidate_count', 0,
      'copied_preview_row_count', 0,
      'legacy_refresh_enqueued_count', 0,
      'more_due', false,
      'next_cursor_json', NULL::jsonb
    );
  END IF;

  v_direct_candidate_id_text := NULLIF(BTRIM(COALESCE(
    v_options_json->>'direct_candidate_id',
    v_options_json->>'candidate_id',
    ''
  )), '');
  IF v_direct_candidate_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_direct_candidate_id := v_direct_candidate_id_text::uuid;
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_options_json->>'source_job_id',v_options_json->>'clone_job_id','')),'')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_clone_owner_job_id := NULLIF(BTRIM(COALESCE(v_options_json->>'source_job_id',v_options_json->>'clone_job_id','')),'')::uuid;
  END IF;

  IF v_source_session_id IS NULL AND v_direct_candidate_id IS NOT NULL THEN
    SELECT coalesce(change_counter.seq,0)::bigint
    INTO v_current_source_change_seq
    FROM (SELECT 1) AS authority_anchor
    LEFT JOIN public.app_change_counters AS change_counter
      ON change_counter.entity_key='pay_candidate:'||v_direct_candidate_id::text;

    v_source_selection := private.pay_workbench_candidate_reuse_source_select_v1(
      p_target_session_id,
      v_direct_candidate_id,
      v_current_source_change_seq,
      v_options_json||jsonb_build_object('source_job_id',CASE WHEN v_clone_owner_job_id IS NULL THEN NULL ELSE v_clone_owner_job_id::text END)
    );
    IF coalesce((v_source_selection->>'reuse_available')::boolean,false) IS TRUE
       AND coalesce(v_source_selection->>'selected_source_session_id','')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_source_session_id := (v_source_selection->>'selected_source_session_id')::uuid;
      v_options_json := v_options_json||jsonb_build_object(
        'source_session_id',v_source_session_id::text,
        'target_session_id',p_target_session_id::text,
        'source_selection_authorised',true,
        'allow_session_rebase',true,
        'rebase_simple_rows_only',true
      );
    END IF;
  END IF;
  IF v_source_session_id IS NULL THEN
    v_source_session_id_text := NULLIF(BTRIM(COALESCE(v_options_json->>'source_session_id', v_options_json->>'replacement_source_session_id', '')), '');
    IF v_source_session_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_source_session_id := v_source_session_id_text::uuid;
    END IF;
  END IF;

  IF v_source_session_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'clone_rebase_applied', false,
      'fallback_required', true,
      'fallback_reason', 'SOURCE_SESSION_ID_REQUIRED',
      'copied_candidate_count', 0,
      'copied_preview_row_count', 0,
      'legacy_refresh_enqueued_count', 0,
      'more_due', false,
      'next_cursor_json', NULL::jsonb
    );
  END IF;

  SELECT target_session.*
  INTO v_target_session
  FROM public.banking_pay_workbench_sessions AS target_session
  WHERE target_session.id = p_target_session_id
    AND UPPER(BTRIM(COALESCE(target_session.status, ''))) = 'OPEN'
    AND target_session.discarded_at_utc IS NULL;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', false,
      'clone_rebase_applied', false,
      'fallback_required', true,
      'fallback_reason', 'TARGET_SESSION_NOT_OPEN',
      'copied_candidate_count', 0,
      'copied_preview_row_count', 0,
      'legacy_refresh_enqueued_count', 0,
      'more_due', false,
      'next_cursor_json', NULL::jsonb
    );
  END IF;

  SELECT source_session.*
  INTO v_source_session
  FROM public.banking_pay_workbench_sessions AS source_session
  WHERE source_session.id = v_source_session_id
    AND (
      (
        UPPER(BTRIM(COALESCE(source_session.status, ''))) = 'OPEN'
        AND source_session.discarded_at_utc IS NULL
      )
      OR (
        UPPER(BTRIM(COALESCE(source_session.status, ''))) IN ('DISCARDED', 'REPLACED')
        AND source_session.discarded_at_utc IS NOT NULL
      )
    );

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', false,
      'clone_rebase_applied', false,
      'fallback_required', true,
      'fallback_reason', 'SOURCE_SESSION_NOT_OPEN',
      'copied_candidate_count', 0,
      'copied_preview_row_count', 0,
      'legacy_refresh_enqueued_count', 0,
      'more_due', false,
      'next_cursor_json', NULL::jsonb
    );
  END IF;

  IF COALESCE(v_cursor_json->>'after_scope_ordinal', v_cursor_json->>'last_scope_ordinal', '') ~ '^[0-9]{1,18}$' THEN
    v_after_scope_ordinal := COALESCE(v_cursor_json->>'after_scope_ordinal', v_cursor_json->>'last_scope_ordinal')::bigint;
  END IF;

  v_after_candidate_id_text := NULLIF(BTRIM(COALESCE(v_cursor_json->>'after_candidate_id', v_cursor_json->>'last_candidate_id', '')), '');
  IF v_after_candidate_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_after_candidate_id := v_after_candidate_id_text::uuid;
  END IF;

  DROP TABLE IF EXISTS pg_temp._bpay_clone_candidate_page;
  CREATE TEMP TABLE _bpay_clone_candidate_page ON COMMIT DROP AS
  SELECT paged_source_scope.candidate_id,
         paged_source_scope.scope_ordinal,
         ROW_NUMBER() OVER (ORDER BY paged_source_scope.scope_ordinal, paged_source_scope.candidate_id) AS page_index
  FROM public.banking_pay_workbench_session_scope AS paged_source_scope
  WHERE paged_source_scope.session_id = v_source_session_id
    AND (v_direct_candidate_id IS NULL OR paged_source_scope.candidate_id=v_direct_candidate_id)
    AND paged_source_scope.candidate_id IS NOT NULL
    AND (
      v_after_scope_ordinal IS NULL
      OR paged_source_scope.scope_ordinal > v_after_scope_ordinal
      OR (
        paged_source_scope.scope_ordinal = v_after_scope_ordinal
        AND v_after_candidate_id IS NOT NULL
        AND paged_source_scope.candidate_id > v_after_candidate_id
      )
    )
  ORDER BY paged_source_scope.scope_ordinal, paged_source_scope.candidate_id
  LIMIT (v_limit + 1);

  -- Freeze candidate ownership before locking either source or target
  -- session. Every concurrent clone page takes these locks in the same UUID
  -- order, then locks the complete session union in UUID order.
  FOR v_lock_candidate_id IN
    SELECT DISTINCT page_row.candidate_id
    FROM pg_temp._bpay_clone_candidate_page AS page_row
    WHERE page_row.page_index<=v_limit
    ORDER BY page_row.candidate_id
  LOOP
    PERFORM pg_catalog.pg_advisory_xact_lock(
      pg_catalog.hashtextextended(
        public._pay_workbench_candidate_serial_key(v_lock_candidate_id),
        24062027
      )
    );
  END LOOP;

  PERFORM candidate_lock.id
  FROM public.candidates AS candidate_lock
  JOIN pg_temp._bpay_clone_candidate_page AS page_row
    ON page_row.candidate_id=candidate_lock.id
   AND page_row.page_index<=v_limit
  ORDER BY candidate_lock.id
  FOR UPDATE;

  PERFORM registry_lock.candidate_id
  FROM private.banking_pay_workbench_candidate_scope_registry AS registry_lock
  JOIN pg_temp._bpay_clone_candidate_page AS page_row
    ON page_row.candidate_id=registry_lock.candidate_id
   AND page_row.page_index<=v_limit
  ORDER BY registry_lock.candidate_id
  FOR UPDATE;

  PERFORM session_lock.id
  FROM public.banking_pay_workbench_sessions AS session_lock
  WHERE session_lock.id=ANY(ARRAY[p_target_session_id,v_source_session_id]::uuid[])
  ORDER BY session_lock.id
  FOR UPDATE;

  SELECT target_session.*
  INTO v_target_session
  FROM public.banking_pay_workbench_sessions AS target_session
  WHERE target_session.id=p_target_session_id
    AND UPPER(BTRIM(COALESCE(target_session.status,'')))='OPEN'
    AND target_session.discarded_at_utc IS NULL;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CLONE_TARGET_CHANGED_BEFORE_LOCK'
      USING ERRCODE='P0001';
  END IF;

  SELECT source_session.*
  INTO v_source_session
  FROM public.banking_pay_workbench_sessions AS source_session
  WHERE source_session.id=v_source_session_id
    AND (
      (UPPER(BTRIM(COALESCE(source_session.status,'')))='OPEN' AND source_session.discarded_at_utc IS NULL)
      OR (UPPER(BTRIM(COALESCE(source_session.status,''))) IN ('DISCARDED','REPLACED')
          AND source_session.discarded_at_utc IS NOT NULL)
    );

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CLONE_SOURCE_CHANGED_BEFORE_LOCK'
      USING ERRCODE='P0001';
  END IF;

  PERFORM scope_lock.id
  FROM public.banking_pay_workbench_session_scope AS scope_lock
  JOIN pg_temp._bpay_clone_candidate_page AS page_row
    ON page_row.candidate_id=scope_lock.candidate_id
   AND page_row.page_index<=v_limit
  WHERE scope_lock.session_id=ANY(ARRAY[p_target_session_id,v_source_session_id]::uuid[])
  ORDER BY scope_lock.session_id,scope_lock.candidate_id
  FOR UPDATE;

  SELECT COUNT(*)::integer
  INTO v_processed_candidate_count
  FROM pg_temp._bpay_clone_candidate_page AS page_row
  WHERE page_row.page_index <= v_limit;

  SELECT EXISTS (
    SELECT 1
    FROM pg_temp._bpay_clone_candidate_page AS page_row
    WHERE page_row.page_index > v_limit
  )
  INTO v_more_due;

  SELECT page_row.scope_ordinal,
         page_row.candidate_id
  INTO v_last_scope_ordinal,
       v_last_candidate_id
  FROM pg_temp._bpay_clone_candidate_page AS page_row
  WHERE page_row.page_index <= v_limit
  ORDER BY page_row.page_index DESC
  LIMIT 1;

  IF v_more_due IS TRUE THEN
    v_next_cursor_json := jsonb_build_object(
      'after_scope_ordinal', v_last_scope_ordinal,
      'after_candidate_id', CASE WHEN v_last_candidate_id IS NULL THEN NULL ELSE v_last_candidate_id::text END,
      'source_session_id', v_source_session_id::text,
      'target_session_id', p_target_session_id::text
    );
  END IF;

  FOR v_candidate_id, v_scope_ordinal IN
    SELECT page_row.candidate_id, page_row.scope_ordinal
    FROM pg_temp._bpay_clone_candidate_page AS page_row
    WHERE page_row.page_index <= v_limit
    ORDER BY page_row.scope_ordinal, page_row.candidate_id
  LOOP
    v_bounded_clone_eligibility := '{}'::jsonb;
    v_bounded_build_clone := false;
    v_clone_projection_run_id := p_target_session_id;

    INSERT INTO public.banking_pay_workbench_session_scope (
      session_id,
      candidate_id,
      scope_ordinal,
      status,
      pending_job_id,
      seeded,
      dirty,
      error_json,
      created_at_utc,
      updated_at_utc
    )
    VALUES (
      p_target_session_id,
      v_candidate_id,
      v_scope_ordinal,
      'PENDING',
      NULL::uuid,
      true,
      false,
      NULL::jsonb,
      v_now,
      v_now
    )
    ON CONFLICT (session_id, candidate_id)
    DO UPDATE
    SET scope_ordinal = LEAST(public.banking_pay_workbench_session_scope.scope_ordinal, EXCLUDED.scope_ordinal),
        status = CASE
          WHEN UPPER(BTRIM(COALESCE(public.banking_pay_workbench_session_scope.status, ''))) IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY') THEN public.banking_pay_workbench_session_scope.status
          ELSE 'PENDING'
        END,
        seeded = true,
        dirty = false,
        error_json = NULL::jsonb,
        updated_at_utc = v_now;

    v_clone_eligibility := public.pay_workbench_session_clone_eligibility_v1(
      v_source_session_id,
      p_target_session_id,
      v_candidate_id,
      v_options_json
    );

    /*
     * The legacy clone proof deliberately recognises only the original
     * one-source-row/one-preview-row projection.  Bounded-source publication
     * has a richer canonical shape, so give it one independent fail-closed
     * proof before selecting the existing full-build fallback.
     */
    IF COALESCE((v_clone_eligibility->>'clone_eligible')::boolean, false) IS NOT TRUE THEN
      v_bounded_clone_eligibility := private.pay_workbench_session_clone_bounded_certification_v1(
        v_source_session_id,
        p_target_session_id,
        v_candidate_id,
        jsonb_strip_nulls(v_options_json||jsonb_build_object(
          'source_session_id',v_source_session_id::text,
          'target_session_id',p_target_session_id::text,
          'source_selection_authorised',true,
          'allow_session_rebase',true,
          'rebase_simple_rows_only',true,
          'clone_job_id',CASE WHEN v_clone_owner_job_id IS NULL THEN NULL ELSE v_clone_owner_job_id::text END
        ))
      );

      IF COALESCE((v_bounded_clone_eligibility->>'clone_eligible')::boolean, false) IS TRUE THEN
        v_clone_eligibility := v_bounded_clone_eligibility;
        v_bounded_build_clone := true;

        IF COALESCE(v_clone_eligibility->>'source_build_run_id', '')
             ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
          v_clone_projection_run_id := (v_clone_eligibility->>'source_build_run_id')::uuid;
        ELSE
          v_clone_eligibility := jsonb_build_object(
            'ok', true,
            'clone_eligible', false,
            'bounded_build_certified', false,
            'reason', 'BOUNDED_BUILD_PROJECTION_ID_INVALID',
            'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
          );
          v_bounded_build_clone := false;
          v_clone_projection_run_id := p_target_session_id;
        END IF;
      END IF;
    END IF;

    IF v_bounded_build_clone IS TRUE THEN
      v_clone_eligibility := v_clone_eligibility||jsonb_build_object(
        'clone_job_id',CASE WHEN v_clone_owner_job_id IS NULL THEN NULL ELSE v_clone_owner_job_id::text END
      );
      v_clone_fence := private.pay_workbench_session_clone_publication_fence_v1(
        v_source_session_id,p_target_session_id,v_candidate_id,v_clone_eligibility
      );
      IF coalesce((v_clone_fence->>'fence_passed')::boolean,false) IS NOT TRUE THEN
        v_clone_eligibility := jsonb_build_object(
          'ok',true,'clone_eligible',false,'bounded_build_certified',false,
          'reason',coalesce(NULLIF(v_clone_fence->>'reason',''),'CLONE_CERTIFICATION_DRIFT'),
          'required_refresh_job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD'
        );
        v_bounded_build_clone := false;
      END IF;
    END IF;

    IF COALESCE((v_clone_eligibility->>'clone_eligible')::boolean, false) IS TRUE THEN
      IF COALESCE((v_clone_eligibility->>'ready_empty')::boolean, false) IS TRUE THEN
        UPDATE public.banking_pay_workbench_session_scope AS target_scope_update
        SET status = 'SOURCE_EMPTY',
            seeded = true,
            dirty = false,
            pending_job_id = NULL,
            error_json = NULL::jsonb,
            updated_at_utc = v_now
        WHERE target_scope_update.session_id = p_target_session_id
          AND target_scope_update.candidate_id = v_candidate_id;

        v_ready_empty_candidate_count := v_ready_empty_candidate_count + 1;
      ELSE
        WITH copied_source_rows AS (
          INSERT INTO public.banking_pay_workbench_candidate_source_lines (
            session_id,
            candidate_id,
            session_version,
            source_change_seq,
            source_build_run_id,
            source_publication_id,
            source_ordinal,
            line_key,
            parent_line_key,
            split_suffix,
            timesheet_id,
            section,
            source_row_json,
            economic_key_json,
            contract_json,
            pay_channel_scope,
            refresh_scope_kind,
            status,
            created_at_utc,
            updated_at_utc
          )
          SELECT
            p_target_session_id,
            source_line.candidate_id,
            v_target_session.version,
            COALESCE(source_line.source_change_seq, 0),
            CASE
              WHEN v_bounded_build_clone IS TRUE THEN source_line.source_build_run_id
              ELSE p_target_session_id
            END,
            CASE WHEN COALESCE((SELECT setting.banking_pay_source_publication_identity_write_v1_enabled
                                 FROM public.settings_defaults AS setting WHERE setting.id=1),false)
              THEN private.pay_workbench_source_publication_identity_v1(
                p_target_session_id,source_line.candidate_id,v_target_session.version,
                COALESCE(source_line.source_change_seq,0),
                CASE WHEN v_bounded_build_clone IS TRUE
                  THEN source_line.source_build_run_id ELSE p_target_session_id END
              ) ELSE NULL::uuid END,
            source_line.source_ordinal,
            source_line.line_key,
            source_line.parent_line_key,
            source_line.split_suffix,
            source_line.timesheet_id,
            source_line.section,
            CASE
              /* The bounded build's canonical digest is over source_row_json.
                 Preserve those bytes exactly so the immutable publication
                 proof remains verifiable through later session clones. */
              WHEN v_bounded_build_clone IS TRUE THEN source_line.source_row_json
              ELSE jsonb_strip_nulls(
                COALESCE(source_line.source_row_json, '{}'::jsonb)
                || jsonb_build_object(
                  'clone_certified', true,
                  'clone_from_session_id', v_source_session_id::text,
                  'clone_to_session_id', p_target_session_id::text,
                  'clone_applied_at_utc', v_now::text,
                  'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
                )
              )
            END,
            source_line.economic_key_json,
            jsonb_strip_nulls(
              COALESCE(source_line.contract_json, '{}'::jsonb)
              || jsonb_build_object(
                'clone_certified', true,
                'clone_from_session_id', v_source_session_id::text,
                'clone_to_session_id', p_target_session_id::text,
                'bounded_build_certified', v_bounded_build_clone,
                'bounded_build_proof_version', CASE WHEN v_bounded_build_clone IS TRUE THEN 1 ELSE NULL::integer END,
                'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
              )
            ),
            source_line.pay_channel_scope,
            'CLONE_REBASE',
            'CURRENT',
            v_now,
            v_now
          FROM public.banking_pay_workbench_candidate_source_lines AS source_line
          WHERE source_line.session_id = v_source_session_id
            AND source_line.candidate_id = v_candidate_id
            AND source_line.session_version = v_source_session.version
            AND source_line.status = 'CURRENT'
            AND (
              COALESCE((SELECT setting.banking_pay_source_publication_identity_enforce_v1_enabled
                          FROM public.settings_defaults AS setting WHERE setting.id=1),false) IS NOT TRUE
              OR source_line.source_publication_id=(
                SELECT source_scope.certified_preview_publication_source_publication_id
                FROM public.banking_pay_workbench_session_scope AS source_scope
                WHERE source_scope.session_id=v_source_session_id
                  AND source_scope.candidate_id=v_candidate_id
                  AND source_scope.certified_preview_publication_parity_ok IS TRUE
                LIMIT 1
              )
            )
          ON CONFLICT (
            session_id,
            candidate_id,
            session_version,
            source_change_seq,
            source_build_run_id,
            (COALESCE(timesheet_id, '00000000-0000-0000-0000-000000000000'::uuid)),
            line_key
          ) WHERE status = 'CURRENT'
          DO UPDATE
          SET source_ordinal = EXCLUDED.source_ordinal,
              parent_line_key = EXCLUDED.parent_line_key,
              split_suffix = EXCLUDED.split_suffix,
              section = EXCLUDED.section,
              source_row_json = EXCLUDED.source_row_json,
              economic_key_json = EXCLUDED.economic_key_json,
              contract_json = EXCLUDED.contract_json,
              pay_channel_scope = EXCLUDED.pay_channel_scope,
              refresh_scope_kind = EXCLUDED.refresh_scope_kind,
              updated_at_utc = v_now
          RETURNING public.banking_pay_workbench_candidate_source_lines.id
        )
        SELECT v_copied_source_row_count + COUNT(*)::integer
        INTO v_copied_source_row_count
        FROM copied_source_rows;

        WITH copied_line_rows AS (
          INSERT INTO public.banking_pay_workbench_candidate_line_work (
            session_id,
            candidate_id,
            timesheet_id,
            line_key,
            line_ordinal,
            status,
            work_payload_json,
            result_row_json,
            error_json,
            created_at_utc,
            updated_at_utc
          )
          SELECT
            p_target_session_id,
            line_work.candidate_id,
            line_work.timesheet_id,
            line_work.line_key,
            line_work.line_ordinal,
            CASE WHEN UPPER(BTRIM(COALESCE(line_work.status, ''))) IN ('MATERIALISED', 'MATERIALIZED') THEN 'MATERIALISED' ELSE 'SKIPPED' END,
            jsonb_strip_nulls(
              COALESCE(line_work.work_payload_json, '{}'::jsonb)
              || jsonb_build_object(
                'clone_certified', true,
                'clone_from_session_id', v_source_session_id::text,
                'clone_to_session_id', p_target_session_id::text,
                'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
              )
            ),
            jsonb_strip_nulls(
              COALESCE(line_work.result_row_json, '{}'::jsonb)
              || jsonb_build_object(
                'clone_certified', true,
                'clone_from_session_id', v_source_session_id::text,
                'clone_to_session_id', p_target_session_id::text,
                'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
              )
            ),
            NULL::jsonb,
            v_now,
            v_now
          FROM public.banking_pay_workbench_candidate_line_work AS line_work
          WHERE line_work.session_id = v_source_session_id
            AND line_work.candidate_id = v_candidate_id
            AND UPPER(BTRIM(COALESCE(line_work.status, ''))) IN ('MATERIALISED', 'MATERIALIZED', 'SKIPPED', 'SUPERSEDED', 'SOURCE_EMPTY', 'NOT_APPLICABLE', 'OBSOLETE')
          ON CONFLICT (session_id, candidate_id, (COALESCE(timesheet_id, '00000000-0000-0000-0000-000000000000'::uuid)), line_key)
          DO UPDATE
          SET line_ordinal = EXCLUDED.line_ordinal,
              status = EXCLUDED.status,
              work_payload_json = EXCLUDED.work_payload_json,
              result_row_json = EXCLUDED.result_row_json,
              error_json = NULL::jsonb,
              updated_at_utc = v_now
          RETURNING public.banking_pay_workbench_candidate_line_work.id
        )
        SELECT v_copied_line_work_count + COUNT(*)::integer
        INTO v_copied_line_work_count
        FROM copied_line_rows;

        WITH copied_preview_rows AS (
          INSERT INTO public.banking_pay_workbench_preview_rows (
            session_id,
            candidate_id,
            section,
            row_key,
            row_ordinal,
            row_json,
            timesheet_id,
            key_type,
            key_value,
            selected,
            selection_state,
            status,
            session_version,
            created_at_utc,
            updated_at_utc
          )
          SELECT
            p_target_session_id,
            preview_row.candidate_id,
            preview_row.section,
            preview_row.row_key,
            preview_row.row_ordinal,
            jsonb_strip_nulls(
              COALESCE(preview_row.row_json, '{}'::jsonb)
              || jsonb_build_object(
                'clone_certified', true,
                'clone_from_session_id', v_source_session_id::text,
                'clone_to_session_id', p_target_session_id::text,
                'clone_applied_at_utc', v_now::text,
                'session_id', p_target_session_id::text,
                'session_version', v_target_session.version,
                'bounded_build_certified', v_bounded_build_clone,
                'bounded_build_proof_version', CASE WHEN v_bounded_build_clone IS TRUE THEN 1 ELSE NULL::integer END,
                'selected', COALESCE(preview_row.selected, false),
                'selection_state', COALESCE(NULLIF(BTRIM(preview_row.selection_state), ''), CASE WHEN COALESCE(preview_row.selected, false) THEN 'SELECTED' ELSE 'NOT_SELECTABLE' END),
                'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
              )
            ),
            preview_row.timesheet_id,
            preview_row.key_type,
            preview_row.key_value,
            COALESCE(preview_row.selected, false),
            COALESCE(NULLIF(BTRIM(preview_row.selection_state), ''), CASE WHEN COALESCE(preview_row.selected, false) THEN 'SELECTED' ELSE 'NOT_SELECTABLE' END),
            'READY',
            v_target_session.version,
            v_now,
            v_now
          FROM public.banking_pay_workbench_preview_rows AS preview_row
          WHERE preview_row.session_id = v_source_session_id
            AND preview_row.candidate_id = v_candidate_id
            AND preview_row.session_version = v_source_session.version
            AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
          ON CONFLICT (session_id, section, candidate_id, row_key)
          DO UPDATE
          SET row_ordinal = EXCLUDED.row_ordinal,
              row_json = EXCLUDED.row_json,
              timesheet_id = EXCLUDED.timesheet_id,
              key_type = EXCLUDED.key_type,
              key_value = EXCLUDED.key_value,
              selected = EXCLUDED.selected,
              selection_state = EXCLUDED.selection_state,
              status = 'READY',
              session_version = EXCLUDED.session_version,
              updated_at_utc = v_now
          RETURNING public.banking_pay_workbench_preview_rows.id
        )
        SELECT v_copied_preview_row_count + COUNT(*)::integer
        INTO v_copied_preview_row_count
        FROM copied_preview_rows;

        INSERT INTO public.banking_pay_workbench_session_candidate_state AS candidate_state (
          session_id,
          candidate_id,
          status,
          effective_candidate_fragment_json,
          effective_summary_fragment_json,
          effective_paye_candidate_json,
          effective_non_paye_payee_json,
          effective_payees_json,
          effective_case_resolution_states_json,
          effective_canonical_preview_lines_json,
          source_change_seq,
          session_version,
          pending_job_id,
          created_at_utc,
          updated_at_utc,
          last_recomputed_at_utc,
          last_error_json
        )
        SELECT
          p_target_session_id,
          v_candidate_id,
          'READY',
          jsonb_build_object(
            'candidate_id', v_candidate_id::text,
            'session_id', p_target_session_id::text,
            'clone_certified', true,
            'clone_from_session_id', v_source_session_id::text
          ),
          jsonb_build_object(
            'candidate_id', v_candidate_id::text,
            'session_id', p_target_session_id::text,
            'status', 'READY',
            'clone_certified', true,
            'clone_from_session_id', v_source_session_id::text,
            'total_preview_rows', COUNT(*)::integer,
            'selected_rows', COUNT(*) FILTER (
              WHERE preview_row.selected IS TRUE
                AND UPPER(BTRIM(COALESCE(preview_row.selection_state, ''))) = 'SELECTED'
            )::integer
          ),
          NULL::jsonb,
          NULL::jsonb,
          '[]'::jsonb,
          '[]'::jsonb,
          COALESCE(jsonb_agg(preview_row.row_json ORDER BY preview_row.row_ordinal, preview_row.id) FILTER (WHERE preview_row.section = 'canonical_preview_lines'), '[]'::jsonb),
          CASE
            WHEN COALESCE(v_clone_eligibility->>'current_source_change_seq', '') ~ '^[0-9]{1,18}$'
              THEN (v_clone_eligibility->>'current_source_change_seq')::bigint
            ELSE 0::bigint
          END,
          v_target_session.version,
          NULL::uuid,
          v_now,
          v_now,
          v_now,
          NULL::jsonb
        FROM public.banking_pay_workbench_preview_rows AS preview_row
        WHERE preview_row.session_id = p_target_session_id
          AND preview_row.candidate_id = v_candidate_id
          AND preview_row.session_version = v_target_session.version
          AND preview_row.status = 'READY'
        ON CONFLICT (session_id, candidate_id)
        DO UPDATE
        SET status = 'READY',
            effective_candidate_fragment_json = EXCLUDED.effective_candidate_fragment_json,
            effective_summary_fragment_json = EXCLUDED.effective_summary_fragment_json,
            effective_paye_candidate_json = EXCLUDED.effective_paye_candidate_json,
            effective_non_paye_payee_json = EXCLUDED.effective_non_paye_payee_json,
            effective_payees_json = EXCLUDED.effective_payees_json,
            effective_case_resolution_states_json = EXCLUDED.effective_case_resolution_states_json,
            effective_canonical_preview_lines_json = EXCLUDED.effective_canonical_preview_lines_json,
            source_change_seq = EXCLUDED.source_change_seq,
            session_version = EXCLUDED.session_version,
            pending_job_id = NULL::uuid,
            updated_at_utc = v_now,
            last_recomputed_at_utc = v_now,
            last_error_json = NULL::jsonb;

        UPDATE public.banking_pay_workbench_session_scope AS target_scope_update
        SET status = 'READY',
            seeded = true,
            dirty = false,
            pending_job_id = NULL,
            error_json = NULL::jsonb,
            updated_at_utc = v_now
        WHERE target_scope_update.session_id = p_target_session_id
          AND target_scope_update.candidate_id = v_candidate_id;

        v_copied_candidate_count := v_copied_candidate_count + 1;
      END IF;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS stale_target_source
      SET status='SUPERSEDED',updated_at_utc=v_now
      WHERE stale_target_source.session_id=p_target_session_id
        AND stale_target_source.candidate_id=v_candidate_id
        AND stale_target_source.session_version=v_target_session.version
        AND stale_target_source.status='CURRENT'
        AND stale_target_source.source_build_run_id IS DISTINCT FROM v_clone_projection_run_id;

      IF v_clone_owner_job_id IS NULL
         OR coalesce(v_clone_eligibility->>'original_economic_build_id','')
              !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'CERTIFIED_CLONE_OWNER_REQUIRED'
          USING ERRCODE='P0001',
                DETAIL=jsonb_build_object(
                  'code','CERTIFIED_CLONE_OWNER_REQUIRED',
                  'candidate_id',v_candidate_id::text
                )::text;
      END IF;

      v_certified_publication := private.pay_workbench_publish_certified_source_preview_v1(
        p_session_id=>p_target_session_id,
        p_candidate_id=>v_candidate_id,
        p_economic_build_id=>(v_clone_eligibility->>'original_economic_build_id')::uuid,
        p_source_build_run_id=>v_clone_projection_run_id,
        p_source_change_seq=>coalesce(NULLIF(v_clone_eligibility->>'current_source_change_seq',''),'0')::bigint,
        p_session_version=>v_target_session.version,
        p_completion_job_id=>v_clone_owner_job_id,
        p_refresh_scope_kind=>'CANDIDATE_FULL_LIVE',
        p_targeted_timesheet_ids=>'[]'::jsonb,
        p_linked_timesheet_ids=>'[]'::jsonb,
        p_publication_options_json=>jsonb_strip_nulls(jsonb_build_object(
          'contract_version',CASE WHEN v_semantic_publication_v3_enabled THEN 3 ELSE 2 END,
          'semantic_contract_version',CASE WHEN v_semantic_publication_v3_enabled THEN 'READY_TO_PAY_SEMANTIC_V2' ELSE NULL END,
          'authority_kind','CERTIFIED_CLONE',
          'invocation_kind','CLONE_OWNER_FINALISE',
          'final_state',CASE
            WHEN coalesce((v_clone_eligibility->>'ready_empty')::boolean,false) THEN 'SOURCE_EMPTY'
            ELSE 'READY'
          END,
          'certification_version',2,
          'certification_digest',v_clone_eligibility->>'certification_digest',
          'source_session_id',v_source_session_id::text,
          'original_economic_build_id',v_clone_eligibility->>'original_economic_build_id',
          'original_source_build_run_id',v_clone_eligibility->>'original_source_build_run_id',
          'clone_job_id',v_clone_owner_job_id::text,
          'post_clone_action','NONE'
        ))
      );

      IF to_regprocedure('public.pay_workbench_delta_update_candidate_state_v1(uuid,uuid,uuid,jsonb)') IS NOT NULL THEN
        v_candidate_state_result := public.pay_workbench_delta_update_candidate_state_v1(
          p_target_session_id,
          v_candidate_id,
          v_clone_projection_run_id,
          jsonb_build_object(
            'context', 'CLONE_REBASE',
            'source_session_id', v_source_session_id::text,
            'target_session_id', p_target_session_id::text,
            'clone_certified', true,
            'bounded_build_certified', v_bounded_build_clone,
            'source_change_seq', COALESCE(v_clone_eligibility->>'current_source_change_seq', '0'),
            'session_version', COALESCE(v_target_session.version, 1)
          )
        );
      END IF;
    ELSE
      v_ineligible_source_change_seq := 0;
      v_ineligible_final_pay_channel_scope := 'ALL';
      v_current_authority_timesheet_ids_json := '[]'::jsonb;
      v_current_authority_timesheet_count := 0;
      v_current_authority_scope_digest := md5('[]');
      v_target_invalidated_preview_count := 0;
      v_target_invalidated_source_count := 0;
      v_existing_source_build_contract_valid := false;

      SELECT COALESCE(
               (
                 SELECT app_counter.seq
                 FROM public.app_change_counters AS app_counter
                 WHERE app_counter.entity_key = 'pay_candidate:' || v_candidate_id::text
                 LIMIT 1
               ),
               0::bigint
             )::bigint
      INTO v_ineligible_source_change_seq;

      /* Clone-ineligible work is always rebuilt from the complete current candidate
         authority.  Source-visible READY rows are diagnostic only and can never
         narrow this fallback scope. */
      DROP TABLE IF EXISTS pg_temp._bpay_clone_ineligible_authority_timesheets;
      CREATE TEMP TABLE _bpay_clone_ineligible_authority_timesheets ON COMMIT DROP AS
      WITH authority_seed AS (
        SELECT current_tsfin.timesheet_id
        FROM public.timesheets_financials AS current_tsfin
        JOIN public.timesheets AS current_timesheet
          ON current_timesheet.timesheet_id = current_tsfin.timesheet_id
         AND current_timesheet.is_current = true
        WHERE current_tsfin.candidate_id = v_candidate_id
          AND current_tsfin.is_current = true

        UNION

        SELECT source_preview.timesheet_id
        FROM public.banking_pay_workbench_preview_rows AS source_preview
        WHERE source_preview.candidate_id = v_candidate_id
          AND source_preview.timesheet_id IS NOT NULL
          AND (
            (source_preview.session_id = v_source_session_id AND source_preview.session_version = v_source_session.version)
            OR (source_preview.session_id = p_target_session_id AND source_preview.session_version = v_target_session.version)
          )

        UNION

        SELECT source_line.timesheet_id
        FROM public.banking_pay_workbench_candidate_source_lines AS source_line
        WHERE source_line.candidate_id = v_candidate_id
          AND source_line.timesheet_id IS NOT NULL
          AND UPPER(BTRIM(COALESCE(source_line.status, ''))) IN ('CURRENT', 'DIRTY', 'ERROR')
          AND (
            (source_line.session_id = v_source_session_id AND source_line.session_version = v_source_session.version)
            OR (source_line.session_id = p_target_session_id AND source_line.session_version = v_target_session.version)
          )

        UNION

        SELECT snapshot_line.timesheet_id
        FROM public.banking_pay_snapshot_line_state AS snapshot_line
        WHERE snapshot_line.snapshot_run_id IN (v_source_session.source_snapshot_run_id, v_target_session.source_snapshot_run_id)
          AND snapshot_line.candidate_id = v_candidate_id
          AND snapshot_line.timesheet_id IS NOT NULL

        UNION

        SELECT advance_row.linked_timesheet_id
        FROM public.pay_advances AS advance_row
        WHERE advance_row.candidate_id = v_candidate_id
          AND advance_row.linked_timesheet_id IS NOT NULL
          AND UPPER(BTRIM(COALESCE(advance_row.status::text, ''))) IN ('ACTIVE', 'PAUSED')
          AND advance_row.cleared_at_utc IS NULL
          AND advance_row.written_off_at_utc IS NULL

        UNION

        SELECT component_row.linked_timesheet_id
        FROM public.pay_finance_case_components AS component_row
        WHERE component_row.candidate_id = v_candidate_id
          AND component_row.linked_timesheet_id IS NOT NULL
          AND component_row.closed_at_utc IS NULL

        UNION

        SELECT override_row.timesheet_id
        FROM public.timesheet_payment_overrides AS override_row
        WHERE override_row.candidate_id = v_candidate_id
          AND override_row.timesheet_id IS NOT NULL
          AND override_row.consumed_at_utc IS NULL
          AND override_row.cleared_at_utc IS NULL

        UNION

        SELECT adjustment_row.timesheet_id
        FROM public.ts_pay_adjustments AS adjustment_row
        WHERE adjustment_row.candidate_id = v_candidate_id
          AND adjustment_row.timesheet_id IS NOT NULL
          AND adjustment_row.paid_at_utc IS NULL

        UNION

        SELECT snooze_row.timesheet_id
        FROM public.pay_item_snoozes AS snooze_row
        WHERE snooze_row.candidate_id = v_candidate_id
          AND snooze_row.timesheet_id IS NOT NULL
          AND snooze_row.cleared_at_utc IS NULL
          AND snooze_row.cancelled_at_utc IS NULL

        UNION

        SELECT resolution_row.timesheet_id
        FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
        WHERE resolution_row.session_id IN (v_source_session_id, p_target_session_id)
          AND resolution_row.candidate_id = v_candidate_id
          AND resolution_row.timesheet_id IS NOT NULL

        UNION

        SELECT batch_item.timesheet_id
        FROM public.pay_batch_candidates AS batch_candidate
        JOIN public.pay_batch_items AS batch_item
          ON batch_item.pay_batch_candidate_id = batch_candidate.id
        WHERE batch_candidate.candidate_id = v_candidate_id
          AND batch_item.timesheet_id IS NOT NULL
          AND COALESCE(batch_item.is_voided, false) IS NOT TRUE

        UNION

        SELECT correction_item.timesheet_id
        FROM public.pay_payment_correction_items AS correction_item
        WHERE correction_item.candidate_id = v_candidate_id
          AND correction_item.timesheet_id IS NOT NULL
          AND UPPER(BTRIM(COALESCE(correction_item.status, ''))) = 'APPLIED'
      ),
      authority_seed_array AS (
        SELECT COALESCE(array_agg(authority_seed.timesheet_id ORDER BY authority_seed.timesheet_id), ARRAY[]::uuid[]) AS timesheet_ids
        FROM authority_seed
        WHERE authority_seed.timesheet_id IS NOT NULL
      ),
      authority_expanded AS (
        SELECT authority_seed.timesheet_id
        FROM authority_seed
        WHERE authority_seed.timesheet_id IS NOT NULL

        UNION

        SELECT rotation_scope.family_timesheet_id
        FROM authority_seed_array
        CROSS JOIN LATERAL public._pay_timesheet_rotation_scope(authority_seed_array.timesheet_ids) AS rotation_scope
        WHERE rotation_scope.family_timesheet_id IS NOT NULL

        UNION

        SELECT rotation_scope.canonical_timesheet_id
        FROM authority_seed_array
        CROSS JOIN LATERAL public._pay_timesheet_rotation_scope(authority_seed_array.timesheet_ids) AS rotation_scope
        WHERE rotation_scope.canonical_timesheet_id IS NOT NULL
      )
      SELECT DISTINCT authority_expanded.timesheet_id
      FROM authority_expanded
      WHERE authority_expanded.timesheet_id IS NOT NULL;

      SELECT COALESCE(jsonb_agg(authority_timesheet.timesheet_id::text ORDER BY authority_timesheet.timesheet_id), '[]'::jsonb),
             COUNT(*)::integer,
             md5(COALESCE(jsonb_agg(authority_timesheet.timesheet_id::text ORDER BY authority_timesheet.timesheet_id), '[]'::jsonb)::text)
      INTO v_current_authority_timesheet_ids_json,
           v_current_authority_timesheet_count,
           v_current_authority_scope_digest
      FROM pg_temp._bpay_clone_ineligible_authority_timesheets AS authority_timesheet;

      SELECT COALESCE(app_counter.seq, 0)
      INTO v_ineligible_source_change_seq
      FROM (SELECT 1) AS authority_anchor
      LEFT JOIN public.app_change_counters AS app_counter
        ON app_counter.entity_key = 'pay_candidate:' || v_candidate_id::text;

      SELECT CASE
               WHEN UPPER(BTRIM(COALESCE(candidate_row.pay_method, ''))) IN ('PAYE', 'UMBRELLA')
                 THEN UPPER(BTRIM(candidate_row.pay_method))
               ELSE 'ALL'
             END
      INTO v_ineligible_final_pay_channel_scope
      FROM public.candidates AS candidate_row
      WHERE candidate_row.id = v_candidate_id
      LIMIT 1;

      v_ineligible_final_pay_channel_scope := COALESCE(NULLIF(v_ineligible_final_pay_channel_scope, ''), 'ALL');
      v_fallback_targeted_timesheet_ids_json := '[]'::jsonb;
      v_fallback_linked_timesheet_ids_json := '[]'::jsonb;
      v_fallback_targeted_timesheet_count := 0;
      v_ineligible_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';

      v_ineligible_source_build_base_dedupe_key := 'WORKBENCH_CANDIDATE_SOURCE_BUILD:session:' || p_target_session_id::text || ':candidate:' || v_candidate_id::text || ':clone_ineligible';
      v_ineligible_source_build_dedupe_key := v_ineligible_source_build_base_dedupe_key;

      v_ineligible_source_build_seed_text := concat_ws(
        ':',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'CLONE_REBASE_INELIGIBLE_SOURCE_BUILD',
        'target_session_id', p_target_session_id::text,
        'target_session_version', COALESCE(v_target_session.version, 1)::text,
        'target_session_signature', COALESCE(v_target_session.session_signature, ''),
        'target_snapshot_run_id', COALESCE(v_target_session.source_snapshot_run_id::text, ''),
        'source_session_id', v_source_session_id::text,
        'source_session_version', COALESCE(v_source_session.version, 1)::text,
        'source_session_signature', COALESCE(v_source_session.session_signature, ''),
        'source_snapshot_run_id', COALESCE(v_source_session.source_snapshot_run_id::text, ''),
        'candidate_id', v_candidate_id::text,
        'source_change_seq', COALESCE(v_ineligible_source_change_seq, 0)::text,
        'refresh_scope_kind', 'CANDIDATE_FULL_LIVE',
        'current_authority_scope_digest', COALESCE(v_current_authority_scope_digest, md5('[]')),
        'pay_channel_scope', v_ineligible_final_pay_channel_scope,
        'clone_from_session_id', v_source_session_id::text,
        'clone_to_session_id', p_target_session_id::text,
        'overpayment_sync_attestation_required', 'true',
        'terminal_coverage_required', 'true',
        'reason', COALESCE(v_clone_eligibility->>'reason', 'CLONE_REBASE_INELIGIBLE')
      );
      v_ineligible_source_build_hash := md5(v_ineligible_source_build_seed_text);
      v_ineligible_source_build_run_id := (
        substr(v_ineligible_source_build_hash, 1, 8) || '-' ||
        substr(v_ineligible_source_build_hash, 9, 4) || '-' ||
        substr(v_ineligible_source_build_hash, 13, 4) || '-' ||
        substr(v_ineligible_source_build_hash, 17, 4) || '-' ||
        substr(v_ineligible_source_build_hash, 21, 12)
      )::uuid;
      v_ineligible_source_build_corrected_dedupe_key := v_ineligible_source_build_base_dedupe_key || ':corrected:' || v_ineligible_source_build_run_id::text;

      v_ineligible_source_build_payload_json := jsonb_strip_nulls(
        jsonb_build_object(
          'session_id', p_target_session_id::text,
          'target_session_id', p_target_session_id::text,
          'candidate_id', v_candidate_id::text,
          'session_version', v_target_session.version,
          'target_session_version', v_target_session.version,
          'source_change_seq', COALESCE(v_ineligible_source_change_seq, 0),
          'source_change_sequence', COALESCE(v_ineligible_source_change_seq, 0),
          'source_build_run_id', v_ineligible_source_build_run_id::text,
          'operation_type', 'CLONE_REBASE_INELIGIBLE_SOURCE_BUILD',
          'fallback_reason', COALESCE(v_clone_eligibility->>'reason', 'CLONE_REBASE_INELIGIBLE'),
          'refresh_reason', COALESCE(v_clone_eligibility->>'reason', 'CLONE_REBASE_INELIGIBLE'),
          'reason', COALESCE(v_clone_eligibility->>'reason', 'CLONE_INELIGIBLE'),
          'refresh_scope_kind', 'CANDIDATE_FULL_LIVE',
          'pay_channel_scope', v_ineligible_final_pay_channel_scope,
          'source_snapshot_run_id', v_target_session.source_snapshot_run_id::text,
          'snapshot_run_id', v_target_session.source_snapshot_run_id::text,
          'target_snapshot_run_id', v_target_session.source_snapshot_run_id::text,
          'source_session_id', v_source_session_id::text,
          'source_session_version', v_source_session.version,
          'source_session_snapshot_run_id', v_source_session.source_snapshot_run_id::text,
          'clone_from_session_id', v_source_session_id::text,
          'clone_to_session_id', p_target_session_id::text,
          'target_session_signature', v_target_session.session_signature,
          'source_session_signature', v_source_session.session_signature,
          'force_legacy', true,
          'source_build_required', true,
          'line_work_required', true,
          'delta_refresh_required', false
        )
        || jsonb_build_object(
          'targeted_timesheet_ids', '[]'::jsonb,
          'linked_timesheet_ids', '[]'::jsonb,
          'targeted_payload_received', false,
          'source_visible_timesheet_count', 0,
          'current_authority_timesheet_ids', COALESCE(v_current_authority_timesheet_ids_json, '[]'::jsonb),
          'current_authority_timesheet_count', COALESCE(v_current_authority_timesheet_count, 0),
          'current_authority_scope_digest', v_current_authority_scope_digest,
          'source_build_allow_full_fallback', true,
          'source_build_fallback_reason', COALESCE(v_clone_eligibility->>'reason', 'CLONE_REBASE_INELIGIBLE'),
          'overpayment_sync_completed', false,
          'overpayment_sync_attestation_required', true,
          'terminal_coverage_required', true,
          'discard_source_session_completion_markers', true,
          'source_session_completion_marker_authoritative', false,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        )
        || jsonb_build_object(
          'source_build_run_id_source', jsonb_build_object(
            'method', 'DETERMINISTIC_MD5_UUID_FROM_CURRENT_AUTHORITY_INPUTS',
            'seed_text', v_ineligible_source_build_seed_text,
            'target_session_id', p_target_session_id::text,
            'target_session_version', COALESCE(v_target_session.version, 1),
            'target_session_signature', v_target_session.session_signature,
            'target_snapshot_run_id', v_target_session.source_snapshot_run_id::text,
            'source_session_id', v_source_session_id::text,
            'source_session_version', COALESCE(v_source_session.version, 1),
            'source_session_signature', v_source_session.session_signature,
            'source_snapshot_run_id', v_source_session.source_snapshot_run_id::text,
            'candidate_id', v_candidate_id::text,
            'source_change_seq', COALESCE(v_ineligible_source_change_seq, 0),
            'refresh_scope_kind', 'CANDIDATE_FULL_LIVE',
            'current_authority_scope_digest', v_current_authority_scope_digest,
            'pay_channel_scope', v_ineligible_final_pay_channel_scope,
            'clone_from_session_id', v_source_session_id::text,
            'clone_to_session_id', p_target_session_id::text
          )
        )
        || jsonb_build_object(
          'source_build', jsonb_build_object(
            'required', true,
            'run_id', v_ineligible_source_build_run_id::text,
            'source_build_run_id', v_ineligible_source_build_run_id::text,
            'source_change_seq', COALESCE(v_ineligible_source_change_seq, 0),
            'source_change_sequence', COALESCE(v_ineligible_source_change_seq, 0),
            'session_id', p_target_session_id::text,
            'target_session_id', p_target_session_id::text,
            'target_session_version', v_target_session.version,
            'source_session_id', v_source_session_id::text,
            'source_session_version', v_source_session.version,
            'session_version', v_target_session.version,
            'refresh_scope_kind', 'CANDIDATE_FULL_LIVE',
            'pay_channel_scope', v_ineligible_final_pay_channel_scope,
            'targeted_timesheet_ids', '[]'::jsonb,
            'linked_timesheet_ids', '[]'::jsonb,
            'targeted_payload_received', false,
            'current_authority_timesheet_ids', COALESCE(v_current_authority_timesheet_ids_json, '[]'::jsonb),
            'current_authority_scope_digest', v_current_authority_scope_digest,
            'overpayment_sync_attestation_required', true,
            'terminal_coverage_required', true,
            'discard_source_session_completion_markers', true,
            'reason', COALESCE(v_clone_eligibility->>'reason', 'CLONE_INELIGIBLE')
          )
        )
      );

      v_job_id := NULL::uuid;
      v_ineligible_source_build_job_status := NULL::text;
      v_ineligible_source_build_job_payload_json := '{}'::jsonb;
      v_existing_source_build_job_id := NULL::uuid;
      v_existing_source_build_job_status := NULL::text;
      v_existing_source_build_job_payload_json := '{}'::jsonb;
      v_existing_source_build_run_id_text := NULL::text;
      v_running_fail_close_result := '{}'::jsonb;
      v_running_conflict_replacement_enqueued := false;
      v_running_conflict_job_id := NULL::uuid;
      v_running_conflict_status := NULL::text;
      v_running_conflict_run_id_text := NULL::text;
      v_active_conflict_replacement_enqueued := false;
      v_active_conflict_job_id := NULL::uuid;
      v_active_conflict_status := NULL::text;
      v_active_conflict_run_id_text := NULL::text;

      SELECT corrected_job.id,
             corrected_job.status,
             COALESCE(corrected_job.payload_json, '{}'::jsonb)
      INTO v_job_id,
           v_ineligible_source_build_job_status,
           v_ineligible_source_build_job_payload_json
      FROM public.banking_pay_workbench_jobs AS corrected_job
      WHERE corrected_job.dedupe_key = v_ineligible_source_build_corrected_dedupe_key
        AND corrected_job.status IN ('QUEUED', 'RUNNING')
      ORDER BY CASE WHEN corrected_job.status = 'QUEUED' THEN 0 ELSE 1 END,
               corrected_job.updated_at_utc DESC NULLS LAST,
               corrected_job.created_at_utc DESC NULLS LAST,
               corrected_job.id DESC
      LIMIT 1
      FOR UPDATE SKIP LOCKED;

      IF v_job_id IS NOT NULL THEN
        v_ineligible_source_build_dedupe_key := v_ineligible_source_build_corrected_dedupe_key;
      ELSE
        SELECT corrected_job.id,
               corrected_job.status,
               COALESCE(corrected_job.payload_json, '{}'::jsonb)
        INTO v_job_id,
             v_ineligible_source_build_job_status,
             v_ineligible_source_build_job_payload_json
        FROM public.banking_pay_workbench_jobs AS corrected_job
        WHERE corrected_job.dedupe_key = v_ineligible_source_build_corrected_dedupe_key
          AND corrected_job.status IN ('QUEUED', 'RUNNING')
        ORDER BY CASE WHEN corrected_job.status = 'QUEUED' THEN 0 ELSE 1 END,
                 corrected_job.updated_at_utc DESC NULLS LAST,
                 corrected_job.created_at_utc DESC NULLS LAST,
                 corrected_job.id DESC
        LIMIT 1;

        IF v_job_id IS NOT NULL THEN
          v_ineligible_source_build_dedupe_key := v_ineligible_source_build_corrected_dedupe_key;
        END IF;
      END IF;

      IF v_job_id IS NOT NULL THEN
        SELECT (
          COALESCE(
            v_ineligible_source_build_job_payload_json->>'source_build_run_id',
            v_ineligible_source_build_job_payload_json#>>'{source_build,source_build_run_id}',
            v_ineligible_source_build_job_payload_json#>>'{source_build,run_id}',
            ''
          ) = v_ineligible_source_build_run_id::text
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'session_id', v_ineligible_source_build_job_payload_json->>'target_session_id', '') = p_target_session_id::text
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'candidate_id', '') = v_candidate_id::text
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'session_version', v_ineligible_source_build_job_payload_json->>'target_session_version', '') = v_target_session.version::text
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_session_id', '') = v_source_session_id::text
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_session_version', '') = v_source_session.version::text
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_change_seq', v_ineligible_source_build_job_payload_json->>'source_change_sequence', '') = v_ineligible_source_change_seq::text
          AND UPPER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'refresh_scope_kind', v_ineligible_source_build_job_payload_json#>>'{source_build,refresh_scope_kind}', ''))) = 'CANDIDATE_FULL_LIVE'
          AND UPPER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'pay_channel_scope', v_ineligible_source_build_job_payload_json#>>'{source_build,pay_channel_scope}', ''))) = v_ineligible_final_pay_channel_scope
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'target_session_signature', '') = COALESCE(v_target_session.session_signature, '')
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_session_signature', '') = COALESCE(v_source_session.session_signature, '')
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'target_snapshot_run_id', v_ineligible_source_build_job_payload_json->>'snapshot_run_id', '') = COALESCE(v_target_session.source_snapshot_run_id::text, '')
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_session_snapshot_run_id', '') = COALESCE(v_source_session.source_snapshot_run_id::text, '')
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'current_authority_scope_digest', '') = COALESCE(v_current_authority_scope_digest, '')
          AND jsonb_typeof(v_ineligible_source_build_job_payload_json->'targeted_timesheet_ids') = 'array'
          AND jsonb_array_length(CASE WHEN jsonb_typeof(v_ineligible_source_build_job_payload_json->'targeted_timesheet_ids') = 'array' THEN v_ineligible_source_build_job_payload_json->'targeted_timesheet_ids' ELSE '[]'::jsonb END) = 0
          AND jsonb_typeof(v_ineligible_source_build_job_payload_json->'linked_timesheet_ids') = 'array'
          AND jsonb_array_length(CASE WHEN jsonb_typeof(v_ineligible_source_build_job_payload_json->'linked_timesheet_ids') = 'array' THEN v_ineligible_source_build_job_payload_json->'linked_timesheet_ids' ELSE '[]'::jsonb END) = 0
          AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'overpayment_sync_attestation_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'terminal_coverage_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'discard_source_session_completion_markers', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'overpayment_sync_completed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        )
        INTO v_existing_source_build_contract_valid;

        IF UPPER(BTRIM(COALESCE(v_ineligible_source_build_job_status, ''))) = 'QUEUED' THEN
          UPDATE public.banking_pay_workbench_jobs AS corrected_queued_job
          SET run_at_utc = LEAST(corrected_queued_job.run_at_utc, v_now),
              priority = LEAST(corrected_queued_job.priority, 60),
              payload_json = jsonb_strip_nulls(
                COALESCE(corrected_queued_job.payload_json, '{}'::jsonb)
                || v_ineligible_source_build_payload_json
                || jsonb_build_object(
                  'queued_payload_repaired_at_utc', v_now::text,
                  'queued_payload_repaired_reason', 'CLONE_REBASE_FULL_LIVE_AUTHORITY_CONTRACT_REASSERTED',
                  'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
                )
              ),
              updated_at_utc = v_now
          WHERE corrected_queued_job.id = v_job_id
            AND corrected_queued_job.status = 'QUEUED'
          RETURNING corrected_queued_job.id, corrected_queued_job.status, corrected_queued_job.payload_json
          INTO v_job_id, v_ineligible_source_build_job_status, v_ineligible_source_build_job_payload_json;
          v_existing_source_build_contract_valid := true;
          v_ineligible_source_build_dedupe_key := v_ineligible_source_build_corrected_dedupe_key;
        ELSIF UPPER(BTRIM(COALESCE(v_ineligible_source_build_job_status, ''))) = 'RUNNING'
              AND COALESCE(v_existing_source_build_contract_valid, false) IS NOT TRUE THEN
          v_active_conflict_replacement_enqueued := true;
          v_active_conflict_job_id := v_job_id;
          v_active_conflict_status := v_ineligible_source_build_job_status;
          v_active_conflict_run_id_text := COALESCE(
            v_ineligible_source_build_job_payload_json->>'source_build_run_id',
            v_ineligible_source_build_job_payload_json#>>'{source_build,source_build_run_id}',
            v_ineligible_source_build_job_payload_json#>>'{source_build,run_id}'
          );
          v_running_conflict_replacement_enqueued := true;
          v_running_conflict_job_id := v_job_id;
          v_running_conflict_status := v_ineligible_source_build_job_status;
          v_running_conflict_run_id_text := v_active_conflict_run_id_text;
          v_job_id := NULL::uuid;
          v_ineligible_source_build_job_status := NULL::text;
          v_ineligible_source_build_job_payload_json := '{}'::jsonb;
          v_ineligible_source_build_dedupe_key := v_ineligible_source_build_base_dedupe_key;
        ELSE
          v_ineligible_source_build_dedupe_key := v_ineligible_source_build_corrected_dedupe_key;
        END IF;
      END IF;

      IF v_job_id IS NULL THEN
        SELECT existing_job.id,
             existing_job.status,
             COALESCE(existing_job.payload_json, '{}'::jsonb)
      INTO v_existing_source_build_job_id,
           v_existing_source_build_job_status,
           v_existing_source_build_job_payload_json
      FROM public.banking_pay_workbench_jobs AS existing_job
      WHERE existing_job.dedupe_key = v_ineligible_source_build_dedupe_key
        AND existing_job.status IN ('QUEUED', 'RUNNING')
      ORDER BY CASE WHEN existing_job.status = 'QUEUED' THEN 0 ELSE 1 END,
               existing_job.updated_at_utc DESC NULLS LAST,
               existing_job.created_at_utc DESC NULLS LAST,
               existing_job.id DESC
      LIMIT 1
      FOR UPDATE SKIP LOCKED;

      IF v_existing_source_build_job_id IS NOT NULL THEN
        v_existing_source_build_run_id_text := NULLIF(BTRIM(COALESCE(
          v_existing_source_build_job_payload_json->>'source_build_run_id',
          v_existing_source_build_job_payload_json#>>'{source_build,source_build_run_id}',
          v_existing_source_build_job_payload_json#>>'{source_build,run_id}',
          v_existing_source_build_job_payload_json#>>'{cursor,source_build_run_id}',
          v_existing_source_build_job_payload_json#>>'{cursor_json,source_build_run_id}',
          v_existing_source_build_job_payload_json#>>'{result_json,source_build_run_id}',
          ''
        )), '');

        SELECT (
          COALESCE(
            v_existing_source_build_job_payload_json->>'source_build_run_id',
            v_existing_source_build_job_payload_json#>>'{source_build,source_build_run_id}',
            v_existing_source_build_job_payload_json#>>'{source_build,run_id}',
            ''
          ) = v_ineligible_source_build_run_id::text
          AND COALESCE(v_existing_source_build_job_payload_json->>'session_id', v_existing_source_build_job_payload_json->>'target_session_id', '') = p_target_session_id::text
          AND COALESCE(v_existing_source_build_job_payload_json->>'candidate_id', '') = v_candidate_id::text
          AND COALESCE(v_existing_source_build_job_payload_json->>'session_version', v_existing_source_build_job_payload_json->>'target_session_version', '') = v_target_session.version::text
          AND COALESCE(v_existing_source_build_job_payload_json->>'source_session_id', '') = v_source_session_id::text
          AND COALESCE(v_existing_source_build_job_payload_json->>'source_session_version', '') = v_source_session.version::text
          AND COALESCE(v_existing_source_build_job_payload_json->>'source_change_seq', v_existing_source_build_job_payload_json->>'source_change_sequence', '') = v_ineligible_source_change_seq::text
          AND UPPER(BTRIM(COALESCE(v_existing_source_build_job_payload_json->>'refresh_scope_kind', v_existing_source_build_job_payload_json#>>'{source_build,refresh_scope_kind}', ''))) = 'CANDIDATE_FULL_LIVE'
          AND UPPER(BTRIM(COALESCE(v_existing_source_build_job_payload_json->>'pay_channel_scope', v_existing_source_build_job_payload_json#>>'{source_build,pay_channel_scope}', ''))) = v_ineligible_final_pay_channel_scope
          AND COALESCE(v_existing_source_build_job_payload_json->>'target_session_signature', '') = COALESCE(v_target_session.session_signature, '')
          AND COALESCE(v_existing_source_build_job_payload_json->>'source_session_signature', '') = COALESCE(v_source_session.session_signature, '')
          AND COALESCE(v_existing_source_build_job_payload_json->>'target_snapshot_run_id', v_existing_source_build_job_payload_json->>'snapshot_run_id', '') = COALESCE(v_target_session.source_snapshot_run_id::text, '')
          AND COALESCE(v_existing_source_build_job_payload_json->>'source_session_snapshot_run_id', '') = COALESCE(v_source_session.source_snapshot_run_id::text, '')
          AND COALESCE(v_existing_source_build_job_payload_json->>'current_authority_scope_digest', '') = COALESCE(v_current_authority_scope_digest, '')
          AND jsonb_typeof(v_existing_source_build_job_payload_json->'targeted_timesheet_ids') = 'array'
          AND jsonb_array_length(CASE WHEN jsonb_typeof(v_existing_source_build_job_payload_json->'targeted_timesheet_ids') = 'array' THEN v_existing_source_build_job_payload_json->'targeted_timesheet_ids' ELSE '[]'::jsonb END) = 0
          AND jsonb_typeof(v_existing_source_build_job_payload_json->'linked_timesheet_ids') = 'array'
          AND jsonb_array_length(CASE WHEN jsonb_typeof(v_existing_source_build_job_payload_json->'linked_timesheet_ids') = 'array' THEN v_existing_source_build_job_payload_json->'linked_timesheet_ids' ELSE '[]'::jsonb END) = 0
          AND LOWER(BTRIM(COALESCE(v_existing_source_build_job_payload_json->>'overpayment_sync_attestation_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND LOWER(BTRIM(COALESCE(v_existing_source_build_job_payload_json->>'terminal_coverage_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND LOWER(BTRIM(COALESCE(v_existing_source_build_job_payload_json->>'discard_source_session_completion_markers', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND LOWER(BTRIM(COALESCE(v_existing_source_build_job_payload_json->>'overpayment_sync_completed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        )
        INTO v_existing_source_build_contract_valid;

        IF UPPER(BTRIM(COALESCE(v_existing_source_build_job_status, ''))) = 'QUEUED' THEN
          UPDATE public.banking_pay_workbench_jobs AS queued_job
          SET run_at_utc = LEAST(queued_job.run_at_utc, v_now),
              priority = LEAST(queued_job.priority, 60),
              payload_json = jsonb_strip_nulls(
                COALESCE(queued_job.payload_json, '{}'::jsonb)
                || v_ineligible_source_build_payload_json
                || jsonb_build_object(
                  'queued_payload_repaired_at_utc', v_now::text,
                  'queued_payload_repaired_reason', 'CLONE_REBASE_INELIGIBLE_SOURCE_BUILD_ENSURED_RUN_ID',
                  'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
                )
              ),
              updated_at_utc = v_now
          WHERE queued_job.id = v_existing_source_build_job_id
            AND queued_job.status = 'QUEUED'
          RETURNING queued_job.id, queued_job.status, queued_job.payload_json
          INTO v_job_id, v_ineligible_source_build_job_status, v_ineligible_source_build_job_payload_json;
        ELSIF UPPER(BTRIM(COALESCE(v_existing_source_build_job_status, ''))) = 'RUNNING'
              AND COALESCE(v_existing_source_build_contract_valid, false) IS NOT TRUE
              AND to_regprocedure('public.pay_workbench_fail_job(uuid,jsonb,integer)') IS NOT NULL THEN
          v_running_fail_close_result := public.pay_workbench_fail_job(
            v_existing_source_build_job_id,
            jsonb_build_object(
              'code', 'CLONE_REBASE_RUNNING_SOURCE_BUILD_AUTHORITY_CONTRACT_INVALID_FAILED_CLOSED',
              'message', 'Running clone-ineligible source-build job was locked by clone rebase, failed the complete current-authority payload contract, and was failed closed before a corrected full-live source-build job was created.',
              'job_id', v_existing_source_build_job_id::text,
              'session_id', p_target_session_id::text,
              'candidate_id', v_candidate_id::text,
              'source_change_seq', COALESCE(v_ineligible_source_change_seq, 0),
              'replacement_source_build_run_id', v_ineligible_source_build_run_id::text,
              'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
            ),
            NULL::integer
          );

          INSERT INTO public.banking_pay_workbench_jobs (
            job_type,
            status,
            priority,
            run_at_utc,
            dedupe_key,
            snapshot_run_id,
            session_id,
            candidate_id,
            payload_json,
            economic_build_id,
            private_stage,
            private_cursor_kind,
            private_cursor_json,
            private_stage_version,
            created_at_utc,
            updated_at_utc
          )
          VALUES (
            'WORKBENCH_CANDIDATE_SOURCE_BUILD',
            'QUEUED',
            60,
            v_now,
            v_ineligible_source_build_dedupe_key,
            v_target_session.source_snapshot_run_id,
            p_target_session_id,
            v_candidate_id,
            v_ineligible_source_build_payload_json || jsonb_build_object(
              'running_malformed_predecessor_failed_closed_job_id', v_existing_source_build_job_id::text,
              'running_malformed_predecessor_failed_closed_result', COALESCE(v_running_fail_close_result, '{}'::jsonb),
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            ),
            NULL::uuid,
            'BUILD_INITIALISE',
            'BUILD_INITIALISE',
            '{}'::jsonb,
            1,
            v_now,
            v_now
          )
          ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
          DO UPDATE
          SET run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
              priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
              payload_json = jsonb_strip_nulls(COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb) || EXCLUDED.payload_json),
              updated_at_utc = v_now
          WHERE public.banking_pay_workbench_jobs.status = 'QUEUED'
          RETURNING id, status, payload_json
          INTO v_job_id, v_ineligible_source_build_job_status, v_ineligible_source_build_job_payload_json;
        ELSE
          v_job_id := v_existing_source_build_job_id;
          v_ineligible_source_build_job_status := v_existing_source_build_job_status;
          v_ineligible_source_build_job_payload_json := v_existing_source_build_job_payload_json;
        END IF;
      ELSE
        SELECT existing_job.id,
               existing_job.status,
               COALESCE(existing_job.payload_json, '{}'::jsonb)
        INTO v_existing_source_build_job_id,
             v_existing_source_build_job_status,
             v_existing_source_build_job_payload_json
        FROM public.banking_pay_workbench_jobs AS existing_job
        WHERE existing_job.dedupe_key = v_ineligible_source_build_dedupe_key
          AND existing_job.status IN ('QUEUED', 'RUNNING')
        ORDER BY CASE WHEN existing_job.status = 'QUEUED' THEN 0 ELSE 1 END,
                 existing_job.updated_at_utc DESC NULLS LAST,
                 existing_job.created_at_utc DESC NULLS LAST,
                 existing_job.id DESC
        LIMIT 1;

        IF v_existing_source_build_job_id IS NOT NULL THEN
          v_job_id := v_existing_source_build_job_id;
          v_ineligible_source_build_job_status := v_existing_source_build_job_status;
          v_ineligible_source_build_job_payload_json := v_existing_source_build_job_payload_json;
        ELSE
          INSERT INTO public.banking_pay_workbench_jobs (
            job_type,
            status,
            priority,
            run_at_utc,
            dedupe_key,
            snapshot_run_id,
            session_id,
            candidate_id,
            payload_json,
            economic_build_id,
            private_stage,
            private_cursor_kind,
            private_cursor_json,
            private_stage_version,
            created_at_utc,
            updated_at_utc
          )
          VALUES (
            'WORKBENCH_CANDIDATE_SOURCE_BUILD',
            'QUEUED',
            60,
            v_now,
            v_ineligible_source_build_dedupe_key,
            v_target_session.source_snapshot_run_id,
            p_target_session_id,
            v_candidate_id,
            v_ineligible_source_build_payload_json,
            NULL::uuid,
            'BUILD_INITIALISE',
            'BUILD_INITIALISE',
            '{}'::jsonb,
            1,
            v_now,
            v_now
          )
          ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
          DO UPDATE
          SET run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
              priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
              payload_json = jsonb_strip_nulls(COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb) || EXCLUDED.payload_json),
              updated_at_utc = v_now
          WHERE public.banking_pay_workbench_jobs.status = 'QUEUED'
          RETURNING id, status, payload_json
          INTO v_job_id, v_ineligible_source_build_job_status, v_ineligible_source_build_job_payload_json;
        END IF;
      END IF;

      IF v_job_id IS NULL THEN
        SELECT existing_job.id,
               existing_job.status,
               COALESCE(existing_job.payload_json, '{}'::jsonb)
        INTO v_job_id,
             v_ineligible_source_build_job_status,
             v_ineligible_source_build_job_payload_json
        FROM public.banking_pay_workbench_jobs AS existing_job
        WHERE existing_job.dedupe_key = v_ineligible_source_build_dedupe_key
          AND existing_job.status IN ('QUEUED', 'RUNNING')
        ORDER BY CASE WHEN existing_job.status = 'QUEUED' THEN 0 ELSE 1 END,
                 existing_job.updated_at_utc DESC NULLS LAST,
                 existing_job.created_at_utc DESC NULLS LAST,
                 existing_job.id DESC
        LIMIT 1;
      END IF;

      END IF;

      IF v_job_id IS NOT NULL
         AND UPPER(BTRIM(COALESCE(v_ineligible_source_build_job_status, ''))) IN ('QUEUED', 'RUNNING') THEN
        v_existing_source_build_run_id_text := NULLIF(BTRIM(COALESCE(
          v_ineligible_source_build_job_payload_json->>'source_build_run_id',
          v_ineligible_source_build_job_payload_json#>>'{source_build,source_build_run_id}',
          v_ineligible_source_build_job_payload_json#>>'{source_build,run_id}',
          v_ineligible_source_build_job_payload_json#>>'{cursor,source_build_run_id}',
          v_ineligible_source_build_job_payload_json#>>'{cursor_json,source_build_run_id}',
          v_ineligible_source_build_job_payload_json#>>'{result_json,source_build_run_id}',
          ''
        )), '');

        SELECT (
          COALESCE(
            v_ineligible_source_build_job_payload_json->>'source_build_run_id',
            v_ineligible_source_build_job_payload_json#>>'{source_build,source_build_run_id}',
            v_ineligible_source_build_job_payload_json#>>'{source_build,run_id}',
            ''
          ) = v_ineligible_source_build_run_id::text
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'session_id', v_ineligible_source_build_job_payload_json->>'target_session_id', '') = p_target_session_id::text
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'candidate_id', '') = v_candidate_id::text
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'session_version', v_ineligible_source_build_job_payload_json->>'target_session_version', '') = v_target_session.version::text
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_session_id', '') = v_source_session_id::text
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_session_version', '') = v_source_session.version::text
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_change_seq', v_ineligible_source_build_job_payload_json->>'source_change_sequence', '') = v_ineligible_source_change_seq::text
          AND UPPER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'refresh_scope_kind', v_ineligible_source_build_job_payload_json#>>'{source_build,refresh_scope_kind}', ''))) = 'CANDIDATE_FULL_LIVE'
          AND UPPER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'pay_channel_scope', v_ineligible_source_build_job_payload_json#>>'{source_build,pay_channel_scope}', ''))) = v_ineligible_final_pay_channel_scope
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'target_session_signature', '') = COALESCE(v_target_session.session_signature, '')
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_session_signature', '') = COALESCE(v_source_session.session_signature, '')
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'target_snapshot_run_id', v_ineligible_source_build_job_payload_json->>'snapshot_run_id', '') = COALESCE(v_target_session.source_snapshot_run_id::text, '')
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_session_snapshot_run_id', '') = COALESCE(v_source_session.source_snapshot_run_id::text, '')
          AND COALESCE(v_ineligible_source_build_job_payload_json->>'current_authority_scope_digest', '') = COALESCE(v_current_authority_scope_digest, '')
          AND jsonb_typeof(v_ineligible_source_build_job_payload_json->'targeted_timesheet_ids') = 'array'
          AND jsonb_array_length(CASE WHEN jsonb_typeof(v_ineligible_source_build_job_payload_json->'targeted_timesheet_ids') = 'array' THEN v_ineligible_source_build_job_payload_json->'targeted_timesheet_ids' ELSE '[]'::jsonb END) = 0
          AND jsonb_typeof(v_ineligible_source_build_job_payload_json->'linked_timesheet_ids') = 'array'
          AND jsonb_array_length(CASE WHEN jsonb_typeof(v_ineligible_source_build_job_payload_json->'linked_timesheet_ids') = 'array' THEN v_ineligible_source_build_job_payload_json->'linked_timesheet_ids' ELSE '[]'::jsonb END) = 0
          AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'overpayment_sync_attestation_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'terminal_coverage_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'discard_source_session_completion_markers', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'overpayment_sync_completed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        )
        INTO v_existing_source_build_contract_valid;

        IF COALESCE(v_existing_source_build_contract_valid, false) IS NOT TRUE THEN
          v_active_conflict_job_id := v_job_id;
          v_active_conflict_status := v_ineligible_source_build_job_status;
          v_active_conflict_run_id_text := v_existing_source_build_run_id_text;
          v_active_conflict_replacement_enqueued := true;
          IF UPPER(BTRIM(COALESCE(v_ineligible_source_build_job_status, ''))) = 'RUNNING' THEN
            v_running_conflict_job_id := v_job_id;
            v_running_conflict_status := v_ineligible_source_build_job_status;
            v_running_conflict_run_id_text := v_existing_source_build_run_id_text;
            v_running_conflict_replacement_enqueued := true;
          END IF;
          v_job_id := NULL::uuid;
          v_ineligible_source_build_job_status := NULL::text;
          v_ineligible_source_build_job_payload_json := '{}'::jsonb;

          INSERT INTO public.banking_pay_workbench_jobs (
            job_type,
            status,
            priority,
            run_at_utc,
            dedupe_key,
            snapshot_run_id,
            session_id,
            candidate_id,
            payload_json,
            economic_build_id,
            private_stage,
            private_cursor_kind,
            private_cursor_json,
            private_stage_version,
            created_at_utc,
            updated_at_utc
          )
          VALUES (
            'WORKBENCH_CANDIDATE_SOURCE_BUILD',
            'QUEUED',
            60,
            v_now,
            v_ineligible_source_build_corrected_dedupe_key,
            v_target_session.source_snapshot_run_id,
            p_target_session_id,
            v_candidate_id,
            v_ineligible_source_build_payload_json || jsonb_build_object(
              'active_malformed_predecessor_not_mutated_job_id', v_active_conflict_job_id::text,
              'active_malformed_predecessor_status', v_active_conflict_status,
              'active_malformed_predecessor_run_id_text', v_active_conflict_run_id_text,
              'running_malformed_predecessor_not_mutated_job_id', CASE WHEN v_running_conflict_job_id IS NULL THEN NULL ELSE v_running_conflict_job_id::text END,
              'running_malformed_predecessor_status', v_running_conflict_status,
              'running_malformed_predecessor_run_id_text', v_running_conflict_run_id_text,
              'original_source_build_dedupe_key', v_ineligible_source_build_dedupe_key,
              'corrected_source_build_dedupe_key', v_ineligible_source_build_corrected_dedupe_key,
              'replacement_enqueued_because_active_job_not_mutated', true,
              'replacement_enqueued_because_running_job_not_mutated', COALESCE(v_running_conflict_replacement_enqueued, false),
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            ),
            NULL::uuid,
            'BUILD_INITIALISE',
            'BUILD_INITIALISE',
            '{}'::jsonb,
            1,
            v_now,
            v_now
          )
          ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
          DO UPDATE
          SET run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
              priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
              payload_json = jsonb_strip_nulls(COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb) || EXCLUDED.payload_json),
              updated_at_utc = v_now
          WHERE public.banking_pay_workbench_jobs.status = 'QUEUED'
          RETURNING id, status, payload_json
          INTO v_job_id, v_ineligible_source_build_job_status, v_ineligible_source_build_job_payload_json;

          IF v_job_id IS NULL THEN
            SELECT corrected_job.id,
                   corrected_job.status,
                   COALESCE(corrected_job.payload_json, '{}'::jsonb)
            INTO v_job_id,
                 v_ineligible_source_build_job_status,
                 v_ineligible_source_build_job_payload_json
            FROM public.banking_pay_workbench_jobs AS corrected_job
            WHERE corrected_job.dedupe_key = v_ineligible_source_build_corrected_dedupe_key
              AND corrected_job.status IN ('QUEUED', 'RUNNING')
            ORDER BY CASE WHEN corrected_job.status = 'QUEUED' THEN 0 ELSE 1 END,
                     corrected_job.updated_at_utc DESC NULLS LAST,
                     corrected_job.created_at_utc DESC NULLS LAST,
                     corrected_job.id DESC
            LIMIT 1;
          END IF;

          IF v_job_id IS NOT NULL THEN
            v_ineligible_source_build_dedupe_key := v_ineligible_source_build_corrected_dedupe_key;
          END IF;
        ELSE
          v_ineligible_source_build_run_id := v_existing_source_build_run_id_text::uuid;
        END IF;
      END IF;

      IF v_job_id IS NULL THEN
        RAISE EXCEPTION 'Unable to enqueue or reuse clone-ineligible source-build job for session %, candidate %', p_target_session_id, v_candidate_id
          USING ERRCODE = '55000';
      END IF;

      SELECT (
        COALESCE(
          v_ineligible_source_build_job_payload_json->>'source_build_run_id',
          v_ineligible_source_build_job_payload_json#>>'{source_build,source_build_run_id}',
          v_ineligible_source_build_job_payload_json#>>'{source_build,run_id}',
          ''
        ) = v_ineligible_source_build_run_id::text
        AND COALESCE(v_ineligible_source_build_job_payload_json->>'session_id', v_ineligible_source_build_job_payload_json->>'target_session_id', '') = p_target_session_id::text
        AND COALESCE(v_ineligible_source_build_job_payload_json->>'candidate_id', '') = v_candidate_id::text
        AND COALESCE(v_ineligible_source_build_job_payload_json->>'session_version', v_ineligible_source_build_job_payload_json->>'target_session_version', '') = v_target_session.version::text
        AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_session_id', '') = v_source_session_id::text
        AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_session_version', '') = v_source_session.version::text
        AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_change_seq', v_ineligible_source_build_job_payload_json->>'source_change_sequence', '') = v_ineligible_source_change_seq::text
        AND UPPER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'refresh_scope_kind', v_ineligible_source_build_job_payload_json#>>'{source_build,refresh_scope_kind}', ''))) = 'CANDIDATE_FULL_LIVE'
        AND UPPER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'pay_channel_scope', v_ineligible_source_build_job_payload_json#>>'{source_build,pay_channel_scope}', ''))) = v_ineligible_final_pay_channel_scope
        AND COALESCE(v_ineligible_source_build_job_payload_json->>'target_session_signature', '') = COALESCE(v_target_session.session_signature, '')
        AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_session_signature', '') = COALESCE(v_source_session.session_signature, '')
        AND COALESCE(v_ineligible_source_build_job_payload_json->>'target_snapshot_run_id', v_ineligible_source_build_job_payload_json->>'snapshot_run_id', '') = COALESCE(v_target_session.source_snapshot_run_id::text, '')
        AND COALESCE(v_ineligible_source_build_job_payload_json->>'source_session_snapshot_run_id', '') = COALESCE(v_source_session.source_snapshot_run_id::text, '')
        AND COALESCE(v_ineligible_source_build_job_payload_json->>'current_authority_scope_digest', '') = COALESCE(v_current_authority_scope_digest, '')
        AND jsonb_typeof(v_ineligible_source_build_job_payload_json->'targeted_timesheet_ids') = 'array'
        AND jsonb_array_length(CASE WHEN jsonb_typeof(v_ineligible_source_build_job_payload_json->'targeted_timesheet_ids') = 'array' THEN v_ineligible_source_build_job_payload_json->'targeted_timesheet_ids' ELSE '[]'::jsonb END) = 0
        AND jsonb_typeof(v_ineligible_source_build_job_payload_json->'linked_timesheet_ids') = 'array'
        AND jsonb_array_length(CASE WHEN jsonb_typeof(v_ineligible_source_build_job_payload_json->'linked_timesheet_ids') = 'array' THEN v_ineligible_source_build_job_payload_json->'linked_timesheet_ids' ELSE '[]'::jsonb END) = 0
        AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'source_build_allow_full_fallback', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'overpayment_sync_attestation_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'terminal_coverage_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'discard_source_session_completion_markers', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'source_session_completion_marker_authoritative', 'true'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(v_ineligible_source_build_job_payload_json->>'overpayment_sync_completed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
      )
      INTO v_existing_source_build_contract_valid;

      IF COALESCE(v_existing_source_build_contract_valid, false) IS NOT TRUE THEN
        RAISE EXCEPTION 'Clone-ineligible source-build job contract could not be proven for session %, candidate %, job %', p_target_session_id, v_candidate_id, v_job_id
          USING ERRCODE = '55000',
                DETAIL = jsonb_build_object(
                  'code', 'CLONE_REBASE_SOURCE_BUILD_JOB_AUTHORITY_CONTRACT_UNPROVEN',
                  'session_id', p_target_session_id::text,
                  'candidate_id', v_candidate_id::text,
                  'job_id', v_job_id::text,
                  'expected_source_build_run_id', v_ineligible_source_build_run_id::text,
                  'expected_source_change_seq', COALESCE(v_ineligible_source_change_seq, 0),
                  'expected_refresh_scope_kind', 'CANDIDATE_FULL_LIVE',
                  'expected_authority_scope_digest', v_current_authority_scope_digest,
                  'expected_pay_channel_scope', v_ineligible_final_pay_channel_scope
                )::text;
      END IF;

      PERFORM public._audit_insert(
        'banking_pay_workbench_job',
        v_job_id::text,
        CASE
          WHEN UPPER(BTRIM(COALESCE(v_ineligible_source_build_job_status, ''))) = 'QUEUED' THEN 'QUEUED'
          WHEN UPPER(BTRIM(COALESCE(v_ineligible_source_build_job_status, ''))) = 'RUNNING' THEN 'RUNNING_NOT_MUTATED'
          ELSE 'REUSED'
        END,
        NULL::jsonb,
        jsonb_build_object(
          'id', v_job_id::text,
          'job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          'status', v_ineligible_source_build_job_status,
          'session_id', p_target_session_id::text,
          'candidate_id', v_candidate_id::text,
          'bad_job_id', CASE WHEN COALESCE(v_running_fail_close_result, '{}'::jsonb) = '{}'::jsonb THEN NULL::text ELSE COALESCE(v_running_fail_close_result->>'job_id', v_existing_source_build_job_id::text) END,
          'source_build_run_id', v_ineligible_source_build_run_id::text,
          'source_change_seq', COALESCE(v_ineligible_source_change_seq, 0),
          'dedupe_key', v_ineligible_source_build_dedupe_key,
          'operation_type', 'CLONE_REBASE_INELIGIBLE_SOURCE_BUILD',
          'running_job_payload_mutated', false,
          'running_malformed_job_failed_closed', COALESCE(v_running_fail_close_result, '{}'::jsonb) <> '{}'::jsonb,
          'active_conflict_replacement_enqueued', COALESCE(v_active_conflict_replacement_enqueued, false),
          'active_conflict_job_id', CASE WHEN v_active_conflict_job_id IS NULL THEN NULL ELSE v_active_conflict_job_id::text END,
          'active_conflict_status', v_active_conflict_status,
          'running_conflict_replacement_enqueued', COALESCE(v_running_conflict_replacement_enqueued, false),
          'running_conflict_job_id', CASE WHEN v_running_conflict_job_id IS NULL THEN NULL ELSE v_running_conflict_job_id::text END,
          'corrected_source_build_dedupe_key', v_ineligible_source_build_corrected_dedupe_key,
          'deterministic_source_build_run_id_seed_text', v_ineligible_source_build_seed_text,
          'reason', COALESCE(v_clone_eligibility->>'reason', 'CLONE_REBASE_INELIGIBLE'),
          'refresh_scope_kind', v_ineligible_refresh_scope_kind,
          'targeted_timesheet_ids', '[]'::jsonb,
          'linked_timesheet_ids', '[]'::jsonb,
          'targeted_payload_received', false,
          'current_authority_timesheet_ids', COALESCE(v_current_authority_timesheet_ids_json, '[]'::jsonb),
          'current_authority_timesheet_count', COALESCE(v_current_authority_timesheet_count, 0),
          'current_authority_scope_digest', v_current_authority_scope_digest,
          'pay_channel_scope', v_ineligible_final_pay_channel_scope,
          'overpayment_sync_attestation_required', true,
          'terminal_coverage_required', true,
          'source_session_completion_marker_authoritative', false,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        ),
        CASE
          WHEN UPPER(BTRIM(COALESCE(v_ineligible_source_build_job_status, ''))) = 'RUNNING'
            THEN 'WORKBENCH_CLONE_REBASE_INELIGIBLE_SOURCE_BUILD_RUNNING_NOT_MUTATED'
          ELSE 'WORKBENCH_CLONE_REBASE_INELIGIBLE_SOURCE_BUILD_ENQUEUED_WITH_RUN_ID'
        END,
        NULL
      );

      UPDATE public.banking_pay_workbench_preview_rows AS stale_target_preview
      SET status = 'DIRTY',
          selected = false,
          selection_state = 'NOT_SELECTABLE',
          row_json = jsonb_strip_nulls(
            COALESCE(stale_target_preview.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'clone_rebase_live_refresh_required', true,
              'clone_rebase_live_refresh_reason', COALESCE(v_clone_eligibility->>'reason', 'CLONE_REBASE_INELIGIBLE'),
              'clone_rebase_invalidated_at_utc', v_now::text,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            )
          ),
          updated_at_utc = v_now
      WHERE stale_target_preview.session_id = p_target_session_id
        AND stale_target_preview.candidate_id = v_candidate_id
        AND stale_target_preview.session_version = v_target_session.version
        AND UPPER(BTRIM(COALESCE(stale_target_preview.status, ''))) = 'READY';

      GET DIAGNOSTICS v_target_invalidated_preview_count = ROW_COUNT;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS stale_target_source
      SET status = 'DIRTY',
          updated_at_utc = v_now
      WHERE stale_target_source.session_id = p_target_session_id
        AND stale_target_source.candidate_id = v_candidate_id
        AND stale_target_source.session_version = v_target_session.version
        AND stale_target_source.status = 'CURRENT';

      GET DIAGNOSTICS v_target_invalidated_source_count = ROW_COUNT;

      UPDATE public.banking_pay_workbench_session_candidate_state AS stale_candidate_state
      SET status = 'PENDING',
          source_change_seq = COALESCE(v_ineligible_source_change_seq, 0),
          session_version = v_target_session.version,
          pending_job_id = v_job_id,
          updated_at_utc = v_now,
          last_error_json = NULL::jsonb
      WHERE stale_candidate_state.session_id = p_target_session_id
        AND stale_candidate_state.candidate_id = v_candidate_id;

      UPDATE public.banking_pay_workbench_session_scope AS target_scope_update
      SET status = 'SOURCE_BUILD_PENDING',
          seeded = false,
          dirty = true,
          pending_job_id = v_job_id,
          error_json = NULL::jsonb,
          updated_at_utc = v_now
      WHERE target_scope_update.session_id = p_target_session_id
        AND target_scope_update.candidate_id = v_candidate_id;

      v_legacy_refresh_enqueued_count := v_legacy_refresh_enqueued_count + 1;
    END IF;
  END LOOP;

  IF v_more_due IS TRUE THEN
    UPDATE public.banking_pay_workbench_sessions AS session_update
    SET scope_seed_complete = false,
        scope_next_cursor_json = COALESCE(v_next_cursor_json, '{}'::jsonb),
        scope_total_count = GREATEST(COALESCE(session_update.scope_total_count, 0) + COALESCE(v_processed_candidate_count, 0), 0),
        scope_seeded_count = GREATEST(COALESCE(session_update.scope_seeded_count, 0) + COALESCE(v_copied_candidate_count, 0) + COALESCE(v_ready_empty_candidate_count, 0), 0),
        scope_ready_count = GREATEST(COALESCE(session_update.scope_ready_count, 0) + COALESCE(v_copied_candidate_count, 0) + COALESCE(v_ready_empty_candidate_count, 0), 0),
        scope_pending_count = GREATEST(COALESCE(session_update.scope_pending_count, 0) + COALESCE(v_legacy_refresh_enqueued_count, 0), 0),
        line_units_total = GREATEST(COALESCE(session_update.line_units_total, 0) + COALESCE(v_copied_line_work_count, 0), 0),
        line_units_ready = GREATEST(COALESCE(session_update.line_units_ready, 0) + COALESCE(v_copied_line_work_count, 0), 0),
        preview_row_count = GREATEST(COALESCE(session_update.preview_row_count, 0) + COALESCE(v_copied_preview_row_count, 0), 0),
        progress_state = 'CLONE_REBASING',
        progress_json = jsonb_strip_nulls(
          COALESCE(session_update.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'clone_rebase_applied', true,
            'clone_rebase_counter_source', 'chunk_delta',
            'clone_rebase_last_source_session_id', v_source_session_id::text,
            'clone_rebase_last_at_utc', v_now::text,
            'clone_rebase_copied_candidate_count', COALESCE(v_copied_candidate_count, 0),
            'clone_rebase_ready_empty_candidate_count', COALESCE(v_ready_empty_candidate_count, 0),
            'clone_rebase_copied_preview_row_count', COALESCE(v_copied_preview_row_count, 0),
            'clone_rebase_legacy_refresh_enqueued_count', COALESCE(v_legacy_refresh_enqueued_count, 0),
            'clone_rebase_more_due', true,
            'clone_rebase_next_cursor_json', v_next_cursor_json
          )
        ),
        progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    WHERE session_update.id = p_target_session_id;
  ELSE
    UPDATE public.banking_pay_workbench_sessions AS session_update
    SET scope_seed_complete = COALESCE(v_more_due, false) IS NOT TRUE,
        scope_next_cursor_json = COALESCE(v_next_cursor_json, '{}'::jsonb),
        scope_total_count = (
          SELECT COUNT(*)::integer
          FROM public.banking_pay_workbench_session_scope AS scope_row
          WHERE scope_row.session_id = p_target_session_id
        ),
        progress_state = CASE
          WHEN v_more_due IS TRUE THEN 'CLONE_REBASING'
          WHEN EXISTS (
            SELECT 1
            FROM public.banking_pay_workbench_session_scope AS failed_scope
            WHERE failed_scope.session_id = p_target_session_id
              AND UPPER(BTRIM(COALESCE(failed_scope.status, ''))) IN ('FAILED', 'ERROR', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR')
          ) OR EXISTS (
            SELECT 1
            FROM public.banking_pay_workbench_candidate_line_work AS failed_line_work
            WHERE failed_line_work.session_id = p_target_session_id
              AND UPPER(BTRIM(COALESCE(failed_line_work.status, ''))) IN ('ERROR', 'FAILED')
          ) THEN 'ERROR'
          WHEN COALESCE(v_legacy_refresh_enqueued_count, 0) > 0
            OR EXISTS (
              SELECT 1
              FROM public.banking_pay_workbench_session_scope AS pending_scope
              WHERE pending_scope.session_id = p_target_session_id
                AND (
                  COALESCE(pending_scope.dirty, false) IS TRUE
                  OR UPPER(BTRIM(COALESCE(pending_scope.status, ''))) NOT IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY', 'FAILED', 'ERROR', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR')
                )
            ) THEN 'REFRESHING_CANDIDATES'
          ELSE 'READY'
        END,
        scope_ready_count = (
          SELECT COUNT(*)::integer
          FROM public.banking_pay_workbench_session_scope AS scope_row
          WHERE scope_row.session_id = p_target_session_id
            AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY')
        ),
        scope_pending_count = (
          SELECT COUNT(*)::integer
          FROM public.banking_pay_workbench_session_scope AS scope_row
          WHERE scope_row.session_id = p_target_session_id
            AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) NOT IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY', 'FAILED', 'ERROR', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR')
        ),
        scope_failed_count = (
          SELECT COUNT(*)::integer
          FROM public.banking_pay_workbench_session_scope AS scope_row
          WHERE scope_row.session_id = p_target_session_id
            AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) IN ('FAILED', 'ERROR', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR')
        ),
        scope_seeded_count = (
          SELECT COUNT(*)::integer
          FROM public.banking_pay_workbench_session_scope AS scope_row
          WHERE scope_row.session_id = p_target_session_id
            AND COALESCE(scope_row.seeded, false) IS TRUE
        ),
        line_units_total = (
          SELECT COUNT(*)::integer
          FROM public.banking_pay_workbench_candidate_line_work AS line_work
          WHERE line_work.session_id = p_target_session_id
        ),
        line_units_ready = (
          SELECT COUNT(*)::integer
          FROM public.banking_pay_workbench_candidate_line_work AS line_work
          WHERE line_work.session_id = p_target_session_id
            AND UPPER(BTRIM(COALESCE(line_work.status, ''))) IN ('MATERIALISED', 'MATERIALIZED', 'SKIPPED', 'SUPERSEDED', 'SOURCE_EMPTY', 'NOT_APPLICABLE', 'OBSOLETE')
        ),
        line_units_pending = (
          SELECT COUNT(*)::integer
          FROM public.banking_pay_workbench_candidate_line_work AS line_work
          WHERE line_work.session_id = p_target_session_id
            AND UPPER(BTRIM(COALESCE(line_work.status, ''))) IN ('PENDING', 'READY')
        ),
        line_units_failed = (
          SELECT COUNT(*)::integer
          FROM public.banking_pay_workbench_candidate_line_work AS line_work
          WHERE line_work.session_id = p_target_session_id
            AND UPPER(BTRIM(COALESCE(line_work.status, ''))) IN ('ERROR', 'FAILED')
        ),
        section_counts_json = (
          SELECT COALESCE(jsonb_object_agg(section_count.section, section_count.row_count ORDER BY section_count.section), '{}'::jsonb)
          FROM (
            SELECT preview_row.section, COUNT(*)::integer AS row_count
            FROM public.banking_pay_workbench_preview_rows AS preview_row
            WHERE preview_row.session_id = p_target_session_id
              AND preview_row.session_version = v_target_session.version
              AND preview_row.status = 'READY'
            GROUP BY preview_row.section
          ) AS section_count
        ),
        candidate_sample_rows_json = (
          SELECT COALESCE(jsonb_agg(sample_row.sample_json ORDER BY sample_row.scope_ordinal), '[]'::jsonb)
          FROM (
            SELECT scope_row.scope_ordinal,
                   jsonb_build_object('candidate_id', scope_row.candidate_id::text, 'status', scope_row.status) AS sample_json
            FROM public.banking_pay_workbench_session_scope AS scope_row
            WHERE scope_row.session_id = p_target_session_id
            ORDER BY scope_row.scope_ordinal
            LIMIT 10
          ) AS sample_row
        ),
        preview_row_count = (
          SELECT COUNT(*)::integer
          FROM public.banking_pay_workbench_preview_rows AS preview_row
          WHERE preview_row.session_id = p_target_session_id
            AND preview_row.session_version = v_target_session.version
            AND preview_row.status = 'READY'
        ),
        selected_row_count = (
          SELECT COUNT(*)::integer
          FROM public.banking_pay_workbench_preview_rows AS preview_row
          WHERE preview_row.session_id = p_target_session_id
            AND preview_row.session_version = v_target_session.version
            AND preview_row.status = 'READY'
            AND preview_row.selected IS TRUE
            AND preview_row.selection_state = 'SELECTED'
        ),
        progress_json = jsonb_strip_nulls(
          COALESCE(session_update.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'clone_rebase_applied', true,
            'clone_rebase_last_source_session_id', v_source_session_id::text,
            'clone_rebase_last_at_utc', v_now::text,
            'clone_rebase_copied_candidate_count', COALESCE(v_copied_candidate_count, 0),
            'clone_rebase_legacy_refresh_enqueued_count', COALESCE(v_legacy_refresh_enqueued_count, 0),
            'clone_rebase_case_resolution_carry_forward_disabled', true,
            'clone_rebase_case_resolution_carried_forward_count', 0,
            'clone_rebase_case_resolution_stale_review_count', 0,
            'clone_rebase_case_resolution_preview_row_count', 0,
            'clone_rebase_more_due', COALESCE(v_more_due, false),
            'clone_rebase_next_cursor_json', v_next_cursor_json
          )
        ),
        progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    WHERE session_update.id = p_target_session_id;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'clone_rebase_applied', true,
    'copied_candidate_count', COALESCE(v_copied_candidate_count, 0),
    'ready_empty_candidate_count', COALESCE(v_ready_empty_candidate_count, 0),
    'copied_source_row_count', COALESCE(v_copied_source_row_count, 0),
    'copied_line_work_count', COALESCE(v_copied_line_work_count, 0),
    'copied_preview_row_count', COALESCE(v_copied_preview_row_count, 0),
    'case_resolution_carry_forward_disabled', true,
    'case_resolution_carried_forward_count', 0,
    'case_resolution_stale_review_count', 0,
    'case_resolution_preview_row_count', 0,
    'legacy_refresh_enqueued_count', COALESCE(v_legacy_refresh_enqueued_count, 0),
    'more_due', COALESCE(v_more_due, false),
    'next_cursor_json', v_next_cursor_json
  );
END;
$function$;
ALTER FUNCTION public.pay_workbench_session_clone_eligible_rows_v1(uuid, uuid, integer, jsonb, jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_clone_eligible_rows_v1(uuid, uuid, integer, jsonb, jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_clone_eligible_rows_v1(uuid, uuid, integer, jsonb, jsonb) TO service_role;
-- Reasserted after the final removal of the legacy monolith's prepare drop.
