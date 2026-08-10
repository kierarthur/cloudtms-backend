-- Prevent a cancellation-generated finance dirty job from overwriting the
-- authoritative full-candidate refresh queued after the cancellation.
--
-- Policy X boundary:
--   * the batch has already been cancelled using frozen batch artefacts;
--   * this function changes only PRE_DRAFT_LIVE_TRUTH refresh orchestration;
--   * no payment, settlement, finance amount, reservation or economic key is
--     created or changed here.

CREATE OR REPLACE FUNCTION public.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1(
  p_session_id uuid,
  p_pay_batch_id uuid,
  p_operation_type text,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_result jsonb := '{}'::jsonb;
  v_operation_type text := UPPER(BTRIM(COALESCE(p_operation_type, '')));
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_candidate_id uuid;
  v_changed_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_enqueue_result jsonb := '{}'::jsonb;
  v_job_id uuid;
  v_full_refresh_count integer := 0;
  v_candidate_full_refresh_count integer := 0;
  v_superseded_targeted_job_count integer := 0;
  v_superseded_targeted_job_chunk_count integer := 0;
  v_superseded_finance_dirty_job_count integer := 0;
  v_full_refresh_job_ids jsonb := '[]'::jsonb;
  v_preserved_selected_preview_row_ids jsonb := '[]'::jsonb;
  v_preserved_selected_row_count integer := 0;
  v_cancelled_row_unselected_count integer := 0;
  v_targeted_timesheet_ids jsonb := '[]'::jsonb;
  v_requested_refresh_scope text := 'CANDIDATE_FULL_LIVE';
BEGIN
  v_result := public.pay_workbench_patch_preview_after_batch_mutation(
    p_session_id,
    p_pay_batch_id,
    p_operation_type,
    p_actor_user_id,
    COALESCE(p_options_json,'{}'::jsonb) || jsonb_build_object('defer_complex_enqueue',true)
  );

  IF COALESCE((v_result->>'ok')::boolean, true) IS NOT TRUE
     OR v_operation_type NOT IN ('DRAFT_DELETE', 'DRAFT_CANCEL') THEN
    RETURN v_result;
  END IF;

  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
    AND UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
    AND session_row.discarded_at_utc IS NULL
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_FULL_REFRESH_SESSION_NOT_OPEN'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_FULL_REFRESH_SESSION_NOT_OPEN',
              'session_id', p_session_id::text,
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  END IF;

  -- Cancelling a draft returns its rows to the live workbench, but it must not
  -- silently select those rows again. Persist an explicit selection snapshot
  -- before the full-candidate refresh is queued so the materialiser preserves
  -- every unrelated selection while restoring the cancelled rows unselected.
  UPDATE public.banking_pay_workbench_preview_rows AS cancelled_preview_row
  SET selected = false,
      selection_state = CASE
        WHEN UPPER(BTRIM(COALESCE(cancelled_preview_row.status, ''))) = 'READY' THEN 'UNSELECTED'
        ELSE 'NOT_SELECTABLE'
      END,
      row_json = jsonb_strip_nulls(
        COALESCE(cancelled_preview_row.row_json, '{}'::jsonb)
        || jsonb_build_object(
          'selected', false,
          'selection_state', CASE
            WHEN UPPER(BTRIM(COALESCE(cancelled_preview_row.status, ''))) = 'READY' THEN 'UNSELECTED'
            ELSE 'NOT_SELECTABLE'
          END,
          'selection_user_override', CASE
            WHEN COALESCE((SELECT setting.banking_pay_selection_intent_identity_v1_enabled
                             FROM public.settings_defaults AS setting WHERE setting.id=1),false)
              THEN 'UNSELECTED' ELSE cancelled_preview_row.row_json->>'selection_user_override' END,
          'selection_origin', CASE
            WHEN COALESCE((SELECT setting.banking_pay_selection_intent_identity_v1_enabled
                             FROM public.settings_defaults AS setting WHERE setting.id=1),false)
              THEN 'POST_CANCEL_RETURN_UNSELECTED' ELSE cancelled_preview_row.row_json->>'selection_origin' END,
          'selection_user_override_at_utc', CASE
            WHEN COALESCE((SELECT setting.banking_pay_selection_intent_identity_v1_enabled
                             FROM public.settings_defaults AS setting WHERE setting.id=1),false)
              THEN v_now::text ELSE cancelled_preview_row.row_json->>'selection_user_override_at_utc' END,
          'post_cancel_selection_restored', true,
          'post_cancel_selection_restored_at_utc', v_now::text,
          'post_cancel_selection_pay_batch_id', p_pay_batch_id::text
        )
      ),
      updated_at_utc = v_now
  WHERE cancelled_preview_row.session_id = p_session_id
    AND cancelled_preview_row.session_version = v_session.version
    AND EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(COALESCE(v_result->'patched_row_ids', '[]'::jsonb)) = 'array'
            THEN COALESCE(v_result->'patched_row_ids', '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) AS patched_preview_row_id(value)
      WHERE patched_preview_row_id.value = cancelled_preview_row.id::text
    );

  GET DIAGNOSTICS v_cancelled_row_unselected_count = ROW_COUNT;

  SELECT COALESCE(
           jsonb_agg(to_jsonb(selected_preview_row.id::text) ORDER BY selected_preview_row.row_ordinal, selected_preview_row.id),
           '[]'::jsonb
         ),
         COUNT(*)::integer
  INTO v_preserved_selected_preview_row_ids,
       v_preserved_selected_row_count
  FROM public.banking_pay_workbench_preview_rows AS selected_preview_row
  WHERE selected_preview_row.session_id = p_session_id
    AND selected_preview_row.session_version = v_session.version
    AND LOWER(COALESCE(NULLIF(BTRIM(selected_preview_row.section), ''), 'canonical_preview_lines')) = 'canonical_preview_lines'
    AND UPPER(BTRIM(COALESCE(selected_preview_row.status, ''))) = 'READY'
    AND COALESCE(selected_preview_row.selected, false) IS TRUE
    AND UPPER(BTRIM(COALESCE(selected_preview_row.selection_state, ''))) = 'SELECTED';

  UPDATE public.banking_pay_workbench_sessions AS selection_session
  SET selected_row_count = COALESCE(v_preserved_selected_row_count, 0),
      server_selected_preview_row_ids = COALESCE(v_preserved_selected_preview_row_ids, '[]'::jsonb),
      server_selected_preview_row_ids_provided = true,
      progress_json = COALESCE(selection_session.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'last_post_cancel_selection_reconcile_at_utc', v_now::text,
          'last_post_cancel_selection_pay_batch_id', p_pay_batch_id::text,
          'last_post_cancel_unselected_row_count', COALESCE(v_cancelled_row_unselected_count, 0),
          'selection_intent_v1', COALESCE(selection_session.progress_json->'selection_intent_v1', '{}'::jsonb)
            || jsonb_build_object(
              'canonical_preview_lines', jsonb_build_object(
                'mode', 'EXPLICIT_INCLUDE',
                'section', 'canonical_preview_lines',
                'identity', 'preview_row_id_with_session_section_candidate_row_key_conflict_identity',
                'updated_at_utc', v_now::text,
                'updated_by_user_id', COALESCE(p_actor_user_id, v_session.actor_user_id)::text,
                'source_selection_mode', 'POST_CANCEL_RECONCILE',
                'source_selection_action', 'RETURN_CANCELLED_ROWS_UNSELECTED',
                'server_selected_preview_row_ids_provided', true,
                'selected_row_count', COALESCE(v_preserved_selected_row_count, 0)
              )
            )
        ),
      progress_counter_version = COALESCE(selection_session.progress_counter_version, 0) + 1,
      progress_updated_at_utc = v_now,
      updated_at_utc = v_now
  WHERE selection_session.id = p_session_id;

  -- The Worker may supply these identifiers, but the cancelled frozen batch is
  -- the fail-closed authority. Reading its retained (now voided) items makes the
  -- race guard independent of optional response fields.
  WITH finance_case_scope AS (
    SELECT batch_item.finance_case_id
    FROM public.pay_batch_items AS batch_item
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = batch_item.pay_batch_candidate_id
    WHERE batch_candidate.pay_batch_id = p_pay_batch_id
      AND batch_item.finance_case_id IS NOT NULL

    UNION ALL

    SELECT raw_value.value_text::uuid
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(p_options_json->'changed_finance_case_ids') = 'array'
          THEN p_options_json->'changed_finance_case_ids'
        ELSE '[]'::jsonb
      END
    ) AS raw_value(value_text)
    WHERE raw_value.value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

    UNION ALL

    SELECT raw_value.value_text::uuid
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(p_options_json#>'{cancellation_result,changed_scope_json,changed_finance_case_ids}') = 'array'
          THEN p_options_json#>'{cancellation_result,changed_scope_json,changed_finance_case_ids}'
        ELSE '[]'::jsonb
      END
    ) AS raw_value(value_text)
    WHERE raw_value.value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  )
  SELECT COALESCE(
    array_agg(DISTINCT finance_case_scope.finance_case_id ORDER BY finance_case_scope.finance_case_id),
    ARRAY[]::uuid[]
  )
  INTO v_changed_finance_case_ids
  FROM finance_case_scope
  WHERE finance_case_scope.finance_case_id IS NOT NULL;

  -- Component restoration and reservation release enqueue finance-case dirty
  -- work in the cancellation transaction. If allowed to fan out afterwards it
  -- can narrow the scope to one linked timesheet and overwrite the correct
  -- candidate-wide recovery allocation. Only still-QUEUED jobs for finance
  -- cases frozen into this cancelled batch are superseded. A supersession is a
  -- successful terminal orchestration result, not a processing failure. Keep
  -- the reason as immutable payload audit evidence while the replacement full
  -- candidate refresh remains responsible for rebuilding the live preview.
  -- RUNNING jobs and all unrelated finance cases are deliberately outside this
  -- update.
  -- Do not terminalise queued finance/targeted work before canonical owner
  -- election.  The enqueue owner now decides whether existing work is covered,
  -- coalesced or genuinely superseded; RUNNING work is never killed here.
  v_superseded_finance_dirty_job_count := 0;

  FOR v_candidate_id IN
    SELECT DISTINCT candidate_value.value::uuid
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_result->'targeted_refresh_candidate_ids') = 'array'
          THEN v_result->'targeted_refresh_candidate_ids'
        ELSE '[]'::jsonb
      END
    ) AS candidate_value(value)
    WHERE candidate_value.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ORDER BY candidate_value.value::uuid
  LOOP
    v_superseded_targeted_job_chunk_count := 0;

    SELECT COALESCE(
      jsonb_agg(DISTINCT batch_item.timesheet_id::text ORDER BY batch_item.timesheet_id::text),
      '[]'::jsonb
    )
    INTO v_targeted_timesheet_ids
    FROM public.pay_batch_items AS batch_item
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id=batch_item.pay_batch_candidate_id
    WHERE batch_candidate.pay_batch_id=p_pay_batch_id
      AND batch_candidate.candidate_id=v_candidate_id
      AND batch_item.timesheet_id IS NOT NULL;

    v_requested_refresh_scope:=CASE
      WHEN jsonb_array_length(COALESCE(v_targeted_timesheet_ids,'[]'::jsonb))>0
        THEN 'TARGETED_TIMESHEETS'
      ELSE 'CANDIDATE_FULL_LIVE'
    END;

    v_enqueue_result := public.pay_workbench_enqueue_candidate_refresh(
      p_snapshot_run_id => v_session.source_snapshot_run_id,
      p_candidate_id => v_candidate_id,
      p_reason => 'POST_DRAFT_CANCEL_CURRENT_AUTHORITY_REFRESH',
      p_actor_user_id => COALESCE(p_actor_user_id, v_session.actor_user_id),
      p_payload_json => jsonb_build_object(
        'session_id', p_session_id::text,
        'source_session_id', p_session_id::text,
        'source_snapshot_run_id', v_session.source_snapshot_run_id::text,
        'snapshot_run_id', v_session.source_snapshot_run_id::text,
        'session_version', v_session.version,
        'session_signature', v_session.session_signature,
        'operation_type', v_operation_type,
        'pay_batch_id', p_pay_batch_id::text,
        'enqueue_origin', 'PAYMENT_CANCEL_CANONICAL_REFRESH_V3',
        'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids,'[]'::jsonb),
        'targeted_timesheet_ids_requested', COALESCE(v_targeted_timesheet_ids,'[]'::jsonb),
        'linked_timesheet_ids', COALESCE(v_targeted_timesheet_ids,'[]'::jsonb),
        'linked_timesheet_ids_requested', COALESCE(v_targeted_timesheet_ids,'[]'::jsonb),
        'refresh_scope_kind', v_requested_refresh_scope,
        'pay_channel_scope', 'ALL',
        'projection_class', 'POST_DRAFT_CANCEL_CURRENT_AUTHORITY',
        'fallback_reason', 'CERTIFIED_CANCELLATION_REVERSION_REJECTED',
        'canonical_route_ladder_required', true,
        'superseded_finance_dirty_job_count', v_superseded_finance_dirty_job_count,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      )
    );

    IF COALESCE((v_enqueue_result->>'ok')::boolean,true) IS NOT TRUE
       OR (
         COALESCE(v_enqueue_result->>'job_id','')
           !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         AND UPPER(BTRIM(COALESCE(v_enqueue_result->>'owner_resolution',''))) NOT IN (
           'ACTIVE_CURRENT_OWNER_COVERS_REQUEST','COMPLETE_CURRENT_AUTHORITY','STALE_INCOMING_REQUEST'
         )
         AND COALESCE((v_enqueue_result->>'coalesced')::boolean,false) IS NOT TRUE
       ) THEN
      RAISE EXCEPTION 'PAYMENT_CANCEL_FULL_REFRESH_JOB_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAYMENT_CANCEL_FULL_REFRESH_JOB_INVALID',
                'session_id', p_session_id::text,
                'candidate_id', v_candidate_id::text,
                'pay_batch_id', p_pay_batch_id::text,
                'enqueue_result', v_enqueue_result
              )::text;
    END IF;

    IF COALESCE(v_enqueue_result->>'job_id','')
         ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_job_id := (v_enqueue_result->>'job_id')::uuid;
      v_full_refresh_job_ids := v_full_refresh_job_ids || jsonb_build_array(v_job_id::text);
      v_full_refresh_count := v_full_refresh_count + 1;
      IF v_requested_refresh_scope='CANDIDATE_FULL_LIVE' THEN
        v_candidate_full_refresh_count:=v_candidate_full_refresh_count+1;
      END IF;
    END IF;
  END LOOP;

  RETURN v_result || jsonb_build_object(
    'targeted_refresh_enqueued', v_full_refresh_count > 0,
    'targeted_refresh_enqueued_count', v_full_refresh_count,
    'full_candidate_refresh_enqueued', v_candidate_full_refresh_count > 0,
    'full_candidate_refresh_enqueued_count', v_candidate_full_refresh_count,
    'full_candidate_refresh_job_ids', v_full_refresh_job_ids,
    'superseded_targeted_refresh_job_count', v_superseded_targeted_job_count,
    'superseded_finance_dirty_job_count', v_superseded_finance_dirty_job_count,
    'changed_finance_case_count', COALESCE(array_length(v_changed_finance_case_ids, 1), 0),
    'cancelled_row_unselected_count', COALESCE(v_cancelled_row_unselected_count, 0),
    'server_selected_preview_row_ids', COALESCE(v_preserved_selected_preview_row_ids, '[]'::jsonb),
    'server_selected_preview_row_ids_provided', true,
    'selected_row_count', COALESCE(v_preserved_selected_row_count, 0),
    'refresh_scope_kind', CASE
      WHEN v_candidate_full_refresh_count>0 AND v_candidate_full_refresh_count=v_full_refresh_count THEN 'CANDIDATE_FULL_LIVE'
      WHEN v_full_refresh_count>0 THEN 'CANONICAL_ROUTE_LADDER'
      ELSE NULL::text END,
    'source_session_preserved', true,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
    'policy_x_checked', true
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1(uuid, uuid, text, uuid, jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1(uuid, uuid, text, uuid, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1(uuid, uuid, text, uuid, jsonb) FROM anon;
REVOKE ALL ON FUNCTION public.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1(uuid, uuid, text, uuid, jsonb) FROM authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1(uuid, uuid, text, uuid, jsonb) TO service_role;
