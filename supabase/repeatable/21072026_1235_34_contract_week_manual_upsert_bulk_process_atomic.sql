-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: fdaafd73c6fe.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.contract_week_manual_upsert_bulk_process_atomic(p_week_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_timesheet_create_json jsonb DEFAULT NULL::jsonb, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_contract_week_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_rotation_json jsonb DEFAULT NULL::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_materialise_staged_evidence boolean DEFAULT true, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text, p_expected_current_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_next_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_response_context text DEFAULT NULL::text, p_queue_timesheet_materialisation_json jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_operation text := CASE WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'contract_week_manual_upsert_bulk_authorise' ELSE 'contract_week_manual_upsert_bulk_process' END;
  v_response_context text := CASE WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'bulk_authorise' ELSE 'bulk_process' END;
  v_result jsonb := '{}'::jsonb;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_error_detail text := NULL;
  v_detail_json jsonb := NULL;
  v_result_error_code text := NULL;
  v_result_message text := NULL;
  v_attempt integer := 0;
  v_max_attempts constant integer := 6;
  v_retry_wait_ms integer := 0;
  v_total_retry_wait_ms integer := 0;
  v_transient_contention boolean := FALSE;
BEGIN

  if p_expected_timesheet_id is not null
     and coalesce((public._ctms_import_correction_classify_v1(p_expected_timesheet_id)
       ->> 'is_import_authoritative_correction')::boolean, false) then
    declare v_transition jsonb;
    begin
      v_transition := public.timesheet_correction_pair_transition_v1(
        p_expected_timesheet_id, 'PROCESS', p_actor_user_id,
        null::uuid, null::text, true, 100
      );
      if coalesce((v_transition ->> 'action_ready')::boolean, false) is not true
         or coalesce((v_transition ->> 'expected_member_count')::integer, 0) > 1 then
        return jsonb_build_object(
          'ok', false,
          'error_code', 'IMPORT_CORRECTION_UNIT_REQUIRES_ATOMIC_PROCESS_ORCHESTRATION',
          'transition', v_transition
        );
      end if;
    end;
  end if;
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_week_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'success', false,
      'operation', v_operation,
      'error_code', 'CONTRACT_WEEK_ID_REQUIRED',
      'message', 'p_week_id is required.'
    );
  END IF;

  LOOP
    v_attempt := v_attempt + 1;
    v_result := '{}'::jsonb;
    v_error_state := NULL;
    v_error_message := NULL;
    v_error_detail := NULL;
    v_detail_json := NULL;
    v_result_error_code := NULL;
    v_result_message := NULL;
    v_transient_contention := FALSE;

    BEGIN
      v_result := public.contract_week_manual_upsert_atomic(
        p_week_id => p_week_id,
        p_expected_timesheet_id => p_expected_timesheet_id,
        p_timesheet_create_json => p_timesheet_create_json,
        p_timesheet_patch_json => p_timesheet_patch_json,
        p_contract_week_patch_json => p_contract_week_patch_json,
        p_tsfin_snapshot_json => COALESCE(p_next_tsfin_snapshot_json, p_tsfin_snapshot_json),
        p_rotation_json => p_rotation_json,
        p_actor_user_id => p_actor_user_id,
        p_materialise_staged_evidence => p_materialise_staged_evidence,
        p_now_utc => v_now,
        p_expected_row_signature => p_expected_row_signature,
        p_queue_timesheet_materialisation_json => p_queue_timesheet_materialisation_json
      );

      v_result_error_code := UPPER(BTRIM(COALESCE(v_result ->> 'error_code', v_result ->> 'error', '')));
      v_result_message := LOWER(BTRIM(COALESCE(v_result ->> 'message', '')));
      v_transient_contention := COALESCE((v_result ->> 'ok')::boolean, true) IS DISTINCT FROM true
        AND (
          v_result_error_code IN ('LOCK_TIMEOUT', '55P03', '40P01', '40001', '57014', 'TRANSIENT_PROCESSING_CONTENTION')
          OR LOWER(v_result_error_code) ~ '(lock[_ ]timeout|deadlock|could not obtain lock|serialization failure|concurrent update)'
          OR LOWER(BTRIM(COALESCE(v_result ->> 'sqlstate', ''))) IN ('55p03', '40p01', '40001', '57014')
          OR LOWER(BTRIM(COALESCE(v_result -> 'detail_json' ->> 'error_state', ''))) IN ('55p03', '40p01', '40001', '57014')
          OR v_result_message ~ '(lock[_ ]timeout|deadlock|could not obtain lock|serialization failure|concurrent update)'
          OR LOWER(COALESCE(v_result ->> 'detail', '')) ~ '(55p03|40p01|40001|57014|lock[_ ]timeout|deadlock|could not obtain lock|serialization failure)'
        );

      IF v_transient_contention IS DISTINCT FROM true THEN
        EXIT;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_error_state = RETURNED_SQLSTATE,
        v_error_message = MESSAGE_TEXT,
        v_error_detail = PG_EXCEPTION_DETAIL;

      BEGIN
        v_detail_json := v_error_detail::jsonb;
      EXCEPTION WHEN OTHERS THEN
        v_detail_json := NULL;
      END;

      v_transient_contention := (
        v_error_state IN ('55P03', '40P01', '40001', '57014')
        OR (
          v_error_state = 'P0001'
          AND UPPER(BTRIM(COALESCE(v_error_message, ''))) IN ('LOCK_TIMEOUT', 'TRANSIENT_PROCESSING_CONTENTION')
        )
        OR LOWER(COALESCE(v_error_message, '')) ~ '(lock[_ ]timeout|deadlock|could not obtain lock|serialization failure|concurrent update)'
        OR LOWER(COALESCE(v_error_detail, '')) ~ '(55p03|40p01|40001|lock[_ ]timeout|deadlock|could not obtain lock|serialization failure)'
      );

      IF v_transient_contention IS DISTINCT FROM true THEN
        RETURN jsonb_build_object(
          'ok', false,
          'success', false,
          'operation', v_operation,
          'response_context', v_response_context,
          'bulk_process', v_response_context = 'bulk_process',
          'bulk_authorise', v_response_context = 'bulk_authorise',
          'error_code', CASE WHEN v_error_state = 'P0001' AND NULLIF(BTRIM(v_error_message), '') IS NOT NULL THEN v_error_message ELSE v_error_state END,
          'sqlstate', v_error_state,
          'message', v_error_message,
          'detail', v_error_detail,
          'detail_json', v_detail_json,
          'contract_week_id', p_week_id,
          'expected_timesheet_id', p_expected_timesheet_id,
          'process_retry_count', GREATEST(v_attempt - 1, 0),
          'process_retry_wait_ms', v_total_retry_wait_ms,
          'refresh_required', true,
          'cache_invalidation_hints', jsonb_build_object(
            'changed_domains', jsonb_build_array('timesheet_lifecycle'),
            'contract_week_id', p_week_id,
            'timesheet_id', p_expected_timesheet_id
          )
        );
      END IF;
    END;

    IF v_attempt >= v_max_attempts THEN
      RETURN jsonb_build_object(
        'ok', false,
        'success', false,
        'operation', v_operation,
        'response_context', v_response_context,
        'bulk_process', v_response_context = 'bulk_process',
        'bulk_authorise', v_response_context = 'bulk_authorise',
        'error_code', 'TRANSIENT_PROCESSING_CONTENTION',
        'message', 'The timesheet could not be completed immediately. The row has been safely refreshed.',
        'contract_week_id', p_week_id,
        'expected_timesheet_id', p_expected_timesheet_id,
        'process_retry_count', GREATEST(v_attempt - 1, 0),
        'process_retry_wait_ms', v_total_retry_wait_ms,
        'refresh_required', true,
        'cache_invalidation_hints', jsonb_build_object(
          'changed_domains', jsonb_build_array('timesheet_lifecycle'),
          'contract_week_id', p_week_id,
          'timesheet_id', p_expected_timesheet_id
        )
      );
    END IF;

    v_retry_wait_ms := LEAST(2000, (150::numeric * POWER(2::numeric, GREATEST(v_attempt - 1, 0)::numeric))::integer) + FLOOR(RANDOM() * 125)::integer;
    v_total_retry_wait_ms := v_total_retry_wait_ms + v_retry_wait_ms;
    PERFORM pg_sleep(v_retry_wait_ms::numeric / 1000::numeric);
  END LOOP;

  RETURN COALESCE(v_result, '{}'::jsonb)
    || jsonb_build_object(
      'ok', COALESCE((v_result ->> 'ok')::boolean, true),
      'success', COALESCE((v_result ->> 'success')::boolean, true),
      'operation', v_operation,
      'response_context', v_response_context,
      'bulk_process', v_response_context = 'bulk_process',
      'bulk_authorise', v_response_context = 'bulk_authorise',
      'process_retry_count', GREATEST(v_attempt - 1, 0),
      'process_retry_wait_ms', v_total_retry_wait_ms,
      'transient_contention_recovered', v_attempt > 1,
      'requires_affected_row_refresh', true,
      'refresh_required', true
    );
END;
$function$;
