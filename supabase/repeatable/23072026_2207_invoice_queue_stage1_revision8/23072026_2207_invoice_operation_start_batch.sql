-- CloudTMS Invoice Async V8/V2 public start authority.
-- Non-selection commands use the promoted current core helper; selection roots
-- are created here without any dependency on retired compatibility functions.

CREATE OR REPLACE FUNCTION public.invoice_operation_start_batch(
  p_commands jsonb,
  p_actor_user_id uuid,
  p_now_utc timestamp with time zone DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := statement_timestamp();
  v_jwt_role text := coalesce(
    nullif(current_setting('request.jwt.claim.role', true), ''),
    auth.jwt()->>'role',
    ''
  );
  v_auth_user uuid := auth.uid();
  v_role text;
  v_has_selection boolean := false;
  v_results jsonb := '[]'::jsonb;
  v_command jsonb;
  v_command_no integer;
  v_command_type text;
  v_action text;
  v_error_code text;
  v_error_detail jsonb;
  v_selection_contract jsonb;
  v_selection jsonb;
  v_query jsonb;
  v_normalised_query jsonb;
  v_allow_early boolean;
  v_deliver boolean;
  v_delivery_intent jsonb;
  v_command_token text;
  v_delivery_request_token text;
  v_filter_hash text;
  v_query_hash text;
  v_selection_hash text;
  v_delivery_hash text;
  v_idempotency_key text;
  v_operation_type text;
  v_chunk_type text;
  v_priority integer;
  v_operation_id uuid;
  v_operation_status text;
  v_operation_phase text;
  v_change_seq bigint;
  v_created boolean;
  v_reused boolean;
  v_core_result jsonb;
  v_result_item jsonb;
  v_summary jsonb;
  v_filtered_total integer;
  v_selected_total integer;
  v_previous_statement_timeout text := current_setting('statement_timeout');
BEGIN
  IF v_jwt_role = 'service_role' THEN
    v_now := coalesce(p_now_utc, statement_timestamp());
  END IF;

  IF jsonb_typeof(p_commands) IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION USING errcode = '22023',
      message = 'p_commands must be a JSON array containing 1..1000 commands';
  END IF;

  IF jsonb_array_length(p_commands) < 1
     OR jsonb_array_length(p_commands) > 1000 THEN
    RAISE EXCEPTION USING errcode = '22023',
      message = 'p_commands must be a JSON array containing 1..1000 commands';
  END IF;

  IF p_actor_user_id IS NULL
     OR NOT EXISTS (
       SELECT 1
       FROM public.tms_users u
       WHERE u.id = p_actor_user_id
         AND u.is_active
         AND lower(u.role) = 'admin'
     )
     OR (v_jwt_role <> 'service_role' AND v_auth_user IS DISTINCT FROM p_actor_user_id) THEN
    RAISE EXCEPTION USING errcode = '42501',
      message = 'Active administrator actor and matching authenticated/service caller required';
  END IF;

  SELECT lower(btrim(coalesce(u.role, '')))
    INTO v_role
  FROM public.tms_users u
  WHERE u.id = p_actor_user_id
    AND u.is_active;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(p_commands) e(value)
    WHERE jsonb_typeof(e.value->'selection_contract') = 'object'
  ) INTO v_has_selection;

  IF NOT v_has_selection THEN
    RETURN private._invoice_operation_start_core_v8(p_commands, p_actor_user_id, v_now);
  END IF;

  FOR v_command, v_command_no IN
    SELECT e.value, e.ordinality::integer
    FROM jsonb_array_elements(p_commands) WITH ORDINALITY e(value, ordinality)
    ORDER BY e.ordinality
  LOOP
    v_command_type := upper(btrim(coalesce(v_command->>'command_type', v_command->>'type', '')));
    v_error_code := NULL;
    v_error_detail := NULL;
    v_result_item := NULL;

    IF jsonb_typeof(v_command->'selection_contract') IS DISTINCT FROM 'object' THEN
      v_core_result := private._invoice_operation_start_core_v8(
        jsonb_build_array(v_command),
        p_actor_user_id,
        v_now
      );

      IF jsonb_typeof(v_core_result) = 'array' AND jsonb_array_length(v_core_result) > 0 THEN
        v_results := v_results || jsonb_build_array((v_core_result->0) || jsonb_build_object('command_no', v_command_no));
      ELSE
        v_results := v_results || jsonb_build_array(jsonb_build_object(
          'command_no', v_command_no,
          'command_type', v_command_type,
          'accepted', false,
          'terminal_error', jsonb_build_object('code', 'CORE_START_RETURNED_NO_RESULT'),
          'error', jsonb_build_object('code', 'CORE_START_RETURNED_NO_RESULT')
        ));
      END IF;

      CONTINUE;
    END IF;

    IF v_command_type = 'GENERATE_SELECTED' THEN
      v_action := 'GENERATE';
      v_operation_type := 'GENERATE_INVOICES';
      v_chunk_type := 'GENERATION_GROUP';
      v_priority := 600;
    ELSIF v_command_type = 'ISSUE_INVOICES' THEN
      v_action := 'ISSUE';
      v_operation_type := 'ISSUE_INVOICES';
      v_chunk_type := 'ISSUE_INVOICE';
      v_priority := 850;
    ELSE
      v_error_code := 'BATCH_SELECTION_COMMAND_UNSUPPORTED';
    END IF;

    v_selection_contract := v_command->'selection_contract';

    IF v_error_code IS NULL
       AND coalesce(v_selection_contract->>'contract_version', '') <> 'INVOICE_BATCH_SELECTION_ROOT_V2' THEN
      v_error_code := 'BATCH_SELECTION_CONTRACT_INVALID';
    END IF;

    IF v_error_code IS NULL
       AND jsonb_typeof(v_selection_contract->'query') IS DISTINCT FROM 'object' THEN
      v_error_code := 'BATCH_QUERY_INVALID';
      v_error_detail := jsonb_build_object('field', 'selection_contract.query', 'reason', 'required_object');
    END IF;

    IF v_error_code IS NULL
       AND jsonb_typeof(v_selection_contract->'selection') IS DISTINCT FROM 'object' THEN
      v_error_code := 'BATCH_SELECTION_INVALID';
      v_error_detail := jsonb_build_object('field', 'selection_contract.selection', 'reason', 'required_object');
    END IF;

    IF v_error_code IS NULL THEN
      v_query := v_selection_contract->'query';
      v_selection := v_selection_contract->'selection';

      IF coalesce(v_query->>'contract_version', '') <> 'INVOICE_BATCH_QUERY_V2' THEN
        v_error_code := 'BATCH_QUERY_INVALID';
      ELSIF upper(coalesce(v_query->>'action', '')) <> v_action THEN
        v_error_code := 'BATCH_QUERY_ACTION_MISMATCH';
      END IF;
    END IF;

    IF v_error_code IS NULL THEN
      BEGIN
        PERFORM 1
        FROM private._invoice_batch_selection_rules_v2(v_selection)
        LIMIT 1;
      EXCEPTION WHEN OTHERS THEN
        v_error_code := CASE
          WHEN SQLSTATE = '22023' THEN 'BATCH_SELECTION_INVALID'
          ELSE 'BATCH_SELECTION_INVALID'
        END;
        v_error_detail := jsonb_build_object('sqlstate', SQLSTATE, 'message', SQLERRM);
      END;
    END IF;

    IF v_error_code IS NULL THEN
      v_command_token := nullif(btrim(coalesce(
        v_command->>'command_token',
        v_selection_contract->>'command_token',
        ''
      )), '');

      IF v_command_token IS NULL THEN
        v_error_code := CASE WHEN v_action = 'ISSUE'
          THEN 'ISSUE_COMMAND_TOKEN_REQUIRED'
          ELSE 'GENERATE_COMMAND_TOKEN_REQUIRED'
        END;
      ELSIF length(v_command_token) > 256 THEN
        v_error_code := 'BATCH_COMMAND_TOKEN_INVALID';
        v_error_detail := jsonb_build_object('field', 'command_token', 'reason', 'too_long', 'max_length', 256);
      END IF;
    END IF;

    IF v_error_code IS NULL
       AND v_command ? 'deliver'
       AND jsonb_typeof(v_command->'deliver') <> 'boolean' THEN
      v_error_code := 'ISSUE_FLAGS_MUST_BE_BOOLEAN';
    END IF;

    IF v_error_code IS NULL
       AND v_action = 'ISSUE'
       AND NOT (v_command ? 'deliver') THEN
      v_error_code := 'ISSUE_DELIVERY_MODE_REQUIRED';
    END IF;

    IF v_error_code IS NULL
       AND v_command ? 'allow_early'
       AND jsonb_typeof(v_command->'allow_early') <> 'boolean' THEN
      v_error_code := CASE WHEN v_action = 'ISSUE'
        THEN 'ISSUE_FLAGS_MUST_BE_BOOLEAN'
        ELSE 'ALLOW_EARLY_MUST_BE_BOOLEAN'
      END;
    END IF;

    IF v_error_code IS NULL
       AND v_command ? 'delivery_intent'
       AND jsonb_typeof(v_command->'delivery_intent') <> 'object' THEN
      v_error_code := 'ISSUE_DELIVERY_INTENT_MUST_BE_OBJECT';
    END IF;

    IF v_error_code IS NULL THEN
      v_allow_early := lower(coalesce(
        v_query->>'allow_early',
        v_query#>>'{filters,allow_early}',
        v_command->>'allow_early',
        'false'
      )) IN ('true','t','1','yes','on');

      v_deliver := CASE
        WHEN jsonb_typeof(v_command->'deliver') = 'boolean'
          THEN (v_command->>'deliver')::boolean
        ELSE false
      END;

      v_delivery_intent := CASE
        WHEN jsonb_typeof(v_command->'delivery_intent') = 'object'
          THEN v_command->'delivery_intent'
        ELSE '{}'::jsonb
      END;

      v_delivery_request_token := nullif(btrim(coalesce(
        v_command->>'delivery_request_token',
        ''
      )), '');

      IF v_error_code IS NULL
         AND v_action = 'ISSUE'
         AND v_deliver
         AND (
           v_delivery_intent ? 'recipient_set'
           OR v_delivery_intent ? 'cc'
           OR v_delivery_intent ? 'bcc'
         ) THEN
        v_error_code := 'ISSUE_BATCH_DELIVERY_RECIPIENT_OVERRIDE_UNSUPPORTED';
      END IF;

      IF v_error_code IS NULL
         AND v_action = 'ISSUE'
         AND v_deliver
         AND upper(coalesce(v_delivery_intent->>'route_mode','')) <> 'SERVER_RESOLVED' THEN
        v_error_code := 'ISSUE_DELIVERY_INTENT_INVALID';
      END IF;

      IF v_error_code IS NULL
         AND v_action = 'ISSUE'
         AND v_deliver
         AND v_delivery_request_token IS NULL THEN
        v_error_code := 'DELIVERY_REQUEST_TOKEN_REQUIRED';
      ELSIF v_error_code IS NULL
         AND v_delivery_request_token IS NOT NULL
         AND length(v_delivery_request_token) > 256 THEN
        v_error_code := 'DELIVERY_REQUEST_TOKEN_INVALID';
        v_error_detail := jsonb_build_object('field', 'delivery_request_token', 'reason', 'too_long', 'max_length', 256);
      END IF;

      IF v_error_code IS NULL THEN
        v_normalised_query := (v_query - 'cursor' - 'after_selection_key' - 'selection' - 'mode' - 'action' - 'page_size')
        || jsonb_build_object(
          'contract_version', 'INVOICE_BATCH_QUERY_V2',
          'action', v_action,
          'mode', 'EXPAND_SELECTION',
          'page_size', 250,
          'allow_early', v_allow_early,
          'selection', v_selection
        );
      END IF;

      IF v_error_code IS NULL THEN
        BEGIN
          perform set_config('statement_timeout','7000',true);
          v_summary := case when v_action='GENERATE'
            then private._invoice_batch_generate_candidate_rows_v2(
              v_normalised_query || jsonb_build_object('mode','SUMMARY'),
              v_now
            )
            else private._invoice_batch_issue_candidate_rows_v2(
              v_normalised_query || jsonb_build_object('mode','SUMMARY'),
              v_now
            )
          end;
          perform set_config('statement_timeout',v_previous_statement_timeout,true);

          v_filtered_total := coalesce(
            (v_summary#>>'{totals,filtered_total}')::integer,
            0
          );
          v_selected_total := coalesce(
            (v_summary#>>'{selection_summary,selected_total}')::integer,
            0
          );

          if v_filtered_total > 25000 then
            v_error_code := 'BATCH_SUMMARY_SCOPE_TOO_LARGE';
          elsif v_selected_total = 0 then
            v_error_code := 'BATCH_SELECTION_EMPTY';
          end if;
        exception
          when query_canceled then
            perform set_config('statement_timeout',v_previous_statement_timeout,true);
            v_error_code := 'BATCH_SUMMARY_TIMEOUT';
          when others then
            perform set_config('statement_timeout',v_previous_statement_timeout,true);
            v_error_code := case
              when sqlerrm in (
                'BATCH_SNAPSHOT_REQUIRED',
                'BATCH_SNAPSHOT_INVALID',
                'BATCH_SNAPSHOT_EXPIRED',
                'BATCH_SNAPSHOT_CHANGED',
                'BATCH_SELECTION_INVALID',
                'BATCH_SELECTION_CONTRACT_INVALID',
                'BATCH_SELECTION_SELECTOR_INVALID'
              ) then sqlerrm
              else 'BATCH_QUERY_INVALID'
            end;
            v_error_detail := jsonb_build_object(
              'sqlstate',sqlstate,
              'message',sqlerrm
            );
        end;
      END IF;

      IF v_error_code IS NULL THEN
      v_filter_hash := private._invoice_batch_hash_v2(
        jsonb_build_object(
          'action',v_action,
          'filters',coalesce(v_normalised_query->'filters','{}'::jsonb),
          'sort',coalesce(v_normalised_query->'sort','{}'::jsonb)
        )
      );
      v_query_hash := private._invoice_batch_hash_v2(
        jsonb_build_object(
          'contract_version','INVOICE_BATCH_QUERY_V2',
          'action',v_action,
          'filters',coalesce(v_normalised_query->'filters','{}'::jsonb),
          'sort',coalesce(v_normalised_query->'sort','{}'::jsonb),
          'snapshot',jsonb_build_object(
            'at_utc',v_normalised_query#>>'{snapshot,at_utc}',
            'revision',v_normalised_query#>>'{snapshot,revision}'
          )
        )
      );
      v_selection_hash := private._invoice_batch_hash_v2(v_selection);
      v_delivery_hash := private._invoice_batch_hash_v2(v_delivery_intent);

      v_idempotency_key := CASE WHEN v_action = 'GENERATE' THEN
        private._invoice_batch_hash_v2(jsonb_build_object(
          'command_type',
          'GENERATE_SELECTED',
          'query_hash',v_query_hash,
          'selection_hash',v_selection_hash,
          'allow_early',v_allow_early,
          'command_token',v_command_token
        ))
      ELSE
        private._invoice_batch_hash_v2(jsonb_build_object(
          'command_type',
          'ISSUE_INVOICES',
          'query_hash',v_query_hash,
          'selection_hash',v_selection_hash,
          'allow_early',v_allow_early,
          'deliver',v_deliver,
          'command_token',v_command_token,
          'delivery_request_token',v_delivery_request_token,
          'delivery_hash',v_delivery_hash
        ))
      END;

      PERFORM pg_advisory_xact_lock(hashtextextended('INVOICE_BATCH_SELECTION_ROOT|' || v_idempotency_key, 0));

      SELECT o.id, o.status, o.phase, o.change_seq
        INTO v_operation_id, v_operation_status, v_operation_phase, v_change_seq
      FROM public.invoice_operations o
      WHERE o.idempotency_key = v_idempotency_key
        AND o.status IN ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED','COMPLETE')
      ORDER BY (o.status = 'COMPLETE') DESC, o.created_at_utc DESC
      LIMIT 1;

      v_created := false;
      v_reused := v_operation_id IS NOT NULL;

      IF v_operation_id IS NULL THEN
        INSERT INTO public.invoice_operations(
          operation_type,
          entity_type,
          entity_id,
          actor_user_id,
          idempotency_key,
          status,
          phase,
          priority,
          source_revision,
          template_version,
          input_json,
          config_json,
          progress_json,
          total_units,
          completed_units,
          failed_units,
          chunk_count,
          control_version,
          change_seq,
          manifest_generation,
          manifest_committed,
          release_complete,
          result_page_revision,
          created_at_utc,
          updated_at_utc
        ) VALUES (
          v_operation_type,
          'INVOICE_BATCH',
          NULL,
          p_actor_user_id,
          v_idempotency_key,
          'QUEUED',
          'BUILD_MANIFEST',
          v_priority,
          v_normalised_query#>>'{snapshot,revision}',
          NULL,
          jsonb_build_object(
            'contract_version', 'INVOICE_BATCH_SELECTION_ROOT_V2',
            'command_type', v_command_type,
            'action', v_action,
            'selection_expansion_pending', true,
            'filter_hash', v_filter_hash,
            'query_hash', v_query_hash,
            'selection_hash', v_selection_hash,
            'snapshot',v_normalised_query->'snapshot',
            'command_token', v_command_token,
            'deliver', v_deliver,
            'delivery_request_token', v_delivery_request_token,
            'delivery_intent', v_delivery_intent,
            'selection_contract', jsonb_build_object(
              'contract_version', 'INVOICE_BATCH_SELECTION_ROOT_V2',
              'action', v_action,
              'query', v_normalised_query,
              'selection', v_selection
            )
          ),
          jsonb_build_object(
            'command_type', v_command_type,
            'processor_policy', private._invoice_processor_limits()
          ),
          jsonb_build_object(
            'contract_version','INVOICE_BATCH_PROGRESS_V2',
            'status_message', 'Building selection manifest',
            'selection_expansion_pending', true,
            'manifest_committed',false,
            'candidate_total', case when v_action='GENERATE' then v_filtered_total else 0 end,
            'invoice_total', case when v_action='ISSUE' then v_filtered_total else 0 end,
            'selected_total', v_selected_total,
            'expanded_total', 0,
            'queued_total', 0,
            'generated_total',0,
            'regenerated_total',0,
            'issued_total',0,
            'issued_send_blocked_total',0,
            'already_active_total',0,
            'blocked_total', 0,
            'changed_total', 0,
            'failed_total', 0,
            'in_progress_total',0,
            'delivery_pending_total',0,
            'delivery_complete_total',0,
            'delivery_blocked_total',0,
            'total_units', 1,
            'completed_units', 0,
            'failed_units', 0
          ),
          1,
          0,
          0,
          1,
          1,
          nextval('public.invoice_operation_change_seq'),
          1,
          false,
          false,
          0,
          v_now,
          v_now
        )
        RETURNING id, status, phase, change_seq
          INTO v_operation_id, v_operation_status, v_operation_phase, v_change_seq;

        v_created := true;
        v_reused := false;
      END IF;

      IF v_operation_status IS DISTINCT FROM 'COMPLETE' THEN
        INSERT INTO public.invoice_operation_chunks(
          operation_id,
          chunk_type,
          phase,
          sequence_no,
          level_no,
          work_key,
          entity_type,
          entity_id,
          status,
          priority,
          run_after_utc,
          payload_json,
          progress_json,
          operation_control_version,
          manifest_generation,
          is_manifest_member,
          manifest_committed,
          result_visible,
          created_at_utc,
          updated_at_utc
        ) VALUES (
          v_operation_id,
          v_chunk_type,
          'BUILD_MANIFEST',
          0,
          0,
          private._invoice_batch_hash_v2(jsonb_build_object(
            'work','BUILD_MANIFEST',
            'root',v_operation_id,
            'manifest_generation',1,
            'action',v_action
          )),
          'OPERATION',
          v_operation_id,
          'QUEUED',
          v_priority,
          v_now,
          jsonb_build_object(
            'is_selection_expander', true,
            'selection_key', NULL,
            'action', v_action,
            'filter_hash', v_filter_hash,
            'query_hash',v_query_hash,
            'selection_hash', v_selection_hash,
            'manifest_generation',1,
            'manifest_committed',false,
            'selection_contract', jsonb_build_object(
              'contract_version', 'INVOICE_BATCH_SELECTION_ROOT_V2',
              'action', v_action,
              'query', v_normalised_query,
              'selection', v_selection
            ),
            'query', v_normalised_query,
            'cursor', jsonb_build_object(),
            'deliver', v_deliver,
            'delivery_request_token', v_delivery_request_token,
            'delivery_intent', v_delivery_intent,
            'scanned', 0,
            'selected', 0,
            'queued', 0,
            'blocked', 0,
            'changed', 0,
            'already_active', 0,
            'completed', false,
            'release_cursor',null
          ),
          jsonb_build_object(
            'contract_version','INVOICE_BATCH_PROGRESS_V2',
            'status_message', 'Building selection manifest'
          ),
          1,
          1,
          false,
          false,
          false,
          v_now,
          v_now
        )
        ON CONFLICT DO NOTHING;

        UPDATE public.invoice_operations o
        SET total_units = greatest(o.total_units, 1),
            chunk_count = greatest(o.chunk_count, 1),
            progress_json = coalesce(o.progress_json, '{}'::jsonb)
              || jsonb_build_object(
                'contract_version','INVOICE_BATCH_PROGRESS_V2',
                'status_message', 'Building selection manifest',
                'selection_expansion_pending', true,
                'manifest_committed',false,
                'total_units', greatest(o.total_units, 1)
              ),
            updated_at_utc = v_now,
            change_seq = CASE
              WHEN v_created THEN o.change_seq
              ELSE nextval('public.invoice_operation_change_seq')
            END
        WHERE o.id = v_operation_id
          AND o.status <> 'COMPLETE'
        RETURNING o.status, o.phase, o.change_seq
          INTO v_operation_status, v_operation_phase, v_change_seq;

        IF NOT FOUND THEN
          SELECT o.status, o.phase, o.change_seq
            INTO v_operation_status, v_operation_phase, v_change_seq
          FROM public.invoice_operations o
          WHERE o.id = v_operation_id;
        END IF;
      END IF;

      v_result_item := jsonb_build_object(
        'command_no', v_command_no,
        'command_type', v_command_type,
        'accepted', true,
        'operation_id', v_operation_id,
        'operation_type', v_operation_type,
        'status', v_operation_status,
        'phase', v_operation_phase,
        'source_revision', v_filter_hash,
        'change_seq', v_change_seq,
        'created', v_created,
        'reused_active', v_reused AND v_operation_status <> 'COMPLETE',
        'reused_ready', v_reused AND v_operation_status = 'COMPLETE',
        'priority_raised', false,
        'blocked', v_operation_status = 'BLOCKED',
        'terminal_error', NULL,
        'chunk_count', 1,
        'selection_expansion_pending', v_operation_status IS DISTINCT FROM 'COMPLETE',
        'selection_contract_version', 'INVOICE_BATCH_SELECTION_V2',
        'estimated_filtered_total', v_filtered_total,
        'estimated_selected_total',v_selected_total,
        'nudge_state', CASE WHEN v_operation_status = 'COMPLETE' THEN 'REUSED_COMPLETE' ELSE 'DB_QUEUE' END
      );
    END IF;
    END IF;

    IF v_error_code IS NOT NULL THEN
      v_result_item := jsonb_build_object(
        'command_no', v_command_no,
        'command_type', v_command_type,
        'accepted', false,
        'created', false,
        'reused_active', false,
        'reused_ready', false,
        'priority_raised', false,
        'blocked', false,
        'selection_expansion_pending', false,
        'terminal_error', jsonb_build_object('code', v_error_code, 'detail', v_error_detail),
        'error', jsonb_build_object('code', v_error_code, 'detail', v_error_detail)
      );
    END IF;

    v_results := v_results || jsonb_build_array(v_result_item);
  END LOOP;

  RETURN v_results;
END;
$function$;

REVOKE ALL ON FUNCTION public.invoice_operation_start_batch(jsonb, uuid, timestamp with time zone) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.invoice_operation_start_batch(jsonb, uuid, timestamp with time zone) TO authenticated, service_role;
