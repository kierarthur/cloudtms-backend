-- Canonical correction-carrier contract and durable deferred carry runtime.
-- All functions in this file are pre-draft only. They do not alter Banking Pay
-- arithmetic and they never read live authority for a frozen pay batch.

CREATE OR REPLACE FUNCTION public._ctms_correction_carrier_identity_v1(
  p_candidate_id uuid,
  p_correction_root_id uuid,
  p_component_key_type text,
  p_component_key_value text
)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
SECURITY INVOKER
SET search_path TO pg_catalog
AS $function$
DECLARE
  v_key_type text := upper(btrim(p_component_key_type));
  v_key_value text := btrim(p_component_key_value);
BEGIN
  IF v_key_type <> 'TS_DAY' THEN
    RAISE EXCEPTION 'CORRECTION_CARRIER_COMPONENT_KEY_TYPE_UNSUPPORTED'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CORRECTION_CARRIER_COMPONENT_KEY_TYPE_UNSUPPORTED',
              'component_key_type', v_key_type
            )::text;
  END IF;

  IF v_key_value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
     OR (v_key_value::date)::text <> v_key_value THEN
    RAISE EXCEPTION 'CORRECTION_CARRIER_TS_DAY_KEY_INVALID'
      USING ERRCODE = '22007',
            DETAIL = jsonb_build_object(
              'code', 'CORRECTION_CARRIER_TS_DAY_KEY_INVALID',
              'component_key_value', v_key_value
            )::text;
  END IF;

  RETURN concat_ws(
    '|',
    'CORRECTION_CHAIN_V1',
    p_candidate_id::text,
    p_correction_root_id::text,
    v_key_type,
    v_key_value
  );
END;
$function$;

ALTER FUNCTION public._ctms_correction_carrier_identity_v1(
  uuid,
  uuid,
  text,
  text
) OWNER TO postgres;

REVOKE ALL ON FUNCTION public._ctms_correction_carrier_identity_v1(
  uuid,
  uuid,
  text,
  text
) FROM PUBLIC, anon, authenticated, service_role;


CREATE OR REPLACE FUNCTION public._pay_workbench_candidate_projection_contract()
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
SECURITY INVOKER
SET search_path TO pg_catalog
AS $function$
BEGIN
  RETURN jsonb_build_object(
    'projection_version', 4,
    'hidden_recovery_template_projection_version', 1,
    'requires_hidden_recovery_templates', true,
    'canonical_correction_carrier_version',
      'BANKING_PAY_CANONICAL_CORRECTION_CARRIER_V1'
  );
END;
$function$;

ALTER FUNCTION public._pay_workbench_candidate_projection_contract()
  OWNER TO postgres;

REVOKE ALL
ON FUNCTION public._pay_workbench_candidate_projection_contract()
FROM PUBLIC, anon, authenticated, service_role;


CREATE OR REPLACE FUNCTION public.pay_workbench_contract_version_get_v1()
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_projection_contract jsonb :=
    public._pay_workbench_candidate_projection_contract();
  v_selection_carry_table_oid oid :=
    to_regclass(
      'public.banking_pay_workbench_selection_carry_registrations'
    );
  v_canonical_contract_version text;
BEGIN
  v_canonical_contract_version := CASE
    WHEN v_selection_carry_table_oid IS NOT NULL
      AND to_regprocedure(
        'public.pay_workbench_session_carry_forward_preview_selections_v1(uuid,uuid,jsonb)'
      ) IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger AS trigger_row
        WHERE trigger_row.tgrelid =
          'public.banking_pay_workbench_preview_rows'::regclass
          AND trigger_row.tgname =
            'trg_banking_pay_preview_selection_carry_apply'
          AND trigger_row.tgenabled <> 'D'
          AND trigger_row.tgisinternal IS FALSE
      )
      THEN v_projection_contract ->> 'canonical_correction_carrier_version'
    ELSE 'BANKING_PAY_CANONICAL_CORRECTION_CARRIER_INCOMPLETE'
  END;

  v_projection_contract := jsonb_set(
    v_projection_contract,
    '{canonical_correction_carrier_version}',
    to_jsonb(v_canonical_contract_version),
    true
  );

  RETURN jsonb_build_object(
    'ok', true,
    'contract_version', 'BANKING_PAY_WORKBENCH_DB_V1',
    'canonical_correction_carrier_version',
      v_canonical_contract_version,
    'candidate_projection_contract', v_projection_contract
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_contract_version_get_v1()
  OWNER TO postgres;

REVOKE ALL
ON FUNCTION public.pay_workbench_contract_version_get_v1()
FROM PUBLIC, anon, authenticated;

GRANT EXECUTE
ON FUNCTION public.pay_workbench_contract_version_get_v1()
TO service_role;


CREATE OR REPLACE FUNCTION public.import_review_contract_version_get_v1()
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_projection_contract jsonb :=
    public._pay_workbench_candidate_projection_contract();
  v_selection_carry_table_oid oid :=
    to_regclass(
      'public.banking_pay_workbench_selection_carry_registrations'
    );
  v_canonical_contract_version text;
BEGIN
  v_canonical_contract_version := CASE
    WHEN v_selection_carry_table_oid IS NOT NULL
      AND to_regprocedure(
        'public.pay_workbench_session_carry_forward_preview_selections_v1(uuid,uuid,jsonb)'
      ) IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger AS trigger_row
        WHERE trigger_row.tgrelid =
          'public.banking_pay_workbench_preview_rows'::regclass
          AND trigger_row.tgname =
            'trg_banking_pay_preview_selection_carry_apply'
          AND trigger_row.tgenabled <> 'D'
          AND trigger_row.tgisinternal IS FALSE
      )
      THEN v_projection_contract ->> 'canonical_correction_carrier_version'
    ELSE 'BANKING_PAY_CANONICAL_CORRECTION_CARRIER_INCOMPLETE'
  END;

  RETURN jsonb_build_object(
    'ok', true,
    'schema_contract_version', 'IMPORT_REVIEW_DB_V1',
    'apply_envelope_version', 'IMPORT_REVIEW_APPLY_V1',
    'apply_operation_version', 'IMPORT_APPLY_OPERATION_V2',
    'correction_operation_version', 'IMPORT_CORRECTION_OPERATION_V2',
    'follow_up_component_version', 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_V1',
    'tsfin_follow_up_settlement_version', 'IMPORT_REVIEW_TSFIN_SETTLEMENT_V1',
    'incremental_apply_version', 'IMPORT_REVIEW_INCREMENTAL_APPLY_V1',
    'review_ui_contract_version', 'IMPORT_REVIEW_UI_V6',
    'email_grouping_version', 'TIMESHEET_QUERY_RECIPIENT_EMAIL_V1',
    'canonical_correction_carrier_version',
      v_canonical_contract_version,
    'legacy_contracts_supported', false
  );
END;
$function$;

ALTER FUNCTION public.import_review_contract_version_get_v1()
  OWNER TO postgres;

REVOKE ALL
ON FUNCTION public.import_review_contract_version_get_v1()
FROM PUBLIC, anon, authenticated;

GRANT EXECUTE
ON FUNCTION public.import_review_contract_version_get_v1()
TO service_role;


CREATE OR REPLACE FUNCTION public.pay_workbench_session_carry_forward_case_resolutions_v1(
  p_source_session_id uuid,
  p_target_session_id uuid,
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'extensions', 'pg_catalog', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_options jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_options_json, '{}'::jsonb)) = 'object'
      THEN COALESCE(p_options_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_source record;
  v_target record;
  v_resolution record;
  v_actor_user_id uuid;
  v_carry_reason text := left(
    upper(
      coalesce(
        nullif(
          btrim(
            coalesce(
              v_options ->> 'carry_forward_reason',
              v_options ->> 'carryForwardReason',
              ''
            )
          ),
          ''
        ),
        'SESSION_REPLACEMENT'
      )
    ),
    128
  );
  v_allow_same_pay_date boolean :=
    lower(
      btrim(
        coalesce(
          v_options ->> 'allow_same_pay_date_duplicate',
          v_options ->> 'allowSamePayDateDuplicate',
          'false'
        )
      )
    ) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_source_priority integer := CASE
    WHEN coalesce(
      v_options ->> 'source_priority',
      v_options ->> 'sourcePriority',
      ''
    ) ~ '^-?[0-9]{1,7}$'
      THEN least(
        greatest(
          coalesce(
            v_options ->> 'source_priority',
            v_options ->> 'sourcePriority'
          )::integer,
          -1000000
        ),
        1000000
      )
    ELSE 0
  END;
  v_residuals jsonb;
  v_residual jsonb;
  v_component jsonb;
  v_canonical_key text;
  v_economic_fingerprint text;
  v_scope_kind text;
  v_registered_count integer := 0;
  v_incompatible_count integer := 0;
  v_existing_count integer := 0;
  v_registration_id uuid;
BEGIN
  IF p_source_session_id IS NULL
     OR p_target_session_id IS NULL
     OR p_source_session_id = p_target_session_id THEN
    RAISE EXCEPTION 'WORKBENCH_CASE_RESOLUTION_CARRY_SESSION_INVALID'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_CASE_RESOLUTION_CARRY_SESSION_INVALID',
              'source_session_id', p_source_session_id,
              'target_session_id', p_target_session_id
            )::text;
  END IF;

  PERFORM 1
  FROM public.banking_pay_workbench_sessions session_lock
  WHERE session_lock.id IN (p_source_session_id, p_target_session_id)
  ORDER BY session_lock.id
  FOR UPDATE;

  SELECT *
  INTO v_source
  FROM public.banking_pay_workbench_sessions
  WHERE id = p_source_session_id;

  SELECT *
  INTO v_target
  FROM public.banking_pay_workbench_sessions
  WHERE id = p_target_session_id;

  IF v_source.id IS NULL OR v_target.id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CASE_RESOLUTION_CARRY_SESSION_NOT_FOUND'
      USING ERRCODE = 'P0002';
  END IF;

  IF v_target.status <> 'OPEN'
     OR v_target.discarded_at_utc IS NOT NULL
     OR v_target.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CASE_RESOLUTION_CARRY_TARGET_OBSOLETE'
      USING ERRCODE = '55000';
  END IF;

  IF v_source.week_ending_cutoff IS DISTINCT FROM v_target.week_ending_cutoff
     OR v_source.pay_date > v_target.pay_date
     OR (
       v_source.pay_date = v_target.pay_date
       AND v_allow_same_pay_date IS NOT TRUE
     ) THEN
    RAISE EXCEPTION 'WORKBENCH_CASE_RESOLUTION_CARRY_SCOPE_MISMATCH'
      USING ERRCODE = '55000';
  END IF;

  IF NOT (
    (
      v_source.status = 'OPEN'
      AND v_source.discarded_at_utc IS NULL
      AND v_source.replacement_session_id IS NULL
    )
    OR (
      v_source.status = 'DISCARDED'
      AND v_source.discarded_at_utc IS NOT NULL
      AND v_source.replacement_session_id = p_target_session_id
    )
  ) THEN
    RAISE EXCEPTION 'WORKBENCH_CASE_RESOLUTION_CARRY_SOURCE_LIFECYCLE_INVALID'
      USING ERRCODE = '55000';
  END IF;

  v_actor_user_id := COALESCE(
    CASE
      WHEN coalesce(
        v_options ->> 'actor_user_id',
        v_options ->> 'actorUserId',
        ''
      ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN coalesce(
          v_options ->> 'actor_user_id',
          v_options ->> 'actorUserId'
        )::uuid
    END,
    v_target.actor_user_id
  );

  IF v_actor_user_id IS DISTINCT FROM v_target.actor_user_id THEN
    RAISE EXCEPTION 'WORKBENCH_CASE_RESOLUTION_CARRY_ACTOR_MISMATCH'
      USING ERRCODE = '42501';
  END IF;

  FOR v_resolution IN
    SELECT resolution_row.*
    FROM public.banking_pay_workbench_session_case_resolutions resolution_row
    WHERE resolution_row.session_id = p_source_session_id
    ORDER BY resolution_row.candidate_id,
             resolution_row.updated_at_utc,
             resolution_row.id
  LOOP
    v_canonical_key := NULL;
    v_economic_fingerprint := NULL;
    v_scope_kind := 'NON_CORRECTION';
    v_residual := NULL;
    v_component := NULL;
    v_registration_id := NULL;

    IF coalesce(v_resolution.source_family_key, '')
         LIKE 'correction-chain:%'
       OR coalesce(
         v_resolution.payload_json ->> 'correction_identity_version',
         ''
       ) = 'CORRECTION_CHAIN_V1'
       OR nullif(
         v_resolution.payload_json ->> 'correction_chain_fingerprint',
         ''
       ) IS NOT NULL THEN
      v_scope_kind := 'CORRECTION_COMPONENT';
      v_residuals :=
        public._ctms_candidate_correction_residuals_v1(
          p_source_session_id,
          v_resolution.candidate_id,
          NULL::uuid,
          'PAY_WORKBENCH_CARRY_REGISTER'
        );

      SELECT residual.value
      INTO v_residual
      FROM jsonb_array_elements(
        coalesce(v_residuals, '[]'::jsonb)
      ) residual(value)
      WHERE residual.value ->> 'source_family_key'
        = v_resolution.source_family_key
      LIMIT 1;

      SELECT component.value
      INTO v_component
      FROM jsonb_array_elements(
        coalesce(v_residual -> 'components', '[]'::jsonb)
      ) component(value)
      WHERE upper(component.value ->> 'component_key_type')
          = upper(v_resolution.component_key_type)
        AND component.value ->> 'component_key_value'
          = v_resolution.component_key_value
      LIMIT 1;

      v_canonical_key :=
        public._ctms_correction_carrier_identity_v1(
          v_resolution.candidate_id,
          nullif(v_residual ->> 'root_timesheet_id', '')::uuid,
          v_component ->> 'component_key_type',
          v_component ->> 'component_key_value'
        );
      v_economic_fingerprint :=
        nullif(v_component ->> 'resolution_economic_fingerprint', '');
    ELSE
      v_canonical_key :=
        nullif(btrim(v_resolution.resolution_identity_key), '');
      IF v_canonical_key IS NOT NULL THEN
        v_economic_fingerprint := encode(
          extensions.digest(
            convert_to(
              jsonb_build_object(
                'resolution_identity_key', v_canonical_key,
                'source_basis_fingerprint',
                  v_resolution.source_basis_fingerprint,
                'source_family_key', v_resolution.source_family_key,
                'component_key_type', v_resolution.component_key_type,
                'component_key_value', v_resolution.component_key_value,
                'payload_json', v_resolution.payload_json
              )::text,
              'UTF8'
            ),
            'sha256'
          ),
          'hex'
        );
      END IF;
    END IF;

    IF v_canonical_key IS NULL OR v_economic_fingerprint IS NULL THEN
      v_canonical_key := 'INCOMPATIBLE|' || v_resolution.id::text;
      v_economic_fingerprint := encode(
        extensions.digest(
          convert_to(
            jsonb_build_object(
              'source_resolution', to_jsonb(v_resolution),
              'correction_residual', v_residual,
              'correction_component', v_component,
              'incompatibility_code',
                'STABLE_IDENTITY_OR_ECONOMIC_FINGERPRINT_UNAVAILABLE'
            )::text,
            'UTF8'
          ),
          'sha256'
        ),
        'hex'
      );
    END IF;

    INSERT INTO
      public.banking_pay_workbench_case_resolution_carry_registrations (
        target_session_id,
        source_session_id,
        candidate_id,
        source_resolution_id,
        source_resolution_identity_key,
        canonical_resolution_key,
        resolution_scope_kind,
        source_economic_fingerprint,
        source_resolution_snapshot_json,
        source_priority,
        carry_reason,
        status,
        state_reason_code,
        created_at_utc,
        updated_at_utc,
        completed_at_utc
      )
    VALUES (
      p_target_session_id,
      p_source_session_id,
      v_resolution.candidate_id,
      v_resolution.id,
      v_resolution.resolution_identity_key,
      v_canonical_key,
      v_scope_kind,
      v_economic_fingerprint,
      jsonb_strip_nulls(
        jsonb_build_object(
          'resolution', to_jsonb(v_resolution),
          'correction_residual', v_residual,
          'correction_component', v_component,
          'registered_at_utc', v_now,
          'policy_x_authority_scope',
            'PRE_DRAFT_CASE_RESOLUTION_STATE_ONLY'
        )
      ),
      v_source_priority,
      v_carry_reason,
      CASE
        WHEN v_canonical_key LIKE 'INCOMPATIBLE|%'
          THEN 'INCOMPATIBLE'
        ELSE 'PENDING'
      END,
      CASE
        WHEN v_canonical_key LIKE 'INCOMPATIBLE|%'
          THEN 'STABLE_IDENTITY_UNAVAILABLE'
      END,
      v_now,
      v_now,
      CASE
        WHEN v_canonical_key LIKE 'INCOMPATIBLE|%'
          THEN v_now
      END
    )
    ON CONFLICT (target_session_id, source_resolution_id)
    DO NOTHING
    RETURNING id
    INTO v_registration_id;

    IF v_registration_id IS NULL THEN
      v_existing_count := v_existing_count + 1;
    ELSIF v_canonical_key LIKE 'INCOMPATIBLE|%' THEN
      v_incompatible_count := v_incompatible_count + 1;
    ELSE
      v_registered_count := v_registered_count + 1;
    END IF;

    IF v_registration_id IS NOT NULL THEN
      INSERT INTO public.audit_events (
        actor_user_id,
        object_type,
        object_id_text,
        action,
        before_json,
        after_json,
        reason
      )
      VALUES (
        v_actor_user_id,
        'BANKING_PAY_CASE_RESOLUTION_CARRY',
        v_registration_id::text,
        CASE
          WHEN v_canonical_key LIKE 'INCOMPATIBLE|%'
            THEN 'CARRY_REGISTRATION_INCOMPATIBLE'
          ELSE 'CARRY_REGISTERED'
        END,
        NULL,
        jsonb_build_object(
          'registration_id', v_registration_id,
          'source_session_id', p_source_session_id,
          'target_session_id', p_target_session_id,
          'source_resolution_id', v_resolution.id,
          'candidate_id', v_resolution.candidate_id,
          'canonical_resolution_key', v_canonical_key,
          'source_economic_fingerprint', v_economic_fingerprint,
          'status', CASE
            WHEN v_canonical_key LIKE 'INCOMPATIBLE|%'
              THEN 'INCOMPATIBLE'
            ELSE 'PENDING'
          END
        ),
        v_carry_reason
      );
    END IF;
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'source_session_id', p_source_session_id,
    'target_session_id', p_target_session_id,
    'total_resolution_count',
      v_registered_count + v_incompatible_count + v_existing_count,
    'valid_resolution_count', 0,
    'stale_resolution_count', 0,
    'saved_resolution_count', 0,
    'preview_row_count', 0,
    'pending_registration_count', v_registered_count,
    'incompatible_registration_count', v_incompatible_count,
    'existing_registration_count', v_existing_count,
    'state_changed', v_registered_count > 0 OR v_incompatible_count > 0,
    'carry_state', CASE
      WHEN v_registered_count > 0 THEN 'DEFERRED'
      ELSE 'NOT_REQUIRED'
    END,
    'policy_x_authority_scope',
      'PRE_DRAFT_CASE_RESOLUTION_STATE_ONLY'
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_session_carry_forward_case_resolutions_v1(
  uuid,
  uuid,
  jsonb
) OWNER TO postgres;

REVOKE ALL
ON FUNCTION public.pay_workbench_session_carry_forward_case_resolutions_v1(
  uuid,
  uuid,
  jsonb
)
FROM PUBLIC, anon, authenticated, service_role;


CREATE OR REPLACE FUNCTION
  public._pay_workbench_case_resolution_carry_process_candidate_v1(
    p_target_session_id uuid,
    p_candidate_id uuid,
    p_target_source_build_run_id uuid,
    p_now_utc timestamptz DEFAULT now()
  )
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'extensions', 'pg_catalog', 'pg_temp'
AS $function$
DECLARE
  v_registration record;
  v_snapshot jsonb;
  v_source_resolution jsonb;
  v_residuals jsonb;
  v_residual jsonb;
  v_component jsonb;
  v_target_fingerprint text;
  v_target_resolution_id uuid;
  v_existing record;
  v_status text;
  v_reason_code text;
  v_carried integer := 0;
  v_stale integer := 0;
  v_incompatible integer := 0;
  v_superseded integer := 0;
  v_deferred integer := 0;
  v_actor_user_id uuid;
  v_target_pay_date date;
BEGIN
  IF p_target_session_id IS NULL OR p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CASE_RESOLUTION_CARRY_TARGET_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  SELECT actor_user_id, pay_date
  INTO v_actor_user_id, v_target_pay_date
  FROM public.banking_pay_workbench_sessions
  WHERE id = p_target_session_id
    AND status = 'OPEN'
    AND discarded_at_utc IS NULL
    AND replacement_session_id IS NULL;

  IF v_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CASE_RESOLUTION_CARRY_TARGET_OBSOLETE'
      USING ERRCODE = '55000';
  END IF;

  FOR v_registration IN
    SELECT registration_row.*
    FROM
      public.banking_pay_workbench_case_resolution_carry_registrations
        registration_row
    WHERE registration_row.target_session_id = p_target_session_id
      AND registration_row.candidate_id = p_candidate_id
      AND registration_row.status = 'PENDING'
    ORDER BY registration_row.source_priority,
             registration_row.created_at_utc,
             registration_row.id
    FOR UPDATE
  LOOP
    v_snapshot := v_registration.source_resolution_snapshot_json;
    v_source_resolution := v_snapshot -> 'resolution';
    v_status := NULL;
    v_reason_code := NULL;
    v_target_fingerprint := NULL;
    v_target_resolution_id := NULL;
    v_residual := NULL;
    v_component := NULL;

    IF v_registration.resolution_scope_kind = 'CORRECTION_COMPONENT' THEN
      v_residuals :=
        public._ctms_candidate_correction_residuals_v1(
          p_target_session_id,
          p_candidate_id,
          NULL::uuid,
          'PAY_WORKBENCH_CARRY_PROCESS'
        );

      SELECT residual.value, component.value
      INTO v_residual, v_component
      FROM jsonb_array_elements(
        coalesce(v_residuals, '[]'::jsonb)
      ) residual(value)
      CROSS JOIN LATERAL jsonb_array_elements(
        coalesce(residual.value -> 'components', '[]'::jsonb)
      ) component(value)
      WHERE component.value ->> 'canonical_correction_key'
        = v_registration.canonical_resolution_key
      LIMIT 1;

      IF v_component IS NULL THEN
        UPDATE
          public.banking_pay_workbench_case_resolution_carry_registrations
        SET attempt_count = attempt_count + 1,
            last_attempt_at_utc = coalesce(p_now_utc, now()),
            last_error_json = jsonb_build_object(
              'code', 'TARGET_EVIDENCE_NOT_READY',
              'target_source_build_run_id', p_target_source_build_run_id
            ),
            updated_at_utc = coalesce(p_now_utc, now())
        WHERE id = v_registration.id;
        v_deferred := v_deferred + 1;
        CONTINUE;
      END IF;

      v_target_fingerprint :=
        nullif(v_component ->> 'resolution_economic_fingerprint', '');

      IF v_target_fingerprint IS NULL THEN
        v_status := 'INCOMPATIBLE';
        v_reason_code := 'TARGET_ECONOMIC_FINGERPRINT_MISSING';
      ELSIF v_target_fingerprint
        IS DISTINCT FROM v_registration.source_economic_fingerprint THEN
        v_status := 'STALE';
        v_reason_code := CASE
          WHEN v_component ->> 'component_lineage_fingerprint'
            IS DISTINCT FROM
              v_snapshot #>>
                '{correction_component,component_lineage_fingerprint}'
            THEN 'MEMBER_SET_CHANGED'
          WHEN v_component ->> 'source_basis_fingerprint'
            IS DISTINCT FROM
              v_snapshot #>>
                '{correction_component,source_basis_fingerprint}'
            THEN 'SOURCE_BASIS_CHANGED'
          WHEN v_residual
              ->> 'correction_financials_policy_envelope_fingerprint'
            IS DISTINCT FROM
              v_snapshot #>>
                '{correction_residual,correction_financials_policy_envelope_fingerprint}'
            THEN 'POLICY_ENVELOPE_CHANGED'
          WHEN v_residual ->> 'target_pay_method'
            IS DISTINCT FROM
              v_snapshot #>> '{correction_residual,target_pay_method}'
            THEN 'TARGET_PAY_METHOD_CHANGED'
          WHEN v_residual ->> 'residual_fingerprint'
            IS DISTINCT FROM
              v_snapshot #>> '{correction_residual,residual_fingerprint}'
            THEN 'RESIDUAL_FINGERPRINT_CHANGED'
          ELSE 'RATE_OR_AMOUNT_CHANGED'
        END;
      ELSE
        SELECT target_resolution.id,
               target_resolution.payload_json,
               target_resolution.updated_at_utc
        INTO v_existing
        FROM public.banking_pay_workbench_session_case_resolutions
          target_resolution
        WHERE target_resolution.session_id = p_target_session_id
          AND target_resolution.resolution_identity_key =
            v_registration.canonical_resolution_key
        ORDER BY target_resolution.updated_at_utc DESC,
                 target_resolution.id DESC
        LIMIT 1
        FOR UPDATE;

        IF v_existing.id IS NOT NULL
           AND coalesce(
             v_existing.payload_json ->> 'carry_registration_id',
             ''
           ) <> v_registration.id::text THEN
          v_status := 'SUPERSEDED';
          v_reason_code := 'TARGET_AUTHORITATIVE_DECISION_EXISTS';
          v_target_resolution_id := v_existing.id;
        ELSE
          INSERT INTO
            public.banking_pay_workbench_session_case_resolutions (
              id,
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
              resolution_origin_session_id,
              resolution_origin_pay_date,
              resolution_origin_source_basis_fingerprint,
              created_at_utc,
              updated_at_utc
            )
          VALUES (
            coalesce(v_existing.id, gen_random_uuid()),
            p_target_session_id,
            p_candidate_id,
            coalesce(
              v_source_resolution ->> 'case_key',
              'timesheet:' ||
                coalesce(v_component ->> 'carrier_timesheet_id', '')
            ),
            coalesce(
              v_source_resolution ->> 'resolution_family',
              'BUCKETED'
            ),
            v_registration.canonical_resolution_key,
            nullif(v_component ->> 'carrier_timesheet_id', '')::uuid,
            v_component ->> 'source_basis_fingerprint',
            v_residual ->> 'source_family_key',
            v_source_resolution ->> 'bucket_code',
            upper(v_component ->> 'component_key_type'),
            v_component ->> 'component_key_value',
            coalesce(v_source_resolution -> 'payload_json', '{}'::jsonb)
              || jsonb_build_object(
                'resolution_identity_key',
                  v_registration.canonical_resolution_key,
                'resolution_identity_version', 'CORRECTION_CHAIN_V1',
                'canonical_correction_key',
                  v_registration.canonical_resolution_key,
                'resolution_economic_fingerprint',
                  v_target_fingerprint,
                'source_resolution_id',
                  v_registration.source_resolution_id,
                'source_resolution_identity_key',
                  v_registration.source_resolution_identity_key,
                'carry_registration_id', v_registration.id,
                'resolution_origin_session_id',
                  v_registration.source_session_id,
                'resolution_origin_pay_date',
                  v_source_resolution ->> 'resolution_origin_pay_date',
                'resolution_origin_source_basis_fingerprint',
                  coalesce(
                    v_source_resolution
                      ->> 'resolution_origin_source_basis_fingerprint',
                    v_source_resolution ->> 'source_basis_fingerprint'
                  ),
                'clone_carried_forward', true,
                'clone_validation_status', 'VALID',
                'requires_review', false,
                'target_session_id', p_target_session_id,
                'target_source_build_run_id',
                  p_target_source_build_run_id,
                'carried_forward_at_utc',
                  coalesce(p_now_utc, now()),
                'correction_chain_fingerprint',
                  v_residual ->> 'chain_fingerprint',
                'correction_chain_residual_fingerprint',
                  v_residual ->> 'residual_fingerprint',
                'correction_financials_policy_envelope',
                  v_residual
                    -> 'correction_financials_policy_envelope',
                'correction_financials_policy_envelope_fingerprint',
                  v_residual
                    ->> 'correction_financials_policy_envelope_fingerprint',
                'policy_x_authority_scope',
                  'PRE_DRAFT_CASE_RESOLUTION_STATE_ONLY'
              ),
            coalesce(
              nullif(
                v_source_resolution ->> 'resolution_origin_session_id',
                ''
              )::uuid,
              v_registration.source_session_id
            ),
            coalesce(
              nullif(
                v_source_resolution ->> 'resolution_origin_pay_date',
                ''
              )::date,
              v_target_pay_date
            ),
            coalesce(
              v_source_resolution
                ->> 'resolution_origin_source_basis_fingerprint',
              v_source_resolution ->> 'source_basis_fingerprint'
            ),
            coalesce(p_now_utc, now()),
            coalesce(p_now_utc, now())
          )
          ON CONFLICT (session_id, resolution_identity_key)
          DO UPDATE
          SET candidate_id = EXCLUDED.candidate_id,
              case_key = EXCLUDED.case_key,
              resolution_family = EXCLUDED.resolution_family,
              timesheet_id = EXCLUDED.timesheet_id,
              source_basis_fingerprint =
                EXCLUDED.source_basis_fingerprint,
              source_family_key = EXCLUDED.source_family_key,
              bucket_code = EXCLUDED.bucket_code,
              component_key_type = EXCLUDED.component_key_type,
              component_key_value = EXCLUDED.component_key_value,
              payload_json = EXCLUDED.payload_json,
              updated_at_utc = EXCLUDED.updated_at_utc
          WHERE
            public.banking_pay_workbench_session_case_resolutions
              .payload_json ->> 'carry_registration_id'
              = v_registration.id::text
          RETURNING id
          INTO v_target_resolution_id;

          IF v_target_resolution_id IS NULL THEN
            v_status := 'SUPERSEDED';
            v_reason_code := 'TARGET_AUTHORITATIVE_DECISION_EXISTS';
          ELSE
            v_status := 'CARRIED';
          END IF;
        END IF;
      END IF;
    ELSE
      IF EXISTS (
        SELECT 1
        FROM public.banking_pay_snapshot_case_component_state
          target_component
        JOIN public.banking_pay_workbench_sessions target_session
          ON target_session.id = p_target_session_id
         AND target_component.snapshot_run_id =
           target_session.source_snapshot_run_id
        WHERE target_component.candidate_id = p_candidate_id
          AND target_component.case_key
            IS NOT DISTINCT FROM
              v_source_resolution ->> 'case_key'
          AND target_component.source_basis_fingerprint
            IS NOT DISTINCT FROM
              v_source_resolution ->> 'source_basis_fingerprint'
      ) THEN
        INSERT INTO
          public.banking_pay_workbench_session_case_resolutions (
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
            resolution_origin_session_id,
            resolution_origin_pay_date,
            resolution_origin_source_basis_fingerprint,
            created_at_utc,
            updated_at_utc
          )
        VALUES (
          p_target_session_id,
          p_candidate_id,
          v_source_resolution ->> 'case_key',
          v_source_resolution ->> 'resolution_family',
          v_registration.canonical_resolution_key,
          nullif(v_source_resolution ->> 'timesheet_id', '')::uuid,
          v_source_resolution ->> 'source_basis_fingerprint',
          v_source_resolution ->> 'source_family_key',
          v_source_resolution ->> 'bucket_code',
          v_source_resolution ->> 'component_key_type',
          v_source_resolution ->> 'component_key_value',
          coalesce(v_source_resolution -> 'payload_json', '{}'::jsonb)
            || jsonb_build_object(
              'carry_registration_id', v_registration.id,
              'source_resolution_id', v_registration.source_resolution_id,
              'source_resolution_identity_key',
                v_registration.source_resolution_identity_key,
              'clone_carried_forward', true,
              'clone_validation_status', 'VALID',
              'requires_review', false,
              'target_source_build_run_id',
                p_target_source_build_run_id,
              'carried_forward_at_utc', coalesce(p_now_utc, now()),
              'policy_x_authority_scope',
                'PRE_DRAFT_CASE_RESOLUTION_STATE_ONLY'
            ),
          coalesce(
            nullif(
              v_source_resolution ->> 'resolution_origin_session_id',
              ''
            )::uuid,
            v_registration.source_session_id
          ),
          coalesce(
            nullif(
              v_source_resolution ->> 'resolution_origin_pay_date',
              ''
            )::date,
            v_target_pay_date
          ),
          coalesce(
            v_source_resolution
              ->> 'resolution_origin_source_basis_fingerprint',
            v_source_resolution ->> 'source_basis_fingerprint'
          ),
          coalesce(p_now_utc, now()),
          coalesce(p_now_utc, now())
        )
        ON CONFLICT (session_id, resolution_identity_key)
        DO NOTHING
        RETURNING id
        INTO v_target_resolution_id;

        IF v_target_resolution_id IS NULL THEN
          SELECT id
          INTO v_target_resolution_id
          FROM public.banking_pay_workbench_session_case_resolutions
          WHERE session_id = p_target_session_id
            AND resolution_identity_key =
              v_registration.canonical_resolution_key;
          v_status := 'SUPERSEDED';
          v_reason_code := 'TARGET_AUTHORITATIVE_DECISION_EXISTS';
        ELSE
          v_status := 'CARRIED';
          v_target_fingerprint :=
            v_registration.source_economic_fingerprint;
        END IF;
      ELSE
        UPDATE
          public.banking_pay_workbench_case_resolution_carry_registrations
        SET attempt_count = attempt_count + 1,
            last_attempt_at_utc = coalesce(p_now_utc, now()),
            last_error_json = jsonb_build_object(
              'code', 'TARGET_EVIDENCE_NOT_READY',
              'target_source_build_run_id', p_target_source_build_run_id
            ),
            updated_at_utc = coalesce(p_now_utc, now())
        WHERE id = v_registration.id;
        v_deferred := v_deferred + 1;
        CONTINUE;
      END IF;
    END IF;

    UPDATE public.banking_pay_workbench_case_resolution_carry_registrations
    SET status = v_status,
        state_reason_code = v_reason_code,
        target_source_build_run_id = p_target_source_build_run_id,
        target_resolution_id = v_target_resolution_id,
        target_economic_fingerprint = v_target_fingerprint,
        attempt_count = attempt_count + 1,
        last_attempt_at_utc = coalesce(p_now_utc, now()),
        last_error_json = CASE
          WHEN v_status IN ('STALE', 'INCOMPATIBLE')
            THEN jsonb_build_object('code', v_reason_code)
          ELSE NULL
        END,
        updated_at_utc = coalesce(p_now_utc, now()),
        completed_at_utc = coalesce(p_now_utc, now())
    WHERE id = v_registration.id;

    INSERT INTO public.audit_events (
      actor_user_id,
      object_type,
      object_id_text,
      action,
      before_json,
      after_json,
      reason
    )
    VALUES (
      v_actor_user_id,
      'BANKING_PAY_CASE_RESOLUTION_CARRY',
      v_registration.id::text,
      'CARRY_' || v_status,
      jsonb_build_object('status', 'PENDING'),
      jsonb_strip_nulls(
        jsonb_build_object(
          'registration_id', v_registration.id,
          'source_session_id', v_registration.source_session_id,
          'target_session_id', p_target_session_id,
          'source_resolution_id',
            v_registration.source_resolution_id,
          'target_resolution_id', v_target_resolution_id,
          'candidate_id', p_candidate_id,
          'canonical_resolution_key',
            v_registration.canonical_resolution_key,
          'source_economic_fingerprint',
            v_registration.source_economic_fingerprint,
          'target_economic_fingerprint', v_target_fingerprint,
          'status', v_status,
          'state_reason_code', v_reason_code
        )
      ),
      v_registration.carry_reason
    );

    v_carried := v_carried + CASE WHEN v_status = 'CARRIED' THEN 1 ELSE 0 END;
    v_stale := v_stale + CASE WHEN v_status = 'STALE' THEN 1 ELSE 0 END;
    v_incompatible :=
      v_incompatible + CASE WHEN v_status = 'INCOMPATIBLE' THEN 1 ELSE 0 END;
    v_superseded :=
      v_superseded + CASE WHEN v_status = 'SUPERSEDED' THEN 1 ELSE 0 END;
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'target_session_id', p_target_session_id,
    'candidate_id', p_candidate_id,
    'target_source_build_run_id', p_target_source_build_run_id,
    'carried_count', v_carried,
    'stale_count', v_stale,
    'incompatible_count', v_incompatible,
    'superseded_count', v_superseded,
    'deferred_count', v_deferred,
    'policy_x_authority_scope',
      'PRE_DRAFT_CASE_RESOLUTION_STATE_ONLY'
  );
END;
$function$;

ALTER FUNCTION
  public._pay_workbench_case_resolution_carry_process_candidate_v1(
    uuid,
    uuid,
    uuid,
    timestamptz
  )
OWNER TO postgres;

REVOKE ALL
ON FUNCTION
  public._pay_workbench_case_resolution_carry_process_candidate_v1(
    uuid,
    uuid,
    uuid,
    timestamptz
  )
FROM PUBLIC, anon, authenticated, service_role;


CREATE OR REPLACE FUNCTION
  public.pay_workbench_case_resolution_carry_status_get_v1(
    p_target_session_id uuid,
    p_candidate_id uuid DEFAULT NULL::uuid
  )
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_result jsonb;
BEGIN
  IF p_target_session_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CASE_RESOLUTION_CARRY_TARGET_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  SELECT jsonb_build_object(
    'ok', true,
    'target_session_id', p_target_session_id,
    'candidate_id', p_candidate_id,
    'total_count', count(*),
    'pending_count', count(*) FILTER (WHERE status = 'PENDING'),
    'carried_count', count(*) FILTER (WHERE status = 'CARRIED'),
    'stale_count', count(*) FILTER (WHERE status = 'STALE'),
    'incompatible_count', count(*) FILTER (WHERE status = 'INCOMPATIBLE'),
    'superseded_count', count(*) FILTER (WHERE status = 'SUPERSEDED'),
    'carry_state', CASE
      WHEN count(*) FILTER (WHERE status = 'PENDING') > 0 THEN 'DEFERRED'
      WHEN count(*) FILTER (WHERE status IN ('STALE', 'INCOMPATIBLE')) > 0
        THEN 'REVIEW_REQUIRED'
      WHEN count(*) FILTER (WHERE status = 'CARRIED') > 0 THEN 'CARRIED'
      ELSE 'NOT_REQUIRED'
    END
  )
  INTO v_result
  FROM public.banking_pay_workbench_case_resolution_carry_registrations
  WHERE target_session_id = p_target_session_id
    AND (p_candidate_id IS NULL OR candidate_id = p_candidate_id);

  RETURN v_result;
END;
$function$;

ALTER FUNCTION
  public.pay_workbench_case_resolution_carry_status_get_v1(uuid, uuid)
OWNER TO postgres;

REVOKE ALL
ON FUNCTION
  public.pay_workbench_case_resolution_carry_status_get_v1(uuid, uuid)
FROM PUBLIC, anon, authenticated;

GRANT EXECUTE
ON FUNCTION
  public.pay_workbench_case_resolution_carry_status_get_v1(uuid, uuid)
TO service_role;

-- Newly introduced RPCs must be visible to the Worker immediately after this
-- repeatable transaction commits.
NOTIFY pgrst, 'reload schema';
