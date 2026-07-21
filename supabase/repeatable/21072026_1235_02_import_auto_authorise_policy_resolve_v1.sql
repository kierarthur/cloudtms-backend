-- ============================================================================
-- 01_import_auto_authorise_policy_resolve_v1.txt
-- New function. Install after the additive schema migration that adds:
--   settings_defaults.healthroster_import_auto_authorise_default
--   settings_defaults.nhsp_import_auto_authorise_default
--   settings_defaults.auto_authorise_on_validation
--   client_settings.healthroster_import_auto_authorise
--   client_settings.nhsp_import_auto_authorise
--   contracts.healthroster_import_auto_authorise_override
--   contracts.nhsp_import_auto_authorise_override
--
-- Limits:
--   4 arguments.
--   No arrays, fanout, writes, or unbounded scans.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.import_auto_authorise_policy_resolve_v1(
  p_source_system public.hr_source_enum,
  p_client_id uuid,
  p_contract_id uuid DEFAULT NULL::uuid,
  p_validation_context boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_source_system text := UPPER(BTRIM(COALESCE(p_source_system::text, '')));
  v_policy_source_system text;
  v_global public.settings_defaults%ROWTYPE;
  v_client_setting public.client_settings%ROWTYPE;
  v_contract public.contracts%ROWTYPE;

  v_client_setting_found boolean := false;
  v_contract_found boolean := false;

  v_global_import_value boolean;
  v_client_import_value boolean;
  v_contract_override_value boolean;
  v_effective_import_value boolean;
  v_effective_value boolean;
  v_resolution_source text;
  v_policy_json jsonb;
  v_policy_fingerprint text;
BEGIN
  IF p_client_id IS NULL THEN
    RAISE EXCEPTION 'IMPORT_AUTO_AUTHORISE_CLIENT_ID_REQUIRED'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_AUTO_AUTHORISE_CLIENT_ID_REQUIRED'
            )::text;
  END IF;

  IF v_source_system NOT IN ('HEALTHROSTER', 'HEALTHROSTER_DAILY', 'NHSP') THEN
    RAISE EXCEPTION 'IMPORT_AUTO_AUTHORISE_SOURCE_SYSTEM_INVALID'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_AUTO_AUTHORISE_SOURCE_SYSTEM_INVALID',
              'source_system', v_source_system
            )::text;
  END IF;

  v_policy_source_system := CASE
    WHEN v_source_system IN ('HEALTHROSTER', 'HEALTHROSTER_DAILY')
      THEN 'HEALTHROSTER'
    ELSE 'NHSP'
  END;

  PERFORM 1
  FROM public.clients AS client_row
  WHERE client_row.id = p_client_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'IMPORT_AUTO_AUTHORISE_CLIENT_NOT_FOUND'
      USING ERRCODE = 'P0002',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_AUTO_AUTHORISE_CLIENT_NOT_FOUND',
              'client_id', p_client_id::text
            )::text;
  END IF;

  SELECT settings_row.*
  INTO v_global
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'IMPORT_AUTO_AUTHORISE_GLOBAL_SETTINGS_MISSING'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_AUTO_AUTHORISE_GLOBAL_SETTINGS_MISSING',
              'settings_defaults_id', 1
            )::text;
  END IF;

  SELECT client_settings_row.*
  INTO v_client_setting
  FROM public.client_settings AS client_settings_row
  WHERE client_settings_row.client_id = p_client_id
    AND (
      client_settings_row.effective_from IS NULL
      OR client_settings_row.effective_from <= (CURRENT_TIMESTAMP AT TIME ZONE 'Europe/London')::date
    )
  ORDER BY
    client_settings_row.effective_from DESC NULLS LAST,
    client_settings_row.updated_at DESC,
    client_settings_row.id DESC
  LIMIT 1;

  v_client_setting_found := FOUND;

  IF p_contract_id IS NOT NULL THEN
    SELECT contract_row.*
    INTO v_contract
    FROM public.contracts AS contract_row
    WHERE contract_row.id = p_contract_id
    LIMIT 1;

    v_contract_found := FOUND;

    IF NOT v_contract_found THEN
      RAISE EXCEPTION 'IMPORT_AUTO_AUTHORISE_CONTRACT_NOT_FOUND'
        USING ERRCODE = 'P0002',
              DETAIL = jsonb_build_object(
                'code', 'IMPORT_AUTO_AUTHORISE_CONTRACT_NOT_FOUND',
                'contract_id', p_contract_id::text
              )::text;
    END IF;

    IF v_contract.client_id IS DISTINCT FROM p_client_id THEN
      RAISE EXCEPTION 'IMPORT_AUTO_AUTHORISE_CONTRACT_CLIENT_MISMATCH'
        USING ERRCODE = '22023',
              DETAIL = jsonb_build_object(
                'code', 'IMPORT_AUTO_AUTHORISE_CONTRACT_CLIENT_MISMATCH',
                'contract_id', p_contract_id::text,
                'expected_client_id', p_client_id::text,
                'actual_client_id', v_contract.client_id::text
              )::text;
    END IF;
  END IF;

  IF v_policy_source_system = 'HEALTHROSTER' THEN
    v_global_import_value :=
      v_global.healthroster_import_auto_authorise_default;

    v_client_import_value := CASE
      WHEN v_client_setting_found
        THEN v_client_setting.healthroster_import_auto_authorise
      ELSE v_global_import_value
    END;

    v_contract_override_value := CASE
      WHEN v_contract_found
        THEN v_contract.healthroster_import_auto_authorise_override
      ELSE NULL::boolean
    END;
  ELSE
    v_global_import_value :=
      v_global.nhsp_import_auto_authorise_default;

    v_client_import_value := CASE
      WHEN v_client_setting_found
        THEN v_client_setting.nhsp_import_auto_authorise
      ELSE v_global_import_value
    END;

    v_contract_override_value := CASE
      WHEN v_contract_found
        THEN v_contract.nhsp_import_auto_authorise_override
      ELSE NULL::boolean
    END;
  END IF;

  IF v_contract_override_value IS NOT NULL THEN
    v_effective_import_value := v_contract_override_value;
    v_resolution_source := 'CONTRACT_OVERRIDE';
  ELSIF v_client_setting_found THEN
    v_effective_import_value := v_client_import_value;
    v_resolution_source := 'CLIENT_SETTING';
  ELSE
    v_effective_import_value := v_global_import_value;
    v_resolution_source := 'GLOBAL_FALLBACK_CLIENT_SETTING_MISSING';
  END IF;

  IF COALESCE(p_validation_context, false) THEN
    v_effective_value := v_global.auto_authorise_on_validation;
    v_resolution_source := 'GLOBAL_VALIDATION_SETTING';
  ELSE
    v_effective_value := v_effective_import_value;
  END IF;

  v_policy_json := jsonb_strip_nulls(
    jsonb_build_object(
      'source_system', v_source_system,
      'policy_source_system', v_policy_source_system,
      'validation_context', COALESCE(p_validation_context, false),
      'client_id', p_client_id::text,
      'contract_id', CASE
        WHEN p_contract_id IS NULL THEN NULL
        ELSE p_contract_id::text
      END,
      'global_import_value', v_global_import_value,
      'client_import_value', v_client_import_value,
      'contract_override_value', v_contract_override_value,
      'effective_import_value', v_effective_import_value,
      'global_validation_value', v_global.auto_authorise_on_validation,
      'effective_value', v_effective_value,
      'resolution_source', v_resolution_source,
      'client_setting_found', v_client_setting_found,
      'client_settings_id', CASE
        WHEN v_client_setting_found THEN v_client_setting.id::text
        ELSE NULL
      END,
      'client_settings_effective_from', CASE
        WHEN v_client_setting_found THEN v_client_setting.effective_from
        ELSE NULL
      END,
      'client_settings_updated_at', CASE
        WHEN v_client_setting_found THEN v_client_setting.updated_at
        ELSE NULL
      END,
      'contract_updated_at', CASE
        WHEN v_contract_found THEN v_contract.updated_at
        ELSE NULL
      END,
      'global_settings_updated_at', v_global.updated_at
    )
  );

  v_policy_fingerprint := encode(
    extensions.digest(
      convert_to(v_policy_json::text, 'UTF8'),
      'sha256'
    ),
    'hex'
  );

  RETURN v_policy_json
    || jsonb_build_object(
      'ok', true,
      'policy_fingerprint', v_policy_fingerprint
    );
END;
$function$;

COMMENT ON FUNCTION public.import_auto_authorise_policy_resolve_v1(
  public.hr_source_enum,
  uuid,
  uuid,
  boolean
)
IS 'Resolves effective HealthRoster/NHSP import or exact-validation auto-authorisation policy. Read-only and bounded.';

REVOKE ALL ON FUNCTION public.import_auto_authorise_policy_resolve_v1(
  public.hr_source_enum,
  uuid,
  uuid,
  boolean
) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.import_auto_authorise_policy_resolve_v1(
  public.hr_source_enum,
  uuid,
  uuid,
  boolean
) TO service_role;
