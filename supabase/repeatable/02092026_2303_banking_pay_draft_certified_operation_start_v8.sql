-- CloudTMS Banking Pay certified DRAFT_CREATE admission V8.
-- Starts the database-owned operation and binds the exact sealed Workbench
-- certificate in one transaction. It carries no selected-row arrays and owns
-- no Draft economics, eligibility, amounts, routing, payment or provider work.

CREATE OR REPLACE FUNCTION public.banking_pay_draft_certified_operation_start_v8(
  p_certificate_reference jsonb,
  p_actor_user_id uuid,
  p_idempotency_key text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
SET statement_timeout = '6000ms'
SET lock_timeout = '1000ms'
AS $function$
DECLARE
  v_certification_id text;
  v_overall_digest_sha256 text;
  v_reference_idempotency_key text;
  v_workbench_session_id uuid;
  v_started record;
  v_admission jsonb;
BEGIN
  IF p_actor_user_id IS NULL
     OR NULLIF(BTRIM(COALESCE(p_idempotency_key,'')),'') IS NULL
     OR jsonb_typeof(p_certificate_reference)<>'object' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OPERATION_ADMISSION_INVALID'
      USING ERRCODE='22023';
  END IF;

  v_certification_id:=NULLIF(BTRIM(COALESCE(p_certificate_reference->>'certification_id','')),'');
  v_overall_digest_sha256:=LOWER(NULLIF(BTRIM(COALESCE(p_certificate_reference->>'overall_digest_sha256','')),''));
  v_reference_idempotency_key:=NULLIF(BTRIM(COALESCE(p_certificate_reference->>'idempotency_key','')),'');

  IF v_certification_id IS NULL
     OR v_overall_digest_sha256 IS NULL
     OR v_reference_idempotency_key IS DISTINCT FROM p_idempotency_key
     OR v_certification_id !~ '^WORKBENCH_SETTLED_CERTIFICATION_V2:[0-9a-f]{64}$'
     OR v_overall_digest_sha256 !~ '^[0-9a-f]{64}$'
     OR v_certification_id IS DISTINCT FROM
          'WORKBENCH_SETTLED_CERTIFICATION_V2:'||v_overall_digest_sha256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REFERENCE_INVALID'
      USING ERRCODE='22023';
  END IF;

  SELECT certificate.workbench_session_id
  INTO v_workbench_session_id
  FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.certification_id=v_certification_id
    AND certificate.overall_digest_sha256=v_overall_digest_sha256
    AND certificate.lifecycle='SEALED_CURRENT';

  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_CURRENT'
      USING ERRCODE='55000';
  END IF;

  SELECT started.*
  INTO STRICT v_started
  FROM public.banking_pay_operation_start(
    'DRAFT_CREATE',
    p_actor_user_id,
    p_idempotency_key,
    v_workbench_session_id,
    NULL::uuid,
    NULL::uuid,
    pg_catalog.jsonb_build_object(
      'workbench_settled_certificate_reference_v8',p_certificate_reference),
    '{}'::jsonb
  ) AS started;

  -- banking_pay_operation_start retains its established one-active-Draft guard.
  -- A different active operation may be returned by that guard; it must never
  -- be rebound to this request or certificate.
  IF v_started.operation_type IS DISTINCT FROM 'DRAFT_CREATE'
     OR v_started.workbench_session_id IS DISTINCT FROM v_workbench_session_id
     OR v_started.idempotency_key IS DISTINCT FROM p_idempotency_key
     OR v_started.input_json->'workbench_settled_certificate_reference_v8'
          IS DISTINCT FROM p_certificate_reference THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_IDEMPOTENCY_CONTEXT_MISMATCH'
      USING ERRCODE='23505';
  END IF;

  v_admission:=private.pay_workbench_settled_certificate_operation_admit_v8(
    v_started.operation_id);

  RETURN v_admission||pg_catalog.jsonb_build_object(
    'operation_status',v_started.status,
    'operation_phase',v_started.phase,
    'operation_is_existing',v_started.is_existing);
END;
$function$;

ALTER FUNCTION public.banking_pay_draft_certified_operation_start_v8(jsonb,uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_pay_draft_certified_operation_start_v8(jsonb,uuid,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.banking_pay_draft_certified_operation_start_v8(jsonb,uuid,text) FROM anon;
REVOKE ALL ON FUNCTION public.banking_pay_draft_certified_operation_start_v8(jsonb,uuid,text) FROM authenticated;
REVOKE ALL ON FUNCTION public.banking_pay_draft_certified_operation_start_v8(jsonb,uuid,text) FROM service_role;
GRANT EXECUTE ON FUNCTION public.banking_pay_draft_certified_operation_start_v8(jsonb,uuid,text) TO service_role;

NOTIFY pgrst, 'reload schema';
