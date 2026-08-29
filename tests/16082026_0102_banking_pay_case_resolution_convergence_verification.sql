-- Banking Pay case-resolution convergence executable contract verification.
-- TEST-only. Every fixture is temporary and the transaction is rolled back.
-- No Draft, payment, provider, settlement, remittance or communication action
-- is created or invoked by this script.
\set ON_ERROR_STOP on

BEGIN;

DO $definition_contract$
DECLARE
  v_normaliser text;
  v_residual text;
  v_apply text;
BEGIN
  SELECT pg_catalog.pg_get_functiondef(
    'public._ctms_normalise_correction_case_resolutions_v1(uuid,uuid,uuid)'::regprocedure)
  INTO STRICT v_normaliser;
  SELECT pg_catalog.pg_get_functiondef(
    'public.pay_correction_chain_residual_v1(uuid,uuid,text,uuid,uuid,integer)'::regprocedure)
  INTO STRICT v_residual;
  SELECT pg_catalog.pg_get_functiondef(
    'public.pay_workbench_session_apply_case_resolution(uuid,uuid,jsonb)'::regprocedure)
  INTO STRICT v_apply;

  IF position('CORRECTION_CHAIN_BUCKET_SET_V1' in v_normaliser)=0
     OR position('physical_decision_set_digest' in v_normaliser)=0
     OR position('CORRECTION_CHAIN_RESOLUTION_PROJECTION_CONFLICT' in v_normaliser)=0
     OR position('source_family_key is distinct from' in lower(v_normaliser))=0
     OR v_normaliser ~* 'delete\s+from\s+public\.banking_pay_workbench_session_case_resolutions' THEN
    RAISE EXCEPTION 'BANKING_PAY_PHYSICAL_RESOLUTION_PRESERVATION_CONTRACT_MISSING';
  END IF;

  IF position('CORRECTION_CHAIN_BUCKET_SET_V1' in v_residual)=0
     OR position('aggregate_proof_valid' in v_residual)=0
     OR position('physical_decision_count' in v_residual)=0
     OR position('physical_decision_set_digest' in v_residual)=0 THEN
    RAISE EXCEPTION 'BANKING_PAY_CORRECTION_AGGREGATE_PROOF_CONTRACT_MISSING';
  END IF;

  IF position('pay_workbench_session_refresh_current_authority_v1' in v_apply)=0
     OR position('WORKBENCH_SESSION_VERSION_REFRESH_NOT_PROVEN' in v_apply)=0
     OR position('session_version_refresh_pages' in v_apply)=0
     OR position('certified_scope.certified_preview_publication_session_version' in v_apply)=0
     OR position('certified_scope.certified_preview_publication_parity_ok IS TRUE' in v_apply)=0 THEN
    RAISE EXCEPTION 'BANKING_PAY_SESSION_VERSION_CONVERGENCE_CONTRACT_MISSING';
  END IF;
END;
$definition_contract$;

CREATE TEMP TABLE banking_pay_rate_bucket_projection_fixture(
  bucket_code text PRIMARY KEY,
  source_amount numeric NOT NULL,
  target_amount numeric NOT NULL
) ON COMMIT DROP;

INSERT INTO banking_pay_rate_bucket_projection_fixture VALUES
  ('DAY',230.00,264.50),
  ('NIGHT',37.50,43.13),
  ('SAT',100.00,115.00),
  ('SUN',80.00,92.00),
  ('BH',60.00,69.00),
  ('ADDITIONAL_RATE',40.00,46.00);

DO $bucket_projection_contract$
DECLARE
  v_bucket_count integer;
  v_projected_target_count integer;
  v_projected_target numeric;
  v_bucket_json jsonb;
  v_bucket_digest text;
  v_conflicting_projection_count integer;
BEGIN
  SELECT count(*)::integer,
         count(DISTINCT round(
           67.50*abs(target_amount)/abs(source_amount),2
         ))::integer,
         min(round(67.50*abs(target_amount)/abs(source_amount),2)),
         jsonb_agg(jsonb_build_object(
           'bucket_code',bucket_code,
           'source_pay_ex_vat',source_amount,
           'target_pay_ex_vat',target_amount
         ) ORDER BY bucket_code)
  INTO v_bucket_count,v_projected_target_count,v_projected_target,v_bucket_json
  FROM banking_pay_rate_bucket_projection_fixture;

  v_bucket_digest:=encode(extensions.digest(
    convert_to(v_bucket_json::text,'UTF8'),'sha256'),'hex');

  IF v_bucket_count<>6 OR v_projected_target_count<>1
     OR v_projected_target<>77.63
     OR length(v_bucket_digest)<>64 THEN
    RAISE EXCEPTION 'BANKING_PAY_ALL_RATE_BUCKET_PROJECTION_FIXTURE_FAILED';
  END IF;

  WITH conflicting AS (
    SELECT source_amount,target_amount
    FROM banking_pay_rate_bucket_projection_fixture
    WHERE bucket_code='DAY'
    UNION ALL
    SELECT 37.50::numeric,50.00::numeric
  )
  SELECT count(DISTINCT round(
    67.50*abs(target_amount)/abs(source_amount),2
  ))::integer
  INTO v_conflicting_projection_count
  FROM conflicting;

  IF v_conflicting_projection_count<=1 THEN
    RAISE EXCEPTION 'BANKING_PAY_CONFLICTING_RATE_BUCKET_MUST_FAIL_CLOSED';
  END IF;
END;
$bucket_projection_contract$;

ROLLBACK;
