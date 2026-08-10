-- Banking Pay physical source-publication identity and bounded Draft step RPC.
--
-- Policy X: the helper below identifies an already-authoritative pre-Draft
-- source cohort. It does not calculate or alter payment economics.

CREATE OR REPLACE FUNCTION private.pay_workbench_source_publication_identity_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_session_version bigint,
  p_source_change_seq bigint,
  p_source_build_run_id uuid
)
RETURNS uuid
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_hash text;
BEGIN
  IF p_session_version <= 0 OR p_source_change_seq < 0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SOURCE_PUBLICATION_AUTHORITY_INVALID'
      USING ERRCODE='22023', DETAIL=pg_catalog.jsonb_build_object(
        'code','PAY_WORKBENCH_SOURCE_PUBLICATION_AUTHORITY_INVALID',
        'reason','VERSION_OR_SEQUENCE_INVALID'
      )::text;
  END IF;

  v_hash := pg_catalog.md5(
    'BANKING_PAY_SOURCE_PUBLICATION_V1|' ||
    p_session_id::text || '|' || p_candidate_id::text || '|' ||
    p_session_version::text || '|' || p_source_change_seq::text || '|' ||
    p_source_build_run_id::text
  );

  RETURN (
    pg_catalog.substr(v_hash,1,8) || '-' ||
    pg_catalog.substr(v_hash,9,4) || '-' ||
    pg_catalog.substr(v_hash,13,4) || '-' ||
    pg_catalog.substr(v_hash,17,4) || '-' ||
    pg_catalog.substr(v_hash,21,12)
  )::uuid;
END;
$function$;

ALTER FUNCTION private.pay_workbench_source_publication_identity_v1(
  uuid,uuid,bigint,bigint,uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION private.pay_workbench_source_publication_identity_v1(
  uuid,uuid,bigint,bigint,uuid
) FROM PUBLIC,anon,authenticated,service_role;

GRANT EXECUTE ON FUNCTION private.pay_workbench_source_publication_identity_v1(
  uuid,uuid,bigint,bigint,uuid
) TO postgres;

COMMENT ON FUNCTION private.pay_workbench_source_publication_identity_v1(
  uuid,uuid,bigint,bigint,uuid
) IS 'Deterministic immutable identity for one physical Banking Pay candidate source cohort.';
