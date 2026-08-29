-- CloudTMS invoice batch public RPC replacement
-- Generated 2026-07-26 from live TEST DB signatures plus locked invoice batch implementation plan.
-- Install after the v3 private helper package and modal-query-support migration are installed.
-- The preservation block keeps a private copy of the live legacy implementation so existing callers remain compatible.

DO $preserve_legacy$
DECLARE
  v_def text;
BEGIN
  IF to_regprocedure('private._invoice_batch_issue_candidates_legacy_20260726(boolean, integer)') IS NULL THEN
    SELECT pg_get_functiondef(p.oid)
      INTO v_def
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public'
      AND p.proname = 'invoice_batch_issue_candidates'
      AND pg_get_function_identity_arguments(p.oid) = 'p_allow_early boolean, p_limit integer';

    IF v_def IS NULL THEN
      RAISE EXCEPTION 'Cannot preserve legacy public.invoice_batch_issue_candidates(boolean, integer): source function not found and private legacy copy missing';
    END IF;

    IF position('_invoice_batch_issue_candidates_legacy_20260726' IN v_def) > 0 THEN
      RAISE EXCEPTION 'Refusing to clone an already-wrapped public.invoice_batch_issue_candidates; private legacy copy is missing';
    END IF;

    v_def := regexp_replace(
      v_def,
      '^CREATE OR REPLACE FUNCTION public\.invoice_batch_issue_candidates',
      'CREATE OR REPLACE FUNCTION private._invoice_batch_issue_candidates_legacy_20260726'
    );

    EXECUTE v_def;
    EXECUTE 'REVOKE ALL ON FUNCTION private._invoice_batch_issue_candidates_legacy_20260726(boolean, integer) FROM PUBLIC, anon, authenticated';
    EXECUTE 'GRANT EXECUTE ON FUNCTION private._invoice_batch_issue_candidates_legacy_20260726(boolean, integer) TO service_role';
  END IF;
END
$preserve_legacy$;

DROP FUNCTION IF EXISTS public.invoice_batch_issue_candidates(boolean, integer);

CREATE OR REPLACE FUNCTION public.invoice_batch_issue_candidates(
  p_allow_early boolean,
  p_limit integer,
  p_query jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_query jsonb;
  v_page_size integer;
BEGIN
  IF p_query IS NULL THEN
    IF to_regprocedure('private._invoice_batch_issue_candidates_legacy_20260726(boolean, integer)') IS NULL THEN
      RAISE EXCEPTION USING
        errcode = '42883',
        message = 'LEGACY_INVOICE_BATCH_ISSUE_CANDIDATES_MISSING';
    END IF;

    RETURN private._invoice_batch_issue_candidates_legacy_20260726(
      p_allow_early,
      p_limit
    );
  END IF;

  IF jsonb_typeof(p_query) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION USING errcode = '22023', message = 'BATCH_QUERY_INVALID';
  END IF;

  IF coalesce(p_query->>'contract_version', 'INVOICE_BATCH_QUERY_V1') <> 'INVOICE_BATCH_QUERY_V1' THEN
    RAISE EXCEPTION USING errcode = '22023', message = 'BATCH_QUERY_INVALID';
  END IF;

  IF upper(coalesce(nullif(p_query->>'action', ''), 'ISSUE')) <> 'ISSUE' THEN
    RAISE EXCEPTION USING errcode = '22023', message = 'BATCH_QUERY_ACTION_MISMATCH';
  END IF;

  v_page_size := CASE
    WHEN coalesce(p_query->>'page_size','') ~ '^[1-9][0-9]{0,8}$'
      THEN greatest(1, least((p_query->>'page_size')::integer, 100))
    WHEN p_limit IS NOT NULL
      THEN greatest(1, least(p_limit, 100))
    ELSE 100
  END;

  v_query := (p_query - 'action')
    || jsonb_build_object(
      'contract_version', 'INVOICE_BATCH_QUERY_V1',
      'action', 'ISSUE',
      'allow_early', CASE
        WHEN p_query ? 'allow_early' THEN lower(coalesce(p_query->>'allow_early','false')) IN ('true','t','1','yes','on')
        WHEN coalesce(p_query#>>'{filters,allow_early}','') <> '' THEN lower(coalesce(p_query#>>'{filters,allow_early}','false')) IN ('true','t','1','yes','on')
        ELSE coalesce(p_allow_early, false)
      END,
      'page_size', v_page_size
    );

  RETURN private._invoice_batch_issue_candidate_rows_v1(v_query, now());
END;
$function$;

-- TEST V8 hard cutover: the multi-argument candidate authority is retained
-- temporarily for forensic comparison only. Runtime callers must use the
-- exact jsonb V2 overload.
REVOKE ALL ON FUNCTION public.invoice_batch_issue_candidates(boolean, integer, jsonb)
  FROM PUBLIC, anon, authenticated, service_role;
