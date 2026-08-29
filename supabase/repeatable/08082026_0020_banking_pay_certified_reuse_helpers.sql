-- Certified historical source selection and publication fencing for Banking Pay.
-- Read-only proof helpers: neither function mutates queue, publication or financial state.

CREATE OR REPLACE FUNCTION private.pay_workbench_candidate_reuse_source_select_v1(
  p_target_session_id uuid,
  p_candidate_id uuid,
  p_source_change_seq bigint,
  p_options_json jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_settings public.settings_defaults%ROWTYPE;
  v_build private.banking_pay_workbench_economic_builds%ROWTYPE;
  v_certification jsonb := '{}'::jsonb;
  v_examined integer := 0;
  v_nonempty_reuse_enabled boolean := false;
  v_empty_reuse_enabled boolean := false;
BEGIN
  IF p_target_session_id IS NULL OR p_candidate_id IS NULL OR p_source_change_seq IS NULL THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'reuse_available',false,
      'reason','TARGET_CANDIDATE_AND_SEQUENCE_REQUIRED',
      'required_refresh_job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'sources_examined',0
    );
  END IF;

  SELECT settings_row.* INTO v_settings
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id=1;

  v_nonempty_reuse_enabled := pg_catalog.lower(coalesce(
    pg_catalog.to_jsonb(v_settings)->>'banking_pay_workbench_clone_bounded_reuse_v2_enabled','false'
  )) IN ('true','t','1','yes','y','on');
  v_empty_reuse_enabled := pg_catalog.lower(coalesce(
    pg_catalog.to_jsonb(v_settings)->>'banking_pay_workbench_clone_source_empty_reuse_enabled','false'
  )) IN ('true','t','1','yes','y','on');

  IF v_nonempty_reuse_enabled IS NOT TRUE
     AND v_empty_reuse_enabled IS NOT TRUE THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'reuse_available',false,
      'reason','CERTIFIED_REUSE_V2_DISABLED',
      'required_refresh_job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'sources_examined',0
    );
  END IF;

  FOR v_build IN
    SELECT candidate_build.*
    FROM private.banking_pay_workbench_economic_builds AS candidate_build
    WHERE candidate_build.candidate_id=p_candidate_id
      AND candidate_build.source_change_seq=p_source_change_seq
      AND candidate_build.status='COMPLETE'
      AND candidate_build.private_stage='COMPLETE'
      AND candidate_build.completed_at_utc IS NOT NULL
      AND candidate_build.failure_json='{}'::jsonb
      AND candidate_build.session_id<>p_target_session_id
      AND candidate_build.dependency_closure_sealed_at_utc IS NOT NULL
      AND candidate_build.dependency_edge_stream_complete IS TRUE
      AND candidate_build.edge_tag_stream_complete IS TRUE
      AND candidate_build.sealed_fingerprint_digest IS NOT NULL
      AND candidate_build.canonical_digest IS NOT NULL
      AND pg_catalog.lower(coalesce(candidate_build.publication_cursor_json->>'terminal','false'))
            IN ('true','t','1','yes','y','on')
    ORDER BY candidate_build.completed_at_utc DESC,candidate_build.id DESC
    LIMIT 16
  LOOP
    v_examined := v_examined+1;

    v_certification := private.pay_workbench_session_clone_bounded_certification_v1(
      v_build.session_id,
      p_target_session_id,
      p_candidate_id,
      pg_catalog.jsonb_strip_nulls(
        coalesce(p_options_json,'{}'::jsonb)
        || pg_catalog.jsonb_build_object(
          'source_session_id',v_build.session_id::text,
          'target_session_id',p_target_session_id::text,
          'source_selection_authorised',true,
          'allow_session_rebase',true,
          'rebase_simple_rows_only',true,
          'certification_version',2
        )
      )
    );

    IF coalesce((v_certification->>'clone_eligible')::boolean,false) IS TRUE
       AND NULLIF(v_certification->>'original_economic_build_id','')=v_build.id::text
       AND (
         (coalesce(v_certification->>'source_mode','NONEMPTY')='SOURCE_EMPTY' AND v_empty_reuse_enabled IS TRUE)
         OR (coalesce(v_certification->>'source_mode','NONEMPTY')<>'SOURCE_EMPTY' AND v_nonempty_reuse_enabled IS TRUE)
       ) THEN
      RETURN pg_catalog.jsonb_strip_nulls(
        v_certification
        || pg_catalog.jsonb_build_object(
          'reuse_available',true,
          'selected_source_session_id',v_build.session_id::text,
          'selected_economic_build_id',v_build.id::text,
          'selected_source_build_run_id',v_build.source_build_run_id::text,
          'sources_examined',v_examined,
          'search_limit',16
        )
      );
    END IF;
  END LOOP;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'reuse_available',false,
    'reason','NO_CERTIFIED_REUSE_SOURCE_IN_BOUNDED_WINDOW',
    'required_refresh_job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
    'sources_examined',v_examined,
    'search_limit',16
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_candidate_reuse_source_select_v1(uuid,uuid,bigint,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_candidate_reuse_source_select_v1(uuid,uuid,bigint,jsonb) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_candidate_reuse_source_select_v1(uuid,uuid,bigint,jsonb) TO postgres;

CREATE OR REPLACE FUNCTION private.pay_workbench_session_clone_publication_fence_v1(
  p_source_session_id uuid,
  p_target_session_id uuid,
  p_candidate_id uuid,
  p_certification_json jsonb
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_expected jsonb := CASE
    WHEN pg_catalog.jsonb_typeof(coalesce(p_certification_json,'{}'::jsonb))='object'
      THEN coalesce(p_certification_json,'{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_current jsonb := '{}'::jsonb;
  v_clone_job_id text := NULLIF(pg_catalog.btrim(coalesce(v_expected->>'clone_job_id','')),'');
BEGIN
  IF NULLIF(v_expected->>'certification_digest','') IS NULL
     OR coalesce(v_expected->>'certification_version','')<>'2' THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'fence_passed',false,
      'reason','CLONE_CERTIFICATION_V2_REQUIRED'
    );
  END IF;

  v_current := private.pay_workbench_session_clone_bounded_certification_v1(
    p_source_session_id,
    p_target_session_id,
    p_candidate_id,
    pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
      'source_session_id',p_source_session_id::text,
      'target_session_id',p_target_session_id::text,
      'source_selection_authorised',true,
      'allow_session_rebase',true,
      'rebase_simple_rows_only',true,
      'certification_version',2,
      'final_fence_mode',true,
      'clone_job_id',v_clone_job_id
    ))
  );

  IF coalesce((v_current->>'clone_eligible')::boolean,false) IS NOT TRUE
     OR NULLIF(v_current->>'certification_digest','')
          IS DISTINCT FROM NULLIF(v_expected->>'certification_digest','')
     OR NULLIF(v_current->>'original_economic_build_id','')
          IS DISTINCT FROM NULLIF(v_expected->>'original_economic_build_id','')
     OR NULLIF(v_current->>'source_build_run_id','')
          IS DISTINCT FROM NULLIF(v_expected->>'source_build_run_id','') THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'fence_passed',false,
      'reason',coalesce(NULLIF(v_current->>'reason',''),'CLONE_CERTIFICATION_DRIFT'),
      'current_certification',v_current
    );
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'fence_passed',true,
    'reason',NULL::text,
    'certification',v_current,
    'certification_digest',v_current->>'certification_digest'
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_session_clone_publication_fence_v1(uuid,uuid,uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_session_clone_publication_fence_v1(uuid,uuid,uuid,jsonb) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_session_clone_publication_fence_v1(uuid,uuid,uuid,jsonb) TO postgres;
