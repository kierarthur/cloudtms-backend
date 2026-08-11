-- Banking Pay Workbench: bounded, fail-closed proof that the physically active
-- source and preview rows agree with the current V3 publication authority.
--
-- Policy X: this function is read-only. It does not calculate economics or
-- change post-Draft evidence; it only proves whether already-published
-- PRE_DRAFT_LIVE_TRUTH is physically current.

CREATE OR REPLACE FUNCTION private.pay_workbench_candidate_physical_currentness_page_v1(
  p_session_id uuid,
  p_candidate_ids uuid[],
  p_mode text DEFAULT 'TERMINAL_CURRENT'::text,
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
PARALLEL RESTRICTED
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_mode text := pg_catalog.upper(pg_catalog.btrim(COALESCE(p_mode,'TERMINAL_CURRENT')));
  v_allow_active_owner boolean := pg_catalog.lower(pg_catalog.btrim(COALESCE(
    p_options_json->>'allow_active_owner','false'
  ))) IN ('true','t','1','yes','y','on');
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_candidate_id uuid;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_state public.banking_pay_workbench_session_candidate_state%ROWTYPE;
  v_registry private.banking_pay_workbench_candidate_scope_registry%ROWTYPE;
  v_attestation jsonb := '{}'::jsonb;
  v_scope_ordinal bigint := 0;
  v_source_count integer := 0;
  v_ready_count integer := 0;
  v_active_noncurrent_preview_count integer := 0;
  v_active_noncurrent_source_count integer := 0;
  v_active_noncurrent_line_work_count integer := 0;
  v_source_minus_preview_count integer := 0;
  v_preview_minus_source_count integer := 0;
  v_duplicate_active_identity_count integer := 0;
  v_source_run_count integer := 0;
  v_source_publication_count integer := 0;
  v_source_build_run_id uuid := NULL::uuid;
  v_source_publication_id uuid := NULL::uuid;
  v_live_source_identity_digest text := NULL::text;
  v_live_preview_identity_digest text := NULL::text;
  v_live_source_digest text := NULL::text;
  v_selected_preview_count integer := 0;
  v_selected_session_count integer := 0;
  v_selection_consistent boolean := false;
  v_attestation_current boolean := false;
  v_active_owner_count integer := 0;
  v_active_owner_job_id uuid := NULL::uuid;
  v_terminal_current boolean := false;
  v_current_or_active_owner boolean := false;
  v_reason text := NULL::text;
  v_proof_digest text := NULL::text;
  v_results jsonb := '[]'::jsonb;
  v_terminal_count integer := 0;
  v_current_or_owner_count integer := 0;
BEGIN
  IF p_session_id IS NULL
     OR p_candidate_ids IS NULL
     OR pg_catalog.cardinality(p_candidate_ids)=0
     OR pg_catalog.cardinality(p_candidate_ids)>100
     OR v_mode NOT IN ('TERMINAL_CURRENT','OBSERVE_ONLY')
     OR pg_catalog.jsonb_typeof(COALESCE(p_options_json,'{}'::jsonb))<>'object'
     OR EXISTS (
       SELECT 1
       FROM pg_catalog.jsonb_object_keys(COALESCE(p_options_json,'{}'::jsonb)) AS option_key(key)
       WHERE option_key.key NOT IN ('contract_version','allow_active_owner')
     )
     OR COALESCE(p_options_json->>'contract_version','1')<>'1'
     OR EXISTS (
       SELECT 1
       FROM pg_catalog.unnest(p_candidate_ids) AS supplied(candidate_id)
       WHERE supplied.candidate_id IS NULL
     )
     OR (SELECT pg_catalog.count(*) FROM pg_catalog.unnest(p_candidate_ids) AS supplied(candidate_id))
          IS DISTINCT FROM
        (SELECT pg_catalog.count(DISTINCT supplied.candidate_id) FROM pg_catalog.unnest(p_candidate_ids) AS supplied(candidate_id)) THEN
    RAISE EXCEPTION 'WORKBENCH_PHYSICAL_CURRENTNESS_ARGUMENT_INVALID'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','WORKBENCH_PHYSICAL_CURRENTNESS_ARGUMENT_INVALID'
      )::text;
  END IF;

  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id=p_session_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_PHYSICAL_CURRENTNESS_SESSION_MISSING'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','WORKBENCH_PHYSICAL_CURRENTNESS_SESSION_MISSING','session_id',p_session_id
      )::text;
  END IF;

  FOR v_candidate_id IN
    SELECT supplied.candidate_id
    FROM pg_catalog.unnest(p_candidate_ids) AS supplied(candidate_id)
    ORDER BY supplied.candidate_id
  LOOP
    v_scope := NULL;
    v_state := NULL;
    v_registry := NULL;
    v_attestation := '{}'::jsonb;
    v_scope_ordinal := 0;
    v_source_count := 0;
    v_ready_count := 0;
    v_active_noncurrent_preview_count := 0;
    v_active_noncurrent_source_count := 0;
    v_active_noncurrent_line_work_count := 0;
    v_source_minus_preview_count := 0;
    v_preview_minus_source_count := 0;
    v_duplicate_active_identity_count := 0;
    v_source_run_count := 0;
    v_source_publication_count := 0;
    v_source_build_run_id := NULL::uuid;
    v_source_publication_id := NULL::uuid;
    v_live_source_identity_digest := NULL::text;
    v_live_preview_identity_digest := NULL::text;
    v_live_source_digest := NULL::text;
    v_selected_preview_count := 0;
    v_selected_session_count := 0;
    v_selection_consistent := false;
    v_attestation_current := false;
    v_active_owner_count := 0;
    v_active_owner_job_id := NULL::uuid;
    v_terminal_current := false;
    v_current_or_active_owner := false;

    SELECT scope_row.* INTO v_scope
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id=p_session_id AND scope_row.candidate_id=v_candidate_id;

    SELECT state_row.* INTO v_state
    FROM public.banking_pay_workbench_session_candidate_state AS state_row
    WHERE state_row.session_id=p_session_id AND state_row.candidate_id=v_candidate_id;

    SELECT registry_row.* INTO v_registry
    FROM private.banking_pay_workbench_candidate_scope_registry AS registry_row
    WHERE registry_row.candidate_id=v_candidate_id;

    v_attestation:=COALESCE(v_scope.certified_preview_publication_attestation_json,'{}'::jsonb);
    v_scope_ordinal:=COALESCE(v_scope.scope_ordinal,0);

    WITH current_source AS (
      SELECT
        public.pay_workbench_preview_section_from_line_json(source_row.source_row_json) AS section,
        source_row.line_key,
        source_row.source_ordinal,
        source_row.source_build_run_id,
        source_row.source_publication_id,
        source_row.source_row_json
      FROM public.banking_pay_workbench_candidate_source_lines AS source_row
      WHERE source_row.session_id=p_session_id
        AND source_row.candidate_id=v_candidate_id
        AND source_row.session_version=v_session.version
        AND source_row.source_change_seq=COALESCE(v_registry.current_source_change_seq,-1)
        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(source_row.status,'')))='CURRENT'
    ), ready_preview AS (
      SELECT preview_row.section,preview_row.row_key,
        (preview_row.row_ordinal-(v_scope_ordinal*1000000))::bigint AS source_ordinal
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.session_id=p_session_id
        AND preview_row.candidate_id=v_candidate_id
        AND preview_row.session_version=v_session.version
        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.status,'')))='READY'
    )
    SELECT
      (SELECT pg_catalog.count(*)::integer FROM current_source),
      (SELECT pg_catalog.count(*)::integer FROM ready_preview),
      (SELECT pg_catalog.count(DISTINCT source_build_run_id)::integer FROM current_source),
      (SELECT pg_catalog.count(DISTINCT source_publication_id)::integer FROM current_source),
      (SELECT pg_catalog.min(source_build_run_id::text)::uuid FROM current_source),
      (SELECT pg_catalog.min(source_publication_id::text)::uuid FROM current_source),
      (SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
        section||E'\x1f'||line_key||E'\x1f'||source_ordinal::text,E'\x1e'
        ORDER BY source_ordinal,section,line_key),'')) FROM current_source),
      (SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
        section||E'\x1f'||row_key||E'\x1f'||source_ordinal::text,E'\x1e'
        ORDER BY source_ordinal,section,row_key),'')) FROM ready_preview),
      (SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
        pg_catalog.md5(source_row_json::text),'' ORDER BY source_ordinal),'')) FROM current_source),
      (SELECT pg_catalog.count(*)::integer FROM (
        SELECT section,line_key,source_ordinal FROM current_source
        EXCEPT ALL
        SELECT section,row_key,source_ordinal FROM ready_preview
      ) AS source_diff),
      (SELECT pg_catalog.count(*)::integer FROM (
        SELECT section,row_key,source_ordinal FROM ready_preview
        EXCEPT ALL
        SELECT section,line_key,source_ordinal FROM current_source
      ) AS preview_diff),
      (SELECT COALESCE(pg_catalog.sum(duplicate_count-1),0)::integer FROM (
        SELECT pg_catalog.count(*)::integer AS duplicate_count
        FROM current_source GROUP BY section,line_key,source_ordinal HAVING pg_catalog.count(*)>1
        UNION ALL
        SELECT pg_catalog.count(*)::integer AS duplicate_count
        FROM ready_preview GROUP BY section,row_key,source_ordinal HAVING pg_catalog.count(*)>1
      ) AS duplicates)
    INTO v_source_count,v_ready_count,v_source_run_count,v_source_publication_count,
      v_source_build_run_id,v_source_publication_id,v_live_source_identity_digest,
      v_live_preview_identity_digest,v_live_source_digest,v_source_minus_preview_count,
      v_preview_minus_source_count,v_duplicate_active_identity_count;

    SELECT pg_catalog.count(*)::integer
    INTO v_active_noncurrent_preview_count
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id=p_session_id
      AND preview_row.candidate_id=v_candidate_id
      AND preview_row.session_version=v_session.version
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.status,''))) IN (
        'DIRTY','PENDING','DELTA_PENDING','RUNNING','QUEUED','ERROR','FAILED'
      );

    SELECT pg_catalog.count(*)::integer
    INTO v_active_noncurrent_source_count
    FROM public.banking_pay_workbench_candidate_source_lines AS source_row
    WHERE source_row.session_id=p_session_id
      AND source_row.candidate_id=v_candidate_id
      AND source_row.session_version=v_session.version
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(source_row.status,''))) IN (
        'DIRTY','PENDING','DELTA_PENDING','RUNNING','QUEUED','ERROR','FAILED'
      );

    SELECT pg_catalog.count(*)::integer
    INTO v_active_noncurrent_line_work_count
    FROM public.banking_pay_workbench_candidate_line_work AS line_work
    WHERE line_work.session_id=p_session_id
      AND line_work.candidate_id=v_candidate_id
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(line_work.status,''))) IN (
        'DIRTY','PENDING','READY','RUNNING','QUEUED','ERROR','FAILED'
      );

    SELECT pg_catalog.count(*)::integer,
           pg_catalog.min(owner_job.id::text)::uuid
    INTO v_active_owner_count,v_active_owner_job_id
    FROM public.banking_pay_workbench_jobs AS owner_job
    WHERE owner_job.session_id=p_session_id
      AND owner_job.candidate_id=v_candidate_id
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(owner_job.status,''))) IN ('QUEUED','RUNNING')
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(owner_job.job_type,''))) IN (
        'WORKBENCH_CANDIDATE_SOURCE_BUILD','WORKBENCH_CANDIDATE_DELTA_REFRESH'
      );

    SELECT pg_catalog.count(*)::integer
    INTO v_selected_preview_count
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id=p_session_id
      AND preview_row.candidate_id=v_candidate_id
      AND preview_row.session_version=v_session.version
      AND preview_row.status='READY'
      AND preview_row.selected IS TRUE
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.selection_state,'')))='SELECTED';

    SELECT pg_catalog.count(*)::integer
    INTO v_selected_session_count
    FROM pg_catalog.jsonb_array_elements_text(
      CASE WHEN pg_catalog.jsonb_typeof(COALESCE(v_session.server_selected_preview_row_ids,'[]'::jsonb))='array'
        THEN COALESCE(v_session.server_selected_preview_row_ids,'[]'::jsonb) ELSE '[]'::jsonb END
    ) AS selected_id(value)
    JOIN public.banking_pay_workbench_preview_rows AS preview_row
      ON selected_id.value=preview_row.id::text
     AND preview_row.session_id=p_session_id
     AND preview_row.candidate_id=v_candidate_id
     AND preview_row.session_version=v_session.version
     AND preview_row.status='READY';

    v_selection_consistent:=v_selected_preview_count=v_selected_session_count
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_preview_rows AS preview_row
        WHERE preview_row.session_id=p_session_id
          AND preview_row.candidate_id=v_candidate_id
          AND preview_row.session_version=v_session.version
          AND preview_row.status='READY'
          AND (
            preview_row.selected IS DISTINCT FROM (pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.selection_state,'')))='SELECTED')
            OR COALESCE((preview_row.row_json->>'selected')::boolean,false) IS DISTINCT FROM COALESCE(preview_row.selected,false)
            OR pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.row_json->>'selection_state','')))
                 IS DISTINCT FROM pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.selection_state,'')))
          )
      );

    v_attestation_current:=
      v_attestation->>'attestation_version'='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
      AND v_attestation->>'contract_version'='3'
      AND v_attestation->>'semantic_contract_version'='READY_TO_PAY_SEMANTIC_V2'
      AND COALESCE((v_attestation->>'semantic_ready')::boolean,false)
      AND COALESCE((v_attestation->>'parity_complete')::boolean,false)
      AND COALESCE(v_attestation->>'session_id','')=p_session_id::text
      AND COALESCE(v_attestation->>'candidate_id','')=v_candidate_id::text
      AND COALESCE(v_attestation->>'session_version','')~'^[0-9]{1,18}$'
      AND (v_attestation->>'session_version')::bigint=v_session.version
      AND COALESCE(v_attestation->>'source_change_seq','')~'^[0-9]{1,18}$'
      AND (v_attestation->>'source_change_seq')::bigint=COALESCE(v_registry.current_source_change_seq,-1)
      AND COALESCE(v_attestation->>'source_row_count','')~'^[0-9]{1,9}$'
      AND (v_attestation->>'source_row_count')::integer=v_source_count
      AND COALESCE(v_attestation->>'preview_row_count','')~'^[0-9]{1,9}$'
      AND (v_attestation->>'preview_row_count')::integer=v_ready_count
      AND v_attestation->>'source_build_run_id'=COALESCE(v_source_build_run_id::text,'')
      AND v_attestation->>'source_publication_id'=COALESCE(v_source_publication_id::text,'')
      AND v_attestation->>'source_identity_digest'=v_live_source_identity_digest
      AND v_attestation->>'preview_identity_digest'=v_live_preview_identity_digest
      AND v_attestation->>'source_digest'=v_live_source_digest;

    v_terminal_current:=
      pg_catalog.upper(pg_catalog.btrim(COALESCE(v_session.status,'')))='OPEN'
      AND v_session.discarded_at_utc IS NULL
      AND v_scope.session_id IS NOT NULL
      AND v_state.session_id IS NOT NULL
      AND v_registry.candidate_id IS NOT NULL
      AND COALESCE(v_registry.current_source_change_seq,-1)=COALESCE(v_state.source_change_seq,-2)
      AND COALESCE(v_registry.current_source_change_seq,-1)=COALESCE(v_scope.certified_preview_publication_source_change_seq,-2)
      AND v_state.session_version=v_session.version
      AND v_scope.certified_preview_publication_session_version=v_session.version
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(v_state.status,''))) IN ('READY','COMPLETE')
      AND v_state.pending_job_id IS NULL
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(v_scope.status,''))) IN ('MATERIALISED','READY','COMPLETE','SOURCE_EMPTY')
      AND v_scope.pending_job_id IS NULL
      AND v_scope.dirty IS FALSE
      AND v_scope.certified_preview_publication_required IS TRUE
      AND v_scope.certified_preview_publication_parity_ok IS TRUE
      AND v_source_count=v_ready_count
      AND v_source_run_count<=1
      AND v_source_publication_count<=1
      AND v_source_minus_preview_count=0
      AND v_preview_minus_source_count=0
      AND v_duplicate_active_identity_count=0
      AND v_active_noncurrent_preview_count=0
      AND v_active_noncurrent_source_count=0
      AND v_active_noncurrent_line_work_count=0
      AND v_active_owner_count=0
      AND v_selection_consistent
      AND v_attestation_current;

    v_current_or_active_owner:=v_terminal_current OR (
      v_allow_active_owner
      AND v_active_owner_count=1
      AND v_scope.pending_job_id=v_active_owner_job_id
      AND v_state.pending_job_id=v_active_owner_job_id
      AND v_scope.dirty IS TRUE
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(v_scope.status,'')))='SOURCE_BUILD_PENDING'
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(v_state.status,''))) IN ('PENDING','DIRTY')
    );

    v_reason:=CASE
      WHEN v_terminal_current THEN 'TERMINAL_CURRENT'
      WHEN v_active_noncurrent_preview_count>0 THEN 'ACTIVE_NONCURRENT_PREVIEW_ROWS'
      WHEN v_active_noncurrent_source_count>0 THEN 'ACTIVE_NONCURRENT_SOURCE_ROWS'
      WHEN v_active_noncurrent_line_work_count>0 THEN 'ACTIVE_NONCURRENT_LINE_WORK'
      WHEN v_source_minus_preview_count>0 OR v_preview_minus_source_count>0 THEN 'SOURCE_PREVIEW_IDENTITY_MISMATCH'
      WHEN v_duplicate_active_identity_count>0 THEN 'DUPLICATE_ACTIVE_IDENTITY'
      WHEN v_attestation_current IS NOT TRUE THEN 'ATTESTATION_NOT_CURRENT'
      WHEN v_selection_consistent IS NOT TRUE THEN 'SELECTION_STATE_MISMATCH'
      WHEN v_active_owner_count=1 AND v_current_or_active_owner THEN 'ACTIVE_OWNER'
      WHEN v_active_owner_count>0 THEN 'ACTIVE_OWNER_NOT_EXACT'
      ELSE 'TERMINAL_STATE_NOT_CURRENT'
    END;

    v_proof_digest:=pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
      pg_catalog.concat_ws('|','WORKBENCH_PHYSICAL_CURRENTNESS_V1',p_session_id::text,
        v_session.version::text,v_candidate_id::text,COALESCE(v_registry.current_source_change_seq,-1)::text,
        COALESCE(v_registry.dirty_generation,-1)::text,COALESCE(v_source_build_run_id::text,''),
        COALESCE(v_source_publication_id::text,''),v_source_count::text,v_ready_count::text,
        v_active_noncurrent_preview_count::text,v_active_noncurrent_source_count::text,
        v_active_noncurrent_line_work_count::text,v_source_minus_preview_count::text,
        v_preview_minus_source_count::text,COALESCE(v_live_source_identity_digest,''),
        COALESCE(v_live_preview_identity_digest,''),v_attestation_current::text,
        v_terminal_current::text,v_current_or_active_owner::text,v_reason),
      'UTF8'),'sha256'),'hex');

    v_results:=v_results||pg_catalog.jsonb_build_array(pg_catalog.jsonb_strip_nulls(
      pg_catalog.jsonb_build_object(
        'candidate_id',v_candidate_id,'terminal_current',v_terminal_current,
        'current_or_active_owner',v_current_or_active_owner,'currentness_reason',v_reason,
        'session_version',v_session.version,'session_status',v_session.status,
        'source_change_seq',v_registry.current_source_change_seq,
        'dirty_generation',v_registry.dirty_generation,'source_build_run_id',v_source_build_run_id,
        'candidate_state_status',v_state.status,
        'candidate_state_source_change_seq',v_state.source_change_seq,
        'candidate_state_pending_job_id',v_state.pending_job_id,
        'scope_status',v_scope.status,'scope_dirty',v_scope.dirty,
        'scope_pending_job_id',v_scope.pending_job_id,
        'scope_publication_required',v_scope.certified_preview_publication_required,
        'scope_publication_parity_ok',v_scope.certified_preview_publication_parity_ok,
        'scope_publication_source_change_seq',v_scope.certified_preview_publication_source_change_seq,
        'source_publication_id',v_source_publication_id,'current_source_count',v_source_count,
        'ready_preview_count',v_ready_count,'active_noncurrent_preview_count',v_active_noncurrent_preview_count,
        'active_noncurrent_source_count',v_active_noncurrent_source_count,
        'active_noncurrent_line_work_count',v_active_noncurrent_line_work_count,
        'source_minus_preview_count',v_source_minus_preview_count,
        'preview_minus_source_count',v_preview_minus_source_count,
        'duplicate_active_identity_count',v_duplicate_active_identity_count,
        'live_source_identity_digest',v_live_source_identity_digest,
        'live_preview_identity_digest',v_live_preview_identity_digest,
        'attested_source_identity_digest',v_attestation->>'source_identity_digest',
        'attested_preview_identity_digest',v_attestation->>'preview_identity_digest',
        'attestation_current',v_attestation_current,'selection_consistent',v_selection_consistent,
        'active_owner_count',v_active_owner_count,'active_owner_job_id',v_active_owner_job_id,
        'proof_digest',v_proof_digest
      )
    ));
    v_terminal_count:=v_terminal_count+CASE WHEN v_terminal_current THEN 1 ELSE 0 END;
    v_current_or_owner_count:=v_current_or_owner_count+CASE WHEN v_current_or_active_owner THEN 1 ELSE 0 END;
  END LOOP;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'contract_version','WORKBENCH_PHYSICAL_CURRENTNESS_V1','mode',v_mode,
    'session_id',p_session_id,'candidate_count',pg_catalog.cardinality(p_candidate_ids),
    'terminal_current_count',v_terminal_count,'current_or_active_owner_count',v_current_or_owner_count,
    'all_terminal_current',v_terminal_count=pg_catalog.cardinality(p_candidate_ids),
    'all_current_or_active_owner',v_current_or_owner_count=pg_catalog.cardinality(p_candidate_ids),
    'candidate_results',v_results
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_candidate_physical_currentness_page_v1(uuid,uuid[],text,jsonb)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_candidate_physical_currentness_page_v1(uuid,uuid[],text,jsonb)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_candidate_physical_currentness_page_v1(uuid,uuid[],text,jsonb)
  TO postgres;
