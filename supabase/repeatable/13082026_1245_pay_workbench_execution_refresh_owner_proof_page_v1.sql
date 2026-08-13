-- CloudTMS Banking Pay: bounded read-only proof that a temporarily absent
-- current publication is owned by one exact execution/V2 source-build chain.
--
-- This is a subordinate currentness proof only.  It never elects a payment
-- route, mutates Workbench state, or changes frozen post-Draft economics.

CREATE OR REPLACE FUNCTION private.pay_workbench_execution_refresh_owner_proof_page_v1(
  p_execution_operation_id uuid,
  p_pay_batch_id uuid,
  p_candidate_ids uuid[],
  p_mode text DEFAULT 'OBSERVE_ONLY'::text,
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
PARALLEL RESTRICTED
SECURITY DEFINER
SET search_path TO ''
AS $function$
DECLARE
  v_mode text:=pg_catalog.upper(pg_catalog.btrim(COALESCE(p_mode,'OBSERVE_ONLY')));
  v_results jsonb:='[]'::jsonb;
  v_count integer:=0;
BEGIN
  IF p_execution_operation_id IS NULL OR p_pay_batch_id IS NULL
     OR p_candidate_ids IS NULL OR pg_catalog.cardinality(p_candidate_ids)<1
     OR pg_catalog.cardinality(p_candidate_ids)>100
     OR v_mode NOT IN ('OBSERVE_ONLY','PRE_REQUEST')
     OR pg_catalog.jsonb_typeof(COALESCE(p_options_json,'{}'::jsonb))<>'object'
     OR EXISTS (
       SELECT 1
       FROM pg_catalog.jsonb_object_keys(COALESCE(p_options_json,'{}'::jsonb)) AS option_key(key)
       WHERE option_key.key NOT IN (
         'contract_version','expected_chain_digest_by_candidate','expected_owner_by_candidate'
       )
     )
     OR COALESCE(p_options_json->>'contract_version','1')<>'1'
     OR pg_catalog.jsonb_typeof(COALESCE(
       p_options_json->'expected_chain_digest_by_candidate','{}'::jsonb
     ))<>'object'
     OR pg_catalog.jsonb_typeof(COALESCE(
       p_options_json->'expected_owner_by_candidate','{}'::jsonb
     ))<>'object'
     OR EXISTS (
       SELECT 1 FROM pg_catalog.unnest(p_candidate_ids) AS supplied(candidate_id)
       WHERE supplied.candidate_id IS NULL
     )
     OR (SELECT pg_catalog.count(*) FROM pg_catalog.unnest(p_candidate_ids) AS supplied(candidate_id))
          IS DISTINCT FROM
        (SELECT pg_catalog.count(DISTINCT supplied.candidate_id)
         FROM pg_catalog.unnest(p_candidate_ids) AS supplied(candidate_id)) THEN
    RAISE EXCEPTION 'EXECUTION_REFRESH_OWNER_PROOF_ARGUMENT_INVALID'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','EXECUTION_REFRESH_OWNER_PROOF_ARGUMENT_INVALID','mode',v_mode
      )::text;
  END IF;

  WITH requested AS (
    SELECT supplied.candidate_id,pg_catalog.min(supplied.ordinality)::bigint AS ordinality
    FROM pg_catalog.unnest(p_candidate_ids) WITH ORDINALITY
      AS supplied(candidate_id,ordinality)
    GROUP BY supplied.candidate_id
  ), authority AS (
    SELECT requested.ordinality,requested.candidate_id,
      execution_operation.id AS execution_operation_id,
      execution_operation.status AS execution_operation_status,
      execution_operation.phase AS execution_operation_phase,
      execution_operation.pay_batch_id AS execution_operation_batch_id,
      COALESCE(
        execution_operation.progress_json->'execution_unsent_overlay_chain_v2'->'candidates'
          ->requested.candidate_id::text,
        execution_operation.result_json->'execution_unsent_overlay_chain_v2'->'candidates'
          ->requested.candidate_id::text
      ) AS chain_receipt,
      batch_row.source_workbench_session_id AS session_id,
      batch_row.source_snapshot_run_id,
      batch_row.source_session_version AS session_version,
      pg_catalog.upper(pg_catalog.btrim(COALESCE(batch_row.execution_commit_state,'NOT_SUBMITTED')))
        AS execution_commit_state,
      scope_row.status AS scope_status,scope_row.pending_job_id AS scope_pending_job_id,
      scope_row.dirty AS scope_dirty,
      scope_row.certified_preview_publication_parity_ok AS current_publication_parity,
      scope_row.certified_preview_publication_source_build_run_id AS current_source_build_run_id,
      scope_row.certified_preview_publication_source_publication_id AS current_source_publication_id,
      state_row.status AS candidate_status,state_row.pending_job_id AS state_pending_job_id,
      state_row.source_change_seq AS candidate_state_source_change_seq,
      state_row.session_version AS candidate_state_session_version,
      registry.current_build_id AS registry_current_build_id,
      registry.current_source_change_seq AS registry_source_change_seq,
      registry.dirty_generation AS registry_dirty_generation,
      COALESCE(change_counter.seq,0) AS live_source_change_seq,
      COALESCE(change_counter.scope_change_generation,0) AS live_dirty_generation,
      COALESCE(current_rows.current_source_count,0) AS current_source_count,
      COALESCE(current_rows.partial_current_publication_count,0)
        AS partial_current_publication_count
    FROM requested
    LEFT JOIN public.banking_pay_operations AS execution_operation
      ON execution_operation.id=p_execution_operation_id
     AND execution_operation.operation_type='PAYMENT_EXECUTE'
     AND execution_operation.pay_batch_id=p_pay_batch_id
    LEFT JOIN public.pay_batches AS batch_row ON batch_row.id=p_pay_batch_id
    LEFT JOIN public.banking_pay_workbench_session_scope AS scope_row
      ON scope_row.session_id=batch_row.source_workbench_session_id
     AND scope_row.candidate_id=requested.candidate_id
    LEFT JOIN public.banking_pay_workbench_session_candidate_state AS state_row
      ON state_row.session_id=batch_row.source_workbench_session_id
     AND state_row.candidate_id=requested.candidate_id
    LEFT JOIN private.banking_pay_workbench_candidate_scope_registry AS registry
      ON registry.candidate_id=requested.candidate_id
    LEFT JOIN public.app_change_counters AS change_counter
      ON change_counter.entity_key='pay_candidate:'||requested.candidate_id::text
    LEFT JOIN LATERAL (
      SELECT
        pg_catalog.count(*) FILTER (
          WHERE source_row.status='CURRENT'
        )::integer AS current_source_count,
        pg_catalog.count(DISTINCT source_row.source_publication_id) FILTER (
          WHERE source_row.status='CURRENT'
        )::integer AS partial_current_publication_count
      FROM public.banking_pay_workbench_candidate_source_lines AS source_row
      WHERE source_row.session_id=batch_row.source_workbench_session_id
        AND source_row.candidate_id=requested.candidate_id
    ) AS current_rows ON true
  ), owner_groups AS (
    SELECT authority.*,
      owner_build.id AS owner_economic_build_id,
      owner_build.source_build_run_id AS owner_source_build_run_id,
      owner_build.source_job_id AS owner_declared_root_job_id,
      owner_build.source_change_seq AS owner_source_change_seq,
      owner_build.captured_candidate_generation AS owner_dirty_generation,
      owner_build.authority_fingerprint AS owner_authority_fingerprint,
      owner_build.status AS owner_status,owner_build.private_stage AS owner_stage,
      root_job.id AS owner_root_job_id,root_job.payload_json AS owner_root_payload,
      root_context.context_json AS owner_execution_context,
      root_context.context_digest AS owner_context_digest,
      COALESCE(active_jobs.active_job_count,0) AS active_job_count,
      COALESCE(active_jobs.active_job_ids,'[]'::jsonb) AS active_job_ids,
      COALESCE(active_jobs.running_job_count,0) AS running_job_count,
      COALESCE(active_builds.active_economic_owner_count,0) AS active_economic_owner_count,
      COALESCE(active_builds.competing_owner_count,0) AS competing_owner_count,
      scope_pending.economic_build_id AS scope_pending_economic_build_id,
      state_pending.economic_build_id AS state_pending_economic_build_id,
      COALESCE(expected.expected_chain_digest,'') AS expected_chain_digest,
      expected.expected_owner_economic_build_id,
      EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(COALESCE(authority.chain_receipt->'transitions','[]'::jsonb))
          AS transition_row(value)
        JOIN LATERAL pg_catalog.jsonb_array_elements_text(
          COALESCE(transition_row.value->'context_digests','[]'::jsonb)
        ) AS context_digest(value) ON true
        WHERE context_digest.value=root_context.context_digest
      ) AS context_digest_in_chain
    FROM authority
    LEFT JOIN LATERAL (
      SELECT build_row.*
      FROM private.banking_pay_workbench_economic_builds AS build_row
      WHERE build_row.candidate_id=authority.candidate_id
        AND build_row.session_id=authority.session_id
        AND build_row.session_version=authority.session_version
        AND build_row.source_snapshot_run_id=authority.source_snapshot_run_id
        AND build_row.source_change_seq=CASE
          WHEN COALESCE(authority.chain_receipt->>'terminal_source_change_seq','')~'^[0-9]{1,18}$'
          THEN (authority.chain_receipt->>'terminal_source_change_seq')::bigint END
        AND build_row.captured_candidate_generation=CASE
          WHEN COALESCE(authority.chain_receipt->>'terminal_execution_generation','')~'^[0-9]{1,18}$'
          THEN (authority.chain_receipt->>'terminal_execution_generation')::bigint END
        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(build_row.status,''))) IN (
          'COLLECTING','READY_FOR_RECONCILIATION','RECONCILING','RECONCILED',
          'PUBLISHING','BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'
        )
        AND EXISTS (
          SELECT 1 FROM public.banking_pay_workbench_jobs AS active_job
          WHERE active_job.economic_build_id=build_row.id
            AND active_job.status IN ('QUEUED','RUNNING')
        )
      ORDER BY build_row.created_at_utc DESC,build_row.id DESC
      LIMIT 1
    ) AS owner_build ON true
    LEFT JOIN LATERAL (
      SELECT source_job.*
      FROM public.banking_pay_workbench_jobs AS source_job
      WHERE source_job.economic_build_id=owner_build.id
        AND source_job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND COALESCE(
          source_job.payload_json->>'source_build_run_id',
          source_job.payload_json#>>'{source_build,source_build_run_id}',''
        )=owner_build.source_build_run_id::text
      ORDER BY source_job.created_at_utc,source_job.id
      LIMIT 1
    ) AS root_job ON true
    LEFT JOIN LATERAL (
      SELECT context_choice.context_json,
             context_choice.context_json->>'context_digest' AS context_digest
      FROM (
        SELECT root_job.payload_json->'execution_overlay_schedule_contexts'
                 ->authority.candidate_id::text AS context_json,1 AS precedence
        UNION ALL
        SELECT root_job.payload_json->'execution_overlay_contexts'
                 ->authority.candidate_id::text AS context_json,2 AS precedence
      ) AS context_choice
      WHERE pg_catalog.jsonb_typeof(context_choice.context_json)='object'
      ORDER BY context_choice.precedence
      LIMIT 1
    ) AS root_context ON true
    LEFT JOIN LATERAL (
      SELECT pg_catalog.count(*)::integer AS active_job_count,
        pg_catalog.count(*) FILTER (WHERE active_job.status='RUNNING')::integer
          AS running_job_count,
        COALESCE(pg_catalog.jsonb_agg(active_job.id::text ORDER BY
          CASE WHEN active_job.status='RUNNING' THEN 0 ELSE 1 END,
          active_job.run_at_utc,active_job.created_at_utc,active_job.id),'[]'::jsonb)
          AS active_job_ids
      FROM (
        SELECT bounded_job.*
        FROM public.banking_pay_workbench_jobs AS bounded_job
        WHERE bounded_job.economic_build_id=owner_build.id
          AND bounded_job.status IN ('QUEUED','RUNNING')
        ORDER BY bounded_job.created_at_utc,bounded_job.id
        LIMIT 17
      ) AS active_job
    ) AS active_jobs ON true
    LEFT JOIN LATERAL (
      SELECT pg_catalog.count(*)::integer AS active_economic_owner_count,
        pg_catalog.count(*) FILTER (WHERE active_build.id IS DISTINCT FROM owner_build.id)::integer
          AS competing_owner_count
      FROM (
        SELECT build_row.id
        FROM private.banking_pay_workbench_economic_builds AS build_row
        WHERE build_row.candidate_id=authority.candidate_id
          AND build_row.session_id=authority.session_id
          AND build_row.session_version=authority.session_version
          AND pg_catalog.upper(pg_catalog.btrim(COALESCE(build_row.status,''))) IN (
            'COLLECTING','READY_FOR_RECONCILIATION','RECONCILING','RECONCILED',
            'PUBLISHING','BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'
          )
          AND EXISTS (
            SELECT 1 FROM public.banking_pay_workbench_jobs AS active_job
            WHERE active_job.economic_build_id=build_row.id
              AND active_job.status IN ('QUEUED','RUNNING')
          )
        ORDER BY build_row.created_at_utc DESC,build_row.id DESC
        LIMIT 2
      ) AS active_build
    ) AS active_builds ON true
    LEFT JOIN public.banking_pay_workbench_jobs AS scope_pending
      ON scope_pending.id=authority.scope_pending_job_id
    LEFT JOIN public.banking_pay_workbench_jobs AS state_pending
      ON state_pending.id=authority.state_pending_job_id
    LEFT JOIN LATERAL (
      SELECT
        p_options_json->'expected_chain_digest_by_candidate'
          ->>authority.candidate_id::text AS expected_chain_digest,
        CASE WHEN COALESCE(p_options_json->'expected_owner_by_candidate'
          ->>authority.candidate_id::text,'')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (p_options_json->'expected_owner_by_candidate'
            ->>authority.candidate_id::text)::uuid END AS expected_owner_economic_build_id
    ) AS expected ON true
  ), classified AS (
    SELECT owner_groups.*,
      CASE
        WHEN execution_operation_id IS NULL OR execution_operation_batch_id IS DISTINCT FROM p_pay_batch_id
          OR execution_operation_status<>'COMPLETE' OR execution_operation_phase<>'COMPLETE'
          THEN 'EXECUTION_REFRESH_OWNER_OPERATION_MISMATCH'
        WHEN COALESCE(chain_receipt->>'contract_version','')<>'EXECUTION_UNSENT_OVERLAY_CHAIN_V2'
          OR pg_catalog.lower(pg_catalog.btrim(COALESCE(chain_receipt->>'closed','')))
               NOT IN ('true','t','1','yes','y','on')
          THEN 'EXECUTION_REFRESH_OWNER_CHAIN_NOT_CLOSED'
        WHEN COALESCE(chain_receipt->>'execution_operation_id','')<>p_execution_operation_id::text
          OR COALESCE(chain_receipt->>'pay_batch_id','')<>p_pay_batch_id::text
          OR COALESCE(chain_receipt->>'candidate_id','')<>candidate_id::text
          OR COALESCE(CASE
            WHEN COALESCE(chain_receipt->>'transition_count','')~'^[0-9]{1,2}$'
            THEN (chain_receipt->>'transition_count')::integer END,0) NOT BETWEEN 1 AND 16
          THEN 'EXECUTION_REFRESH_OWNER_CHAIN_SCOPE_MISMATCH'
        WHEN execution_commit_state<>'NOT_SUBMITTED'
          OR COALESCE(chain_receipt->>'execution_commit_state','')<>'NOT_SUBMITTED'
          OR COALESCE(CASE WHEN COALESCE(chain_receipt->>'provider_attempt_count','')~'^[0-9]{1,9}$'
            THEN (chain_receipt->>'provider_attempt_count')::integer END,-1)<>0
          OR COALESCE(CASE WHEN COALESCE(chain_receipt->>'rail_transaction_count','')~'^[0-9]{1,9}$'
            THEN (chain_receipt->>'rail_transaction_count')::integer END,-1)<>0
          OR COALESCE(CASE WHEN COALESCE(chain_receipt->>'settlement_count','')~'^[0-9]{1,9}$'
            THEN (chain_receipt->>'settlement_count')::integer END,-1)<>0
          OR COALESCE(CASE WHEN COALESCE(chain_receipt->>'remittance_count','')~'^[0-9]{1,9}$'
            THEN (chain_receipt->>'remittance_count')::integer END,-1)<>0
          THEN 'EXECUTION_REFRESH_OWNER_MONEY_MOVEMENT_FENCE'
        WHEN expected_chain_digest<>''
          AND expected_chain_digest IS DISTINCT FROM chain_receipt->>'chain_digest'
          THEN 'EXECUTION_REFRESH_OWNER_EXPECTED_CHAIN_MISMATCH'
        WHEN live_source_change_seq IS DISTINCT FROM
          CASE WHEN COALESCE(chain_receipt->>'terminal_source_change_seq','')~'^[0-9]{1,18}$'
            THEN (chain_receipt->>'terminal_source_change_seq')::bigint END
          OR live_dirty_generation IS DISTINCT FROM
          CASE WHEN COALESCE(chain_receipt->>'terminal_execution_generation','')~'^[0-9]{1,18}$'
            THEN (chain_receipt->>'terminal_execution_generation')::bigint END
          OR registry_source_change_seq IS DISTINCT FROM live_source_change_seq
          OR registry_dirty_generation IS DISTINCT FROM live_dirty_generation
          OR candidate_state_source_change_seq IS DISTINCT FROM live_source_change_seq
          OR candidate_state_session_version IS DISTINCT FROM session_version
          THEN 'EXECUTION_REFRESH_OWNER_LIVE_AUTHORITY_MISMATCH'
        WHEN current_source_publication_id IS NOT NULL OR current_source_build_run_id IS NOT NULL
          OR current_source_count<>0 OR partial_current_publication_count<>0
          OR current_publication_parity IS TRUE
          THEN 'EXECUTION_REFRESH_OWNER_PARTIAL_OR_CURRENT_PUBLICATION'
        WHEN pg_catalog.upper(pg_catalog.btrim(COALESCE(scope_status,'')))<>'SOURCE_BUILD_PENDING'
          OR pg_catalog.upper(pg_catalog.btrim(COALESCE(candidate_status,'')))<>'PENDING'
          OR scope_pending_job_id IS NULL OR state_pending_job_id IS NULL
          THEN 'EXECUTION_REFRESH_OWNER_TRANSIENT_SHAPE_MISMATCH'
        WHEN owner_economic_build_id IS NULL OR owner_source_build_run_id IS NULL
          OR owner_root_job_id IS NULL OR active_job_count<1 OR active_job_count>16
          THEN 'EXECUTION_REFRESH_OWNER_CANONICAL_OWNER_MISSING'
        WHEN active_economic_owner_count<>1 OR competing_owner_count<>0
          THEN 'EXECUTION_REFRESH_OWNER_COMPETING_OWNER'
        WHEN scope_pending_economic_build_id IS DISTINCT FROM owner_economic_build_id
          OR state_pending_economic_build_id IS DISTINCT FROM owner_economic_build_id
          OR registry_current_build_id IS DISTINCT FROM owner_economic_build_id
          THEN 'EXECUTION_REFRESH_OWNER_PENDING_POINTER_MISMATCH'
        WHEN owner_source_change_seq IS DISTINCT FROM live_source_change_seq
          OR owner_dirty_generation IS DISTINCT FROM live_dirty_generation
          OR COALESCE(owner_root_payload->>'authority_fingerprint','')
               IS DISTINCT FROM COALESCE(owner_authority_fingerprint,'')
          OR COALESCE(owner_root_payload->>'source_build_run_id',
               owner_root_payload#>>'{source_build,source_build_run_id}','')
               IS DISTINCT FROM owner_source_build_run_id::text
          THEN 'EXECUTION_REFRESH_OWNER_FINGERPRINT_MISMATCH'
        WHEN pg_catalog.jsonb_typeof(owner_execution_context)<>'object'
          OR COALESCE(owner_execution_context->>'execution_operation_id','')
               <>p_execution_operation_id::text
          OR COALESCE(owner_execution_context->>'pay_batch_id','')<>p_pay_batch_id::text
          OR COALESCE(owner_execution_context->>'candidate_id','')<>candidate_id::text
          OR COALESCE(owner_execution_context->>'source_workbench_session_id','')
               <>COALESCE(session_id::text,'')
          OR COALESCE(owner_execution_context->>'source_snapshot_run_id','')
               <>COALESCE(source_snapshot_run_id::text,'')
          OR COALESCE(owner_execution_context->>'source_session_version','')
               <>COALESCE(session_version::text,'')
          OR NULLIF(COALESCE(owner_context_digest,''),'') IS NULL
          OR context_digest_in_chain IS NOT TRUE
          THEN 'EXECUTION_REFRESH_OWNER_CAUSAL_PROVENANCE_MISMATCH'
        WHEN expected_owner_economic_build_id IS NOT NULL
          AND expected_owner_economic_build_id IS DISTINCT FROM owner_economic_build_id
          THEN 'EXECUTION_REFRESH_OWNER_EXPECTED_OWNER_MISMATCH'
        ELSE NULL
      END AS rejection_reason
    FROM owner_groups
  ), result_rows AS (
    SELECT classified.ordinality,pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
      'candidate_id',classified.candidate_id,
      'bridge_admitted',classified.rejection_reason IS NULL,
      'admitted',classified.rejection_reason IS NULL,
      'currentness_basis','EXACT_UNSENT_EXECUTION_REFRESH_OWNER',
      'rejection_reason',classified.rejection_reason,
      'execution_operation_id',classified.execution_operation_id,
      'pay_batch_id',p_pay_batch_id,
      'execution_overlay_chain_digest',classified.chain_receipt->>'chain_digest',
      'terminal_source_change_seq',classified.chain_receipt->'terminal_source_change_seq',
      'terminal_dirty_generation',classified.chain_receipt->'terminal_execution_generation',
      'scope_status',classified.scope_status,'scope_dirty',classified.scope_dirty,
      'candidate_status',classified.candidate_status,
      'current_source_count',classified.current_source_count,
      'owner_economic_build_id',classified.owner_economic_build_id,
      'owner_source_build_run_id',classified.owner_source_build_run_id,
      'owner_root_job_id',classified.owner_root_job_id,
      'owner_active_job_ids',classified.active_job_ids,
      'owner_authority_fingerprint',classified.owner_authority_fingerprint,
      'owner_source_change_seq',classified.owner_source_change_seq,
      'owner_dirty_generation',classified.owner_dirty_generation,
      'owner_status',classified.owner_status,'owner_stage',classified.owner_stage,
      'competing_owner_count',classified.competing_owner_count,
      'partial_publication_count',classified.partial_current_publication_count,
      'context_digest',classified.owner_context_digest,
      'proof_digest',pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
        pg_catalog.concat_ws('|','EXECUTION_REFRESH_OWNER_PROOF_V1',
          p_execution_operation_id::text,p_pay_batch_id::text,classified.candidate_id::text,
          COALESCE(classified.chain_receipt->>'chain_digest',''),
          COALESCE(classified.owner_economic_build_id::text,''),
          COALESCE(classified.owner_source_build_run_id::text,''),
          COALESCE(classified.owner_authority_fingerprint,''),
          COALESCE(classified.owner_context_digest,''),
          COALESCE(classified.owner_source_change_seq,-1)::text,
          COALESCE(classified.owner_dirty_generation,-1)::text,
          (classified.rejection_reason IS NULL)::text,COALESCE(classified.rejection_reason,'')),
        'UTF8'),'sha256'),'hex')
    )) AS result_json
    FROM classified
  )
  SELECT COALESCE(pg_catalog.jsonb_agg(result_json ORDER BY ordinality),'[]'::jsonb),
         pg_catalog.count(*)::integer
  INTO v_results,v_count
  FROM result_rows;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'contract_version','EXECUTION_REFRESH_OWNER_PROOF_PAGE_V1',
    'mode',v_mode,'execution_operation_id',p_execution_operation_id,
    'pay_batch_id',p_pay_batch_id,'candidate_count',v_count,
    'admitted_count',(SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_array_elements(v_results) AS r(value)
      WHERE COALESCE((r.value->>'admitted')::boolean,false)),
    'candidate_results',v_results,
    'policy_x_authority_scope','POST_DRAFT_FROZEN_PAYMENT_PROOF_PLUS_EXACT_V2_EXECUTION_REFRESH_OWNER'
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_execution_refresh_owner_proof_page_v1(uuid,uuid,uuid[],text,jsonb)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_execution_refresh_owner_proof_page_v1(uuid,uuid,uuid[],text,jsonb)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_execution_refresh_owner_proof_page_v1(uuid,uuid,uuid[],text,jsonb)
  TO postgres;
