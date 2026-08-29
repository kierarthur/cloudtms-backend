-- Banking Pay invalid source-authority owner repair.
--
-- Historical queued source-build jobs can predate the current source-owner
-- fingerprint contract.  Such a row can never be claimed safely: the claim
-- authority rejects it before an economic build is created.  This bounded
-- pre-claim repair terminalises only those proved-invalid queued rows under
-- the existing Candidate serial authority and then delegates replacement
-- ownership to the established canonical owner-repair/enqueue functions.
-- No financial source, selection, Draft, provider, payment or settlement
-- authority is read or changed here.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION public.pay_workbench_repair_invalid_source_authority_jobs_v1(
  p_session_id uuid DEFAULT NULL::uuid,
  p_candidate_id uuid DEFAULT NULL::uuid,
  p_limit integer DEFAULT 10,
  p_reason text DEFAULT 'INVALID_SOURCE_AUTHORITY_OWNER_REPAIR'::text
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_now timestamptz:=pg_catalog.clock_timestamp();
  v_limit integer:=LEAST(GREATEST(COALESCE(p_limit,10),1),25);
  v_reason text:=COALESCE(NULLIF(pg_catalog.btrim(COALESCE(p_reason,'')),''),
    'INVALID_SOURCE_AUTHORITY_OWNER_REPAIR');
  v_same_authority_election_enabled boolean:=false;
  v_candidate record;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_state public.banking_pay_workbench_session_candidate_state%ROWTYPE;
  v_owner public.banking_pay_workbench_jobs%ROWTYPE;
  v_repair_result jsonb:='{}'::jsonb;
  v_live_change_seq bigint:=0;
  v_terminalised integer:=0;
  v_total_terminalised integer:=0;
  v_examined integer:=0;
  v_repaired integer:=0;
  v_skipped integer:=0;
  v_failed integer:=0;
  v_remaining_invalid_active integer:=0;
  v_results jsonb:='[]'::jsonb;
  v_owner_valid boolean:=false;
  v_scope_current boolean:=false;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  SELECT COALESCE(
    (pg_catalog.to_jsonb(settings_row)->>'banking_pay_same_authority_build_election_v1_enabled')::boolean,
    false
  )
  INTO v_same_authority_election_enabled
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id=1;

  FOR v_candidate IN
    SELECT DISTINCT invalid_job.session_id,invalid_job.candidate_id
    FROM public.banking_pay_workbench_jobs AS invalid_job
    JOIN public.banking_pay_workbench_sessions AS open_session
      ON open_session.id=invalid_job.session_id
    JOIN public.banking_pay_workbench_session_scope AS current_scope
      ON current_scope.session_id=invalid_job.session_id
     AND current_scope.candidate_id=invalid_job.candidate_id
    WHERE invalid_job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND invalid_job.status='QUEUED'
      AND invalid_job.economic_build_id IS NULL
      AND invalid_job.candidate_id IS NOT NULL
      AND invalid_job.session_id IS NOT NULL
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(open_session.status,'')))='OPEN'
      AND open_session.discarded_at_utc IS NULL
      AND (p_session_id IS NULL OR invalid_job.session_id=p_session_id)
      AND (p_candidate_id IS NULL OR invalid_job.candidate_id=p_candidate_id)
      AND (
        COALESCE(invalid_job.payload_json->>'source_build_run_id','')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR (
          v_same_authority_election_enabled
          AND (
            COALESCE(invalid_job.payload_json->>'authority_fingerprint_version','') NOT IN ('2','3')
            OR COALESCE(invalid_job.payload_json->>'authority_fingerprint','') !~ '^[0-9a-f]{64}$'
            OR (
              COALESCE(invalid_job.payload_json->>'authority_fingerprint_version','')='3'
              AND COALESCE(invalid_job.payload_json->>'required_physical_publication_contract_version','')<>''
              AND NOT CASE
                WHEN COALESCE(invalid_job.payload_json->>'required_physical_publication_contract_version','')
                    ~ '^[[:space:]]*[+-]?[0-9]+[[:space:]]*$'
                  THEN (invalid_job.payload_json->>'required_physical_publication_contract_version')::numeric
                    IN (0,1)
                ELSE false
              END
            )
          )
        )
      )
    ORDER BY invalid_job.session_id,invalid_job.candidate_id
    LIMIT v_limit
  LOOP
    v_examined:=v_examined+1;
    BEGIN
      IF NOT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended(
        public._pay_workbench_candidate_serial_key(v_candidate.candidate_id),24062027
      )) THEN
        v_skipped:=v_skipped+1;
        v_results:=v_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'session_id',v_candidate.session_id::text,
          'candidate_id',v_candidate.candidate_id::text,
          'action','SKIPPED_CANDIDATE_SERIAL_BUSY'
        ));
        CONTINUE;
      END IF;

      PERFORM 1
      FROM public.candidates AS candidate_row
      WHERE candidate_row.id=v_candidate.candidate_id
      FOR UPDATE;

      PERFORM 1
      FROM private.banking_pay_workbench_candidate_scope_registry AS registry_row
      WHERE registry_row.candidate_id=v_candidate.candidate_id
      FOR UPDATE;

      SELECT session_row.* INTO STRICT v_session
      FROM public.banking_pay_workbench_sessions AS session_row
      WHERE session_row.id=v_candidate.session_id
        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(session_row.status,'')))='OPEN'
        AND session_row.discarded_at_utc IS NULL
      FOR UPDATE;

      SELECT scope_row.* INTO STRICT v_scope
      FROM public.banking_pay_workbench_session_scope AS scope_row
      WHERE scope_row.session_id=v_candidate.session_id
        AND scope_row.candidate_id=v_candidate.candidate_id
      FOR UPDATE;

      UPDATE public.banking_pay_workbench_jobs AS invalid_job
      SET status='DEAD',
          completed_at_utc=NULL::timestamptz,
          failed_at_utc=COALESCE(invalid_job.failed_at_utc,v_now),
          updated_at_utc=v_now,
          private_stage=CASE
            WHEN invalid_job.economic_build_id IS NULL
             AND invalid_job.private_stage='BUILD_INITIALISE' THEN NULL::text
            ELSE invalid_job.private_stage END,
          private_cursor_kind=CASE
            WHEN invalid_job.economic_build_id IS NULL
             AND invalid_job.private_stage='BUILD_INITIALISE' THEN NULL::text
            ELSE invalid_job.private_cursor_kind END,
          private_cursor_json=CASE
            WHEN invalid_job.economic_build_id IS NULL
             AND invalid_job.private_stage='BUILD_INITIALISE' THEN '{}'::jsonb
            ELSE invalid_job.private_cursor_json END,
          private_stage_version=CASE
            WHEN invalid_job.economic_build_id IS NULL
             AND invalid_job.private_stage='BUILD_INITIALISE' THEN NULL::integer
            ELSE invalid_job.private_stage_version END,
          last_error_json=pg_catalog.jsonb_build_object(
            'code','SOURCE_BUILD_AUTHORITY_PAYLOAD_INVALID_TERMINALISED',
            'message','Historical candidate refresh work was replaced by the current canonical owner.',
            'repair_reason',v_reason,
            'repaired_at_utc',v_now::text
          ),
          payload_json=COALESCE(invalid_job.payload_json,'{}'::jsonb)
            || pg_catalog.jsonb_build_object(
              'invalid_source_authority_terminalised',true,
              'invalid_source_authority_terminalised_at_utc',v_now::text,
              'invalid_source_authority_repair_reason',v_reason
            )
      WHERE invalid_job.session_id=v_candidate.session_id
        AND invalid_job.candidate_id=v_candidate.candidate_id
        AND invalid_job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND invalid_job.status='QUEUED'
        AND invalid_job.economic_build_id IS NULL
        AND (
          COALESCE(invalid_job.payload_json->>'source_build_run_id','')
            !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          OR (
            v_same_authority_election_enabled
            AND (
              COALESCE(invalid_job.payload_json->>'authority_fingerprint_version','') NOT IN ('2','3')
              OR COALESCE(invalid_job.payload_json->>'authority_fingerprint','') !~ '^[0-9a-f]{64}$'
              OR (
                COALESCE(invalid_job.payload_json->>'authority_fingerprint_version','')='3'
                AND COALESCE(invalid_job.payload_json->>'required_physical_publication_contract_version','')<>''
                AND NOT CASE
                  WHEN COALESCE(invalid_job.payload_json->>'required_physical_publication_contract_version','')
                      ~ '^[[:space:]]*[+-]?[0-9]+[[:space:]]*$'
                    THEN (invalid_job.payload_json->>'required_physical_publication_contract_version')::numeric
                      IN (0,1)
                  ELSE false
                END
              )
            )
          )
        );
      GET DIAGNOSTICS v_terminalised=ROW_COUNT;

      IF v_terminalised<1 THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_INVALID_SOURCE_AUTHORITY_REPAIR_TARGET_NOT_PROVEN'
          USING ERRCODE='40001';
      END IF;

      v_repair_result:=public.pay_workbench_repair_orphaned_pending_source_build(
        p_session_id=>v_candidate.session_id,
        p_candidate_id=>v_candidate.candidate_id,
        p_limit=>1,
        p_now_utc=>v_now,
        p_reason=>'INVALID_SOURCE_AUTHORITY_CANONICAL_OWNER_REPAIR'
      );

      IF pg_catalog.jsonb_typeof(v_repair_result) IS DISTINCT FROM 'object'
         OR pg_catalog.lower(pg_catalog.btrim(COALESCE(v_repair_result->>'ok','false'))) <> 'true'
         OR COALESCE((v_repair_result->>'unresolved_count')::integer,0)<>0
         OR COALESCE((v_repair_result->>'progress_recompute_failed_count')::integer,0)<>0
         OR pg_catalog.lower(pg_catalog.btrim(COALESCE(
              v_repair_result->>'all_state_transitions_proven','false'
            ))) <> 'true' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_INVALID_SOURCE_AUTHORITY_OWNER_REPAIR_NOT_PROVEN'
          USING ERRCODE='40001';
      END IF;

      SELECT scope_row.* INTO STRICT v_scope
      FROM public.banking_pay_workbench_session_scope AS scope_row
      WHERE scope_row.session_id=v_candidate.session_id
        AND scope_row.candidate_id=v_candidate.candidate_id
      FOR UPDATE;

      v_scope_current:=pg_catalog.upper(pg_catalog.btrim(COALESCE(v_scope.status,''))) IN (
          'SOURCE_READY','LINE_WORK_PENDING','MATERIALISED','MATERIALIZED','READY','SOURCE_EMPTY'
        )
        AND v_scope.pending_job_id IS NULL
        AND COALESCE(v_scope.dirty,false) IS NOT TRUE
        AND v_scope.error_json IS NULL;
      v_owner_valid:=v_scope_current;

      IF NOT v_owner_valid AND v_scope.pending_job_id IS NOT NULL THEN
        v_owner:=NULL;
        SELECT owner_job.* INTO v_owner
        FROM public.banking_pay_workbench_jobs AS owner_job
        WHERE owner_job.id=v_scope.pending_job_id
          AND owner_job.session_id=v_candidate.session_id
          AND owner_job.candidate_id=v_candidate.candidate_id
          AND owner_job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
          AND owner_job.status IN ('QUEUED','RUNNING')
        FOR UPDATE;

        SELECT COALESCE(change_counter.seq,0)
        INTO v_live_change_seq
        FROM public.app_change_counters AS change_counter
        WHERE change_counter.entity_key='pay_candidate:' || v_candidate.candidate_id::text;
        v_live_change_seq:=COALESCE(v_live_change_seq,0);

        v_owner_valid:=v_owner.id IS NOT NULL
          AND (CASE
            WHEN COALESCE(v_owner.payload_json->>'session_version','') ~ '^[0-9]{1,18}$'
              THEN (v_owner.payload_json->>'session_version')::bigint
            ELSE NULL::bigint END)=v_session.version
          AND (CASE
            WHEN COALESCE(v_owner.payload_json->>'source_change_seq','') ~ '^[0-9]{1,18}$'
              THEN (v_owner.payload_json->>'source_change_seq')::bigint
            ELSE NULL::bigint END)>=v_live_change_seq
          AND COALESCE(v_owner.payload_json->>'source_build_run_id','') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND COALESCE(v_owner.payload_json->>'created_by_helper','')=
            'pay_workbench_enqueue_candidate_refresh'
          AND (
            NOT v_same_authority_election_enabled
            OR (
              COALESCE(v_owner.payload_json->>'authority_fingerprint_version','') IN ('2','3')
              AND COALESCE(v_owner.payload_json->>'authority_fingerprint','') ~ '^[0-9a-f]{64}$'
              AND (
                COALESCE(v_owner.payload_json->>'authority_fingerprint_version','')<>'3'
                OR COALESCE(v_owner.payload_json->>'required_physical_publication_contract_version','')=''
                OR CASE
                  WHEN COALESCE(v_owner.payload_json->>'required_physical_publication_contract_version','')
                      ~ '^[[:space:]]*[+-]?[0-9]+[[:space:]]*$'
                    THEN (v_owner.payload_json->>'required_physical_publication_contract_version')::numeric
                      IN (0,1)
                  ELSE false
                END
              )
            )
          );

        IF v_owner_valid THEN
          v_state:=NULL;
          SELECT state_row.* INTO v_state
          FROM public.banking_pay_workbench_session_candidate_state AS state_row
          WHERE state_row.session_id=v_candidate.session_id
            AND state_row.candidate_id=v_candidate.candidate_id
          FOR UPDATE;
          v_owner_valid:=v_state.session_id IS NOT NULL
            AND pg_catalog.upper(pg_catalog.btrim(COALESCE(v_state.status,'')))='PENDING'
            AND v_state.pending_job_id=v_owner.id
            AND v_state.session_version=v_session.version
            AND v_state.source_change_seq>=v_live_change_seq
            AND pg_catalog.upper(pg_catalog.btrim(COALESCE(v_scope.status,'')))='SOURCE_BUILD_PENDING'
            AND COALESCE(v_scope.dirty,false) IS TRUE
            AND v_scope.error_json IS NULL;
        END IF;
      END IF;

      IF v_owner_valid IS NOT TRUE OR EXISTS(
        SELECT 1
        FROM public.banking_pay_workbench_jobs AS remaining_invalid
        WHERE remaining_invalid.session_id=v_candidate.session_id
          AND remaining_invalid.candidate_id=v_candidate.candidate_id
          AND remaining_invalid.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
          AND remaining_invalid.status IN ('QUEUED','RUNNING')
          AND remaining_invalid.economic_build_id IS NULL
          AND (
            COALESCE(remaining_invalid.payload_json->>'source_build_run_id','')
              !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            OR (
              v_same_authority_election_enabled
              AND (
                COALESCE(remaining_invalid.payload_json->>'authority_fingerprint_version','') NOT IN ('2','3')
                OR COALESCE(remaining_invalid.payload_json->>'authority_fingerprint','') !~ '^[0-9a-f]{64}$'
                OR (
                  COALESCE(remaining_invalid.payload_json->>'authority_fingerprint_version','')='3'
                  AND COALESCE(remaining_invalid.payload_json->>'required_physical_publication_contract_version','')<>''
                  AND NOT CASE
                    WHEN COALESCE(remaining_invalid.payload_json->>'required_physical_publication_contract_version','')
                        ~ '^[[:space:]]*[+-]?[0-9]+[[:space:]]*$'
                      THEN (remaining_invalid.payload_json->>'required_physical_publication_contract_version')::numeric
                        IN (0,1)
                    ELSE false
                  END
                )
              )
            )
          )
      ) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_INVALID_SOURCE_AUTHORITY_REPAIR_POSTCONDITION_FAILED'
          USING ERRCODE='40001';
      END IF;

      PERFORM public._audit_insert(
        'banking_pay_workbench_session_scope',
        v_candidate.candidate_id::text,
        'INVALID_SOURCE_AUTHORITY_OWNER_REPAIRED',
        pg_catalog.jsonb_build_object(
          'session_id',v_candidate.session_id::text,
          'candidate_id',v_candidate.candidate_id::text,
          'terminalised_job_count',v_terminalised
        ),
        pg_catalog.jsonb_build_object(
          'session_id',v_candidate.session_id::text,
          'candidate_id',v_candidate.candidate_id::text,
          'canonical_pending_job_id',CASE
            WHEN v_scope.pending_job_id IS NULL THEN NULL::text
            ELSE v_scope.pending_job_id::text END,
          'current_terminal_authority',v_scope_current,
          'state_transition_proven',true,
          'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
        ),
        v_reason,
        v_session.actor_user_id
      );

      v_total_terminalised:=v_total_terminalised+v_terminalised;
      v_repaired:=v_repaired+1;
      v_results:=v_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'session_id',v_candidate.session_id::text,
        'candidate_id',v_candidate.candidate_id::text,
        'action','REPAIRED_CANONICAL_OWNER',
        'terminalised_job_count',v_terminalised,
        'canonical_pending_job_id',CASE
          WHEN v_scope.pending_job_id IS NULL THEN NULL::text
          ELSE v_scope.pending_job_id::text END,
        'current_terminal_authority',v_scope_current
      ));
    EXCEPTION WHEN OTHERS THEN
      v_failed:=v_failed+1;
      v_results:=v_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'session_id',v_candidate.session_id::text,
        'candidate_id',v_candidate.candidate_id::text,
        'action','UNRESOLVED_POSTCONDITION_NOT_PROVEN',
        'error_state',SQLSTATE
      ));
    END;
  END LOOP;

  SELECT COUNT(*)::integer
  INTO v_remaining_invalid_active
  FROM public.banking_pay_workbench_jobs AS invalid_job
  JOIN public.banking_pay_workbench_sessions AS open_session
    ON open_session.id=invalid_job.session_id
  WHERE invalid_job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
    AND invalid_job.status IN ('QUEUED','RUNNING')
    AND invalid_job.economic_build_id IS NULL
    AND invalid_job.candidate_id IS NOT NULL
    AND invalid_job.session_id IS NOT NULL
    AND pg_catalog.upper(pg_catalog.btrim(COALESCE(open_session.status,'')))='OPEN'
    AND open_session.discarded_at_utc IS NULL
    AND (p_session_id IS NULL OR invalid_job.session_id=p_session_id)
    AND (p_candidate_id IS NULL OR invalid_job.candidate_id=p_candidate_id)
    AND (
      COALESCE(invalid_job.payload_json->>'source_build_run_id','')
        !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      OR (
        v_same_authority_election_enabled
        AND (
          COALESCE(invalid_job.payload_json->>'authority_fingerprint_version','') NOT IN ('2','3')
          OR COALESCE(invalid_job.payload_json->>'authority_fingerprint','') !~ '^[0-9a-f]{64}$'
          OR (
            COALESCE(invalid_job.payload_json->>'authority_fingerprint_version','')='3'
            AND COALESCE(invalid_job.payload_json->>'required_physical_publication_contract_version','')<>''
            AND NOT CASE
              WHEN COALESCE(invalid_job.payload_json->>'required_physical_publication_contract_version','')
                  ~ '^[[:space:]]*[+-]?[0-9]+[[:space:]]*$'
                THEN (invalid_job.payload_json->>'required_physical_publication_contract_version')::numeric
                  IN (0,1)
              ELSE false
            END
          )
        )
      )
    );

  RETURN pg_catalog.jsonb_build_object(
    'ok',v_failed=0 AND v_remaining_invalid_active=0,
    'reason',v_reason,
    'examined_candidate_count',v_examined,
    'repaired_candidate_count',v_repaired,
    'terminalised_job_count',v_total_terminalised,
    'skipped_count',v_skipped,
    'failed_count',v_failed,
    'remaining_invalid_active_count',v_remaining_invalid_active,
    'all_state_transitions_proven',v_failed=0 AND v_remaining_invalid_active=0,
    'partial',v_failed>0 OR v_remaining_invalid_active>0,
    'more_due',v_remaining_invalid_active>0,
    'results',v_results
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_repair_invalid_source_authority_jobs_v1(
  uuid,uuid,integer,text
) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_repair_invalid_source_authority_jobs_v1(
  uuid,uuid,integer,text
) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_repair_invalid_source_authority_jobs_v1(
  uuid,uuid,integer,text
) TO postgres,service_role;

NOTIFY pgrst, 'reload schema';

commit;
