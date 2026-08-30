-- Banking Pay dirty-apply family-authority repair.
--
-- A trigger can know one changed Timesheet while the worker later proves that
-- the payment identity belongs to a wider version/rotation family.  Older
-- dirty-apply work can also become stale before it runs.  Both cases must fail
-- closed, but a still-open Workbench session must then receive one fresh
-- candidate-wide canonical invalidation instead of remaining blocked forever.
-- DEAD rows remain immutable audit history.  This repair never changes a
-- financial source, selection, Draft, batch, provider, payment or settlement.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION public.pay_workbench_repair_invalid_dirty_apply_jobs_v1(
  p_session_id uuid DEFAULT NULL::uuid,
  p_candidate_id uuid DEFAULT NULL::uuid,
  p_limit integer DEFAULT 10,
  p_reason text DEFAULT 'INVALID_DIRTY_APPLY_CANONICAL_REPAIR'::text
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
    'INVALID_DIRTY_APPLY_CANONICAL_REPAIR');
  v_candidate record;
  v_live_source_change_seq bigint:=0;
  v_invalidation_result jsonb:='{}'::jsonb;
  v_successor public.banking_pay_workbench_jobs%ROWTYPE;
  v_successor_tx_state text:=NULL::text;
  v_successor_tx_generation bigint:=NULL::bigint;
  v_registry_tx_token uuid:=NULL::uuid;
  v_counter_tx_token uuid:=NULL::uuid;
  v_examined integer:=0;
  v_repaired integer:=0;
  v_skipped integer:=0;
  v_failed integer:=0;
  v_successor_count integer:=0;
  v_remaining_invalid integer:=0;
  v_results jsonb:='[]'::jsonb;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  FOR v_candidate IN
    WITH latest_invalid AS (
      SELECT DISTINCT ON (invalid_job.candidate_id)
        invalid_job.id AS invalid_job_id,
        invalid_job.candidate_id,
        invalid_job.updated_at_utc,
        invalid_job.last_error_json,
        invalid_job.payload_json
      FROM public.banking_pay_workbench_jobs AS invalid_job
      WHERE invalid_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        AND invalid_job.status='DEAD'
        AND invalid_job.candidate_id IS NOT NULL
        AND COALESCE(invalid_job.last_error_json->>'message','') IN (
          'PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED',
          'PAY_WORKBENCH_STALE_PREINVALIDATED_AUTHORITY_NOT_CURRENT'
        )
        AND (p_candidate_id IS NULL OR invalid_job.candidate_id=p_candidate_id)
      ORDER BY invalid_job.candidate_id,invalid_job.updated_at_utc DESC,invalid_job.id DESC
    )
    SELECT latest_invalid.*
    FROM latest_invalid
    WHERE EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_sessions AS open_session
      JOIN public.banking_pay_workbench_session_scope AS current_scope
        ON current_scope.session_id=open_session.id
       AND current_scope.candidate_id=latest_invalid.candidate_id
      LEFT JOIN public.banking_pay_workbench_session_candidate_state AS candidate_state
        ON candidate_state.session_id=open_session.id
       AND candidate_state.candidate_id=latest_invalid.candidate_id
      WHERE pg_catalog.upper(pg_catalog.btrim(COALESCE(open_session.status,'')))='OPEN'
        AND open_session.discarded_at_utc IS NULL
        AND (p_session_id IS NULL OR open_session.id=p_session_id)
        AND (
          COALESCE(current_scope.dirty,false)
          OR current_scope.error_json IS NOT NULL
          OR pg_catalog.upper(pg_catalog.btrim(COALESCE(candidate_state.status,'')))='FAILED'
          OR COALESCE(open_session.scope_change_generation_applied,0)
             < COALESCE(open_session.scope_change_generation_target,0)
        )
    )
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_jobs AS active_job
        WHERE active_job.candidate_id=latest_invalid.candidate_id
          AND active_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
          AND active_job.status IN ('QUEUED','RUNNING')
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_jobs AS successful_job
        WHERE successful_job.candidate_id=latest_invalid.candidate_id
          AND successful_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
          AND successful_job.status='SUCCEEDED'
          AND (successful_job.updated_at_utc,successful_job.id)>
              (latest_invalid.updated_at_utc,latest_invalid.invalid_job_id)
      )
    ORDER BY latest_invalid.updated_at_utc,latest_invalid.invalid_job_id
    LIMIT v_limit
  LOOP
    v_examined:=v_examined+1;
    BEGIN
      IF NOT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended(
        public._pay_workbench_candidate_serial_key(v_candidate.candidate_id),24062027
      )) THEN
        v_skipped:=v_skipped+1;
        v_results:=v_results||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'candidate_id',v_candidate.candidate_id,
          'source_dead_job_id',v_candidate.invalid_job_id,
          'action','SKIPPED_CANDIDATE_SERIAL_BUSY'
        ));
        CONTINUE;
      END IF;

      PERFORM 1
      FROM public.candidates AS candidate_row
      WHERE candidate_row.id=v_candidate.candidate_id
      FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_INVALID_DIRTY_APPLY_CANDIDATE_MISSING'
          USING ERRCODE='40001';
      END IF;

      PERFORM 1
      FROM private.banking_pay_workbench_candidate_scope_registry AS registry_row
      WHERE registry_row.candidate_id=v_candidate.candidate_id
      FOR UPDATE;

      IF NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_jobs AS invalid_job
        WHERE invalid_job.id=v_candidate.invalid_job_id
          AND invalid_job.candidate_id=v_candidate.candidate_id
          AND invalid_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
          AND invalid_job.status='DEAD'
          AND COALESCE(invalid_job.last_error_json->>'message','') IN (
            'PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED',
            'PAY_WORKBENCH_STALE_PREINVALIDATED_AUTHORITY_NOT_CURRENT'
          )
      ) OR EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_jobs AS active_job
        WHERE active_job.candidate_id=v_candidate.candidate_id
          AND active_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
          AND active_job.status IN ('QUEUED','RUNNING')
      ) OR EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_jobs AS successful_job
        WHERE successful_job.candidate_id=v_candidate.candidate_id
          AND successful_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
          AND successful_job.status='SUCCEEDED'
          AND (successful_job.updated_at_utc,successful_job.id)>
              (v_candidate.updated_at_utc,v_candidate.invalid_job_id)
      ) THEN
        v_skipped:=v_skipped+1;
        v_results:=v_results||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'candidate_id',v_candidate.candidate_id,
          'source_dead_job_id',v_candidate.invalid_job_id,
          'action','SKIPPED_ALREADY_REPAIRED_OR_ACTIVE'
        ));
        CONTINUE;
      END IF;

      SELECT COALESCE(change_counter.seq,0)
      INTO v_live_source_change_seq
      FROM public.app_change_counters AS change_counter
      WHERE change_counter.entity_key='pay_candidate:'||v_candidate.candidate_id::text;

      v_invalidation_result:=private.pay_workbench_scope_invalidate_v1(
        ARRAY[v_candidate.candidate_id],
        ARRAY[NULL::uuid],
        'DIRTY_APPLY_CANONICAL_REPAIR:'||v_reason,
        NULL::uuid,
        pg_catalog.jsonb_build_object(
          'invalid_dirty_apply_repair',true,
          'invalid_dirty_apply_repair_source_job_id',v_candidate.invalid_job_id::text,
          'invalid_dirty_apply_repair_error',
            v_candidate.last_error_json->>'message',
          'source_change_seq',v_live_source_change_seq,
          'source_change_sequence',v_live_source_change_seq,
          'latest_source_change_seq',v_live_source_change_seq,
          'trigger_source','WORKBENCH_INVALID_DIRTY_APPLY_REPAIR',
          'priority',-1100,
          'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH',
          'economic_truth_mutation_allowed',false
        )
      );

      v_successor:=NULL;
      SELECT successor_job.*
      INTO v_successor
      FROM public.banking_pay_workbench_jobs AS successor_job
      WHERE successor_job.candidate_id=v_candidate.candidate_id
        AND successor_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        AND successor_job.status IN ('QUEUED','RUNNING')
        AND COALESCE(successor_job.payload_json->>'invalid_dirty_apply_repair_source_job_id','')
            =v_candidate.invalid_job_id::text
      ORDER BY successor_job.created_at_utc DESC,successor_job.id DESC
      LIMIT 1
      FOR UPDATE;

      SELECT scope_tx.state,scope_tx.allocated_generation
      INTO v_successor_tx_state,v_successor_tx_generation
      FROM public.banking_pay_scope_change_transactions AS scope_tx
      WHERE scope_tx.tx_token=v_successor.scope_change_tx_token;

      SELECT registry.last_scope_change_tx_token,change_counter.scope_change_tx_token
      INTO v_registry_tx_token,v_counter_tx_token
      FROM private.banking_pay_workbench_candidate_scope_registry AS registry
      LEFT JOIN public.app_change_counters AS change_counter
        ON change_counter.entity_key='pay_candidate:'||v_candidate.candidate_id::text
      WHERE registry.candidate_id=v_candidate.candidate_id;

      IF pg_catalog.lower(pg_catalog.btrim(COALESCE(v_invalidation_result->>'ok','false')))
           NOT IN ('true','t','1','yes','y','on')
         OR v_successor.id IS NULL
         OR v_successor.scope_change_tx_token IS NULL
         OR pg_catalog.jsonb_array_length(CASE
              WHEN pg_catalog.jsonb_typeof(v_successor.payload_json->'targeted_timesheet_ids')='array'
                THEN v_successor.payload_json->'targeted_timesheet_ids'
              ELSE '[]'::jsonb END)<>0
         OR pg_catalog.lower(pg_catalog.btrim(COALESCE(
              v_successor.payload_json->>'bounded_scope_state_precedes_job','false'
            ))) NOT IN ('true','t','1','yes','y','on')
         OR v_successor_tx_state IS DISTINCT FROM 'PENDING'
         OR v_successor_tx_generation IS NOT NULL
         OR v_successor.scope_change_tx_token IS NULL
         OR v_successor.scope_change_generation IS NOT NULL
         OR v_registry_tx_token IS DISTINCT FROM v_successor.scope_change_tx_token
         OR v_counter_tx_token IS DISTINCT FROM v_successor.scope_change_tx_token THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_INVALID_DIRTY_APPLY_REPAIR_POSTCONDITION_FAILED'
          USING ERRCODE='40001';
      END IF;

      PERFORM public._audit_insert(
        'banking_pay_workbench_job',
        v_candidate.invalid_job_id::text,
        'INVALID_DIRTY_APPLY_CANONICAL_REPAIR',
        pg_catalog.jsonb_build_object(
          'candidate_id',v_candidate.candidate_id,
          'source_dead_job_id',v_candidate.invalid_job_id,
          'source_error',v_candidate.last_error_json->>'message'
        ),
        pg_catalog.jsonb_build_object(
          'candidate_id',v_candidate.candidate_id,
          'source_dead_job_retained',true,
          'successor_job_id',v_successor.id,
          'successor_scope_change_tx_token',v_successor.scope_change_tx_token,
          'successor_finalization_staged',true,
          'successor_full_candidate_scope',true,
          'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
        ),
        v_reason,
        NULL::uuid
      );

      v_repaired:=v_repaired+1;
      v_successor_count:=v_successor_count+1;
      v_results:=v_results||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'candidate_id',v_candidate.candidate_id,
        'source_dead_job_id',v_candidate.invalid_job_id,
        'source_dead_job_retained',true,
        'successor_job_id',v_successor.id,
        'successor_scope_change_tx_token',v_successor.scope_change_tx_token,
        'successor_finalization_staged',true,
        'action','REPAIRED_CANONICAL_FULL_CANDIDATE_OWNER'
      ));
    EXCEPTION WHEN OTHERS THEN
      v_failed:=v_failed+1;
      v_results:=v_results||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'candidate_id',v_candidate.candidate_id,
        'source_dead_job_id',v_candidate.invalid_job_id,
        'action','UNRESOLVED_POSTCONDITION_NOT_PROVEN',
        'error_state',SQLSTATE
      ));
    END;
  END LOOP;

  WITH latest_invalid AS (
    SELECT DISTINCT ON (invalid_job.candidate_id)
      invalid_job.id AS invalid_job_id,
      invalid_job.candidate_id,
      invalid_job.updated_at_utc
    FROM public.banking_pay_workbench_jobs AS invalid_job
    WHERE invalid_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
      AND invalid_job.status='DEAD'
      AND invalid_job.candidate_id IS NOT NULL
      AND COALESCE(invalid_job.last_error_json->>'message','') IN (
        'PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED',
        'PAY_WORKBENCH_STALE_PREINVALIDATED_AUTHORITY_NOT_CURRENT'
      )
      AND (p_candidate_id IS NULL OR invalid_job.candidate_id=p_candidate_id)
    ORDER BY invalid_job.candidate_id,invalid_job.updated_at_utc DESC,invalid_job.id DESC
  )
  SELECT count(*)::integer
  INTO v_remaining_invalid
  FROM latest_invalid
  WHERE EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_sessions AS open_session
    JOIN public.banking_pay_workbench_session_scope AS current_scope
      ON current_scope.session_id=open_session.id
     AND current_scope.candidate_id=latest_invalid.candidate_id
    LEFT JOIN public.banking_pay_workbench_session_candidate_state AS candidate_state
      ON candidate_state.session_id=open_session.id
     AND candidate_state.candidate_id=latest_invalid.candidate_id
    WHERE pg_catalog.upper(pg_catalog.btrim(COALESCE(open_session.status,'')))='OPEN'
      AND open_session.discarded_at_utc IS NULL
      AND (p_session_id IS NULL OR open_session.id=p_session_id)
      AND (
        COALESCE(current_scope.dirty,false)
        OR current_scope.error_json IS NOT NULL
        OR pg_catalog.upper(pg_catalog.btrim(COALESCE(candidate_state.status,'')))='FAILED'
        OR COALESCE(open_session.scope_change_generation_applied,0)
           < COALESCE(open_session.scope_change_generation_target,0)
      )
  )
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_jobs AS active_job
      WHERE active_job.candidate_id=latest_invalid.candidate_id
        AND active_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        AND active_job.status IN ('QUEUED','RUNNING')
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_jobs AS successful_job
      WHERE successful_job.candidate_id=latest_invalid.candidate_id
        AND successful_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        AND successful_job.status='SUCCEEDED'
        AND (successful_job.updated_at_utc,successful_job.id)>
            (latest_invalid.updated_at_utc,latest_invalid.invalid_job_id)
    );

  RETURN pg_catalog.jsonb_build_object(
    'ok',v_failed=0 AND v_remaining_invalid=0,
    'reason',v_reason,
    'examined_candidate_count',v_examined,
    'repaired_candidate_count',v_repaired,
    'successor_job_count',v_successor_count,
    'skipped_count',v_skipped,
    'failed_count',v_failed,
    'remaining_invalid_unrepaired_count',v_remaining_invalid,
    'all_state_transitions_proven',v_failed=0 AND v_remaining_invalid=0,
    'partial',v_failed>0 OR v_remaining_invalid>0,
    'more_due',v_remaining_invalid>0,
    'results',v_results
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_repair_invalid_dirty_apply_jobs_v1(
  uuid,uuid,integer,text
) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_repair_invalid_dirty_apply_jobs_v1(
  uuid,uuid,integer,text
) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_repair_invalid_dirty_apply_jobs_v1(
  uuid,uuid,integer,text
) TO postgres,service_role;

NOTIFY pgrst, 'reload schema';

commit;
