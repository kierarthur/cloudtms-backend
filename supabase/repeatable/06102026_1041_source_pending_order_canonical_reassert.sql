-- Reassert exact canonical successors after the managed pending H1/H2 closure.
-- Contract-neutral: existing function bodies, planner settings and ACLs are preserved.
-- Do not include the whole 22092026_1052 page: its older publishers are superseded.
\set ON_ERROR_STOP on

\ir 07092026_2013_banking_pay_unpaid_cancellation_communication_v2_prepare_v1.sql
\ir 09092026_0020_banking_pay_no_money_workbench_return_v1.sql
\ir 03102026_0400_stage2_canonical_producer_after_h1h2_retry.sql
\ir 03102026_0600_stage2_plan_cache_after_h1h2_retry.sql
\ir 03102026_0700_stage2_finance_baseline_after_h1h2_retry.sql

begin;
CREATE OR REPLACE FUNCTION private.weekly_source_invalidation_contract_assert_v1(p_candidate_id uuid, p_declared_root_ids uuid[], p_token uuid, p_context text, p_pre_existing_job_ids uuid[] DEFAULT ARRAY[]::uuid[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'pg_catalog', 'pg_temp'
AS $function$
declare
  v_declared uuid[];
  v_jobs jsonb:='[]'::jsonb;
  v_complete integer:=0;
  v_whole_candidate integer:=0;
  v_tokens integer;
  v_failure text;
  v_job record;
  v_scope_row record;
  v_registry_row record;
  v_scope uuid[];
  v_targeted uuid[];
  v_frame uuid;
begin
  perform private.weekly_source_observation_storage_v1();
  select frame_id into v_frame from pg_temp.ws_observation_frame_v1 where singleton;
  if v_frame is null or coalesce(pg_catalog.cardinality(p_pre_existing_job_ids),0)<>0 then
    raise exception 'WEEKLY_SOURCE_OBSERVATION_REQUIRED' using errcode='55000';
  end if;
  if exists(select 1 from pg_temp.ws_observation_rows_v1
      where frame_id=v_frame and kind='TOKEN' and row_id is distinct from p_token) then
    raise exception 'WEEKLY_SOURCE_INVALIDATION_SECOND_TOKEN' using errcode='55000';
  end if;
  if p_token is null then
    raise exception 'WEEKLY_SOURCE_INVALIDATION_TOKEN_MISSING'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_INVALIDATION_TOKEN_MISSING',
              'context',p_context)::text;
  end if;

  -- The declared complete aligned scope, expanded through the installed
  -- normaliser exactly as the Workbench expands a queued job.
  select coalesce(private.weekly_source_withdrawal_uuid_array_v1(
           public._pay_workbench_normalise_timesheet_rotation_scope_payload(
             coalesce(p_declared_root_ids,array[]::uuid[]),array[]::uuid[]
           )->'family_timesheet_ids'),array[]::uuid[])
    into v_declared;

  -- The controlling token row must exist and still be open.
  select pg_catalog.count(*)::integer into v_tokens
  from public.banking_pay_scope_change_transactions as token_row
  where token_row.tx_token=p_token and token_row.state='PENDING';
  if v_tokens<>1 then
    v_failure:='CONTROLLING_TOKEN_NOT_OPEN';
  end if;

  -- The protected observer captures INSERT and UPDATE alike, including reused
  -- jobs, independently of their claimed token/Candidate. Rolled-back writes
  -- and other sessions' writes cannot enter this invocation's membership.
  for v_job in
    select job_row.id, job_row.candidate_id, job_row.job_type, job_row.dedupe_key,
           job_row.scope_change_tx_token, job_row.scope_change_generation,
           job_row.payload_json
    from pg_temp.ws_observation_rows_v1 as effect
    cross join lateral (select native.* from public.banking_pay_workbench_jobs native
      where native.id=effect.row_id offset 0) as job_row
    where effect.frame_id=v_frame and effect.kind='JOB' and job_row.status in ('QUEUED','RUNNING')
      and exists(select 1 from pg_temp.ws_observation_rows_v1 observed
        where observed.frame_id=v_frame and observed.kind='JOB'
          and observed.row_id=job_row.id)
    order by job_row.id
  loop
    -- Point 1: every invalidation produced by this operation carries the same
    -- token, whatever registered path produced it — including the
    -- CONTRACT_CLIENT_DIRTY_FANOUT and finance-case paths, which carry no
    -- Candidate at all.
    if v_job.scope_change_tx_token is distinct from p_token then
      v_failure:=coalesce(v_failure,'JOB_CARRIES_A_DIFFERENT_TOKEN');
    end if;

    v_targeted:=private.weekly_source_withdrawal_uuid_array_v1(
      v_job.payload_json->'targeted_timesheet_ids');

    select coalesce(private.weekly_source_withdrawal_uuid_array_v1(
             public._pay_workbench_normalise_timesheet_rotation_scope_payload(
               v_targeted,
               private.weekly_source_withdrawal_uuid_array_v1(
                 v_job.payload_json->'linked_timesheet_ids')
             )->'family_timesheet_ids'),array[]::uuid[])
      into v_scope;

    -- Point 5 is a rule about CANDIDATE jobs: a job queued for a different
    -- Candidate, or naming a root outside the declared aligned scope, is a
    -- contract failure.  A job with no Candidate is a registered non-Candidate
    -- fanout path; it is recorded and its token is still required to match.
    if v_job.candidate_id is not null then
      if v_job.candidate_id is distinct from p_candidate_id then
        v_failure:=coalesce(v_failure,'JOB_FOR_A_DIFFERENT_CANDIDATE');
      end if;

      if exists (select 1 from pg_catalog.unnest(v_scope) as scope_id(value)
                 where not (scope_id.value=any(v_declared))) then
        v_failure:=coalesce(v_failure,'JOB_SCOPE_OUTSIDE_THE_DECLARED_SCOPE');
      end if;

      if coalesce(pg_catalog.cardinality(v_targeted),0)=0 then
        -- An empty target list is the Workbench's whole-Candidate job.  It
        -- cannot name a root outside the Candidate and it can only widen the
        -- refresh, never narrow it, so it is neither a subset violation nor the
        -- declared complete-scope job.  It is counted and reported separately.
        v_whole_candidate:=v_whole_candidate+1;
      elsif coalesce(pg_catalog.cardinality(v_scope),0)
            =coalesce(pg_catalog.cardinality(v_declared),0)
        and not exists (select 1 from pg_catalog.unnest(v_declared) as declared_id(value)
                        where not (declared_id.value=any(v_scope))) then
        v_complete:=v_complete+1;
      end if;
    end if;

    v_jobs:=v_jobs||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'job_id',v_job.id,
      'candidate_id',v_job.candidate_id,
      'job_type',v_job.job_type,
      'dedupe_key',v_job.dedupe_key,
      'scope_change_tx_token',v_job.scope_change_tx_token,
      'scope_change_generation',v_job.scope_change_generation,
      'targeted_timesheet_ids',pg_catalog.to_jsonb(v_targeted),
      'normalised_scope',pg_catalog.to_jsonb(v_scope),
      'whole_candidate',v_job.candidate_id is not null
        and coalesce(pg_catalog.cardinality(v_targeted),0)=0));
  end loop;

  -- Scope-only invalidation is legitimate (enqueue may be coalesced/skipped),
  -- but every captured row must still belong to this operation's declared
  -- Candidate, token and aligned family. Drive native PK probes from the frame,
  -- retaining missing/deleted identities as failures, never as filtered evidence.
  for v_scope_row in
    select effect.row_id,effect.deleted,scope_state_row.timesheet_id,
           scope_state_row.candidate_id,scope_state_row.last_scope_change_tx_token
    from pg_temp.ws_observation_rows_v1 as effect
    left join lateral (
      select native.* from private.banking_pay_workbench_timesheet_scope_state native
      where native.timesheet_id=effect.row_id offset 0
    ) as scope_state_row on true
    where effect.frame_id=v_frame and effect.kind='SCOPE'
    order by effect.row_id
  loop
    if v_scope_row.deleted or v_scope_row.timesheet_id is null then
      v_failure:=coalesce(v_failure,'SCOPE_ROW_MISSING_OR_DELETED');
    elsif v_scope_row.candidate_id is distinct from p_candidate_id then
      v_failure:=coalesce(v_failure,'SCOPE_FOR_A_DIFFERENT_CANDIDATE');
    elsif v_scope_row.last_scope_change_tx_token is distinct from p_token then
      v_failure:=coalesce(v_failure,'SCOPE_CARRIES_A_DIFFERENT_TOKEN');
    elsif not (v_scope_row.timesheet_id=any(v_declared)) then
      v_failure:=coalesce(v_failure,'SCOPE_OUTSIDE_THE_DECLARED_SCOPE');
    end if;
  end loop;

  -- Candidate-only native invalidation must not escape a Timesheet-only proof.
  -- Capture is independent of claimed Candidate/token; only current-frame keys
  -- drive these native PK probes. Deletion is sticky, including delete/reinsert.
  for v_registry_row in
    select effect.row_id,effect.deleted,registry_row.candidate_id,
           registry_row.last_scope_change_tx_token
    from pg_temp.ws_observation_rows_v1 as effect
    left join lateral (
      select native.candidate_id,native.last_scope_change_tx_token
      from private.banking_pay_workbench_candidate_scope_registry native
      where native.candidate_id=effect.row_id offset 0
    ) as registry_row on true
    where effect.frame_id=v_frame and effect.kind='REGISTRY'
    order by effect.row_id
  loop
    if v_registry_row.deleted or v_registry_row.candidate_id is null then
      v_failure:=coalesce(v_failure,'REGISTRY_ROW_MISSING_OR_DELETED');
    elsif v_registry_row.candidate_id is distinct from p_candidate_id then
      v_failure:=coalesce(v_failure,'REGISTRY_FOR_A_DIFFERENT_CANDIDATE');
    elsif v_registry_row.last_scope_change_tx_token is distinct from p_token then
      v_failure:=coalesce(v_failure,'REGISTRY_CARRIES_A_DIFFERENT_TOKEN');
    end if;
  end loop;

  -- Point 6: exactly one effective complete-scope dirty result per Candidate.
  if v_complete<>1 then
    v_failure:=coalesce(v_failure,
      case when v_complete=0 then 'NO_COMPLETE_SCOPE_JOB'
           else 'MORE_THAN_ONE_COMPLETE_SCOPE_JOB' end);
  end if;

  if v_failure is not null then
    raise exception 'WEEKLY_SOURCE_INVALIDATION_CONTRACT_FAILED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_INVALIDATION_CONTRACT_FAILED',
              'reason',v_failure,
              'context',p_context,
              'scope_change_tx_token',p_token,
              'declared_scope',pg_catalog.to_jsonb(v_declared),
              'jobs',v_jobs)::text;
  end if;

  perform private.weekly_source_observation_seal_v1();
  return pg_catalog.jsonb_build_object(
    'ok',true,
    'scope_change_tx_token',p_token,
    'declared_scope',pg_catalog.to_jsonb(v_declared),
    'complete_scope_job_count',v_complete,
    'whole_candidate_job_count',v_whole_candidate,
    'jobs',v_jobs);
end;
$function$;

commit;
