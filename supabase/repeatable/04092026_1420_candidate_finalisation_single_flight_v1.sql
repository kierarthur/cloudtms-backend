-- Candidate manager finalisation must never allow an abandoned PostgREST call
-- to overlap a later recovery attempt for the same immutable workflow.

begin;

create or replace function public.candidate_submission_finalize_single_flight_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_expected_row_signature text,
  p_idempotency_key text,
  p_now_utc timestamptz default now(),
  p_daily_materialisation_json jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
begin
  if p_workflow_id is null or p_expected_generation is null or p_expected_generation < 1 then
    raise exception 'CANDIDATE_FINALISATION_IDENTITY_REQUIRED' using errcode='22023';
  end if;

  -- Finalisation is normally a short transaction. A single global non-waiting
  -- gate reserves the remaining PostgREST pool for login, reads and unrelated
  -- app actions even if several independent claims become stuck at once.
  if not pg_catalog.pg_try_advisory_xact_lock(
    pg_catalog.hashtextextended('CANDIDATE_FINALISATION_GLOBAL', 0)
  ) then
    return jsonb_build_object(
      'ok',true,
      'workflow_id',p_workflow_id,
      'generation',p_expected_generation,
      'state','FINALISATION_PENDING',
      'finalisation_pending',true,
      'single_flight_deferred',true,
      'reason','FINALISATION_CAPACITY_BUSY'
    );
  end if;

  if not pg_catalog.pg_try_advisory_xact_lock(
    pg_catalog.hashtextextended(
      'CANDIDATE_FINALISATION|' || p_workflow_id::text || '|' || p_expected_generation::text,
      0
    )
  ) then
    return jsonb_build_object(
      'ok',true,
      'workflow_id',p_workflow_id,
      'generation',p_expected_generation,
      'state','FINALISATION_PENDING',
      'finalisation_pending',true,
      'single_flight_deferred',true,
      'reason','ALREADY_BEING_PROCESSED'
    );
  end if;

  return public.candidate_submission_finalize_atomic_v1(
    p_session_id,
    p_environment,
    p_workflow_id,
    p_expected_generation,
    p_expected_row_signature,
    p_idempotency_key,
    p_now_utc,
    p_daily_materialisation_json
  );
end;
$function$;

alter function public.candidate_submission_finalize_single_flight_v1(
  uuid,text,uuid,integer,text,text,timestamptz,jsonb
) owner to postgres;
revoke all on function public.candidate_submission_finalize_single_flight_v1(
  uuid,text,uuid,integer,text,text,timestamptz,jsonb
) from public,anon,authenticated;
grant execute on function public.candidate_submission_finalize_single_flight_v1(
  uuid,text,uuid,integer,text,text,timestamptz,jsonb
) to service_role;

notify pgrst, 'reload schema';

commit;
