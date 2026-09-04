\set ON_ERROR_STOP on

begin;

do $verify$
declare
  v_definition text;
begin
  select pg_catalog.pg_get_functiondef(
    'public.candidate_submission_finalize_single_flight_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)'::regprocedure
  ) into strict v_definition;

  if v_definition not like '%pg_try_advisory_xact_lock%' then
    raise exception 'CANDIDATE_FINALISATION_SINGLE_FLIGHT_LOCK_MISSING';
  end if;
  if v_definition not like '%CANDIDATE_FINALISATION_GLOBAL%'
     or v_definition not like '%FINALISATION_CAPACITY_BUSY%' then
    raise exception 'CANDIDATE_FINALISATION_GLOBAL_CAPACITY_GATE_MISSING';
  end if;
  if v_definition not like '%candidate_submission_finalize_atomic_v1%' then
    raise exception 'CANDIDATE_FINALISATION_ATOMIC_DELEGATE_MISSING';
  end if;
  if v_definition not like '%single_flight_deferred%' then
    raise exception 'CANDIDATE_FINALISATION_DEFERRED_RECEIPT_MISSING';
  end if;
  if pg_catalog.has_function_privilege(
       'anon',
       'public.candidate_submission_finalize_single_flight_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)',
       'execute'
     )
     or pg_catalog.has_function_privilege(
       'authenticated',
       'public.candidate_submission_finalize_single_flight_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)',
       'execute'
     )
     or not pg_catalog.has_function_privilege(
       'service_role',
       'public.candidate_submission_finalize_single_flight_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)',
       'execute'
     ) then
    raise exception 'CANDIDATE_FINALISATION_SINGLE_FLIGHT_ACL_INVALID';
  end if;
end;
$verify$;

rollback;
