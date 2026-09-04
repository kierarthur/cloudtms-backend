\set ON_ERROR_STOP on

begin;

do $verify$
declare
  v_config text[];
begin
  select p.proconfig into strict v_config
  from pg_catalog.pg_proc p
  where p.oid='public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)'::regprocedure;

  if not ('lock_timeout=5s'=any(coalesce(v_config,array[]::text[]))) then
    raise exception 'CANDIDATE_FINALISATION_LOCK_TIMEOUT_NOT_INSTALLED';
  end if;
  if not ('statement_timeout=120s'=any(coalesce(v_config,array[]::text[]))) then
    raise exception 'CANDIDATE_FINALISATION_STATEMENT_TIMEOUT_NOT_INSTALLED';
  end if;
end;
$verify$;

rollback;
