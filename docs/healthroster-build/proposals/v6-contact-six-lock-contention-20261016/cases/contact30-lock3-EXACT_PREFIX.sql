begin;
do $managed_writer$ begin
  if not exists(select 1 from pg_catalog.pg_locks where locktype='advisory'
    and pid=pg_catalog.pg_backend_pid() and granted and mode='ExclusiveLock' and objsubid=1
    and classid=((pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)>>32)&4294967295)::oid
    and objid=(pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)&4294967295)::oid)
  then raise exception 'CLOUDTMS_MANAGED_WRITER_GUARD_LOST'; end if;
end $managed_writer$;
-- One-time CloudTMS migration: complete frozen Contact one-time/03_submission_cohort_identity.sqlinc.
-- Source-only proposal; exact current prerequisites/native contract qualification required.




-- Hold the exact prerequisite objects before every original assertion.
-- Existing native owners acquire these relations in differing paths; NOWAIT
-- refuses an active incompatible owner before checks instead of waiting with
-- a partial witness set. All locks remain in this existing atomic unit.
lock table public.weekly_candidate_cohorts in share mode nowait;
lock table public.weekly_timesheet_submission_requests in access exclusive mode nowait;
lock table public.weekly_candidate_outreach_generations in share mode nowait;
