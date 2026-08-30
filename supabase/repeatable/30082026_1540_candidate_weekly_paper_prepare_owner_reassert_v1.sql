-- Final upgrade-order ownership and ACL authority for the weekly Candidate
-- printed-pack adapter.  The historical installed database can retain the
-- provider's former owner when CREATE OR REPLACE does not specify an owner.
-- CloudTMS contracts use postgres as the portable logical owner, so this
-- newest repeatable makes NEW and UPGRADE converge without changing the
-- adapter's business behaviour.

\set ON_ERROR_STOP on

begin;

alter function public.candidate_weekly_paper_prepare_atomic_v1(
  uuid,text,uuid,text,integer,jsonb,text,timestamptz
) owner to postgres;

revoke all on function public.candidate_weekly_paper_prepare_atomic_v1(
  uuid,text,uuid,text,integer,jsonb,text,timestamptz
) from public,anon,authenticated;

grant execute on function public.candidate_weekly_paper_prepare_atomic_v1(
  uuid,text,uuid,text,integer,jsonb,text,timestamptz
) to service_role;

notify pgrst, 'reload schema';

commit;
