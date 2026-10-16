-- One-time CloudTMS schema/data migration: nhsp_v6_source_owner_only_rls
-- Source-only Health assembly; actual cross-owner dependencies remain mandatory.

\set ON_ERROR_STOP on

begin;

-- Exact d863577918ce7d439206296e91ad78de298e98b2:docs/healthroster-build/proposals/source-pay-decision-v5-foundation-20261015/one-time/05_complete_owner_only_rls.sqlinc
alter table private.weekly_source_pay_decisions owner to postgres;
alter table private.weekly_source_pay_decision_requests owner to postgres;
alter table private.weekly_source_pay_decision_preview_assemblies owner to postgres;
alter table private.weekly_source_pay_decision_preview_quotes owner to postgres;
alter table private.weekly_source_pay_decision_reviews owner to postgres;
alter table private.weekly_source_pay_decision_members owner to postgres;
alter table private.weekly_source_pay_decision_bindings owner to postgres;
alter table private.weekly_source_pay_decision_unprotect_preparations owner to postgres;
alter table private.weekly_source_pay_decision_acceptance_applications owner to postgres;
alter table private.weekly_source_pay_decision_native_adoptions owner to postgres;
alter table private.weekly_source_pay_decision_publication_adoptions owner to postgres;
do $source_pay_private_rls$
declare name text;
begin
 foreach name in array array['weekly_source_pay_decisions','weekly_source_pay_decision_requests',
  'weekly_source_pay_decision_preview_assemblies','weekly_source_pay_decision_preview_quotes','weekly_source_pay_decision_reviews',
  'weekly_source_pay_decision_members','weekly_source_pay_decision_bindings','weekly_source_pay_decision_unprotect_preparations',
  'weekly_source_pay_decision_acceptance_applications','weekly_source_pay_decision_native_adoptions','weekly_source_pay_decision_publication_adoptions'] loop
  execute format('alter table private.%I enable row level security',name);
  execute format('alter table private.%I force row level security',name);
  execute format('revoke all on table private.%I from public,anon,authenticated,service_role',name);
  execute format('create policy cloudtms_source_pay_decision_owner_only on private.%I for all to %I using(true) with check(true)',name,current_user);
 end loop;
end;
$source_pay_private_rls$;

commit;
