-- Existing cancellation owner gains a genuinely typed UM DRAFT alternative.
-- No NET row/id, counter, cash, pay authority or balance owner is invented.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_case_cancel_binding
  add column umbrella_draft_state_id uuid,
  add constraint bpay_next_case_cancel_umbrella_draft_fk_v2
    foreign key (umbrella_draft_state_id,run_worker_id,candidate_id,preparation_revision,selection_revision)
    references private.bpay_next_case_allocation_state(id,run_worker_id,candidate_id,preparation_revision,selection_revision) on delete restrict;
-- Same exact-expression admission used by actual0400; unknown shapes fail closed.
do $cancel_projection_shape$
declare old_constraint name;
  expected text:=pg_catalog.regexp_replace($audited$
    (((accepted_projection_revision IS NULL) AND (projection_id IS NULL) AND (net_state_id IS NULL))
      OR ((accepted_projection_revision = 0) AND (projection_id IS NULL) AND (net_state_id IS NULL))
      OR ((accepted_projection_revision > 0) AND (projection_id IS NOT NULL) AND (net_state_id IS NOT NULL)))
  $audited$,'[[:space:]()]','','g');
begin
  if not exists(select 1 from pg_catalog.pg_class c where c.oid='private.bpay_next_case_cancel_binding'::regclass
    and c.relowner=current_user::regrole) then raise exception 'BPAY_NEXT_UM_CANCEL_BINDING_OWNER_MISMATCH' using errcode='42501';end if;
  select c.conname into strict old_constraint from pg_catalog.pg_constraint c
    where c.conrelid='private.bpay_next_case_cancel_binding'::regclass and c.contype='c'
      and c.convalidated and c.conislocal and c.coninhcount=0
      and pg_catalog.regexp_replace(pg_catalog.pg_get_expr(c.conbin,c.conrelid),'[[:space:]()]','','g')=expected;
  execute pg_catalog.format('alter table private.bpay_next_case_cancel_binding drop constraint %I',old_constraint);
end $cancel_projection_shape$;
alter table private.bpay_next_case_cancel_binding add constraint bpay_next_case_cancel_projection_shape_v2 check ((
  (accepted_projection_revision is null and projection_id is null and net_state_id is null and umbrella_draft_state_id is null)
  or (accepted_projection_revision=0 and projection_id is null and net_state_id is null and umbrella_draft_state_id is null)
  or (accepted_projection_revision>0 and projection_id is not null and
    ((net_state_id is not null and umbrella_draft_state_id is null)
      or (net_state_id is null and umbrella_draft_state_id is not null and umbrella_draft_state_id=draft_state_id)))) is true);
-- One actual cancellation can consume an accepted projection only once; this
-- permits a constant point proof without enumerating old blocked commands.
create unique index bpay_next_case_cancel_projection_once_v2
  on private.bpay_next_case_cancel_binding(projection_id) where projection_id is not null;
commit;
