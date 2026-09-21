begin;

-- An invoice-discounting Draft freezes more than the three current money
-- totals.  A source presentation can move away and back before the Draft is
-- committed, leaving the same totals but a different financial history.  A
-- monotonic ledger revision makes that intervening change durable and lets
-- the commit owner refuse the stale Draft.
alter table public.id_invoice_ledger
  add column if not exists ledger_revision bigint;

update public.id_invoice_ledger
set ledger_revision = 1
where ledger_revision is null;

alter table public.id_invoice_ledger
  alter column ledger_revision set default 1,
  alter column ledger_revision set not null;

do $constraint$
begin
  if not exists (
    select 1
    from pg_catalog.pg_constraint
    where conrelid = 'public.id_invoice_ledger'::pg_catalog.regclass
      and conname = 'id_invoice_ledger_revision_positive_chk'
  ) then
    alter table public.id_invoice_ledger
      add constraint id_invoice_ledger_revision_positive_chk
      check (ledger_revision > 0);
  end if;
end;
$constraint$;

-- Deliberately nullable for Drafts created before this migration.  Their
-- missing frozen revision causes commit to fail closed; Office can cancel and
-- rebuild them from the current ledger truth.
alter table public.id_consolidation_run_lines
  add column if not exists ledger_revision bigint;

do $constraint$
begin
  if not exists (
    select 1
    from pg_catalog.pg_constraint
    where conrelid = 'public.id_consolidation_run_lines'::pg_catalog.regclass
      and conname = 'id_consolidation_run_lines_revision_positive_chk'
  ) then
    alter table public.id_consolidation_run_lines
      add constraint id_consolidation_run_lines_revision_positive_chk
      check (ledger_revision is null or ledger_revision > 0);
  end if;
end;
$constraint$;

commit;
