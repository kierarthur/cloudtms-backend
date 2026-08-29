-- CloudTMS TEST Invoice Async V8/V2: additive workflow/manifest authority.
-- Existing rows remain claim-compatible through committed/released defaults.

alter table public.invoice_operations
  add column if not exists manifest_generation integer not null default 0,
  add column if not exists manifest_committed boolean not null default true,
  add column if not exists release_complete boolean not null default true,
  add column if not exists result_page_revision bigint not null default 0;

do $migration$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.invoice_operations'::regclass
      and conname = 'invoice_operations_manifest_generation_ck'
  ) then
    alter table public.invoice_operations
      add constraint invoice_operations_manifest_generation_ck
      check (manifest_generation >= 0);
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.invoice_operations'::regclass
      and conname = 'invoice_operations_result_page_revision_ck'
  ) then
    alter table public.invoice_operations
      add constraint invoice_operations_result_page_revision_ck
      check (result_page_revision >= 0);
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.invoice_operations'::regclass
      and conname = 'invoice_operations_manifest_release_ck'
  ) then
    alter table public.invoice_operations
      add constraint invoice_operations_manifest_release_ck
      check (not release_complete or manifest_committed);
  end if;
end;
$migration$;

alter table public.invoice_operation_chunks
  add column if not exists manifest_generation integer not null default 0,
  add column if not exists is_manifest_member boolean not null default false,
  add column if not exists manifest_committed boolean not null default true,
  add column if not exists result_visible boolean not null default false;

do $migration$
begin
  if not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'invoice_operation_chunks'
      and column_name = 'selection_key'
  ) then
    alter table public.invoice_operation_chunks
      add column selection_key text
      generated always as (nullif(payload_json ->> 'selection_key', '')) stored;
  end if;

  if not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'invoice_operation_chunks'
      and column_name = 'result_category'
  ) then
    alter table public.invoice_operation_chunks
      add column result_category text
      generated always as (
        nullif(
          coalesce(
            result_json ->> 'result_category',
            payload_json ->> 'result_category'
          ),
          ''
        )
      ) stored;
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.invoice_operation_chunks'::regclass
      and conname = 'invoice_operation_chunks_manifest_generation_ck'
  ) then
    alter table public.invoice_operation_chunks
      add constraint invoice_operation_chunks_manifest_generation_ck
      check (manifest_generation >= 0);
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.invoice_operation_chunks'::regclass
      and conname = 'invoice_operation_chunks_manifest_member_ck'
  ) then
    alter table public.invoice_operation_chunks
      add constraint invoice_operation_chunks_manifest_member_ck
      check (not is_manifest_member or manifest_generation > 0);
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.invoice_operation_chunks'::regclass
      and conname = 'invoice_operation_chunks_result_visible_ck'
  ) then
    alter table public.invoice_operation_chunks
      add constraint invoice_operation_chunks_result_visible_ck
      check (not result_visible or selection_key is not null);
  end if;
end;
$migration$;

comment on column public.invoice_operations.manifest_generation is
  'V8 selection-manifest generation; zero identifies pre-V8/non-manifest work.';
comment on column public.invoice_operations.result_page_revision is
  'Atomic monotonic revision for signed commercial result paging.';
comment on column public.invoice_operation_chunks.is_manifest_member is
  'True only for a carrier owned by a V8 selection manifest.';
comment on column public.invoice_operation_chunks.result_visible is
  'Physical authority for inclusion in bounded commercial result pages.';
