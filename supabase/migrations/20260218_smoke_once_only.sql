create table if not exists public.migration_smoke_once_only (
  id bigserial primary key,
  created_at timestamptz not null default now()
);

insert into public.migration_smoke_once_only default values;
