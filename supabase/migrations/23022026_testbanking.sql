alter table public.settings_defaults
  add column if not exists payroll_testing boolean not null default false;
