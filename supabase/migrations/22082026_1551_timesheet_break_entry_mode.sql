begin;

do $migration$
begin
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where n.nspname = 'public'
      and t.typname = 'timesheet_break_entry_mode_enum'
  ) then
    create type public.timesheet_break_entry_mode_enum as enum (
      'START_END_TIMES',
      'DURATION_MINUTES'
    );
  end if;
end
$migration$;

alter table public.client_settings
  add column if not exists timesheet_break_entry_mode
    public.timesheet_break_entry_mode_enum
    not null
    default 'START_END_TIMES'::public.timesheet_break_entry_mode_enum;

alter table public.contracts
  add column if not exists timesheet_break_entry_mode
    public.timesheet_break_entry_mode_enum;

comment on column public.client_settings.timesheet_break_entry_mode is
  'Office-configured break entry presentation for standard validation timesheets.';

comment on column public.contracts.timesheet_break_entry_mode is
  'Optional contract override for break entry presentation; null inherits the client setting.';

do $verification$
begin
  if (
    select array_agg(e.enumlabel order by e.enumsortorder)
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    join pg_enum e on e.enumtypid = t.oid
    where n.nspname = 'public'
      and t.typname = 'timesheet_break_entry_mode_enum'
  ) is distinct from array['START_END_TIMES', 'DURATION_MINUTES']::text[] then
    raise exception 'TIMESHEET_BREAK_ENTRY_MODE_ENUM_INVALID';
  end if;

  if not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'client_settings'
      and column_name = 'timesheet_break_entry_mode'
      and udt_name = 'timesheet_break_entry_mode_enum'
      and is_nullable = 'NO'
  ) then
    raise exception 'CLIENT_TIMESHEET_BREAK_ENTRY_MODE_COLUMN_INVALID';
  end if;

  if not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'contracts'
      and column_name = 'timesheet_break_entry_mode'
      and udt_name = 'timesheet_break_entry_mode_enum'
      and is_nullable = 'YES'
  ) then
    raise exception 'CONTRACT_TIMESHEET_BREAK_ENTRY_MODE_COLUMN_INVALID';
  end if;
end
$verification$;

commit;
