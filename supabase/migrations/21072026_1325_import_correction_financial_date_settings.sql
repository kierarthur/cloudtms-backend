-- CloudTMS import-authoritative correction financial-date settings.
--
-- Intended repository destination:
--   cloudtms-backend/supabase/migrations/
--
-- This migration is deliberately rerunnable:
--   * a compatible enum/column layout is accepted;
--   * existing non-null global values are preserved;
--   * null global values are initialised to PAID_DATE;
--   * client overrides remain nullable with no column default;
--   * no existing client override is backfilled or overwritten;
--   * incompatible pre-existing objects fail atomically;
--   * no function, trigger, payment, TSFIN or invoice data is changed.

begin;

set local lock_timeout = '10s';
set local statement_timeout = '60s';

do $migration$
declare
  v_relkind "char";
begin
  if not pg_catalog.pg_try_advisory_xact_lock(
    pg_catalog.hashtextextended(
      'cloudtms|import-correction-financial-date-settings|v1',
      21072026
    )
  ) then
    raise exception 'IMPORT_CORRECTION_FINANCIAL_DATE_SETTINGS_MIGRATION_BUSY'
      using errcode = '55P03';
  end if;

  select c.relkind
    into v_relkind
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public'
    and c.relname = 'settings_defaults';

  if not found or v_relkind not in ('r', 'p') then
    raise exception 'SETTINGS_DEFAULTS_TABLE_REQUIRED'
      using errcode = '42P01';
  end if;

  select c.relkind
    into v_relkind
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public'
    and c.relname = 'client_settings';

  if not found or v_relkind not in ('r', 'p') then
    raise exception 'CLIENT_SETTINGS_TABLE_REQUIRED'
      using errcode = '42P01';
  end if;

  if not exists (
    select 1
    from public.settings_defaults d
    where d.id = 1
  ) then
    raise exception 'GLOBAL_SETTINGS_DEFAULTS_ROW_REQUIRED'
      using errcode = 'P0001',
            detail = 'Expected public.settings_defaults.id = 1.';
  end if;
end
$migration$;

do $migration$
declare
  v_type_oid oid;
  v_type_kind "char";
  v_labels text[];
begin
  select t.oid, t.typtype
    into v_type_oid, v_type_kind
  from pg_catalog.pg_type t
  join pg_catalog.pg_namespace n on n.oid = t.typnamespace
  where n.nspname = 'public'
    and t.typname = 'correction_financials_date_basis_enum';

  if not found then
    create type public.correction_financials_date_basis_enum as enum (
      'PAID_DATE',
      'NOW'
    );

    select t.oid, t.typtype
      into v_type_oid, v_type_kind
    from pg_catalog.pg_type t
    join pg_catalog.pg_namespace n on n.oid = t.typnamespace
    where n.nspname = 'public'
      and t.typname = 'correction_financials_date_basis_enum';
  end if;

  if v_type_kind <> 'e' then
    raise exception 'CORRECTION_FINANCIALS_DATE_BASIS_TYPE_INCOMPATIBLE'
      using errcode = 'P0001',
            detail = pg_catalog.jsonb_build_object(
              'expected_type_kind', 'enum',
              'actual_type_kind', v_type_kind
            )::text;
  end if;

  select pg_catalog.array_agg(e.enumlabel order by e.enumsortorder)
    into v_labels
  from pg_catalog.pg_enum e
  where e.enumtypid = v_type_oid;

  if v_labels is distinct from array['PAID_DATE', 'NOW']::text[] then
    raise exception 'CORRECTION_FINANCIALS_DATE_BASIS_ENUM_INCOMPATIBLE'
      using errcode = 'P0001',
            detail = pg_catalog.jsonb_build_object(
              'expected_labels', pg_catalog.jsonb_build_array('PAID_DATE', 'NOW'),
              'actual_labels', pg_catalog.to_jsonb(v_labels)
            )::text;
  end if;
end
$migration$;

-- Validate any pre-existing target columns before making changes. A column
-- with the correct name but a different type is not silently converted.
do $migration$
declare
  v_target record;
  v_enum_oid oid := pg_catalog.to_regtype(
    'public.correction_financials_date_basis_enum'
  )::oid;
  v_actual_type_oid oid;
  v_actual_type text;
begin
  for v_target in
    select *
    from (values
      ('settings_defaults'::text, 'reversal_complete_financials_date'::text),
      ('settings_defaults', 'reversal_replacement_financials_date'),
      ('client_settings', 'reversal_complete_financials_date'),
      ('client_settings', 'reversal_replacement_financials_date')
    ) as target(table_name, column_name)
  loop
    select a.atttypid, pg_catalog.format_type(a.atttypid, a.atttypmod)
      into v_actual_type_oid, v_actual_type
    from pg_catalog.pg_attribute a
    join pg_catalog.pg_class c on c.oid = a.attrelid
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public'
      and c.relname = v_target.table_name
      and a.attname = v_target.column_name
      and a.attnum > 0
      and not a.attisdropped;

    if found and v_actual_type_oid <> v_enum_oid then
      raise exception 'CORRECTION_FINANCIALS_DATE_COLUMN_TYPE_INCOMPATIBLE'
        using errcode = 'P0001',
              detail = pg_catalog.jsonb_build_object(
                'table', v_target.table_name,
                'column', v_target.column_name,
                'expected_type', 'public.correction_financials_date_basis_enum',
                'actual_type', v_actual_type
              )::text;
    end if;
  end loop;
end
$migration$;

alter table public.settings_defaults
  add column if not exists reversal_complete_financials_date
    public.correction_financials_date_basis_enum;

alter table public.settings_defaults
  add column if not exists reversal_replacement_financials_date
    public.correction_financials_date_basis_enum;

alter table public.client_settings
  add column if not exists reversal_complete_financials_date
    public.correction_financials_date_basis_enum;

alter table public.client_settings
  add column if not exists reversal_replacement_financials_date
    public.correction_financials_date_basis_enum;

-- Initialise only missing global defaults. Existing PAID_DATE or NOW values
-- survive both the first run and every subsequent raw-SQL rerun.
update public.settings_defaults
set reversal_complete_financials_date = coalesce(
      reversal_complete_financials_date,
      'PAID_DATE'::public.correction_financials_date_basis_enum
    ),
    reversal_replacement_financials_date = coalesce(
      reversal_replacement_financials_date,
      'PAID_DATE'::public.correction_financials_date_basis_enum
    )
where reversal_complete_financials_date is null
   or reversal_replacement_financials_date is null;

alter table public.settings_defaults
  alter column reversal_complete_financials_date
    set default 'PAID_DATE'::public.correction_financials_date_basis_enum,
  alter column reversal_complete_financials_date set not null,
  alter column reversal_replacement_financials_date
    set default 'PAID_DATE'::public.correction_financials_date_basis_enum,
  alter column reversal_replacement_financials_date set not null;

-- NULL means dynamic global inheritance. Removing a client-level default is
-- intentional: new client_settings rows inherit through the resolver rather
-- than persisting a copy of the then-current global value.
alter table public.client_settings
  alter column reversal_complete_financials_date drop default,
  alter column reversal_complete_financials_date drop not null,
  alter column reversal_replacement_financials_date drop default,
  alter column reversal_replacement_financials_date drop not null;

comment on type public.correction_financials_date_basis_enum is
  'Allowed frozen financial-date bases for import-authoritative correction legs.';

comment on column public.settings_defaults.reversal_complete_financials_date is
  'Global reversal-leg default: PAID_DATE means original/root frozen treatment; NOW means the frozen correction-operation date.';

comment on column public.settings_defaults.reversal_replacement_financials_date is
  'Global replacement-leg default: PAID_DATE means original/root frozen treatment; NOW means the frozen correction-operation date.';

comment on column public.client_settings.reversal_complete_financials_date is
  'Nullable import-authoritative-client reversal override. NULL dynamically inherits settings_defaults and retained values are ignored while the client is ineligible.';

comment on column public.client_settings.reversal_replacement_financials_date is
  'Nullable import-authoritative-client replacement override. NULL dynamically inherits settings_defaults and retained values are ignored while the client is ineligible.';

-- Fail the transaction if the final schema differs from the RPC contract.
do $migration$
declare
  v_type_oid oid;
  v_labels text[];
  v_column record;
  v_actual_type_oid oid;
  v_not_null boolean;
  v_has_default boolean;
  v_default_expression text;
begin
  select t.oid
    into v_type_oid
  from pg_catalog.pg_type t
  join pg_catalog.pg_namespace n on n.oid = t.typnamespace
  where n.nspname = 'public'
    and t.typname = 'correction_financials_date_basis_enum'
    and t.typtype = 'e';

  if not found then
    raise exception 'CORRECTION_FINANCIALS_DATE_BASIS_ENUM_POSTCONDITION_FAILED'
      using errcode = 'P0001';
  end if;

  select pg_catalog.array_agg(e.enumlabel order by e.enumsortorder)
    into v_labels
  from pg_catalog.pg_enum e
  where e.enumtypid = v_type_oid;

  if v_labels is distinct from array['PAID_DATE', 'NOW']::text[] then
    raise exception 'CORRECTION_FINANCIALS_DATE_BASIS_ENUM_POSTCONDITION_FAILED'
      using errcode = 'P0001';
  end if;

  for v_column in
    select *
    from (values
      ('settings_defaults'::text, 'reversal_complete_financials_date'::text, true, true),
      ('settings_defaults', 'reversal_replacement_financials_date', true, true),
      ('client_settings', 'reversal_complete_financials_date', false, false),
      ('client_settings', 'reversal_replacement_financials_date', false, false)
    ) as expected(table_name, column_name, expected_not_null, default_required)
  loop
    select
      a.atttypid,
      a.attnotnull,
      ad.oid is not null,
      case
        when ad.oid is null then null
        else pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)
      end
    into
      v_actual_type_oid,
      v_not_null,
      v_has_default,
      v_default_expression
    from pg_catalog.pg_attribute a
    join pg_catalog.pg_class c on c.oid = a.attrelid
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    left join pg_catalog.pg_attrdef ad
      on ad.adrelid = a.attrelid
     and ad.adnum = a.attnum
    where n.nspname = 'public'
      and c.relname = v_column.table_name
      and a.attname = v_column.column_name
      and a.attnum > 0
      and not a.attisdropped;

    if not found
       or v_actual_type_oid <> v_type_oid
       or v_not_null is distinct from v_column.expected_not_null
       or (
         v_column.default_required
         and (
           not v_has_default
           or pg_catalog.strpos(
             coalesce(v_default_expression, ''),
             'PAID_DATE'
           ) = 0
         )
       )
       or (not v_column.default_required and v_has_default) then
      raise exception 'CORRECTION_FINANCIALS_DATE_COLUMN_POSTCONDITION_FAILED'
        using errcode = 'P0001',
              detail = pg_catalog.jsonb_build_object(
                'table', v_column.table_name,
                'column', v_column.column_name,
                'actual_type_oid', v_actual_type_oid,
                'expected_type_oid', v_type_oid,
                'actual_not_null', v_not_null,
                'expected_not_null', v_column.expected_not_null,
                'default_expression', v_default_expression,
                'default_required', v_column.default_required
              )::text;
    end if;
  end loop;

  if exists (
    select 1
    from public.settings_defaults d
    where d.reversal_complete_financials_date is null
       or d.reversal_replacement_financials_date is null
  ) then
    raise exception 'CORRECTION_FINANCIALS_DATE_GLOBAL_VALUE_POSTCONDITION_FAILED'
      using errcode = 'P0001';
  end if;
end
$migration$;

commit;
