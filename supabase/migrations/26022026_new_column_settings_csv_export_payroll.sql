do $$
declare
  v_col_exists boolean;
  v_con_exists boolean;

  -- Keep the default as TEXT so we can safely embed it into dynamic SQL for the DEFAULT expression.
  v_default_text text := '["payment_reference","payee_name","sort_code","account_number","account_type","amount"]';
begin
  -- 1) Add column (safe to rerun)
  select exists (
    select 1
    from information_schema.columns c
    where c.table_schema = 'public'
      and c.table_name = 'settings_defaults'
      and c.column_name = 'pay_export_csv_columns_json'
  ) into v_col_exists;

  if not v_col_exists then
    alter table public.settings_defaults
      add column pay_export_csv_columns_json jsonb;
  end if;

  -- 2) Set default (safe to rerun)
  execute
    'alter table public.settings_defaults ' ||
    'alter column pay_export_csv_columns_json ' ||
    'set default ' || quote_literal(v_default_text) || '::jsonb';

  -- 3) Backfill existing rows if NULL (safe to rerun)
  update public.settings_defaults sd
     set pay_export_csv_columns_json = v_default_text::jsonb
   where sd.pay_export_csv_columns_json is null;

  -- 4) Add CHECK: NULL or JSON array (safe to rerun)
  select exists (
    select 1
    from pg_constraint con
    join pg_class rel on rel.oid = con.conrelid
    join pg_namespace nsp on nsp.oid = rel.relnamespace
    where nsp.nspname = 'public'
      and rel.relname = 'settings_defaults'
      and con.conname = 'settings_defaults_pay_export_csv_columns_json_is_array_chk'
  ) into v_con_exists;

  if not v_con_exists then
    alter table public.settings_defaults
      add constraint settings_defaults_pay_export_csv_columns_json_is_array_chk
      check (
        pay_export_csv_columns_json is null
        or jsonb_typeof(pay_export_csv_columns_json) = 'array'
      );
  end if;
end
$$;
