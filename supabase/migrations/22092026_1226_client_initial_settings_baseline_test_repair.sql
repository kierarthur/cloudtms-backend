-- One-time repair for the TEST client created before the initial-settings
-- baseline correction. On databases without this exact client the migration
-- is an intentional no-op.

\set ON_ERROR_STOP on

begin;

do $repair$
declare
  v_client_id uuid;
  v_settings_id uuid;
  v_settings_count integer;
begin
  select c.id into v_client_id
  from public.clients c
  where c.cli_ref='CLI-04170'
    and c.name='CloudTMS Stage 8 NHSP Test Trust'
  for update;

  if v_client_id is null then
    return;
  end if;

  select count(*), min(cs.id::text)::uuid
  into v_settings_count, v_settings_id
  from public.client_settings cs
  where cs.client_id=v_client_id;

  if v_settings_count<>1 or v_settings_id is null then
    raise exception 'CLIENT_INITIAL_SETTINGS_TEST_REPAIR_UNSAFE'
      using errcode='55000';
  end if;

  update public.client_settings
  set effective_from=DATE '1900-01-01',
      updated_at=statement_timestamp()
  where id=v_settings_id
    and effective_from=DATE '2026-09-22';

  if not found then
    raise exception 'CLIENT_INITIAL_SETTINGS_TEST_REPAIR_SOURCE_MISMATCH'
      using errcode='55000';
  end if;
end
$repair$;

commit;
