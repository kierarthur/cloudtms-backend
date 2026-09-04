-- One-time TEST compatibility repair for historical Clients created without
-- the settings canvas now created atomically for every new Client.
-- Existing Client settings are never updated or replaced.

\set ON_ERROR_STOP on

begin;

do $migration$
declare
  v_inserted integer := 0;
begin
  if not exists(select 1 from public.settings_defaults where id=1) then
    raise exception 'CLIENT_SETTINGS_REPAIR_GLOBAL_SETTINGS_MISSING';
  end if;

  with earliest_contract as (
    select c.client_id,min(c.start_date) as earliest_contract_date
    from public.contracts c
    group by c.client_id
  )
  insert into public.client_settings(
    client_id,
    effective_from,
    timezone_id,
    day_start,
    day_end,
    night_start,
    night_end,
    sat_start,
    sat_end,
    sun_start,
    sun_end,
    bh_start,
    bh_end,
    bh_source,
    bh_list,
    bh_feed_url,
    ts_reference_required,
    hr_attach_to_invoice,
    ts_attach_to_invoice,
    healthroster_import_auto_authorise,
    nhsp_import_auto_authorise,
    candidate_electronic_auto_authorise,
    candidate_paper_submission_enabled
  )
  select
    cl.id,
    coalesce(
      ec.earliest_contract_date,
      (cl.created_at at time zone 'Europe/London')::date,
      (clock_timestamp() at time zone 'Europe/London')::date
    ),
    d.timezone_id,
    d.day_start,
    d.day_end,
    d.night_start,
    d.night_end,
    d.sat_start,
    d.sat_end,
    d.sun_start,
    d.sun_end,
    d.bh_start,
    d.bh_end,
    d.bh_source,
    d.bh_list,
    d.bh_feed_url,
    d.ts_reference_required,
    d.hr_attach_to_invoice,
    d.ts_attach_to_invoice,
    d.healthroster_import_auto_authorise_default,
    d.nhsp_import_auto_authorise_default,
    d.candidate_electronic_auto_authorise_default,
    true
  from public.clients cl
  cross join public.settings_defaults d
  left join earliest_contract ec on ec.client_id=cl.id
  where d.id=1
    and not exists(
      select 1 from public.client_settings cs where cs.client_id=cl.id
    );

  get diagnostics v_inserted = row_count;

  if exists(
    select 1
    from public.clients cl
    where not exists(
      select 1 from public.client_settings cs where cs.client_id=cl.id
    )
  ) then
    raise exception 'CLIENT_SETTINGS_REPAIR_INCOMPLETE';
  end if;

  raise notice 'Inserted default Client settings canvases for % historical Client records.',v_inserted;
end
$migration$;

commit;
