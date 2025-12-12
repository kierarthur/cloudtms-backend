create or replace function public.weekly_import_apply_phase2(
  p_import_id uuid,
  p_system_type text
)
returns table (
  hr_row_id         uuid,
  external_row_key  text,
  work_date         date,
  incoming_code     text,
  candidate_id      uuid,
  client_id         uuid,
  week_ending_date  date,
  contract_id       uuid,
  action            text,
  reason            text,
  shift_updated     boolean
)
language plpgsql
as $$
declare
  v_sys text := upper(trim(coalesce(p_system_type,'')));
  v_src public.hr_source_enum;
begin
  if v_sys not in ('NHSP','HR_WEEKLY') then
    raise exception 'weekly_import_apply_phase2: invalid p_system_type=% (expected NHSP or HR_WEEKLY)', p_system_type;
  end if;

  v_src := case
    when v_sys = 'NHSP' then 'NHSP'::hr_source_enum
    else 'HEALTHROSTER'::hr_source_enum
  end;

  return query
  with p2 as (
    select *
    from public.weekly_import_phase2(p_import_id, v_sys)
  ),
  upd as (
    update public.nhsp_shifts s
    set
      contract_id = p2.contract_id,
      updated_at  = now(),

      -- ✅ FIX: only fill ids if currently null (do NOT overwrite existing)
      candidate_id = coalesce(s.candidate_id, p2.candidate_id),
      client_id    = coalesce(s.client_id,    p2.client_id)

    from p2
    where p2.action = 'OK'
      and p2.contract_id is not null
      and p2.external_row_key is not null
      and s.external_row_key = p2.external_row_key
      and s.latest_import_id = p_import_id
      and s.source_system    = v_src
    returning s.external_row_key
  )
  select
    p2.hr_row_id,
    p2.external_row_key,
    p2.work_date,
    p2.incoming_code,
    p2.candidate_id,
    p2.client_id,
    p2.week_ending_date,
    p2.contract_id,
    p2.action,
    p2.reason,
    (u.external_row_key is not null) as shift_updated
  from p2
  left join upd u
    on u.external_row_key = p2.external_row_key;

end;
$$;
