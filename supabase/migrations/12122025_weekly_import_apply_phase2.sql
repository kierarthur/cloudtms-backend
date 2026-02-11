
CREATE OR REPLACE FUNCTION public.weekly_import_phase2(p_import_id uuid, p_system_type text)
 RETURNS TABLE(hr_row_id uuid, external_row_key text, work_date date, incoming_code text, candidate_id uuid, client_id uuid, week_ending_date date, contract_id uuid, action text, reason text, matched_shift_id uuid, is_new boolean, is_noop boolean, is_changed boolean, old_start_utc timestamp with time zone, old_end_utc timestamp with time zone, old_break_mins integer, old_paid_minutes integer, new_start_utc timestamp with time zone, new_end_utc timestamp with time zone, new_break_mins integer, new_paid_minutes integer, delta_paid_minutes integer, is_changed_hours boolean)
 LANGUAGE plpgsql
AS $function$
declare
  v_sys text := upper(trim(coalesce(p_system_type,'')));
  v_src public.hr_source_enum;
begin
  if p_import_id is null then
    raise exception 'weekly_import_phase2: import_id is required';
  end if;

  if v_sys not in ('NHSP','HR_WEEKLY') then
    raise exception 'weekly_import_phase2: invalid p_system_type=% (expected NHSP or HR_WEEKLY)', p_system_type;
  end if;

  v_src := case
    when v_sys = 'NHSP' then 'NHSP'::public.hr_source_enum
    else 'HEALTHROSTER'::public.hr_source_enum
  end;

  return query
  with import_hdr as (
    select
      hi.id as import_id,
      hi.client_id as import_client_id
    from public.hr_imports hi
    where hi.id = p_import_id
    limit 1
  ),
  cs_we as (
    select
      cs.client_id,
      cs.week_ending_weekday
    from import_hdr ih
    join public.client_settings cs
      on cs.client_id = ih.import_client_id
    where cs.effective_from is null or cs.effective_from <= (now() at time zone 'Europe/London')::date
    order by cs.effective_from desc nulls last, cs.updated_at desc nulls last
    limit 1
  ),
  hr_raw as (
    select
      r.id as hr_row_id,
      r.external_row_key,
      r.date_local as work_date,
      nullif(btrim(coalesce(r.assignment_grade_norm,'')), '') as incoming_code,
      r.candidate_id,
      r.client_id,
      r.contract_id,
      (r.payload_json ->> 'start_utc')::timestamptz as start_utc,
      (r.payload_json ->> 'end_utc')::timestamptz as end_utc,

      -- ✅ UPDATED: "Actual Break" is authoritative (accept either key), then fall back to booked break.
      greatest(
        0,
        coalesce(
          nullif((r.payload_json ->> 'actual_break_mins'), '')::int,
          nullif((r.payload_json ->> 'actual_break_minutes'), '')::int,
          nullif((r.payload_json ->> 'break_mins'), '')::int,
          nullif((r.payload_json ->> 'break_minutes'), '')::int,
          0
        )
      ) as new_break_mins,

      greatest(
        0,
        (
          (extract(epoch from ((r.payload_json ->> 'end_utc')::timestamptz - (r.payload_json ->> 'start_utc')::timestamptz)) / 60)::int
          - coalesce(
              nullif((r.payload_json ->> 'actual_break_mins'), '')::int,
              nullif((r.payload_json ->> 'actual_break_minutes'), '')::int,
              nullif((r.payload_json ->> 'break_mins'), '')::int,
              nullif((r.payload_json ->> 'break_minutes'), '')::int,
              0
            )
        )
      ) as new_paid_minutes,

      -- week ending derived from client settings weekday (0=Sun) relative to work_date
      (
        r.date_local
        + (
            (
              coalesce((select cs_we.week_ending_weekday from cs_we limit 1), 0)
              - extract(dow from r.date_local)::int + 7
            ) % 7
          )
      )::date as week_ending_date

    from public.hr_rows r
    join import_hdr ih
      on ih.import_id = r.import_id
    where r.import_id = p_import_id
      and r.external_row_key is not null
      and r.date_local is not null
      and (r.payload_json ->> 'start_utc') is not null
      and (r.payload_json ->> 'end_utc') is not null
  ),
  matched as (
    select
      hr.hr_row_id,
      hr.external_row_key,
      hr.work_date,
      hr.incoming_code,
      hr.candidate_id,
      hr.client_id,
      hr.week_ending_date,
      hr.contract_id,

      s.id as shift_id,
      s.start_utc as old_start_utc,
      s.end_utc as old_end_utc,
      coalesce(s.break_mins, 0) as old_break_mins,

      greatest(
        0,
        (extract(epoch from (s.end_utc - s.start_utc)) / 60)::int - coalesce(s.break_mins, 0)
      ) as old_paid_minutes,

      hr.start_utc as new_start_utc,
      hr.end_utc as new_end_utc,
      hr.new_break_mins,
      hr.new_paid_minutes

    from hr_raw hr
    left join public.nhsp_shifts s
      on s.external_row_key = hr.external_row_key
     and s.source_system = v_src
     and s.cancelled_at_utc is null
  ),
  delta as (
    select
      m.*,
      (coalesce(m.new_paid_minutes,0) - coalesce(m.old_paid_minutes,0)) as delta_paid_minutes,
      (
        m.shift_id is null
      ) as is_new,
      (
        m.shift_id is not null
        and coalesce(m.old_start_utc, null) = coalesce(m.new_start_utc, null)
        and coalesce(m.old_end_utc, null)   = coalesce(m.new_end_utc, null)
        and coalesce(m.old_break_mins,0)    = coalesce(m.new_break_mins,0)
      ) as is_noop
    from matched m
  )
  select
    d.hr_row_id,
    d.external_row_key,
    d.work_date,
    d.incoming_code,
    d.candidate_id,
    d.client_id,
    d.week_ending_date,
    d.contract_id,

    case
      when d.external_row_key is null then 'REJECT_MISSING_KEY'
      when d.candidate_id is null then 'REJECT_NO_CANDIDATE'
      when d.client_id is null then 'REJECT_NO_CLIENT'
      when d.contract_id is null then 'REJECT_NO_CONTRACT'
      when d.work_date is null then 'REJECT_NO_WORK_DATE'
      when d.week_ending_date is null then 'REJECT_NO_WEEK_ENDING'
      else 'OK'
    end as action,

    ''::text as reason,

    d.shift_id as matched_shift_id,
    d.is_new,
    d.is_noop,

    (not d.is_noop) as is_changed,

    d.old_start_utc,
    d.old_end_utc,
    d.old_break_mins,
    d.old_paid_minutes,

    d.new_start_utc,
    d.new_end_utc,
    d.new_break_mins,
    d.new_paid_minutes,

    d.delta_paid_minutes,

    (
      d.shift_id is not null
      and (coalesce(d.delta_paid_minutes,0) <> 0 or coalesce(d.old_break_mins,0) <> coalesce(d.new_break_mins,0))
    ) as is_changed_hours

  from delta d
  order by d.work_date, d.external_row_key;

end;
$function$;
