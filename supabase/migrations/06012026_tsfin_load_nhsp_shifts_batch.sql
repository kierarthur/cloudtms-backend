-- 06012026_tsfin_load_nhsp_shifts_batch.sql
-- Batch-load nhsp_shifts for multiple timesheet_ids in one RPC.
--
-- Why:
-- - Weekly NHSP rebuild currently does one REST call per timesheet to fetch nhsp_shifts.
-- - With TSFIN batching, this becomes the dominant subrequest cost for NHSP-heavy runs.
--
-- Behaviour:
-- - Returns one row per input timesheet_id (even if no shifts -> empty array).
-- - Includes all columns from public.nhsp_shifts via to_jsonb(s).
-- - Orders shifts by work_date asc, start_utc asc, id asc (stable ordering).

create or replace function public.tsfin_load_nhsp_shifts_batch(p_timesheet_ids uuid[])
returns table (
  timesheet_id uuid,
  shifts jsonb
)
language sql
stable
as $$
  with input_ids as (
    select distinct unnest(p_timesheet_ids) as timesheet_id
    where p_timesheet_ids is not null
  )
  select
    i.timesheet_id,
    coalesce(
      jsonb_agg(to_jsonb(s) order by s.work_date asc, s.start_utc asc, s.id asc)
        filter (where s.id is not null),
      '[]'::jsonb
    ) as shifts
  from input_ids i
  left join public.nhsp_shifts s
    on s.timesheet_id = i.timesheet_id
  group by i.timesheet_id
  order by i.timesheet_id;
$$;


-- Optional but recommended index to speed up ordered retrieval per timesheet_id
-- (your current idx_nhsp_shifts_timesheet only indexes timesheet_id, so Postgres may still sort).
create index if not exists idx_nhsp_shifts_timesheet_date_start
  on public.nhsp_shifts (timesheet_id, work_date, start_utc, id)
  where timesheet_id is not null;
