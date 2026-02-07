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
    when v_sys = 'NHSP' then 'NHSP'::public.hr_source_enum
    else 'HEALTHROSTER'::public.hr_source_enum
  end;

  return query
  with p2 as (
    select *
    from public.weekly_import_phase2(p_import_id, v_sys)
  ),

  -- Current shift state + informational finance flags (NOT used to block)
  cur as (
    select
      p2r.hr_row_id,
      p2r.external_row_key,
      p2r.work_date,
      p2r.incoming_code,
      p2r.candidate_id as p2_candidate_id,
      p2r.client_id    as p2_client_id,
      p2r.week_ending_date,
      p2r.contract_id  as p2_contract_id,
      p2r.action       as p2_action,
      p2r.reason       as p2_reason,

      s.id            as shift_id,
      s.timesheet_id  as existing_timesheet_id,
      s.candidate_id  as existing_candidate_id,
      s.client_id     as existing_client_id,
      s.contract_id   as existing_contract_id,

      (ts.timesheet_id is not null) as existing_timesheet_exists,

      fin.locked_by_invoice_id as fin_locked_by_invoice_id,
      fin.paid_at_utc          as fin_paid_at_utc,

      -- Only detach/reassign if mapping differs vs Phase2
      (
        (s.candidate_id is distinct from p2r.candidate_id)
        or (s.client_id  is distinct from p2r.client_id)
        or (s.contract_id is distinct from p2r.contract_id)
      ) as needs_reassign,

      -- Also detach if the shift points at a missing/deleted timesheet row
      (
        s.timesheet_id is not null
        and ts.timesheet_id is null
      ) as needs_relink_missing_timesheet,

      (
        coalesce(
          (
            (s.candidate_id is distinct from p2r.candidate_id)
            or (s.client_id  is distinct from p2r.client_id)
            or (s.contract_id is distinct from p2r.contract_id)
          ),
          false
        )
        or
        coalesce(
          (
            s.timesheet_id is not null
            and ts.timesheet_id is null
          ),
          false
        )
      ) as needs_detach

    from p2 p2r
    left join public.nhsp_shifts s
      on s.external_row_key = p2r.external_row_key
     and s.latest_import_id = p_import_id
     and s.source_system    = v_src
    left join public.timesheets ts
      on ts.timesheet_id = s.timesheet_id
    left join public.timesheets_financials fin
      on fin.timesheet_id = s.timesheet_id
     and fin.is_current   = true
  ),

  -- Apply Phase2 mapping into nhsp_shifts:
  -- POLICY: paid/locked does NOT block truth repair. If Phase2 says OK and mapping differs, detach+overwrite.
  upd as (
    update public.nhsp_shifts su
    set
      updated_at = now(),

      -- Update mapping keys from Phase2
      contract_id = cur.p2_contract_id,
      candidate_id = coalesce(cur.p2_candidate_id, su.candidate_id),
      client_id    = coalesce(cur.p2_client_id, su.client_id),

      -- Detach if mapping differs OR if timesheet row is missing (deleted), so downstream ensure+attach can relink.
      timesheet_id = case
        when coalesce(cur.needs_detach, false) then null
        else su.timesheet_id
      end

    from cur
    where cur.p2_action = 'OK'
      and cur.p2_contract_id is not null
      and cur.external_row_key is not null
      and su.external_row_key = cur.external_row_key
      and su.latest_import_id = p_import_id
      and su.source_system    = v_src
      and (
        coalesce(cur.needs_detach,false) = true
        or su.contract_id is distinct from cur.p2_contract_id
        or su.candidate_id is distinct from cur.p2_candidate_id
        or su.client_id is distinct from cur.p2_client_id
      )
    returning su.external_row_key
  )

  select
    cur.hr_row_id,
    cur.external_row_key,
    cur.work_date,
    cur.incoming_code,
    cur.p2_candidate_id as candidate_id,
    cur.p2_client_id    as client_id,
    cur.week_ending_date,
    cur.p2_contract_id  as contract_id,

    -- No paid/locked blocking in Phase2. Keep Phase2 action as-is.
    cur.p2_action as action,

    -- Informational reason stitching for detach scenarios (no blocking)
    case
      when cur.p2_action = 'OK'
        and coalesce(cur.needs_detach,false) = true
        and cur.shift_id is not null
      then
        (
          case
            when nullif(btrim(coalesce(cur.p2_reason,'')),'') is null then ''
            else cur.p2_reason || ' '
          end
        )
        ||
        (
          case
            when coalesce(cur.needs_reassign,false) is true and coalesce(cur.needs_relink_missing_timesheet,false) is true
              then 'Shift mapping changed and the shift was linked to a missing/deleted timesheet; truth was updated and shift detached for relink.'
            when coalesce(cur.needs_reassign,false) is true
              then 'Shift mapping changed; truth was updated and shift detached for relink.'
            when coalesce(cur.needs_relink_missing_timesheet,false) is true
              then 'Shift was linked to a missing/deleted timesheet; truth was updated and shift detached for relink.'
            else 'Truth was updated and shift detached for relink.'
          end
        )
        ||
        (
          case
            when (cur.fin_locked_by_invoice_id is not null or cur.fin_paid_at_utc is not null)
              then ' Issued invoices remain immutable; any financial correction must be represented by standard reversal/adjustment artefacts.'
            else ''
          end
        )
      else cur.p2_reason
    end as reason,

    (u.external_row_key is not null) as shift_updated

  from cur
  left join upd u
    on u.external_row_key = cur.external_row_key;

end;
$$;
