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

  -- Current shift state + “safe to repair?” flags
  cur as (
    select
      p2.hr_row_id,
      p2.external_row_key,
      p2.work_date,
      p2.incoming_code,
      p2.candidate_id as p2_candidate_id,
      p2.client_id    as p2_client_id,
      p2.week_ending_date,
      p2.contract_id  as p2_contract_id,
      p2.action       as p2_action,
      p2.reason       as p2_reason,

      s.id            as shift_id,
      s.timesheet_id  as existing_timesheet_id,
      s.candidate_id  as existing_candidate_id,
      s.client_id     as existing_client_id,
      s.contract_id   as existing_contract_id,

      fin.locked_by_invoice_id as fin_locked_by_invoice_id,
      fin.paid_at_utc          as fin_paid_at_utc,

      -- if there is no current TSFIN row, treat as “safe/unlocked” for repair purposes
      (fin.timesheet_id is null) as tsfin_missing,

      -- Safe if:
      --  - shift not linked to TS yet, OR
      --  - no current TSFIN row exists, OR
      --  - current TSFIN exists and is not paid and not invoice-locked
      (
        s.timesheet_id is null
        or fin.timesheet_id is null
        or (fin.locked_by_invoice_id is null and fin.paid_at_utc is null)
      ) as safe_to_move,

      -- Only detach/reassign if anything differs vs Phase2
      (
        (s.candidate_id is distinct from p2.candidate_id)
        or (s.client_id  is distinct from p2.client_id)
        or (s.contract_id is distinct from p2.contract_id)
      ) as needs_reassign

    from p2
    left join public.nhsp_shifts s
      on s.external_row_key = p2.external_row_key
     and s.latest_import_id = p_import_id
     and s.source_system    = v_src
    left join public.timesheets_financials fin
      on fin.timesheet_id = s.timesheet_id
     and fin.is_current   = true
  ),

  -- Apply Phase2 mapping into nhsp_shifts with safe repair:
  -- - if safe_to_move AND needs_reassign: detach from old timesheet (timesheet_id=NULL) and overwrite ids/contract
  -- - if NOT safe_to_move (paid/locked): do NOT change anything
  upd as (
    update public.nhsp_shifts s
    set
      updated_at = now(),

      -- Only update mapping when safe (prevents moving paid/invoiced weeks implicitly)
      contract_id = case
        when coalesce(cur.safe_to_move,false) then cur.p2_contract_id
        else s.contract_id
      end,

      candidate_id = case
        when coalesce(cur.safe_to_move,false) then coalesce(cur.p2_candidate_id, s.candidate_id)
        else s.candidate_id
      end,

      client_id = case
        when coalesce(cur.safe_to_move,false) then coalesce(cur.p2_client_id, s.client_id)
        else s.client_id
      end,

      -- If we are changing any of the mapping keys AND it is safe, detach so Apply can re-link correctly
      timesheet_id = case
        when coalesce(cur.safe_to_move,false) and coalesce(cur.needs_reassign,false) then null
        else s.timesheet_id
      end

    from cur
    where cur.p2_action = 'OK'
      and cur.p2_contract_id is not null
      and cur.external_row_key is not null
      and s.external_row_key = cur.external_row_key
      and s.latest_import_id = p_import_id
      and s.source_system    = v_src
      and coalesce(cur.safe_to_move,false) = true
      and (
        -- only do work if something would actually change
        coalesce(cur.needs_reassign,false) = true
        or s.contract_id is distinct from cur.p2_contract_id
      )
    returning s.external_row_key
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

    -- Override action/reason when Phase2 says OK but we cannot safely detach/reassign
    case
      when cur.p2_action = 'OK'
        and coalesce(cur.needs_reassign,false) = true
        and coalesce(cur.safe_to_move,false) = false
        and cur.shift_id is not null
      then 'BLOCK_LOCKED_OR_PAID'
      else cur.p2_action
    end as action,

    case
      when cur.p2_action = 'OK'
        and coalesce(cur.needs_reassign,false) = true
        and coalesce(cur.safe_to_move,false) = false
        and cur.shift_id is not null
      then 'Shift is linked to a timesheet that is paid and/or invoice-locked; cannot reassign automatically.'
      else cur.p2_reason
    end as reason,

    (u.external_row_key is not null) as shift_updated

  from cur
  left join upd u
    on u.external_row_key = cur.external_row_key;

end;
$$;
