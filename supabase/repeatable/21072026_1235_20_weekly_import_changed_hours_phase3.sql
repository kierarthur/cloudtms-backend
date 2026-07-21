-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: d4953e1502ac.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.weekly_import_changed_hours_phase3(p_import_id uuid, p_system_type text)
 RETURNS TABLE(hr_row_id uuid, external_row_key text, shift_id uuid, source_system text, candidate_id uuid, client_id uuid, contract_id uuid, timesheet_id uuid, contract_self_bill boolean, work_date date, week_ending_date date, old_start_utc timestamp with time zone, old_end_utc timestamp with time zone, old_break_mins integer, new_start_utc timestamp with time zone, new_end_utc timestamp with time zone, new_break_mins integer, old_paid_minutes integer, new_paid_minutes integer, is_changed_hours boolean, is_paid boolean, is_invoiced boolean, invoice_id_detected uuid, old_pay_ex numeric, old_charge_ex numeric, new_pay_ex numeric, new_charge_ex numeric, delta_pay_ex numeric, delta_charge_ex numeric, requires_pay_decision boolean, requires_invoice_decision boolean, requires_any_decision boolean)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_sys public.hr_source_enum;
begin
  v_sys :=
    case
      when upper(coalesce(p_system_type,'')) = 'NHSP' then 'NHSP'::public.hr_source_enum
      when upper(coalesce(p_system_type,'')) = 'HEALTHROSTER' then 'HEALTHROSTER'::public.hr_source_enum
      else null::public.hr_source_enum
    end;

  if v_sys is null then
    raise exception 'weekly_import_changed_hours_phase3: invalid p_system_type "%". Expected NHSP or HEALTHROSTER.', p_system_type;
  end if;

  return query
  with rows_in as (
    select
      r.id as hr_row_id,
      r.external_row_key,

      -- POLICY: shift "date" is derived from start time local date (Europe/London), not date_local.
      ((date_trunc('minute', (r.payload_json->>'start_utc')::timestamptz) at time zone 'Europe/London')::date) as work_date,

      date_trunc('minute', (r.payload_json->>'start_utc')::timestamptz) as new_start_utc,
      date_trunc('minute', (r.payload_json->>'end_utc')::timestamptz)   as new_end_utc,

      -- ✅ FIX: HealthRoster weekly uses Actual Break as authoritative.
      -- Priority: actual_break_mins / actual_break_minutes -> break_mins / break_minutes -> 0
      case
        when (r.payload_json ? 'actual_break_mins') and ((r.payload_json->>'actual_break_mins') ~ '^[0-9]+$')
          then (r.payload_json->>'actual_break_mins')::int
        when (r.payload_json ? 'actual_break_minutes') and ((r.payload_json->>'actual_break_minutes') ~ '^[0-9]+$')
          then (r.payload_json->>'actual_break_minutes')::int
        when (r.payload_json ? 'break_mins') and ((r.payload_json->>'break_mins') ~ '^[0-9]+$')
          then (r.payload_json->>'break_mins')::int
        when (r.payload_json ? 'break_minutes') and ((r.payload_json->>'break_minutes') ~ '^[0-9]+$')
          then (r.payload_json->>'break_minutes')::int
        else 0
      end as new_break_mins
    from public.hr_rows r
    where r.import_id = p_import_id
      and r.external_row_key is not null
      and (r.payload_json->>'start_utc') is not null
      and (r.payload_json->>'end_utc')   is not null
  ),
  matched as (
    select
      ri.*,
      s.id as shift_id,
      s.source_system::text as source_system,
      s.candidate_id,
      s.client_id,
      s.contract_id,
      s.timesheet_id,

      -- week_ending_date resolution (DO NOT assume Sunday):
      -- 1) base timesheet week_ending_date (authoritative) if present
      -- 2) nhsp_shifts.week_ending_date if present
      -- 3) derived from contracts.week_ending_weekday_snapshot (0=Sun) and basis_date (old shift start local date, else import work_date)
      coalesce(
        ts.week_ending_date,
        s.week_ending_date,
        (
          coalesce(
            (date_trunc('minute', s.start_utc) at time zone 'Europe/London')::date,
            ri.work_date
          )
          +
          (
            (
              (
                case
                  when c.week_ending_weekday_snapshot is null then 0
                  when c.week_ending_weekday_snapshot between 0 and 6 then c.week_ending_weekday_snapshot
                  else 0
                end
                -
                extract(dow from coalesce(
                  (date_trunc('minute', s.start_utc) at time zone 'Europe/London')::date,
                  ri.work_date
                ))::int
                + 7
              ) % 7
            )::int
          )
        )::date
      ) as week_ending_date,

      -- old values truncated to minute precision for comparison + output consistency
      date_trunc('minute', s.start_utc) as old_start_utc,
      date_trunc('minute', s.end_utc)   as old_end_utc,
      coalesce(s.break_mins,0) as old_break_mins,
      coalesce(s.pay_minutes,0) as old_paid_minutes,

      s.invoice_id as shift_invoice_id,

      c.self_bill as contract_self_bill
    from rows_in ri
    left join public.nhsp_shifts s
      on s.external_row_key = ri.external_row_key
     and s.source_system = v_sys
     and s.cancelled_at_utc is null
    left join public.contracts c
      on c.id = s.contract_id
    left join public.timesheets ts
      on ts.timesheet_id = s.timesheet_id
     and ts.is_current = true
  ),
  fin as (
    select
      m.*,
      tf.id as tsfin_id,
      tf.paid_at_utc,
      tf.locked_by_invoice_id,
      tf.invoice_breakdown_json,
      tf.policy_snapshot_json,
      tf.pay_day, tf.pay_night, tf.pay_sat, tf.pay_sun, tf.pay_bh,
      tf.charge_day, tf.charge_night, tf.charge_sat, tf.charge_sun, tf.charge_bh,
      invoice_line.invoice_id as invoice_line_invoice_id,
      chain_scope.chain_json as correction_chain_json
    from matched m
    left join public.timesheets_financials tf
      on tf.timesheet_id = m.timesheet_id
     and tf.is_current = true
    left join lateral (
      select il.invoice_id
      from public.invoice_lines il
      join public.invoices i on i.id = il.invoice_id
      where il.timesheet_id = m.timesheet_id
      order by coalesce(i.issued_at_utc, i.created_at) desc, il.invoice_id desc
      limit 1
    ) invoice_line on true
    left join lateral (
      select public.timesheet_correction_chain_scope_v1(
        m.timesheet_id,
        false,
        32,
        100
      ) as chain_json
      where m.timesheet_id is not null
    ) chain_scope on true
  ),
  seg_old as (
    select
      f.*,
      (seg->>'pay_amount')::numeric     as seg_old_pay_ex,
      (seg->>'charge_amount')::numeric  as seg_old_charge_ex,
      nullif(seg->>'invoice_locked_invoice_id','')::uuid as seg_invoice_id
    from fin f
    left join lateral (
      select t.seg
      from jsonb_array_elements(coalesce(f.invoice_breakdown_json->'segments','[]'::jsonb)) as t(seg)
      where (
        (t.seg->>'nhsp_shift_id') = f.shift_id::text
        or (t.seg->>'external_row_key') = f.external_row_key
      )
      order by
        case when (t.seg->>'nhsp_shift_id') = f.shift_id::text then 0 else 1 end
      limit 1
    ) x(seg) on true
  ),
  invline_old as (
    select
      s.*,
      case
        when upper(coalesce(s.source_system,'')) = 'NHSP' then (
          select max(il.total_charge_ex_vat)
          from public.invoice_lines il
          where il.meta_json->>'nhsp_shift_id' = s.shift_id::text
        )
        else null
      end as invline_old_charge_ex
    from seg_old s
  ),
  new_hours as (
    select
      a.*,
      h.hours_day, h.hours_night, h.hours_sat, h.hours_sun, h.hours_bh, h.total_hours,
      greatest(
        0,
        (extract(epoch from (a.new_end_utc - a.new_start_utc))/60)::int - coalesce(a.new_break_mins,0)
      ) as new_paid_minutes
    from invline_old a
    left join lateral public._wkimp_bucket_hours_from_policy(
      coalesce(a.policy_snapshot_json, '{}'::jsonb),
      a.new_start_utc,
      a.new_end_utc,
      a.new_break_mins
    ) h on true
  ),
  amounts as (
    select
      n.*,

      coalesce(n.seg_old_pay_ex, null) as old_pay_ex,
      coalesce(n.seg_old_charge_ex, n.invline_old_charge_ex, null) as old_charge_ex,

      case
        when n.policy_snapshot_json is null then null
        when coalesce((n.correction_chain_json->>'valid')::boolean, false) is false then null
        else round(
          coalesce(n.hours_day,0)   * coalesce(n.pay_day,0) +
          coalesce(n.hours_night,0) * coalesce(n.pay_night,0) +
          coalesce(n.hours_sat,0)   * coalesce(n.pay_sat,0) +
          coalesce(n.hours_sun,0)   * coalesce(n.pay_sun,0) +
          coalesce(n.hours_bh,0)    * coalesce(n.pay_bh,0)
        , 2)
      end as new_pay_ex,

      case
        when n.policy_snapshot_json is null then null
        when coalesce((n.correction_chain_json->>'valid')::boolean, false) is false then null
        else round(
          coalesce(n.hours_day,0)   * coalesce(n.charge_day,0) +
          coalesce(n.hours_night,0) * coalesce(n.charge_night,0) +
          coalesce(n.hours_sat,0)   * coalesce(n.charge_sat,0) +
          coalesce(n.hours_sun,0)   * coalesce(n.charge_sun,0) +
          coalesce(n.hours_bh,0)    * coalesce(n.charge_bh,0)
        , 2)
      end as new_charge_ex
    from new_hours n
  ),
  final_rows as (
    select
      a.hr_row_id,
      a.external_row_key,

      a.shift_id,
      a.source_system,

      a.candidate_id,
      a.client_id,
      a.contract_id,
      a.timesheet_id,

      a.contract_self_bill,

      -- POLICY: shift date is start-date (computed from new_start_utc).
      a.work_date as work_date,
      a.week_ending_date as week_ending_date,

      a.old_start_utc,
      a.old_end_utc,
      a.old_break_mins,

      a.new_start_utc,
      a.new_end_utc,
      a.new_break_mins,

      a.old_paid_minutes,
      a.new_paid_minutes,

      (
        a.shift_id is not null
        and (
          a.old_start_utc is distinct from a.new_start_utc
          or a.old_end_utc is distinct from a.new_end_utc
          or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
        )
      ) as is_changed_hours,

      (a.paid_at_utc is not null) as is_paid,

      (
        a.seg_invoice_id is not null
        or a.locked_by_invoice_id is not null
        or a.shift_invoice_id is not null
        or a.invoice_line_invoice_id is not null
      ) as is_invoiced,

      coalesce(
        a.seg_invoice_id,
        a.locked_by_invoice_id,
        a.shift_invoice_id,
        a.invoice_line_invoice_id
      ) as invoice_id_detected,

      a.old_pay_ex,
      a.old_charge_ex,

      a.new_pay_ex,
      a.new_charge_ex,

      case when a.new_pay_ex is null or a.old_pay_ex is null then null else round(a.new_pay_ex - a.old_pay_ex, 2) end as delta_pay_ex,
      case when a.new_charge_ex is null or a.old_charge_ex is null then null else round(a.new_charge_ex - a.old_charge_ex, 2) end as delta_charge_ex,

      (
        (a.paid_at_utc is not null)
        and (
          a.shift_id is not null
          and (
            a.old_start_utc is distinct from a.new_start_utc
            or a.old_end_utc is distinct from a.new_end_utc
            or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
          )
        )
      ) as requires_pay_decision,

      (
        (
          a.seg_invoice_id is not null
          or a.locked_by_invoice_id is not null
          or a.shift_invoice_id is not null
          or a.invoice_line_invoice_id is not null
        )
        and (
          a.shift_id is not null
          and (
            a.old_start_utc is distinct from a.new_start_utc
            or a.old_end_utc is distinct from a.new_end_utc
            or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
          )
        )
      ) as requires_invoice_decision,

      (
        (
          (
            (a.paid_at_utc is not null)
            or
            (
              a.seg_invoice_id is not null
              or a.locked_by_invoice_id is not null
              or a.shift_invoice_id is not null
              or a.invoice_line_invoice_id is not null
            )
          )
          and (
            a.shift_id is not null
            and (
              a.old_start_utc is distinct from a.new_start_utc
              or a.old_end_utc is distinct from a.new_end_utc
              or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
            )
          )
        )
        or (
          a.shift_id is not null
          and (
            a.old_start_utc is distinct from a.new_start_utc
            or a.old_end_utc is distinct from a.new_end_utc
            or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
          )
          and (
            coalesce((a.correction_chain_json->>'valid')::boolean,false) is false
          )
        )
      ) as requires_any_decision
    from amounts a
  )
  select fr.*
  from final_rows fr
  where fr.is_changed_hours = true
    and fr.timesheet_id is not null
  order by fr.work_date asc, fr.external_row_key asc;

end;
$function$;
-- CloudTMS deployment metadata preserved from the installed TEST definition.
ALTER FUNCTION public.weekly_import_changed_hours_phase3(uuid, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.weekly_import_changed_hours_phase3(uuid, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.weekly_import_changed_hours_phase3(uuid, text) TO authenticated, service_role;
