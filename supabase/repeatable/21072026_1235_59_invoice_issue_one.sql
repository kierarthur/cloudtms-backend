-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 44c461dba610.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
create or replace function public.invoice_issue_one(
  p_invoice_id uuid,
  p_actor_user_id uuid
)
returns table (
  status text,
  issued_at_utc timestamptz,
  on_hold_reason text,
  reasons text[]
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_anchor_ymd date := (now() at time zone 'Europe/London')::date;

  v_terms_days int := null;
  v_due_at timestamptz := null;
  v_hdr jsonb := null;
  v_client_id uuid := null;

  v_group_nightsat_sunbh boolean := null;

  v_ts_ids uuid[];
  v_worked_ts_ids uuid[];

  v_reasons text[] := array[]::text[];
  v_precheck_reasons text[] := array[]::text[];
  v_hr_reasons text[] := array[]::text[];
  v_issue_ref_reasons text[] := array[]::text[];

  v_on_hold_reason text := null;

  -- ref-to-issue flag (effective: contract override aware via v_ts_invoice_precheck)
  v_ref_required_to_issue boolean := false;

  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_started timestamptz := now();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_ts jsonb := '[]'::jsonb;

  -- ======================================================
  -- PDF invalidation helpers (optional columns; dynamic update)
  -- ======================================================
  v_has_updated_at boolean := false;
  v_has_pdf_fresh boolean := false;

begin
  -- Load invoice_debug flag (safe even if column not yet present)
  begin
    select coalesce(sd.invoice_debug, false)
    into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step','start',
    'now_utc', public._inv_iso_utc(v_now),
    'anchor_ymd', v_anchor_ymd::text,
    'invoice_id', case when p_invoice_id is null then null else p_invoice_id::text end
  ));

  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  perform public._ctms_assert_invoice_correction_lines_v1(
    p_invoice_id,p_actor_user_id,true,'INVOICE_ISSUE'
  );

  -- Detect optional invoice columns used to invalidate invoice PDF freshness on ISSUE
  begin
    select exists(
      select 1
      from information_schema.columns c
      where c.table_schema = 'public'
        and c.table_name = 'invoices'
        and c.column_name = 'updated_at'
    )
    into v_has_updated_at;

    select exists(
      select 1
      from information_schema.columns c
      where c.table_schema = 'public'
        and c.table_name = 'invoices'
        and c.column_name = 'pdf_fresh'
    )
    into v_has_pdf_fresh;
  exception when others then
    v_has_updated_at := false;
    v_has_pdf_fresh := false;
  end;

  -- Load invoice + basic guards
  declare
    v_inv record;
  begin
    select *
    into v_inv
    from public.invoices
    where id = p_invoice_id;

    if not found then
      raise exception 'Invoice not found';
    end if;

    if v_inv.type::text = 'CREDIT_NOTE' then
      raise exception 'Cannot issue a CREDIT_NOTE';
    end if;

    if v_inv.status::text = 'PAID' then
      raise exception 'Cannot issue a PAID invoice';
    end if;

    if v_inv.status::text = 'ISSUED' then
      status := 'ISSUED';
      issued_at_utc := v_inv.issued_at_utc;
      on_hold_reason := v_inv.on_hold_reason;
      reasons := array[]::text[];

      if v_invoice_debug then
        begin
          perform public._inv_write_audit(
            p_actor_user_id,
            'INVOICE_ISSUE_DEBUG',
            jsonb_build_object(
              'result','ALREADY_ISSUED',
              'invoice_id', p_invoice_id::text,
              'status', v_inv.status::text,
              'issued_at_utc', to_jsonb(v_inv.issued_at_utc),
              'steps', v_dbg_steps
            ),
            'invoices',
            p_invoice_id::text,
            null,
            'INVOICE_DEBUG',
            null, null, null
          );
        exception when others then
          null;
        end;
      end if;

      return next;
      return;
    end if;

    -- ✅ HARD BLOCK:
    -- If invoice is currently ON_HOLD, do not re-evaluate blockers; require UNHOLD first.
    if v_inv.status::text = 'ON_HOLD' then
      status := 'ON_HOLD';
      issued_at_utc := null;
      on_hold_reason := 'Unhold first';
      reasons := array['Unhold first']::text[];
      return next;
      return;
    end if;

    if v_inv.status::text not in ('DRAFT','ON_HOLD') then
      raise exception 'Only DRAFT/ON_HOLD invoices can be issued (current status=%)', v_inv.status::text;
    end if;
  end;

  -- Invoice client_id (used for settings lookup and header snapshot)
  select i.client_id
  into v_client_id
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  -- Timesheets on invoice
  select array_agg(distinct l.timesheet_id)
  into v_ts_ids
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and l.timesheet_id is not null;

  if v_ts_ids is null or coalesce(array_length(v_ts_ids, 1), 0) = 0 then
    raise exception 'Invoice has no timesheets to validate';
  end if;

  -- Timesheets that have WORKED content on this invoice (hours/additional only)
  select array_agg(distinct l.timesheet_id)
  into v_worked_ts_ids
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and l.timesheet_id is not null
    and upper(coalesce(l.meta_json->>'line_type','')) in (
      'HOURS_DAILY','HOURS_WEEKLY','ADDITIONAL_RATE','ADDITIONAL_RATE_DAILY'
    );

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step','collected_timesheets',
    'timesheet_count', coalesce(array_length(v_ts_ids,1),0),
    'worked_timesheet_count', coalesce(array_length(v_worked_ts_ids,1),0)
  ));

  -- ✅ Determine whether ref-to-issue is enabled for THIS invoice (contract override aware)
  -- We treat this as: any worked timesheet on the invoice has reference_number_required_to_issue_invoice = true
  begin
    select coalesce(bool_or(coalesce(pc.reference_number_required_to_issue_invoice,false)), false)
    into v_ref_required_to_issue
    from unnest(coalesce(v_worked_ts_ids, array[]::uuid[])) as x(timesheet_id)
    left join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = x.timesheet_id;
  exception when others then
    v_ref_required_to_issue := false;
  end;

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step','computed_issue_ref_policy',
    'reference_number_required_to_issue_invoice', v_ref_required_to_issue
  ));

  -- ------------------------------------------------------------
  -- 1) Precheck blockers (authoritative: PDF/reference/evidence)
  -- ------------------------------------------------------------
  select array_agg(
    case
      when pc.timesheet_id is null
        then 'TS ' || x.timesheet_id::text || ': precheck missing'

      when upper(coalesce(pc.precheck_status,'')) = 'OK'
        then null

      when upper(coalesce(pc.precheck_status,'')) = 'BLOCK_NO_REFERENCE'
        then 'TS ' || pc.timesheet_id::text || ': missing reference/PO'

      when upper(coalesce(pc.precheck_status,'')) = 'BLOCK_NO_PDF'
        then 'TS ' || pc.timesheet_id::text || ': missing timesheet PDF'

      when upper(coalesce(pc.precheck_status,'')) = 'BLOCK_NO_MILEAGE_EVIDENCE'
        then 'TS ' || pc.timesheet_id::text || ': missing mileage evidence'

      when upper(coalesce(pc.precheck_status,'')) = 'BLOCK_NO_EXPENSES_EVIDENCE'
        then 'TS ' || pc.timesheet_id::text || ': missing expenses evidence'

      else
        'TS ' || pc.timesheet_id::text || ': precheck blocker ' || upper(coalesce(pc.precheck_status,''))
    end
  )
  into v_precheck_reasons
  from unnest(v_ts_ids) as x(timesheet_id)
  left join public.v_ts_invoice_precheck pc
    on pc.timesheet_id = x.timesheet_id;

  v_precheck_reasons := array_remove(v_precheck_reasons, null);

  -- ------------------------------------------------------------
  -- 2) OPTIONAL HARDENING: HR validation blockers
  -- ------------------------------------------------------------
  select array_agg(
    case
      when s.timesheet_id is null
        then 'TS ' || x.timesheet_id::text || ': summary missing'

      when coalesce(s.hr_validation_required_for_invoice, false) = true
        and (
          s.validation_status is null
          or s.validation_status <> all (array[
            'VALIDATION_OK'::public.validation_status_enum,
            'OVERRIDDEN'::public.validation_status_enum
          ])
        )
        then 'TS ' || x.timesheet_id::text || ': HR validation not passed'

      else null
    end
  )
  into v_hr_reasons
  from unnest(v_ts_ids) as x(timesheet_id)
  left join public.v_timesheets_summary_base s
    on s.timesheet_id = x.timesheet_id;

  v_hr_reasons := array_remove(v_hr_reasons, null);

  -- ------------------------------------------------------------
  -- 3) ISSUE-TIME reference gating (contract override aware, per-timesheet)
  -- ------------------------------------------------------------

  -- 3) ISSUE-TIME reference gating (scoped to this invoice)
  -- Only segments locked to THIS invoice can block issuing (so other-invoice segments never block).
  select array_agg(
    case
      when t.timesheet_id is null then null
      when t.issue_missing_count > 0
        then 'TS '||t.timesheet_id::text||': missing reference/PO for '||t.issue_missing_count::text||' shift(s) (required to issue)'
      else null
    end
  )
  into v_issue_ref_reasons
  from (
    select
      x.timesheet_id,
      case
        when coalesce(pc.reference_number_required_to_issue_invoice,false) is not true then 0

        when tf.invoice_breakdown_json is not null
          and jsonb_typeof(tf.invoice_breakdown_json) = 'object'
          and upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
          and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
        then (
          select count(*)::int
          from jsonb_array_elements(tf.invoice_breakdown_json->'segments') as s(seg)
          where nullif(btrim(coalesce(s.seg->>'invoice_locked_invoice_id','')), '') = p_invoice_id::text
            and (
              (
                (case when coalesce(s.seg->>'hours_day','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_day')::numeric else 0 end)
              + (case when coalesce(s.seg->>'hours_night','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_night')::numeric else 0 end)
              + (case when coalesce(s.seg->>'hours_sat','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_sat')::numeric else 0 end)
              + (case when coalesce(s.seg->>'hours_sun','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_sun')::numeric else 0 end)
              + (case when coalesce(s.seg->>'hours_bh','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_bh')::numeric else 0 end)
              ) > 0
              or (case when coalesce(s.seg->>'charge_amount','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'charge_amount')::numeric else 0 end) > 0
            )
            and nullif(btrim(coalesce(s.seg->>'ref_num','')), '') is null
        )

        else (
          case
            when public._inv_timesheet_has_invoice_reference(
              ts.sheet_scope::text,
              coalesce(ts.submission_mode::text,''),
              ts.reference_number,
              ts.day_references_json,
              ts.actual_schedule_json
            )
            then 0 else 1 end
        )
      end as issue_missing_count
    from unnest(coalesce(v_worked_ts_ids, array[]::uuid[])) as x(timesheet_id)
    left join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = x.timesheet_id
    left join public.timesheets ts
      on ts.timesheet_id = x.timesheet_id
     and ts.is_current = true
    left join public.timesheets_financials tf
      on tf.timesheet_id = x.timesheet_id
     and tf.is_current = true
  ) t;

  v_issue_ref_reasons := array_remove(v_issue_ref_reasons, null);

  -- Merge blockers
  v_reasons := array_cat(v_precheck_reasons, v_hr_reasons);
  v_reasons := array_cat(v_reasons, v_issue_ref_reasons);
  v_reasons := array_remove(v_reasons, null);

  -- Debug per-timesheet snapshot (only if enabled)
  if v_invoice_debug then
    begin
      select coalesce(jsonb_agg(
        jsonb_build_object(
          'timesheet_id', x.timesheet_id::text,
          'has_worked_lines', (v_worked_ts_ids is not null and x.timesheet_id = any(v_worked_ts_ids)),
          'precheck_status', coalesce(pc.precheck_status,'')::text,
          'ref_required_to_issue_effective', coalesce(pc.reference_number_required_to_issue_invoice,false),
          'issue_missing_reference_on_invoice', ((
            case
              when x.has_worked_lines is not true then 0
              when coalesce(pc.reference_number_required_to_issue_invoice,false) is not true then 0

              when s.invoice_breakdown_json is not null
                and jsonb_typeof(s.invoice_breakdown_json) = 'object'
                and upper(coalesce(s.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
                and jsonb_typeof(s.invoice_breakdown_json->'segments') = 'array'
              then coalesce((
                select count(*)::int
                from jsonb_array_elements(s.invoice_breakdown_json->'segments') as sg(seg)
                where nullif(btrim(coalesce(sg.seg->>'invoice_locked_invoice_id','')), '') = p_invoice_id::text
                  and (
                    (
                      (case when coalesce(sg.seg->>'hours_day','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_day')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_night','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_night')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_sat','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_sat')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_sun','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_sun')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_bh','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_bh')::numeric else 0 end)
                    ) > 0
                    or (case when coalesce(sg.seg->>'charge_amount','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'charge_amount')::numeric else 0 end) > 0
                  )
                  and nullif(btrim(coalesce(sg.seg->>'ref_num','')), '') is null
              ),0)

              else (
                case
                  when public._inv_timesheet_has_invoice_reference(
                    t0.sheet_scope::text,
                    coalesce(t0.submission_mode::text,''),
                    t0.reference_number,
                    t0.day_references_json,
                    t0.actual_schedule_json
                  ) then 0 else 1 end
              )
            end
          ) > 0),
          'issue_missing_reference_on_invoice_count', (
            case
              when x.has_worked_lines is not true then 0
              when coalesce(pc.reference_number_required_to_issue_invoice,false) is not true then 0

              when s.invoice_breakdown_json is not null
                and jsonb_typeof(s.invoice_breakdown_json) = 'object'
                and upper(coalesce(s.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
                and jsonb_typeof(s.invoice_breakdown_json->'segments') = 'array'
              then coalesce((
                select count(*)::int
                from jsonb_array_elements(s.invoice_breakdown_json->'segments') as sg(seg)
                where nullif(btrim(coalesce(sg.seg->>'invoice_locked_invoice_id','')), '') = p_invoice_id::text
                  and (
                    (
                      (case when coalesce(sg.seg->>'hours_day','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_day')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_night','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_night')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_sat','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_sat')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_sun','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_sun')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_bh','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_bh')::numeric else 0 end)
                    ) > 0
                    or (case when coalesce(sg.seg->>'charge_amount','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'charge_amount')::numeric else 0 end) > 0
                  )
                  and nullif(btrim(coalesce(sg.seg->>'ref_num','')), '') is null
              ),0)

              else (
                case
                  when public._inv_timesheet_has_invoice_reference(
                    t0.sheet_scope::text,
                    coalesce(t0.submission_mode::text,''),
                    t0.reference_number,
                    t0.day_references_json,
                    t0.actual_schedule_json
                  ) then 0 else 1 end
              )
            end
          ),
          'hr_required', coalesce(s.hr_validation_required_for_invoice,false),
          'validation_status', case when s.validation_status is null then null else s.validation_status::text end
        )
      ), '[]'::jsonb)
      into v_dbg_ts
      from unnest(v_ts_ids) as x(timesheet_id)
      left join public.v_ts_invoice_precheck pc on pc.timesheet_id = x.timesheet_id
      left join public.v_timesheets_summary_base s on s.timesheet_id = x.timesheet_id;
    exception when others then
      v_dbg_ts := '[]'::jsonb;
    end;
  end if;

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step','computed_blockers',
    'precheck_reasons_count', coalesce(array_length(v_precheck_reasons,1),0),
    'hr_reasons_count', coalesce(array_length(v_hr_reasons,1),0),
    'issue_ref_reasons_count', coalesce(array_length(v_issue_ref_reasons,1),0),
    'total_reasons_count', coalesce(array_length(v_reasons,1),0)
  ));

  -- Any blockers => ON_HOLD
  if coalesce(array_length(v_reasons, 1), 0) > 0 then
    v_on_hold_reason := array_to_string(v_reasons, '; ');

    update public.invoices
    set status = 'ON_HOLD'::public.invoice_status_enum,
        status_date_utc = v_now,
        issued_at_utc = null,
        on_hold_reason = v_on_hold_reason
    where id = p_invoice_id;

    perform public._audit_insert(
      'invoice',
      p_invoice_id::text,
      'INVOICE_ON_HOLD',
      null,
      jsonb_build_object('reasons', v_reasons),
      null,
      p_actor_user_id
    );

    status := 'ON_HOLD';
    issued_at_utc := null;
    on_hold_reason := v_on_hold_reason;
    reasons := v_reasons;

    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_ISSUE_DEBUG',
          jsonb_build_object(
            'result','ON_HOLD',
            'invoice_id', p_invoice_id::text,
            'client_id', case when v_client_id is null then null else v_client_id::text end,
            'reference_number_required_to_issue_invoice', v_ref_required_to_issue,
            'timesheet_ids', to_jsonb(coalesce(v_ts_ids, array[]::uuid[])),
            'worked_timesheet_ids', to_jsonb(coalesce(v_worked_ts_ids, array[]::uuid[])),
            'reasons', to_jsonb(coalesce(v_reasons, array[]::text[])),
            'timesheets_debug', v_dbg_ts,
            'steps', v_dbg_steps,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoices',
          p_invoice_id::text,
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;

    return next;
    return;
  end if;

  -- ------------------------------------------------------------
  -- No blockers => issue
  -- ------------------------------------------------------------
  select i.client_id, i.header_snapshot_json
  into v_client_id, v_hdr
  from public.invoices i
  where i.id = p_invoice_id;

  if v_hdr is null or jsonb_typeof(v_hdr) <> 'object' then
    v_hdr := '{}'::jsonb;
  end if;

  if not (v_hdr ? 'group_nightsat_sunbh') then
    select cs0.group_nightsat_sunbh
    into v_group_nightsat_sunbh
    from public.client_settings cs0
    where cs0.client_id = v_client_id
      and (cs0.effective_from <= v_anchor_ymd or cs0.effective_from is null)
    order by cs0.effective_from desc nulls last
    limit 1;

    v_hdr := v_hdr || jsonb_build_object('group_nightsat_sunbh', coalesce(v_group_nightsat_sunbh, false));
  end if;

  begin
    if (v_hdr ? 'payment_terms_days') then
      v_terms_days := (v_hdr->>'payment_terms_days')::int;
    end if;
  exception when others then
    v_terms_days := null;
  end;

  if v_terms_days is null then
    begin
      select c.payment_terms_days
      into v_terms_days
      from public.clients c
      where c.id = v_client_id;
    exception when others then
      v_terms_days := null;
    end;
  end if;

  v_terms_days := coalesce(v_terms_days, 30);
  v_due_at := v_now + make_interval(days => v_terms_days);

  update public.invoices
  set status = 'ISSUED'::public.invoice_status_enum,
      status_date_utc = v_now,
      issued_at_utc = v_now,
      due_at_utc = v_due_at,
      on_hold_reason = null,
      header_snapshot_json = v_hdr
  where id = p_invoice_id;

  -- ✅ FIX: invalidate invoice PDF freshness on ISSUE so invpdf regeneration will occur
  -- (uses optional columns if present; never fails function creation)
  begin
    if v_has_updated_at then
      execute 'update public.invoices set updated_at = $1 where id = $2'
      using v_now, p_invoice_id;
    end if;

    if v_has_pdf_fresh then
      execute 'update public.invoices set pdf_fresh = false where id = $1'
      using p_invoice_id;
    end if;
  exception when others then
    null;
  end;

  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_ISSUED',
    null,
    '{}'::jsonb,
    null,
    p_actor_user_id
  );

  status := 'ISSUED';
  issued_at_utc := v_now;
  on_hold_reason := null;
  reasons := array[]::text[];

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_ISSUE_DEBUG',
        jsonb_build_object(
          'result','ISSUED',
          'invoice_id', p_invoice_id::text,
          'client_id', case when v_client_id is null then null else v_client_id::text end,
          'reference_number_required_to_issue_invoice', v_ref_required_to_issue,
          'timesheet_ids', to_jsonb(coalesce(v_ts_ids, array[]::uuid[])),
          'worked_timesheet_ids', to_jsonb(coalesce(v_worked_ts_ids, array[]::uuid[])),
          'timesheets_debug', v_dbg_ts,
          'steps', v_dbg_steps,
          'run_started_at_utc', public._inv_iso_utc(v_dbg_started),
          'run_finished_at_utc', public._inv_iso_utc(now())
        ),
        'invoices',
        p_invoice_id::text,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  return next;

exception when others then
  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_ISSUE_DEBUG',
        jsonb_build_object(
          'result','ERROR',
          'invoice_id', case when p_invoice_id is null then null else p_invoice_id::text end,
          'client_id', case when v_client_id is null then null else v_client_id::text end,
          'reference_number_required_to_issue_invoice', v_ref_required_to_issue,
          'sqlstate', sqlstate,
          'error', sqlerrm,
          'timesheet_ids', to_jsonb(coalesce(v_ts_ids, array[]::uuid[])),
          'worked_timesheet_ids', to_jsonb(coalesce(v_worked_ts_ids, array[]::uuid[])),
          'timesheets_debug', v_dbg_ts,
          'steps', v_dbg_steps,
          'run_started_at_utc', public._inv_iso_utc(v_dbg_started),
          'run_finished_at_utc', public._inv_iso_utc(now())
        ),
        'invoices',
        case when p_invoice_id is null then null else p_invoice_id::text end,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;
  raise;
end;
$$;
