-- ============================================================
-- CloudTMS: Invoice edit RPCs + manifest extensions
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION statements only
-- Includes:
--  - public._inv_unlock_segments_for_invoice
--  - public.invoice_apply_edits (Option 1 payload contract)
--  - patched public.invoice_render_manifest (email summary + adjustment classification)
-- ============================================================

create or replace function public._inv_unlock_segments_for_invoice(
  p_invoice_id uuid,
  p_timesheet_ids uuid[]
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  tsid uuid;
  snap record;
  ib jsonb;
  seg jsonb;
  segs jsonb;
  out_segs jsonb;
  locked_text text;
  any_unlocked boolean;
  all_locked boolean;
begin
  if p_invoice_id is null then
    return;
  end if;

  if p_timesheet_ids is null or coalesce(array_length(p_timesheet_ids,1),0) = 0 then
    return;
  end if;

  foreach tsid in array p_timesheet_ids loop
    exit when tsid is null;
  end loop;

  foreach tsid in array p_timesheet_ids loop
    if tsid is null then
      continue;
    end if;

    select *
    into snap
    from public.timesheets_financials tf
    where tf.is_current = true
      and tf.timesheet_id = tsid
    order by tf.created_at desc
    limit 1;

    if not found then
      continue;
    end if;

    ib := snap.invoice_breakdown_json;

    -- SEGMENTS mode: unlock only segments locked to THIS invoice
    if ib is not null
      and jsonb_typeof(ib) = 'object'
      and coalesce(ib->>'mode','') = 'SEGMENTS'
      and jsonb_typeof(ib->'segments') = 'array'
    then
      out_segs := '[]'::jsonb;
      any_unlocked := false;
      all_locked := true;

      for seg in
        select value from jsonb_array_elements(ib->'segments') value
      loop
        if seg is null or jsonb_typeof(seg) <> 'object' then
          out_segs := out_segs || jsonb_build_array(seg);
          all_locked := false;
          continue;
        end if;

        locked_text := nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '');

        if locked_text = p_invoice_id::text then
          -- unlock this segment
          seg := jsonb_set(seg, '{invoice_locked_invoice_id}', 'null'::jsonb, true);
          locked_text := null;
          any_unlocked := true;
        end if;

        if locked_text is null then
          all_locked := false;
        end if;

        out_segs := out_segs || jsonb_build_array(seg);
      end loop;

      ib := jsonb_set(ib, '{segments}', out_segs, true);

      update public.timesheets_financials
      set
        updated_at = v_now,
        invoice_breakdown_json = ib,
        locked_by_invoice_id = case
          when any_unlocked then null
          else locked_by_invoice_id
        end,
        locked_at_utc = case
          when any_unlocked then null
          else locked_at_utc
        end
      where id = snap.id;

    else
      -- Non-segments: unlock whole snapshot if it was locked to this invoice
      if snap.locked_by_invoice_id = p_invoice_id then
        update public.timesheets_financials
        set
          updated_at = v_now,
          locked_by_invoice_id = null,
          locked_at_utc = null
        where id = snap.id;
      end if;
    end if;

  end loop;
end;
$$;
create or replace function public.invoice_apply_edits(
  p_invoice_id uuid,
  p_payload jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_anchor_ymd date := (now() at time zone 'Europe/London')::date;

  v_inv record;
  v_week_start date;
  v_week_end date;

  v_remove_ids uuid[];
  v_add_ts_ids uuid[];
  v_ts_ids_touched uuid[] := array[]::uuid[];

  v_vat_chargeable boolean := true;
  v_vat_rate numeric := 0;

  -- adjustments
  adj jsonb;
  v_adj_token text;
  v_adj_desc text;
  v_adj_ex numeric;
  v_adj_vat numeric;
  v_adj_inc numeric;
  v_adj_source_key text;
  v_meta jsonb;

  -- timesheet loop
  tsid uuid;
  snap record;
  ts record;
  pc record;
  contract_id uuid;
  c_daily_calc boolean := false;
  c_bucket_labels jsonb := null;
  c_role text := null;
  c_display_site text := null;
  c_ward_hint text := null;

  -- segment filtering
  seg jsonb;
  segments jsonb := '[]'::jsonb;
  seg_target date;
  seg_date text;
  natural_start date;
  seg_locked text;
  seg_ref text;

  -- aggregation for weekly line
  h_day numeric; h_night numeric; h_sat numeric; h_sun numeric; h_bh numeric;
  pay_ex numeric; chg_ex numeric; margin_ex numeric;
  vat_amt numeric; inc_amt numeric;
  line_desc text;
  source_key text;

  -- segment refs for locking
  seg_refs jsonb := '[]'::jsonb;

  -- additional units
  kv record;
  ex jsonb;
  code text;
  unit_count numeric;
  bucket_name text;
  unit_name text;

  -- expenses notes
  v_note_travel text;
  v_note_accom text;
  v_note_other text;

  -- recompute totals
  v_new_ex numeric := 0;
  v_new_vat numeric := 0;
  v_new_inc numeric := 0;

  v_manifest jsonb;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  select *
  into v_inv
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  if not found then
    raise exception 'Invoice not found';
  end if;

  -- Editable gate: DRAFT/ON_HOLD and unpaid
  if v_inv.status::text not in ('DRAFT','ON_HOLD') then
    raise exception 'Invoice is not editable (status=%)', v_inv.status::text;
  end if;

  if v_inv.paid_at_utc is not null then
    raise exception 'Invoice is not editable (already paid)';
  end if;

  -- Require invoice_week_start in header_snapshot_json.meta
  if v_inv.header_snapshot_json is null
     or btrim(coalesce(v_inv.header_snapshot_json #>> '{meta,invoice_week_start}','')) = ''
     or (v_inv.header_snapshot_json #>> '{meta,invoice_week_start}') !~ '^\d{4}-\d{2}-\d{2}$'
  then
    raise exception 'Invoice header_snapshot_json.meta.invoice_week_start is required for edits';
  end if;

  v_week_start := (v_inv.header_snapshot_json #>> '{meta,invoice_week_start}')::date;
  v_week_end := (v_week_start + interval '6 days')::date;

  -- VAT settings from invoice snapshot
  if jsonb_typeof(v_inv.header_snapshot_json->'vat_chargeable') = 'boolean' then
    v_vat_chargeable := (v_inv.header_snapshot_json->>'vat_chargeable')::boolean;
  else
    v_vat_chargeable := true;
  end if;

  if (v_inv.header_snapshot_json ? 'applied_vat_rate_pct') then
    begin
      v_vat_rate := (v_inv.header_snapshot_json->>'applied_vat_rate_pct')::numeric;
    exception when others then
      v_vat_rate := 0;
    end;
  else
    v_vat_rate := 0;
  end if;

  if v_vat_chargeable = false then
    v_vat_rate := 0;
  end if;

  -- Parse payload arrays
  v_remove_ids := null;
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'remove_invoice_line_ids') then
    select array_agg((x)::uuid)
    into v_remove_ids
    from jsonb_array_elements_text(coalesce(p_payload->'remove_invoice_line_ids','[]'::jsonb)) x
    where nullif(btrim(coalesce(x,'')),'') is not null;
  end if;

  v_add_ts_ids := null;
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_timesheet_ids') then
    select array_agg((x)::uuid)
    into v_add_ts_ids
    from jsonb_array_elements_text(coalesce(p_payload->'add_timesheet_ids','[]'::jsonb)) x
    where nullif(btrim(coalesce(x,'')),'') is not null;
  end if;

  -- 1) Removals (by invoice_line_id)
  if v_remove_ids is not null and coalesce(array_length(v_remove_ids,1),0) > 0 then
    -- collect timesheet_ids touched
    select array_agg(distinct l.timesheet_id) filter (where l.timesheet_id is not null)
    into v_ts_ids_touched
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
      and l.id = any(v_remove_ids);

    delete from public.invoice_lines
    where invoice_id = p_invoice_id
      and id = any(v_remove_ids);

    if v_ts_ids_touched is not null and coalesce(array_length(v_ts_ids_touched,1),0) > 0 then
      perform public._inv_unlock_segments_for_invoice(p_invoice_id, v_ts_ids_touched);
    end if;
  end if;

  -- 2) Add adjustments
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_adjustments') then
    if jsonb_typeof(p_payload->'add_adjustments') = 'array' then
      for adj in
        select value from jsonb_array_elements(p_payload->'add_adjustments') value
      loop
        if adj is null or jsonb_typeof(adj) <> 'object' then
          continue;
        end if;

        v_adj_token := nullif(btrim(coalesce(adj->>'client_token','')), '');
        v_adj_desc  := nullif(btrim(coalesce(adj->>'description','')), '');
        begin
          v_adj_ex := (adj->>'amount_ex_vat')::numeric;
        exception when others then
          v_adj_ex := null;
        end;

        if v_adj_token is null or v_adj_desc is null or v_adj_ex is null then
          continue;
        end if;

        v_adj_vat := public._inv_round2(v_adj_ex * v_vat_rate / 100);
        if v_vat_rate = 0 then v_adj_vat := 0; end if;
        v_adj_inc := public._inv_round2(v_adj_ex + v_adj_vat);

        v_adj_source_key := 'ADJ:' || v_adj_token;

        v_meta := jsonb_build_object(
          'line_type','ADJUSTMENT',
          'client_token', v_adj_token,
          'description', v_adj_desc,
          'amount_ex_vat', public._inv_round2(v_adj_ex),
          'vat_rate_pct', v_vat_rate,
          'vat_chargeable', v_vat_chargeable
        );

        insert into public.invoice_lines(
          invoice_id, timesheet_id, booking_id, description,
          hours_day, hours_night, hours_sat, hours_sun, hours_bh,
          pay_day, pay_night, pay_sat, pay_sun, pay_bh,
          charge_day, charge_night, charge_sat, charge_sun, charge_bh,
          total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
          vat_rate_pct, vat_amount, total_inc_vat,
          paper_ts_r2_key, meta_json, source_key
        )
        values (
          p_invoice_id, null, null, v_adj_desc,
          0,0,0,0,0,
          null,null,null,null,null,
          null,null,null,null,null,
          0, public._inv_round2(v_adj_ex), public._inv_round2(v_adj_ex),
          v_vat_rate, v_adj_vat, v_adj_inc,
          null,
          v_meta,
          v_adj_source_key
        )
        on conflict (invoice_id, source_key) do nothing;
      end loop;
    end if;
  end if;

  -- 3) Add timesheets (full parity: hours + additional + expenses + mileage), for THIS invoice week_start
  if v_add_ts_ids is not null and coalesce(array_length(v_add_ts_ids,1),0) > 0 then
    foreach tsid in array v_add_ts_ids loop
      if tsid is null then
        continue;
      end if;

      -- Load snapshot + timesheet + precheck
      select
        tf.*,
        tsr.booking_id,
        tsr.week_ending_date,
        tsr.reference_number,
        tsr.sheet_scope::text as sheet_scope,
        coalesce(tsr.submission_mode::text,'') as submission_mode,
        tsr.day_references_json,
        tsr.actual_schedule_json,
        cw.contract_id
      into snap
      from public.timesheets_financials tf
      join public.timesheets tsr on tsr.timesheet_id = tf.timesheet_id and tsr.is_current = true
      left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
      join public.v_ts_invoice_precheck pcv on pcv.timesheet_id = tf.timesheet_id
      where tf.timesheet_id = tsid
        and tf.is_current = true
        and tf.locked_by_invoice_id is null
        and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        and upper(coalesce(pcv.precheck_status,'')) = 'OK'
        and tf.client_id = v_inv.client_id
        and (
          pcv.require_reference_to_invoice is not true
          or public._inv_timesheet_has_invoice_reference(
                tsr.sheet_scope::text,
                coalesce(tsr.submission_mode::text,''),
                tsr.reference_number,
                tsr.day_references_json,
                tsr.actual_schedule_json
             )
        )
      limit 1;

      if not found then
        continue;
      end if;

      contract_id := snap.contract_id;
      c_daily_calc := false;
      c_bucket_labels := null;
      c_role := null;
      c_display_site := null;
      c_ward_hint := null;

      if contract_id is not null then
        select
          coalesce(daily_calc_of_invoices,false),
          bucket_labels_json,
          nullif(btrim(coalesce(role,'')), ''),
          nullif(btrim(coalesce(display_site,'')), ''),
          nullif(btrim(coalesce(ward_hint,'')), '')
        into
          c_daily_calc, c_bucket_labels, c_role, c_display_site, c_ward_hint
        from public.contracts
        where id = contract_id
        limit 1;
      end if;

      if c_bucket_labels is null then
        c_bucket_labels := jsonb_build_object('day','Day','night','Night','sat','Sat','sun','Sun','bh','BH');
      end if;

      natural_start := (snap.week_ending_date::date - 6);

      segments := '[]'::jsonb;

      -- Build invoiceable segment set for THIS invoice week_start
      if snap.invoice_breakdown_json is not null
         and jsonb_typeof(snap.invoice_breakdown_json)='object'
         and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
         and jsonb_typeof(snap.invoice_breakdown_json->'segments')='array'
      then
        for seg in
          select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
        loop
          if seg is null or jsonb_typeof(seg) <> 'object' then
            continue;
          end if;

          seg_locked := nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '');
          if seg_locked is not null then
            continue; -- already invoiced
          end if;

          seg_target := nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date;
          seg_ref := btrim(coalesce(seg->>'ref_num',''));

          -- segment-level ref gating if required
          select * into pc from public.v_ts_invoice_precheck where timesheet_id = tsid limit 1;
          if pc.require_reference_to_invoice is true and seg_ref = '' then
            continue;
          end if;

          -- delayed detection
          if seg_target is null or seg_target = natural_start then
            -- not delayed: belongs to natural week only
            if v_week_start <> natural_start then
              continue;
            end if;
            -- allow early is implicit for invoice edits (invoice is already being edited)
            segments := segments || jsonb_build_array(seg);
          else
            -- delayed: only if this invoice week matches target AND target has arrived (no early invoicing for delayed)
            if v_week_start = seg_target and seg_target <= v_anchor_ymd then
              segments := segments || jsonb_build_array(seg);
            end if;
          end if;
        end loop;
      else
        -- Non-segments: include only when invoice week matches natural week
        if v_week_start = natural_start then
          segments := jsonb_build_array(
            jsonb_build_object(
              'segment_id', null,
              'date', coalesce(snap.week_ending_date::text, v_week_start::text),
              'hours_day', public._inv_round2(coalesce(snap.hours_day,0)),
              'hours_night', public._inv_round2(coalesce(snap.hours_night,0)),
              'hours_sat', public._inv_round2(coalesce(snap.hours_sat,0)),
              'hours_sun', public._inv_round2(coalesce(snap.hours_sun,0)),
              'hours_bh', public._inv_round2(coalesce(snap.hours_bh,0)),
              'pay_amount', public._inv_round2(
                coalesce(snap.total_pay_ex_vat,0)
                - coalesce(snap.additional_pay_ex_vat,0)
                - coalesce(snap.expenses_pay_ex_vat,0)
                - coalesce(snap.mileage_pay_ex_vat,0)
                - coalesce(snap.travel_pay_ex_vat,0)
                - coalesce(snap.accommodation_pay_ex_vat,0)
                - coalesce(snap.other_pay_ex_vat,0)
              ),
              'charge_amount', public._inv_round2(
                coalesce(snap.total_charge_ex_vat,0)
                - coalesce(snap.additional_charge_ex_vat,0)
                - coalesce(snap.expenses_charge_ex_vat,0)
                - coalesce(snap.mileage_charge_ex_vat,0)
                - coalesce(snap.travel_charge_ex_vat,0)
                - coalesce(snap.accommodation_charge_ex_vat,0)
                - coalesce(snap.other_charge_ex_vat,0)
              ),
              'ref_num', coalesce(snap.reference_number,'')
            )
          );
        end if;
      end if;

      if jsonb_array_length(coalesce(segments,'[]'::jsonb)) = 0 then
        continue;
      end if;

      -- HOURS lines
      if c_daily_calc then
        -- Daily: group by segment.date
        for ts in
          with rows as (
            select
              nullif(btrim(coalesce(seg->>'date','')), '') as ymd,
              coalesce((seg_el->>'hours_day')::numeric,0)   as h_day,
              coalesce((seg_el->>'hours_night')::numeric,0) as h_night,
              coalesce((seg_el->>'hours_sat')::numeric,0)   as h_sat,
              coalesce((seg_el->>'hours_sun')::numeric,0)   as h_sun,
              coalesce((seg_el->>'hours_bh')::numeric,0)    as h_bh,
              coalesce((seg_el->>'pay_amount')::numeric,0)  as pay_ex,
              coalesce((seg_el->>'charge_amount')::numeric,0) as chg_ex
            from jsonb_array_elements(segments) seg_el
          ),
          agg as (
            select
              ymd,
              sum(h_day)::numeric as hours_day,
              sum(h_night)::numeric as hours_night,
              sum(h_sat)::numeric as hours_sat,
              sum(h_sun)::numeric as hours_sun,
              sum(h_bh)::numeric as hours_bh,
              sum(pay_ex)::numeric as pay_ex,
              sum(chg_ex)::numeric as chg_ex
            from rows
            where ymd is not null and ymd ~ '^\d{4}-\d{2}-\d{2}$'
            group by ymd
          )
          select * from agg order by ymd
        loop
          chg_ex := public._inv_round2(ts.chg_ex);
          if chg_ex <= 0 then continue; end if;

          if (coalesce(ts.hours_day,0)+coalesce(ts.hours_night,0)+coalesce(ts.hours_sat,0)+coalesce(ts.hours_sun,0)+coalesce(ts.hours_bh,0)) <= 0 then
            continue;
          end if;

          pay_ex := public._inv_round2(ts.pay_ex);
          margin_ex := public._inv_round2(chg_ex - pay_ex);
          vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
          inc_amt := public._inv_round2(chg_ex + vat_amt);

          line_desc := coalesce(nullif(btrim(coalesce(c_display_site,'')) ,''), ('TS '||tsid::text)) ||
                       ' – '|| ts.ymd || ' – W/E '|| coalesce(snap.week_ending_date::text,'');

          v_meta := jsonb_build_object(
            'line_type','HOURS_DAILY',
            'timesheet_id', tsid::text,
            'tsfin_id', snap.id::text,
            'candidate_display', coalesce(nullif(btrim(coalesce(c_display_site,'')),''), null),
            'role', c_role,
            'hospital', c_display_site,
            'ward', c_ward_hint,
            'week_ending_date', snap.week_ending_date::text,
            'date', ts.ymd,
            'bucket_labels', c_bucket_labels
          );

          source_key := 'TS:' || tsid::text || ':HOURS:' || ts.ymd;

          insert into public.invoice_lines(
            invoice_id, timesheet_id, booking_id, description,
            hours_day, hours_night, hours_sat, hours_sun, hours_bh,
            pay_day, pay_night, pay_sat, pay_sun, pay_bh,
            charge_day, charge_night, charge_sat, charge_sun, charge_bh,
            total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
            vat_rate_pct, vat_amount, total_inc_vat,
            paper_ts_r2_key, meta_json, source_key
          )
          values (
            p_invoice_id, tsid, snap.booking_id, line_desc,
            public._inv_round2(ts.hours_day), public._inv_round2(ts.hours_night), public._inv_round2(ts.hours_sat), public._inv_round2(ts.hours_sun), public._inv_round2(ts.hours_bh),
            null,null,null,null,null,
            null,null,null,null,null,
            pay_ex, chg_ex, margin_ex,
            v_vat_rate, vat_amt, inc_amt,
            ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
            v_meta,
            source_key
          )
          on conflict (invoice_id, source_key) do nothing;
        end loop;
      else
        -- Weekly hours line
        select
          public._inv_round2(coalesce(sum((seg_el->>'hours_day')::numeric),0)),
          public._inv_round2(coalesce(sum((seg_el->>'hours_night')::numeric),0)),
          public._inv_round2(coalesce(sum((seg_el->>'hours_sat')::numeric),0)),
          public._inv_round2(coalesce(sum((seg_el->>'hours_sun')::numeric),0)),
          public._inv_round2(coalesce(sum((seg_el->>'hours_bh')::numeric),0)),
          public._inv_round2(coalesce(sum((seg_el->>'pay_amount')::numeric),0)),
          public._inv_round2(coalesce(sum((seg_el->>'charge_amount')::numeric),0))
        into h_day, h_night, h_sat, h_sun, h_bh, pay_ex, chg_ex
        from jsonb_array_elements(segments) seg_el;

        if chg_ex > 0 and (coalesce(h_day,0)+coalesce(h_night,0)+coalesce(h_sat,0)+coalesce(h_sun,0)+coalesce(h_bh,0)) > 0 then
          margin_ex := public._inv_round2(chg_ex - pay_ex);
          vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
          inc_amt := public._inv_round2(chg_ex + vat_amt);

          line_desc := coalesce(nullif(btrim(coalesce(c_display_site,'')) ,''), ('TS '||tsid::text)) ||
                       ' – W/E '|| coalesce(snap.week_ending_date::text,'');

          v_meta := jsonb_build_object(
            'line_type','HOURS_WEEKLY',
            'timesheet_id', tsid::text,
            'tsfin_id', snap.id::text,
            'week_ending_date', snap.week_ending_date::text,
            'bucket_labels', c_bucket_labels
          );

          source_key := 'TS:' || tsid::text || ':HOURS:WEEK';

          insert into public.invoice_lines(
            invoice_id, timesheet_id, booking_id, description,
            hours_day, hours_night, hours_sat, hours_sun, hours_bh,
            pay_day, pay_night, pay_sat, pay_sun, pay_bh,
            charge_day, charge_night, charge_sat, charge_sun, charge_bh,
            total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
            vat_rate_pct, vat_amount, total_inc_vat,
            paper_ts_r2_key, meta_json, source_key
          )
          values (
            p_invoice_id, tsid, snap.booking_id, line_desc,
            h_day, h_night, h_sat, h_sun, h_bh,
            null,null,null,null,null,
            null,null,null,null,null,
            pay_ex, chg_ex, margin_ex,
            v_vat_rate, vat_amt, inc_amt,
            ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
            v_meta,
            source_key
          )
          on conflict (invoice_id, source_key) do nothing;
        end if;
      end if;

      -- Additional rates (WEEKLY; mirrors generator patterns at a high level)
      if snap.additional_units_json is not null and jsonb_typeof(snap.additional_units_json) = 'object' then
        for kv in
          select key as k, value as v
          from jsonb_each(snap.additional_units_json)
        loop
          ex := kv.v;
          if ex is null or jsonb_typeof(ex) <> 'object' then
            continue;
          end if;

          code := upper(btrim(coalesce(kv.k,'')));
          if code = '' then continue; end if;

          unit_count := coalesce((ex->>'unit_count')::numeric, 0);
          if unit_count <= 0 then continue; end if;

          pay_ex := public._inv_round2(coalesce((ex->>'pay_ex_vat')::numeric, 0));
          chg_ex := public._inv_round2(coalesce((ex->>'charge_ex_vat')::numeric, 0));
          if chg_ex <= 0 then continue; end if;

          margin_ex := public._inv_round2(chg_ex - pay_ex);
          vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
          inc_amt := public._inv_round2(chg_ex + vat_amt);

          bucket_name := nullif(btrim(coalesce(ex->>'bucket_name','')), '');
          if bucket_name is null then bucket_name := code; end if;

          unit_name := nullif(btrim(coalesce(ex->>'unit_name','')), '');
          if unit_name is null then unit_name := 'units'; end if;

          line_desc := 'Additional – ' || bucket_name || ' – ' || unit_count::text || ' ' || unit_name ||
                       case when snap.week_ending_date is not null then ' (W/E ' || snap.week_ending_date::text || ')' else '' end;

          v_meta := jsonb_build_object(
            'line_type','ADDITIONAL_RATE',
            'timesheet_id', tsid::text,
            'tsfin_id', snap.id::text,
            'week_ending_date', snap.week_ending_date::text,
            'bucket', jsonb_build_object('code', code, 'name', bucket_name),
            'units', jsonb_build_object('unit_count', unit_count, 'unit_name', unit_name)
          );

          source_key := 'TS:' || tsid::text || ':ADD:' || code || ':WEEK';

          insert into public.invoice_lines(
            invoice_id, timesheet_id, booking_id, description,
            hours_day, hours_night, hours_sat, hours_sun, hours_bh,
            pay_day, pay_night, pay_sat, pay_sun, pay_bh,
            charge_day, charge_night, charge_sat, charge_sun, charge_bh,
            total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
            vat_rate_pct, vat_amount, total_inc_vat,
            paper_ts_r2_key, meta_json, source_key
          )
          values (
            p_invoice_id, tsid, snap.booking_id, line_desc,
            0,0,0,0,0,
            null,null,null,null,null,
            null,null,null,null,null,
            pay_ex, chg_ex, margin_ex,
            v_vat_rate, vat_amt, inc_amt,
            ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
            v_meta,
            source_key
          )
          on conflict (invoice_id, source_key) do nothing;
        end loop;
      end if;

      -- Expense categories: travel/accommodation/other (weekly)
      v_note_travel := null;
      v_note_accom := null;
      v_note_other := null;
      if snap.expenses_description is not null and length(btrim(snap.expenses_description)) > 0 then
        v_note_other := btrim(snap.expenses_description);
      end if;

      -- TRAVEL
      if public._inv_round2(coalesce(snap.travel_charge_ex_vat,0)) > 0 then
        pay_ex := public._inv_round2(coalesce(snap.travel_pay_ex_vat,0));
        chg_ex := public._inv_round2(coalesce(snap.travel_charge_ex_vat,0));
        margin_ex := public._inv_round2(chg_ex - pay_ex);
        vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
        inc_amt := public._inv_round2(chg_ex + vat_amt);

        line_desc := 'Travel expenses' || case when snap.week_ending_date is not null then ' (W/E ' || snap.week_ending_date::text || ')' else '' end;

        v_meta := jsonb_build_object(
          'line_type','EXPENSE_TRAVEL',
          'timesheet_id', tsid::text,
          'tsfin_id', snap.id::text,
          'week_ending_date', snap.week_ending_date::text
        );

        source_key := 'TS:' || tsid::text || ':EXP:TRAVEL';

        insert into public.invoice_lines(
          invoice_id, timesheet_id, booking_id, description,
          hours_day, hours_night, hours_sat, hours_sun, hours_bh,
          pay_day, pay_night, pay_sat, pay_sun, pay_bh,
          charge_day, charge_night, charge_sat, charge_sun, charge_bh,
          total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
          vat_rate_pct, vat_amount, total_inc_vat,
          paper_ts_r2_key, meta_json, source_key
        )
        values (
          p_invoice_id, tsid, snap.booking_id, line_desc,
          0,0,0,0,0,
          null,null,null,null,null,
          null,null,null,null,null,
          pay_ex, chg_ex, margin_ex,
          v_vat_rate, vat_amt, inc_amt,
          ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
          v_meta,
          source_key
        )
        on conflict (invoice_id, source_key) do nothing;
      end if;

      -- ACCOMMODATION
      if public._inv_round2(coalesce(snap.accommodation_charge_ex_vat,0)) > 0 then
        pay_ex := public._inv_round2(coalesce(snap.accommodation_pay_ex_vat,0));
        chg_ex := public._inv_round2(coalesce(snap.accommodation_charge_ex_vat,0));
        margin_ex := public._inv_round2(chg_ex - pay_ex);
        vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
        inc_amt := public._inv_round2(chg_ex + vat_amt);

        line_desc := 'Accommodation expenses' || case when snap.week_ending_date is not null then ' (W/E ' || snap.week_ending_date::text || ')' else '' end;

        v_meta := jsonb_build_object(
          'line_type','EXPENSE_ACCOMMODATION',
          'timesheet_id', tsid::text,
          'tsfin_id', snap.id::text,
          'week_ending_date', snap.week_ending_date::text
        );

        source_key := 'TS:' || tsid::text || ':EXP:ACCOM';

        insert into public.invoice_lines(
          invoice_id, timesheet_id, booking_id, description,
          hours_day, hours_night, hours_sat, hours_sun, hours_bh,
          pay_day, pay_night, pay_sat, pay_sun, pay_bh,
          charge_day, charge_night, charge_sat, charge_sun, charge_bh,
          total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
          vat_rate_pct, vat_amount, total_inc_vat,
          paper_ts_r2_key, meta_json, source_key
        )
        values (
          p_invoice_id, tsid, snap.booking_id, line_desc,
          0,0,0,0,0,
          null,null,null,null,null,
          null,null,null,null,null,
          pay_ex, chg_ex, margin_ex,
          v_vat_rate, vat_amt, inc_amt,
          ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
          v_meta,
          source_key
        )
        on conflict (invoice_id, source_key) do nothing;
      end if;

      -- OTHER
      if public._inv_round2(coalesce(snap.other_charge_ex_vat,0)) > 0 then
        pay_ex := public._inv_round2(coalesce(snap.other_pay_ex_vat,0));
        chg_ex := public._inv_round2(coalesce(snap.other_charge_ex_vat,0));
        margin_ex := public._inv_round2(chg_ex - pay_ex);
        vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
        inc_amt := public._inv_round2(chg_ex + vat_amt);

        line_desc := 'Other expenses' || case when snap.week_ending_date is not null then ' (W/E ' || snap.week_ending_date::text || ')' else '' end;

        v_meta := jsonb_build_object(
          'line_type','EXPENSE_OTHER',
          'timesheet_id', tsid::text,
          'tsfin_id', snap.id::text,
          'week_ending_date', snap.week_ending_date::text
        );

        source_key := 'TS:' || tsid::text || ':EXP:OTHER';

        insert into public.invoice_lines(
          invoice_id, timesheet_id, booking_id, description,
          hours_day, hours_night, hours_sat, hours_sun, hours_bh,
          pay_day, pay_night, pay_sat, pay_sun, pay_bh,
          charge_day, charge_night, charge_sat, charge_sun, charge_bh,
          total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
          vat_rate_pct, vat_amount, total_inc_vat,
          paper_ts_r2_key, meta_json, source_key
        )
        values (
          p_invoice_id, tsid, snap.booking_id, line_desc,
          0,0,0,0,0,
          null,null,null,null,null,
          null,null,null,null,null,
          pay_ex, chg_ex, margin_ex,
          v_vat_rate, vat_amt, inc_amt,
          ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
          v_meta,
          source_key
        )
        on conflict (invoice_id, source_key) do nothing;
      end if;

      -- Mileage (one per timesheet)
      if public._inv_round2(coalesce(snap.mileage_charge_ex_vat,0)) > 0 then
        pay_ex := public._inv_round2(coalesce(snap.mileage_pay_ex_vat,0));
        chg_ex := public._inv_round2(coalesce(snap.mileage_charge_ex_vat,0));
        margin_ex := public._inv_round2(chg_ex - pay_ex);
        vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
        inc_amt := public._inv_round2(chg_ex + vat_amt);

        line_desc := 'Mileage' || case when snap.week_ending_date is not null then ' (W/E ' || snap.week_ending_date::text || ')' else '' end;

        v_meta := jsonb_build_object(
          'line_type','MILEAGE',
          'timesheet_id', tsid::text,
          'tsfin_id', snap.id::text,
          'week_ending_date', snap.week_ending_date::text
        );

        source_key := 'TS:' || tsid::text || ':MILEAGE';

        insert into public.invoice_lines(
          invoice_id, timesheet_id, booking_id, description,
          hours_day, hours_night, hours_sat, hours_sun, hours_bh,
          pay_day, pay_night, pay_sat, pay_sun, pay_bh,
          charge_day, charge_night, charge_sat, charge_sun, charge_bh,
          total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
          vat_rate_pct, vat_amount, total_inc_vat,
          paper_ts_r2_key, meta_json, source_key
        )
        values (
          p_invoice_id, tsid, snap.booking_id, line_desc,
          0,0,0,0,0,
          null,null,null,null,null,
          null,null,null,null,null,
          pay_ex, chg_ex, margin_ex,
          v_vat_rate, vat_amt, inc_amt,
          ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
          v_meta,
          source_key
        )
        on conflict (invoice_id, source_key) do nothing;
      end if;

      -- Build segment refs to lock
      if snap.invoice_breakdown_json is not null
         and jsonb_typeof(snap.invoice_breakdown_json)='object'
         and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
      then
        for seg in
          select value from jsonb_array_elements(segments) value
        loop
          if seg is null or jsonb_typeof(seg) <> 'object' then
            continue;
          end if;

          seg_refs := seg_refs || jsonb_build_array(
            jsonb_build_object(
              'tsfin_id', snap.id::text,
              'segment_id', nullif(btrim(coalesce(seg->>'segment_id','')), '')
            )
          );
        end loop;
      else
        -- lock whole for non-segments
        seg_refs := seg_refs || jsonb_build_array(
          jsonb_build_object('tsfin_id', snap.id::text, 'segment_id', null)
        );
      end if;

    end loop;

    -- Apply segment locks
    if jsonb_typeof(seg_refs) = 'array' and jsonb_array_length(seg_refs) > 0 then
      perform public._inv_lock_segments_for_invoice(p_invoice_id, seg_refs);
    end if;

    -- Mark contract weeks INVOICED only when fully locked by this invoice
    update public.contract_weeks cw
    set status = 'INVOICED'::public.contract_week_status_enum
    where cw.timesheet_id in (
      select tf.timesheet_id
      from public.timesheets_financials tf
      where tf.is_current = true
        and tf.locked_by_invoice_id = p_invoice_id
    );
  end if;

  -- 4) Recompute invoice totals from invoice_lines and clear PDF key
  select
    public._inv_round2(coalesce(sum(coalesce(l.total_charge_ex_vat,0)),0)),
    public._inv_round2(coalesce(sum(coalesce(l.vat_amount,0)),0)),
    public._inv_round2(coalesce(sum(coalesce(l.total_inc_vat,0)),0))
  into v_new_ex, v_new_vat, v_new_inc
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id;

  perform public.invoice_recompute_totals(p_invoice_id);
-- Return updated manifest
  select public.invoice_render_manifest(p_invoice_id) into v_manifest;
  return coalesce(v_manifest, '{}'::jsonb);
end;
$$;




