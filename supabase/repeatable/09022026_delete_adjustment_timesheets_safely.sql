CREATE OR REPLACE FUNCTION public.timesheet_weekly_chain_delete_apply(
  p_timesheet_id uuid,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_preview jsonb;
  v_eligible boolean := false;

  v_contract_id uuid := null;
  v_week_ending_date date := null;

  v_timesheet_ids uuid[] := array[]::uuid[];
  v_contract_week_ids uuid[] := array[]::uuid[];

  v_deleted_timesheets int := 0;
  v_deleted_contract_weeks int := 0;
  v_deleted_shifts int := 0;

  v_current_timesheet_id uuid := null;

  v_before jsonb := null;
  v_after jsonb := null;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_weekly_chain_delete_apply: timesheet_id is required';
  end if;

  v_preview := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);

  v_eligible := coalesce((v_preview->>'eligible')::boolean, false);

  if not v_eligible then
    raise exception 'timesheet_weekly_chain_delete_apply: not eligible: %', v_preview::text;
  end if;

  v_current_timesheet_id := nullif(v_preview->>'current_timesheet_id','')::uuid;
  v_contract_id := nullif(v_preview->>'contract_id','')::uuid;
  v_week_ending_date := (v_preview->>'week_ending_date')::date;

  -- Extract arrays from preview JSON
  select coalesce(array_agg(x::uuid), array[]::uuid[])
  into v_timesheet_ids
  from jsonb_array_elements_text(coalesce(v_preview->'timesheet_ids','[]'::jsonb)) as e(x);

  select coalesce(array_agg(y::uuid), array[]::uuid[])
  into v_contract_week_ids
  from jsonb_array_elements_text(coalesce(v_preview->'contract_week_ids','[]'::jsonb)) as e2(y);

  -- Snapshot for audit
  v_before := jsonb_build_object(
    'requested_timesheet_id', p_timesheet_id::text,
    'current_timesheet_id', v_current_timesheet_id::text,
    'contract_id', case when v_contract_id is null then null else v_contract_id::text end,
    'week_ending_date', case when v_week_ending_date is null then null else v_week_ending_date::text end,
    'timesheet_ids', to_jsonb(coalesce(v_timesheet_ids, array[]::uuid[])),
    'contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[]))
  );

  -- ─────────────────────────────────────────────────────────────
  -- Deletions (transactional)
  -- ─────────────────────────────────────────────────────────────

  -- NHSP/HR shifts: delete week truth rows (as if shifts never happened) + any rows linked to timesheets being deleted
  if v_contract_id is not null and v_week_ending_date is not null then
    delete from public.nhsp_shifts ns
    where (ns.contract_id = v_contract_id
           and ns.week_ending_date = v_week_ending_date
           and ns.source_system in ('NHSP'::public.hr_source_enum, 'HEALTHROSTER'::public.hr_source_enum))
       or (ns.timesheet_id = any(v_timesheet_ids));
  else
    -- Fallback: just detach any remaining nhsp_shifts references (should be none for eligible chain)
    update public.nhsp_shifts ns2
    set timesheet_id = null
    where ns2.timesheet_id = any(v_timesheet_ids);
  end if;

  get diagnostics v_deleted_shifts = row_count;

  -- Dependent rows (same pattern used elsewhere in your import cleanup)
  delete from public.pay_item_snoozes ps where ps.timesheet_id = any(v_timesheet_ids);
  delete from public.pay_batch_items pbi where pbi.timesheet_id = any(v_timesheet_ids);
  delete from public.ts_pay_adjustments tpa where tpa.timesheet_id = any(v_timesheet_ids);
  delete from public.timesheet_pay_state_history tpsh where tpsh.timesheet_id = any(v_timesheet_ids);
  delete from public.timesheet_pay_state tps where tps.timesheet_id = any(v_timesheet_ids);

  delete from public.timesheet_validations tv where tv.timesheet_id = any(v_timesheet_ids);
  delete from public.hr_results hr where hr.timesheet_id = any(v_timesheet_ids);
  delete from public.hr_issue_emails hie where hie.timesheet_id = any(v_timesheet_ids);

  delete from public.timesheet_evidence te where te.timesheet_id = any(v_timesheet_ids);
  delete from public.manual_timesheet_queue mtq where mtq.timesheet_id = any(v_timesheet_ids);
  delete from public.ts_pdfs_outbox tpo where tpo.timesheet_id = any(v_timesheet_ids);
  delete from public.ts_financials_outbox tfo where tfo.timesheet_id = any(v_timesheet_ids);

  -- TSFIN rows
  delete from public.timesheets_financials tfz where tfz.timesheet_id = any(v_timesheet_ids);

  -- Contract weeks must be explicitly removed (FK is ON DELETE SET NULL)
  delete from public.contract_weeks cwz
  where cwz.id = any(v_contract_week_ids)
     or cwz.timesheet_id = any(v_timesheet_ids)
     or (v_contract_id is not null and v_week_ending_date is not null
         and cwz.contract_id = v_contract_id
         and cwz.week_ending_date = v_week_ending_date
         and cwz.is_adjustment is true
         and cwz.additional_seq > 0
         and cwz.timesheet_id is null);

  get diagnostics v_deleted_contract_weeks = row_count;

  -- Timesheets (all versions already included)
  delete from public.timesheets tz where tz.timesheet_id = any(v_timesheet_ids);

  get diagnostics v_deleted_timesheets = row_count;

  v_after := jsonb_build_object(
    'deleted_timesheets', v_deleted_timesheets,
    'deleted_contract_weeks', v_deleted_contract_weeks,
    'deleted_nhsp_shifts', v_deleted_shifts
  );

  -- Ungated user-facing audit
  begin
    perform public._audit_insert(
      'timesheets',
      coalesce(v_current_timesheet_id::text, p_timesheet_id::text),
      'WEEKLY_CHAIN_DELETE_APPLIED',
      v_before,
      v_after,
      null,
      p_actor_user_id
    );
  exception when others then
    null;
  end;

  return jsonb_build_object(
    'ok', true,
    'kind', 'WEEKLY_CHAIN_DELETE_PARENT',
    'current_timesheet_id', coalesce(v_current_timesheet_id::text, p_timesheet_id::text),
    'deleted_timesheets', v_deleted_timesheets,
    'deleted_contract_weeks', v_deleted_contract_weeks,
    'deleted_nhsp_shifts', v_deleted_shifts
  );
end;
$function$;




CREATE OR REPLACE FUNCTION public.timesheet_weekly_manual_adjustment_delete_apply(
  p_timesheet_id uuid,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_preview jsonb;
  v_eligible boolean := false;

  v_current_timesheet_id uuid := null;
  v_contract_week_id uuid := null;

  v_timesheet_ids uuid[] := array[]::uuid[];

  v_deleted_timesheets int := 0;
  v_deleted_contract_weeks int := 0;

  v_before jsonb := null;
  v_after jsonb := null;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_weekly_manual_adjustment_delete_apply: timesheet_id is required';
  end if;

  v_preview := public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id, p_actor_user_id);
  v_eligible := coalesce((v_preview->>'eligible')::boolean, false);

  if not v_eligible then
    raise exception 'timesheet_weekly_manual_adjustment_delete_apply: not eligible: %', v_preview::text;
  end if;

  v_current_timesheet_id := nullif(v_preview->>'current_timesheet_id','')::uuid;
  v_contract_week_id := nullif(v_preview->>'contract_week_id','')::uuid;

  select coalesce(array_agg(x::uuid), array[]::uuid[])
  into v_timesheet_ids
  from jsonb_array_elements_text(coalesce(v_preview->'timesheet_ids','[]'::jsonb)) as e(x);

  v_before := jsonb_build_object(
    'requested_timesheet_id', p_timesheet_id::text,
    'current_timesheet_id', v_current_timesheet_id::text,
    'contract_week_id', v_contract_week_id::text,
    'timesheet_ids', to_jsonb(coalesce(v_timesheet_ids, array[]::uuid[]))
  );

  -- Detach any unexpected nhsp_shifts references (preserve truth rows)
  update public.nhsp_shifts ns
  set timesheet_id = null
  where ns.timesheet_id = any(v_timesheet_ids);

  -- Dependent rows
  delete from public.pay_item_snoozes ps where ps.timesheet_id = any(v_timesheet_ids);
  delete from public.pay_batch_items pbi where pbi.timesheet_id = any(v_timesheet_ids);
  delete from public.ts_pay_adjustments tpa where tpa.timesheet_id = any(v_timesheet_ids);
  delete from public.timesheet_pay_state_history tpsh where tpsh.timesheet_id = any(v_timesheet_ids);
  delete from public.timesheet_pay_state tps where tps.timesheet_id = any(v_timesheet_ids);

  delete from public.timesheet_validations tv where tv.timesheet_id = any(v_timesheet_ids);
  delete from public.hr_results hr where hr.timesheet_id = any(v_timesheet_ids);
  delete from public.hr_issue_emails hie where hie.timesheet_id = any(v_timesheet_ids);

  delete from public.timesheet_evidence te where te.timesheet_id = any(v_timesheet_ids);
  delete from public.manual_timesheet_queue mtq where mtq.timesheet_id = any(v_timesheet_ids);
  delete from public.ts_pdfs_outbox tpo where tpo.timesheet_id = any(v_timesheet_ids);
  delete from public.ts_financials_outbox tfo where tfo.timesheet_id = any(v_timesheet_ids);

  delete from public.timesheets_financials tfz where tfz.timesheet_id = any(v_timesheet_ids);

  -- Contract-week row MUST be deleted (avoid orphan adjustment week)
  delete from public.contract_weeks cw
  where cw.id = v_contract_week_id
     or cw.timesheet_id = any(v_timesheet_ids);

  get diagnostics v_deleted_contract_weeks = row_count;

  delete from public.timesheets tdel
  where tdel.timesheet_id = any(v_timesheet_ids);

  get diagnostics v_deleted_timesheets = row_count;

  v_after := jsonb_build_object(
    'deleted_timesheets', v_deleted_timesheets,
    'deleted_contract_weeks', v_deleted_contract_weeks
  );

  begin
    perform public._audit_insert(
      'timesheets',
      coalesce(v_current_timesheet_id::text, p_timesheet_id::text),
      'WEEKLY_MANUAL_ADJUSTMENT_DELETE_APPLIED',
      v_before,
      v_after,
      null,
      p_actor_user_id
    );
  exception when others then
    null;
  end;

  return jsonb_build_object(
    'ok', true,
    'kind', 'WEEKLY_MANUAL_ADJUSTMENT_DELETE',
    'current_timesheet_id', coalesce(v_current_timesheet_id::text, p_timesheet_id::text),
    'deleted_timesheets', v_deleted_timesheets,
    'deleted_contract_weeks', v_deleted_contract_weeks
  );
end;
$function$;




CREATE OR REPLACE FUNCTION public.timesheet_weekly_chain_delete_preview(
  p_timesheet_id uuid,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_in_ts public.timesheets%rowtype;
  v_current_ts public.timesheets%rowtype;

  v_contract_id uuid := null;
  v_week_ending_date date := null;

  v_base_booking_id text := null;

  v_booking_ids text[] := array[]::text[];
  v_timesheet_ids uuid[] := array[]::uuid[];
  v_contract_week_ids uuid[] := array[]::uuid[];
  v_nhsp_shift_ids uuid[] := array[]::uuid[];

  v_blocked jsonb := '[]'::jsonb;

  v_has_tsfin_lock boolean := false;
  v_has_seg_locks boolean := false;
  v_has_invoice_lines boolean := false;
  v_has_pay_state boolean := false;

  v_invoice_info jsonb := '[]'::jsonb;

  v_delete_items jsonb := '[]'::jsonb;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_weekly_chain_delete_preview: timesheet_id is required';
  end if;

  select t.*
  into v_in_ts
  from public.timesheets t
  where t.timesheet_id = p_timesheet_id;

  if not found then
    raise exception 'timesheet_weekly_chain_delete_preview: timesheet % not found', p_timesheet_id;
  end if;

  v_base_booking_id := v_in_ts.booking_id;

  -- Resolve "current" by booking_id (robust even if is_current flags drift)
  select tcur.*
  into v_current_ts
  from public.timesheets tcur
  where tcur.booking_id = v_base_booking_id
  order by tcur.is_current desc, tcur.version desc
  limit 1;

  if not found then
    raise exception 'timesheet_weekly_chain_delete_preview: booking_id % not found', v_base_booking_id;
  end if;

  -- Must be WEEKLY parent (not adjustment)
  if v_current_ts.sheet_scope <> 'WEEKLY'::public.timesheet_scope_enum then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','NOT_WEEKLY',
      'message','Parent-chain delete applies only to WEEKLY parent timesheets.',
      'timesheet_id', v_current_ts.timesheet_id::text
    ));
  end if;

  if v_current_ts.is_adjustment is true then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','NOT_PARENT',
      'message','Timesheet is an adjustment; parent-chain delete can only be requested for a non-adjustment WEEKLY parent timesheet.',
      'timesheet_id', v_current_ts.timesheet_id::text
    ));
  end if;

  if v_current_ts.contract_id is null or v_current_ts.week_ending_date is null then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','MISSING_CONTEXT',
      'message','Timesheet is missing contract_id and/or week_ending_date; cannot resolve chain safely.',
      'timesheet_id', v_current_ts.timesheet_id::text
    ));
  end if;

  v_contract_id := v_current_ts.contract_id;
  v_week_ending_date := v_current_ts.week_ending_date;

  -- Booking IDs in scope:
  -- - the parent booking_id
  -- - import-derived weekly adjustments in the same contract/week (NHSP/HR corrections/cancellations)
  if v_contract_id is not null and v_week_ending_date is not null then
    select coalesce(array_agg(distinct s.booking_id), array[]::text[])
    into v_booking_ids
    from (
      select v_current_ts.booking_id as booking_id
      union all
      select t_adj.booking_id
      from public.timesheets t_adj
      where t_adj.contract_id = v_contract_id
        and t_adj.week_ending_date = v_week_ending_date
        and t_adj.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
        and t_adj.is_adjustment is true
        and (
          upper(coalesce(t_adj.adjustment_origin,'')) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
          or nullif(btrim(coalesce(t_adj.correction_kind,'')),'') is not null
          or nullif(btrim(coalesce(t_adj.correction_id,'')),'') is not null
        )
    ) s;
  else
    v_booking_ids := array[v_current_ts.booking_id];
  end if;

  -- All timesheet IDs (all versions) for those booking IDs
  select coalesce(array_agg(t_all.timesheet_id), array[]::uuid[])
  into v_timesheet_ids
  from public.timesheets t_all
  where t_all.booking_id = any(v_booking_ids);

  -- Contract-week IDs to delete:
  -- (a) base week row additional_seq=0 (if exists)
  -- (b) any contract_week row linked to any timesheet in scope
  -- (c) orphan adjustment contract weeks for this contract/week (timesheet_id null)
  if v_contract_id is not null and v_week_ending_date is not null then
    select coalesce(array_agg(distinct s2.cw_id), array[]::uuid[])
    into v_contract_week_ids
    from (
      select cw0.id as cw_id
      from public.contract_weeks cw0
      where cw0.contract_id = v_contract_id
        and cw0.week_ending_date = v_week_ending_date
        and cw0.additional_seq = 0
      union all
      select cw1.id as cw_id
      from public.contract_weeks cw1
      where cw1.timesheet_id = any(v_timesheet_ids)
      union all
      select cw2.id as cw_id
      from public.contract_weeks cw2
      where cw2.contract_id = v_contract_id
        and cw2.week_ending_date = v_week_ending_date
        and cw2.is_adjustment is true
        and cw2.additional_seq > 0
        and cw2.timesheet_id is null
    ) s2;
  else
    v_contract_week_ids := array[]::uuid[];
  end if;

  -- NHSP/HR truth rows in this contract/week (for informational preview)
  if v_contract_id is not null and v_week_ending_date is not null then
    select coalesce(array_agg(ns.id), array[]::uuid[])
    into v_nhsp_shift_ids
    from public.nhsp_shifts ns
    where ns.contract_id = v_contract_id
      and ns.week_ending_date = v_week_ending_date
      and ns.source_system in ('NHSP'::public.hr_source_enum, 'HEALTHROSTER'::public.hr_source_enum);
  else
    v_nhsp_shift_ids := array[]::uuid[];
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Safety gates (must be chain-safe across all versions)
  -- ─────────────────────────────────────────────────────────────

  -- TSFIN lock/paid markers (current snapshots only)
  select exists(
    select 1
    from public.timesheets_financials tf
    where tf.timesheet_id = any(v_timesheet_ids)
      and tf.is_current is true
      and (tf.locked_by_invoice_id is not null or tf.paid_at_utc is not null)
  )
  into v_has_tsfin_lock;

  if v_has_tsfin_lock then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','TSFIN_LOCK_OR_PAID',
      'message','One or more timesheets in the chain have TSFIN locked_by_invoice_id and/or paid_at_utc set.'
    ));
  end if;

  -- Segment-level invoice locks (SEGMENTS mode)
  select exists(
    select 1
    from public.timesheets_financials tf2
    cross join lateral jsonb_array_elements(
      case
        when upper(coalesce(tf2.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
             and jsonb_typeof(tf2.invoice_breakdown_json->'segments') = 'array'
          then tf2.invoice_breakdown_json->'segments'
        else '[]'::jsonb
      end
    ) seg(seg_elem)
    where tf2.timesheet_id = any(v_timesheet_ids)
      and tf2.is_current is true
      and nullif(btrim(coalesce(seg.seg_elem->>'invoice_locked_invoice_id','')),'') is not null
  )
  into v_has_seg_locks;

  if v_has_seg_locks then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','SEGMENT_LOCKED',
      'message','One or more timesheets in the chain have segment-level invoice locks (invoice_locked_invoice_id) in TSFIN invoice_breakdown_json.'
    ));
  end if;

  -- Any invoice_lines referencing any timesheet/booking in scope blocks delete
  select exists(
    select 1
    from public.invoice_lines il
    where (il.timesheet_id is not null and il.timesheet_id = any(v_timesheet_ids))
       or (il.booking_id is not null and il.booking_id = any(v_booking_ids))
  )
  into v_has_invoice_lines;

  if v_has_invoice_lines then
    select coalesce(
      jsonb_agg(distinct jsonb_build_object(
        'invoice_id', inv.id::text,
        'status', inv.status::text
      )),
      '[]'::jsonb
    )
    into v_invoice_info
    from public.invoice_lines il2
    join public.invoices inv
      on inv.id = il2.invoice_id
    where (il2.timesheet_id is not null and il2.timesheet_id = any(v_timesheet_ids))
       or (il2.booking_id is not null and il2.booking_id = any(v_booking_ids));

    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','INVOICE_LINES_PRESENT',
      'message','One or more invoice_lines reference a timesheet/booking in this chain; parent-chain delete requires the chain to be not invoiced.',
      'invoices', v_invoice_info
    ));
  end if;

  -- Pay state present blocks delete
  select (
    exists(
      select 1
      from public.timesheet_pay_state tps
      where tps.timesheet_id = any(v_timesheet_ids)
    )
    or exists(
      select 1
      from public.timesheet_pay_state_history tpsh
      where tpsh.timesheet_id = any(v_timesheet_ids)
    )
  )
  into v_has_pay_state;

  if v_has_pay_state then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','PAY_STATE_PRESENT',
      'message','One or more timesheets in the chain have pay settlement state/history; parent-chain delete requires chain to be unpaid.'
    ));
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- delete_items[] for warning modal (one "display row" per booking_id)
  -- Use best-current row per booking_id (is_current desc, version desc).
  -- Totals pulled from current TSFIN row for that timesheet_id.
  -- ─────────────────────────────────────────────────────────────
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'timesheet_id', tsel.timesheet_id::text,
        'booking_id', tsel.booking_id,
        'week_ending_date', tsel.week_ending_date::text,
        'status', tsel.status::text,
        'is_adjustment', tsel.is_adjustment,
        'adjustment_origin', tsel.adjustment_origin,
        'correction_id', tsel.correction_id,
        'correction_kind', tsel.correction_kind,
        'display_role',
          case
            when tsel.booking_id = v_current_ts.booking_id then 'PARENT'
            else 'ADJUSTMENT'
          end,
        'total_hours', coalesce(tfsel.total_hours, 0),
        'total_pay_ex_vat', coalesce(tfsel.total_pay_ex_vat, 0),
        'total_charge_ex_vat', coalesce(tfsel.total_charge_ex_vat, 0)
      )
      ORDER BY
        (case when tsel.booking_id = v_current_ts.booking_id then 0 else 1 end),
        tsel.booking_id
    ),
    '[]'::jsonb
  )
  into v_delete_items
  from (
    select distinct on (t_pick.booking_id)
      t_pick.booking_id,
      t_pick.timesheet_id,
      t_pick.week_ending_date,
      t_pick.status,
      t_pick.is_adjustment,
      t_pick.adjustment_origin,
      t_pick.correction_id,
      t_pick.correction_kind
    from public.timesheets t_pick
    where t_pick.booking_id = any(v_booking_ids)
    order by t_pick.booking_id, t_pick.is_current desc, t_pick.version desc
  ) tsel
  left join public.timesheets_financials tfsel
    on tfsel.timesheet_id = tsel.timesheet_id
   and tfsel.is_current is true;

  return jsonb_build_object(
    'kind', 'WEEKLY_CHAIN_DELETE_PARENT',
    'requested_timesheet_id', p_timesheet_id::text,
    'current_timesheet_id', v_current_ts.timesheet_id::text,
    'contract_id', case when v_contract_id is null then null else v_contract_id::text end,
    'week_ending_date', case when v_week_ending_date is null then null else v_week_ending_date::text end,
    'booking_ids', to_jsonb(coalesce(v_booking_ids, array[]::text[])),
    'timesheet_ids', to_jsonb(coalesce(v_timesheet_ids, array[]::uuid[])),
    'contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[])),
    'nhsp_shift_ids', to_jsonb(coalesce(v_nhsp_shift_ids, array[]::uuid[])),
    'delete_items', v_delete_items,
    'eligible', (jsonb_array_length(v_blocked) = 0),
    'blocked_reasons', v_blocked
  );
end;
$function$;



CREATE OR REPLACE FUNCTION public.timesheet_weekly_manual_adjustment_delete_preview(
  p_timesheet_id uuid,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_in_ts public.timesheets%rowtype;
  v_current_ts public.timesheets%rowtype;

  v_booking_id text := null;

  v_timesheet_ids uuid[] := array[]::uuid[];
  v_contract_week_ids uuid[] := array[]::uuid[];
  v_contract_week_id uuid := null;

  v_blocked jsonb := '[]'::jsonb;

  v_has_tsfin_lock boolean := false;
  v_has_seg_locks boolean := false;
  v_has_invoice_lines boolean := false;
  v_has_pay_state boolean := false;

  v_invoice_info jsonb := '[]'::jsonb;

  v_delete_items jsonb := '[]'::jsonb;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_weekly_manual_adjustment_delete_preview: timesheet_id is required';
  end if;

  select t.*
  into v_in_ts
  from public.timesheets t
  where t.timesheet_id = p_timesheet_id;

  if not found then
    raise exception 'timesheet_weekly_manual_adjustment_delete_preview: timesheet % not found', p_timesheet_id;
  end if;

  v_booking_id := v_in_ts.booking_id;

  -- Resolve "current" by booking_id (robust even if is_current flags drift)
  select tcur.*
  into v_current_ts
  from public.timesheets tcur
  where tcur.booking_id = v_booking_id
  order by tcur.is_current desc, tcur.version desc
  limit 1;

  if not found then
    raise exception 'timesheet_weekly_manual_adjustment_delete_preview: booking_id % not found', v_booking_id;
  end if;

  -- Must be WEEKLY + adjustment
  if v_current_ts.sheet_scope <> 'WEEKLY'::public.timesheet_scope_enum then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','NOT_WEEKLY',
      'message','Manual adjustment delete applies only to WEEKLY adjustment timesheets.',
      'timesheet_id', v_current_ts.timesheet_id::text
    ));
  end if;

  if v_current_ts.is_adjustment is not true then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','NOT_ADJUSTMENT',
      'message','Timesheet is not an adjustment; this preview is only for WEEKLY manual adjustment deletes.',
      'timesheet_id', v_current_ts.timesheet_id::text
    ));
  end if;

  -- Must NOT be import-derived (manual adjustment must never delete parent or chain)
  if (
    upper(coalesce(v_current_ts.adjustment_origin,'')) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
    or nullif(btrim(coalesce(v_current_ts.correction_kind,'')),'') is not null
    or nullif(btrim(coalesce(v_current_ts.correction_id,'')),'') is not null
  ) then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','IMPORT_DERIVED_CHILD',
      'message','Import-derived adjustments cannot be deleted directly; they must be deleted via the WEEKLY parent-chain delete.',
      'timesheet_id', v_current_ts.timesheet_id::text,
      'adjustment_origin', v_current_ts.adjustment_origin,
      'correction_id', v_current_ts.correction_id,
      'correction_kind', v_current_ts.correction_kind
    ));
  end if;

  -- Expand to all versions for this single booking_id (manual adjustment only)
  select coalesce(array_agg(t_all.timesheet_id), array[]::uuid[])
  into v_timesheet_ids
  from public.timesheets t_all
  where t_all.booking_id = v_current_ts.booking_id;

  -- Find linked adjustment contract_week row (may point at any version in the booking series)
  select coalesce(array_agg(distinct cw.id), array[]::uuid[])
  into v_contract_week_ids
  from public.contract_weeks cw
  where cw.timesheet_id = any(v_timesheet_ids)
    and cw.is_adjustment is true
    and cw.additional_seq > 0;

  if coalesce(array_length(v_contract_week_ids,1),0) = 0 then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','MISSING_CONTRACT_WEEK',
      'message','No linked adjustment contract_week found for this weekly manual adjustment; refusing delete to avoid orphan/corruption.',
      'timesheet_id', v_current_ts.timesheet_id::text
    ));
  end if;

  -- Choose one contract_week_id for convenience (still return full list)
  if coalesce(array_length(v_contract_week_ids,1),0) > 0 then
    v_contract_week_id := v_contract_week_ids[1];
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Safety gates (apply to all versions in this single booking_id)
  -- ─────────────────────────────────────────────────────────────

  select exists(
    select 1
    from public.timesheets_financials tf
    where tf.timesheet_id = any(v_timesheet_ids)
      and tf.is_current is true
      and (tf.locked_by_invoice_id is not null or tf.paid_at_utc is not null)
  )
  into v_has_tsfin_lock;

  if v_has_tsfin_lock then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','TSFIN_LOCK_OR_PAID',
      'message','Timesheet has TSFIN locked_by_invoice_id and/or paid_at_utc set; cannot delete.'
    ));
  end if;

  select exists(
    select 1
    from public.timesheets_financials tf2
    cross join lateral jsonb_array_elements(
      case
        when upper(coalesce(tf2.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
             and jsonb_typeof(tf2.invoice_breakdown_json->'segments') = 'array'
          then tf2.invoice_breakdown_json->'segments'
        else '[]'::jsonb
      end
    ) seg(seg_elem)
    where tf2.timesheet_id = any(v_timesheet_ids)
      and tf2.is_current is true
      and nullif(btrim(coalesce(seg.seg_elem->>'invoice_locked_invoice_id','')),'') is not null
  )
  into v_has_seg_locks;

  if v_has_seg_locks then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','SEGMENT_LOCKED',
      'message','Timesheet has segment-level invoice locks (invoice_locked_invoice_id) in TSFIN.'
    ));
  end if;

  select exists(
    select 1
    from public.invoice_lines il
    where (il.timesheet_id is not null and il.timesheet_id = any(v_timesheet_ids))
       or (il.booking_id is not null and il.booking_id = v_current_ts.booking_id)
  )
  into v_has_invoice_lines;

  if v_has_invoice_lines then
    select coalesce(
      jsonb_agg(distinct jsonb_build_object(
        'invoice_id', inv.id::text,
        'status', inv.status::text
      )),
      '[]'::jsonb
    )
    into v_invoice_info
    from public.invoice_lines il2
    join public.invoices inv
      on inv.id = il2.invoice_id
    where (il2.timesheet_id is not null and il2.timesheet_id = any(v_timesheet_ids))
       or (il2.booking_id is not null and il2.booking_id = v_current_ts.booking_id);

    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','INVOICE_LINES_PRESENT',
      'message','One or more invoice_lines reference this adjustment timesheet; cannot delete.',
      'invoices', v_invoice_info
    ));
  end if;

  select (
    exists(
      select 1
      from public.timesheet_pay_state tps
      where tps.timesheet_id = any(v_timesheet_ids)
    )
    or exists(
      select 1
      from public.timesheet_pay_state_history tpsh
      where tpsh.timesheet_id = any(v_timesheet_ids)
    )
  )
  into v_has_pay_state;

  if v_has_pay_state then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','PAY_STATE_PRESENT',
      'message','Timesheet has pay settlement state/history; cannot delete.'
    ));
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- delete_items[] for warning modal (exactly one display row: this manual adjustment)
  -- ─────────────────────────────────────────────────────────────
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'timesheet_id', v_current_ts.timesheet_id::text,
        'booking_id', v_current_ts.booking_id,
        'week_ending_date', v_current_ts.week_ending_date::text,
        'status', v_current_ts.status::text,
        'is_adjustment', v_current_ts.is_adjustment,
        'adjustment_origin', v_current_ts.adjustment_origin,
        'correction_id', v_current_ts.correction_id,
        'correction_kind', v_current_ts.correction_kind,
        'display_role', 'MANUAL_ADJUSTMENT',
        'total_hours', coalesce(tfsel.total_hours, 0),
        'total_pay_ex_vat', coalesce(tfsel.total_pay_ex_vat, 0),
        'total_charge_ex_vat', coalesce(tfsel.total_charge_ex_vat, 0)
      )
    ),
    '[]'::jsonb
  )
  into v_delete_items
  from public.timesheets_financials tfsel
  where tfsel.timesheet_id = v_current_ts.timesheet_id
    and tfsel.is_current is true;

  -- If there is no TSFIN row for any reason, still return a single item with zeros
  if jsonb_array_length(v_delete_items) = 0 then
    v_delete_items := jsonb_build_array(jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id::text,
      'booking_id', v_current_ts.booking_id,
      'week_ending_date', v_current_ts.week_ending_date::text,
      'status', v_current_ts.status::text,
      'is_adjustment', v_current_ts.is_adjustment,
      'adjustment_origin', v_current_ts.adjustment_origin,
      'correction_id', v_current_ts.correction_id,
      'correction_kind', v_current_ts.correction_kind,
      'display_role', 'MANUAL_ADJUSTMENT',
      'total_hours', 0,
      'total_pay_ex_vat', 0,
      'total_charge_ex_vat', 0
    ));
  end if;

  return jsonb_build_object(
    'kind', 'WEEKLY_MANUAL_ADJUSTMENT_DELETE',
    'requested_timesheet_id', p_timesheet_id::text,
    'current_timesheet_id', v_current_ts.timesheet_id::text,
    'booking_id', v_current_ts.booking_id,
    'contract_week_id', case when v_contract_week_id is null then null else v_contract_week_id::text end,
    'contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[])),
    'timesheet_ids', to_jsonb(coalesce(v_timesheet_ids, array[]::uuid[])),
    'delete_items', v_delete_items,
    'eligible', (jsonb_array_length(v_blocked) = 0),
    'blocked_reasons', v_blocked
  );
end;
$function$;

