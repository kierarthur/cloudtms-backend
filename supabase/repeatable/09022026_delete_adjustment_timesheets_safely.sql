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
  v_invalid_parent_target boolean := false;
  v_manual_contract_week_targeted boolean := false;

  v_before jsonb := null;
  v_after jsonb := null;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_weekly_chain_delete_apply: timesheet_id is required';
  end if;

  v_preview := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);

  if coalesce(v_preview->>'kind', '') <> 'WEEKLY_CHAIN_DELETE_PARENT' then
    raise exception 'timesheet_weekly_chain_delete_apply: unexpected preview kind: %', coalesce(v_preview->>'kind', '');
  end if;

  v_eligible := coalesce((v_preview->>'eligible')::boolean, false);

  if not v_eligible then
    raise exception 'timesheet_weekly_chain_delete_apply: not eligible: %', v_preview::text;
  end if;

  v_current_timesheet_id := nullif(v_preview->>'current_timesheet_id','')::uuid;
  v_contract_id := nullif(v_preview->>'contract_id','')::uuid;
  v_week_ending_date := (v_preview->>'week_ending_date')::date;

  select coalesce(array_agg(timesheet_id_values.timesheet_id_value::uuid), array[]::uuid[])
  into v_timesheet_ids
  from jsonb_array_elements_text(coalesce(v_preview->'timesheet_ids','[]'::jsonb)) as timesheet_id_values(timesheet_id_value);

  select coalesce(array_agg(contract_week_id_values.contract_week_id_value::uuid), array[]::uuid[])
  into v_contract_week_ids
  from jsonb_array_elements_text(coalesce(v_preview->'contract_week_ids','[]'::jsonb)) as contract_week_id_values(contract_week_id_value);

  select exists(
    select 1
    from public.timesheets as current_timesheet_check
    left join public.contract_weeks as current_contract_week_check
      on current_contract_week_check.timesheet_id = current_timesheet_check.timesheet_id
    where current_timesheet_check.timesheet_id = v_current_timesheet_id
      and (
        current_timesheet_check.is_adjustment is true
        or coalesce(current_contract_week_check.is_adjustment, false) = true
        or coalesce(current_contract_week_check.additional_seq, 0) > 0
      )
  )
  into v_invalid_parent_target;

  if coalesce(v_invalid_parent_target, false) = true then
    raise exception 'timesheet_weekly_chain_delete_apply: refusing parent-chain delete for an adjustment/additional timesheet %', v_current_timesheet_id;
  end if;

  select exists(
    select 1
    from public.contract_weeks as guarded_contract_week
    left join public.timesheets as guarded_timesheet
      on guarded_timesheet.timesheet_id = guarded_contract_week.timesheet_id
    where guarded_contract_week.id = any(v_contract_week_ids)
      and (coalesce(guarded_contract_week.is_adjustment, false) = true or coalesce(guarded_contract_week.additional_seq, 0) > 0)
      and not (
        upper(coalesce(guarded_timesheet.adjustment_origin,'')) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
        or nullif(btrim(coalesce(guarded_timesheet.correction_kind,'')),'') is not null
        or guarded_timesheet.correction_id is not null
      )
  )
  into v_manual_contract_week_targeted;

  if coalesce(v_manual_contract_week_targeted, false) = true then
    raise exception 'timesheet_weekly_chain_delete_apply: refusing parent-chain delete because the target set contains a manual additional contract_week: %', to_jsonb(v_contract_week_ids)::text;
  end if;

  v_before := jsonb_build_object(
    'requested_timesheet_id', p_timesheet_id::text,
    'current_timesheet_id', v_current_timesheet_id::text,
    'contract_id', case when v_contract_id is null then null else v_contract_id::text end,
    'week_ending_date', case when v_week_ending_date is null then null else v_week_ending_date::text end,
    'timesheet_ids', to_jsonb(coalesce(v_timesheet_ids, array[]::uuid[])),
    'contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[]))
  );

  -- NHSP/HR shifts: delete week truth rows only for verified parent-chain deletes.
  if v_contract_id is not null and v_week_ending_date is not null then
    delete from public.nhsp_shifts as nhsp_shift_delete
    where (nhsp_shift_delete.contract_id = v_contract_id
           and nhsp_shift_delete.week_ending_date = v_week_ending_date
           and nhsp_shift_delete.source_system in ('NHSP'::public.hr_source_enum, 'HEALTHROSTER'::public.hr_source_enum))
       or (nhsp_shift_delete.timesheet_id = any(v_timesheet_ids));
  else
    update public.nhsp_shifts as nhsp_shift_detach
    set timesheet_id = null
    where nhsp_shift_detach.timesheet_id = any(v_timesheet_ids);
  end if;

  get diagnostics v_deleted_shifts = row_count;

  delete from public.pay_item_snoozes as pay_item_snooze_delete where pay_item_snooze_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.pay_batch_items as pay_batch_item_delete where pay_batch_item_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.ts_pay_adjustments as ts_pay_adjustment_delete where ts_pay_adjustment_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.timesheet_pay_state_history as timesheet_pay_state_history_delete where timesheet_pay_state_history_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.timesheet_pay_state as timesheet_pay_state_delete where timesheet_pay_state_delete.timesheet_id = any(v_timesheet_ids);

  delete from public.timesheet_validations as timesheet_validation_delete where timesheet_validation_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.hr_results as hr_result_delete where hr_result_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.hr_issue_emails as hr_issue_email_delete where hr_issue_email_delete.timesheet_id = any(v_timesheet_ids);

  delete from public.timesheet_evidence as timesheet_evidence_delete where timesheet_evidence_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.manual_timesheet_queue as manual_timesheet_queue_delete where manual_timesheet_queue_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.ts_pdfs_outbox as ts_pdfs_outbox_delete where ts_pdfs_outbox_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.ts_financials_outbox as ts_financials_outbox_delete where ts_financials_outbox_delete.timesheet_id = any(v_timesheet_ids);

  delete from public.timesheets_financials as timesheet_financials_delete where timesheet_financials_delete.timesheet_id = any(v_timesheet_ids);

  delete from public.contract_weeks as contract_week_delete
  where contract_week_delete.id = any(v_contract_week_ids);

  get diagnostics v_deleted_contract_weeks = row_count;

  delete from public.timesheets as timesheet_delete where timesheet_delete.timesheet_id = any(v_timesheet_ids);

  get diagnostics v_deleted_timesheets = row_count;

  v_after := jsonb_build_object(
    'deleted_timesheets', v_deleted_timesheets,
    'deleted_contract_weeks', v_deleted_contract_weeks,
    'deleted_nhsp_shifts', v_deleted_shifts,
    'deleted_timesheet_ids', to_jsonb(coalesce(v_timesheet_ids, array[]::uuid[])),
    'deleted_contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[]))
  );

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
    'deleted_nhsp_shifts', v_deleted_shifts,
    'deleted_timesheet_ids', to_jsonb(coalesce(v_timesheet_ids, array[]::uuid[])),
    'deleted_contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[]))
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
  v_contract_week_ids uuid[] := array[]::uuid[];
  v_preserved_source_timesheet_ids uuid[] := array[]::uuid[];
  v_preserved_source_contract_week_ids uuid[] := array[]::uuid[];

  v_deleted_timesheets int := 0;
  v_deleted_contract_weeks int := 0;

  v_invalid_contract_week_targets boolean := false;
  v_missing_preserved_source_timesheets boolean := false;
  v_missing_preserved_source_contract_weeks boolean := false;

  v_before jsonb := null;
  v_after jsonb := null;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_weekly_manual_adjustment_delete_apply: timesheet_id is required';
  end if;

  v_preview := public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id, p_actor_user_id);

  if coalesce(v_preview->>'kind', '') <> 'WEEKLY_MANUAL_ADJUSTMENT_DELETE' then
    raise exception 'timesheet_weekly_manual_adjustment_delete_apply: unexpected preview kind: %', coalesce(v_preview->>'kind', '');
  end if;

  v_eligible := coalesce((v_preview->>'eligible')::boolean, false);

  if not v_eligible then
    raise exception 'timesheet_weekly_manual_adjustment_delete_apply: not eligible: %', v_preview::text;
  end if;

  v_current_timesheet_id := nullif(v_preview->>'current_timesheet_id','')::uuid;
  v_contract_week_id := nullif(v_preview->>'contract_week_id','')::uuid;

  select coalesce(array_agg(timesheet_id_values.timesheet_id_value::uuid order by timesheet_id_values.timesheet_id_value::uuid), array[]::uuid[])
  into v_timesheet_ids
  from jsonb_array_elements_text(coalesce(v_preview->'timesheet_ids','[]'::jsonb)) as timesheet_id_values(timesheet_id_value);

  select coalesce(array_agg(contract_week_id_values.contract_week_id_value::uuid order by contract_week_id_values.contract_week_id_value::uuid), array[]::uuid[])
  into v_contract_week_ids
  from jsonb_array_elements_text(coalesce(v_preview->'contract_week_ids','[]'::jsonb)) as contract_week_id_values(contract_week_id_value);

  select coalesce(array_agg(preserved_timesheet_id_values.preserved_timesheet_id_value::uuid order by preserved_timesheet_id_values.preserved_timesheet_id_value::uuid), array[]::uuid[])
  into v_preserved_source_timesheet_ids
  from jsonb_array_elements_text(coalesce(v_preview->'preserved_source_timesheet_ids','[]'::jsonb)) as preserved_timesheet_id_values(preserved_timesheet_id_value);

  select coalesce(array_agg(preserved_contract_week_id_values.preserved_contract_week_id_value::uuid order by preserved_contract_week_id_values.preserved_contract_week_id_value::uuid), array[]::uuid[])
  into v_preserved_source_contract_week_ids
  from jsonb_array_elements_text(coalesce(v_preview->'preserved_source_contract_week_ids','[]'::jsonb)) as preserved_contract_week_id_values(preserved_contract_week_id_value);

  if coalesce(array_length(v_timesheet_ids, 1), 0) = 0 then
    raise exception 'timesheet_weekly_manual_adjustment_delete_apply: preview returned no timesheet_ids';
  end if;

  if coalesce(array_length(v_contract_week_ids, 1), 0) = 0 then
    raise exception 'timesheet_weekly_manual_adjustment_delete_apply: preview returned no contract_week_ids';
  end if;

  select exists(
    select 1
    from unnest(coalesce(v_contract_week_ids, array[]::uuid[])) as requested_contract_week(contract_week_id)
    left join public.contract_weeks as contract_week_check
      on contract_week_check.id = requested_contract_week.contract_week_id
    where contract_week_check.id is null
       or coalesce(contract_week_check.is_adjustment, false) is not true
       or coalesce(contract_week_check.additional_seq, 0) <= 0
  )
  into v_invalid_contract_week_targets;

  if coalesce(v_invalid_contract_week_targets, false) = true then
    raise exception 'timesheet_weekly_manual_adjustment_delete_apply: refusing delete because target contract_week_ids are not all additional/manual adjustment rows: %', to_jsonb(v_contract_week_ids)::text;
  end if;

  v_before := jsonb_build_object(
    'requested_timesheet_id', p_timesheet_id::text,
    'current_timesheet_id', v_current_timesheet_id::text,
    'contract_week_id', v_contract_week_id::text,
    'timesheet_ids', to_jsonb(coalesce(v_timesheet_ids, array[]::uuid[])),
    'contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[])),
    'preserved_source_timesheet_ids', to_jsonb(coalesce(v_preserved_source_timesheet_ids, array[]::uuid[])),
    'preserved_source_contract_week_ids', to_jsonb(coalesce(v_preserved_source_contract_week_ids, array[]::uuid[]))
  );

  update public.nhsp_shifts as nhsp_shift_detach
  set timesheet_id = null
  where nhsp_shift_detach.timesheet_id = any(v_timesheet_ids);

  delete from public.pay_item_snoozes as pay_item_snooze_delete where pay_item_snooze_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.ts_pay_adjustments as ts_pay_adjustment_delete where ts_pay_adjustment_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.timesheet_pay_state_history as timesheet_pay_state_history_delete where timesheet_pay_state_history_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.timesheet_pay_state as timesheet_pay_state_delete where timesheet_pay_state_delete.timesheet_id = any(v_timesheet_ids);

  delete from public.timesheet_validations as timesheet_validation_delete where timesheet_validation_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.hr_results as hr_result_delete where hr_result_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.hr_issue_emails as hr_issue_email_delete where hr_issue_email_delete.timesheet_id = any(v_timesheet_ids);

  delete from public.timesheet_evidence as timesheet_evidence_delete where timesheet_evidence_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.manual_timesheet_queue as manual_timesheet_queue_delete
  where manual_timesheet_queue_delete.timesheet_id = any(v_timesheet_ids)
     or (
       manual_timesheet_queue_delete.meta_json ? 'contract_week_id'
       and manual_timesheet_queue_delete.meta_json->>'contract_week_id' in (
        select selected_contract_week.selected_contract_week_id::text
        from unnest(v_contract_week_ids) as selected_contract_week(selected_contract_week_id)
      )
     );
  delete from public.ts_pdfs_outbox as ts_pdfs_outbox_delete where ts_pdfs_outbox_delete.timesheet_id = any(v_timesheet_ids);
  delete from public.ts_financials_outbox as ts_financials_outbox_delete where ts_financials_outbox_delete.timesheet_id = any(v_timesheet_ids);

  delete from public.timesheets_financials as timesheet_financials_delete where timesheet_financials_delete.timesheet_id = any(v_timesheet_ids);

  delete from public.contract_weeks as contract_week_delete
  where contract_week_delete.id = any(v_contract_week_ids)
    and coalesce(contract_week_delete.is_adjustment, false) = true
    and coalesce(contract_week_delete.additional_seq, 0) > 0;

  get diagnostics v_deleted_contract_weeks = row_count;

  if v_deleted_contract_weeks <> coalesce(array_length(v_contract_week_ids, 1), 0) then
    raise exception 'timesheet_weekly_manual_adjustment_delete_apply: contract_week delete count mismatch; expected %, deleted %', coalesce(array_length(v_contract_week_ids, 1), 0), v_deleted_contract_weeks;
  end if;

  delete from public.timesheets as timesheet_delete
  where timesheet_delete.timesheet_id = any(v_timesheet_ids);

  get diagnostics v_deleted_timesheets = row_count;

  if v_deleted_timesheets <> coalesce(array_length(v_timesheet_ids, 1), 0) then
    raise exception 'timesheet_weekly_manual_adjustment_delete_apply: timesheet delete count mismatch; expected %, deleted %', coalesce(array_length(v_timesheet_ids, 1), 0), v_deleted_timesheets;
  end if;

  if coalesce(array_length(v_preserved_source_timesheet_ids, 1), 0) > 0 then
    select exists(
      select 1
      from unnest(v_preserved_source_timesheet_ids) as preserved_source_timesheet(preserved_timesheet_id)
      left join public.timesheets as source_timesheet_check
        on source_timesheet_check.timesheet_id = preserved_source_timesheet.preserved_timesheet_id
      where source_timesheet_check.timesheet_id is null
    )
    into v_missing_preserved_source_timesheets;

    if coalesce(v_missing_preserved_source_timesheets, false) = true then
      raise exception 'timesheet_weekly_manual_adjustment_delete_apply: preserved source timesheet missing after delete: %', to_jsonb(v_preserved_source_timesheet_ids)::text;
    end if;
  end if;

  if coalesce(array_length(v_preserved_source_contract_week_ids, 1), 0) > 0 then
    select exists(
      select 1
      from unnest(v_preserved_source_contract_week_ids) as preserved_source_contract_week(preserved_contract_week_id)
      left join public.contract_weeks as source_contract_week_check
        on source_contract_week_check.id = preserved_source_contract_week.preserved_contract_week_id
      where source_contract_week_check.id is null
         or coalesce(source_contract_week_check.is_adjustment, false) = true
         or coalesce(source_contract_week_check.additional_seq, 0) <> 0
    )
    into v_missing_preserved_source_contract_weeks;

    if coalesce(v_missing_preserved_source_contract_weeks, false) = true then
      raise exception 'timesheet_weekly_manual_adjustment_delete_apply: preserved source contract_week missing or mutated after delete: %', to_jsonb(v_preserved_source_contract_week_ids)::text;
    end if;
  end if;

  v_after := jsonb_build_object(
    'deleted_timesheets', v_deleted_timesheets,
    'deleted_contract_weeks', v_deleted_contract_weeks,
    'deleted_timesheet_ids', to_jsonb(coalesce(v_timesheet_ids, array[]::uuid[])),
    'deleted_contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[])),
    'preserved_source_timesheet_ids', to_jsonb(coalesce(v_preserved_source_timesheet_ids, array[]::uuid[])),
    'preserved_source_contract_week_ids', to_jsonb(coalesce(v_preserved_source_contract_week_ids, array[]::uuid[]))
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
    'deleted_contract_weeks', v_deleted_contract_weeks,
    'deleted_timesheet_ids', to_jsonb(coalesce(v_timesheet_ids, array[]::uuid[])),
    'deleted_contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[])),
    'preserved_source_timesheet_ids', to_jsonb(coalesce(v_preserved_source_timesheet_ids, array[]::uuid[])),
    'preserved_source_contract_week_ids', to_jsonb(coalesce(v_preserved_source_contract_week_ids, array[]::uuid[]))
  );
end;
$function$;



CREATE OR REPLACE FUNCTION public.timesheet_weekly_chain_delete_preview(p_timesheet_id uuid, p_actor_user_id uuid)
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
  v_current_contract_week_ids uuid[] := array[]::uuid[];
  v_nhsp_shift_ids uuid[] := array[]::uuid[];

  v_blocked jsonb := '[]'::jsonb;

  v_history jsonb := '{}'::jsonb;
  v_decision text := 'BLOCKED';
  v_current_effective_adjustment boolean := false;
  v_manual_contract_week_targeted boolean := false;

  v_invoice_info jsonb := '[]'::jsonb;

  v_delete_items jsonb := '[]'::jsonb;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_weekly_chain_delete_preview: timesheet_id is required';
  end if;

  if p_actor_user_id is null then
    raise exception using message = 'ACTOR_USER_ID_REQUIRED';
  end if;

  if not exists (
    select 1
    from public.tms_users as actor
    where actor.id = p_actor_user_id
      and actor.is_active = true
  ) then
    raise exception using message = 'ACTOR_NOT_FOUND_OR_INACTIVE';
  end if;

  select t.*
  into v_in_ts
  from public.timesheets as t
  where t.timesheet_id = p_timesheet_id;

  if not found then
    raise exception 'timesheet_weekly_chain_delete_preview: timesheet % not found', p_timesheet_id;
  end if;

  v_base_booking_id := v_in_ts.booking_id;

  -- Resolve "current" by booking_id (robust even if is_current flags drift)
  select tcur.*
  into v_current_ts
  from public.timesheets as tcur
  where tcur.booking_id = v_base_booking_id
  order by tcur.is_current desc,
           tcur.version desc nulls last,
           tcur.updated_at desc nulls last,
           tcur.created_at desc nulls last,
           tcur.timesheet_id desc
  limit 1;

  if not found then
    raise exception 'timesheet_weekly_chain_delete_preview: booking_id % not found', v_base_booking_id;
  end if;

  select
    coalesce(array_agg(distinct cw_current.id order by cw_current.id), array[]::uuid[]),
    coalesce(bool_or(coalesce(cw_current.is_adjustment, false) = true OR coalesce(cw_current.additional_seq, 0) > 0), false)
  into v_current_contract_week_ids,
       v_current_effective_adjustment
  from public.contract_weeks as cw_current
  where cw_current.timesheet_id = v_current_ts.timesheet_id;

  -- Must be WEEKLY parent (not adjustment and not linked to an additional/manual adjustment contract_week)
  if v_current_ts.sheet_scope <> 'WEEKLY'::public.timesheet_scope_enum then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','NOT_WEEKLY',
      'message','Parent-chain delete applies only to WEEKLY parent timesheets.',
      'timesheet_id', v_current_ts.timesheet_id::text
    ));
  end if;

  if v_current_ts.is_adjustment is true OR coalesce(v_current_effective_adjustment, false) = true then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','NOT_PARENT',
      'message','Timesheet is an adjustment/additional manual row; parent-chain delete can only be requested for a non-adjustment WEEKLY parent timesheet.',
      'timesheet_id', v_current_ts.timesheet_id::text,
      'contract_week_ids', to_jsonb(coalesce(v_current_contract_week_ids, array[]::uuid[]))
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
    select coalesce(array_agg(distinct scoped_booking.booking_id order by scoped_booking.booking_id), array[]::text[])
    into v_booking_ids
    from (
      select v_current_ts.booking_id as booking_id
      union all
      select import_adjustment_timesheet.booking_id
      from public.timesheets as import_adjustment_timesheet
      where import_adjustment_timesheet.contract_id = v_contract_id
        and import_adjustment_timesheet.week_ending_date = v_week_ending_date
        and import_adjustment_timesheet.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
        and import_adjustment_timesheet.is_adjustment is true
        and (
          upper(coalesce(import_adjustment_timesheet.adjustment_origin,'')) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
          or nullif(btrim(coalesce(import_adjustment_timesheet.correction_kind,'')),'') is not null
          or import_adjustment_timesheet.correction_id is not null
        )
    ) as scoped_booking;
  else
    v_booking_ids := array[v_current_ts.booking_id];
  end if;

  -- All timesheet IDs (all versions) for those booking IDs
  select coalesce(array_agg(all_versions.timesheet_id order by all_versions.timesheet_id), array[]::uuid[])
  into v_timesheet_ids
  from public.timesheets as all_versions
  where all_versions.booking_id = any(v_booking_ids);

  -- Contract-week IDs to delete for a true parent chain only:
  -- (a) base week row additional_seq=0 / non-adjustment
  -- (b) contract_week rows linked to timesheets in scope, but only when they are base rows or import-derived children
  -- Manual additional contract_weeks are deliberately excluded and guarded against.
  if v_contract_id is not null and v_week_ending_date is not null then
    select coalesce(array_agg(distinct scoped_contract_week.cw_id order by scoped_contract_week.cw_id), array[]::uuid[])
    into v_contract_week_ids
    from (
      select base_contract_week.id as cw_id
      from public.contract_weeks as base_contract_week
      where base_contract_week.contract_id = v_contract_id
        and base_contract_week.week_ending_date = v_week_ending_date
        and coalesce(base_contract_week.additional_seq, 0) = 0
        and coalesce(base_contract_week.is_adjustment, false) = false
      union all
      select linked_contract_week.id as cw_id
      from public.contract_weeks as linked_contract_week
      join public.timesheets as linked_timesheet
        on linked_timesheet.timesheet_id = linked_contract_week.timesheet_id
      where linked_contract_week.timesheet_id = any(v_timesheet_ids)
        and (
          (
            coalesce(linked_contract_week.is_adjustment, false) = false
            and coalesce(linked_contract_week.additional_seq, 0) = 0
          )
          or (
            (coalesce(linked_contract_week.is_adjustment, false) = true or coalesce(linked_contract_week.additional_seq, 0) > 0)
            and (
              upper(coalesce(linked_timesheet.adjustment_origin,'')) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
              or nullif(btrim(coalesce(linked_timesheet.correction_kind,'')),'') is not null
              or linked_timesheet.correction_id is not null
            )
          )
        )
    ) as scoped_contract_week;
  else
    v_contract_week_ids := array[]::uuid[];
  end if;

  select exists(
    select 1
    from public.contract_weeks as guarded_contract_week
    left join public.timesheets as guarded_timesheet
      on guarded_timesheet.timesheet_id = guarded_contract_week.timesheet_id
    where guarded_contract_week.id = any(v_contract_week_ids)
      and (coalesce(guarded_contract_week.is_adjustment, false) = true or coalesce(guarded_contract_week.additional_seq, 0) > 0)
      and not (
        upper(coalesce(guarded_timesheet.adjustment_origin,'')) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
        or nullif(btrim(coalesce(guarded_timesheet.correction_kind,'')),'') is not null
        or guarded_timesheet.correction_id is not null
      )
  )
  into v_manual_contract_week_targeted;

  if coalesce(v_manual_contract_week_targeted, false) = true then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','MANUAL_ADJUSTMENT_CONTRACT_WEEK_IN_CHAIN',
      'message','Parent-chain delete would target a manual additional contract_week. Use additional-only delete for manual adjustment rows.',
      'contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[]))
    ));
  end if;

  -- NHSP/HR truth rows in this contract/week (for informational preview)
  if v_contract_id is not null and v_week_ending_date is not null then
    select coalesce(array_agg(nhsp_shift.id order by nhsp_shift.id), array[]::uuid[])
    into v_nhsp_shift_ids
    from public.nhsp_shifts as nhsp_shift
    where nhsp_shift.contract_id = v_contract_id
      and nhsp_shift.week_ending_date = v_week_ending_date
      and nhsp_shift.source_system in ('NHSP'::public.hr_source_enum, 'HEALTHROSTER'::public.hr_source_enum);
  else
    v_nhsp_shift_ids := array[]::uuid[];
  end if;

  v_history := public.timesheet_removal_financial_history_v1(
    v_timesheet_ids,
    v_booking_ids,
    v_contract_week_ids
  );

  v_blocked := v_blocked || COALESCE(v_history -> 'blockers', '[]'::jsonb);

  IF jsonb_array_length(v_blocked) > 0 THEN
    v_decision := 'BLOCKED';
  ELSIF COALESCE((v_history ->> 'archive_required')::boolean, false) THEN
    v_decision := 'ARCHIVE_REQUIRED';
  ELSE
    v_decision := 'PERMANENT_DELETE';
  END IF;

  -- delete_items[] for warning modal (one display row per booking_id)
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'timesheet_id', selected_timesheet.timesheet_id::text,
        'booking_id', selected_timesheet.booking_id,
        'week_ending_date', selected_timesheet.week_ending_date::text,
        'status', selected_timesheet.status::text,
        'is_adjustment', selected_timesheet.is_adjustment,
        'adjustment_origin', selected_timesheet.adjustment_origin,
        'correction_id', selected_timesheet.correction_id,
        'correction_kind', selected_timesheet.correction_kind,
        'display_role',
          case
            when selected_timesheet.booking_id = v_current_ts.booking_id then 'PARENT'
            else 'ADJUSTMENT'
          end,
        'total_hours', coalesce(selected_tsfin.total_hours, 0),
        'total_pay_ex_vat', coalesce(selected_tsfin.total_pay_ex_vat, 0),
        'total_charge_ex_vat', coalesce(selected_tsfin.total_charge_ex_vat, 0)
      )
      ORDER BY
        (case when selected_timesheet.booking_id = v_current_ts.booking_id then 0 else 1 end),
        selected_timesheet.booking_id,
        selected_timesheet.timesheet_id,
        selected_tsfin.id
    ),
    '[]'::jsonb
  )
  into v_delete_items
  from (
    select distinct on (timesheet_pick.booking_id)
      timesheet_pick.booking_id,
      timesheet_pick.timesheet_id,
      timesheet_pick.week_ending_date,
      timesheet_pick.status,
      timesheet_pick.is_adjustment,
      timesheet_pick.adjustment_origin,
      timesheet_pick.correction_id,
      timesheet_pick.correction_kind
    from public.timesheets as timesheet_pick
    where timesheet_pick.booking_id = any(v_booking_ids)
    order by timesheet_pick.booking_id,
             timesheet_pick.is_current desc,
             timesheet_pick.version desc nulls last,
             timesheet_pick.updated_at desc nulls last,
             timesheet_pick.created_at desc nulls last,
             timesheet_pick.timesheet_id desc
  ) as selected_timesheet
  left join public.timesheets_financials as selected_tsfin
    on selected_tsfin.timesheet_id = selected_timesheet.timesheet_id
   and selected_tsfin.is_current is true;

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
    'decision', v_decision,
    'eligible', (v_decision = 'PERMANENT_DELETE'),
    'blocked_reasons', v_blocked,
    'blockers', v_blocked,
    'retention_reasons', COALESCE(v_history -> 'retention_reasons', '[]'::jsonb),
    'advance', COALESCE(v_history -> 'advance', '{}'::jsonb)
  );
end;
$function$;


CREATE OR REPLACE FUNCTION public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id uuid, p_actor_user_id uuid)
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
  v_preserved_source_contract_week_ids uuid[] := array[]::uuid[];
  v_preserved_source_timesheet_ids uuid[] := array[]::uuid[];
  v_effective_manual_adjustment boolean := false;
  v_linked_adjustment_contract_week_exists boolean := false;
  v_has_base_contract_week_target boolean := false;

  v_blocked jsonb := '[]'::jsonb;

  v_history jsonb := '{}'::jsonb;
  v_decision text := 'BLOCKED';

  v_delete_items jsonb := '[]'::jsonb;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_weekly_manual_adjustment_delete_preview: timesheet_id is required';
  end if;

  if p_actor_user_id is null then
    raise exception using message = 'ACTOR_USER_ID_REQUIRED';
  end if;

  if not exists (
    select 1
    from public.tms_users as actor
    where actor.id = p_actor_user_id
      and actor.is_active = true
  ) then
    raise exception using message = 'ACTOR_NOT_FOUND_OR_INACTIVE';
  end if;

  select input_timesheet.*
  into v_in_ts
  from public.timesheets as input_timesheet
  where input_timesheet.timesheet_id = p_timesheet_id;

  if not found then
    raise exception 'timesheet_weekly_manual_adjustment_delete_preview: timesheet % not found', p_timesheet_id;
  end if;

  v_booking_id := v_in_ts.booking_id;

  select current_timesheet.*
  into v_current_ts
  from public.timesheets as current_timesheet
  where current_timesheet.booking_id = v_booking_id
  order by current_timesheet.is_current desc, current_timesheet.version desc, current_timesheet.updated_at desc nulls last, current_timesheet.created_at desc nulls last, current_timesheet.timesheet_id desc
  limit 1;

  if not found then
    raise exception 'timesheet_weekly_manual_adjustment_delete_preview: booking_id % not found', v_booking_id;
  end if;

  select coalesce(array_agg(all_versions.timesheet_id order by all_versions.version asc nulls last, all_versions.created_at asc nulls last, all_versions.timesheet_id asc), array[]::uuid[])
  into v_timesheet_ids
  from public.timesheets as all_versions
  where all_versions.booking_id = v_current_ts.booking_id;

  select
    coalesce(bool_or(coalesce(linked_contract_week.is_adjustment, false) = true or coalesce(linked_contract_week.additional_seq, 0) > 0), false),
    coalesce(bool_or(coalesce(linked_contract_week.is_adjustment, false) = false or coalesce(linked_contract_week.additional_seq, 0) <= 0), false)
  into v_linked_adjustment_contract_week_exists,
       v_has_base_contract_week_target
  from public.contract_weeks as linked_contract_week
  where linked_contract_week.timesheet_id = any(v_timesheet_ids);

  select coalesce(array_agg(distinct adjustment_contract_week.id order by adjustment_contract_week.id), array[]::uuid[])
  into v_contract_week_ids
  from public.contract_weeks as adjustment_contract_week
  where adjustment_contract_week.timesheet_id = any(v_timesheet_ids)
    and coalesce(adjustment_contract_week.is_adjustment, false) = true
    and coalesce(adjustment_contract_week.additional_seq, 0) > 0;

  v_effective_manual_adjustment :=
    coalesce(v_current_ts.is_adjustment, false) = true
    or coalesce(v_linked_adjustment_contract_week_exists, false) = true
    or v_current_ts.parent_timesheet_id is not null
    or v_current_ts.correction_id is not null
    or nullif(btrim(coalesce(v_current_ts.correction_kind, '')), '') is not null
    or upper(coalesce(v_current_ts.adjustment_origin, '')) in ('MANUAL_ADJUSTMENT', 'IMPORT_CORRECTION', 'IMPORT_CANCELLATION');

  if coalesce(array_length(v_contract_week_ids, 1), 0) > 0 then
    v_contract_week_id := v_contract_week_ids[1];
  end if;

  select coalesce(array_agg(distinct source_contract_week.id order by source_contract_week.id), array[]::uuid[])
  into v_preserved_source_contract_week_ids
  from public.contract_weeks as adjustment_contract_week_for_source
  join public.contract_weeks as source_contract_week
    on source_contract_week.contract_id = adjustment_contract_week_for_source.contract_id
   and source_contract_week.week_ending_date = adjustment_contract_week_for_source.week_ending_date
   and coalesce(source_contract_week.additional_seq, 0) = 0
   and coalesce(source_contract_week.is_adjustment, false) = false
  where adjustment_contract_week_for_source.id = any(v_contract_week_ids);

  select coalesce(array_agg(distinct preserved_timesheet_id order by preserved_timesheet_id), array[]::uuid[])
  into v_preserved_source_timesheet_ids
  from (
    select source_contract_week_for_ts.timesheet_id as preserved_timesheet_id
    from public.contract_weeks as source_contract_week_for_ts
    where source_contract_week_for_ts.id = any(v_preserved_source_contract_week_ids)
      and source_contract_week_for_ts.timesheet_id is not null
    union
    select v_current_ts.parent_timesheet_id as preserved_timesheet_id
    where v_current_ts.parent_timesheet_id is not null
  ) as preserved_source_timesheet_ids
  where preserved_source_timesheet_ids.preserved_timesheet_id is not null;

  if v_current_ts.sheet_scope <> 'WEEKLY'::public.timesheet_scope_enum then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','NOT_WEEKLY',
      'message','Manual adjustment delete applies only to WEEKLY adjustment timesheets.',
      'timesheet_id', v_current_ts.timesheet_id::text
    ));
  end if;

  if coalesce(v_effective_manual_adjustment, false) is not true then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','NOT_ADJUSTMENT',
      'message','Timesheet is not an adjustment/additional manual row; this preview is only for WEEKLY manual adjustment deletes.',
      'timesheet_id', v_current_ts.timesheet_id::text,
      'contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[]))
    ));
  end if;

  if (
    upper(coalesce(v_current_ts.adjustment_origin,'')) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
    or nullif(btrim(coalesce(v_current_ts.correction_kind,'')),'') is not null
    or v_current_ts.correction_id is not null
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

  if coalesce(array_length(v_contract_week_ids,1),0) = 0 then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','MISSING_CONTRACT_WEEK',
      'message','No linked additional/manual adjustment contract_week found for this weekly manual adjustment; refusing delete to avoid orphan/corruption.',
      'timesheet_id', v_current_ts.timesheet_id::text
    ));
  end if;

  if coalesce(v_has_base_contract_week_target, false) = true then
    v_blocked := v_blocked || jsonb_build_array(jsonb_build_object(
      'code','BASE_CONTRACT_WEEK_TARGETED',
      'message','Delete preview resolved a base/source contract_week for a manual adjustment delete; refusing delete to protect the parent/source row.',
      'contract_week_ids', to_jsonb(coalesce(v_contract_week_ids, array[]::uuid[])),
      'preserved_source_contract_week_ids', to_jsonb(coalesce(v_preserved_source_contract_week_ids, array[]::uuid[]))
    ));
  end if;

  v_history := public.timesheet_removal_financial_history_v1(
    v_timesheet_ids,
    ARRAY[v_current_ts.booking_id],
    v_contract_week_ids
  );

  v_blocked := v_blocked || COALESCE(v_history -> 'blockers', '[]'::jsonb);

  IF jsonb_array_length(v_blocked) > 0 THEN
    v_decision := 'BLOCKED';
  ELSIF COALESCE((v_history ->> 'archive_required')::boolean, false) THEN
    v_decision := 'ARCHIVE_REQUIRED';
  ELSE
    v_decision := 'PERMANENT_DELETE';
  END IF;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'timesheet_id', v_current_ts.timesheet_id::text,
        'booking_id', v_current_ts.booking_id,
        'week_ending_date', v_current_ts.week_ending_date::text,
        'status', v_current_ts.status::text,
        'is_adjustment', coalesce(v_effective_manual_adjustment, false),
        'adjustment_origin', v_current_ts.adjustment_origin,
        'correction_id', v_current_ts.correction_id,
        'correction_kind', v_current_ts.correction_kind,
        'display_role', 'MANUAL_ADJUSTMENT',
        'total_hours', coalesce(selected_tsfin.total_hours, 0),
        'total_pay_ex_vat', coalesce(selected_tsfin.total_pay_ex_vat, 0),
        'total_charge_ex_vat', coalesce(selected_tsfin.total_charge_ex_vat, 0)
      )
      order by selected_tsfin.id
    ),
    '[]'::jsonb
  )
  into v_delete_items
  from public.timesheets_financials as selected_tsfin
  where selected_tsfin.timesheet_id = v_current_ts.timesheet_id
    and selected_tsfin.is_current is true;

  if jsonb_array_length(v_delete_items) = 0 then
    v_delete_items := jsonb_build_array(jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id::text,
      'booking_id', v_current_ts.booking_id,
      'week_ending_date', v_current_ts.week_ending_date::text,
      'status', v_current_ts.status::text,
      'is_adjustment', coalesce(v_effective_manual_adjustment, false),
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
    'preserved_source_timesheet_ids', to_jsonb(coalesce(v_preserved_source_timesheet_ids, array[]::uuid[])),
    'preserved_source_contract_week_ids', to_jsonb(coalesce(v_preserved_source_contract_week_ids, array[]::uuid[])),
    'delete_items', v_delete_items,
    'decision', v_decision,
    'eligible', (v_decision = 'PERMANENT_DELETE'),
    'blocked_reasons', v_blocked,
    'blockers', v_blocked,
    'retention_reasons', COALESCE(v_history -> 'retention_reasons', '[]'::jsonb),
    'advance', COALESCE(v_history -> 'advance', '{}'::jsonb)
  );
end;
$function$;

