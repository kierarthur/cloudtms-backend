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

  if private.timesheet_correction_cancel_capability_v1(v_current_ts.timesheet_id,p_actor_user_id) is null and (
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
