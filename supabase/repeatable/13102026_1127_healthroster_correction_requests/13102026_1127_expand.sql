create or replace function public._ctms_expand_lifecycle_items_v1(
  p_items jsonb,
  p_action text,
  p_actor_user_id uuid,
  p_max_members integer default 100
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_action text := upper(btrim(coalesce(p_action, '')));
  v_result jsonb := '[]'::jsonb;
  v_seen_correction_ids text[] := array[]::text[];
  v_item jsonb;
  v_id_text text;
  v_id uuid;
  v_class jsonb;
  v_transition jsonb;
  v_preview jsonb;
  v_group jsonb;
  v_transition_item jsonb;
  v_expected_pair_fingerprint text;
  v_correction_id text;
begin
  if v_action not in ('AUTHORISE', 'UNAUTHORISE') then
    raise exception 'CORRECTION_LIFECYCLE_ACTION_INVALID' using errcode = '22023';
  end if;
  if p_actor_user_id is null then
    raise exception 'CORRECTION_LIFECYCLE_ACTOR_REQUIRED' using errcode = '22023';
  end if;
  if jsonb_typeof(coalesce(p_items, '[]'::jsonb)) <> 'array'
     or jsonb_array_length(coalesce(p_items, '[]'::jsonb)) > p_max_members then
    raise exception 'CORRECTION_LIFECYCLE_ITEMS_INVALID' using errcode = '22023';
  end if;

  for v_item in
    select value from jsonb_array_elements(coalesce(p_items, '[]'::jsonb))
  loop
    v_id_text := nullif(btrim(coalesce(
      v_item ->> 'timesheet_id', v_item ->> 'timesheetId',
      v_item ->> 'current_timesheet_id', v_item ->> 'currentTimesheetId',
      v_item ->> 'requested_timesheet_id', v_item ->> 'requestedTimesheetId',
      case when coalesce(v_item ->> 'row_key', '') like 'timesheet:%'
        then substring(v_item ->> 'row_key' from 11) end,
      ''
    )), '');

    if v_id_text is null
       or v_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      v_result := v_result || jsonb_build_array(v_item);
      continue;
    end if;

    v_id := v_id_text::uuid;
    if exists(select 1 from private.timesheet_correction_requests request
      where request.state='APPLIED' and request.reversed_timesheet_id=v_id) then
      if v_action='AUTHORISE' and exists(select 1 from private.timesheet_correction_requests request
        where request.state='APPLIED' and request.reversed_timesheet_id=v_id
          and coalesce((request.terminal_receipt_json->>'original_authorisation_required')::boolean,false)
          and exists(select 1 from public.timesheets t where t.timesheet_id=v_id and t.authorised_at_server is null)
          and (request.replacement_timesheet_id=any(public._ctms_payload_timesheet_ids_v1(p_items,p_max_members))
            or request.reversal_timesheet_id=any(public._ctms_payload_timesheet_ids_v1(p_items,p_max_members)))) then
        -- The adjacent WHOLE arm adds this A0 member exactly once. T0-only
        -- ordinary Authorise remains available while the request is WAITING.
        continue;
      end if;
      raise exception 'AGENCY_INVOICE_SUPERSEDED' using errcode='55000';
    end if;
    v_class := public._ctms_import_correction_classify_v1(v_id);
    if exists(select 1 from private.timesheet_correction_requests request
      where v_id in(request.replacement_timesheet_id,request.reversal_timesheet_id)
        and request.correction_id=(select t.correction_id from public.timesheets t where t.timesheet_id=v_id)) then
      select jsonb_build_object('is_import_authoritative_correction',true,'correction_id',request.correction_id)
        into v_class from private.timesheet_correction_requests request
        where v_id in(request.replacement_timesheet_id,request.reversal_timesheet_id);
    end if;
    if coalesce((v_class ->> 'is_import_authoritative_correction')::boolean, false) is not true then
      v_result := v_result || jsonb_build_array(v_item);
      continue;
    end if;

    v_correction_id := nullif(v_class ->> 'correction_id', '');
    if v_correction_id = any(v_seen_correction_ids) then
      continue;
    end if;

    v_preview:=public.timesheet_correction_pair_lifecycle_preview_v1(
      jsonb_build_array(v_item),v_action,p_actor_user_id,p_max_members);
    v_group:=v_preview#>'{groups,0}';
    v_transition:=public.timesheet_correction_pair_transition_v1(
      v_id,v_action,p_actor_user_id,null::uuid,v_group->>'chain_fingerprint',true,p_max_members);
    v_expected_pair_fingerprint:=nullif(btrim(coalesce(
      v_item->>'expected_pair_fingerprint',v_item->>'pair_fingerprint','')),'');
    if v_expected_pair_fingerprint is not null
       and v_expected_pair_fingerprint is distinct from v_group->>'pair_fingerprint' then
      raise exception 'CORRECTION_UNIT_LIFECYCLE_PREVIEW_STALE'
        using errcode='40001',detail=v_group::text;
    end if;
    if coalesce((v_group->>'valid')::boolean,false) is not true
       or coalesce((v_group->>'action_ready')::boolean,false) is not true then
      raise exception 'CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED'
        using errcode = 'P0001', detail = v_group::text;
    end if;

    v_seen_correction_ids := array_append(v_seen_correction_ids, v_correction_id);
    for v_transition_item in
      select value from jsonb_array_elements(coalesce(v_transition->'transition_items','[]'::jsonb))
    loop
      v_result:=v_result||jsonb_build_array(v_transition_item||jsonb_build_object(
        'lifecycle_group_id','correction:'||v_correction_id,
        'lifecycle_group_size',coalesce((v_transition->>'expected_member_count')::integer,2),
        'pair_fingerprint',v_group->>'pair_fingerprint',
        'selected_timesheet_id',v_id,
        'repairing_legacy_mixed_state',coalesce((v_group->>'repairing_legacy_mixed_state')::boolean,false)
      ));
    end loop;
  end loop;

  if jsonb_array_length(v_result) > p_max_members then
    raise exception 'CORRECTION_LIFECYCLE_EXPANSION_TOO_LARGE' using errcode = '22023';
  end if;
  return v_result;
end;
$function$;
