create or replace function private.timesheet_correction_replacement_empty_v1(p_request_id uuid)
returns boolean language sql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
  select coalesce((select jsonb_typeof(t.actual_schedule_json)='array' and jsonb_array_length(t.actual_schedule_json)=0
    and tf.total_hours=0 and tf.total_pay_ex_vat=0 and tf.total_charge_ex_vat=0
    and tf.additional_pay_ex_vat=0 and tf.additional_charge_ex_vat=0
    and tf.expenses_pay_ex_vat=0 and tf.expenses_charge_ex_vat=0
    and tf.mileage_pay_ex_vat=0 and tf.mileage_charge_ex_vat=0
  from private.timesheet_correction_requests request join public.timesheets t on t.timesheet_id=request.replacement_timesheet_id
    join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
  where request.id=p_request_id),false);
$function$;

-- A return hold is a fact of this same request and actual placement. It does
-- not require another queue/table or mutation of a terminal request receipt.
create or replace function private.timesheet_correction_draft_holds_v1(p_invoice_id uuid)
returns jsonb language sql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
  select coalesce(jsonb_agg(jsonb_build_object('request_id',request.id,'replacement_timesheet_id',request.replacement_timesheet_id,
    'return_invoice_id',request.return_invoice_id,'blocker_code','AGENCY_INVOICE_TIMESHEET_RETURNING') order by request.id),'[]'::jsonb)
  from private.timesheet_correction_requests request
  where request.return_invoice_id=p_invoice_id and request.state='APPLIED'
    and not private.timesheet_correction_replacement_empty_v1(request.id)
    and not coalesce((private.timesheet_correction_invoice_scope_v1(request.replacement_timesheet_id,p_invoice_id,null,false,100)
      ->>'placement_complete')::boolean,false);
$function$;

create or replace function private.timesheet_correction_draft_round_trip_v1(p_request_id uuid,p_action text,p_actor_user_id uuid)
returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare
  v_request private.timesheet_correction_requests%rowtype; v_invoice public.invoices%rowtype; v_action text:=upper(btrim(p_action));
  v_unrelated jsonb; v_lines jsonb; v_result jsonb; v_scope jsonb; v_fin public.timesheets_financials%rowtype;
begin
  perform private.timesheet_correction_actor_assert_v1(p_actor_user_id);
  select request.* into strict v_request from private.timesheet_correction_requests request where request.id=p_request_id;
  if v_request.return_invoice_id is null then return jsonb_build_object('ok',true,'not_required',true); end if;
  select i.* into strict v_invoice from public.invoices i where i.id=v_request.return_invoice_id for update;
  select request.* into strict v_request from private.timesheet_correction_requests request where request.id=p_request_id for update;
  if v_action not in('REMOVE','RETURN') or v_invoice.type<>'INVOICE' or v_invoice.status not in('DRAFT','ON_HOLD')
    or v_invoice.paid_at_utc is not null or v_invoice.client_id is distinct from v_request.client_id then
    raise exception 'AGENCY_ROUND_TRIP_INVOICE_NOT_DRAFT' using errcode='55000';
  end if;
  select coalesce(jsonb_agg(to_jsonb(l) order by l.id),'[]'::jsonb) into v_unrelated from public.invoice_lines l
    where l.invoice_id=v_invoice.id and l.timesheet_id is distinct from v_request.reversed_timesheet_id
      and l.timesheet_id is distinct from v_request.replacement_timesheet_id;
  if v_action='REMOVE' then
    if v_request.state<>'WAITING' or jsonb_array_length(private.timesheet_correction_draft_holds_v1(v_invoice.id))>0 then
      raise exception 'AGENCY_ROUND_TRIP_RETURN_HOLD_EXISTS' using errcode='55000';
    end if;
    if exists(select 1 from public.invoice_lines l where l.timesheet_id=v_request.reversed_timesheet_id and l.invoice_id<>v_invoice.id) then
      raise exception 'AGENCY_ROUND_TRIP_TARGET_MISMATCH' using errcode='55000';
    end if;
    select coalesce(jsonb_agg(l.id order by l.id),'[]'::jsonb) into v_lines from public.invoice_lines l
      where l.invoice_id=v_invoice.id and l.timesheet_id=v_request.reversed_timesheet_id;
    if jsonb_array_length(v_lines)=0 then raise exception 'AGENCY_ROUND_TRIP_ORIGINAL_PLACEMENT_CHANGED' using errcode='40001'; end if;
    v_result:=public.invoice_apply_edits(v_invoice.id,jsonb_build_object('remove_invoice_line_ids',v_lines),p_actor_user_id);
    if exists(select 1 from public.invoice_lines l where l.timesheet_id=v_request.reversed_timesheet_id) then
      raise exception 'AGENCY_ROUND_TRIP_REMOVE_NOT_PROVED' using errcode='55000';
    end if;
  else
    if v_request.state<>'APPLIED' then raise exception 'AGENCY_ROUND_TRIP_REQUEST_NOT_APPLIED' using errcode='55000'; end if;
    if exists(select 1 from public.invoice_lines l where l.timesheet_id in(v_request.reversed_timesheet_id,v_request.reversal_timesheet_id))
      or exists(select 1 from public.invoice_lines l where l.timesheet_id=v_request.replacement_timesheet_id and l.invoice_id<>v_invoice.id) then
      raise exception 'AGENCY_ROUND_TRIP_TARGET_MISMATCH' using errcode='55000';
    end if;
    if private.timesheet_correction_replacement_empty_v1(v_request.id) then
      if exists(select 1 from public.invoice_lines l where l.timesheet_id=v_request.replacement_timesheet_id) then
        raise exception 'AGENCY_ROUND_TRIP_EMPTY_REPLACEMENT_HAS_LINE' using errcode='55000';
      end if;
      return jsonb_build_object('ok',true,'return_invoice_id',v_invoice.id,'empty_replacement',true,'return_pending',false);
    end if;
    select tf.* into strict v_fin from public.timesheets_financials tf
      where tf.timesheet_id=v_request.replacement_timesheet_id and tf.is_current for update;
    if v_fin.authorised_at_utc is null or v_fin.is_stale or v_fin.processing_status<>'READY_FOR_INVOICE' then
      return jsonb_build_object('ok',true,'return_invoice_id',v_invoice.id,'return_pending',true,
        'blocker_code','AGENCY_INVOICE_TIMESHEET_RETURNING');
    end if;
    if exists(select 1 from public.invoice_lines l where l.invoice_id=v_invoice.id and l.timesheet_id=v_request.replacement_timesheet_id) then
      v_scope:=private.timesheet_correction_invoice_scope_v1(v_request.replacement_timesheet_id,v_invoice.id,p_actor_user_id,false,100);
      if (v_scope->>'valid')::boolean is distinct from true or (v_scope->>'placement_complete')::boolean is distinct from true then
        raise exception 'AGENCY_ROUND_TRIP_RETURN_COMPONENTS_UNPROVED' using errcode='55000';
      end if;
      return jsonb_build_object('ok',true,'return_invoice_id',v_invoice.id,'return_pending',false,'replayed',true);
    end if;
    v_result:=public.invoice_apply_edits(v_invoice.id,jsonb_build_object('add_timesheet_ids',jsonb_build_array(v_request.replacement_timesheet_id)),p_actor_user_id);
    if not exists(select 1 from public.invoice_lines l where l.invoice_id=v_invoice.id and l.timesheet_id=v_request.replacement_timesheet_id)
      or v_fin.locked_by_invoice_id is not null and v_fin.locked_by_invoice_id<>v_invoice.id then
      raise exception 'AGENCY_ROUND_TRIP_RETURN_NOT_PROVED' using errcode='55000';
    end if;
    v_scope:=private.timesheet_correction_invoice_scope_v1(v_request.replacement_timesheet_id,v_invoice.id,p_actor_user_id,false,100);
    if (v_scope->>'valid')::boolean is distinct from true or (v_scope->>'placement_complete')::boolean is distinct from true then
      raise exception 'AGENCY_ROUND_TRIP_RETURN_COMPONENTS_UNPROVED' using errcode='55000';
    end if;
  end if;
  if v_result->>'ok'='false' or v_result->>'success'='false'
    or (select i.invoice_no from public.invoices i where i.id=v_invoice.id) is distinct from v_invoice.invoice_no
    or (select coalesce(jsonb_agg(to_jsonb(l) order by l.id),'[]'::jsonb) from public.invoice_lines l
      where l.invoice_id=v_invoice.id and l.timesheet_id is distinct from v_request.reversed_timesheet_id
        and l.timesheet_id is distinct from v_request.replacement_timesheet_id) is distinct from v_unrelated then
    raise exception 'AGENCY_ROUND_TRIP_UNRELATED_PLACEMENT_CHANGED' using errcode='55000';
  end if;
  return jsonb_build_object('ok',true,'request_id',v_request.id,'action',v_action,'return_invoice_id',v_invoice.id,
    'invoice_no',v_invoice.invoice_no,'return_pending',v_action='REMOVE');
end;
$function$;

create or replace function public.timesheet_correction_return_to_draft_v1(p_request_id uuid,p_actor_user_id uuid)
returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
begin
  return private.timesheet_correction_draft_round_trip_v1(p_request_id,'RETURN',p_actor_user_id);
end;
$function$;
