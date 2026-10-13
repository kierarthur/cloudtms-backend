-- Exact additional authority for the existing batch validator. All native
-- financial/state/lock/VAT/member checks still run on the selected T0.
create or replace function private.timesheet_correction_scoped_source_identity_v1(p_scope jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_request private.timesheet_correction_requests%rowtype; v_context jsonb; v_operation uuid; v_planned uuid;
begin
  if upper(coalesce(p_scope->>'validation_purpose',''))<>'CREDIT_SOURCE'
    or not pg_input_is_valid(p_scope#>>'{credit_scope,reversal_timesheet_id}','uuid')
    or not pg_input_is_valid(p_scope->>'invoice_id','uuid')
    or not pg_input_is_valid(p_scope->>'request_id','uuid')
    or not pg_input_is_valid(p_scope->>'operation_id','uuid')
    or not pg_input_is_valid(p_scope->>'planned_credit_note_id','uuid') then return null; end if;
  select request.* into v_request from private.timesheet_correction_requests request
    where request.id=(p_scope->>'request_id')::uuid and request.state='APPLIED'
      and request.reversal_timesheet_id=(p_scope#>>'{credit_scope,reversal_timesheet_id}')::uuid
      and request.original_invoice_id=(p_scope->>'invoice_id')::uuid
      and request.target_revision=p_scope->>'target_revision';
  if not found then return null; end if;
  v_operation:=(p_scope->>'operation_id')::uuid; v_planned:=(p_scope->>'planned_credit_note_id')::uuid;
  if not exists(select 1 from public.invoice_operations operation where operation.id=v_operation
    and operation.operation_type='GENERATE_CREDIT_NOTE' and operation.entity_type='INVOICE'
    and operation.entity_id=v_request.original_invoice_id
    and exists(select 1 from public.tms_users actor where actor.id=operation.actor_user_id and actor.is_active and lower(actor.role)='admin')) then
    return null;
  end if;
  v_context:=private.invoice_credit_scope_context_v1(v_request.original_invoice_id,v_request.reversal_timesheet_id,v_operation,v_planned,false);
  if (v_context->>'valid')::boolean is distinct from true then return null; end if;
  perform private.timesheet_correction_applied_assert_v1(v_request.id);
  perform private.timesheet_correction_reversal_assert_v1(
    coalesce(nullif(v_request.frozen_original_json->'financial_snapshot','null'::jsonb),
      (select to_jsonb(tf) from public.timesheets_financials tf where tf.timesheet_id=v_request.reversed_timesheet_id and tf.is_current)),
    (select to_jsonb(tf) from public.timesheets_financials tf where tf.timesheet_id=v_request.reversal_timesheet_id and tf.is_current));
  return jsonb_build_object('request_id',v_request.id,'timesheet_id',v_request.reversed_timesheet_id,'origin',v_request.origin,
    'root_timesheet_id',v_request.root_timesheet_id,'contract_id',v_request.contract_id,
    'sheet_scope',(select t.sheet_scope::text from public.timesheets t where t.timesheet_id=v_request.reversed_timesheet_id));
exception when sqlstate '23514' or sqlstate '55000' then return null;
end;
$function$;

create or replace function private.timesheet_correction_nullable_daily_contract_v1(p_scope jsonb,p_timesheet_id uuid)
returns boolean language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_request private.timesheet_correction_requests%rowtype; v_scope jsonb; v_witness jsonb;
begin
  if upper(coalesce(p_scope->>'validation_purpose',''))='CREDIT_SOURCE' then
    v_scope:=private.timesheet_correction_scoped_source_identity_v1(p_scope);
    return v_scope->>'timesheet_id'=p_timesheet_id::text and v_scope->>'origin'='SIGNED_TIMESHEET_CORRECTION'
      and v_scope->>'contract_id' is null and v_scope->>'sheet_scope'='DAILY';
  end if;
  -- The only ordinary null-Contract exception is the exact finalized P0 of an
  -- APPLIED signed Daily request, not an ordinary Daily or forged WHOLE row.
  select request.* into v_request from private.timesheet_correction_requests request
    where request.replacement_timesheet_id=p_timesheet_id and request.state='APPLIED'
      and request.origin='SIGNED_TIMESHEET_CORRECTION' and request.contract_id is null;
  if not found or not exists(select 1 from public.timesheets t where t.timesheet_id=p_timesheet_id
    and t.sheet_scope='DAILY' and t.contract_id is null and t.is_current and t.correction_id=v_request.correction_id
    and t.correction_kind='WHOLE_TIMESHEET_REPLACEMENT' and t.parent_timesheet_id=v_request.root_timesheet_id) then return false; end if;
  perform private.timesheet_correction_applied_assert_v1(v_request.id);
  v_witness:=private.timesheet_correction_signed_witness_v1(p_timesheet_id,false);
  return v_witness->>'workflow_id'=v_request.workflow_id::text
    and v_witness->>'workflow_generation'=v_request.workflow_generation::text;
exception when sqlstate '23514' or sqlstate '55000' then return false;
end;
$function$;
