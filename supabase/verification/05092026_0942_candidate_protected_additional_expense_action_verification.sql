\set ON_ERROR_STOP on

-- A manager-approved Candidate submission is immutable while it awaits Office
-- authorisation.  A later expense-only carrier is available only after the
-- current hours carrier is Office-protected.
begin;

do $verification$
declare
  v_timesheet uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_action jsonb;
  v_action_contract jsonb;
  v_workflows jsonb;
  v_definition text;
  v_protected_status text;
begin
  select lower(pg_get_functiondef(to_regprocedure(
    'private._candidate_timesheet_primary_action_v1(text,jsonb,jsonb,uuid,uuid)'
  ))) into v_definition;
  if position('v_hours_office_protected' in v_definition)=0
     or position('hours_financial.authorised_at_utc is not null' in v_definition)=0
     or position('hours_financial.locked_by_invoice_id is not null' in v_definition)=0
     or position('hours_financial.paid_at_utc is not null' in v_definition)=0
     or v_definition~'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' then
    raise exception 'Protected additional-expense action authority is not installed safely';
  end if;

  v_workflows:=jsonb_build_array(jsonb_build_object(
    'workflow_id',v_workflow,'workflow_kind','CONTRACT_COMBINED',
    'state','FINALISED','generation',2,'detail_action_owner',true,
    'updated_at_utc',now()
  ));

  v_action:=private._candidate_timesheet_primary_action_v1(
    'PENDING_AUTH',v_workflows,
    jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
    v_timesheet,v_week
  );
  if v_action is not null then
    raise exception 'Unprotected finalised combined claim exposed another expense claim: %',v_action;
  end if;
  v_action_contract:=private._candidate_timesheet_action_contract_v1(
    'PENDING_AUTH',v_workflows,
    jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
    v_timesheet,v_week,now()
  );
  if nullif(v_action_contract->>'primary_action','') is not null
     or exists(
       select 1
       from jsonb_array_elements(v_action_contract->'available_actions') item
       where item->>'code'='ADD_EXPENSES'
     ) then
    raise exception 'Unprotected finalised combined claim retained a secondary expense action: %',
      v_action_contract;
  end if;

  foreach v_protected_status in array array['AUTHORISED','INVOICED_NOT_PAID','PAID'] loop
    v_action:=private._candidate_timesheet_primary_action_v1(
      v_protected_status,v_workflows,
      jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
      v_timesheet,v_week
    );
    if v_action->>'code'<>'ADD_EXPENSES'
       or v_action->>'label'<>'Add Expenses' then
      raise exception 'Office-protected status % did not expose a new expense carrier: %',
        v_protected_status,v_action;
    end if;
  end loop;

  v_action_contract:=private._candidate_timesheet_action_contract_v1(
    'AUTHORISED',v_workflows,
    jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
    v_timesheet,v_week,now()
  );
  if v_action_contract#>>'{primary_action,code}'<>'ADD_EXPENSES'
     or not exists(
       select 1
       from jsonb_array_elements(v_action_contract->'available_actions') item
       where item->>'code'='ADD_EXPENSES'
     ) then
    raise exception 'Office-protected claim lost its authorised expense action: %',
      v_action_contract;
  end if;

  v_workflows:=jsonb_build_array(jsonb_build_object(
    'workflow_id',v_workflow,'workflow_kind','CONTRACT_HOURS',
    'state','FINALISED','generation',2,'detail_action_owner',true,
    'updated_at_utc',now()
  ));
  v_action:=private._candidate_timesheet_primary_action_v1(
    'PENDING_AUTH',v_workflows,
    jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
    v_timesheet,v_week
  );
  if v_action is not null then
    raise exception 'Unprotected finalised hours exposed another expense claim: %',v_action;
  end if;
end;
$verification$;

rollback;
