\set ON_ERROR_STOP on

-- A manager-approved Candidate submission remains immutable while it awaits
-- Office authorisation, but a later claim may use a separate expense-only
-- carrier immediately after the approved submission safely finalises.
-- Before manager approval, the active claim must still be withdrawn and
-- restarted rather than accepting another expense in parallel.
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
  v_status text;
begin
  select lower(pg_get_functiondef(to_regprocedure(
    'private._candidate_timesheet_primary_action_v1(text,jsonb,jsonb,uuid,uuid)'
  ))) into v_definition;
  if position('v_has_finalised_hours' in v_definition)=0
     or position('v_hours_office_protected' in v_definition)<>0
     or v_definition~'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' then
    raise exception 'Manager-approved additional-expense action authority is not installed safely';
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
  if v_action->>'code'<>'ADD_EXPENSES'
     or v_action->>'label'<>'Add Expenses' then
    raise exception 'Manager-approved combined claim did not expose a separate expense claim: %',v_action;
  end if;
  v_action_contract:=private._candidate_timesheet_action_contract_v1(
    'PENDING_AUTH',v_workflows,
    jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
    v_timesheet,v_week,now()
  );
  if v_action_contract#>>'{primary_action,code}'<>'ADD_EXPENSES'
     or not exists(
       select 1
       from jsonb_array_elements(v_action_contract->'available_actions') item
       where item->>'code'='ADD_EXPENSES'
     ) then
    raise exception 'Manager-approved combined claim lost its separate expense action: %',
      v_action_contract;
  end if;

  foreach v_status in array array['AUTHORISED','INVOICED_NOT_PAID','PAID'] loop
    v_action:=private._candidate_timesheet_primary_action_v1(
      v_status,v_workflows,
      jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
      v_timesheet,v_week
    );
    if v_action->>'code'<>'ADD_EXPENSES'
       or v_action->>'label'<>'Add Expenses' then
      raise exception 'Later status % did not retain the separate expense action: %',
        v_status,v_action;
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
    raise exception 'Authorised claim lost its separate expense action: %',
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
  if v_action->>'code'<>'ADD_EXPENSES' then
    raise exception 'Manager-approved hours did not expose a separate expense claim: %',v_action;
  end if;

  v_workflows:=jsonb_build_array(jsonb_build_object(
    'workflow_id',v_workflow,'workflow_kind','CONTRACT_EXPENSE',
    'state','FINALISED','generation',2,'detail_action_owner',true,
    'updated_at_utc',now()
  ));
  v_action:=private._candidate_timesheet_primary_action_v1(
    'PENDING_AUTH',v_workflows,
    jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
    v_timesheet,v_week
  );
  if v_action->>'code'<>'ADD_EXPENSES' then
    raise exception 'Manager-approved expense still blocked the next claim: %',v_action;
  end if;

  v_workflows:=jsonb_build_array(jsonb_build_object(
    'workflow_id',v_workflow,'workflow_kind','CONTRACT_COMBINED',
    'state','AWAITING_MANAGER_APPROVAL','generation',2,'detail_action_owner',true,
    'updated_at_utc',now()
  ));
  v_action:=private._candidate_timesheet_primary_action_v1(
    'AWAITING_MANAGER_APPROVAL',v_workflows,
    jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
    v_timesheet,v_week
  );
  if v_action->>'code'<>'CONTINUE_TIMESHEET' then
    raise exception 'Pre-approval submission did not remain on its existing claim: %',v_action;
  end if;
  v_action_contract:=private._candidate_timesheet_action_contract_v1(
    'AWAITING_MANAGER_APPROVAL',v_workflows,
    jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
    v_timesheet,v_week,now()
  );
  if exists(
    select 1
    from jsonb_array_elements(v_action_contract->'available_actions') item
    where item->>'code'='ADD_EXPENSES'
  ) then
    raise exception 'Pre-approval submission exposed a parallel expense claim: %',
      v_action_contract;
  end if;
end;
$verification$;

rollback;
