\set ON_ERROR_STOP on

-- A finalised hours/combined workflow is a submitted Timesheet. It must never
-- fall back to fresh hours entry, and it may start a later expense claim only
-- after Office protection.
begin;

do $verification$
declare
  v_timesheet uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_hours_workflow uuid:=gen_random_uuid();
  v_expense_workflow uuid:=gen_random_uuid();
  v_action jsonb;
  v_definition text;
begin
  select lower(pg_get_functiondef(to_regprocedure(
    'private._candidate_timesheet_primary_action_v1(text,jsonb,jsonb,uuid,uuid)'
  ))) into v_definition;
  if position('v_has_finalised_hours' in v_definition)=0
     or position('v_hours_office_protected' in v_definition)=0
     or position('expense_financial.authorised_at_utc is not null' in v_definition)=0
     or v_definition~'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' then
    raise exception 'Finalised-hours primary-action authority is not installed safely';
  end if;

  v_action:=private._candidate_timesheet_primary_action_v1(
    'AWAITING_MANUAL_SIGNATURE',
    jsonb_build_array(jsonb_build_object(
      'workflow_id',v_hours_workflow,'workflow_kind','CONTRACT_COMBINED',
      'state','FINALISED','generation',3,'detail_action_owner',true,
      'updated_at_utc',now()
    )),
    jsonb_build_object('can_edit_hours',true,'can_edit_expenses',true),
    v_timesheet,v_week
  );
  if v_action is not null then
    raise exception 'Unprotected finalised combined claim exposed another action: %',v_action;
  end if;

  v_action:=private._candidate_timesheet_primary_action_v1(
    'AWAITING_MANUAL_SIGNATURE',
    jsonb_build_array(jsonb_build_object(
      'workflow_id',v_hours_workflow,'workflow_kind','CONTRACT_HOURS',
      'state','FINALISED','generation',3,'detail_action_owner',true,
      'updated_at_utc',now()
    )),
    jsonb_build_object('can_edit_hours',true,'can_edit_expenses',false),
    v_timesheet,v_week
  );
  if v_action is not null then
    raise exception 'Finalised submitted hours exposed an ineligible action: %',v_action;
  end if;

  v_action:=private._candidate_timesheet_primary_action_v1(
    'AUTHORISED',
    jsonb_build_array(jsonb_build_object(
      'workflow_id',v_hours_workflow,'workflow_kind','CONTRACT_COMBINED',
      'state','FINALISED','generation',3,'detail_action_owner',true,
      'updated_at_utc',now()
    )),
    jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
    v_timesheet,v_week
  );
  if v_action->>'code'<>'ADD_EXPENSES' then
    raise exception 'Office-authorised finalised hours did not expose a new expense claim: %',v_action;
  end if;

  v_action:=private._candidate_timesheet_primary_action_v1(
    'OPEN',
    jsonb_build_array(jsonb_build_object(
      'workflow_id',v_expense_workflow,'workflow_kind','CONTRACT_EXPENSE',
      'state','FINALISED','generation',2,'detail_action_owner',true,
      'updated_at_utc',now()
    )),
    jsonb_build_object('can_edit_hours',true,'can_edit_expenses',true),
    v_timesheet,v_week
  );
  if v_action->>'code'<>'CONTINUE_EXPENSE_CLAIM' then
    raise exception 'A finalised but not Office-authorised expense claim was not kept current: %',v_action;
  end if;

  v_action:=private._candidate_timesheet_primary_action_v1(
    'AUTHORISED','[]'::jsonb,
    jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
    v_timesheet,v_week
  );
  if v_action->>'code'<>'ADD_EXPENSES' then
    raise exception 'An Office-authorised worked week did not offer a new separate expense claim: %',v_action;
  end if;
end;
$verification$;

rollback;
