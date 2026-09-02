\set ON_ERROR_STOP on

-- A finalised hours/combined workflow is a submitted Timesheet, even when the
-- record is still editable for later expenses.  It must not be labelled as a
-- fresh unsubmitted Timesheet.
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
  if v_action->>'code'<>'ADD_EXPENSES'
     or v_action->>'label'<>'Add Expenses'
     or v_action->>'code'='ENTER_TIMESHEET' then
    raise exception 'Finalised submitted hours regressed to a fresh Timesheet action: %',v_action;
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
    'OPEN',
    jsonb_build_array(jsonb_build_object(
      'workflow_id',v_expense_workflow,'workflow_kind','CONTRACT_EXPENSE',
      'state','FINALISED','generation',2,'detail_action_owner',true,
      'updated_at_utc',now()
    )),
    jsonb_build_object('can_edit_hours',true,'can_edit_expenses',true),
    v_timesheet,v_week
  );
  if v_action->>'code'<>'ENTER_TIMESHEET' then
    raise exception 'A finalised expense-only claim wrongly hid the outstanding hours journey: %',v_action;
  end if;
end;
$verification$;

rollback;
