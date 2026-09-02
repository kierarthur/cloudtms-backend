-- Repeatable CloudTMS function/view authority: candidate_finalised_hours_primary_action_v1
-- A completed Candidate hours submission remains the current hours truth until
-- it is explicitly rejected/cancelled.  It must never fall back to a fresh
-- ENTER_TIMESHEET action merely because FINALISED is a terminal workflow state.

\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_timesheet_primary_action_v1(
  p_candidate_status_code text,
  p_workflows jsonb,
  p_capabilities jsonb,
  p_timesheet_id uuid,
  p_contract_week_id uuid
)
returns jsonb
language plpgsql
stable
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow jsonb;
  v_action text;
  v_draft_has_content boolean:=false;
  v_has_finalised_hours boolean:=false;
begin
  if upper(coalesce(p_candidate_status_code,'')) in ('PAID','AUTHORISED','INVOICED_NOT_PAID') then
    return null;
  end if;
  select item into v_workflow
  from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
  where item->>'state'='REJECTED'
    and coalesce((item->>'rejection_actionable')::boolean,false)
    and nullif(item->>'required_resubmission_action','') is not null
  order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
    item->>'updated_at_utc' desc,item->>'workflow_id'
  limit 1;
  if v_workflow is not null then
    v_action:=v_workflow->>'required_resubmission_action';
    return jsonb_build_object(
      'code',v_action,
      'label',case v_action
        when 'RESUBMIT_EXPENSE_CLAIM' then 'Resubmit Expense Claim'
        when 'RESUBMIT_TIMESHEET_AND_EXPENSES' then 'Resubmit Timesheet and Expenses'
        else 'Resubmit Timesheet' end,
      'method','POST',
      'path','/candidate-app/v1/workflows/'||(v_workflow->>'workflow_id')||'/resubmit',
      'requires_confirmation',false,'requires_reason',false,
      'enabled',true,'disabled_reason',null,
      'workflow_id',v_workflow->>'workflow_id',
      'workflow_generation',nullif(v_workflow->>'generation','')::integer
    );
  end if;
  select item into v_workflow
  from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
  where item->>'state' in (
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
  )
    and (
      item->>'state'<>'REFUSED'
      or not private._candidate_rejection_replaced_v1(
        nullif(item->>'workflow_id','')::uuid
      )
    )
  order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
    item->>'updated_at_utc' desc,item->>'workflow_id'
  limit 1;
  if v_workflow is not null then
    if v_workflow->>'workflow_kind'='CONTRACT_EXPENSE'
       and v_workflow->>'state' in ('CREATED','WORKER_DRAFT') then
      select exists(
        select 1
        from public.candidate_submission_components component
        where component.workflow_id=nullif(v_workflow->>'workflow_id','')::uuid
          and component.workflow_generation=nullif(v_workflow->>'generation','')::integer
          and component.superseded_at_utc is null
          and component.component_kind in (
            'HOURS_TIMESHEET','CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE'
          )
      ) into v_draft_has_content;
    end if;
    if v_workflow->>'workflow_kind'='CONTRACT_EXPENSE'
       and v_workflow->>'state' in ('CREATED','WORKER_DRAFT')
       and not v_draft_has_content
       and coalesce((p_capabilities->>'can_edit_hours')::boolean,false) then
      v_workflow:=null;
    end if;
  end if;
  if v_workflow is not null then
    v_action:=case
      when v_workflow->>'state'='REFUSED' then 'REVIEW_AND_RESUBMIT'
      when v_workflow->>'workflow_kind'='CONTRACT_EXPENSE'
        and v_workflow->>'state' in ('CREATED','WORKER_DRAFT')
        and not v_draft_has_content then 'ADD_EXPENSES'
      when v_workflow->>'workflow_kind'='CONTRACT_EXPENSE' then 'CONTINUE_EXPENSE_CLAIM'
      else 'CONTINUE_TIMESHEET' end;
    return jsonb_build_object(
      'code',v_action,
      'label',case v_action
        when 'REVIEW_AND_RESUBMIT' then 'Review and Resubmit'
        when 'ADD_EXPENSES' then 'Add Expenses'
        when 'CONTINUE_EXPENSE_CLAIM' then 'Continue Expense Claim'
        else 'Continue Timesheet' end,
      'method',case when v_action='REVIEW_AND_RESUBMIT' then 'POST' else 'GET' end,
      'path',case when v_action='REVIEW_AND_RESUBMIT'
        then '/candidate-app/v1/workflows/'||(v_workflow->>'workflow_id')||'/actions/amend'
        else '/candidate-app/v1/workflows/'||(v_workflow->>'workflow_id')||'/timesheet-detail' end,
      'requires_confirmation',false,'enabled',true,'disabled_reason',null,
      'workflow_id',v_workflow->>'workflow_id',
      'workflow_generation',nullif(v_workflow->>'generation','')::integer
    );
  end if;

  select exists(
    select 1
    from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
    where item->>'state'='FINALISED'
      and item->>'workflow_kind' in ('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY')
  ) into v_has_finalised_hours;
  if v_has_finalised_hours then
    if coalesce((p_capabilities->>'can_edit_expenses')::boolean,false) then
      return jsonb_build_object(
        'code','ADD_EXPENSES','label','Add Expenses',
        'method','POST','path','/candidate-app/v1/workflows',
        'requires_confirmation',false,'requires_reason',false,
        'enabled',true,'disabled_reason',null,'timesheet_id',p_timesheet_id,
        'contract_week_id',p_contract_week_id
      );
    end if;
    return null;
  end if;

  if coalesce((p_capabilities->>'can_edit_hours')::boolean,false) then
    return jsonb_build_object(
      'code','ENTER_TIMESHEET','label','Enter Timesheet',
      'method','POST','path','/candidate-app/v1/workflows',
      'requires_confirmation',false,'requires_reason',false,
      'enabled',true,'disabled_reason',null,'timesheet_id',p_timesheet_id,
      'contract_week_id',p_contract_week_id
    );
  end if;
  if coalesce((p_capabilities->>'can_edit_expenses')::boolean,false) then
    return jsonb_build_object(
      'code','ADD_EXPENSES','label','Add Expenses',
      'method','POST','path','/candidate-app/v1/workflows',
      'requires_confirmation',false,'requires_reason',false,
      'enabled',true,'disabled_reason',null,'timesheet_id',p_timesheet_id,
      'contract_week_id',p_contract_week_id
    );
  end if;
  return null;
end;
$function$;

alter function private._candidate_timesheet_primary_action_v1(text,jsonb,jsonb,uuid,uuid)
  owner to postgres;
revoke all on function private._candidate_timesheet_primary_action_v1(text,jsonb,jsonb,uuid,uuid)
  from public,anon,authenticated,service_role;

commit;
