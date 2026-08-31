-- Repeatable CloudTMS function/view authority: candidate_empty_expense_carrier_action_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

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

create or replace function private._candidate_action_invocation_v1(p_action jsonb)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_code text:=upper(coalesce(p_action->>'code',''));
  v_method text:=upper(coalesce(p_action->>'method',''));
  v_week public.contract_weeks%rowtype;
  v_daily public.timesheets%rowtype;
  v_route_family text;
  v_fixed jsonb:='{}'::jsonb;
  v_inputs jsonb:='[]'::jsonb;
  v_invocation jsonb;
begin
  if v_code in ('CONTINUE_TIMESHEET','CONTINUE_EXPENSE_CLAIM')
     or (v_code='ADD_EXPENSES' and nullif(p_action->>'workflow_id','') is not null) then
    v_invocation:=jsonb_build_object('version',1,'kind','CLIENT_DESTINATION',
      'destination',case when v_code in ('CONTINUE_EXPENSE_CLAIM','ADD_EXPENSES')
        then 'EXPENSE_CLAIM_EDITOR' else 'TIMESHEET_EDITOR' end,
      'context',jsonb_build_object('workflow_id',p_action->'workflow_id',
        'workflow_generation',p_action->'workflow_generation'));
    return (p_action-'method'-'path')||jsonb_build_object(
      'method',null,'path',null,'invocation',v_invocation
    );
  end if;
  if v_code in ('ENTER_TIMESHEET','ADD_EXPENSES') then
    if v_code='ENTER_TIMESHEET' and nullif(p_action->>'contract_week_id','') is null then
      select * into v_daily from public.timesheets
        where timesheet_id=nullif(p_action->>'timesheet_id','')::uuid
          and sheet_scope='DAILY' and is_current and archived_at_utc is null;
      if found then
        return (p_action-'method'-'path')||jsonb_build_object('method',null,'path',null,
          'invocation',jsonb_build_object('version',1,'kind','CLIENT_DESTINATION',
            'destination','TIMESHEET_EDITOR','context',
            jsonb_build_object('timesheet_id',v_daily.timesheet_id)));
      end if;
    end if;
    select * into v_week from public.contract_weeks
    where id=nullif(p_action->>'contract_week_id','')::uuid;
    if not found then
      return p_action||jsonb_build_object('enabled',false,
        'disabled_reason','CANDIDATE_ACTION_CONTEXT_STALE');
    end if;
    v_route_family:=private._candidate_route_family_v1(v_week.timesheet_id,v_week.id)->>'route_family';
    v_fixed:=jsonb_build_object('workflow',jsonb_strip_nulls(jsonb_build_object(
      'workflow_kind',case when v_code='ADD_EXPENSES' then 'CONTRACT_EXPENSE' else 'CONTRACT_HOURS' end,
      'scope','WEEKLY','route',case when v_route_family='QR' then 'PAPER' else 'ELECTRONIC' end,
      'contract_id',v_week.contract_id,'contract_week_id',v_week.id,
      'week_ending_date',v_week.week_ending_date,
      'timesheet_id',case when v_code='ADD_EXPENSES' then p_action->'timesheet_id' else null end
    )));
    v_inputs:='[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb;
  elsif v_code in (
    'RESUBMIT_TIMESHEET','RESUBMIT_TIMESHEET_AND_EXPENSES',
    'RESUBMIT_EXPENSE_CLAIM','REVIEW_AND_RESUBMIT'
  ) then
    v_fixed:=jsonb_build_object('generation',p_action->'workflow_generation');
    v_inputs:='[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb;
  elsif v_method='POST' then
    if p_action ? 'workflow_generation' then
      v_fixed:=jsonb_build_object('generation',p_action->'workflow_generation');
    end if;
    if v_code in ('SEND_MANAGER_REMINDER','REQUEST_APPROVAL_AGAIN') then
      v_fixed:=v_fixed||jsonb_strip_nulls(jsonb_build_object(
        'approval_request_id',p_action->'approval_request_id',
        'approval_request_generation',p_action->'approval_request_generation'
      ));
    end if;
    v_inputs:=case v_code
      when 'DISCARD_EXPENSE_CLAIM' then '[{"name":"reason_note","type":"string","required":true},{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb
      when 'CANCEL_ENTIRE_CLAIM_AND_START_AGAIN' then '[{"name":"reason_note","type":"string","required":true},{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb
      when 'UPLOAD_SIGNED_RETURN' then '[{"name":"returned_pages","type":"array","required":true},{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb
      when 'NO_WORK_THIS_WEEK' then '[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb
      else '[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb end;
  end if;
  v_invocation:=jsonb_build_object('version',1,'kind','HTTP','method',nullif(v_method,''),
    'path',p_action->>'path','fixed_body',v_fixed,'required_user_inputs',v_inputs,
    'idempotency',case when v_method='POST' then 'REQUIRED' else 'NOT_REQUIRED' end);
  return p_action||jsonb_build_object('invocation',v_invocation);
end;
$function$;

alter function private._candidate_timesheet_primary_action_v1(text,jsonb,jsonb,uuid,uuid)
  owner to postgres;
alter function private._candidate_action_invocation_v1(jsonb) owner to postgres;

revoke all on function private._candidate_timesheet_primary_action_v1(text,jsonb,jsonb,uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_action_invocation_v1(jsonb)
  from public,anon,authenticated,service_role;

commit;
