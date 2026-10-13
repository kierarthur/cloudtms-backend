create or replace function private._candidate_timesheet_delete_retire_workflows_v1(
  p_environment text,
  p_timesheet_ids uuid[],
  p_contract_week_ids uuid[],
  p_excluded_workflow_ids uuid[],
  p_actor_user_id uuid,
  p_delete_operation_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_timesheet_ids uuid[];
  v_contract_week_ids uuid[];
  v_excluded_workflow_ids uuid[];
  v_workflow public.candidate_submission_workflows%rowtype;
  v_expense_component public.candidate_expense_components%rowtype;
  v_expense_before jsonb;
  v_retired_workflow_ids uuid[]:=array[]::uuid[];
  v_previous_state text;
  v_correction_cap jsonb;
begin
  select coalesce(array_agg(distinct supplied_id order by supplied_id),array[]::uuid[])
  into v_timesheet_ids
  from unnest(coalesce(p_timesheet_ids,array[]::uuid[])) supplied(supplied_id)
  where supplied_id is not null;
  select coalesce(array_agg(distinct supplied_id order by supplied_id),array[]::uuid[])
  into v_contract_week_ids
  from unnest(coalesce(p_contract_week_ids,array[]::uuid[])) supplied(supplied_id)
  where supplied_id is not null;
  select coalesce(array_agg(distinct supplied_id order by supplied_id),array[]::uuid[])
  into v_excluded_workflow_ids
  from unnest(coalesce(p_excluded_workflow_ids,array[]::uuid[])) supplied(supplied_id)
  where supplied_id is not null;

  if p_actor_user_id is null or p_delete_operation_id is null
     or cardinality(v_timesheet_ids)<1 or cardinality(v_timesheet_ids)>64
     or cardinality(v_contract_week_ids)>64
     or cardinality(v_excluded_workflow_ids)>20 then
    raise exception 'CANDIDATE_TIMESHEET_DELETE_RETIREMENT_INVALID'
      using errcode='22023';
  end if;

  for v_workflow in
    select workflow_row.*
    from public.candidate_submission_workflows workflow_row
    where workflow_row.environment=v_environment
      and workflow_row.workflow_kind in (
        'CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED','DAILY'
      )
      and (workflow_row.workflow_kind<>'DAILY' or exists(select 1 from unnest(v_timesheet_ids) id
        where private.timesheet_correction_cancel_capability_v1(id,p_actor_user_id)->>'workflow_id'=workflow_row.id::text))
      and not (workflow_row.id=any(v_excluded_workflow_ids))
      and (
        workflow_row.target_timesheet_id=any(v_timesheet_ids)
        or workflow_row.anchor_timesheet_id=any(v_timesheet_ids)
        or workflow_row.contract_week_id=any(v_contract_week_ids)
      )
    order by workflow_row.id
    for update
  loop
    v_correction_cap:=null;
    select private.timesheet_correction_cancel_capability_v1(id,p_actor_user_id) into v_correction_cap
      from unnest(v_timesheet_ids) id where private.timesheet_correction_cancel_capability_v1(id,p_actor_user_id)->>'workflow_id'=v_workflow.id::text;
    if v_correction_cap is not null and (cardinality(v_timesheet_ids)<>1
      or v_correction_cap->>'delete_operation_id' is distinct from p_delete_operation_id::text
      or v_correction_cap->>'workflow_generation' is distinct from v_workflow.generation::text
      or to_jsonb(v_contract_week_ids) is distinct from case when v_correction_cap->>'contract_week_id' is null
        then '[]'::jsonb else jsonb_build_array((v_correction_cap->>'contract_week_id')::uuid) end) then
      raise exception 'CORRECTION_CANCEL_RETIREMENT_SCOPE_CHANGED' using errcode='40001';
    end if;
    if v_correction_cap is null and v_workflow.state not in (
      'CREATED','WORKER_DRAFT','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'
    ) then
      raise exception 'CANDIDATE_SUBMISSION_REJECTION_REQUIRED'
        using errcode='55000',detail=jsonb_build_object(
          'workflow_id',v_workflow.id,
          'workflow_state',v_workflow.state
        )::text;
    end if;
    v_previous_state:=v_workflow.state;

    update public.candidate_approval_requests approval
    set state='CANCELLED',
        cancelled_at_utc=coalesce(approval.cancelled_at_utc,p_now_utc),
        updated_at_utc=p_now_utc
    where approval.workflow_id=v_workflow.id
      and approval.state='PENDING';

    -- The stable category ledger must be retired before the owning Timesheet
    -- FK is released.  Otherwise a draft component can survive the generic
    -- Office delete with no owner and still project a Candidate Remove action.
    -- Only components proved to belong to this deletion unit (or an unowned
    -- component on the exact deleted workflow anchor) are affected.
    for v_expense_component in
      select component.*
      from public.candidate_expense_components component
      where component.workflow_id=v_workflow.id
        and component.lifecycle_state not in (
          'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
        )
        and (
          component.owning_timesheet_id=any(v_timesheet_ids)
          or (
            component.owning_timesheet_id is null
            and (
              v_workflow.target_timesheet_id=any(v_timesheet_ids)
              or v_workflow.anchor_timesheet_id=any(v_timesheet_ids)
            )
          )
        )
      order by component.expense_component_id
      for update
    loop
      v_expense_before:=to_jsonb(v_expense_component);
      update public.candidate_expense_components component set
        component_generation=component.component_generation+1,
        owning_timesheet_id=null,
        lifecycle_state='CANCELLED',
        manager_approval_state=case
          when component.manager_approval_state in ('APPROVED','REFUSED')
            then component.manager_approval_state
          else 'NOT_REQUESTED' end,
        approval_request_id=case
          when component.manager_approval_state in ('APPROVED','REFUSED')
            then component.approval_request_id
          else null end,
        removed_at_utc=coalesce(component.removed_at_utc,p_now_utc),
        updated_at_utc=p_now_utc
      where component.expense_component_id=v_expense_component.expense_component_id
      returning component.* into v_expense_component;
      insert into public.candidate_expense_component_events(
        expense_component_id,workflow_id,component_generation,event_type,
        actor_kind,actor_id,before_state_json,after_state_json,idempotency_key,
        occurred_at_utc
      ) values (
        v_expense_component.expense_component_id,v_workflow.id,
        v_expense_component.component_generation,'CANCELLED','OFFICE',
        p_actor_user_id,v_expense_before,to_jsonb(v_expense_component),
        'office-timesheet-delete:'||p_delete_operation_id::text||':'
          ||v_expense_component.expense_component_id::text,p_now_utc
      ) on conflict(expense_component_id,idempotency_key) do nothing;
    end loop;

    update public.candidate_submission_components component
    set timesheet_id=null,
        state=case
          when v_previous_state in ('CREATED','WORKER_DRAFT')
            and component.state not in ('SUPERSEDED','REJECTED','ABANDONED')
            then 'ABANDONED'
          else component.state
        end,
        superseded_at_utc=case
          when v_previous_state in ('CREATED','WORKER_DRAFT')
            then coalesce(component.superseded_at_utc,p_now_utc)
          else component.superseded_at_utc
        end
    where component.workflow_id=v_workflow.id
      and component.timesheet_id=any(v_timesheet_ids);

    update public.candidate_notifications notification
    set timesheet_id=null,
        deep_link_json=(coalesce(notification.deep_link_json,'{}'::jsonb)
          -'timesheet_id'-'contract_week_id')||jsonb_build_object(
            'type','workflow','workflow_id',v_workflow.id
          ),
        state='DISMISSED',
        dismissed_at_utc=coalesce(notification.dismissed_at_utc,p_now_utc),
        push_state=case
          when notification.push_state in ('PENDING','FAILED','CLAIMED') then 'SKIPPED'
          else notification.push_state end
    where notification.workflow_id=v_workflow.id;

    update public.candidate_submission_workflows workflow_row
    set state=case
          when v_correction_cap is not null or workflow_row.state in ('CREATED','WORKER_DRAFT')
            then 'CANCELLED'
          else workflow_row.state
        end,
        contract_week_id=null,
        anchor_timesheet_id=null,
        target_timesheet_id=null,
        cancelled_at_utc=case
          when v_correction_cap is not null or workflow_row.state in ('CREATED','WORKER_DRAFT')
            then coalesce(workflow_row.cancelled_at_utc,p_now_utc)
          else workflow_row.cancelled_at_utc
        end,
        input_snapshot_json=coalesce(workflow_row.input_snapshot_json,'{}'::jsonb)
          ||jsonb_build_object(
            'office_permanent_delete_tombstone',jsonb_build_object(
              'delete_operation_id',p_delete_operation_id,
              'deleted_timesheet_ids',to_jsonb(v_timesheet_ids),
              'deleted_contract_week_ids',to_jsonb(v_contract_week_ids),
              'previous_contract_week_id',v_workflow.contract_week_id,
              'previous_anchor_timesheet_id',v_workflow.anchor_timesheet_id,
              'previous_target_timesheet_id',v_workflow.target_timesheet_id,
              'retired_at_utc',p_now_utc
            )
          ),
        issue_codes=case
          when workflow_row.issue_codes
            @> '["OFFICE_PERMANENTLY_DELETED_TIMESHEET"]'::jsonb
            then workflow_row.issue_codes
          else workflow_row.issue_codes
            ||'["OFFICE_PERMANENTLY_DELETED_TIMESHEET"]'::jsonb
        end,
        updated_at_utc=p_now_utc
    where workflow_row.id=v_workflow.id;

    insert into public.audit_events(
      actor_user_id,object_type,object_id_text,action,before_json,after_json,
      reason,correlation_id,ts_utc
    ) values (
      p_actor_user_id,'candidate_submission_workflows',v_workflow.id::text,
      'CANDIDATE_WORKFLOW_RETAINED_AFTER_TIMESHEET_DELETE',
      jsonb_build_object(
        'state',v_previous_state,
        'contract_week_id',v_workflow.contract_week_id,
        'anchor_timesheet_id',v_workflow.anchor_timesheet_id,
        'target_timesheet_id',v_workflow.target_timesheet_id
      ),
      jsonb_build_object(
        'state',case when v_correction_cap is not null or v_previous_state in ('CREATED','WORKER_DRAFT')
          then 'CANCELLED' else v_previous_state end,
        'live_timesheet_links_released',true,
        'terminal_audit_retained',true
      ),
      'OFFICE_PERMANENTLY_DELETED_TIMESHEET',p_delete_operation_id,p_now_utc
    );
    v_retired_workflow_ids:=array_append(
      v_retired_workflow_ids,v_workflow.id
    );
  end loop;

  return jsonb_build_object(
    'ok',true,
    'contract_version','CANDIDATE_TIMESHEET_DELETE_WORKFLOW_RETIREMENT_V1',
    'retired_workflow_count',cardinality(v_retired_workflow_ids),
    'retired_workflow_ids',to_jsonb(v_retired_workflow_ids)
  );
end;
$function$;
