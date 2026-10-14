create or replace function private._timesheet_candidate_submission_delete_guard_v1(
  p_environment text,
  p_timesheet_ids uuid[],
  p_lock_workflows boolean default false
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
  v_timesheet_id uuid;
  v_target_context jsonb;
  v_target jsonb;
  v_guard_items jsonb:='[]'::jsonb;
  v_all_workflow_ids uuid[]:=array[]::uuid[];
  v_linked_pending_expense_workflow_ids uuid[]:=array[]::uuid[];
  v_stage text;
  v_linked_pending_expense_count integer:=0;
begin
  select coalesce(array_agg(distinct supplied_id order by supplied_id),array[]::uuid[])
  into v_timesheet_ids
  from unnest(coalesce(p_timesheet_ids,array[]::uuid[])) supplied(supplied_id)
  where supplied_id is not null;

  if cardinality(v_timesheet_ids)<1 or cardinality(v_timesheet_ids)>64 then
    raise exception 'CANDIDATE_SUBMISSION_DELETE_TARGET_SET_INVALID' using errcode='22023';
  end if;

  foreach v_timesheet_id in array v_timesheet_ids loop
    v_target_context:=private._candidate_office_rejection_targets_v2(
      v_environment,v_timesheet_id
    );
    for v_target in
      select value from jsonb_array_elements(coalesce(
        v_target_context->'target_workflows','[]'::jsonb
      )) target(value)
    loop
      if not ((v_target->>'workflow_id')::uuid=any(v_all_workflow_ids)) then
        v_all_workflow_ids:=array_append(
          v_all_workflow_ids,(v_target->>'workflow_id')::uuid
        );
      end if;
      if coalesce((v_target->>'delete_guard_applicable')::boolean,false)
         and coalesce((v_target->>'linked_pending_expense')::boolean,false)
         and not ((v_target->>'workflow_id')::uuid
           =any(v_linked_pending_expense_workflow_ids)) then
        v_linked_pending_expense_workflow_ids:=array_append(
          v_linked_pending_expense_workflow_ids,(v_target->>'workflow_id')::uuid
        );
      end if;
      if coalesce((v_target->>'delete_guard_applicable')::boolean,false)
         and not (private.timesheet_correction_cancel_capability_v1(v_timesheet_id,null) is not null
           and v_target->>'workflow_id'=private.timesheet_correction_cancel_capability_v1(v_timesheet_id,null)->>'workflow_id')
         and v_target->>'candidate_submission_stage'<>'DRAFT'
         and not exists(
           select 1 from jsonb_array_elements(v_guard_items) existing(item)
           where existing.item->>'workflow_id'=v_target->>'workflow_id'
         ) then
        v_guard_items:=v_guard_items||jsonb_build_array(
          v_target||jsonb_build_object('rejection_timesheet_id',v_timesheet_id)
        );
      end if;
    end loop;
  end loop;

  if p_lock_workflows and cardinality(v_all_workflow_ids)>0 then
    perform 1
    from public.candidate_submission_workflows workflow_row
    where workflow_row.id=any(v_all_workflow_ids)
    order by workflow_row.id
    for update;
    return private._timesheet_candidate_submission_delete_guard_v1(
      v_environment,v_timesheet_ids,false
    );
  end if;

  v_stage:=case
    when exists(
      select 1 from jsonb_array_elements(v_guard_items) item
      where item->>'candidate_submission_stage'='MANAGER_APPROVED'
    ) then 'MANAGER_APPROVED'
    when jsonb_array_length(v_guard_items)>0 then 'CANDIDATE_SUBMITTED'
    else null
  end;
  v_linked_pending_expense_count:=cardinality(
    v_linked_pending_expense_workflow_ids
  );

  return jsonb_build_object(
    'ok',true,
    'contract_version','TIMESHEET_CANDIDATE_SUBMISSION_DELETE_GUARD_V1',
    'environment',v_environment,
    'timesheet_ids',to_jsonb(v_timesheet_ids),
    'candidate_submission_rejection_required',jsonb_array_length(v_guard_items)>0,
    'candidate_submission_stage',v_stage,
    'guarded_workflow_count',jsonb_array_length(v_guard_items),
    'guarded_workflows',v_guard_items,
    'related_workflow_ids',to_jsonb(v_all_workflow_ids),
    'linked_pending_expense_claim_count',v_linked_pending_expense_count
  );
end;
$function$;
