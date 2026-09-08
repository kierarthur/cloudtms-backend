begin;

create or replace function public.candidate_expense_update_render_recovery_list_v1(
  p_environment text,
  p_limit integer default 1,
  p_update_id uuid default null,
  p_operation_id uuid default null,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_rows jsonb;
begin
  if p_limit not between 1 and 5 then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_RECOVERY_LIMIT_INVALID' using errcode='22023';
  end if;
  if p_now_utc is null then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_RECOVERY_TIME_INVALID' using errcode='22023';
  end if;
  if p_update_id is null and p_operation_id is not null then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_RECOVERY_IDENTITY_INVALID' using errcode='22023';
  end if;

  select coalesce(jsonb_agg(jsonb_build_object(
    'update_id',target.update_id,
    'workflow_id',target.workflow_id,
    'operation_id',target.operation_id,
    'submit_result_json',target.submit_result_json,
    'updated_at_utc',target.updated_at_utc
  ) order by target.updated_at_utc,target.update_id),'[]'::jsonb)
  into v_rows
  from (
    select update_row.update_id,update_row.workflow_id,update_row.operation_id,
           update_row.submit_result_json,update_row.updated_at_utc
    from public.candidate_pending_expense_updates update_row
    join public.candidate_submission_workflows workflow
      on workflow.id=update_row.workflow_id
     and workflow.generation=update_row.current_workflow_generation
    left join public.candidate_expense_operations operation
      on operation.operation_id=update_row.operation_id
     and operation.workflow_id=update_row.workflow_id
    where workflow.environment=v_environment
      and update_row.state='RENDERING'
      and (update_row.operation_id is null or operation.state='RENDERING')
      and (p_update_id is not null or update_row.updated_at_utc<=p_now_utc-interval '30 seconds')
      and (p_update_id is null or update_row.update_id=p_update_id)
      and (p_operation_id is null or update_row.operation_id=p_operation_id)
      and jsonb_typeof(update_row.submit_result_json)='object'
      and update_row.submit_result_json->>'update_id'=update_row.update_id::text
      and update_row.submit_result_json->>'workflow_id'=update_row.workflow_id::text
      and jsonb_typeof(update_row.submit_result_json->'render_contract')='object'
    order by update_row.updated_at_utc,update_row.update_id
    limit p_limit
  ) target;

  return jsonb_build_object(
    'ok',true,
    'contract_version','CANDIDATE_EXPENSE_RENDER_RECOVERY_LIST_V1',
    'items',v_rows,
    'count',jsonb_array_length(v_rows)
  );
end;
$function$;

alter function public.candidate_expense_update_render_recovery_list_v1(
  text,integer,uuid,uuid,timestamptz
) owner to postgres;
revoke all on function public.candidate_expense_update_render_recovery_list_v1(
  text,integer,uuid,uuid,timestamptz
) from public,anon,authenticated,service_role;
grant execute on function public.candidate_expense_update_render_recovery_list_v1(
  text,integer,uuid,uuid,timestamptz
) to service_role;

comment on function public.candidate_expense_update_render_recovery_list_v1(
  text,integer,uuid,uuid,timestamptz
) is 'Returns a bounded service-only list of exact saved review renders, including ordinary Candidate updates that do not own an expense operation row.';

notify pgrst, 'reload schema';

commit;
