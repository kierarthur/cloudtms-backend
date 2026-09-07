begin;

create or replace function public.candidate_review_render_recovery_list_v1(
  p_environment text,
  p_limit integer default 1,
  p_workflow_id uuid default null,
  p_workflow_generation integer default null,
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
    raise exception 'CANDIDATE_REVIEW_RENDER_RECOVERY_LIMIT_INVALID' using errcode='22023';
  end if;
  if p_now_utc is null then
    raise exception 'CANDIDATE_REVIEW_RENDER_RECOVERY_TIME_INVALID' using errcode='22023';
  end if;
  if (p_workflow_id is null)<>(p_workflow_generation is null) then
    raise exception 'CANDIDATE_REVIEW_RENDER_RECOVERY_IDENTITY_INVALID' using errcode='22023';
  end if;

  select coalesce(jsonb_agg(jsonb_build_object(
    'workflow_id',target.workflow_id,
    'workflow_generation',target.workflow_generation,
    'render_contract',target.render_contract,
    'updated_at_utc',target.updated_at_utc
  ) order by target.updated_at_utc,target.workflow_id),'[]'::jsonb)
  into v_rows
  from (
    select workflow.id as workflow_id,
           workflow.generation as workflow_generation,
           private._candidate_render_contract_v1(
             workflow.id,workflow.generation,'ELECTRONIC_MANAGER_REVIEW'
           ) as render_contract,
           workflow.updated_at_utc
    from public.candidate_submission_workflows workflow
    where workflow.environment=v_environment
      and workflow.state='WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT'
      and workflow.route in ('ELECTRONIC','PHONE','EMAIL')
      and (p_workflow_id is not null or workflow.updated_at_utc<=p_now_utc-interval '30 seconds')
      and (p_workflow_id is null or workflow.id=p_workflow_id)
      and (p_workflow_generation is null or workflow.generation=p_workflow_generation)
      and not exists(
        select 1
        from public.candidate_pending_expense_updates pending_update
        where pending_update.workflow_id=workflow.id
          and pending_update.state in ('EDITING','RENDERING')
      )
    order by workflow.updated_at_utc,workflow.id
    limit p_limit
  ) target;

  return jsonb_build_object(
    'ok',true,
    'contract_version','CANDIDATE_REVIEW_RENDER_RECOVERY_LIST_V1',
    'items',v_rows,
    'count',jsonb_array_length(v_rows)
  );
end;
$function$;

alter function public.candidate_review_render_recovery_list_v1(
  text,integer,uuid,integer,timestamptz
) owner to postgres;
revoke all on function public.candidate_review_render_recovery_list_v1(
  text,integer,uuid,integer,timestamptz
) from public,anon,authenticated,service_role;
grant execute on function public.candidate_review_render_recovery_list_v1(
  text,integer,uuid,integer,timestamptz
) to service_role;

comment on function public.candidate_review_render_recovery_list_v1(
  text,integer,uuid,integer,timestamptz
) is 'Returns a bounded service-only review-render contract for an exact queued submission or an abandoned pending submission.';

notify pgrst, 'reload schema';

commit;
