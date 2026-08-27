\set ON_ERROR_STOP on

begin;

create or replace function public.candidate_manager_finalisation_recovery_v1(
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_approval_count integer:=0;
begin
  if p_workflow_id is null or p_expected_generation is null or p_expected_generation<1 then
    raise exception 'CANDIDATE_MANAGER_FINALISATION_RECOVERY_INVALID'
      using errcode='22023';
  end if;

  select workflow.* into v_workflow
  from public.candidate_submission_workflows as workflow
  where workflow.id=p_workflow_id
    and workflow.environment=v_environment
  for share;

  if not found then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.generation<>p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;
  if v_workflow.route not in ('PHONE','EMAIL')
     or v_workflow.state not in (
       'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE'
     ) then
    raise exception 'CANDIDATE_MANAGER_FINALISATION_RECOVERY_NOT_ALLOWED'
      using errcode='42501';
  end if;

  select count(*)::integer into v_approval_count
  from public.candidate_approval_requests as approval
  where approval.workflow_id=v_workflow.id
    and approval.workflow_generation=v_workflow.generation
    and approval.state='APPROVED'
    and approval.method=v_workflow.route;

  if v_approval_count<>1 then
    raise exception 'CANDIDATE_MANAGER_APPROVAL_NOT_CURRENT' using errcode='55000';
  end if;

  select approval.* into strict v_approval
  from public.candidate_approval_requests as approval
  where approval.workflow_id=v_workflow.id
    and approval.workflow_generation=v_workflow.generation
    and approval.state='APPROVED'
    and approval.method=v_workflow.route;

  if v_approval.approved_at_utc is null
     or v_approval.signature_component_id is null
     or v_approval.review_manifest_sha256 is null then
    raise exception 'CANDIDATE_MANAGER_APPROVAL_INCOMPLETE' using errcode='55000';
  end if;

  return jsonb_build_object(
    'ok',true,
    'workflow_id',v_workflow.id,
    'generation',v_workflow.generation,
    'state',v_workflow.state,
    'recovered_at_utc',coalesce(p_now_utc,now()),
    'final_render_contract',private._candidate_render_contract_v1(
      v_workflow.id,v_workflow.generation,'FINAL_SIGNED'
    )
  );
end;
$function$;

revoke all on function public.candidate_manager_finalisation_recovery_v1(text,uuid,integer,timestamptz)
  from public,anon,authenticated;
grant execute on function public.candidate_manager_finalisation_recovery_v1(text,uuid,integer,timestamptz)
  to service_role;

commit;
