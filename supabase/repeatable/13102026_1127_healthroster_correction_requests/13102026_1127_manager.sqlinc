-- Service-only enrichment after the existing manager channel has established
-- its exact workflow/generation/approval/manifest. No cross-auth fetch exists.
create or replace function public.timesheet_correction_manager_summary_v1(
  p_workflow_id uuid,p_workflow_generation integer,p_approval_request_id uuid,p_manifest_sha256 text
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_request private.timesheet_correction_requests%rowtype; v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype; v_hours public.candidate_submission_components%rowtype;
  v_render_input jsonb; v_schedule jsonb;
begin
  select w.* into v_workflow from public.candidate_submission_workflows w
    where w.id=p_workflow_id and w.generation=p_workflow_generation;
  select request.* into v_request from private.timesheet_correction_requests request
    where request.origin in('SIGNED_TIMESHEET_CORRECTION','AGENCY_INVOICE_CORRECTION') and request.state='WAITING'
      and request.replacement_timesheet_id=v_workflow.target_timesheet_id
      and request.candidate_id=v_workflow.candidate_id;
  if not found then return null; end if;
  select a.* into v_approval from public.candidate_approval_requests a
    where a.id=p_approval_request_id and a.workflow_id=p_workflow_id and a.workflow_generation=p_workflow_generation
      and a.state in('PENDING','APPROVED') and a.review_manifest_sha256=v_workflow.review_manifest_sha256
      and encode(a.review_manifest_sha256,'hex')=p_manifest_sha256;
  select c.* into v_hours from public.candidate_submission_components c
    where c.id=v_approval.manager_review_timesheet_component_id and c.workflow_id=p_workflow_id
      and c.workflow_generation=p_workflow_generation and c.component_kind='HOURS_TIMESHEET'
      and c.required and c.state='IMMUTABLE' and c.review_render_state='READY'
      and c.review_content_sha256=v_approval.manager_review_timesheet_sha256;
  if v_approval.id is null or v_hours.id is null
    or v_workflow.review_manifest_sha256 is null or v_workflow.immutable_submission_sha256 is null
    or private._candidate_sha256_jsonb_v1(v_workflow.immutable_submission_json) is distinct from v_workflow.immutable_submission_sha256 then
    raise exception 'CORRECTION_MANAGER_REVIEW_WITNESS_CHANGED' using errcode='40001';
  end if;
  v_render_input:=private._candidate_component_render_input_v1(p_workflow_id,p_workflow_generation,v_hours.id);
  if v_render_input->>'render_input_sha256' is distinct from encode(v_hours.review_render_input_sha256,'hex') then
    raise exception 'CORRECTION_MANAGER_REVIEW_INPUT_CHANGED' using errcode='40001';
  end if;
  v_schedule:=coalesce(v_workflow.immutable_submission_json#>'{timesheet_patch_json,actual_schedule_json}',
    v_workflow.immutable_submission_json#>'{hours_submission,timesheet_patch_json,actual_schedule_json}');
  if jsonb_typeof(v_schedule) is distinct from 'array' then
    raise exception 'CORRECTION_MANAGER_REVIEW_PROPOSAL_UNPROVED' using errcode='55000';
  end if;
  return jsonb_build_object('schema_version',1,'request_id',v_request.id,'correction_id',v_request.correction_id,
    'reason',v_request.reason,'original_schedule',v_request.frozen_original_json->'schedule',
    'reviewed_schedule',v_schedule,'review_witness',jsonb_build_object('workflow_id',p_workflow_id,
      'workflow_generation',p_workflow_generation,'approval_request_id',p_approval_request_id,
      'manifest_sha256',p_manifest_sha256,'hours_component_id',v_hours.id,
      'review_render_input_sha256',encode(v_hours.review_render_input_sha256,'hex'),
      'review_content_sha256',encode(v_hours.review_content_sha256,'hex')));
end;
$function$;
