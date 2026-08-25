-- Repeatable CloudTMS function/view authority: candidate_signature_evidence_timestamp_compatibility_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_signature_component_v1(
  p_timesheet_id uuid default null,
  p_contract_week_id uuid default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_capabilities jsonb:='{}'::jsonb;
  v_workflows jsonb:='[]'::jsonb;
  v_evidence jsonb:='[]'::jsonb;
  v_components jsonb:='[]'::jsonb;
begin
  if not private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities') then
    return null;
  end if;
  begin
    v_capabilities:=private._candidate_record_capabilities_v1(p_timesheet_id,p_contract_week_id,'{}'::jsonb);
  exception when others then
    v_capabilities:=jsonb_build_object('unavailable',true,'sqlstate',sqlstate);
  end;

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',w.id,'generation',w.generation,'state',w.state,'route',w.route,
    'issue_codes',w.issue_codes,'target_timesheet_id',w.target_timesheet_id,
    'contract_week_id',w.contract_week_id,
    'review_manifest_sha256',case when w.review_manifest_sha256 is null then null else encode(w.review_manifest_sha256,'hex') end,
    'candidate_signature_sha256',case when w.candidate_signature_sha256 is null then null else encode(w.candidate_signature_sha256,'hex') end,
    'manager_signature_sha256',case when w.manager_signature_sha256 is null then null else encode(w.manager_signature_sha256,'hex') end,
    'manager_approved_at_utc',w.manager_approved_at_utc,'updated_at_utc',w.updated_at_utc
  ) order by w.updated_at_utc,w.id),'[]'::jsonb)
  into v_workflows
  from public.candidate_submission_workflows w
  where (p_timesheet_id is not null and w.target_timesheet_id=p_timesheet_id)
     or (p_contract_week_id is not null and w.contract_week_id=p_contract_week_id);

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',e.id,'timesheet_id',e.timesheet_id,'kind',e.kind,
    'processing_state',e.processing_state,'candidate_component_id',e.candidate_component_id,
    'updated_at',e.created_at
  ) order by e.created_at,e.id),'[]'::jsonb)
  into v_evidence
  from public.timesheet_evidence e
  where p_timesheet_id is not null and e.timesheet_id=p_timesheet_id;

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',c.id,'workflow_id',c.workflow_id,'workflow_generation',c.workflow_generation,
    'component_kind',c.component_kind,'required',c.required,'review_ordinal',c.review_ordinal,
    'state',c.state,'review_render_state',c.review_render_state,
    'review_content_sha256',case when c.review_content_sha256 is null then null else encode(c.review_content_sha256,'hex') end,
    'review_render_input_sha256',case when c.review_render_input_sha256 is null then null else encode(c.review_render_input_sha256,'hex') end,
    'final_signed_render_state',c.final_signed_render_state,
    'final_signed_content_sha256',case when c.final_signed_content_sha256 is null then null else encode(c.final_signed_content_sha256,'hex') end,
    'final_signed_render_input_sha256',case when c.final_signed_render_input_sha256 is null then null else encode(c.final_signed_render_input_sha256,'hex') end
  ) order by c.workflow_generation,c.review_ordinal,c.id),'[]'::jsonb)
  into v_components
  from public.candidate_submission_components c
  join public.candidate_submission_workflows w on w.id=c.workflow_id
  where ((p_timesheet_id is not null and w.target_timesheet_id=p_timesheet_id)
      or (p_contract_week_id is not null and w.contract_week_id=p_contract_week_id))
    and c.state<>'SUPERSEDED';

  return jsonb_build_object(
    'capabilities',v_capabilities,
    'workflows',v_workflows,
    'evidence',v_evidence,
    'components',v_components
  );
end;
$function$;

alter function private._candidate_signature_component_v1(uuid,uuid) owner to postgres;
revoke all on function private._candidate_signature_component_v1(uuid,uuid) from public,anon,authenticated,service_role;
grant execute on function private._candidate_signature_component_v1(uuid,uuid) to postgres;

commit;
