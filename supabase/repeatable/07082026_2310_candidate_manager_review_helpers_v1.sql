-- Private helpers for the Candidate App manager-review/final-document contract.
--
-- Source, review and final-signed byte identities are deliberately distinct:
--   * source_content_sha256 is the globally unique candidate-origin digest;
--   * review_content_sha256 identifies the exact page shown to the manager;
--   * final_signed_content_sha256 identifies the signed derivative.

create or replace function private._candidate_sha256_jsonb_v1(p_value jsonb)
returns bytea
language sql
immutable
parallel safe
set search_path = pg_catalog, extensions, pg_temp
as $function$
  select extensions.digest(convert_to(coalesce(p_value,'null'::jsonb)::text,'UTF8'),'sha256');
$function$;

create or replace function private._candidate_render_input_v1(
  p_workflow_id uuid,
  p_workflow_generation integer
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_candidate_signature public.candidate_submission_components%rowtype;
  v_signature_digest bytea;
  v_core jsonb;
  v_signature_json jsonb := 'null'::jsonb;
begin
  select * into v_workflow
  from public.candidate_submission_workflows
  where id=p_workflow_id and generation=p_workflow_generation;
  if not found then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;
  if v_workflow.immutable_submission_json is null
     or v_workflow.immutable_submission_sha256 is null then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_REQUIRED' using errcode='55000';
  end if;

  if v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
    select * into v_candidate_signature
    from public.candidate_submission_components
    where id=v_workflow.candidate_signature_component_id
      and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation
      and component_kind='CANDIDATE_SIGNATURE'
      and document_role='CANDIDATE_SIGNATURE'
      and state='IMMUTABLE';
    if not found then
      raise exception 'CANDIDATE_SIGNATURE_REQUIRED' using errcode='55000';
    end if;
    v_signature_digest:=v_candidate_signature.source_content_sha256;
    if v_signature_digest is null and v_candidate_signature.source_component_id is not null then
      select source_content_sha256 into v_signature_digest
      from public.candidate_submission_components
      where id=v_candidate_signature.source_component_id and state='IMMUTABLE';
    end if;
    if v_signature_digest is null
       or v_signature_digest is distinct from v_workflow.candidate_signature_sha256 then
      raise exception 'CANDIDATE_SIGNATURE_REQUIRED' using errcode='55000';
    end if;
    v_signature_json:=jsonb_build_object(
      'component_id',v_candidate_signature.id,
      'source_component_id',coalesce(v_candidate_signature.source_component_id,v_candidate_signature.id),
      'storage_key',v_candidate_signature.storage_key,
      'sha256',encode(v_signature_digest,'hex'),
      'media_type',v_candidate_signature.media_type,
      'signed_at_utc',v_workflow.candidate_signed_at_utc
    );
  elsif v_workflow.candidate_signature_component_id is not null
        or v_workflow.candidate_signature_sha256 is not null then
    raise exception 'CONTRACT_EXPENSE_CANDIDATE_SIGNATURE_FORBIDDEN' using errcode='55000';
  end if;

  v_core:=jsonb_build_object(
    'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation,
    'scope',v_workflow.scope,
    'workflow_kind',v_workflow.workflow_kind,
    'contract_id',v_workflow.contract_id,
    'contract_week_id',v_workflow.contract_week_id,
    'work_date',v_workflow.work_date,
    'week_ending_date',v_workflow.week_ending_date,
    'immutable_submission',v_workflow.immutable_submission_json,
    'immutable_submission_sha256',encode(v_workflow.immutable_submission_sha256,'hex'),
    'policy_snapshot_sha256',encode(v_workflow.policy_snapshot_sha256,'hex'),
    'candidate_signature',v_signature_json,
    'renderer_contract_version',v_workflow.renderer_contract_version
  );
  return v_core||jsonb_build_object(
    'render_input_sha256',encode(private._candidate_sha256_jsonb_v1(v_core),'hex')
  );
end;
$function$;

create or replace function private._candidate_component_render_input_v1(
  p_workflow_id uuid,
  p_workflow_generation integer,
  p_component_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_component public.candidate_submission_components%rowtype;
  v_source public.candidate_submission_components%rowtype;
  v_workflow_input jsonb;
  v_source_digest bytea;
  v_core jsonb;
begin
  select * into v_component
  from public.candidate_submission_components
  where id=p_component_id
    and workflow_id=p_workflow_id
    and workflow_generation=p_workflow_generation
    and required=true
    and state<>'SUPERSEDED';
  if not found then
    raise exception 'MANAGER_REVIEW_DOCUMENT_STALE' using errcode='55000';
  end if;
  v_workflow_input:=private._candidate_render_input_v1(p_workflow_id,p_workflow_generation);

  if v_component.source_component_id is not null then
    select * into v_source from public.candidate_submission_components
    where id=v_component.source_component_id
      and state in ('IMMUTABLE','SUPERSEDED','REJECTED')
      and immutable_at_utc is not null
      and source_content_sha256 is not null;
    if not found then
      raise exception 'CANDIDATE_SOURCE_COMPONENT_NOT_ALLOWED' using errcode='55000';
    end if;
  else
    v_source:=v_component;
  end if;
  v_source_digest:=coalesce(v_component.source_content_sha256,v_source.source_content_sha256);
  if v_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
     and v_source_digest is null then
    raise exception 'CANDIDATE_EVIDENCE_COMPONENT_INVALID' using errcode='55000';
  end if;

  v_core:=jsonb_build_object(
    'workflow_render_input_sha256',v_workflow_input->>'render_input_sha256',
    'component_id',v_component.id,
    'component_kind',v_component.component_kind,
    'document_role',v_component.document_role,
    'expense_category',v_component.expense_category,
    'review_ordinal',v_component.review_ordinal,
    'source_component_id',case
      when v_component.source_component_id is not null then v_component.source_component_id
      when v_source_digest is not null then v_component.id
      else null end,
    'source_content_sha256',case when v_source_digest is null then null else encode(v_source_digest,'hex') end,
    'renderer_contract_version',(select renderer_contract_version
      from public.candidate_submission_workflows where id=p_workflow_id)
  );
  return v_core||jsonb_build_object(
    'render_input_sha256',encode(private._candidate_sha256_jsonb_v1(v_core),'hex')
  );
end;
$function$;

create or replace function private._candidate_review_manifest_v1(
  p_workflow_id uuid,
  p_workflow_generation integer
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_components jsonb;
  v_component_ids jsonb;
  v_manifest_core jsonb;
  v_required_count integer;
  v_ready_count integer;
  v_hours_component_id uuid;
  v_hours_sha256 bytea;
begin
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id and generation=p_workflow_generation;
  if not found then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;

  select
    count(*)::integer,
    count(*) filter (where c.review_render_state='READY')::integer,
    coalesce(jsonb_agg(jsonb_build_object(
      'component_id',c.id,
      'component_kind',c.component_kind,
      'document_role',c.document_role,
      'expense_category',c.expense_category,
      'ordinal',c.review_ordinal,
      'required',c.required,
      'content_sha256',case when c.review_content_sha256 is null then null else encode(c.review_content_sha256,'hex') end,
      'media_type',c.review_media_type,
      'byte_size',c.review_byte_size,
      'page_count',c.review_page_count,
      'render_input_sha256',case when c.review_render_input_sha256 is null then null else encode(c.review_render_input_sha256,'hex') end,
      'renderer_contract_version',c.review_renderer_contract_version,
      'render_state',c.review_render_state
    ) order by c.review_ordinal,c.id),'[]'::jsonb),
    coalesce(jsonb_agg(to_jsonb(c.id) order by c.review_ordinal,c.id),'[]'::jsonb),
    (array_agg(c.id order by c.review_ordinal,c.id)
      filter (where c.component_kind='HOURS_TIMESHEET'))[1],
    (array_agg(c.review_content_sha256 order by c.review_ordinal,c.id)
      filter (where c.component_kind='HOURS_TIMESHEET'))[1]
  into v_required_count,v_ready_count,v_components,v_component_ids,
       v_hours_component_id,v_hours_sha256
  from public.candidate_submission_components c
  where c.workflow_id=p_workflow_id
    and c.workflow_generation=p_workflow_generation
    and c.required=true
    and c.state<>'SUPERSEDED';

  if v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY')
     and v_hours_component_id is null then
    raise exception 'MANAGER_REVIEW_DOCUMENT_NOT_READY' using errcode='55000';
  end if;
  if v_workflow.workflow_kind='CONTRACT_EXPENSE' and v_hours_component_id is not null then
    raise exception 'CONTRACT_EXPENSE_HOURS_COMPONENT_FORBIDDEN' using errcode='55000';
  end if;

  v_manifest_core:=jsonb_build_object(
    'workflow_id',p_workflow_id,
    'workflow_generation',p_workflow_generation,
    'workflow_kind',v_workflow.workflow_kind,
    'required_components',v_components
  );
  return jsonb_build_object(
    'workflow_id',p_workflow_id,
    'workflow_generation',p_workflow_generation,
    'workflow_kind',v_workflow.workflow_kind,
    'all_ready',v_required_count>0 and v_ready_count=v_required_count,
    'required_count',v_required_count,
    'ready_count',v_ready_count,
    'required_component_ids',v_component_ids,
    'required_components',v_components,
    'manager_review_timesheet_component_id',v_hours_component_id,
    'manager_review_timesheet_sha256',case when v_hours_sha256 is null then null else encode(v_hours_sha256,'hex') end,
    'manifest_sha256',encode(private._candidate_sha256_jsonb_v1(v_manifest_core),'hex')
  );
end;
$function$;

create or replace function private._candidate_component_render_contract_v1(
  p_workflow_id uuid,
  p_workflow_generation integer,
  p_component_id uuid,
  p_phase text
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_phase text:=upper(btrim(coalesce(p_phase,'')));
  v_workflow public.candidate_submission_workflows%rowtype;
  v_component public.candidate_submission_components%rowtype;
  v_manager_signature public.candidate_submission_components%rowtype;
  v_component_input jsonb;
  v_form_variant text;
  v_manager jsonb:='null'::jsonb;
  v_candidate_required boolean;
begin
  if v_phase not in ('REVIEW','FINAL') then
    raise exception 'CANDIDATE_RENDER_PHASE_INVALID' using errcode='22023';
  end if;
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id and generation=p_workflow_generation;
  if not found then raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001'; end if;
  select * into v_component from public.candidate_submission_components
  where id=p_component_id and workflow_id=v_workflow.id
    and workflow_generation=v_workflow.generation and required=true and state<>'SUPERSEDED';
  if not found then raise exception 'MANAGER_REVIEW_DOCUMENT_STALE' using errcode='55000'; end if;

  v_candidate_required:=v_component.component_kind='HOURS_TIMESHEET';
  v_form_variant:=case
    when v_phase='REVIEW' and v_candidate_required then 'ELECTRONIC_MANAGER_REVIEW'
    when v_phase='REVIEW' then 'EXPENSE_MANAGER_REVIEW'
    when v_candidate_required then 'ELECTRONIC_SIGNED'
    else 'EXPENSE_MANAGER_SIGNED' end;
  v_component_input:=private._candidate_component_render_input_v1(
    p_workflow_id,p_workflow_generation,p_component_id);

  if v_phase='FINAL' then
    select * into v_manager_signature
    from public.candidate_submission_components
    where id=v_workflow.manager_signature_component_id
      and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation
      and component_kind='MANAGER_SIGNATURE'
      and document_role='MANAGER_SIGNATURE'
      and state='IMMUTABLE';
    if not found
       or v_manager_signature.source_content_sha256 is distinct from v_workflow.manager_signature_sha256
       or v_workflow.manager_approved_at_utc is null then
      raise exception 'MANAGER_SIGNATURE_REQUIRED' using errcode='55000';
    end if;
    v_manager:=jsonb_build_object(
      'name',v_workflow.manager_name,
      'position',v_workflow.manager_position,
      'signature_component_id',v_manager_signature.id,
      'signature_storage_key',v_manager_signature.storage_key,
      'signature_sha256',encode(v_manager_signature.source_content_sha256,'hex'),
      'approval_date_utc',v_workflow.manager_approved_at_utc
    );
  end if;

  return jsonb_build_object(
    'form_variant',v_form_variant,
    'phase',v_phase,
    'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation,
    'workflow_kind',v_workflow.workflow_kind,
    'scope',v_workflow.scope,
    'component_id',v_component.id,
    'component_kind',v_component.component_kind,
    'document_role',v_component.document_role,
    'expense_category',v_component.expense_category,
    'review_ordinal',v_component.review_ordinal,
    'render_input_sha256',v_component_input->>'render_input_sha256',
    'render_input',v_component_input,
    'manager',v_manager,
    'candidate_signature_embedded',v_candidate_required,
    'manager_signature_embedded',v_phase='FINAL',
    'manager_approval_date_embedded',v_phase='FINAL'
  );
end;
$function$;

create or replace function private._candidate_render_contract_v1(
  p_workflow_id uuid,
  p_workflow_generation integer,
  p_form_variant text
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_variant text:=upper(btrim(coalesce(p_form_variant,'')));
  v_phase text;
  v_components jsonb;
begin
  if v_variant in ('ELECTRONIC_MANAGER_REVIEW','EXPENSE_MANAGER_REVIEW','MANAGER_REVIEW') then
    v_phase:='REVIEW';
  elsif v_variant in ('ELECTRONIC_SIGNED','EXPENSE_MANAGER_SIGNED','FINAL_SIGNED') then
    v_phase:='FINAL';
  else
    raise exception 'CANDIDATE_RENDER_VARIANT_INVALID' using errcode='22023';
  end if;

  select coalesce(jsonb_agg(
    private._candidate_component_render_contract_v1(
      p_workflow_id,p_workflow_generation,c.id,v_phase)
    order by c.review_ordinal,c.id
  ),'[]'::jsonb)
  into v_components
  from public.candidate_submission_components c
  where c.workflow_id=p_workflow_id
    and c.workflow_generation=p_workflow_generation
    and c.required=true
    and c.state<>'SUPERSEDED';
  if jsonb_array_length(v_components)=0 then
    raise exception 'MANAGER_REVIEW_DOCUMENT_NOT_READY' using errcode='55000';
  end if;

  return jsonb_build_object(
    'workflow_id',p_workflow_id,
    'workflow_generation',p_workflow_generation,
    'phase',v_phase,
    'components',v_components
  );
end;
$function$;

create or replace function private._candidate_component_immutability_guard_v1()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
begin
  if tg_op='DELETE' and old.immutable_at_utc is not null then
    raise exception 'CANDIDATE_COMPONENT_IMMUTABLE' using errcode='55000';
  end if;
  if tg_op='UPDATE' and old.immutable_at_utc is not null then
    if new.workflow_id is distinct from old.workflow_id
       or new.workflow_generation is distinct from old.workflow_generation
       or new.component_no is distinct from old.component_no
       or new.component_kind is distinct from old.component_kind
       or new.expense_category is distinct from old.expense_category
       or new.document_role is distinct from old.document_role
       or new.source_component_id is distinct from old.source_component_id
       or new.storage_key is distinct from old.storage_key
       or new.media_type is distinct from old.media_type
       or new.byte_size is distinct from old.byte_size
       or new.source_content_sha256 is distinct from old.source_content_sha256
       or new.immutable_at_utc is distinct from old.immutable_at_utc
       or new.required is distinct from old.required
       or new.review_ordinal is distinct from old.review_ordinal
       or new.paper_return_page_key is distinct from old.paper_return_page_key then
      raise exception 'CANDIDATE_COMPONENT_IMMUTABLE' using errcode='55000';
    end if;
    if old.review_render_state='READY' and (
       new.review_storage_key is distinct from old.review_storage_key
       or new.review_content_sha256 is distinct from old.review_content_sha256
       or new.review_media_type is distinct from old.review_media_type
       or new.review_byte_size is distinct from old.review_byte_size
       or new.review_page_count is distinct from old.review_page_count
       or new.review_render_input_sha256 is distinct from old.review_render_input_sha256
       or new.review_renderer_contract_version is distinct from old.review_renderer_contract_version
       or new.review_renderer_receipt_json is distinct from old.review_renderer_receipt_json
       or new.review_generated_at_utc is distinct from old.review_generated_at_utc
       or new.review_render_state not in ('READY','SUPERSEDED')) then
      raise exception 'CANDIDATE_REVIEW_DOCUMENT_IMMUTABLE' using errcode='55000';
    end if;
    if old.final_signed_render_state='READY' and (
       new.final_signed_storage_key is distinct from old.final_signed_storage_key
       or new.final_signed_content_sha256 is distinct from old.final_signed_content_sha256
       or new.final_signed_media_type is distinct from old.final_signed_media_type
       or new.final_signed_byte_size is distinct from old.final_signed_byte_size
       or new.final_signed_page_count is distinct from old.final_signed_page_count
       or new.final_signed_render_input_sha256 is distinct from old.final_signed_render_input_sha256
       or new.final_signed_renderer_contract_version is distinct from old.final_signed_renderer_contract_version
       or new.final_signed_renderer_receipt_json is distinct from old.final_signed_renderer_receipt_json
       or new.final_signed_generated_at_utc is distinct from old.final_signed_generated_at_utc
       or new.final_signed_render_state not in ('READY','SUPERSEDED')) then
      raise exception 'CANDIDATE_FINAL_DOCUMENT_IMMUTABLE' using errcode='55000';
    end if;
  end if;
  if tg_op='DELETE' then return old; end if;
  return new;
end;
$function$;

create or replace function private._candidate_evidence_lineage_guard_v1()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_component public.candidate_submission_components%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_paper_page jsonb;
  v_expected_kind text;
  v_expected_role text;
begin
  if new.candidate_component_id is null then return new; end if;
  select * into v_component from public.candidate_submission_components
  where id=new.candidate_component_id;
  if not found or v_component.state<>'IMMUTABLE' or v_component.immutable_at_utc is null then
    raise exception 'CANDIDATE_COMPONENT_NOT_FINAL_SIGNED' using errcode='55000';
  end if;
  if v_component.component_kind='SIGNED_RETURN' then
    if v_component.storage_key is distinct from new.storage_key then
      raise exception 'CANDIDATE_COMPONENT_NOT_FINAL_SIGNED' using errcode='55000';
    end if;
    select * into v_workflow from public.candidate_submission_workflows
    where id=v_component.workflow_id and generation=v_component.workflow_generation
      and route='PAPER' and paper_return_manifest_sha256 is not null;
    if not found then raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='55000'; end if;
    select expected_page into v_paper_page
    from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
    where expected_page->>'page_key'=v_component.paper_return_page_key;
    if not found then raise exception 'CANDIDATE_PAPER_RETURN_PAGE_NOT_EXPECTED' using errcode='55000'; end if;
  elsif v_component.final_signed_render_state<>'READY'
     or v_component.final_signed_storage_key is distinct from new.storage_key then
    raise exception 'CANDIDATE_COMPONENT_NOT_FINAL_SIGNED' using errcode='55000';
  end if;
  if v_component.timesheet_id is not null and v_component.timesheet_id is distinct from new.timesheet_id then
    raise exception 'CANDIDATE_COMPONENT_TIMESHEET_MISMATCH' using errcode='22023';
  end if;

  if v_component.component_kind='SIGNED_RETURN' then
    v_expected_kind:=case v_paper_page->>'component_kind'
      when 'HOURS_TIMESHEET' then 'TIMESHEET'
      when 'MILEAGE_FORM' then 'MILEAGE'
      when 'EXPENSE_SUMMARY' then 'OTHER'
      when 'EXPENSE_EVIDENCE' then v_paper_page->>'expense_category'
      else null end;
    v_expected_role:=case v_paper_page->>'component_kind'
      when 'HOURS_TIMESHEET' then 'SIGNED_TIMESHEET'
      when 'MILEAGE_FORM' then 'MILEAGE_CLAIM_FORM'
      when 'EXPENSE_SUMMARY' then 'EXPENSE_MILEAGE_APPROVAL_SUMMARY'
      when 'EXPENSE_EVIDENCE' then 'SOURCE_EVIDENCE'
      else null end;
  else
    v_expected_kind:=case v_component.component_kind
      when 'HOURS_TIMESHEET' then 'TIMESHEET'
      when 'MILEAGE_FORM' then 'MILEAGE'
      when 'EXPENSE_SUMMARY' then 'OTHER'
      when 'EXPENSE_EVIDENCE' then v_component.expense_category
      else null end;
    v_expected_role:=case v_component.component_kind
      when 'HOURS_TIMESHEET' then 'SIGNED_TIMESHEET'
      when 'MILEAGE_FORM' then 'MILEAGE_CLAIM_FORM'
      when 'EXPENSE_SUMMARY' then 'EXPENSE_MILEAGE_APPROVAL_SUMMARY'
      when 'EXPENSE_EVIDENCE' then 'SOURCE_EVIDENCE'
      else null end;
  end if;
  if v_expected_kind is null
     or upper(btrim(new.kind))<>v_expected_kind
     or new.document_role<>v_expected_role then
    raise exception 'CANDIDATE_COMPONENT_DOCUMENT_ROLE_MISMATCH' using errcode='22023';
  end if;
  return new;
end;
$function$;

drop trigger if exists candidate_submission_components_immutability_guard
  on public.candidate_submission_components;
create trigger candidate_submission_components_immutability_guard
before update or delete on public.candidate_submission_components
for each row execute function private._candidate_component_immutability_guard_v1();

drop trigger if exists timesheet_evidence_candidate_lineage_guard on public.timesheet_evidence;
create trigger timesheet_evidence_candidate_lineage_guard
before insert or update of candidate_component_id,document_role,timesheet_id,storage_key
on public.timesheet_evidence
for each row execute function private._candidate_evidence_lineage_guard_v1();

revoke all on function private._candidate_sha256_jsonb_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private._candidate_render_input_v1(uuid,integer) from public,anon,authenticated,service_role;
revoke all on function private._candidate_component_render_input_v1(uuid,integer,uuid) from public,anon,authenticated,service_role;
revoke all on function private._candidate_review_manifest_v1(uuid,integer) from public,anon,authenticated,service_role;
revoke all on function private._candidate_component_render_contract_v1(uuid,integer,uuid,text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_render_contract_v1(uuid,integer,text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_component_immutability_guard_v1() from public,anon,authenticated,service_role;
revoke all on function private._candidate_evidence_lineage_guard_v1() from public,anon,authenticated,service_role;
