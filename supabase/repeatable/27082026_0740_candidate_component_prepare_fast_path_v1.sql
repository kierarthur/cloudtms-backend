-- Keep Candidate source-component reservation out of the all-purpose workflow
-- transition.  The public Candidate API operation is unchanged; this narrow
-- service-only RPC still authenticates the Candidate session, locks the exact
-- workflow, enforces generation/state/type/idempotency authority and records
-- the existing mutation receipt before it returns an upload contract.

create or replace function public.candidate_component_prepare_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_payload jsonb default '{}'::jsonb,
  p_idempotency_key text default null,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_context jsonb;
  v_account_id uuid;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_component public.candidate_submission_components%rowtype;
  v_source_component public.candidate_submission_components%rowtype;
  v_component_kind text;
  v_document_role text;
  v_expense_category text;
  v_paper_page_key text;
  v_requested_media_type text;
  v_requested_byte_size bigint;
  v_component_no integer;
  v_mutation_semantic_payload jsonb;
  v_mutation_request_sha256 text;
  v_mutation_receipt jsonb;
  v_response jsonb;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_session_id is null or p_workflow_id is null
     or jsonb_typeof(v_payload)<>'object' then
    raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';
  end if;
  if v_payload ?| array['password','refresh_token','token'] then
    raise exception 'CANDIDATE_WORKFLOW_PLAINTEXT_SECRET_FORBIDDEN' using errcode='22023';
  end if;
  if p_expected_generation is null or p_expected_generation<1 then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;
  if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023';
  end if;

  perform private._candidate_require_feature_v1(v_environment,'candidate_app_writes');
  v_context:=private._candidate_session_context_v1(
    p_session_id,v_environment,null,p_now_utc,true
  );
  v_account_id:=nullif(v_context->>'account_id','')::uuid;
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then
    raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
  end if;

  select * into v_workflow
  from public.candidate_submission_workflows
  where id=p_workflow_id
    and environment=v_environment
    and account_id=v_account_id
    and candidate_id=v_candidate_id
  for update;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if nullif(btrim(coalesce(v_workflow.idempotency_key,'')),'')=btrim(p_idempotency_key) then
    raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_IDEMPOTENCY_CONFLICT',
        'workflow_id',v_workflow.id,
        'idempotency_key',btrim(p_idempotency_key),
        'reason','CREATION_KEY_REUSED_FOR_MUTATION'
      )::text;
  end if;

  v_mutation_semantic_payload:=v_payload-'storage_key';
  v_mutation_request_sha256:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','CANDIDATE_WORKFLOW_MUTATION_REQUEST_V1',
    'workflow_id',v_workflow.id,
    'action','COMPONENT_PREPARE',
    'expected_generation',p_expected_generation,
    'payload',v_mutation_semantic_payload,
    'channel','CANDIDATE_CLIENT',
    'actor_identity','ACCOUNT:'||v_account_id::text||':CANDIDATE:'||v_candidate_id::text
  )::text,'UTF8'),'sha256'),'hex');
  v_mutation_receipt:=private._candidate_workflow_mutation_receipt_v1(
    v_workflow.id,btrim(p_idempotency_key),v_mutation_request_sha256,
    'COMPONENT_PREPARE','CANDIDATE_CLIENT',
    'ACCOUNT:'||v_account_id::text||':CANDIDATE:'||v_candidate_id::text,
    null,p_now_utc
  );
  if coalesce((v_mutation_receipt->>'found')::boolean,false) then
    return v_mutation_receipt->'response';
  end if;

  if v_workflow.generation<>p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','WORKFLOW_GENERATION_CONFLICT','current_generation',v_workflow.generation
      )::text;
  end if;
  if v_workflow.state in ('FINALISED','CANCELLED','REJECTED','SUPERSEDED') then
    raise exception 'CANDIDATE_WORKFLOW_NOT_MUTABLE' using errcode='55000';
  end if;

  v_component_kind:=upper(btrim(coalesce(v_payload->>'component_kind','')));
  v_document_role:=upper(btrim(coalesce(v_payload->>'document_role','')));
  v_expense_category:=nullif(upper(btrim(coalesce(v_payload->>'expense_category',''))),'');
  v_paper_page_key:=nullif(btrim(coalesce(v_payload->>'paper_return_page_key','')),'');
  v_requested_media_type:=nullif(lower(btrim(coalesce(v_payload->>'media_type',''))),'');
  begin
    v_requested_byte_size:=nullif(v_payload->>'byte_size','')::bigint;
  exception when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'CANDIDATE_COMPONENT_SIZE_INVALID' using errcode='22023';
  end;

  select * into v_component
  from public.candidate_submission_components
  where workflow_id=v_workflow.id
    and upload_idempotency_key=btrim(p_idempotency_key);
  if found then
    if v_component.workflow_generation is distinct from v_workflow.generation then
      raise exception 'CANDIDATE_COMPONENT_PREPARE_GENERATION_CONFLICT' using errcode='40001';
    end if;
    if v_component.state not in ('PENDING','IMMUTABLE') then
      raise exception 'CANDIDATE_COMPONENT_PREPARE_STATE_CONFLICT' using errcode='55000';
    end if;
    if v_component.component_kind is distinct from v_component_kind
       or v_component.document_role is distinct from v_document_role
       or v_component.expense_category is distinct from v_expense_category
       or lower(v_component.media_type) is distinct from v_requested_media_type
       or v_component.byte_size is distinct from v_requested_byte_size
       or v_component.manager_signature_capture_method is not null
       or v_component.expected_source_content_sha256 is not null
       or v_component.paper_return_page_key is distinct from v_paper_page_key then
      raise exception 'CANDIDATE_COMPONENT_PREPARE_IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    v_response:=jsonb_build_object(
      'ok',true,'idempotent_replay',true,
      'component_id',v_component.id,'component_no',v_component.component_no,
      'workflow_generation',v_component.workflow_generation,
      'storage_key',v_component.storage_key,'media_type',v_component.media_type,
      'byte_size',v_component.byte_size,'component_kind',v_component.component_kind,
      'document_role',v_component.document_role,'expense_category',v_component.expense_category,
      'paper_return_page_key',v_component.paper_return_page_key,'state',v_component.state
    );
    perform private._candidate_workflow_mutation_receipt_v1(
      v_workflow.id,btrim(p_idempotency_key),v_mutation_request_sha256,
      'COMPONENT_PREPARE','CANDIDATE_CLIENT',
      'ACCOUNT:'||v_account_id::text||':CANDIDATE:'||v_candidate_id::text,
      v_response,p_now_utc
    );
    return v_response;
  end if;

  if not (
    (v_component_kind='CANDIDATE_SIGNATURE'
      and v_document_role='CANDIDATE_SIGNATURE' and v_expense_category is null)
    or (v_component_kind='MILEAGE_FORM'
      and v_document_role='MILEAGE_CLAIM_FORM' and v_expense_category='MILEAGE')
    or (v_component_kind='EXPENSE_EVIDENCE'
      and v_document_role='SOURCE_EVIDENCE'
      and v_expense_category in ('TRAVEL','ACCOMMODATION','OTHER','MILEAGE'))
    or (v_component_kind='SIGNED_RETURN'
      and v_document_role='SIGNED_RETURN' and v_expense_category is null)
  ) then
    raise exception 'CANDIDATE_COMPONENT_TYPE_INVALID' using errcode='22023';
  end if;
  if v_workflow.state<>'WORKER_DRAFT'
     and v_component_kind in ('CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE') then
    raise exception 'CANDIDATE_COMPONENT_AMENDMENT_REQUIRED' using errcode='55000';
  end if;
  if v_component_kind='SIGNED_RETURN' then
    if v_workflow.route<>'PAPER' or v_workflow.state<>'AWAITING_PAPER_RETURN'
       or v_workflow.paper_return_manifest_sha256 is null
       or not exists(
         select 1
         from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') page
         where page->>'page_key'=v_paper_page_key
       ) then
      raise exception 'CANDIDATE_PAPER_RETURN_PAGE_NOT_EXPECTED' using errcode='22023';
    end if;
  elsif v_paper_page_key is not null then
    raise exception 'CANDIDATE_PAPER_RETURN_PAGE_KEY_FORBIDDEN' using errcode='22023';
  end if;

  if nullif(v_payload->>'source_component_id','') is not null then
    select source_component.* into v_source_component
    from public.candidate_submission_components source_component
    join public.candidate_submission_workflows source_workflow
      on source_workflow.id=source_component.workflow_id
    where source_component.id=(v_payload->>'source_component_id')::uuid
      and source_component.state in ('IMMUTABLE','SUPERSEDED','REJECTED')
      and source_component.immutable_at_utc is not null
      and source_component.source_content_sha256 is not null
      and source_component.source_component_id is null
      and source_workflow.environment=v_environment
      and source_workflow.account_id=v_account_id
      and source_workflow.candidate_id=v_candidate_id
      and (
        source_workflow.id=v_workflow.id
        or (
          source_workflow.contract_id is not distinct from v_workflow.contract_id
          and source_workflow.week_ending_date is not distinct from v_workflow.week_ending_date
          and source_workflow.state in ('CANCELLED','REJECTED','REFUSED','SUPERSEDED')
        )
      );
    if not found or v_source_component.component_kind<>v_component_kind
       or v_source_component.document_role<>v_document_role
       or v_source_component.expense_category is distinct from v_expense_category then
      raise exception 'CANDIDATE_SOURCE_COMPONENT_NOT_ALLOWED' using errcode='28000';
    end if;
  end if;

  select coalesce(max(component_no),0)+1 into v_component_no
  from public.candidate_submission_components
  where workflow_id=v_workflow.id
    and workflow_generation=v_workflow.generation;
  insert into public.candidate_submission_components(
    workflow_id,workflow_generation,component_no,approval_request_id,timesheet_id,
    component_kind,expense_category,document_role,state,source_component_id,
    storage_key,media_type,byte_size,source_content_sha256,upload_idempotency_key,
    immutable_at_utc,required,review_ordinal,review_render_state,
    final_signed_render_state,paper_return_page_key,
    manager_signature_capture_method,expected_source_content_sha256,created_at_utc
  ) values (
    v_workflow.id,v_workflow.generation,v_component_no,null,v_workflow.target_timesheet_id,
    v_component_kind,v_expense_category,v_document_role,
    case when v_source_component.id is null then 'PENDING' else 'IMMUTABLE' end,
    v_source_component.id,
    coalesce(v_source_component.storage_key,nullif(v_payload->>'storage_key','')),
    coalesce(v_source_component.media_type,v_requested_media_type),
    coalesce(v_source_component.byte_size,v_requested_byte_size),
    v_source_component.source_content_sha256,btrim(p_idempotency_key),
    case when v_source_component.id is null then null else p_now_utc end,
    false,null,'NOT_REQUIRED','NOT_REQUIRED',v_paper_page_key,null,null,p_now_utc
  ) returning * into v_component;
  v_response:=jsonb_build_object(
    'ok',true,'idempotent_replay',false,
    'component_id',v_component.id,'component_no',v_component.component_no,
    'workflow_generation',v_component.workflow_generation,
    'storage_key',v_component.storage_key,'media_type',v_component.media_type,
    'byte_size',v_component.byte_size,'component_kind',v_component.component_kind,
    'document_role',v_component.document_role,'expense_category',v_component.expense_category,
    'paper_return_page_key',v_component.paper_return_page_key,'state',v_component.state
  );
  perform private._candidate_workflow_mutation_receipt_v1(
    v_workflow.id,btrim(p_idempotency_key),v_mutation_request_sha256,
    'COMPONENT_PREPARE','CANDIDATE_CLIENT',
    'ACCOUNT:'||v_account_id::text||':CANDIDATE:'||v_candidate_id::text,
    v_response,p_now_utc
  );
  return v_response;
end;
$function$;

revoke all on function public.candidate_component_prepare_atomic_v1(
  uuid,text,uuid,integer,jsonb,text,timestamptz
) from public,anon,authenticated;
grant execute on function public.candidate_component_prepare_atomic_v1(
  uuid,text,uuid,integer,jsonb,text,timestamptz
) to service_role;

-- The service-only RPC is called through PostgREST. Refresh the schema cache in
-- the same transaction so the newly installed signature is immediately usable.
notify pgrst, 'reload schema';
