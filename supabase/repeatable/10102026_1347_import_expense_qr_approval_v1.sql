-- Repeatable CloudTMS function/view authority: import_expense_qr_approval_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Purpose-scoped permission. Hours authority and financial snapshots remain unchanged.
create or replace function private._expense_approval_policy_v1(
  p_client_id uuid,p_contract_id uuid,p_evaluation_date date,p_workflow_kind text
) returns jsonb language plpgsql stable security definer
set search_path = pg_catalog,public,private,extensions,pg_temp
as $function$
declare
  v_policy jsonb;
  v_settings_id uuid;
  v_override boolean;
  v_client_enabled boolean;
  v_authority jsonb;
begin
  v_policy:=private._candidate_policy_resolve_v1(p_client_id,p_contract_id,p_evaluation_date);
  if p_workflow_kind is distinct from 'CONTRACT_EXPENSE'
     or v_policy->>'paper_submission_enabled_source' is distinct from 'IMPORT_DISABLED' then
    return v_policy;
  end if;
  v_authority:=private._contract_settings_effective_core_v1(
    p_client_id,p_contract_id,p_evaluation_date,'WEEKLY',null);
  if not coalesce((v_authority#>>'{applicability,configuration_valid}')::boolean,false) then
    return v_policy;
  end if;
  v_settings_id:=nullif(v_policy->>'client_settings_id','')::uuid;
  select c.candidate_paper_submission_enabled_override into v_override
  from public.contracts c where c.id=p_contract_id and c.client_id=p_client_id;
  select s.candidate_paper_submission_enabled into v_client_enabled
  from public.client_settings s where s.id=v_settings_id and s.client_id=p_client_id;
  v_policy:=(v_policy-'policy_fingerprint')||jsonb_build_object(
    'paper_submission_enabled',coalesce(v_override,v_client_enabled,false),
    'paper_submission_enabled_source',case when v_override is not null then 'CONTRACT' else 'CLIENT' end);
  return v_policy||jsonb_build_object('policy_fingerprint',
    encode(digest(convert_to(v_policy::text,'UTF8'),'sha256'),'hex'));
end;
$function$;
alter function private._expense_approval_policy_v1(uuid,uuid,date,text) owner to postgres;
revoke all on function private._expense_approval_policy_v1(uuid,uuid,date,text)
  from public,anon,authenticated,service_role;

create or replace function private._expense_approval_route_v1(
  p_timesheet_id uuid,p_contract_week_id uuid,p_workflow_kind text
) returns jsonb language plpgsql stable security definer
set search_path = pg_catalog,public,private,pg_temp
as $function$
declare
  v_route jsonb;
  v_policy jsonb;
  v_client_id uuid;
  v_contract_id uuid;
  v_date date;
begin
  v_route:=private._candidate_route_family_v1(p_timesheet_id,p_contract_week_id);
  if p_workflow_kind is distinct from 'CONTRACT_EXPENSE' then return v_route; end if;
  select c.client_id,c.id,w.week_ending_date into v_client_id,v_contract_id,v_date
  from public.contract_weeks w join public.contracts c on c.id=w.contract_id
  where w.id=p_contract_week_id;
  if not found then return v_route; end if;
  v_policy:=private._expense_approval_policy_v1(v_client_id,v_contract_id,v_date,p_workflow_kind);
  return v_route||jsonb_build_object(
    'candidate_paper_submission_allowed',
      coalesce((v_route->>'candidate_expenses_allowed')::boolean,false)
      and v_route->>'route_family' in ('ELECTRONIC','QR','IMPORT_AUTHORITATIVE')
      and coalesce((v_policy->>'paper_submission_enabled')::boolean,false),
    'policy',v_policy);
end;
$function$;
alter function private._expense_approval_route_v1(uuid,uuid,text) owner to postgres;
revoke all on function private._expense_approval_route_v1(uuid,uuid,text)
  from public,anon,authenticated,service_role;


create or replace function public.candidate_workflow_transition_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_action text,
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
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_context jsonb;
  v_account_id uuid;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_source_workflow public.candidate_submission_workflows%rowtype;
  v_existing_workflow public.candidate_submission_workflows%rowtype;
  v_contract public.contracts%rowtype;
  v_week public.contract_weeks%rowtype;
  v_anchor_week public.contract_weeks%rowtype;
  v_anchor_timesheet public.timesheets%rowtype;
  v_daily_timesheet public.timesheets%rowtype;
  v_daily_fin public.timesheets_financials%rowtype;
  v_daily_receipt_context jsonb;
  v_daily_first_source boolean:=false;
  v_policy jsonb;
  v_approval public.candidate_approval_requests%rowtype;
  v_component public.candidate_submission_components%rowtype;
  v_signature_component public.candidate_submission_components%rowtype;
  v_email_check jsonb;
  v_token_hash bytea;
  v_digest bytea;
  v_submission_hash bytea;
  v_render_input_hash bytea;
  v_manifest_hash bytea;
  v_mail_id uuid;
  v_response jsonb;
  v_manifest jsonb;
  v_render_contract jsonb;
  v_receipt jsonb;
  v_immutable_submission jsonb;
  v_expense_submission jsonb;
  v_component_ids uuid[];
  v_method text;
  v_next_generation integer;
  v_component_no integer;
  v_request_generation integer;
  v_reviewed_count integer;
  v_required_count integer;
  v_constraint_name text;
  v_is_service_action boolean;
  v_is_public_manager_action boolean;
  v_is_electronic boolean;
  v_component_kind text;
  v_document_role text;
  v_expense_category text;
  v_requested_media_type text;
  v_requested_byte_size bigint;
  v_manager_capture_method text;
  v_expected_source_digest bytea;
  v_verified_image_width integer;
  v_verified_image_height integer;
  v_review_ordinal integer;
  v_has_expenses boolean:=false;
  v_has_mileage boolean:=false;
  v_expense_value numeric:=0;
  v_required_categories text[]:=array[]::text[];
  v_required_category text;
  v_paper_manifest jsonb;
  v_paper_source_pages jsonb;
  v_paper_mileage_only boolean:=false;
  v_paper_page_key text;
  v_paper_timesheet_id uuid;
  v_paper_pack_result jsonb;
  v_paper_retirement_result jsonb;
  v_paper_mail public.mail_outbox%rowtype;
  v_paper_mail_id uuid;
  v_paper_manifest_sha256 text;
  v_paper_pack_storage_key text;
  v_paper_pack_sha256 text;
  v_paper_pack_media_type text;
  v_paper_pack_byte_size bigint;
  v_paper_pack_page_count integer;
  v_paper_pack_attachment jsonb;
  v_paper_notification_id uuid;
  v_paper_release_idempotent boolean:=false;
  v_paper_expense_update_active boolean:=false;
  v_paper_outbox_count integer:=0;
  v_paper_base_document_sha256 text;
  v_paper_branding_contract_sha256 text;
  v_paper_renderer_contract_version text;
  v_paper_expected_storage_key text;
  v_provider_lease_token text;
  v_provider_permit_expires_at timestamptz;
  v_manager_mail public.mail_outbox%rowtype;
  v_manager_route_receipt public.candidate_manager_email_route_receipts%rowtype;
  v_manager_mail_kind text;
  v_manager_provider_accepted_at timestamptz;
  v_manager_pending_mail_count integer:=0;
  v_manager_request_ids uuid[]:=array[]::uuid[];
  v_manager_withdrawal_request_id uuid;
  v_manager_withdrawal_count integer:=0;
  v_manager_retirement_result jsonb;
  v_submission_withdrawal_reset jsonb;
  v_cancel_reason text;
  v_cancel_reason_code text;
  v_audit_reason text;
  v_unlocked_workflow_updated_at timestamptz;
  v_paper_family_key text;
  v_source_component public.candidate_submission_components%rowtype;
  v_all_final_ready boolean:=false;
  v_server_issue_codes jsonb:='[]'::jsonb;
  v_duplicate_expense_review jsonb:='{}'::jsonb;
  v_duplicate_expense_category text;
  v_request_id uuid;
  v_workflow_kind text;
  v_scope text;
  v_route text;
  v_canonical_work_date date;
  v_canonical_week_ending_date date;
  v_client_id uuid;
  v_anchor_candidate_count integer:=0;
  v_anchor_week_id uuid;
  v_anchor_submitted_work boolean:=false;
  v_target_capabilities jsonb;
  v_route_authority jsonb;
  v_weekly_source_candidate_request_allowed boolean:=false;
  v_daily_input jsonb;
  v_daily_patch jsonb;
  v_expected_save_hash bytea;
  v_daily_context jsonb;
  v_daily_context_hash bytea;
  v_current_row_signature text;
  v_insert_workflow_id uuid;
  v_replacement_of_workflow_id uuid;
  v_is_rejected_resubmission boolean:=false;
  v_creation_request_identity jsonb;
  v_creation_identity jsonb;
  v_creation_request_sha256 bytea;
  v_initial_route text;
  v_source_anchor public.timesheets%rowtype;
  v_daily_booking_id text;
  v_paper_failure_code text;
  v_paper_failure_class text;
  v_paper_failure_retryable boolean:=false;
  v_paper_pack_attempt_token text;
  v_paper_pack_attempt_count integer:=0;
  v_paper_pack_attempt_expires_at timestamptz;
  v_paper_pack_next_retry_at timestamptz;
  v_paper_pack_operation_id text;
  v_office_actor_user_id uuid;
  v_is_office_service_action boolean:=false;
  v_is_internal_paper_service_action boolean:=false;
  v_mutation_channel text;
  v_mutation_actor_identity text;
  v_mutation_semantic_payload jsonb;
  v_mutation_request_sha256 text;
  v_mutation_receipt jsonb;
  v_mutation_replay_probe_only boolean:=false;
  v_expense_update_context jsonb;
  v_pending_expense_update public.candidate_pending_expense_updates%rowtype;
  v_is_pending_expense_update boolean:=false;
  v_replaced_manager_signature_ids uuid[]:=array[]::uuid[];
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_workflow_id is null or jsonb_typeof(v_payload)<>'object' then
    raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';
  end if;
  if v_payload ?| array['password','refresh_token','token'] then
    raise exception 'CANDIDATE_WORKFLOW_PLAINTEXT_SECRET_FORBIDDEN' using errcode='22023';
  end if;

  if v_action='SELECT_APPROVAL_METHOD' then
    v_method:=upper(coalesce(v_payload->>'method',''));
    v_action:=case v_method
      when 'PHONE' then 'SELECT_PHONE_APPROVAL'
      when 'EMAIL' then 'CREATE_EMAIL_APPROVAL_REQUEST'
      when 'PAPER' then 'PAPER_PREPARE'
      else v_action end;
  elsif v_action='EMAIL_REQUEST' then
    v_action:='CREATE_EMAIL_APPROVAL_REQUEST';
  elsif v_action='MANAGER_REVIEW' then
    v_action:='BEGIN_MANAGER_REVIEW';
  elsif v_action='REGISTER_MANAGER_REVIEW_DOCUMENT' then
    v_action:='REGISTER_REVIEW_COMPONENT';
  end if;
  v_mutation_replay_probe_only:=p_session_id is not null
    and coalesce((v_payload->>'mutation_replay_probe_only')::boolean,false);
  if v_mutation_replay_probe_only
     and jsonb_typeof(v_payload->'mutation_replay_semantic_payload') is distinct from 'object' then
    raise exception 'CANDIDATE_IDEMPOTENCY_RECEIPT_INVALID' using errcode='22023';
  end if;

  if coalesce(v_payload->>'actor_user_id','')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
    v_office_actor_user_id:=(v_payload->>'actor_user_id')::uuid;
  end if;
  v_is_office_service_action:=p_session_id is null
    and coalesce((v_payload->>'service_office_action')::boolean,false)
    and private._candidate_office_service_context_valid_v1(
      v_environment,v_office_actor_user_id,v_action
    );
  v_is_internal_paper_service_action:=p_session_id is null and (
    (v_action='PAPER_PACK_RELEASE'
      and coalesce((v_payload->>'service_paper_pack_release')::boolean,false))
    or (v_action='PAPER_PROVIDER_SUBMIT_PERMIT'
      and coalesce((v_payload->>'service_paper_provider_submit_permit')::boolean,false))
    or (v_action='PAPER_PACK_MARK_FAILURE'
      and coalesce((v_payload->>'service_paper_pack_failure')::boolean,false))
    or (v_action='PAPER_PACK_ATTEMPT_CLAIM'
      and coalesce((v_payload->>'service_paper_pack_attempt')::boolean,false))
  );
  if not v_is_office_service_action and not v_is_internal_paper_service_action then
    perform private._candidate_require_feature_v1(v_environment,'candidate_app_writes');
  end if;

  if v_action in (
    'SELECT_PHONE_APPROVAL','CREATE_EMAIL_APPROVAL_REQUEST','BEGIN_MANAGER_REVIEW',
    'RECORD_REVIEW_PROGRESS','PHONE_APPROVE','EMAIL_APPROVE','MANAGER_REFUSE',
    'REMIND','RENEW','MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF',
    'REGISTER_REVIEW_COMPONENT','REGISTER_FINAL_SIGNED_DOCUMENT',
    'MANAGER_PROVIDER_SUBMIT_PERMIT'
  ) or (p_session_id is null and v_action in ('COMPONENT_PREPARE','COMPONENT_COMPLETE')) then
    if not v_is_office_service_action and not v_is_internal_paper_service_action then
    perform private._candidate_require_feature_v1(v_environment,'candidate_manager_approval');
    end if;
  end if;
  if v_action='BEGIN_CANONICAL_DAILY_SAVE' then
    if not v_is_office_service_action then
    perform private._candidate_require_feature_v1(v_environment,'candidate_daily_finalisation');
    end if;
  end if;
  if v_action in (
    'PAPER_PREPARE','PAPER_RETURN','PAPER_PACK_RELEASE',
    'PAPER_PROVIDER_SUBMIT_PERMIT','PAPER_PACK_MARK_FAILURE','PAPER_PACK_ATTEMPT_CLAIM'
  ) then
    if not v_is_office_service_action and not v_is_internal_paper_service_action then
    perform private._candidate_require_feature_v1(v_environment,'candidate_paper_qr');
    end if;
  end if;
  if v_action='MARK_READ' then
    perform private._candidate_require_feature_v1(v_environment,'candidate_notifications');
  end if;

  v_is_service_action:=v_action in (
    'REGISTER_REVIEW_COMPONENT','REGISTER_FINAL_SIGNED_DOCUMENT',
    'BEGIN_CANONICAL_DAILY_SAVE'
  ) or (p_session_id is null
    and v_action='PAPER_PACK_RELEASE'
    and coalesce((v_payload->>'service_paper_pack_release')::boolean,false)
  ) or (p_session_id is null
    and v_action='PAPER_PROVIDER_SUBMIT_PERMIT'
    and coalesce((v_payload->>'service_paper_provider_submit_permit')::boolean,false)
  ) or (p_session_id is null
    and v_action='PAPER_PACK_MARK_FAILURE'
    and coalesce((v_payload->>'service_paper_pack_failure')::boolean,false)
  ) or (p_session_id is null
    and v_action='PAPER_PACK_ATTEMPT_CLAIM'
    and coalesce((v_payload->>'service_paper_pack_attempt')::boolean,false)
  ) or (p_session_id is null
    and v_action='MANAGER_PROVIDER_SUBMIT_PERMIT'
    and coalesce((v_payload->>'service_manager_provider_submit_permit')::boolean,false)
  ) or (p_session_id is null
    and coalesce((v_payload->>'service_phone_approval')::boolean,false)
    and v_action in ('BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','MANAGER_REFUSE',
      'COMPONENT_PREPARE','COMPONENT_COMPLETE'))
  or (v_is_office_service_action
    and v_action in ('CANCEL','REMIND','RENEW','MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF',
      'BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','MANAGER_REFUSE',
      'REGISTER_REVIEW_COMPONENT','REGISTER_FINAL_SIGNED_DOCUMENT',
      'BEGIN_CANONICAL_DAILY_SAVE','PAPER_PACK_RELEASE','PAPER_PACK_ATTEMPT_CLAIM',
      'PAPER_PACK_MARK_FAILURE','WORKER_SUBMIT'));
  v_is_public_manager_action:=not v_is_service_action and p_session_id is null and v_action in (
    'BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','EMAIL_APPROVE','MANAGER_REFUSE',
    'COMPONENT_PREPARE','COMPONENT_COMPLETE'
  );

  if v_is_service_action then
    if v_action='PAPER_PACK_RELEASE' then
      select * into v_workflow
      from public.candidate_submission_workflows
      where id=p_workflow_id and environment=v_environment;
      if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
      v_paper_timesheet_id:=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id);
      if v_paper_timesheet_id is null then
        raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
      end if;
      perform 1 from public.timesheets
      where timesheet_id=v_paper_timesheet_id and is_current=true and archived_at_utc is null
      for update;
      if not found then raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000'; end if;
      select * into v_workflow
      from public.candidate_submission_workflows
      where id=p_workflow_id and environment=v_environment
      for update;
      if not found
         or coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
              is distinct from v_paper_timesheet_id then
        raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
      end if;
    else
      select * into v_workflow
      from public.candidate_submission_workflows
      where id=p_workflow_id and environment=v_environment
      for update;
    end if;
    if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
    if v_is_office_service_action
       and v_action in ('REMIND','RENEW','MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF',
         'BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','MANAGER_REFUSE') then
      if coalesce(v_payload->>'approval_request_id','')
           !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
         or coalesce(v_payload->>'approval_request_generation','') !~ '^[1-9][0-9]{0,8}$' then
        raise exception 'CANDIDATE_REQUEST_GENERATION_STALE' using errcode='40001';
      end if;
      perform 1
      from public.candidate_approval_requests office_request
      where office_request.id=(v_payload->>'approval_request_id')::uuid
        and office_request.workflow_id=v_workflow.id
        and office_request.workflow_generation=v_workflow.generation
        and office_request.request_generation=(v_payload->>'approval_request_generation')::integer;
      if not found then
        raise exception 'CANDIDATE_REQUEST_GENERATION_STALE' using errcode='40001';
      end if;
    end if;
    v_account_id:=v_workflow.account_id;
    v_candidate_id:=v_workflow.candidate_id;
  elsif v_is_public_manager_action then
    if coalesce(v_payload->>'approval_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
    end if;
    v_token_hash:=decode(v_payload->>'approval_token_hash_hex','hex');
    select a.id,a.workflow_id into v_request_id,v_workflow.id
    from public.candidate_approval_requests a
    where a.token_hash=v_token_hash and a.workflow_id=p_workflow_id;
    if not found then raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000'; end if;
    select * into v_workflow
    from public.candidate_submission_workflows
    where id=p_workflow_id and environment=v_environment
    for update;
    if not found then raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000'; end if;
    select * into v_approval
    from public.candidate_approval_requests
    where id=v_request_id
      and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation
      and method in ('EMAIL','PHONE')
      and state='PENDING'
      and expires_at_utc>p_now_utc
      and review_manifest_sha256=v_workflow.review_manifest_sha256
    for update;
    if not found then raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000'; end if;
    v_account_id:=v_workflow.account_id;
    v_candidate_id:=v_workflow.candidate_id;
  else
    v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,true);
    v_account_id:=nullif(v_context->>'account_id','')::uuid;
    v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
    if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;
  end if;

  if v_action='RESUBMIT_REJECTED' then
    if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null
       or btrim(p_idempotency_key) !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
      raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023';
    end if;
    if p_expected_generation is null or p_expected_generation<1 then
      raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='55000';
    end if;

    v_creation_request_identity:=jsonb_build_object(
      'version','CANDIDATE_WORKFLOW_CREATION_REQUEST_V1',
      'action','RESUBMIT_REJECTED',
      'environment',v_environment,
      'account_id',v_account_id,
      'candidate_id',v_candidate_id,
      'rejected_workflow_id',p_workflow_id,
      'rejected_workflow_generation',p_expected_generation
    );
    v_creation_request_sha256:=private._candidate_workflow_creation_request_sha256_v1(
      v_creation_request_identity
    );

    -- One account/key lock serialises exact retries. The source lock then
    -- serialises every key that attempts to replace the same rejected row.
    perform pg_advisory_xact_lock(hashtextextended(
      'candidate-workflow-idempotency|'||v_account_id::text||'|'||btrim(p_idempotency_key),0
    ));
    perform pg_advisory_xact_lock(hashtextextended(
      'candidate-rejected-source|'||p_workflow_id::text,0
    ));

    select * into v_source_workflow
    from public.candidate_submission_workflows
    where id=p_workflow_id
      and environment=v_environment
      and account_id=v_account_id
      and candidate_id=v_candidate_id
    for update;
    if not found then
      raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
    end if;
    if v_source_workflow.generation<>p_expected_generation then
      raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='55000';
    end if;
    if v_source_workflow.state not in ('REJECTED','REFUSED') then
      raise exception 'CANDIDATE_REJECTED_WORKFLOW_NOT_RESUBMITTABLE' using errcode='55000';
    end if;

    select * into v_existing_workflow
    from public.candidate_submission_workflows
    where account_id=v_account_id
      and idempotency_key=btrim(p_idempotency_key)
    for update;
    if found then
      if v_existing_workflow.replacement_of_workflow_id is distinct from v_source_workflow.id
         or v_existing_workflow.candidate_id is distinct from v_source_workflow.candidate_id
         or v_existing_workflow.creation_request_sha256 is distinct from v_creation_request_sha256 then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='55000';
      end if;
      return jsonb_build_object(
        'ok',true,'idempotent_replay',true,
        'rejected_workflow_id',v_source_workflow.id,
        'replacement_workflow_id',v_existing_workflow.id,
        'replacement_created',false,
        'workflow_id',v_existing_workflow.id,
        'state',v_existing_workflow.state,
        'generation',v_existing_workflow.generation
      );
    end if;

    select * into v_existing_workflow
    from public.candidate_submission_workflows
    where replacement_of_workflow_id=v_source_workflow.id
    for update;
    if found or private._candidate_rejection_replaced_v1(v_source_workflow.id) then
      raise exception 'CANDIDATE_REJECTED_WORKFLOW_ALREADY_REPLACED'
        using errcode='55000',detail=case when v_existing_workflow.id is null then null
          else jsonb_build_object('replacement_workflow_id',v_existing_workflow.id)::text end;
    end if;

    v_is_rejected_resubmission:=true;
    v_replacement_of_workflow_id:=v_source_workflow.id;
    v_insert_workflow_id:=gen_random_uuid();
    if v_source_workflow.workflow_kind='DAILY' then
      v_daily_booking_id:=nullif(btrim(coalesce(
        v_source_workflow.creation_identity_json#>>'{derived,daily_booking_id}',
        ''
      )), '');
      if v_daily_booking_id is null then
        select nullif(btrim(coalesce(source_timesheet.booking_id,'')),'')
        into v_daily_booking_id
        from public.timesheets source_timesheet
        where source_timesheet.timesheet_id=coalesce(
          v_source_workflow.target_timesheet_id,v_source_workflow.anchor_timesheet_id
        );
      end if;
      select current_timesheet.* into v_daily_timesheet
      from public.timesheets current_timesheet
      where current_timesheet.booking_id=v_daily_booking_id
        and current_timesheet.is_current=true
        and current_timesheet.archived_at_utc is null
        and current_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum;
      if not found then
        raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002';
      end if;
      v_workflow_kind:='DAILY';
      v_scope:='DAILY';
      v_initial_route:=upper(coalesce(
        v_source_workflow.creation_identity_json#>>'{derived,initial_route}',
        v_source_workflow.route
      ));
      if v_initial_route not in ('EMAIL','PHONE') then
        raise exception 'CANDIDATE_REJECTED_REPLACEMENT_ROUTE_INVALID' using errcode='55000';
      end if;
      v_payload:=jsonb_build_object(
        'workflow_kind',v_workflow_kind,
        'scope',v_scope,
        'route',v_initial_route,
        'target_timesheet_id',v_daily_timesheet.timesheet_id
      );
    else
      v_workflow_kind:=case
        when v_source_workflow.rejection_scope='COMPLETE_EXPENSE_CLAIM'
          or v_source_workflow.workflow_kind='CONTRACT_EXPENSE'
          then 'CONTRACT_EXPENSE'
        when v_source_workflow.workflow_kind='CONTRACT_COMBINED'
          then 'CONTRACT_COMBINED'
        else 'CONTRACT_HOURS'
      end;
      v_scope:='WEEKLY';

      -- Refusal can return the exact Contract Week to an empty editable state:
      -- the refused Timesheet remains revoked history and timesheet_id becomes
      -- null.  The Contract Week is therefore the stable weekly authority.
      -- When it does own a current Timesheet, use only that exact row; never
      -- adopt another same-contract/date carrier.
      select current_week.* into v_week
      from public.contract_weeks current_week
      where current_week.id=v_source_workflow.contract_week_id
        and current_week.contract_id is not distinct from v_source_workflow.contract_id
        and current_week.week_ending_date is not distinct from v_source_workflow.week_ending_date
      for update;
      if not found then
        raise exception 'CANDIDATE_WORKFLOW_WEEK_NOT_FOUND' using errcode='P0002';
      end if;
      if v_week.timesheet_id is not null then
        select current_timesheet.* into v_anchor_timesheet
        from public.timesheets current_timesheet
        where current_timesheet.timesheet_id=v_week.timesheet_id
          and current_timesheet.is_current=true
          and current_timesheet.archived_at_utc is null
          and current_timesheet.sheet_scope='WEEKLY'::public.timesheet_scope_enum
        for update;
        if not found then
          raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='55000';
        end if;
      end if;

      v_initial_route:=upper(coalesce(
        v_source_workflow.creation_identity_json#>>'{derived,initial_route}',
        case when v_source_workflow.route='PAPER' then 'PAPER' else 'ELECTRONIC' end
      ));
      v_route_authority:=private._candidate_route_family_v1(
        v_week.timesheet_id,v_week.id
      );
      if v_initial_route='PAPER'
         and (
           v_route_authority->>'route_family'='QR'
           or (
             v_route_authority->>'route_family'='ELECTRONIC'
             and coalesce(
               (v_route_authority->>'candidate_paper_submission_allowed')::boolean,
               false
             )
           )
         ) then
        v_initial_route:='PAPER';
      else
        v_initial_route:='ELECTRONIC';
      end if;
      v_payload:=jsonb_build_object(
        'workflow_kind',v_workflow_kind,
        'scope',v_scope,
        'route',v_initial_route,
        'contract_id',v_source_workflow.contract_id,
        'contract_week_id',v_week.id,
        'week_ending_date',v_week.week_ending_date,
        'anchor_timesheet_id',case when v_workflow_kind='CONTRACT_EXPENSE'
          then v_source_workflow.anchor_timesheet_id else v_week.timesheet_id end
      );
    end if;
    v_action:='CREATE';
  end if;

  if v_action='CREATE' then
    if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
      raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023';
    end if;
    if not v_is_rejected_resubmission then
      perform pg_advisory_xact_lock(hashtextextended(
        'candidate-workflow-idempotency|'||v_account_id::text||'|'||btrim(p_idempotency_key),0
      ));
      v_insert_workflow_id:=p_workflow_id;
    end if;
    v_workflow_kind:=upper(coalesce(v_payload->>'workflow_kind',''));
    v_scope:=upper(coalesce(v_payload->>'scope',''));
    v_route:=upper(coalesce(v_payload->>'route',''));
    if v_workflow_kind not in ('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED','DAILY')
       or v_scope not in ('WEEKLY','DAILY')
       or v_route not in ('ELECTRONIC','PHONE','EMAIL','PAPER') then
      raise exception 'CANDIDATE_WORKFLOW_TYPE_INVALID' using errcode='22023';
    end if;
    v_daily_first_source:=v_workflow_kind='DAILY' and v_payload ? 'daily_source';
    if v_daily_first_source and (
      v_scope<>'DAILY' or v_route<>'PHONE'
      or jsonb_typeof(v_payload->'daily_source') is distinct from 'object'
      or v_payload->'submission_requested' is distinct from 'true'::jsonb
      or v_payload ?| array['target_timesheet_id','contract_id','contract_week_id','week_ending_date','anchor_timesheet_id']
    ) then
      raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
    end if;
    if not v_is_rejected_resubmission then
      if v_workflow_kind='DAILY' and not v_daily_first_source then
        select requested_timesheet.* into v_source_anchor
        from public.timesheets requested_timesheet
        where requested_timesheet.timesheet_id=nullif(v_payload->>'target_timesheet_id','')::uuid;
        if not found then
          raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002';
        end if;
      end if;
      v_creation_request_identity:=jsonb_strip_nulls(jsonb_build_object(
        'version','CANDIDATE_WORKFLOW_CREATION_REQUEST_V1',
        'action','CREATE',
        'environment',v_environment,
        'account_id',v_account_id,
        'candidate_id',v_candidate_id,
        'workflow_kind',v_workflow_kind,
        'scope',v_scope,
        'initial_route',v_route,
        'contract_id',nullif(v_payload->>'contract_id','')::uuid,
        'contract_week_id',nullif(v_payload->>'contract_week_id','')::uuid,
        'week_ending_date',nullif(v_payload->>'week_ending_date','')::date,
        'work_date',nullif(v_payload->>'work_date','')::date,
        'daily_source',case when v_daily_first_source then v_payload->'daily_source' else null end,
        'submission_requested',case when v_daily_first_source then true else null end,
        'daily_booking_id',case when v_workflow_kind='DAILY'
          then coalesce(nullif(btrim(coalesce(v_source_anchor.booking_id,'')),''),
            v_payload#>>'{daily_source,booking_id}') else null end,
        'target_timesheet_id',case when v_workflow_kind='DAILY'
          and nullif(btrim(coalesce(v_source_anchor.booking_id,'')),'') is null
          then v_source_anchor.timesheet_id else null end,
        'requested_anchor_timesheet_id',nullif(v_payload->>'anchor_timesheet_id','')::uuid,
        'input_snapshot',coalesce(v_payload->'input_snapshot','{}'::jsonb),
        'expected_row_signature',nullif(v_payload->>'expected_row_signature','')
      ));
      v_creation_request_sha256:=private._candidate_workflow_creation_request_sha256_v1(
        v_creation_request_identity
      );
      select * into v_existing_workflow
      from public.candidate_submission_workflows
      where account_id=v_account_id
        and idempotency_key=btrim(p_idempotency_key)
      for update;
      if found then
        if v_existing_workflow.candidate_id is distinct from v_candidate_id
           or v_existing_workflow.replacement_of_workflow_id is not null
           or v_existing_workflow.creation_request_sha256 is distinct from v_creation_request_sha256 then
          raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='40001';
        end if;
        return jsonb_build_object(
          'ok',true,'idempotent_replay',true,
          'workflow_id',v_existing_workflow.id,
          'state',v_existing_workflow.state,
          'generation',v_existing_workflow.generation
        );
      end if;
    end if;
    if v_workflow_kind='DAILY' then
      if v_daily_first_source then
        -- CREATE is invoked by the explicit submit action, not by editing a day.
        -- Its exact request receipt above is replayed before any source recheck.
        v_payload:=v_payload||jsonb_build_object('target_timesheet_id',
          private._candidate_daily_first_receipt_v1(v_environment,v_candidate_id,
            v_payload->'daily_source',v_insert_workflow_id,true,p_now_utc));
      end if;
      if v_scope<>'DAILY' or v_route<>'PHONE'
         or nullif(v_payload->>'target_timesheet_id','') is null
         or nullif(v_payload->>'contract_week_id','') is not null
         or nullif(v_payload->>'week_ending_date','') is not null then
        raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
      end if;
      select * into v_daily_timesheet
      from public.timesheets
      where timesheet_id=(v_payload->>'target_timesheet_id')::uuid
        and is_current=true
        and archived_at_utc is null
        and sheet_scope='DAILY'::public.timesheet_scope_enum
        and nullif(btrim(coalesce(booking_id,'')),'') is not null
      for update;
      if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
      v_daily_receipt_context:=private._candidate_daily_receipt_context_v1(
        v_environment,v_candidate_id,v_daily_timesheet.timesheet_id,true,p_now_utc);
      if not private._candidate_daily_entitled_v1(v_candidate_id) then
        raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
      end if;
      select * into v_daily_fin
      from public.timesheets_financials
      where timesheet_id=v_daily_timesheet.timesheet_id
        and is_current=true
        and candidate_id=v_candidate_id
      order by computed_at_utc desc nulls last,updated_at desc,id desc
      limit 1
      for update;
      -- The locked receipt context proves ownership even before Office has
      -- assigned financial context. Absence of TSFIN is not permission to write it.
      if v_daily_fin.authorised_at_utc is not null
         or v_daily_fin.paid_at_utc is not null
         or v_daily_fin.locked_by_invoice_id is not null
         or v_daily_timesheet.archived_at_utc is not null then
        raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
      end if;
      v_canonical_work_date:=private._candidate_daily_work_date_v1(
        coalesce(v_daily_fin.worked_start_iso,v_daily_timesheet.worked_start_iso),
        v_daily_timesheet.scheduled_start_iso,
        v_daily_timesheet.week_ending_date
      );
      if v_canonical_work_date is null
         or (nullif(v_payload->>'work_date','') is not null
             and (v_payload->>'work_date')::date<>v_canonical_work_date)
         or (nullif(v_payload->>'anchor_timesheet_id','') is not null
             and (v_payload->>'anchor_timesheet_id')::uuid<>v_daily_timesheet.timesheet_id) then
        raise exception 'CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH' using errcode='22023';
      end if;
      if v_daily_timesheet.contract_id is not null then
        select * into v_contract
        from public.contracts
        where id=v_daily_timesheet.contract_id and candidate_id=v_candidate_id
        for update;
        if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
      end if;
      v_client_id:=nullif(v_daily_receipt_context->>'client_id','')::uuid;
      v_policy:=v_daily_receipt_context->'policy';
      if (v_route='PHONE' and not coalesce((v_policy->>'allow_daily_manager_authorise_on_phone')::boolean,false))
         or (v_route='EMAIL' and not coalesce((v_policy->>'allow_daily_manager_authorise_by_email')::boolean,false)) then
        raise exception 'CANDIDATE_DAILY_APPROVAL_ROUTE_NOT_ALLOWED' using errcode='55000';
      end if;
      v_canonical_week_ending_date:=null;
    else
      if v_scope<>'WEEKLY' or v_route not in ('ELECTRONIC','PAPER')
         or nullif(v_payload->>'contract_id','') is null
         or nullif(v_payload->>'contract_week_id','') is null then
        raise exception 'CANDIDATE_CONTRACT_WORKFLOW_IDENTITY_REQUIRED' using errcode='22023';
      end if;
      select * into v_contract
      from public.contracts
      where id=(v_payload->>'contract_id')::uuid and candidate_id=v_candidate_id
      for update;
      if not found then raise exception 'CANDIDATE_WORKFLOW_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
      select * into v_week
      from public.contract_weeks
      where id=(v_payload->>'contract_week_id')::uuid and contract_id=v_contract.id
      for update;
      if not found then raise exception 'CANDIDATE_WORKFLOW_WEEK_NOT_FOUND' using errcode='P0002'; end if;
      v_canonical_week_ending_date:=v_week.week_ending_date;
      if nullif(v_payload->>'week_ending_date','') is not null
         and (v_payload->>'week_ending_date')::date<>v_canonical_week_ending_date then
        raise exception 'CANDIDATE_WORKFLOW_WEEK_MISMATCH' using errcode='22023';
      end if;
      v_policy:=private._expense_approval_policy_v1(v_contract.client_id,v_contract.id,v_canonical_week_ending_date,v_workflow_kind);
      -- Resolve and validate the worked anchor before route admission.  A
      -- server-created expense carrier deliberately has no worked Timesheet
      -- and uses MANUAL storage, so it cannot define the Candidate's manager
      -- approval route.
      if nullif(v_payload->>'anchor_timesheet_id','') is not null then
        select cw.* into v_anchor_week
        from public.contract_weeks cw
        join public.timesheets t on t.timesheet_id=cw.timesheet_id
          and t.is_current=true and t.archived_at_utc is null
          and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
        where cw.timesheet_id=(v_payload->>'anchor_timesheet_id')::uuid
          and cw.contract_id=v_contract.id
          and cw.week_ending_date=v_canonical_week_ending_date;
        if not found then raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='22023'; end if;
        if v_workflow_kind='CONTRACT_EXPENSE'
           and coalesce((private._candidate_record_capabilities_v1(v_anchor_week.timesheet_id,v_anchor_week.id,'{}'::jsonb)->>'hours_value')::numeric,0)<=0
           and coalesce((private._candidate_record_capabilities_v1(v_anchor_week.timesheet_id,v_anchor_week.id,'{}'::jsonb)->>'additional_units_value')::numeric,0)<=0 then
          -- Import-authoritative hours remain outside TSFIN until source
          -- finalisation.  The immutable Candidate hours submission for this
          -- exact anchor is positive worked-week evidence, while the expense
          -- itself still opens as a separate manager-approved workflow.
          select exists(
            select 1
            from public.candidate_submission_workflows submitted_workflow
            cross join lateral (
              select
                coalesce(
                  submitted_workflow.input_snapshot_json#>'{hours_submission,timesheet_patch_json,actual_schedule_json}',
                  submitted_workflow.input_snapshot_json#>'{timesheet_patch_json,actual_schedule_json}',
                  submitted_workflow.input_snapshot_json->'actual_schedule_json',
                  '[]'::jsonb
                ) as actual_schedule_json,
                coalesce(
                  submitted_workflow.input_snapshot_json#>'{hours_submission,timesheet_patch_json,additional_units_week}',
                  submitted_workflow.input_snapshot_json#>'{timesheet_patch_json,additional_units_week}',
                  submitted_workflow.input_snapshot_json->'additional_units_week',
                  '{}'::jsonb
                ) as additional_units_week,
                coalesce(
                  submitted_workflow.input_snapshot_json#>'{hours_submission,timesheet_patch_json,additional_units_per_day}',
                  submitted_workflow.input_snapshot_json#>'{timesheet_patch_json,additional_units_per_day}',
                  submitted_workflow.input_snapshot_json->'additional_units_per_day',
                  '{}'::jsonb
                ) as additional_units_per_day
            ) submitted
            where submitted_workflow.environment=v_environment
              and submitted_workflow.candidate_id=v_candidate_id
              and submitted_workflow.contract_id=v_contract.id
              and submitted_workflow.contract_week_id=v_anchor_week.id
              and submitted_workflow.week_ending_date=v_canonical_week_ending_date
              and submitted_workflow.anchor_timesheet_id=v_anchor_week.timesheet_id
              and submitted_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
              and submitted_workflow.state in (
                'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',
                'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL',
                'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
                'READY_TO_FINALISE','RECEIVED','FINALISED'
              )
              and (
                (pg_catalog.jsonb_typeof(submitted.actual_schedule_json)='array'
                  and pg_catalog.jsonb_array_length(submitted.actual_schedule_json)>0)
                or private._candidate_json_numeric_sum(submitted.additional_units_week)>0
                or private._candidate_json_numeric_sum(submitted.additional_units_per_day)>0
              )
          ) into v_anchor_submitted_work;
          if not v_anchor_submitted_work then
            raise exception 'CANDIDATE_WORKFLOW_ANCHOR_NOT_WORKED' using errcode='22023';
          end if;
        end if;
      elsif v_workflow_kind='CONTRACT_EXPENSE' then
        -- A terminal workflow does not invalidate its immutable anchor receipt.
        -- Reuse one exact historical anchor for this carrier; contradictory
        -- history remains blocked. Only a carrier with no history uses the
        -- original same-week single-worked-row fallback.
        select count(distinct prior.anchor_timesheet_id)::integer,
          case when count(distinct prior.anchor_timesheet_id)=1
            then min(prior.anchor_timesheet_id::text)::uuid else null::uuid end
        into v_anchor_candidate_count,v_anchor_week_id
        from public.candidate_submission_workflows prior
        where prior.environment=v_environment
          and prior.candidate_id=v_candidate_id
          and prior.contract_id=v_contract.id
          and prior.contract_week_id=v_week.id
          and prior.week_ending_date=v_canonical_week_ending_date
          and prior.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
          and prior.anchor_timesheet_id is not null;

        if v_anchor_candidate_count>1 then
          raise exception 'EXPENSE_WORKED_ANCHOR_HISTORY_AMBIGUOUS' using errcode='55000';
        elsif v_anchor_candidate_count=1 then
          select cw.* into v_anchor_week
          from public.contract_weeks cw
          join public.timesheets t on t.timesheet_id=cw.timesheet_id
            and t.is_current=true and t.archived_at_utc is null
            and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
          where cw.timesheet_id=v_anchor_week_id
            and cw.contract_id=v_contract.id
            and cw.week_ending_date=v_canonical_week_ending_date;
          if not found then
            raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='22023';
          end if;
          if coalesce((private._candidate_record_capabilities_v1(
               v_anchor_week.timesheet_id,v_anchor_week.id,'{}'::jsonb
             )->>'hours_value')::numeric,0)<=0
             and coalesce((private._candidate_record_capabilities_v1(
               v_anchor_week.timesheet_id,v_anchor_week.id,'{}'::jsonb
             )->>'additional_units_value')::numeric,0)<=0 then
            raise exception 'CANDIDATE_WORKFLOW_ANCHOR_NOT_WORKED' using errcode='22023';
          end if;
        else
          select count(*)::integer,min(worked.id::text)::uuid
          into v_anchor_candidate_count,v_anchor_week_id
          from public.contract_weeks worked
          join public.timesheets t on t.timesheet_id=worked.timesheet_id
            and t.is_current=true and t.archived_at_utc is null
            and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
          join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current=true
          where worked.contract_id=v_contract.id
            and worked.week_ending_date=v_canonical_week_ending_date
            and (
              coalesce(tf.total_hours,0)>0
              or private._candidate_json_numeric_sum(coalesce(tf.additional_units_json,'{}'::jsonb))>0
              or private._candidate_json_numeric_sum(coalesce(t.additional_units_week,'{}'::jsonb))
                +private._candidate_json_numeric_sum(coalesce(t.additional_units_per_day,'{}'::jsonb))>0
            );
          if v_anchor_candidate_count=0 then raise exception 'NO_POSITIVE_WORKED_TIME' using errcode='55000'; end if;
          if v_anchor_candidate_count>1 then raise exception 'EXPENSE_WORKED_ANCHOR_AMBIGUOUS' using errcode='55000'; end if;
          select * into v_anchor_week from public.contract_weeks where id=v_anchor_week_id;
        end if;
      elsif v_week.timesheet_id is not null then
        v_anchor_week:=v_week;
      end if;
      v_route_authority:=private._expense_approval_route_v1(
        case when v_workflow_kind='CONTRACT_EXPENSE' then v_anchor_week.timesheet_id else v_week.timesheet_id end,
        case when v_workflow_kind='CONTRACT_EXPENSE' then v_anchor_week.id else v_week.id end,v_workflow_kind
      );
      -- Source-authority hours use either an exact live Office request or the
      -- candidate-initiated CHECK_ONLY week route.  This only admits a draft;
      -- the dedicated signed submit RPC rechecks all source and week guards.
      if v_route_authority->>'route_family'='IMPORT_AUTHORITATIVE'
         and v_workflow_kind='CONTRACT_HOURS'
         and v_route='ELECTRONIC' then
        select exists(
          select 1
          from public.weekly_timesheet_submission_requests submission
          join public.weekly_candidate_outreach_generations generation
            on generation.candidate_cohort_id=submission.candidate_cohort_id
           and generation.source_cycle_id=submission.source_cycle_id
           and generation.candidate_id=submission.candidate_id
           and generation.generation_number=submission.request_generation
           and generation.request_kind='SUBMIT_TIMESHEET'
          join public.weekly_timesheet_submission_request_memberships membership
            on membership.submission_request_id=submission.id
          where v_week.timesheet_id is null
            and submission.candidate_id=v_candidate_id
            and submission.state in ('ACTIVE','PARTLY_SUBMITTED')
            and generation.state='ACTIVE'
            and generation.deadline_at_utc>=p_now_utc
            and membership.state='WAITING'
            and membership.contract_id=v_contract.id
            and membership.week_ending=v_canonical_week_ending_date
        ) or exists(
          select 1
          from public.weekly_candidate_outreach_generations generation
          join public.weekly_candidate_outreach_memberships membership
            on membership.candidate_generation_id=generation.id
          join public.weekly_discrepancy_incidents incident
            on incident.id=membership.incident_id
          join public.weekly_issue_comparison_revisions comparison
            on comparison.id=incident.current_comparison_revision_id
          where v_week.timesheet_id is not null
            and generation.candidate_id=v_candidate_id
            and generation.request_kind='CHECK_HOURS'
            and generation.state='ACTIVE'
            and generation.deadline_at_utc>=p_now_utc
            and membership.state='ACTIONABLE'
            and comparison.candidate_timesheet_id=v_week.timesheet_id
            and comparison.contract_id=v_contract.id
        ) into v_weekly_source_candidate_request_allowed;
        if not v_weekly_source_candidate_request_allowed then
          v_weekly_source_candidate_request_allowed:=
            v_canonical_week_ending_date-6 <= (p_now_utc at time zone 'Europe/London')::date
            and v_week.additional_seq=0 and not v_week.is_adjustment
            and v_week.status not in (
              'AUTHORISED'::public.contract_week_status_enum,
              'INVOICED'::public.contract_week_status_enum,
              'CANCELLED'::public.contract_week_status_enum
            )
            and (private._weekly_source_effective_policy_v1(
              v_contract.client_id,v_contract.id,v_canonical_week_ending_date
            )->>'authority_mode')='SOURCE_AUTHORITY'
            and (private._weekly_source_effective_policy_v1(
              v_contract.client_id,v_contract.id,v_canonical_week_ending_date
            )->>'document_mode')='CHECK_ONLY';
        end if;
      end if;
      -- A later separate expense starts on the neutral ELECTRONIC draft route.
      -- Its approval-method step may then select PHONE, EMAIL or PAPER even
      -- when the completed worked Timesheet used the QR/PAPER family.
      if v_route_authority->>'route_family'='MANUAL_NON_QR'
         or (v_route_authority->>'route_family'='IMPORT_AUTHORITATIVE'
           and v_workflow_kind<>'CONTRACT_EXPENSE'
           and not v_weekly_source_candidate_request_allowed) then
        raise exception 'CANDIDATE_RECORD_VIEW_ONLY' using errcode='55000',detail=v_route_authority::text;
      end if;
      if (v_route_authority->>'route_family'='QR' and v_route<>'PAPER'
            and v_workflow_kind<>'CONTRACT_EXPENSE')
         or (v_route_authority->>'route_family'='ELECTRONIC' and v_route='PAPER'
           and not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))
         or (v_route_authority->>'route_family'='IMPORT_AUTHORITATIVE' and v_route='PAPER'
           and (v_workflow_kind<>'CONTRACT_EXPENSE'
             or not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))) then
        raise exception 'CANDIDATE_ROUTE_FAMILY_MISMATCH' using errcode='55000',detail=v_route_authority::text;
      end if;
      if v_week.timesheet_id is not null then
        select * into v_anchor_timesheet
        from public.timesheets
        where timesheet_id=v_week.timesheet_id
          and is_current=true
          and archived_at_utc is null;
        if not found then raise exception 'CANDIDATE_WORKFLOW_TARGET_NOT_CURRENT' using errcode='55000'; end if;
        if v_anchor_timesheet.sheet_scope is distinct from 'WEEKLY'::public.timesheet_scope_enum then
          raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='22023';
        end if;
        if v_workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED') then
          v_target_capabilities:=private._candidate_record_capabilities_v1(
            v_week.timesheet_id,v_week.id,'{}'::jsonb
          );
          if coalesce((v_target_capabilities->>'candidate_mutation_locked')::boolean,false)
             or coalesce((v_target_capabilities->>'protected')::boolean,false)
             or (
               not coalesce((v_target_capabilities->>'can_edit_hours')::boolean,false)
               and not v_weekly_source_candidate_request_allowed
             ) then
            raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
          end if;
        end if;
      end if;
      if nullif(v_payload->>'target_timesheet_id','') is not null then
        if v_workflow_kind='CONTRACT_EXPENSE' then
          raise exception 'CANDIDATE_EXPENSE_TARGET_SERVER_RESOLVED' using errcode='22023';
        end if;
        if v_week.timesheet_id is null
           or (v_payload->>'target_timesheet_id')::uuid<>v_week.timesheet_id then
          raise exception 'CANDIDATE_WORKFLOW_TARGET_MISMATCH' using errcode='22023';
        end if;
      end if;
    end if;

    -- Store the immutable request receipt together with the canonical identity
    -- that the server derived at creation time. Later workflow lifecycle changes
    -- may alter route, target, contract-week and generation fields, but must never
    -- alter this receipt or make an exact creation retry conflict.
    v_creation_identity:=jsonb_build_object(
      'version','CANDIDATE_WORKFLOW_CREATION_IDENTITY_V1',
      'request',v_creation_request_identity,
      'derived',jsonb_strip_nulls(jsonb_build_object(
        'workflow_kind',v_workflow_kind,
        'scope',v_scope,
        'initial_route',v_route,
        'contract_id',v_contract.id,
        'contract_week_id',case when v_workflow_kind='DAILY' then null else v_week.id end,
        'anchor_timesheet_id',case when v_workflow_kind='DAILY'
          then v_daily_timesheet.timesheet_id else v_anchor_week.timesheet_id end,
        'daily_booking_id',case when v_workflow_kind='DAILY'
          then nullif(btrim(coalesce(v_daily_timesheet.booking_id,'')),'') else null end,
        'work_date',v_canonical_work_date,
        'week_ending_date',v_canonical_week_ending_date,
        'replacement_of_workflow_id',v_replacement_of_workflow_id
      ))
    );

    select * into v_existing_workflow
    from public.candidate_submission_workflows
    where account_id=v_account_id
      and idempotency_key=btrim(p_idempotency_key)
    for update;
    if found then
      if v_existing_workflow.candidate_id is distinct from v_candidate_id
         or v_existing_workflow.replacement_of_workflow_id is distinct from v_replacement_of_workflow_id
         or v_existing_workflow.creation_request_sha256 is distinct from v_creation_request_sha256 then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
          using errcode=case when v_is_rejected_resubmission then '55000' else '40001' end;
      end if;
      if v_is_rejected_resubmission then
        return jsonb_build_object(
          'ok',true,'idempotent_replay',true,
          'rejected_workflow_id',v_replacement_of_workflow_id,
          'replacement_workflow_id',v_existing_workflow.id,
          'replacement_created',false,
          'workflow_id',v_existing_workflow.id,
          'state',v_existing_workflow.state,
          'generation',v_existing_workflow.generation
        );
      end if;
      return jsonb_build_object('ok',true,'idempotent_replay',true,
        'workflow_id',v_existing_workflow.id,'state',v_existing_workflow.state,
        'generation',v_existing_workflow.generation);
    end if;

    if v_workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED') then
      perform pg_advisory_xact_lock(hashtext(
        v_candidate_id::text||'|'||v_contract.id::text||'|'||
        v_canonical_week_ending_date::text||
        '|CANDIDATE_EXPENSE_CLAIM'
      ));
      if exists(
        select 1
        from public.candidate_submission_workflows prior
        where prior.candidate_id=v_candidate_id
          and prior.contract_id=v_contract.id
          and prior.week_ending_date=v_canonical_week_ending_date
          and prior.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
          and prior.state not in (
            'CANCELLED','REJECTED','REFUSED','EXPIRED','SUPERSEDED','FINALISED'
          )
      ) or exists(
        select 1
        from public.contract_weeks prior_week
        join public.timesheets prior_timesheet
          on prior_timesheet.timesheet_id=prior_week.timesheet_id
         and prior_timesheet.is_current=true
         and prior_timesheet.archived_at_utc is null
        join public.timesheets_financials prior_fin
          on prior_fin.timesheet_id=prior_timesheet.timesheet_id
         and prior_fin.is_current=true
        where prior_week.contract_id=v_contract.id
          and prior_week.week_ending_date=v_canonical_week_ending_date
          and prior_fin.authorised_at_utc is null
          and not exists(
            select 1
            from public.candidate_submission_workflows approved_claim
            where approved_claim.candidate_id=v_candidate_id
              and approved_claim.contract_id=v_contract.id
              and approved_claim.week_ending_date=v_canonical_week_ending_date
              and approved_claim.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
              and approved_claim.state='FINALISED'
              and approved_claim.target_timesheet_id=prior_timesheet.timesheet_id
          )
          and (
            abs(coalesce(prior_fin.expenses_pay_ex_vat,0))
            +abs(coalesce(prior_fin.expenses_charge_ex_vat,0))
            +abs(coalesce(prior_fin.mileage_units,0))
            +abs(coalesce(prior_fin.mileage_pay_ex_vat,0))
            +abs(coalesce(prior_fin.mileage_charge_ex_vat,0))
            +abs(coalesce(prior_fin.travel_pay_ex_vat,0))
            +abs(coalesce(prior_fin.travel_charge_ex_vat,0))
            +abs(coalesce(prior_fin.accommodation_pay_ex_vat,0))
            +abs(coalesce(prior_fin.accommodation_charge_ex_vat,0))
            +abs(coalesce(prior_fin.other_pay_ex_vat,0))
            +abs(coalesce(prior_fin.other_charge_ex_vat,0))
          )>0
      ) then
        raise exception 'CANDIDATE_EXPENSE_CLAIM_ALREADY_ACTIVE' using errcode='55000';
      end if;
    end if;
    insert into public.candidate_submission_workflows(
      id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
      contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,work_date,week_ending_date,
      policy_snapshot_json,input_snapshot_json,issue_codes,expected_row_signature,idempotency_key,
      replacement_of_workflow_id,creation_request_sha256,creation_identity_json,
      last_mutation_idempotency_key,created_at_utc,updated_at_utc
    ) values (
      v_insert_workflow_id,v_environment,v_account_id,v_candidate_id,v_workflow_kind,
      v_scope,v_route,'WORKER_DRAFT',1,v_contract.id,
      case when v_workflow_kind='DAILY' then null else v_week.id end,
      case when v_workflow_kind='DAILY' then v_daily_timesheet.timesheet_id else v_anchor_week.timesheet_id end,
      case
        when v_workflow_kind='DAILY' then v_daily_timesheet.timesheet_id
        when v_workflow_kind='CONTRACT_EXPENSE' then null
        else v_week.timesheet_id
      end,
      v_canonical_work_date,v_canonical_week_ending_date,
      v_policy,coalesce(v_payload->'input_snapshot','{}'::jsonb),'[]'::jsonb,
      nullif(v_payload->>'expected_row_signature',''),btrim(p_idempotency_key),
      v_replacement_of_workflow_id,v_creation_request_sha256,v_creation_identity,
      btrim(p_idempotency_key),p_now_utc,p_now_utc
    ) returning * into v_workflow;
    perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,
      'CANDIDATE_WORKFLOW_CREATED',null,
      jsonb_build_object('kind',v_workflow.workflow_kind,'scope',v_workflow.scope,'route',v_workflow.route),
      null,null,p_idempotency_key,p_now_utc);
    if v_is_rejected_resubmission then
      return jsonb_build_object(
        'ok',true,'idempotent_replay',false,
        'rejected_workflow_id',v_replacement_of_workflow_id,
        'replacement_workflow_id',v_workflow.id,
        'replacement_created',true,
        'workflow_id',v_workflow.id,'state',v_workflow.state,
        'generation',v_workflow.generation,'policy',v_policy
      );
    end if;
    return jsonb_build_object('ok',true,'idempotent_replay',false,
      'workflow_id',v_workflow.id,'state',v_workflow.state,
      'generation',v_workflow.generation,'policy',v_policy);
  end if;

  if v_workflow.id is null then
    if v_action in ('PAPER_PREPARE','PAPER_RETURN','AMEND','CANCEL','SUPERSEDE') then
      -- Keep the canonical route/document lock order used by
      -- timesheet_qr_send_enqueue_v1: current timesheet, then workflow.
      -- The unlocked identity read is rechecked after both locks are held.
      select * into v_workflow
      from public.candidate_submission_workflows
      where id=p_workflow_id
        and environment=v_environment
        and candidate_id=v_candidate_id;
      if not found then
        raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
      end if;
      v_unlocked_workflow_updated_at:=v_workflow.updated_at_utc;
      v_paper_timesheet_id:=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id);
      v_paper_family_key:='CANDIDATE_PAPER_FAMILY:'||v_workflow.environment||':'
        ||coalesce(v_workflow.contract_id::text,'-')||':'
        ||coalesce(v_workflow.week_ending_date::text,v_workflow.work_date::text,'-');
      perform pg_advisory_xact_lock(hashtextextended(v_paper_family_key,0));
      if v_action='PAPER_PREPARE' and v_paper_timesheet_id is null then
        raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
      end if;
      if v_paper_timesheet_id is not null
         and (v_action in ('PAPER_PREPARE','PAPER_RETURN')
           or (v_workflow.route='PAPER'
             and v_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED'))) then
        perform 1
        from public.timesheets
        where timesheet_id=v_paper_timesheet_id
          and is_current=true
          and archived_at_utc is null
        for update;
        if not found then
          raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
        end if;
      end if;
      select * into v_workflow
      from public.candidate_submission_workflows
      where id=p_workflow_id
        and environment=v_environment
        and candidate_id=v_candidate_id
      for update;
      if not found
         or coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
              is distinct from v_paper_timesheet_id then
        raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
      end if;
      if v_action in ('PAPER_RETURN','AMEND','CANCEL','SUPERSEDE')
         and v_workflow.updated_at_utc is distinct from v_unlocked_workflow_updated_at then
        raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
      end if;
    else
      select * into v_workflow
      from public.candidate_submission_workflows
      where id=p_workflow_id
      for update;
    end if;
  end if;
  if not found or v_workflow.environment<>v_environment
     or (not v_is_service_action and not v_is_public_manager_action and v_workflow.candidate_id<>v_candidate_id) then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_action='WORKER_SUBMIT' and nullif(v_payload->>'update_id','') is not null then
    begin
      v_expense_update_context:=nullif(current_setting(
        'cloudtms.candidate_expense_update_submit_context',true
      ),'')::jsonb;
    exception when others then
      v_expense_update_context:=null;
    end;
    if coalesce(v_expense_update_context->>'contract_version','')
         <>'CANDIDATE_EXPENSE_UPDATE_SUBMIT_CONTEXT_V1'
       or v_expense_update_context->>'workflow_id'<>v_workflow.id::text
       or v_expense_update_context->>'workflow_generation'<>v_workflow.generation::text
       or v_expense_update_context->>'update_id'<>v_payload->>'update_id'
       or v_expense_update_context->>'idempotency_key'<>btrim(coalesce(p_idempotency_key,'')) then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_RECEIPT_INVALID' using errcode='28000';
    end if;
    select update_row.* into v_pending_expense_update
    from public.candidate_pending_expense_updates update_row
    where update_row.update_id=(v_payload->>'update_id')::uuid
      and update_row.workflow_id=v_workflow.id
      and update_row.current_workflow_generation=v_workflow.generation
      and update_row.state='EDITING'
      and update_row.actor_kind=v_expense_update_context->>'actor_kind'
      and update_row.actor_id is not distinct from nullif(
        v_expense_update_context->>'actor_id',''
      )::uuid
    for update;
    if not found then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
    end if;
    v_is_pending_expense_update:=true;
  end if;
  if nullif(btrim(coalesce(p_idempotency_key,'')),'') is not null then
    if nullif(btrim(coalesce(v_workflow.idempotency_key,'')),'')=btrim(p_idempotency_key) then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_IDEMPOTENCY_CONFLICT',
          'workflow_id',v_workflow.id,
          'idempotency_key',btrim(p_idempotency_key),
          'reason','CREATION_KEY_REUSED_FOR_MUTATION'
        )::text;
    end if;
    v_mutation_channel:=case
      when v_is_office_service_action then 'OFFICE'
      when v_is_public_manager_action then 'MANAGER_PUBLIC'
      when v_is_service_action then 'SERVICE'
      else 'CANDIDATE_CLIENT' end;
    v_mutation_actor_identity:=case
      when v_is_office_service_action then v_office_actor_user_id::text
      when v_is_public_manager_action then 'MANAGER_REQUEST:'||coalesce(v_request_id::text,'UNKNOWN')
      when v_is_service_action then 'SERVICE:'||v_action
      else 'ACCOUNT:'||coalesce(v_account_id::text,'UNKNOWN')
        ||':CANDIDATE:'||coalesce(v_candidate_id::text,'UNKNOWN') end;
    v_mutation_semantic_payload:=case
      when v_mutation_replay_probe_only then
        v_payload->'mutation_replay_semantic_payload'
      when v_action='COMPONENT_PREPARE' then
        v_payload-'service_office_action'-'actor_user_id'-'storage_key'
      when v_action='SELECT_PHONE_APPROVAL' then
        v_payload-'service_office_action'-'actor_user_id'-'expires_at_utc'
          -'approval_token_hash_hex'-'handoff_token_key_version'-'broker_handoff_key_version'
      when v_action='WORKER_SUBMIT' then
        case when jsonb_typeof(v_payload->'submission_request_identity')='object'
          then jsonb_build_object(
            'submission_request_identity',v_payload->'submission_request_identity'
          )
          else (v_payload-'service_office_action'-'actor_user_id'-'renderer_contract_version'-'immutable_submission')
            ||jsonb_build_object(
              'immutable_submission',coalesce(v_payload->'immutable_submission','{}'::jsonb)-'official_presentation'
            )
        end
      when v_action='CREATE_EMAIL_APPROVAL_REQUEST' then
        v_payload-'service_office_action'-'actor_user_id'-'mail'-'approval_token_hash_hex'
      when v_action in ('REMIND','RENEW') then
        v_payload-'service_office_action'-'actor_user_id'-'mail'-'approval_token_hash_hex'-'manager_email'
      else v_payload-'service_office_action'-'actor_user_id'
    end;
    v_mutation_request_sha256:=encode(extensions.digest(convert_to(jsonb_build_object(
      'contract_version','CANDIDATE_WORKFLOW_MUTATION_REQUEST_V1',
      'workflow_id',v_workflow.id,
      'action',v_action,
      'expected_generation',p_expected_generation,
      'payload',v_mutation_semantic_payload,
      'channel',v_mutation_channel,
      'actor_identity',v_mutation_actor_identity
    )::text,'UTF8'),'sha256'),'hex');
    v_mutation_receipt:=private._candidate_workflow_mutation_receipt_v1(
      v_workflow.id,btrim(p_idempotency_key),v_mutation_request_sha256,
      v_action,v_mutation_channel,v_mutation_actor_identity,null,p_now_utc
    );
    if coalesce((v_mutation_receipt->>'found')::boolean,false) then
      return v_mutation_receipt->'response';
    end if;
    if v_mutation_replay_probe_only then
      return jsonb_build_object(
        'ok',true,'replay_found',false,'workflow_id',v_workflow.id,
        'expected_generation',p_expected_generation
      );
    end if;
  end if;
  if p_expected_generation is not null and v_workflow.generation<>p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','WORKFLOW_GENERATION_CONFLICT','current_generation',v_workflow.generation)::text;
  end if;
  v_next_generation:=v_workflow.generation+1;
  if v_workflow.workflow_kind='DAILY' then
    v_daily_receipt_context:=private._candidate_daily_receipt_context_v1(
      v_environment,v_workflow.candidate_id,v_workflow.target_timesheet_id,true,p_now_utc);
    v_policy:=v_daily_receipt_context->'policy';
  elsif v_workflow.contract_id is not null then
    select * into v_contract from public.contracts where id=v_workflow.contract_id;
    v_policy:=private._expense_approval_policy_v1(v_contract.client_id,v_contract.id,
      coalesce(v_workflow.week_ending_date,v_workflow.work_date,(p_now_utc at time zone 'Europe/London')::date),v_workflow.workflow_kind);
  else
    v_policy:=v_workflow.policy_snapshot_json;
  end if;

  if v_action='AMEND' then
    if v_workflow.state not in (
      'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',
      'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL',
      'AWAITING_PAPER_RETURN','REFUSED'
    ) then
      raise exception 'CANDIDATE_WORKFLOW_AMENDMENT_NOT_ALLOWED' using errcode='55000';
    end if;
    if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
      raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023';
    end if;
    if v_workflow.route='PAPER' and v_workflow.state='AWAITING_PAPER_RETURN' then
      v_paper_retirement_result:=private._candidate_paper_delivery_retire_v1(
        v_workflow.id,v_workflow.generation,'WORKFLOW_AMENDED',p_now_utc
      );
    end if;
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and state in ('PENDING','APPROVED');
    v_component_no:=0;
    for v_source_component in
      select canonical_source.*
      from (
        select distinct on (
          source_component.component_kind,source_component.expense_category,
          source_component.document_role,
          coalesce(source_component.source_component_id,source_component.id),
          source_component.source_content_sha256
        ) source_component.*
        from public.candidate_submission_components source_component
        where source_component.workflow_id=v_workflow.id
          and source_component.workflow_generation=v_workflow.generation
          and source_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
          and source_component.state='IMMUTABLE'
          and source_component.source_content_sha256 is not null
        order by source_component.component_kind,source_component.expense_category,
          source_component.document_role,
          coalesce(source_component.source_component_id,source_component.id),
          source_component.source_content_sha256,
          source_component.component_no,source_component.id
      ) canonical_source
      order by canonical_source.component_no,canonical_source.id
    loop
      v_component_no:=v_component_no+1;
      insert into public.candidate_submission_components(
        workflow_id,workflow_generation,component_no,timesheet_id,component_kind,expense_category,
        document_role,state,source_component_id,storage_key,media_type,byte_size,source_content_sha256,
        immutable_at_utc,required,review_render_state,final_signed_render_state,created_at_utc
      ) values (
        v_workflow.id,v_next_generation,v_component_no,v_workflow.target_timesheet_id,
        v_source_component.component_kind,v_source_component.expense_category,v_source_component.document_role,
        'IMMUTABLE',coalesce(v_source_component.source_component_id,v_source_component.id),
        v_source_component.storage_key,v_source_component.media_type,v_source_component.byte_size,
        v_source_component.source_content_sha256,p_now_utc,false,'NOT_REQUIRED','NOT_REQUIRED',p_now_utc
      );
    end loop;
    update public.candidate_submission_components set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,
      review_render_state=case when review_render_state='NOT_REQUIRED' then review_render_state else 'SUPERSEDED' end,
      final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED' then final_signed_render_state else 'SUPERSEDED' end
    where workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation
      and state<>'SUPERSEDED';
    v_response:=jsonb_build_object(
      'ok',true,'workflow_id',v_workflow.id,'state','WORKER_DRAFT',
      'generation',v_next_generation,'preserved_source_component_count',v_component_no
    );
    update public.candidate_submission_workflows set
      state='WORKER_DRAFT',generation=v_next_generation,
      input_snapshot_json=coalesce(v_payload->'input_snapshot',input_snapshot_json),
      candidate_signature_component_id=null,candidate_signature_sha256=null,candidate_signed_at_utc=null,
      review_manifest_json=null,review_manifest_sha256=null,paper_return_manifest_json=null,
      paper_return_manifest_sha256=null,renderer_contract_version=null,
      manager_name=null,manager_position=null,manager_signature_component_id=null,
      manager_signature_sha256=null,manager_approved_at_utc=null,
      issue_codes='[]'::jsonb,worker_submitted_at_utc=null,
      daily_context_sha256=null,canonical_financial_sha256=null,
      canonical_save_input_sha256=null,canonical_save_row_signature=null,
      canonical_save_financials_id=null,canonical_save_receipt_json=null,canonical_saved_at_utc=null,
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc
    where id=v_workflow.id returning * into v_workflow;
    perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,
      'CANDIDATE_WORKFLOW_AMENDED',null,
      jsonb_build_object('generation',v_workflow.generation,'preserved_source_component_count',v_component_no),
      null,v_candidate_id,p_idempotency_key,p_now_utc);
    if v_mutation_request_sha256 is not null then
      perform private._candidate_workflow_mutation_receipt_v1(
        v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
        v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
      );
    end if;
    return v_response;
  elsif v_action='COMPONENT_PREPARE' then
    if v_workflow.state in ('FINALISED','CANCELLED','REJECTED','SUPERSEDED') then
      raise exception 'CANDIDATE_WORKFLOW_NOT_MUTABLE' using errcode='55000';
    end if;
    if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
      raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023';
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
    v_manager_capture_method:=nullif(upper(btrim(coalesce(
      v_payload->>'manager_signature_capture_method',''
    ))),'');
    if nullif(v_payload->>'expected_source_content_sha256_hex','') is not null then
      if (v_payload->>'expected_source_content_sha256_hex') !~ '^[0-9a-fA-F]{64}$' then
        raise exception 'CANDIDATE_COMPONENT_DIGEST_INVALID' using errcode='22023';
      end if;
      v_expected_source_digest:=decode(v_payload->>'expected_source_content_sha256_hex','hex');
    end if;
    select * into v_component from public.candidate_submission_components
    where workflow_id=v_workflow.id and upload_idempotency_key=p_idempotency_key;
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
         or v_component.manager_signature_capture_method is distinct from v_manager_capture_method
         or v_component.expected_source_content_sha256 is distinct from v_expected_source_digest
         or v_component.paper_return_page_key is distinct from v_paper_page_key then
        raise exception 'CANDIDATE_COMPONENT_PREPARE_IDEMPOTENCY_CONFLICT' using errcode='23505';
      end if;
      v_response:=jsonb_build_object('ok',true,'idempotent_replay',true,
        'component_id',v_component.id,'component_no',v_component.component_no,
        'workflow_generation',v_component.workflow_generation,
        'storage_key',v_component.storage_key,'media_type',v_component.media_type,
        'byte_size',v_component.byte_size,'component_kind',v_component.component_kind,
        'document_role',v_component.document_role,'expense_category',v_component.expense_category,
        'paper_return_page_key',v_component.paper_return_page_key,'state',v_component.state)
        ||case when v_component.component_kind='MANAGER_SIGNATURE' then jsonb_build_object(
          'approval_request_id',v_component.approval_request_id,
          'approval_request_generation',v_approval.request_generation
        ) else '{}'::jsonb end;
      if v_mutation_request_sha256 is not null then
        perform private._candidate_workflow_mutation_receipt_v1(
          v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
          v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
        );
      end if;
      return v_response;
    end if;
    select coalesce(max(component_no),0)+1 into v_component_no
    from public.candidate_submission_components
    where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation;
    if v_is_public_manager_action then
      if v_component_kind<>'MANAGER_SIGNATURE' or v_document_role<>'MANAGER_SIGNATURE' then
        raise exception 'MANAGER_SIGNATURE_COMPONENT_REQUIRED' using errcode='28000';
      end if;
      if v_approval.id is null
         or v_approval.state<>'PENDING'
         or v_approval.expires_at_utc<=p_now_utc
         or v_approval.workflow_generation<>v_workflow.generation
         or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256 then
        raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
      end if;
    elsif v_component_kind='MANAGER_SIGNATURE' then
      select * into v_approval
      from public.candidate_approval_requests
      where id=nullif(v_payload->>'approval_request_id','')::uuid
        and workflow_id=v_workflow.id
        and workflow_generation=v_workflow.generation
        and method='PHONE'
        and state='PENDING'
        and review_manifest_sha256=v_workflow.review_manifest_sha256
      for update;
      if not found then raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000'; end if;
    elsif not coalesce((
      (v_component_kind='CANDIDATE_SIGNATURE' and v_document_role='CANDIDATE_SIGNATURE' and v_expense_category is null)
      or (v_component_kind='MILEAGE_FORM' and v_document_role='MILEAGE_CLAIM_FORM' and v_expense_category='MILEAGE')
      or (v_component_kind='EXPENSE_EVIDENCE' and v_document_role='SOURCE_EVIDENCE'
          and v_expense_category in ('TRAVEL','ACCOMMODATION','OTHER','MILEAGE'))
      or (v_component_kind='SIGNED_RETURN' and v_document_role='SIGNED_RETURN' and v_expense_category is null)
      or (v_component_kind='MANAGER_SIGNATURE' and v_document_role='MANAGER_SIGNATURE'
          and v_expense_category is null
          and v_workflow.state='AWAITING_MANAGER_APPROVAL'
          and exists(
            select 1 from public.candidate_approval_requests approval
            where approval.workflow_id=v_workflow.id
              and approval.workflow_generation=v_workflow.generation
              and approval.method='PHONE' and approval.state='PENDING'
          ))
    ),false) then
      raise exception 'CANDIDATE_COMPONENT_TYPE_INVALID' using errcode='22023';
    end if;
    if v_component_kind='MANAGER_SIGNATURE' and v_approval.method='PHONE'
       and v_manager_capture_method is null then
      v_manager_capture_method:='DRAW';
    end if;
    if v_component_kind='MANAGER_SIGNATURE'
       and (coalesce(v_manager_capture_method,'') not in ('DRAW','UPLOAD')
         or (v_is_public_manager_action and v_approval.method='EMAIL'
           and v_expected_source_digest is null)) then
      raise exception 'MANAGER_SIGNATURE_CAPTURE_METHOD_INVALID' using errcode='22023';
    elsif v_component_kind<>'MANAGER_SIGNATURE'
       and (v_manager_capture_method is not null or v_expected_source_digest is not null) then
      raise exception 'CANDIDATE_COMPONENT_DIGEST_INVALID' using errcode='22023';
    end if;
    if v_component_kind in ('CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE')
       and v_workflow.state<>'WORKER_DRAFT' then
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
    -- A manager may safely reopen the original approval link after a browser,
    -- network or dependency interruption.  The approval itself is still
    -- PENDING, so any earlier signature reservation is not an approval fact.
    -- Replace that unfinished/unused reservation atomically before inserting
    -- the newly drawn signature.  Exact retries already returned above by
    -- idempotency key, and an approved request can never enter this branch.
    if v_component_kind='MANAGER_SIGNATURE' and v_approval.id is not null then
      with replaced as (
        update public.candidate_submission_components
        set state='SUPERSEDED',superseded_at_utc=p_now_utc,
            review_render_state=case
              when review_render_state='NOT_REQUIRED' then review_render_state
              else 'SUPERSEDED'
            end,
            final_signed_render_state=case
              when final_signed_render_state='NOT_REQUIRED' then final_signed_render_state
              else 'SUPERSEDED'
            end
        where approval_request_id=v_approval.id
          and component_kind='MANAGER_SIGNATURE'
          and state in ('PENDING','IMMUTABLE')
          and upload_idempotency_key is distinct from p_idempotency_key
        returning id
      )
      select coalesce(array_agg(id),array[]::uuid[])
      into v_replaced_manager_signature_ids
      from replaced;
      if cardinality(v_replaced_manager_signature_ids)>0 then
        perform private._candidate_audit_v1(
          'candidate_approval_request',v_approval.id::text,
          'MANAGER_SIGNATURE_REPLACED_BEFORE_DECISION',null,
          jsonb_build_object(
            'workflow_id',v_workflow.id,
            'workflow_generation',v_workflow.generation,
            'replaced_component_ids',to_jsonb(v_replaced_manager_signature_ids)
          ),null,null,p_idempotency_key,p_now_utc
        );
      end if;
    end if;
    if nullif(v_payload->>'source_component_id','') is not null then
      select source_component.* into v_source_component
      from public.candidate_submission_components source_component
      join public.candidate_submission_workflows source_workflow on source_workflow.id=source_component.workflow_id
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
    insert into public.candidate_submission_components(
      workflow_id,workflow_generation,component_no,approval_request_id,timesheet_id,component_kind,expense_category,
      document_role,state,source_component_id,storage_key,media_type,byte_size,source_content_sha256,
      upload_idempotency_key,immutable_at_utc,
      required,review_ordinal,review_render_state,final_signed_render_state,paper_return_page_key,
      manager_signature_capture_method,expected_source_content_sha256,created_at_utc
    ) values (
      v_workflow.id,v_workflow.generation,v_component_no,
      case when v_component_kind='MANAGER_SIGNATURE' then v_approval.id else null end,
      v_workflow.target_timesheet_id,
      v_component_kind,v_expense_category,v_document_role,
      case when v_source_component.id is null then 'PENDING' else 'IMMUTABLE' end,
      v_source_component.id,
      coalesce(v_source_component.storage_key,nullif(v_payload->>'storage_key','')),
      coalesce(v_source_component.media_type,v_requested_media_type),
      coalesce(v_source_component.byte_size,v_requested_byte_size),
      v_source_component.source_content_sha256,p_idempotency_key,
      case when v_source_component.id is null then null else p_now_utc end,
      false,null,'NOT_REQUIRED','NOT_REQUIRED',v_paper_page_key,
      v_manager_capture_method,v_expected_source_digest,
      p_now_utc
    ) returning * into v_component;
    v_response:=jsonb_build_object('ok',true,'idempotent_replay',false,'component_id',v_component.id,
      'component_no',v_component.component_no,'workflow_generation',v_component.workflow_generation,
      'storage_key',v_component.storage_key,'media_type',v_component.media_type,
      'byte_size',v_component.byte_size,'component_kind',v_component.component_kind,
      'document_role',v_component.document_role,'expense_category',v_component.expense_category,
      'paper_return_page_key',v_component.paper_return_page_key,'state',v_component.state)
      ||case when v_component.component_kind='MANAGER_SIGNATURE' then jsonb_build_object(
        'approval_request_id',v_component.approval_request_id,
        'approval_request_generation',v_approval.request_generation
      ) else '{}'::jsonb end;
    perform private._candidate_workflow_mutation_receipt_v1(
      v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
      v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
    );
    return v_response;
  elsif v_action='COMPONENT_COMPLETE' then
    select * into v_component from public.candidate_submission_components
    where id=nullif(v_payload->>'component_id','')::uuid and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation for update;
    if not found then raise exception 'CANDIDATE_COMPONENT_NOT_FOUND' using errcode='P0002'; end if;
    if v_is_public_manager_action and v_component.component_kind<>'MANAGER_SIGNATURE' then
      raise exception 'MANAGER_SIGNATURE_COMPONENT_REQUIRED' using errcode='28000';
    end if;
    if v_component.component_kind='MANAGER_SIGNATURE' then
      if v_approval.id is null then
        select * into v_approval
        from public.candidate_approval_requests
        where id=v_component.approval_request_id
          and workflow_id=v_workflow.id
          and workflow_generation=v_workflow.generation
          and method='PHONE'
          and state='PENDING'
          and review_manifest_sha256=v_workflow.review_manifest_sha256
        for update;
      end if;
      if not found or v_component.approval_request_id is distinct from v_approval.id
         or v_approval.state<>'PENDING'
         or v_approval.workflow_generation<>v_workflow.generation
         or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256
         or v_approval.expires_at_utc<=p_now_utc then
        raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
      end if;
    elsif v_component.component_kind in ('CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE')
          and v_workflow.state<>'WORKER_DRAFT' then
      raise exception 'CANDIDATE_COMPONENT_AMENDMENT_REQUIRED' using errcode='55000';
    elsif v_component.component_kind='SIGNED_RETURN' and v_workflow.state<>'AWAITING_PAPER_RETURN' then
      raise exception 'CANDIDATE_PAPER_RETURN_PAGE_NOT_EXPECTED' using errcode='55000';
    end if;
    if coalesce(v_payload->>'source_content_sha256_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'CANDIDATE_COMPONENT_DIGEST_INVALID' using errcode='22023';
    end if;
    v_digest:=decode(v_payload->>'source_content_sha256_hex','hex');
    if v_component.expected_source_content_sha256 is not null
       and v_component.expected_source_content_sha256<>v_digest then
      raise exception 'CANDIDATE_COMPONENT_DIGEST_MISMATCH' using errcode='22023';
    end if;
    if v_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
       and v_component.expected_source_content_sha256 is not null
       and (
         nullif(btrim(coalesce(v_component.storage_key,'')),'') is null
         or coalesce(nullif(v_payload->>'verified_byte_size','')::bigint,
              v_component.byte_size) is distinct from v_component.byte_size
         or lower(coalesce(nullif(v_payload->>'verified_media_type',''),
              v_component.media_type)) is distinct from lower(v_component.media_type)
       ) then
      raise exception 'CANDIDATE_COMPONENT_MEDIA_INVALID' using errcode='22023';
    end if;
    v_manager_capture_method:=nullif(upper(btrim(coalesce(
      v_payload->>'manager_signature_capture_method',''
    ))),'');
    if v_component.component_kind='MANAGER_SIGNATURE' and v_approval.method='PHONE'
       and v_manager_capture_method is null then
      v_manager_capture_method:='DRAW';
    end if;
    begin
      v_verified_image_width:=nullif(v_payload->>'verified_image_width','')::integer;
      v_verified_image_height:=nullif(v_payload->>'verified_image_height','')::integer;
    exception when invalid_text_representation or numeric_value_out_of_range then
      raise exception 'CANDIDATE_COMPONENT_MEDIA_INVALID' using errcode='22023';
    end;
    if v_component.component_kind='MANAGER_SIGNATURE'
       and (v_manager_capture_method is distinct from v_component.manager_signature_capture_method
         or (v_approval.method='EMAIL' and (
           v_verified_image_width not between 1 and 10000
           or v_verified_image_height not between 1 and 10000
         ))) then
      raise exception 'MANAGER_SIGNATURE_CAPTURE_METHOD_INVALID' using errcode='22023';
    end if;
    if (v_component.component_kind in ('CANDIDATE_SIGNATURE','MANAGER_SIGNATURE')
          and lower(coalesce(v_payload->>'verified_media_type',v_component.media_type,''))
            not in ('image/jpeg','image/png','image/webp'))
       or (v_component.component_kind in ('MILEAGE_FORM','SIGNED_RETURN','EXPENSE_EVIDENCE')
          and lower(coalesce(v_payload->>'verified_media_type',v_component.media_type,''))
            not in ('application/pdf','image/jpeg','image/png','image/webp'))
       or coalesce(nullif(v_payload->>'verified_byte_size','')::bigint,v_component.byte_size,0)
          not between 1 and 15728640 then
      raise exception 'CANDIDATE_COMPONENT_MEDIA_INVALID' using errcode='22023';
    end if;
    if v_component.state='IMMUTABLE' then
      if v_component.source_content_sha256=v_digest
         and v_component.byte_size=coalesce(nullif(v_payload->>'verified_byte_size','')::bigint,v_component.byte_size)
         and lower(v_component.media_type)=lower(coalesce(nullif(v_payload->>'verified_media_type',''),v_component.media_type)) then
        v_response:=jsonb_build_object('ok',true,'idempotent_replay',true,
          'component_id',v_component.id,'state',v_component.state);
        if v_mutation_request_sha256 is not null then
          perform private._candidate_workflow_mutation_receipt_v1(
            v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
            v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
          );
        end if;
        return v_response;
      end if;
      raise exception 'CANDIDATE_COMPONENT_IMMUTABLE_CONFLICT' using errcode='40001';
    end if;
    if v_component.state<>'PENDING' then
      raise exception 'CANDIDATE_COMPONENT_COMPLETE_STATE_CONFLICT' using errcode='55000';
    end if;
    update public.candidate_submission_components set
      state='IMMUTABLE',source_content_sha256=v_digest,
      byte_size=coalesce(nullif(v_payload->>'verified_byte_size','')::bigint,byte_size),
      media_type=coalesce(nullif(lower(v_payload->>'verified_media_type'),''),media_type),
      validated_image_width=case when component_kind='MANAGER_SIGNATURE' then v_verified_image_width else validated_image_width end,
      validated_image_height=case when component_kind='MANAGER_SIGNATURE' then v_verified_image_height else validated_image_height end,
      immutable_at_utc=p_now_utc
    where id=v_component.id and state='PENDING' returning * into v_component;
    if not found then
      raise exception 'CANDIDATE_COMPONENT_COMPLETE_STATE_CONFLICT' using errcode='40001';
    end if;
    v_response:=jsonb_build_object('ok',true,'idempotent_replay',false,'component_id',v_component.id,
      'state',v_component.state,'immutable_at_utc',v_component.immutable_at_utc);
    perform private._candidate_workflow_mutation_receipt_v1(
      v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
      v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
    );
    return v_response;
  elsif v_action='COMPONENT_SUPERSEDE' then
    if v_workflow.state<>'WORKER_DRAFT' then
      raise exception 'CANDIDATE_COMPONENT_AMENDMENT_REQUIRED' using errcode='55000';
    end if;
    update public.candidate_submission_components set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,
      review_render_state=case when review_render_state='NOT_REQUIRED' then review_render_state else 'SUPERSEDED' end,
      final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED' then final_signed_render_state else 'SUPERSEDED' end
    where id=nullif(v_payload->>'component_id','')::uuid and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation and state<>'SUPERSEDED'
      and component_kind in ('CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE')
    returning * into v_component;
    if not found then raise exception 'CANDIDATE_COMPONENT_NOT_FOUND' using errcode='P0002'; end if;
    v_response:=jsonb_build_object('ok',true,'component_id',v_component.id,'state',v_component.state);
    if v_mutation_request_sha256 is not null then
      perform private._candidate_workflow_mutation_receipt_v1(
        v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
        v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
      );
    end if;
    return v_response;
  end if;

  if v_action='WORKER_SUBMIT' then
    if v_workflow.state<>'WORKER_DRAFT' then
      raise exception 'CANDIDATE_WORKFLOW_TRANSITION_INVALID' using errcode='55000';
    end if;
    if v_workflow.workflow_kind='DAILY'
       and upper(coalesce(v_payload->>'approval_route',v_workflow.route))<>'PHONE' then
      raise exception 'CANDIDATE_DAILY_APPROVAL_ROUTE_INVALID' using errcode='22023';
    end if;
    v_is_electronic:=upper(coalesce(v_payload->>'approval_route',v_workflow.route))<>'PAPER';
    v_immutable_submission:=coalesce(v_payload->'immutable_submission',v_payload->'input_snapshot');
    if jsonb_typeof(v_immutable_submission)<>'object' then
      raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_REQUIRED' using errcode='22023';
    end if;
    if v_workflow.workflow_kind='DAILY' then
      if not private._candidate_daily_entitled_v1(v_candidate_id) then
        raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
      end if;
      select * into v_daily_timesheet
      from public.timesheets
      where timesheet_id=v_workflow.target_timesheet_id
        and is_current=true and archived_at_utc is null
        and sheet_scope='DAILY'::public.timesheet_scope_enum
      for update;
      if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
      select * into v_daily_fin
      from public.timesheets_financials
      where timesheet_id=v_daily_timesheet.timesheet_id and is_current=true and candidate_id=v_candidate_id
      order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1 for update;
      -- _candidate_daily_receipt_context_v1 already locked and verified this
      -- exact Candidate receipt and any existing protected financial state.
      v_canonical_work_date:=private._candidate_daily_work_date_v1(
        coalesce(
          nullif(v_immutable_submission#>>'{timesheet_patch_json,worked_start_iso}','')::timestamptz,
          nullif(v_immutable_submission->>'worked_start_iso','')::timestamptz,
          v_daily_fin.worked_start_iso,
          v_daily_timesheet.worked_start_iso
        ),
        v_daily_timesheet.scheduled_start_iso,
        v_daily_timesheet.week_ending_date
      );
      if v_canonical_work_date is distinct from v_workflow.work_date then
        raise exception 'CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH' using errcode='22023';
      end if;
    elsif v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED') then
      select * into v_week
      from public.contract_weeks
      where id=v_workflow.contract_week_id
        and contract_id=v_workflow.contract_id
        and week_ending_date=v_workflow.week_ending_date
      for update;
      if not found then
        raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001';
      end if;
      if v_week.timesheet_id is null then
        -- A first electronic submission is reviewed before its Timesheet is
        -- materialised.  Its frozen canonical create/snapshot payload is
        -- applied only by the established manager-approved finalisation.
        if v_workflow.target_timesheet_id is not null then
          raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001';
        end if;
      else
        select * into v_anchor_timesheet
        from public.timesheets
        where timesheet_id=v_workflow.target_timesheet_id
          and timesheet_id=v_week.timesheet_id
          and is_current=true
          and archived_at_utc is null
          and sheet_scope='WEEKLY'::public.timesheet_scope_enum
        for update;
        if not found then
          raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001';
        end if;
      end if;
      v_target_capabilities:=private._candidate_record_capabilities_v1(v_workflow.target_timesheet_id,v_week.id,'{}'::jsonb);
      v_route_authority:=private._candidate_route_family_v1(v_workflow.target_timesheet_id,v_week.id);
      if not coalesce((v_target_capabilities->>'can_edit_hours')::boolean,false)
         or (v_workflow.route='PAPER' and not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))
         or (v_workflow.route<>'PAPER' and v_route_authority->>'route_family'<>'ELECTRONIC') then
        raise exception 'CANDIDATE_ROUTE_FAMILY_MISMATCH' using errcode='55000',detail=v_route_authority::text;
      end if;
    elsif v_workflow.workflow_kind='CONTRACT_EXPENSE' then
      select week_row.* into v_anchor_week
      from public.contract_weeks week_row
      join public.timesheets anchor_timesheet
        on anchor_timesheet.timesheet_id=week_row.timesheet_id
       and anchor_timesheet.is_current=true
       and anchor_timesheet.archived_at_utc is null
       and anchor_timesheet.sheet_scope='WEEKLY'::public.timesheet_scope_enum
      where week_row.timesheet_id=v_workflow.anchor_timesheet_id
        and week_row.contract_id=v_workflow.contract_id
        and week_row.week_ending_date=v_workflow.week_ending_date
      for update of week_row,anchor_timesheet;
      if not found then raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001'; end if;
      v_route_authority:=private._expense_approval_route_v1(v_workflow.anchor_timesheet_id,v_anchor_week.id,v_workflow.workflow_kind);
      if not coalesce((v_route_authority->>'candidate_expenses_allowed')::boolean,false)
         or (v_workflow.route='PAPER' and not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))
         or (v_workflow.route<>'PAPER' and v_route_authority->>'route_family' not in ('ELECTRONIC','IMPORT_AUTHORITATIVE','QR')) then
        raise exception 'CANDIDATE_ROUTE_FAMILY_MISMATCH' using errcode='55000',detail=v_route_authority::text;
      end if;
    end if;
    if exists(
      select 1
      from (values
        (v_immutable_submission#>>'{canonical_tsfin_snapshot,candidate_id}'),
        (v_immutable_submission#>>'{hours_submission,canonical_tsfin_snapshot,candidate_id}'),
        (v_immutable_submission#>>'{expense_submission,canonical_tsfin_snapshot,candidate_id}')
      ) supplied(candidate_id_text)
      where nullif(supplied.candidate_id_text,'') is not null
        and supplied.candidate_id_text::uuid<>v_workflow.candidate_id
    ) or exists(
      select 1
      from (values
        (v_immutable_submission#>>'{canonical_tsfin_snapshot,client_id}'),
        (v_immutable_submission#>>'{hours_submission,canonical_tsfin_snapshot,client_id}'),
        (v_immutable_submission#>>'{expense_submission,canonical_tsfin_snapshot,client_id}')
      ) supplied(client_id_text)
      where nullif(supplied.client_id_text,'') is not null
        and supplied.client_id_text::uuid<>coalesce(v_contract.client_id,v_daily_fin.client_id)
    ) then
      raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_IDENTITY_MISMATCH' using errcode='22023';
    end if;
    if v_workflow.workflow_kind='DAILY' and exists(
      select 1
      from jsonb_array_elements(coalesce(
        v_immutable_submission#>'{timesheet_patch_json,actual_schedule_json}',
        v_immutable_submission#>'{hours_submission,timesheet_patch_json,actual_schedule_json}',
        '[]'::jsonb
      )) schedule_row
      where nullif(schedule_row->>'date','') is null
         or (schedule_row->>'date')::date<>v_workflow.work_date
    ) then
      raise exception 'CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH' using errcode='22023';
    elsif v_workflow.scope='WEEKLY' and exists(
      select 1
      from jsonb_array_elements(coalesce(
        v_immutable_submission#>'{timesheet_patch_json,actual_schedule_json}',
        v_immutable_submission#>'{hours_submission,timesheet_patch_json,actual_schedule_json}',
        '[]'::jsonb
      )) schedule_row
      where nullif(schedule_row->>'date','') is null
         or (schedule_row->>'date')::date not between v_workflow.week_ending_date-6 and v_workflow.week_ending_date
    ) then
      raise exception 'CANDIDATE_WEEK_DATE_OUT_OF_RANGE' using errcode='22023';
    end if;
    v_submission_hash:=private._candidate_sha256_jsonb_v1(v_immutable_submission);
    v_expense_submission:=case
      when v_workflow.workflow_kind='CONTRACT_COMBINED'
        and jsonb_typeof(v_immutable_submission->'expense_submission')='object'
        then v_immutable_submission->'expense_submission'
      else v_immutable_submission
    end;
    begin
      v_expense_value:=
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,expenses_pay_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,expenses_charge_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,mileage_pay_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,mileage_charge_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,mileage_units}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,travel_pay_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,travel_charge_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,accommodation_pay_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,accommodation_charge_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,other_pay_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,other_charge_ex_vat}','')::numeric,0));
      v_has_mileage:=
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,mileage_units}','')::numeric,0))>0
        or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,mileage_pay_ex_vat}','')::numeric,0))>0
        or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,mileage_charge_ex_vat}','')::numeric,0))>0;
      v_required_categories:=array[]::text[];
      if abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,travel_pay_ex_vat}','')::numeric,0))>0
         or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,travel_charge_ex_vat}','')::numeric,0))>0 then
        v_required_categories:=array_append(v_required_categories,'TRAVEL');
      end if;
      if abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,accommodation_pay_ex_vat}','')::numeric,0))>0
         or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,accommodation_charge_ex_vat}','')::numeric,0))>0 then
        v_required_categories:=array_append(v_required_categories,'ACCOMMODATION');
      end if;
      if abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,other_pay_ex_vat}','')::numeric,0))>0
         or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,other_charge_ex_vat}','')::numeric,0))>0
         or (
           (
             abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,expenses_pay_ex_vat}','')::numeric,0))>0
             or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,expenses_charge_ex_vat}','')::numeric,0))>0
           )
           and (
             abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,travel_pay_ex_vat}','')::numeric,0))
             +abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,travel_charge_ex_vat}','')::numeric,0))
             +abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,accommodation_pay_ex_vat}','')::numeric,0))
             +abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,accommodation_charge_ex_vat}','')::numeric,0))
             +abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,other_pay_ex_vat}','')::numeric,0))
             +abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,other_charge_ex_vat}','')::numeric,0))
           )=0
         ) then
        v_required_categories:=array_append(v_required_categories,'OTHER');
      end if;
      if v_has_mileage then
        v_required_categories:=array_append(v_required_categories,'MILEAGE');
      end if;
    exception when invalid_text_representation or numeric_value_out_of_range then
      raise exception 'CANDIDATE_EXPENSE_VALUE_INVALID' using errcode='22023';
    end;
    v_has_expenses:=v_expense_value>0;
    if v_workflow.workflow_kind in ('CONTRACT_HOURS','DAILY') and v_has_expenses then
      raise exception 'CANDIDATE_WORKFLOW_KIND_ECONOMICS_MISMATCH' using errcode='22023';
    end if;
    if v_workflow.workflow_kind in ('CONTRACT_COMBINED','CONTRACT_EXPENSE') and not v_has_expenses then
      raise exception 'CANDIDATE_EXPENSE_CLAIM_REQUIRED' using errcode='22023';
    end if;
    v_server_issue_codes:=private._candidate_submission_issue_codes_v1(
      v_workflow.id,v_immutable_submission,v_policy
    );
    v_duplicate_expense_review:=case
      when v_workflow.workflow_kind in ('CONTRACT_COMBINED','CONTRACT_EXPENSE')
       and cardinality(v_required_categories)>0
      then private._expense_duplicate_review_v1(v_workflow.id,v_required_categories)
      else jsonb_build_object(
        'required',false,
        'categories','[]'::jsonb,
        'prior_claim_count',0,
        'confirmation_digest',null
      )
    end;
    if coalesce((v_duplicate_expense_review->>'required')::boolean,false) then
      if not v_is_pending_expense_update and (
         lower(coalesce(v_payload#>>'{duplicate_expense_confirmation,confirmed}','false'))
           not in ('true','t','1','yes')
         or nullif(v_payload#>>'{duplicate_expense_confirmation,confirmation_digest}','')
              is distinct from v_duplicate_expense_review->>'confirmation_digest') then
        raise exception 'CANDIDATE_DUPLICATE_EXPENSE_CONFIRMATION_REQUIRED'
          using errcode='PT409',detail=v_duplicate_expense_review::text;
      end if;
      v_server_issue_codes:=v_server_issue_codes||jsonb_build_array('DUPLICATE_EXPENSE_REVIEW');
      for v_duplicate_expense_category in
        select jsonb_array_elements_text(v_duplicate_expense_review->'categories')
      loop
        v_server_issue_codes:=v_server_issue_codes
          ||jsonb_build_array('DUPLICATE_EXPENSE_'||v_duplicate_expense_category);
      end loop;
      select coalesce(jsonb_agg(distinct issue_code order by issue_code),'[]'::jsonb)
      into v_server_issue_codes
      from jsonb_array_elements_text(v_server_issue_codes) issue_code;
    end if;
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and state in ('PENDING','APPROVED')
      and not exists (
        select 1 from public.candidate_pending_expense_updates pending_update
        where pending_update.workflow_id=v_workflow.id
          and pending_update.state in ('EDITING','RENDERING')
      );

    if v_is_electronic then
      v_component_no:=0;
      v_review_ordinal:=0;
      if v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
        select * into v_source_component
        from public.candidate_submission_components
        where id=nullif(v_payload->>'candidate_signature_component_id','')::uuid
          and workflow_id=v_workflow.id and component_kind='CANDIDATE_SIGNATURE'
          and document_role='CANDIDATE_SIGNATURE' and state='IMMUTABLE'
          and source_content_sha256 is not null
          and (
            v_workflow.immutable_submission_sha256 is null
            or v_workflow.immutable_submission_sha256=v_submission_hash
            or (
              v_is_pending_expense_update
              and id=v_workflow.candidate_signature_component_id
              and exists(
                select 1
                from public.candidate_submission_components current_hours
                join public.candidate_submission_components prior_hours
                  on prior_hours.workflow_id=current_hours.workflow_id
                  and prior_hours.workflow_generation=
                    v_pending_expense_update.from_workflow_generation
                  and prior_hours.component_kind='HOURS_TIMESHEET'
                  and prior_hours.state='IMMUTABLE'
                  and prior_hours.source_content_sha256=current_hours.source_content_sha256
                where current_hours.workflow_id=v_workflow.id
                  and current_hours.workflow_generation=v_workflow.generation
                  and current_hours.component_kind='HOURS_TIMESHEET'
                  and current_hours.state='IMMUTABLE'
              )
            )
            or (
              source_component_id is null
              and created_at_utc>=coalesce(v_workflow.worker_submitted_at_utc,'-infinity'::timestamptz)
            )
          )
        for update;
        if not found then
          if v_workflow.immutable_submission_sha256 is not null
             and v_workflow.immutable_submission_sha256<>v_submission_hash then
            raise exception 'CANDIDATE_SIGNATURE_REQUIRED_AFTER_AMENDMENT' using errcode='55000';
          end if;
          raise exception 'CANDIDATE_SIGNATURE_REQUIRED' using errcode='55000';
        end if;
        v_component_no:=v_component_no+1;
        insert into public.candidate_submission_components(
          workflow_id,workflow_generation,component_no,timesheet_id,component_kind,document_role,state,
          source_component_id,storage_key,media_type,byte_size,source_content_sha256,immutable_at_utc,
          required,review_ordinal,review_render_state,final_signed_render_state,created_at_utc
        ) values (
          v_workflow.id,v_next_generation,v_component_no,v_workflow.target_timesheet_id,
          'CANDIDATE_SIGNATURE','CANDIDATE_SIGNATURE','IMMUTABLE',
          coalesce(v_source_component.source_component_id,v_source_component.id),v_source_component.storage_key,
          v_source_component.media_type,v_source_component.byte_size,v_source_component.source_content_sha256,p_now_utc,
          false,null,'NOT_REQUIRED','NOT_REQUIRED',p_now_utc
        ) returning * into v_signature_component;
        v_component_no:=v_component_no+1;
        v_review_ordinal:=v_review_ordinal+1;
        insert into public.candidate_submission_components(
          workflow_id,workflow_generation,component_no,timesheet_id,component_kind,document_role,state,
          immutable_at_utc,required,review_ordinal,review_render_state,final_signed_render_state,created_at_utc
        ) values (
          v_workflow.id,v_next_generation,v_component_no,v_workflow.target_timesheet_id,'HOURS_TIMESHEET',
          'ELECTRONIC_TIMESHEET_MANAGER_REVIEW','IMMUTABLE',p_now_utc,true,v_review_ordinal,
          'PENDING','PENDING',p_now_utc
        ) returning * into v_component;
      elsif nullif(v_payload->>'candidate_signature_component_id','') is not null then
        raise exception 'CONTRACT_EXPENSE_CANDIDATE_SIGNATURE_FORBIDDEN' using errcode='22023';
      end if;

      if v_workflow.workflow_kind in ('CONTRACT_COMBINED','CONTRACT_EXPENSE') then
        foreach v_required_category in array v_required_categories loop
          if not exists(
            select 1 from public.candidate_submission_components source_component
            where source_component.workflow_id=v_workflow.id
              and source_component.workflow_generation=v_workflow.generation
              and source_component.component_kind in ('EXPENSE_EVIDENCE','MILEAGE_FORM')
              and source_component.expense_category=v_required_category
              and source_component.state='IMMUTABLE'
              and source_component.source_content_sha256 is not null
          ) then raise exception 'EXPENSE_EVIDENCE_REQUIRED'
            using errcode='22023',detail=jsonb_build_object('category',v_required_category)::text; end if;
        end loop;
        if v_has_mileage and not exists(
          select 1 from public.candidate_submission_components source_component
          where source_component.workflow_id=v_workflow.id
            and source_component.workflow_generation=v_workflow.generation
            and source_component.component_kind='MILEAGE_FORM'
            and source_component.state='IMMUTABLE'
            and source_component.source_content_sha256 is not null
        ) then raise exception 'EXPENSE_EVIDENCE_REQUIRED'
          using errcode='22023',detail=jsonb_build_object('category','MILEAGE')::text; end if;

        v_component_no:=v_component_no+1;
        insert into public.candidate_submission_components(
          workflow_id,workflow_generation,component_no,timesheet_id,component_kind,document_role,state,
          immutable_at_utc,required,review_ordinal,review_render_state,final_signed_render_state,created_at_utc
        ) values (
          v_workflow.id,v_next_generation,v_component_no,v_workflow.target_timesheet_id,'EXPENSE_SUMMARY',
          'EXPENSE_MILEAGE_APPROVAL_SUMMARY','IMMUTABLE',p_now_utc,false,null,
          'NOT_REQUIRED','NOT_REQUIRED',p_now_utc
        );

        for v_source_component in
          select canonical_source.*
          from (
            select distinct on (
              source_component.component_kind,source_component.expense_category,
              source_component.document_role,
              coalesce(source_component.source_component_id,source_component.id),
              source_component.source_content_sha256
            ) source_component.*
            from public.candidate_submission_components source_component
            where source_component.workflow_id=v_workflow.id
              and source_component.workflow_generation=v_workflow.generation
              and source_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
              and source_component.state='IMMUTABLE'
              and source_component.source_content_sha256 is not null
            order by source_component.component_kind,source_component.expense_category,
              source_component.document_role,
              coalesce(source_component.source_component_id,source_component.id),
              source_component.source_content_sha256,
              source_component.component_no,source_component.id
          ) canonical_source
          order by case canonical_source.component_kind when 'MILEAGE_FORM' then 0 else 1 end,
            case canonical_source.expense_category
              when 'ACCOMMODATION' then 1 when 'TRAVEL' then 2 when 'MILEAGE' then 3
              when 'OTHER' then 4 else 5 end,
            canonical_source.component_no,canonical_source.id
        loop
          v_component_no:=v_component_no+1;
          v_review_ordinal:=v_review_ordinal+1;
          insert into public.candidate_submission_components(
            workflow_id,workflow_generation,component_no,timesheet_id,component_kind,expense_category,
            document_role,state,source_component_id,storage_key,media_type,byte_size,source_content_sha256,
            immutable_at_utc,required,review_ordinal,review_render_state,final_signed_render_state,created_at_utc
          ) values (
            v_workflow.id,v_next_generation,v_component_no,v_workflow.target_timesheet_id,
            v_source_component.component_kind,v_source_component.expense_category,v_source_component.document_role,
            'IMMUTABLE',coalesce(v_source_component.source_component_id,v_source_component.id),
            v_source_component.storage_key,v_source_component.media_type,v_source_component.byte_size,
            v_source_component.source_content_sha256,p_now_utc,true,v_review_ordinal,'PENDING','PENDING',p_now_utc
          );
        end loop;
      end if;

      update public.candidate_submission_components set
        state='SUPERSEDED',superseded_at_utc=p_now_utc,
        review_render_state=case when review_render_state='NOT_REQUIRED' then review_render_state else 'SUPERSEDED' end,
        final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED' then final_signed_render_state else 'SUPERSEDED' end
      where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
        and (required=true or document_role='MANAGER_SIGNATURE') and state<>'SUPERSEDED'
        and not exists(
          select 1 from public.candidate_pending_expense_updates pending_update
          where pending_update.workflow_id=v_workflow.id
            and pending_update.state in ('EDITING','RENDERING')
        );

      update public.candidate_submission_workflows set
        state='WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',generation=v_next_generation,
        route=case when upper(coalesce(v_payload->>'approval_route',route)) in ('PHONE','EMAIL')
          then upper(coalesce(v_payload->>'approval_route',route)) else 'ELECTRONIC' end,
        input_snapshot_json=v_immutable_submission,immutable_submission_json=v_immutable_submission,
        immutable_submission_sha256=v_submission_hash,
        policy_snapshot_json=v_policy,policy_snapshot_sha256=private._candidate_sha256_jsonb_v1(v_policy),
        candidate_signature_component_id=case when workflow_kind='CONTRACT_EXPENSE' then null else v_signature_component.id end,
        candidate_signature_sha256=case when workflow_kind='CONTRACT_EXPENSE' then null else v_signature_component.source_content_sha256 end,
        candidate_signed_at_utc=case when workflow_kind='CONTRACT_EXPENSE' then null
          when v_is_pending_expense_update then nullif(
            v_pending_expense_update.prior_workflow_snapshot_json->>'candidate_signed_at_utc',''
          )::timestamptz
          else coalesce(nullif(v_payload->>'candidate_signed_at_utc','')::timestamptz,p_now_utc) end,
        renderer_contract_version=coalesce(nullif(btrim(v_payload->>'renderer_contract_version'),''),'TIMESHEET_OFFICIAL_PDF_V1'),
        review_manifest_json=null,review_manifest_sha256=null,
        manager_name=null,manager_position=null,manager_signature_component_id=null,
        manager_signature_sha256=null,manager_approved_at_utc=null,
        issue_codes=v_server_issue_codes,worker_submitted_at_utc=p_now_utc,
        daily_context_sha256=null,canonical_financial_sha256=null,
        canonical_save_input_sha256=null,canonical_save_row_signature=null,
        canonical_save_financials_id=null,canonical_save_receipt_json=null,canonical_saved_at_utc=null,
        last_mutation_idempotency_key=p_idempotency_key,updated_at_utc=p_now_utc
      where id=v_workflow.id returning * into v_workflow;
      v_render_contract:=private._candidate_render_contract_v1(
        v_workflow.id,v_workflow.generation,'ELECTRONIC_MANAGER_REVIEW');
      v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
        'state',v_workflow.state,'generation',v_workflow.generation,
        'review_document_component_id',case when v_workflow.workflow_kind='CONTRACT_EXPENSE' then null else v_component.id end,
        'render_contract',v_render_contract,'idempotent_replay',false);
    else
      v_component_no:=0;
      if v_workflow.workflow_kind in ('CONTRACT_COMBINED','CONTRACT_EXPENSE') then
        foreach v_required_category in array v_required_categories loop
          if not exists(
            select 1 from public.candidate_submission_components source_component
            where source_component.workflow_id=v_workflow.id
              and source_component.workflow_generation=v_workflow.generation
              and source_component.component_kind in ('EXPENSE_EVIDENCE','MILEAGE_FORM')
              and source_component.expense_category=v_required_category
              and source_component.state='IMMUTABLE'
              and source_component.source_content_sha256 is not null
          ) then raise exception 'EXPENSE_EVIDENCE_REQUIRED'
            using errcode='22023',detail=jsonb_build_object('category',v_required_category)::text; end if;
        end loop;
        if v_has_mileage and not exists(
          select 1 from public.candidate_submission_components source_component
          where source_component.workflow_id=v_workflow.id
            and source_component.workflow_generation=v_workflow.generation
            and source_component.component_kind='MILEAGE_FORM'
            and source_component.expense_category='MILEAGE'
            and source_component.state='IMMUTABLE'
            and source_component.source_content_sha256 is not null
        ) then raise exception 'EXPENSE_EVIDENCE_REQUIRED'
          using errcode='22023',detail=jsonb_build_object('category','MILEAGE')::text; end if;

        for v_source_component in
          select canonical_source.*
          from (
            select distinct on (
              source_component.component_kind,source_component.expense_category,
              source_component.document_role,
              coalesce(source_component.source_component_id,source_component.id),
              source_component.source_content_sha256
            ) source_component.*
            from public.candidate_submission_components source_component
            where source_component.workflow_id=v_workflow.id
              and source_component.workflow_generation=v_workflow.generation
              and source_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
              and source_component.state='IMMUTABLE'
              and source_component.source_content_sha256 is not null
            order by source_component.component_kind,source_component.expense_category,
              source_component.document_role,
              coalesce(source_component.source_component_id,source_component.id),
              source_component.source_content_sha256,
              source_component.component_no,source_component.id
          ) canonical_source
          order by case canonical_source.component_kind when 'MILEAGE_FORM' then 0 else 1 end,
            canonical_source.component_no,canonical_source.id
        loop
          v_component_no:=v_component_no+1;
          insert into public.candidate_submission_components(
            workflow_id,workflow_generation,component_no,timesheet_id,component_kind,expense_category,
            document_role,state,source_component_id,storage_key,media_type,byte_size,source_content_sha256,
            immutable_at_utc,required,review_ordinal,review_render_state,final_signed_render_state,created_at_utc
          ) values (
            v_workflow.id,v_next_generation,v_component_no,v_workflow.target_timesheet_id,
            v_source_component.component_kind,v_source_component.expense_category,v_source_component.document_role,
            'IMMUTABLE',coalesce(v_source_component.source_component_id,v_source_component.id),
            v_source_component.storage_key,v_source_component.media_type,v_source_component.byte_size,
            v_source_component.source_content_sha256,p_now_utc,false,null,'NOT_REQUIRED','NOT_REQUIRED',p_now_utc
          );
        end loop;
      end if;
      update public.candidate_submission_components set
        state='SUPERSEDED',superseded_at_utc=p_now_utc,
        review_render_state=case when review_render_state='NOT_REQUIRED' then review_render_state else 'SUPERSEDED' end,
        final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED' then final_signed_render_state else 'SUPERSEDED' end
      where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
        and (required=true or document_role='MANAGER_SIGNATURE') and state<>'SUPERSEDED'
        and not exists(
          select 1 from public.candidate_pending_expense_updates pending_update
          where pending_update.workflow_id=v_workflow.id
            and pending_update.state in ('EDITING','RENDERING')
        );
      update public.candidate_submission_workflows set
        state='WORKER_SUBMITTED',generation=v_next_generation,route='PAPER',
        input_snapshot_json=v_immutable_submission,immutable_submission_json=v_immutable_submission,
        immutable_submission_sha256=private._candidate_sha256_jsonb_v1(v_immutable_submission),
        policy_snapshot_json=v_policy,policy_snapshot_sha256=private._candidate_sha256_jsonb_v1(v_policy),
        paper_return_manifest_json=null,paper_return_manifest_sha256=null,
        issue_codes=v_server_issue_codes,worker_submitted_at_utc=p_now_utc,
        daily_context_sha256=null,canonical_financial_sha256=null,
        canonical_save_input_sha256=null,canonical_save_row_signature=null,
        canonical_save_financials_id=null,canonical_save_receipt_json=null,canonical_saved_at_utc=null,
        last_mutation_idempotency_key=p_idempotency_key,updated_at_utc=p_now_utc
      where id=v_workflow.id returning * into v_workflow;
      v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
        'state',v_workflow.state,'generation',v_workflow.generation,'idempotent_replay',false);
    end if;
    update public.candidate_submission_workflows
    set last_mutation_response_json=v_response where id=v_workflow.id;

  elsif v_action='REGISTER_REVIEW_COMPONENT' then
    if v_workflow.state not in ('WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL') then
      raise exception 'MANAGER_REVIEW_DOCUMENT_STALE' using errcode='55000';
    end if;
    select * into v_component from public.candidate_submission_components
    where id=nullif(v_payload->>'component_id','')::uuid
      and workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and required=true and state<>'SUPERSEDED' for update;
    if not found then raise exception 'MANAGER_REVIEW_DOCUMENT_STALE' using errcode='55000'; end if;
    if coalesce(v_payload->>'content_sha256_hex','') !~ '^[0-9a-fA-F]{64}$'
       or coalesce(v_payload->>'render_input_sha256_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'MANAGER_REVIEW_RENDER_FAILED' using errcode='22023',
        detail=jsonb_build_object(
          'component_id',v_component.id,
          'expected_contract',v_render_contract,
          'received_receipt',v_receipt,
          'received_render_input_sha256',lower(coalesce(v_payload->>'render_input_sha256_hex',''))
        )::text;
    end if;
    v_digest:=decode(v_payload->>'content_sha256_hex','hex');
    v_render_input_hash:=decode(v_payload->>'render_input_sha256_hex','hex');
    v_receipt:=coalesce(v_payload->'renderer_receipt','{}'::jsonb);
    v_render_contract:=private._candidate_component_render_contract_v1(
      v_workflow.id,v_workflow.generation,v_component.id,'REVIEW');
    if v_render_input_hash<>decode(v_render_contract->>'render_input_sha256','hex')
       or nullif(btrim(v_payload->>'storage_key'),'') is null
       or lower(coalesce(v_payload->>'media_type','')) not in ('application/pdf','image/jpeg','image/png','image/webp')
       or coalesce((v_payload->>'page_count')::integer,0)<>1
       or coalesce((v_payload->>'byte_size')::bigint,0)<=0
       or upper(coalesce(v_receipt->>'form_variant',''))<>v_render_contract->>'form_variant'
       or nullif(v_receipt->>'workflow_id','')::uuid is distinct from v_workflow.id
       or coalesce((v_receipt->>'workflow_generation')::integer,0)<>v_workflow.generation
       or nullif(v_receipt->>'component_id','')::uuid is distinct from v_component.id
       or upper(coalesce(v_receipt->>'component_kind',''))<>v_component.component_kind
       or upper(coalesce(v_receipt->>'document_role',''))<>v_component.document_role
       or coalesce((v_receipt->>'review_ordinal')::integer,0)<>v_component.review_ordinal
       or upper(coalesce(v_receipt->>'scope',''))<>v_workflow.scope
       or coalesce((v_receipt->>'candidate_signature_embedded')::boolean,false)
          is distinct from (v_component.component_kind='HOURS_TIMESHEET')
       or coalesce((v_receipt->>'manager_signature_embedded')::boolean,false)=true
       or coalesce((v_receipt->>'manager_approval_date_embedded')::boolean,false)=true
       or coalesce((v_receipt->>'page_count')::integer,0)<>1
       or lower(coalesce(v_receipt->>'render_input_sha256',''))<>encode(v_render_input_hash,'hex') then
      raise exception 'MANAGER_REVIEW_RENDER_FAILED' using errcode='22023',
        detail=jsonb_build_object(
          'component_id',v_component.id,
          'expected_contract',v_render_contract,
          'received_receipt',v_receipt,
          'received_render_input_sha256',lower(coalesce(v_payload->>'render_input_sha256_hex',''))
        )::text;
    end if;
    if v_component.review_render_state='READY' then
      if v_component.review_storage_key=v_payload->>'storage_key'
         and v_component.review_content_sha256=v_digest
         and v_component.review_render_input_sha256=v_render_input_hash then
        v_response:=jsonb_build_object('ok',true,'idempotent_replay',true,
          'workflow_id',v_workflow.id,'generation',v_workflow.generation,
          'component_id',v_component.id,'state',v_workflow.state);
        if v_mutation_request_sha256 is not null then
          perform private._candidate_workflow_mutation_receipt_v1(
            v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
            v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
          );
        end if;
        return v_response;
      end if;
      raise exception 'MANAGER_REVIEW_DOCUMENT_STALE' using errcode='55000';
    end if;
    update public.candidate_submission_components set
      review_storage_key=v_payload->>'storage_key',review_content_sha256=v_digest,
      review_media_type=lower(v_payload->>'media_type'),review_byte_size=(v_payload->>'byte_size')::bigint,
      review_page_count=1,review_render_input_sha256=v_render_input_hash,
      review_renderer_contract_version=coalesce(nullif(v_payload->>'renderer_contract_version',''),v_workflow.renderer_contract_version),
      review_renderer_receipt_json=v_receipt,review_generated_at_utc=p_now_utc,
      review_render_state='READY'
    where id=v_component.id returning * into v_component;
    v_manifest:=private._candidate_review_manifest_v1(v_workflow.id,v_workflow.generation);
    if coalesce((v_manifest->>'all_ready')::boolean,false) then
      update public.candidate_submission_workflows set
        state='READY_FOR_MANAGER_APPROVAL',review_manifest_json=v_manifest,
        review_manifest_sha256=decode(v_manifest->>'manifest_sha256','hex'),
        last_mutation_idempotency_key=p_idempotency_key,updated_at_utc=p_now_utc
      where id=v_workflow.id returning * into v_workflow;
    end if;
    v_response:=jsonb_build_object('ok',true,'idempotent_replay',false,
      'workflow_id',v_workflow.id,'state',v_workflow.state,'generation',v_workflow.generation,
      'component_id',v_component.id,'review_document_ready',v_component.review_render_state='READY',
      'review_manifest',v_manifest);
    update public.candidate_submission_workflows set last_mutation_response_json=v_response where id=v_workflow.id;

  elsif v_action in ('SELECT_PHONE_APPROVAL','CREATE_EMAIL_APPROVAL_REQUEST') then
    if v_workflow.state not in ('READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL') then
      raise exception 'MANAGER_REVIEW_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
    v_manifest:=private._candidate_review_manifest_v1(v_workflow.id,v_workflow.generation);
    if coalesce((v_manifest->>'all_ready')::boolean,false)=false
       or decode(v_manifest->>'manifest_sha256','hex') is distinct from v_workflow.review_manifest_sha256 then
      raise exception 'MANAGER_REVIEW_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
    select array_agg(c.id order by c.review_ordinal,c.id) into v_component_ids
    from public.candidate_submission_components c
    where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
      and c.required=true and c.state<>'SUPERSEDED';
    select coalesce(max(request_generation),0)+1 into v_request_generation
    from public.candidate_approval_requests where workflow_id=v_workflow.id;
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and state='PENDING';
    if v_action='CREATE_EMAIL_APPROVAL_REQUEST' then
      if coalesce(v_payload->>'approval_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$' then
        raise exception 'CANDIDATE_APPROVAL_TOKEN_INVALID' using errcode='22023';
      end if;
      v_token_hash:=decode(v_payload->>'approval_token_hash_hex','hex');
      v_email_check:=private._candidate_manager_email_allowed_v1(
        v_policy->'manager_approval_policy',v_payload->>'manager_email',
        v_policy->'barred_manager_email_domains');
      if coalesce((v_email_check->>'allowed')::boolean,false)=false then
        raise exception 'MANAGER_EMAIL_NOT_ALLOWED' using errcode='22023',detail=v_email_check::text;
      end if;
      insert into public.candidate_approval_requests(
        workflow_id,workflow_generation,request_generation,method,state,manager_email_normalized,
        token_hash,expires_at_utc,initial_sent_at_utc,last_sent_at_utc,next_reminder_at_utc,
        review_manifest_sha256,required_component_ids,required_component_manifest_json,
        manager_review_timesheet_component_id,manager_review_timesheet_sha256,
        idempotency_key,created_at_utc,updated_at_utc
      ) values (
        v_workflow.id,v_workflow.generation,v_request_generation,'EMAIL','PENDING',
        v_email_check->>'email_normalized',v_token_hash,p_now_utc+interval '7 days',
        null,null,null,
        decode(v_manifest->>'manifest_sha256','hex'),v_component_ids,v_manifest->'required_components',
        nullif(v_manifest->>'manager_review_timesheet_component_id','')::uuid,
        case when nullif(v_manifest->>'manager_review_timesheet_sha256','') is null then null
          else decode(v_manifest->>'manager_review_timesheet_sha256','hex') end,
        p_idempotency_key,p_now_utc,p_now_utc
      ) returning * into v_approval;
      v_mail_id:=private._candidate_queue_mail_v1(
        coalesce(v_payload->'mail','{}'::jsonb)||jsonb_build_object(
          'payment_scope_json',jsonb_build_object(
            'candidate_mail_authority','MANAGER_APPROVAL_V1',
            'candidate_manager_mail_kind','INITIAL',
            'candidate_manager_workflow_id',v_workflow.id,
            'candidate_manager_workflow_generation',v_workflow.generation,
            'candidate_approval_request_id',v_approval.id,
            'candidate_approval_request_generation',v_approval.request_generation,
            'candidate_manager_mail_retired',false
          )
        ),v_approval.manager_email_normalized,
        'CANDIDATE_MANAGER_APPROVAL_V1:'||v_approval.id::text||':0',
        'candidate-manager-approval:'||v_approval.id::text,v_workflow.id,p_now_utc);
    else
      if coalesce(v_payload->>'approval_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$'
         or nullif(v_payload->>'expires_at_utc','')::timestamptz<=p_now_utc
         or nullif(v_payload->>'expires_at_utc','')::timestamptz>p_now_utc+interval '2 hours' then
        raise exception 'CANDIDATE_APPROVAL_TOKEN_INVALID' using errcode='22023';
      end if;
      v_token_hash:=decode(v_payload->>'approval_token_hash_hex','hex');
      insert into public.candidate_approval_requests(
        workflow_id,workflow_generation,request_generation,method,state,token_hash,expires_at_utc,
        review_manifest_sha256,required_component_ids,required_component_manifest_json,
        manager_review_timesheet_component_id,manager_review_timesheet_sha256,
        idempotency_key,created_at_utc,updated_at_utc
      ) values (
        v_workflow.id,v_workflow.generation,v_request_generation,'PHONE','PENDING',v_token_hash,
        nullif(v_payload->>'expires_at_utc','')::timestamptz,
        decode(v_manifest->>'manifest_sha256','hex'),v_component_ids,v_manifest->'required_components',
        nullif(v_manifest->>'manager_review_timesheet_component_id','')::uuid,
        case when nullif(v_manifest->>'manager_review_timesheet_sha256','') is null then null
          else decode(v_manifest->>'manager_review_timesheet_sha256','hex') end,
        p_idempotency_key,p_now_utc,p_now_utc
      ) returning * into v_approval;
    end if;
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state','AWAITING_MANAGER_APPROVAL','generation',v_workflow.generation,
      'approval_request_id',v_approval.id,'approval_request_generation',v_approval.request_generation,
      'method',v_approval.method,
      'review_manifest_sha256',encode(v_approval.review_manifest_sha256,'hex'),
      'issued_at_utc',v_approval.created_at_utc,
      'expires_at_utc',v_approval.expires_at_utc,'mail_outbox_id',v_mail_id);
    if v_action='SELECT_PHONE_APPROVAL' then
      if coalesce(v_payload->>'handoff_token_key_version','') !~ '^[1-9][0-9]{0,2}$'
         or (v_payload->>'handoff_token_key_version')::integer>32 then
        raise exception 'CANDIDATE_REPLAY_KEY_VERSION_INVALID' using errcode='22023';
      end if;
      if jsonb_typeof(v_payload->'public_broker_binding')<>'object'
         or coalesce(v_payload#>>'{public_broker_binding,contract_version}','')
              <>'CANDIDATE_PUBLIC_PHONE_BINDING_V1'
         or coalesce(v_payload#>>'{public_broker_binding,public_session_binding_sha256}','')
              !~ '^[0-9a-f]{64}$'
         or (v_payload#>'{public_broker_binding,device_binding_sha256}') is not null
            and jsonb_typeof(v_payload#>'{public_broker_binding,device_binding_sha256}')<>'null'
            and coalesce(v_payload#>>'{public_broker_binding,device_binding_sha256}','')
                  !~ '^[0-9a-f]{64}$'
         or coalesce(v_payload->>'broker_handoff_key_version','') !~ '^[1-9][0-9]{0,4}$'
         or (v_payload->>'broker_handoff_key_version')::integer>65535 then
        raise exception 'CANDIDATE_PHONE_HANDOFF_BINDING_INVALID' using errcode='22023';
      end if;
      v_response:=v_response||jsonb_build_object(
        'approval_token_hash_hex',encode(v_approval.token_hash,'hex'),
        'handoff_token_key_version',(v_payload->>'handoff_token_key_version')::integer,
        'public_broker_binding',v_payload->'public_broker_binding',
        'broker_handoff_key_version',(v_payload->>'broker_handoff_key_version')::integer
      );
    end if;
    update public.candidate_submission_workflows set
      state='AWAITING_MANAGER_APPROVAL',route=v_approval.method,policy_snapshot_json=v_policy,
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action='BEGIN_MANAGER_REVIEW' then
    if v_is_public_manager_action then
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and token_hash=v_token_hash for update;
    else
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
        and method='PHONE' and state='PENDING'
        and (not coalesce((v_payload->>'service_office_action')::boolean,false)
          or (id=(v_payload->>'approval_request_id')::uuid
            and request_generation=(v_payload->>'approval_request_generation')::integer))
      for update;
    end if;
    if not found or v_approval.state<>'PENDING' then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
    end if;
    if v_approval.expires_at_utc<=p_now_utc then
      raise exception 'MANAGER_APPROVAL_REQUEST_EXPIRED' using errcode='28000';
    end if;
    if v_approval.workflow_generation<>v_workflow.generation
       or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256 then
      raise exception 'MANAGER_APPROVAL_REQUEST_SUPERSEDED' using errcode='40001';
    end if;
    update public.candidate_approval_requests set
      review_started_at_utc=coalesce(review_started_at_utc,p_now_utc),updated_at_utc=p_now_utc
    where id=v_approval.id returning * into v_approval;
    select count(*) into v_reviewed_count from unnest(v_approval.required_component_ids) u(id)
    where v_approval.review_progress_json ? u.id::text;
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'workflow_generation',v_workflow.generation,'approval_request_id',v_approval.id,
      'approval_request_generation',v_approval.request_generation,
      'workflow_kind',v_workflow.workflow_kind,
      'method',v_approval.method,'expires_at_utc',v_approval.expires_at_utc,
      'manifest_sha256',encode(v_approval.review_manifest_sha256,'hex'),
      'page_count',coalesce((select sum(coalesce(c.review_page_count,1))
        from public.candidate_submission_components c where c.id=any(v_approval.required_component_ids)),0),
      'ordered_components',(select coalesce(jsonb_agg(
        component.value||jsonb_build_object(
          'viewed',v_approval.review_progress_json ? (component.value->>'component_id')
        ) order by (component.value->>'ordinal')::integer
      ),'[]'::jsonb) from jsonb_array_elements(v_approval.required_component_manifest_json) component),
      'reviewed_count',v_reviewed_count,
      'all_pages_viewed',v_reviewed_count=cardinality(v_approval.required_component_ids),
      'manager_identity_requirements',jsonb_build_object('name_required',true,'position_required',true,'signature_required',true),
      'can_approve',true,'can_refuse',true);
    if v_mutation_request_sha256 is not null then
      perform private._candidate_workflow_mutation_receipt_v1(
        v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
        v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
      );
    end if;
    return v_response;

  elsif v_action='RECORD_REVIEW_PROGRESS' then
    if v_is_public_manager_action then
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and token_hash=v_token_hash for update;
    else
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
        and method='PHONE' and state='PENDING'
        and (not coalesce((v_payload->>'service_office_action')::boolean,false)
          or (id=(v_payload->>'approval_request_id')::uuid
            and request_generation=(v_payload->>'approval_request_generation')::integer))
      for update;
    end if;
    if not found or v_approval.state<>'PENDING' then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
    end if;
    if v_approval.expires_at_utc<=p_now_utc then
      raise exception 'MANAGER_APPROVAL_REQUEST_EXPIRED' using errcode='28000';
    end if;
    if lower(coalesce(v_payload->>'manifest_sha256_hex',''))<>encode(v_approval.review_manifest_sha256,'hex') then
      raise exception 'MANAGER_REVIEW_MANIFEST_MISMATCH' using errcode='40001';
    end if;
    select count(*) into v_reviewed_count from unnest(v_approval.required_component_ids) u(id)
    where v_approval.review_progress_json ? u.id::text;
    if (v_approval.method='EMAIL' or nullif(v_payload->>'progress_version','') is not null)
       and coalesce(nullif(v_payload->>'progress_version','')::integer,-1)<>v_reviewed_count then
      raise exception 'MANAGER_REVIEW_PROGRESS_CONFLICT' using errcode='40001';
    end if;
    select * into v_component from public.candidate_submission_components
    where id=nullif(v_payload->>'component_id','')::uuid
      and id=any(v_approval.required_component_ids)
      and workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and review_render_state='READY' for update;
    if not found or lower(coalesce(v_payload->>'component_sha256_hex',''))<>encode(v_component.review_content_sha256,'hex') then
      raise exception 'MANAGER_REVIEW_MANIFEST_MISMATCH' using errcode='40001';
    end if;
    update public.candidate_approval_requests set
      review_progress_json=review_progress_json||jsonb_build_object(v_component.id::text,jsonb_build_object(
        'component_id',v_component.id,'component_sha256',encode(v_component.review_content_sha256,'hex'),
        'manifest_sha256',encode(v_approval.review_manifest_sha256,'hex'),
        'viewed_receipt',coalesce(v_payload->'viewed_receipt','{}'::jsonb),
        'reviewed_at_utc',p_now_utc)),
      review_started_at_utc=coalesce(review_started_at_utc,p_now_utc),updated_at_utc=p_now_utc
    where id=v_approval.id returning * into v_approval;
    update public.candidate_submission_components set manager_reviewed_at_utc=p_now_utc
    where id=v_component.id;
    select count(*) into v_reviewed_count from unnest(v_approval.required_component_ids) u(id)
    where v_approval.review_progress_json ? u.id::text;
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'generation',v_workflow.generation,'approval_request_id',v_approval.id,
      'approval_request_generation',v_approval.request_generation,
      'component_id',v_component.id,'reviewed_count',v_reviewed_count,
      'required_count',cardinality(v_approval.required_component_ids),
      'progress_version',v_reviewed_count,
      'all_pages_viewed',v_reviewed_count=cardinality(v_approval.required_component_ids));
    update public.candidate_submission_workflows set
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action in ('PHONE_APPROVE','EMAIL_APPROVE') then
    if v_is_public_manager_action then
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and token_hash=v_token_hash for update;
    else
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
        and method='PHONE' and state='PENDING'
        and (not coalesce((v_payload->>'service_office_action')::boolean,false)
          or (id=(v_payload->>'approval_request_id')::uuid
            and request_generation=(v_payload->>'approval_request_generation')::integer))
      for update;
    end if;
    if not found or v_approval.state<>'PENDING' then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
    end if;
    if v_approval.expires_at_utc<=p_now_utc then
      raise exception 'MANAGER_APPROVAL_REQUEST_EXPIRED' using errcode='28000';
    end if;
    if (v_action='EMAIL_APPROVE' and v_approval.method<>'EMAIL')
       or (v_action='PHONE_APPROVE' and v_approval.method<>'PHONE') then
      raise exception 'MANAGER_APPROVAL_METHOD_MISMATCH' using errcode='28000';
    end if;
    if lower(coalesce(v_payload->>'manifest_sha256_hex',''))<>encode(v_approval.review_manifest_sha256,'hex')
       or v_approval.workflow_generation<>v_workflow.generation
       or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256 then
      raise exception 'MANAGER_REVIEW_MANIFEST_MISMATCH' using errcode='40001';
    end if;
    if exists(
      select 1 from unnest(v_approval.required_component_ids) u(id)
      join public.candidate_submission_components c on c.id=u.id
      where not (v_approval.review_progress_json ? u.id::text)
         or v_approval.review_progress_json#>>array[u.id::text,'component_sha256']
            is distinct from encode(c.review_content_sha256,'hex')
    ) then
      raise exception 'MANAGER_REVIEW_COMPONENT_NOT_REVIEWED' using errcode='55000';
    end if;
    if nullif(btrim(coalesce(v_payload->>'manager_name','')),'') is null
       or nullif(btrim(coalesce(v_payload->>'manager_position','')),'') is null
       or pg_catalog.length(btrim(v_payload->>'manager_name'))>200
       or pg_catalog.length(btrim(v_payload->>'manager_position'))>200
       or (v_approval.method='EMAIL' and (
         coalesce(v_payload->>'attestation_version','')<>'MANAGER_APPROVAL_ATTESTATION_V1'
         or coalesce((v_payload->>'attestation_accepted')::boolean,false)<>true
       )) then
      raise exception 'MANAGER_SIGNATURE_REQUIRED' using errcode='22023';
    end if;
    select * into v_signature_component from public.candidate_submission_components
    where id=nullif(v_payload->>'signature_component_id','')::uuid
      and workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and document_role='MANAGER_SIGNATURE' and state='IMMUTABLE'
      and approval_request_id=v_approval.id
      and source_content_sha256 is not null for update;
    if not found then raise exception 'MANAGER_SIGNATURE_REQUIRED' using errcode='22023'; end if;
    update public.candidate_approval_requests set
      state='APPROVED',manager_name=btrim(v_payload->>'manager_name'),
      manager_position=btrim(v_payload->>'manager_position'),signature_component_id=v_signature_component.id,
      approved_at_utc=p_now_utc,review_completed_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where id=v_approval.id returning * into v_approval;
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and id<>v_approval.id and state='PENDING';
    perform public.candidate_manager_email_route_receipt_retire_v1(
      v_workflow.id,v_approval.id,'MANAGER_APPROVED',p_now_utc
    );
    update public.candidate_submission_components set manager_approved_at_utc=p_now_utc
    where id=any(v_approval.required_component_ids);
    update public.candidate_submission_workflows set
      state='MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',route=v_approval.method,
      manager_name=v_approval.manager_name,manager_position=v_approval.manager_position,
      manager_signature_component_id=v_signature_component.id,
      manager_signature_sha256=v_signature_component.source_content_sha256,
      manager_approved_at_utc=p_now_utc,last_mutation_idempotency_key=p_idempotency_key,
      updated_at_utc=p_now_utc
    where id=v_workflow.id returning * into v_workflow;
    v_render_contract:=private._candidate_render_contract_v1(
      v_workflow.id,v_workflow.generation,'FINAL_SIGNED');
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state',v_workflow.state,'generation',v_workflow.generation,
      'approval_request_id',v_approval.id,'approval_request_generation',v_approval.request_generation,
      'approved_at_utc',v_approval.approved_at_utc,
      'final_render_contract',v_render_contract);
    update public.candidate_submission_workflows set last_mutation_response_json=v_response
    where id=v_workflow.id;
    perform private._candidate_notification_insert_v1(v_account_id,v_candidate_id,v_workflow.id,
      v_workflow.target_timesheet_id,'MANAGER_APPROVED','manager_approval',
      'candidate-manager-approved-v1','{}'::jsonb,
      jsonb_build_object('type','workflow','workflow_id',v_workflow.id),
      'CANDIDATE_MANAGER_APPROVED_V1:'||v_workflow.id::text||':'||v_workflow.generation::text,p_now_utc);

  elsif v_action='REGISTER_FINAL_SIGNED_DOCUMENT' then
    if v_workflow.state not in ('MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE') then
      raise exception 'FINAL_SIGNED_DOCUMENT_STALE' using errcode='55000';
    end if;
    select * into v_component from public.candidate_submission_components
    where id=nullif(v_payload->>'component_id','')::uuid
      and workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and required=true and state='IMMUTABLE'
      and review_render_state='READY' for update;
    if not found then raise exception 'FINAL_SIGNED_DOCUMENT_STALE' using errcode='55000'; end if;
    if coalesce(v_payload->>'content_sha256_hex','') !~ '^[0-9a-fA-F]{64}$'
       or coalesce(v_payload->>'render_input_sha256_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='22023';
    end if;
    v_digest:=decode(v_payload->>'content_sha256_hex','hex');
    v_render_input_hash:=decode(v_payload->>'render_input_sha256_hex','hex');
    v_receipt:=coalesce(v_payload->'renderer_receipt','{}'::jsonb);
    v_render_contract:=private._candidate_component_render_contract_v1(
      v_workflow.id,v_workflow.generation,v_component.id,'FINAL');
    if v_render_input_hash<>decode(v_render_contract->>'render_input_sha256','hex')
       or v_render_input_hash is distinct from v_component.review_render_input_sha256
       or nullif(btrim(v_payload->>'storage_key'),'') is null
       or lower(coalesce(v_payload->>'media_type',''))<>'application/pdf'
       or coalesce((v_payload->>'page_count')::integer,0)<>1
       or coalesce((v_payload->>'byte_size')::bigint,0)<=0
       or upper(coalesce(v_receipt->>'form_variant',''))<>v_render_contract->>'form_variant'
       or nullif(v_receipt->>'workflow_id','')::uuid is distinct from v_workflow.id
       or coalesce((v_receipt->>'workflow_generation')::integer,0)<>v_workflow.generation
       or nullif(v_receipt->>'component_id','')::uuid is distinct from v_component.id
       or upper(coalesce(v_receipt->>'component_kind',''))<>v_component.component_kind
       or upper(coalesce(v_receipt->>'document_role',''))<>v_component.document_role
       or coalesce((v_receipt->>'review_ordinal')::integer,0)<>v_component.review_ordinal
       or upper(coalesce(v_receipt->>'scope',''))<>v_workflow.scope
       or coalesce((v_receipt->>'candidate_signature_embedded')::boolean,false)
          is distinct from (v_component.component_kind='HOURS_TIMESHEET')
       or coalesce((v_receipt->>'manager_signature_embedded')::boolean,false)=false
       or coalesce((v_receipt->>'manager_approval_date_embedded')::boolean,false)=false
       or coalesce((v_receipt->>'page_count')::integer,0)<>1
       or lower(coalesce(v_receipt->>'render_input_sha256',''))<>encode(v_render_input_hash,'hex')
       or (v_component.component_kind='HOURS_TIMESHEET' and
          lower(coalesce(v_receipt->>'candidate_signature_sha256',''))<>encode(v_workflow.candidate_signature_sha256,'hex'))
       or lower(coalesce(v_receipt->>'manager_signature_sha256',''))<>encode(v_workflow.manager_signature_sha256,'hex')
       or btrim(coalesce(v_receipt->>'manager_name',''))<>v_workflow.manager_name
       or btrim(coalesce(v_receipt->>'manager_position',''))<>v_workflow.manager_position
       or nullif(v_receipt->>'manager_approved_at_utc','')::timestamptz is distinct from v_workflow.manager_approved_at_utc then
      raise exception 'FINAL_RENDER_INPUT_MISMATCH' using errcode='40001';
    end if;
    if v_component.final_signed_render_state='READY' then
      if v_component.final_signed_storage_key=v_payload->>'storage_key'
         and v_component.final_signed_content_sha256=v_digest
         and v_component.final_signed_render_input_sha256=v_render_input_hash then
        v_response:=jsonb_build_object('ok',true,'idempotent_replay',true,
          'workflow_id',v_workflow.id,'generation',v_workflow.generation,
          'component_id',v_component.id,'state',v_workflow.state);
        if v_mutation_request_sha256 is not null then
          perform private._candidate_workflow_mutation_receipt_v1(
            v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
            v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
          );
        end if;
        return v_response;
      end if;
      raise exception 'FINAL_SIGNED_DOCUMENT_STALE' using errcode='55000';
    end if;
    update public.candidate_submission_components set
      final_signed_storage_key=v_payload->>'storage_key',final_signed_content_sha256=v_digest,
      final_signed_media_type='application/pdf',final_signed_byte_size=(v_payload->>'byte_size')::bigint,
      final_signed_page_count=1,final_signed_render_input_sha256=v_render_input_hash,
      final_signed_renderer_contract_version=coalesce(nullif(v_payload->>'renderer_contract_version',''),v_workflow.renderer_contract_version),
      final_signed_renderer_receipt_json=v_receipt,final_signed_generated_at_utc=p_now_utc,
      final_signed_render_state='READY'
    where id=v_component.id returning * into v_component;
    select exists(
      select 1 from public.candidate_submission_components c
      where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
        and c.required=true and c.state<>'SUPERSEDED'
    ) and not exists(
      select 1 from public.candidate_submission_components c
      where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
        and c.required=true and c.state<>'SUPERSEDED'
        and c.final_signed_render_state<>'READY'
    ) into v_all_final_ready;
    v_response:=jsonb_build_object('ok',true,'idempotent_replay',false,
      'workflow_id',v_workflow.id,
      'state',case when v_all_final_ready then 'READY_TO_FINALISE' else 'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT' end,
      'generation',v_workflow.generation,'component_id',v_component.id,
      'component_final_signed_document_ready',true,'all_final_signed_documents_ready',v_all_final_ready);
    update public.candidate_submission_workflows set
      state=case when v_all_final_ready then 'READY_TO_FINALISE' else 'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT' end,
      last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action='BEGIN_CANONICAL_DAILY_SAVE' then
    if v_workflow.workflow_kind<>'DAILY' or v_workflow.scope<>'DAILY'
       or v_workflow.state<>'READY_TO_FINALISE' or v_workflow.route not in ('PHONE','EMAIL') then
      raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_NOT_READY' using errcode='55000';
    end if;
    if not private._candidate_daily_entitled_v1(v_workflow.candidate_id) then
      raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
    end if;
    v_daily_input:=private._candidate_daily_canonical_save_input_v1(v_workflow.id,v_workflow.generation);
    v_expected_save_hash:=private._candidate_sha256_jsonb_v1(v_daily_input);
    -- An unresolved new Daily receipt must not enter the financial save/process
    -- path. Its finaliser still verifies the same manager, documents and input.
    if coalesce((v_daily_receipt_context->>'candidate_first_receipt')::boolean,false)
       and coalesce((v_daily_receipt_context->>'office_resolution_pending')::boolean,false) then
      v_response:=jsonb_build_object(
        'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
        'state',v_workflow.state,'receipt_mode','DAILY_FACTUAL',
        'timesheet_id',v_workflow.target_timesheet_id,
        'canonical_save_input_sha256_hex',encode(v_expected_save_hash,'hex'));
      if v_mutation_request_sha256 is not null then
        perform private._candidate_workflow_mutation_receipt_v1(
          v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
          v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc);
      end if;
      return v_response;
    end if;
    v_daily_context:=private._candidate_daily_context_contract_v1(
      v_workflow.id,v_workflow.generation
    );
    v_daily_context_hash:=private._candidate_sha256_jsonb_v1(v_daily_context);
    if v_workflow.canonical_saved_at_utc is not null
       and v_workflow.canonical_save_input_sha256=v_expected_save_hash
       and v_workflow.canonical_save_financials_id is not null
       and nullif(btrim(coalesce(v_workflow.canonical_save_row_signature,'')),'') is not null then
      v_response:=jsonb_build_object(
        'ok',true,'idempotent_replay',true,'workflow_id',v_workflow.id,
        'generation',v_workflow.generation,'state',v_workflow.state,
        'canonical_save_registered',true,
        'canonical_save_input_sha256_hex',encode(v_expected_save_hash,'hex'),
        'canonical_context_sha256_hex',encode(v_workflow.daily_context_sha256,'hex'),
        'canonical_save_row_signature',v_workflow.canonical_save_row_signature,
        'canonical_save_financials_id',v_workflow.canonical_save_financials_id
      );
      if v_mutation_request_sha256 is not null then
        perform private._candidate_workflow_mutation_receipt_v1(
          v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
          v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
        );
      end if;
      return v_response;
    end if;
    v_current_row_signature:=nullif(btrim(v_daily_context->>'pre_save_row_signature'),'');
    if v_current_row_signature is null then
      raise exception 'CANDIDATE_DAILY_ROW_SIGNATURE_REQUIRED' using errcode='55000';
    end if;
    v_response:=jsonb_build_object(
      'ok',true,'idempotent_replay',false,'workflow_id',v_workflow.id,
      'generation',v_workflow.generation,'state',v_workflow.state,
      'canonical_save_registered',false,'canonical_save_input',v_daily_input,
      'canonical_save_input_sha256_hex',encode(v_expected_save_hash,'hex'),
      'canonical_context',v_daily_context,
      'canonical_context_sha256_hex',encode(v_daily_context_hash,'hex'),
      'expected_row_signature',v_current_row_signature
    );
    update public.candidate_submission_workflows set
      daily_context_sha256=v_daily_context_hash,canonical_financial_sha256=null,
      canonical_save_input_sha256=null,canonical_save_row_signature=null,
      canonical_save_financials_id=null,canonical_save_receipt_json=null,canonical_saved_at_utc=null,
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc
    where id=v_workflow.id returning * into v_workflow;

  elsif v_action='MANAGER_REFUSE' then
    if v_is_public_manager_action then
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and token_hash=v_token_hash for update;
    else
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
        and state='PENDING'
        and (not coalesce((v_payload->>'service_office_action')::boolean,false)
          or (id=(v_payload->>'approval_request_id')::uuid
            and request_generation=(v_payload->>'approval_request_generation')::integer))
      for update;
    end if;
    if not found or v_approval.state<>'PENDING' then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
    end if;
    if v_approval.expires_at_utc<=p_now_utc then
      raise exception 'MANAGER_APPROVAL_REQUEST_EXPIRED' using errcode='28000';
    end if;
    if nullif(btrim(coalesce(v_payload->>'reason','')),'') is null
       or pg_catalog.length(btrim(v_payload->>'reason'))>1000 then
      raise exception 'MANAGER_REFUSAL_REASON_REQUIRED' using errcode='22023';
    end if;
    update public.candidate_approval_requests set
      state='REFUSED',refusal_reason=btrim(v_payload->>'reason'),refused_at_utc=p_now_utc,
      updated_at_utc=p_now_utc where id=v_approval.id;
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and id<>v_approval.id and state='PENDING';
    perform public.candidate_manager_email_route_receipt_retire_v1(
      v_workflow.id,v_approval.id,'MANAGER_REFUSED',p_now_utc
    );
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,'state','REFUSED',
      'generation',v_workflow.generation,'approval_request_id',v_approval.id,
      'approval_request_generation',v_approval.request_generation,
      'refused_at_utc',p_now_utc,'rejection_scope','COMPLETE_ELECTRONIC_TRANSACTION');
    update public.candidate_submission_workflows set
      state='REFUSED',rejection_reason=btrim(v_payload->>'reason'),
      rejection_scope='COMPLETE_ELECTRONIC_TRANSACTION',last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,updated_at_utc=p_now_utc where id=v_workflow.id;
    perform private._candidate_notification_insert_v1(v_account_id,v_candidate_id,v_workflow.id,
      v_workflow.target_timesheet_id,'MANAGER_REFUSED','manager_refusal','candidate-manager-refused-v1',
      jsonb_build_object('reason',btrim(v_payload->>'reason')),
      jsonb_build_object('type','workflow','workflow_id',v_workflow.id),
      'CANDIDATE_MANAGER_REFUSED_V1:'||v_workflow.id::text||':'||v_workflow.generation::text,p_now_utc);

  elsif v_action='REMIND' then
    if nullif(v_payload->>'approval_request_id','') is null
       or nullif(v_payload->>'approval_request_generation','') is null then
      raise exception 'CANDIDATE_REQUEST_GENERATION_STALE' using errcode='40001';
    end if;
    select * into v_approval from public.candidate_approval_requests
    where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and method='EMAIL' and state='PENDING'
      and id=(v_payload->>'approval_request_id')::uuid
      and request_generation=(v_payload->>'approval_request_generation')::integer
    for update skip locked;
    if not found or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256 then
      raise exception 'MANAGER_REMINDER_NOT_ELIGIBLE' using errcode='55000';
    end if;
    if coalesce(v_payload->>'approval_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'CANDIDATE_APPROVAL_TOKEN_INVALID' using errcode='22023';
    end if;
    select max(manager_mail.sent_at),count(*) filter (
      where manager_mail.status='QUEUED' and manager_mail.sent_at is null
        and lower(coalesce(manager_mail.payment_scope_json->>'candidate_manager_mail_retired','false'))
              in ('false','f','0','no')
    )::integer
    into v_manager_provider_accepted_at,v_manager_pending_mail_count
    from public.mail_outbox manager_mail
    where upper(coalesce(manager_mail.payment_scope_json->>'candidate_mail_authority',''))
            ='MANAGER_APPROVAL_V1'
      and manager_mail.payment_scope_json->>'candidate_manager_workflow_id'=v_workflow.id::text
      and manager_mail.payment_scope_json->>'candidate_manager_workflow_generation'=v_workflow.generation::text
      and manager_mail.payment_scope_json->>'candidate_approval_request_id'=v_approval.id::text
      and manager_mail.payment_scope_json->>'candidate_approval_request_generation'=v_approval.request_generation::text
      and upper(coalesce(manager_mail.payment_scope_json->>'candidate_manager_mail_kind',''))
            in ('INITIAL','REMINDER','RENEWAL')
      and (
        manager_mail.status='QUEUED'
        or (manager_mail.status='SENT' and manager_mail.sent_at is not null
          and upper(coalesce(manager_mail.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK'))
      );
    if v_approval.expires_at_utc<=p_now_utc or v_approval.resend_count>=5
       or v_manager_provider_accepted_at is null
       or v_manager_provider_accepted_at+interval '24 hours'>p_now_utc
       or v_manager_pending_mail_count>0 then
      raise exception 'MANAGER_REMINDER_NOT_ELIGIBLE' using errcode='55000';
    end if;
    v_token_hash:=decode(v_payload->>'approval_token_hash_hex','hex');
    update public.candidate_approval_requests set
      token_hash=v_token_hash,resend_count=resend_count+1,
      initial_sent_at_utc=null,last_sent_at_utc=null,next_reminder_at_utc=null,
      updated_at_utc=p_now_utc
    where id=v_approval.id returning * into v_approval;
    v_mail_id:=private._candidate_queue_mail_v1(
      coalesce(v_payload->'mail','{}'::jsonb)||jsonb_build_object(
        'payment_scope_json',jsonb_build_object(
          'candidate_mail_authority','MANAGER_APPROVAL_V1',
          'candidate_manager_mail_kind','REMINDER',
          'candidate_manager_workflow_id',v_workflow.id,
          'candidate_manager_workflow_generation',v_workflow.generation,
          'candidate_approval_request_id',v_approval.id,
          'candidate_approval_request_generation',v_approval.request_generation,
          'candidate_manager_mail_retired',false
        )
      ),v_approval.manager_email_normalized,
      'CANDIDATE_MANAGER_REMINDER_V1:'||v_approval.id::text||':'||v_approval.resend_count::text,
      'candidate-manager-reminder:'||v_approval.id::text||':'||v_approval.resend_count::text,
      v_workflow.id,p_now_utc);
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,'state',v_workflow.state,
      'generation',v_workflow.generation,'approval_request_id',v_approval.id,
      'resend_count',v_approval.resend_count,
      'previous_provider_accepted_at_utc',v_manager_provider_accepted_at,
      'reminder_delivery_pending',true,
      'mail_outbox_id',v_mail_id);
    update public.candidate_submission_workflows set
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action='RENEW' then
    if v_workflow.state<>'AWAITING_MANAGER_APPROVAL'
       or v_workflow.review_manifest_sha256 is null then
      raise exception 'MANAGER_APPROVAL_NOT_RENEWABLE' using errcode='55000';
    end if;
    if nullif(v_payload->>'approval_request_id','') is null
       or nullif(v_payload->>'approval_request_generation','') is null then
      raise exception 'CANDIDATE_REQUEST_GENERATION_STALE' using errcode='40001';
    end if;
    select * into v_approval from public.candidate_approval_requests
    where workflow_id=v_workflow.id and method='EMAIL'
      and id=(v_payload->>'approval_request_id')::uuid
      and request_generation=(v_payload->>'approval_request_generation')::integer
    for update;
    if found and v_approval.state='PENDING' and v_approval.expires_at_utc<=p_now_utc then
      update public.candidate_approval_requests set
        state='EXPIRED',updated_at_utc=p_now_utc
      where id=v_approval.id returning * into v_approval;
    end if;
    if not found or v_approval.state<>'EXPIRED'
       or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256 then
      raise exception 'MANAGER_APPROVAL_NOT_RENEWABLE' using errcode='55000';
    end if;
    if coalesce(v_payload->>'approval_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'CANDIDATE_APPROVAL_TOKEN_INVALID' using errcode='22023';
    end if;
    v_token_hash:=decode(v_payload->>'approval_token_hash_hex','hex');
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc where id=v_approval.id;
    insert into public.candidate_approval_requests(
      workflow_id,workflow_generation,request_generation,method,state,manager_email_normalized,
      token_hash,expires_at_utc,initial_sent_at_utc,last_sent_at_utc,next_reminder_at_utc,
      renewal_count,review_manifest_sha256,required_component_ids,required_component_manifest_json,
      manager_review_timesheet_component_id,manager_review_timesheet_sha256,
      idempotency_key,created_at_utc,updated_at_utc
    ) values (
      v_workflow.id,v_workflow.generation,v_approval.request_generation+1,'EMAIL','PENDING',
      v_approval.manager_email_normalized,v_token_hash,p_now_utc+interval '7 days',null,null,
      null,v_approval.renewal_count+1,v_approval.review_manifest_sha256,
      v_approval.required_component_ids,v_approval.required_component_manifest_json,
      v_approval.manager_review_timesheet_component_id,v_approval.manager_review_timesheet_sha256,
      p_idempotency_key,p_now_utc,p_now_utc
    ) returning * into v_approval;
    v_mail_id:=private._candidate_queue_mail_v1(
      coalesce(v_payload->'mail','{}'::jsonb)||jsonb_build_object(
        'payment_scope_json',jsonb_build_object(
          'candidate_mail_authority','MANAGER_APPROVAL_V1',
          'candidate_manager_mail_kind','RENEWAL',
          'candidate_manager_workflow_id',v_workflow.id,
          'candidate_manager_workflow_generation',v_workflow.generation,
          'candidate_approval_request_id',v_approval.id,
          'candidate_approval_request_generation',v_approval.request_generation,
          'candidate_manager_mail_retired',false
        )
      ),v_approval.manager_email_normalized,
      'CANDIDATE_MANAGER_RENEW_V1:'||v_approval.id::text,
      'candidate-manager-renew:'||v_approval.id::text,v_workflow.id,p_now_utc);
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state','AWAITING_MANAGER_APPROVAL','generation',v_workflow.generation,
      'approval_request_id',v_approval.id,'expires_at_utc',v_approval.expires_at_utc,
      'renewal_count',v_approval.renewal_count,'mail_outbox_id',v_mail_id);
    update public.candidate_submission_workflows set
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action='MANAGER_REQUEST_CANCEL' then
    if not v_is_service_action then
      raise exception 'CANDIDATE_MANAGER_REQUEST_CANCEL_SERVICE_REQUIRED' using errcode='42501';
    end if;
    v_cancel_reason:=nullif(btrim(coalesce(v_payload->>'reason_note',v_payload->>'reason','')), '');
    v_cancel_reason_code:=coalesce(
      nullif(upper(btrim(coalesce(v_payload->>'reason_code',''))),''),
      'OFFICE_MANAGER_REQUEST_CANCELLED'
    );
    if v_cancel_reason is null then
      raise exception 'CANDIDATE_CANCELLATION_REASON_REQUIRED' using errcode='22023';
    end if;
    if length(v_cancel_reason)>1000 then
      raise exception 'CANDIDATE_CANCELLATION_REASON_INVALID' using errcode='22023';
    end if;
    v_audit_reason:=v_cancel_reason;
    if v_workflow.state<>'AWAITING_MANAGER_APPROVAL' then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_CANCELLABLE' using errcode='55000';
    end if;
    select request_row.* into v_approval
    from public.candidate_approval_requests request_row
    where request_row.id=(v_payload->>'approval_request_id')::uuid
      and request_row.workflow_id=v_workflow.id
      and request_row.workflow_generation=v_workflow.generation
      and request_row.request_generation=(v_payload->>'approval_request_generation')::integer
      and request_row.method='EMAIL'
      and request_row.state='PENDING'
    for update;
    if not found then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_CANCELLABLE' using errcode='55000';
    end if;
    v_manager_retirement_result:=private._candidate_manager_mail_retire_v1(
      v_workflow.id,v_workflow.generation,array[v_approval.id],
      'APPROVAL_REQUEST_CANCELLED',p_now_utc
    );
    perform public.candidate_manager_email_route_receipt_retire_v1(
      v_workflow.id,v_approval.id,'APPROVAL_REQUEST_CANCELLED',p_now_utc
    );
    update public.candidate_approval_requests set
      state='CANCELLED',cancelled_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where id=v_approval.id;
    update public.candidate_submission_components set
      state='SUPERSEDED',superseded_at_utc=p_now_utc
    where workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation
      and approval_request_id=v_approval.id
      and component_kind='MANAGER_SIGNATURE'
      and state<>'SUPERSEDED';
    if coalesce((v_manager_retirement_result->>'withdrawal_required')::boolean,false) then
      perform private._candidate_queue_mail_v1(
        private._candidate_manager_terminal_mail_payload_v1(
          v_payload->'manager_terminal_mail','WITHDRAWAL'
        )||jsonb_build_object(
          'payment_scope_json',jsonb_build_object(
            'candidate_mail_authority','MANAGER_APPROVAL_V1',
            'candidate_manager_mail_kind','WITHDRAWAL',
            'candidate_manager_workflow_id',v_workflow.id,
            'candidate_manager_workflow_generation',v_workflow.generation,
            'candidate_approval_request_id',v_approval.id,
            'candidate_approval_request_generation',v_approval.request_generation,
            'candidate_manager_template_version',(v_payload->'manager_terminal_mail'->>'manager_template_version')::bigint,
            'candidate_manager_template_sha256',v_payload->'manager_terminal_mail'->>'manager_template_sha256',
            'candidate_manager_submission_type',v_payload->'manager_terminal_mail'->>'manager_submission_type',
            'candidate_manager_mail_retired',false
          )
        ),v_approval.manager_email_normalized,
        'CANDIDATE_MANAGER_WITHDRAWAL_V1:'||v_approval.id::text||':'||v_workflow.generation::text,
        'candidate-manager-withdrawal:'||v_approval.id::text,v_workflow.id,p_now_utc
      );
      v_manager_withdrawal_count:=1;
    end if;
    v_response:=jsonb_build_object(
      'ok',true,'workflow_id',v_workflow.id,
      'state','READY_FOR_MANAGER_APPROVAL','generation',v_workflow.generation,
      'approval_request_id',v_approval.id,'manager_request_cancelled',true,
      'cancellation_reason',v_cancel_reason,
      'cancellation_reason_code',v_cancel_reason_code,
      'manager_withdrawal_count',v_manager_withdrawal_count,
      'manager_mail_retirement',v_manager_retirement_result,
      'claim_cancelled',false
    );
    update public.candidate_submission_workflows set
      state='READY_FOR_MANAGER_APPROVAL',route='ELECTRONIC',
      last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,updated_at_utc=p_now_utc
    where id=v_workflow.id returning * into v_workflow;

  elsif v_action='CANCEL_MANAGER_HANDOFF' then
    if v_workflow.state<>'AWAITING_MANAGER_APPROVAL' or v_workflow.route<>'PHONE' then
      raise exception 'MANAGER_PHONE_HANDOFF_NOT_CANCELLABLE' using errcode='55000';
    end if;
    select * into v_approval from public.candidate_approval_requests
    where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and method='PHONE' and state='PENDING'
      and (not coalesce((v_payload->>'service_office_action')::boolean,false)
        or (id=(v_payload->>'approval_request_id')::uuid
          and request_generation=(v_payload->>'approval_request_generation')::integer))
    order by request_generation desc limit 1 for update;
    if not found then raise exception 'MANAGER_PHONE_HANDOFF_NOT_CANCELLABLE' using errcode='55000'; end if;
    update public.candidate_approval_requests set
      state='CANCELLED',cancelled_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where id=v_approval.id;
    update public.candidate_submission_components set
      state='SUPERSEDED',superseded_at_utc=p_now_utc
    where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and approval_request_id=v_approval.id and component_kind='MANAGER_SIGNATURE'
      and state<>'SUPERSEDED';
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state','READY_FOR_MANAGER_APPROVAL','generation',v_workflow.generation,
      'approval_request_id',v_approval.id,'handoff_cancelled',true);
    update public.candidate_submission_workflows set
      state='READY_FOR_MANAGER_APPROVAL',
      route=case when v_workflow.workflow_kind='DAILY' then 'PHONE' else 'ELECTRONIC' end,
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id returning * into v_workflow;

  elsif v_action='MANAGER_PROVIDER_SUBMIT_PERMIT' then
    if not v_is_service_action then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_SUBMIT_PERMIT_SERVICE_REQUIRED'
        using errcode='42501';
    end if;
    v_provider_lease_token:=nullif(btrim(coalesce(v_payload->>'attempt_lease_token','')),'');
    if v_provider_lease_token is null
       or coalesce(v_payload->>'mail_outbox_id','')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_PERMIT_INVALID' using errcode='22023';
    end if;
    select candidate_mail.* into v_manager_mail
    from public.mail_outbox candidate_mail
    where candidate_mail.id=(v_payload->>'mail_outbox_id')::uuid
      and candidate_mail.type='TIMESHEET_GENERAL'
      and candidate_mail.context_kind='CANDIDATE_WORKFLOW'
      and candidate_mail.context_id=v_workflow.id
      and candidate_mail.status='QUEUED'
      and candidate_mail.sent_at is null
      and candidate_mail.attempt_lease_token=v_provider_lease_token
      and candidate_mail.attempt_lease_expires_at_utc>p_now_utc
      and upper(coalesce(candidate_mail.payment_scope_json->>'candidate_mail_authority',''))
            ='MANAGER_APPROVAL_V1'
      and lower(coalesce(candidate_mail.payment_scope_json->>'candidate_manager_mail_retired','false'))
            in ('false','f','0','no')
    for update;
    if not found then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    v_manager_mail_kind:=upper(coalesce(
      v_manager_mail.payment_scope_json->>'candidate_manager_mail_kind',''
    ));
    if v_manager_mail_kind not in ('INITIAL','REMINDER','RENEWAL','WITHDRAWAL','CANCELLATION')
       or coalesce(v_manager_mail.payment_scope_json->>'candidate_approval_request_id','')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_manager_mail.payment_scope_json->>'candidate_approval_request_generation','')
          !~ '^[1-9][0-9]{0,8}$'
       or coalesce(v_manager_mail.payment_scope_json->>'candidate_manager_workflow_generation','')
          !~ '^[1-9][0-9]{0,8}$' then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    select request_row.* into v_approval
    from public.candidate_approval_requests request_row
    where request_row.id=(v_manager_mail.payment_scope_json->>'candidate_approval_request_id')::uuid
      and request_row.workflow_id=v_workflow.id
      and request_row.workflow_generation=
            (v_manager_mail.payment_scope_json->>'candidate_manager_workflow_generation')::integer
      and request_row.request_generation=
            (v_manager_mail.payment_scope_json->>'candidate_approval_request_generation')::integer
      and request_row.method='EMAIL'
      and request_row.manager_email_normalized=v_manager_mail."to"
    for update;
    if not found then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    if v_manager_mail_kind in ('INITIAL','REMINDER','RENEWAL') then
      select route_receipt.* into v_manager_route_receipt
      from public.candidate_manager_email_route_receipts route_receipt
      where route_receipt.route_receipt_id=v_approval.current_manager_route_receipt_id
        and route_receipt.workflow_id=v_workflow.id
        and route_receipt.approval_request_id=v_approval.id
        and route_receipt.request_generation=v_approval.request_generation
        and route_receipt.manager_token_hash_snapshot=v_approval.token_hash
        and route_receipt.state='CURRENT'
        and route_receipt.route_receipt_id::text=
              v_manager_mail.payment_scope_json->>'candidate_manager_route_receipt_id'
        and route_receipt.manager_route_ticket_id::text=
              v_manager_mail.payment_scope_json->>'candidate_manager_route_ticket_id'
        and route_receipt.route_revision::text=
              v_manager_mail.payment_scope_json->>'candidate_manager_route_revision'
        and pg_catalog.encode(route_receipt.registration_receipt_sha256,'hex')=
              v_manager_mail.payment_scope_json->>'candidate_manager_route_registration_sha256'
      for update;
      if not found then
        raise exception 'CANDIDATE_MANAGER_ROUTE_RECEIPT_NOT_CURRENT' using errcode='40001';
      end if;
    end if;
    if v_manager_mail_kind in ('WITHDRAWAL','CANCELLATION') then
      if v_approval.state not in ('CANCELLED','SUPERSEDED','EXPIRED','REFUSED') then
        raise exception 'CANDIDATE_MANAGER_PROVIDER_MAIL_STALE' using errcode='40001';
      end if;
    elsif v_approval.state<>'PENDING' or v_approval.expires_at_utc<=p_now_utc
       or v_workflow.route<>'EMAIL' or v_workflow.state<>'AWAITING_MANAGER_APPROVAL'
       or v_workflow.generation<>v_approval.workflow_generation
       or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256 then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    v_provider_permit_expires_at:=greatest(
      v_manager_mail.attempt_lease_expires_at_utc,p_now_utc+interval '15 minutes'
    );
    update public.mail_outbox candidate_mail
    set attempt_lease_expires_at_utc=v_provider_permit_expires_at
    where candidate_mail.id=v_manager_mail.id
      and candidate_mail.status='QUEUED' and candidate_mail.sent_at is null
      and candidate_mail.attempt_lease_token=v_provider_lease_token
      and candidate_mail.attempt_lease_expires_at_utc>p_now_utc
      and lower(coalesce(candidate_mail.payment_scope_json->>'candidate_manager_mail_retired','false'))
            in ('false','f','0','no');
    if not found then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    v_response:=jsonb_build_object(
      'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
      'approval_request_id',v_approval.id,'mail_outbox_id',v_manager_mail.id,
      'approval_request_generation',v_approval.request_generation,
      'approval_workflow_generation',v_approval.workflow_generation,
      'manager_mail_kind',v_manager_mail_kind,'provider_submit_permit',true,
      'provider_submit_permit_expires_at_utc',v_provider_permit_expires_at
    );

  elsif v_action='PAPER_PROVIDER_SUBMIT_PERMIT' then
    if not v_is_service_action or p_session_id is not null then
      raise exception 'CANDIDATE_PAPER_PROVIDER_SUBMIT_PERMIT_SERVICE_REQUIRED'
        using errcode='28000';
    end if;
    v_paper_mail_id:=nullif(btrim(coalesce(v_payload->>'mail_outbox_id','')),'')::uuid;
    v_provider_lease_token:=nullif(btrim(coalesce(v_payload->>'attempt_lease_token','')),'');
    v_paper_manifest_sha256:=lower(btrim(coalesce(
      v_payload->>'paper_return_manifest_sha256',''
    )));
    if v_paper_mail_id is null or v_provider_lease_token is null
       or v_paper_manifest_sha256 !~ '^[0-9a-f]{64}$' then
      raise exception 'CANDIDATE_PAPER_PROVIDER_BINDING_INVALID' using errcode='22023';
    end if;
    if v_workflow.route<>'PAPER'
       or v_workflow.state<>'AWAITING_PAPER_RETURN'
       or v_workflow.paper_return_manifest_sha256 is null
       or encode(v_workflow.paper_return_manifest_sha256,'hex')<>v_paper_manifest_sha256
       or jsonb_typeof(v_workflow.paper_return_manifest_json->'pages') is distinct from 'array'
       or exists(
         select 1
         from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') manifest_page
         where upper(coalesce(manifest_page->>'component_kind',''))='EXPENSE_SUMMARY'
            or upper(coalesce(manifest_page->>'page_key',''))='EXPENSE_SUMMARY'
       ) then
      raise exception 'CANDIDATE_PAPER_PROVIDER_WORKFLOW_STALE' using errcode='40001';
    end if;
    if exists(
      select 1 from public.candidate_pending_expense_updates update_row
      where update_row.workflow_id=v_workflow.id
        and update_row.state in ('EDITING','RENDERING')
    ) then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_IN_PROGRESS' using errcode='40001';
    end if;
    select candidate_mail.* into v_paper_mail
    from public.mail_outbox candidate_mail
    where candidate_mail.id=v_paper_mail_id
      and candidate_mail.type='TIMESHEET_QR'
      and candidate_mail.status='QUEUED'
      and candidate_mail.sent_at is null
      and candidate_mail.context_kind='timesheets'
      and candidate_mail.context_id=coalesce(
        v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id
      )
      and candidate_mail.attempt_lease_token=v_provider_lease_token
      and candidate_mail.attempt_lease_expires_at_utc>p_now_utc
      and candidate_mail.payment_scope_json->>'candidate_mail_authority' in (
        'CANDIDATE_PAPER_V1','CANDIDATE_PAPER_PACK_EMAIL_V1'
      )
      and candidate_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =v_paper_manifest_sha256
      and lower(coalesce(candidate_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
            in ('false','f','0','no')
      and lower(coalesce(candidate_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
            in ('true','t','1','yes')
      and lower(coalesce(candidate_mail.payment_scope_json->>'mail_held_until_pdf_rendered','false'))
            in ('false','f','0','no')
      and jsonb_typeof(candidate_mail.attachments)='array'
      and jsonb_array_length(candidate_mail.attachments)>0
      and candidate_mail.attachments->0->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_mail.attachments->0->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_mail.attachments->0->>'paper_return_manifest_sha256',''))
            =v_paper_manifest_sha256
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    v_provider_permit_expires_at:=greatest(
      v_paper_mail.attempt_lease_expires_at_utc,
      p_now_utc+interval '15 minutes'
    );
    update public.mail_outbox candidate_mail
    set attempt_lease_expires_at_utc=v_provider_permit_expires_at
    where candidate_mail.id=v_paper_mail.id
      and candidate_mail.status='QUEUED'
      and candidate_mail.sent_at is null
      and candidate_mail.attempt_lease_token=v_provider_lease_token
      and candidate_mail.attempt_lease_expires_at_utc>p_now_utc
      and lower(coalesce(candidate_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
            in ('false','f','0','no');
    if not found then
      raise exception 'CANDIDATE_PAPER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    v_response:=jsonb_build_object(
      'ok',true,'workflow_id',v_workflow.id,'state',v_workflow.state,
      'generation',v_workflow.generation,'mail_outbox_id',v_paper_mail.id,
      'provider_submit_permit',true,
      'provider_submit_permit_expires_at_utc',v_provider_permit_expires_at
    );

  elsif v_action in ('CANCEL','SUPERSEDE') then
    if v_workflow.state in ('CANCELLED','REJECTED','SUPERSEDED')
       or (v_action='SUPERSEDE' and v_workflow.state='FINALISED') then
      raise exception 'CANDIDATE_WORKFLOW_NOT_CANCELLABLE' using errcode='55000';
    end if;
    if v_action='CANCEL' then
      v_cancel_reason:=nullif(btrim(coalesce(v_payload->>'reason_note',v_payload->>'reason','')), '');
      v_cancel_reason_code:=nullif(upper(btrim(coalesce(v_payload->>'reason_code',''))),'');
      if v_cancel_reason is null then
        raise exception 'CANDIDATE_CANCELLATION_REASON_REQUIRED' using errcode='22023';
      end if;
      if length(v_cancel_reason)>1000 then
        raise exception 'CANDIDATE_CANCELLATION_REASON_INVALID' using errcode='22023';
      end if;
      v_audit_reason:=v_cancel_reason;
    end if;
    if v_workflow.route='PAPER'
       and v_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED') then
      v_paper_retirement_result:=private._candidate_paper_delivery_retire_set_v1(
        array[v_workflow.id],array[v_workflow.generation],
        case when v_action='CANCEL' then 'WORKFLOW_CANCELLED' else 'WORKFLOW_SUPERSEDED' end,
        p_now_utc
      );
      if not coalesce((v_paper_retirement_result->>'retired')::boolean,false)
         or not coalesce(
           (v_paper_retirement_result->>'qr_invalidation_proven')::boolean,false
         ) then
        raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN'
          using errcode='40001',detail=v_paper_retirement_result::text;
      end if;
    end if;
    select coalesce(array_agg(locked_request.id order by locked_request.id),array[]::uuid[])
    into v_manager_request_ids
    from (
      select request_row.id
      from public.candidate_approval_requests request_row
      where request_row.workflow_id=v_workflow.id
        and request_row.workflow_generation=v_workflow.generation
        and request_row.method='EMAIL'
        and request_row.state in ('PENDING','APPROVED')
      order by request_row.id
      for update
    ) locked_request;
    if cardinality(v_manager_request_ids)>0 then
      v_manager_retirement_result:=private._candidate_manager_mail_retire_v1(
        v_workflow.id,v_workflow.generation,v_manager_request_ids,
        case when v_action='CANCEL' then 'WORKFLOW_CANCELLED' else 'WORKFLOW_SUPERSEDED' end,
        p_now_utc
      );
      foreach v_manager_withdrawal_request_id in array v_manager_request_ids loop
        perform public.candidate_manager_email_route_receipt_retire_v1(
          v_workflow.id,v_manager_withdrawal_request_id,
          case when v_action='CANCEL' then 'WORKFLOW_CANCELLED' else 'WORKFLOW_SUPERSEDED' end,
          p_now_utc
        );
      end loop;
    end if;
    update public.candidate_approval_requests set
      state=case when v_action='CANCEL' then 'CANCELLED' else 'SUPERSEDED' end,
      cancelled_at_utc=case when v_action='CANCEL' then p_now_utc else cancelled_at_utc end,
      superseded_at_utc=case when v_action='SUPERSEDE' then p_now_utc else superseded_at_utc end,
      updated_at_utc=p_now_utc where workflow_id=v_workflow.id and state in ('PENDING','APPROVED');
    if v_action='CANCEL' and coalesce((v_manager_retirement_result->>'withdrawal_required')::boolean,false) then
      for v_manager_withdrawal_request_id in
        select value::uuid
        from jsonb_array_elements_text(coalesce(
          v_manager_retirement_result->'withdrawal_request_ids','[]'::jsonb
        )) value
      loop
        select request_row.* into v_approval
        from public.candidate_approval_requests request_row
        where request_row.id=v_manager_withdrawal_request_id
          and request_row.workflow_id=v_workflow.id
          and request_row.workflow_generation=v_workflow.generation
          and request_row.method='EMAIL'
          and request_row.state='CANCELLED';
        if found then
          perform private._candidate_queue_mail_v1(
            private._candidate_manager_terminal_mail_payload_v1(
              v_payload->'manager_terminal_mail','CANCELLATION'
            )||jsonb_build_object(
              'payment_scope_json',jsonb_build_object(
                'candidate_mail_authority','MANAGER_APPROVAL_V1',
                'candidate_manager_mail_kind','CANCELLATION',
                'candidate_manager_workflow_id',v_workflow.id,
                'candidate_manager_workflow_generation',v_workflow.generation,
                'candidate_approval_request_id',v_approval.id,
                'candidate_approval_request_generation',v_approval.request_generation,
                'candidate_manager_template_version',(v_payload->'manager_terminal_mail'->>'manager_template_version')::bigint,
                'candidate_manager_template_sha256',v_payload->'manager_terminal_mail'->>'manager_template_sha256',
                'candidate_manager_submission_type',v_payload->'manager_terminal_mail'->>'manager_submission_type',
                'candidate_manager_mail_retired',false
              )
            ),v_approval.manager_email_normalized,
            'CANDIDATE_MANAGER_CANCELLATION_V1:'||v_approval.id::text||':'||v_workflow.generation::text,
            'candidate-manager-cancellation:'||v_approval.id::text,v_workflow.id,p_now_utc
          );
          v_manager_withdrawal_count:=v_manager_withdrawal_count+1;
        end if;
      end loop;
    end if;
    update public.candidate_submission_components set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,
      review_render_state=case when review_render_state='NOT_REQUIRED' then review_render_state else 'SUPERSEDED' end,
      final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED' then final_signed_render_state else 'SUPERSEDED' end
    where workflow_id=v_workflow.id and state not in ('SUPERSEDED','REJECTED');
    if v_action='CANCEL'
       and v_workflow.scope='WEEKLY'
       and v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED') then
      v_submission_withdrawal_reset:=private._candidate_weekly_withdrawal_reset_v1(
        v_workflow.id,v_cancel_reason,p_now_utc
      );
    elsif v_action='CANCEL'
       and v_workflow.scope='DAILY'
       and v_workflow.workflow_kind='DAILY' then
      v_submission_withdrawal_reset:=private._candidate_daily_receipt_reset_v1(
        v_environment,v_workflow.candidate_id,
        coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id),
        coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id),
        v_cancel_reason,v_workflow.candidate_id,'CANDIDATE_WITHDRAWN',p_now_utc
      );
    end if;
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state',v_action||case when v_action='CANCEL' then 'LED' else 'D' end,
      'generation',v_next_generation,
      'cancellation_reason',case when v_action='CANCEL' then v_cancel_reason else null end,
      'cancellation_reason_code',case when v_action='CANCEL' then v_cancel_reason_code else null end,
      'submission_withdrawal_reset',v_submission_withdrawal_reset,
      'manager_withdrawal_count',v_manager_withdrawal_count,
      'manager_mail_retirement',v_manager_retirement_result);
    update public.candidate_submission_workflows set
      state=case when v_action='CANCEL' then 'CANCELLED' else 'SUPERSEDED' end,
      generation=v_next_generation,cancelled_at_utc=case when v_action='CANCEL' then p_now_utc else cancelled_at_utc end,
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action='PAPER_PREPARE' then
    if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
      raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023';
    end if;
    if v_workflow.state not in ('WORKER_SUBMITTED','AWAITING_PAPER_RETURN') then
      raise exception 'CANDIDATE_WORKFLOW_TRANSITION_INVALID' using errcode='55000';
    end if;
    if v_workflow.scope='DAILY' then
      raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000';
    end if;
    if v_workflow.workflow_kind='CONTRACT_EXPENSE'
       and v_workflow.target_timesheet_id is null then
      select week_row.* into v_anchor_week
      from public.contract_weeks week_row
      where week_row.timesheet_id=v_workflow.anchor_timesheet_id
        and week_row.contract_id=v_workflow.contract_id
        and week_row.week_ending_date=v_workflow.week_ending_date;
      if not found then raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001'; end if;
      if not coalesce((v_policy->>'paper_submission_enabled')::boolean,false) then
        raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000';
      end if;
    else
      v_route_authority:=private._candidate_route_family_v1(v_workflow.target_timesheet_id,v_workflow.contract_week_id);
      if not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false) then
        raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000',detail=v_route_authority::text;
      end if;
    end if;
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and state='PENDING';

    select coalesce(jsonb_agg(jsonb_build_object(
      'page_key',case source_component.component_kind
        when 'MILEAGE_FORM' then 'MILEAGE_FORM:' else 'EXPENSE_EVIDENCE:' end
        ||coalesce(source_component.source_component_id,source_component.id)::text,
      'component_kind',source_component.component_kind,
      'expense_category',source_component.expense_category,
      'source_component_id',coalesce(source_component.source_component_id,source_component.id),
      'source_content_sha256',encode(source_component.source_content_sha256,'hex')
    ) order by source_component.component_no,source_component.id),'[]'::jsonb)
    into v_paper_source_pages
    from (
      select distinct on (
        component.component_kind,component.expense_category,component.document_role,
        coalesce(component.source_component_id,component.id),component.source_content_sha256
      ) component.*
      from public.candidate_submission_components component
      where component.workflow_id=v_workflow.id
        and component.workflow_generation=v_workflow.generation
        and component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
        and component.state='IMMUTABLE'
        and component.source_content_sha256 is not null
      order by component.component_kind,component.expense_category,component.document_role,
        coalesce(component.source_component_id,component.id),component.source_content_sha256,
        component.component_no,component.id
    ) source_component;

    v_paper_mileage_only:=jsonb_array_length(v_paper_source_pages)>0
      and not exists(
        select 1
        from jsonb_array_elements(v_paper_source_pages) source_page
        where source_page->>'component_kind'<>'MILEAGE_FORM'
           or source_page->>'expense_category'<>'MILEAGE'
      );

    v_paper_manifest:=jsonb_build_object(
      'workflow_id',v_workflow.id,
      'workflow_generation',v_workflow.generation,
      'immutable_submission_sha256',encode(v_workflow.immutable_submission_sha256,'hex'),
      'pages',
        case when v_workflow.workflow_kind<>'CONTRACT_EXPENSE'
          then jsonb_build_array(jsonb_build_object(
            'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))
          else '[]'::jsonb end
        -- The expense summary is an internal, automatically regenerated aid;
        -- it is never a manager-decision or signed-return page.
        || '[]'::jsonb
        || v_paper_source_pages
    );
    update public.candidate_submission_workflows set
      state='AWAITING_PAPER_RETURN',route='PAPER',policy_snapshot_json=v_policy,
      policy_snapshot_sha256=private._candidate_sha256_jsonb_v1(v_policy),
      paper_return_manifest_json=v_paper_manifest,
      paper_return_manifest_sha256=private._candidate_sha256_jsonb_v1(v_paper_manifest),
      updated_at_utc=p_now_utc where id=v_workflow.id returning * into v_workflow;

    -- The existing QR/document/email authority is composed inside this
    -- transaction. A PAPER workflow is not accepted unless its exact held
    -- email operation exists and is bound to this frozen manifest.
    if v_workflow.workflow_kind='CONTRACT_EXPENSE'
       and v_workflow.target_timesheet_id is null then
      execute
        'select public.candidate_targetless_expense_paper_pack_enqueue_v1($1,$2,$3,$4,$5)'
        into v_paper_pack_result
        using v_environment,v_workflow.id,v_workflow.generation,
          p_idempotency_key||':paper-pack',p_now_utc;
    else
      execute
        'select public.timesheet_qr_send_enqueue_v1($1,$2,$3,$4,$5)'
        into v_paper_pack_result
        using v_paper_timesheet_id,v_paper_timesheet_id,null::uuid,
          p_idempotency_key||':paper-pack',p_now_utc;
    end if;

    if not coalesce((v_paper_pack_result->>'ok')::boolean,false)
       or not coalesce((v_paper_pack_result->>'queued')::boolean,false)
       or not coalesce((v_paper_pack_result->>'recipient_available')::boolean,false) then
      if coalesce(v_paper_pack_result->>'error_code','') in (
        'CANDIDATE_EMAIL_MISSING','CANDIDATE_EMAIL_OPTED_OUT','CANDIDATE_NOT_FOUND'
      ) then
        raise exception 'CANDIDATE_PAPER_EMAIL_NOT_AVAILABLE'
          using errcode='55000',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_EMAIL_NOT_AVAILABLE',
            'cause',v_paper_pack_result->>'error_code'
          )::text;
      end if;
      raise exception 'CANDIDATE_PAPER_PACK_QUEUE_FAILED'
        using errcode='55000',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_PACK_QUEUE_FAILED',
          'cause',coalesce(v_paper_pack_result->>'error_code','UNKNOWN')
        )::text;
    end if;

    v_mail_id:=nullif(v_paper_pack_result->>'mail_outbox_id','')::uuid;
    if v_mail_id is null then
      raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='55000';
    end if;
    perform 1
    from public.mail_outbox candidate_paper_mail
    where candidate_paper_mail.id=v_mail_id
      and candidate_paper_mail.type='TIMESHEET_QR'
      and candidate_paper_mail.context_kind='timesheets'
      and candidate_paper_mail.context_id=v_paper_timesheet_id
      and candidate_paper_mail.status='QUEUED'
      and candidate_paper_mail.attempt_lease_token is null
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =encode(v_workflow.paper_return_manifest_sha256,'hex')
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
            in ('false','f','0','no')
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'mail_held_until_pdf_rendered','false'))
            in ('true','t','1','yes')
      and candidate_paper_mail.payment_scope_json->>'mail_hold_reason'='CANDIDATE_PAPER_PACK_PENDING'
      and jsonb_typeof(candidate_paper_mail.attachments)='array'
      and jsonb_array_length(candidate_paper_mail.attachments)=0
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='55000';
    end if;

    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state','AWAITING_PAPER_RETURN','generation',v_workflow.generation,
      'paper_return_manifest_sha256',encode(v_workflow.paper_return_manifest_sha256,'hex'),
      'paper_return_page_count',jsonb_array_length(v_paper_manifest->'pages'),
      'paper_pack',jsonb_build_object(
        'queued',true,
        'recipient_available',true,
        'mail_outbox_id',v_mail_id,
        'send_state',v_paper_pack_result->>'send_state',
        'document_operation_id',v_paper_pack_result->>'document_operation_id',
        'document_version_id',v_paper_pack_result->>'document_version_id',
        'document_version_status',v_paper_pack_result->>'document_version_status',
        'current_timesheet_id',v_paper_pack_result->>'current_timesheet_id',
        'current_version',v_paper_pack_result->'current_version'
      ));
    update public.candidate_submission_workflows set
      last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc
    where id=v_workflow.id returning * into v_workflow;

  elsif v_action='PAPER_PACK_ATTEMPT_CLAIM' then
    if not v_is_service_action or p_session_id is not null
       or not coalesce((v_payload->>'service_paper_pack_attempt')::boolean,false) then
      raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_SERVICE_REQUIRED' using errcode='28000';
    end if;
    if v_workflow.state<>'AWAITING_PAPER_RETURN' or v_workflow.route<>'PAPER'
       or v_workflow.paper_return_manifest_sha256 is null then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;
    v_paper_mail_id:=nullif(btrim(coalesce(v_payload->>'mail_outbox_id','')),'')::uuid;
    v_paper_manifest_sha256:=lower(btrim(coalesce(v_payload->>'paper_return_manifest_sha256','')));
    v_paper_pack_attempt_token:=lower(btrim(coalesce(v_payload->>'paper_pack_attempt_token','')));
    v_paper_pack_operation_id:=nullif(btrim(coalesce(
      v_payload->>'paper_pack_operation_id',p_idempotency_key,''
    )), '');
    if v_paper_mail_id is null
       or v_paper_manifest_sha256 !~ '^[0-9a-f]{64}$'
       or v_paper_manifest_sha256<>encode(v_workflow.paper_return_manifest_sha256,'hex')
       or v_paper_pack_attempt_token !~ '^[0-9a-f]{64}$'
       or v_paper_pack_operation_id is null
       or length(v_paper_pack_operation_id)>200 then
      raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_RECEIPT_INVALID' using errcode='22023';
    end if;
    select * into v_paper_mail
    from public.mail_outbox candidate_paper_mail
    where candidate_paper_mail.id=v_paper_mail_id
      and candidate_paper_mail.type='TIMESHEET_QR'
      and candidate_paper_mail.context_kind='timesheets'
      and candidate_paper_mail.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1'
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =v_paper_manifest_sha256
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='40001';
    end if;
    if lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
         in ('true','t','1','yes') then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;
    if lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
         in ('true','t','1','yes') then
      v_response:=jsonb_build_object(
        'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
        'mail_outbox_id',v_paper_mail.id,'paper_pack_attempt_state','READY',
        'paper_pack_operation_id',v_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id',
        'claim_acquired_new',false,
        'paper_pack_attempt_count',coalesce(
          nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_count','')::integer,0
        )
      );
    else
      v_paper_failure_class:=upper(coalesce(
        v_paper_mail.payment_scope_json->>'candidate_paper_pack_failure_class',''
      ));
      if v_paper_failure_class='TERMINAL' then
        raise exception 'CANDIDATE_PAPER_PACK_FAILED_TERMINAL' using errcode='55000';
      end if;
      v_paper_pack_next_retry_at:=nullif(
        v_paper_mail.payment_scope_json->>'candidate_paper_pack_next_retry_at_utc',''
      )::timestamptz;
      if v_paper_failure_class='RETRYABLE'
         and v_paper_pack_next_retry_at is not null and v_paper_pack_next_retry_at>p_now_utc then
        raise exception 'CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE'
          using errcode='55000',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE',
            'next_retry_at_utc',v_paper_pack_next_retry_at
          )::text;
      end if;
      if nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_token','') is not null
         and nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_expires_at_utc','')::timestamptz
               >p_now_utc then
        raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_IN_PROGRESS' using errcode='40001';
      end if;
      v_paper_pack_attempt_count:=coalesce(
        nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_count','')::integer,0
      )+1;
      v_paper_pack_attempt_expires_at:=p_now_utc+interval '10 minutes';
      update public.mail_outbox candidate_paper_mail
      set payment_scope_json=candidate_paper_mail.payment_scope_json||jsonb_build_object(
        'candidate_paper_pack_attempt_token',v_paper_pack_attempt_token,
        'candidate_paper_pack_attempt_expires_at_utc',v_paper_pack_attempt_expires_at,
        'candidate_paper_pack_attempt_count',v_paper_pack_attempt_count,
        'candidate_paper_pack_last_attempted_at_utc',p_now_utc,
        'candidate_paper_pack_operation_id',v_paper_pack_operation_id,
        'candidate_paper_pack_operation_state','CLAIMED'
      )
      where candidate_paper_mail.id=v_paper_mail.id;
      v_response:=jsonb_build_object(
        'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
        'mail_outbox_id',v_paper_mail.id,'paper_pack_attempt_state','CLAIMED',
        'paper_pack_operation_id',v_paper_pack_operation_id,
        'claim_acquired_new',true,
        'paper_pack_attempt_token',v_paper_pack_attempt_token,
        'paper_pack_attempt_count',v_paper_pack_attempt_count,
        'paper_pack_attempt_expires_at_utc',v_paper_pack_attempt_expires_at
      );
    end if;

  elsif v_action='PAPER_PACK_MARK_FAILURE' then
    if not v_is_service_action or p_session_id is not null
       or not coalesce((v_payload->>'service_paper_pack_failure')::boolean,false) then
      raise exception 'CANDIDATE_PAPER_PACK_FAILURE_SERVICE_REQUIRED' using errcode='28000';
    end if;
    if v_workflow.state<>'AWAITING_PAPER_RETURN' or v_workflow.route<>'PAPER'
       or v_workflow.paper_return_manifest_sha256 is null then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;
    v_paper_mail_id:=nullif(btrim(coalesce(v_payload->>'mail_outbox_id','')),'')::uuid;
    v_paper_manifest_sha256:=lower(btrim(coalesce(v_payload->>'paper_return_manifest_sha256','')));
    v_paper_failure_code:=upper(btrim(coalesce(v_payload->>'error_code','')));
    v_paper_pack_operation_id:=nullif(btrim(coalesce(
      v_payload->>'paper_pack_operation_id',''
    )), '');
    if v_paper_manifest_sha256 !~ '^[0-9a-f]{64}$'
       or v_paper_manifest_sha256<>encode(v_workflow.paper_return_manifest_sha256,'hex')
       or v_paper_failure_code='' then
      raise exception 'CANDIDATE_PAPER_PACK_FAILURE_RECEIPT_INVALID' using errcode='22023';
    end if;
    if v_paper_failure_code not in (
      'CANDIDATE_PAPER_PACK_ASSEMBLY_TRANSIENT',
      'CANDIDATE_PAPER_SOURCE_READ_TRANSIENT',
      'CANDIDATE_PAPER_R2_WRITE_TRANSIENT',
      'CANDIDATE_PAPER_DOCUMENT_FAILED',
      'CANDIDATE_PAPER_OUTBOX_NOT_READY',
      'CANDIDATE_PAPER_RETURN_MANIFEST_STALE',
      'CANDIDATE_PAPER_PACK_MEDIA_TYPE_INVALID',
      'CANDIDATE_PAPER_PACK_COMPONENT_MISSING',
      'CANDIDATE_PAPER_PACK_INCOMPLETE',
      'CANDIDATE_PAPER_PACK_IDENTITY_INVALID',
      'CANDIDATE_PAPER_PACK_IDENTITY_CONFLICT',
      'CANDIDATE_PAPER_OUTBOX_CONFLICT',
      'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
    ) then
      v_paper_failure_code:='CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED';
    end if;
    v_paper_failure_class:=case
      when v_paper_failure_code in (
        'CANDIDATE_PAPER_PACK_ASSEMBLY_TRANSIENT',
        'CANDIDATE_PAPER_SOURCE_READ_TRANSIENT',
        'CANDIDATE_PAPER_R2_WRITE_TRANSIENT'
      ) then 'RETRYABLE'
      when v_paper_failure_code in (
        'CANDIDATE_PAPER_RETURN_MANIFEST_STALE',
        'CANDIDATE_PAPER_DOCUMENT_FAILED',
        'CANDIDATE_PAPER_OUTBOX_NOT_READY',
        'CANDIDATE_PAPER_PACK_MEDIA_TYPE_INVALID',
        'CANDIDATE_PAPER_PACK_COMPONENT_MISSING',
        'CANDIDATE_PAPER_PACK_INCOMPLETE',
        'CANDIDATE_PAPER_PACK_IDENTITY_INVALID',
        'CANDIDATE_PAPER_PACK_IDENTITY_CONFLICT',
        'CANDIDATE_PAPER_OUTBOX_CONFLICT',
        'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
      ) then 'TERMINAL'
      else 'TERMINAL' end;
    v_paper_failure_retryable:=v_paper_failure_class='RETRYABLE';
    v_paper_pack_attempt_token:=lower(btrim(coalesce(v_payload->>'paper_pack_attempt_token','')));
    if v_paper_mail_id is null then
      v_response:=jsonb_build_object(
        'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
        'mail_outbox_id',null,'paper_pack_state','FAILED_TERMINAL',
        'failure_scope','WORKFLOW','failure_class','TERMINAL',
        'failure_code',v_paper_failure_code,'retryable',false,
        'paper_pack_operation_id',v_paper_pack_operation_id,
        'failure_contract_version','CANDIDATE_PAPER_PACK_FAILURE_V2'
      );
    else
    select * into v_paper_mail
    from public.mail_outbox candidate_paper_mail
    where candidate_paper_mail.id=v_paper_mail_id
      and candidate_paper_mail.type='TIMESHEET_QR'
      and candidate_paper_mail.context_kind='timesheets'
      and candidate_paper_mail.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1'
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =v_paper_manifest_sha256
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='40001';
    end if;
    if nullif(btrim(coalesce(v_paper_mail.attempt_lease_token,'')),'') is not null
       and v_paper_mail.attempt_lease_expires_at_utc>p_now_utc then
      raise exception 'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS' using errcode='40001';
    end if;
    if lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
         in ('true','t','1','yes') then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;
    if nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_token','') is not null
       and lower(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_token')
             is distinct from nullif(v_paper_pack_attempt_token,'') then
      raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_STALE' using errcode='40001';
    end if;
    if v_paper_pack_operation_id is not null
       and nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id','')
             is distinct from v_paper_pack_operation_id then
      raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_STALE' using errcode='40001';
    end if;
    v_paper_pack_attempt_count:=coalesce(
      nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_count','')::integer,0
    );
    v_paper_pack_next_retry_at:=case when v_paper_failure_retryable then p_now_utc+case
      when v_paper_pack_attempt_count<=1 then interval '1 minute'
      when v_paper_pack_attempt_count=2 then interval '5 minutes'
      when v_paper_pack_attempt_count=3 then interval '15 minutes'
      else interval '30 minutes' end else null end;
    update public.mail_outbox candidate_paper_mail
    set payment_scope_json=candidate_paper_mail.payment_scope_json||jsonb_build_object(
      'candidate_paper_pack_ready',false,
      'candidate_paper_pack_retryable',v_paper_failure_retryable,
      'candidate_paper_pack_failure_class',v_paper_failure_class,
      'candidate_paper_pack_failure_code',v_paper_failure_code,
      'candidate_paper_pack_failure_contract_version','CANDIDATE_PAPER_PACK_FAILURE_V2',
      'candidate_paper_pack_failed_at_utc',p_now_utc,
      'candidate_paper_pack_next_retry_at_utc',v_paper_pack_next_retry_at,
      'candidate_paper_pack_attempt_token',null,
      'candidate_paper_pack_attempt_expires_at_utc',null,
      'candidate_paper_pack_operation_id',coalesce(
        v_paper_pack_operation_id,
        candidate_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id'
      ),
      'candidate_paper_pack_operation_state',case when v_paper_failure_retryable
        then 'FAILED_RETRYABLE' else 'FAILED_TERMINAL' end
    )
    where candidate_paper_mail.id=v_paper_mail.id;
    v_response:=jsonb_build_object(
      'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
      'mail_outbox_id',v_paper_mail.id,'paper_pack_state',
        case when v_paper_failure_retryable then 'FAILED_RETRYABLE' else 'FAILED_TERMINAL' end,
      'failure_class',v_paper_failure_class,'failure_code',v_paper_failure_code,
      'paper_pack_operation_id',coalesce(
        v_paper_pack_operation_id,
        v_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id'
      ),
      'retryable',v_paper_failure_retryable,
      'failure_contract_version','CANDIDATE_PAPER_PACK_FAILURE_V2',
      'paper_pack_attempt_count',v_paper_pack_attempt_count,
      'next_retry_at_utc',v_paper_pack_next_retry_at
    );
    end if;
    update public.candidate_submission_workflows
    set last_mutation_idempotency_key=p_idempotency_key,
        last_mutation_response_json=v_response,updated_at_utc=p_now_utc
    where id=v_workflow.id and generation=v_workflow.generation;

  elsif v_action='PAPER_PACK_RELEASE' then
    if not v_is_service_action or p_session_id is not null then
      raise exception 'CANDIDATE_PAPER_PACK_RELEASE_SERVICE_REQUIRED' using errcode='28000';
    end if;
    if v_workflow.state<>'AWAITING_PAPER_RETURN' or v_workflow.route<>'PAPER' then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;
    if v_workflow.paper_return_manifest_sha256 is null
       or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
          is distinct from v_workflow.paper_return_manifest_sha256
       or jsonb_typeof(v_workflow.paper_return_manifest_json->'pages') is distinct from 'array'
       or exists(
         select 1
         from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') manifest_page
         where upper(coalesce(manifest_page->>'component_kind',''))='EXPENSE_SUMMARY'
            or upper(coalesce(manifest_page->>'page_key',''))='EXPENSE_SUMMARY'
       ) then
      raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='55000';
    end if;
    -- RELEASE is a separate transaction from ATTEMPT_CLAIM. Recompute the
    -- update hold while the workflow row is locked; rebind/abort take the
    -- same lock, so this truth cannot change before the outbox update.
    select exists(
      select 1 from public.candidate_pending_expense_updates update_row
      where update_row.workflow_id=v_workflow.id
        and update_row.update_mode='PAPER_REPLACEMENT'
        and update_row.current_workflow_generation=v_workflow.generation
        and update_row.state in ('EDITING','RENDERING')
    ) into v_paper_expense_update_active;

    v_paper_timesheet_id:=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id);
    v_paper_mail_id:=nullif(btrim(coalesce(v_payload->>'mail_outbox_id','')),'')::uuid;
    v_paper_manifest_sha256:=lower(btrim(coalesce(v_payload->>'paper_return_manifest_sha256','')));
    v_paper_pack_storage_key:=nullif(btrim(coalesce(v_payload->>'complete_pack_storage_key','')),'');
    v_paper_pack_sha256:=lower(btrim(coalesce(v_payload->>'complete_pack_sha256','')));
    v_paper_pack_media_type:=lower(btrim(coalesce(v_payload->>'complete_pack_media_type','')));
    v_paper_base_document_sha256:=lower(btrim(coalesce(v_payload->>'base_document_sha256','')));
    v_paper_branding_contract_sha256:=lower(btrim(coalesce(v_payload->>'branding_contract_sha256','')));
    v_paper_renderer_contract_version:=btrim(coalesce(v_payload->>'renderer_contract_version',''));
    v_paper_pack_attempt_token:=lower(btrim(coalesce(v_payload->>'paper_pack_attempt_token','')));
    v_paper_pack_operation_id:=nullif(btrim(coalesce(
      v_payload->>'paper_pack_operation_id',''
    )), '');
    begin
      v_paper_pack_byte_size:=(v_payload->>'complete_pack_byte_size')::bigint;
      v_paper_pack_page_count:=(v_payload->>'complete_pack_page_count')::integer;
    exception when others then
      raise exception 'CANDIDATE_PAPER_PACK_RECEIPT_INVALID' using errcode='22023';
    end;
    if v_paper_mail_id is null
       or v_paper_manifest_sha256 !~ '^[0-9a-f]{64}$'
       or v_paper_pack_sha256 !~ '^[0-9a-f]{64}$'
       or v_paper_base_document_sha256 !~ '^[0-9a-f]{64}$'
       or v_paper_branding_contract_sha256 !~ '^[0-9a-f]{64}$'
       or v_paper_pack_media_type<>'application/pdf'
       or coalesce(v_paper_pack_byte_size,0)<1
       or coalesce(v_paper_pack_page_count,0)<1
       or v_paper_renderer_contract_version=''
       or (v_paper_pack_attempt_token<>'' and v_paper_pack_operation_id is null)
       or v_paper_manifest_sha256<>encode(v_workflow.paper_return_manifest_sha256,'hex')
       or v_paper_renderer_contract_version<>coalesce(
         v_workflow.renderer_contract_version,
         v_workflow.immutable_submission_json#>>'{official_presentation,renderer_contract_version}'
       )
       or v_paper_branding_contract_sha256<>lower(coalesce(
         v_workflow.immutable_submission_json#>>'{official_presentation,branding,branding_contract_sha256}',''
       )) then
      raise exception 'CANDIDATE_PAPER_PACK_RECEIPT_INVALID' using errcode='22023';
    end if;
    v_paper_expected_storage_key:='candidate-app/'||lower(v_environment)||'/'
      ||v_workflow.id::text||'/'||v_workflow.generation::text||'/paper-pack/'
      ||v_paper_manifest_sha256||'-'||v_paper_base_document_sha256||'-'
      ||v_paper_branding_contract_sha256||'-'||v_paper_renderer_contract_version||'.pdf';
    if v_paper_pack_storage_key is distinct from v_paper_expected_storage_key then
      raise exception 'CANDIDATE_PAPER_PACK_IDENTITY_CONFLICT' using errcode='40001';
    end if;
    if jsonb_array_length(coalesce(v_workflow.paper_return_manifest_json->'pages','[]'::jsonb))
         <>v_paper_pack_page_count then
      raise exception 'CANDIDATE_PAPER_PACK_PAGE_COUNT_MISMATCH' using errcode='22023';
    end if;

    select count(*)::integer into v_paper_outbox_count
    from public.mail_outbox candidate_paper_mail
    where candidate_paper_mail.type='TIMESHEET_QR'
      and candidate_paper_mail.context_kind='timesheets'
      and candidate_paper_mail.context_id=v_paper_timesheet_id
      and candidate_paper_mail.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1'
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =v_paper_manifest_sha256;
    if v_paper_outbox_count<>1 then
      raise exception 'CANDIDATE_PAPER_OUTBOX_CONFLICT' using errcode='40001';
    end if;

    select * into v_paper_mail
    from public.mail_outbox candidate_paper_mail
    where candidate_paper_mail.id=v_paper_mail_id
      and candidate_paper_mail.type='TIMESHEET_QR'
      and candidate_paper_mail.context_kind='timesheets'
      and candidate_paper_mail.context_id=v_paper_timesheet_id
      and candidate_paper_mail.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1'
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =v_paper_manifest_sha256
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='40001';
    end if;
    if v_paper_mail.status='FAILED' then
      raise exception 'CANDIDATE_PAPER_OUTBOX_FAILED' using errcode='55000';
    end if;
    if nullif(btrim(coalesce(v_paper_mail.attempt_lease_token,'')),'') is not null then
      raise exception 'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS' using errcode='40001';
    end if;
    if nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_token','') is not null
       and lower(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_token')
             is distinct from nullif(v_paper_pack_attempt_token,'') then
      raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_STALE' using errcode='40001';
    end if;
    if v_paper_pack_operation_id is not null
       and nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id','')
             is distinct from v_paper_pack_operation_id then
      raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_STALE' using errcode='40001';
    end if;
    if lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
         in ('true','t','1','yes') then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;

    v_paper_pack_attachment:=jsonb_build_array(jsonb_build_object(
      'r2_key',v_paper_pack_storage_key,
      'filename',case when v_workflow.workflow_kind='CONTRACT_EXPENSE'
        then 'Expense_' else 'Timesheet_' end
        ||coalesce(v_workflow.week_ending_date::text,v_paper_timesheet_id::text)||'.pdf',
      'content_type','application/pdf','sha256',v_paper_pack_sha256,
      'size_bytes',v_paper_pack_byte_size,'page_count',v_paper_pack_page_count,
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'paper_return_manifest_sha256',v_paper_manifest_sha256
    ));

    if v_paper_mail.status in ('QUEUED','SENT')
       and lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
            in ('true','t','1','yes')
       and v_paper_mail.attachments=v_paper_pack_attachment
       and v_paper_mail.payment_scope_json->>'candidate_complete_pack_storage_key'=v_paper_pack_storage_key
       and lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_complete_pack_sha256',''))=v_paper_pack_sha256
       and v_paper_mail.payment_scope_json->>'candidate_complete_pack_size_bytes'=v_paper_pack_byte_size::text
       and v_paper_mail.payment_scope_json->>'candidate_complete_pack_page_count'=v_paper_pack_page_count::text then
      v_paper_release_idempotent:=true;
    elsif v_paper_mail.status='SENT' then
      raise exception 'CANDIDATE_PAPER_OUTBOX_ALREADY_SENT' using errcode='55000';
    elsif v_paper_mail.status<>'QUEUED'
       or lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
            not in ('false','f','0','no')
       or lower(coalesce(v_paper_mail.payment_scope_json->>'mail_held_until_pdf_rendered','false'))
            not in ('true','t','1','yes')
       or v_paper_mail.payment_scope_json->>'mail_hold_reason'<>'CANDIDATE_PAPER_PACK_PENDING'
       or jsonb_typeof(v_paper_mail.attachments)<>'array'
       or jsonb_array_length(v_paper_mail.attachments)<>0 then
      raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='40001';
    else
      update public.mail_outbox candidate_paper_mail
      set attachments=v_paper_pack_attachment,
          scheduled_for_utc=case when v_paper_expense_update_active
            then 'infinity'::timestamptz else p_now_utc end,
          next_attempt_at_utc=case when v_paper_expense_update_active
            then 'infinity'::timestamptz else p_now_utc end,
          payment_scope_json=candidate_paper_mail.payment_scope_json||jsonb_build_object(
            'candidate_paper_pack_ready',true,
            'candidate_paper_pack_retryable',false,
            'candidate_paper_pack_failure_class',null,
            'candidate_paper_pack_failure_code',null,
            'candidate_paper_pack_failure_contract_version',null,
            'candidate_paper_pack_failed_at_utc',null,
            'candidate_paper_pack_next_retry_at_utc',null,
            'candidate_paper_pack_attempt_token',null,
            'candidate_paper_pack_attempt_expires_at_utc',null,
            'candidate_paper_pack_operation_id',coalesce(
              v_paper_pack_operation_id,
              candidate_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id'
            ),
            'candidate_paper_pack_operation_state','READY',
            'mail_held_until_pdf_rendered',v_paper_expense_update_active,
            'mail_delayed_for_pdf_render',v_paper_expense_update_active,
            'mail_hold_reason',case when v_paper_expense_update_active
              then 'CANDIDATE_EXPENSE_UPDATE_PENDING' end,
            'candidate_expense_update_hold',v_paper_expense_update_active,
            'candidate_complete_pack_storage_key',v_paper_pack_storage_key,
            'candidate_complete_pack_sha256',v_paper_pack_sha256,
            'candidate_complete_pack_size_bytes',v_paper_pack_byte_size,
            'candidate_complete_pack_page_count',v_paper_pack_page_count,
            'candidate_complete_pack_media_type','application/pdf',
            'candidate_complete_pack_ready_at_utc',p_now_utc,
            'candidate_complete_pack_base_document_sha256',v_paper_base_document_sha256,
            'candidate_complete_pack_branding_contract_sha256',v_paper_branding_contract_sha256,
            'candidate_complete_pack_renderer_contract_version',v_paper_renderer_contract_version
          )
      where candidate_paper_mail.id=v_paper_mail.id
        and candidate_paper_mail.status='QUEUED'
        and candidate_paper_mail.attempt_lease_token is null
        and lower(coalesce(candidate_paper_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
              in ('false','f','0','no')
        and lower(coalesce(candidate_paper_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
              in ('false','f','0','no');
      if not found then
        raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='40001';
      end if;
    end if;

    if not v_paper_expense_update_active then
      insert into public.candidate_notifications(
        account_id,candidate_id,workflow_id,timesheet_id,event_type,preference_category,
        template_key,template_params,deep_link_json,state,push_state,dedupe_key,created_at_utc
      ) values (
        v_workflow.account_id,v_workflow.candidate_id,v_workflow.id,v_paper_timesheet_id,
        'PAPER_PACK_READY','resubmission_required','candidate-paper-pack-ready-v1',
        jsonb_build_object('page_count',v_paper_pack_page_count,'workflow_generation',v_workflow.generation),
        jsonb_build_object('type','paper_pack','timesheet_id',v_paper_timesheet_id,
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation),
        'UNREAD','PENDING',
        'CANDIDATE_PAPER_PACK_READY_V1:'||v_workflow.id::text||':'
          ||v_workflow.generation::text||':'||v_paper_manifest_sha256,
        p_now_utc
      ) on conflict(dedupe_key) do nothing
      returning id into v_paper_notification_id;
      if v_paper_notification_id is null then
        select notification.id into v_paper_notification_id
        from public.candidate_notifications notification
        where notification.dedupe_key='CANDIDATE_PAPER_PACK_READY_V1:'||v_workflow.id::text||':'
          ||v_workflow.generation::text||':'||v_paper_manifest_sha256;
      end if;
    end if;

    update public.candidate_submission_workflows
    set updated_at_utc=p_now_utc
    where id=v_workflow.id
      and generation=v_workflow.generation
      and state='AWAITING_PAPER_RETURN'
      and route='PAPER';
    if not found then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;

    v_response:=jsonb_build_object(
      'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
      'state',v_workflow.state,'timesheet_id',v_paper_timesheet_id,
      'mail_outbox_id',v_paper_mail.id,'notification_id',v_paper_notification_id,
      'paper_return_manifest_sha256',v_paper_manifest_sha256,
      'paper_pack_operation_id',coalesce(
        v_paper_pack_operation_id,
        v_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id'
      ),
      'complete_pack_storage_key',v_paper_pack_storage_key,
      'complete_pack_sha256',v_paper_pack_sha256,
      'complete_pack_byte_size',v_paper_pack_byte_size,
      'complete_pack_page_count',v_paper_pack_page_count,
      'paper_pack_held_for_expense_update',v_paper_expense_update_active,
      'idempotent_replay',v_paper_release_idempotent
    );

  elsif v_action='PAPER_RETURN' then
    if v_workflow.state<>'AWAITING_PAPER_RETURN' then
      raise exception 'CANDIDATE_WORKFLOW_TRANSITION_INVALID' using errcode='55000';
    end if;
    if v_workflow.paper_return_manifest_sha256 is null
       or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
          is distinct from v_workflow.paper_return_manifest_sha256 then
      raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='55000';
    end if;
    -- The claimed mail lease is the provider-submit permit. PAPER return and
    -- every authority-changing transition lock the same exact delivery row,
    -- so provider submission cannot be authorised and then invalidated before
    -- its external call. An active permit is a controlled retryable conflict.
    select count(*)::integer into v_paper_outbox_count
    from public.mail_outbox candidate_mail
    where candidate_mail.type='TIMESHEET_QR'
      and candidate_mail.context_kind='timesheets'
      and candidate_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =encode(v_workflow.paper_return_manifest_sha256,'hex');
    if v_paper_outbox_count<>1 then
      raise exception 'CANDIDATE_PAPER_OUTBOX_CONFLICT' using errcode='40001';
    end if;
    select candidate_mail.* into v_paper_mail
    from public.mail_outbox candidate_mail
    where candidate_mail.type='TIMESHEET_QR'
      and candidate_mail.context_kind='timesheets'
      and candidate_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =encode(v_workflow.paper_return_manifest_sha256,'hex')
    for update;
    if nullif(btrim(coalesce(v_paper_mail.attempt_lease_token,'')),'') is not null
       and (v_paper_mail.attempt_lease_expires_at_utc is null
         or v_paper_mail.attempt_lease_expires_at_utc>p_now_utc) then
      raise exception 'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS'
        using errcode='40001',detail=jsonb_build_object(
          'workflow_id',v_workflow.id,'generation',v_workflow.generation,
          'mail_outbox_id',v_paper_mail.id
        )::text;
    end if;
    if lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
         in ('true','t','1','yes') then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;
    if exists(
      select 1
      from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
      where (
        select count(*)
        from public.candidate_submission_components returned_page
        where returned_page.workflow_id=v_workflow.id
          and returned_page.workflow_generation=v_workflow.generation
          and returned_page.component_kind='SIGNED_RETURN'
          and returned_page.paper_return_page_key=expected_page->>'page_key'
          and returned_page.state='IMMUTABLE'
          and returned_page.source_content_sha256 is not null
      )<>1
    ) or exists(
      select 1
      from public.candidate_submission_components returned_page
      where returned_page.workflow_id=v_workflow.id
        and returned_page.workflow_generation=v_workflow.generation
        and returned_page.component_kind='SIGNED_RETURN'
        and returned_page.state='IMMUTABLE'
        and not exists(
          select 1
          from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
          where expected_page->>'page_key'=returned_page.paper_return_page_key
        )
    ) then raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='22023'; end if;
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state','RECEIVED','generation',v_workflow.generation);
    update public.candidate_submission_workflows set
      state='RECEIVED',last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,updated_at_utc=p_now_utc
    where id=v_workflow.id returning * into v_workflow;

  elsif v_action='MARK_READ' then
    update public.candidate_notifications set state='READ',read_at_utc=p_now_utc
    where id=nullif(v_payload->>'notification_id','')::uuid
      and account_id=v_account_id and state='UNREAD';
    v_response:=jsonb_build_object('ok',true,
      'notification_id',nullif(v_payload->>'notification_id','')::uuid,'state','READ');
  else
    raise exception 'CANDIDATE_WORKFLOW_ACTION_INVALID'
      using errcode='22023',detail=jsonb_build_object(
        'code','CANDIDATE_WORKFLOW_ACTION_INVALID','action',v_action)::text;
  end if;

  perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,
    'CANDIDATE_WORKFLOW_'||v_action,
    jsonb_build_object('state',v_workflow.state,'generation',v_workflow.generation),
    v_response,v_audit_reason,null,p_idempotency_key,p_now_utc);
  if v_mutation_request_sha256 is not null then
    perform private._candidate_workflow_mutation_receipt_v1(
      v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
      v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
    );
  end if;
  return v_response;
exception
  when unique_violation then
    get stacked diagnostics v_constraint_name=constraint_name;
    if v_constraint_name='candidate_submission_components_source_sha256_uq' then
      raise exception 'CANDIDATE_EVIDENCE_BYTES_ALREADY_USED' using errcode='23505';
    elsif v_constraint_name in (
      'candidate_submission_components_hours_review_uq',
      'candidate_submission_components_required_ordinal_uq'
    ) then
      raise exception 'MANAGER_REVIEW_DOCUMENT_STALE' using errcode='23505';
    elsif v_constraint_name='candidate_submission_workflows_one_active_expense_uq' then
      raise exception 'CANDIDATE_EXPENSE_CLAIM_ALREADY_ACTIVE' using errcode='23505';
    elsif v_constraint_name='candidate_submission_workflows_account_idempotency_uq' then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
        using errcode=case when v_is_rejected_resubmission then '55000' else '40001' end;
    elsif v_constraint_name='candidate_submission_workflows_replacement_source_uq' then
      raise exception 'CANDIDATE_REJECTED_WORKFLOW_ALREADY_REPLACED' using errcode='55000';
    elsif v_constraint_name='candidate_submission_components_paper_return_page_uq' then
      raise exception 'CANDIDATE_PAPER_RETURN_PAGE_DUPLICATE' using errcode='23505';
    end if;
    raise;
end;
$function$;
alter function public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz) owner to postgres;
revoke all on function public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz) to service_role;


create or replace function public.candidate_submission_finalize_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_expected_row_signature text default null,
  p_idempotency_key text default null,
  p_now_utc timestamptz default now(),
  p_daily_materialisation_json jsonb default null
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
set lock_timeout = '5s'
set statement_timeout = '120s'
as $function$
declare
  v_environment text;
  v_context jsonb;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_hours_component public.candidate_submission_components%rowtype;
  v_candidate_signature public.candidate_submission_components%rowtype;
  v_manager_signature public.candidate_submission_components%rowtype;
  v_approved_request public.candidate_approval_requests%rowtype;
  v_paper_hours_return public.candidate_submission_components%rowtype;
  v_contract public.contracts%rowtype;
  v_week public.contract_weeks%rowtype;
  v_anchor_week public.contract_weeks%rowtype;
  v_anchor_timesheet public.timesheets%rowtype;
  v_daily_timesheet public.timesheets%rowtype;
  v_daily_fin public.timesheets_financials%rowtype;
  v_daily_receipt_context jsonb;
  v_daily_receipt_only boolean:=false;
  v_completion_state text:='FINALISED';
  v_completion_generation integer;
  v_current_policy jsonb;
  v_system_actor uuid;
  v_input jsonb;
  v_electronic_patch jsonb:='{}'::jsonb;
  v_render_input jsonb;
  v_result jsonb;
  v_authorise_result jsonb;
  v_expense_authorise_result jsonb;
  v_response jsonb;
  v_target_timesheet_id uuid;
  v_hours_timesheet_id uuid;
  v_expense_timesheet_id uuid;
  v_evidence_component_ids uuid[];
  v_placement jsonb;
  v_hours_result jsonb;
  v_hours_input jsonb;
  v_expense_input jsonb;
  v_effective_separation boolean:=false;
  v_after_signature text;
  v_auto_requested boolean:=false;
  v_auto_blocked boolean:=false;
  v_auto_blockers jsonb:='[]'::jsonb;
  v_constraint_name text;
  v_target_capabilities jsonb;
  v_route_authority jsonb;
  v_daily_save_input jsonb;
  v_daily_patch jsonb;
  v_daily_signature jsonb;
  v_daily_save_receipt jsonb;
  v_canonical_financials_id uuid;
  v_canonical_financial_sha256 bytea;
  v_service_finalisation jsonb;
  v_is_office_service boolean:=false;
  v_replay_probe_only boolean:=false;
  v_key_replay_probe_only boolean:=false;
  v_mutation_channel text;
  v_mutation_actor_identity text;
  v_mutation_request_hash text;
  v_mutation_receipt jsonb;
  v_prior_receipt_before jsonb;
  v_prior_receipt_after jsonb;
  v_finalisation_identity jsonb;
  v_finalisation_identity_hash text;
  v_completion_before jsonb;
  v_completion_after jsonb;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  v_service_finalisation:=coalesce(p_daily_materialisation_json->'service_finalisation','{}'::jsonb);
  v_is_office_service:=p_session_id is null
    and private._candidate_office_service_context_valid_v1(
      v_environment,nullif(v_service_finalisation->>'actor_user_id','')::uuid,'RETRY_FINALISATION'
    );
  v_replay_probe_only:=p_session_id is null
    and coalesce((v_service_finalisation->>'replay_probe_only')::boolean,false);
  v_key_replay_probe_only:=p_session_id is null
    and coalesce((v_service_finalisation->>'replay_key_probe_only')::boolean,false);
  if not v_is_office_service then
    perform private._candidate_require_feature_v1(v_environment,'candidate_app_writes');
  end if;
  if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023'; end if;

  select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id for update;
  if not found or v_workflow.environment<>v_environment then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if p_session_id is null then
    v_candidate_id:=v_workflow.candidate_id;
  else
    v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,true);
    v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
    if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;
  end if;
  if v_workflow.candidate_id<>v_candidate_id then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if nullif(btrim(coalesce(v_workflow.idempotency_key,'')),'')=btrim(p_idempotency_key) then
    raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_IDEMPOTENCY_CONFLICT','workflow_id',v_workflow.id,
        'idempotency_key',btrim(p_idempotency_key),
        'reason','CREATION_KEY_REUSED_FOR_MUTATION'
      )::text;
  end if;
  v_mutation_channel:=case when v_is_office_service then 'OFFICE'
    when p_session_id is null then 'SERVICE' else 'CANDIDATE_CLIENT' end;
  v_mutation_actor_identity:=case when v_is_office_service
    then v_service_finalisation->>'actor_user_id' else coalesce(p_session_id::text,'SERVICE') end;
  if v_key_replay_probe_only then
    select ae.before_json,ae.after_json
    into v_prior_receipt_before,v_prior_receipt_after
    from public.audit_events ae
    where ae.object_type='candidate_workflow_mutation_receipt'
      and ae.object_id_text=v_workflow.id::text
      and ae.correlation_id=btrim(p_idempotency_key)
    order by ae.ts_utc desc,ae.id desc
    limit 1;
    if found then
      if upper(coalesce(v_prior_receipt_before->>'workflow_action',''))<>'RETRY_FINALISATION'
         or upper(coalesce(v_prior_receipt_before->>'channel',''))<>v_mutation_channel
         or coalesce(v_prior_receipt_before->>'actor_identity','')
              is distinct from coalesce(v_mutation_actor_identity,'')
         or nullif(v_prior_receipt_after->>'generation','')::integer
              is distinct from (p_expected_generation+case
                when v_workflow.workflow_kind='DAILY'
                  and v_prior_receipt_after->>'state'='RECEIVED'
                  and v_prior_receipt_after->>'office_resolution_pending'='true' then 0 else 1 end) then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_IDEMPOTENCY_CONFLICT','workflow_id',v_workflow.id,
            'idempotency_key',btrim(p_idempotency_key)
          )::text;
      end if;
      return coalesce(v_prior_receipt_after,'{}'::jsonb)
        ||jsonb_build_object('idempotent_replay',true);
    end if;
    return jsonb_build_object(
      'ok',true,'replay_found',false,'workflow_id',v_workflow.id,
      'expected_generation',p_expected_generation
    );
  end if;
  -- Candidate-session calls retain their established lifecycle errors before
  -- an immutable approval identity exists. Service replay/finalisation calls
  -- continue through the receipt path below before mutable lifecycle checks.
  if p_session_id is not null then
    if v_workflow.route='PAPER' and v_workflow.state<>'RECEIVED' then
      raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='55000';
    elsif v_workflow.route<>'PAPER' and v_workflow.state<>'READY_TO_FINALISE' then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
  end if;
  v_finalisation_identity:=v_service_finalisation->'finalisation_identity';
  if jsonb_typeof(v_finalisation_identity) is distinct from 'object' then
    if v_workflow.route='PAPER' then
      v_finalisation_identity:=jsonb_build_object(
        'contract_version','CANDIDATE_FINALISATION_IDENTITY_V1',
        'workflow_id',v_workflow.id,'workflow_generation',p_expected_generation,
        'approval_method','PAPER','approval_request_id',null,
        'approval_request_generation',null,'review_manifest_sha256_hex',null,
        'paper_return_manifest_sha256_hex',case when v_workflow.paper_return_manifest_sha256 is null
          then null else encode(v_workflow.paper_return_manifest_sha256,'hex') end
      );
    else
      select approved.* into v_approved_request
      from public.candidate_approval_requests approved
      where approved.workflow_id=v_workflow.id
        and approved.workflow_generation=p_expected_generation
        and approved.state='APPROVED'
        and (nullif(v_service_finalisation->>'approval_request_id','') is null
          or approved.id=(v_service_finalisation->>'approval_request_id')::uuid)
      order by approved.approved_at_utc desc,approved.id desc
      limit 1;
      v_finalisation_identity:=jsonb_build_object(
        'contract_version','CANDIDATE_FINALISATION_IDENTITY_V1',
        'workflow_id',v_workflow.id,'workflow_generation',p_expected_generation,
        'approval_method',coalesce(v_approved_request.method,v_service_finalisation->>'approval_method'),
        'approval_request_id',coalesce(v_approved_request.id,
          nullif(v_service_finalisation->>'approval_request_id','')::uuid),
        'approval_request_generation',v_approved_request.request_generation,
        'review_manifest_sha256_hex',case when v_approved_request.review_manifest_sha256 is null
          then null else encode(v_approved_request.review_manifest_sha256,'hex') end,
        'paper_return_manifest_sha256_hex',null
      );
    end if;
    v_service_finalisation:=v_service_finalisation||jsonb_build_object(
      'contract_version','CANDIDATE_MANAGER_FINALISATION_V1',
      'workflow_generation',p_expected_generation,
      'approval_method',v_finalisation_identity->>'approval_method',
      'approval_request_id',v_finalisation_identity->'approval_request_id',
      'approval_request_generation',v_finalisation_identity->'approval_request_generation',
      'review_manifest_sha256_hex',coalesce(v_finalisation_identity->>'review_manifest_sha256_hex',''),
      'paper_return_manifest_sha256_hex',coalesce(v_finalisation_identity->>'paper_return_manifest_sha256_hex',''),
      'finalisation_identity',v_finalisation_identity
    );
  end if;
  if jsonb_typeof(v_finalisation_identity) is distinct from 'object'
     or coalesce(v_finalisation_identity->>'contract_version','')
          <>'CANDIDATE_FINALISATION_IDENTITY_V1'
     or nullif(v_finalisation_identity->>'workflow_id','')::uuid is distinct from v_workflow.id
     or coalesce((v_finalisation_identity->>'workflow_generation')::integer,0)
          <>p_expected_generation
     or upper(coalesce(v_finalisation_identity->>'approval_method','')) not in ('EMAIL','PHONE','PAPER') then
    raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
      using errcode='28000',detail=jsonb_build_object('stage','IDENTITY')::text;
  end if;
  v_finalisation_identity_hash:=encode(extensions.digest(convert_to(
    v_finalisation_identity::text,'UTF8'
  ),'sha256'),'hex');
  v_mutation_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','CANDIDATE_FINALISATION_MUTATION_REQUEST_V3',
    'workflow_id',v_workflow.id,
    'action','RETRY_FINALISATION',
    'expected_generation',p_expected_generation,
    'service_finalisation',v_service_finalisation-'replay_probe_only',
    'channel',v_mutation_channel,
    'actor_identity',v_mutation_actor_identity
  )::text,'UTF8'),'sha256'),'hex');
  if v_replay_probe_only then
    if p_session_id is null and (
      coalesce(v_service_finalisation->>'contract_version','')
        <>'CANDIDATE_MANAGER_FINALISATION_V1'
       or coalesce((v_service_finalisation->>'workflow_generation')::integer,0)
         <>p_expected_generation
    ) then
      raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
        using errcode='28000',detail=jsonb_build_object('stage','REPLAY_ENVELOPE')::text;
    end if;
    select ae.before_json,ae.after_json
    into v_prior_receipt_before,v_prior_receipt_after
    from public.audit_events ae
    where ae.object_type='candidate_workflow_mutation_receipt'
      and ae.object_id_text=v_workflow.id::text
      and ae.correlation_id=btrim(p_idempotency_key)
    order by ae.ts_utc desc,ae.id desc
    limit 1;
    if found then
      if v_prior_receipt_before->>'request_sha256' is distinct from v_mutation_request_hash
         or upper(coalesce(v_prior_receipt_before->>'workflow_action',''))<>'RETRY_FINALISATION'
         or upper(coalesce(v_prior_receipt_before->>'channel',''))<>v_mutation_channel
         or coalesce(v_prior_receipt_before->>'actor_identity','')
              is distinct from coalesce(v_mutation_actor_identity,'')
         or nullif(v_prior_receipt_after->>'generation','')::integer
              is distinct from (p_expected_generation+case
                when v_workflow.workflow_kind='DAILY'
                  and v_prior_receipt_after->>'state'='RECEIVED'
                  and v_prior_receipt_after->>'office_resolution_pending'='true' then 0 else 1 end) then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_IDEMPOTENCY_CONFLICT','workflow_id',v_workflow.id,
            'idempotency_key',btrim(p_idempotency_key)
          )::text;
      end if;
      return coalesce(v_prior_receipt_after,'{}'::jsonb)
        ||jsonb_build_object('idempotent_replay',true);
    end if;
    select ae.before_json,ae.after_json
    into v_completion_before,v_completion_after
    from public.audit_events ae
    where ae.object_type='candidate_workflow_finalisation_completion'
      and ae.object_id_text=v_workflow.id::text
      and ae.correlation_id=p_expected_generation::text||':'||v_finalisation_identity_hash
    order by ae.ts_utc desc,ae.id desc
    limit 1;
    if found then
      if v_completion_before->>'finalisation_identity_sha256'
           is distinct from v_finalisation_identity_hash
         or nullif(v_completion_after->>'generation','')::integer
           is distinct from (p_expected_generation+case
             when v_workflow.workflow_kind='DAILY'
               and v_completion_after->>'state'='RECEIVED'
               and v_completion_after->>'office_resolution_pending'='true' then 0 else 1 end) then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='40001';
      end if;
      return coalesce(v_completion_after,'{}'::jsonb)
        ||jsonb_build_object('idempotent_replay',true);
    end if;
    return jsonb_build_object('ok',true,'replay_found',false,'workflow_id',v_workflow.id,
      'expected_generation',p_expected_generation);
  end if;
  v_mutation_receipt:=private._candidate_workflow_mutation_receipt_v1(
    v_workflow.id,p_idempotency_key,v_mutation_request_hash,'RETRY_FINALISATION',
    v_mutation_channel,v_mutation_actor_identity,
    null,p_now_utc
  );
  if coalesce((v_mutation_receipt->>'found')::boolean,false) then
    return coalesce(v_mutation_receipt->'response','{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
  end if;
  select ae.before_json,ae.after_json
  into v_completion_before,v_completion_after
  from public.audit_events ae
  where ae.object_type='candidate_workflow_finalisation_completion'
    and ae.object_id_text=v_workflow.id::text
    and ae.correlation_id=p_expected_generation::text||':'||v_finalisation_identity_hash
  order by ae.ts_utc desc,ae.id desc
  limit 1;
  if found then
    return coalesce(v_completion_after,'{}'::jsonb)
      ||jsonb_build_object('idempotent_replay',true);
  end if;
  if p_session_id is null then
    if coalesce(v_service_finalisation->>'contract_version','')<>'CANDIDATE_MANAGER_FINALISATION_V1'
       or coalesce((v_service_finalisation->>'workflow_generation')::integer,0)<>v_workflow.generation
       or upper(coalesce(v_service_finalisation->>'approval_method',''))<>v_workflow.route then
      raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
        using errcode='28000',detail=jsonb_build_object('stage','SERVICE_ENVELOPE')::text;
    end if;
    if v_workflow.route='PAPER' then
      if nullif(v_service_finalisation->>'approval_request_id','') is not null
         or nullif(v_finalisation_identity->>'approval_request_id','') is not null
         or upper(v_finalisation_identity->>'approval_method')<>'PAPER'
         or lower(coalesce(v_finalisation_identity->>'paper_return_manifest_sha256_hex',''))
              <>encode(v_workflow.paper_return_manifest_sha256,'hex') then
        raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
          using errcode='28000',detail=jsonb_build_object('stage','PAPER_IDENTITY')::text;
      end if;
    else
      select * into v_approved_request
      from public.candidate_approval_requests a
      where a.id=nullif(v_service_finalisation->>'approval_request_id','')::uuid
        and a.workflow_id=v_workflow.id
        and a.workflow_generation=p_expected_generation
        and a.request_generation=coalesce(
          nullif(v_service_finalisation->>'approval_request_generation','')::integer,0
        )
        and a.method=upper(coalesce(v_service_finalisation->>'approval_method',''))
        and a.state='APPROVED'
        and encode(a.review_manifest_sha256,'hex')=lower(coalesce(v_service_finalisation->>'review_manifest_sha256_hex',''))
        and v_finalisation_identity->>'approval_request_id'=a.id::text
        and coalesce((v_finalisation_identity->>'approval_request_generation')::integer,0)=a.request_generation
        and upper(v_finalisation_identity->>'approval_method')=a.method
        and lower(coalesce(v_finalisation_identity->>'review_manifest_sha256_hex',''))
              =encode(a.review_manifest_sha256,'hex')
      for update;
      if not found then
        raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
          using errcode='28000',detail=jsonb_build_object('stage','APPROVAL_IDENTITY')::text;
      end if;
    end if;
  end if;
  if v_workflow.generation<>p_expected_generation then
    raise exception 'WORKFLOW_VERSION_MISMATCH'
      using errcode='40001',detail=jsonb_build_object('code','WORKFLOW_VERSION_MISMATCH','current_generation',v_workflow.generation)::text;
  end if;
  if v_workflow.workflow_kind='DAILY' then
    if v_workflow.scope<>'DAILY' or v_workflow.route not in ('PHONE','EMAIL')
       or v_workflow.contract_week_id is not null or v_workflow.week_ending_date is not null
       or v_workflow.target_timesheet_id is null
       or v_workflow.anchor_timesheet_id is distinct from v_workflow.target_timesheet_id then
      raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
    end if;
    v_daily_receipt_context:=private._candidate_daily_receipt_context_v1(
      v_environment,v_candidate_id,v_workflow.target_timesheet_id,true,p_now_utc);
    v_daily_receipt_only:=coalesce((v_daily_receipt_context->>'candidate_first_receipt')::boolean,false)
      and coalesce((v_daily_receipt_context->>'office_resolution_pending')::boolean,false);
    select * into v_daily_timesheet
    from public.timesheets
    where timesheet_id=v_workflow.target_timesheet_id
      and is_current=true and archived_at_utc is null
      and sheet_scope='DAILY'::public.timesheet_scope_enum
      and nullif(btrim(coalesce(booking_id,'')),'') is not null
    for update;
    if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
    if not private._candidate_daily_entitled_v1(v_candidate_id) then
      raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
    end if;
    select * into v_daily_fin
    from public.timesheets_financials
    where timesheet_id=v_daily_timesheet.timesheet_id
      and is_current=true and candidate_id=v_candidate_id
    order by computed_at_utc desc nulls last,updated_at desc,id desc
    limit 1
    for update;
    if (not found and not v_daily_receipt_only)
       or v_workflow.work_date is distinct from private._candidate_daily_work_date_v1(
         coalesce(v_daily_fin.worked_start_iso,v_daily_timesheet.worked_start_iso),
         v_daily_timesheet.scheduled_start_iso,
         v_daily_timesheet.week_ending_date
       ) then
      raise exception 'CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH' using errcode='40001';
    end if;
    if v_daily_fin.authorised_at_utc is not null
       or v_daily_fin.paid_at_utc is not null
       or v_daily_fin.locked_by_invoice_id is not null
       or v_daily_timesheet.archived_at_utc is not null then
      raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
    end if;
    if v_daily_timesheet.contract_id is not null then
      select * into v_contract
      from public.contracts
      where id=v_daily_timesheet.contract_id and candidate_id=v_candidate_id
      for update;
      if not found then raise exception 'CANDIDATE_WORKFLOW_OWNERSHIP_MISMATCH' using errcode='28000'; end if;
    end if;
    if v_daily_receipt_only then
      v_current_policy:=v_daily_receipt_context->'policy';
    else
      if coalesce(v_daily_fin.client_id,v_contract.client_id) is null then
        raise exception 'CANDIDATE_DAILY_CLIENT_NOT_FOUND' using errcode='P0002';
      end if;
      v_current_policy:=private._candidate_policy_resolve_v1(
        coalesce(v_daily_fin.client_id,v_contract.client_id),v_contract.id,v_workflow.work_date
      );
    end if;
    if (v_workflow.route='PHONE' and not coalesce((v_current_policy->>'allow_daily_manager_authorise_on_phone')::boolean,false))
       or (v_workflow.route='EMAIL' and not coalesce((v_current_policy->>'allow_daily_manager_authorise_by_email')::boolean,false)) then
      raise exception 'CANDIDATE_DAILY_APPROVAL_ROUTE_NOT_ALLOWED' using errcode='55000';
    end if;
  else
    if v_workflow.workflow_kind not in ('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED')
       or v_workflow.scope<>'WEEKLY' or v_workflow.contract_id is null
       or v_workflow.contract_week_id is null or v_workflow.week_ending_date is null then
      raise exception 'CANDIDATE_CONTRACT_WORKFLOW_IDENTITY_INVALID' using errcode='22023';
    end if;
    select * into v_contract
    from public.contracts
    where id=v_workflow.contract_id and candidate_id=v_candidate_id
    for update;
    if not found then raise exception 'CANDIDATE_WORKFLOW_OWNERSHIP_MISMATCH' using errcode='28000'; end if;
    select * into v_week
    from public.contract_weeks
    where id=v_workflow.contract_week_id
      and contract_id=v_contract.id
      and week_ending_date=v_workflow.week_ending_date
    for update;
    if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_IDENTITY_MISMATCH' using errcode='40001'; end if;
    if v_workflow.anchor_timesheet_id is not null then
      select cw.* into v_anchor_week
      from public.contract_weeks cw
      join public.timesheets t on t.timesheet_id=cw.timesheet_id
        and t.is_current=true and t.archived_at_utc is null
      where cw.timesheet_id=v_workflow.anchor_timesheet_id
        and cw.contract_id=v_contract.id
        and cw.week_ending_date=v_workflow.week_ending_date;
      if not found then raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001'; end if;
    end if;
    if v_workflow.workflow_kind='CONTRACT_EXPENSE' then
      if v_workflow.anchor_timesheet_id is null
         or coalesce((private._candidate_record_capabilities_v1(
           v_workflow.anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb
         )->>'hours_value')::numeric,0)<=0
            and coalesce((private._candidate_record_capabilities_v1(
              v_workflow.anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb
            )->>'additional_units_value')::numeric,0)<=0 then
        -- Reuse the existing expense-admission authority for source weeks:
        -- immutable Candidate worked-hours evidence may precede source TSFIN.
        -- This neither writes nor unlocks the source-owned hours record.
        if v_workflow.anchor_timesheet_id is null
           or not coalesce((private._candidate_record_capabilities_v1(
             v_workflow.anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb
           )->>'import_authoritative')::boolean,false) then
          raise exception 'NO_POSITIVE_WORKED_TIME' using errcode='55000';
        end if;
        v_placement:=public.expense_placement_resolve_v1(
          v_candidate_id,v_environment,v_workflow.anchor_timesheet_id,
          v_anchor_week.id,'{}'::jsonb,p_now_utc);
        if not coalesce((v_placement->>'ok')::boolean,false)
           or coalesce(v_placement->>'placement','') not in ('REUSE_CARRIER','CREATE_CARRIER') then
          raise exception 'CANDIDATE_EXPENSE_FINALISATION_ADMISSION_BLOCKED'
            using errcode='55000',detail=coalesce(v_placement->>'reason_code','NO_POSITIVE_WORKED_TIME');
        end if;
      end if;
    end if;
    if v_workflow.workflow_kind='CONTRACT_EXPENSE' and v_workflow.target_timesheet_id is not null then
      raise exception 'CANDIDATE_EXPENSE_TARGET_SERVER_RESOLVED' using errcode='40001';
    elsif v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
       and v_workflow.target_timesheet_id is distinct from v_week.timesheet_id then
      raise exception 'CANDIDATE_WORKFLOW_TARGET_MISMATCH' using errcode='40001';
    end if;
    if v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
       and v_week.timesheet_id is not null then
      v_target_capabilities:=private._candidate_record_capabilities_v1(
        v_week.timesheet_id,v_week.id,'{}'::jsonb
      );
      if coalesce((v_target_capabilities->>'candidate_mutation_locked')::boolean,false)
         or coalesce((v_target_capabilities->>'protected')::boolean,false)
         or not coalesce((v_target_capabilities->>'can_edit_hours')::boolean,false) then
        raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
      end if;
    end if;
    v_route_authority:=private._expense_approval_route_v1(
      case when v_workflow.workflow_kind='CONTRACT_EXPENSE' then v_workflow.anchor_timesheet_id
        else v_week.timesheet_id end,
      case when v_workflow.workflow_kind='CONTRACT_EXPENSE' then v_anchor_week.id else v_week.id end,v_workflow.workflow_kind
    );
    if v_route_authority->>'route_family'='MANUAL_NON_QR'
       or (v_route_authority->>'route_family'='IMPORT_AUTHORITATIVE'
         and v_workflow.workflow_kind<>'CONTRACT_EXPENSE')
       or (v_workflow.route='PAPER' and not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))
       or (v_workflow.route<>'PAPER' and v_route_authority->>'route_family'='QR') then
      raise exception 'CANDIDATE_ROUTE_FAMILY_MISMATCH' using errcode='55000',detail=v_route_authority::text;
    end if;
    v_current_policy:=private._expense_approval_policy_v1(
      v_contract.client_id,v_contract.id,v_workflow.week_ending_date,v_workflow.workflow_kind
    );
    -- Already-approved electronic claims retain their original policy comparison.
    -- Switching to PAPER always requires the current expense permission.
    if v_workflow.route<>'PAPER'
       and v_workflow.policy_snapshot_json->>'paper_submission_enabled_source'='IMPORT_DISABLED' then
      v_current_policy:=private._candidate_policy_resolve_v1(
        v_contract.client_id,v_contract.id,v_workflow.week_ending_date);
    end if;
    if v_workflow.route='PAPER'
       and not coalesce((v_current_policy->>'paper_submission_enabled')::boolean,false) then
      raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000';
    end if;
  end if;
  if v_workflow.route='PAPER' then
    if v_workflow.state<>'RECEIVED'
       or v_workflow.paper_return_manifest_sha256 is null
       or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
          is distinct from v_workflow.paper_return_manifest_sha256
       or exists(
         select 1
         from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
         where (
           select count(*)
           from public.candidate_submission_components returned_page
           where returned_page.workflow_id=v_workflow.id
             and returned_page.workflow_generation=v_workflow.generation
             and returned_page.component_kind='SIGNED_RETURN'
             and returned_page.paper_return_page_key=expected_page->>'page_key'
             and returned_page.state='IMMUTABLE'
             and returned_page.source_content_sha256 is not null
         )<>1
       )
       or exists(
         select 1
         from public.candidate_submission_components returned_page
         where returned_page.workflow_id=v_workflow.id
           and returned_page.workflow_generation=v_workflow.generation
           and returned_page.component_kind='SIGNED_RETURN'
           and returned_page.state='IMMUTABLE'
           and not exists(
             select 1
             from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
             where expected_page->>'page_key'=returned_page.paper_return_page_key
           )
       ) then
      raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='55000';
    end if;
  else
    if v_workflow.state<>'READY_TO_FINALISE' then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
    select * into v_approved_request
    from public.candidate_approval_requests a
      where a.workflow_id=v_workflow.id and a.workflow_generation=v_workflow.generation
        and a.state='APPROVED' and a.review_manifest_sha256=v_workflow.review_manifest_sha256
    for update;
    if not found then raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000'; end if;

    if not exists(
      select 1 from public.candidate_submission_components c
      where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
        and c.required=true and c.state<>'SUPERSEDED'
    ) or exists(
      select 1 from public.candidate_submission_components c
      where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
        and c.required=true and c.state<>'SUPERSEDED'
        and (c.state<>'IMMUTABLE' or c.review_render_state<>'READY'
          or c.final_signed_render_state<>'READY'
          or c.review_render_input_sha256 is distinct from c.final_signed_render_input_sha256)
    ) then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
    select * into v_hours_component from public.candidate_submission_components c
    where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
      and c.component_kind='HOURS_TIMESHEET' and c.required=true and c.state='IMMUTABLE'
    for update;
    if v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY') and not found then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    elsif v_workflow.workflow_kind='CONTRACT_EXPENSE' and found then
      raise exception 'CONTRACT_EXPENSE_HOURS_COMPONENT_FORBIDDEN' using errcode='55000';
    end if;
    if v_hours_component.id is not null and v_hours_component.review_render_input_sha256
       is distinct from v_hours_component.final_signed_render_input_sha256 then
      raise exception 'FINAL_RENDER_INPUT_MISMATCH' using errcode='40001';
    end if;
    if v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
      select * into v_candidate_signature from public.candidate_submission_components c
      where c.id=v_workflow.candidate_signature_component_id and c.workflow_id=v_workflow.id
        and c.document_role='CANDIDATE_SIGNATURE' and c.state='IMMUTABLE' for update;
    elsif v_workflow.candidate_signature_component_id is not null
       or v_workflow.candidate_signature_sha256 is not null then
      raise exception 'CONTRACT_EXPENSE_CANDIDATE_SIGNATURE_FORBIDDEN' using errcode='55000';
    end if;
    select * into v_manager_signature from public.candidate_submission_components c
    where c.id=v_workflow.manager_signature_component_id and c.workflow_id=v_workflow.id
      and c.document_role='MANAGER_SIGNATURE' and c.state='IMMUTABLE'
      and c.approval_request_id=v_approved_request.id for update;
    if (v_workflow.workflow_kind<>'CONTRACT_EXPENSE' and (
          v_candidate_signature.id is null
          or v_candidate_signature.source_content_sha256 is distinct from v_workflow.candidate_signature_sha256))
       or v_manager_signature.id is null
       or v_manager_signature.source_content_sha256 is distinct from v_workflow.manager_signature_sha256
       or v_workflow.manager_approved_at_utc is null then
      raise exception 'ELECTRONIC_SIGNATURE_PAIR_INCOMPLETE' using errcode='55000';
    end if;
    v_render_input:=private._candidate_render_input_v1(v_workflow.id,v_workflow.generation);
    if v_hours_component.id is not null and decode(v_render_input->>'render_input_sha256','hex')
       is distinct from decode(
         private._candidate_component_render_input_v1(
           v_workflow.id,v_workflow.generation,v_hours_component.id
         )->>'workflow_render_input_sha256','hex'
       ) then
      raise exception 'FINAL_RENDER_INPUT_MISMATCH' using errcode='40001';
    end if;
    v_electronic_patch:=jsonb_build_object(
      'submission_mode','ELECTRONIC',
      'auth_name',v_workflow.manager_name,
      'auth_job_title',v_workflow.manager_position,
      'r2_nurse_key',v_candidate_signature.storage_key,
      'r2_auth_key',v_manager_signature.storage_key,
      'img_sha256_nurse',case when v_candidate_signature.source_content_sha256 is null then null
        else encode(v_candidate_signature.source_content_sha256,'hex') end,
      'img_sha256_auth',encode(v_manager_signature.source_content_sha256,'hex'),
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',v_workflow.manager_approved_at_utc
    );
  end if;
  if v_workflow.route='PAPER' then
    v_electronic_patch:=jsonb_build_object(
      'submission_mode','MANUAL',
      'r2_nurse_key',null,
      'r2_auth_key',null,
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',null
    );
  end if;

  if coalesce(v_current_policy->>'policy_fingerprint','')
     is distinct from coalesce(v_workflow.policy_snapshot_json->>'policy_fingerprint','') then
    update public.candidate_approval_requests set state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and state='PENDING';
    v_response:=jsonb_build_object('ok',false,'error_code','CANDIDATE_POLICY_CHANGED','workflow_id',v_workflow.id,
      'state','SUPERSEDED','generation',v_workflow.generation+1,'current_policy',v_current_policy);
    update public.candidate_submission_workflows set state='SUPERSEDED',generation=generation+1,
      policy_snapshot_json=v_current_policy,policy_snapshot_sha256=private._candidate_sha256_jsonb_v1(v_current_policy),
      last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,updated_at_utc=p_now_utc where id=v_workflow.id;
    perform private._candidate_workflow_mutation_receipt_v1(
      v_workflow.id,p_idempotency_key,v_mutation_request_hash,'RETRY_FINALISATION',
      case when v_is_office_service then 'OFFICE' when p_session_id is null then 'SERVICE' else 'CANDIDATE_CLIENT' end,
      case when v_is_office_service then v_service_finalisation->>'actor_user_id' else coalesce(p_session_id::text,'SERVICE') end,
      v_response,p_now_utc
    );
    return v_response;
  end if;

  select candidate_app_system_actor_user_id into v_system_actor from public.settings_defaults where id=1;
  if v_system_actor is null then raise exception 'CANDIDATE_SYSTEM_ACTOR_NOT_CONFIGURED' using errcode='55000'; end if;
  v_input:=v_workflow.immutable_submission_json;
  if v_input is null or private._candidate_sha256_jsonb_v1(v_input)
     is distinct from v_workflow.immutable_submission_sha256 then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_MISMATCH' using errcode='40001';
  end if;
  v_workflow.issue_codes:=private._candidate_finalisation_issue_codes_v1(
    v_workflow.issue_codes,
    private._candidate_submission_issue_codes_v1(
      v_workflow.id,v_input,v_current_policy
    )
  );
  v_effective_separation:=coalesce((v_current_policy->>'expenses_require_separate_timesheet')::boolean,false);
  if v_workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
     and coalesce((v_current_policy->>'import_expense_separation_mandatory')::boolean,false)
     and not coalesce((v_current_policy->>'expense_invoice_email_ready')::boolean,false) then
    raise exception 'EXPENSE_INVOICE_EMAIL_REQUIRED' using errcode='55000';
  end if;

  if v_workflow.scope='WEEKLY' then
    select * into v_week from public.contract_weeks where id=v_workflow.contract_week_id for update;
    if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
    if v_workflow.workflow_kind='CONTRACT_COMBINED' then
      v_hours_input:=coalesce(v_input->'hours_submission',v_input);
      v_expense_input:=coalesce(v_input->'expense_submission',v_input);
      if jsonb_typeof(v_hours_input)<>'object' or jsonb_typeof(v_expense_input)<>'object' then
        raise exception 'CANDIDATE_COMBINED_SNAPSHOTS_REQUIRED' using errcode='22023';
      end if;
    elsif v_workflow.workflow_kind='CONTRACT_EXPENSE' then
      v_expense_input:=v_input;
    else
      v_hours_input:=v_input;
    end if;

    if v_hours_input is not null then
      if v_hours_input->'canonical_tsfin_snapshot' is null then
        raise exception 'CANDIDATE_CANONICAL_TSFIN_SNAPSHOT_REQUIRED' using errcode='22023';
      end if;
      perform set_config('cloudtms.candidate_electronic_finalise',v_workflow.id::text||':'||v_workflow.generation::text,true);
      v_hours_result:=public.contract_week_manual_upsert_atomic(
        p_week_id=>v_week.id,
        p_expected_timesheet_id=>v_workflow.target_timesheet_id,
        p_timesheet_create_json=>case when v_workflow.target_timesheet_id is null
          then coalesce(v_hours_input->'timesheet_create_json','{}'::jsonb)||v_electronic_patch else null end,
        p_timesheet_patch_json=>coalesce(v_hours_input->'timesheet_patch_json','{}'::jsonb)||v_electronic_patch,
        p_contract_week_patch_json=>coalesce(v_hours_input->'contract_week_patch_json','{}'::jsonb),
        p_tsfin_snapshot_json=>v_hours_input->'canonical_tsfin_snapshot',
        p_rotation_json=>null,p_actor_user_id=>v_system_actor,p_materialise_staged_evidence=>false,
        p_now_utc=>p_now_utc,p_expected_row_signature=>coalesce(p_expected_row_signature,v_workflow.expected_row_signature),
        p_queue_timesheet_materialisation_json=>jsonb_build_object('suppress_timesheet_evidence_materialisation',true)
      );
      if coalesce((v_hours_result->>'ok')::boolean,false)=false then
        raise exception 'CANDIDATE_FINALISE_CANONICAL_APPLY_FAILED' using errcode='55000',detail=v_hours_result::text;
      end if;
      v_hours_timesheet_id:=coalesce(nullif(v_hours_result->>'timesheet_id','')::uuid,
        nullif(v_hours_result#>>'{timesheet,timesheet_id}','')::uuid);
      v_after_signature:=coalesce(v_hours_result->>'row_signature',v_hours_result->>'backend_row_signature',
        v_hours_result#>>'{timesheet,row_signature}');
    end if;

    if v_expense_input is not null then
      if v_workflow.workflow_kind='CONTRACT_EXPENSE' or v_effective_separation then
        v_placement:=public.expense_carrier_resolve_or_create_atomic_v1(
          v_candidate_id,v_environment,coalesce(v_hours_timesheet_id,v_workflow.anchor_timesheet_id),
          coalesce(v_after_signature,p_expected_row_signature,v_workflow.expected_row_signature),
          p_idempotency_key||':carrier',p_now_utc);
      else
        v_placement:=jsonb_build_object(
          'placement','SAME_RECORD','target_timesheet_id',coalesce(v_hours_timesheet_id,v_workflow.target_timesheet_id),
          'target_contract_week_id',v_week.id);
      end if;
      if v_workflow.route='PAPER' then
        select array_agg(c.id order by c.component_no,c.id) into v_evidence_component_ids
        from public.candidate_submission_components c
        where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
          and c.component_kind='SIGNED_RETURN' and c.state='IMMUTABLE'
          and c.paper_return_page_key<>'HOURS_TIMESHEET';
      else
        select array_agg(c.id order by c.review_ordinal,c.id) into v_evidence_component_ids
        from public.candidate_submission_components c
        where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
          and c.required=true and c.state<>'SUPERSEDED' and c.component_kind<>'HOURS_TIMESHEET';
      end if;
      update public.candidate_submission_workflows set
        -- A first combined weekly submission has no anchor until its hours row
        -- is materialised above. Bind that worked row before expense apply so
        -- SAME_RECORD stays a combined HOURS Timesheet. A genuinely separate
        -- expense carrier still has a different target and remains EXPENSES.
        anchor_timesheet_id=case when v_workflow.workflow_kind='CONTRACT_COMBINED'
          then coalesce(anchor_timesheet_id,v_hours_timesheet_id)
          else anchor_timesheet_id end,
        contract_week_id=nullif(v_placement->>'target_contract_week_id','')::uuid,
        target_timesheet_id=nullif(v_placement->>'target_timesheet_id','')::uuid,
        updated_at_utc=p_now_utc
      where id=v_workflow.id;
      perform set_config('cloudtms.candidate_finalize_workflow',v_workflow.id::text||':'||v_workflow.generation::text,true);
      v_result:=public.timesheet_expense_apply_atomic_v1(
        v_candidate_id,v_environment,nullif(v_placement->>'target_timesheet_id','')::uuid,
        v_workflow.id,v_workflow.generation,
        case when v_placement->>'placement'='SAME_RECORD' then coalesce(v_after_signature,p_expected_row_signature,v_workflow.expected_row_signature)
          else null end,
        v_expense_input,v_evidence_component_ids,p_idempotency_key||':expense',p_now_utc);
      v_expense_timesheet_id:=nullif(v_result->>'target_timesheet_id','')::uuid;
      v_target_timesheet_id:=coalesce(v_expense_timesheet_id,v_hours_timesheet_id);
      if v_hours_result is not null then
        v_result:=jsonb_build_object('ok',true,'hours_result',v_hours_result,'expense_result',v_result,
          'hours_timesheet_id',v_hours_timesheet_id,'expense_timesheet_id',v_expense_timesheet_id);
      end if;
    else
      v_result:=v_hours_result;
      v_target_timesheet_id:=v_hours_timesheet_id;
    end if;
  else
    if not v_is_office_service then
      perform private._candidate_require_feature_v1(v_environment,'candidate_daily_finalisation');
    end if;
    v_target_timesheet_id:=v_workflow.target_timesheet_id;
    if v_target_timesheet_id is null then
      raise exception 'CANDIDATE_DAILY_TIMESHEET_REQUIRED' using errcode='22023';
    end if;
    v_daily_save_input:=private._candidate_daily_canonical_save_input_v1(
      v_workflow.id,v_workflow.generation
    );
    v_daily_patch:=v_daily_save_input->'timesheet_patch_json';
    if jsonb_typeof(p_daily_materialisation_json)<>'object' then
      raise exception 'CANDIDATE_DAILY_MATERIALISATION_REQUIRED' using errcode='22023';
    end if;
    if v_daily_receipt_only then
      if p_daily_materialisation_json->>'contract_version' is distinct from 'CANDIDATE_DAILY_FACTUAL_RECEIPT_V1'
         or p_daily_materialisation_json->>'workflow_id' is distinct from v_workflow.id::text
         or (p_daily_materialisation_json->>'workflow_generation')::integer is distinct from v_workflow.generation
         or p_daily_materialisation_json->>'timesheet_id' is distinct from v_target_timesheet_id::text then
        raise exception 'CANDIDATE_DAILY_RECEIPT_CONTEXT_CHANGED' using errcode='40001';
      end if;
      v_result:=private._candidate_daily_factual_receipt_v1(
        v_workflow.id,v_workflow.generation,
        p_daily_materialisation_json->>'canonical_save_input_sha256_hex',v_electronic_patch,p_now_utc);
      v_completion_state:='RECEIVED';
    else
    -- This private composition performs the pre-write row-signature check,
    -- factual save and bounded TSFIN write in this same finalisation transaction.
    -- Any later Process/Authorise error rolls the factual and financial write back.
    v_daily_save_receipt:=private._candidate_daily_save_recalculate_atomic_v1(
      v_workflow.id,v_workflow.generation,p_daily_materialisation_json,
      v_system_actor,p_now_utc
    );
    select * into v_daily_timesheet from public.timesheets
    where timesheet_id=v_target_timesheet_id and is_current=true for update;
    select * into v_daily_fin from public.timesheets_financials
    where id=nullif(v_daily_save_receipt->>'financials_id','')::uuid
      and timesheet_id=v_target_timesheet_id and is_current=true
    for update;
    if not found or v_daily_fin.processing_status<>'UNPROCESSED' then
      raise exception 'CANDIDATE_DAILY_CANONICAL_RECALCULATION_NOT_READY' using errcode='55000';
    end if;
    v_after_signature:=nullif(v_daily_save_receipt->>'post_save_row_signature','');
    if v_after_signature is null then
      raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_RECEIPT_INVALID' using errcode='55000';
    end if;
    perform set_config('cloudtms.candidate_electronic_finalise','on',true);
    v_result:=public.timesheet_daily_manual_process_atomic(
      v_target_timesheet_id,v_target_timesheet_id,v_system_actor,
      v_electronic_patch,'{}'::jsonb,p_now_utc,v_after_signature
    );
    v_result:=jsonb_build_object(
      'ok',coalesce((v_result->>'ok')::boolean,false),
      'canonical_save_receipt',v_daily_save_receipt,
      'process_result',v_result,'timesheet_id',v_target_timesheet_id,
      'row_signature',coalesce(v_result->>'row_signature',v_result->>'backend_row_signature')
    );
    end if;
    v_hours_timesheet_id:=v_target_timesheet_id;
    v_after_signature:=coalesce(v_result->>'row_signature',v_result->>'backend_row_signature');
  end if;
  if coalesce((v_result->>'ok')::boolean,false)=false then
    raise exception 'CANDIDATE_FINALISE_CANONICAL_APPLY_FAILED'
      using errcode='55000',detail=jsonb_build_object(
        'code',coalesce(v_result->>'error_code','CANDIDATE_FINALISE_CANONICAL_APPLY_FAILED'),
        'canonical_result',v_result)::text;
  end if;
  if v_target_timesheet_id is null then raise exception 'CANDIDATE_FINALISE_TARGET_MISSING' using errcode='55000'; end if;

  if v_workflow.route<>'PAPER' and v_hours_component.id is not null then
    update public.candidate_submission_components set timesheet_id=coalesce(v_hours_timesheet_id,v_target_timesheet_id)
    where id=v_hours_component.id;
    insert into public.timesheet_evidence(
      timesheet_id,kind,display_name,storage_key,created_at,created_by,
      document_role,candidate_component_id,processing_state
    ) values (
      coalesce(v_hours_timesheet_id,v_target_timesheet_id),'TIMESHEET','Official electronically signed timesheet',
      v_hours_component.final_signed_storage_key,p_now_utc,v_system_actor,
      'SIGNED_TIMESHEET',v_hours_component.id,'READY'
    ) on conflict (candidate_component_id) where candidate_component_id is not null do nothing;
  elsif v_workflow.route='PAPER' and v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
    select * into v_paper_hours_return
    from public.candidate_submission_components c
    where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
      and c.component_kind='SIGNED_RETURN' and c.paper_return_page_key='HOURS_TIMESHEET'
      and c.state='IMMUTABLE' and c.source_content_sha256 is not null
    for update;
    if not found then raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='55000'; end if;
    update public.candidate_submission_components set
      timesheet_id=coalesce(v_hours_timesheet_id,v_target_timesheet_id)
    where id=v_paper_hours_return.id;
    insert into public.timesheet_evidence(
      timesheet_id,kind,display_name,storage_key,created_at,created_by,
      document_role,candidate_component_id,processing_state
    ) values (
      coalesce(v_hours_timesheet_id,v_target_timesheet_id),'TIMESHEET','Returned signed paper timesheet',
      v_paper_hours_return.storage_key,p_now_utc,v_system_actor,
      'SIGNED_TIMESHEET',v_paper_hours_return.id,'READY'
    ) on conflict (candidate_component_id) where candidate_component_id is not null do nothing;
  end if;

  v_auto_requested:=coalesce((v_current_policy->>'candidate_electronic_auto_authorise')::boolean,false)
    and v_workflow.route<>'PAPER';
  if v_workflow.issue_codes ?| array[
    'UNEXPECTED_HOURS','DAILY_BREAK_UNEXPECTED','DUPLICATE_EXPENSE_REVIEW',
    'HEALTHROSTER_VALIDATION_REQUIRED','EVIDENCE_REVIEW_REQUIRED',
    'ADDITIONAL_UNITS_NEEDS_CHECKING','PLANNED_HOURS_UNRESOLVED'
  ] then
    v_auto_blocked:=true;
    v_auto_blockers:=v_auto_blockers||v_workflow.issue_codes;
  end if;
  if v_workflow.route='PAPER' then
    v_auto_blocked:=true;v_auto_blockers:=v_auto_blockers||'"PAPER_NEVER_AUTO_AUTHORISES"'::jsonb;
  end if;
  if v_daily_receipt_only then
    v_auto_blocked:=true;
    v_auto_blockers:=v_auto_blockers||'"OFFICE_RESOLUTION_REQUIRED"'::jsonb;
  end if;
  if v_auto_requested and not v_auto_blocked then
    if v_hours_timesheet_id is not null then
      v_authorise_result:=public.timesheet_authorise_generic_atomic(
        v_hours_timesheet_id,v_hours_timesheet_id,v_system_actor,p_now_utc,v_after_signature
      );
      if coalesce((v_authorise_result->>'ok')::boolean,false)=false then
        raise exception 'CANDIDATE_AUTO_AUTHORISE_FAILED' using errcode='55000',detail=v_authorise_result::text;
      end if;
    end if;
    if v_expense_timesheet_id is not null and v_expense_timesheet_id is distinct from v_hours_timesheet_id then
      v_expense_authorise_result:=public.timesheet_authorise_generic_atomic(
        v_expense_timesheet_id,v_expense_timesheet_id,v_system_actor,p_now_utc,null);
      if coalesce((v_expense_authorise_result->>'ok')::boolean,false)=false then
        raise exception 'CANDIDATE_AUTO_AUTHORISE_FAILED' using errcode='55000',detail=v_expense_authorise_result::text;
      end if;
      v_authorise_result:=jsonb_build_object(
        'ok',true,'hours',v_authorise_result,'expenses',v_expense_authorise_result);
    end if;
    if coalesce((v_authorise_result->>'ok')::boolean,false)=false then
      v_auto_blocked:=true;
      v_auto_blockers:=v_auto_blockers||jsonb_build_array(coalesce(v_authorise_result->>'error_code','AUTHORISE_NOT_ADVANCED'));
    end if;
  end if;

  select financials.id into v_canonical_financials_id
  from public.timesheets_financials financials
  where financials.timesheet_id=coalesce(v_hours_timesheet_id,v_target_timesheet_id)
    and financials.is_current=true
  order by financials.computed_at_utc desc nulls last,financials.updated_at desc,financials.id desc
  limit 1;
  if not v_daily_receipt_only then
    if v_canonical_financials_id is null then
      raise exception 'CANDIDATE_CANONICAL_FINANCIALS_NOT_FOUND' using errcode='55000';
    end if;
    v_canonical_financial_sha256:=private._candidate_financial_content_sha256_v1(
      v_canonical_financials_id
    );
  end if;
  -- A received factual claim retains its artifact generation. It is not
  -- financial finalisation and never carries a fabricated financial hash.
  v_completion_generation:=v_workflow.generation+case when v_daily_receipt_only then 0 else 1 end;

  v_response:=jsonb_build_object(
    'ok',true,'idempotent_replay',false,'workflow_id',v_workflow.id,
    'state',v_completion_state,'generation',v_completion_generation,
    'office_resolution_pending',v_daily_receipt_only,
    'timesheet_id',v_target_timesheet_id,'canonical_result',v_result,
    'candidate_auto_authorise_effective',v_auto_requested,
    'auto_authorised',v_auto_requested and not v_auto_blocked,
    'canonical_financial_sha256_hex',encode(v_canonical_financial_sha256,'hex'),
    'auto_authorise_blockers',v_auto_blockers,
    'authorise_result',v_authorise_result
  );
  update public.candidate_submission_workflows set state=v_completion_state,generation=v_completion_generation,
    target_timesheet_id=v_target_timesheet_id,policy_snapshot_json=v_current_policy,
    canonical_financial_sha256=v_canonical_financial_sha256,
    issue_codes=v_workflow.issue_codes,
    finalised_at_utc=case when v_daily_receipt_only then null else p_now_utc end,
    last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,updated_at_utc=p_now_utc
  where id=v_workflow.id;
  perform private._candidate_notification_insert_v1(v_workflow.account_id,v_candidate_id,v_workflow.id,v_target_timesheet_id,
    case when v_auto_requested and not v_auto_blocked then 'AUTHORISED' else 'SUBMISSION_RECEIVED' end,
    'authorisation','candidate-submission-finalised-v1',jsonb_build_object('auto_authorised',v_auto_requested and not v_auto_blocked),
    jsonb_build_object('type','timesheet','timesheet_id',v_target_timesheet_id),
    'CANDIDATE_FINALISED_V1:'||v_workflow.id::text||':'||v_completion_generation::text,p_now_utc);
  perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,
    case when v_daily_receipt_only then 'CANDIDATE_SUBMISSION_RECEIVED' else 'CANDIDATE_SUBMISSION_FINALISED' end,
    jsonb_build_object('state',v_workflow.state,'generation',v_workflow.generation),
    jsonb_build_object('state',v_completion_state,'generation',v_completion_generation,'timesheet_id',v_target_timesheet_id,
      'auto_authorised',v_auto_requested and not v_auto_blocked),null,v_system_actor,p_idempotency_key,p_now_utc);
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,
    reason,correlation_id,ts_utc
  ) values (
    case when v_is_office_service then nullif(v_service_finalisation->>'actor_user_id','')::uuid
      else null end,
    'candidate_workflow_finalisation_completion',v_workflow.id::text,
    'CANDIDATE_WORKFLOW_FINALISATION_COMPLETED',jsonb_build_object(
      'contract_version','CANDIDATE_FINALISATION_COMPLETION_V1',
      'workflow_generation',p_expected_generation,
      'finalisation_identity_sha256',v_finalisation_identity_hash,
      'finalisation_identity',v_finalisation_identity
    ),v_response,'Canonical finalisation completion receipt',
    p_expected_generation::text||':'||v_finalisation_identity_hash,p_now_utc
  );
  perform private._candidate_workflow_mutation_receipt_v1(
    v_workflow.id,p_idempotency_key,v_mutation_request_hash,'RETRY_FINALISATION',
    case when v_is_office_service then 'OFFICE' when p_session_id is null then 'SERVICE' else 'CANDIDATE_CLIENT' end,
    case when v_is_office_service then v_service_finalisation->>'actor_user_id' else coalesce(p_session_id::text,'SERVICE') end,
    v_response,p_now_utc
  );
  return v_response;
exception
  when unique_violation then
    get stacked diagnostics v_constraint_name=constraint_name;
    if v_constraint_name='timesheet_evidence_one_active_timesheet_uq' then
      raise exception 'TIMESHEET_EVIDENCE_ALREADY_ATTACHED' using errcode='23505';
    end if;
    raise;
end;
$function$;
alter function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) owner to postgres;
revoke all on function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) from public,anon,authenticated;
grant execute on function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) to service_role;


create or replace function public.candidate_app_timesheet_detail_v1(
  p_session_id uuid,
  p_environment text,
  p_timesheet_id uuid default null,
  p_contract_week_id uuid default null,
  p_workflow_id uuid default null,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_context jsonb;
  v_daily boolean:=false;
  v_daily_projection jsonb;
  v_candidate_id uuid;
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_client public.clients%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_capabilities jsonb;
  v_evidence jsonb;
  v_components jsonb;
  v_claims jsonb;
  v_document_state jsonb:='{}'::jsonb;
  v_effective_week_ending_weekday integer;
  v_effective_current_week_ending_date date;
  v_tab_bucket text;
  v_candidate_status_code text;
  v_active_workflow_state text;
  v_rejected_workflow jsonb;
  v_primary_action jsonb;
  v_action_contract jsonb;
  v_detail_source_timesheet_id uuid;
  v_effective_pay_status_code text:='UNPAID';
  v_effective_paid_at_utc timestamptz;
  v_is_expense_only boolean:=false;
  v_expense_route text;
  v_expense_route_kind text:='UNKNOWN';
  v_display_route_label text;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_reads');
  if num_nonnulls(p_timesheet_id,p_contract_week_id,p_workflow_id)<>1 then
    raise exception 'CANDIDATE_DETAIL_IDENTITY_INVALID' using errcode='22023';
  end if;
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,null,p_now_utc,false);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;

  if p_workflow_id is not null then
    select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id and candidate_id=v_candidate_id;
    if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
    v_detail_source_timesheet_id:=case
      when v_workflow.workflow_kind='CONTRACT_EXPENSE'
        or v_workflow.rejection_scope='COMPLETE_EXPENSE_CLAIM'
        then coalesce(v_workflow.anchor_timesheet_id,v_workflow.target_timesheet_id)
      else coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id) end;
    if v_detail_source_timesheet_id is not null then
      select current_version.timesheet_id into p_timesheet_id
      from public.timesheets source_version
      join public.timesheets current_version on current_version.is_current=true
        and current_version.archived_at_utc is null
        and (
          (nullif(btrim(coalesce(source_version.booking_id,'')),'') is not null
            and current_version.booking_id=source_version.booking_id
            and current_version.contract_id is not distinct from source_version.contract_id
            and current_version.week_ending_date is not distinct from source_version.week_ending_date)
          or (nullif(btrim(coalesce(source_version.booking_id,'')),'') is null
            and current_version.timesheet_id=source_version.timesheet_id)
        )
      where source_version.timesheet_id=v_detail_source_timesheet_id
        and upper(coalesce(current_version.line_type::text,'')) not in ('EXPENSES','MILEAGE')
      order by current_version.version desc,current_version.timesheet_id
      limit 1;
    end if;
    if p_timesheet_id is not null then
      select week_row.id into p_contract_week_id
      from public.contract_weeks week_row
      where week_row.timesheet_id=p_timesheet_id
        and week_row.contract_id=v_workflow.contract_id
        and week_row.week_ending_date=v_workflow.week_ending_date
      order by week_row.updated_at desc,week_row.id desc limit 1;
    end if;
    if p_contract_week_id is null then p_contract_week_id:=v_workflow.contract_week_id; end if;
  end if;
  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets
      where timesheet_id=p_timesheet_id and is_current and archived_at_utc is null;
    v_daily:=found and v_timesheet.sheet_scope='DAILY';
  end if;
  if v_daily then
    if p_contract_week_id is not null then
      raise exception 'CANDIDATE_DETAIL_IDENTITY_INVALID' using errcode='22023';
    end if;
    v_daily_projection:=private._candidate_daily_read_projection_v1(
      p_environment,v_candidate_id,p_timesheet_id,p_now_utc);
    v_capabilities:=v_daily_projection->'capabilities';
    -- A local date value only: no Contract Week row is read, fabricated or written.
    v_week.week_ending_date:=v_timesheet.week_ending_date;
    v_effective_week_ending_weekday:=0;
    v_effective_current_week_ending_date:=(p_now_utc at time zone 'Europe/London')::date
      +mod(7-extract(dow from (p_now_utc at time zone 'Europe/London')::date)::integer,7);
    select * into v_fin from public.timesheets_financials
      where timesheet_id=p_timesheet_id and is_current
      order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  else
  if p_contract_week_id is not null then
    select * into v_week from public.contract_weeks where id=p_contract_week_id;
  else
    select * into v_week from public.contract_weeks where timesheet_id=p_timesheet_id order by updated_at desc,id desc limit 1;
  end if;
  if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id and candidate_id=v_candidate_id;
  if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_client from public.clients where id=v_contract.client_id;
  select coalesce(
    v_contract.week_ending_weekday_snapshot,
    (
      select settings.week_ending_weekday
      from public.client_settings settings
      where settings.client_id=v_contract.client_id
        and settings.effective_from<=(p_now_utc at time zone 'Europe/London')::date
      order by settings.effective_from desc,settings.updated_at desc nulls last,settings.id desc
      limit 1
    ),0
  ) into v_effective_week_ending_weekday;
  v_effective_current_week_ending_date:=(
    (p_now_utc at time zone 'Europe/London')::date
    +mod(
      v_effective_week_ending_weekday
      -extract(dow from (p_now_utc at time zone 'Europe/London')::date)::integer+7,7
    )
  )::date;
  if p_timesheet_id is null then p_timesheet_id:=v_week.timesheet_id; end if;
  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id and is_current=true and archived_at_utc is null;
    if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
    select * into v_fin from public.timesheets_financials where timesheet_id=p_timesheet_id and is_current=true
    order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  end if;
  v_capabilities:=private._candidate_record_capabilities_v1(p_timesheet_id,v_week.id,'{}'::jsonb);
  if p_workflow_id is not null and v_workflow.workflow_kind='CONTRACT_EXPENSE'
     and v_workflow.state in ('DRAFT','WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',
       'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL','AWAITING_PAPER_RETURN','REFUSED') then
    v_capabilities:=jsonb_set(v_capabilities,'{policy}',
      private._expense_approval_policy_v1(v_contract.client_id,v_contract.id,
        v_workflow.week_ending_date,v_workflow.workflow_kind));
  end if;
  end if;

  if p_timesheet_id is not null then
    select
      coalesce(
        case when coalesce(summary_pay_cache.summary_state_applies,false)
          then summary_pay_cache.summary_pay_status_code end,
        pay_state.summary_pay_status_code,
        case when pay_state.last_settled_at_utc is not null
            or v_fin.paid_at_utc is not null
          then 'PAID' else 'UNPAID' end
      )::text,
      case
        when coalesce(summary_pay_cache.summary_state_applies,false)
          then summary_pay_cache.last_paid_at_utc
        when pay_state.summary_pay_status_code is not null
          or pay_state.summary_pay_icon_code is not null
          then pay_state.summary_pay_paid_at_utc
        else coalesce(pay_state.last_settled_at_utc,v_fin.paid_at_utc)
      end
    into v_effective_pay_status_code,v_effective_paid_at_utc
    from (select 1) seed(seed_id)
    left join public.timesheet_summary_pay_state_cache summary_pay_cache
      on summary_pay_cache.timesheet_id=p_timesheet_id
    left join public.timesheet_pay_state pay_state
      on pay_state.timesheet_id=p_timesheet_id;
    if upper(coalesce(v_effective_pay_status_code,'UNPAID'))<>'PAID' then
      v_effective_paid_at_utc:=null;
    end if;
  end if;

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',e.id,'kind',e.kind,'document_role',e.document_role,'display_name',e.display_name,
    'processing_state',e.processing_state,'created_at',e.created_at
  ) order by e.created_at,e.id),'[]'::jsonb)
  into v_evidence from public.timesheet_evidence e
  where e.timesheet_id=p_timesheet_id and e.processing_state<>'SUPERSEDED';

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',c.id,'workflow_id',c.workflow_id,'workflow_generation',c.workflow_generation,
    'component_kind',c.component_kind,
    'expense_category',c.expense_category,'document_role',c.document_role,'state',c.state,
    'media_type',c.media_type,'byte_size',c.byte_size,'created_at_utc',c.created_at_utc,
    'required',c.required,'review_ordinal',c.review_ordinal,
    'review_document_ready',c.review_render_state='READY',
    'review_document_generation',c.workflow_generation,
    'review_page_count',c.review_page_count,
    'review_content_sha256',case when c.review_content_sha256 is null then null else encode(c.review_content_sha256,'hex') end,
    'final_signed_document_ready',c.final_signed_render_state='READY',
    'final_signed_page_count',c.final_signed_page_count
  ) order by c.created_at_utc,c.component_no),'[]'::jsonb)
  into v_components from public.candidate_submission_components c
  join public.candidate_submission_workflows w on w.id=c.workflow_id
  where w.candidate_id=v_candidate_id
    and private._candidate_workflow_maps_to_card_v1(w.id,p_timesheet_id,v_week.id)
    and c.state<>'SUPERSEDED';

  select coalesce(jsonb_agg(jsonb_build_object(
    'workflow_id',w.id,'workflow_kind',w.workflow_kind,'state',w.state,'generation',w.generation,
    'detail_action_owner',w.id=p_workflow_id,
    'claim_family',rejection_policy.claim_family,
    'route',w.route,'target_timesheet_id',w.target_timesheet_id,
    'anchor_timesheet_id',w.anchor_timesheet_id,'issue_codes',w.issue_codes,
    'rejection_reason',w.rejection_reason,'rejection_scope',w.rejection_scope,
    'required_resubmission_action',case
      when w.state<>'REJECTED' or not rejection_policy.rejection_actionable then null
      when w.workflow_kind='CONTRACT_EXPENSE' or w.rejection_scope='COMPLETE_EXPENSE_CLAIM'
        then 'RESUBMIT_EXPENSE_CLAIM'
      when w.workflow_kind='CONTRACT_COMBINED' then 'RESUBMIT_TIMESHEET_AND_EXPENSES'
      else 'RESUBMIT_TIMESHEET' end,
    'rejection_actionable',rejection_policy.rejection_actionable,
    'review_document_ready',exists(select 1 from public.candidate_submission_components review_component
      where review_component.workflow_id=w.id and review_component.workflow_generation=case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end
        and review_component.required=true and review_component.state<>'SUPERSEDED')
      and not exists(select 1 from public.candidate_submission_components review_component
      where review_component.workflow_id=w.id and review_component.workflow_generation=case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end
        and review_component.required=true and review_component.state<>'SUPERSEDED'
        and review_component.review_render_state<>'READY'),
    'review_document_generation',case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end,
    'review_page_count',(select sum(coalesce(review_component.review_page_count,0))
      from public.candidate_submission_components review_component
      where review_component.workflow_id=w.id and review_component.workflow_generation=case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end
        and review_component.required=true and review_component.state<>'SUPERSEDED'),
    'manager_approval_state',coalesce((select approval.state
      from public.candidate_approval_requests approval where approval.workflow_id=w.id
        and approval.workflow_generation=case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end
      order by approval.request_generation desc,approval.created_at_utc desc limit 1),w.state),
    'final_signed_document_ready',exists(select 1 from public.candidate_submission_components final_component
      where final_component.workflow_id=w.id and final_component.workflow_generation=case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end
        and final_component.required=true and final_component.state<>'SUPERSEDED')
      and not exists(select 1 from public.candidate_submission_components final_component
      where final_component.workflow_id=w.id and final_component.workflow_generation=case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end
        and final_component.required=true and final_component.state<>'SUPERSEDED'
        and final_component.final_signed_render_state<>'READY'),
    'updated_at_utc',w.updated_at_utc
  ) order by w.updated_at_utc desc,w.id desc),'[]'::jsonb)
  into v_claims from public.candidate_submission_workflows w
  cross join lateral (
    select
      case when w.workflow_kind='CONTRACT_EXPENSE'
          or w.rejection_scope='COMPLETE_EXPENSE_CLAIM'
        then 'EXPENSES' else 'HOURS' end as claim_family,
      case when w.state<>'REJECTED' then false
        else not private._candidate_rejection_replaced_v1(w.id)
      end as rejection_actionable
  ) rejection_policy
  where w.candidate_id=v_candidate_id and (v_daily or w.contract_id=v_contract.id)
    and (v_daily or w.week_ending_date=v_week.week_ending_date)
    and w.state not in ('CANCELLED','SUPERSEDED')
    and private._candidate_workflow_maps_to_card_v1(w.id,p_timesheet_id,v_week.id);

  select jsonb_build_object(
    'workflow_id',document_workflow.id,
    'workflow_generation',document_workflow.generation,
    'review_document_ready',coalesce(document_readiness.review_ready,false),
    'review_document_component_id',hours_component.id,
    'review_document_generation',hours_component.workflow_generation,
    'review_page_count',hours_component.review_page_count,
    'manager_approval_state',coalesce(latest_approval.state,document_workflow.state),
    'final_signed_document_ready',coalesce(document_readiness.final_ready,false)
  ) into v_document_state
  from public.candidate_submission_workflows document_workflow
  left join lateral (
    select component.* from public.candidate_submission_components component
    where component.workflow_id=document_workflow.id
      and component.workflow_generation=case when document_workflow.state='FINALISED' then greatest(document_workflow.generation-1,1) else document_workflow.generation end
      and component.component_kind='HOURS_TIMESHEET' and component.state<>'SUPERSEDED'
    order by component.review_ordinal,component.id limit 1
  ) hours_component on true
  left join lateral (
    select
      count(*)>0 and bool_and(component.review_render_state='READY') as review_ready,
      count(*)>0 and bool_and(component.final_signed_render_state='READY') as final_ready
    from public.candidate_submission_components component
    where component.workflow_id=document_workflow.id
      and component.workflow_generation=case when document_workflow.state='FINALISED' then greatest(document_workflow.generation-1,1) else document_workflow.generation end
      and component.required=true and component.state<>'SUPERSEDED'
  ) document_readiness on true
  left join lateral (
    select approval.* from public.candidate_approval_requests approval
    where approval.workflow_id=document_workflow.id
      and approval.workflow_generation=case when document_workflow.state='FINALISED' then greatest(document_workflow.generation-1,1) else document_workflow.generation end
    order by approval.request_generation desc,approval.created_at_utc desc limit 1
  ) latest_approval on true
  where document_workflow.candidate_id=v_candidate_id
    and private._candidate_workflow_maps_to_card_v1(
      document_workflow.id,p_timesheet_id,v_week.id
    )
    and document_workflow.state not in ('CANCELLED','SUPERSEDED')
  order by (document_workflow.id=p_workflow_id) desc,document_workflow.updated_at_utc desc
  limit 1;

  select workflow_item->>'state' into v_active_workflow_state
  from jsonb_array_elements(coalesce(v_claims,'[]'::jsonb)) workflow_item
  where workflow_item->>'state' in (
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
  )
  order by coalesce((workflow_item->>'detail_action_owner')::boolean,false) desc,
    workflow_item->>'updated_at_utc' desc,workflow_item->>'workflow_id'
  limit 1;
  select workflow_item into v_rejected_workflow
  from jsonb_array_elements(coalesce(v_claims,'[]'::jsonb)) workflow_item
  where workflow_item->>'state'='REJECTED'
    and coalesce((workflow_item->>'rejection_actionable')::boolean,false)
  order by coalesce((workflow_item->>'detail_action_owner')::boolean,false) desc,
    workflow_item->>'updated_at_utc' desc,workflow_item->>'workflow_id'
  limit 1;
  v_candidate_status_code:=private._candidate_status_code_v1(
    v_effective_paid_at_utc is not null,
    v_fin.authorised_at_utc is not null or (v_daily and v_timesheet.authorised_at_server is not null),
    v_fin.locked_by_invoice_id is not null
      or upper(coalesce(v_timesheet.status::text,''))='INVOICED',
    v_active_workflow_state,v_rejected_workflow is not null,
    v_fin.processing_status::text,v_week.status::text
  );
  v_tab_bucket:=case
    when v_week.week_ending_date>v_effective_current_week_ending_date then 'EXCLUDED'
    when v_effective_paid_at_utc is null or v_effective_paid_at_utc>p_now_utc then 'CURRENT'
    when v_effective_paid_at_utc>=p_now_utc-interval '7 days' then 'CURRENT'
    when v_effective_paid_at_utc<p_now_utc-interval '7 days'
      and v_week.week_ending_date between v_effective_current_week_ending_date-105
        and v_effective_current_week_ending_date then 'HISTORY'
    else 'EXCLUDED' end;
  v_primary_action:=private._candidate_timesheet_primary_action_v1(
    v_candidate_status_code,v_claims,v_capabilities,p_timesheet_id,v_week.id
  );
  v_action_contract:=private._candidate_timesheet_action_contract_v1(
    v_candidate_status_code,v_claims,v_capabilities,p_timesheet_id,v_week.id,p_now_utc
  );
  v_primary_action:=v_action_contract->'primary_action';
  if v_daily and v_active_workflow_state='RECEIVED' then
    -- The complete receipt is waiting for Office, not another Candidate submit.
    -- Keep the separately guarded cancellation action until Office authorises.
    v_primary_action:=null;
    v_action_contract:=v_action_contract||jsonb_build_object('primary_action',null,
      'available_actions',(select coalesce(jsonb_agg(a),'[]'::jsonb)
        from jsonb_array_elements(v_action_contract->'available_actions') a
        where a->>'code'<>'CONTINUE_TIMESHEET'));
  end if;
  if v_active_workflow_state='AWAITING_PAPER_RETURN' then
    v_candidate_status_code:=case v_action_contract->'paper_pack'->>'state'
      when 'READY' then 'AWAITING_SIGNED_DOCUMENTS'
      when 'FAILED' then 'DOCUMENT_PREPARATION_FAILED'
      when 'RETIRED' then 'PAPER_DELIVERY_RETIRED'
      when 'STALE' then 'PAPER_DELIVERY_STALE'
      else 'PREPARING_DOCUMENTS' end;
  end if;

  v_is_expense_only:=
    upper(coalesce(v_timesheet.line_type::text,'')) in ('EXPENSES','MILEAGE')
    and v_fin.total_hours is not null
    and v_fin.total_hours=0::numeric
    and coalesce(v_timesheet.actual_schedule_json,'[]'::jsonb) in ('[]'::jsonb,'{}'::jsonb,'null'::jsonb)
    and coalesce(v_fin.actual_schedule_json,'[]'::jsonb) in ('[]'::jsonb,'{}'::jsonb,'null'::jsonb)
    and not jsonb_path_exists(coalesce(v_timesheet.additional_units_week,'{}'::jsonb),
      'lax $.** ? (@.type() == "number" && @ != 0)')
    and not jsonb_path_exists(coalesce(v_timesheet.additional_units_per_day,'{}'::jsonb),
      'lax $.** ? (@.type() == "number" && @ != 0)')
    and not jsonb_path_exists(coalesce(v_fin.additional_units_json,'{}'::jsonb),
      'lax $.** ? (@.type() == "number" && @ != 0)')
    and v_timesheet.worked_start_iso is null
    and v_timesheet.worked_end_iso is null
    and (
      abs(coalesce(v_fin.expenses_pay_ex_vat,0::numeric))
      +abs(coalesce(v_fin.expenses_charge_ex_vat,0::numeric))
      +abs(coalesce(v_fin.mileage_units,0::numeric))
      +abs(coalesce(v_fin.mileage_pay_ex_vat,0::numeric))
      +abs(coalesce(v_fin.mileage_charge_ex_vat,0::numeric))
      +abs(coalesce(v_fin.travel_pay_ex_vat,0::numeric))
      +abs(coalesce(v_fin.travel_charge_ex_vat,0::numeric))
      +abs(coalesce(v_fin.accommodation_pay_ex_vat,0::numeric))
      +abs(coalesce(v_fin.accommodation_charge_ex_vat,0::numeric))
      +abs(coalesce(v_fin.other_pay_ex_vat,0::numeric))
      +abs(coalesce(v_fin.other_charge_ex_vat,0::numeric))
    )>0::numeric;
  if v_is_expense_only then
    select upper(nullif(btrim(workflow_item->>'route'),'')) into v_expense_route
    from jsonb_array_elements(coalesce(v_claims,'[]'::jsonb)) workflow_item
    where workflow_item->>'workflow_id'=v_timesheet.candidate_workflow_id::text
       or workflow_item->>'target_timesheet_id'=v_timesheet.timesheet_id::text
    order by (workflow_item->>'workflow_id'=v_timesheet.candidate_workflow_id::text) desc,
      workflow_item->>'updated_at_utc' desc,workflow_item->>'workflow_id'
    limit 1;
    v_expense_route_kind:=case
      when v_expense_route='PAPER' then 'QR'
      when v_expense_route in ('PHONE','EMAIL','ELECTRONIC') then 'ELECTRONIC'
      when v_timesheet.submission_mode='MANUAL'::public.submission_mode_enum
        and upper(coalesce(v_timesheet.status::text,''))='SUBMITTED'
        and v_timesheet.candidate_workflow_id is null then 'MANUAL'
      else 'UNKNOWN' end;
    v_display_route_label:=case v_expense_route_kind
      when 'QR' then 'QR Expense'
      when 'ELECTRONIC' then 'Electronic Expense'
      when 'MANUAL' then 'Manual Expense'
      else 'Expense' end;
  end if;

  return jsonb_build_object(
    'ok',true,
    'is_expense_only',v_is_expense_only,
    'expense_route_kind',v_expense_route_kind,
    'display_route_label',v_display_route_label,
    'week_ending_label',private._candidate_week_ending_label_v1(v_week.week_ending_date),
    'candidate_status_code',v_candidate_status_code,
    'list_membership',jsonb_build_object(
      'tab_bucket',v_tab_bucket,
      'effective_current_week_ending_date',v_effective_current_week_ending_date,
      'paid_current_cutoff_utc',p_now_utc-interval '7 days'
    ),
    'primary_action',v_primary_action,
    'available_actions',coalesce(v_action_contract->'available_actions','[]'::jsonb),
    'manager_approval',v_action_contract->'manager_approval',
    'paper_pack',v_action_contract->'paper_pack',
    'daily_shift',case when v_daily then v_daily_projection->'daily_shift' else null end,
    'contract_week',case when v_daily then null else jsonb_build_object(
      'id',v_week.id,'contract_id',v_contract.id,'week_ending_date',v_week.week_ending_date,
      'week_ending_weekday',btrim(to_char(v_week.week_ending_date,'FMDay')),
      'client_name',v_client.name,'job_title',v_contract.role,'band',v_contract.band,
      'additional_seq',v_week.additional_seq,'status',v_week.status,'planned_schedule_json',v_week.planned_schedule_json
    ) end,
    'timesheet',case when v_timesheet.timesheet_id is null then null else jsonb_build_object(
      'id',v_timesheet.timesheet_id,'status',v_timesheet.status,'submission_mode',v_timesheet.submission_mode,
      'sheet_scope',v_timesheet.sheet_scope,'actual_schedule_json',v_timesheet.actual_schedule_json,
      'additional_units_week',v_timesheet.additional_units_week,'qr_status',v_timesheet.qr_status,
      'canonical_work_date',case when v_timesheet.sheet_scope='DAILY' then private._candidate_daily_work_date_v1(
        v_timesheet.worked_start_iso,v_timesheet.scheduled_start_iso,v_timesheet.week_ending_date) else null end
    ) end,
    'hours',jsonb_build_object('total_hours',case when v_daily then (v_daily_projection->>'hours')::numeric else coalesce(v_fin.total_hours,0) end,
      'actual_schedule_json',coalesce(v_fin.actual_schedule_json,v_timesheet.actual_schedule_json)),
    -- Daily Timesheets are hours-only.  The current detail authority must
    -- preserve that rule even when an old financial row contains expense-like
    -- values from legacy data.
    'expenses',case when v_daily then jsonb_build_object(
      'expenses_pay_ex_vat',0,'expenses_description',null,
      'mileage_units',0,'mileage_pay_ex_vat',0,
      'travel_pay_ex_vat',0,
      'accommodation_pay_ex_vat',0,
      'other_pay_ex_vat',0
    ) else jsonb_build_object(
      'expenses_pay_ex_vat',coalesce(v_fin.expenses_pay_ex_vat,0),'expenses_description',v_fin.expenses_description,
      'mileage_units',coalesce(v_fin.mileage_units,0),'mileage_pay_ex_vat',coalesce(v_fin.mileage_pay_ex_vat,0),
      'travel_pay_ex_vat',coalesce(v_fin.travel_pay_ex_vat,0),
      'accommodation_pay_ex_vat',coalesce(v_fin.accommodation_pay_ex_vat,0),
      'other_pay_ex_vat',coalesce(v_fin.other_pay_ex_vat,0)
    ) end,
    'lifecycle',jsonb_build_object(
      'processing_status',v_fin.processing_status,'authorised_at_utc',coalesce(v_fin.authorised_at_utc,case when v_daily then v_timesheet.authorised_at_server else null end),
      'paid_at_utc',v_effective_paid_at_utc,'invoice_locked',v_fin.locked_by_invoice_id is not null
    ),
    'capabilities',v_capabilities,
    'manager_review',coalesce(v_document_state,'{}'::jsonb),
    'evidence',v_evidence,
    'components',v_components,
    'workflows',v_claims,
    'rejections',(
      select coalesce(jsonb_agg(workflow_item order by
        workflow_item->>'updated_at_utc' desc,workflow_item->>'workflow_id'),'[]'::jsonb)
      from jsonb_array_elements(v_claims) workflow_item
      where workflow_item->>'state'='REJECTED'
        and coalesce((workflow_item->>'rejection_actionable')::boolean,false)
    )
  );
end;
$function$;
alter function public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz) owner to postgres;
revoke all on function public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz) to service_role;



notify pgrst, 'reload schema';

commit;
