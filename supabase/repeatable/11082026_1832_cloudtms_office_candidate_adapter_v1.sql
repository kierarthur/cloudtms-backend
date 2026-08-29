-- CloudTMS office-only Candidate adapter.
--
-- This adapter is not part of the public Candidate client RPC surface.  It is
-- executable only by service_role behind the authenticated normal CloudTMS
-- Worker.  It owns bounded projection snapshots, closed rejection previews
-- and confirmations, typed workflow-action idempotency, and the Summary
-- manager-reminder batch receipt.  It does not calculate financial truth.

create or replace function private._candidate_office_capabilities_v1(
  p_environment text,
  p_actor_user_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_environment text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_actor_user_id is null then
    raise exception 'OFFICE_AUTH_REQUIRED' using errcode='28000';
  end if;
  return jsonb_build_object(
    'ok',true,
    'contract_version','CLOUDTMS_OFFICE_CANDIDATE_API_V1',
    'capabilities_version','OFFICE_CANDIDATE_CAPABILITIES_V1',
    'office_contract_version','CLOUDTMS_OFFICE_CANDIDATE_API_V1',
    'projection_version','OFFICE_CANDIDATE_TIMESHEET_V1',
    'typed_action_version',1,
    'batch_version','OFFICE_CANDIDATE_REMINDER_BATCH_V1',
    'environment',v_environment,
    'authority_applies',true,
    'mode','ENABLED',
    'required_office_role','admin',
    'permission_source','OFFICE_ADMIN_ROLE_V1',
    'observed_at_utc',coalesce(p_now_utc,now()),
    'surfaces',jsonb_build_object(
      'simple_timesheet',true,
      'timesheet_summary',true,
      'bulk_process',true,
      'bulk_authorise',true,
      'invoice_generator',true,
      'invoice_issuer',true
    ),
    'permissions',jsonb_build_object(
      'view_candidate_state',true,
      'change_route',true,
      'reject_submission',true,
      'resubmit_rejected',true,
      'send_manager_reminder',true,
      'send_manager_reminder_batch',true,
      'renew_manager_request',true,
      'cancel_manager_request',true,
      'manage_phone_approval',true,
      'manage_paper',true,
      'retry_finalisation',true,
      'mark_no_work',true
    )
  );
end;
$function$;

create or replace function private._candidate_office_action_v1(
  p_code text,
  p_label text,
  p_group text,
  p_enabled boolean,
  p_disabled_reason_code text,
  p_disabled_reason text,
  p_requires_confirmation boolean,
  p_requires_reason boolean,
  p_method text,
  p_path text,
  p_fixed_body jsonb default '{}'::jsonb,
  p_required_user_inputs jsonb default '[]'::jsonb,
  p_idempotency text default 'REQUIRED',
  p_prominent boolean default false
)
returns jsonb
language sql
immutable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
  select jsonb_build_object(
    'contract_version','OFFICE_CANDIDATE_ACTION_V1',
    'code',upper(btrim(coalesce(p_code,''))),
    'label',coalesce(p_label,''),
    'group',upper(btrim(coalesce(p_group,'GENERAL'))),
    'prominent',coalesce(p_prominent,false),
    'enabled',coalesce(p_enabled,false),
    'disabled_reason_code',case when coalesce(p_enabled,false) then null else p_disabled_reason_code end,
    'disabled_reason',case when coalesce(p_enabled,false) then null else p_disabled_reason end,
    'requires_confirmation',coalesce(p_requires_confirmation,false),
    'requires_reason',coalesce(p_requires_reason,false),
    'invocation',jsonb_build_object(
      'version',1,
      'kind',case when upper(btrim(coalesce(p_method,'')))='CLIENT'
        then 'CLIENT_DESTINATION' else 'HTTP' end,
      'method',case when upper(btrim(coalesce(p_method,'')))='CLIENT'
        then null else upper(btrim(coalesce(p_method,''))) end,
      'path',p_path,
      'fixed_body',coalesce(p_fixed_body,'{}'::jsonb),
      'required_user_inputs',coalesce(p_required_user_inputs,'[]'::jsonb),
      'idempotency',coalesce(p_idempotency,'REQUIRED')
    )
  );
$function$;

create or replace function private._candidate_office_reject_preview_v1(
  p_environment text,
  p_timesheet_id uuid,
  p_actor_user_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_environment text;
  v_timesheet public.timesheets%rowtype;
  v_week public.contract_weeks%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_capabilities jsonb;
  v_signature text;
  v_targets jsonb:='[]'::jsonb;
  v_context jsonb;
  v_context_sha text;
  v_permitted boolean:=false;
  v_disabled_code text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_actor_user_id is null or p_timesheet_id is null then
    raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
  end if;
  select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
  if not found then raise exception 'CANDIDATE_OFFICE_PROJECTION_NOT_FOUND' using errcode='P0002'; end if;
  if not v_timesheet.is_current or v_timesheet.archived_at_utc is not null then
    raise exception 'TIMESHEET_MOVED' using errcode='40001';
  end if;
  select * into v_week from public.contract_weeks where timesheet_id=v_timesheet.timesheet_id
  order by updated_at desc,id desc limit 1;
  if v_week.id is null and v_timesheet.sheet_scope<>'DAILY'::public.timesheet_scope_enum then
    raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
  end if;
  select * into v_fin from public.timesheets_financials
  where timesheet_id=v_timesheet.timesheet_id and is_current=true
  order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  v_signature:=coalesce(
    public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,v_week.id,false)->>'row_signature',
    public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,v_week.id,false)->>'backend_row_signature'
  );
  v_capabilities:=private._candidate_record_capabilities_v1(v_timesheet.timesheet_id,v_week.id,'{}'::jsonb);
  select coalesce(jsonb_agg(jsonb_build_object(
    'workflow_id',w.id,
    'workflow_generation',w.generation,
    'workflow_kind',w.workflow_kind,
    'route',w.route,
    'state',w.state,
    'scope',case when w.workflow_kind='CONTRACT_EXPENSE' then 'EXPENSE'
      when w.workflow_kind='CONTRACT_COMBINED' then 'COMBINED' else 'HOURS' end
  ) order by w.id),'[]'::jsonb)
  into v_targets
  from public.candidate_submission_workflows w
  where w.environment=v_environment and (
    (w.state not in ('FINALISED','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED')
      and (w.target_timesheet_id=v_timesheet.timesheet_id or w.anchor_timesheet_id=v_timesheet.timesheet_id))
    or (w.state='FINALISED' and w.target_timesheet_id=v_timesheet.timesheet_id)
  );
  v_permitted:=coalesce((v_capabilities->>'can_reject_candidate_submission')::boolean,false)
    and coalesce(v_fin.authorised_at_utc,v_timesheet.authorised_at_server) is null
    and v_fin.paid_at_utc is null and v_fin.locked_by_invoice_id is null
    and jsonb_array_length(v_targets)>0;
  v_disabled_code:=case
    when coalesce(v_fin.authorised_at_utc,v_timesheet.authorised_at_server) is not null then 'CANDIDATE_REQUIRES_UNAUTHORISE'
    when v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null then 'CANDIDATE_PROTECTED_FINANCIAL_HISTORY'
    when not coalesce((v_capabilities->>'can_reject_candidate_submission')::boolean,false) then 'CANDIDATE_ACTION_NOT_ELIGIBLE'
    when jsonb_array_length(v_targets)=0 then 'CANDIDATE_REJECTION_SCOPE_CONFLICT'
    else null end;
  v_context:=jsonb_build_object(
    'contract_version','OFFICE_CANDIDATE_REJECTION_PREVIEW_V1',
    'environment',v_environment,
    'timesheet_id',v_timesheet.timesheet_id,
    'timesheet_version',v_timesheet.version,
    'contract_week_id',v_week.id,
    'row_signature',v_signature,
    'reject_scope',v_capabilities->>'reject_scope',
    'target_workflows',v_targets,
    'permitted',v_permitted,
    'requires_unauthorise',coalesce(v_fin.authorised_at_utc,v_timesheet.authorised_at_server) is not null,
    'protected_financial_history',v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null
  );
  v_context_sha:=encode(extensions.digest(convert_to(v_context::text,'UTF8'),'sha256'),'hex');
  return v_context||jsonb_build_object(
    'ok',true,
    'observed_at_utc',coalesce(p_now_utc,now()),
    'expected_timesheet_id',v_timesheet.timesheet_id,
    'expected_row_signature',v_signature,
    'scope',case v_capabilities->>'reject_scope'
      when 'COMPLETE_EXPENSE_CLAIM' then 'EXPENSE'
      else case when exists(select 1 from jsonb_array_elements(v_targets) x where x->>'workflow_kind'='CONTRACT_COMBINED') then 'COMBINED' else 'HOURS' end end,
    'target_workflow_id',case when jsonb_array_length(v_targets)=1 then (v_targets->0)->>'workflow_id' else null end,
    'target_workflow_generation',case when jsonb_array_length(v_targets)=1 then ((v_targets->0)->>'workflow_generation')::integer else null end,
    'requires_reason',true,
    'disabled_reason_code',v_disabled_code,
    'disabled_reason',case v_disabled_code
      when 'CANDIDATE_REQUIRES_UNAUTHORISE' then 'Unauthorise this timesheet before rejecting the Candidate submission.'
      when 'CANDIDATE_PROTECTED_FINANCIAL_HISTORY' then 'This submission can no longer be rejected because protected financial history exists.'
      when 'CANDIDATE_REJECTION_SCOPE_CONFLICT' then 'CloudTMS could not establish one safe Candidate rejection scope.'
      when 'CANDIDATE_ACTION_NOT_ELIGIBLE' then 'This Candidate submission is not currently eligible for rejection.'
      else null end,
    'context_sha256',v_context_sha,
    'affected_rows',jsonb_build_array(jsonb_build_object(
      'timesheet_id',v_timesheet.timesheet_id,'contract_week_id',v_week.id
    ))
  );
end;
$function$;

create or replace function private._candidate_office_projection_identity_v1(
  p_timesheet_id uuid,
  p_contract_week_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_requested public.timesheets%rowtype;
  v_requested_current_id uuid;
  v_week public.contract_weeks%rowtype;
  v_week_timesheet public.timesheets%rowtype;
  v_current public.timesheets%rowtype;
  v_week_current_id uuid;
  v_current_id uuid;
  v_current_count integer:=0;
  v_week_count integer:=0;
  v_resolved_week_id uuid;
begin
  if p_timesheet_id is null and p_contract_week_id is null then
    raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
  end if;

  if p_timesheet_id is not null then
    select requested.* into v_requested
    from public.timesheets requested
    where requested.timesheet_id=p_timesheet_id;
    if not found then
      raise exception 'CANDIDATE_OFFICE_PROJECTION_NOT_FOUND' using errcode='P0002';
    end if;
    if v_requested.is_current and v_requested.archived_at_utc is null then
      v_requested_current_id:=v_requested.timesheet_id;
    elsif nullif(btrim(coalesce(v_requested.booking_id,'')),'') is not null then
      select count(distinct current_row.timesheet_id)::integer,
        min(current_row.timesheet_id::text)::uuid
      into v_current_count,v_requested_current_id
      from public.timesheets current_row
      where current_row.is_current=true
        and current_row.archived_at_utc is null
        and current_row.contract_id is not distinct from v_requested.contract_id
        and current_row.week_ending_date is not distinct from v_requested.week_ending_date
        and nullif(btrim(coalesce(current_row.booking_id,'')),'')
          =nullif(btrim(v_requested.booking_id),'');
      if v_current_count<>1 then
        raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
      end if;
    else
      raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
    end if;
  end if;

  if p_contract_week_id is not null then
    select week_row.* into v_week
    from public.contract_weeks week_row
    where week_row.id=p_contract_week_id;
    if not found or v_week.timesheet_id is null then
      raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
    end if;
    select week_timesheet.* into v_week_timesheet
    from public.timesheets week_timesheet
    where week_timesheet.timesheet_id=v_week.timesheet_id;
    if not found then
      raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
    end if;
    if v_week_timesheet.is_current and v_week_timesheet.archived_at_utc is null then
      v_week_current_id:=v_week_timesheet.timesheet_id;
    elsif nullif(btrim(coalesce(v_week_timesheet.booking_id,'')),'') is not null then
      select count(distinct current_row.timesheet_id)::integer,
        min(current_row.timesheet_id::text)::uuid
      into v_current_count,v_week_current_id
      from public.timesheets current_row
      where current_row.is_current=true
        and current_row.archived_at_utc is null
        and current_row.contract_id is not distinct from v_week_timesheet.contract_id
        and current_row.week_ending_date is not distinct from v_week_timesheet.week_ending_date
        and nullif(btrim(coalesce(current_row.booking_id,'')),'')
          =nullif(btrim(v_week_timesheet.booking_id),'');
      if v_current_count<>1 then
        raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
      end if;
    else
      raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
    end if;
  end if;

  if v_requested_current_id is not null and v_week_current_id is not null
     and v_requested_current_id is distinct from v_week_current_id then
    raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
  end if;
  v_current_id:=coalesce(v_requested_current_id,v_week_current_id);

  if p_contract_week_id is null then
    select current_row.* into v_current
    from public.timesheets current_row
    where current_row.timesheet_id=v_current_id;
    if not found then
      raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
    end if;
    if v_current.sheet_scope='DAILY'::public.timesheet_scope_enum then
      -- DAILY is owned by its current timesheet/booking family and has no
      -- contract_weeks row. The requested timesheet is the complete identity.
      v_resolved_week_id:=null;
    else
      select count(*)::integer,min(week_row.id::text)::uuid
      into v_week_count,v_resolved_week_id
      from public.contract_weeks week_row
      where week_row.timesheet_id=v_current_id;
      if v_week_count<>1 then
        raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
      end if;
      select week_row.* into v_week
      from public.contract_weeks week_row
      where week_row.id=v_resolved_week_id;
    end if;
  elsif v_week_current_id is distinct from v_current_id then
    raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
  end if;

  return jsonb_build_object(
    'requested_timesheet_id',p_timesheet_id,
    'current_timesheet_id',v_current_id,
    'contract_week_id',v_week.id,
    'additional_seq',v_week.additional_seq,
    'moved',p_timesheet_id is not null and p_timesheet_id is distinct from v_current_id
  );
end;
$function$;

create or replace function private._candidate_office_timesheet_projection_v1(
  p_environment text,
  p_timesheet_id uuid,
  p_contract_week_id uuid,
  p_row_key text,
  p_expected_row_signature text,
  p_actor_user_id uuid,
  p_observed_at_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_environment text;
  v_identity jsonb;
  v_week public.contract_weeks%rowtype;
  v_requested public.timesheets%rowtype;
  v_current public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_capabilities jsonb;
  v_signature text;
  v_manager_first_accepted timestamptz;
  v_manager_accepted timestamptz;
  v_manager_delivery_state text;
  v_manager_provider_status text;
  v_manager_pending integer:=0;
  v_manager_lease boolean:=false;
  v_reminder_eligible boolean:=false;
  v_renewal_eligible boolean:=false;
  v_cancel_eligible boolean:=false;
  v_resends_remaining integer:=0;
  v_actions jsonb:='[]'::jsonb;
  v_action jsonb;
  v_rejections jsonb:='[]'::jsonb;
  v_paper_state text:='NOT_APPLICABLE';
  v_paper_pack jsonb:='{}'::jsonb;
  v_paper_delivery_generation integer;
  v_paper_outbox public.mail_outbox%rowtype;
  v_candidate_status_code text;
  v_candidate_status_label text;
  v_candidate_status_tone text;
  v_reject_preview jsonb;
  v_primary_action jsonb;
  v_writes boolean:=true;
  v_manager_enabled boolean:=true;
  v_paper_enabled boolean:=true;
  v_route_enabled boolean:=true;
  v_expense_email_missing boolean:=false;
  v_active_workflow_state text;
  v_actionable_rejection boolean:=false;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_actor_user_id is null or (p_timesheet_id is null and p_contract_week_id is null) then
    raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
  end if;
  v_identity:=private._candidate_office_projection_identity_v1(
    p_timesheet_id,p_contract_week_id
  );
  if p_timesheet_id is not null then
    select * into v_requested from public.timesheets where timesheet_id=p_timesheet_id;
  end if;
  select * into v_week from public.contract_weeks
  where id=(v_identity->>'contract_week_id')::uuid;
  select * into v_current from public.timesheets
  where timesheet_id=(v_identity->>'current_timesheet_id')::uuid;
  select * into v_fin from public.timesheets_financials
  where timesheet_id=v_current.timesheet_id and is_current=true
  order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  -- Financial truth outranks Candidate workflow lifecycle on every surface.
  -- Protected records remain readable, but no Candidate mutation may be
  -- advertised once paid, authorised or invoice-locked.
  v_writes:=not (
    v_fin.paid_at_utc is not null
    or v_fin.authorised_at_utc is not null
    or v_current.authorised_at_server is not null
    or v_fin.locked_by_invoice_id is not null
    or upper(coalesce(v_current.status::text,''))='INVOICED'
  );
  v_manager_enabled:=v_writes;
  v_paper_enabled:=v_writes;
  v_route_enabled:=v_writes;
  v_capabilities:=private._candidate_record_capabilities_v1(v_current.timesheet_id,v_week.id,'{}'::jsonb);
  v_signature:=coalesce(
    public.timesheet_lifecycle_guard_signature_v1(v_current.timesheet_id,v_week.id,false)->>'row_signature',
    public.timesheet_lifecycle_guard_signature_v1(v_current.timesheet_id,v_week.id,false)->>'backend_row_signature'
  );

  select w.* into v_workflow
  from public.candidate_submission_workflows w
  where w.environment=v_environment and (
    w.target_timesheet_id=v_current.timesheet_id
    or w.anchor_timesheet_id=v_current.timesheet_id
    or w.contract_week_id=v_week.id
  )
  order by (w.state not in ('FINALISED','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED')) desc,
    w.updated_at_utc desc,w.id desc limit 1;
  if v_workflow.id is not null then
    select ar.* into v_approval from public.candidate_approval_requests ar
    where ar.workflow_id=v_workflow.id and ar.workflow_generation=v_workflow.generation
    order by ar.request_generation desc,ar.updated_at_utc desc,ar.id desc limit 1;
  end if;

  if v_approval.id is not null and v_approval.method='EMAIL' then
    select min(m.sent_at) filter (where m.status='SENT' and m.sent_at is not null
        and upper(coalesce(m.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK')),
      max(m.sent_at) filter (where m.status='SENT' and m.sent_at is not null
        and upper(coalesce(m.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK')),
      (array_agg(m.status::text order by m.created_at_utc desc,m.id desc))[1],
      (array_agg(m.provider_status order by m.created_at_utc desc,m.id desc))[1],
      count(*) filter (where m.status='QUEUED' and m.sent_at is null
        and lower(coalesce(m.payment_scope_json->>'candidate_manager_mail_retired','false')) in ('false','f','0','no'))::integer,
      bool_or(m.attempt_lease_token is not null and m.attempt_lease_expires_at_utc>p_observed_at_utc)
    into v_manager_first_accepted,v_manager_accepted,v_manager_delivery_state,
      v_manager_provider_status,v_manager_pending,v_manager_lease
    from public.mail_outbox m
    where upper(coalesce(m.payment_scope_json->>'candidate_mail_authority',''))='MANAGER_APPROVAL_V1'
      and m.payment_scope_json->>'candidate_manager_workflow_id'=v_workflow.id::text
      and m.payment_scope_json->>'candidate_manager_workflow_generation'=v_workflow.generation::text
      and m.payment_scope_json->>'candidate_approval_request_id'=v_approval.id::text
      and m.payment_scope_json->>'candidate_approval_request_generation'=v_approval.request_generation::text;
  end if;
  v_resends_remaining:=greatest(0,5-coalesce(v_approval.resend_count,0));
  v_reminder_eligible:=v_manager_enabled and v_approval.id is not null
    and v_approval.method='EMAIL' and v_approval.state='PENDING'
    and v_approval.expires_at_utc>p_observed_at_utc
    and v_manager_accepted is not null
    and v_manager_accepted+interval '24 hours'<=p_observed_at_utc
    and v_resends_remaining>0 and coalesce(v_manager_pending,0)=0 and not coalesce(v_manager_lease,false)
    and v_approval.review_manifest_sha256 is not distinct from v_workflow.review_manifest_sha256;
  v_renewal_eligible:=v_manager_enabled and v_approval.id is not null
    and v_approval.method='EMAIL' and (
      v_approval.state='EXPIRED'
      or (v_approval.state='PENDING' and v_approval.expires_at_utc<=p_observed_at_utc)
    ) and v_workflow.state='AWAITING_MANAGER_APPROVAL';
  v_cancel_eligible:=v_manager_enabled and v_approval.id is not null
    and v_approval.state='PENDING' and v_workflow.state='AWAITING_MANAGER_APPROVAL'
    and not coalesce(v_manager_lease,false);

  if v_workflow.route='PAPER' then
    v_paper_delivery_generation:=case
      when v_workflow.state='FINALISED' then greatest(v_workflow.generation-1,1)
      else v_workflow.generation
    end;
    select m.* into v_paper_outbox from public.mail_outbox m
    where m.type='TIMESHEET_QR'
      and m.context_kind='timesheets'
      and m.context_id=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
      and upper(coalesce(m.payment_scope_json->>'candidate_mail_authority',''))='CANDIDATE_PAPER_V1'
      and m.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and m.payment_scope_json->>'candidate_workflow_generation'=v_paper_delivery_generation::text
      and lower(coalesce(m.payment_scope_json->>'paper_return_manifest_sha256',''))
        =encode(v_workflow.paper_return_manifest_sha256,'hex')
    order by m.created_at_utc desc,m.id desc limit 1;
    if v_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED') then
      v_paper_pack:=private._candidate_paper_pack_readiness_v1(
        v_workflow.id,v_workflow.generation
      );
      v_paper_state:=coalesce(v_paper_pack->>'state','STALE');
    else
    v_paper_state:=case
      when v_workflow.state in ('CANCELLED','REJECTED','SUPERSEDED','EXPIRED') then 'RETIRED'
      when lower(coalesce(v_paper_outbox.payment_scope_json->>'candidate_paper_generation_retired','false'))
        in ('true','t','1','yes') then 'RETIRED'
      when lower(coalesce(v_paper_outbox.payment_scope_json->>'candidate_paper_pack_retryable','false'))
        in ('true','t','1','yes') then 'FAILED_RETRYABLE'
      when upper(coalesce(v_paper_outbox.payment_scope_json->>'candidate_paper_pack_failure_class',''))='TERMINAL'
        then 'FAILED_TERMINAL'
      when v_paper_outbox.status::text='FAILED'
        or upper(coalesce(v_current.document_state,''))='FAILED' then 'FAILED_TERMINAL'
      when coalesce((v_paper_outbox.payment_scope_json->>'candidate_paper_pack_ready')::boolean,false) then 'READY'
      when v_workflow.state='AWAITING_PAPER_RETURN' then 'PREPARING'
      else 'STALE' end;
    end if;
  end if;

  select coalesce(jsonb_agg(jsonb_build_object(
    'scope',case when w.workflow_kind='CONTRACT_EXPENSE' then 'EXPENSE'
      when w.workflow_kind='CONTRACT_COMBINED' then 'COMBINED' else 'HOURS' end,
    'workflow_id',w.id,'workflow_generation',w.generation,'state',w.state,
    'reason',w.rejection_reason,
    'rejection_actionable',case when w.state='REJECTED'
      then not private._candidate_rejection_replaced_v1(w.id) else false end,
    'replacement_workflow_id',replacement.id,
    'replacement_state',replacement.state,
    'recovery_action',case when w.state='REJECTED'
      and not private._candidate_rejection_replaced_v1(w.id) then private._candidate_office_action_v1(
      'RESUBMIT_REJECTED','Resubmit rejected submission','REJECTION',v_writes,null,null,true,false,
      'CLIENT','CANDIDATE_APP:WORKFLOW_RESUBMISSION',
      jsonb_build_object('generation',w.generation),'[]'::jsonb,'REQUIRED',true
    ) else null end
  ) order by w.updated_at_utc desc,w.id desc),'[]'::jsonb)
  into v_rejections
  from public.candidate_submission_workflows w
  left join lateral (
    select later.id,later.state
    from public.candidate_submission_workflows later
    where later.replacement_of_workflow_id=w.id
    order by later.created_at_utc, later.id
    limit 1
  ) replacement on true
  where w.environment=v_environment and w.state in ('REFUSED','REJECTED')
    and (w.contract_week_id=v_week.id or w.target_timesheet_id=v_current.timesheet_id
      or w.anchor_timesheet_id=v_current.timesheet_id);
  select exists(
    select 1 from jsonb_array_elements(v_rejections) rejection
    where rejection->>'state'='REJECTED'
      and coalesce((rejection->>'rejection_actionable')::boolean,false)
  ) into v_actionable_rejection;

  if v_workflow.id is not null and v_approval.id is not null and v_approval.method='EMAIL' then
    v_action:=private._candidate_office_action_v1(
      'SEND_MANAGER_REMINDER','Send manager reminder','MANAGER_APPROVAL',v_reminder_eligible,
      case when v_approval.expires_at_utc<=p_observed_at_utc then 'MANAGER_REQUEST_EXPIRED'
        when v_manager_accepted is null then 'MANAGER_DELIVERY_NOT_PROVIDER_ACCEPTED'
        when v_manager_accepted+interval '24 hours'>p_observed_at_utc then 'MANAGER_REMINDER_TOO_EARLY'
        when v_resends_remaining=0 then 'MANAGER_REMINDER_LIMIT_REACHED'
        when coalesce(v_manager_pending,0)>0 then 'MANAGER_DELIVERY_PENDING'
        when coalesce(v_manager_lease,false) then 'CANDIDATE_PROVIDER_HANDOFF_IN_PROGRESS'
        else 'CANDIDATE_ACTION_NOT_ELIGIBLE' end,
      'The manager reminder is not currently available.',true,false,
      'POST','/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/remind',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),'[]'::jsonb,'REQUIRED',false
    );
    v_actions:=v_actions||jsonb_build_array(v_action);
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'RENEW_MANAGER_REQUEST','Renew manager approval request','MANAGER_APPROVAL',v_renewal_eligible,
      'MANAGER_REQUEST_NOT_EXPIRED','Renewal becomes available only after the current request expires.',true,false,
      'POST','/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/renew',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),'[]'::jsonb,'REQUIRED',false
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'CANCEL_MANAGER_REQUEST','Cancel manager approval request','MANAGER_APPROVAL',v_cancel_eligible,
      case when coalesce(v_manager_lease,false) then 'CANDIDATE_PROVIDER_HANDOFF_IN_PROGRESS' else 'CANDIDATE_ACTION_NOT_ELIGIBLE' end,
      'This manager approval request cannot currently be cancelled.',true,true,
      'POST','/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/cancel',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),
      jsonb_build_array(jsonb_build_object('name','reason','type','string','required',true,'max_length',1000)),
      'REQUIRED',false
    ));
  elsif v_workflow.id is not null and v_approval.id is not null and v_approval.method='PHONE' then
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'BEGIN_PHONE_REVIEW','Review manager approval by phone','MANAGER_APPROVAL',v_manager_enabled and v_approval.state='PENDING',
      'CANDIDATE_ACTION_NOT_ELIGIBLE','Phone review is not currently available.',false,false,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/phone-review',
       jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),'[]'::jsonb,'REQUIRED',true
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'PREPARE_PHONE_SIGNATURE','Prepare manager signature','MANAGER_APPROVAL',v_manager_enabled and v_approval.state='PENDING',
      'CANDIDATE_ACTION_NOT_ELIGIBLE','A phone-approval signature cannot currently be prepared.',false,false,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/signature/prepare',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),
      jsonb_build_array(
        jsonb_build_object('name','media_type','type','enum','values',jsonb_build_array('image/png','image/jpeg'),'required',true),
        jsonb_build_object('name','byte_size','type','integer','required',true,'minimum',1)
      ),'REQUIRED',false
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'RECORD_PHONE_REVIEW_PROGRESS','Record reviewed document','MANAGER_APPROVAL',v_manager_enabled and v_approval.state='PENDING',
      'CANDIDATE_ACTION_NOT_ELIGIBLE','Phone review progress cannot currently be recorded.',false,false,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/phone-progress',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),
      jsonb_build_array(
        jsonb_build_object('name','manifest_sha256_hex','type','sha256','required',true),
        jsonb_build_object('name','component_id','type','uuid','required',true),
        jsonb_build_object('name','component_sha256_hex','type','sha256','required',true),
        jsonb_build_object('name','viewed_receipt','type','object','required',true)
      ),'REQUIRED',false
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'APPROVE_BY_PHONE','Approve by phone','MANAGER_APPROVAL',v_manager_enabled and v_approval.state='PENDING',
      'CANDIDATE_ACTION_NOT_ELIGIBLE','Phone approval cannot currently be completed.',true,false,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/phone-approve',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),
      jsonb_build_array(
        jsonb_build_object('name','manifest_sha256_hex','type','sha256','required',true),
        jsonb_build_object('name','manager_name','type','string','required',true,'max_length',200),
        jsonb_build_object('name','manager_position','type','string','required',true,'max_length',200),
        jsonb_build_object('name','signature_component_id','type','uuid','required',true)
      ),'REQUIRED',true
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'REFUSE_BY_PHONE','Refuse by phone','MANAGER_APPROVAL',v_manager_enabled and v_approval.state='PENDING',
      'CANDIDATE_ACTION_NOT_ELIGIBLE','Phone refusal cannot currently be recorded.',true,true,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/phone-refuse',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),
      jsonb_build_array(jsonb_build_object('name','reason','type','string','required',true,'max_length',1000)),
      'REQUIRED',false
    ));
  end if;

  if v_workflow.id is not null and v_workflow.state in ('RECEIVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE') then
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'RETRY_FINALISATION','Retry finalisation','FINALISATION',v_writes,
      'CANDIDATE_ACTION_NOT_ELIGIBLE','Finalisation cannot currently be retried.',true,false,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/retry-finalisation',
      jsonb_build_object('generation',v_workflow.generation),'[]'::jsonb,'REQUIRED',false
    ));
  end if;
  if v_workflow.id is not null and v_workflow.route='PAPER' then
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'VIEW_PAPER_PACK','View current paper pack','PAPER',v_paper_state='READY',
      'CANDIDATE_PAPER_PACK_NOT_READY','The current paper pack is not ready to view.',false,false,'GET',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/paper-pack?generation='||v_workflow.generation::text,
      jsonb_build_object('generation',v_workflow.generation),'[]'::jsonb,'NONE',false
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'REVIEW_PAPER_RETURN','Review returned paper documents','PAPER',v_paper_state='RETURN_RECEIVED',
      'CANDIDATE_PAPER_RETURN_NOT_RECEIVED','A complete paper return has not been received.',false,false,'GET',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/paper-return-review?generation='||v_workflow.generation::text,
      jsonb_build_object('generation',v_workflow.generation),'[]'::jsonb,'NONE',false
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'RETRY_PAPER_PREPARATION','Retry paper pack preparation','PAPER',
      v_paper_enabled and v_paper_state='FAILED_RETRYABLE',
      'CANDIDATE_PAPER_PACK_RETRY_NOT_READY','Paper pack preparation is not currently retryable.',true,false,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/retry-paper-preparation',
      jsonb_build_object('generation',v_workflow.generation),'[]'::jsonb,'REQUIRED',false
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'ISSUE_REPLACEMENT_PAPER_PACK','Issue replacement paper pack','PAPER',v_route_enabled and v_paper_state in ('READY','FAILED_RETRYABLE','FAILED_TERMINAL','STALE'),
      'CANDIDATE_PAPER_REPLACEMENT_NOT_READY','A replacement paper pack cannot currently be issued.',true,true,'GET',
      '/api/candidate-app/timesheets/'||v_current.timesheet_id::text||'/route-preview?action=REISSUE_QR',
      '{}'::jsonb,'[]'::jsonb,'NONE',false
    ));
  end if;

  begin
    v_reject_preview:=private._candidate_office_reject_preview_v1(
      v_environment,v_current.timesheet_id,p_actor_user_id,p_observed_at_utc
    );
  exception when others then
    v_reject_preview:=jsonb_build_object('permitted',false,'disabled_reason_code','CANDIDATE_ACTION_NOT_ELIGIBLE');
  end;
  v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
    'REJECT_CANDIDATE_SUBMISSION','Reject Candidate submission','REJECTION',
    v_writes and coalesce((v_reject_preview->>'permitted')::boolean,false),
    coalesce(v_reject_preview->>'disabled_reason_code','CANDIDATE_ACTION_NOT_ELIGIBLE'),
    coalesce(v_reject_preview->>'disabled_reason','This Candidate submission is not currently eligible for rejection.'),
    true,true,'GET','/api/candidate-app/timesheets/'||v_current.timesheet_id::text||'/reject-preview',
    '{}'::jsonb,'[]'::jsonb,'NONE',false
  ));
  if coalesce((v_capabilities->>'candidate_no_work_allowed')::boolean,false) then
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'MARK_NO_WORK','I did not work this week','CANDIDATE_DESTINATION',v_writes,
      'CANDIDATE_ACTION_NOT_ELIGIBLE','No-work is not currently available for this record.',true,false,
      'CLIENT','CANDIDATE_APP:CONTRACT_WEEK_NO_WORK',jsonb_build_object(
        'contract_week_id',v_week.id,'row_signature',v_signature
      ),'[]'::jsonb,'REQUIRED',false
    ));
  end if;

  v_active_workflow_state:=case when v_workflow.state in (
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
  ) then v_workflow.state else null end;
  v_candidate_status_code:=private._candidate_status_code_v1(
    v_fin.paid_at_utc is not null,
    v_fin.authorised_at_utc is not null or v_current.authorised_at_server is not null,
    v_fin.locked_by_invoice_id is not null
      or upper(coalesce(v_current.status::text,''))='INVOICED',
    v_active_workflow_state,v_actionable_rejection,v_fin.processing_status::text,v_week.status::text
  );
  v_candidate_status_label:=initcap(replace(lower(v_candidate_status_code),'_',' '));
  v_candidate_status_tone:=case
    when v_candidate_status_code in ('PAID','AUTHORISED','FINALISED','MANAGER_APPROVED','READY_TO_FINALISE') then 'success'
    when v_candidate_status_code in ('REFUSED','REJECTED','FAILED') then 'danger'
    when v_candidate_status_code in ('AWAITING_MANAGER_APPROVAL','AWAITING_PAPER_RETURN','RECEIVED') then 'warning'
    else 'neutral' end;
  v_expense_email_missing:=coalesce((v_capabilities->>'expense_value')::numeric,0)<>0
    and not coalesce((v_capabilities->>'expense_invoice_email_ready')::boolean,false);
  select action_item into v_primary_action
  from jsonb_array_elements(v_actions) with ordinality actions(action_item,ordinality)
  where coalesce((action_item->>'enabled')::boolean,false)
    and coalesce((action_item->>'prominent')::boolean,false)
  order by ordinality
  limit 1;

  return jsonb_build_object(
    'ok',true,
    'contract_version','OFFICE_CANDIDATE_TIMESHEET_V1',
    'office_contract_version','CLOUDTMS_OFFICE_CANDIDATE_API_V1',
    'typed_action_version',1,
    'observed_at_utc',p_observed_at_utc,
    'requested_identity',jsonb_build_object(
      'timesheet_id',p_timesheet_id,'contract_week_id',p_contract_week_id,
      'row_key',nullif(btrim(coalesce(p_row_key,'')),''),
      'expected_row_signature',nullif(btrim(coalesce(p_expected_row_signature,'')),'')
    ),
    'current_identity',jsonb_build_object(
      'timesheet_id',v_current.timesheet_id,'timesheet_version',v_current.version,
      'contract_week_id',v_week.id,
      'row_key',coalesce(
        nullif(btrim(coalesce(p_row_key,'')),''),v_week.id::text,v_current.timesheet_id::text
      ),
      'row_signature',v_signature,'route_family',v_capabilities->>'route_family',
      'record_role',v_capabilities->>'record_role',
      'moved',coalesce((v_identity->>'moved')::boolean,false),
      'stale_signature',nullif(btrim(coalesce(p_expected_row_signature,'')),'') is not null
        and p_expected_row_signature is distinct from v_signature
    ),
    'candidate_status',jsonb_build_object(
      'code',v_candidate_status_code,'label',v_candidate_status_label,
      'tone',v_candidate_status_tone,'description',null
    ),
    'workflow',case when v_workflow.id is null then null else jsonb_build_object(
      'workflow_id',v_workflow.id,'generation',v_workflow.generation,
      'state',v_workflow.state,'workflow_kind',v_workflow.workflow_kind,
      'route',v_workflow.route,'approval_method',v_approval.method,
      'is_current_action_workflow',v_active_workflow_state is not null,
      'historical',v_active_workflow_state is null
    ) end,
    'manager_approval',case when v_approval.id is null then null else jsonb_build_object(
      'method',v_approval.method,'request_id',v_approval.id,
      'request_generation',v_approval.request_generation,'state',v_approval.state,
      'provider_first_accepted_at_utc',v_manager_first_accepted,
      'provider_accepted_at_utc',v_manager_accepted,
      'delivery_state',v_manager_delivery_state,'provider_status',v_manager_provider_status,
      'delivery_pending',coalesce(v_manager_pending,0)>0,
      'provider_handoff_in_progress',coalesce(v_manager_lease,false),
      'expires_at_utc',v_approval.expires_at_utc,'resend_count',v_approval.resend_count,
      'resends_remaining',v_resends_remaining,
      'next_reminder_at_utc',case when v_manager_accepted is not null then v_manager_accepted+interval '24 hours' else null end,
      'reminder_eligible',v_reminder_eligible,'renewal_eligible',v_renewal_eligible,
      'cancel_eligible',v_cancel_eligible
    ) end,
    'paper_pack',jsonb_build_object(
      'state',v_paper_state,'lifecycle_code',v_workflow.state,
      'delivery_generation',v_paper_delivery_generation,
      'page_count',case when coalesce(v_paper_outbox.payment_scope_json,'{}'::jsonb)->>'candidate_complete_pack_page_count'~'^[1-9][0-9]*$'
        then (v_paper_outbox.payment_scope_json->>'candidate_complete_pack_page_count')::integer else null end,
      'reason_code',case when v_paper_state in ('FAILED_RETRYABLE','FAILED_TERMINAL')
        then coalesce(v_paper_pack->>'failure_code',
          v_paper_outbox.payment_scope_json->>'candidate_paper_pack_failure_code',
          'CANDIDATE_PAPER_PACK_'||v_paper_state)
        when v_paper_state='BACKOFF' then 'CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE'
        when v_paper_state='STALE' then 'CANDIDATE_PAPER_PACK_STALE' else null end,
      'failure_scope',coalesce(v_paper_pack->>'failure_scope',
        case when v_paper_outbox.id is not null then 'OUTBOX' else null end),
      'retryable',v_paper_state='FAILED_RETRYABLE',
      'attempt_count',coalesce((v_paper_pack->>'attempt_count')::integer,
        case when coalesce(v_paper_outbox.payment_scope_json,'{}'::jsonb)
          ->>'candidate_paper_pack_attempt_count'~'^[0-9]+$'
        then (v_paper_outbox.payment_scope_json->>'candidate_paper_pack_attempt_count')::integer else 0 end),
      'next_retry_at_utc',coalesce(nullif(v_paper_pack->>'next_retry_at_utc','')::timestamptz,nullif(
        v_paper_outbox.payment_scope_json->>'candidate_paper_pack_next_retry_at_utc',''
      )::timestamptz),
      'retry_in_progress',coalesce((v_paper_pack->>'retry_in_progress')::boolean,false),
      'operation_id',coalesce(v_paper_pack->>'operation_id',
        v_paper_outbox.payment_scope_json->>'candidate_paper_pack_operation_id'),
      'issued_at_utc',v_paper_outbox.sent_at,
      'returned_at_utc',case when v_workflow.state='RECEIVED' then v_workflow.updated_at_utc else null end
    ),
    'rejections',v_rejections,
    'primary_action',v_primary_action,
    'available_actions',v_actions,
    'diagnostics',case when v_expense_email_missing then jsonb_build_array(jsonb_build_object(
      'code','EXPENSE_EMAIL_MISSING','severity','WARNING','message','Expense Email missing',
      'calculation_effect','NONE','authority_effect','PRESENTATION_ONLY'
    )) else '[]'::jsonb end,
    'refresh_hints',jsonb_build_object(
      'summary',true,'simple_timesheet',true,'bulk_process',true,'bulk_authorise',true,
      'affected_timesheet_ids',jsonb_build_array(v_current.timesheet_id),
      'affected_contract_week_ids',case when v_week.id is null
        then '[]'::jsonb else jsonb_build_array(v_week.id) end
    )
  );
end;
$function$;

-- One transaction-owned Office PAPER retry completion receipt.  The helper can
-- also reconstruct a legacy missing outer receipt from the exact durable inner
-- mail operation, closing the worker-crash boundary without another table.
create or replace function private._candidate_office_paper_retry_receipt_v1(
  p_actor_user_id uuid,
  p_workflow_id uuid,
  p_generation integer,
  p_idempotency_key text,
  p_http_status integer default null,
  p_result jsonb default null,
  p_reconstruct boolean default false,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_key text:=nullif(btrim(coalesce(p_idempotency_key,'')),'');
  v_request_hash text;
  v_existing_before jsonb;
  v_existing_after jsonb;
  v_result jsonb:=p_result;
  v_http_status integer:=p_http_status;
  v_mail public.mail_outbox%rowtype;
  v_scope jsonb;
  v_state text;
begin
  if p_actor_user_id is null or p_workflow_id is null or p_generation is null
     or p_generation<1 or v_key is null or length(v_key)>200 then
    raise exception 'CANDIDATE_PAPER_RETRY_PAYLOAD_INVALID' using errcode='22023';
  end if;
  v_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_REQUEST_V1',
    'workflow_id',p_workflow_id,'generation',p_generation,
    'idempotency_key',v_key
  )::text,'UTF8'),'sha256'),'hex');
  perform pg_advisory_xact_lock(hashtextextended(
    'OFFICE_CANDIDATE_PAPER_RETRY:'||p_actor_user_id::text||':'
      ||p_workflow_id::text||':'||v_key,0
  ));
  select ae.before_json,ae.after_json into v_existing_before,v_existing_after
  from public.audit_events ae
  where ae.object_type='cloudtms_office_candidate_paper_retry'
    and ae.object_id_text=p_workflow_id::text
    and ae.actor_user_id=p_actor_user_id
    and ae.correlation_id=v_key
  order by ae.ts_utc desc,ae.id desc limit 1;
  if found then
    if v_existing_before->>'request_sha256' is distinct from v_request_hash then
      raise exception 'IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    return coalesce(v_existing_after,'{}'::jsonb)||jsonb_build_object(
      'found',true,'idempotent_replay',true
    );
  end if;

  if v_result is null and coalesce(p_reconstruct,false) then
    select mail.* into v_mail
    from public.mail_outbox mail
    where mail.type='TIMESHEET_QR'
      and mail.context_kind='timesheets'
      and upper(coalesce(mail.payment_scope_json->>'candidate_mail_authority',''))
        ='CANDIDATE_PAPER_V1'
      and mail.payment_scope_json->>'candidate_workflow_id'=p_workflow_id::text
      and mail.payment_scope_json->>'candidate_workflow_generation'=p_generation::text
      and mail.payment_scope_json->>'candidate_paper_pack_operation_id'=v_key
    order by mail.created_at_utc desc,mail.id desc
    limit 1;
    if found then
      v_scope:=coalesce(v_mail.payment_scope_json,'{}'::jsonb);
      v_state:=upper(coalesce(v_scope->>'candidate_paper_pack_operation_state',''));
      if v_state='READY' then
        v_http_status:=200;
        v_result:=jsonb_build_object(
          'ok',true,'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
          'idempotency_key',v_key,'workflow_id',p_workflow_id,
          'generation',p_generation,'paper_pack_state','READY',
          'page_count',coalesce(
            nullif(v_scope->>'candidate_complete_pack_page_count','')::integer,0
          ),'reconstructed_from_inner_receipt',true
        );
      elsif v_state in ('FAILED_RETRYABLE','FAILED_TERMINAL') then
        v_http_status:=case when v_state='FAILED_RETRYABLE' then 503 else 409 end;
        v_result:=jsonb_build_object(
          'ok',false,'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
          'idempotency_key',v_key,'workflow_id',p_workflow_id,
          'generation',p_generation,'paper_pack_state',v_state,
          'retryable',v_state='FAILED_RETRYABLE',
          'error_code',coalesce(
            nullif(v_scope->>'candidate_paper_pack_failure_code',''),
            'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
          ),
          'next_retry_at_utc',v_scope->'candidate_paper_pack_next_retry_at_utc',
          'reconstructed_from_inner_receipt',true
        );
      end if;
    end if;
  end if;
  if v_result is null then
    return jsonb_build_object(
      'ok',true,'found',false,
      'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_RECEIPT_V1'
    );
  end if;
  if jsonb_typeof(v_result)<>'object'
     or v_result->>'contract_version'<>'OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3'
     or v_result->>'workflow_id'<>p_workflow_id::text
     or nullif(v_result->>'generation','')::integer<>p_generation
     or v_result->>'idempotency_key'<>v_key
     or upper(coalesce(v_result->>'paper_pack_state','')) not in (
       'READY','FAILED_RETRYABLE','FAILED_TERMINAL'
     )
     or v_http_status not in (200,409,503) then
    raise exception 'CANDIDATE_PAPER_RETRY_RESULT_INVALID' using errcode='22023';
  end if;
  v_result:=jsonb_build_object(
    'ok',true,'found',true,
    'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_RECEIPT_V1',
    'http_status',v_http_status,'result',v_result,'idempotent_replay',false
  );
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,
    reason,correlation_id,ts_utc
  ) values (
    p_actor_user_id,'cloudtms_office_candidate_paper_retry',p_workflow_id::text,
    'CANDIDATE_OFFICE_PAPER_RETRY_COMPLETED',
    jsonb_build_object('request_sha256',v_request_hash),v_result,
    'Durable Office PAPER retry operation result',v_key,coalesce(p_now_utc,now())
  );
  return v_result;
end;
$function$;

create or replace function public.cloudtms_office_candidate_adapter_v1(
  p_action text,
  p_actor_user_id uuid,
  p_environment text,
  p_payload jsonb default '{}'::jsonb,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_environment text;
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_observed timestamptz:=coalesce(p_now_utc,now());
  v_items jsonb;
  v_results jsonb:='[]'::jsonb;
  v_item jsonb;
  v_result jsonb;
  v_retry_result jsonb;
  v_retry_receipt jsonb;
  v_preview jsonb;
  v_context jsonb;
  v_context_sha text;
  v_batch_id uuid;
  v_idempotency_key text;
  v_request_hash text;
  v_client_request_hash text;
  v_existing_before jsonb;
  v_existing_after jsonb;
  v_workflow_action text;
  v_office_permission text;
  v_workflow_id uuid;
  v_generation integer;
  v_approval_id uuid;
  v_approval_generation integer;
  v_error_code text;
  v_success_count integer:=0;
  v_failure_count integer:=0;
  v_skipped_count integer:=0;
  v_selection_fingerprint text;
  v_current_fingerprint text;
  v_timesheet public.timesheets%rowtype;
  v_family_key text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_actor_user_id is null or jsonb_typeof(v_payload)<>'object' then
    raise exception 'OFFICE_AUTH_REQUIRED' using errcode='28000';
  end if;
  if v_action='CAPABILITIES' then
    return private._candidate_office_capabilities_v1(v_environment,p_actor_user_id,v_observed);
  end if;
  if v_action='PROJECT_ONE' then
    return private._candidate_office_timesheet_projection_v1(
      v_environment,nullif(v_payload->>'timesheet_id','')::uuid,
      nullif(v_payload->>'contract_week_id','')::uuid,v_payload->>'row_key',
      v_payload->>'expected_row_signature',p_actor_user_id,v_observed
    );
  elsif v_action='PROJECT_BATCH' then
    if upper(coalesce(v_payload->>'surface','')) not in (
      'SIMPLE_TIMESHEET','TIMESHEET_SUMMARY','BULK_PROCESS','BULK_AUTHORISE','INVOICE_GENERATOR','INVOICE_ISSUER'
    ) then
      raise exception 'CANDIDATE_OFFICE_PROJECTION_SURFACE_INVALID' using errcode='22023';
    end if;
    v_items:=coalesce(v_payload->'identities','[]'::jsonb);
    if jsonb_typeof(v_items)<>'array' or jsonb_array_length(v_items)<1 or jsonb_array_length(v_items)>100 then
      raise exception 'CANDIDATE_OFFICE_PROJECTION_BATCH_INVALID' using errcode='22023';
    end if;
    if (select count(*)<>count(distinct coalesce(value->>'row_key',value->>'timesheet_id',value->>'contract_week_id'))
        from jsonb_array_elements(v_items)) then
      raise exception 'CANDIDATE_OFFICE_PROJECTION_DUPLICATE_IDENTITY' using errcode='22023';
    end if;
    for v_item in select value from jsonb_array_elements(v_items) with ordinality x(value,ordinality)
    loop
      begin
        v_result:=private._candidate_office_timesheet_projection_v1(
          v_environment,nullif(v_item->>'timesheet_id','')::uuid,
          nullif(v_item->>'contract_week_id','')::uuid,v_item->>'row_key',
          v_item->>'expected_row_signature',p_actor_user_id,v_observed
        );
        v_results:=v_results||jsonb_build_array(jsonb_build_object(
          'ok',true,'correlation_key',coalesce(v_item->>'row_key',v_item->>'timesheet_id',v_item->>'contract_week_id'),
          'projection',v_result
        ));
      exception when others then
        v_error_code:=coalesce(nullif(substring(sqlerrm from '([A-Z][A-Z0-9_]{2,})'),''),'CANDIDATE_OFFICE_PROJECTION_FAILED');
        v_results:=v_results||jsonb_build_array(jsonb_build_object(
          'ok',false,'correlation_key',coalesce(v_item->>'row_key',v_item->>'timesheet_id',v_item->>'contract_week_id'),
          'error',jsonb_build_object('code',v_error_code,'retryable',sqlstate in ('40001','40P01'))
        ));
      end;
    end loop;
    return jsonb_build_object(
      'ok',true,'contract_version','OFFICE_CANDIDATE_PROJECTION_BATCH_V1',
      'surface',upper(coalesce(v_payload->>'surface','')),
      'observed_at_utc',v_observed,'result_count',jsonb_array_length(v_results),'results',v_results
    );
  elsif v_action='REJECT_PREVIEW' then
    return private._candidate_office_reject_preview_v1(
      v_environment,nullif(v_payload->>'timesheet_id','')::uuid,p_actor_user_id,v_observed
    );
  elsif v_action='REJECT_CONFIRM' then
    v_idempotency_key:=nullif(btrim(coalesce(v_payload->>'idempotency_key','')),'');
    if v_idempotency_key is null or coalesce(v_payload->>'context_sha256','') !~ '^[0-9a-fA-F]{64}$'
       or nullif(btrim(coalesce(v_payload->>'reason','')),'') is null
       or length(btrim(v_payload->>'reason'))>1000 then
      raise exception 'CANDIDATE_REJECT_PAYLOAD_INVALID' using errcode='22023';
    end if;
    v_request_hash:=encode(extensions.digest(convert_to((v_payload-'request_id')::text,'UTF8'),'sha256'),'hex');
    select ae.before_json,ae.after_json into v_existing_before,v_existing_after
    from public.audit_events ae where ae.object_type='cloudtms_office_candidate_rejection'
      and ae.actor_user_id=p_actor_user_id and ae.correlation_id=v_idempotency_key
    order by ae.ts_utc desc,ae.id desc limit 1;
    if found then
      if v_existing_before->>'request_sha256' is distinct from v_request_hash then
        raise exception 'IDEMPOTENCY_CONFLICT' using errcode='23505';
      end if;
      return coalesce(v_existing_after,'{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
    end if;
    select * into v_timesheet from public.timesheets
    where timesheet_id=nullif(v_payload->>'timesheet_id','')::uuid;
    if not found then raise exception 'TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
    v_family_key:='CANDIDATE_PAPER_FAMILY:'||v_environment||':'
      ||coalesce(v_timesheet.contract_id::text,'-')||':'||coalesce(v_timesheet.week_ending_date::text,'-');
    perform pg_advisory_xact_lock(hashtextextended(v_family_key,0));
    select ae.before_json,ae.after_json into v_existing_before,v_existing_after
    from public.audit_events ae where ae.object_type='cloudtms_office_candidate_rejection'
      and ae.actor_user_id=p_actor_user_id and ae.correlation_id=v_idempotency_key
    order by ae.ts_utc desc,ae.id desc limit 1;
    if found then
      if v_existing_before->>'request_sha256' is distinct from v_request_hash then
        raise exception 'IDEMPOTENCY_CONFLICT' using errcode='23505';
      end if;
      return coalesce(v_existing_after,'{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
    end if;
    perform 1 from public.timesheets where timesheet_id=v_timesheet.timesheet_id for update;
    v_preview:=private._candidate_office_reject_preview_v1(
      v_environment,v_timesheet.timesheet_id,p_actor_user_id,v_observed
    );
    if not coalesce((v_preview->>'permitted')::boolean,false)
       or lower(v_preview->>'context_sha256') is distinct from lower(v_payload->>'context_sha256')
       or v_preview->>'expected_row_signature' is distinct from v_payload->>'expected_row_signature'
       or v_preview->>'expected_timesheet_id' is distinct from coalesce(v_payload->>'expected_timesheet_id',v_payload->>'timesheet_id') then
      raise exception 'CANDIDATE_CONTEXT_STALE' using errcode='40001',detail=v_preview::text;
    end if;
    perform private._candidate_office_service_context_open_v1(
      v_environment,p_actor_user_id,'reject_submission','REJECT_CONFIRM',v_observed
    );
    v_result:=public.candidate_submission_reject_atomic_v1(
      p_actor_user_id,v_environment,v_timesheet.timesheet_id,
      (v_preview->>'expected_timesheet_id')::uuid,v_preview->>'expected_row_signature',
      btrim(v_payload->>'reason'),v_idempotency_key,v_observed
    );
    perform private._candidate_office_service_context_close_v1();
    v_result:=v_result||jsonb_build_object(
      'contract_version','OFFICE_CANDIDATE_MUTATION_RESULT_V1',
      'context_sha256',v_preview->>'context_sha256',
      'refresh_hints',jsonb_build_object(
        'summary',true,'simple_timesheet',true,'bulk_process',true,'bulk_authorise',true,
        'refetch','AFFECTED_ROWS'
      )
    );
    insert into public.audit_events(actor_user_id,object_type,object_id_text,action,before_json,after_json,reason,correlation_id,ts_utc)
    values(p_actor_user_id,'cloudtms_office_candidate_rejection',v_timesheet.timesheet_id::text,
      'CANDIDATE_OFFICE_REJECTION_CONFIRMED',jsonb_build_object('request_sha256',v_request_hash),
      v_result,btrim(v_payload->>'reason'),v_idempotency_key,v_observed);
    return v_result;
  elsif v_action='ROUTE_CONFIRM' then
    perform private._candidate_office_service_context_open_v1(
      v_environment,p_actor_user_id,'change_route','ROUTE_CONFIRM',v_observed
    );
    v_result:=public.timesheet_route_version_confirmed_v1(
      nullif(v_payload->>'current_timesheet_id','')::uuid,
      nullif(v_payload->>'expected_timesheet_id','')::uuid,
      v_payload->>'expected_row_signature',
      v_payload->>'expected_context_sha256',
      v_payload->>'target_action',
      p_actor_user_id,
      v_payload->>'reason_code',
      v_payload->>'reason_note',
      v_payload->>'idempotency_key',
      coalesce((v_payload->>'allow_manual_only')::boolean,false),
      v_observed
    )||jsonb_build_object('contract_version','OFFICE_CANDIDATE_MUTATION_RESULT_V1');
    perform private._candidate_office_service_context_close_v1();
    return v_result;
  elsif v_action in ('FINALISE_EXECUTE','FINALISE_REPLAY_LOOKUP') then
    v_workflow_id:=nullif(v_payload->>'workflow_id','')::uuid;
    v_generation:=nullif(v_payload->>'generation','')::integer;
    v_idempotency_key:=nullif(btrim(coalesce(v_payload->>'idempotency_key','')),'');
    if v_workflow_id is null or v_generation is null or v_generation<1
       or v_idempotency_key is null then
      raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';
    end if;
    perform private._candidate_office_service_context_open_v1(
      v_environment,p_actor_user_id,'retry_finalisation','RETRY_FINALISATION',v_observed
    );
    v_result:=public.candidate_submission_finalize_atomic_v1(
      null,v_environment,v_workflow_id,v_generation,
      v_payload->>'expected_row_signature',v_idempotency_key,v_observed,
      coalesce(v_payload->'daily_materialisation_json','{}'::jsonb)||jsonb_build_object(
        'service_finalisation',
        coalesce(v_payload->'daily_materialisation_json'->'service_finalisation','{}'::jsonb)
          ||jsonb_build_object(
            'actor_user_id',p_actor_user_id,
            'replay_probe_only',v_action='FINALISE_REPLAY_LOOKUP'
              and not coalesce((v_payload->>'replay_key_probe_only')::boolean,false),
            'replay_key_probe_only',coalesce(
              (v_payload->>'replay_key_probe_only')::boolean,false
            )
          )
      )
    )||jsonb_build_object('contract_version','OFFICE_CANDIDATE_MUTATION_RESULT_V1');
    perform private._candidate_office_service_context_close_v1();
    return v_result;
  elsif v_action in ('PAPER_RETRY_REPLAY','PAPER_RETRY_RECORD') then
    v_workflow_id:=nullif(v_payload->>'workflow_id','')::uuid;
    v_generation:=nullif(v_payload->>'generation','')::integer;
    v_idempotency_key:=nullif(btrim(coalesce(v_payload->>'idempotency_key','')),'');
    if v_workflow_id is null or v_generation is null or v_generation<1
       or v_idempotency_key is null or length(v_idempotency_key)>200 then
      raise exception 'CANDIDATE_PAPER_RETRY_PAYLOAD_INVALID' using errcode='22023';
    end if;
    return private._candidate_office_paper_retry_receipt_v1(
      p_actor_user_id,v_workflow_id,v_generation,v_idempotency_key,
      case when v_action='PAPER_RETRY_RECORD'
        then nullif(v_payload->>'http_status','')::integer else null end,
      case when v_action='PAPER_RETRY_RECORD' then v_payload->'result' else null end,
      v_action='PAPER_RETRY_REPLAY',v_observed
    );
  elsif v_action='WORKFLOW_ACTION_EXECUTE' then
    v_workflow_action:=upper(btrim(coalesce(v_payload->>'workflow_action','')));
    if v_workflow_action not in ('REMIND','RENEW','MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF',
        'BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','MANAGER_REFUSE',
        'REGISTER_REVIEW_COMPONENT','REGISTER_FINAL_SIGNED_DOCUMENT','BEGIN_CANONICAL_DAILY_SAVE',
        'PAPER_PACK_RELEASE','PAPER_PACK_ATTEMPT_CLAIM','PAPER_PACK_MARK_FAILURE') then
      raise exception 'CANDIDATE_WORKFLOW_ACTION_INVALID' using errcode='22023';
    end if;
    v_workflow_id:=nullif(v_payload->>'workflow_id','')::uuid;
    v_generation:=nullif(v_payload->>'generation','')::integer;
    v_idempotency_key:=nullif(btrim(coalesce(v_payload->>'idempotency_key','')),'');
    if v_workflow_id is null or v_generation is null or v_generation<1 or v_idempotency_key is null then
      raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';
    end if;
    v_request_hash:=encode(extensions.digest(convert_to((v_payload-'request_id')::text,'UTF8'),'sha256'),'hex');
    select ae.before_json,ae.after_json into v_existing_before,v_existing_after
    from public.audit_events ae where ae.object_type='cloudtms_office_candidate_action'
      and ae.object_id_text=v_workflow_id::text and ae.actor_user_id=p_actor_user_id
      and ae.correlation_id=v_idempotency_key
    order by ae.ts_utc desc,ae.id desc limit 1;
    if found then
      if v_existing_before->>'request_sha256' is distinct from v_request_hash then
        raise exception 'IDEMPOTENCY_CONFLICT' using errcode='23505';
      end if;
      v_result:=coalesce(v_existing_after,'{}'::jsonb);
      if v_workflow_action='PAPER_PACK_ATTEMPT_CLAIM' then
        v_result:=v_result||jsonb_build_object('claim_acquired_new',false);
      end if;
      return v_result||jsonb_build_object('idempotent_replay',true);
    end if;
    v_approval_id:=nullif(v_payload->>'approval_request_id','')::uuid;
    v_approval_generation:=nullif(v_payload->>'approval_request_generation','')::integer;
    if v_workflow_action in ('REMIND','RENEW','MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF',
        'BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','MANAGER_REFUSE')
       and (v_approval_id is null or v_approval_generation is null or v_approval_generation<1) then
      raise exception 'CANDIDATE_REQUEST_GENERATION_STALE' using errcode='22023';
    end if;
    if v_approval_id is not null then
      perform 1 from public.candidate_approval_requests ar
      where ar.id=v_approval_id and ar.workflow_id=v_workflow_id
        and ar.workflow_generation=v_generation and ar.request_generation=v_approval_generation;
      if not found then raise exception 'CANDIDATE_REQUEST_GENERATION_STALE' using errcode='40001'; end if;
    end if;
    v_office_permission:=case
      when v_workflow_action='REMIND' then 'send_manager_reminder'
      when v_workflow_action='RENEW' then 'renew_manager_request'
      when v_workflow_action in ('MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF') then 'cancel_manager_request'
      when v_workflow_action in ('BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','MANAGER_REFUSE')
        then 'manage_phone_approval'
      when v_workflow_action in ('REGISTER_REVIEW_COMPONENT','REGISTER_FINAL_SIGNED_DOCUMENT','BEGIN_CANONICAL_DAILY_SAVE')
        then 'retry_finalisation'
      when v_workflow_action in ('PAPER_PACK_RELEASE','PAPER_PACK_ATTEMPT_CLAIM','PAPER_PACK_MARK_FAILURE')
        then 'manage_paper'
      else null end;
    perform private._candidate_office_service_context_open_v1(
      v_environment,p_actor_user_id,v_office_permission,v_workflow_action,v_observed
    );
    v_result:=public.candidate_workflow_transition_atomic_v1(
      null,v_environment,v_workflow_id,v_workflow_action,v_generation,
      coalesce(v_payload->'payload','{}'::jsonb)||jsonb_build_object(
        'service_office_action',true,'actor_user_id',p_actor_user_id,
        'approval_request_id',v_approval_id,'approval_request_generation',v_approval_generation
      ),v_idempotency_key,v_observed
    );
    perform private._candidate_office_service_context_close_v1();
    if v_workflow_action in ('PAPER_PACK_RELEASE','PAPER_PACK_MARK_FAILURE')
       and nullif(btrim(coalesce(
         v_payload#>>'{payload,paper_pack_operation_id}',''
       )),'') is not null then
      v_idempotency_key:=btrim(v_payload#>>'{payload,paper_pack_operation_id}');
      if v_workflow_action='PAPER_PACK_RELEASE' then
        v_retry_result:=jsonb_build_object(
          'ok',true,
          'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
          'idempotency_key',v_idempotency_key,
          'workflow_id',v_workflow_id,
          'generation',v_generation,
          'paper_pack_state','READY',
          'page_count',coalesce(
            nullif(v_result->>'complete_pack_page_count','')::integer,0
          ),
          'release',jsonb_build_object(
            'ok',coalesce((v_result->>'ok')::boolean,false),
            'workflow_id',v_workflow_id,
            'generation',v_generation,
            'state',v_result->>'state',
            'timesheet_id',v_result->'timesheet_id',
            'mail_outbox_id',v_result->'mail_outbox_id',
            'notification_id',v_result->'notification_id',
            'complete_pack_page_count',coalesce(
              nullif(v_result->>'complete_pack_page_count','')::integer,0
            ),
            'idempotent_replay',coalesce((v_result->>'idempotent_replay')::boolean,false)
          )
        );
        v_retry_receipt:=private._candidate_office_paper_retry_receipt_v1(
          p_actor_user_id,v_workflow_id,v_generation,v_idempotency_key,
          200,v_retry_result,false,v_observed
        );
      else
        v_retry_result:=jsonb_build_object(
          'ok',false,
          'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
          'idempotency_key',v_idempotency_key,
          'workflow_id',v_workflow_id,
          'generation',v_generation,
          'paper_pack_state',v_result->>'paper_pack_state',
          'retryable',coalesce((v_result->>'retryable')::boolean,false),
          'error_code',v_result->>'failure_code',
          'next_retry_at_utc',v_result->'next_retry_at_utc'
        );
        v_retry_receipt:=private._candidate_office_paper_retry_receipt_v1(
          p_actor_user_id,v_workflow_id,v_generation,v_idempotency_key,
          case when coalesce((v_result->>'retryable')::boolean,false) then 503 else 409 end,
          v_retry_result,false,v_observed
        );
      end if;
      v_result:=v_result||jsonb_build_object(
        'office_paper_retry_receipt',v_retry_receipt
      );
    end if;
    v_result:=v_result||jsonb_build_object(
      'contract_version','OFFICE_CANDIDATE_MUTATION_RESULT_V1',
      'refresh_hints',jsonb_build_object(
        'summary',true,'simple_timesheet',true,'bulk_process',true,'bulk_authorise',true,
        'refetch','AFFECTED_ROWS'
      )
    );
    insert into public.audit_events(actor_user_id,object_type,object_id_text,action,before_json,after_json,reason,correlation_id,ts_utc)
    values(p_actor_user_id,'cloudtms_office_candidate_action',v_workflow_id::text,
      'CANDIDATE_OFFICE_'||v_workflow_action,jsonb_build_object('request_sha256',v_request_hash),
      v_result,null,v_idempotency_key,v_observed);
    return v_result;
  elsif v_action='REMINDER_BATCH_REPLAY' then
    v_items:=coalesce(v_payload->'identities','[]'::jsonb);
    v_batch_id:=nullif(v_payload->>'batch_id','')::uuid;
    v_idempotency_key:=nullif(btrim(coalesce(v_payload->>'idempotency_key','')),'');
    if jsonb_typeof(v_items)<>'array' or jsonb_array_length(v_items)<1 or jsonb_array_length(v_items)>1000
       or v_batch_id is null or v_idempotency_key is null or v_batch_id::text<>v_idempotency_key
       or coalesce(v_payload->>'preview_context_hash','') !~ '^[0-9a-fA-F]{64}$'
       or coalesce(v_payload->>'selection_fingerprint','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'CANDIDATE_REMINDER_BATCH_SELECTION_INVALID' using errcode='22023';
    end if;
    if (select count(*)<>count(distinct coalesce(value->>'row_key',value->>'timesheet_id',value->>'contract_week_id'))
        from jsonb_array_elements(v_items)) then
      raise exception 'CANDIDATE_REMINDER_BATCH_DUPLICATE_IDENTITY' using errcode='22023';
    end if;
    v_client_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
      'identities',v_items,'batch_id',v_batch_id,'idempotency_key',v_idempotency_key,
      'preview_context_hash',lower(v_payload->>'preview_context_hash'),
      'selection_fingerprint',lower(v_payload->>'selection_fingerprint')
    )::text,'UTF8'),'sha256'),'hex');
    select ae.before_json,ae.after_json into v_existing_before,v_existing_after
    from public.audit_events ae where ae.object_type='cloudtms_office_candidate_reminder_batch'
      and ae.object_id_text=v_batch_id::text and ae.actor_user_id=p_actor_user_id
    order by ae.ts_utc desc,ae.id desc limit 1;
    if not found then
      return jsonb_build_object('ok',true,'found',false,'batch_id',v_batch_id);
    end if;
    if v_existing_before->>'client_request_sha256' is distinct from v_client_request_hash then
      raise exception 'IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    return coalesce(v_existing_after,'{}'::jsonb)||jsonb_build_object('found',true,'idempotent_replay',true);
  elsif v_action in ('REMINDER_BATCH_PREVIEW','REMINDER_BATCH_EXECUTE') then
    v_items:=coalesce(v_payload->'identities','[]'::jsonb);
    if jsonb_typeof(v_items)<>'array' or jsonb_array_length(v_items)<1 or jsonb_array_length(v_items)>1000 then
      raise exception 'CANDIDATE_REMINDER_BATCH_SELECTION_INVALID' using errcode='22023';
    end if;
    if (select count(*)<>count(distinct coalesce(value->>'row_key',value->>'timesheet_id',value->>'contract_week_id'))
        from jsonb_array_elements(v_items)) then
      raise exception 'CANDIDATE_REMINDER_BATCH_DUPLICATE_IDENTITY' using errcode='22023';
    end if;
    if v_action='REMINDER_BATCH_EXECUTE' then
      v_batch_id:=nullif(v_payload->>'batch_id','')::uuid;
      v_idempotency_key:=nullif(btrim(coalesce(v_payload->>'idempotency_key','')),'');
      if v_batch_id is null or v_idempotency_key is null or v_batch_id::text<>v_idempotency_key then
        raise exception 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED' using errcode='40001';
      end if;
      v_client_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
        'identities',v_items,'batch_id',v_batch_id,'idempotency_key',v_idempotency_key,
        'preview_context_hash',lower(v_payload->>'preview_context_hash'),
        'selection_fingerprint',lower(v_payload->>'selection_fingerprint')
      )::text,'UTF8'),'sha256'),'hex');
      v_request_hash:=encode(extensions.digest(convert_to((v_payload-'request_id')::text,'UTF8'),'sha256'),'hex');
      perform pg_advisory_xact_lock(hashtextextended('CANDIDATE_OFFICE_REMINDER_BATCH:'||p_actor_user_id::text||':'||v_batch_id::text,0));
      select ae.before_json,ae.after_json into v_existing_before,v_existing_after
      from public.audit_events ae where ae.object_type='cloudtms_office_candidate_reminder_batch'
        and ae.object_id_text=v_batch_id::text and ae.actor_user_id=p_actor_user_id
      order by ae.ts_utc desc,ae.id desc limit 1;
      if found then
        if v_existing_before->>'request_sha256' is distinct from v_request_hash then
          raise exception 'IDEMPOTENCY_CONFLICT' using errcode='23505';
        end if;
        return coalesce(v_existing_after,'{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
      end if;
    end if;
    for v_item in select value from jsonb_array_elements(v_items) with ordinality x(value,ordinality)
    loop
      begin
        v_result:=private._candidate_office_timesheet_projection_v1(
          v_environment,nullif(v_item->>'timesheet_id','')::uuid,
          nullif(v_item->>'contract_week_id','')::uuid,v_item->>'row_key',
          v_item->>'expected_row_signature',p_actor_user_id,v_observed
        );
        v_preview:=(select action_item from jsonb_array_elements(v_result->'available_actions') action_item
          where action_item->>'code'='SEND_MANAGER_REMINDER' limit 1);
        v_results:=v_results||jsonb_build_array(jsonb_build_object(
          'correlation_key',coalesce(v_item->>'row_key',v_item->>'timesheet_id',v_item->>'contract_week_id'),
          'eligible',coalesce((v_preview->>'enabled')::boolean,false),
          'disabled_reason_code',v_preview->>'disabled_reason_code',
          'workflow_id',v_result#>>'{workflow,workflow_id}',
          'workflow_generation',v_result#>>'{workflow,generation}',
          'approval_request_id',v_result#>>'{manager_approval,request_id}',
          'approval_request_generation',v_result#>>'{manager_approval,request_generation}',
          'row_signature',v_result#>>'{current_identity,row_signature}'
        ));
      exception when others then
        v_error_code:=coalesce(nullif(substring(sqlerrm from '([A-Z][A-Z0-9_]{2,})'),''),'CANDIDATE_OFFICE_PROJECTION_FAILED');
        v_results:=v_results||jsonb_build_array(jsonb_build_object(
          'correlation_key',coalesce(v_item->>'row_key',v_item->>'timesheet_id',v_item->>'contract_week_id'),
          'eligible',false,'disabled_reason_code',v_error_code
        ));
      end;
    end loop;
    v_selection_fingerprint:=encode(extensions.digest(convert_to(v_results::text,'UTF8'),'sha256'),'hex');
    if v_action='REMINDER_BATCH_PREVIEW' then
      return jsonb_build_object(
        'ok',true,'contract_version','OFFICE_CANDIDATE_REMINDER_BATCH_PREVIEW_V1',
        'batch_version','OFFICE_CANDIDATE_REMINDER_BATCH_V1','observed_at_utc',v_observed,
        'preview_context_hash',v_selection_fingerprint,
        'selection_fingerprint',v_selection_fingerprint,
        'selected_count',jsonb_array_length(v_results),
        'eligible_count',(select count(*) from jsonb_array_elements(v_results) r where (r->>'eligible')::boolean),
        'ineligible_count',(select count(*) from jsonb_array_elements(v_results) r where not (r->>'eligible')::boolean),
        'changed_count',(select count(*) from jsonb_array_elements(v_results) r
          where r->>'disabled_reason_code' in ('TIMESHEET_MOVED','ROW_SIGNATURE_MISMATCH','CANDIDATE_CONTEXT_STALE')),
        'items',v_results
      );
    end if;
    if lower(coalesce(v_payload->>'preview_context_hash','')) is distinct from v_selection_fingerprint
       or lower(coalesce(v_payload->>'selection_fingerprint','')) is distinct from v_selection_fingerprint then
      raise exception 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED' using errcode='40001';
    end if;
    v_items:=coalesce(v_payload->'reminders','[]'::jsonb);
    if jsonb_typeof(v_items)<>'array'
       or jsonb_array_length(v_items)<>jsonb_array_length(v_results)
       or exists(
         select 1
         from jsonb_array_elements(v_items) supplied
         where not exists(
           select 1
           from jsonb_array_elements(v_results) current_item
           where current_item->>'correlation_key'=supplied->>'correlation_key'
             and current_item->>'eligible'=supplied->>'eligible'
             and current_item->>'workflow_id' is not distinct from supplied->>'workflow_id'
             and current_item->>'workflow_generation' is not distinct from supplied->>'workflow_generation'
             and current_item->>'approval_request_id' is not distinct from supplied->>'approval_request_id'
             and current_item->>'approval_request_generation' is not distinct from supplied->>'approval_request_generation'
         )
       ) then
      raise exception 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED' using errcode='40001';
    end if;
    v_results:='[]'::jsonb;
    for v_item in select value from jsonb_array_elements(v_items) with ordinality x(value,ordinality)
    loop
      if not coalesce((v_item->>'eligible')::boolean,false) then
        v_skipped_count:=v_skipped_count+1;
        v_results:=v_results||jsonb_build_array(v_item||jsonb_build_object('outcome','SKIPPED'));
        continue;
      end if;
      begin
        perform private._candidate_office_service_context_open_v1(
          v_environment,p_actor_user_id,'send_manager_reminder_batch','REMIND',v_observed
        );
        v_result:=public.candidate_workflow_transition_atomic_v1(
          null,v_environment,(v_item->>'workflow_id')::uuid,'REMIND',(v_item->>'workflow_generation')::integer,
          coalesce(v_item->'payload','{}'::jsonb)||jsonb_build_object(
            'service_office_action',true,'actor_user_id',p_actor_user_id,
            'approval_request_id',(v_item->>'approval_request_id')::uuid,
            'approval_request_generation',(v_item->>'approval_request_generation')::integer
          ),'office-reminder-batch:'||v_batch_id::text||':'||(v_item->>'workflow_id')||':'||(v_item->>'approval_request_generation'),v_observed
        );
        perform private._candidate_office_service_context_close_v1();
        v_success_count:=v_success_count+1;
        v_results:=v_results||jsonb_build_array(jsonb_build_object(
          'correlation_key',v_item->>'correlation_key','workflow_id',v_item->>'workflow_id',
          'approval_request_id',v_item->>'approval_request_id','outcome','QUEUED','result',v_result
        ));
      exception when others then
        perform private._candidate_office_service_context_close_v1();
        v_failure_count:=v_failure_count+1;
        v_error_code:=coalesce(nullif(substring(sqlerrm from '([A-Z][A-Z0-9_]{2,})'),''),'CANDIDATE_REMINDER_BATCH_ITEM_FAILED');
        v_results:=v_results||jsonb_build_array(jsonb_build_object(
          'correlation_key',v_item->>'correlation_key','workflow_id',v_item->>'workflow_id',
          'approval_request_id',v_item->>'approval_request_id','outcome','FAILED',
          'error_code',v_error_code,'retryable',sqlstate in ('40001','40P01')
        ));
      end;
    end loop;
    v_result:=jsonb_build_object(
      -- The batch operation itself completed durably even when one or more
      -- independently fenced items failed. Item outcome truth is represented
      -- by status/counts/items; top-level ok=false is reserved for a request or
      -- transaction that did not produce a durable batch result.
      'ok',true,'contract_version','OFFICE_CANDIDATE_REMINDER_BATCH_RESULT_V1',
      'batch_version','OFFICE_CANDIDATE_REMINDER_BATCH_V1','batch_id',v_batch_id,
      'status',case when v_failure_count=0 then 'COMPLETED' when v_success_count>0 then 'PARTIAL' else 'FAILED' end,
      'observed_at_utc',v_observed,'completed_at_utc',v_observed,
      'selection_fingerprint',v_selection_fingerprint,
      'success_count',v_success_count,'failure_count',v_failure_count,'skipped_count',v_skipped_count,
      'items',v_results,'idempotent_replay',false
    );
    insert into public.audit_events(actor_user_id,object_type,object_id_text,action,before_json,after_json,correlation_id,ts_utc)
    values(p_actor_user_id,'cloudtms_office_candidate_reminder_batch',v_batch_id::text,
      'CANDIDATE_OFFICE_REMINDER_BATCH_COMPLETED',jsonb_build_object(
        'request_sha256',v_request_hash,'client_request_sha256',v_client_request_hash
      ),
      v_result,v_idempotency_key,v_observed);
    return v_result;
  elsif v_action='REMINDER_BATCH_STATUS' then
    v_batch_id:=nullif(v_payload->>'batch_id','')::uuid;
    if v_batch_id is null then raise exception 'CANDIDATE_REMINDER_BATCH_NOT_FOUND' using errcode='P0002'; end if;
    select ae.after_json into v_result from public.audit_events ae
    where ae.object_type='cloudtms_office_candidate_reminder_batch'
      and ae.object_id_text=v_batch_id::text and ae.actor_user_id=p_actor_user_id
    order by ae.ts_utc desc,ae.id desc limit 1;
    if not found then raise exception 'CANDIDATE_REMINDER_BATCH_NOT_FOUND' using errcode='P0002'; end if;
    return v_result;
  end if;
  raise exception 'CANDIDATE_OFFICE_ADAPTER_ACTION_INVALID' using errcode='22023';
end;
$function$;

alter function private._candidate_office_capabilities_v1(text,uuid,timestamptz) owner to postgres;
alter function private._candidate_office_action_v1(text,text,text,boolean,text,text,boolean,boolean,text,text,jsonb,jsonb,text,boolean) owner to postgres;
alter function private._candidate_office_reject_preview_v1(text,uuid,uuid,timestamptz) owner to postgres;
alter function private._candidate_office_projection_identity_v1(uuid,uuid) owner to postgres;
alter function private._candidate_office_timesheet_projection_v1(text,uuid,uuid,text,text,uuid,timestamptz) owner to postgres;
alter function public.cloudtms_office_candidate_adapter_v1(text,uuid,text,jsonb,timestamptz) owner to postgres;

revoke all on function private._candidate_office_capabilities_v1(text,uuid,timestamptz) from public,anon,authenticated,service_role;
revoke all on function private._candidate_office_action_v1(text,text,text,boolean,text,text,boolean,boolean,text,text,jsonb,jsonb,text,boolean) from public,anon,authenticated,service_role;
revoke all on function private._candidate_office_reject_preview_v1(text,uuid,uuid,timestamptz) from public,anon,authenticated,service_role;
revoke all on function private._candidate_office_projection_identity_v1(uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function private._candidate_office_timesheet_projection_v1(text,uuid,uuid,text,text,uuid,timestamptz) from public,anon,authenticated,service_role;
revoke all on function private._candidate_office_paper_retry_receipt_v1(uuid,uuid,integer,text,integer,jsonb,boolean,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.cloudtms_office_candidate_adapter_v1(text,uuid,text,jsonb,timestamptz) from public,anon,authenticated;
grant execute on function public.cloudtms_office_candidate_adapter_v1(text,uuid,text,jsonb,timestamptz) to service_role;

comment on function public.cloudtms_office_candidate_adapter_v1(text,uuid,text,jsonb,timestamptz) is
  'Service-role-only normal CloudTMS office Candidate adapter. Not a Candidate client business RPC. Owns bounded projections, closed rejection confirmation, typed office workflow actions, and durable manager-reminder batches.';
