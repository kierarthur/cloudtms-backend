-- Repeatable CloudTMS authority: weekly_source_completed_pack_copy_v1
--
-- Owns the optional informational copy of a completed Weekly Timesheet pack.
-- It is deliberately independent of Candidate/manager query messages and has
-- no Timesheet authorisation, validation, pay, invoice, Workbench or Banking
-- Pay effect.  One immutable completion generation owns at most one copy,
-- even if the physical Timesheet id later rotates inside the booking family.

\set ON_ERROR_STOP on

begin;

-- The generic mail claimant runs as service_role, which must not receive
-- direct SELECT on Weekly Source fact tables.  This narrow owner-owned
-- predicate validates only the event binding for an informational copy.
create or replace function private._weekly_source_completed_pack_copy_claim_event_valid_v1(
  p_mail public.mail_outbox
) returns boolean
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_valid boolean:=false;
begin
  if p_mail.id is null
    or p_mail.email_type is distinct from 'WEEKLY_COMPLETED_TIMESHEET_COPY'
  then
    return false;
  end if;

  select exists (
    select 1
    from public.weekly_completed_pack_copy_events event
    join public.timesheets event_timesheet
      on event_timesheet.timesheet_id=event.timesheet_id
    join public.candidate_submission_workflows workflow
      on workflow.id::text=p_mail.payment_scope_json->>'candidate_workflow_id'
    where event.id::text=p_mail.payment_scope_json->>'completed_pack_copy_event_id'
      and event.id::text=p_mail.attachments->0->>'completed_pack_copy_event_id'
      and event.state='READY'
      and event.timesheet_id=p_mail.context_id
      and event.timesheet_id::text=p_mail.payment_scope_json->>'timesheet_id'
      and event.timesheet_revision=case
        when coalesce(p_mail.payment_scope_json->>'timesheet_revision','')
          ~ '^[1-9][0-9]{0,8}$'
        then (p_mail.payment_scope_json->>'timesheet_revision')::integer end
      and btrim(event_timesheet.booking_id)
        =p_mail.payment_scope_json->>'timesheet_family'
      and event_timesheet.contract_id=workflow.contract_id
      and event.completion_generation=case
        when coalesce(p_mail.payment_scope_json->>'candidate_workflow_generation','')
          ~ '^[1-9][0-9]{0,8}$'
        then (p_mail.payment_scope_json->>'candidate_workflow_generation')::integer end
      and workflow.generation=event.completion_generation
      and workflow.id::text=p_mail.attachments->0->>'candidate_workflow_id'
      and p_mail.payment_scope_json->>'candidate_workflow_generation'
        =p_mail.attachments->0->>'candidate_workflow_generation'
      and workflow.candidate_signed_at_utc is not null
      and ((event.document_mode='CHECK_ONLY'
            and workflow.state='WORKER_SUBMITTED')
        or (event.document_mode='INVOICE_EVIDENCE_REQUIRED'
            and workflow.state='FINALISED'))
      and event.document_mode=p_mail.payment_scope_json->>'document_mode'
      and event.recipient_snapshot=lower(btrim(p_mail."to"))
      and encode(event.final_document_hash,'hex')
        =lower(p_mail.attachments->0->>'sha256')
      and lower(p_mail.attachments->0->>'sha256')
        =lower(p_mail.payment_scope_json->>'final_document_sha256')
      and p_mail.attachment_total_bytes=case
        when coalesce(p_mail.attachments->0->>'size_bytes','')
          ~ '^[1-9][0-9]{0,18}$'
        then (p_mail.attachments->0->>'size_bytes')::bigint end
  ) into v_valid;
  return v_valid;
end;
$function$;

alter function private._weekly_source_completed_pack_copy_claim_event_valid_v1(public.mail_outbox)
  owner to postgres;
revoke all on function private._weekly_source_completed_pack_copy_claim_event_valid_v1(public.mail_outbox)
  from public,anon,authenticated,service_role;
grant execute on function private._weekly_source_completed_pack_copy_claim_event_valid_v1(public.mail_outbox)
  to service_role;

create or replace function private._weekly_source_completed_pack_copy_eligibility_v1(
  p_workflow_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_anchor public.timesheets%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_candidate public.candidates%rowtype;
  v_client public.clients%rowtype;
  v_policy jsonb;
  v_document_mode text;
  v_artifact_generation integer;
  v_recipient text;
  v_component_manifest jsonb;
  v_signature_component_id uuid;
  v_render_input jsonb;
begin
  if p_workflow_id is null then
    return null;
  end if;

  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id
    and workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
    and workflow.scope='WEEKLY';
  if not found or v_workflow.contract_id is null
     or coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id) is null then
    return null;
  end if;

  select timesheet.* into v_anchor
  from public.timesheets timesheet
  where timesheet.timesheet_id=coalesce(
    v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id
  );
  if not found then return null; end if;

  select timesheet.* into v_timesheet
  from public.timesheets timesheet
  where pg_catalog.btrim(timesheet.booking_id)=pg_catalog.btrim(v_anchor.booking_id)
    and timesheet.is_current
  order by timesheet.updated_at desc,timesheet.version desc,timesheet.timesheet_id
  limit 1;
  if not found then return null; end if;

  select contract.* into v_contract
  from public.contracts contract where contract.id=v_workflow.contract_id;
  if not found then return null; end if;
  select candidate.* into v_candidate
  from public.candidates candidate where candidate.id=v_workflow.candidate_id;
  if not found then return null; end if;
  select client.* into v_client
  from public.clients client where client.id=v_contract.client_id;
  if not found then return null; end if;

  -- Ordinary weekly Timesheets have no Weekly Source configuration.  They
  -- must not make this source-only copy sweep fail before it reaches an
  -- eligible client.  A partially configured source client still proceeds
  -- to the authoritative policy resolver and fails closed there.
  if v_contract.weekly_timesheet_source is null and not exists (
    select 1 from public.weekly_source_group_clients membership
    where membership.client_id=v_contract.client_id
  ) and not exists (
    select 1 from public.weekly_source_client_policies policy
    where policy.client_id=v_contract.client_id
  ) and not exists (
    select 1 from public.weekly_source_contract_policies policy
    where policy.contract_id=v_contract.id
  ) then
    return null;
  end if;

  v_policy:=private._weekly_source_effective_policy_v1(
    v_contract.client_id,v_contract.id,
    coalesce(v_workflow.week_ending_date,v_timesheet.week_ending_date)
  );
  if not coalesce((v_policy->>'completed_pack_copy_enabled')::boolean,false) then
    return null;
  end if;
  v_recipient:=pg_catalog.lower(pg_catalog.btrim(coalesce(
    v_policy->>'completed_pack_recipient',''
  )));
  if v_recipient='' or v_recipient !~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' then
    return null;
  end if;

  v_document_mode:=pg_catalog.upper(pg_catalog.btrim(coalesce(
    v_policy->>'document_mode',''
  )));
  if v_document_mode='CHECK_ONLY' then
    if v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
       or v_workflow.route<>'ELECTRONIC'
       or v_workflow.state<>'WORKER_SUBMITTED'
       or v_workflow.candidate_signed_at_utc is null
       or v_workflow.candidate_signature_component_id is null
       or v_workflow.candidate_signature_sha256 is null then
      return null;
    end if;
    v_artifact_generation:=v_workflow.generation;
    select component.id into v_signature_component_id
    from public.candidate_submission_components component
    where component.id=v_workflow.candidate_signature_component_id
      and component.workflow_id=v_workflow.id
      and component.workflow_generation=v_artifact_generation
      and component.component_kind='CANDIDATE_SIGNATURE'
      and component.document_role='CANDIDATE_SIGNATURE'
      and component.state='IMMUTABLE'
      and component.source_content_sha256=v_workflow.candidate_signature_sha256;
    if not found then return null; end if;
    v_component_manifest:=pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'component_id',v_signature_component_id,
        'component_kind','CANDIDATE_SIGNATURE',
        'content_sha256',pg_catalog.encode(v_workflow.candidate_signature_sha256,'hex')
      )
    );
  elsif v_document_mode='INVOICE_EVIDENCE_REQUIRED' then
    if v_policy->>'authority_mode'<>'TIMESHEET_AUTHORITY'
       or v_workflow.state<>'FINALISED'
       or v_workflow.candidate_signed_at_utc is null
       or v_workflow.candidate_signature_component_id is null
       or v_workflow.candidate_signature_sha256 is null
       or v_workflow.manager_approved_at_utc is null
       or v_workflow.manager_signature_component_id is null
       or v_workflow.manager_signature_sha256 is null then
      return null;
    end if;
    v_artifact_generation:=greatest(v_workflow.generation-1,1);
    if not exists(
      select 1 from public.candidate_submission_components component
      where component.workflow_id=v_workflow.id
        and component.workflow_generation=v_artifact_generation
        and component.required and component.state='IMMUTABLE'
    ) or exists(
      select 1 from public.candidate_submission_components component
      where component.workflow_id=v_workflow.id
        and component.workflow_generation=v_artifact_generation
        and component.required and component.state<>'SUPERSEDED'
        and (
          component.final_signed_render_state<>'READY'
          or component.final_signed_storage_key is null
          or component.final_signed_content_sha256 is null
          or component.final_signed_media_type<>'application/pdf'
          or coalesce(component.final_signed_byte_size,0)<=0
          or coalesce(component.final_signed_page_count,0)<=0
        )
    ) then
      return null;
    end if;
    select pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'component_id',component.id,
        'component_kind',component.component_kind,
        'expense_category',component.expense_category,
        'document_role',component.document_role,
        'review_ordinal',component.review_ordinal,
        'storage_key',component.final_signed_storage_key,
        'content_sha256',pg_catalog.encode(component.final_signed_content_sha256,'hex'),
        'media_type',component.final_signed_media_type,
        'byte_size',component.final_signed_byte_size,
        'page_count',component.final_signed_page_count
      ) order by component.review_ordinal,component.id
    ) into v_component_manifest
    from public.candidate_submission_components component
    where component.workflow_id=v_workflow.id
      and component.workflow_generation=v_artifact_generation
      and component.required and component.state='IMMUTABLE';
  else
    -- IMPORT_ONLY never has a Candidate-completed pack to copy.
    return null;
  end if;

  v_render_input:=pg_catalog.jsonb_build_object(
    'contract_version','WEEKLY_COMPLETED_PACK_COPY_V1',
    'workflow_id',v_workflow.id,
    'workflow_generation',v_artifact_generation,
    'workflow_state',v_workflow.state,
    'document_mode',v_document_mode,
    'timesheet_family',pg_catalog.btrim(v_timesheet.booking_id),
    'timesheet_id',v_timesheet.timesheet_id,
    'timesheet_revision',v_timesheet.version,
    'week_ending_date',v_timesheet.week_ending_date,
    'candidate_id',v_candidate.id,
    'client_id',v_client.id,
    'contract_id',v_contract.id,
    'recipient',v_recipient,
    'policy_sha256',v_policy->>'policy_sha256',
    'immutable_submission_sha256',case when v_workflow.immutable_submission_sha256 is null
      then null else pg_catalog.encode(v_workflow.immutable_submission_sha256,'hex') end,
    'candidate_signature_sha256',case when v_workflow.candidate_signature_sha256 is null
      then null else pg_catalog.encode(v_workflow.candidate_signature_sha256,'hex') end,
    'manager_signature_sha256',case when v_workflow.manager_signature_sha256 is null
      then null else pg_catalog.encode(v_workflow.manager_signature_sha256,'hex') end,
    'components',coalesce(v_component_manifest,'[]'::jsonb)
  );

  return v_render_input || pg_catalog.jsonb_build_object(
    'render_input_sha256',pg_catalog.encode(
      private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_COMPLETED_PACK_COPY_RENDER_INPUT_V1',v_render_input
      ),'hex'
    ),
    'candidate_display_name',coalesce(
      nullif(pg_catalog.btrim(v_candidate.display_name),''),
      nullif(pg_catalog.btrim(pg_catalog.concat_ws(' ',v_candidate.first_name,v_candidate.last_name)),''),
      'Candidate'
    ),
    'client_display_name',coalesce(nullif(pg_catalog.btrim(v_client.name),''),'Client'),
    'candidate_signature_component_id',v_signature_component_id
  );
exception
  when no_data_found or too_many_rows then
    return null;
end;
$function$;

create or replace function public.weekly_source_completed_pack_copy_status_sync_v1(
  p_request jsonb
) returns jsonb
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_updated integer:=0;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) supplied(key)
       where supplied.key not in ('limit')
     ) then
    raise exception 'WEEKLY_COMPLETED_PACK_STATUS_REQUEST_INVALID' using errcode='22023';
  end if;
  with candidates as (
    select event.id,
      case
        when outbox.status='SENT' then 'SENT'
        when outbox.status='FAILED' then 'FAILED'
        else 'READY'
      end as next_state
    from public.weekly_completed_pack_copy_events event
    join public.mail_outbox outbox
      on outbox.payment_scope_json->>'completed_pack_copy_event_id'=event.id::text
    where event.state is distinct from case
      when outbox.status='SENT' then 'SENT'
      when outbox.status='FAILED' then 'FAILED'
      else 'READY'
    end
    order by event.created_at_utc,event.id
    limit least(greatest(
      coalesce((p_request->>'limit')::integer,100),1
    ),500)
  )
  update public.weekly_completed_pack_copy_events event
  set state=candidates.next_state
  from candidates where event.id=candidates.id;
  get diagnostics v_updated=row_count;
  return pg_catalog.jsonb_build_object('ok',true,'updated_count',v_updated);
end;
$function$;

create or replace function public.weekly_source_completed_pack_copy_due_list_v1(
  p_request jsonb
) returns jsonb
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_limit integer;
  v_items jsonb;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) supplied(key)
       where supplied.key not in ('limit')
     ) then
    raise exception 'WEEKLY_COMPLETED_PACK_DUE_REQUEST_INVALID' using errcode='22023';
  end if;
  v_limit:=least(greatest(
    coalesce((p_request->>'limit')::integer,25),1
  ),100);

  with eligible as (
    select workflow.id as workflow_id,
      private._weekly_source_completed_pack_copy_eligibility_v1(workflow.id) as facts
    from public.candidate_submission_workflows workflow
    where workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
      and workflow.scope='WEEKLY'
      and workflow.state in ('WORKER_SUBMITTED','FINALISED')
    order by workflow.updated_at_utc,workflow.id
  ), due as (
    select eligible.workflow_id,eligible.facts
    from eligible
    where eligible.facts is not null
      and not exists(
        select 1
        from public.weekly_completed_pack_copy_events event
        join public.timesheets event_timesheet
          on event_timesheet.timesheet_id=event.timesheet_id
        where pg_catalog.btrim(event_timesheet.booking_id)=eligible.facts->>'timesheet_family'
          and event.completion_generation=(eligible.facts->>'workflow_generation')::integer
          and event.document_mode=eligible.facts->>'document_mode'
      )
    order by eligible.workflow_id
    limit v_limit
  )
  select coalesce(pg_catalog.jsonb_agg(
    due.facts || pg_catalog.jsonb_build_object('workflow_id',due.workflow_id)
    order by due.workflow_id
  ),'[]'::jsonb) into v_items
  from due;
  return pg_catalog.jsonb_build_object(
    'ok',true,'count',pg_catalog.jsonb_array_length(v_items),'items',v_items
  );
end;
$function$;

create or replace function public.weekly_source_completed_pack_copy_commit_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_workflow_id uuid;
  v_facts jsonb;
  v_timesheet public.timesheets%rowtype;
  v_existing public.weekly_completed_pack_copy_events%rowtype;
  v_event public.weekly_completed_pack_copy_events%rowtype;
  v_outbox public.mail_outbox%rowtype;
  v_hash bytea;
  v_hash_hex text;
  v_storage_key text;
  v_filename text;
  v_media_type text;
  v_byte_size bigint;
  v_page_count integer;
  v_now timestamptz:=pg_catalog.transaction_timestamp();
  v_outbox_key text;
  v_subject text;
  v_body_text text;
  v_body_html text;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) supplied(key)
       where supplied.key not in (
         'workflow_id','render_input_sha256','storage_key','final_document_sha256',
         'filename','media_type','byte_size','page_count','content_policy_version'
       )
     ) then
    raise exception 'WEEKLY_COMPLETED_PACK_COMMIT_REQUEST_INVALID' using errcode='22023';
  end if;
  v_workflow_id:=nullif(p_request->>'workflow_id','')::uuid;
  v_hash_hex:=pg_catalog.lower(pg_catalog.btrim(coalesce(
    p_request->>'final_document_sha256',''
  )));
  v_storage_key:=pg_catalog.btrim(coalesce(p_request->>'storage_key',''));
  v_filename:=pg_catalog.btrim(coalesce(p_request->>'filename',''));
  v_media_type:=pg_catalog.lower(pg_catalog.btrim(coalesce(p_request->>'media_type','')));
  v_byte_size:=coalesce((p_request->>'byte_size')::bigint,0);
  v_page_count:=coalesce((p_request->>'page_count')::integer,0);
  if v_workflow_id is null
     or coalesce(p_request->>'render_input_sha256','') !~ '^[0-9a-f]{64}$'
     or v_hash_hex !~ '^[0-9a-f]{64}$'
     or v_storage_key='' or v_filename='' or v_media_type<>'application/pdf'
     or v_byte_size<=0 or v_page_count<=0
     or p_request->>'content_policy_version'<>'WEEKLY_COMPLETED_PACK_COPY_CONTENT_V1' then
    raise exception 'WEEKLY_COMPLETED_PACK_COMMIT_REQUEST_INVALID' using errcode='22023';
  end if;

  v_facts:=private._weekly_source_completed_pack_copy_eligibility_v1(v_workflow_id);
  if v_facts is null
     or v_facts->>'render_input_sha256'<>p_request->>'render_input_sha256' then
    raise exception 'WEEKLY_COMPLETED_PACK_COPY_STALE' using errcode='40001';
  end if;

  select timesheet.* into strict v_timesheet
  from public.timesheets timesheet
  where timesheet.timesheet_id=(v_facts->>'timesheet_id')::uuid;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext(
    'WEEKLY_COMPLETED_PACK_COPY:'||pg_catalog.btrim(v_timesheet.booking_id)
  ));

  select event.* into v_existing
  from public.weekly_completed_pack_copy_events event
  join public.timesheets event_timesheet on event_timesheet.timesheet_id=event.timesheet_id
  where pg_catalog.btrim(event_timesheet.booking_id)=pg_catalog.btrim(v_timesheet.booking_id)
    and event.completion_generation=(v_facts->>'workflow_generation')::integer
    and event.document_mode=v_facts->>'document_mode'
  order by event.created_at_utc,event.id
  limit 1
  for update of event;
  if found then
    select outbox.* into v_outbox
    from public.mail_outbox outbox
    where outbox.payment_scope_json->>'completed_pack_copy_event_id'=v_existing.id::text
    order by outbox.created_at_utc,outbox.id limit 1;
    return pg_catalog.jsonb_build_object(
      'ok',true,'idempotent_replay',true,'event_id',v_existing.id,
      'mail_outbox_id',v_outbox.id,'state',v_existing.state
    );
  end if;

  v_hash:=pg_catalog.decode(v_hash_hex,'hex');
  insert into public.weekly_completed_pack_copy_events(
    document_mode,timesheet_id,timesheet_revision,final_document_hash,
    completion_generation,recipient_snapshot,content_policy_version,state
  ) values (
    v_facts->>'document_mode',(v_facts->>'timesheet_id')::uuid,
    (v_facts->>'timesheet_revision')::integer,v_hash,
    (v_facts->>'workflow_generation')::integer,v_facts->>'recipient',
    p_request->>'content_policy_version','READY'
  ) returning * into v_event;

  v_subject:='Completed Timesheet for '||(v_facts->>'candidate_display_name')
    ||' - week ending '||pg_catalog.to_char(
      (v_facts->>'week_ending_date')::date,'DD/MM/YYYY'
    );
  v_body_text:='A completed Timesheet for '||(v_facts->>'candidate_display_name')
    ||' is attached for your information.'||pg_catalog.chr(10)||pg_catalog.chr(10)
    ||'Client: '||(v_facts->>'client_display_name')||pg_catalog.chr(10)
    ||'Week ending: '||pg_catalog.to_char((v_facts->>'week_ending_date')::date,'DD/MM/YYYY')
    ||pg_catalog.chr(10)||pg_catalog.chr(10)
    ||'No approval, signature or other action is required.';
  v_body_html:='<p>A completed Timesheet for <strong>'
    ||pg_catalog.replace(pg_catalog.replace(pg_catalog.replace(
      v_facts->>'candidate_display_name','&','&amp;'),'<','&lt;'),'>','&gt;')
    ||'</strong> is attached for your information.</p><p><strong>Client:</strong> '
    ||pg_catalog.replace(pg_catalog.replace(pg_catalog.replace(
      v_facts->>'client_display_name','&','&amp;'),'<','&lt;'),'>','&gt;')
    ||'<br><strong>Week ending:</strong> '
    ||pg_catalog.to_char((v_facts->>'week_ending_date')::date,'DD/MM/YYYY')
    ||'</p><p>No approval, signature or other action is required.</p>';
  v_outbox_key:='WEEKLY_COMPLETED_PACK_COPY:'||v_event.id::text||':'||v_hash_hex;

  insert into public.mail_outbox(
    type,"to",subject,body_html,body_text,attachments,status,reference,
    recipient_kind,recipient_id,context_kind,context_id,email_type,
    scheduled_for_utc,next_attempt_at_utc,deterministic_outbox_key,
    payment_scope_json,attachments_ready,attachment_total_bytes,
    attachment_delivery_policy
  ) values (
    'TIMESHEET_GENERAL',v_facts->>'recipient',v_subject,v_body_html,v_body_text,
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'r2_key',v_storage_key,'filename',v_filename,'content_type','application/pdf',
      'sha256',v_hash_hex,'size_bytes',v_byte_size,'page_count',v_page_count,
      'completed_pack_copy_event_id',v_event.id,
      'candidate_workflow_id',v_workflow_id,
      'candidate_workflow_generation',(v_facts->>'workflow_generation')::integer
    )),
    'QUEUED'::public.mail_status_enum,
    'weekly-completed-pack-copy:'||v_event.id::text,
    'CLIENT_INFORMATIONAL_COPY',(v_facts->>'client_id')::uuid,
    'timesheets',(v_facts->>'timesheet_id')::uuid,
    'WEEKLY_COMPLETED_TIMESHEET_COPY',v_now,v_now,v_outbox_key,
    pg_catalog.jsonb_build_object(
      'completed_pack_copy_authority','WEEKLY_COMPLETED_PACK_COPY_V1',
      'completed_pack_copy_event_id',v_event.id,
      'candidate_workflow_id',v_workflow_id,
      'candidate_workflow_generation',(v_facts->>'workflow_generation')::integer,
      'timesheet_family',v_facts->>'timesheet_family',
      'timesheet_id',v_facts->>'timesheet_id',
      'timesheet_revision',(v_facts->>'timesheet_revision')::integer,
      'document_mode',v_facts->>'document_mode',
      'render_input_sha256',v_facts->>'render_input_sha256',
      'final_document_sha256',v_hash_hex,
      'informational_only',true,
      'changes_validation',false,'changes_pay',false,'changes_invoice',false
    ),true,v_byte_size,'ATTACH'
  ) on conflict (deterministic_outbox_key) do update
    set deterministic_outbox_key=excluded.deterministic_outbox_key
  returning * into v_outbox;

  return pg_catalog.jsonb_build_object(
    'ok',true,'idempotent_replay',false,'event_id',v_event.id,
    'mail_outbox_id',v_outbox.id,'state',v_event.state
  );
end;
$function$;

alter function private._weekly_source_completed_pack_copy_eligibility_v1(uuid)
  owner to postgres;
alter function public.weekly_source_completed_pack_copy_status_sync_v1(jsonb)
  owner to postgres;
alter function public.weekly_source_completed_pack_copy_due_list_v1(jsonb)
  owner to postgres;
alter function public.weekly_source_completed_pack_copy_commit_atomic_v1(jsonb)
  owner to postgres;

revoke all on function private._weekly_source_completed_pack_copy_eligibility_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_completed_pack_copy_status_sync_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_completed_pack_copy_due_list_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_completed_pack_copy_commit_atomic_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_completed_pack_copy_status_sync_v1(jsonb)
  to service_role;
grant execute on function public.weekly_source_completed_pack_copy_due_list_v1(jsonb)
  to service_role;
grant execute on function public.weekly_source_completed_pack_copy_commit_atomic_v1(jsonb)
  to service_role;

comment on function public.weekly_source_completed_pack_copy_status_sync_v1(jsonb) is
  'Synchronises informational completed-pack lifecycle state from the durable mail outbox only; no business state is changed.';
comment on function public.weekly_source_completed_pack_copy_due_list_v1(jsonb) is
  'Lists bounded, policy-eligible completed Weekly Timesheet packs that have never been copied for this immutable completion generation.';
comment on function public.weekly_source_completed_pack_copy_commit_atomic_v1(jsonb) is
  'Atomically records one immutable completed-pack copy and queues its exact R2 PDF as a non-blocking informational email.';

notify pgrst, 'reload schema';

commit;
