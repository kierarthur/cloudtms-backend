-- Candidate App workflow, immutable official review document, manager approval,
-- final signed document, paper and reminder state machine.

create or replace function private._candidate_queue_mail_v1(
  p_mail jsonb,
  p_to text,
  p_deterministic_key text,
  p_reference text,
  p_context_id uuid,
  p_now_utc timestamptz default now()
)
returns uuid
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare v_id uuid;
begin
  if jsonb_typeof(p_mail)<>'object'
     or nullif(btrim(coalesce(p_mail->>'subject','')),'') is null
     or (nullif(btrim(coalesce(p_mail->>'body_text','')),'') is null
       and nullif(btrim(coalesce(p_mail->>'body_html','')),'') is null) then
    raise exception 'CANDIDATE_MAIL_PAYLOAD_REQUIRED' using errcode='22023';
  end if;
  insert into public.mail_outbox(
    type,"to",subject,body_html,body_text,attachments,status,created_at_utc,
    reference,recipient_kind,context_kind,context_id,email_type,scheduled_for_utc,
    next_attempt_at_utc,deterministic_outbox_key,payment_scope_json
  ) values (
    'TIMESHEET_GENERAL',p_to,btrim(p_mail->>'subject'),nullif(p_mail->>'body_html',''),
    nullif(p_mail->>'body_text',''),coalesce(p_mail->'attachments','[]'::jsonb),'QUEUED',p_now_utc,
    p_reference,'CANDIDATE_MANAGER','CANDIDATE_WORKFLOW',p_context_id,
    coalesce(nullif(btrim(p_mail->>'email_type'),''),'CANDIDATE_APP_TRANSACTIONAL'),
    coalesce(nullif(p_mail->>'scheduled_for_utc','')::timestamptz,p_now_utc),p_now_utc,
    p_deterministic_key,'{}'::jsonb
  ) on conflict (deterministic_outbox_key) do update
    set deterministic_outbox_key=excluded.deterministic_outbox_key
  returning id into v_id;
  return v_id;
end;
$function$;

create or replace function private._candidate_paper_delivery_retire_v1(
  p_workflow_id uuid,
  p_expected_generation integer,
  p_reason_code text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_mail public.mail_outbox%rowtype;
  v_qr_source public.timesheets%rowtype;
  v_qr_current public.timesheets%rowtype;
  v_rejected_target_timesheet_id uuid;
  v_qr_source_timesheet_id uuid;
  v_delivery_generation integer;
  v_source_key text;
  v_reason text:=upper(btrim(coalesce(p_reason_code,'')));
  v_qr_token_hash text;
  v_mail_count integer:=0;
  v_qr_token_hash_missing_count integer:=0;
  v_mail_retired_count integer:=0;
  v_notification_count integer:=0;
  v_qr_invalidated boolean:=false;
  v_qr_already_invalidated boolean:=false;
  v_current_source_count integer:=0;
begin
  if p_workflow_id is null or coalesce(p_expected_generation,0)<1 or v_reason='' then
    raise exception 'CANDIDATE_PAPER_RETIREMENT_CONTEXT_INVALID' using errcode='22023';
  end if;

  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id
  for update;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.generation<>p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;
  if v_workflow.route<>'PAPER'
     or v_workflow.state not in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED') then
    return jsonb_build_object(
      'retired',false,'workflow_id',v_workflow.id,
      'generation',v_workflow.generation,'reason_code',v_reason
    );
  end if;

  -- Finalisation advances the workflow generation after freezing the PAPER
  -- return artefacts. RECEIVED remains on the immutable delivery generation
  -- while canonical finalisation is retryable; FINALISED owns the immediately
  -- preceding generation.
  v_delivery_generation:=case
    when v_workflow.state='FINALISED' then greatest(v_workflow.generation-1,1)
    else v_workflow.generation
  end;

  v_rejected_target_timesheet_id:=coalesce(
    v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id
  );
  if v_rejected_target_timesheet_id is null then
    raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
  end if;

  -- Lock every delivery row for this exact immutable PAPER generation before
  -- deciding whether retirement can proceed. A provider-owned active lease is
  -- an explicit retryable lifecycle conflict: the workflow remains current.
  for v_mail in
    select mail_row.*
    from public.mail_outbox mail_row
    where mail_row.type='TIMESHEET_QR'
      and mail_row.context_kind='timesheets'
      and mail_row.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and mail_row.payment_scope_json->>'candidate_workflow_generation'=v_delivery_generation::text
    order by mail_row.id
    for update
  loop
    v_mail_count:=v_mail_count+1;
    if v_mail.context_id is null then
      raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
          'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
          'reason','MAIL_CONTEXT_MISSING'
        )::text;
    end if;
    if v_qr_source_timesheet_id is null then
      v_qr_source_timesheet_id:=v_mail.context_id;
    elsif v_qr_source_timesheet_id<>v_mail.context_id then
      raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
          'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
          'reason','MULTIPLE_MAIL_CONTEXTS'
        )::text;
    end if;
    if v_mail.status<>'SENT'
       and nullif(btrim(coalesce(v_mail.attempt_lease_token,'')),'') is not null
       and (v_mail.attempt_lease_expires_at_utc is null
         or v_mail.attempt_lease_expires_at_utc>p_now_utc) then
      raise exception 'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS',
          'workflow_id',v_workflow.id,'generation',v_workflow.generation,
          'delivery_generation',v_delivery_generation,
          'mail_outbox_id',v_mail.id
        )::text;
    end if;
    if nullif(btrim(coalesce(v_mail.payment_scope_json->>'qr_token_hash','')),'') is null then
      v_qr_token_hash_missing_count:=v_qr_token_hash_missing_count+1;
    elsif lower(v_mail.payment_scope_json->>'qr_token_hash') !~ '^[0-9a-f]{64}$' then
      raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
          'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
          'reason','QR_TOKEN_HASH_INVALID'
        )::text;
    else
      if v_qr_token_hash is null then
        v_qr_token_hash:=lower(v_mail.payment_scope_json->>'qr_token_hash');
      elsif v_qr_token_hash<>lower(v_mail.payment_scope_json->>'qr_token_hash') then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
            'reason','MULTIPLE_QR_TOKEN_HASHES'
          )::text;
      end if;
    end if;
  end loop;

  if v_mail_count<1 or v_qr_source_timesheet_id is null then
    raise exception 'CANDIDATE_PAPER_DELIVERY_RECEIPT_NOT_FOUND'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_PAPER_DELIVERY_RECEIPT_NOT_FOUND',
        'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation
      )::text;
  end if;

  select source_row.* into v_qr_source
  from public.timesheets source_row
  where source_row.timesheet_id=v_qr_source_timesheet_id;
  if not found
     or v_qr_source.contract_id is distinct from v_workflow.contract_id
     or v_qr_source.week_ending_date is distinct from v_workflow.week_ending_date
     or upper(coalesce(v_qr_source.line_type::text,'')) in ('EXPENSES','MILEAGE') then
    raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
        'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
        'reason','QR_SOURCE_SCOPE_MISMATCH'
      )::text;
  end if;

  v_source_key:=case
    when nullif(btrim(coalesce(v_qr_source.booking_id,'')),'') is not null
      then 'BOOKING:'||v_qr_source.booking_id
    else 'TIMESHEET:'||v_qr_source.timesheet_id::text
  end;
  perform pg_advisory_xact_lock(hashtextextended(
    'CANDIDATE_PAPER_SOURCE:'||v_source_key,0
  ));

  if nullif(btrim(coalesce(v_qr_source.booking_id,'')),'') is not null then
    select count(*)::integer into v_current_source_count
    from public.timesheets current_source
    where current_source.booking_id=v_qr_source.booking_id
      and current_source.contract_id is not distinct from v_qr_source.contract_id
      and current_source.week_ending_date is not distinct from v_qr_source.week_ending_date
      and upper(coalesce(current_source.line_type::text,'')) not in ('EXPENSES','MILEAGE')
      and current_source.is_current=true
      and current_source.archived_at_utc is null;
    if v_current_source_count<>1 then
      raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
          'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
          'reason','QR_SOURCE_CURRENT_VERSION_CONFLICT'
        )::text;
    end if;
    select current_source.* into v_qr_current
    from public.timesheets current_source
    where current_source.booking_id=v_qr_source.booking_id
      and current_source.contract_id is not distinct from v_qr_source.contract_id
      and current_source.week_ending_date is not distinct from v_qr_source.week_ending_date
      and upper(coalesce(current_source.line_type::text,'')) not in ('EXPENSES','MILEAGE')
      and current_source.is_current=true
      and current_source.archived_at_utc is null
    for update;
  else
    select current_source.* into v_qr_current
    from public.timesheets current_source
    where current_source.timesheet_id=v_qr_source.timesheet_id
      and current_source.is_current=true
      and current_source.archived_at_utc is null
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
          'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
          'reason','QR_SOURCE_CURRENT_VERSION_MISSING'
        )::text;
    end if;
  end if;

  update public.mail_outbox mail_row
  set attachments='[]'::jsonb,
      scheduled_for_utc='infinity'::timestamptz,
      next_attempt_at_utc='infinity'::timestamptz,
      attempt_lease_token=null,
      attempt_leased_at_utc=null,
      attempt_lease_expires_at_utc=null,
      payment_scope_json=coalesce(mail_row.payment_scope_json,'{}'::jsonb)
        ||jsonb_build_object(
          'candidate_paper_generation_retired',true,
          'candidate_paper_generation_retired_at_utc',p_now_utc,
          'candidate_paper_generation_retired_reason',v_reason,
          'candidate_paper_pack_ready',false,
          'mail_held_until_pdf_rendered',true,
          'mail_delayed_for_pdf_render',true,
          'mail_hold_reason','CANDIDATE_PAPER_GENERATION_RETIRED',
          'candidate_retired_delivery_receipt',coalesce(
            mail_row.payment_scope_json->'candidate_retired_delivery_receipt',
            jsonb_build_object(
              'attachments',coalesce(mail_row.attachments,'[]'::jsonb),
              'candidate_complete_pack_storage_key',mail_row.payment_scope_json->>'candidate_complete_pack_storage_key',
              'candidate_complete_pack_sha256',mail_row.payment_scope_json->>'candidate_complete_pack_sha256',
              'candidate_complete_pack_size_bytes',mail_row.payment_scope_json->>'candidate_complete_pack_size_bytes',
              'candidate_complete_pack_page_count',mail_row.payment_scope_json->>'candidate_complete_pack_page_count',
              'candidate_complete_pack_media_type',mail_row.payment_scope_json->>'candidate_complete_pack_media_type'
            )
          )
        )
  where mail_row.type='TIMESHEET_QR'
    and mail_row.context_kind='timesheets'
    and mail_row.status<>'SENT'
    and mail_row.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
    and mail_row.payment_scope_json->>'candidate_workflow_generation'=v_delivery_generation::text;
  get diagnostics v_mail_retired_count=row_count;

  update public.candidate_notifications notification
  set state='DISMISSED',
      dismissed_at_utc=coalesce(notification.dismissed_at_utc,p_now_utc),
      push_state=case
        when notification.push_state in ('PENDING','FAILED','CLAIMED') then 'SKIPPED'
        else notification.push_state end,
      last_error=case
        when notification.push_state in ('PENDING','FAILED','CLAIMED')
          then 'CANDIDATE_PAPER_GENERATION_RETIRED'
        else notification.last_error end,
      deep_link_json=coalesce(notification.deep_link_json,'{}'::jsonb)
        ||jsonb_build_object(
          'obsolete',true,'obsolete_reason',v_reason,
          'obsolete_at_utc',p_now_utc
        )
  where notification.workflow_id=v_workflow.id
    and notification.event_type='PAPER_PACK_READY'
    and notification.dedupe_key like
      'CANDIDATE_PAPER_PACK_READY_V1:'||v_workflow.id::text||':'||v_delivery_generation::text||':%'
    and (notification.state<>'DISMISSED'
      or notification.push_state in ('PENDING','FAILED','CLAIMED'));
  get diagnostics v_notification_count=row_count;

  -- QR token/document ownership is generation-scoped. Clear it only where the
  -- current token still matches the retiring generation's bound mail receipt;
  -- the existing document invalidation trigger preserves historical bytes and
  -- makes the old printable document non-current.
  if nullif(btrim(coalesce(v_qr_current.qr_token,'')),'') is null then
    v_qr_already_invalidated:=true;
  elsif v_qr_token_hash is null or v_qr_token_hash_missing_count>0 then
    raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
        'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
        'reason','QR_TOKEN_HASH_MISSING',
        'qr_source_timesheet_id',v_qr_current.timesheet_id
      )::text;
  elsif encode(extensions.digest(
      convert_to(v_qr_current.qr_token,'UTF8'),'sha256'
    ),'hex')<>v_qr_token_hash then
    raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
        'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
        'reason','CURRENT_QR_TOKEN_HASH_MISMATCH',
        'qr_source_timesheet_id',v_qr_current.timesheet_id
      )::text;
  else
    update public.timesheets timesheet_row
    set qr_token=null,
        qr_payload_json='{}'::jsonb,
        qr_generated_at=null,
        qr_scanned_at=null,
        qr_scan_info_json=null,
        qr_r2_key=null,
        qr_last_sent_hash=null,
        qr_last_sent_at_utc=null,
        qr_signed_hash=null,
        qr_signed_at_utc=null,
        updated_at=p_now_utc
    where timesheet_row.timesheet_id=v_qr_current.timesheet_id
      and timesheet_row.is_current=true
      and nullif(btrim(coalesce(timesheet_row.qr_token,'')),'') is not null
      and encode(extensions.digest(
        convert_to(timesheet_row.qr_token,'UTF8'),'sha256'
      ),'hex')=v_qr_token_hash;
    v_qr_invalidated:=found;
    if not v_qr_invalidated then
      raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
          'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
          'reason','QR_TOKEN_INVALIDATION_LOST_RACE',
          'qr_source_timesheet_id',v_qr_current.timesheet_id
        )::text;
    end if;
  end if;

  perform private._candidate_audit_v1(
    'candidate_submission_workflow',v_workflow.id::text,
    'CANDIDATE_PAPER_DELIVERY_RETIRED',
    jsonb_build_object('state',v_workflow.state,'generation',v_workflow.generation),
    jsonb_build_object(
      'delivery_generation',v_delivery_generation,
      'reason_code',v_reason,'mail_count',v_mail_count,
      'mail_retired_count',v_mail_retired_count,
      'notification_retired_count',v_notification_count,
      'qr_source_timesheet_id',v_qr_current.timesheet_id,
      'rejected_target_timesheet_id',v_rejected_target_timesheet_id,
      'qr_invalidated',v_qr_invalidated,
      'qr_already_invalidated',v_qr_already_invalidated
    ),v_reason,null,
    'candidate-paper-retire:'||v_workflow.id::text||':'||v_workflow.generation::text||':'
      ||v_delivery_generation::text||':'||v_reason,
    p_now_utc
  );

  return jsonb_build_object(
    'retired',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
    'delivery_generation',v_delivery_generation,
    'reason_code',v_reason,'mail_count',v_mail_count,
    'mail_retired_count',v_mail_retired_count,
    'notification_retired_count',v_notification_count,
    'qr_source_timesheet_id',v_qr_current.timesheet_id,
    'rejected_target_timesheet_id',v_rejected_target_timesheet_id,
    'qr_invalidated',v_qr_invalidated,
    'qr_already_invalidated',v_qr_already_invalidated
  );
end;
$function$;

create or replace function private._candidate_paper_source_workflow_context_v1(
  p_source_timesheet_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_requested public.timesheets%rowtype;
  v_current_source public.timesheets%rowtype;
  v_source_key text;
  v_current_token_hash text;
  v_source_workflow_count integer:=0;
  v_nonterminal_count integer:=0;
  v_affected_nonterminal_count integer:=0;
  v_token_owner_count integer:=0;
  v_source_workflows jsonb:='[]'::jsonb;
  v_affected_nonterminal_workflows jsonb:='[]'::jsonb;
  v_affected_nonterminal_workflow_ids uuid[]:='{}'::uuid[];
  v_token_owner_workflow_id uuid;
  v_token_owner_generation integer;
  v_token_owner_state text;
  v_sole_nonterminal_workflow_id uuid;
  v_sole_nonterminal_generation integer;
  v_sole_nonterminal_state text;
  v_latest_finalised_workflow_id uuid;
  v_latest_finalised_generation integer;
  v_latest_finalised_state text;
  v_selected_workflow_id uuid;
  v_selected_generation integer;
  v_selected_state text;
  v_identity_conflict boolean:=false;
  v_conflict_reason text;
begin
  if p_source_timesheet_id is null then
    raise exception 'CANDIDATE_PAPER_QR_SOURCE_REQUIRED' using errcode='22023';
  end if;
  select source_row.* into v_requested
  from public.timesheets source_row
  where source_row.timesheet_id=p_source_timesheet_id;
  if not found then
    raise exception 'CANDIDATE_PAPER_QR_SOURCE_NOT_FOUND' using errcode='P0002';
  end if;

  if nullif(btrim(coalesce(v_requested.booking_id,'')),'') is not null then
    select source_row.* into v_current_source
    from public.timesheets source_row
    where source_row.booking_id=v_requested.booking_id
      and source_row.is_current=true
      and source_row.archived_at_utc is null
      and upper(coalesce(source_row.line_type::text,'')) not in ('EXPENSES','MILEAGE')
    order by source_row.version desc,source_row.updated_at desc,source_row.timesheet_id
    limit 1;
    v_source_key:='BOOKING:'||v_requested.booking_id;
  else
    select source_row.* into v_current_source
    from public.timesheets source_row
    where source_row.timesheet_id=v_requested.timesheet_id
      and source_row.is_current=true
      and source_row.archived_at_utc is null
      and upper(coalesce(source_row.line_type::text,'')) not in ('EXPENSES','MILEAGE');
    v_source_key:='TIMESHEET:'||v_requested.timesheet_id::text;
  end if;
  if not found then
    raise exception 'CANDIDATE_PAPER_QR_SOURCE_CURRENT_VERSION_MISSING'
      using errcode='40001';
  end if;

  if nullif(btrim(coalesce(v_current_source.qr_token,'')),'') is not null then
    v_current_token_hash:=encode(extensions.digest(
      convert_to(v_current_source.qr_token,'UTF8'),'sha256'
    ),'hex');
  end if;

  with source_workflows as (
    select workflow.id,workflow.generation,workflow.state,
      workflow.workflow_kind,workflow.updated_at_utc,workflow.created_at_utc,
      exists(
        select 1
        from public.mail_outbox owner_mail
        join public.timesheets owner_source
          on owner_source.timesheet_id=owner_mail.context_id
        where v_current_token_hash is not null
          and owner_mail.type='TIMESHEET_QR'
          and owner_mail.context_kind='timesheets'
          and owner_mail.payment_scope_json->>'candidate_workflow_id'=workflow.id::text
          and owner_mail.payment_scope_json->>'candidate_workflow_generation'=
            (case when workflow.state='FINALISED'
              then greatest(workflow.generation-1,1)
              else workflow.generation end)::text
          and lower(coalesce(owner_mail.payment_scope_json->>'qr_token_hash',''))=
            v_current_token_hash
          and (case
            when nullif(btrim(coalesce(owner_source.booking_id,'')),'') is not null
              then 'BOOKING:'||owner_source.booking_id
            else 'TIMESHEET:'||owner_source.timesheet_id::text end)=v_source_key
      ) as owns_current_token
    from public.candidate_submission_workflows workflow
    where workflow.route='PAPER'
      and workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
      and workflow.contract_id is not distinct from v_current_source.contract_id
      and workflow.week_ending_date is not distinct from v_current_source.week_ending_date
      and exists(
        select 1
        from public.mail_outbox source_mail
        join public.timesheets mail_source on mail_source.timesheet_id=source_mail.context_id
        where source_mail.type='TIMESHEET_QR'
          and source_mail.context_kind='timesheets'
          and source_mail.payment_scope_json->>'candidate_workflow_id'=workflow.id::text
          and source_mail.payment_scope_json->>'candidate_workflow_generation'=
            (case when workflow.state='FINALISED'
              then greatest(workflow.generation-1,1)
              else workflow.generation end)::text
          and (case
            when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
              then 'BOOKING:'||mail_source.booking_id
            else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
      )
  )
  select count(*)::integer,
    count(*) filter(where state in ('AWAITING_PAPER_RETURN','RECEIVED'))::integer,
    count(*) filter(where owns_current_token)::integer,
    coalesce(jsonb_agg(jsonb_build_object(
      'workflow_id',id,'generation',generation,'state',state,
      'workflow_kind',workflow_kind,'owns_current_qr_token',owns_current_token
    ) order by id),'[]'::jsonb),
    (array_agg(id order by id) filter(where owns_current_token))[1],
    (array_agg(generation order by id) filter(where owns_current_token))[1],
    (array_agg(state order by id) filter(where owns_current_token))[1],
    (array_agg(id order by updated_at_utc desc,created_at_utc desc,id)
      filter(where state in ('AWAITING_PAPER_RETURN','RECEIVED')))[1],
    (array_agg(generation order by updated_at_utc desc,created_at_utc desc,id)
      filter(where state in ('AWAITING_PAPER_RETURN','RECEIVED')))[1],
    (array_agg(state order by updated_at_utc desc,created_at_utc desc,id)
      filter(where state in ('AWAITING_PAPER_RETURN','RECEIVED')))[1],
    (array_agg(id order by updated_at_utc desc,created_at_utc desc,id)
      filter(where state='FINALISED'))[1],
    (array_agg(generation order by updated_at_utc desc,created_at_utc desc,id)
      filter(where state='FINALISED'))[1],
    (array_agg(state order by updated_at_utc desc,created_at_utc desc,id)
      filter(where state='FINALISED'))[1]
  into v_source_workflow_count,v_nonterminal_count,v_token_owner_count,
    v_source_workflows,
    v_token_owner_workflow_id,v_token_owner_generation,v_token_owner_state,
    v_sole_nonterminal_workflow_id,v_sole_nonterminal_generation,
    v_sole_nonterminal_state,
    v_latest_finalised_workflow_id,v_latest_finalised_generation,
    v_latest_finalised_state
  from source_workflows;

  -- Delivery ownership and workflow lifecycle are deliberately separate
  -- catalogues.  WORKER_DRAFT and the approval states do not yet have an
  -- immutable mail receipt, but rotating their worked-row source would still
  -- make their PAPER anchor historical and leave the claim unusably active.
  -- Resolve those workflows through the stable booking/version family rather
  -- than requiring an exact current timesheet id or a mail_outbox row.
  with affected_nonterminal_workflows as (
    select workflow.id,workflow.generation,workflow.state,
      workflow.workflow_kind,workflow.updated_at_utc,workflow.created_at_utc
    from public.candidate_submission_workflows workflow
    where workflow.route='PAPER'
      and workflow.state not in (
        'FINALISED','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'
      )
      and workflow.contract_id is not distinct from v_current_source.contract_id
      and workflow.week_ending_date is not distinct from v_current_source.week_ending_date
      and exists(
        select 1
        from public.timesheets binding_source
        where binding_source.timesheet_id in (
            workflow.target_timesheet_id,workflow.anchor_timesheet_id
          )
          and (case
            when nullif(btrim(coalesce(binding_source.booking_id,'')),'') is not null
              then 'BOOKING:'||binding_source.booking_id
            else 'TIMESHEET:'||binding_source.timesheet_id::text end)=v_source_key
      )
  )
  select count(*)::integer,
    coalesce(jsonb_agg(jsonb_build_object(
      'workflow_id',id,'generation',generation,'state',state,
      'workflow_kind',workflow_kind
    ) order by id),'[]'::jsonb),
    coalesce(array_agg(id order by id),'{}'::uuid[])
  into v_affected_nonterminal_count,v_affected_nonterminal_workflows,
    v_affected_nonterminal_workflow_ids
  from affected_nonterminal_workflows;

  if v_source_workflow_count=0 then
    -- Ordinary pre-Candidate QR remains an exact legacy route family. With no
    -- Candidate delivery receipts there is no Candidate workflow to select or
    -- supersede, and the public compatibility authority remains unchanged.
    null;
  elsif v_current_token_hash is not null then
    if v_token_owner_count<>1 then
      v_identity_conflict:=true;
      v_conflict_reason:='CURRENT_QR_TOKEN_OWNER_CONFLICT';
    elsif v_nonterminal_count>1 then
      v_identity_conflict:=true;
      v_conflict_reason:='MULTIPLE_NONTERMINAL_PAPER_WORKFLOWS';
    elsif v_token_owner_state='FINALISED' and v_nonterminal_count>0 then
      v_identity_conflict:=true;
      v_conflict_reason:='CURRENT_QR_TOKEN_OWNER_TERMINAL_WITH_LIVE_WORKFLOW';
    else
      v_selected_workflow_id:=v_token_owner_workflow_id;
      v_selected_generation:=v_token_owner_generation;
      v_selected_state:=v_token_owner_state;
    end if;
  elsif v_nonterminal_count>1 then
    v_identity_conflict:=true;
    v_conflict_reason:='MULTIPLE_NONTERMINAL_PAPER_WORKFLOWS';
  elsif v_nonterminal_count=1 then
    v_selected_workflow_id:=v_sole_nonterminal_workflow_id;
    v_selected_generation:=v_sole_nonterminal_generation;
    v_selected_state:=v_sole_nonterminal_state;
  else
    v_selected_workflow_id:=v_latest_finalised_workflow_id;
    v_selected_generation:=v_latest_finalised_generation;
    v_selected_state:=v_latest_finalised_state;
  end if;

  -- A source-rotating route action may proceed only when its selected delivery
  -- owner is also the sole affected nonterminal PAPER workflow.  A distinct
  -- draft/submitted/approval workflow has no receipt to retire yet, so the
  -- only safe automatic outcome is a controlled zero-mutation conflict.
  if not v_identity_conflict and v_affected_nonterminal_count>0 and (
       v_selected_workflow_id is null
       or v_affected_nonterminal_count>1
       or not (v_selected_workflow_id=any(v_affected_nonterminal_workflow_ids))
     ) then
    v_identity_conflict:=true;
    v_conflict_reason:='CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT';
  end if;

  return jsonb_build_object(
    'contract_version','CANDIDATE_PAPER_SOURCE_WORKFLOW_CONTEXT_V1',
    'qr_source_timesheet_id',v_current_source.timesheet_id,
    'source_key',v_source_key,
    'current_qr_token_present',v_current_token_hash is not null,
    'source_workflow_count',v_source_workflow_count,
    'nonterminal_workflow_count',v_nonterminal_count,
    'affected_nonterminal_workflow_count',v_affected_nonterminal_count,
    'current_token_owner_count',v_token_owner_count,
    'current_token_owner_workflow_id',v_token_owner_workflow_id,
    'current_token_owner_generation',v_token_owner_generation,
    'current_token_owner_state',v_token_owner_state,
    'selected_workflow_id',v_selected_workflow_id,
    'selected_workflow_generation',v_selected_generation,
    'selected_workflow_state',v_selected_state,
    'identity_conflict',v_identity_conflict,
    'conflict_reason',v_conflict_reason,
    'source_workflows',v_source_workflows,
    'affected_nonterminal_workflows',v_affected_nonterminal_workflows
  );
end;
$function$;

create or replace function private._candidate_paper_delivery_retire_set_v1(
  p_workflow_ids uuid[],
  p_expected_generations integer[],
  p_reason_code text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_reason text:=upper(btrim(coalesce(p_reason_code,'')));
  v_workflow public.candidate_submission_workflows%rowtype;
  v_source public.timesheets%rowtype;
  v_current_source public.timesheets%rowtype;
  v_mail public.mail_outbox%rowtype;
  v_input record;
  v_owner record;
  v_source_key text;
  v_workflow_source_key text;
  v_source_keys text[]:='{}'::text[];
  v_family_key text;
  v_family_keys text[]:='{}'::text[];
  v_processed_keys text[]:='{}'::text[];
  v_selected_workflow_ids uuid[]:='{}'::uuid[];
  v_selected_count integer:=0;
  v_selected_mail_count integer:=0;
  v_current_source_count integer:=0;
  v_current_token_owner_count integer:=0;
  v_current_token_owner_workflow_id uuid;
  v_current_token_owner_generation integer;
  v_current_token_owner_state text;
  v_current_token_hash text;
  v_result jsonb;
  v_retirement_receipts jsonb:='[]'::jsonb;
  v_source_receipts jsonb:='[]'::jsonb;
  v_preserved_workflows jsonb:='[]'::jsonb;
  v_unselected_nonterminal_workflows jsonb:='[]'::jsonb;
  v_source_invalidated boolean;
  v_source_already_invalidated boolean;
begin
  if coalesce(cardinality(p_workflow_ids),0)<1
     or cardinality(p_workflow_ids) is distinct from cardinality(p_expected_generations)
     or v_reason='' then
    raise exception 'CANDIDATE_PAPER_RETIREMENT_SET_CONTEXT_INVALID' using errcode='22023';
  end if;
  if cardinality(p_workflow_ids)<>cardinality(array(select distinct item from unnest(p_workflow_ids) item)) then
    raise exception 'CANDIDATE_PAPER_RETIREMENT_SET_DUPLICATE_WORKFLOW' using errcode='22023';
  end if;

  -- Establish the common family lock order before freezing any selected
  -- workflow. Each identity is revalidated under row locks below.
  for v_input in
    select workflow.environment,workflow.contract_id,
      coalesce(workflow.week_ending_date,workflow.work_date) as family_date
    from unnest(p_workflow_ids) input(workflow_id)
    join public.candidate_submission_workflows workflow on workflow.id=input.workflow_id
    order by workflow.environment,workflow.contract_id,
      coalesce(workflow.week_ending_date,workflow.work_date)
  loop
    v_family_key:='CANDIDATE_PAPER_FAMILY:'||v_input.environment||':'
      ||coalesce(v_input.contract_id::text,'-')||':'
      ||coalesce(v_input.family_date::text,'-');
    if not (v_family_key=any(v_family_keys)) then
      v_family_keys:=array_append(v_family_keys,v_family_key);
    end if;
  end loop;
  if cardinality(v_family_keys)<1 then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  for v_family_key in
    select family_item from unnest(v_family_keys) family_item order by family_item
  loop
    perform pg_advisory_xact_lock(hashtextextended(v_family_key,0));
  end loop;

  -- Freeze the exact selected workflow set first. Each selected immutable
  -- delivery generation must identify exactly one QR source family.
  for v_input in
    select input.workflow_id,input.expected_generation
    from unnest(p_workflow_ids,p_expected_generations)
      as input(workflow_id,expected_generation)
    order by input.workflow_id
  loop
    select workflow.* into v_workflow
    from public.candidate_submission_workflows workflow
    where workflow.id=v_input.workflow_id
    for update;
    if not found then
      raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
    end if;
    if v_workflow.generation<>v_input.expected_generation then
      raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
    end if;
    if v_workflow.route<>'PAPER'
       or v_workflow.state not in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED') then
      raise exception 'CANDIDATE_PAPER_RETIREMENT_SET_WORKFLOW_INVALID'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_RETIREMENT_SET_WORKFLOW_INVALID',
          'workflow_id',v_workflow.id,'generation',v_workflow.generation,
          'state',v_workflow.state,'route',v_workflow.route
        )::text;
    end if;

    v_selected_count:=v_selected_count+1;
    v_selected_workflow_ids:=array_append(v_selected_workflow_ids,v_workflow.id);
    v_selected_mail_count:=0;
    v_workflow_source_key:=null;
    for v_mail in
      select mail_row.*
      from public.mail_outbox mail_row
      where mail_row.type='TIMESHEET_QR'
        and mail_row.context_kind='timesheets'
        and mail_row.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
        and mail_row.payment_scope_json->>'candidate_workflow_generation'=
          (case when v_workflow.state='FINALISED'
            then greatest(v_workflow.generation-1,1)
            else v_workflow.generation end)::text
      order by mail_row.id
    loop
      v_selected_mail_count:=v_selected_mail_count+1;
      if v_mail.context_id is null then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'workflow_id',v_workflow.id,'reason','MAIL_CONTEXT_MISSING'
          )::text;
      end if;
      select source_row.* into v_source
      from public.timesheets source_row
      where source_row.timesheet_id=v_mail.context_id;
      if not found
         or v_source.contract_id is distinct from v_workflow.contract_id
         or v_source.week_ending_date is distinct from v_workflow.week_ending_date
         or upper(coalesce(v_source.line_type::text,'')) in ('EXPENSES','MILEAGE') then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'workflow_id',v_workflow.id,'reason','QR_SOURCE_SCOPE_MISMATCH'
          )::text;
      end if;
      v_source_key:=case
        when nullif(btrim(coalesce(v_source.booking_id,'')),'') is not null
          then 'BOOKING:'||v_source.booking_id
        else 'TIMESHEET:'||v_source.timesheet_id::text
      end;
      if v_workflow_source_key is null then
        v_workflow_source_key:=v_source_key;
      elsif v_workflow_source_key<>v_source_key then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'workflow_id',v_workflow.id,'reason','MULTIPLE_QR_SOURCE_FAMILIES'
          )::text;
      end if;
    end loop;
    if v_selected_mail_count<1 or v_workflow_source_key is null then
      raise exception 'CANDIDATE_PAPER_DELIVERY_RECEIPT_NOT_FOUND'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_DELIVERY_RECEIPT_NOT_FOUND',
          'workflow_id',v_workflow.id,'generation',v_workflow.generation
        )::text;
    end if;
    if not (v_workflow_source_key=any(v_source_keys)) then
      v_source_keys:=array_append(v_source_keys,v_workflow_source_key);
    end if;
  end loop;

  -- Coordinate by QR source, not by workflow UUID. Resolve and invalidate the
  -- current token owner first, then retire every selected older generation.
  for v_source_key in
    select source_item from unnest(v_source_keys) as source_item order by source_item
  loop
    perform pg_advisory_xact_lock(hashtextextended(
      'CANDIDATE_PAPER_SOURCE:'||v_source_key,0
    ));
    if v_source_key like 'BOOKING:%' then
      select count(*)::integer into v_current_source_count
      from public.timesheets current_source
      where current_source.booking_id=substring(v_source_key from 9)
        and current_source.is_current=true
        and current_source.archived_at_utc is null
        and upper(coalesce(current_source.line_type::text,'')) not in ('EXPENSES','MILEAGE');
      if v_current_source_count<>1 then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'source_key',v_source_key,'reason','QR_SOURCE_CURRENT_VERSION_CONFLICT'
          )::text;
      end if;
      select current_source.* into v_current_source
      from public.timesheets current_source
      where current_source.booking_id=substring(v_source_key from 9)
        and current_source.is_current=true
        and current_source.archived_at_utc is null
        and upper(coalesce(current_source.line_type::text,'')) not in ('EXPENSES','MILEAGE')
      for update;
    else
      select current_source.* into v_current_source
      from public.timesheets current_source
      where current_source.timesheet_id=substring(v_source_key from 11)::uuid
        and current_source.is_current=true
        and current_source.archived_at_utc is null
        and upper(coalesce(current_source.line_type::text,'')) not in ('EXPENSES','MILEAGE')
      for update;
      if not found then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'source_key',v_source_key,'reason','QR_SOURCE_CURRENT_VERSION_MISSING'
          )::text;
      end if;
    end if;

    -- Freeze every live/received/finalised PAPER workflow and bound outbox row on
    -- this source before checking provider leases or changing any lifecycle.
    perform 1
    from public.candidate_submission_workflows relevant_workflow
    where relevant_workflow.route='PAPER'
      and relevant_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
      and exists(
        select 1
        from public.mail_outbox relevant_mail
        join public.timesheets mail_source on mail_source.timesheet_id=relevant_mail.context_id
        where relevant_mail.type='TIMESHEET_QR'
          and relevant_mail.context_kind='timesheets'
          and relevant_mail.payment_scope_json->>'candidate_workflow_id'=relevant_workflow.id::text
          and relevant_mail.payment_scope_json->>'candidate_workflow_generation'=
            (case when relevant_workflow.state='FINALISED'
              then greatest(relevant_workflow.generation-1,1)
              else relevant_workflow.generation end)::text
          and (case
            when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
              then 'BOOKING:'||mail_source.booking_id
            else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
      )
    order by relevant_workflow.id
    for update;

    perform 1
    from public.mail_outbox relevant_mail
    join public.candidate_submission_workflows relevant_workflow
      on relevant_workflow.id::text=relevant_mail.payment_scope_json->>'candidate_workflow_id'
    join public.timesheets mail_source on mail_source.timesheet_id=relevant_mail.context_id
    where relevant_mail.type='TIMESHEET_QR'
      and relevant_mail.context_kind='timesheets'
      and relevant_workflow.route='PAPER'
      and relevant_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
      and relevant_mail.payment_scope_json->>'candidate_workflow_generation'=
        (case when relevant_workflow.state='FINALISED'
          then greatest(relevant_workflow.generation-1,1)
          else relevant_workflow.generation end)::text
      and (case
        when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
          then 'BOOKING:'||mail_source.booking_id
        else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
    order by relevant_mail.id
    for update of relevant_mail;

    -- Source-wide retirement may preserve immutable FINALISED history, but it
    -- must never destroy the only delivery surface of an unselected live or
    -- retryable workflow. Claim-level callers therefore fail closed, while
    -- source-wide callers must explicitly select every affected nonterminal
    -- workflow before any mail, notification or QR mutation occurs.
    select coalesce(jsonb_agg(jsonb_build_object(
      'workflow_id',relevant_workflow.id,
      'generation',relevant_workflow.generation,
      'state',relevant_workflow.state,
      'workflow_kind',relevant_workflow.workflow_kind
    ) order by relevant_workflow.id),'[]'::jsonb)
    into v_unselected_nonterminal_workflows
    from public.candidate_submission_workflows relevant_workflow
    where relevant_workflow.route='PAPER'
      and relevant_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED')
      and not (relevant_workflow.id=any(v_selected_workflow_ids))
      and exists(
        select 1
        from public.mail_outbox relevant_mail
        join public.timesheets mail_source
          on mail_source.timesheet_id=relevant_mail.context_id
        where relevant_mail.type='TIMESHEET_QR'
          and relevant_mail.context_kind='timesheets'
          and relevant_mail.payment_scope_json->>'candidate_workflow_id'=
            relevant_workflow.id::text
          and relevant_mail.payment_scope_json->>'candidate_workflow_generation'=
            relevant_workflow.generation::text
          and (case
            when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
              then 'BOOKING:'||mail_source.booking_id
            else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
      );
    if jsonb_array_length(v_unselected_nonterminal_workflows)>0 then
      raise exception 'CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT',
          'source_key',v_source_key,
          'selected_workflow_ids',to_jsonb(v_selected_workflow_ids),
          'unselected_nonterminal_workflows',v_unselected_nonterminal_workflows
        )::text;
    end if;

    if exists(
      select 1
      from public.mail_outbox leased_mail
      join public.candidate_submission_workflows leased_workflow
        on leased_workflow.id::text=leased_mail.payment_scope_json->>'candidate_workflow_id'
      join public.timesheets mail_source on mail_source.timesheet_id=leased_mail.context_id
      where leased_mail.type='TIMESHEET_QR'
        and leased_mail.context_kind='timesheets'
        and leased_workflow.route='PAPER'
        and leased_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
        and leased_mail.payment_scope_json->>'candidate_workflow_generation'=
          (case when leased_workflow.state='FINALISED'
            then greatest(leased_workflow.generation-1,1)
            else leased_workflow.generation end)::text
        and (case
          when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
            then 'BOOKING:'||mail_source.booking_id
          else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
        and leased_mail.status<>'SENT'
        and nullif(btrim(coalesce(leased_mail.attempt_lease_token,'')),'') is not null
        and (leased_mail.attempt_lease_expires_at_utc is null
          or leased_mail.attempt_lease_expires_at_utc>p_now_utc)
    ) then
      raise exception 'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS',
          'source_key',v_source_key
        )::text;
    end if;

    v_current_token_owner_workflow_id:=null;
    v_current_token_owner_generation:=null;
    v_current_token_owner_count:=0;
    v_current_token_hash:=null;
    v_source_invalidated:=false;
    v_source_already_invalidated:=false;
    if nullif(btrim(coalesce(v_current_source.qr_token,'')),'') is null then
      v_source_already_invalidated:=true;
    else
      v_current_token_hash:=encode(extensions.digest(
        convert_to(v_current_source.qr_token,'UTF8'),'sha256'
      ),'hex');
      select count(*)::integer,
        (array_agg(owner.workflow_id order by owner.workflow_id))[1],
        (array_agg(owner.workflow_generation order by owner.workflow_id))[1],
        (array_agg(owner.workflow_state order by owner.workflow_id))[1]
      into v_current_token_owner_count,
        v_current_token_owner_workflow_id,v_current_token_owner_generation,
        v_current_token_owner_state
      from (
        select distinct relevant_workflow.id as workflow_id,
          relevant_workflow.generation as workflow_generation,
          relevant_workflow.state as workflow_state
        from public.mail_outbox owner_mail
        join public.candidate_submission_workflows relevant_workflow
          on relevant_workflow.id::text=owner_mail.payment_scope_json->>'candidate_workflow_id'
        join public.timesheets mail_source on mail_source.timesheet_id=owner_mail.context_id
        where owner_mail.type='TIMESHEET_QR'
          and owner_mail.context_kind='timesheets'
          and relevant_workflow.route='PAPER'
          and relevant_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
          and owner_mail.payment_scope_json->>'candidate_workflow_generation'=
            (case when relevant_workflow.state='FINALISED'
              then greatest(relevant_workflow.generation-1,1)
              else relevant_workflow.generation end)::text
          and lower(coalesce(owner_mail.payment_scope_json->>'qr_token_hash',''))=v_current_token_hash
          and (case
            when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
              then 'BOOKING:'||mail_source.booking_id
            else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
      ) owner;
      if v_current_token_owner_count<>1 then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'source_key',v_source_key,
            'reason','CURRENT_QR_TOKEN_OWNER_CONFLICT',
            'owner_count',v_current_token_owner_count
          )::text;
      end if;

      v_result:=private._candidate_paper_delivery_retire_v1(
        v_current_token_owner_workflow_id,v_current_token_owner_generation,
        v_reason,p_now_utc
      );
      if not coalesce((v_result->>'retired')::boolean,false)
         or not coalesce((v_result->>'qr_invalidated')::boolean,false) then
        raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN',
            'source_key',v_source_key,'retirement_receipt',v_result
          )::text;
      end if;
      v_source_invalidated:=true;
      v_processed_keys:=array_append(
        v_processed_keys,
        v_current_token_owner_workflow_id::text||':'||v_current_token_owner_generation::text
      );
      v_retirement_receipts:=v_retirement_receipts||jsonb_build_array(v_result);
      if not (v_current_token_owner_workflow_id=any(v_selected_workflow_ids)) then
        v_preserved_workflows:=v_preserved_workflows||jsonb_build_array(
          jsonb_build_object(
            'workflow_id',v_current_token_owner_workflow_id,
            'generation',v_current_token_owner_generation,
            'workflow_state',v_current_token_owner_state,
            'workflow_preserved',true,'delivery_surface_retired',true
          )
        );
      end if;
    end if;

    -- Every live/received/finalised delivery surface on the source becomes obsolete
    -- when that source is rejected/rotated. Retire all of them, even where the
    -- QR token had already been cleared, while preserving workflows that are
    -- outside the selected rejection set and all immutable sent/R2 history.
    for v_owner in
      select distinct relevant_workflow.id as workflow_id,
        relevant_workflow.generation as expected_generation,
        relevant_workflow.state as workflow_state
      from public.candidate_submission_workflows relevant_workflow
      join public.mail_outbox relevant_mail
        on relevant_mail.payment_scope_json->>'candidate_workflow_id'=
          relevant_workflow.id::text
      join public.timesheets mail_source on mail_source.timesheet_id=relevant_mail.context_id
      where relevant_workflow.route='PAPER'
        and relevant_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
        and relevant_mail.type='TIMESHEET_QR'
        and relevant_mail.context_kind='timesheets'
        and relevant_mail.payment_scope_json->>'candidate_workflow_generation'=
          (case when relevant_workflow.state='FINALISED'
            then greatest(relevant_workflow.generation-1,1)
            else relevant_workflow.generation end)::text
        and (case
          when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
            then 'BOOKING:'||mail_source.booking_id
          else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
      order by relevant_workflow.id
    loop
      if (v_owner.workflow_id::text||':'||v_owner.expected_generation::text)=any(v_processed_keys) then
        continue;
      end if;
      v_result:=private._candidate_paper_delivery_retire_v1(
        v_owner.workflow_id,v_owner.expected_generation,v_reason,p_now_utc
      );
      if not coalesce((v_result->>'retired')::boolean,false)
         or (
           not coalesce((v_result->>'qr_invalidated')::boolean,false)
           and not coalesce((v_result->>'qr_already_invalidated')::boolean,false)
         ) then
        raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN',
            'workflow_id',v_owner.workflow_id,
            'generation',v_owner.expected_generation,
            'retirement_receipt',v_result
          )::text;
      end if;
      v_processed_keys:=array_append(
        v_processed_keys,v_owner.workflow_id::text||':'||v_owner.expected_generation::text
      );
      v_retirement_receipts:=v_retirement_receipts||jsonb_build_array(v_result);
      if not (v_owner.workflow_id=any(v_selected_workflow_ids)) then
        v_preserved_workflows:=v_preserved_workflows||jsonb_build_array(
          jsonb_build_object(
            'workflow_id',v_owner.workflow_id,
            'generation',v_owner.expected_generation,
            'workflow_state',v_owner.workflow_state,
            'workflow_preserved',true,'delivery_surface_retired',true
          )
        );
      end if;
    end loop;

    v_source_receipts:=v_source_receipts||jsonb_build_array(jsonb_build_object(
      'qr_source_timesheet_id',v_current_source.timesheet_id,
      'source_key',v_source_key,
      'current_token_owner_workflow_id',v_current_token_owner_workflow_id,
      'current_token_owner_generation',v_current_token_owner_generation,
      'qr_invalidated',v_source_invalidated,
      'qr_already_invalidated',v_source_already_invalidated,
      'invalidation_proven',v_source_invalidated or v_source_already_invalidated
    ));
  end loop;

  for v_input in
    select input.workflow_id,input.expected_generation
    from unnest(p_workflow_ids,p_expected_generations)
      as input(workflow_id,expected_generation)
  loop
    if not (v_input.workflow_id::text||':'||v_input.expected_generation::text)=any(v_processed_keys) then
      raise exception 'CANDIDATE_PAPER_RETIREMENT_SET_INCOMPLETE'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_RETIREMENT_SET_INCOMPLETE',
          'workflow_id',v_input.workflow_id,'generation',v_input.expected_generation
        )::text;
    end if;
  end loop;

  return jsonb_build_object(
    'retired',true,
    'qr_invalidation_proven',true,
    'selected_workflow_count',v_selected_count,
    'selected_workflow_ids',to_jsonb(v_selected_workflow_ids),
    'source_count',cardinality(v_source_keys),
    'source_receipts',v_source_receipts,
    'retirement_receipts',v_retirement_receipts,
    'preserved_workflows',v_preserved_workflows,
    'reason_code',v_reason
  );
end;
$function$;

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
  v_contract public.contracts%rowtype;
  v_week public.contract_weeks%rowtype;
  v_anchor_week public.contract_weeks%rowtype;
  v_anchor_timesheet public.timesheets%rowtype;
  v_daily_timesheet public.timesheets%rowtype;
  v_daily_fin public.timesheets_financials%rowtype;
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
  v_review_ordinal integer;
  v_has_expenses boolean:=false;
  v_has_mileage boolean:=false;
  v_expense_value numeric:=0;
  v_required_categories text[]:=array[]::text[];
  v_required_category text;
  v_paper_manifest jsonb;
  v_paper_source_pages jsonb;
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
  v_paper_outbox_count integer:=0;
  v_paper_base_document_sha256 text;
  v_paper_branding_contract_sha256 text;
  v_paper_renderer_contract_version text;
  v_paper_expected_storage_key text;
  v_provider_lease_token text;
  v_provider_permit_expires_at timestamptz;
  v_unlocked_workflow_updated_at timestamptz;
  v_paper_family_key text;
  v_source_component public.candidate_submission_components%rowtype;
  v_all_final_ready boolean:=false;
  v_server_issue_codes jsonb:='[]'::jsonb;
  v_request_id uuid;
  v_workflow_kind text;
  v_scope text;
  v_route text;
  v_canonical_work_date date;
  v_canonical_week_ending_date date;
  v_client_id uuid;
  v_anchor_candidate_count integer:=0;
  v_anchor_week_id uuid;
  v_target_capabilities jsonb;
  v_route_authority jsonb;
  v_daily_input jsonb;
  v_daily_patch jsonb;
  v_expected_save_hash bytea;
  v_daily_context jsonb;
  v_daily_context_hash bytea;
  v_current_row_signature text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_app_writes');
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

  if v_action in (
    'SELECT_PHONE_APPROVAL','CREATE_EMAIL_APPROVAL_REQUEST','BEGIN_MANAGER_REVIEW',
    'RECORD_REVIEW_PROGRESS','PHONE_APPROVE','EMAIL_APPROVE','MANAGER_REFUSE',
    'REMIND','RENEW','CANCEL_MANAGER_HANDOFF','REGISTER_REVIEW_COMPONENT','REGISTER_FINAL_SIGNED_DOCUMENT'
  ) or (p_session_id is null and v_action in ('COMPONENT_PREPARE','COMPONENT_COMPLETE')) then
    perform private._candidate_require_feature_v1(v_environment,'candidate_manager_approval');
  end if;
  if v_action='BEGIN_CANONICAL_DAILY_SAVE' then
    perform private._candidate_require_feature_v1(v_environment,'candidate_daily_finalisation');
  end if;
  if v_action in (
    'PAPER_PREPARE','PAPER_RETURN','PAPER_PACK_RELEASE',
    'PAPER_PROVIDER_SUBMIT_PERMIT'
  ) then
    perform private._candidate_require_feature_v1(v_environment,'candidate_paper_qr');
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
    and coalesce((v_payload->>'service_phone_approval')::boolean,false)
    and v_action in ('BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','MANAGER_REFUSE',
      'COMPONENT_PREPARE','COMPONENT_COMPLETE'));
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

  if v_action='CREATE' then
    if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
      raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023';
    end if;
    select * into v_workflow from public.candidate_submission_workflows
    where account_id=v_account_id and idempotency_key=p_idempotency_key;
    if found then
      return jsonb_build_object('ok',true,'idempotent_replay',true,'workflow_id',v_workflow.id,
        'state',v_workflow.state,'generation',v_workflow.generation);
    end if;
    v_workflow_kind:=upper(coalesce(v_payload->>'workflow_kind',''));
    v_scope:=upper(coalesce(v_payload->>'scope',''));
    v_route:=upper(coalesce(v_payload->>'route',''));
    if v_workflow_kind not in ('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED','DAILY')
       or v_scope not in ('WEEKLY','DAILY')
       or v_route not in ('ELECTRONIC','PHONE','EMAIL','PAPER') then
      raise exception 'CANDIDATE_WORKFLOW_TYPE_INVALID' using errcode='22023';
    end if;
    if v_workflow_kind='DAILY' then
      if v_scope<>'DAILY' or v_route not in ('PHONE','EMAIL')
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
      if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
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
      v_client_id:=coalesce(v_daily_fin.client_id,v_contract.client_id);
      if v_client_id is null then raise exception 'CANDIDATE_DAILY_CLIENT_NOT_FOUND' using errcode='P0002'; end if;
      v_route_authority:=private._candidate_route_family_v1(v_daily_timesheet.timesheet_id,null);
      if v_route_authority->>'route_family'<>'ELECTRONIC' then
        raise exception 'CANDIDATE_ROUTE_FAMILY_MISMATCH'
          using errcode='55000',detail=v_route_authority::text;
      end if;
      v_policy:=private._candidate_policy_resolve_v1(v_client_id,v_contract.id,v_canonical_work_date);
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
      v_policy:=private._candidate_policy_resolve_v1(v_contract.client_id,v_contract.id,v_canonical_week_ending_date);
      v_route_authority:=private._candidate_route_family_v1(v_week.timesheet_id,v_week.id);
      if v_route_authority->>'route_family'='MANUAL_NON_QR'
         or (v_route_authority->>'route_family'='IMPORT_AUTHORITATIVE'
           and v_workflow_kind<>'CONTRACT_EXPENSE') then
        raise exception 'CANDIDATE_RECORD_VIEW_ONLY' using errcode='55000',detail=v_route_authority::text;
      end if;
      if (v_route_authority->>'route_family'='QR' and v_route<>'PAPER')
         or (v_route_authority->>'route_family'='ELECTRONIC' and v_route='PAPER'
           and not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))
         or (v_route_authority->>'route_family'='IMPORT_AUTHORITATIVE' and v_route='PAPER') then
        raise exception 'CANDIDATE_ROUTE_FAMILY_MISMATCH' using errcode='55000',detail=v_route_authority::text;
      end if;
      if v_week.timesheet_id is not null then
        select * into v_anchor_timesheet
        from public.timesheets
        where timesheet_id=v_week.timesheet_id
          and is_current=true
          and archived_at_utc is null;
        if not found then raise exception 'CANDIDATE_WORKFLOW_TARGET_NOT_CURRENT' using errcode='55000'; end if;
        if v_workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED') then
          v_target_capabilities:=private._candidate_record_capabilities_v1(
            v_week.timesheet_id,v_week.id,'{}'::jsonb
          );
          if coalesce((v_target_capabilities->>'candidate_mutation_locked')::boolean,false)
             or coalesce((v_target_capabilities->>'protected')::boolean,false)
             or not coalesce((v_target_capabilities->>'can_edit_hours')::boolean,false) then
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
      if nullif(v_payload->>'anchor_timesheet_id','') is not null then
        select cw.* into v_anchor_week
        from public.contract_weeks cw
        join public.timesheets t on t.timesheet_id=cw.timesheet_id
          and t.is_current=true and t.archived_at_utc is null
        where cw.timesheet_id=(v_payload->>'anchor_timesheet_id')::uuid
          and cw.contract_id=v_contract.id
          and cw.week_ending_date=v_canonical_week_ending_date;
        if not found then raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='22023'; end if;
        if v_workflow_kind='CONTRACT_EXPENSE'
           and coalesce((private._candidate_record_capabilities_v1(v_anchor_week.timesheet_id,v_anchor_week.id,'{}'::jsonb)->>'hours_value')::numeric,0)<=0
           and coalesce((private._candidate_record_capabilities_v1(v_anchor_week.timesheet_id,v_anchor_week.id,'{}'::jsonb)->>'additional_units_value')::numeric,0)<=0 then
          raise exception 'CANDIDATE_WORKFLOW_ANCHOR_NOT_WORKED' using errcode='22023';
        end if;
      elsif v_workflow_kind='CONTRACT_EXPENSE' then
        select count(*)::integer,min(worked.id::text)::uuid
        into v_anchor_candidate_count,v_anchor_week_id
        from public.contract_weeks worked
        join public.timesheets t on t.timesheet_id=worked.timesheet_id
          and t.is_current=true and t.archived_at_utc is null
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
      elsif v_week.timesheet_id is not null then
        v_anchor_week:=v_week;
      end if;
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
          and prior.state not in ('CANCELLED','REJECTED','REFUSED','EXPIRED','SUPERSEDED')
          and (
            prior.state<>'FINALISED'
            or prior.target_timesheet_id is null
            or not exists(
              select 1
              from public.timesheets_financials prior_fin
              where prior_fin.timesheet_id=prior.target_timesheet_id
                and prior_fin.is_current=true
                and prior_fin.authorised_at_utc is not null
            )
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
      last_mutation_idempotency_key,created_at_utc,updated_at_utc
    ) values (
      p_workflow_id,v_environment,v_account_id,v_candidate_id,v_workflow_kind,
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
      nullif(v_payload->>'expected_row_signature',''),p_idempotency_key,p_idempotency_key,p_now_utc,p_now_utc
    ) returning * into v_workflow;
    perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,
      'CANDIDATE_WORKFLOW_CREATED',null,
      jsonb_build_object('kind',v_workflow.workflow_kind,'scope',v_workflow.scope,'route',v_workflow.route),
      null,null,p_idempotency_key,p_now_utc);
    return jsonb_build_object('ok',true,'idempotent_replay',false,'workflow_id',v_workflow.id,
      'state',v_workflow.state,'generation',v_workflow.generation,'policy',v_policy);
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
  if nullif(btrim(coalesce(p_idempotency_key,'')),'') is not null
     and v_workflow.last_mutation_idempotency_key=p_idempotency_key
     and v_workflow.last_mutation_response_json is not null then
    return v_workflow.last_mutation_response_json||jsonb_build_object('idempotent_replay',true);
  end if;
  if p_expected_generation is not null and v_workflow.generation<>p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','WORKFLOW_GENERATION_CONFLICT','current_generation',v_workflow.generation)::text;
  end if;
  v_next_generation:=v_workflow.generation+1;
  if v_workflow.contract_id is not null then
    select * into v_contract from public.contracts where id=v_workflow.contract_id;
    v_policy:=private._candidate_policy_resolve_v1(v_contract.client_id,v_contract.id,
      coalesce(v_workflow.week_ending_date,v_workflow.work_date,(p_now_utc at time zone 'Europe/London')::date));
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
      select source_component.*
      from public.candidate_submission_components source_component
      where source_component.workflow_id=v_workflow.id
        and source_component.workflow_generation=v_workflow.generation
        and source_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
        and source_component.state='IMMUTABLE'
        and source_component.source_content_sha256 is not null
      order by source_component.component_no,source_component.id
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
         or v_component.paper_return_page_key is distinct from v_paper_page_key then
        raise exception 'CANDIDATE_COMPONENT_PREPARE_IDEMPOTENCY_CONFLICT' using errcode='23505';
      end if;
      return jsonb_build_object('ok',true,'idempotent_replay',true,
        'component_id',v_component.id,'component_no',v_component.component_no,
        'workflow_generation',v_component.workflow_generation,
        'storage_key',v_component.storage_key,'media_type',v_component.media_type,
        'byte_size',v_component.byte_size,'component_kind',v_component.component_kind,
        'document_role',v_component.document_role,'expense_category',v_component.expense_category,
        'paper_return_page_key',v_component.paper_return_page_key,'state',v_component.state);
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
      required,review_ordinal,review_render_state,final_signed_render_state,paper_return_page_key,created_at_utc
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
      p_now_utc
    ) returning * into v_component;
    return jsonb_build_object('ok',true,'idempotent_replay',false,'component_id',v_component.id,
      'component_no',v_component.component_no,'workflow_generation',v_component.workflow_generation,
      'storage_key',v_component.storage_key,'media_type',v_component.media_type,
      'byte_size',v_component.byte_size,'component_kind',v_component.component_kind,
      'document_role',v_component.document_role,'expense_category',v_component.expense_category,
      'paper_return_page_key',v_component.paper_return_page_key,'state',v_component.state);
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
        return jsonb_build_object('ok',true,'idempotent_replay',true,
          'component_id',v_component.id,'state',v_component.state);
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
      immutable_at_utc=p_now_utc
    where id=v_component.id and state='PENDING' returning * into v_component;
    if not found then
      raise exception 'CANDIDATE_COMPONENT_COMPLETE_STATE_CONFLICT' using errcode='40001';
    end if;
    return jsonb_build_object('ok',true,'idempotent_replay',false,'component_id',v_component.id,
      'state',v_component.state,'immutable_at_utc',v_component.immutable_at_utc);
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
    return jsonb_build_object('ok',true,'component_id',v_component.id,'state',v_component.state);
  end if;

  if v_action='WORKER_SUBMIT' then
    if v_workflow.state<>'WORKER_DRAFT' then
      raise exception 'CANDIDATE_WORKFLOW_TRANSITION_INVALID' using errcode='55000';
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
      if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
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
      select * into v_week from public.contract_weeks where id=v_workflow.contract_week_id for update;
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
      where week_row.timesheet_id=v_workflow.anchor_timesheet_id
        and week_row.contract_id=v_workflow.contract_id
        and week_row.week_ending_date=v_workflow.week_ending_date
      for update;
      if not found then raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001'; end if;
      v_route_authority:=private._candidate_route_family_v1(v_workflow.anchor_timesheet_id,v_anchor_week.id);
      if not coalesce((v_route_authority->>'candidate_expenses_allowed')::boolean,false)
         or (v_workflow.route='PAPER' and not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))
         or (v_workflow.route<>'PAPER' and v_route_authority->>'route_family' not in ('ELECTRONIC','IMPORT_AUTHORITATIVE')) then
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
         or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,expenses_pay_ex_vat}','')::numeric,0))>0
         or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,expenses_charge_ex_vat}','')::numeric,0))>0 then
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
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and state in ('PENDING','APPROVED');

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
        v_review_ordinal:=v_review_ordinal+1;
        insert into public.candidate_submission_components(
          workflow_id,workflow_generation,component_no,timesheet_id,component_kind,document_role,state,
          immutable_at_utc,required,review_ordinal,review_render_state,final_signed_render_state,created_at_utc
        ) values (
          v_workflow.id,v_next_generation,v_component_no,v_workflow.target_timesheet_id,'EXPENSE_SUMMARY',
          'EXPENSE_MILEAGE_APPROVAL_SUMMARY','IMMUTABLE',p_now_utc,true,v_review_ordinal,
          'PENDING','PENDING',p_now_utc
        );

        for v_source_component in
          select source_component.*
          from public.candidate_submission_components source_component
          where source_component.workflow_id=v_workflow.id
            and source_component.workflow_generation=v_workflow.generation
            and source_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
            and source_component.state='IMMUTABLE'
            and source_component.source_content_sha256 is not null
          order by case source_component.component_kind when 'MILEAGE_FORM' then 0 else 1 end,
            case source_component.expense_category
              when 'ACCOMMODATION' then 1 when 'TRAVEL' then 2 when 'MILEAGE' then 3
              when 'OTHER' then 4 else 5 end,
            source_component.component_no,source_component.id
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
        and (required=true or document_role='MANAGER_SIGNATURE') and state<>'SUPERSEDED';

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
          select source_component.*
          from public.candidate_submission_components source_component
          where source_component.workflow_id=v_workflow.id
            and source_component.workflow_generation=v_workflow.generation
            and source_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
            and source_component.state='IMMUTABLE'
            and source_component.source_content_sha256 is not null
          order by case source_component.component_kind when 'MILEAGE_FORM' then 0 else 1 end,
            source_component.component_no,source_component.id
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
        and (required=true or document_role='MANAGER_SIGNATURE') and state<>'SUPERSEDED';
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
        return jsonb_build_object('ok',true,'idempotent_replay',true,
          'workflow_id',v_workflow.id,'generation',v_workflow.generation,
          'component_id',v_component.id,'state',v_workflow.state);
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
        p_now_utc,p_now_utc,p_now_utc+interval '24 hours',
        decode(v_manifest->>'manifest_sha256','hex'),v_component_ids,v_manifest->'required_components',
        nullif(v_manifest->>'manager_review_timesheet_component_id','')::uuid,
        case when nullif(v_manifest->>'manager_review_timesheet_sha256','') is null then null
          else decode(v_manifest->>'manager_review_timesheet_sha256','hex') end,
        p_idempotency_key,p_now_utc,p_now_utc
      ) returning * into v_approval;
      v_mail_id:=private._candidate_queue_mail_v1(v_payload->'mail',v_approval.manager_email_normalized,
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
      'approval_request_id',v_approval.id,'method',v_approval.method,
      'review_manifest_sha256',encode(v_approval.review_manifest_sha256,'hex'),
      'expires_at_utc',v_approval.expires_at_utc,'mail_outbox_id',v_mail_id);
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
        and method='PHONE' and state='PENDING' for update;
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
    where id=v_approval.id;
    return jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'workflow_generation',v_workflow.generation,'approval_request_id',v_approval.id,
      'method',v_approval.method,'expires_at_utc',v_approval.expires_at_utc,
      'manifest_sha256',encode(v_approval.review_manifest_sha256,'hex'),
      'page_count',coalesce((select sum(coalesce(c.review_page_count,1))
        from public.candidate_submission_components c where c.id=any(v_approval.required_component_ids)),0),
      'ordered_components',v_approval.required_component_manifest_json,
      'manager_identity_requirements',jsonb_build_object('name_required',true,'position_required',true,'signature_required',true),
      'can_approve',true,'can_refuse',true);

  elsif v_action='RECORD_REVIEW_PROGRESS' then
    if v_is_public_manager_action then
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and token_hash=v_token_hash for update;
    else
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
        and method='PHONE' and state='PENDING' for update;
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
      'component_id',v_component.id,'reviewed_count',v_reviewed_count,
      'required_count',cardinality(v_approval.required_component_ids));
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
        and method='PHONE' and state='PENDING' for update;
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
       or nullif(btrim(coalesce(v_payload->>'manager_position','')),'') is null then
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
      'approval_request_id',v_approval.id,'approved_at_utc',v_approval.approved_at_utc,
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
        return jsonb_build_object('ok',true,'idempotent_replay',true,
          'workflow_id',v_workflow.id,'generation',v_workflow.generation,
          'component_id',v_component.id,'state',v_workflow.state);
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
    v_daily_context:=private._candidate_daily_context_contract_v1(
      v_workflow.id,v_workflow.generation
    );
    v_daily_context_hash:=private._candidate_sha256_jsonb_v1(v_daily_context);
    if v_workflow.canonical_saved_at_utc is not null
       and v_workflow.canonical_save_input_sha256=v_expected_save_hash
       and v_workflow.canonical_save_financials_id is not null
       and nullif(btrim(coalesce(v_workflow.canonical_save_row_signature,'')),'') is not null then
      return jsonb_build_object(
        'ok',true,'idempotent_replay',true,'workflow_id',v_workflow.id,
        'generation',v_workflow.generation,'state',v_workflow.state,
        'canonical_save_registered',true,
        'canonical_save_input_sha256_hex',encode(v_expected_save_hash,'hex'),
        'canonical_context_sha256_hex',encode(v_workflow.daily_context_sha256,'hex'),
        'canonical_save_row_signature',v_workflow.canonical_save_row_signature,
        'canonical_save_financials_id',v_workflow.canonical_save_financials_id
      );
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
        and state='PENDING' for update;
    end if;
    if not found or v_approval.state<>'PENDING' then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
    end if;
    if v_approval.expires_at_utc<=p_now_utc then
      raise exception 'MANAGER_APPROVAL_REQUEST_EXPIRED' using errcode='28000';
    end if;
    if nullif(btrim(coalesce(v_payload->>'reason','')),'') is null then
      raise exception 'MANAGER_REFUSAL_REASON_REQUIRED' using errcode='22023';
    end if;
    update public.candidate_approval_requests set
      state='REFUSED',refusal_reason=btrim(v_payload->>'reason'),refused_at_utc=p_now_utc,
      updated_at_utc=p_now_utc where id=v_approval.id;
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and id<>v_approval.id and state='PENDING';
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,'state','REFUSED',
      'generation',v_workflow.generation,'rejection_scope','COMPLETE_ELECTRONIC_TRANSACTION');
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
    select * into v_approval from public.candidate_approval_requests
    where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and method='EMAIL' and state='PENDING' for update skip locked;
    if not found or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256 then
      raise exception 'MANAGER_REMINDER_NOT_ELIGIBLE' using errcode='55000';
    end if;
    if v_approval.expires_at_utc<=p_now_utc or v_approval.resend_count>=5
       or coalesce(v_approval.last_sent_at_utc,v_approval.initial_sent_at_utc)+interval '24 hours'>p_now_utc then
      raise exception 'MANAGER_REMINDER_NOT_ELIGIBLE' using errcode='55000';
    end if;
    update public.candidate_approval_requests set
      resend_count=resend_count+1,last_sent_at_utc=p_now_utc,
      next_reminder_at_utc=p_now_utc+interval '24 hours',updated_at_utc=p_now_utc
    where id=v_approval.id returning * into v_approval;
    v_mail_id:=private._candidate_queue_mail_v1(v_payload->'mail',v_approval.manager_email_normalized,
      'CANDIDATE_MANAGER_REMINDER_V1:'||v_approval.id::text||':'||v_approval.resend_count::text,
      'candidate-manager-reminder:'||v_approval.id::text||':'||v_approval.resend_count::text,
      v_workflow.id,p_now_utc);
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,'state',v_workflow.state,
      'generation',v_workflow.generation,'approval_request_id',v_approval.id,
      'resend_count',v_approval.resend_count,'next_reminder_at_utc',v_approval.next_reminder_at_utc,
      'mail_outbox_id',v_mail_id);
    update public.candidate_submission_workflows set
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action='RENEW' then
    if v_workflow.state<>'AWAITING_MANAGER_APPROVAL'
       or v_workflow.review_manifest_sha256 is null then
      raise exception 'MANAGER_APPROVAL_NOT_RENEWABLE' using errcode='55000';
    end if;
    select * into v_approval from public.candidate_approval_requests
    where workflow_id=v_workflow.id and method='EMAIL'
    order by request_generation desc,created_at_utc desc limit 1 for update;
    if not found or v_approval.state not in ('PENDING','EXPIRED','SUPERSEDED') then
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
      v_approval.manager_email_normalized,v_token_hash,p_now_utc+interval '7 days',p_now_utc,p_now_utc,
      p_now_utc+interval '24 hours',v_approval.renewal_count+1,v_approval.review_manifest_sha256,
      v_approval.required_component_ids,v_approval.required_component_manifest_json,
      v_approval.manager_review_timesheet_component_id,v_approval.manager_review_timesheet_sha256,
      p_idempotency_key,p_now_utc,p_now_utc
    ) returning * into v_approval;
    v_mail_id:=private._candidate_queue_mail_v1(v_payload->'mail',v_approval.manager_email_normalized,
      'CANDIDATE_MANAGER_RENEW_V1:'||v_approval.id::text,
      'candidate-manager-renew:'||v_approval.id::text,v_workflow.id,p_now_utc);
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state','AWAITING_MANAGER_APPROVAL','generation',v_workflow.generation,
      'approval_request_id',v_approval.id,'expires_at_utc',v_approval.expires_at_utc,
      'renewal_count',v_approval.renewal_count,'mail_outbox_id',v_mail_id);
    update public.candidate_submission_workflows set
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action='CANCEL_MANAGER_HANDOFF' then
    if v_workflow.state<>'AWAITING_MANAGER_APPROVAL' or v_workflow.route<>'PHONE' then
      raise exception 'MANAGER_PHONE_HANDOFF_NOT_CANCELLABLE' using errcode='55000';
    end if;
    select * into v_approval from public.candidate_approval_requests
    where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and method='PHONE' and state='PENDING'
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
      state='READY_FOR_MANAGER_APPROVAL',route='ELECTRONIC',
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id returning * into v_workflow;

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
       or encode(v_workflow.paper_return_manifest_sha256,'hex')<>v_paper_manifest_sha256 then
      raise exception 'CANDIDATE_PAPER_PROVIDER_WORKFLOW_STALE' using errcode='40001';
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
    if v_workflow.state in ('FINALISED','CANCELLED','REJECTED','SUPERSEDED') then
      raise exception 'CANDIDATE_WORKFLOW_NOT_CANCELLABLE' using errcode='55000';
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
    update public.candidate_approval_requests set
      state=case when v_action='CANCEL' then 'CANCELLED' else 'SUPERSEDED' end,
      cancelled_at_utc=case when v_action='CANCEL' then p_now_utc else cancelled_at_utc end,
      superseded_at_utc=case when v_action='SUPERSEDE' then p_now_utc else superseded_at_utc end,
      updated_at_utc=p_now_utc where workflow_id=v_workflow.id and state in ('PENDING','APPROVED');
    update public.candidate_submission_components set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,
      review_render_state=case when review_render_state='NOT_REQUIRED' then review_render_state else 'SUPERSEDED' end,
      final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED' then final_signed_render_state else 'SUPERSEDED' end
    where workflow_id=v_workflow.id and state not in ('SUPERSEDED','REJECTED');
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state',v_action||case when v_action='CANCEL' then 'LED' else 'D' end,
      'generation',v_next_generation);
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
    if v_workflow.workflow_kind='CONTRACT_EXPENSE' then
      select week_row.* into v_anchor_week
      from public.contract_weeks week_row
      where week_row.timesheet_id=v_workflow.anchor_timesheet_id
        and week_row.contract_id=v_workflow.contract_id
        and week_row.week_ending_date=v_workflow.week_ending_date;
      if not found then raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001'; end if;
      v_route_authority:=private._candidate_route_family_v1(v_workflow.anchor_timesheet_id,v_anchor_week.id);
    else
      v_route_authority:=private._candidate_route_family_v1(v_workflow.target_timesheet_id,v_workflow.contract_week_id);
    end if;
    if not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false) then
      raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000',detail=v_route_authority::text;
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
    from public.candidate_submission_components source_component
    where source_component.workflow_id=v_workflow.id
      and source_component.workflow_generation=v_workflow.generation
      and source_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
      and source_component.state='IMMUTABLE'
      and source_component.source_content_sha256 is not null;

    v_paper_manifest:=jsonb_build_object(
      'workflow_id',v_workflow.id,
      'workflow_generation',v_workflow.generation,
      'immutable_submission_sha256',encode(v_workflow.immutable_submission_sha256,'hex'),
      'pages',
        case when v_workflow.workflow_kind<>'CONTRACT_EXPENSE'
          then jsonb_build_array(jsonb_build_object(
            'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))
          else '[]'::jsonb end
        || case when v_workflow.workflow_kind in ('CONTRACT_COMBINED','CONTRACT_EXPENSE')
          then jsonb_build_array(jsonb_build_object(
            'page_key','EXPENSE_SUMMARY','component_kind','EXPENSE_SUMMARY'))
          else '[]'::jsonb end
        || v_paper_source_pages
    );
    update public.candidate_submission_workflows set
      state='AWAITING_PAPER_RETURN',route='PAPER',policy_snapshot_json=v_policy,
      paper_return_manifest_json=v_paper_manifest,
      paper_return_manifest_sha256=private._candidate_sha256_jsonb_v1(v_paper_manifest),
      updated_at_utc=p_now_utc where id=v_workflow.id returning * into v_workflow;

    -- The existing QR/document/email authority is composed inside this
    -- transaction. A PAPER workflow is not accepted unless its exact held
    -- email operation exists and is bound to this frozen manifest.
    execute
      'select public.timesheet_qr_send_enqueue_v1($1,$2,$3,$4,$5)'
      into v_paper_pack_result
      using v_paper_timesheet_id,v_paper_timesheet_id,null::uuid,
        p_idempotency_key||':paper-pack',p_now_utc;

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

  elsif v_action='PAPER_PACK_RELEASE' then
    if not v_is_service_action or p_session_id is not null then
      raise exception 'CANDIDATE_PAPER_PACK_RELEASE_SERVICE_REQUIRED' using errcode='28000';
    end if;
    if v_workflow.state<>'AWAITING_PAPER_RETURN' or v_workflow.route<>'PAPER' then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;
    if v_workflow.paper_return_manifest_sha256 is null
       or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
          is distinct from v_workflow.paper_return_manifest_sha256 then
      raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='55000';
    end if;

    v_paper_timesheet_id:=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id);
    v_paper_mail_id:=nullif(btrim(coalesce(v_payload->>'mail_outbox_id','')),'')::uuid;
    v_paper_manifest_sha256:=lower(btrim(coalesce(v_payload->>'paper_return_manifest_sha256','')));
    v_paper_pack_storage_key:=nullif(btrim(coalesce(v_payload->>'complete_pack_storage_key','')),'');
    v_paper_pack_sha256:=lower(btrim(coalesce(v_payload->>'complete_pack_sha256','')));
    v_paper_pack_media_type:=lower(btrim(coalesce(v_payload->>'complete_pack_media_type','')));
    v_paper_base_document_sha256:=lower(btrim(coalesce(v_payload->>'base_document_sha256','')));
    v_paper_branding_contract_sha256:=lower(btrim(coalesce(v_payload->>'branding_contract_sha256','')));
    v_paper_renderer_contract_version:=btrim(coalesce(v_payload->>'renderer_contract_version',''));
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
    if lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
         in ('true','t','1','yes') then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;

    v_paper_pack_attachment:=jsonb_build_array(jsonb_build_object(
      'r2_key',v_paper_pack_storage_key,
      'filename','Timesheet_'||coalesce(v_workflow.week_ending_date::text,v_paper_timesheet_id::text)||'.pdf',
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
          scheduled_for_utc=p_now_utc,
          next_attempt_at_utc=p_now_utc,
          payment_scope_json=candidate_paper_mail.payment_scope_json||jsonb_build_object(
            'candidate_paper_pack_ready',true,
            'mail_held_until_pdf_rendered',false,
            'mail_delayed_for_pdf_render',false,
            'mail_hold_reason',null,
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
      'complete_pack_storage_key',v_paper_pack_storage_key,
      'complete_pack_sha256',v_paper_pack_sha256,
      'complete_pack_byte_size',v_paper_pack_byte_size,
      'complete_pack_page_count',v_paper_pack_page_count,
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
    return jsonb_build_object('ok',true,
      'notification_id',nullif(v_payload->>'notification_id','')::uuid,'state','READ');
  else
    raise exception 'CANDIDATE_WORKFLOW_ACTION_INVALID'
      using errcode='22023',detail=jsonb_build_object(
        'code','CANDIDATE_WORKFLOW_ACTION_INVALID','action',v_action)::text;
  end if;

  perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,
    'CANDIDATE_WORKFLOW_'||v_action,
    jsonb_build_object('state',v_workflow.state,'generation',v_workflow.generation),
    v_response,null,null,p_idempotency_key,p_now_utc);
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
    elsif v_constraint_name='candidate_submission_components_paper_return_page_uq' then
      raise exception 'CANDIDATE_PAPER_RETURN_PAGE_DUPLICATE' using errcode='23505';
    end if;
    raise;
end;
$function$;

revoke all on function private._candidate_queue_mail_v1(jsonb,text,text,text,uuid,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_paper_delivery_retire_v1(uuid,integer,text,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_paper_source_workflow_context_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_paper_delivery_retire_set_v1(uuid[],integer[],text,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz)
  from public,anon,authenticated;
grant execute on function public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz)
  to service_role;
